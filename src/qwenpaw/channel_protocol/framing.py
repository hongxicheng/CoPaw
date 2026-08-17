# -*- coding: utf-8 -*-
"""Strict LSP Content-Length framing and bounded asynchronous transport."""

from __future__ import annotations

import asyncio
import contextlib
import inspect
import math
from collections.abc import Awaitable
from dataclasses import dataclass
from typing import Any, Callable

from .errors import (
    FrameClosedError,
    FrameEOFError,
    FrameError,
    FrameLimitError,
    FrameProtocolError,
    FrameTimeoutError,
    FrameWriteError,
)


_HEADER_SEPARATOR = b"\r\n\r\n"
_HEADER_PREFIX = b"content-length:"
_DEFAULT_MAX_FRAME_BYTES = 1024 * 1024
_DEFAULT_MAX_HEADER_BYTES = 8 * 1024
_DEFAULT_READ_TIMEOUT = 30.0
_DEFAULT_WRITE_TIMEOUT = 10.0
_DEFAULT_WRITE_QUEUE_SIZE = 64


@dataclass(frozen=True)
class FramingLimits:
    """Safety limits for one stdio framing transport."""

    max_frame_bytes: int = _DEFAULT_MAX_FRAME_BYTES
    max_header_bytes: int = _DEFAULT_MAX_HEADER_BYTES
    read_timeout: float = _DEFAULT_READ_TIMEOUT
    write_timeout: float = _DEFAULT_WRITE_TIMEOUT
    write_queue_size: int = _DEFAULT_WRITE_QUEUE_SIZE

    def __post_init__(self) -> None:
        """Reject disabled or nonsensical safety limits."""
        integer_fields = (
            "max_frame_bytes",
            "max_header_bytes",
            "write_queue_size",
        )
        for field in integer_fields:
            value = getattr(self, field)
            if (
                not isinstance(value, int)
                or isinstance(value, bool)
                or value <= 0
            ):
                raise ValueError(f"{field} must be positive")
        for field in ("read_timeout", "write_timeout"):
            value = getattr(self, field)
            if (
                not isinstance(value, (int, float))
                or isinstance(value, bool)
                or not math.isfinite(value)
                or value <= 0
            ):
                raise ValueError(f"{field} must be positive and finite")


def encode_frame(
    message: str,
    *,
    limits: FramingLimits | None = None,
) -> bytes:
    """Encode one non-empty UTF-8 text message using LSP framing."""
    selected = limits or FramingLimits()
    if not isinstance(message, str) or not message:
        raise FrameProtocolError("frame message must be non-empty text")
    try:
        body = message.encode("utf-8", "strict")
    except UnicodeEncodeError as exc:
        raise FrameProtocolError("frame message is not valid UTF-8") from exc
    if len(body) > selected.max_frame_bytes:
        raise FrameLimitError("frame body exceeds maximum length")
    header = f"Content-Length: {len(body)}\r\n\r\n".encode("ascii")
    if len(header) > selected.max_header_bytes:
        raise FrameLimitError("frame header exceeds maximum length")
    return header + body


class FrameReader:
    """Read strict Content-Length frames from an asyncio byte stream."""

    def __init__(
        self,
        reader: asyncio.StreamReader,
        *,
        limits: FramingLimits | None = None,
    ) -> None:
        self._reader = reader
        self._limits = limits or FramingLimits()
        self._closed = False

    async def _read_chunk(self, size: int, deadline: float) -> bytes:
        """Read bytes before a frame's single absolute deadline."""
        remaining = deadline - asyncio.get_running_loop().time()
        if remaining <= 0:
            raise FrameTimeoutError("frame read timed out")
        try:
            return await asyncio.wait_for(
                self._reader.read(size),
                timeout=remaining,
            )
        except asyncio.TimeoutError as exc:
            raise FrameTimeoutError("frame read timed out") from exc

    async def _read_header(self) -> tuple[bytes, float]:
        """Read a CRLF-terminated header without unbounded buffering."""
        first = await self._reader.read(1)
        if not first:
            raise FrameEOFError("stdio reached EOF at frame boundary")
        deadline = (
            asyncio.get_running_loop().time() + self._limits.read_timeout
        )
        header = bytearray(first)
        while not header.endswith(_HEADER_SEPARATOR):
            if len(header) >= self._limits.max_header_bytes:
                raise FrameLimitError("frame header exceeds maximum length")
            chunk = await self._read_chunk(1, deadline)
            if not chunk:
                raise FrameProtocolError("truncated frame header")
            header.extend(chunk)
        if len(header) > self._limits.max_header_bytes:
            raise FrameLimitError("frame header exceeds maximum length")
        return bytes(header[:-4]), deadline

    def _parse_header(self, header: bytes) -> int:
        """Validate the v1 header grammar and return the declared length."""
        lines = header.split(b"\r\n")
        if len(lines) != 1:
            raise FrameProtocolError("only Content-Length header is allowed")
        line = lines[0]
        name, separator, value = line.partition(b":")
        if not separator or name.lower() != _HEADER_PREFIX[:-1]:
            raise FrameProtocolError("invalid Content-Length header")
        if not value or value[:1] not in b" \t":
            raise FrameProtocolError("invalid Content-Length header")
        value = value.strip(b" \t")
        if not value.isdigit() or value.startswith(b"0") and len(value) > 1:
            raise FrameProtocolError("invalid Content-Length value")
        length = 0
        for digit in value:
            length = length * 10 + digit - ord("0")
            if length > self._limits.max_frame_bytes:
                raise FrameLimitError("frame body exceeds maximum length")
        if length <= 0:
            raise FrameProtocolError("Content-Length must be positive")
        return length

    async def read_message(self) -> str:
        """Read and strictly decode one message, retaining sticky bytes."""
        if self._closed:
            raise FrameClosedError("frame reader is closed")
        try:
            header, deadline = await self._read_header()
            length = self._parse_header(header)
            try:
                body = await asyncio.wait_for(
                    self._reader.readexactly(length),
                    timeout=max(
                        0,
                        deadline - asyncio.get_running_loop().time(),
                    ),
                )
            except asyncio.IncompleteReadError as exc:
                raise FrameProtocolError("truncated frame body") from exc
            except asyncio.TimeoutError as exc:
                raise FrameTimeoutError("frame read timed out") from exc
            try:
                return body.decode("utf-8", "strict")
            except UnicodeDecodeError as exc:
                raise FrameProtocolError(
                    "frame body is not valid UTF-8",
                ) from exc
        except FrameError as exc:
            if not isinstance(exc, FrameEOFError):
                self._closed = True
            raise


@dataclass
class _WriteItem:
    """One queued encoded frame and its completion future."""

    data: bytes
    future: asyncio.Future[None]
    prepare_write: Callable[[], str | Awaitable[str]] | None = None
    on_write_succeeded: Callable[[], None] | None = None
    on_write_failed: Callable[[], None] | None = None


class FramedTransport:
    """Bidirectional framing transport with one bounded writer task."""

    def __init__(
        self,
        reader: asyncio.StreamReader,
        writer: Any,
        *,
        limits: FramingLimits | None = None,
    ) -> None:
        self._limits = limits or FramingLimits()
        self._reader = FrameReader(reader, limits=self._limits)
        self._writer = writer
        self._queue: asyncio.Queue[_WriteItem] = asyncio.Queue(
            maxsize=self._limits.write_queue_size,
        )
        self._pending_writes: set[asyncio.Future[None]] = set()
        self._closed = False
        self._closed_event = asyncio.Event()
        self._close_lock = asyncio.Lock()
        self._write_lock = asyncio.Lock()
        self._writer_task = asyncio.create_task(self._write_loop())

    @property
    def is_closed(self) -> bool:
        """Return whether the complete transport has been closed."""
        return self._closed

    async def _prepare_write_item(self, item: _WriteItem) -> bytes:
        """Build an optional final frame before writer visibility."""
        if item.prepare_write is None:
            return item.data
        message = item.prepare_write()
        if inspect.isawaitable(message):
            message = await message
        if not isinstance(message, str):
            raise FrameProtocolError("prepared frame must be text")
        return encode_frame(message, limits=self._limits)

    @staticmethod
    def _rollback_write_item(item: _WriteItem) -> None:
        """Rollback one prepared item before transport cleanup."""
        if item.on_write_failed is None:
            return
        with contextlib.suppress(Exception):
            item.on_write_failed()

    async def _write_item(self, item: _WriteItem) -> None:
        """Process one queued item under the single writer lock."""
        prepared = False
        visible = False
        try:
            async with self._write_lock:
                if item.future.done():
                    return
                prepared = item.prepare_write is not None
                data = await self._prepare_write_item(item)
                if item.future.done():
                    return
                self._writer.write(data)
                visible = True
                if item.on_write_succeeded is not None:
                    item.on_write_succeeded()
                await asyncio.wait_for(
                    self._writer.drain(),
                    timeout=self._limits.write_timeout,
                )
        finally:
            if prepared and not visible:
                self._rollback_write_item(item)

    async def _fail_write_item(
        self,
        item: _WriteItem,
        error: FrameError,
    ) -> None:
        """Fail one caller and close the complete transport."""
        if not item.future.done():
            item.future.set_exception(error)
        await self._close(error)

    async def _handle_write_error(
        self,
        item: _WriteItem,
        error: BaseException,
    ) -> None:
        """Translate one writer exception into the framing error model."""
        if isinstance(error, asyncio.CancelledError):
            if not item.future.done():
                item.future.set_exception(
                    FrameClosedError("stdio writer stopped"),
                )
            raise error
        if isinstance(error, asyncio.TimeoutError):
            await self._fail_write_item(
                item,
                FrameTimeoutError("frame write timed out"),
            )
            return
        if isinstance(error, FrameError):
            await self._fail_write_item(item, error)
            return
        if isinstance(error, (BrokenPipeError, ConnectionError, OSError)):
            await self._fail_write_item(
                item,
                FrameWriteError("stdio write failed"),
            )
            return
        await self._fail_write_item(
            item,
            FrameWriteError("stdio write preparation failed"),
        )

    async def _write_loop(self) -> None:
        """Serialize writes and resolve each caller's completion future."""
        try:
            while True:
                item = await self._queue.get()
                try:
                    await self._write_item(item)
                except BaseException as error:
                    await self._handle_write_error(item, error)
                    return
                else:
                    if not item.future.done():
                        item.future.set_result(None)
                finally:
                    self._queue.task_done()
        except asyncio.CancelledError:
            return

    async def send(
        self,
        message: str,
        *,
        prepare_write: Callable[[], str | Awaitable[str]] | None = None,
        on_write_succeeded: Callable[[], None] | None = None,
        on_write_failed: Callable[[], None] | None = None,
    ) -> None:
        """Enqueue one frame and wait until the OS stream accepts it."""
        if self._closed:
            raise FrameClosedError("stdio transport is closed")
        data = encode_frame(message, limits=self._limits)
        future: asyncio.Future[
            None
        ] = asyncio.get_running_loop().create_future()
        item = _WriteItem(
            data,
            future,
            prepare_write,
            on_write_succeeded,
            on_write_failed,
        )
        self._pending_writes.add(future)
        put_task = asyncio.create_task(self._queue.put(item))
        close_task = asyncio.create_task(self._closed_event.wait())
        try:
            done, _ = await asyncio.wait(
                {put_task, close_task},
                timeout=self._limits.write_timeout,
                return_when=asyncio.FIRST_COMPLETED,
            )
            if not done:
                raise asyncio.TimeoutError
            if put_task not in done:
                raise FrameClosedError("stdio transport is closed")
            await future
        except asyncio.TimeoutError as exc:
            if not future.done():
                future.cancel()
            await self._close(exc)
            raise FrameTimeoutError("frame write queue timed out") from exc
        except FrameClosedError:
            if future.done():
                with contextlib.suppress(FrameError, asyncio.CancelledError):
                    future.result()
            raise
        except asyncio.CancelledError:
            if not future.done():
                future.cancel()
            raise
        finally:
            put_task.cancel()
            close_task.cancel()
            await asyncio.gather(
                put_task,
                close_task,
                return_exceptions=True,
            )
            self._pending_writes.discard(future)
            if future.done():
                with contextlib.suppress(FrameError, asyncio.CancelledError):
                    future.result()

    async def receive(self) -> str:
        """Read one frame and close both directions on any transport error."""
        if self._closed:
            raise FrameClosedError("stdio transport is closed")
        read_task = asyncio.create_task(self._reader.read_message())
        close_task = asyncio.create_task(self._closed_event.wait())
        try:
            done, _ = await asyncio.wait(
                {read_task, close_task},
                return_when=asyncio.FIRST_COMPLETED,
            )
            if close_task in done or self._closed:
                raise FrameClosedError("stdio transport is closed")
            return await read_task
        except FrameEOFError as exc:
            await self._close(exc)
            raise FrameClosedError("stdio peer closed") from exc
        except (FrameProtocolError, FrameTimeoutError) as exc:
            await self._close(exc)
            raise
        finally:
            read_task.cancel()
            close_task.cancel()
            await asyncio.gather(
                read_task,
                close_task,
                return_exceptions=True,
            )

    async def _close(self, reason: BaseException | None = None) -> None:
        """Close both halves and fail every queued or active write."""
        async with self._close_lock:
            if self._closed:
                return
            self._closed = True
            self._closed_event.set()
            error = reason if isinstance(reason, FrameError) else None
            if not isinstance(error, FrameClosedError):
                error = FrameClosedError("stdio transport is closed")
            while not self._queue.empty():
                item = self._queue.get_nowait()
                if not item.future.done():
                    item.future.set_exception(error)
                self._queue.task_done()
            for future in self._pending_writes:
                if not future.done():
                    future.set_exception(error)
            with contextlib.suppress(Exception):
                self._writer.close()
            with contextlib.suppress(Exception, asyncio.TimeoutError):
                await asyncio.wait_for(
                    self._writer.wait_closed(),
                    timeout=self._limits.write_timeout,
                )
            current = asyncio.current_task()
            if self._writer_task is not current:
                self._writer_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await self._writer_task

    async def aclose(self) -> None:
        """Explicitly close the complete stdio transport."""
        await self._close()
