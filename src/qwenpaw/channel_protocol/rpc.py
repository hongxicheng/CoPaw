# -*- coding: utf-8 -*-
"""Bidirectional JSON-RPC dispatcher over the Channel framed transport."""

from __future__ import annotations

import asyncio
import contextlib
import inspect
import json
from collections.abc import Callable, Mapping
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Any

from .errors import (
    ProtocolValidationError,
    RpcCancelledError,
    RpcClosedError,
    RpcError,
    RpcTimeoutError,
)
from .framing import FramedTransport
from .models import (
    CancelParams,
    RpcErrorObject,
    RpcMessage,
    RpcNotification,
    RpcRequest,
    RpcResponse,
    parse_rpc_message,
    validate_method_params,
)


JSONRPC_PARSE_ERROR = -32700
JSONRPC_INVALID_REQUEST = -32600
JSONRPC_METHOD_NOT_FOUND = -32601
JSONRPC_INVALID_PARAMS = -32602
JSONRPC_INTERNAL_ERROR = -32603


def _reject_non_finite(value: str) -> object:
    """Reject non-standard JSON numeric constants."""
    raise ValueError(f"non-finite JSON constant is not allowed: {value}")


def _strict_json_dumps(value: object) -> str:
    """Encode one JSON-RPC message using strict JSON numbers."""
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
    )


def _recover_request_id(
    raw: str,
) -> tuple[bool, str | int | None] | None:
    """Recover request-ID presence from a loosely parsed JSON object."""
    try:
        value = json.loads(raw)
    except (json.JSONDecodeError, TypeError, ValueError):
        return None
    if not isinstance(value, Mapping) or "id" not in value:
        return (False, None)
    request_id = value["id"]
    if isinstance(request_id, (str, int)) and not isinstance(request_id, bool):
        return True, request_id
    return True, None


RequestHandler = Callable[[Any, RpcRequest], Any]
NotificationHandler = Callable[[Any, RpcNotification], Any]


@dataclass(frozen=True)
class RpcLimits:
    """Limits for one JSON-RPC peer."""

    max_pending_requests: int = 64
    request_timeout: float = 30.0

    def __post_init__(self) -> None:
        """Reject disabled or invalid RPC limits."""
        if (
            not isinstance(self.max_pending_requests, int)
            or isinstance(self.max_pending_requests, bool)
            or self.max_pending_requests <= 0
        ):
            raise ValueError("max_pending_requests must be positive")
        if (
            not isinstance(self.request_timeout, (int, float))
            or isinstance(self.request_timeout, bool)
            or self.request_timeout <= 0
        ):
            raise ValueError("request_timeout must be positive")


@dataclass
class _PendingRequest:
    """One locally originated request and its completion future."""

    future: asyncio.Future[Any]
    timeout: float


@dataclass
class _RequestCancellation:
    """Track cancellation independently from task exception handling."""

    requested: bool = False


@dataclass
class _IncomingRequest:
    """Bind one inbound request task to its cancellation state."""

    task: asyncio.Task[Any]
    cancellation: _RequestCancellation


_CURRENT_CANCELLATION: ContextVar[_RequestCancellation | None] = ContextVar(
    "channel_protocol_request_cancellation",
    default=None,
)


def request_was_cancelled() -> bool:
    """Return whether the current request or task was cancelled."""
    cancellation = _CURRENT_CANCELLATION.get()
    if cancellation is not None and cancellation.requested:
        return True
    task = asyncio.current_task()
    return task is not None and task.cancelling() > 0


class RpcPeer:
    """Run a continuously reading, bidirectional JSON-RPC peer."""

    def __init__(
        self,
        transport: FramedTransport,
        *,
        limits: RpcLimits | None = None,
    ) -> None:
        self._transport = transport
        self._limits = limits or RpcLimits()
        self._handlers: dict[str, RequestHandler] = {}
        self._notification_handlers: dict[str, NotificationHandler] = {}
        self._pending: dict[str | int, _PendingRequest] = {}
        self._incoming: dict[str | int, _IncomingRequest] = {}
        self._request_counter = 0
        self._reader_task: asyncio.Task[None] | None = None
        self._closed = False
        self._close_lock = asyncio.Lock()
        self._started = asyncio.Event()
        self._duplicate_responses = 0

    @property
    def is_closed(self) -> bool:
        """Return whether the peer has stopped dispatching."""
        return self._closed

    @property
    def duplicate_responses(self) -> int:
        """Return the number of ignored duplicate or late responses."""
        return self._duplicate_responses

    def register_method(self, method: str, handler: RequestHandler) -> None:
        """Register a request handler for one method."""
        if not method or method in self._handlers:
            raise ValueError("method must be non-empty and unique")
        self._handlers[method] = handler

    def register_notification(
        self,
        method: str,
        handler: NotificationHandler,
    ) -> None:
        """Register a notification handler for one method."""
        if not method or method in self._notification_handlers:
            raise ValueError("method must be non-empty and unique")
        self._notification_handlers[method] = handler

    async def start(self) -> None:
        """Start the reader loop exactly once."""
        if self._reader_task is not None:
            return
        if self._closed:
            raise RpcClosedError("RPC peer is closed")
        self._reader_task = asyncio.create_task(self._reader_loop())
        self._started.set()

    async def wait_closed(self) -> None:
        """Wait until the reader loop has ended."""
        await self._started.wait()
        if self._reader_task is not None:
            with contextlib.suppress(asyncio.CancelledError, RpcClosedError):
                await self._reader_task

    def _next_request_id(self) -> str:
        """Return a process-local request identifier."""
        self._request_counter += 1
        return f"rpc-{self._request_counter}"

    async def call(
        self,
        method: str,
        params: Mapping[str, Any] | list[Any] | None = None,
        *,
        timeout: float | None = None,
    ) -> Any:
        """Send a request and await its matching response."""
        if self._closed:
            raise RpcClosedError("RPC peer is closed")
        if self._reader_task is None:
            await self.start()
        if len(self._pending) >= self._limits.max_pending_requests:
            raise RpcError(
                JSONRPC_INVALID_REQUEST,
                "pending request limit reached",
                data={"reason_code": "TEMPORARY_UNAVAILABLE"},
            )
        request_id = self._next_request_id()
        future = asyncio.get_running_loop().create_future()
        request_timeout = (
            timeout if timeout is not None else self._limits.request_timeout
        )
        if request_timeout <= 0:
            raise ValueError("timeout must be positive")
        self._pending[request_id] = _PendingRequest(future, request_timeout)
        try:
            await self._send(RpcRequest(request_id, method, params))
            return await asyncio.wait_for(future, timeout=request_timeout)
        except asyncio.TimeoutError as exc:
            if not future.done():
                future.cancel()
            await self.notify(
                "request.cancel",
                CancelParams(
                    request_id=request_id,
                    reason="timeout",
                ).to_mapping(),
            )
            raise RpcTimeoutError(f"request timed out: {method}") from exc
        except asyncio.CancelledError:
            if not future.done():
                future.cancel()
            with contextlib.suppress(RpcClosedError):
                await self.notify(
                    "request.cancel",
                    CancelParams(
                        request_id=request_id,
                        reason="caller_cancelled",
                    ).to_mapping(),
                )
            raise
        finally:
            self._pending.pop(request_id, None)

    async def notify(
        self,
        method: str,
        params: Mapping[str, Any] | list[Any] | None = None,
    ) -> None:
        """Send a notification without creating a pending request."""
        if self._closed:
            raise RpcClosedError("RPC peer is closed")
        if self._reader_task is None:
            await self.start()
        await self._send(RpcNotification(method, params))

    async def _send(self, message: RpcMessage) -> None:
        """Serialize and send one validated envelope."""
        try:
            encoded = _strict_json_dumps(message.to_mapping())
        except (TypeError, ValueError) as exc:
            raise ProtocolValidationError(
                "RPC message contains non-JSON data",
                reason_code="SCHEMA_MISMATCH",
            ) from exc
        await self._transport.send(encoded)

    async def _reader_loop(self) -> None:
        """Read frames and dispatch without waiting for handlers."""
        try:
            while not self._closed:
                raw = await self._transport.receive()
                await self._dispatch_raw(raw)
        except (RpcClosedError, asyncio.CancelledError):
            pass
        except Exception:
            await self._fail_pending()
        finally:
            await self.aclose()

    async def _dispatch_raw(self, raw: str) -> None:
        """Parse one JSON frame and schedule its work."""
        try:
            value = json.loads(raw, parse_constant=_reject_non_finite)
            message = parse_rpc_message(value)
        except json.JSONDecodeError as exc:
            recovered = _recover_request_id(raw)
            if recovered is None or recovered[0]:
                await self._send_error(
                    recovered[1] if recovered is not None else None,
                    JSONRPC_PARSE_ERROR,
                    "Parse error",
                    allow_null_id=True,
                )
            _ = exc
            return
        except ProtocolValidationError as exc:
            request_id = (
                value.get("id") if isinstance(value, Mapping) else None
            )
            if isinstance(request_id, (str, int)) and not isinstance(
                request_id,
                bool,
            ):
                await self._send_error(
                    request_id,
                    JSONRPC_INVALID_PARAMS,
                    str(exc),
                    data={"reason_code": exc.reason_code},
                )
            return
        except ValueError as exc:
            recovered = _recover_request_id(raw)
            if recovered is None or recovered[0]:
                await self._send_error(
                    recovered[1] if recovered is not None else None,
                    JSONRPC_PARSE_ERROR,
                    "Parse error",
                    allow_null_id=True,
                )
            _ = exc
            return
        if isinstance(message, RpcResponse):
            self._resolve_response(message)
            return
        if isinstance(message, RpcNotification):
            if message.method == "request.cancel":
                self._cancel_incoming(message.params)
            task = asyncio.create_task(self._dispatch_notification(message))
            task.add_done_callback(self._consume_task_exception)
            return
        cancellation = _RequestCancellation()
        task = asyncio.create_task(
            self._dispatch_request(message, cancellation),
        )
        self._incoming[message.id] = _IncomingRequest(
            task=task,
            cancellation=cancellation,
        )
        task.add_done_callback(self._request_callback(message.id))

    def _request_callback(
        self,
        request_id: str | int,
    ) -> Callable[[asyncio.Task[Any]], None]:
        """Build a typed callback for one inbound request task."""

        def callback(completed: asyncio.Task[Any]) -> None:
            self._request_done(request_id, completed)

        return callback

    def _resolve_response(self, response: RpcResponse) -> None:
        """Complete one pending call and ignore late duplicate responses."""
        pending = self._pending.get(response.id)
        if pending is None:
            self._duplicate_responses += 1
            return
        if pending.future.done():
            self._duplicate_responses += 1
            return
        if response.error is not None:
            pending.future.set_exception(
                RpcError(
                    response.error.code,
                    response.error.message,
                    data=response.error.data,
                ),
            )
        else:
            pending.future.set_result(response.result)

    async def _dispatch_request(
        self,
        request: RpcRequest,
        cancellation: _RequestCancellation,
    ) -> None:
        """Run one request handler and return exactly one response."""
        token = _CURRENT_CANCELLATION.set(cancellation)
        try:
            handler = self._handlers.get(request.method)
            if handler is None:
                await self._send_error(
                    request.id,
                    JSONRPC_METHOD_NOT_FOUND,
                    "method not found",
                    data={"reason_code": "METHOD_NOT_FOUND"},
                )
                return
            params = validate_method_params(request.method, request.params)
            result = handler(params, request)
            if inspect.isawaitable(result):
                result = await result
            if cancellation.requested:
                raise asyncio.CancelledError
            await self._send(RpcResponse(request.id, result=result))
        except ProtocolValidationError as exc:
            await self._send_error(
                request.id,
                JSONRPC_INVALID_PARAMS,
                str(exc),
                data={"reason_code": exc.reason_code, "path": list(exc.path)},
            )
        except RpcError as exc:
            await self._send_error(
                request.id,
                exc.code,
                exc.message,
                data=exc.data,
            )
        except asyncio.CancelledError:
            await self._send_error(
                request.id,
                JSONRPC_INTERNAL_ERROR,
                "request cancelled",
                data={"reason_code": "REQUEST_CANCELLED"},
            )
        except Exception as exc:
            await self._send_error(
                request.id,
                JSONRPC_INTERNAL_ERROR,
                "internal error",
                data={"reason_code": "INTERNAL_ERROR"},
            )
            _ = exc
        finally:
            _CURRENT_CANCELLATION.reset(token)

    async def _dispatch_notification(
        self,
        notification: RpcNotification,
    ) -> None:
        """Run one notification handler without sending a response."""
        handler = self._notification_handlers.get(notification.method)
        if handler is None:
            return
        try:
            params = validate_method_params(
                notification.method,
                notification.params,
            )
            result = handler(params, notification)
            if inspect.isawaitable(result):
                await result
        except Exception as exc:
            _ = exc

    def _cancel_incoming(self, params: object) -> None:
        """Cancel a running inbound request by protocol request ID."""
        try:
            cancel = CancelParams.from_mapping(params)
        except ProtocolValidationError:
            return
        incoming = self._incoming.get(cancel.request_id)
        if incoming is not None and not incoming.task.done():
            incoming.cancellation.requested = True
            incoming.task.cancel()
            return

        pending = self._pending.get(cancel.request_id)
        if pending is not None and not pending.future.done():
            pending.future.set_exception(
                RpcCancelledError(
                    f"request cancelled: {cancel.reason or 'unspecified'}",
                ),
            )

    async def _send_error(
        self,
        request_id: str | int | None,
        code: int,
        message: str,
        *,
        data: object = None,
        allow_null_id: bool = False,
    ) -> None:
        """Send one JSON-RPC error response when an ID is available."""
        if (request_id is None and not allow_null_id) or self._closed:
            return
        with contextlib.suppress(RpcClosedError):
            await self._send(
                RpcResponse(
                    request_id,
                    error=RpcErrorObject(code, message, data),
                ),
            )

    async def _fail_pending(self) -> None:
        """Fail all locally pending calls after a transport error."""
        for pending in self._pending.values():
            if not pending.future.done():
                pending.future.set_exception(
                    RpcClosedError("RPC peer stopped"),
                )

    def _request_done(
        self,
        request_id: str | int,
        task: asyncio.Task[Any],
    ) -> None:
        """Remove a completed inbound request task."""
        self._incoming.pop(request_id, None)
        self._consume_task_exception(task)

    @staticmethod
    def _consume_task_exception(task: asyncio.Task[Any]) -> None:
        """Consume callback exceptions to avoid unhandled-task warnings."""
        with contextlib.suppress(asyncio.CancelledError, Exception):
            task.result()

    async def aclose(self) -> None:
        """Close the peer and resolve every pending operation."""
        async with self._close_lock:
            if self._closed:
                return
            self._closed = True
            current = asyncio.current_task()
            for incoming in self._incoming.values():
                task = incoming.task
                if task is not current and not task.done():
                    incoming.cancellation.requested = True
                    task.cancel()
            self._incoming.clear()
            await self._fail_pending()
            with contextlib.suppress(Exception):
                await self._transport.aclose()


__all__ = [
    "JSONRPC_INVALID_PARAMS",
    "JSONRPC_INVALID_REQUEST",
    "JSONRPC_INTERNAL_ERROR",
    "JSONRPC_METHOD_NOT_FOUND",
    "JSONRPC_PARSE_ERROR",
    "request_was_cancelled",
    "RpcLimits",
    "RpcPeer",
]
