# -*- coding: utf-8 -*-
"""Shared fixtures for CH-0-004 lifecycle and outbound tests."""

from __future__ import annotations

import asyncio
import inspect
import threading
from collections.abc import Awaitable, Callable

from qwenpaw.channel_protocol import (
    EndpointParams,
    FramedTransport,
    HelloParams,
    HostStateStore,
    InboundEvent,
    LeaseParams,
    LifecycleController,
    PrepareParams,
)


class MemoryTransport:
    """Small in-memory full-duplex transport for lifecycle tests."""

    def __init__(self) -> None:
        self.inbox: asyncio.Queue[str | None] = asyncio.Queue()
        self.peer: MemoryTransport | None = None
        self.closed = False
        self.sent_messages: list[str] = []

    async def send(
        self,
        message: str,
        *,
        prepare_write: Callable[[], str | Awaitable[str]] | None = None,
        on_write_succeeded: Callable[[], None] | None = None,
        on_write_failed: Callable[[], None] | None = None,
        on_write_deferred: Callable[[], None] | None = None,
    ) -> None:
        """Deliver one message to the peer."""
        _ = on_write_failed, on_write_deferred
        if self.closed or self.peer is None or self.peer.closed:
            raise ConnectionError("transport closed")
        if prepare_write is not None:
            message = prepare_write()
            if inspect.isawaitable(message):
                message = await message
        self.sent_messages.append(message)
        self.peer.inbox.put_nowait(message)
        if on_write_succeeded is not None:
            on_write_succeeded()

    async def receive(self) -> str:
        """Receive one message from the peer."""
        message = await self.inbox.get()
        if message is None:
            raise ConnectionError("transport closed")
        return message

    async def aclose(self) -> None:
        """Close this side and wake the peer."""
        if self.closed:
            return
        self.closed = True
        if self.peer is not None:
            await self.peer.inbox.put(None)


class BlockingSendResponseTransport(MemoryTransport):
    """Block the acknowledged response for one outbound RPC request."""

    def __init__(self, request_id: str) -> None:
        super().__init__()
        self.request_id = request_id
        self.response_started = asyncio.Event()
        self.release_response = asyncio.Event()

    async def send(
        self,
        message: str,
        *,
        prepare_write: Callable[[], str | Awaitable[str]] | None = None,
        on_write_succeeded: Callable[[], None] | None = None,
        on_write_failed: Callable[[], None] | None = None,
        on_write_deferred: Callable[[], None] | None = None,
    ) -> None:
        """Pause only the selected successful response publication."""
        if (
            f'"id":"{self.request_id}"' in message
            and '"state":"acknowledged"' in message
        ):
            self.response_started.set()
            await self.release_response.wait()
        await super().send(
            message,
            prepare_write=prepare_write,
            on_write_succeeded=on_write_succeeded,
            on_write_failed=on_write_failed,
            on_write_deferred=on_write_deferred,
        )


class VisibleBlockingSendResponseTransport(MemoryTransport):
    """Block after the selected response is visible to the peer."""

    def __init__(self, request_id: str) -> None:
        super().__init__()
        self.request_id = request_id
        self.response_started = asyncio.Event()
        self.release_response = asyncio.Event()

    async def send(
        self,
        message: str,
        *,
        prepare_write: Callable[[], str | Awaitable[str]] | None = None,
        on_write_succeeded: Callable[[], None] | None = None,
        on_write_failed: Callable[[], None] | None = None,
        on_write_deferred: Callable[[], None] | None = None,
    ) -> None:
        """Deliver the selected response before blocking send completion."""
        selected = (
            f'"id":"{self.request_id}"' in message
            and '"state":"acknowledged"' in message
        )
        await super().send(
            message,
            prepare_write=prepare_write,
            on_write_succeeded=on_write_succeeded,
            on_write_failed=on_write_failed,
            on_write_deferred=on_write_deferred,
        )
        if selected:
            self.response_started.set()
            await self.release_response.wait()


class FailingSendResponseTransport(MemoryTransport):
    """Fail the acknowledged response for one outbound RPC request."""

    def __init__(self, request_id: str) -> None:
        super().__init__()
        self.request_id = request_id
        self.response_failed = asyncio.Event()

    async def send(
        self,
        message: str,
        *,
        prepare_write: Callable[[], str | Awaitable[str]] | None = None,
        on_write_succeeded: Callable[[], None] | None = None,
        on_write_failed: Callable[[], None] | None = None,
        on_write_deferred: Callable[[], None] | None = None,
    ) -> None:
        """Raise once the selected successful response is published."""
        if (
            f'"id":"{self.request_id}"' in message
            and '"state":"acknowledged"' in message
        ):
            self.response_failed.set()
            if prepare_write is not None:
                prepared = prepare_write()
                if inspect.isawaitable(prepared):
                    await prepared
            if on_write_failed is not None:
                on_write_failed()
            raise ConnectionError("response write failed")
        await super().send(
            message,
            prepare_write=prepare_write,
            on_write_succeeded=on_write_succeeded,
            on_write_failed=on_write_failed,
            on_write_deferred=on_write_deferred,
        )


class LinkedFrameWriter:
    """Expose frames to a peer reader before controllable drain returns."""

    def __init__(self, peer_reader: asyncio.StreamReader) -> None:
        self.peer_reader = peer_reader
        self.closed = False
        self.block_next = False
        self.block_request_id: str | None = None
        self.frame_visible = asyncio.Event()
        self.drain_release = asyncio.Event()
        self.drain_release.set()
        self._current_write_blocked = False

    def block_next_write(self) -> None:
        """Block drain for the next accepted frame."""
        self.block_next = True
        self.block_request_id = None
        self.frame_visible = asyncio.Event()
        self.drain_release = asyncio.Event()

    def block_response(self, request_id: str) -> None:
        """Block drain after one selected response becomes visible."""
        self.block_next = False
        self.block_request_id = request_id
        self.frame_visible = asyncio.Event()
        self.drain_release = asyncio.Event()

    def write(self, data: bytes) -> None:
        """Make one frame visible before the matching drain call."""
        if self.closed:
            raise BrokenPipeError("writer closed")
        marker = (
            self.block_request_id is not None
            and f'"id":"{self.block_request_id}"'.encode() in data
        )
        self._current_write_blocked = self.block_next or marker
        self.block_next = False
        if marker:
            self.block_request_id = None
        self.peer_reader.feed_data(data)
        if self._current_write_blocked:
            self.frame_visible.set()

    async def drain(self) -> None:
        """Hold the selected write after its bytes are peer-visible."""
        if self._current_write_blocked:
            await self.drain_release.wait()
            self._current_write_blocked = False

    def close(self) -> None:
        """Close the linked reader and release any blocked drain."""
        if self.closed:
            return
        self.closed = True
        self.drain_release.set()
        self.peer_reader.feed_eof()

    async def wait_closed(self) -> None:
        """Match the asyncio writer shutdown API."""


class RejectingPipeHandle:
    """Reject a Windows pipe frame without accepting any bytes."""

    def __init__(self) -> None:
        self.closed = False

    def write(self, _data: bytes) -> int:
        """Report a zero-progress synchronous HANDLE write."""
        return 0

    def close(self) -> None:
        """Record closure of the protocol handle."""
        self.closed = True


class LateSuccessfulPipeHandle:
    """Accept one Windows frame only after the write deadline expires."""

    def __init__(self) -> None:
        self.data = bytearray()
        self.started = threading.Event()
        self.release = threading.Event()
        self.closed = False

    def write(self, data: bytes) -> int:
        """Block in the writer thread, then accept the complete frame."""
        self.started.set()
        self.release.wait(timeout=1)
        self.data.extend(data)
        return len(data)

    def close(self) -> None:
        """Record closure without changing the pending write result."""
        self.closed = True


class LateRejectingLinkedPipeHandle:
    """Reject one delayed frame while forwarding all earlier frames."""

    def __init__(
        self,
        loop: asyncio.AbstractEventLoop,
        peer_reader: asyncio.StreamReader,
        marker: bytes,
    ) -> None:
        self._loop = loop
        self._peer_reader = peer_reader
        self._marker = marker
        self.started = threading.Event()
        self.release = threading.Event()
        self.closed = False

    def write(self, data: bytes) -> int:
        """Delay and reject the selected frame without exposing its bytes."""
        frame = bytes(data)
        if self._marker in frame:
            self.started.set()
            self.release.wait(timeout=1)
            return 0
        self._loop.call_soon_threadsafe(self._peer_reader.feed_data, frame)
        return len(data)

    def close(self) -> None:
        """Close the linked reader after the writer thread exits."""
        if self.closed:
            return
        self.closed = True
        self._loop.call_soon_threadsafe(self._peer_reader.feed_eof)


class FakeWindowsThreadHandle:
    """Provide the cancellable thread-handle surface for one test."""

    def __init__(self) -> None:
        self.closed = False

    def cancel(self) -> None:
        """Match cancellation of an already completed fake write."""

    def close(self) -> None:
        """Record closure of the fake thread handle."""
        self.closed = True


def _transport_pair() -> tuple[MemoryTransport, MemoryTransport]:
    """Create two linked memory transports."""
    left = MemoryTransport()
    right = MemoryTransport()
    left.peer = right
    right.peer = left
    return left, right


def _framed_transport_pair() -> (
    tuple[
        FramedTransport,
        FramedTransport,
        LinkedFrameWriter,
        LinkedFrameWriter,
    ]
):
    """Create linked framed transports with observable writer boundaries."""
    left_reader = asyncio.StreamReader()
    right_reader = asyncio.StreamReader()
    left_writer = LinkedFrameWriter(right_reader)
    right_writer = LinkedFrameWriter(left_reader)
    return (
        FramedTransport(left_reader, left_writer),
        FramedTransport(right_reader, right_writer),
        left_writer,
        right_writer,
    )


class Clock:
    """Deterministic millisecond clock for lease tests."""

    def __init__(self) -> None:
        self.now = 1000

    def __call__(self) -> int:
        """Return current fake time."""
        return self.now


class BlockingHostStateStore(HostStateStore):
    """Block one mutation to deterministically exercise lifecycle fencing."""

    def __init__(self) -> None:
        super().__init__()
        self.started = asyncio.Event()
        self.release = asyncio.Event()

    async def put(self, key: str, schema_version: int, value: object) -> None:
        """Pause mutation until the test releases the store."""
        self.started.set()
        await self.release.wait()
        await super().put(key, schema_version, value)


class RecordingRpcPeer:
    """Record registered RPC methods without exposing RpcPeer internals."""

    def __init__(self) -> None:
        self.methods: set[str] = set()

    def register_method(self, method: str, _: object) -> None:
        """Record one uniquely owned request method."""
        assert method not in self.methods
        self.methods.add(method)


def _identity(generation: int = 7) -> dict[str, object]:
    """Return a valid control identity fixture."""
    return {
        "channel_key": "voice",
        "instance_id": "instance-1",
        "generation": generation,
    }


def _response_event(handle: str = "response-1") -> InboundEvent:
    """Return one inbound event with a request-scoped response handle."""
    return InboundEvent.from_mapping(
        {
            **_identity(),
            "event_id": "event-1",
            "event_kind": "message.received",
            "conversation": {"id": "chat-1", "type": "group"},
            "sender_id": "sender-1",
            "acl_sender_id": "sender-1",
            "sender_name": "Sender",
            "content_parts": [{"type": "text", "text": "hello"}],
            "metadata": {},
            "response_handle": handle,
        },
    )


def _hello() -> HelloParams:
    """Return a valid handshake fixture."""
    return HelloParams.from_mapping(
        {
            "protocol_min": 1,
            "protocol_max": 1,
            "qwenpaw_version": "0.1",
            "channel_key": "voice",
            "instance_id": "instance-1",
            "environment_spec_id": "ches1_" + "1" * 64,
            "environment_id": "ches1_" + "1" * 64 + ".install1_" + "2" * 32,
            "lock_sha256": "0" * 64,
            "python_abi": "cp313-cp313",
            "platform_tag": "macosx_11_0_arm64",
            "capabilities": [
                "approval_card",
                "host_state",
                "ingress_endpoint",
                "media",
                "reaction",
                "response_lifecycle",
                "streaming",
            ],
        },
    )


def _endpoint() -> EndpointParams:
    """Return one loopback Runner-owned endpoint fixture."""
    return EndpointParams.from_mapping(
        {
            **_identity(),
            "protocol": "http",
            "host": "127.0.0.1",
            "port": 8080,
            "path": "/voice",
            "public_base_url": None,
            "readiness": "ready",
            "bound_externally": False,
            "auth_required": False,
            "quiescing": False,
        },
    )


def _controller(clock: Clock) -> LifecycleController:
    """Create a controller matching the hello fixture."""
    return LifecycleController(
        channel_key="voice",
        instance_id="instance-1",
        generation=7,
        environment_spec_id="ches1_" + "1" * 64,
        environment_id="ches1_" + "1" * 64 + ".install1_" + "2" * 32,
        capabilities=(
            "approval_card",
            "host_state",
            "ingress_endpoint",
            "media",
            "reaction",
            "response_lifecycle",
            "streaming",
        ),
        clock_ms=clock,
    )


async def activate_outbound_controller(
    controller: LifecycleController,
    capabilities: list[str],
) -> None:
    """Prepare and commit one controller with selected capabilities."""
    controller.accept_hello(_hello())
    await controller.prepare(
        PrepareParams.from_mapping(
            {
                **_identity(),
                "host_context": {},
                "capabilities": capabilities,
            },
        ),
    )
    lease = LeaseParams.from_mapping(
        {**_identity(), "lease_token": "outbound", "lease_ttl_ms": 100},
    )
    await controller.activate(lease)
    await controller.commit(lease)
