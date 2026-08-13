# -*- coding: utf-8 -*-
"""Tests for CH-0-004 bidirectional JSON-RPC dispatch."""

from __future__ import annotations

import asyncio

import pytest

from qwenpaw.channel_protocol import (
    RpcError,
    RpcLimits,
    RpcPeer,
    RpcTimeoutError,
)


class MemoryTransport:
    """Small in-memory full-duplex transport for protocol tests."""

    def __init__(self) -> None:
        self.inbox: asyncio.Queue[str | None] = asyncio.Queue()
        self.peer: MemoryTransport | None = None
        self.closed = False

    async def send(self, message: str) -> None:
        """Deliver one complete framed message to the peer."""
        if self.closed or self.peer is None or self.peer.closed:
            raise ConnectionError("transport closed")
        await self.peer.inbox.put(message)

    async def receive(self) -> str:
        """Receive one complete framed message."""
        message = await self.inbox.get()
        if message is None:
            raise ConnectionError("transport closed")
        return message

    async def aclose(self) -> None:
        """Close this side and wake its peer."""
        if self.closed:
            return
        self.closed = True
        if self.peer is not None:
            await self.peer.inbox.put(None)


def _transport_pair() -> tuple[MemoryTransport, MemoryTransport]:
    """Create two linked memory transports."""
    left = MemoryTransport()
    right = MemoryTransport()
    left.peer = right
    right.peer = left
    return left, right


@pytest.mark.asyncio
async def test_nested_bidirectional_calls_and_out_of_order_responses() -> None:
    """Handlers can call back while the reader continues matching responses."""
    left_transport, right_transport = _transport_pair()
    core = RpcPeer(left_transport)
    runner = RpcPeer(right_transport)

    async def runner_inner(params: object, _: object) -> dict[str, object]:
        """Answer the nested request."""
        assert params == {"value": 2}
        return {"value": 3}

    async def core_outer(_: object, __: object) -> dict[str, object]:
        """Issue a reverse request from inside a request handler."""
        result = await core.call("runner.inner", {"value": 2})
        return {"nested": result["value"]}

    runner.register_method("runner.inner", runner_inner)
    core.register_method("core.outer", core_outer)
    await asyncio.gather(core.start(), runner.start())

    result = await runner.call("core.outer")
    assert result == {"nested": 3}
    await asyncio.gather(core.aclose(), runner.aclose())


@pytest.mark.asyncio
async def test_unknown_method_and_duplicate_response() -> None:
    """Unknown methods error and late responses are ignored."""
    left_transport, right_transport = _transport_pair()
    core = RpcPeer(left_transport)
    runner = RpcPeer(right_transport)
    await asyncio.gather(core.start(), runner.start())

    with pytest.raises(RpcError) as exc_info:
        await core.call("missing.method")
    assert exc_info.value.code == -32601

    await right_transport.send(
        '{"jsonrpc":"2.0","id":"rpc-999","result":null}',
    )
    await asyncio.sleep(0)
    assert core.duplicate_responses == 1
    await asyncio.gather(core.aclose(), runner.aclose())


@pytest.mark.asyncio
async def test_pending_limit_timeout_and_cancel_notification() -> None:
    """The peer bounds pending calls and emits cancellation on timeout."""
    left_transport, right_transport = _transport_pair()
    core = RpcPeer(
        left_transport,
        limits=RpcLimits(max_pending_requests=1, request_timeout=0.01),
    )
    runner = RpcPeer(right_transport)
    cancellations: list[object] = []

    async def remember_cancel(params: object, _: object) -> None:
        """Record cancellation notifications."""
        cancellations.append(params)

    async def never_respond(_: object, __: object) -> None:
        """Keep one request pending until the caller timeout."""
        await asyncio.Future()

    runner.register_notification("request.cancel", remember_cancel)
    runner.register_method("never.respond", never_respond)
    await asyncio.gather(core.start(), runner.start())

    pending = asyncio.create_task(core.call("never.respond"))
    await asyncio.sleep(0)
    with pytest.raises(RpcError):
        await core.call("second")
    with pytest.raises(RpcTimeoutError):
        await pending
    await asyncio.sleep(0)
    assert cancellations
    await asyncio.gather(core.aclose(), runner.aclose())
