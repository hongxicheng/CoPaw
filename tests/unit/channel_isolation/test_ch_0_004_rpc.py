# -*- coding: utf-8 -*-
"""Tests for CH-0-004 bidirectional JSON-RPC dispatch."""

from __future__ import annotations

import asyncio
import json
import math

import pytest

from qwenpaw.channel_protocol import (
    ProtocolValidationError,
    CoreLifecycleAdapter,
    LifecycleController,
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
    await asyncio.sleep(0.05)
    assert core.duplicate_responses == 1
    await asyncio.gather(core.aclose(), runner.aclose())


@pytest.mark.asyncio
async def test_null_id_error_never_resolves_a_pending_request() -> None:
    """An uncorrelated JSON-RPC error cannot complete a normal call."""
    left_transport, right_transport = _transport_pair()
    core = RpcPeer(left_transport)
    runner = RpcPeer(right_transport)
    started = asyncio.Event()

    async def never_respond(_: object, __: object) -> None:
        """Keep one correlated request pending for the null-ID response."""
        started.set()
        await asyncio.Future()

    runner.register_method("never.respond", never_respond)
    await asyncio.gather(core.start(), runner.start())
    pending = asyncio.create_task(core.call("never.respond", timeout=1.0))
    await asyncio.wait_for(started.wait(), timeout=1.0)
    await right_transport.send(
        '{"jsonrpc":"2.0","id":null,"error":'
        '{"code":-32700,"message":"Parse error"}}',
    )
    await asyncio.sleep(0)
    assert not pending.done()
    assert core.duplicate_responses == 1
    pending.cancel()
    with pytest.raises(asyncio.CancelledError):
        await pending
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
    await asyncio.sleep(0.05)
    with pytest.raises(RpcError):
        await core.call("second")
    with pytest.raises(RpcTimeoutError):
        await pending
    await asyncio.sleep(0)
    assert cancellations
    await asyncio.gather(core.aclose(), runner.aclose())


@pytest.mark.asyncio
async def test_explicit_cancel_notification_cancels_incoming_request() -> None:
    """A caller can explicitly cancel a running request."""
    left_transport, right_transport = _transport_pair()
    core = RpcPeer(left_transport)
    runner = RpcPeer(right_transport)
    cancelled = asyncio.Event()
    started = asyncio.Event()

    async def never_respond(_: object, __: object) -> None:
        """Keep the inbound request running until cancellation."""
        started.set()
        try:
            await asyncio.Future()
        except asyncio.CancelledError:
            cancelled.set()
            raise

    runner.register_method("never.respond", never_respond)
    await asyncio.gather(core.start(), runner.start())
    request = asyncio.create_task(
        core.call("never.respond", timeout=1.0),
    )
    await asyncio.wait_for(started.wait(), timeout=1.0)
    await core.notify(
        "request.cancel",
        {"request_id": "rpc-1", "reason": "user_cancelled"},
    )
    await asyncio.wait_for(cancelled.wait(), timeout=1.0)
    with pytest.raises(RpcError) as cancelled_error:
        await request
    assert cancelled_error.value.data["reason_code"] == "REQUEST_CANCELLED"
    await asyncio.gather(core.aclose(), runner.aclose())


@pytest.mark.asyncio
async def test_protocol_and_schema_errors_are_stable() -> None:
    """Protocol mismatch and invalid DTOs expose stable reason codes."""
    left_transport, right_transport = _transport_pair()
    core = RpcPeer(left_transport)
    runner = RpcPeer(right_transport)
    runner.register_method("channel.health", lambda params, _: params)
    await asyncio.gather(core.start(), runner.start())
    with pytest.raises(RpcError) as unknown:
        await core.call("missing.method")
    assert unknown.value.data["reason_code"] == "METHOD_NOT_FOUND"
    with pytest.raises(RpcError) as schema_error:
        await core.call("channel.health", {"unexpected": True})
    assert schema_error.value.data["reason_code"] == "SCHEMA_MISMATCH"
    await asyncio.gather(core.aclose(), runner.aclose())


@pytest.mark.asyncio
async def test_protocol_mismatch_returns_rpc_error_envelope() -> None:
    """An incompatible hello is rejected through bidirectional RPC."""
    left_transport, right_transport = _transport_pair()
    core = RpcPeer(left_transport)
    runner = RpcPeer(right_transport)
    controller = LifecycleController(
        channel_key="voice",
        instance_id="instance-1",
        generation=7,
        environment_spec_id="ches1_" + "1" * 64,
        environment_id="ches1_" + "1" * 64 + ".install1_" + "2" * 32,
        protocol_min=2,
        protocol_max=2,
    )
    CoreLifecycleAdapter(controller).register_rpc_methods(core)
    await asyncio.gather(core.start(), runner.start())
    hello = {
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
        "capabilities": [],
    }
    with pytest.raises(RpcError) as mismatch:
        await runner.call("runner.hello", hello)
    assert mismatch.value.code == -32010
    assert mismatch.value.data["reason_code"] == "PROTOCOL_MISMATCH"
    await asyncio.gather(core.aclose(), runner.aclose())


@pytest.mark.asyncio
async def test_rpc_strict_json_rejects_non_finite_values() -> None:
    """RPC serialization and parsing reject non-standard JSON numbers."""
    left_transport, right_transport = _transport_pair()
    core = RpcPeer(left_transport)
    runner = RpcPeer(right_transport)
    handled = False

    async def echo(params: object, _: object) -> object:
        """Record that a strict-invalid request was not dispatched."""
        nonlocal handled
        handled = True
        return params

    runner.register_method("echo", echo)
    await asyncio.gather(core.start(), runner.start())
    with pytest.raises(ProtocolValidationError) as outbound:
        await core.call("echo", {"score": math.inf})
    assert outbound.value.reason_code == "SCHEMA_MISMATCH"
    await asyncio.gather(core.aclose(), runner.aclose())

    input_transport, output_transport = _transport_pair()
    parser = RpcPeer(output_transport)
    parser.register_method("echo", echo)
    await parser.start()
    await input_transport.send(
        '{"jsonrpc":"2.0","id":"bad","method":"echo",'
        '"params":{"score":NaN}}',
    )
    response = json.loads(await input_transport.receive())
    assert response["id"] == "bad"
    assert response["error"]["code"] == -32700
    assert handled is False
    await input_transport.send("not-json")
    response = json.loads(await input_transport.receive())
    assert response["id"] is None
    assert response["error"]["code"] == -32700
    await parser.aclose()
