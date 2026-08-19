# -*- coding: utf-8 -*-
"""Tests for CH-0-004 outbound delivery and publication state."""

# pylint: disable=protected-access

from __future__ import annotations

import asyncio
import json
from typing import Any

import pytest

from qwenpaw.channel_protocol import (
    CoreLifecycleAdapter,
    FramedTransport,
    FramingLimits,
    IdentityParams,
    LeaseParams,
    LifecycleController,
    PrepareParams,
    ProtocolValidationError,
    QuiesceParams,
    ReactionParams,
    RpcClosedError,
    RpcError,
    RpcPeer,
    RpcTimeoutError,
    RunnerState,
    SendParams,
    runner_bootstrap,
)
from tests.unit.channel_isolation._ch_0_004_support import (
    BlockingSendResponseTransport,
    Clock,
    FailingSendResponseTransport,
    FakeWindowsThreadHandle,
    LateRejectingLinkedPipeHandle,
    LateSuccessfulPipeHandle,
    LinkedFrameWriter,
    MemoryTransport,
    RejectingPipeHandle,
    VisibleBlockingSendResponseTransport,
    _controller,
    _framed_transport_pair,
    _hello,
    _identity,
    _transport_pair,
)


async def _activate_outbound_controller(
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


async def _active_outbound_rpc_pair(
    right_transport: MemoryTransport,
    capabilities: list[str],
    *,
    clock: Clock | None = None,
) -> tuple[LifecycleController, RpcPeer, RpcPeer]:
    """Create one active RPC pair for outbound publication tests."""
    left_transport = MemoryTransport()
    left_transport.peer = right_transport
    right_transport.peer = left_transport
    core = RpcPeer(left_transport)
    runner = RpcPeer(right_transport)
    controller = LifecycleController(
        channel_key="voice",
        instance_id="instance-1",
        generation=7,
        environment_spec_id="ches1_" + "1" * 64,
        environment_id="ches1_" + "1" * 64 + ".install1_" + "2" * 32,
        capabilities=tuple(capabilities),
        clock_ms=clock or Clock(),
    )
    controller.register_rpc_methods(runner)
    CoreLifecycleAdapter(controller).register_rpc_methods(core)
    await asyncio.gather(core.start(), runner.start())
    await runner.call("runner.hello", _hello().to_mapping())
    await core.call(
        "channel.prepare",
        {
            **_identity(),
            "host_context": {},
            "capabilities": capabilities,
        },
    )
    lease = LeaseParams.from_mapping(
        {**_identity(), "lease_token": "publication", "lease_ttl_ms": 100},
    )
    await core.call("channel.activate", lease.to_mapping())
    await core.call("channel.commit", lease.to_mapping())
    return controller, core, runner


async def _active_framed_outbound_rpc_pair(
    capabilities: list[str],
    *,
    clock: Clock | None = None,
) -> tuple[
    LifecycleController,
    RpcPeer,
    RpcPeer,
    LinkedFrameWriter,
    LinkedFrameWriter,
]:
    """Create an active RPC pair over real framed transports."""
    (
        left_transport,
        right_transport,
        left_writer,
        right_writer,
    ) = _framed_transport_pair()
    core = RpcPeer(left_transport)
    runner = RpcPeer(right_transport)
    controller = LifecycleController(
        channel_key="voice",
        instance_id="instance-1",
        generation=7,
        environment_spec_id="ches1_" + "1" * 64,
        environment_id="ches1_" + "1" * 64 + ".install1_" + "2" * 32,
        capabilities=tuple(capabilities),
        clock_ms=clock or Clock(),
    )
    controller.register_rpc_methods(runner)
    CoreLifecycleAdapter(controller).register_rpc_methods(core)
    await asyncio.gather(core.start(), runner.start())
    await runner.call("runner.hello", _hello().to_mapping())
    await core.call(
        "channel.prepare",
        {
            **_identity(),
            "host_context": {},
            "capabilities": capabilities,
        },
    )
    lease = LeaseParams.from_mapping(
        {**_identity(), "lease_token": "publication", "lease_ttl_ms": 100},
    )
    await core.call("channel.activate", lease.to_mapping())
    await core.call("channel.commit", lease.to_mapping())
    return controller, core, runner, left_writer, right_writer


async def test_outbound_operations_enforce_capabilities_and_order() -> None:
    """Outbound targets, sequences, capabilities, and reactions are stable."""
    clock = Clock()
    controller = _controller(clock)
    await _activate_outbound_controller(
        controller,
        ["approval_card", "reaction", "streaming"],
    )
    approval = SendParams.from_mapping(
        {
            **_identity(),
            "delivery_id": "approval-1",
            "to_handle": "chat-1",
            "operation": "message.create",
            "content_parts": [{"type": "text", "text": "Approve?"}],
            "approval": {
                "request_id": "request-1",
                "tool_name": "shell",
                "severity": "high",
            },
        },
    )
    assert (await controller.send(approval))["state"] == "acknowledged"
    stream_start = SendParams.from_mapping(
        {
            **_identity(),
            "delivery_id": "stream-1",
            "to_handle": "chat-1",
            "operation": "stream.start",
            "stream_type": "message",
            "sequence": 0,
            "accumulated_text": "",
        },
    )
    await controller.send(stream_start)
    premature_reaction = ReactionParams.from_mapping(
        {
            **_identity(),
            "delivery_id": "reaction-premature",
            "to_handle": "chat-1",
            "target_delivery_id": "stream-1",
            "reaction": "completed",
        },
    )
    assert (await controller.reaction(premature_reaction))[
        "state"
    ] == "acknowledged"
    sequence_gap = SendParams.from_mapping(
        {
            **_identity(),
            "delivery_id": "delta-gap",
            "to_handle": "chat-1",
            "operation": "stream.delta",
            "target_delivery_id": "stream-1",
            "stream_type": "message",
            "sequence": 2,
            "accumulated_text": "gap",
        },
    )
    with pytest.raises(RpcError) as gap:
        await controller.send(sequence_gap)
    assert gap.value.data["reason_code"] == "OUTBOUND_ORDER_VIOLATION"
    wrong_type = SendParams.from_mapping(
        {
            **_identity(),
            "delivery_id": "delta-reasoning",
            "to_handle": "chat-1",
            "operation": "stream.delta",
            "target_delivery_id": "stream-1",
            "stream_type": "reasoning",
            "sequence": 1,
            "accumulated_text": "wrong",
        },
    )
    with pytest.raises(RpcError) as mismatch:
        await controller.send(wrong_type)
    assert mismatch.value.data["reason_code"] == "OUTBOUND_ORDER_VIOLATION"
    for delivery_id, operation, sequence, text in (
        ("delta-1", "stream.delta", 1, "h"),
        ("update-1", "message.update", 2, "hello"),
        ("end-1", "stream.end", 3, "hello"),
    ):
        mapping: dict[str, object] = {
            **_identity(),
            "delivery_id": delivery_id,
            "to_handle": "chat-1",
            "operation": operation,
            "target_delivery_id": "stream-1",
            "sequence": sequence,
        }
        if operation == "message.update":
            mapping["content_parts"] = [{"type": "text", "text": text}]
        else:
            mapping["stream_type"] = "message"
            mapping["accumulated_text"] = text
        await controller.send(SendParams.from_mapping(mapping))
    reaction = ReactionParams.from_mapping(
        {
            **_identity(),
            "delivery_id": "reaction-1",
            "to_handle": "chat-1",
            "target_delivery_id": "stream-1",
            "reaction": "completed",
        },
    )
    assert (await controller.reaction(reaction))["state"] == "acknowledged"
    late_delta = SendParams.from_mapping(
        {
            **_identity(),
            "delivery_id": "delta-late",
            "to_handle": "chat-1",
            "operation": "stream.delta",
            "target_delivery_id": "stream-1",
            "stream_type": "message",
            "sequence": 4,
            "accumulated_text": "late",
        },
    )
    with pytest.raises(RpcError) as ended:
        await controller.send(late_delta)
    assert ended.value.data["reason_code"] == "OUTBOUND_ORDER_VIOLATION"
    unknown = ReactionParams.from_mapping(
        {
            **_identity(),
            "delivery_id": "reaction-unknown",
            "to_handle": "chat-1",
            "target_delivery_id": "missing",
            "reaction": "completed",
        },
    )
    with pytest.raises(RpcError) as missing:
        await controller.reaction(unknown)
    assert missing.value.data["reason_code"] == "OUTBOUND_TARGET_UNKNOWN"


async def test_outbound_capability_bindings_use_effective_set() -> None:
    """Stream, approval, and reaction require their selected capability."""
    clock = Clock()
    without_features = _controller(clock)
    await _activate_outbound_controller(without_features, ["media"])
    stream_start = SendParams.from_mapping(
        {
            **_identity(),
            "delivery_id": "stream-1",
            "to_handle": "chat-1",
            "operation": "stream.start",
            "stream_type": "message",
            "sequence": 0,
            "accumulated_text": "",
        },
    )
    with pytest.raises(RpcError) as stream_capability:
        await without_features.send(stream_start)
    assert stream_capability.value.data == {
        "reason_code": "CAPABILITY_REQUIRED",
        "capability": "streaming",
    }
    approval = SendParams.from_mapping(
        {
            **_identity(),
            "delivery_id": "approval-1",
            "to_handle": "chat-1",
            "operation": "message.create",
            "content_parts": [{"type": "text", "text": "Approve?"}],
            "approval": {
                "request_id": "request-1",
                "tool_name": "shell",
                "severity": "high",
            },
        },
    )
    with pytest.raises(RpcError) as approval_capability:
        await without_features.send(approval)
    assert approval_capability.value.data == {
        "reason_code": "CAPABILITY_REQUIRED",
        "capability": "approval_card",
    }
    plain = SendParams.from_mapping(
        {
            **_identity(),
            "delivery_id": "plain-1",
            "to_handle": "chat-1",
            "content_parts": [{"type": "text", "text": "plain"}],
        },
    )
    await without_features.send(plain)
    unsupported_reaction = ReactionParams.from_mapping(
        {
            **_identity(),
            "delivery_id": "plain-reaction",
            "to_handle": "chat-1",
            "target_delivery_id": "plain-1",
            "reaction": "completed",
        },
    )
    with pytest.raises(RpcError) as reaction_capability:
        await without_features.reaction(unsupported_reaction)
    assert reaction_capability.value.data == {
        "reason_code": "CAPABILITY_REQUIRED",
        "capability": "reaction",
    }


async def test_stop_fences_inflight_outbound_without_waiting() -> None:
    """Stop returns without waiting for an unbounded platform handler."""
    clock = Clock()
    started = asyncio.Event()
    release = asyncio.Event()

    async def blocked_send(_: SendParams) -> dict[str, str]:
        """Pause one platform side effect until the test releases it."""
        started.set()
        await release.wait()
        return {
            "delivery_id": "blocked-send",
            "state": "acknowledged",
        }

    controller = LifecycleController(
        channel_key="voice",
        instance_id="instance-1",
        generation=7,
        environment_spec_id="ches1_" + "1" * 64,
        environment_id="ches1_" + "1" * 64 + ".install1_" + "2" * 32,
        capabilities=("media",),
        send_handler=blocked_send,
        clock_ms=clock,
    )
    await _activate_outbound_controller(controller, ["media"])
    send = asyncio.create_task(
        controller.send(
            SendParams.from_mapping(
                {
                    **_identity(),
                    "delivery_id": "blocked-send",
                    "to_handle": "chat-1",
                    "content_parts": [{"type": "text", "text": "hello"}],
                },
            ),
        ),
    )
    await started.wait()
    stop = asyncio.create_task(
        controller.stop(IdentityParams.from_mapping(_identity())),
    )
    await asyncio.sleep(0)
    assert stop.done()
    assert (await stop)["state"] == "stopped"
    with pytest.raises(asyncio.CancelledError):
        await send
    assert controller._outbound.delivery_state("blocked-send").value == (
        "unknown"
    )
    release.set()
    with pytest.raises(RpcError) as stopped:
        await controller.send(
            SendParams.from_mapping(
                {
                    **_identity(),
                    "delivery_id": "late-send",
                    "to_handle": "chat-1",
                    "content_parts": [{"type": "text", "text": "late"}],
                },
            ),
        )
    assert stopped.value.data["reason_code"] == "INVALID_STATE_TRANSITION"


@pytest.mark.asyncio
async def test_lease_expiry_linearizes_after_inflight_send() -> None:
    """Lease expiry cannot accept a result from an expired generation."""
    clock = Clock()
    started = asyncio.Event()
    release = asyncio.Event()

    async def blocked_send(params: SendParams) -> dict[str, object]:
        """Pause one platform attempt while the lease expires."""
        started.set()
        await release.wait()
        return {
            "delivery_id": params.delivery_id,
            "state": "acknowledged",
        }

    controller = LifecycleController(
        channel_key="voice",
        instance_id="instance-1",
        generation=7,
        environment_spec_id="ches1_" + "1" * 64,
        environment_id="ches1_" + "1" * 64 + ".install1_" + "2" * 32,
        capabilities=("media",),
        send_handler=blocked_send,
        clock_ms=clock,
    )
    await _activate_outbound_controller(controller, ["media"])
    params = SendParams.from_mapping(
        {
            **_identity(),
            "delivery_id": "expiring-send",
            "to_handle": "chat-1",
            "content_parts": [{"type": "text", "text": "hello"}],
        },
    )
    sending = asyncio.create_task(controller.send(params))
    await started.wait()
    clock.now = 1100
    health = asyncio.create_task(
        controller.health(IdentityParams.from_mapping(_identity())),
    )
    await asyncio.sleep(0)
    assert health.done()
    assert (await health)["state"] == "failed"
    release.set()
    result = await sending
    assert result == {
        "delivery_id": "expiring-send",
        "state": "unknown",
        "reason_code": "LEASE_EXPIRED",
        "retryable": False,
    }
    with pytest.raises(RpcError) as duplicate:
        await controller.send(params)
    assert duplicate.value.data["reason_code"] == "INVALID_STATE_TRANSITION"


@pytest.mark.asyncio
async def test_cancelled_and_timed_out_send_ids_cannot_repeat() -> None:
    """Cancelled RPC attempts keep immutable delivery IDs occupied."""
    for mode in ("cancel", "timeout"):
        clock = Clock()
        started = asyncio.Event()
        calls = 0

        async def blocked_send(
            params: SendParams,
            started_event: asyncio.Event = started,
        ) -> dict[str, object]:
            """Wait until RPC cancellation interrupts the platform attempt."""
            nonlocal calls
            calls += 1
            started_event.set()
            await asyncio.Future()
            return {
                "delivery_id": params.delivery_id,
                "state": "acknowledged",
            }

        left_transport, right_transport = _transport_pair()
        core = RpcPeer(left_transport)
        runner = RpcPeer(right_transport)
        controller = LifecycleController(
            channel_key="voice",
            instance_id="instance-1",
            generation=7,
            environment_spec_id="ches1_" + "1" * 64,
            environment_id=("ches1_" + "1" * 64 + ".install1_" + "2" * 32),
            capabilities=("media",),
            send_handler=blocked_send,
            clock_ms=clock,
        )
        controller.register_rpc_methods(runner)
        CoreLifecycleAdapter(controller).register_rpc_methods(core)
        await asyncio.gather(core.start(), runner.start())
        await runner.call("runner.hello", _hello().to_mapping())
        await core.call(
            "channel.prepare",
            {
                **_identity(),
                "host_context": {},
                "capabilities": ["media"],
            },
        )
        lease = LeaseParams.from_mapping(
            {**_identity(), "lease_token": "rpc", "lease_ttl_ms": 100},
        )
        await core.call("channel.activate", lease.to_mapping())
        await core.call("channel.commit", lease.to_mapping())
        payload = {
            **_identity(),
            "delivery_id": f"{mode}-send",
            "to_handle": "chat-1",
            "content_parts": [{"type": "text", "text": "hello"}],
        }
        request = asyncio.create_task(
            core.call(
                "channel.send",
                payload,
                timeout=0.01 if mode == "timeout" else 1.0,
            ),
        )
        await asyncio.wait_for(started.wait(), timeout=1.0)
        if mode == "cancel":
            await core.notify(
                "request.cancel",
                {"request_id": "rpc-4", "reason": "user_cancelled"},
            )
            with pytest.raises(RpcError):
                await request
        else:
            with pytest.raises(RpcTimeoutError):
                await request
        with pytest.raises(RpcError) as duplicate:
            await core.call("channel.send", payload)
        assert duplicate.value.data["reason_code"] == (
            "OUTBOUND_ORDER_VIOLATION"
        )
        assert calls == 1
        await asyncio.gather(core.aclose(), runner.aclose())


@pytest.mark.asyncio
async def test_timely_lease_renewal_is_not_blocked_by_slow_send() -> None:
    """A renewal received before expiry proceeds during platform I/O."""
    clock = Clock()
    started = asyncio.Event()
    release = asyncio.Event()

    async def blocked_send(params: SendParams) -> dict[str, object]:
        """Hold platform I/O across the original lease deadline."""
        started.set()
        await release.wait()
        return {
            "delivery_id": params.delivery_id,
            "state": "acknowledged",
        }

    controller = LifecycleController(
        channel_key="voice",
        instance_id="instance-1",
        generation=7,
        environment_spec_id="ches1_" + "1" * 64,
        environment_id="ches1_" + "1" * 64 + ".install1_" + "2" * 32,
        capabilities=("media",),
        send_handler=blocked_send,
        clock_ms=clock,
    )
    await _activate_outbound_controller(controller, ["media"])
    sending = asyncio.create_task(
        controller.send(
            SendParams.from_mapping(
                {
                    **_identity(),
                    "delivery_id": "renewed-send",
                    "to_handle": "chat-1",
                    "content_parts": [{"type": "text", "text": "hello"}],
                },
            ),
        ),
    )
    await started.wait()
    clock.now = 1050
    renewed = await controller.lease_renew(
        LeaseParams.from_mapping(
            {
                **_identity(),
                "lease_token": "outbound",
                "lease_ttl_ms": 100,
            },
        ),
    )
    assert renewed["lease_expires_at_ms"] == 1150
    clock.now = 1110
    release.set()
    assert (await sending)["state"] == "acknowledged"
    assert controller.state is RunnerState.ACTIVE


@pytest.mark.asyncio
async def test_send_handler_can_call_reverse_host_state_rpc() -> None:
    """Platform handlers can call Core-owned methods without lock cycles."""
    clock = Clock()
    left_transport, right_transport = _transport_pair()
    core = RpcPeer(left_transport)
    runner = RpcPeer(right_transport)

    async def send_with_state(params: SendParams) -> dict[str, object]:
        """Write Core-owned state before acknowledging the platform send."""
        await runner.call(
            "host.state.put",
            {
                **_identity(),
                "key": "reverse-send",
                "value": {"delivery_id": params.delivery_id},
            },
        )
        return {
            "delivery_id": params.delivery_id,
            "state": "acknowledged",
        }

    controller = LifecycleController(
        channel_key="voice",
        instance_id="instance-1",
        generation=7,
        environment_spec_id="ches1_" + "1" * 64,
        environment_id="ches1_" + "1" * 64 + ".install1_" + "2" * 32,
        capabilities=("host_state",),
        send_handler=send_with_state,
        clock_ms=clock,
    )
    controller.register_rpc_methods(runner)
    adapter = CoreLifecycleAdapter(controller)
    adapter.register_rpc_methods(core)
    await asyncio.gather(core.start(), runner.start())
    await runner.call("runner.hello", _hello().to_mapping())
    await core.call(
        "channel.prepare",
        {
            **_identity(),
            "host_context": {},
            "capabilities": ["host_state"],
        },
    )
    lease = LeaseParams.from_mapping(
        {**_identity(), "lease_token": "reverse", "lease_ttl_ms": 100},
    )
    await core.call("channel.activate", lease.to_mapping())
    await core.call("channel.commit", lease.to_mapping())
    result = await core.call(
        "channel.send",
        {
            **_identity(),
            "delivery_id": "reverse-send",
            "to_handle": "chat-1",
            "content_parts": [{"type": "text", "text": "hello"}],
        },
    )
    assert result["state"] == "acknowledged"
    assert await adapter.host_state_store.get("reverse-send") == (
        1,
        {"delivery_id": "reverse-send"},
    )
    await asyncio.gather(core.aclose(), runner.aclose())


@pytest.mark.asyncio
async def test_quiesce_honors_zero_drain_timeout() -> None:
    """Quiesce closes admission and returns at its declared deadline."""
    clock = Clock()
    started = asyncio.Event()

    async def blocked_send(_: SendParams) -> dict[str, object]:
        """Model platform work that has no natural completion deadline."""
        started.set()
        await asyncio.Future()
        return {"delivery_id": "quiesce-send", "state": "acknowledged"}

    controller = LifecycleController(
        channel_key="voice",
        instance_id="instance-1",
        generation=7,
        environment_spec_id="ches1_" + "1" * 64,
        environment_id="ches1_" + "1" * 64 + ".install1_" + "2" * 32,
        capabilities=("media",),
        send_handler=blocked_send,
        clock_ms=clock,
    )
    await _activate_outbound_controller(controller, ["media"])
    params = SendParams.from_mapping(
        {
            **_identity(),
            "delivery_id": "quiesce-send",
            "to_handle": "chat-1",
            "content_parts": [{"type": "text", "text": "hello"}],
        },
    )
    sending = asyncio.create_task(controller.send(params))
    await started.wait()
    result = await asyncio.wait_for(
        controller.quiesce(
            QuiesceParams.from_mapping(
                {**_identity(), "drain_timeout_ms": 0},
            ),
        ),
        timeout=0.1,
    )
    assert result["state"] == "quiescing"
    with pytest.raises(asyncio.CancelledError):
        await sending
    assert controller._outbound.delivery_state("quiesce-send").value == (
        "unknown"
    )
    with pytest.raises(RpcError) as closed:
        await controller.send(
            SendParams.from_mapping(
                {**params.to_mapping(), "delivery_id": "late-quiesce"},
            ),
        )
    assert closed.value.data["reason_code"] == "INVALID_STATE_TRANSITION"


@pytest.mark.asyncio
async def test_quiesce_deadline_prevents_late_ack_commit() -> None:
    """An attempt waiting on the state lock cannot ACK after deadline."""
    clock = Clock()
    started = asyncio.Event()
    release = asyncio.Event()

    async def finishing_send(params: SendParams) -> dict[str, object]:
        """Finish platform work while finalization is lock-blocked."""
        started.set()
        await release.wait()
        return {
            "delivery_id": params.delivery_id,
            "state": "acknowledged",
        }

    controller = LifecycleController(
        channel_key="voice",
        instance_id="instance-1",
        generation=7,
        environment_spec_id="ches1_" + "1" * 64,
        environment_id="ches1_" + "1" * 64 + ".install1_" + "2" * 32,
        capabilities=("media",),
        send_handler=finishing_send,
        clock_ms=clock,
    )
    await _activate_outbound_controller(controller, ["media"])
    sending = asyncio.create_task(
        controller.send(
            SendParams.from_mapping(
                {
                    **_identity(),
                    "delivery_id": "deadline-send",
                    "to_handle": "chat-1",
                    "content_parts": [{"type": "text", "text": "hello"}],
                },
            ),
        ),
    )
    await started.wait()
    quiescing = asyncio.create_task(
        controller.quiesce(
            QuiesceParams.from_mapping(
                {**_identity(), "drain_timeout_ms": 10},
            ),
        ),
    )
    while controller.state is not RunnerState.QUIESCING:
        await asyncio.sleep(0)
    await controller._lock.acquire()
    release.set()
    await asyncio.sleep(0.03)
    controller._lock.release()
    with pytest.raises(asyncio.CancelledError):
        await sending
    assert (
        controller._outbound.delivery_state("deadline-send").value == "unknown"
    )
    assert (await quiescing)["state"] == "quiescing"
    assert controller._outbound.target_snapshot("deadline-send") is None


@pytest.mark.asyncio
async def test_swallowed_send_cancel_stays_unknown() -> None:
    """Explicit cancellation wins over a handler's late ACK result."""
    clock = Clock()
    started = asyncio.Event()
    left_transport, right_transport = _transport_pair()
    core = RpcPeer(left_transport)
    runner = RpcPeer(right_transport)

    async def swallowing_send(params: SendParams) -> dict[str, object]:
        """Suppress task cancellation and return a misleading ACK."""
        started.set()
        try:
            await asyncio.Future()
        except asyncio.CancelledError:
            pass
        return {
            "delivery_id": params.delivery_id,
            "state": "acknowledged",
        }

    controller = LifecycleController(
        channel_key="voice",
        instance_id="instance-1",
        generation=7,
        environment_spec_id="ches1_" + "1" * 64,
        environment_id="ches1_" + "1" * 64 + ".install1_" + "2" * 32,
        capabilities=("media",),
        send_handler=swallowing_send,
        clock_ms=clock,
    )
    controller.register_rpc_methods(runner)
    CoreLifecycleAdapter(controller).register_rpc_methods(core)
    await asyncio.gather(core.start(), runner.start())
    await runner.call("runner.hello", _hello().to_mapping())
    await core.call(
        "channel.prepare",
        {
            **_identity(),
            "host_context": {},
            "capabilities": ["media"],
        },
    )
    lease = LeaseParams.from_mapping(
        {**_identity(), "lease_token": "cancel", "lease_ttl_ms": 100},
    )
    await core.call("channel.activate", lease.to_mapping())
    await core.call("channel.commit", lease.to_mapping())
    payload = {
        **_identity(),
        "delivery_id": "swallowed-cancel",
        "to_handle": "chat-1",
        "content_parts": [{"type": "text", "text": "hello"}],
    }
    request = asyncio.create_task(core.call("channel.send", payload))
    await asyncio.wait_for(started.wait(), timeout=1.0)
    await core.notify(
        "request.cancel",
        {"request_id": "rpc-4", "reason": "user_cancelled"},
    )
    with pytest.raises(RpcError) as cancelled:
        await request
    assert cancelled.value.data["reason_code"] == "REQUEST_CANCELLED"
    assert controller._outbound.delivery_state("swallowed-cancel").value == (
        "unknown"
    )
    assert controller._outbound.target_snapshot("swallowed-cancel") is None
    with pytest.raises(RpcError) as duplicate:
        await core.call("channel.send", payload)
    assert duplicate.value.data["reason_code"] == ("OUTBOUND_ORDER_VIOLATION")
    await asyncio.gather(core.aclose(), runner.aclose())


@pytest.mark.asyncio
async def test_cancel_during_response_publication_rolls_back_ack() -> None:
    """Cancellation before response publication retracts ACK ordering."""
    right_transport = BlockingSendResponseTransport("rpc-4")
    controller, core, runner = await _active_outbound_rpc_pair(
        right_transport,
        ["media"],
    )
    payload = {
        **_identity(),
        "delivery_id": "publication-send",
        "to_handle": "chat-1",
        "content_parts": [{"type": "text", "text": "hello"}],
    }
    request = asyncio.create_task(core.call("channel.send", payload))
    await asyncio.wait_for(
        right_transport.response_started.wait(),
        timeout=1.0,
    )
    await core.notify(
        "request.cancel",
        {"request_id": "rpc-4", "reason": "user_cancelled"},
    )
    with pytest.raises(RpcError) as cancelled:
        await request
    assert cancelled.value.data["reason_code"] == "REQUEST_CANCELLED"
    assert (
        controller._outbound.delivery_state("publication-send").value
        == "unknown"
    )
    assert controller._outbound.target_snapshot("publication-send") is None
    with pytest.raises(RpcError) as duplicate:
        await core.call("channel.send", payload)
    assert duplicate.value.data["reason_code"] == ("OUTBOUND_ORDER_VIOLATION")
    right_transport.release_response.set()
    await asyncio.gather(core.aclose(), runner.aclose())


@pytest.mark.asyncio
async def test_sent_ack_linearizes_before_stop_fencing() -> None:
    """A sent ACK remains authoritative while its callback waits."""
    right_transport = VisibleBlockingSendResponseTransport("rpc-4")
    controller, core, runner = await _active_outbound_rpc_pair(
        right_transport,
        ["media"],
    )
    payload = {
        **_identity(),
        "delivery_id": "published-before-stop",
        "to_handle": "chat-1",
        "content_parts": [{"type": "text", "text": "hello"}],
    }
    request = asyncio.create_task(core.call("channel.send", payload))
    await right_transport.response_started.wait()
    await controller._lock.acquire()
    try:
        stopping = asyncio.create_task(
            controller.stop(IdentityParams.from_mapping(_identity())),
        )
        await asyncio.sleep(0)
        right_transport.release_response.set()
        result = await request
        assert result["state"] == "acknowledged"
        assert (
            controller._outbound.delivery_state(
                "published-before-stop",
            ).value
            == "acknowledged"
        )
        assert (
            controller._outbound.target_snapshot(
                "published-before-stop",
            )
            is not None
        )
        assert (
            controller._outbound.attempt_snapshot(
                "published-before-stop",
            )
            is None
        )
    finally:
        controller._lock.release()
    stopped = await stopping
    assert stopped["state"] == "stopped"
    assert (
        controller._outbound.delivery_state("published-before-stop").value
        == "acknowledged"
    )
    assert (
        controller._outbound.target_snapshot(
            "published-before-stop",
        )
        is not None
    )
    await asyncio.gather(core.aclose(), runner.aclose())


@pytest.mark.asyncio
async def test_visible_framed_ack_survives_stop_during_drain() -> None:
    """A peer-visible framed ACK cannot be retracted during drain."""
    (
        controller,
        core,
        runner,
        _,
        right_writer,
    ) = await _active_framed_outbound_rpc_pair(["media"])
    right_writer.block_response("rpc-4")
    payload = {
        **_identity(),
        "delivery_id": "visible-before-stop",
        "to_handle": "chat-1",
        "content_parts": [{"type": "text", "text": "hello"}],
    }
    request = asyncio.create_task(core.call("channel.send", payload))

    await asyncio.wait_for(right_writer.frame_visible.wait(), timeout=1.0)
    result = await asyncio.wait_for(request, timeout=1.0)
    await core.notify(
        "request.cancel",
        {"request_id": "rpc-4", "reason": "late_cancel"},
    )
    await asyncio.sleep(0)
    stopped = await controller.stop(
        IdentityParams.from_mapping(_identity()),
    )

    assert result["state"] == "acknowledged"
    assert stopped["state"] == "stopped"
    assert (
        controller._outbound.delivery_state("visible-before-stop").value
        == "acknowledged"
    )
    assert (
        controller._outbound.target_snapshot(
            "visible-before-stop",
        )
        is not None
    )
    right_writer.drain_release.set()
    await asyncio.gather(core.aclose(), runner.aclose())


@pytest.mark.asyncio
async def test_sent_ack_linearizes_before_lease_expiry() -> None:
    """Lease fencing cannot retract an already sent ACK."""
    clock = Clock()
    right_transport = VisibleBlockingSendResponseTransport("rpc-4")
    controller, core, runner = await _active_outbound_rpc_pair(
        right_transport,
        ["media"],
        clock=clock,
    )
    payload = {
        **_identity(),
        "delivery_id": "published-before-expiry",
        "to_handle": "chat-1",
        "content_parts": [{"type": "text", "text": "hello"}],
    }
    request = asyncio.create_task(core.call("channel.send", payload))
    await right_transport.response_started.wait()
    await controller._lock.acquire()
    try:
        health = asyncio.create_task(
            controller.health(IdentityParams.from_mapping(_identity())),
        )
        await asyncio.sleep(0)
        right_transport.release_response.set()
        result = await request
        assert result["state"] == "acknowledged"
        assert (
            controller._outbound.delivery_state(
                "published-before-expiry",
            ).value
            == "acknowledged"
        )
        assert (
            controller._outbound.target_snapshot(
                "published-before-expiry",
            )
            is not None
        )
        assert (
            controller._outbound.attempt_snapshot(
                "published-before-expiry",
            )
            is None
        )
        clock.now = 1100
    finally:
        controller._lock.release()
    status = await health
    assert status["state"] == "failed"
    assert (
        controller._outbound.delivery_state("published-before-expiry").value
        == "acknowledged"
    )
    assert (
        controller._outbound.target_snapshot(
            "published-before-expiry",
        )
        is not None
    )
    await asyncio.gather(core.aclose(), runner.aclose())


@pytest.mark.asyncio
async def test_framed_writer_rechecks_lease_before_visible_result() -> None:
    """Writer publication emits unknown after a queued lease expires."""
    clock = Clock()
    (
        controller,
        core,
        runner,
        _,
        right_writer,
    ) = await _active_framed_outbound_rpc_pair(["media"], clock=clock)
    right_writer.block_next_write()
    blocker = asyncio.create_task(runner.notify("test.blocker"))
    await asyncio.wait_for(right_writer.frame_visible.wait(), timeout=1.0)
    payload = {
        **_identity(),
        "delivery_id": "expired-before-write",
        "to_handle": "chat-1",
        "content_parts": [{"type": "text", "text": "hello"}],
    }
    request = asyncio.create_task(core.call("channel.send", payload))

    while (
        snapshot := controller._outbound.attempt_snapshot(
            "expired-before-write",
        )
    ) is None or not snapshot.provisional:
        await asyncio.sleep(0)
    clock.now = 1100
    right_writer.drain_release.set()
    await blocker
    result = await asyncio.wait_for(request, timeout=1.0)

    assert result["state"] == "unknown"
    assert result["reason_code"] == "LEASE_EXPIRED"
    assert controller.state is RunnerState.FAILED
    assert (
        controller._outbound.delivery_state("expired-before-write").value
        == "unknown"
    )
    assert controller._outbound.target_snapshot("expired-before-write") is None
    await asyncio.gather(core.aclose(), runner.aclose())


@pytest.mark.asyncio
async def test_sent_ack_linearizes_before_zero_drain_deadline() -> None:
    """A sent ACK wins before a queued zero-deadline quiesce."""
    right_transport = VisibleBlockingSendResponseTransport("rpc-4")
    controller, core, runner = await _active_outbound_rpc_pair(
        right_transport,
        ["media"],
    )
    payload = {
        **_identity(),
        "delivery_id": "published-before-drain",
        "to_handle": "chat-1",
        "content_parts": [{"type": "text", "text": "hello"}],
    }
    request = asyncio.create_task(core.call("channel.send", payload))
    await right_transport.response_started.wait()
    await controller._lock.acquire()
    try:
        quiescing = asyncio.create_task(
            controller.quiesce(
                QuiesceParams.from_mapping(
                    {**_identity(), "drain_timeout_ms": 0},
                ),
            ),
        )
        await asyncio.sleep(0)
        right_transport.release_response.set()
        result = await request
        assert result["state"] == "acknowledged"
        assert (
            controller._outbound.delivery_state(
                "published-before-drain",
            ).value
            == "acknowledged"
        )
        assert (
            controller._outbound.target_snapshot(
                "published-before-drain",
            )
            is not None
        )
        assert (
            controller._outbound.attempt_snapshot(
                "published-before-drain",
            )
            is None
        )
    finally:
        controller._lock.release()
    status = await quiescing
    assert status["state"] == "quiescing"
    assert (
        controller._outbound.delivery_state("published-before-drain").value
        == "acknowledged"
    )
    assert (
        controller._outbound.target_snapshot(
            "published-before-drain",
        )
        is not None
    )
    await asyncio.gather(core.aclose(), runner.aclose())


@pytest.mark.asyncio
async def test_zero_drain_deadline_prevents_unsent_ack_publication() -> None:
    """A zero drain deadline wins while the ACK write is blocked."""
    right_transport = BlockingSendResponseTransport("rpc-4")
    controller, core, runner = await _active_outbound_rpc_pair(
        right_transport,
        ["media"],
    )
    payload = {
        **_identity(),
        "delivery_id": "drain-before-publication",
        "to_handle": "chat-1",
        "content_parts": [{"type": "text", "text": "hello"}],
    }
    request = asyncio.create_task(core.call("channel.send", payload))
    await right_transport.response_started.wait()
    status = await core.call(
        "channel.quiesce",
        {**_identity(), "drain_timeout_ms": 0},
    )
    assert status["state"] == "quiescing"
    with pytest.raises(RpcError) as cancelled:
        await request
    assert cancelled.value.data["reason_code"] == "REQUEST_CANCELLED"
    assert (
        controller._outbound.delivery_state(
            "drain-before-publication",
        ).value
        == "unknown"
    )
    assert (
        controller._outbound.target_snapshot(
            "drain-before-publication",
        )
        is None
    )
    await asyncio.gather(core.aclose(), runner.aclose())


@pytest.mark.asyncio
async def test_framed_writer_rechecks_absolute_drain_deadline() -> None:
    """A past absolute deadline changes the written result to unknown."""
    (
        controller,
        core,
        runner,
        _,
        right_writer,
    ) = await _active_framed_outbound_rpc_pair(["media"])
    right_writer.block_next_write()
    blocker = asyncio.create_task(runner.notify("test.blocker"))
    await asyncio.wait_for(right_writer.frame_visible.wait(), timeout=1.0)
    payload = {
        **_identity(),
        "delivery_id": "deadline-before-write",
        "to_handle": "chat-1",
        "content_parts": [{"type": "text", "text": "hello"}],
    }
    request = asyncio.create_task(core.call("channel.send", payload))

    while (
        snapshot := controller._outbound.attempt_snapshot(
            "deadline-before-write",
        )
    ) is None or not snapshot.provisional:
        await asyncio.sleep(0)
    assert controller._outbound.set_drain_deadline(
        "deadline-before-write",
        asyncio.get_running_loop().time(),
    )
    right_writer.drain_release.set()
    await blocker
    result = await asyncio.wait_for(request, timeout=1.0)

    assert result["state"] == "unknown"
    assert result["reason_code"] == "DRAIN_TIMEOUT"
    assert (
        controller._outbound.delivery_state("deadline-before-write").value
        == "unknown"
    )
    assert (
        controller._outbound.target_snapshot(
            "deadline-before-write",
        )
        is None
    )
    await asyncio.gather(core.aclose(), runner.aclose())


@pytest.mark.asyncio
async def test_publication_cancel_preserves_outbound_order() -> None:
    """Unpublished updates and reactions leave no ordering side effects."""
    right_transport = BlockingSendResponseTransport("rpc-5")
    controller, core, runner = await _active_outbound_rpc_pair(
        right_transport,
        ["reaction", "streaming"],
    )
    await core.call(
        "channel.send",
        {
            **_identity(),
            "delivery_id": "published-stream",
            "to_handle": "chat-1",
            "operation": "stream.start",
            "stream_type": "message",
            "sequence": 0,
            "accumulated_text": "",
        },
    )
    delta = asyncio.create_task(
        core.call(
            "channel.send",
            {
                **_identity(),
                "delivery_id": "unpublished-delta",
                "to_handle": "chat-1",
                "operation": "stream.delta",
                "target_delivery_id": "published-stream",
                "stream_type": "message",
                "sequence": 1,
                "accumulated_text": "hello",
            },
        ),
    )
    await asyncio.wait_for(
        right_transport.response_started.wait(),
        timeout=1.0,
    )
    await core.notify(
        "request.cancel",
        {"request_id": "rpc-5", "reason": "user_cancelled"},
    )
    with pytest.raises(RpcError):
        await delta
    target = controller._outbound.target_snapshot("published-stream")
    assert target is not None
    assert target.sequence == 0
    right_transport.release_response.set()

    right_transport.request_id = "rpc-6"
    right_transport.response_started = asyncio.Event()
    right_transport.release_response = asyncio.Event()
    reaction = asyncio.create_task(
        core.call(
            "channel.reaction",
            {
                **_identity(),
                "delivery_id": "unpublished-reaction",
                "to_handle": "chat-1",
                "target_delivery_id": "published-stream",
                "reaction": "completed",
            },
        ),
    )
    await asyncio.wait_for(
        right_transport.response_started.wait(),
        timeout=1.0,
    )
    await core.notify(
        "request.cancel",
        {"request_id": "rpc-6", "reason": "user_cancelled"},
    )
    with pytest.raises(RpcError):
        await reaction
    assert (
        controller._outbound.delivery_state("unpublished-reaction").value
        == "unknown"
    )
    assert controller._outbound.target_snapshot("published-stream") == target
    right_transport.release_response.set()
    await asyncio.gather(core.aclose(), runner.aclose())


@pytest.mark.asyncio
async def test_response_write_failure_converges_outbound_to_unknown() -> None:
    """A failed response publication never leaves an acknowledged target."""
    right_transport = FailingSendResponseTransport("rpc-4")
    controller, core, runner = await _active_outbound_rpc_pair(
        right_transport,
        ["media"],
    )
    request = asyncio.create_task(
        core.call(
            "channel.send",
            {
                **_identity(),
                "delivery_id": "failed-publication",
                "to_handle": "chat-1",
                "content_parts": [{"type": "text", "text": "hello"}],
            },
            timeout=0.02,
        ),
    )
    await asyncio.wait_for(right_transport.response_failed.wait(), timeout=1.0)
    with pytest.raises(RpcError) as failed:
        await request
    assert failed.value.data["reason_code"] == "INTERNAL_ERROR"
    assert (
        controller._outbound.delivery_state("failed-publication").value
        == "unknown"
    )
    assert controller._outbound.target_snapshot("failed-publication") is None
    await asyncio.gather(core.aclose(), runner.aclose())


@pytest.mark.asyncio
async def test_windows_handle_failure_rolls_back_outbound_publication(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A rejected Windows HANDLE frame cannot publish an outbound ACK."""
    handle = RejectingPipeHandle()
    thread_handle = FakeWindowsThreadHandle()
    monkeypatch.setattr(
        runner_bootstrap,
        "_open_windows_thread_handle",
        lambda: thread_handle,
    )
    writer = runner_bootstrap._ThreadPipeWriter(handle)
    transport = FramedTransport(asyncio.StreamReader(), writer)
    runner = RpcPeer(transport)
    controller = _controller(Clock())
    controller.register_rpc_methods(runner)
    await _activate_outbound_controller(controller, ["media"])
    payload = {
        **_identity(),
        "delivery_id": "windows-rejected-publication",
        "to_handle": "chat-1",
        "content_parts": [{"type": "text", "text": "hello"}],
    }

    await runner._dispatch_raw(
        json.dumps(
            {
                "jsonrpc": "2.0",
                "id": "windows-response",
                "method": "channel.send",
                "params": payload,
            },
            separators=(",", ":"),
        ),
    )
    while not transport.is_closed:
        await asyncio.sleep(0)
    while (
        controller._outbound.attempt_snapshot(
            "windows-rejected-publication",
        )
        is not None
    ):
        await asyncio.sleep(0)

    assert (
        controller._outbound.delivery_state(
            "windows-rejected-publication",
        ).value
        == "unknown"
    )
    assert (
        controller._outbound.target_snapshot(
            "windows-rejected-publication",
        )
        is None
    )
    assert (
        controller._outbound.attempt_snapshot(
            "windows-rejected-publication",
        )
        is None
    )
    await runner.aclose()
    assert handle.closed
    assert thread_handle.closed


@pytest.mark.asyncio
async def test_windows_late_ack_keeps_publication_and_releases_lifecycle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A late Windows ACK survives timeout without blocking lifecycle work."""
    handle = LateSuccessfulPipeHandle()
    thread_handle = FakeWindowsThreadHandle()
    monkeypatch.setattr(
        runner_bootstrap,
        "_open_windows_thread_handle",
        lambda: thread_handle,
    )
    writer = runner_bootstrap._ThreadPipeWriter(handle)
    transport = FramedTransport(
        asyncio.StreamReader(),
        writer,
        limits=FramingLimits(write_timeout=0.02),
    )
    runner = RpcPeer(transport)
    controller = _controller(Clock())
    controller.register_rpc_methods(runner)
    await _activate_outbound_controller(controller, ["media"])
    payload = {
        **_identity(),
        "delivery_id": "windows-late-publication",
        "to_handle": "chat-1",
        "content_parts": [{"type": "text", "text": "hello"}],
    }

    await runner._dispatch_raw(
        json.dumps(
            {
                "jsonrpc": "2.0",
                "id": "windows-late-response",
                "method": "channel.send",
                "params": payload,
            },
            separators=(",", ":"),
        ),
    )
    assert await asyncio.to_thread(handle.started.wait, 1)
    while not transport.is_closed:
        await asyncio.sleep(0)

    stopping = asyncio.create_task(
        controller.stop(IdentityParams.from_mapping(_identity())),
    )
    stopped = await asyncio.wait_for(stopping, timeout=0.1)
    assert stopped["state"] == "stopped"

    handle.release.set()
    while (
        controller._outbound.delivery_state(
            "windows-late-publication",
        ).value
        != "acknowledged"
    ):
        await asyncio.sleep(0)

    assert (
        controller._outbound.target_snapshot(
            "windows-late-publication",
        )
        is not None
    )
    assert (
        controller._outbound.attempt_snapshot(
            "windows-late-publication",
        )
        is None
    )
    assert not controller._lock.locked()
    await runner.aclose()
    assert handle.closed
    assert thread_handle.closed


@pytest.mark.asyncio
async def test_deferred_start_target_is_hidden_until_handle_settlement(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A deferred stream target cannot admit dependent platform work."""
    loop = asyncio.get_running_loop()
    core_reader = asyncio.StreamReader()
    runner_reader = asyncio.StreamReader()
    core_writer = LinkedFrameWriter(runner_reader)
    handle = LateRejectingLinkedPipeHandle(
        loop,
        core_reader,
        b'"id":"rpc-1"',
    )
    thread_handle = FakeWindowsThreadHandle()
    monkeypatch.setattr(
        runner_bootstrap,
        "_open_windows_thread_handle",
        lambda: thread_handle,
    )
    runner_writer = runner_bootstrap._ThreadPipeWriter(handle)
    core = RpcPeer(FramedTransport(core_reader, core_writer))
    runner_transport = FramedTransport(
        runner_reader,
        runner_writer,
        limits=FramingLimits(write_timeout=0.02),
    )
    runner = RpcPeer(runner_transport)
    side_effects: list[tuple[str, str]] = []

    async def record_send(params: SendParams) -> dict[str, str]:
        """Record platform work before returning an acknowledged result."""
        side_effects.append((params.delivery_id, params.operation.value))
        return {
            "delivery_id": params.delivery_id,
            "state": "acknowledged",
        }

    controller = LifecycleController(
        channel_key="voice",
        instance_id="instance-1",
        generation=7,
        environment_spec_id="ches1_" + "1" * 64,
        environment_id="ches1_" + "1" * 64 + ".install1_" + "2" * 32,
        capabilities=("streaming",),
        send_handler=record_send,
        clock_ms=Clock(),
    )
    controller.register_rpc_methods(runner)
    await _activate_outbound_controller(controller, ["streaming"])
    await asyncio.gather(core.start(), runner.start())

    start = asyncio.create_task(
        core.call(
            "channel.send",
            {
                **_identity(),
                "delivery_id": "deferred-start",
                "to_handle": "chat-1",
                "operation": "stream.start",
                "stream_type": "message",
                "sequence": 0,
                "accumulated_text": "",
            },
        ),
    )
    assert await asyncio.to_thread(handle.started.wait, 1)
    delta = asyncio.create_task(
        core.call(
            "channel.send",
            {
                **_identity(),
                "delivery_id": "waiting-delta",
                "to_handle": "chat-1",
                "operation": "stream.delta",
                "target_delivery_id": "deferred-start",
                "stream_type": "message",
                "sequence": 1,
                "accumulated_text": "hello",
            },
        ),
    )
    while "rpc-2" not in runner._incoming:
        await asyncio.sleep(0)
    while not runner_transport.is_closed:
        await asyncio.sleep(0)

    assert controller._outbound.target_snapshot("deferred-start") is None
    assert side_effects == [("deferred-start", "stream.start")]
    stopped = await asyncio.wait_for(
        controller.stop(IdentityParams.from_mapping(_identity())),
        timeout=0.1,
    )
    assert stopped["state"] == "stopped"

    handle.release.set()
    results = await asyncio.gather(start, delta, return_exceptions=True)
    while controller._outbound.attempt_snapshot("deferred-start") is not None:
        await asyncio.sleep(0)

    assert all(isinstance(result, Exception) for result in results)
    assert (
        controller._outbound.delivery_state("deferred-start").value
        == "unknown"
    )
    assert controller._outbound.target_snapshot("deferred-start") is None
    assert side_effects == [("deferred-start", "stream.start")]
    await asyncio.gather(core.aclose(), runner.aclose())
    assert handle.closed
    assert thread_handle.closed


@pytest.mark.asyncio
async def test_transport_close_while_publication_prepares_clears_attempt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Closing while prepare waits for state cannot leave a busy stream."""
    (
        controller,
        core,
        runner,
        _,
        right_writer,
    ) = await _active_framed_outbound_rpc_pair(["streaming"])
    await core.call(
        "channel.send",
        {
            **_identity(),
            "delivery_id": "close-prepare-stream",
            "to_handle": "chat-1",
            "operation": "stream.start",
            "stream_type": "message",
            "sequence": 0,
            "accumulated_text": "",
        },
    )
    right_writer.block_next_write()
    blocker = asyncio.create_task(runner.notify("test.blocker"))
    await asyncio.wait_for(right_writer.frame_visible.wait(), timeout=1.0)
    prepare_started = asyncio.Event()
    original_prepare = controller._prepare_outbound_publication

    async def observed_prepare(attempt: Any) -> dict[str, Any]:
        """Expose when response preparation starts waiting for the lock."""
        prepare_started.set()
        return await original_prepare(attempt)

    monkeypatch.setattr(
        controller,
        "_prepare_outbound_publication",
        observed_prepare,
    )
    delta = asyncio.create_task(
        core.call(
            "channel.send",
            {
                **_identity(),
                "delivery_id": "close-during-prepare",
                "to_handle": "chat-1",
                "operation": "stream.delta",
                "target_delivery_id": "close-prepare-stream",
                "stream_type": "message",
                "sequence": 1,
                "accumulated_text": "hello",
            },
        ),
    )
    while (
        snapshot := controller._outbound.attempt_snapshot(
            "close-during-prepare",
        )
    ) is None or not snapshot.provisional:
        await asyncio.sleep(0)
    await controller._lock.acquire()
    try:
        right_writer.drain_release.set()
        await blocker
        await asyncio.wait_for(prepare_started.wait(), timeout=1.0)
        await asyncio.wait_for(runner.aclose(), timeout=1.0)
    finally:
        controller._lock.release()

    with pytest.raises(RpcClosedError):
        await delta
    while (
        controller._outbound.attempt_snapshot(
            "close-during-prepare",
        )
        is not None
    ):
        await asyncio.sleep(0)
    target = controller._outbound.target_snapshot("close-prepare-stream")
    assert target is not None
    assert (
        controller._outbound.delivery_state("close-during-prepare").value
        == "unknown"
    )
    assert target.pending_delivery_id is None
    assert target.sequence == 0
    await core.aclose()


@pytest.mark.asyncio
async def test_cancel_during_send_finalization_cannot_leave_sending() -> None:
    """Cancellation while reacquiring the state lock converges to unknown."""
    clock = Clock()
    started = asyncio.Event()
    release = asyncio.Event()

    async def finishing_send(params: SendParams) -> dict[str, object]:
        """Return an ACK only after the test blocks finalization."""
        started.set()
        await release.wait()
        return {
            "delivery_id": params.delivery_id,
            "state": "acknowledged",
        }

    controller = LifecycleController(
        channel_key="voice",
        instance_id="instance-1",
        generation=7,
        environment_spec_id="ches1_" + "1" * 64,
        environment_id="ches1_" + "1" * 64 + ".install1_" + "2" * 32,
        capabilities=("media",),
        send_handler=finishing_send,
        clock_ms=clock,
    )
    await _activate_outbound_controller(controller, ["media"])
    params = SendParams.from_mapping(
        {
            **_identity(),
            "delivery_id": "finalizing-cancel",
            "to_handle": "chat-1",
            "content_parts": [{"type": "text", "text": "hello"}],
        },
    )
    sending = asyncio.create_task(controller.send(params))
    await started.wait()
    state_lock = controller._lock
    await state_lock.acquire()
    try:
        release.set()
        await asyncio.sleep(0)
        sending.cancel()
        await asyncio.sleep(0)
    finally:
        state_lock.release()
    with pytest.raises(asyncio.CancelledError):
        await sending
    assert controller._outbound.delivery_state("finalizing-cancel").value == (
        "unknown"
    )
    assert controller._outbound.target_snapshot("finalizing-cancel") is None


@pytest.mark.asyncio
async def test_non_acknowledged_send_does_not_establish_target() -> None:
    """Failed, timeout, and unknown results do not mutate target ordering."""
    for state in ("failed", "timeout", "unknown"):
        clock = Clock()

        async def result_handler(
            params: SendParams,
            result_state: str = state,
        ) -> dict[str, object]:
            """Return one non-acknowledged terminal platform result."""
            return {
                "delivery_id": params.delivery_id,
                "state": result_state,
            }

        controller = LifecycleController(
            channel_key="voice",
            instance_id="instance-1",
            generation=7,
            environment_spec_id="ches1_" + "1" * 64,
            environment_id=("ches1_" + "1" * 64 + ".install1_" + "2" * 32),
            capabilities=("reaction", "streaming"),
            send_handler=result_handler,
            clock_ms=clock,
        )
        await _activate_outbound_controller(
            controller,
            ["reaction", "streaming"],
        )
        start = SendParams.from_mapping(
            {
                **_identity(),
                "delivery_id": f"{state}-stream",
                "to_handle": "chat-1",
                "operation": "stream.start",
                "stream_type": "message",
                "sequence": 0,
                "accumulated_text": "",
            },
        )
        assert (await controller.send(start))["state"] == state
        reaction = ReactionParams.from_mapping(
            {
                **_identity(),
                "delivery_id": f"{state}-reaction",
                "to_handle": "chat-1",
                "target_delivery_id": f"{state}-stream",
                "reaction": "completed",
            },
        )
        with pytest.raises(RpcError) as unknown:
            await controller.reaction(reaction)
        assert unknown.value.data["reason_code"] == ("OUTBOUND_TARGET_UNKNOWN")


@pytest.mark.asyncio
async def test_failed_stream_update_does_not_advance_sequence() -> None:
    """Only acknowledged stream operations advance target ordering."""
    clock = Clock()

    async def result_handler(params: SendParams) -> dict[str, object]:
        """Fail the first delta while acknowledging other operations."""
        state = (
            "failed"
            if params.delivery_id == "failed-delta"
            else "acknowledged"
        )
        return {"delivery_id": params.delivery_id, "state": state}

    controller = LifecycleController(
        channel_key="voice",
        instance_id="instance-1",
        generation=7,
        environment_spec_id="ches1_" + "1" * 64,
        environment_id="ches1_" + "1" * 64 + ".install1_" + "2" * 32,
        capabilities=("streaming",),
        send_handler=result_handler,
        clock_ms=clock,
    )
    await _activate_outbound_controller(controller, ["streaming"])
    await controller.send(
        SendParams.from_mapping(
            {
                **_identity(),
                "delivery_id": "sequence-stream",
                "to_handle": "chat-1",
                "operation": "stream.start",
                "stream_type": "message",
                "sequence": 0,
                "accumulated_text": "",
            },
        ),
    )
    failed = SendParams.from_mapping(
        {
            **_identity(),
            "delivery_id": "failed-delta",
            "to_handle": "chat-1",
            "operation": "stream.delta",
            "target_delivery_id": "sequence-stream",
            "stream_type": "message",
            "sequence": 1,
            "accumulated_text": "first",
        },
    )
    assert (await controller.send(failed))["state"] == "failed"
    retry_with_new_id = SendParams.from_mapping(
        {
            **failed.to_mapping(),
            "delivery_id": "replacement-delta",
        },
    )
    assert (await controller.send(retry_with_new_id))[
        "state"
    ] == "acknowledged"


@pytest.mark.asyncio
async def test_outbound_result_id_mismatch_is_unknown_and_not_reused() -> None:
    """A mismatched handler result cannot release the attempted ID."""
    clock = Clock()
    calls = 0

    async def mismatched_result(_: SendParams) -> dict[str, object]:
        """Return a valid result shape for the wrong delivery ID."""
        nonlocal calls
        calls += 1
        return {"delivery_id": "wrong-id", "state": "acknowledged"}

    controller = LifecycleController(
        channel_key="voice",
        instance_id="instance-1",
        generation=7,
        environment_spec_id="ches1_" + "1" * 64,
        environment_id="ches1_" + "1" * 64 + ".install1_" + "2" * 32,
        capabilities=("media",),
        send_handler=mismatched_result,
        clock_ms=clock,
    )
    await _activate_outbound_controller(controller, ["media"])
    params = SendParams.from_mapping(
        {
            **_identity(),
            "delivery_id": "expected-id",
            "to_handle": "chat-1",
            "content_parts": [{"type": "text", "text": "hello"}],
        },
    )
    with pytest.raises(ProtocolValidationError) as mismatch:
        await controller.send(params)
    assert mismatch.value.reason_code == "SCHEMA_MISMATCH"
    with pytest.raises(RpcError) as duplicate:
        await controller.send(params)
    assert duplicate.value.data["reason_code"] == "OUTBOUND_ORDER_VIOLATION"
    assert calls == 1
