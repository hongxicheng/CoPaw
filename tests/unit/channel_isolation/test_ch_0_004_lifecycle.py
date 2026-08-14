# -*- coding: utf-8 -*-
"""Tests for CH-0-004 Runner lifecycle and fencing."""

from __future__ import annotations

import asyncio
import math

import pytest

from qwenpaw.channel_protocol import (
    EndpointParams,
    HelloParams,
    HostContext,
    HostStateParams,
    IdentityParams,
    LeaseParams,
    LifecycleController,
    CoreLifecycleAdapter,
    HostStateStore,
    FixtureSecretHandleConsumer,
    PrepareParams,
    QuiesceParams,
    RpcError,
    ProtocolValidationError,
    RunnerState,
    RpcPeer,
    ReactionParams,
    SendParams,
)


class MemoryTransport:
    """Small in-memory full-duplex transport for lifecycle tests."""

    def __init__(self) -> None:
        self.inbox: asyncio.Queue[str | None] = asyncio.Queue()
        self.peer: MemoryTransport | None = None
        self.closed = False
        self.sent_messages: list[str] = []

    async def send(self, message: str) -> None:
        """Deliver one message to the peer."""
        if self.closed or self.peer is None or self.peer.closed:
            raise ConnectionError("transport closed")
        self.sent_messages.append(message)
        await self.peer.inbox.put(message)

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


def _transport_pair() -> tuple[MemoryTransport, MemoryTransport]:
    """Create two linked memory transports."""
    left = MemoryTransport()
    right = MemoryTransport()
    left.peer = right
    right.peer = left
    return left, right


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


def _identity(generation: int = 7) -> dict[str, object]:
    """Return a valid control identity fixture."""
    return {
        "channel_key": "voice",
        "instance_id": "instance-1",
        "generation": generation,
    }


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
                "streaming",
            ],
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
            "streaming",
        ),
        clock_ms=clock,
    )


@pytest.mark.asyncio
async def test_lifecycle_requires_hello_and_commit() -> None:
    """Prepare and activate remain standby until commit succeeds."""
    clock = Clock()
    controller = _controller(clock)
    with pytest.raises(RpcError):
        await controller.prepare(
            PrepareParams.from_mapping(
                {
                    **_identity(),
                    "host_context": HostContext(
                        media_work_dir="/tmp/media",
                    ).to_mapping(),
                    "capabilities": ["media"],
                },
            ),
        )
    assert controller.state is RunnerState.CREATED
    assert controller.accept_hello(_hello())["protocol_version"] == 1
    prepared = await controller.prepare(
        PrepareParams.from_mapping(
            {
                **_identity(),
                "host_context": {"media_work_dir": "/tmp/media"},
                "capabilities": ["ingress_endpoint", "media"],
            },
        ),
    )
    assert prepared["state"] == "standby"
    lease = LeaseParams.from_mapping(
        {**_identity(), "lease_token": "opaque", "lease_ttl_ms": 100},
    )
    activated = await controller.activate(lease)
    assert activated["state"] == "standby"
    with pytest.raises(RpcError):
        await controller.send(
            SendParams.from_mapping(
                {
                    **_identity(),
                    "delivery_id": "d1",
                    "to_handle": "call-1",
                    "content_parts": [{"type": "text", "text": "hello"}],
                },
            ),
        )
    committed = await controller.commit(lease)
    assert committed["state"] == "active"
    sent = await controller.send(
        SendParams.from_mapping(
            {
                **_identity(),
                "delivery_id": "d1",
                "to_handle": "call-1",
                "content_parts": [{"type": "text", "text": "hello"}],
            },
        ),
    )
    assert sent["status"] == "accepted"


@pytest.mark.asyncio
async def test_generation_fencing_expiry_and_quiesce() -> None:
    """Stale generations and expired leases cannot continue operating."""
    clock = Clock()
    controller = _controller(clock)
    controller.accept_hello(_hello())
    await controller.prepare(
        PrepareParams.from_mapping(
            {
                **_identity(),
                "host_context": {},
                "capabilities": [
                    "host_state",
                    "ingress_endpoint",
                    "media",
                ],
            },
        ),
    )
    lease = LeaseParams.from_mapping(
        {**_identity(), "lease_token": "opaque", "lease_ttl_ms": 10},
    )
    await controller.activate(lease)
    await controller.commit(lease)
    clock.now = 1011
    with pytest.raises(RpcError) as expired_write:
        await controller.host_state_put(
            HostStateParams.from_mapping(
                {
                    **_identity(),
                    "key": "stale",
                    "value": {"blocked": True},
                },
            ),
        )
    assert expired_write.value.data["reason_code"] == "LEASE_EXPIRED"
    health = await controller.health(IdentityParams.from_mapping(_identity()))
    assert health["state"] == "failed"
    assert controller.state is RunnerState.FAILED
    with pytest.raises(RpcError) as fenced:
        await controller.stop(IdentityParams.from_mapping(_identity(8)))
    assert fenced.value.data["reason_code"] == "GENERATION_FENCED"


@pytest.mark.asyncio
async def test_endpoint_and_host_state_require_active_generation() -> None:
    """Endpoint exposure and host writes honor standby and active fencing."""
    clock = Clock()
    controller = _controller(clock)
    controller.accept_hello(_hello())
    await controller.prepare(
        PrepareParams.from_mapping(
            {
                **_identity(),
                "host_context": {},
                "capabilities": [
                    "host_state",
                    "ingress_endpoint",
                    "media",
                ],
            },
        ),
    )
    endpoint = EndpointParams.from_mapping(
        {
            **_identity(),
            "protocol": "http",
            "host": "127.0.0.1",
            "port": 8080,
            "path": "/voice",
            "public_base_url": "https://example",
            "readiness": "ready",
            "bound_externally": False,
            "auth_required": False,
            "quiescing": False,
        },
    )
    registered = await controller.endpoint_register(endpoint)
    assert registered["status"] == "registered"
    lease = LeaseParams.from_mapping(
        {**_identity(), "lease_token": "opaque", "lease_ttl_ms": 100},
    )
    await controller.activate(lease)
    with pytest.raises(RpcError):
        await controller.endpoint_register(
            EndpointParams.from_mapping(
                {
                    **endpoint.to_mapping(),
                    "host": "0.0.0.0",
                    "bound_externally": False,
                    "auth_required": True,
                },
            ),
        )
    await controller.commit(lease)
    await controller.endpoint_register(endpoint)
    state = HostStateParams.from_mapping(
        {
            **_identity(),
            "key": "checkpoint",
            "schema_version": 1,
            "value": {"ok": True},
        },
    )
    assert (await controller.host_state_put(state))["status"] == "stored"
    assert (await controller.host_state_get(state))["value"] == {"ok": True}
    assert (await controller.endpoint_update(endpoint))["status"] == "updated"
    await controller.quiesce(
        QuiesceParams.from_mapping({**_identity(), "drain_timeout_ms": 10}),
    )
    assert controller.state is RunnerState.QUIESCING
    assert controller.endpoint is None
    assert (
        await controller.endpoint_unregister(
            IdentityParams.from_mapping(_identity()),
        )
    )["status"] == "unregistered"
    with pytest.raises(RpcError):
        await controller.host_state_put(state)


@pytest.mark.asyncio
async def test_mock_core_runner_completes_control_lifecycle() -> None:
    """A mock Core and Runner complete the v1 control handshake."""
    clock = Clock()
    left_transport, right_transport = _transport_pair()
    core = RpcPeer(left_transport)
    runner = RpcPeer(right_transport)
    controller = _controller(clock)
    controller.register_rpc_methods(runner)
    adapter = CoreLifecycleAdapter(controller)
    adapter.register_rpc_methods(core)
    await core.start()
    await runner.start()

    hello = await runner.call("runner.hello", _hello().to_mapping())
    assert hello["protocol_version"] == 1
    prepared = await core.call(
        "channel.prepare",
        PrepareParams.from_mapping(
            {
                **_identity(),
                "host_context": {},
                "capabilities": [
                    "host_state",
                    "ingress_endpoint",
                    "media",
                ],
            },
        ).to_mapping(),
    )
    assert prepared["state"] == "standby"
    lease = LeaseParams.from_mapping(
        {**_identity(), "lease_token": "rpc-token", "lease_ttl_ms": 100},
    )
    assert (await core.call("channel.activate", lease.to_mapping()))[
        "state"
    ] == "standby"
    assert (await core.call("channel.commit", lease.to_mapping()))[
        "state"
    ] == "active"
    assert (await core.call("channel.health", _identity()))[
        "consuming"
    ] is True
    renewed = await core.call(
        "channel.lease_renew",
        LeaseParams.from_mapping(
            {**_identity(), "lease_token": "rpc-token", "lease_ttl_ms": 100},
        ).to_mapping(),
    )
    assert renewed["consuming"] is True
    endpoint = EndpointParams.from_mapping(
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
    registered = await runner.call(
        "ingress.endpoint.register",
        endpoint.to_mapping(),
    )
    assert registered["status"] == "registered"
    assert adapter.endpoints[7] == endpoint
    state = HostStateParams.from_mapping(
        {
            **_identity(),
            "key": "rpc-checkpoint",
            "value": {"ok": True},
        },
    )
    await runner.call("host.state.put", state.to_mapping())
    assert await adapter.host_state_store.get("rpc-checkpoint") == (
        1,
        {"ok": True},
    )
    await runner.call(
        "ingress.endpoint.unregister",
        IdentityParams.from_mapping(_identity()).to_mapping(),
    )
    assert not adapter.endpoints
    assert (await core.call("channel.stop", _identity()))["state"] == "stopped"
    await core.aclose()
    await runner.aclose()


@pytest.mark.asyncio
async def test_capability_gates_and_bounded_host_state() -> None:
    """Methods requiring undeclared capabilities return stable errors."""
    clock = Clock()
    controller = LifecycleController(
        channel_key="voice",
        instance_id="instance-1",
        generation=7,
        environment_spec_id="ches1_" + "1" * 64,
        environment_id="ches1_" + "1" * 64 + ".install1_" + "2" * 32,
        capabilities=("media",),
        clock_ms=clock,
    )
    controller.accept_hello(_hello())
    with pytest.raises(RpcError) as prepare_error:
        await controller.prepare(
            PrepareParams.from_mapping(
                {
                    **_identity(),
                    "host_context": {},
                    "capabilities": ["host_state"],
                },
            ),
        )
    assert prepare_error.value.data["reason_code"] == "CAPABILITY_REQUIRED"

    store = HostStateStore(
        max_value_bytes=8,
        max_total_bytes=12,
        max_keys=1,
    )
    controller = _controller(clock)
    controller.host_state_store = store
    controller.accept_hello(_hello())
    await controller.prepare(
        PrepareParams.from_mapping(
            {
                **_identity(),
                "host_context": {},
                "capabilities": [
                    "host_state",
                    "ingress_endpoint",
                    "media",
                ],
            },
        ),
    )
    lease = LeaseParams.from_mapping(
        {**_identity(), "lease_token": "opaque", "lease_ttl_ms": 100},
    )
    await controller.activate(lease)
    await controller.commit(lease)
    with pytest.raises(ProtocolValidationError) as limit_error:
        await controller.host_state_put(
            HostStateParams.from_mapping(
                {
                    **_identity(),
                    "key": "large",
                    "value": "0123456789",
                },
            ),
        )
    assert limit_error.value.reason_code == "STATE_LIMIT_EXCEEDED"


@pytest.mark.asyncio
async def test_prepare_subset_is_the_effective_capability_set() -> None:
    """Prepare cannot be bypassed by hello-only capabilities."""
    clock = Clock()
    controller = _controller(clock)
    controller.accept_hello(_hello())
    await controller.prepare(
        PrepareParams.from_mapping(
            {
                **_identity(),
                "host_context": {},
                "capabilities": ["ingress_endpoint"],
            },
        ),
    )
    lease = LeaseParams.from_mapping(
        {**_identity(), "lease_token": "subset", "lease_ttl_ms": 100},
    )
    await controller.activate(lease)
    await controller.commit(lease)
    media = SendParams.from_mapping(
        {
            **_identity(),
            "delivery_id": "media-1",
            "to_handle": "call-1",
            "content_parts": [
                {"type": "image", "image_url": "https://example/image"},
            ],
        },
    )
    with pytest.raises(RpcError) as media_error:
        await controller.send(media)
    assert media_error.value.data["reason_code"] == "CAPABILITY_REQUIRED"
    state = HostStateParams.from_mapping(
        {**_identity(), "key": "checkpoint", "value": {"ok": True}},
    )
    with pytest.raises(RpcError) as state_error:
        await controller.host_state_put(state)
    assert state_error.value.data["reason_code"] == "CAPABILITY_REQUIRED"


@pytest.mark.asyncio
async def test_lease_expiry_removes_core_endpoint_registry() -> None:
    """Lease fencing also revokes Core routing state."""
    clock = Clock()
    controller = _controller(clock)
    adapter = CoreLifecycleAdapter(controller)
    controller.accept_hello(_hello())
    await controller.prepare(
        PrepareParams.from_mapping(
            {
                **_identity(),
                "host_context": {},
                "capabilities": [
                    "host_state",
                    "ingress_endpoint",
                    "media",
                ],
            },
        ),
    )
    lease = LeaseParams.from_mapping(
        {**_identity(), "lease_token": "expire", "lease_ttl_ms": 10},
    )
    await controller.activate(lease)
    await controller.commit(lease)
    endpoint = EndpointParams.from_mapping(
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
    await adapter.endpoint_register(endpoint)
    assert adapter.endpoints[7] == endpoint
    clock.now = 1011
    await controller.health(IdentityParams.from_mapping(_identity()))
    assert controller.state is RunnerState.FAILED
    assert not adapter.endpoints


@pytest.mark.asyncio
async def test_core_host_state_put_linearizes_before_stop() -> None:
    """A blocked state write holds the lifecycle lock across Store mutation."""
    clock = Clock()
    controller = _controller(clock)
    store = BlockingHostStateStore()
    adapter = CoreLifecycleAdapter(controller, host_state_store=store)
    controller.accept_hello(_hello())
    await controller.prepare(
        PrepareParams.from_mapping(
            {
                **_identity(),
                "host_context": {},
                "capabilities": [
                    "host_state",
                    "ingress_endpoint",
                    "media",
                ],
            },
        ),
    )
    lease = LeaseParams.from_mapping(
        {**_identity(), "lease_token": "blocked", "lease_ttl_ms": 100},
    )
    await controller.activate(lease)
    await controller.commit(lease)
    params = HostStateParams.from_mapping(
        {**_identity(), "key": "blocked", "value": {"ok": True}},
    )
    write = asyncio.create_task(adapter.host_state_put(params))
    await store.started.wait()
    stop = asyncio.create_task(
        controller.stop(IdentityParams.from_mapping(_identity())),
    )
    await asyncio.sleep(0)
    assert not stop.done()
    store.release.set()
    assert (await write)["status"] == "stored"
    assert (await stop)["state"] == "stopped"
    assert await store.get("blocked") == (1, {"ok": True})


@pytest.mark.asyncio
async def test_host_state_rejects_non_finite_numbers() -> None:
    """Host State accepts only strict JSON values."""
    clock = Clock()
    controller = _controller(clock)
    controller.accept_hello(_hello())
    await controller.prepare(
        PrepareParams.from_mapping(
            {
                **_identity(),
                "host_context": {},
                "capabilities": [
                    "host_state",
                    "ingress_endpoint",
                    "media",
                ],
            },
        ),
    )
    lease = LeaseParams.from_mapping(
        {**_identity(), "lease_token": "finite", "lease_ttl_ms": 100},
    )
    await controller.activate(lease)
    await controller.commit(lease)
    with pytest.raises(ProtocolValidationError) as value_error:
        await controller.host_state_put(
            HostStateParams.from_mapping(
                {
                    **_identity(),
                    "key": "non-finite",
                    "value": {"score": math.nan},
                },
            ),
        )
    assert value_error.value.reason_code == "SCHEMA_MISMATCH"


def test_protocol_version_mismatch_has_stable_reason() -> None:
    """Hello rejects non-overlapping protocol versions deterministically."""
    clock = Clock()
    controller = LifecycleController(
        channel_key="voice",
        instance_id="instance-1",
        generation=7,
        environment_spec_id="ches1_" + "1" * 64,
        environment_id="ches1_" + "1" * 64 + ".install1_" + "2" * 32,
        protocol_min=2,
        protocol_max=2,
        clock_ms=clock,
    )
    with pytest.raises(RpcError) as mismatch:
        controller.accept_hello(_hello())
    assert mismatch.value.data["reason_code"] == "PROTOCOL_MISMATCH"


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


@pytest.mark.asyncio
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
    assert (await controller.send(approval))["status"] == "accepted"
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
    with pytest.raises(RpcError) as premature:
        await controller.reaction(premature_reaction)
    assert premature.value.data["reason_code"] == "OUTBOUND_ORDER_VIOLATION"
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
    assert (await controller.reaction(reaction))["status"] == "accepted"
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


@pytest.mark.asyncio
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


@pytest.mark.asyncio
async def test_stop_waits_for_inflight_outbound_side_effect() -> None:
    """Stop linearizes after an outbound handler already in progress."""
    clock = Clock()
    started = asyncio.Event()
    release = asyncio.Event()

    async def blocked_send(_: SendParams) -> dict[str, str]:
        """Pause one platform side effect until the test releases it."""
        started.set()
        await release.wait()
        return {"status": "accepted"}

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
    assert not stop.done()
    release.set()
    assert (await send)["status"] == "accepted"
    assert (await stop)["state"] == "stopped"
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
async def test_secret_handle_is_consumed_once_during_prepare() -> None:
    """Fixture handles are generation-scoped, single-use, and not retained."""
    clock = Clock()
    secret_value = "fixture-secret-value"
    consumed_values: list[object] = []
    consumer = FixtureSecretHandleConsumer(
        {("fixture-handle", 7): secret_value},
        consumed_values.append,
    )
    controller = LifecycleController(
        channel_key="voice",
        instance_id="instance-1",
        generation=7,
        environment_spec_id="ches1_" + "1" * 64,
        environment_id="ches1_" + "1" * 64 + ".install1_" + "2" * 32,
        secret_handle_consumer=consumer,
        clock_ms=clock,
    )
    controller.accept_hello(_hello())
    prepared = await controller.prepare(
        PrepareParams.from_mapping(
            {
                **_identity(),
                "host_context": {"secret_handle": "fixture-handle"},
                "capabilities": [],
            },
        ),
    )
    assert prepared["state"] == "standby"
    assert consumed_values == [secret_value]
    assert controller.host_context is not None
    assert "secret_handle" not in controller.host_context.to_mapping()
    assert secret_value not in repr(consumer)
    assert "fixture-handle" not in repr(consumer)

    repeated = LifecycleController(
        channel_key="voice",
        instance_id="instance-1",
        generation=7,
        environment_spec_id="ches1_" + "1" * 64,
        environment_id="ches1_" + "1" * 64 + ".install1_" + "2" * 32,
        secret_handle_consumer=consumer,
        clock_ms=clock,
    )
    repeated.accept_hello(_hello())
    with pytest.raises(RpcError) as consumed:
        await repeated.prepare(
            PrepareParams.from_mapping(
                {
                    **_identity(),
                    "host_context": {"secret_handle": "fixture-handle"},
                    "capabilities": [],
                },
            ),
        )
    assert consumed.value.data["reason_code"] == "SECRET_HANDLE_CONSUMED"
    assert repeated.state is RunnerState.FAILED


@pytest.mark.asyncio
async def test_secret_handle_invalid_and_auth_errors_are_stable() -> None:
    """Handle lookup failures differ from invalid consumed credentials."""
    clock = Clock()
    consumer = FixtureSecretHandleConsumer({}, lambda _: None)
    controller = LifecycleController(
        channel_key="voice",
        instance_id="instance-1",
        generation=7,
        environment_spec_id="ches1_" + "1" * 64,
        environment_id="ches1_" + "1" * 64 + ".install1_" + "2" * 32,
        secret_handle_consumer=consumer,
        clock_ms=clock,
    )
    controller.accept_hello(_hello())
    with pytest.raises(RpcError) as invalid:
        await controller.prepare(
            PrepareParams.from_mapping(
                {
                    **_identity(),
                    "host_context": {"secret_handle": "missing"},
                    "capabilities": [],
                },
            ),
        )
    assert invalid.value.data["reason_code"] == "SECRET_HANDLE_INVALID"

    def reject_credentials(_: object) -> None:
        """Simulate fixture platform authentication failure."""
        raise ValueError("invalid fixture credential")

    auth_consumer = FixtureSecretHandleConsumer(
        {("auth-handle", 7): "invalid-secret"},
        reject_credentials,
    )
    auth_controller = LifecycleController(
        channel_key="voice",
        instance_id="instance-1",
        generation=7,
        environment_spec_id="ches1_" + "1" * 64,
        environment_id="ches1_" + "1" * 64 + ".install1_" + "2" * 32,
        secret_handle_consumer=auth_consumer,
        clock_ms=clock,
    )
    auth_controller.accept_hello(_hello())
    with pytest.raises(RpcError) as auth_failed:
        await auth_controller.prepare(
            PrepareParams.from_mapping(
                {
                    **_identity(),
                    "host_context": {"secret_handle": "auth-handle"},
                    "capabilities": [],
                },
            ),
        )
    assert auth_failed.value.data["reason_code"] == "PLATFORM_AUTH_FAILED"
    retried_controller = LifecycleController(
        channel_key="voice",
        instance_id="instance-1",
        generation=7,
        environment_spec_id="ches1_" + "1" * 64,
        environment_id="ches1_" + "1" * 64 + ".install1_" + "2" * 32,
        secret_handle_consumer=auth_consumer,
        clock_ms=clock,
    )
    retried_controller.accept_hello(_hello())
    with pytest.raises(RpcError) as consumed:
        await retried_controller.prepare(
            PrepareParams.from_mapping(
                {
                    **_identity(),
                    "host_context": {"secret_handle": "auth-handle"},
                    "capabilities": [],
                },
            ),
        )
    assert consumed.value.data["reason_code"] == "SECRET_HANDLE_CONSUMED"


@pytest.mark.asyncio
async def test_secret_handle_without_consumer_is_invalid() -> None:
    """An opaque handle cannot be accepted without a prepare consumer."""
    clock = Clock()
    controller = LifecycleController(
        channel_key="voice",
        instance_id="instance-1",
        generation=7,
        environment_spec_id="ches1_" + "1" * 64,
        environment_id="ches1_" + "1" * 64 + ".install1_" + "2" * 32,
        clock_ms=clock,
    )
    controller.accept_hello(_hello())
    with pytest.raises(RpcError) as invalid:
        await controller.prepare(
            PrepareParams.from_mapping(
                {
                    **_identity(),
                    "host_context": {"secret_handle": "no-consumer"},
                    "capabilities": [],
                },
            ),
        )
    assert invalid.value.data["reason_code"] == "SECRET_HANDLE_INVALID"
    assert controller.state is RunnerState.FAILED


@pytest.mark.asyncio
async def test_outbound_and_secret_contract_crosses_rpc_dispatch() -> None:
    """RPC carries stable operations while secret values stay in-process."""
    clock = Clock()
    secret_value = "fixture-secret-never-on-wire"
    consumed: list[object] = []
    consumer = FixtureSecretHandleConsumer(
        {("rpc-fixture-handle", 7): secret_value},
        consumed.append,
    )
    left_transport, right_transport = _transport_pair()
    core = RpcPeer(left_transport)
    runner = RpcPeer(right_transport)
    controller = LifecycleController(
        channel_key="voice",
        instance_id="instance-1",
        generation=7,
        environment_spec_id="ches1_" + "1" * 64,
        environment_id="ches1_" + "1" * 64 + ".install1_" + "2" * 32,
        capabilities=("approval_card", "reaction", "streaming"),
        secret_handle_consumer=consumer,
        clock_ms=clock,
    )
    controller.register_rpc_methods(runner)
    CoreLifecycleAdapter(controller).register_rpc_methods(core)
    await asyncio.gather(core.start(), runner.start())
    await runner.call("runner.hello", _hello().to_mapping())
    prepared = await core.call(
        "channel.prepare",
        PrepareParams.from_mapping(
            {
                **_identity(),
                "host_context": {
                    "secret_handle": "rpc-fixture-handle",
                },
                "capabilities": [
                    "approval_card",
                    "reaction",
                    "streaming",
                ],
            },
        ).to_mapping(),
    )
    assert prepared["state"] == "standby"
    lease = LeaseParams.from_mapping(
        {**_identity(), "lease_token": "rpc-outbound", "lease_ttl_ms": 100},
    )
    await core.call("channel.activate", lease.to_mapping())
    await core.call("channel.commit", lease.to_mapping())
    created = await core.call(
        "channel.send",
        {
            **_identity(),
            "delivery_id": "rpc-message-1",
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
    assert created["status"] == "accepted"
    await core.call(
        "channel.send",
        {
            **_identity(),
            "delivery_id": "rpc-stream-1",
            "to_handle": "chat-1",
            "operation": "stream.start",
            "stream_type": "message",
            "sequence": 0,
            "accumulated_text": "",
        },
    )
    await core.call(
        "channel.send",
        {
            **_identity(),
            "delivery_id": "rpc-stream-end-1",
            "to_handle": "chat-1",
            "operation": "stream.end",
            "target_delivery_id": "rpc-stream-1",
            "stream_type": "message",
            "sequence": 1,
            "accumulated_text": "done",
        },
    )
    reaction = await core.call(
        "channel.reaction",
        {
            **_identity(),
            "delivery_id": "rpc-reaction-1",
            "to_handle": "chat-1",
            "target_delivery_id": "rpc-stream-1",
            "reaction": "completed",
        },
    )
    assert reaction["status"] == "accepted"
    assert consumed == [secret_value]
    frames = left_transport.sent_messages + right_transport.sent_messages
    assert any("rpc-fixture-handle" in frame for frame in frames)
    assert all(secret_value not in frame for frame in frames)
    assert controller.host_context is not None
    assert "secret_handle" not in controller.host_context.to_mapping()
    await asyncio.gather(core.aclose(), runner.aclose())
