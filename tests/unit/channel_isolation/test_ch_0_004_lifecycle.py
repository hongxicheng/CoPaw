# -*- coding: utf-8 -*-
"""Tests for CH-0-004 Runner lifecycle and fencing."""

from __future__ import annotations

import asyncio

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
    PrepareParams,
    QuiesceParams,
    RpcError,
    ProtocolValidationError,
    RunnerState,
    RpcPeer,
    SendParams,
)


class MemoryTransport:
    """Small in-memory full-duplex transport for lifecycle tests."""

    def __init__(self) -> None:
        self.inbox: asyncio.Queue[str | None] = asyncio.Queue()
        self.peer: MemoryTransport | None = None
        self.closed = False

    async def send(self, message: str) -> None:
        """Deliver one message to the peer."""
        if self.closed or self.peer is None or self.peer.closed:
            raise ConnectionError("transport closed")
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
            "capabilities": ["host_state", "ingress_endpoint", "media"],
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
        capabilities=("host_state", "ingress_endpoint", "media"),
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
