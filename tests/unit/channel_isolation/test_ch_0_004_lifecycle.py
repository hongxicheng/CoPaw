# -*- coding: utf-8 -*-
"""Tests for CH-0-004 Runner lifecycle and fencing."""

# pylint: disable=protected-access

from __future__ import annotations

import asyncio
import math
import traceback

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
    RpcTimeoutError,
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


class BlockingSendResponseTransport(MemoryTransport):
    """Block the acknowledged response for one outbound RPC request."""

    def __init__(self, request_id: str) -> None:
        super().__init__()
        self.request_id = request_id
        self.response_started = asyncio.Event()
        self.release_response = asyncio.Event()

    async def send(self, message: str) -> None:
        """Pause only the selected successful response publication."""
        if (
            f'"id":"{self.request_id}"' in message
            and '"state":"acknowledged"' in message
        ):
            self.response_started.set()
            await self.release_response.wait()
        await super().send(message)


class FailingSendResponseTransport(MemoryTransport):
    """Fail the acknowledged response for one outbound RPC request."""

    def __init__(self, request_id: str) -> None:
        super().__init__()
        self.request_id = request_id
        self.response_failed = asyncio.Event()

    async def send(self, message: str) -> None:
        """Raise once the selected successful response is published."""
        if (
            f'"id":"{self.request_id}"' in message
            and '"state":"acknowledged"' in message
        ):
            self.response_failed.set()
            raise ConnectionError("response write failed")
        await super().send(message)


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
    assert sent["state"] == "acknowledged"


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
@pytest.mark.parametrize("operation", ["quiesce", "stop"])
async def test_shutdown_detaches_endpoint_before_blocked_hook(
    operation: str,
) -> None:
    """Endpoint cleanup cannot delay lifecycle fencing or Core removal."""
    clock = Clock()
    hook_started = asyncio.Event()

    async def blocked_hook(_: str, __: EndpointParams | None) -> None:
        """Model external unregister cleanup with no natural deadline."""
        hook_started.set()
        await asyncio.Future()

    controller = LifecycleController(
        channel_key="voice",
        instance_id="instance-1",
        generation=7,
        environment_spec_id="ches1_" + "1" * 64,
        environment_id="ches1_" + "1" * 64 + ".install1_" + "2" * 32,
        capabilities=("ingress_endpoint",),
        endpoint_handler=blocked_hook,
        clock_ms=clock,
    )
    adapter = CoreLifecycleAdapter(controller)
    await _activate_outbound_controller(controller, ["ingress_endpoint"])
    await adapter.endpoint_register(_endpoint())
    assert adapter.endpoints
    if operation == "quiesce":
        result = await asyncio.wait_for(
            controller.quiesce(
                QuiesceParams.from_mapping(
                    {**_identity(), "drain_timeout_ms": 10},
                ),
            ),
            timeout=0.1,
        )
        assert result["state"] == "quiescing"
    else:
        result = await asyncio.wait_for(
            controller.stop(IdentityParams.from_mapping(_identity())),
            timeout=0.1,
        )
        assert result["state"] == "stopped"
    await asyncio.wait_for(hook_started.wait(), timeout=0.1)
    assert controller.endpoint is None
    assert not adapter.endpoints


@pytest.mark.asyncio
async def test_endpoint_unregister_hook_can_reenter_lifecycle() -> None:
    """Unregister callbacks run after releasing the lifecycle lock."""
    clock = Clock()
    callback_state: list[str] = []
    controller: LifecycleController

    async def reentrant_hook(
        operation: str,
        _: EndpointParams | None,
    ) -> None:
        """Read lifecycle health from an unregister callback."""
        if operation == "unregister":
            health = await controller.health(
                IdentityParams.from_mapping(_identity()),
            )
            callback_state.append(health["state"])

    controller = LifecycleController(
        channel_key="voice",
        instance_id="instance-1",
        generation=7,
        environment_spec_id="ches1_" + "1" * 64,
        environment_id="ches1_" + "1" * 64 + ".install1_" + "2" * 32,
        capabilities=("ingress_endpoint",),
        endpoint_handler=reentrant_hook,
        clock_ms=clock,
    )
    adapter = CoreLifecycleAdapter(controller)
    await _activate_outbound_controller(controller, ["ingress_endpoint"])
    await adapter.endpoint_register(_endpoint())
    result = await asyncio.wait_for(
        controller.quiesce(
            QuiesceParams.from_mapping(
                {**_identity(), "drain_timeout_ms": 20},
            ),
        ),
        timeout=0.1,
    )
    assert result["state"] == "quiescing"
    assert callback_state == ["quiescing"]


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
    delivery_states = controller._outbound_delivery_states
    assert delivery_states["blocked-send"].value == "unknown"
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
    delivery_states = controller._outbound_delivery_states
    assert delivery_states["quiesce-send"].value == "unknown"
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
        controller._outbound_delivery_states["deadline-send"].value
        == "unknown"
    )
    assert (await quiescing)["state"] == "quiescing"
    assert "deadline-send" not in controller._outbound_targets


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
    delivery_states = controller._outbound_delivery_states
    targets = controller._outbound_targets
    assert delivery_states["swallowed-cancel"].value == "unknown"
    assert "swallowed-cancel" not in targets
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
        controller._outbound_delivery_states["publication-send"].value
        == "unknown"
    )
    assert "publication-send" not in controller._outbound_targets
    with pytest.raises(RpcError) as duplicate:
        await core.call("channel.send", payload)
    assert duplicate.value.data["reason_code"] == ("OUTBOUND_ORDER_VIOLATION")
    right_transport.release_response.set()
    await asyncio.gather(core.aclose(), runner.aclose())


@pytest.mark.asyncio
async def test_sent_ack_linearizes_before_stop_fencing() -> None:
    """A sent ACK remains authoritative while its callback waits."""
    right_transport = BlockingSendResponseTransport("rpc-4")
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
            controller._outbound_delivery_states["published-before-stop"].value
            == "acknowledged"
        )
        assert "published-before-stop" in controller._outbound_targets
        assert "published-before-stop" not in controller._outbound_attempts
    finally:
        controller._lock.release()
    stopped = await stopping
    assert stopped["state"] == "stopped"
    assert (
        controller._outbound_delivery_states["published-before-stop"].value
        == "acknowledged"
    )
    assert "published-before-stop" in controller._outbound_targets
    await asyncio.gather(core.aclose(), runner.aclose())


@pytest.mark.asyncio
async def test_sent_ack_linearizes_before_lease_expiry() -> None:
    """Lease fencing cannot retract an already sent ACK."""
    clock = Clock()
    right_transport = BlockingSendResponseTransport("rpc-4")
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
            controller._outbound_delivery_states[
                "published-before-expiry"
            ].value
            == "acknowledged"
        )
        assert "published-before-expiry" in controller._outbound_targets
        assert "published-before-expiry" not in (controller._outbound_attempts)
        clock.now = 1100
    finally:
        controller._lock.release()
    status = await health
    assert status["state"] == "failed"
    assert (
        controller._outbound_delivery_states["published-before-expiry"].value
        == "acknowledged"
    )
    assert "published-before-expiry" in controller._outbound_targets
    await asyncio.gather(core.aclose(), runner.aclose())


@pytest.mark.asyncio
async def test_sent_ack_linearizes_before_zero_drain_deadline() -> None:
    """A sent ACK wins before a queued zero-deadline quiesce."""
    right_transport = BlockingSendResponseTransport("rpc-4")
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
            controller._outbound_delivery_states[
                "published-before-drain"
            ].value
            == "acknowledged"
        )
        assert "published-before-drain" in controller._outbound_targets
        assert "published-before-drain" not in controller._outbound_attempts
    finally:
        controller._lock.release()
    status = await quiescing
    assert status["state"] == "quiescing"
    assert (
        controller._outbound_delivery_states["published-before-drain"].value
        == "acknowledged"
    )
    assert "published-before-drain" in controller._outbound_targets
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
        controller._outbound_delivery_states["drain-before-publication"].value
        == "unknown"
    )
    assert "drain-before-publication" not in controller._outbound_targets
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
    target = controller._outbound_targets["published-stream"]
    assert target["sequence"] == 0
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
        controller._outbound_delivery_states["unpublished-reaction"].value
        == "unknown"
    )
    assert controller._outbound_targets["published-stream"] is target
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
        controller._outbound_delivery_states["failed-publication"].value
        == "unknown"
    )
    assert "failed-publication" not in controller._outbound_targets
    await asyncio.gather(core.aclose(), runner.aclose())


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
    delivery_states = controller._outbound_delivery_states
    targets = controller._outbound_targets
    assert delivery_states["finalizing-cancel"].value == "unknown"
    assert "finalizing-cancel" not in targets


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

    def reject_credentials(value: object) -> None:
        """Simulate fixture platform authentication failure."""
        raise ValueError(f"invalid fixture credential: {value}")

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
    auth_traceback = "".join(
        traceback.format_exception(auth_failed.value),
    )
    assert "invalid-secret" not in auth_traceback
    assert auth_failed.value.__cause__ is None
    assert auth_failed.value.__context__ is None
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
async def test_secret_rpc_error_is_sanitized_before_rpc_response() -> None:
    """Consumer errors cannot copy a secret into an RPC error frame."""
    clock = Clock()
    secret_value = "secret-must-not-cross-rpc"

    def leak_rpc_error(value: object) -> None:
        """Raise a deliberately unsafe consumer error for regression."""
        raise RpcError(
            -32012,
            f"unsafe {value}",
            data={"credential": value},
        )

    consumer = FixtureSecretHandleConsumer(
        {("rpc-error-handle", 7): secret_value},
        leak_rpc_error,
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
        secret_handle_consumer=consumer,
        clock_ms=clock,
    )
    controller.register_rpc_methods(runner)
    CoreLifecycleAdapter(controller).register_rpc_methods(core)
    await asyncio.gather(core.start(), runner.start())
    await runner.call("runner.hello", _hello().to_mapping())
    with pytest.raises(RpcError) as failed:
        await core.call(
            "channel.prepare",
            {
                **_identity(),
                "host_context": {"secret_handle": "rpc-error-handle"},
                "capabilities": [],
            },
        )
    assert failed.value.data == {"reason_code": "PLATFORM_AUTH_FAILED"}
    assert failed.value.__cause__ is None
    frames = left_transport.sent_messages + right_transport.sent_messages
    assert all(secret_value not in frame for frame in frames)
    assert secret_value not in repr(consumer)
    await asyncio.gather(core.aclose(), runner.aclose())


@pytest.mark.asyncio
async def test_cancelled_prepare_fails_and_consumes_secret_handle() -> None:
    """Cancelled prepare clears context but never restores its handle."""
    clock = Clock()
    started = asyncio.Event()
    release = asyncio.Event()

    async def blocked_sink(_: object) -> None:
        """Block credential initialization until the request is cancelled."""
        started.set()
        await release.wait()

    consumer = FixtureSecretHandleConsumer(
        {("cancelled-handle", 7): "cancelled-secret"},
        blocked_sink,
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
        secret_handle_consumer=consumer,
        clock_ms=clock,
    )
    controller.register_rpc_methods(runner)
    CoreLifecycleAdapter(controller).register_rpc_methods(core)
    await asyncio.gather(core.start(), runner.start())
    await runner.call("runner.hello", _hello().to_mapping())
    prepare = asyncio.create_task(
        core.call(
            "channel.prepare",
            {
                **_identity(),
                "host_context": {"secret_handle": "cancelled-handle"},
                "capabilities": [],
            },
            timeout=1.0,
        ),
    )
    await asyncio.wait_for(started.wait(), timeout=1.0)
    await core.notify(
        "request.cancel",
        {"request_id": "rpc-1", "reason": "user_cancelled"},
    )
    with pytest.raises(RpcError) as cancelled:
        await prepare
    assert cancelled.value.data["reason_code"] == "REQUEST_CANCELLED"
    assert controller.state is RunnerState.FAILED
    assert controller.host_context is None
    assert not controller.effective_capabilities
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
                    "host_context": {"secret_handle": "cancelled-handle"},
                    "capabilities": [],
                },
            ),
        )
    assert consumed.value.data["reason_code"] == "SECRET_HANDLE_CONSUMED"
    await asyncio.gather(core.aclose(), runner.aclose())


@pytest.mark.asyncio
async def test_swallowed_secret_cancel_fails_prepare() -> None:
    """Prepare cancellation cannot be converted into a standby success."""
    clock = Clock()
    started = asyncio.Event()

    async def swallowing_sink(_: object) -> None:
        """Suppress cancellation after fixture credential initialization."""
        started.set()
        try:
            await asyncio.Future()
        except asyncio.CancelledError:
            pass

    consumer = FixtureSecretHandleConsumer(
        {("swallowed-handle", 7): "swallowed-secret"},
        swallowing_sink,
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
        secret_handle_consumer=consumer,
        clock_ms=clock,
    )
    controller.register_rpc_methods(runner)
    CoreLifecycleAdapter(controller).register_rpc_methods(core)
    await asyncio.gather(core.start(), runner.start())
    await runner.call("runner.hello", _hello().to_mapping())
    prepare = asyncio.create_task(
        core.call(
            "channel.prepare",
            {
                **_identity(),
                "host_context": {"secret_handle": "swallowed-handle"},
                "capabilities": [],
            },
        ),
    )
    await asyncio.wait_for(started.wait(), timeout=1.0)
    await core.notify(
        "request.cancel",
        {"request_id": "rpc-1", "reason": "user_cancelled"},
    )
    with pytest.raises(RpcError) as cancelled:
        await prepare
    assert cancelled.value.data["reason_code"] == "REQUEST_CANCELLED"
    assert controller.state is RunnerState.FAILED
    assert controller.host_context is None
    assert not controller.effective_capabilities
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
                    "host_context": {"secret_handle": "swallowed-handle"},
                    "capabilities": [],
                },
            ),
        )
    assert consumed.value.data["reason_code"] == "SECRET_HANDLE_CONSUMED"
    await asyncio.gather(core.aclose(), runner.aclose())


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
    assert created["state"] == "acknowledged"
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
    assert reaction["state"] == "acknowledged"
    assert consumed == [secret_value]
    frames = left_transport.sent_messages + right_transport.sent_messages
    assert any("rpc-fixture-handle" in frame for frame in frames)
    assert all(secret_value not in frame for frame in frames)
    assert controller.host_context is not None
    assert "secret_handle" not in controller.host_context.to_mapping()
    await asyncio.gather(core.aclose(), runner.aclose())
