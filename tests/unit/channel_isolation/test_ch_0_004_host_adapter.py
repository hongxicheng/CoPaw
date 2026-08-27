# -*- coding: utf-8 -*-
"""Tests for CH-0-004 Core-owned host adapter behavior."""

# pylint: disable=protected-access

from __future__ import annotations

import asyncio
import math
from typing import Any, cast

import pytest

from qwenpaw.channel_protocol import (
    CoreEndpointRegistry,
    CoreGenerationAuthority,
    CoreLifecycleAdapter,
    DeliveryUpdateParams,
    EndpointParams,
    EventBatchParams,
    HostStateParams,
    IdentityParams,
    LeaseParams,
    LifecycleController,
    HostStateStore,
    PrepareParams,
    ProtocolValidationError,
    QuiesceParams,
    RpcError,
    RpcPeer,
    RunnerState,
    SendParams,
)
from tests.unit.channel_isolation._ch_0_004_support import (
    BlockingHostStateStore,
    Clock,
    RecordingRpcPeer,
    _controller,
    _endpoint,
    _hello,
    _hello_expectation,
    _identity,
    _response_event,
    _transport_pair,
    activate_outbound_controller,
)


def _prepare_params(generation: int = 7) -> PrepareParams:
    """Return the prepared endpoint capability set for one generation."""
    return PrepareParams.from_mapping(
        {
            **_identity(generation=generation),
            "host_context": {},
            "capabilities": ["ingress_endpoint"],
        },
    )


def _lease_params(
    *,
    generation: int = 7,
    token: str = "route-lease",
    ttl_ms: int = 100,
) -> LeaseParams:
    """Return a Core route lease fixture."""
    return LeaseParams.from_mapping(
        {
            **_identity(generation=generation),
            "lease_token": token,
            "lease_ttl_ms": ttl_ms,
        },
    )


class _LifecyclePeer:
    """Return valid lifecycle responses for Core control-path tests."""

    def __init__(self, generation: int = 7) -> None:
        self.generation = generation
        self.calls: list[str] = []

    async def call(
        self,
        method: str,
        _: object,
        *,
        timeout: float | None = None,
    ) -> object:
        """Record one control call and return its Runner state."""
        _ = timeout
        self.calls.append(method)
        if method == "channel.commit":
            return {
                "state": "active",
                "generation": self.generation,
                "consuming": True,
            }
        if method in {"channel.prepare", "channel.activate"}:
            return {"state": "standby", "generation": self.generation}
        if method == "channel.stop":
            return {"state": "stopped", "generation": self.generation}
        raise AssertionError(f"unexpected lifecycle method: {method}")


async def _authorize_registry(
    registry: CoreEndpointRegistry,
    *,
    generation: int = 7,
    token: str = "route-lease",
    ttl_ms: int = 100,
) -> LeaseParams:
    """Prepare, lease, and commit one Core-owned route generation."""
    lease = _lease_params(
        generation=generation,
        token=token,
        ttl_ms=ttl_ms,
    )
    prepare_token = await registry.authority.prepare_start(
        _prepare_params(generation),
    )
    await registry.authority.prepare_complete(prepare_token)
    activate_token = await registry.authority.activate_start(lease)
    await registry.authority.activate_complete(activate_token, lease)
    commit_token = await registry.authority.commit_start(lease)
    await registry.authority.commit_complete(commit_token, lease)
    return lease


async def _authorize_adapter(
    adapter: CoreLifecycleAdapter,
    *,
    capabilities: tuple[str, ...],
    token: str = "route-lease",
    ttl_ms: int = 100,
) -> LeaseParams:
    """Prepare, activate, and commit one Core adapter authority."""
    prepare = PrepareParams.from_mapping(
        {
            **_identity(),
            "host_context": {},
            "capabilities": list(capabilities),
        },
    )
    prepare_token = await adapter.authority.prepare_start(prepare)
    await adapter.authority.prepare_complete(prepare_token)
    lease = _lease_params(token=token, ttl_ms=ttl_ms)
    activate_token = await adapter.authority.activate_start(lease)
    await adapter.authority.activate_complete(activate_token, lease)
    commit_token = await adapter.authority.commit_start(lease)
    await adapter.authority.commit_complete(commit_token, lease)
    return lease


def test_rpc_method_registration_preserves_owner_direction() -> None:
    """Core and Runner register disjoint frozen method sets."""
    controller = _controller(Clock())
    core: Any = RecordingRpcPeer()
    runner: Any = RecordingRpcPeer()
    CoreLifecycleAdapter(controller).register_rpc_methods(core)
    controller.register_rpc_methods(runner)
    assert core.methods == {
        "runner.hello",
        "ingress.endpoint.register",
        "ingress.endpoint.update",
        "ingress.endpoint.unregister",
        "host.state.get",
        "host.state.put",
        "host.state.delete",
        "delivery.update",
        "event.batch",
    }
    assert runner.methods == {
        "channel.prepare",
        "channel.activate",
        "channel.commit",
        "channel.lease_renew",
        "channel.quiesce",
        "channel.health",
        "channel.generation_status",
        "channel.stop",
        "channel.send",
        "channel.reaction",
        "channel.response.finish",
    }
    assert core.methods.isdisjoint(runner.methods)


def test_lifecycle_module_preserves_host_imports() -> None:
    """The pre-refactor lifecycle import path remains compatible."""
    from qwenpaw.channel_protocol.lifecycle import (
        CoreLifecycleAdapter as LegacyCoreLifecycleAdapter,
    )
    from qwenpaw.channel_protocol.lifecycle import (
        HostStateStore as LegacyHostStateStore,
    )

    assert LegacyCoreLifecycleAdapter is CoreLifecycleAdapter
    assert LegacyHostStateStore is HostStateStore


@pytest.mark.asyncio
async def test_endpoint_and_host_state_require_active_generation() -> None:
    """Endpoint exposure and host writes honor standby and active fencing."""
    clock = Clock()
    controller = _controller(clock)
    adapter = CoreLifecycleAdapter(controller)
    peer = _LifecyclePeer()
    lifecycle = adapter.lifecycle_client(cast(RpcPeer, peer))
    prepare = PrepareParams.from_mapping(
        {
            **_identity(),
            "host_context": {},
            "capabilities": [
                "host_state",
                "ingress_endpoint",
                "media",
            ],
        },
    )
    await lifecycle.prepare(prepare)
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
    registered = await adapter.endpoint_register(endpoint)
    assert registered["status"] == "registered"
    lease = LeaseParams.from_mapping(
        {**_identity(), "lease_token": "opaque", "lease_ttl_ms": 100},
    )
    await lifecycle.activate(lease)
    with pytest.raises(RpcError):
        await adapter.endpoint_register(
            EndpointParams.from_mapping(
                {
                    **endpoint.to_mapping(),
                    "host": "0.0.0.0",
                    "bound_externally": False,
                    "auth_required": True,
                },
            ),
        )
    await lifecycle.commit(lease)
    await adapter.endpoint_register(endpoint)
    state = HostStateParams.from_mapping(
        {
            **_identity(),
            "key": "checkpoint",
            "schema_version": 1,
            "value": {"ok": True},
        },
    )
    assert (await adapter.host_state_put(state))["status"] == "stored"
    assert (await adapter.host_state_get(state))["value"] == {"ok": True}
    assert (await adapter.endpoint_update(endpoint))["status"] == "updated"
    await lifecycle.stop(
        IdentityParams.from_mapping(_identity()),
    )
    assert (
        await adapter.endpoint_unregister(
            IdentityParams.from_mapping(_identity()),
        )
    )["status"] == "unregistered"
    with pytest.raises(RpcError):
        await adapter.host_state_put(state)


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["preparing", "standby", "active"])
async def test_host_state_get_is_available_during_import_phases(
    phase: str,
) -> None:
    """Checkpoint reads remain available throughout candidate import."""
    adapter = CoreLifecycleAdapter(_controller(Clock()))
    await adapter.host_state_store.put("checkpoint", 1, {"ok": True})
    prepare = PrepareParams.from_mapping(
        {
            **_identity(),
            "host_context": {},
            "capabilities": ["host_state"],
        },
    )
    prepare_token = await adapter.authority.prepare_start(prepare)
    if phase in {"standby", "active"}:
        await adapter.authority.prepare_complete(prepare_token)
    if phase == "active":
        lease = _lease_params()
        activate = await adapter.authority.activate_start(lease)
        await adapter.authority.activate_complete(activate, lease)
        commit = await adapter.authority.commit_start(lease)
        await adapter.authority.commit_complete(commit, lease)
    result = await adapter.host_state_get(
        HostStateParams.from_mapping(
            {**_identity(), "key": "checkpoint"},
        ),
    )
    assert result["value"] == {"ok": True}


@pytest.mark.asyncio
async def test_mock_core_runner_completes_control_lifecycle() -> None:
    """A mock Core and Runner complete the v1 control handshake."""
    clock = Clock()
    left_transport, right_transport = _transport_pair()
    core = RpcPeer(left_transport)
    runner = RpcPeer(right_transport)
    controller = _controller(clock)
    controller.register_rpc_methods(runner)
    adapter = CoreLifecycleAdapter(controller, clock_ms=clock)
    adapter.register_rpc_methods(core)
    lifecycle = adapter.lifecycle_client(core)
    await core.start()
    await runner.start()

    hello = await runner.call("runner.hello", _hello().to_mapping())
    assert hello["protocol_version"] == 1
    prepare = PrepareParams.from_mapping(
        {
            **_identity(),
            "host_context": {},
            "capabilities": [
                "host_state",
                "ingress_endpoint",
                "media",
            ],
        },
    )
    prepared = await lifecycle.prepare(prepare)
    assert prepared["state"] == "standby"
    lease = LeaseParams.from_mapping(
        {**_identity(), "lease_token": "rpc-token", "lease_ttl_ms": 100},
    )
    assert (await lifecycle.activate(lease))["state"] == "standby"
    assert (await lifecycle.commit(lease))["state"] == "active"
    assert (await core.call("channel.health", _identity()))[
        "consuming"
    ] is True
    renewed = await lifecycle.lease_renew(
        LeaseParams.from_mapping(
            {
                **_identity(),
                "lease_token": "rpc-token",
                "lease_ttl_ms": 100,
            },
        ),
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
    assert (await lifecycle.stop(IdentityParams.from_mapping(_identity())))[
        "state"
    ] == "stopped"
    await core.aclose()
    await runner.aclose()


@pytest.mark.asyncio
async def test_core_route_authority_is_independent_from_runner_state() -> None:
    """Core fencing does not read the separate Runner controller object."""
    clock = Clock()
    left_transport, right_transport = _transport_pair()
    core = RpcPeer(left_transport)
    runner = RpcPeer(right_transport)
    runner_controller = _controller(clock)
    core_controller = _controller(clock)
    runner_controller.accept_hello(_hello())
    runner_controller.register_rpc_methods(runner)
    registry = CoreEndpointRegistry(
        channel_key="voice",
        instance_id="instance-1",
        clock_ms=clock,
    )
    adapter = CoreLifecycleAdapter(
        core_controller,
        endpoint_registry=registry,
    )
    adapter.register_rpc_methods(core)
    lifecycle = adapter.lifecycle_client(core)
    await core.start()
    await runner.start()
    try:
        await runner.call("runner.hello", _hello().to_mapping())
        prepare = _prepare_params()
        await lifecycle.prepare(prepare)
        await runner.call(
            "ingress.endpoint.register",
            _endpoint().to_mapping(),
        )
        assert adapter.resolve_endpoint(7) is None
        lease = _lease_params()
        await lifecycle.activate(lease)
        assert adapter.resolve_endpoint(7) is None
        await lifecycle.commit(lease)
        assert runner_controller.state is RunnerState.ACTIVE
        assert core_controller.state is RunnerState.CREATED
        assert adapter.resolve_endpoint(7) == _endpoint()
        await lifecycle.quiesce(
            QuiesceParams.from_mapping(
                {**_identity(), "drain_timeout_ms": 10},
            ),
        )
        assert runner_controller.state is RunnerState.QUIESCING
        assert core_controller.state is RunnerState.CREATED
        assert adapter.resolve_endpoint(7) is None
        await runner.call(
            "ingress.endpoint.unregister",
            IdentityParams.from_mapping(_identity()).to_mapping(),
        )
        assert adapter.resolve_endpoint(7) is None
    finally:
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
        **_hello_expectation(),
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
    adapter = CoreLifecycleAdapter(controller, host_state_store=store)
    await _authorize_adapter(
        adapter,
        capabilities=("host_state", "ingress_endpoint", "media"),
        token="opaque",
    )
    with pytest.raises(ProtocolValidationError) as limit_error:
        await adapter.host_state_put(
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
    adapter = CoreLifecycleAdapter(controller)
    await activate_outbound_controller(controller, ["ingress_endpoint"])
    await _authorize_adapter(
        adapter,
        capabilities=("ingress_endpoint",),
        token="subset",
    )
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
        await adapter.host_state_put(state)
    assert state_error.value.data["reason_code"] == "CAPABILITY_REQUIRED"


@pytest.mark.asyncio
async def test_response_handle_requires_capability_at_event_boundary() -> None:
    """Response-scoped events are rejected when the capability is absent."""
    clock = Clock()
    controller = _controller(clock)
    await activate_outbound_controller(controller, [])
    adapter = CoreLifecycleAdapter(controller)
    await _authorize_adapter(adapter, capabilities=())
    ack = await adapter.event_batch(
        EventBatchParams.from_mapping(
            {
                "batch_id": "batch-1",
                "events": [_response_event().to_mapping()],
            },
        ),
    )
    assert ack["accepted_event_ids"] == []
    assert ack["rejected_events"] == [
        {
            "event_id": "event-1",
            "reason_code": "CAPABILITY_REQUIRED",
            "retryable": False,
        },
    ]
    assert not adapter.inbound_inbox.persisted_event_ids


def test_core_authority_must_match_hello_identity() -> None:
    """Compatibility hello identity cannot diverge from Core authority."""
    authority = CoreGenerationAuthority(
        channel_key="other",
        instance_id="instance-1",
    )
    with pytest.raises(ValueError, match="identity must match"):
        CoreLifecycleAdapter(_controller(Clock()), authority=authority)


@pytest.mark.asyncio
async def test_revoked_authority_rejects_all_formal_host_operations() -> None:
    """Host State, events, and delivery share one revoked Core authority."""
    adapter = CoreLifecycleAdapter(_controller(Clock()))
    lifecycle = adapter.lifecycle_client(
        cast(RpcPeer, _LifecyclePeer()),
    )
    await lifecycle.prepare(
        PrepareParams.from_mapping(
            {
                **_identity(),
                "host_context": {},
                "capabilities": ["host_state", "response_lifecycle"],
            },
        ),
    )
    lease = _lease_params()
    await lifecycle.activate(lease)
    await lifecycle.commit(lease)
    await lifecycle.stop(
        IdentityParams.from_mapping(_identity()),
    )
    state = HostStateParams.from_mapping(
        {**_identity(), "key": "revoked", "value": {"ok": True}},
    )
    event = EventBatchParams(
        batch_id="revoked-batch",
        events=(_response_event(),),
        identity=IdentityParams.from_mapping(_identity()),
    )
    delivery = DeliveryUpdateParams.from_mapping(
        {
            **_identity(),
            "delivery_id": "revoked-delivery",
            "state": "acknowledged",
        },
    )
    for operation in (
        adapter.host_state_put(state),
        adapter.event_batch(event),
        adapter.delivery_update(delivery),
    ):
        with pytest.raises(RpcError) as error:
            await operation
        assert error.value.data["reason_code"] == "GENERATION_REVOKED"


@pytest.mark.asyncio
async def test_independent_rpc_authority_admits_core_host_methods() -> None:
    """Core Host RPCs use authority rather than Runner lifecycle state."""
    clock = Clock()
    left_transport, right_transport = _transport_pair()
    core = RpcPeer(left_transport)
    runner = RpcPeer(right_transport)
    runner_controller = _controller(clock)
    runner_controller.register_rpc_methods(runner)
    core_controller = _controller(clock)
    adapter = CoreLifecycleAdapter(core_controller, clock_ms=clock)
    adapter.register_rpc_methods(core)
    lifecycle = adapter.lifecycle_client(core)
    await core.start()
    await runner.start()
    try:
        runner_controller.accept_hello(_hello())
        prepare = PrepareParams.from_mapping(
            {
                **_identity(),
                "host_context": {},
                "capabilities": [
                    "host_state",
                    "response_lifecycle",
                ],
            },
        )
        await lifecycle.prepare(prepare)
        lease = _lease_params()
        await lifecycle.activate(lease)
        await lifecycle.commit(lease)
        state = HostStateParams.from_mapping(
            {**_identity(), "key": "independent", "value": {"ok": True}},
        )
        assert (await runner.call("host.state.put", state.to_mapping()))[
            "status"
        ] == "stored"
        event = _response_event()
        ack = await runner.call(
            "event.batch",
            EventBatchParams(
                batch_id="independent-batch",
                events=(event,),
                identity=IdentityParams.from_mapping(_identity()),
            ).to_mapping(),
        )
        assert ack["accepted_event_ids"] == ["event-1"]
        delivery = DeliveryUpdateParams.from_mapping(
            {
                **_identity(),
                "delivery_id": "independent-delivery",
                "state": "acknowledged",
            },
        )
        adapter.delivery_ledger.request("independent-delivery")
        result = await runner.call(
            "delivery.update",
            delivery.to_mapping(),
        )
        assert result["state"] == "acknowledged"
        assert core_controller.state is RunnerState.CREATED
        assert runner_controller.state is RunnerState.ACTIVE
    finally:
        await core.aclose()
        await runner.aclose()


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
        **_hello_expectation(),
        capabilities=("ingress_endpoint",),
        endpoint_handler=reentrant_hook,
        clock_ms=clock,
    )
    await activate_outbound_controller(controller, ["ingress_endpoint"])
    await controller.endpoint_register(_endpoint())
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
    lifecycle = adapter.lifecycle_client(
        cast(RpcPeer, _LifecyclePeer()),
    )
    await lifecycle.prepare(
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
    lease = _lease_params(token="blocked")
    await lifecycle.activate(lease)
    await lifecycle.commit(lease)
    params = HostStateParams.from_mapping(
        {**_identity(), "key": "blocked", "value": {"ok": True}},
    )
    write = asyncio.create_task(adapter.host_state_put(params))
    await store.started.wait()
    stop = asyncio.create_task(
        lifecycle.stop(
            IdentityParams.from_mapping(_identity()),
        ),
    )
    await asyncio.sleep(0)
    assert not stop.done()
    store.release.set()
    assert (await write)["status"] == "stored"
    await stop
    assert await store.get("blocked") == (1, {"ok": True})


@pytest.mark.asyncio
async def test_host_state_rejects_non_finite_numbers() -> None:
    """Host State accepts only strict JSON values."""
    clock = Clock()
    controller = _controller(clock)
    adapter = CoreLifecycleAdapter(controller)
    await _authorize_adapter(
        adapter,
        capabilities=("host_state", "ingress_endpoint", "media"),
        token="finite",
    )
    with pytest.raises(ProtocolValidationError) as value_error:
        await adapter.host_state_put(
            HostStateParams.from_mapping(
                {
                    **_identity(),
                    "key": "non-finite",
                    "value": {"score": math.nan},
                },
            ),
        )
    assert value_error.value.reason_code == "SCHEMA_MISMATCH"
