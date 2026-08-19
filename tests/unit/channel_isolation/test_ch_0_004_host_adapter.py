# -*- coding: utf-8 -*-
"""Tests for CH-0-004 Core-owned host adapter behavior."""

# pylint: disable=protected-access

from __future__ import annotations

import asyncio
import math
from typing import Any

import pytest

from qwenpaw.channel_protocol import (
    CoreLifecycleAdapter,
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
    _identity,
    _response_event,
    _transport_pair,
    activate_outbound_controller,
)


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
    assert (await adapter.host_state_put(state))["status"] == "stored"
    assert (await adapter.host_state_get(state))["value"] == {"ok": True}
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
        await adapter.host_state_put(state)


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
        {**_identity(), "lease_token": "opaque", "lease_ttl_ms": 100},
    )
    await controller.activate(lease)
    await controller.commit(lease)
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
        await adapter.host_state_put(state)
    assert state_error.value.data["reason_code"] == "CAPABILITY_REQUIRED"


@pytest.mark.asyncio
async def test_response_handle_requires_capability_at_event_boundary() -> None:
    """Response-scoped events are rejected when the capability is absent."""
    clock = Clock()
    controller = _controller(clock)
    await activate_outbound_controller(controller, [])
    adapter = CoreLifecycleAdapter(controller)
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


@pytest.mark.asyncio
async def test_lease_expiry_removes_core_endpoint_registry() -> None:
    """Lease fencing also revokes Core routing state."""
    clock = Clock()
    adapter: CoreLifecycleAdapter
    unregistered = asyncio.Event()

    async def unregister_core_endpoint(
        operation: str,
        _: EndpointParams | None,
    ) -> None:
        """Model the Runner-to-Core unregister RPC client."""
        if operation == "unregister":
            await adapter.endpoint_unregister(
                IdentityParams.from_mapping(_identity()),
            )
            unregistered.set()

    controller = LifecycleController(
        channel_key="voice",
        instance_id="instance-1",
        generation=7,
        environment_spec_id="ches1_" + "1" * 64,
        environment_id="ches1_" + "1" * 64 + ".install1_" + "2" * 32,
        capabilities=(
            "host_state",
            "ingress_endpoint",
            "media",
        ),
        endpoint_handler=unregister_core_endpoint,
        clock_ms=clock,
    )
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
    await controller.endpoint_register(endpoint)
    await adapter.endpoint_register(endpoint)
    assert adapter.endpoints[7] == endpoint
    assert adapter.resolve_endpoint(7) == endpoint
    clock.now = 1011
    assert adapter.resolve_endpoint(7) is None
    assert not adapter.endpoints
    await controller.health(IdentityParams.from_mapping(_identity()))
    await asyncio.wait_for(unregistered.wait(), timeout=0.1)
    assert controller.state is RunnerState.FAILED
    assert not adapter.endpoints


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["quiesce", "stop"])
async def test_shutdown_detaches_endpoint_before_blocked_hook(
    operation: str,
) -> None:
    """Core routing is fenced before a blocked unregister RPC can run."""
    clock = Clock()
    hook_started = asyncio.Event()
    release_hook = asyncio.Event()
    core_unregistered = asyncio.Event()
    adapter: CoreLifecycleAdapter

    async def blocked_hook(
        hook_operation: str,
        _: EndpointParams | None,
    ) -> None:
        """Block before the Runner can issue endpoint.unregister."""
        if hook_operation == "unregister":
            hook_started.set()
            await release_hook.wait()
            await adapter.endpoint_unregister(
                IdentityParams.from_mapping(_identity()),
            )
            core_unregistered.set()

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
    await activate_outbound_controller(controller, ["ingress_endpoint"])
    await controller.endpoint_register(_endpoint())
    await adapter.endpoint_register(_endpoint())
    assert adapter.endpoints
    assert adapter.resolve_endpoint(7) == _endpoint()
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
    assert not core_unregistered.is_set()
    assert not adapter.endpoints
    assert adapter.resolve_endpoint(7) is None
    release_hook.set()
    if operation == "stop":
        await asyncio.wait_for(core_unregistered.wait(), timeout=0.1)


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
    await activate_outbound_controller(controller, ["ingress_endpoint"])
    await controller.endpoint_register(_endpoint())
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
        {**_identity(), "lease_token": "finite", "lease_ttl_ms": 100},
    )
    await controller.activate(lease)
    await controller.commit(lease)
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
