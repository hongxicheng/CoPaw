# -*- coding: utf-8 -*-
"""Tests for CH-0-004 Core-owned generation authority."""

from __future__ import annotations

import asyncio
from dataclasses import FrozenInstanceError
from typing import Awaitable, Callable, cast

import pytest

from qwenpaw.channel_protocol import (
    CoreEndpointRegistry,
    CoreGenerationAuthority,
    CoreLifecycleAdapter,
    CoreLifecycleClient,
    EndpointParams,
    HostStateParams,
    IdentityParams,
    LeaseParams,
    LifecycleController,
    PrepareParams,
    QuiesceParams,
    RpcError,
    RpcPeer,
    RpcTimeoutError,
)
from qwenpaw.channel_protocol.core_lifecycle import CoreOperationToken
from tests.unit.channel_isolation._ch_0_004_support import (
    Clock,
    _controller,
    _endpoint,
    _hello,
    _identity,
    _transport_pair,
)


def _prepare_params(generation: int = 7) -> PrepareParams:
    """Return the endpoint capability set for one candidate."""
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
    prepare = await registry.authority.prepare_start(
        _prepare_params(generation),
    )
    await registry.authority.prepare_complete(prepare)
    activate = await registry.authority.activate_start(lease)
    await registry.authority.activate_complete(activate, lease)
    commit = await registry.authority.commit_start(lease)
    await registry.authority.commit_complete(commit, lease)
    return lease


def _runner_result(method: str, generation: int) -> dict[str, object]:
    """Return one valid Runner lifecycle response."""
    if method == "channel.commit":
        return {
            "state": "active",
            "generation": generation,
            "consuming": True,
        }
    if method in {"channel.prepare", "channel.activate"}:
        return {"state": "standby", "generation": generation}
    if method == "channel.lease_renew":
        return {"state": "active", "generation": generation}
    if method == "channel.quiesce":
        return {"state": "quiescing", "generation": generation}
    if method == "channel.stop":
        return {"state": "stopped", "generation": generation}
    raise AssertionError(f"unexpected lifecycle method: {method}")


class _ScriptedPeer:
    """Provide default lifecycle results with focused method overrides."""

    def __init__(
        self,
        generation: int,
        handlers: dict[str, Callable[[], Awaitable[object]]] | None = None,
    ) -> None:
        self.generation = generation
        self.handlers = handlers or {}
        self.calls: list[str] = []

    async def call(
        self,
        method: str,
        _: object,
        *,
        timeout: float | None = None,
    ) -> object:
        """Run an override or return the default generation result."""
        _ = timeout
        self.calls.append(method)
        handler = self.handlers.get(method)
        if handler is not None:
            return await handler()
        return _runner_result(method, self.generation)


async def _activate_client(
    lifecycle: CoreLifecycleClient,
    *,
    generation: int = 7,
    token: str = "route-lease",
) -> LeaseParams:
    """Prepare, activate, and commit one peer-bound client."""
    lease = _lease_params(generation=generation, token=token)
    await lifecycle.prepare(_prepare_params(generation))
    await lifecycle.activate(lease)
    await lifecycle.commit(lease)
    return lease


def test_endpoint_registry_must_match_authority_identity() -> None:
    """Endpoint storage cannot diverge from its lifecycle authority."""
    authority = CoreGenerationAuthority(
        channel_key="other",
        instance_id="instance-1",
    )
    with pytest.raises(ValueError, match="identity must match"):
        CoreEndpointRegistry(
            channel_key="voice",
            instance_id="instance-1",
            authority=authority,
        )


@pytest.mark.asyncio
async def test_control_timeout_keeps_generation_revoked() -> None:
    """A failed control RPC cannot restore a route revoked before send."""
    clock = Clock()
    registry = CoreEndpointRegistry(
        channel_key="voice",
        instance_id="instance-1",
        clock_ms=clock,
    )

    async def timeout_stop() -> object:
        """Fail after observing the pre-call Core fencing boundary."""
        assert registry.resolve(7) is None
        raise RpcTimeoutError("stop timed out")

    peer = _ScriptedPeer(7, {"channel.stop": timeout_stop})
    lifecycle = CoreLifecycleClient(
        cast(RpcPeer, peer),
        registry.authority,
        registry.prune,
    )
    lease = await _activate_client(lifecycle)
    registry.register(_endpoint())
    with pytest.raises(RpcTimeoutError):
        await lifecycle.stop(IdentityParams.from_mapping(_identity()))
    assert registry.resolve(7) is None
    with pytest.raises(RpcError) as renew_error:
        await registry.authority.renew_start(lease)
    assert renew_error.value.data["reason_code"] == "GENERATION_REVOKED"


@pytest.mark.asyncio
async def test_stop_can_retry_after_timeout_without_restoring_route() -> None:
    """A timed-out stop remains callable while authorization stays fenced."""
    registry = CoreEndpointRegistry(
        channel_key="voice",
        instance_id="instance-1",
    )

    stop_calls = 0

    async def retry_stop() -> object:
        """Fail the first stop call and accept its explicit retry."""
        nonlocal stop_calls
        assert registry.resolve(7) is None
        stop_calls += 1
        if stop_calls == 1:
            raise RpcTimeoutError("stop timed out")
        return {"state": "stopped", "generation": 7}

    peer = _ScriptedPeer(7, {"channel.stop": retry_stop})
    lifecycle = CoreLifecycleClient(
        cast(RpcPeer, peer),
        registry.authority,
        registry.prune,
    )
    await _activate_client(lifecycle)
    registry.register(_endpoint())
    params = IdentityParams.from_mapping(_identity())
    with pytest.raises(RpcTimeoutError):
        await lifecycle.stop(params)
    assert registry.resolve(7) is None
    assert (await lifecycle.stop(params))["state"] == "stopped"
    assert stop_calls == 2
    assert registry.resolve(7) is None


@pytest.mark.asyncio
async def test_old_stop_retry_survives_candidate_prepare_abort() -> None:
    """A failed candidate cannot erase an old peer's stop capability."""
    registry = CoreEndpointRegistry(
        channel_key="voice",
        instance_id="instance-1",
    )

    stop_calls = 0

    async def retry_old_stop() -> object:
        """Time out once, then accept the old Runner stop retry."""
        nonlocal stop_calls
        stop_calls += 1
        if stop_calls == 1:
            raise RpcTimeoutError("old stop timed out")
        return {"state": "stopped", "generation": 7}

    old_peer = _ScriptedPeer(7, {"channel.stop": retry_old_stop})
    old_client = CoreLifecycleClient(
        cast(RpcPeer, old_peer),
        registry.authority,
        registry.prune,
    )
    await _activate_client(old_client)
    old_params = IdentityParams.from_mapping(_identity())
    with pytest.raises(RpcTimeoutError):
        await old_client.stop(old_params)

    async def fail_candidate_prepare() -> object:
        """Fail the replacement after local candidate admission."""
        raise RpcTimeoutError("candidate prepare timed out")

    candidate_peer = _ScriptedPeer(
        8,
        {"channel.prepare": fail_candidate_prepare},
    )
    candidate = CoreLifecycleClient(
        cast(RpcPeer, candidate_peer),
        registry.authority,
        registry.prune,
    )
    with pytest.raises(RpcTimeoutError):
        await candidate.prepare(_prepare_params(generation=8))
    assert registry.authority.snapshot.candidate is None
    assert (await old_client.stop(old_params))["state"] == "stopped"
    assert stop_calls == 2


@pytest.mark.asyncio
async def test_old_stop_retry_does_not_revoke_new_active_generation() -> None:
    """An old peer remains stoppable after a replacement commits."""
    registry = CoreEndpointRegistry(
        channel_key="voice",
        instance_id="instance-1",
    )

    stop_calls = 0

    async def retry_old_stop() -> object:
        """Keep the first old stop result uncertain."""
        nonlocal stop_calls
        stop_calls += 1
        if stop_calls == 1:
            raise RpcTimeoutError("old stop timed out")
        assert registry.resolve(8) is not None
        return {"state": "stopped", "generation": 7}

    old_peer = _ScriptedPeer(7, {"channel.stop": retry_old_stop})
    old_client = CoreLifecycleClient(
        cast(RpcPeer, old_peer),
        registry.authority,
        registry.prune,
    )
    await _activate_client(old_client)
    with pytest.raises(RpcTimeoutError):
        await old_client.stop(IdentityParams.from_mapping(_identity()))

    new_client = CoreLifecycleClient(
        cast(RpcPeer, _ScriptedPeer(8)),
        registry.authority,
        registry.prune,
    )
    await _activate_client(
        new_client,
        generation=8,
        token="next-lease",
    )
    endpoint = EndpointParams.from_mapping(
        {**_endpoint().to_mapping(), **_identity(generation=8)},
    )
    registry.register(endpoint)
    assert registry.resolve(8) == endpoint

    await old_client.stop(IdentityParams.from_mapping(_identity()))
    assert stop_calls == 2
    assert registry.resolve(8) == endpoint


@pytest.mark.asyncio
async def test_old_token_cannot_revoke_same_generation_new_epoch() -> None:
    """An old peer token cannot fence a reused generation slot."""
    registry = CoreEndpointRegistry(
        channel_key="voice",
        instance_id="instance-1",
    )

    stop_calls = 0

    async def fail_old_prepare() -> object:
        """Keep the old candidate result uncertain."""
        raise RpcTimeoutError("old prepare timed out")

    async def stop_old_peer() -> object:
        """Accept shutdown on the failed old peer."""
        nonlocal stop_calls
        stop_calls += 1
        return {"state": "stopped", "generation": 7}

    old_peer = _ScriptedPeer(
        7,
        {
            "channel.prepare": fail_old_prepare,
            "channel.stop": stop_old_peer,
        },
    )
    old_client = CoreLifecycleClient(
        cast(RpcPeer, old_peer),
        registry.authority,
        registry.prune,
    )
    with pytest.raises(RpcTimeoutError):
        await old_client.prepare(_prepare_params())

    new_client = CoreLifecycleClient(
        cast(RpcPeer, _ScriptedPeer(7)),
        registry.authority,
        registry.prune,
    )
    await _activate_client(new_client)
    registry.register(_endpoint())
    assert registry.resolve(7) == _endpoint()

    await old_client.stop(IdentityParams.from_mapping(_identity()))
    assert stop_calls == 1
    assert registry.resolve(7) == _endpoint()


@pytest.mark.asyncio
async def test_quiesced_old_peer_survives_candidate_replacement() -> None:
    """Candidate replacement cannot erase an old peer's stop capability."""
    registry = CoreEndpointRegistry(
        channel_key="voice",
        instance_id="instance-1",
    )

    stop_calls = 0

    async def timeout_old_quiesce() -> object:
        """Keep the old quiesce result uncertain."""
        raise RpcTimeoutError("old quiesce timed out")

    async def stop_old_peer() -> object:
        """Accept stop on the same old peer."""
        nonlocal stop_calls
        stop_calls += 1
        return {"state": "stopped", "generation": 7}

    old_peer = _ScriptedPeer(
        7,
        {
            "channel.quiesce": timeout_old_quiesce,
            "channel.stop": stop_old_peer,
        },
    )
    old_client = CoreLifecycleClient(
        cast(RpcPeer, old_peer),
        registry.authority,
        registry.prune,
    )
    await _activate_client(old_client)
    with pytest.raises(RpcTimeoutError):
        await old_client.quiesce(
            QuiesceParams.from_mapping(
                {**_identity(), "drain_timeout_ms": 10},
            ),
        )

    candidate_8 = CoreLifecycleClient(
        cast(RpcPeer, _ScriptedPeer(8)),
        registry.authority,
        registry.prune,
    )
    candidate_9 = CoreLifecycleClient(
        cast(RpcPeer, _ScriptedPeer(9)),
        registry.authority,
        registry.prune,
    )
    await candidate_8.prepare(_prepare_params(generation=8))
    await candidate_9.prepare(_prepare_params(generation=9))
    assert registry.authority.snapshot.candidate is not None
    assert registry.authority.snapshot.candidate.generation == 9

    result = await old_client.stop(
        IdentityParams.from_mapping(_identity()),
    )
    assert result["state"] == "stopped"
    assert stop_calls == 1
    assert registry.authority.snapshot.candidate is not None
    assert registry.authority.snapshot.candidate.generation == 9


@pytest.mark.asyncio
async def test_real_rpc_timeout_keeps_generation_revoked() -> None:
    """The exported Core call path fences before a real blocked RPC."""
    clock = Clock()
    hook_started = asyncio.Event()

    async def blocked_unregister(
        operation: str,
        _: EndpointParams | None,
    ) -> None:
        """Keep quiesce inside the Runner after Core sends the request."""
        if operation == "unregister":
            hook_started.set()
            await asyncio.Event().wait()

    left_transport, right_transport = _transport_pair()
    core = RpcPeer(left_transport)
    runner = RpcPeer(right_transport)
    controller = LifecycleController(
        channel_key="voice",
        instance_id="instance-1",
        generation=7,
        environment_spec_id="ches1_" + "1" * 64,
        environment_id="ches1_" + "1" * 64 + ".install1_" + "2" * 32,
        capabilities=("ingress_endpoint",),
        endpoint_handler=blocked_unregister,
        clock_ms=clock,
    )
    controller.accept_hello(_hello())
    controller.register_rpc_methods(runner)
    registry = CoreEndpointRegistry(
        channel_key="voice",
        instance_id="instance-1",
        clock_ms=clock,
    )
    adapter = CoreLifecycleAdapter(
        _controller(clock),
        endpoint_registry=registry,
    )
    adapter.register_rpc_methods(core)
    lifecycle = adapter.lifecycle_client(core)
    await core.start()
    await runner.start()
    try:
        await lifecycle.prepare(_prepare_params())
        lease = _lease_params()
        await lifecycle.activate(lease)
        await lifecycle.commit(lease)
        await controller.endpoint_register(_endpoint())
        await adapter.endpoint_register(_endpoint())
        assert adapter.resolve_endpoint(7) == _endpoint()
        with pytest.raises(RpcTimeoutError):
            await lifecycle.quiesce(
                QuiesceParams.from_mapping(
                    {**_identity(), "drain_timeout_ms": 1000},
                ),
                timeout=0.01,
            )
        await asyncio.wait_for(hook_started.wait(), timeout=0.1)
        assert adapter.resolve_endpoint(7) is None
        with pytest.raises(RpcError) as update_error:
            await adapter.endpoint_update(_endpoint())
        assert update_error.value.data["reason_code"] == "GENERATION_REVOKED"
    finally:
        await core.aclose()
        await runner.aclose()


@pytest.mark.parametrize(
    ("readiness", "quiescing", "expected"),
    [
        ("starting", False, False),
        ("ready", False, True),
        ("degraded", False, False),
        ("stopped", False, False),
        ("ready", True, False),
    ],
)
@pytest.mark.asyncio
async def test_endpoint_readiness_controls_formal_routing(
    readiness: str,
    quiescing: bool,
    expected: bool,
) -> None:
    """Only ready, non-quiescing endpoints accept formal traffic."""
    clock = Clock()
    registry = CoreEndpointRegistry(
        channel_key="voice",
        instance_id="instance-1",
        clock_ms=clock,
    )
    await _authorize_registry(registry)
    endpoint = EndpointParams.from_mapping(
        {
            **_endpoint().to_mapping(),
            "readiness": readiness,
            "quiescing": quiescing,
        },
    )
    registry.register(endpoint)
    assert (registry.resolve(7) is not None) is expected


@pytest.mark.asyncio
async def test_ready_update_recovers_only_before_lifecycle_revoke() -> None:
    """Health updates recover routes, but lifecycle fencing is monotonic."""
    clock = Clock()
    registry = CoreEndpointRegistry(
        channel_key="voice",
        instance_id="instance-1",
        clock_ms=clock,
    )
    lifecycle = CoreLifecycleClient(
        cast(RpcPeer, _ScriptedPeer(7)),
        registry.authority,
        registry.prune,
    )
    await _activate_client(lifecycle)
    degraded = EndpointParams.from_mapping(
        {**_endpoint().to_mapping(), "readiness": "degraded"},
    )
    registry.register(degraded)
    assert registry.resolve(7) is None
    registry.register(_endpoint())
    assert registry.resolve(7) == _endpoint()
    await lifecycle.stop(IdentityParams.from_mapping(_identity()))
    with pytest.raises(RpcError) as update_error:
        registry.register(_endpoint())
    assert update_error.value.data["reason_code"] == "GENERATION_REVOKED"


@pytest.mark.asyncio
async def test_new_generation_commit_fences_late_old_updates() -> None:
    """A newer commit prevents old generation traffic from returning."""
    clock = Clock()
    registry = CoreEndpointRegistry(
        channel_key="voice",
        instance_id="instance-1",
        clock_ms=clock,
    )
    await _authorize_registry(registry)
    registry.register(_endpoint())
    await _authorize_registry(
        registry,
        generation=8,
        token="next-lease",
    )
    next_endpoint = EndpointParams.from_mapping(
        {**_endpoint().to_mapping(), **_identity(generation=8)},
    )
    registry.register(next_endpoint)
    assert registry.resolve(7) is None
    assert registry.resolve(8) == next_endpoint
    with pytest.raises(RpcError) as old_update:
        registry.register(_endpoint())
    assert old_update.value.data["reason_code"] == "GENERATION_REVOKED"


@pytest.mark.asyncio
async def test_lease_expiry_removes_core_endpoint_registry() -> None:
    """Core-clock expiry fences routing without Runner health polling."""
    clock = Clock()
    registry = CoreEndpointRegistry(
        channel_key="voice",
        instance_id="instance-1",
        clock_ms=clock,
    )
    lease = await _authorize_registry(
        registry,
        token="expire",
        ttl_ms=10,
    )
    registry.register(_endpoint())
    assert registry.resolve(7) == _endpoint()
    clock.now = 1011
    assert registry.resolve(7) is None
    with pytest.raises(RpcError) as renew_error:
        await registry.authority.renew_start(lease)
    assert renew_error.value.data["reason_code"] == "LEASE_EXPIRED"
    with pytest.raises(RpcError) as register_error:
        registry.register(_endpoint())
    assert register_error.value.data["reason_code"] == "GENERATION_REVOKED"
    with pytest.raises(RpcError) as prepare_error:
        await registry.authority.prepare_start(_prepare_params())
    assert prepare_error.value.data["reason_code"] == "GENERATION_REVOKED"


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["quiesce", "stop"])
async def test_shutdown_detaches_endpoint_before_blocked_hook(
    operation: str,
) -> None:
    """Core revoke precedes a blocked Runner control request."""
    clock = Clock()
    registry = CoreEndpointRegistry(
        channel_key="voice",
        instance_id="instance-1",
        clock_ms=clock,
    )
    entered = asyncio.Event()

    class BlockingPeer:
        """Assert route fencing before blocking one control RPC."""

        async def call(
            self,
            method: str,
            _: object,
            *,
            timeout: float | None = None,
        ) -> object:
            _ = timeout
            if method not in {"channel.quiesce", "channel.stop"}:
                return _runner_result(method, 7)
            assert method == f"channel.{operation}"
            assert registry.resolve(7) is None
            entered.set()
            await asyncio.Event().wait()
            raise AssertionError("blocking call unexpectedly resumed")

    lifecycle = CoreLifecycleClient(
        cast(RpcPeer, BlockingPeer()),
        registry.authority,
        registry.prune,
    )
    await _activate_client(lifecycle)
    registry.register(_endpoint())
    if operation == "quiesce":
        request = asyncio.create_task(
            lifecycle.quiesce(
                QuiesceParams.from_mapping(
                    {**_identity(), "drain_timeout_ms": 10},
                ),
            ),
        )
    else:
        request = asyncio.create_task(
            lifecycle.stop(IdentityParams.from_mapping(_identity())),
        )
    await asyncio.wait_for(entered.wait(), timeout=0.1)
    assert registry.resolve(7) is None
    request.cancel()
    with pytest.raises(asyncio.CancelledError):
        await request
    with pytest.raises(RpcError) as update_error:
        registry.register(_endpoint())
    assert update_error.value.data["reason_code"] == "GENERATION_REVOKED"


@pytest.mark.asyncio
async def test_candidate_keeps_active_route_until_commit() -> None:
    """A standby replacement cannot hide the currently active route."""
    clock = Clock()
    registry = CoreEndpointRegistry(
        channel_key="voice",
        instance_id="instance-1",
        clock_ms=clock,
    )
    await _authorize_registry(registry)
    registry.register(_endpoint())
    assert registry.resolve(7) == _endpoint()

    candidate = _prepare_params(generation=8)
    prepare_token = await registry.authority.prepare_start(candidate)
    await registry.authority.prepare_complete(prepare_token)
    candidate_endpoint = EndpointParams.from_mapping(
        {**_endpoint().to_mapping(), **_identity(generation=8)},
    )
    registry.register(candidate_endpoint)
    assert registry.resolve(7) == _endpoint()
    assert registry.resolve(8) is None

    lease = _lease_params(generation=8, token="next")
    activate_token = await registry.authority.activate_start(lease)
    await registry.authority.activate_complete(activate_token, lease)
    assert registry.resolve(7) == _endpoint()
    commit_token = await registry.authority.commit_start(lease)
    await registry.authority.commit_complete(commit_token, lease)
    registry.prune()
    assert registry.resolve(7) is None
    assert registry.resolve(8) == candidate_endpoint


@pytest.mark.asyncio
async def test_reprepared_candidate_cannot_reuse_old_endpoint() -> None:
    """A failed candidate endpoint is fenced by its authority epoch."""
    registry = CoreEndpointRegistry(
        channel_key="voice",
        instance_id="instance-1",
    )
    params = _prepare_params()
    first = await registry.authority.prepare_start(params)
    registry.register(_endpoint())
    first_epoch = registry.authority.snapshot.candidate.epoch
    await registry.authority.prepare_abort(first)
    second = await registry.authority.prepare_start(params)
    await registry.authority.prepare_complete(second)
    second_epoch = registry.authority.snapshot.candidate.epoch
    assert registry.resolve(7) is None
    assert second_epoch > first_epoch


@pytest.mark.asyncio
async def test_bound_client_rejects_second_prepare_before_runner() -> None:
    """One client cannot silently attach to another Runner generation."""
    registry = CoreEndpointRegistry(
        channel_key="voice",
        instance_id="instance-1",
    )

    peer = _ScriptedPeer(7)
    client = CoreLifecycleClient(
        cast(RpcPeer, peer),
        registry.authority,
        registry.prune,
    )
    await _activate_client(client)
    with pytest.raises(RpcError) as error:
        await client.prepare(_prepare_params(generation=8))
    assert error.value.data["reason_code"] == "INVALID_STATE_TRANSITION"
    assert peer.calls.count("channel.prepare") == 1


@pytest.mark.asyncio
async def test_concurrent_prepare_binds_client_only_once() -> None:
    """Concurrent prepare calls cannot bind one client to two epochs."""
    registry = CoreEndpointRegistry(
        channel_key="voice",
        instance_id="instance-1",
    )
    entered = asyncio.Event()
    release = asyncio.Event()

    class BlockingPeer:
        """Hold the first prepare while the second attempts admission."""

        def __init__(self) -> None:
            self.prepare_calls = 0

        async def call(
            self,
            method: str,
            _: object,
            *,
            timeout: float | None = None,
        ) -> object:
            _ = timeout
            assert method == "channel.prepare"
            self.prepare_calls += 1
            entered.set()
            await release.wait()
            return _runner_result(method, 7)

    peer = BlockingPeer()
    client = CoreLifecycleClient(
        cast(RpcPeer, peer),
        registry.authority,
        registry.prune,
    )
    first = asyncio.create_task(client.prepare(_prepare_params()))
    await asyncio.wait_for(entered.wait(), timeout=0.1)
    with pytest.raises(RpcError) as error:
        await client.prepare(_prepare_params(generation=8))
    assert error.value.data["reason_code"] == "INVALID_STATE_TRANSITION"
    release.set()
    await first
    assert peer.prepare_calls == 1
    assert registry.authority.snapshot.candidate is not None
    assert registry.authority.snapshot.candidate.generation == 7


@pytest.mark.asyncio
async def test_unbound_client_cannot_control_another_client_slot() -> None:
    """A new client cannot stop, activate, or renew another peer's slot."""
    registry = CoreEndpointRegistry(
        channel_key="voice",
        instance_id="instance-1",
    )

    owner = CoreLifecycleClient(
        cast(RpcPeer, _ScriptedPeer(7)),
        registry.authority,
        registry.prune,
    )
    lease = await _activate_client(owner)

    class RejectUnexpectedPeer:
        """Fail if another client reaches any Runner control method."""

        async def call(self, *_: object, **__: object) -> object:
            raise AssertionError("unbound client reached Runner")

    other = CoreLifecycleClient(
        cast(RpcPeer, RejectUnexpectedPeer()),
        registry.authority,
        registry.prune,
    )
    with pytest.raises(RpcError) as renew_error:
        await other.lease_renew(lease)
    assert renew_error.value.data["reason_code"] == "GENERATION_UNKNOWN"
    with pytest.raises(RpcError) as stop_error:
        await other.stop(IdentityParams.from_mapping(_identity()))
    assert stop_error.value.data["reason_code"] == "GENERATION_UNKNOWN"
    assert registry.authority.snapshot.active is not None
    assert registry.authority.snapshot.active.generation == 7


@pytest.mark.asyncio
async def test_control_token_cannot_move_between_clients() -> None:
    """A copied opaque token is still bound to its original client."""
    registry = CoreEndpointRegistry(
        channel_key="voice",
        instance_id="instance-1",
    )

    owner = CoreLifecycleClient(
        cast(RpcPeer, _ScriptedPeer(7)),
        registry.authority,
        registry.prune,
    )
    await _activate_client(owner)

    class RejectUnexpectedPeer:
        """Fail if a copied token reaches another Runner peer."""

        async def call(self, *_: object, **__: object) -> object:
            raise AssertionError("copied token reached another peer")

    other = CoreLifecycleClient(
        cast(RpcPeer, RejectUnexpectedPeer()),
        registry.authority,
        registry.prune,
    )
    owner_state = vars(owner)["_state"]
    other_state = vars(other)["_state"]
    other_state.control_token = owner_state.control_token
    with pytest.raises(RpcError) as error:
        await other.stop(IdentityParams.from_mapping(_identity()))
    assert error.value.data["reason_code"] == "GENERATION_UNKNOWN"
    assert registry.authority.snapshot.active is not None
    assert registry.authority.snapshot.active.generation == 7


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["stop", "quiesce"])
async def test_control_client_peer_cannot_be_replaced(
    operation: str,
) -> None:
    """A control capability remains bound to its construction peer."""
    registry = CoreEndpointRegistry(
        channel_key="voice",
        instance_id="instance-1",
    )
    peer_a = _ScriptedPeer(7)
    peer_b = _ScriptedPeer(7)
    lifecycle = CoreLifecycleClient(
        cast(RpcPeer, peer_a),
        registry.authority,
        registry.prune,
    )
    await _activate_client(lifecycle)

    with pytest.raises(FrozenInstanceError):
        setattr(lifecycle, "peer", cast(RpcPeer, peer_b))

    if operation == "stop":
        await lifecycle.stop(IdentityParams.from_mapping(_identity()))
    else:
        await lifecycle.quiesce(
            QuiesceParams.from_mapping(
                {**_identity(), "drain_timeout_ms": 10},
            ),
        )
    method = f"channel.{operation}"
    assert peer_a.calls.count(method) == 1
    assert method not in peer_b.calls


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["stop", "quiesce"])
@pytest.mark.parametrize(
    ("field", "value", "reason_code"),
    [
        ("channel_key", "other", "CHANNEL_KEY_MISMATCH"),
        ("instance_id", "other", "INSTANCE_ID_MISMATCH"),
    ],
)
async def test_invalid_shutdown_identity_does_not_revoke_or_call_runner(
    operation: str,
    field: str,
    value: str,
    reason_code: str,
) -> None:
    """Invalid shutdown identity cannot damage a valid active route."""
    registry = CoreEndpointRegistry(
        channel_key="voice",
        instance_id="instance-1",
    )

    class RejectUnexpectedPeer:
        """Fail if an invalid control request reaches the Runner."""

        async def call(
            self,
            method: str,
            _: object,
            *,
            timeout: float | None = None,
        ) -> object:
            _ = timeout
            if method in {"channel.stop", "channel.quiesce"}:
                raise AssertionError("invalid identity reached Runner")
            return _runner_result(method, 7)

    lifecycle = CoreLifecycleClient(
        cast(RpcPeer, RejectUnexpectedPeer()),
        registry.authority,
        registry.prune,
    )
    await _activate_client(lifecycle)
    registry.register(_endpoint())
    identity = {**_identity(), field: value}
    with pytest.raises(RpcError) as error:
        if operation == "stop":
            await lifecycle.stop(IdentityParams.from_mapping(identity))
        else:
            await lifecycle.quiesce(
                QuiesceParams.from_mapping(
                    {**identity, "drain_timeout_ms": 10},
                ),
            )
    assert error.value.data["reason_code"] == reason_code
    assert registry.resolve(7) == _endpoint()


@pytest.mark.asyncio
async def test_late_commit_result_cannot_cross_shutdown_fencing() -> None:
    """A commit response arriving after stop cannot authorize its slot."""
    registry = CoreEndpointRegistry(
        channel_key="voice",
        instance_id="instance-1",
    )
    commit_entered = asyncio.Event()
    release_commit = asyncio.Event()

    async def delayed_commit() -> object:
        """Return commit only after shutdown fences its candidate."""
        commit_entered.set()
        await release_commit.wait()
        return _runner_result("channel.commit", 7)

    peer = _ScriptedPeer(7, {"channel.commit": delayed_commit})
    lifecycle = CoreLifecycleClient(
        cast(RpcPeer, peer),
        registry.authority,
        registry.prune,
    )
    await lifecycle.prepare(_prepare_params())
    lease = _lease_params()
    await lifecycle.activate(lease)
    commit = asyncio.create_task(lifecycle.commit(lease))
    await asyncio.wait_for(commit_entered.wait(), timeout=0.1)
    await lifecycle.stop(IdentityParams.from_mapping(_identity()))
    release_commit.set()
    with pytest.raises(RpcError) as error:
        await commit
    assert error.value.data["reason_code"] == "GENERATION_REVOKED"
    assert registry.authority.snapshot.active is None


@pytest.mark.asyncio
async def test_late_prepare_result_cannot_replace_newer_candidate() -> None:
    """A replaced prepare operation cannot mutate its successor slot."""
    authority = CoreGenerationAuthority(
        channel_key="voice",
        instance_id="instance-1",
    )
    stale = await authority.prepare_start(_prepare_params())
    current = await authority.prepare_start(_prepare_params(generation=8))
    with pytest.raises(RpcError) as error:
        await authority.prepare_complete(stale)
    assert error.value.data["reason_code"] == "GENERATION_REVOKED"
    await authority.prepare_complete(current)
    assert authority.snapshot.candidate is not None
    assert authority.snapshot.candidate.generation == 8
    assert authority.snapshot.candidate.phase == "standby"


@pytest.mark.asyncio
async def test_late_activate_result_cannot_replace_newer_candidate() -> None:
    """A replaced activation cannot install a lease on a newer slot."""
    authority = CoreGenerationAuthority(
        channel_key="voice",
        instance_id="instance-1",
    )
    prepare = await authority.prepare_start(_prepare_params())
    await authority.prepare_complete(prepare)
    stale_lease = _lease_params()
    stale = await authority.activate_start(stale_lease)
    current = await authority.prepare_start(_prepare_params(generation=8))
    with pytest.raises(RpcError) as error:
        await authority.activate_complete(stale, stale_lease)
    assert error.value.data["reason_code"] == "GENERATION_REVOKED"
    await authority.prepare_complete(current)
    assert authority.snapshot.candidate is not None
    assert authority.snapshot.candidate.generation == 8
    assert authority.snapshot.candidate.lease_token is None


@pytest.mark.asyncio
async def test_late_renew_result_cannot_cross_new_generation_commit() -> None:
    """A retired generation cannot apply a delayed lease renewal."""
    registry = CoreEndpointRegistry(
        channel_key="voice",
        instance_id="instance-1",
    )
    old_lease = await _authorize_registry(registry)
    stale = await registry.authority.renew_start(old_lease)
    prepare = await registry.authority.prepare_start(
        _prepare_params(generation=8),
    )
    await registry.authority.prepare_complete(prepare)
    new_lease = _lease_params(generation=8, token="next")
    activate = await registry.authority.activate_start(new_lease)
    await registry.authority.activate_complete(activate, new_lease)
    commit = await registry.authority.commit_start(new_lease)
    await registry.authority.commit_complete(commit, new_lease)
    with pytest.raises(RpcError) as error:
        await registry.authority.renew_complete(stale, old_lease)
    assert error.value.data["reason_code"] == "GENERATION_REVOKED"
    assert registry.authority.snapshot.active is not None
    assert registry.authority.snapshot.active.generation == 8


@pytest.mark.asyncio
async def test_cancelled_commit_finishes_core_settlement() -> None:
    """Cancellation after Runner success cannot roll Core commit backward."""

    class BlockingAuthority(CoreGenerationAuthority):
        """Expose the Core settlement window after Runner success."""

        def __init__(self) -> None:
            super().__init__(
                channel_key="voice",
                instance_id="instance-1",
            )
            self.entered = asyncio.Event()
            self.release = asyncio.Event()

        async def commit_complete(
            self,
            token: CoreOperationToken,
            params: LeaseParams,
        ) -> None:
            self.entered.set()
            await self.release.wait()
            await super().commit_complete(token, params)

    authority = BlockingAuthority()
    registry = CoreEndpointRegistry(
        channel_key="voice",
        instance_id="instance-1",
        authority=authority,
    )

    class SuccessfulPeer:
        """Return the Runner commit result before Core settlement blocks."""

        async def call(
            self,
            method: str,
            _: object,
            *,
            timeout: float | None = None,
        ) -> object:
            _ = timeout
            return _runner_result(method, 7)

    lifecycle = CoreLifecycleClient(
        cast(RpcPeer, SuccessfulPeer()),
        registry.authority,
        registry.prune,
    )
    await lifecycle.prepare(_prepare_params())
    lease = _lease_params()
    await lifecycle.activate(lease)
    request = asyncio.create_task(lifecycle.commit(lease))
    await asyncio.wait_for(authority.entered.wait(), timeout=0.1)
    request.cancel()
    authority.release.set()
    with pytest.raises(asyncio.CancelledError):
        await request
    assert registry.authority.snapshot.active is not None
    assert registry.authority.snapshot.active.generation == 7


@pytest.mark.asyncio
async def test_failed_candidate_prepare_preserves_active_route() -> None:
    """A failed replacement prepare cannot disturb the live generation."""
    registry = CoreEndpointRegistry(
        channel_key="voice",
        instance_id="instance-1",
    )
    await _authorize_registry(registry)
    registry.register(_endpoint())

    class FailingPeer:
        """Reject the candidate after Core stages its bounded slot."""

        async def call(
            self,
            method: str,
            _: object,
            *,
            timeout: float | None = None,
        ) -> object:
            _ = timeout
            assert method == "channel.prepare"
            assert registry.resolve(7) == _endpoint()
            raise RpcTimeoutError("prepare timed out")

    lifecycle = CoreLifecycleClient(
        cast(RpcPeer, FailingPeer()),
        registry.authority,
        registry.prune,
    )
    with pytest.raises(RpcTimeoutError):
        await lifecycle.prepare(_prepare_params(generation=8))
    assert registry.resolve(7) == _endpoint()
    assert registry.authority.snapshot.candidate is None


@pytest.mark.asyncio
async def test_cancelled_candidate_prepare_preserves_active_route() -> None:
    """Candidate cancellation cleans its slot without touching the active."""
    registry = CoreEndpointRegistry(
        channel_key="voice",
        instance_id="instance-1",
    )
    await _authorize_registry(registry)
    registry.register(_endpoint())
    entered = asyncio.Event()

    class BlockingPeer:
        """Keep candidate prepare pending until its Core task is cancelled."""

        async def call(
            self,
            method: str,
            _: object,
            *,
            timeout: float | None = None,
        ) -> object:
            _ = timeout
            assert method == "channel.prepare"
            entered.set()
            await asyncio.Event().wait()
            raise AssertionError("cancelled prepare unexpectedly resumed")

    lifecycle = CoreLifecycleClient(
        cast(RpcPeer, BlockingPeer()),
        registry.authority,
        registry.prune,
    )
    request = asyncio.create_task(
        lifecycle.prepare(_prepare_params(generation=8)),
    )
    await asyncio.wait_for(entered.wait(), timeout=0.1)
    request.cancel()
    with pytest.raises(asyncio.CancelledError):
        await request
    assert registry.resolve(7) == _endpoint()
    assert registry.authority.snapshot.candidate is None


@pytest.mark.asyncio
async def test_revoked_candidate_endpoint_unregister_is_idempotent() -> None:
    """Late candidate cleanup succeeds without restoring authorization."""
    adapter = CoreLifecycleAdapter(_controller(Clock()))
    token = await adapter.authority.prepare_start(_prepare_params())
    await adapter.endpoint_register(_endpoint())
    await adapter.authority.prepare_abort(token)
    result = await adapter.endpoint_unregister(
        IdentityParams.from_mapping(_identity()),
    )
    assert result["status"] == "unregistered"
    assert adapter.resolve_endpoint(7) is None


@pytest.mark.asyncio
async def test_core_authority_error_matrix_is_stable() -> None:
    """Core-owned admission preserves exact protocol error categories."""
    clock = Clock()
    authority = CoreGenerationAuthority(
        channel_key="voice",
        instance_id="instance-1",
        clock_ms=clock,
    )
    invalid_identity = PrepareParams.from_mapping(
        {
            **_prepare_params().to_mapping(),
            "channel_key": "other",
        },
    )
    with pytest.raises(RpcError) as identity_error:
        await authority.prepare_start(invalid_identity)
    assert identity_error.value.code == -32011
    assert identity_error.value.data["reason_code"] == "CHANNEL_KEY_MISMATCH"

    lifecycle = CoreLifecycleClient(
        cast(RpcPeer, _ScriptedPeer(8)),
        authority,
    )
    with pytest.raises(RpcError) as unknown_error:
        await lifecycle.stop(
            IdentityParams.from_mapping(_identity(generation=8)),
        )
    assert unknown_error.value.code == -32011
    assert unknown_error.value.data["reason_code"] == "GENERATION_UNKNOWN"

    prepare = await authority.prepare_start(_prepare_params())
    await authority.prepare_complete(prepare)
    with pytest.raises(RpcError) as stale_error:
        await authority.prepare_start(_prepare_params(generation=6))
    assert stale_error.value.code == -32011
    assert stale_error.value.data["reason_code"] == "GENERATION_STALE"

    params = HostStateParams.from_mapping(
        {**_identity(), "key": "matrix"},
    )
    with pytest.raises(RpcError) as phase_error:
        async with authority.host_operation(
            params,
            allowed_phases=("active",),
        ):
            pass
    assert phase_error.value.code == -32010
    assert phase_error.value.data["reason_code"] == (
        "INVALID_STATE_TRANSITION"
    )

    with pytest.raises(RpcError) as capability_error:
        async with authority.host_operation(
            params,
            capability="host_state",
            allowed_phases=("standby",),
        ):
            pass
    assert capability_error.value.code == -32013
    assert capability_error.value.data["reason_code"] == "CAPABILITY_REQUIRED"

    lease = _lease_params(ttl_ms=10)
    activate = await authority.activate_start(lease)
    await authority.activate_complete(activate, lease)
    wrong_lease = _lease_params(token="wrong", ttl_ms=10)
    with pytest.raises(RpcError) as token_error:
        await authority.commit_start(wrong_lease)
    assert token_error.value.code == -32012
    assert token_error.value.data["reason_code"] == "LEASE_TOKEN_MISMATCH"

    clock.now = 1011
    with pytest.raises(RpcError) as expiry_error:
        await authority.renew_start(lease)
    assert expiry_error.value.code == -32010
    assert expiry_error.value.data["reason_code"] == "LEASE_EXPIRED"
    with pytest.raises(RpcError) as revoked_error:
        await authority.renew_start(lease)
    assert revoked_error.value.code == -32011
    assert revoked_error.value.data["reason_code"] == "GENERATION_REVOKED"
