# -*- coding: utf-8 -*-
"""Core-owned Channel host RPC services and bounded state."""

from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass
from typing import Any, Callable, Mapping

from .errors import ProtocolValidationError, RpcError
from .lifecycle import (
    RPC_AUTH_ERROR,
    RPC_FENCING_ERROR,
    RPC_LIFECYCLE_ERROR,
    LifecycleController,
    RunnerState,
)
from .models import (
    DeliveryUpdateParams,
    EndpointParams,
    EventBatchParams,
    HostStateParams,
    IdentityParams,
    LeaseParams,
    PrepareParams,
    QuiesceParams,
    RejectedEvent,
    is_external_host,
)
from .reliability import InboundInbox, OutboundDeliveryLedger
from .rpc import RpcPeer


def _strict_json_dumps(value: object) -> str:
    """Encode the protocol JSON subset without non-finite numbers."""
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
        )
    except (TypeError, ValueError) as exc:
        raise ProtocolValidationError(
            "value must be strict JSON serializable",
            reason_code="SCHEMA_MISMATCH",
        ) from exc


class HostStateStore:
    """Bounded Core-owned store for instance-scoped host state."""

    def __init__(
        self,
        *,
        max_value_bytes: int = 64 * 1024,
        max_total_bytes: int = 1024 * 1024,
        max_keys: int = 1024,
    ) -> None:
        if max_value_bytes <= 0 or max_total_bytes <= 0 or max_keys <= 0:
            raise ValueError("host state limits must be positive")
        if max_value_bytes > max_total_bytes:
            raise ValueError("max_value_bytes cannot exceed max_total_bytes")
        self.max_value_bytes = max_value_bytes
        self.max_total_bytes = max_total_bytes
        self.max_keys = max_keys
        self._values: dict[str, tuple[int, object, int]] = {}
        self._lock = asyncio.Lock()

    async def get(self, key: str) -> tuple[int, object] | None:
        """Read one value from the bounded store."""
        async with self._lock:
            entry = self._values.get(key)
            if entry is None:
                return None
            return entry[0], entry[1]

    async def put(self, key: str, schema_version: int, value: object) -> None:
        """Atomically validate and replace one value."""
        encoded = _strict_json_dumps(value)
        size = len(encoded.encode("utf-8"))
        if size > self.max_value_bytes:
            raise ProtocolValidationError(
                "host state value exceeds size limit",
                reason_code="STATE_LIMIT_EXCEEDED",
            )
        async with self._lock:
            old = self._values.get(key)
            total = sum(entry[2] for entry in self._values.values())
            total -= old[2] if old is not None else 0
            if old is None and len(self._values) >= self.max_keys:
                raise ProtocolValidationError(
                    "host state key limit exceeded",
                    reason_code="STATE_LIMIT_EXCEEDED",
                )
            if total + size > self.max_total_bytes:
                raise ProtocolValidationError(
                    "host state total size limit exceeded",
                    reason_code="STATE_LIMIT_EXCEEDED",
                )
            self._values[key] = (schema_version, value, size)

    async def delete(self, key: str) -> None:
        """Atomically delete one value."""
        async with self._lock:
            self._values.pop(key, None)


@dataclass
class _GenerationRouteState:
    """Core-owned route facts for one Runner generation."""

    capabilities: frozenset[str] = frozenset()
    lease_token: str | None = None
    lease_expires_at_ms: int | None = None
    authorized: bool = False
    revoked: bool = False


class CoreEndpointRegistry:
    """Own candidate endpoints and monotonic Core route authorization."""

    def __init__(
        self,
        *,
        channel_key: str,
        instance_id: str,
        clock_ms: Callable[[], int] | None = None,
    ) -> None:
        self.channel_key = channel_key
        self.instance_id = instance_id
        self._clock_ms = clock_ms or (lambda: time.monotonic_ns() // 1_000_000)
        self._entries: dict[int, EndpointParams] = {}
        self._generations: dict[int, _GenerationRouteState] = {}
        self._authorized_generation: int | None = None
        self._highest_generation = -1

    def prepare(self, params: PrepareParams) -> None:
        """Stage a candidate so prepare-time endpoint RPC can be accepted."""
        self._check_identity(params)
        state = self._generations.get(params.generation)
        if state is not None and state.revoked:
            raise self._fencing_error("GENERATION_REVOKED")
        if state is not None and (
            state.authorized or state.lease_token is not None
        ):
            raise self._fencing_error("GENERATION_ALREADY_PREPARED")
        if params.generation < self._highest_generation:
            raise self._fencing_error("GENERATION_STALE")
        self._highest_generation = max(
            self._highest_generation,
            params.generation,
        )
        self._entries.pop(params.generation, None)
        self._generations[params.generation] = _GenerationRouteState(
            capabilities=frozenset(params.capabilities),
        )

    def abort_prepare(self, generation: int) -> None:
        """Discard an uncommitted candidate after prepare RPC failure."""
        state = self._generations.get(generation)
        if state is None or state.authorized or state.revoked:
            return
        self._entries.pop(generation, None)
        self._generations.pop(generation, None)

    def activate(self, params: LeaseParams) -> None:
        """Record a provisional Core lease without authorizing routing."""
        state = self._candidate_state(params)
        state.lease_token = params.lease_token
        state.lease_expires_at_ms = self._clock_ms() + params.lease_ttl_ms

    def commit(self, params: LeaseParams) -> None:
        """Authorize one successfully committed, unexpired generation."""
        state = self._lease_state(params)
        self._expire_if_needed(params.generation, state)
        if state.revoked:
            raise self._fencing_error("GENERATION_REVOKED")
        previous = self._authorized_generation
        if previous is not None and previous != params.generation:
            self.revoke(previous)
        state.authorized = True
        self._authorized_generation = params.generation

    def renew(self, params: LeaseParams) -> None:
        """Extend a live Core lease without reviving a fenced generation."""
        state = self._lease_state(params)
        self._expire_if_needed(params.generation, state)
        if state.revoked:
            raise self._fencing_error("GENERATION_REVOKED")
        state.lease_expires_at_ms = self._clock_ms() + params.lease_ttl_ms

    def assert_renewable(self, params: LeaseParams) -> None:
        """Reject a lease renewal before contacting a stale Runner."""
        state = self._lease_state(params)
        self._expire_if_needed(params.generation, state)
        if state.revoked:
            raise self._fencing_error("GENERATION_REVOKED")

    def revoke(self, generation: int) -> None:
        """Irreversibly revoke formal routing for one generation."""
        state = self._generations.setdefault(
            generation,
            _GenerationRouteState(),
        )
        state.authorized = False
        state.revoked = True
        state.lease_token = None
        state.lease_expires_at_ms = None
        if self._authorized_generation == generation:
            self._authorized_generation = None

    def register(self, endpoint: EndpointParams) -> None:
        """Store one generation endpoint without making it routable early."""
        self._check_identity(endpoint)
        state = self._generations.get(endpoint.generation)
        if state is None:
            raise self._fencing_error("GENERATION_UNKNOWN")
        self._expire_if_needed(endpoint.generation, state)
        if state.revoked:
            raise self._fencing_error("GENERATION_REVOKED")
        if "ingress_endpoint" not in state.capabilities:
            raise RpcError(
                RPC_LIFECYCLE_ERROR,
                "ingress endpoint capability was not selected",
                data={
                    "reason_code": "CAPABILITY_REQUIRED",
                    "capability": "ingress_endpoint",
                },
            )
        if is_external_host(endpoint.host) and not self.is_authorized(
            endpoint.generation,
        ):
            raise self._fencing_error("STANDBY_ENDPOINT_FORBIDDEN")
        self._entries[endpoint.generation] = endpoint

    def unregister(self, params: IdentityParams) -> None:
        """Idempotently remove one generation endpoint."""
        self._check_identity(params)
        self._entries.pop(params.generation, None)

    def resolve(self, generation: int) -> EndpointParams | None:
        """Return only an endpoint whose generation is currently routable."""
        endpoint = self._entries.get(generation)
        if endpoint is None:
            return None
        if endpoint.readiness != "ready" or endpoint.quiescing:
            return None
        return endpoint if self.is_authorized(generation) else None

    def is_authorized(self, generation: int) -> bool:
        """Return whether Core currently authorizes formal routing."""
        state = self._generations.get(generation)
        if state is None:
            return False
        self._expire_if_needed(generation, state)
        return (
            not state.revoked
            and state.authorized
            and self._authorized_generation == generation
        )

    def snapshot(self) -> dict[int, EndpointParams]:
        """Return a copy containing only currently routable endpoints."""
        snapshot: dict[int, EndpointParams] = {}
        for generation in tuple(self._entries):
            endpoint = self.resolve(generation)
            if endpoint is not None:
                snapshot[generation] = endpoint
        return snapshot

    def _candidate_state(
        self,
        params: IdentityParams,
    ) -> _GenerationRouteState:
        """Return a prepared, non-revoked generation state."""
        self._check_identity(params)
        state = self._generations.get(params.generation)
        if state is None:
            raise self._fencing_error("GENERATION_UNKNOWN")
        if state.revoked:
            raise self._fencing_error("GENERATION_REVOKED")
        return state

    def _lease_state(self, params: LeaseParams) -> _GenerationRouteState:
        """Return a generation whose Core lease token still matches."""
        state = self._candidate_state(params)
        if state.lease_token != params.lease_token:
            raise self._fencing_error("LEASE_TOKEN_MISMATCH")
        return state

    def _expire_if_needed(
        self,
        generation: int,
        state: _GenerationRouteState,
    ) -> None:
        """Apply Core-clock lease expiry as irreversible fencing."""
        expires_at = state.lease_expires_at_ms
        if expires_at is None or self._clock_ms() < expires_at:
            return
        self.revoke(generation)

    def _check_identity(self, params: IdentityParams) -> None:
        """Validate stable instance identity independently of Runner state."""
        if params.channel_key != self.channel_key:
            raise self._fencing_error("CHANNEL_KEY_MISMATCH")
        if params.instance_id != self.instance_id:
            raise self._fencing_error("INSTANCE_ID_MISMATCH")

    @staticmethod
    def _fencing_error(reason_code: str) -> RpcError:
        """Return one stable Core route fencing error."""
        return RpcError(
            RPC_FENCING_ERROR,
            reason_code,
            data={"reason_code": reason_code},
        )


@dataclass
class CoreLifecycleClient:
    """Linearize Core route state with Core-to-Runner control calls."""

    peer: RpcPeer
    endpoints: CoreEndpointRegistry

    async def prepare(
        self,
        params: PrepareParams,
        *,
        timeout: float | None = None,
    ) -> Any:
        """Stage Core admission around one Runner prepare request."""
        self.endpoints.prepare(params)
        try:
            result = await self.peer.call(
                "channel.prepare",
                params.to_mapping(),
                timeout=timeout,
            )
        except asyncio.CancelledError:
            self.endpoints.abort_prepare(params.generation)
            raise
        except Exception:
            self.endpoints.abort_prepare(params.generation)
            raise
        return result

    async def activate(
        self,
        params: LeaseParams,
        *,
        timeout: float | None = None,
    ) -> Any:
        """Record the provisional Core lease after Runner activation."""
        result = await self.peer.call(
            "channel.activate",
            params.to_mapping(),
            timeout=timeout,
        )
        self.endpoints.activate(params)
        return result

    async def commit(
        self,
        params: LeaseParams,
        *,
        timeout: float | None = None,
    ) -> Any:
        """Authorize routing only after Runner commit succeeds."""
        result = await self.peer.call(
            "channel.commit",
            params.to_mapping(),
            timeout=timeout,
        )
        self._require_active_result(result, params.generation)
        self.endpoints.commit(params)
        return result

    async def lease_renew(
        self,
        params: LeaseParams,
        *,
        timeout: float | None = None,
    ) -> Any:
        """Renew Runner and Core leases without reviving an expired route."""
        self.endpoints.assert_renewable(params)
        result = await self.peer.call(
            "channel.lease_renew",
            params.to_mapping(),
            timeout=timeout,
        )
        self._require_generation_result(result, params.generation)
        self.endpoints.renew(params)
        return result

    async def quiesce(
        self,
        params: QuiesceParams,
        *,
        timeout: float | None = None,
    ) -> Any:
        """Revoke formal routing before asking Runner to quiesce."""
        self.endpoints.revoke(params.generation)
        return await self.peer.call(
            "channel.quiesce",
            params.to_mapping(),
            timeout=timeout,
        )

    async def stop(
        self,
        params: IdentityParams,
        *,
        timeout: float | None = None,
    ) -> Any:
        """Revoke formal routing before asking Runner to stop."""
        self.endpoints.revoke(params.generation)
        return await self.peer.call(
            "channel.stop",
            params.to_mapping(),
            timeout=timeout,
        )

    @staticmethod
    def _require_active_result(result: Any, generation: int) -> None:
        """Require a successful active commit response."""
        CoreLifecycleClient._require_generation_result(result, generation)
        if result.get("state") != "active" or not result.get("consuming"):
            raise RpcError(
                RPC_LIFECYCLE_ERROR,
                "Runner commit did not establish an active generation",
                data={"reason_code": "INVALID_COMMIT_RESULT"},
            )

    @staticmethod
    def _require_generation_result(result: Any, generation: int) -> None:
        """Require a mapping response for the controlled generation."""
        if (
            not isinstance(result, Mapping)
            or result.get(
                "generation",
            )
            != generation
        ):
            raise RpcError(
                RPC_LIFECYCLE_ERROR,
                "Runner returned an invalid generation result",
                data={"reason_code": "INVALID_GENERATION_RESULT"},
            )


# Package-internal lifecycle guards intentionally cross this module boundary.
# pylint: disable=protected-access
class CoreLifecycleAdapter:
    """Receive Runner-owned requests and retain Core-owned state."""

    def __init__(
        self,
        controller: LifecycleController,
        *,
        host_state_store: HostStateStore | None = None,
        delivery_ledger: OutboundDeliveryLedger | None = None,
        inbound_inbox: InboundInbox | None = None,
        endpoint_registry: CoreEndpointRegistry | None = None,
        clock_ms: Callable[[], int] | None = None,
    ) -> None:
        self.controller = controller
        self.host_state_store = host_state_store or HostStateStore()
        self.delivery_ledger = delivery_ledger or OutboundDeliveryLedger()
        self.inbound_inbox = inbound_inbox or InboundInbox()
        self._endpoint_registry = endpoint_registry or CoreEndpointRegistry(
            channel_key=controller.channel_key,
            instance_id=controller.instance_id,
            clock_ms=clock_ms,
        )

    @property
    def endpoints(self) -> dict[int, EndpointParams]:
        """Return an immutable-by-copy view of authorized Core routes."""
        return self._endpoint_registry.snapshot()

    def resolve_endpoint(self, generation: int) -> EndpointParams | None:
        """Resolve one route after lifecycle, lease, and generation fencing."""
        return self._endpoint_registry.resolve(generation)

    def lifecycle_client(self, peer: RpcPeer) -> CoreLifecycleClient:
        """Return the Core control path bound to this route registry."""
        return CoreLifecycleClient(peer, self._endpoint_registry)

    def register_rpc_methods(self, peer: RpcPeer) -> None:
        """Register Core-owned Runner-to-Core methods."""
        peer.register_method(
            "runner.hello",
            lambda params, _: self.controller.accept_hello(params),
        )
        peer.register_method(
            "ingress.endpoint.register",
            lambda params, _: self.endpoint_register(params),
        )
        peer.register_method(
            "ingress.endpoint.update",
            lambda params, _: self.endpoint_update(params),
        )
        peer.register_method(
            "ingress.endpoint.unregister",
            lambda params, _: self.endpoint_unregister(params),
        )
        peer.register_method(
            "host.state.get",
            lambda params, _: self.host_state_get(params),
        )
        peer.register_method(
            "host.state.put",
            lambda params, _: self.host_state_put(params),
        )
        peer.register_method(
            "host.state.delete",
            lambda params, _: self.host_state_delete(params),
        )
        peer.register_method(
            "delivery.update",
            lambda params, _: self.delivery_update(params),
        )
        peer.register_method(
            "event.batch",
            lambda params, _: self.event_batch(params),
        )

    async def event_batch(
        self,
        params: EventBatchParams,
    ) -> dict[str, Any]:
        """Persist and deduplicate a reliable inbound event batch."""
        if params.identity is None:
            raise RpcError(
                RPC_LIFECYCLE_ERROR,
                "INVALID_EVENT_BATCH",
                data={"reason_code": "INVALID_EVENT_BATCH"},
            )
        async with self.controller._host_operation(
            params.identity,
            allowed_states=(RunnerState.ACTIVE,),
            expire_lease=True,
        ):
            if "response_lifecycle" not in (
                self.controller.effective_capabilities
            ):
                rejected = tuple(
                    RejectedEvent(
                        event_id=event.event_id,
                        reason_code="CAPABILITY_REQUIRED",
                        retryable=False,
                    )
                    for event in params.events
                    if event.response_handle is not None
                )
                if rejected:
                    rejected_ids = {item.event_id for item in rejected}
                    params = EventBatchParams(
                        batch_id=params.batch_id,
                        events=tuple(
                            event
                            for event in params.events
                            if event.event_id not in rejected_ids
                        ),
                        invalid_events=params.invalid_events + rejected,
                        identity=params.identity,
                    )
            ack = self.inbound_inbox.accept_batch(params)
            return ack.to_mapping()

    async def delivery_update(
        self,
        params: DeliveryUpdateParams,
    ) -> dict[str, Any]:
        """Record a Runner delivery result after identity validation."""
        async with self.controller._host_operation(
            params,
            allowed_states=(RunnerState.ACTIVE,),
            expire_lease=True,
        ):
            state = self.delivery_ledger.apply(params)
            return {
                "status": "recorded",
                "delivery_id": params.delivery_id,
                "state": state.value,
            }

    async def endpoint_register(
        self,
        params: EndpointParams,
    ) -> dict[str, Any]:
        """Register a Runner-owned endpoint in Core."""
        if is_external_host(params.host) and not params.auth_required:
            raise RpcError(
                RPC_AUTH_ERROR,
                "external endpoint requires authentication",
                data={"reason_code": "AUTH_FAILED"},
            )
        self._endpoint_registry.register(params)
        return {
            "status": "registered",
            "generation": params.generation,
            "readiness": params.readiness,
        }

    async def endpoint_update(self, params: EndpointParams) -> dict[str, Any]:
        """Update a Runner-owned endpoint in Core."""
        result = await self.endpoint_register(params)
        result["status"] = "updated"
        return result

    async def endpoint_unregister(
        self,
        params: IdentityParams,
    ) -> dict[str, Any]:
        """Idempotently unregister a Runner-owned endpoint."""
        self._endpoint_registry.unregister(params)
        return {
            "status": "unregistered",
            "generation": params.generation,
        }

    async def host_state_get(
        self,
        params: HostStateParams,
    ) -> dict[str, Any]:
        """Read Core-owned host state."""
        async with self.controller._host_operation(
            params,
            capability="host_state",
        ):
            pass
        entry = await self.host_state_store.get(params.key)
        if entry is None:
            return {"found": False, "key": params.key}
        schema_version, value = entry
        return {
            "found": True,
            "key": params.key,
            "schema_version": schema_version,
            "value": value,
        }

    async def host_state_put(
        self,
        params: HostStateParams,
    ) -> dict[str, Any]:
        """Write Core-owned host state with generation fencing."""
        async with self.controller._host_operation(
            params,
            capability="host_state",
            allowed_states=(RunnerState.ACTIVE,),
            expire_lease=True,
            expired_reason="LEASE_EXPIRED",
        ):
            await self.host_state_store.put(
                params.key,
                params.schema_version,
                params.value,
            )
            return {"status": "stored", "key": params.key}

    async def host_state_delete(
        self,
        params: HostStateParams,
    ) -> dict[str, Any]:
        """Delete Core-owned host state with generation fencing."""
        async with self.controller._host_operation(
            params,
            capability="host_state",
            allowed_states=(RunnerState.ACTIVE,),
            expire_lease=True,
            expired_reason="LEASE_EXPIRED",
        ):
            await self.host_state_store.delete(params.key)
            return {"status": "deleted", "key": params.key}


__all__ = [
    "CoreEndpointRegistry",
    "CoreLifecycleAdapter",
    "CoreLifecycleClient",
    "HostStateStore",
]
