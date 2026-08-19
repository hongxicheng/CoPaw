# -*- coding: utf-8 -*-
"""Core-owned Channel host RPC services and bounded state."""

from __future__ import annotations

import asyncio
import json
from typing import Any, Callable

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


class CoreEndpointRegistry:
    """Keep Core-owned endpoints behind generation route authorization."""

    def __init__(
        self,
        route_state: Callable[[int], tuple[bool, bool]],
    ) -> None:
        self._route_state = route_state
        self._entries: dict[int, EndpointParams] = {}

    def register(self, endpoint: EndpointParams) -> None:
        """Store one generation endpoint without making it routable early."""
        self._entries[endpoint.generation] = endpoint

    def unregister(self, generation: int) -> None:
        """Idempotently remove one generation endpoint."""
        self._entries.pop(generation, None)

    def resolve(self, generation: int) -> EndpointParams | None:
        """Return only an endpoint whose generation is currently routable."""
        endpoint = self._entries.get(generation)
        if endpoint is None:
            return None
        routable, revoked = self._route_state(generation)
        if revoked:
            self._entries.pop(generation, None)
            return None
        return endpoint if routable else None

    def snapshot(self) -> dict[int, EndpointParams]:
        """Return a copy containing only currently routable endpoints."""
        snapshot: dict[int, EndpointParams] = {}
        for generation in tuple(self._entries):
            endpoint = self.resolve(generation)
            if endpoint is not None:
                snapshot[generation] = endpoint
        return snapshot


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
    ) -> None:
        self.controller = controller
        self.host_state_store = host_state_store or HostStateStore()
        self.delivery_ledger = delivery_ledger or OutboundDeliveryLedger()
        self.inbound_inbox = inbound_inbox or InboundInbox()
        self._endpoint_registry = CoreEndpointRegistry(
            self._generation_is_routable,
        )

    @property
    def endpoints(self) -> dict[int, EndpointParams]:
        """Return an immutable-by-copy view of authorized Core routes."""
        return self._endpoint_registry.snapshot()

    def resolve_endpoint(self, generation: int) -> EndpointParams | None:
        """Resolve one route after lifecycle, lease, and generation fencing."""
        return self._endpoint_registry.resolve(generation)

    def _generation_is_routable(self, generation: int) -> tuple[bool, bool]:
        """Return current route authorization and irreversible revocation."""
        controller = self.controller
        revoked = generation != controller.generation or controller.state in {
            RunnerState.QUIESCING,
            RunnerState.STOPPED,
            RunnerState.FAILED,
        }
        if revoked:
            return False, True
        active = (
            controller.state is RunnerState.ACTIVE
            and controller.lease_token is not None
        )
        expires_at = controller.lease_expires_at_ms
        if not active or expires_at is None:
            return False, False
        expired = controller._clock_ms() >= expires_at
        return not expired, expired

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
        async with self.controller._host_operation(
            params,
            capability="ingress_endpoint",
            allowed_states=(RunnerState.STANDBY, RunnerState.ACTIVE),
            expire_lease=True,
            expired_reason="LEASE_EXPIRED",
        ) as state:
            if state != RunnerState.ACTIVE and is_external_host(params.host):
                raise RpcError(
                    RPC_FENCING_ERROR,
                    "standby endpoint cannot be externally exposed",
                    data={"reason_code": "STANDBY_ENDPOINT_FORBIDDEN"},
                )
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
        async with self.controller._host_operation(
            params,
            capability="ingress_endpoint",
        ):
            self._endpoint_registry.unregister(params.generation)
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


__all__ = ["CoreLifecycleAdapter", "HostStateStore"]
