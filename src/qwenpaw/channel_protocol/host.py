# -*- coding: utf-8 -*-
"""Core-owned Channel host RPC services and bounded state."""

from __future__ import annotations

import asyncio
import json
from typing import Any, Callable

from .core_lifecycle import (
    CoreGenerationAuthority,
    CoreLifecycleClient,
)
from .errors import (
    ProtocolValidationError,
    RPC_AUTH_ERROR,
    RPC_CAPABILITY_ERROR,
    RPC_FENCING_ERROR,
    RPC_LIFECYCLE_ERROR,
    RpcError,
)
from .lifecycle import LifecycleController
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
    """Own bounded candidate and active endpoints for Core routing."""

    def __init__(
        self,
        *,
        channel_key: str,
        instance_id: str,
        clock_ms: Callable[[], int] | None = None,
        authority: CoreGenerationAuthority | None = None,
    ) -> None:
        self.channel_key = channel_key
        self.instance_id = instance_id
        if authority is not None and (
            authority.channel_key != channel_key
            or authority.instance_id != instance_id
        ):
            raise ValueError(
                "endpoint registry and authority identity must match",
            )
        self.authority = authority or CoreGenerationAuthority(
            channel_key=channel_key,
            instance_id=instance_id,
            clock_ms=clock_ms,
        )
        self._entries: dict[int, tuple[int, EndpointParams]] = {}

    def register(self, endpoint: EndpointParams) -> None:
        """Store one generation endpoint without making it routable early."""
        self.authority.check_identity(endpoint)
        slot = self.authority.endpoint_snapshot(endpoint.generation)
        if slot is None:
            raise self.authority.generation_error(endpoint.generation)
        if "ingress_endpoint" not in slot.capabilities:
            raise RpcError(
                RPC_CAPABILITY_ERROR,
                "ingress endpoint capability was not selected",
                data={
                    "reason_code": "CAPABILITY_REQUIRED",
                    "capability": "ingress_endpoint",
                },
            )
        if is_external_host(endpoint.host) and slot.phase != "active":
            raise self._fencing_error("STANDBY_ENDPOINT_FORBIDDEN")
        self._entries[endpoint.generation] = (slot.epoch, endpoint)

    def unregister(self, params: IdentityParams) -> None:
        """Idempotently remove one generation endpoint."""
        self.authority.check_identity(params)
        self._entries.pop(params.generation, None)

    def resolve(self, generation: int) -> EndpointParams | None:
        """Return only an endpoint whose generation is currently routable."""
        entry = self._entries.get(generation)
        if entry is None:
            return None
        epoch, endpoint = entry
        slot = self.authority.route_snapshot(generation)
        if slot is None or slot.epoch != epoch:
            return None
        if endpoint.readiness != "ready" or endpoint.quiescing:
            return None
        return endpoint

    def snapshot(self) -> dict[int, EndpointParams]:
        """Return a copy containing only currently routable endpoints."""
        snapshot: dict[int, EndpointParams] = {}
        for generation in tuple(self._entries):
            endpoint = self.resolve(generation)
            if endpoint is not None:
                snapshot[generation] = endpoint
        return snapshot

    def prune(self) -> None:
        """Keep endpoint storage bounded to active and candidate slots."""
        snapshot = self.authority.snapshot
        retained = {
            slot.generation
            for slot in (snapshot.active, snapshot.candidate)
            if slot is not None
        }
        for generation in tuple(self._entries):
            if generation not in retained:
                self._entries.pop(generation, None)

    @staticmethod
    def _fencing_error(reason_code: str) -> RpcError:
        """Return one stable Core route fencing error."""
        return RpcError(
            RPC_FENCING_ERROR,
            reason_code,
            data={"reason_code": reason_code},
        )


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
        authority: CoreGenerationAuthority | None = None,
        clock_ms: Callable[[], int] | None = None,
    ) -> None:
        self.controller = controller
        self.host_state_store = host_state_store or HostStateStore()
        self.delivery_ledger = delivery_ledger or OutboundDeliveryLedger()
        self.inbound_inbox = inbound_inbox or InboundInbox()
        if authority is not None and endpoint_registry is not None:
            if endpoint_registry.authority is not authority:
                raise ValueError(
                    "endpoint registry and authority must share ownership",
                )
        self.authority = authority or (
            endpoint_registry.authority
            if endpoint_registry is not None
            else CoreGenerationAuthority(
                channel_key=controller.channel_key,
                instance_id=controller.instance_id,
                clock_ms=clock_ms,
            )
        )
        if (
            controller.channel_key != self.authority.channel_key
            or controller.instance_id != self.authority.instance_id
        ):
            raise ValueError(
                "hello controller and Core authority identity must match",
            )
        self._endpoint_registry = endpoint_registry or CoreEndpointRegistry(
            channel_key=controller.channel_key,
            instance_id=controller.instance_id,
            clock_ms=clock_ms,
            authority=self.authority,
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
        return CoreLifecycleClient(
            peer,
            self.authority,
            self._endpoint_registry.prune,
        )

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
        async with self.authority.host_operation(
            params.identity,
            allowed_phases=("active",),
        ) as slot:
            if "response_lifecycle" not in slot.capabilities:
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
        async with self.authority.host_operation(
            params,
            allowed_phases=("active",),
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
        async with self.authority.endpoint_operation(params) as slot:
            if slot is None:
                raise RpcError(
                    RPC_FENCING_ERROR,
                    "GENERATION_REVOKED",
                    data={"reason_code": "GENERATION_REVOKED"},
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
        async with self.authority.endpoint_operation(
            params,
            allow_revoked=True,
        ):
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
        async with self.authority.host_operation(
            params,
            capability="host_state",
            allowed_phases=("preparing", "standby", "active"),
        ):
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
        async with self.authority.host_operation(
            params,
            capability="host_state",
            allowed_phases=("active",),
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
        async with self.authority.host_operation(
            params,
            capability="host_state",
            allowed_phases=("active",),
        ):
            await self.host_state_store.delete(params.key)
            return {"status": "deleted", "key": params.key}


__all__ = [
    "CoreEndpointRegistry",
    "CoreLifecycleAdapter",
    "CoreLifecycleClient",
    "HostStateStore",
]
