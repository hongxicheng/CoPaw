# -*- coding: utf-8 -*-
"""Runner lifecycle, lease, generation, endpoint, and host-state rules."""

from __future__ import annotations

import asyncio
import json
import time
from enum import StrEnum
from typing import Any, Callable, Mapping

from .errors import (
    ProtocolValidationError,
    RpcError,
    SecretHandleConsumedError,
    SecretHandleInvalidError,
)
from .models import (
    DeliveryUpdateParams,
    EventBatchParams,
    EndpointParams,
    GenerationStatus,
    HelloParams,
    HostContext,
    HostStateParams,
    IdentityParams,
    LeaseParams,
    OutboundOperation,
    PrepareParams,
    QuiesceParams,
    ReactionParams,
    SendParams,
    is_external_host,
)
from .reliability import InboundInbox, OutboundDeliveryLedger
from .rpc import RpcPeer


RPC_LIFECYCLE_ERROR = -32010
RPC_FENCING_ERROR = -32011
RPC_AUTH_ERROR = -32012
RPC_CAPABILITY_ERROR = -32013


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


class FixtureSecretHandleConsumer:
    """Consume in-memory fixture secrets without exposing them on the wire."""

    def __init__(
        self,
        handles: Mapping[tuple[str, int], object],
        sink: Callable[[object], Any],
    ) -> None:
        self._pending = dict(handles)
        self._consumed: set[tuple[str, int]] = set()
        self._sink = sink

    async def __call__(self, handle: str, generation: int) -> None:
        """Consume one fixture handle before invoking its in-process sink."""
        key = (handle, generation)
        if key in self._consumed:
            raise SecretHandleConsumedError(
                "fixture secret handle was already consumed",
            )
        if key not in self._pending:
            raise SecretHandleInvalidError("fixture secret handle is invalid")
        secret_value = self._pending.pop(key)
        self._consumed.add(key)
        result = self._sink(secret_value)
        if hasattr(result, "__await__"):
            await result

    def __repr__(self) -> str:
        """Return diagnostics that reveal neither handles nor secret values."""
        return (
            f"{type(self).__name__}(pending={len(self._pending)}, "
            f"consumed={len(self._consumed)})"
        )


# pylint: disable=protected-access
class CoreLifecycleAdapter:
    """Receive Runner-owned requests and retain Core-owned state."""

    def __init__(
        self,
        controller: "LifecycleController",
        *,
        host_state_store: HostStateStore | None = None,
        delivery_ledger: OutboundDeliveryLedger | None = None,
        inbound_inbox: InboundInbox | None = None,
    ) -> None:
        self.controller = controller
        self.host_state_store = host_state_store or HostStateStore()
        self.delivery_ledger = delivery_ledger or OutboundDeliveryLedger()
        self.inbound_inbox = inbound_inbox or InboundInbox()
        self.controller.host_state_store = self.host_state_store
        self.endpoints: dict[int, EndpointParams] = {}
        previous_handler = self.controller._endpoint_handler

        async def endpoint_handler(
            operation: str,
            params: EndpointParams | None,
        ) -> None:
            """Forward lifecycle callbacks and update the Core registry."""
            if previous_handler is not None:
                result = previous_handler(operation, params)
                if hasattr(result, "__await__"):
                    await result
            await self._endpoint_handler(operation, params)

        self.controller._endpoint_handler = endpoint_handler

    async def _endpoint_handler(
        self,
        operation: str,
        params: EndpointParams | None,
    ) -> None:
        """Keep the Core endpoint registry in sync with lifecycle changes."""
        if operation == "unregister":
            self.endpoints.pop(self.controller.generation, None)
        elif params is not None:
            self.endpoints[params.generation] = params

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
            raise self.controller._lifecycle_error("INVALID_EVENT_BATCH")
        self.controller._check_identity(params.identity)
        await self.controller._expire_lease_if_needed_async()
        self.controller._ensure_state(RunnerState.ACTIVE)
        ack = self.inbound_inbox.accept_batch(params)
        return ack.to_mapping()

    async def delivery_update(
        self,
        params: DeliveryUpdateParams,
    ) -> dict[str, Any]:
        """Record a Runner delivery result after identity validation."""
        self.controller._check_identity(params)
        await self.controller._expire_lease_if_needed_async()
        self.controller._ensure_state(RunnerState.ACTIVE)
        state = self.delivery_ledger.apply(params)
        return {
            "status": "recorded",
            "delivery_id": params.delivery_id,
            "state": state.value,
        }

    def _check_capability(self, capability: str) -> None:
        """Reject a Core-owned operation without negotiated capability."""
        if capability not in self.controller.effective_capabilities:
            raise RpcError(
                RPC_CAPABILITY_ERROR,
                "capability was not negotiated",
                data={
                    "reason_code": "CAPABILITY_REQUIRED",
                    "capability": capability,
                },
            )

    async def endpoint_register(
        self,
        params: EndpointParams,
    ) -> dict[str, Any]:
        """Register a Runner-owned endpoint in Core."""
        self._check_capability("ingress_endpoint")
        self.controller._check_identity(params)
        await self.controller._expire_lease_if_needed_async()
        if self.controller.state == RunnerState.FAILED:
            raise self.controller._lifecycle_error("LEASE_EXPIRED")
        if self.controller.state not in {
            RunnerState.STANDBY,
            RunnerState.ACTIVE,
        }:
            raise self.controller._lifecycle_error("INVALID_STATE_TRANSITION")
        if self.controller.state != RunnerState.ACTIVE and is_external_host(
            params.host,
        ):
            raise RpcError(
                RPC_FENCING_ERROR,
                "standby endpoint cannot be externally exposed",
                data={"reason_code": "STANDBY_ENDPOINT_FORBIDDEN"},
            )
        if is_external_host(params.host) and not params.auth_required:
            raise RpcError(
                RPC_AUTH_ERROR,
                "externally bound endpoint must require authentication",
                data={"reason_code": "AUTH_FAILED"},
            )
        self.endpoints[params.generation] = params
        self.controller.endpoint = params
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
        self._check_capability("ingress_endpoint")
        self.controller._check_identity(params)
        self.endpoints.pop(params.generation, None)
        self.controller.endpoint = None
        return {"status": "unregistered", "generation": params.generation}

    async def host_state_get(self, params: HostStateParams) -> dict[str, Any]:
        """Read Core-owned host state."""
        self._check_capability("host_state")
        self.controller._check_identity(params)
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

    async def host_state_put(self, params: HostStateParams) -> dict[str, Any]:
        """Write Core-owned host state with generation fencing."""
        async with self.controller._lock:
            self._check_capability("host_state")
            self.controller._check_identity(params)
            expired = (
                self.controller.lease_expires_at_ms is not None
                and self.controller._clock_ms()
                >= self.controller.lease_expires_at_ms
            )
            await self.controller._expire_lease_if_needed_async()
            if expired:
                raise self.controller._lifecycle_error("LEASE_EXPIRED")
            self.controller._ensure_state(RunnerState.ACTIVE)
            self.controller._ensure_json_value(params.value)
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
        async with self.controller._lock:
            self._check_capability("host_state")
            self.controller._check_identity(params)
            expired = (
                self.controller.lease_expires_at_ms is not None
                and self.controller._clock_ms()
                >= self.controller.lease_expires_at_ms
            )
            await self.controller._expire_lease_if_needed_async()
            if expired:
                raise self.controller._lifecycle_error("LEASE_EXPIRED")
            self.controller._ensure_state(RunnerState.ACTIVE)
            await self.host_state_store.delete(params.key)
            return {"status": "deleted", "key": params.key}


class RunnerState(StrEnum):
    """Stable Runner lifecycle states."""

    CREATED = "created"
    PREPARING = "preparing"
    STANDBY = "standby"
    ACTIVE = "active"
    QUIESCING = "quiescing"
    STOPPED = "stopped"
    FAILED = "failed"


class LifecycleController:
    """Own one Runner generation's protocol-visible lifecycle."""

    def __init__(
        self,
        *,
        channel_key: str,
        instance_id: str,
        environment_spec_id: str,
        environment_id: str,
        generation: int,
        protocol_min: int = 1,
        protocol_max: int = 1,
        capabilities: tuple[str, ...] = (),
        qwenpaw_version: str = "",
        send_handler: Callable[[SendParams], Any] | None = None,
        reaction_handler: Callable[[ReactionParams], Any] | None = None,
        secret_handle_consumer: Callable[[str, int], Any] | None = None,
        endpoint_handler: Callable[[str, EndpointParams | None], Any]
        | None = None,
        host_state_store: HostStateStore | None = None,
        clock_ms: Callable[[], int] | None = None,
    ) -> None:
        self.channel_key = channel_key
        self.instance_id = instance_id
        self.environment_spec_id = environment_spec_id
        self.environment_id = environment_id
        self.generation = generation
        self.protocol_min = protocol_min
        self.protocol_max = protocol_max
        self.capabilities = tuple(capabilities)
        self.qwenpaw_version = qwenpaw_version
        self.state = RunnerState.CREATED
        self.hello: HelloParams | None = None
        self.host_context: HostContext | None = None
        self.lease_token: str | None = None
        self.lease_expires_at_ms: int | None = None
        self.endpoint: EndpointParams | None = None
        self.host_state_store = host_state_store or HostStateStore()
        self.negotiated_capabilities: frozenset[str] = frozenset()
        self.effective_capabilities: frozenset[str] = frozenset()
        self._send_handler = send_handler
        self._reaction_handler = reaction_handler
        self._secret_handle_consumer = secret_handle_consumer
        self._secret_handle_attempted = False
        self._outbound_delivery_ids: set[str] = set()
        self._outbound_targets: dict[str, dict[str, Any]] = {}
        self._endpoint_handler = endpoint_handler
        self._clock_ms = clock_ms or (lambda: time.monotonic_ns() // 1_000_000)
        self._lock = asyncio.Lock()
        self._outbound_lock = asyncio.Lock()

    def _identity_error(self, reason: str) -> RpcError:
        """Create an identity or fencing error."""
        return RpcError(
            RPC_FENCING_ERROR,
            reason,
            data={"reason_code": reason},
        )

    def _lifecycle_error(self, reason: str) -> RpcError:
        """Create a lifecycle error."""
        return RpcError(
            RPC_LIFECYCLE_ERROR,
            reason,
            data={"reason_code": reason},
        )

    def _check_identity(self, params: IdentityParams) -> None:
        """Reject stale or cross-instance control parameters."""
        if params.channel_key != self.channel_key:
            raise self._identity_error("CHANNEL_KEY_MISMATCH")
        if params.instance_id != self.instance_id:
            raise self._identity_error("INSTANCE_ID_MISMATCH")
        if params.generation != self.generation:
            raise self._identity_error("GENERATION_FENCED")

    def _check_lease(self, params: LeaseParams) -> None:
        """Validate the current generation's lease token and expiry."""
        self._check_identity(params)
        if self.lease_token != params.lease_token:
            raise RpcError(
                RPC_AUTH_ERROR,
                "lease token mismatch",
                data={"reason_code": "LEASE_TOKEN_MISMATCH"},
            )
        if (
            self.lease_expires_at_ms is None
            or self._clock_ms() >= self.lease_expires_at_ms
        ):
            self.lease_token = None
            self.lease_expires_at_ms = None
            if self.state in {RunnerState.STANDBY, RunnerState.ACTIVE}:
                self.state = RunnerState.FAILED
            raise self._lifecycle_error("LEASE_EXPIRED")

    def _ensure_state(self, *allowed: RunnerState) -> None:
        """Require the current state to be one of the allowed states."""
        if self.state not in allowed:
            raise self._lifecycle_error("INVALID_STATE_TRANSITION")

    def accept_hello(self, params: HelloParams) -> dict[str, Any]:
        """Validate and accept a Runner hello handshake."""
        if params.channel_key != self.channel_key:
            raise self._identity_error("CHANNEL_KEY_MISMATCH")
        if params.instance_id != self.instance_id:
            raise self._identity_error("INSTANCE_ID_MISMATCH")
        if params.environment_spec_id != self.environment_spec_id:
            raise self._identity_error("ENVIRONMENT_SPEC_MISMATCH")
        if params.environment_id != self.environment_id:
            raise self._identity_error("ENVIRONMENT_ID_MISMATCH")
        if not (
            params.protocol_min <= self.protocol_max
            and self.protocol_min <= params.protocol_max
        ):
            raise RpcError(
                RPC_LIFECYCLE_ERROR,
                "protocol versions do not overlap",
                data={"reason_code": "PROTOCOL_MISMATCH"},
            )
        if self.state != RunnerState.CREATED:
            raise self._lifecycle_error("HELLO_ALREADY_ACCEPTED")
        self.hello = params
        self.negotiated_capabilities = frozenset(
            set(self.capabilities).intersection(params.capabilities),
        )
        return {
            "protocol_version": min(self.protocol_max, params.protocol_max),
            "capabilities": list(
                sorted(
                    self.negotiated_capabilities,
                ),
            ),
        }

    async def prepare(self, params: PrepareParams) -> dict[str, Any]:
        """Import and validate a candidate without consuming formal traffic."""
        async with self._lock:
            self._check_identity(params)
            self._ensure_state(RunnerState.CREATED)
            if self.hello is None:
                raise self._lifecycle_error("HELLO_REQUIRED")
            requested = set(params.capabilities)
            if not requested.issubset(self.negotiated_capabilities):
                raise RpcError(
                    RPC_CAPABILITY_ERROR,
                    "prepare requested an unnegotiated capability",
                    data={
                        "reason_code": "CAPABILITY_REQUIRED",
                        "capabilities": sorted(
                            requested - self.negotiated_capabilities,
                        ),
                    },
                )
            self.state = RunnerState.PREPARING
            try:
                await self._consume_secret_handle_locked(
                    params.host_context.secret_handle,
                )
                self.host_context = HostContext(
                    media_work_dir=params.host_context.media_work_dir,
                    config_snapshot=params.host_context.config_snapshot,
                )
                self.effective_capabilities = frozenset(requested)
                self.state = RunnerState.STANDBY
                return GenerationStatus(
                    state=self.state.value,
                    generation=self.generation,
                    consuming=False,
                ).to_mapping()
            except Exception:
                self.host_context = None
                self.effective_capabilities = frozenset()
                self.state = RunnerState.FAILED
                raise

    async def _consume_secret_handle_locked(
        self,
        secret_handle: str | None,
    ) -> None:
        """Consume one opaque prepare-scoped handle without retaining it."""
        if secret_handle is None:
            return
        if self._secret_handle_attempted:
            raise RpcError(
                RPC_AUTH_ERROR,
                "secret handle was already consumed",
                data={"reason_code": "SECRET_HANDLE_CONSUMED"},
            )
        self._secret_handle_attempted = True
        consumer = self._secret_handle_consumer
        if consumer is None:
            raise RpcError(
                RPC_AUTH_ERROR,
                "secret handle consumer is unavailable",
                data={"reason_code": "SECRET_HANDLE_INVALID"},
            )
        try:
            result = consumer(secret_handle, self.generation)
            if hasattr(result, "__await__"):
                await result
        except SecretHandleConsumedError as exc:
            raise RpcError(
                RPC_AUTH_ERROR,
                "secret handle was already consumed",
                data={"reason_code": "SECRET_HANDLE_CONSUMED"},
            ) from exc
        except SecretHandleInvalidError as exc:
            raise RpcError(
                RPC_AUTH_ERROR,
                "secret handle is invalid",
                data={"reason_code": "SECRET_HANDLE_INVALID"},
            ) from exc
        except RpcError:
            raise
        except Exception as exc:
            raise RpcError(
                RPC_AUTH_ERROR,
                "platform authentication failed",
                data={"reason_code": "PLATFORM_AUTH_FAILED"},
            ) from exc

    async def activate(self, params: LeaseParams) -> dict[str, Any]:
        """Grant a provisional lease while retaining standby semantics."""
        async with self._lock:
            self._check_identity(params)
            self._ensure_state(RunnerState.STANDBY)
            if params.lease_ttl_ms <= 0:
                raise self._lifecycle_error("INVALID_LEASE_TTL")
            self.lease_token = params.lease_token
            self.lease_expires_at_ms = self._clock_ms() + params.lease_ttl_ms
            return GenerationStatus(
                state=self.state.value,
                generation=self.generation,
                lease_expires_at_ms=self.lease_expires_at_ms,
                consuming=False,
            ).to_mapping()

    async def commit(self, params: LeaseParams) -> dict[str, Any]:
        """Turn the provisional lease into the active lease."""
        async with self._lock:
            self._ensure_state(RunnerState.STANDBY)
            self._check_lease(params)
            self.state = RunnerState.ACTIVE
            return GenerationStatus(
                state=self.state.value,
                generation=self.generation,
                lease_expires_at_ms=self.lease_expires_at_ms,
                consuming=True,
            ).to_mapping()

    async def lease_renew(self, params: LeaseParams) -> dict[str, Any]:
        """Renew a valid standby or active lease."""
        async with self._lock:
            self._ensure_state(RunnerState.STANDBY, RunnerState.ACTIVE)
            self._check_lease(params)
            self.lease_expires_at_ms = self._clock_ms() + params.lease_ttl_ms
            return GenerationStatus(
                state=self.state.value,
                generation=self.generation,
                lease_expires_at_ms=self.lease_expires_at_ms,
                consuming=self.state == RunnerState.ACTIVE,
            ).to_mapping()

    async def quiesce(self, params: QuiesceParams) -> dict[str, Any]:
        """Stop new work and enter the quiescing state."""
        async with self._outbound_lock:
            async with self._lock:
                self._check_identity(params)
                self._ensure_state(RunnerState.ACTIVE, RunnerState.STANDBY)
                await self._expire_lease_if_needed_async()
                if self.state == RunnerState.FAILED:
                    raise self._lifecycle_error("LEASE_EXPIRED")
                if self.state == RunnerState.ACTIVE:
                    if self.lease_token is None:
                        raise self._lifecycle_error("LEASE_REQUIRED")
                await self._unregister_endpoint_locked()
                self.state = RunnerState.QUIESCING
                self.lease_token = None
                self.lease_expires_at_ms = None
                return GenerationStatus(
                    state=self.state.value,
                    generation=self.generation,
                    consuming=False,
                ).to_mapping()

    async def health(self, params: IdentityParams) -> dict[str, Any]:
        """Return read-only lifecycle health."""
        async with self._lock:
            self._check_identity(params)
            if self.state in {RunnerState.STANDBY, RunnerState.ACTIVE}:
                await self._expire_lease_if_needed_async()
            return GenerationStatus(
                state=self.state.value,
                generation=self.generation,
                lease_expires_at_ms=self.lease_expires_at_ms,
                consuming=self.state == RunnerState.ACTIVE,
            ).to_mapping()

    async def generation_status(
        self,
        params: IdentityParams,
    ) -> dict[str, Any]:
        """Return the generation status without side effects."""
        return await self.health(params)

    async def stop(self, params: IdentityParams) -> dict[str, Any]:
        """Stop the Runner from any non-terminal state."""
        async with self._outbound_lock:
            async with self._lock:
                self._check_identity(params)
                await self._unregister_endpoint_locked()
                if self.state != RunnerState.STOPPED:
                    self.state = RunnerState.STOPPED
                self.lease_token = None
                self.lease_expires_at_ms = None
                return GenerationStatus(
                    state=self.state.value,
                    generation=self.generation,
                    consuming=False,
                ).to_mapping()

    async def send(self, params: SendParams) -> dict[str, Any]:
        """Send only from the committed active generation."""
        async with self._outbound_lock:
            async with self._lock:
                self._check_identity(params)
                self._ensure_state(RunnerState.ACTIVE)
                await self._expire_lease_if_needed_async()
                if self.state != RunnerState.ACTIVE:
                    raise self._lifecycle_error("LEASE_EXPIRED")
                self._check_send_capabilities(params)
                target = self._check_outbound_send_order(params)
                callback = self._send_handler
            if callback is not None:
                result = callback(params)
                if hasattr(result, "__await__"):
                    result = await result
            else:
                result = {
                    "status": "accepted",
                    "delivery_id": params.delivery_id,
                }
            async with self._lock:
                self._record_outbound_send(params, target)
            if isinstance(result, Mapping):
                return dict(result)
            return {"result": result}

    def _check_send_capabilities(self, params: SendParams) -> None:
        """Bind each outbound operation to its effective capability."""
        required: list[str] = []
        if params.operation in {
            OutboundOperation.MESSAGE_UPDATE,
            OutboundOperation.STREAM_START,
            OutboundOperation.STREAM_DELTA,
            OutboundOperation.STREAM_END,
        }:
            required.append("streaming")
        if params.approval is not None:
            required.append("approval_card")
        if any(part.get("type") != "text" for part in params.content_parts):
            required.append("media")
        missing = [
            capability
            for capability in required
            if capability not in self.effective_capabilities
        ]
        if missing:
            raise RpcError(
                RPC_CAPABILITY_ERROR,
                f"{missing[0]} capability was not negotiated",
                data={
                    "reason_code": "CAPABILITY_REQUIRED",
                    "capability": missing[0],
                },
            )

    def _check_outbound_send_order(
        self,
        params: SendParams,
    ) -> dict[str, Any] | None:
        """Validate delivery uniqueness and stream target ordering."""
        if params.delivery_id in self._outbound_delivery_ids:
            raise self._outbound_order_error("duplicate delivery_id")
        if params.target_delivery_id is None:
            return None
        target = self._outbound_targets.get(params.target_delivery_id)
        if target is None:
            raise self._outbound_target_error()
        if (
            target["operation"] is not OutboundOperation.STREAM_START
            or target["to_handle"] != params.to_handle
            or target["ended"]
        ):
            raise self._outbound_order_error("invalid outbound target")
        if params.sequence != target["sequence"] + 1:
            raise self._outbound_order_error("non-contiguous sequence")
        if (
            params.stream_type is not None
            and params.stream_type is not target["stream_type"]
        ):
            raise self._outbound_order_error("stream type mismatch")
        return target

    def _record_outbound_send(
        self,
        params: SendParams,
        target: dict[str, Any] | None,
    ) -> None:
        """Record one successfully accepted outbound operation."""
        self._outbound_delivery_ids.add(params.delivery_id)
        if params.operation in {
            OutboundOperation.MESSAGE_CREATE,
            OutboundOperation.STREAM_START,
        }:
            self._outbound_targets[params.delivery_id] = {
                "operation": params.operation,
                "to_handle": params.to_handle,
                "stream_type": params.stream_type,
                "sequence": params.sequence,
                "ended": False,
            }
        if target is not None:
            target["sequence"] = params.sequence
            if params.operation is OutboundOperation.STREAM_END:
                target["ended"] = True

    async def reaction(self, params: ReactionParams) -> dict[str, Any]:
        """Apply the v1 completed reaction to an accepted outbound target."""
        async with self._outbound_lock:
            async with self._lock:
                self._check_identity(params)
                self._ensure_state(RunnerState.ACTIVE)
                await self._expire_lease_if_needed_async()
                if self.state != RunnerState.ACTIVE:
                    raise self._lifecycle_error("LEASE_EXPIRED")
                if "reaction" not in self.effective_capabilities:
                    raise RpcError(
                        RPC_CAPABILITY_ERROR,
                        "reaction capability was not negotiated",
                        data={
                            "reason_code": "CAPABILITY_REQUIRED",
                            "capability": "reaction",
                        },
                    )
                if params.delivery_id in self._outbound_delivery_ids:
                    raise self._outbound_order_error(
                        "duplicate delivery_id",
                    )
                target = self._outbound_targets.get(
                    params.target_delivery_id,
                )
                if target is None:
                    raise self._outbound_target_error()
                if (
                    target["operation"]
                    not in {
                        OutboundOperation.MESSAGE_CREATE,
                        OutboundOperation.STREAM_START,
                    }
                    or target["to_handle"] != params.to_handle
                ):
                    raise self._outbound_order_error(
                        "invalid reaction target",
                    )
                if (
                    target["operation"] is OutboundOperation.STREAM_START
                    and not target["ended"]
                ):
                    raise self._outbound_order_error(
                        "stream target has not ended",
                    )
                callback = self._reaction_handler
            if callback is not None:
                result = callback(params)
                if hasattr(result, "__await__"):
                    result = await result
            else:
                result = {
                    "status": "accepted",
                    "delivery_id": params.delivery_id,
                }
            async with self._lock:
                self._outbound_delivery_ids.add(params.delivery_id)
            if isinstance(result, Mapping):
                return dict(result)
            return {"result": result}

    def _outbound_target_error(self) -> RpcError:
        """Create the stable error for an unknown outbound target."""
        return RpcError(
            RPC_LIFECYCLE_ERROR,
            "outbound target is unknown",
            data={"reason_code": "OUTBOUND_TARGET_UNKNOWN"},
        )

    def _outbound_order_error(self, message: str) -> RpcError:
        """Create the stable error for invalid outbound ordering."""
        return RpcError(
            RPC_LIFECYCLE_ERROR,
            message,
            data={"reason_code": "OUTBOUND_ORDER_VIOLATION"},
        )

    async def endpoint_register(
        self,
        params: EndpointParams,
    ) -> dict[str, Any]:
        """Register a candidate or active Runner-owned endpoint."""
        return await self._endpoint_change("register", params)

    async def endpoint_update(self, params: EndpointParams) -> dict[str, Any]:
        """Update a Runner-owned endpoint after rebinding or health change."""
        return await self._endpoint_change("update", params)

    async def endpoint_unregister(
        self,
        params: IdentityParams,
    ) -> dict[str, Any]:
        """Unregister the endpoint for this generation."""
        async with self._lock:
            self._check_identity(params)
            await self._unregister_endpoint_locked()
            return {"status": "unregistered", "generation": self.generation}

    async def _unregister_endpoint_locked(self) -> None:
        """Clear the endpoint and invoke the unregister hook once if needed."""
        had_endpoint = self.endpoint is not None
        self.endpoint = None
        if had_endpoint and self._endpoint_handler is not None:
            result = self._endpoint_handler("unregister", None)
            if hasattr(result, "__await__"):
                await result

    async def host_state_get(self, params: HostStateParams) -> dict[str, Any]:
        """Read instance-scoped host state."""
        async with self._lock:
            self._check_identity(params)
            if "host_state" not in self.effective_capabilities:
                raise RpcError(
                    RPC_CAPABILITY_ERROR,
                    "host state capability was not negotiated",
                    data={
                        "reason_code": "CAPABILITY_REQUIRED",
                        "capability": "host_state",
                    },
                )
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

    async def host_state_put(self, params: HostStateParams) -> dict[str, Any]:
        """Write host state only from the active generation."""
        async with self._lock:
            self._check_identity(params)
            expired = (
                self.lease_expires_at_ms is not None
                and self._clock_ms() >= self.lease_expires_at_ms
            )
            await self._expire_lease_if_needed_async()
            if expired:
                raise self._lifecycle_error("LEASE_EXPIRED")
            self._ensure_state(RunnerState.ACTIVE)
            if "host_state" not in self.effective_capabilities:
                raise RpcError(
                    RPC_CAPABILITY_ERROR,
                    "host state capability was not negotiated",
                    data={
                        "reason_code": "CAPABILITY_REQUIRED",
                        "capability": "host_state",
                    },
                )
            self._ensure_json_value(params.value)
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
        """Delete host state only from the active generation."""
        async with self._lock:
            self._check_identity(params)
            expired = (
                self.lease_expires_at_ms is not None
                and self._clock_ms() >= self.lease_expires_at_ms
            )
            await self._expire_lease_if_needed_async()
            if expired:
                raise self._lifecycle_error("LEASE_EXPIRED")
            self._ensure_state(RunnerState.ACTIVE)
            if "host_state" not in self.effective_capabilities:
                raise RpcError(
                    RPC_CAPABILITY_ERROR,
                    "host state capability was not negotiated",
                    data={
                        "reason_code": "CAPABILITY_REQUIRED",
                        "capability": "host_state",
                    },
                )
            await self.host_state_store.delete(params.key)
            return {"status": "deleted", "key": params.key}

    async def _endpoint_change(
        self,
        operation: str,
        params: EndpointParams,
    ) -> dict[str, Any]:
        """Apply a register or update operation with generation fencing."""
        async with self._lock:
            self._check_identity(params)
            self._ensure_state(RunnerState.STANDBY, RunnerState.ACTIVE)
            await self._expire_lease_if_needed_async()
            if self.state == RunnerState.FAILED:
                raise self._lifecycle_error("LEASE_EXPIRED")
            if "ingress_endpoint" not in self.effective_capabilities:
                raise RpcError(
                    RPC_CAPABILITY_ERROR,
                    "ingress endpoint capability was not negotiated",
                    data={
                        "reason_code": "CAPABILITY_REQUIRED",
                        "capability": "ingress_endpoint",
                    },
                )
            if self.state != RunnerState.ACTIVE and is_external_host(
                params.host,
            ):
                raise RpcError(
                    RPC_FENCING_ERROR,
                    "standby endpoint cannot be externally exposed",
                    data={"reason_code": "STANDBY_ENDPOINT_FORBIDDEN"},
                )
            self.endpoint = params
            if self._endpoint_handler is not None:
                result = self._endpoint_handler(operation, params)
                if hasattr(result, "__await__"):
                    await result
            return {
                "status": {
                    "register": "registered",
                    "update": "updated",
                }[operation],
                "generation": self.generation,
                "readiness": params.readiness,
            }

    def _expire_lease_if_needed(self) -> None:
        """Fence this generation when its lease has expired."""
        if self.lease_expires_at_ms is None:
            return
        if self._clock_ms() < self.lease_expires_at_ms:
            return
        self.lease_token = None
        self.lease_expires_at_ms = None
        if self.state in {RunnerState.STANDBY, RunnerState.ACTIVE}:
            self.state = RunnerState.FAILED

    async def _expire_lease_if_needed_async(self) -> None:
        """Fence an expired lease and revoke its endpoint registration."""
        had_endpoint = self.endpoint is not None
        self._expire_lease_if_needed()
        if had_endpoint and self.state is RunnerState.FAILED:
            await self._unregister_endpoint_locked()

    @staticmethod
    def _ensure_json_value(value: object) -> None:
        """Reject values that cannot cross the JSON protocol boundary."""
        try:
            _strict_json_dumps(value)
        except (TypeError, ValueError) as exc:
            raise ProtocolValidationError(
                "host state value must be JSON serializable",
                reason_code="SCHEMA_MISMATCH",
            ) from exc

    def register_rpc_methods(self, peer: RpcPeer) -> None:
        """Register Core-to-Runner lifecycle methods on a peer."""
        peer.register_method(
            "channel.prepare",
            lambda params, _: self.prepare(params),
        )
        peer.register_method(
            "channel.activate",
            lambda params, _: self.activate(params),
        )
        peer.register_method(
            "channel.commit",
            lambda params, _: self.commit(params),
        )
        peer.register_method(
            "channel.lease_renew",
            lambda params, _: self.lease_renew(params),
        )
        peer.register_method(
            "channel.quiesce",
            lambda params, _: self.quiesce(params),
        )
        peer.register_method(
            "channel.health",
            lambda params, _: self.health(params),
        )
        peer.register_method(
            "channel.generation_status",
            lambda params, _: self.generation_status(params),
        )
        peer.register_method(
            "channel.stop",
            lambda params, _: self.stop(params),
        )
        peer.register_method(
            "channel.send",
            lambda params, _: self.send(params),
        )
        peer.register_method(
            "channel.reaction",
            lambda params, _: self.reaction(params),
        )


__all__ = [
    "LifecycleController",
    "CoreLifecycleAdapter",
    "FixtureSecretHandleConsumer",
    "HostStateStore",
    "RPC_AUTH_ERROR",
    "RPC_FENCING_ERROR",
    "RPC_LIFECYCLE_ERROR",
    "RPC_CAPABILITY_ERROR",
    "RunnerState",
]
