# -*- coding: utf-8 -*-
"""Runner lifecycle, lease, generation, endpoint, and host-state rules."""

from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, Callable, Mapping

from .errors import (
    PlatformAuthenticationError,
    ProtocolValidationError,
    RpcError,
    SecretHandleConsumedError,
    SecretHandleInvalidError,
)
from .models import (
    DeliveryState,
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
    OutboundResult,
    PrepareParams,
    QuiesceParams,
    ReactionParams,
    SendParams,
    is_external_host,
)
from .reliability import InboundInbox, OutboundDeliveryLedger
from .rpc import RpcPeer, RpcResponsePublication, request_was_cancelled


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
        try:
            result = self._sink(secret_value)
            if hasattr(result, "__await__"):
                await result
        finally:
            secret_value = None

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
        self.controller._endpoint_registry_handler = self._endpoint_handler

    def _endpoint_handler(
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
        async with self.controller._lock:
            if params.identity is None:
                raise self.controller._lifecycle_error(
                    "INVALID_EVENT_BATCH",
                )
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
        async with self.controller._lock:
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
        async with self.controller._lock:
            self._check_capability("ingress_endpoint")
            self.controller._check_identity(params)
            await self.controller._expire_lease_if_needed_async()
            if self.controller.state == RunnerState.FAILED:
                raise self.controller._lifecycle_error("LEASE_EXPIRED")
            if self.controller.state not in {
                RunnerState.STANDBY,
                RunnerState.ACTIVE,
            }:
                raise self.controller._lifecycle_error(
                    "INVALID_STATE_TRANSITION",
                )
            if (
                self.controller.state != RunnerState.ACTIVE
                and is_external_host(params.host)
            ):
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


@dataclass
class _OutboundAttempt:
    """Track one immutable platform-side attempt across lifecycle changes."""

    delivery_id: str
    epoch: int
    task: asyncio.Task[Any]
    target: dict[str, Any] | None = None
    forced_reason: str | None = None
    drain_deadline: float | None = None
    terminal_result: OutboundResult | None = None
    send_params: SendParams | None = None
    provisional: bool = False
    drain_timer: asyncio.TimerHandle | None = None
    done: asyncio.Event = field(default_factory=asyncio.Event)


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
        self._outbound_delivery_states: dict[str, DeliveryState] = {}
        self._outbound_targets: dict[str, dict[str, Any]] = {}
        self._outbound_attempts: dict[str, _OutboundAttempt] = {}
        self._lifecycle_epoch = 0
        self._endpoint_handler = endpoint_handler
        self._endpoint_registry_handler: Callable[
            [str, EndpointParams | None],
            None,
        ] | None = None
        self._clock_ms = clock_ms or (lambda: time.monotonic_ns() // 1_000_000)
        self._lock = asyncio.Lock()

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
                if request_was_cancelled():
                    raise asyncio.CancelledError
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
            except asyncio.CancelledError:
                self.host_context = None
                self.effective_capabilities = frozenset()
                self.state = RunnerState.FAILED
                raise
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
        failure_reason: str | None = None
        try:
            result = consumer(secret_handle, self.generation)
            if hasattr(result, "__await__"):
                await result
        except SecretHandleConsumedError:
            failure_reason = "SECRET_HANDLE_CONSUMED"
        except SecretHandleInvalidError:
            failure_reason = "SECRET_HANDLE_INVALID"
        except PlatformAuthenticationError:
            failure_reason = "PLATFORM_AUTH_FAILED"
        except Exception:
            failure_reason = "PLATFORM_AUTH_FAILED"
        if failure_reason is not None:
            raise self._secret_handle_error(failure_reason)

    @staticmethod
    def _secret_handle_error(reason_code: str) -> RpcError:
        """Create a fixed secret error without retaining consumer failures."""
        messages = {
            "SECRET_HANDLE_CONSUMED": "secret handle was already consumed",
            "SECRET_HANDLE_INVALID": "secret handle is invalid",
            "PLATFORM_AUTH_FAILED": "platform authentication failed",
        }
        return RpcError(
            RPC_AUTH_ERROR,
            messages[reason_code],
            data={"reason_code": reason_code},
        )

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
            await self._expire_lease_if_needed_async()
            if self.state is RunnerState.FAILED:
                raise self._lifecycle_error("LEASE_EXPIRED")
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
            await self._expire_lease_if_needed_async()
            if self.state is RunnerState.FAILED:
                raise self._lifecycle_error("LEASE_EXPIRED")
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
        async with self._lock:
            self._check_identity(params)
            self._ensure_state(RunnerState.ACTIVE, RunnerState.STANDBY)
            await self._expire_lease_if_needed_async()
            if self.state == RunnerState.FAILED:
                raise self._lifecycle_error("LEASE_EXPIRED")
            if self.state == RunnerState.ACTIVE and self.lease_token is None:
                raise self._lifecycle_error("LEASE_REQUIRED")
            endpoint_hook = self._detach_endpoint_locked()
            self.state = RunnerState.QUIESCING
            self.lease_token = None
            self.lease_expires_at_ms = None
            drain_deadline = (
                asyncio.get_running_loop().time()
                + params.drain_timeout_ms / 1000
            )
            attempts = self._fence_outbound_attempts_locked()
            for attempt in attempts:
                attempt.drain_deadline = drain_deadline
                attempt.drain_timer = asyncio.get_running_loop().call_at(
                    drain_deadline,
                    self._expire_outbound_drain,
                    attempt,
                )
        await self._wait_for_endpoint_hook(endpoint_hook, drain_deadline)
        pending = await self._wait_for_outbound_attempts(
            attempts,
            drain_deadline,
        )
        if pending:
            async with self._lock:
                for attempt in pending:
                    self._force_outbound_unknown_locked(
                        attempt,
                        "DRAIN_TIMEOUT",
                        cancel=True,
                    )
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
        async with self._lock:
            self._check_identity(params)
            endpoint_hook = self._detach_endpoint_locked()
            if self.state != RunnerState.STOPPED:
                self.state = RunnerState.STOPPED
                attempts = self._fence_outbound_attempts_locked()
                for attempt in attempts:
                    self._force_outbound_unknown_locked(
                        attempt,
                        "LIFECYCLE_FENCED",
                        cancel=True,
                    )
            self.lease_token = None
            self.lease_expires_at_ms = None
            result = GenerationStatus(
                state=self.state.value,
                generation=self.generation,
                consuming=False,
            ).to_mapping()
        self._schedule_endpoint_hook(endpoint_hook)
        return result

    async def send(
        self,
        params: SendParams,
        *,
        defer_response_publication: bool = False,
    ) -> dict[str, Any] | RpcResponsePublication:
        """Send only from the committed active generation."""
        async with self._lock:
            self._check_identity(params)
            self._ensure_state(RunnerState.ACTIVE)
            await self._expire_lease_if_needed_async()
            if self.state != RunnerState.ACTIVE:
                raise self._lifecycle_error("LEASE_EXPIRED")
            self._check_send_capabilities(params)
            target = self._check_outbound_send_order(params)
            attempt = self._reserve_outbound_delivery(
                params.delivery_id,
                target,
            )
            callback = self._send_handler
        try:
            if callback is not None:
                raw_result = callback(params)
                if hasattr(raw_result, "__await__"):
                    raw_result = await raw_result
            else:
                raw_result = {
                    "delivery_id": params.delivery_id,
                    "state": DeliveryState.ACKNOWLEDGED.value,
                }
            result = self._parse_outbound_result(
                raw_result,
                params.delivery_id,
            )
            if request_was_cancelled():
                raise asyncio.CancelledError
            result = await self._finish_outbound_attempt(
                attempt,
                result,
                send_params=params,
                defer_completion=defer_response_publication,
            )
        except asyncio.CancelledError:
            await self._finish_outbound_unknown_resilient(
                attempt,
                "REQUEST_CANCELLED",
            )
            raise
        except Exception:
            await self._finish_outbound_unknown_resilient(
                attempt,
                "PLATFORM_RESULT_UNKNOWN",
            )
            raise
        mapping = result.to_mapping()
        if not defer_response_publication:
            return mapping
        return self._outbound_response_publication(attempt, mapping)

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
        if params.delivery_id in self._outbound_delivery_states:
            raise self._outbound_order_error("duplicate delivery_id")
        if params.target_delivery_id is None:
            return None
        target = self._outbound_targets.get(params.target_delivery_id)
        if target is None:
            raise self._outbound_target_error()
        if target.get("pending_delivery_id") is not None:
            raise self._outbound_order_error("outbound target is busy")
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
                "pending_delivery_id": None,
            }
        if target is not None:
            target["sequence"] = params.sequence
            if params.operation is OutboundOperation.STREAM_END:
                target["ended"] = True

    async def reaction(
        self,
        params: ReactionParams,
        *,
        defer_response_publication: bool = False,
    ) -> dict[str, Any] | RpcResponsePublication:
        """Apply the v1 completed reaction to an accepted outbound target."""
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
            if params.delivery_id in self._outbound_delivery_states:
                raise self._outbound_order_error("duplicate delivery_id")
            self._check_reaction_target(params)
            attempt = self._reserve_outbound_delivery(params.delivery_id)
            callback = self._reaction_handler
        try:
            if callback is not None:
                raw_result = callback(params)
                if hasattr(raw_result, "__await__"):
                    raw_result = await raw_result
            else:
                raw_result = {
                    "delivery_id": params.delivery_id,
                    "state": DeliveryState.ACKNOWLEDGED.value,
                }
            result = self._parse_outbound_result(
                raw_result,
                params.delivery_id,
            )
            if request_was_cancelled():
                raise asyncio.CancelledError
            result = await self._finish_outbound_attempt(
                attempt,
                result,
                defer_completion=defer_response_publication,
            )
        except asyncio.CancelledError:
            await self._finish_outbound_unknown_resilient(
                attempt,
                "REQUEST_CANCELLED",
            )
            raise
        except Exception:
            await self._finish_outbound_unknown_resilient(
                attempt,
                "PLATFORM_RESULT_UNKNOWN",
            )
            raise
        mapping = result.to_mapping()
        if not defer_response_publication:
            return mapping
        return self._outbound_response_publication(attempt, mapping)

    def _check_reaction_target(self, params: ReactionParams) -> None:
        """Require a published create or stream target for reactions."""
        target = self._outbound_targets.get(params.target_delivery_id)
        if target is None:
            raise self._outbound_target_error()
        if target.get("pending_delivery_id") is not None:
            raise self._outbound_order_error("outbound target is busy")
        if (
            target["operation"]
            not in {
                OutboundOperation.MESSAGE_CREATE,
                OutboundOperation.STREAM_START,
            }
            or target["to_handle"] != params.to_handle
        ):
            raise self._outbound_order_error("invalid reaction target")

    def _reserve_outbound_delivery(
        self,
        delivery_id: str,
        target: dict[str, Any] | None = None,
    ) -> _OutboundAttempt:
        """Occupy a delivery ID before any platform side effect starts."""
        task = asyncio.current_task()
        if task is None:
            raise RuntimeError("outbound attempt requires an asyncio task")
        attempt = _OutboundAttempt(
            delivery_id=delivery_id,
            epoch=self._lifecycle_epoch,
            task=task,
            target=target,
        )
        self._outbound_delivery_states[delivery_id] = DeliveryState.SENDING
        self._outbound_attempts[delivery_id] = attempt
        if target is not None:
            target["pending_delivery_id"] = delivery_id
        return attempt

    @staticmethod
    def _parse_outbound_result(
        value: object,
        delivery_id: str,
    ) -> OutboundResult:
        """Validate a handler result and bind it to the request ID."""
        result = OutboundResult.from_mapping(value)
        if result.delivery_id != delivery_id:
            raise ProtocolValidationError(
                "outbound result delivery_id does not match request",
                path=("delivery_id",),
                reason_code="SCHEMA_MISMATCH",
            )
        return result

    async def _finish_outbound_attempt(
        self,
        attempt: _OutboundAttempt,
        result: OutboundResult,
        *,
        send_params: SendParams | None = None,
        defer_completion: bool = False,
    ) -> OutboundResult:
        """Commit a terminal attempt result at one lifecycle boundary."""
        async with self._lock:
            await self._expire_lease_if_needed_async()
            result = self._fence_outbound_result_locked(attempt, result)
            if defer_completion:
                attempt.terminal_result = result
                attempt.send_params = send_params
                attempt.provisional = True
                return result
            self._commit_outbound_result_locked(
                attempt,
                result,
                send_params,
            )
            return result

    def _outbound_response_publication(
        self,
        attempt: _OutboundAttempt,
        result: dict[str, Any],
    ) -> RpcResponsePublication:
        """Keep an outbound result retractable until its response is sent."""
        return RpcResponsePublication(
            result=result,
            on_published=lambda: self._publish_outbound_attempt(attempt),
            on_aborted=lambda reason_code: self._finish_outbound_unknown(
                attempt,
                reason_code,
            ),
        )

    async def _publish_outbound_attempt(
        self,
        attempt: _OutboundAttempt,
    ) -> None:
        """Finalize one provisional attempt after response publication."""
        async with self._lock:
            if self._outbound_attempts.get(attempt.delivery_id) is attempt:
                result = attempt.terminal_result
                if result is None:
                    return
                self._commit_outbound_result_locked(
                    attempt,
                    result,
                    attempt.send_params,
                )

    def _commit_outbound_result_locked(
        self,
        attempt: _OutboundAttempt,
        result: OutboundResult,
        send_params: SendParams | None,
    ) -> None:
        """Make one terminal result and its ordering effects visible."""
        self._outbound_delivery_states[attempt.delivery_id] = result.state
        if (
            send_params is not None
            and result.state is DeliveryState.ACKNOWLEDGED
        ):
            self._record_outbound_send(send_params, attempt.target)
        attempt.provisional = False
        attempt.terminal_result = None
        attempt.send_params = None
        self._complete_outbound_attempt_locked(attempt)

    def _fence_outbound_result_locked(
        self,
        attempt: _OutboundAttempt,
        result: OutboundResult,
    ) -> OutboundResult:
        """Apply lifecycle, drain, and lease fencing before result commit."""
        if attempt.forced_reason is not None:
            return self._unknown_outbound_result(
                attempt.delivery_id,
                attempt.forced_reason,
            )
        if (
            attempt.drain_deadline is not None
            and asyncio.get_running_loop().time() >= attempt.drain_deadline
        ):
            return self._unknown_outbound_result(
                attempt.delivery_id,
                "DRAIN_TIMEOUT",
            )
        if self.state not in {RunnerState.ACTIVE, RunnerState.QUIESCING}:
            return self._unknown_outbound_result(
                attempt.delivery_id,
                "LIFECYCLE_FENCED",
            )
        if (
            self.state is RunnerState.ACTIVE
            and attempt.epoch != self._lifecycle_epoch
        ):
            return self._unknown_outbound_result(
                attempt.delivery_id,
                "LIFECYCLE_FENCED",
            )
        return result

    async def _finish_outbound_unknown_resilient(
        self,
        attempt: _OutboundAttempt,
        reason_code: str,
    ) -> None:
        """Finish cleanup despite repeated task cancellation."""
        cleanup = asyncio.create_task(
            self._finish_outbound_unknown(attempt, reason_code),
        )
        while not cleanup.done():
            try:
                await asyncio.shield(cleanup)
            except asyncio.CancelledError:
                continue
        cleanup.result()

    async def _finish_outbound_unknown(
        self,
        attempt: _OutboundAttempt,
        reason_code: str,
    ) -> None:
        """Retain an attempted delivery ID after an uncertain outcome."""
        async with self._lock:
            if self._outbound_attempts.get(attempt.delivery_id) is attempt:
                self._outbound_delivery_states[
                    attempt.delivery_id
                ] = DeliveryState.UNKNOWN
                if attempt.forced_reason is None:
                    attempt.forced_reason = reason_code
                self._complete_outbound_attempt_locked(attempt)

    def _complete_outbound_attempt_locked(
        self,
        attempt: _OutboundAttempt,
    ) -> None:
        """Release in-flight ordering state after a terminal result."""
        if (
            attempt.target is not None
            and attempt.target.get("pending_delivery_id")
            == attempt.delivery_id
        ):
            attempt.target["pending_delivery_id"] = None
        self._outbound_attempts.pop(attempt.delivery_id, None)
        if attempt.drain_timer is not None:
            attempt.drain_timer.cancel()
            attempt.drain_timer = None
        attempt.done.set()

    @staticmethod
    def _unknown_outbound_result(
        delivery_id: str,
        reason_code: str,
    ) -> OutboundResult:
        """Create one stable unknown attempt result."""
        return OutboundResult(
            delivery_id=delivery_id,
            state=DeliveryState.UNKNOWN,
            reason_code=reason_code,
        )

    def _fence_outbound_attempts_locked(self) -> list[_OutboundAttempt]:
        """Close the current admission epoch and snapshot in-flight work."""
        self._lifecycle_epoch += 1
        return list(self._outbound_attempts.values())

    async def _wait_for_outbound_attempts(
        self,
        attempts: list[_OutboundAttempt],
        deadline: float,
    ) -> list[_OutboundAttempt]:
        """Wait at most the declared drain duration for attempt completion."""
        remaining = deadline - asyncio.get_running_loop().time()
        if attempts and remaining > 0:
            try:
                await asyncio.wait_for(
                    asyncio.gather(
                        *(attempt.done.wait() for attempt in attempts),
                    ),
                    timeout=remaining,
                )
            except asyncio.TimeoutError:
                pass
        return [attempt for attempt in attempts if not attempt.done.is_set()]

    def _force_outbound_unknown_locked(
        self,
        attempt: _OutboundAttempt,
        reason_code: str,
        *,
        cancel: bool,
    ) -> None:
        """Fence one unfinished attempt without waiting for its handler."""
        if self._outbound_attempts.get(attempt.delivery_id) is not attempt:
            return
        was_provisional = attempt.provisional
        attempt.forced_reason = reason_code
        attempt.provisional = False
        attempt.terminal_result = None
        attempt.send_params = None
        self._outbound_delivery_states[
            attempt.delivery_id
        ] = DeliveryState.UNKNOWN
        self._complete_outbound_attempt_locked(attempt)
        if (cancel or was_provisional) and not attempt.task.done():
            attempt.task.cancel()

    @staticmethod
    def _expire_outbound_drain(attempt: _OutboundAttempt) -> None:
        """Fence and interrupt one drain cohort at its absolute deadline."""
        if attempt.done.is_set():
            return
        if attempt.forced_reason is None:
            attempt.forced_reason = "DRAIN_TIMEOUT"
        if not attempt.task.done():
            attempt.task.cancel()

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
            endpoint_hook = self._detach_endpoint_locked()
        self._schedule_endpoint_hook(endpoint_hook)
        return {"status": "unregistered", "generation": self.generation}

    def _detach_endpoint_locked(self) -> Any:
        """Clear routing state and return optional best-effort hook work."""
        had_endpoint = self.endpoint is not None
        self.endpoint = None
        if not had_endpoint:
            return None
        if self._endpoint_registry_handler is not None:
            self._endpoint_registry_handler("unregister", None)
        endpoint_handler = self._endpoint_handler
        if endpoint_handler is None:
            return None

        async def notify_endpoint_handler() -> None:
            """Invoke unregister cleanup after releasing the state lock."""
            try:
                result = endpoint_handler("unregister", None)
                if hasattr(result, "__await__"):
                    await result
            except Exception:
                return

        return notify_endpoint_handler()

    @staticmethod
    def _consume_endpoint_hook(task: asyncio.Future[Any]) -> None:
        """Consume completion of a detached best-effort endpoint hook."""
        if task.cancelled():
            return
        try:
            task.exception()
        except Exception:
            return

    def _schedule_endpoint_hook(self, result: Any) -> None:
        """Run a detached endpoint hook without blocking lifecycle state."""
        if not hasattr(result, "__await__"):
            return
        task = asyncio.ensure_future(result)
        task.add_done_callback(self._consume_endpoint_hook)

    async def _wait_for_endpoint_hook(
        self,
        result: Any,
        deadline: float,
    ) -> None:
        """Wait for unregister only within the quiesce drain deadline."""
        if not hasattr(result, "__await__"):
            return
        task = asyncio.ensure_future(result)
        task.add_done_callback(self._consume_endpoint_hook)
        remaining = deadline - asyncio.get_running_loop().time()
        if remaining > 0:
            await asyncio.wait({task}, timeout=remaining)
        if not task.done():
            task.cancel()

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
            if self._endpoint_registry_handler is not None:
                self._endpoint_registry_handler(operation, params)
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
        was_failed = self.state is RunnerState.FAILED
        self._expire_lease_if_needed()
        if self.state is RunnerState.FAILED and not was_failed:
            attempts = self._fence_outbound_attempts_locked()
            for attempt in attempts:
                self._force_outbound_unknown_locked(
                    attempt,
                    "LEASE_EXPIRED",
                    cancel=False,
                )
            if had_endpoint:
                self._schedule_endpoint_hook(self._detach_endpoint_locked())

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
            lambda params, _: self.send(
                params,
                defer_response_publication=True,
            ),
        )
        peer.register_method(
            "channel.reaction",
            lambda params, _: self.reaction(
                params,
                defer_response_publication=True,
            ),
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
