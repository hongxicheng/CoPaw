# -*- coding: utf-8 -*-
"""Runner lifecycle, lease, generation, endpoint, and host-state rules."""

from __future__ import annotations

import asyncio
import json
import time
from enum import StrEnum
from typing import Any, Callable, Mapping

from .errors import ProtocolValidationError, RpcError
from .models import (
    EndpointParams,
    GenerationStatus,
    HelloParams,
    HostContext,
    HostStateParams,
    IdentityParams,
    LeaseParams,
    PrepareParams,
    QuiesceParams,
    SendParams,
)
from .rpc import RpcPeer


RPC_LIFECYCLE_ERROR = -32010
RPC_FENCING_ERROR = -32011
RPC_AUTH_ERROR = -32012


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
        endpoint_handler: Callable[[str, EndpointParams | None], Any]
        | None = None,
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
        self._host_state: dict[str, tuple[int, object]] = {}
        self._send_handler = send_handler
        self._endpoint_handler = endpoint_handler
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
        return {
            "protocol_version": min(self.protocol_max, params.protocol_max),
            "capabilities": list(
                sorted(
                    set(self.capabilities).intersection(params.capabilities),
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
            self.state = RunnerState.PREPARING
            self.host_context = params.host_context
            self.capabilities = tuple(
                sorted(
                    set(self.capabilities).intersection(params.capabilities),
                ),
            )
            self.state = RunnerState.STANDBY
            return GenerationStatus(
                state=self.state.value,
                generation=self.generation,
                consuming=False,
            ).to_mapping()

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
        async with self._lock:
            self._check_identity(params)
            self._ensure_state(RunnerState.ACTIVE, RunnerState.STANDBY)
            if self.state == RunnerState.ACTIVE:
                if self.lease_token is None:
                    raise self._lifecycle_error("LEASE_REQUIRED")
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
                self._expire_lease_if_needed()
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
        async with self._lock:
            self._check_identity(params)
            self._ensure_state(RunnerState.ACTIVE)
            self._expire_lease_if_needed()
            if self.state != RunnerState.ACTIVE:
                raise self._lifecycle_error("LEASE_EXPIRED")
            callback = self._send_handler
        if callback is not None:
            result = callback(params)
            if hasattr(result, "__await__"):
                result = await result
        else:
            result = {"status": "accepted", "delivery_id": params.delivery_id}
        if isinstance(result, Mapping):
            return dict(result)
        return {"result": result}

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
            self.endpoint = None
            if self._endpoint_handler is not None:
                result = self._endpoint_handler("unregister", None)
                if hasattr(result, "__await__"):
                    await result
            return {"status": "unregistered", "generation": self.generation}

    async def host_state_get(self, params: HostStateParams) -> dict[str, Any]:
        """Read instance-scoped host state."""
        async with self._lock:
            self._check_identity(params)
            entry = self._host_state.get(params.key)
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
            self._ensure_state(RunnerState.ACTIVE)
            self._ensure_json_value(params.value)
            self._host_state[params.key] = (
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
            self._ensure_state(RunnerState.ACTIVE)
            self._host_state.pop(params.key, None)
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
            if self.state != RunnerState.ACTIVE and params.bound_externally:
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
                "status": operation + "ed",
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

    @staticmethod
    def _ensure_json_value(value: object) -> None:
        """Reject values that cannot cross the JSON protocol boundary."""
        try:
            json.dumps(value, ensure_ascii=False)
        except (TypeError, ValueError) as exc:
            raise ProtocolValidationError(
                "host state value must be JSON serializable",
                reason_code="SCHEMA_MISMATCH",
            ) from exc

    def register_rpc_methods(self, peer: RpcPeer) -> None:
        """Register the controller on a bidirectional RPC peer."""
        peer.register_method(
            "runner.hello",
            lambda params, _: self.accept_hello(params),
        )
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


__all__ = [
    "LifecycleController",
    "RPC_AUTH_ERROR",
    "RPC_FENCING_ERROR",
    "RPC_LIFECYCLE_ERROR",
    "RunnerState",
]
