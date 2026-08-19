# -*- coding: utf-8 -*-
"""Core-owned generation authority for isolated Channel protocol calls."""

from __future__ import annotations

import asyncio
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncIterator,
    Awaitable,
    Callable,
    Literal,
    Mapping,
)

from .errors import (
    RPC_AUTH_ERROR,
    RPC_CAPABILITY_ERROR,
    RPC_FENCING_ERROR,
    RPC_LIFECYCLE_ERROR,
    RpcError,
)
from .models import IdentityParams, LeaseParams, PrepareParams, QuiesceParams

if TYPE_CHECKING:
    from .rpc import RpcPeer


@dataclass(frozen=True)
class CoreSlotSnapshot:
    """Immutable Core view of one active or candidate generation."""

    generation: int
    epoch: int
    phase: str
    capabilities: frozenset[str]
    lease_token: str | None = None
    lease_expires_at_ms: int | None = None


@dataclass(frozen=True)
class CoreAuthorizationSnapshot:
    """Immutable route authorization published by the Core authority."""

    active: CoreSlotSnapshot | None = None
    candidate: CoreSlotSnapshot | None = None
    highest_generation: int = -1
    fenced_generation: int | None = None


@dataclass(frozen=True)
class CoreOperationToken:
    """Fencing token for one in-flight Core-to-Runner operation."""

    kind: str
    generation: int
    epoch: int
    sequence: int


@dataclass(frozen=True, slots=True, eq=False)
class _CoreControlToken:
    """Opaque capability binding one Core client to one Runner slot."""

    authority_nonce: object
    client_nonce: object
    generation: int
    epoch: int


class _Slot:
    """Mutable state for one bounded Core generation slot."""

    def __init__(
        self,
        generation: int,
        epoch: int,
        capabilities: frozenset[str],
    ) -> None:
        self.generation = generation
        self.epoch = epoch
        self.phase = "preparing"
        self.capabilities = capabilities
        self.lease_token: str | None = None
        self.lease_expires_at_ms: int | None = None
        self.operation: CoreOperationToken | None = None

    def snapshot(self) -> CoreSlotSnapshot:
        """Return an immutable copy of this slot."""
        return CoreSlotSnapshot(
            generation=self.generation,
            epoch=self.epoch,
            phase=self.phase,
            capabilities=self.capabilities,
            lease_token=self.lease_token,
            lease_expires_at_ms=self.lease_expires_at_ms,
        )


class CoreGenerationAuthority:
    """Own bounded Core lifecycle and route authorization state."""

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
        self._active: _Slot | None = None
        self._candidate: _Slot | None = None
        self._highest_generation = -1
        self._fenced_generation = -1
        self._next_epoch = 0
        self._next_sequence = 0
        self._control_nonce = object()
        self._snapshot = CoreAuthorizationSnapshot()
        self._lock = asyncio.Lock()

    @property
    def snapshot(self) -> CoreAuthorizationSnapshot:
        """Return the last immutable authorization snapshot."""
        return self._snapshot

    def _identity_error(self, reason: str) -> RpcError:
        """Create one stable Core fencing error."""
        return RpcError(
            RPC_FENCING_ERROR,
            reason,
            data={"reason_code": reason},
        )

    def _lifecycle_error(self, reason: str) -> RpcError:
        """Create one stable Core lifecycle error."""
        return RpcError(
            RPC_LIFECYCLE_ERROR,
            reason,
            data={"reason_code": reason},
        )

    def _check_identity(self, params: IdentityParams) -> None:
        """Validate stable identity before any generation mutation."""
        if params.channel_key != self.channel_key:
            raise self._identity_error("CHANNEL_KEY_MISMATCH")
        if params.instance_id != self.instance_id:
            raise self._identity_error("INSTANCE_ID_MISMATCH")

    def check_identity(self, params: IdentityParams) -> None:
        """Validate stable identity for synchronous Core callers."""
        self._check_identity(params)

    def _publish(self) -> None:
        """Publish a frozen view after a synchronous state transition."""
        self._snapshot = CoreAuthorizationSnapshot(
            active=self._active.snapshot() if self._active else None,
            candidate=(
                self._candidate.snapshot() if self._candidate else None
            ),
            highest_generation=self._highest_generation,
            fenced_generation=(
                self._fenced_generation
                if self._fenced_generation >= 0
                else None
            ),
        )

    def _next_operation(self, kind: str, slot: _Slot) -> CoreOperationToken:
        """Reserve one operation on a slot."""
        if slot.operation is not None:
            raise self._lifecycle_error("INVALID_STATE_TRANSITION")
        self._next_sequence += 1
        token = CoreOperationToken(
            kind=kind,
            generation=slot.generation,
            epoch=slot.epoch,
            sequence=self._next_sequence,
        )
        slot.operation = token
        return token

    def _slot(self, generation: int) -> _Slot | None:
        """Return the bounded slot for one generation."""
        if self._active and self._active.generation == generation:
            return self._active
        if self._candidate and self._candidate.generation == generation:
            return self._candidate
        return None

    def _require_slot(self, generation: int) -> _Slot:
        """Return a slot or a stable generation fencing error."""
        slot = self._slot(generation)
        if slot is not None:
            return slot
        if generation <= self._highest_generation:
            raise self._identity_error("GENERATION_REVOKED")
        raise self._identity_error("GENERATION_UNKNOWN")

    def _expire(self, slot: _Slot) -> bool:
        """Fence a slot whose Core lease has expired."""
        expiry = slot.lease_expires_at_ms
        if expiry is None or self._clock_ms() < expiry:
            return False
        self._revoke_slot(slot)
        return True

    def _revoke_slot(self, slot: _Slot) -> None:
        """Irreversibly revoke one active or candidate slot."""
        slot.operation = None
        self._fenced_generation = max(
            self._fenced_generation,
            slot.generation,
        )
        if self._active is slot:
            self._active = None
        if self._candidate is slot:
            self._candidate = None
        self._publish()

    def _validate_operation(
        self,
        token: CoreOperationToken,
        kind: str,
    ) -> _Slot:
        """Validate an operation token after a Runner response."""
        slot = self._slot(token.generation)
        if (
            slot is None
            or slot.epoch != token.epoch
            or slot.operation != token
            or token.kind != kind
        ):
            raise self._identity_error("GENERATION_REVOKED")
        if self._expire(slot):
            raise self._lifecycle_error("LEASE_EXPIRED")
        if self._slot(token.generation) is not slot:
            raise self._identity_error("GENERATION_REVOKED")
        return slot

    def _prepare_start(self, params: PrepareParams) -> CoreOperationToken:
        """Stage one bounded candidate and reserve its prepare operation."""
        self._check_identity(params)
        if params.generation < self._highest_generation:
            raise self._identity_error("GENERATION_STALE")
        if params.generation <= self._fenced_generation:
            raise self._identity_error("GENERATION_REVOKED")
        if self._active and self._active.generation == params.generation:
            raise self._identity_error("GENERATION_ALREADY_PREPARED")
        if self._candidate is not None:
            if self._candidate.generation == params.generation:
                raise self._identity_error("GENERATION_ALREADY_PREPARED")
            self._candidate = None
        self._highest_generation = max(
            self._highest_generation,
            params.generation,
        )
        self._next_epoch += 1
        self._candidate = _Slot(
            params.generation,
            self._next_epoch,
            frozenset(params.capabilities),
        )
        token = self._next_operation("prepare", self._candidate)
        self._publish()
        return token

    def _prepare_abort(self, token: CoreOperationToken) -> None:
        """Abort a matching candidate without affecting the active slot."""
        slot = self._slot(token.generation)
        if slot is not None and slot.operation == token:
            self._candidate = None
            self._publish()

    def _prepare_complete(self, token: CoreOperationToken) -> None:
        """Complete prepare and expose standby capabilities."""
        slot = self._validate_operation(token, "prepare")
        slot.operation = None
        slot.phase = "standby"
        self._publish()

    def _activate_start(
        self,
        params: LeaseParams,
    ) -> CoreOperationToken:
        """Reserve a provisional lease activation."""
        self._check_identity(params)
        slot = self._require_slot(params.generation)
        if slot is not self._candidate or slot.phase != "standby":
            raise self._lifecycle_error("INVALID_STATE_TRANSITION")
        if slot.lease_token is not None:
            raise self._identity_error("GENERATION_ALREADY_PREPARED")
        return self._next_operation("activate", slot)

    def _activate_complete(
        self,
        token: CoreOperationToken,
        params: LeaseParams,
    ) -> None:
        """Record a provisional lease after a successful Runner call."""
        slot = self._validate_operation(token, "activate")
        self._validate_lease_operation(token, params)
        slot.lease_token = params.lease_token
        slot.lease_expires_at_ms = self._clock_ms() + params.lease_ttl_ms
        slot.operation = None
        self._publish()

    def _commit_start(self, params: LeaseParams) -> CoreOperationToken:
        """Reserve a generation commit."""
        self._check_identity(params)
        slot = self._require_slot(params.generation)
        if self._expire(slot):
            raise self._lifecycle_error("LEASE_EXPIRED")
        if slot is not self._candidate or slot.phase != "standby":
            raise self._lifecycle_error("INVALID_STATE_TRANSITION")
        if slot.lease_token != params.lease_token:
            raise RpcError(
                RPC_AUTH_ERROR,
                "lease token mismatch",
                data={"reason_code": "LEASE_TOKEN_MISMATCH"},
            )
        return self._next_operation("commit", slot)

    def _commit_complete(
        self,
        token: CoreOperationToken,
        params: LeaseParams,
    ) -> None:
        """Promote a validated candidate and fence the previous active."""
        slot = self._validate_operation(token, "commit")
        self._validate_lease_operation(token, params, slot=slot)
        previous = self._active
        slot.operation = None
        slot.phase = "active"
        self._active = slot
        self._candidate = None
        if previous is not None and previous is not slot:
            self._fenced_generation = max(
                self._fenced_generation,
                previous.generation,
            )
            previous.operation = None
        self._publish()

    def _renew_start(self, params: LeaseParams) -> CoreOperationToken:
        """Reserve a lease renewal for active or standby generation."""
        self._check_identity(params)
        slot = self._require_slot(params.generation)
        if self._expire(slot):
            raise self._lifecycle_error("LEASE_EXPIRED")
        if slot.lease_token != params.lease_token:
            raise RpcError(
                RPC_AUTH_ERROR,
                "lease token mismatch",
                data={"reason_code": "LEASE_TOKEN_MISMATCH"},
            )
        if slot.phase not in {"standby", "active"}:
            raise self._lifecycle_error("INVALID_STATE_TRANSITION")
        return self._next_operation("renew", slot)

    def _renew_complete(
        self,
        token: CoreOperationToken,
        params: LeaseParams,
    ) -> None:
        """Apply a lease renewal after validating its operation token."""
        slot = self._validate_operation(token, "renew")
        self._validate_lease_operation(token, params, slot=slot)
        slot.lease_expires_at_ms = self._clock_ms() + params.lease_ttl_ms
        slot.operation = None
        self._publish()

    def _validate_lease_operation(
        self,
        token: CoreOperationToken,
        params: LeaseParams,
        *,
        slot: _Slot | None = None,
    ) -> None:
        """Validate identity and lease facts for a completed operation."""
        self._check_identity(params)
        if params.generation != token.generation:
            raise self._identity_error("GENERATION_REVOKED")
        if slot is not None and slot.lease_token != params.lease_token:
            raise RpcError(
                RPC_AUTH_ERROR,
                "lease token mismatch",
                data={"reason_code": "LEASE_TOKEN_MISMATCH"},
            )

    def _abort_operation(self, token: CoreOperationToken) -> None:
        """Release a still-pending operation without changing its slot."""
        slot = self._slot(token.generation)
        if slot is not None and slot.operation == token:
            slot.operation = None
            self._publish()

    def _issue_control_token(
        self,
        slot: _Slot,
        client_nonce: object,
    ) -> _CoreControlToken:
        """Create one opaque Runner control capability."""
        return _CoreControlToken(
            authority_nonce=self._control_nonce,
            client_nonce=client_nonce,
            generation=slot.generation,
            epoch=slot.epoch,
        )

    def _validate_control_token(
        self,
        params: IdentityParams,
        token: _CoreControlToken | None,
        client_nonce: object,
    ) -> _CoreControlToken:
        """Validate a peer-bound control capability without slot lookup."""
        self._check_identity(params)
        if (
            token is None
            or token.authority_nonce is not self._control_nonce
            or token.client_nonce is not client_nonce
            or token.generation != params.generation
        ):
            raise self._identity_error("GENERATION_UNKNOWN")
        return token

    def _revoke_for_control(
        self,
        params: IdentityParams,
        token: _CoreControlToken | None,
        client_nonce: object,
    ) -> None:
        """Fence a current slot while retaining old peer control rights."""
        control = self._validate_control_token(
            params,
            token,
            client_nonce,
        )
        slot = self._slot(control.generation)
        if slot is not None and slot.epoch == control.epoch:
            self._revoke_slot(slot)

    def _current_control_slot(
        self,
        params: IdentityParams,
        token: _CoreControlToken | None,
        client_nonce: object,
    ) -> _Slot:
        """Return the current slot owned by one peer-bound client."""
        control = self._validate_control_token(
            params,
            token,
            client_nonce,
        )
        slot = self._slot(control.generation)
        if slot is None or slot.epoch != control.epoch:
            raise self._identity_error("GENERATION_REVOKED")
        return slot

    def _reject_bound_prepare(
        self,
        params: PrepareParams,
        token: _CoreControlToken,
        client_nonce: object,
    ) -> None:
        """Reject reuse of one client for another Runner admission."""
        self._check_identity(params)
        if (
            token.authority_nonce is not self._control_nonce
            or token.client_nonce is not client_nonce
        ):
            raise self._identity_error("GENERATION_UNKNOWN")
        raise self._lifecycle_error("INVALID_STATE_TRANSITION")

    def endpoint_snapshot(self, generation: int) -> CoreSlotSnapshot | None:
        """Return a slot snapshot for synchronous endpoint admission."""
        snapshot = self._snapshot
        for slot in (snapshot.active, snapshot.candidate):
            if slot is not None and slot.generation == generation:
                return slot
        return None

    def route_snapshot(self, generation: int) -> CoreSlotSnapshot | None:
        """Return an active, unexpired slot from the immutable snapshot."""
        slot = self._snapshot.active
        if slot is None or slot.generation != generation:
            return None
        expiry = slot.lease_expires_at_ms
        if expiry is None or self._clock_ms() >= expiry:
            return None
        return slot

    def generation_error(self, generation: int) -> RpcError:
        """Classify a generation absent from the bounded slots."""
        if generation <= self._fenced_generation:
            return self._identity_error("GENERATION_REVOKED")
        if generation <= self._highest_generation:
            return self._identity_error("GENERATION_REVOKED")
        return self._identity_error("GENERATION_UNKNOWN")

    @asynccontextmanager
    async def host_operation(
        self,
        params: IdentityParams,
        *,
        capability: str | None = None,
        allowed_phases: tuple[str, ...] = (),
    ) -> AsyncIterator[CoreSlotSnapshot]:
        """Hold the Core lock across admission and Core-owned mutation."""
        async with self._lock:
            self._check_identity(params)
            slot = self._require_slot(params.generation)
            if self._expire(slot):
                raise self._lifecycle_error("LEASE_EXPIRED")
            if allowed_phases and slot.phase not in allowed_phases:
                raise self._lifecycle_error("INVALID_STATE_TRANSITION")
            if capability is not None and capability not in slot.capabilities:
                raise RpcError(
                    RPC_CAPABILITY_ERROR,
                    "capability was not negotiated",
                    data={
                        "reason_code": "CAPABILITY_REQUIRED",
                        "capability": capability,
                    },
                )
            yield slot.snapshot()

    @asynccontextmanager
    async def endpoint_operation(
        self,
        params: IdentityParams,
        *,
        allow_revoked: bool = False,
    ) -> AsyncIterator[CoreSlotSnapshot | None]:
        """Hold the Core lock across endpoint admission and mutation."""
        async with self._lock:
            self._check_identity(params)
            slot = self._slot(params.generation)
            if slot is None:
                if (
                    allow_revoked
                    and params.generation <= self._highest_generation
                ):
                    yield None
                    return
                raise self.generation_error(params.generation)
            expired = self._expire(slot)
            if expired and not allow_revoked:
                raise self._lifecycle_error("LEASE_EXPIRED")
            if self._slot(params.generation) is None:
                if allow_revoked:
                    yield None
                    return
                raise self._identity_error("GENERATION_REVOKED")
            yield slot.snapshot()

    async def prepare_start(
        self,
        params: PrepareParams,
    ) -> CoreOperationToken:
        """Reserve a prepare operation under the Core lock."""
        async with self._lock:
            return self._prepare_start(params)

    async def control_start(
        self,
        kind: Literal["prepare", "activate", "commit", "renew"],
        params: PrepareParams | LeaseParams,
        token: _CoreControlToken | None,
        client_nonce: object,
    ) -> tuple[CoreOperationToken, _CoreControlToken]:
        """Reserve one operation for the peer that owns the slot."""
        async with self._lock:
            if kind == "prepare":
                if not isinstance(params, PrepareParams):
                    raise TypeError("prepare requires PrepareParams")
                if token is not None:
                    self._reject_bound_prepare(params, token, client_nonce)
                operation = self._prepare_start(params)
                slot = self._candidate
                if slot is None or slot.operation != operation:
                    raise AssertionError(
                        "prepare admission lost its candidate",
                    )
                control = self._issue_control_token(slot, client_nonce)
                return operation, control
            if not isinstance(params, LeaseParams):
                raise TypeError(f"{kind} requires LeaseParams")
            self._current_control_slot(params, token, client_nonce)
            if kind == "activate":
                operation = self._activate_start(params)
            elif kind == "commit":
                operation = self._commit_start(params)
            else:
                operation = self._renew_start(params)
            if token is None:
                raise AssertionError("validated control token is missing")
            return operation, token

    async def prepare_abort(self, token: CoreOperationToken) -> None:
        """Abort a prepare operation if it is still current."""
        async with self._lock:
            self._prepare_abort(token)

    async def prepare_complete(self, token: CoreOperationToken) -> None:
        """Commit a successful prepare operation."""
        async with self._lock:
            self._prepare_complete(token)

    async def activate_start(self, params: LeaseParams) -> CoreOperationToken:
        """Reserve activation under the Core lock."""
        async with self._lock:
            return self._activate_start(params)

    async def activate_complete(
        self,
        token: CoreOperationToken,
        params: LeaseParams,
    ) -> None:
        """Commit a successful activation under the Core lock."""
        async with self._lock:
            self._activate_complete(token, params)

    async def commit_start(self, params: LeaseParams) -> CoreOperationToken:
        """Reserve commit under the Core lock."""
        async with self._lock:
            return self._commit_start(params)

    async def commit_complete(
        self,
        token: CoreOperationToken,
        params: LeaseParams,
    ) -> None:
        """Commit a successful generation promotion."""
        async with self._lock:
            self._commit_complete(token, params)

    async def renew_start(self, params: LeaseParams) -> CoreOperationToken:
        """Reserve renewal under the Core lock."""
        async with self._lock:
            return self._renew_start(params)

    async def renew_complete(
        self,
        token: CoreOperationToken,
        params: LeaseParams,
    ) -> None:
        """Apply a successful renewal under the Core lock."""
        async with self._lock:
            self._renew_complete(token, params)

    async def control_shutdown(
        self,
        params: IdentityParams,
        token: _CoreControlToken | None,
        client_nonce: object,
    ) -> None:
        """Fence a current slot for one peer-bound shutdown call."""
        async with self._lock:
            self._revoke_for_control(params, token, client_nonce)

    async def abort_operation(self, token: CoreOperationToken) -> None:
        """Release a failed or cancelled control operation."""
        async with self._lock:
            self._abort_operation(token)


@dataclass
class _CoreClientState:
    """Retain mutable control state for one immutable peer binding."""

    client_nonce: object = field(default_factory=object)
    control_token: _CoreControlToken | None = None
    binding_lock: asyncio.Lock = field(default_factory=asyncio.Lock)


@dataclass(frozen=True, init=False)
class CoreLifecycleClient:
    """Linearize Core authority with one immutable Runner peer."""

    _peer: RpcPeer = field(repr=False, compare=False)
    authority: CoreGenerationAuthority
    prune_endpoints: Callable[[], None] | None = field(
        default=None,
        repr=False,
        compare=False,
    )
    _state: _CoreClientState = field(
        default_factory=_CoreClientState,
        init=False,
        repr=False,
        compare=False,
    )

    def __init__(
        self,
        peer: RpcPeer,
        authority: CoreGenerationAuthority,
        prune_endpoints: Callable[[], None] | None = None,
    ) -> None:
        object.__setattr__(self, "_peer", peer)
        object.__setattr__(self, "authority", authority)
        object.__setattr__(self, "prune_endpoints", prune_endpoints)
        object.__setattr__(self, "_state", _CoreClientState())

    @property
    def peer(self) -> RpcPeer:
        """Return the immutable Runner peer bound at construction."""
        return self._peer

    def _prune_endpoints(self) -> None:
        """Discard endpoint records outside the two authority slots."""
        if self.prune_endpoints is not None:
            self.prune_endpoints()

    @staticmethod
    async def _cleanup(
        callback: Callable[[], Awaitable[None]],
    ) -> bool:
        """Run an authority transition despite caller cancellation."""
        task = asyncio.ensure_future(callback())
        cancelled = False
        while not task.done():
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError:
                cancelled = True
        task.result()
        return cancelled

    async def prepare(
        self,
        params: PrepareParams,
        *,
        timeout: float | None = None,
    ) -> Any:
        """Stage Core admission around one Runner prepare request."""
        async with self._state.binding_lock:
            token, control_token = await self.authority.control_start(
                "prepare",
                params,
                self._state.control_token,
                self._state.client_nonce,
            )
            self._state.control_token = control_token
        self._prune_endpoints()
        try:
            result = await self.peer.call(
                "channel.prepare",
                params.to_mapping(),
                timeout=timeout,
            )
            self._require_generation_result(result, params.generation)
            cancelled = await self._cleanup(
                lambda: self.authority.prepare_complete(token),
            )
            if cancelled:
                raise asyncio.CancelledError
        except BaseException:
            await self._cleanup(
                lambda: self.authority.prepare_abort(token),
            )
            self._prune_endpoints()
            raise
        return result

    async def activate(
        self,
        params: LeaseParams,
        *,
        timeout: float | None = None,
    ) -> Any:
        """Record a provisional Core lease after Runner activation."""
        token, _ = await self.authority.control_start(
            "activate",
            params,
            self._state.control_token,
            self._state.client_nonce,
        )
        try:
            result = await self.peer.call(
                "channel.activate",
                params.to_mapping(),
                timeout=timeout,
            )
            self._require_generation_result(result, params.generation)
            cancelled = await self._cleanup(
                lambda: self.authority.activate_complete(token, params),
            )
            if cancelled:
                raise asyncio.CancelledError
        except BaseException:
            await self._cleanup(
                lambda: self.authority.abort_operation(token),
            )
            self._prune_endpoints()
            raise
        return result

    async def commit(
        self,
        params: LeaseParams,
        *,
        timeout: float | None = None,
    ) -> Any:
        """Authorize routing only after Runner commit succeeds."""
        token, _ = await self.authority.control_start(
            "commit",
            params,
            self._state.control_token,
            self._state.client_nonce,
        )
        try:
            result = await self.peer.call(
                "channel.commit",
                params.to_mapping(),
                timeout=timeout,
            )
            self._require_active_result(result, params.generation)
            cancelled = await self._cleanup(
                lambda: self.authority.commit_complete(token, params),
            )
            if cancelled:
                raise asyncio.CancelledError
        except BaseException:
            await self._cleanup(
                lambda: self.authority.abort_operation(token),
            )
            self._prune_endpoints()
            raise
        return result

    async def lease_renew(
        self,
        params: LeaseParams,
        *,
        timeout: float | None = None,
    ) -> Any:
        """Renew Runner and Core leases without reviving fenced state."""
        token, _ = await self.authority.control_start(
            "renew",
            params,
            self._state.control_token,
            self._state.client_nonce,
        )
        try:
            result = await self.peer.call(
                "channel.lease_renew",
                params.to_mapping(),
                timeout=timeout,
            )
            self._require_generation_result(result, params.generation)
            cancelled = await self._cleanup(
                lambda: self.authority.renew_complete(token, params),
            )
            if cancelled:
                raise asyncio.CancelledError
        except BaseException:
            await self._cleanup(
                lambda: self.authority.abort_operation(token),
            )
            self._prune_endpoints()
            raise
        self._prune_endpoints()
        return result

    async def quiesce(
        self,
        params: QuiesceParams,
        *,
        timeout: float | None = None,
    ) -> Any:
        """Revoke formal routing before asking Runner to quiesce."""
        await self.authority.control_shutdown(
            params,
            self._state.control_token,
            self._state.client_nonce,
        )
        self._prune_endpoints()
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
        await self.authority.control_shutdown(
            params,
            self._state.control_token,
            self._state.client_nonce,
        )
        self._prune_endpoints()
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
            or result.get("generation") != generation
        ):
            raise RpcError(
                RPC_LIFECYCLE_ERROR,
                "Runner returned an invalid generation result",
                data={"reason_code": "INVALID_GENERATION_RESULT"},
            )


__all__ = [
    "CoreAuthorizationSnapshot",
    "CoreGenerationAuthority",
    "CoreLifecycleClient",
    "CoreOperationToken",
    "CoreSlotSnapshot",
]
