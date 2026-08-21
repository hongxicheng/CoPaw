# -*- coding: utf-8 -*-
"""Runner lifecycle, lease, endpoint, and outbound orchestration."""

from __future__ import annotations

import asyncio
import time
from contextlib import asynccontextmanager
from collections.abc import AsyncIterator
from enum import StrEnum
from typing import TYPE_CHECKING, Any, Callable, Mapping

from .errors import (
    PlatformAuthenticationError,
    RPC_AUTH_ERROR,
    RPC_CAPABILITY_ERROR,
    RPC_FENCING_ERROR,
    RPC_LIFECYCLE_ERROR,
    RpcError,
    SecretHandleConsumedError,
    SecretHandleInvalidError,
)
from .models import (
    DeliveryState,
    EndpointParams,
    GenerationStatus,
    HelloParams,
    HostContext,
    IdentityParams,
    LeaseParams,
    OutboundOperation,
    OutboundResult,
    PrepareParams,
    QuiesceParams,
    ReactionParams,
    ResponseFinishParams,
    ResponseFinishResult,
    ResponseOutcome,
    SendParams,
    validate_response_handle,
    is_external_host,
)
from .outbound import (
    OutboundAttempt,
    OutboundDeliveryState,
    OutboundStateError,
)
from .response_lifecycle import (
    ResponseCheckpointUnknownError,
    ResponseCleanupState,
    ResponseResourceRef,
    ResponseRouteAggregate,
    ResponseRouteSnapshot,
    ResponseStateError,
    RunnerDeliveryResult,
)
from .rpc import RpcPeer, RpcResponsePublication, request_was_cancelled

if TYPE_CHECKING:
    from .host import CoreLifecycleAdapter, HostStateStore


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


class RunnerState(StrEnum):
    """Stable Runner lifecycle states."""

    CREATED = "created"
    PREPARING = "preparing"
    STANDBY = "standby"
    ACTIVE = "active"
    QUIESCING = "quiescing"
    STOPPED = "stopped"
    FAILED = "failed"


class LifecycleController:  # pylint: disable=too-many-public-methods
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
        response_finish_handler: Callable[
            [ResponseFinishParams, ResponseRouteSnapshot],
            Any,
        ]
        | None = None,
        response_checkpoint_put: Callable[[ResponseRouteSnapshot, bool], Any]
        | None = None,
        response_checkpoint_delete: Callable[[str, int], Any] | None = None,
        secret_handle_consumer: Callable[[str, int], Any] | None = None,
        endpoint_handler: Callable[[str, EndpointParams | None], Any]
        | None = None,
        clock_ms: Callable[[], int] | None = None,
        max_response_routes: int = 1024,
        response_clock_ms: Callable[[], int] | None = None,
    ) -> None:
        if max_response_routes <= 0:
            raise ValueError("max_response_routes must be positive")
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
        self.negotiated_capabilities: frozenset[str] = frozenset()
        self.effective_capabilities: frozenset[str] = frozenset()
        self._send_handler = send_handler
        self._reaction_handler = reaction_handler
        self._response_finish_handler = response_finish_handler
        self._response_checkpoint_put = response_checkpoint_put
        self._response_checkpoint_delete = response_checkpoint_delete
        self._secret_handle_consumer = secret_handle_consumer
        self._secret_handle_attempted = False
        self._outbound = OutboundDeliveryState()
        self._response_routes = ResponseRouteAggregate(
            max_entries=max_response_routes,
            clock_ms=response_clock_ms,
        )
        self._response_finish_tasks: dict[str, asyncio.Task[bool]] = {}
        self._response_discard_tasks: dict[str, asyncio.Task[None]] = {}
        self._response_checkpoint_pending: set[str] = set()
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

    @asynccontextmanager
    async def _host_operation(
        self,
        params: IdentityParams,
        *,
        capability: str | None = None,
        allowed_states: tuple[RunnerState, ...] = (),
        expire_lease: bool = False,
        expired_reason: str | None = None,
    ) -> AsyncIterator[RunnerState]:
        """Guard one Core-owned operation with lifecycle fencing."""
        async with self._lock:
            self._check_identity(params)
            if (
                capability is not None
                and capability not in self.effective_capabilities
            ):
                raise RpcError(
                    RPC_CAPABILITY_ERROR,
                    "capability was not negotiated",
                    data={
                        "reason_code": "CAPABILITY_REQUIRED",
                        "capability": capability,
                    },
                )
            expired = (
                self.lease_expires_at_ms is not None
                and self._clock_ms() >= self.lease_expires_at_ms
            )
            if expire_lease:
                self._expire_lease_and_revoke()
            if expired and expired_reason is not None:
                raise self._lifecycle_error(expired_reason)
            if allowed_states:
                self._ensure_state(*allowed_states)
            yield self.state

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
            attempts = self._outbound.fence_attempts()
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

    async def open_response_route(
        self,
        response_handle: str,
        route_refs: tuple[ResponseResourceRef, ...] = (),
    ) -> None:
        """Persist one active response route before event submission."""
        handle = validate_response_handle(response_handle)
        async with self._lock:
            self._check_response_capability()
            self._ensure_state(RunnerState.ACTIVE)
            await self._expire_lease_if_needed_async()
            if self.state != RunnerState.ACTIVE:
                raise self._lifecycle_error("LEASE_EXPIRED")
            if handle in self._response_checkpoint_pending:
                raise self._response_error(
                    "RESPONSE_BUSY",
                    "response route checkpoint is pending",
                    retryable=True,
                )
            try:
                snapshot, created = self._response_routes.open(
                    handle,
                    route_refs,
                )
            except ResponseStateError as exc:
                raise self._response_error_from_state(exc) from exc
            self._response_checkpoint_pending.add(handle)
        try:
            await self._put_response_snapshot(snapshot, provisional=created)
        except BaseException as exc:
            settlement_unknown = isinstance(
                exc,
                (ResponseCheckpointUnknownError, asyncio.CancelledError),
            )
            if created and not settlement_unknown:
                async with self._lock:
                    self._response_checkpoint_pending.discard(handle)
                    self._response_routes.rollback_open(
                        handle,
                        snapshot.version,
                    )
            else:
                async with self._lock:
                    self._response_checkpoint_pending.discard(handle)
            raise
        async with self._lock:
            self._response_checkpoint_pending.discard(handle)

    async def restore_response_routes(
        self,
        snapshots: tuple[ResponseRouteSnapshot, ...],
    ) -> None:
        """Restore persisted aggregate snapshots before generation commit."""
        expired: list[ResponseRouteSnapshot] = []
        async with self._lock:
            self._check_response_capability()
            self._ensure_state(
                RunnerState.PREPARING,
                RunnerState.STANDBY,
                RunnerState.ACTIVE,
            )
            for snapshot in snapshots:
                try:
                    restored = self._response_routes.restore(snapshot)
                except ResponseStateError as exc:
                    raise self._response_error_from_state(exc) from exc
                if (
                    not restored
                    and self._response_routes.snapshot(
                        snapshot.response_handle,
                    )
                    is None
                ):
                    expired.append(snapshot)
        for snapshot in expired:
            await self._delete_response_snapshot(
                snapshot.response_handle,
                snapshot.version,
            )

    async def discard_response_route(self, response_handle: str) -> None:
        """Revoke and reconcile one permanently rejected response route."""
        handle = validate_response_handle(response_handle)
        async with self._lock:
            task = self._response_discard_tasks.get(handle)
            if task is None and (
                handle in self._response_checkpoint_pending
                or self._outbound.has_inflight_response(handle)
            ):
                raise self._response_error(
                    "RESPONSE_BUSY",
                    "response route has an in-flight operation",
                    retryable=True,
                )
            if task is None:
                try:
                    snapshot = self._response_routes.begin_revocation(handle)
                except ResponseStateError as exc:
                    raise self._response_error_from_state(exc) from exc
                if snapshot is None:
                    return
                task = self._response_discard_task_locked(snapshot)
        await asyncio.shield(task)

    async def _run_response_discard(
        self,
        snapshot: ResponseRouteSnapshot,
    ) -> None:
        """Persist a revocation, delete its checkpoint, then remove it."""
        try:
            await self._put_response_snapshot(snapshot)
            async with self._lock:
                self._response_routes.commit_revocation(snapshot)
            await self._delete_response_snapshot(
                snapshot.response_handle,
                snapshot.version,
            )
            async with self._lock:
                self._response_routes.commit_discard(snapshot)
        finally:
            async with self._lock:
                task = asyncio.current_task()
                if (
                    self._response_discard_tasks.get(
                        snapshot.response_handle,
                    )
                    is task
                ):
                    self._response_discard_tasks.pop(
                        snapshot.response_handle,
                        None,
                    )

    def _response_discard_task_locked(
        self,
        snapshot: ResponseRouteSnapshot,
    ) -> asyncio.Task[None]:
        """Return the one temporary discard task for a response handle."""
        task = self._response_discard_tasks.get(snapshot.response_handle)
        if task is not None:
            return task
        task = asyncio.create_task(self._run_response_discard(snapshot))
        self._response_discard_tasks[snapshot.response_handle] = task
        return task

    async def resume_response_cleanups(self) -> None:
        """Retry both terminal cleanup and route revocation tasks."""
        async with self._lock:
            pending = self._response_routes.pending_cleanups()
            finish_tasks = [
                self._response_finish_task_locked(item) for item in pending
            ]
            revoked = self._response_routes.pending_revocations()
            discard_tasks = [
                self._response_discard_task_locked(item) for item in revoked
            ]
        tasks = [*finish_tasks, *discard_tasks]
        if tasks:
            await asyncio.gather(
                *(asyncio.shield(task) for task in tasks),
                return_exceptions=True,
            )

    async def response_route_snapshot(
        self,
        response_handle: str,
    ) -> ResponseRouteSnapshot | None:
        """Return one immutable response snapshot for diagnostics."""
        handle = validate_response_handle(response_handle)
        async with self._lock:
            return self._response_routes.snapshot(handle)

    async def response_route_refs(
        self,
        response_handle: str,
    ) -> tuple[ResponseResourceRef, ...]:
        """Resolve active route refs without exposing aggregate mutation."""
        handle = validate_response_handle(response_handle)
        async with self._lock:
            try:
                return self._response_routes.active_route_refs(handle)
            except ResponseStateError as exc:
                raise self._response_error_from_state(exc) from exc

    async def response_resource_ref(
        self,
        response_handle: str,
        resource_id: str,
    ) -> ResponseResourceRef | None:
        """Resolve one active platform resource by delivery ID."""
        handle = validate_response_handle(response_handle)
        async with self._lock:
            try:
                return self._response_routes.resource_ref(
                    handle,
                    resource_id,
                )
            except ResponseStateError as exc:
                raise self._response_error_from_state(exc) from exc

    async def gc_response_routes(self) -> None:
        """Delete only expired cleanup-complete response receipts."""
        async with self._lock:
            expired = tuple(
                snapshot
                for snapshot in self._response_routes.expired_completed()
                if snapshot.response_handle
                not in self._response_checkpoint_pending
            )
            self._response_checkpoint_pending.update(
                snapshot.response_handle for snapshot in expired
            )
        first_error: BaseException | None = None
        for snapshot in expired:
            try:
                await self._delete_response_snapshot(
                    snapshot.response_handle,
                    snapshot.version,
                )
            except BaseException as exc:
                if first_error is None:
                    first_error = exc
            else:
                async with self._lock:
                    self._response_routes.commit_gc(snapshot)
            finally:
                async with self._lock:
                    self._response_checkpoint_pending.discard(
                        snapshot.response_handle,
                    )
        if first_error is not None:
            raise first_error

    async def response_finish(
        self,
        params: ResponseFinishParams,
    ) -> dict[str, Any]:
        """Finish one response scope after Core reaches a terminal outcome."""
        async with self._lock:
            self._check_identity(params)
            self._ensure_state(RunnerState.ACTIVE)
            await self._expire_lease_if_needed_async()
            if self.state != RunnerState.ACTIVE:
                raise self._lifecycle_error("LEASE_EXPIRED")
            self._check_response_capability()
            if self._outbound.has_inflight_response(
                params.response_handle,
            ):
                raise self._response_error(
                    "RESPONSE_BUSY",
                    "response has an in-flight delivery",
                    retryable=True,
                )
            if params.response_handle in self._response_checkpoint_pending:
                raise self._response_error(
                    "RESPONSE_BUSY",
                    "response route checkpoint is pending",
                    retryable=True,
                )
            try:
                snapshot = self._response_routes.begin_finish(
                    params.response_handle,
                    params.outcome,
                )
            except ResponseStateError as exc:
                raise self._response_error_from_state(exc) from exc
            if snapshot.cleanup_state is ResponseCleanupState.COMPLETE:
                return ResponseFinishResult(
                    response_handle=params.response_handle,
                    outcome=params.outcome,
                ).to_mapping()
            task = self._response_finish_task_locked(snapshot)
        cleanup_complete = await asyncio.shield(task)
        if not cleanup_complete:
            raise self._response_error(
                "RESPONSE_FINISH_FAILED",
                "response finish handler failed",
                retryable=True,
            )
        return ResponseFinishResult(
            response_handle=params.response_handle,
            outcome=params.outcome,
        ).to_mapping()

    async def _run_response_finish(
        self,
        params: ResponseFinishParams,
    ) -> bool:
        """Persist close, clean resources, and persist completion."""
        success = False
        try:
            async with self._lock:
                snapshot = self._response_routes.snapshot(
                    params.response_handle,
                )
            if snapshot is None:
                return False
            await self._put_response_snapshot(snapshot)
            handler = self._response_finish_handler
            if handler is not None:
                result = handler(params, snapshot)
                if hasattr(result, "__await__"):
                    await result
            async with self._lock:
                candidate = self._response_routes.cleanup_candidate(
                    params.response_handle,
                    params.outcome,
                )
            await self._put_response_snapshot(candidate)
            async with self._lock:
                self._response_routes.commit_cleanup(candidate)
            success = True
        except asyncio.CancelledError:
            pass
        except Exception:
            pass
        finally:
            async with self._lock:
                task = asyncio.current_task()
                if (
                    self._response_finish_tasks.get(
                        params.response_handle,
                    )
                    is task
                ):
                    self._response_finish_tasks.pop(
                        params.response_handle,
                        None,
                    )
        return success

    def _response_finish_task_locked(
        self,
        snapshot: ResponseRouteSnapshot,
    ) -> asyncio.Task[bool]:
        """Return the one temporary cleanup task for a response handle."""
        task = self._response_finish_tasks.get(snapshot.response_handle)
        if task is not None:
            return task
        if snapshot.outcome is None:
            raise RuntimeError("terminal response receipt requires an outcome")
        params = ResponseFinishParams(
            channel_key=self.channel_key,
            instance_id=self.instance_id,
            generation=self.generation,
            response_handle=snapshot.response_handle,
            outcome=snapshot.outcome,
        )
        task = asyncio.create_task(self._run_response_finish(params))
        self._response_finish_tasks[snapshot.response_handle] = task
        return task

    async def _put_response_snapshot(
        self,
        snapshot: ResponseRouteSnapshot,
        *,
        provisional: bool = False,
    ) -> None:
        handler = self._response_checkpoint_put
        if handler is None:
            return
        result = handler(snapshot, provisional)
        if hasattr(result, "__await__"):
            await result

    async def _delete_response_snapshot(
        self,
        response_handle: str,
        version: int,
    ) -> None:
        handler = self._response_checkpoint_delete
        if handler is None:
            return
        result = handler(response_handle, version)
        if hasattr(result, "__await__"):
            await result

    def _check_response_capability(self) -> None:
        """Require the negotiated response lifecycle capability."""
        if "response_lifecycle" not in self.effective_capabilities:
            raise RpcError(
                RPC_CAPABILITY_ERROR,
                "response lifecycle capability was not negotiated",
                data={
                    "reason_code": "CAPABILITY_REQUIRED",
                    "capability": "response_lifecycle",
                },
            )

    def _response_scope_for_operation_locked(
        self,
        response_handle: str,
    ) -> bool:
        """Check one response-scoped outbound operation before admission."""
        try:
            return self._response_routes.admit_operation(response_handle)
        except ResponseStateError as exc:
            raise self._response_error_from_state(exc) from exc

    def _response_error_from_state(self, exc: ResponseStateError) -> RpcError:
        """Convert a synchronous registry violation to the wire error."""
        return self._response_error(
            exc.reason_code,
            exc.message,
            retryable=exc.retryable,
        )

    def _response_error(
        self,
        reason_code: str,
        message: str,
        *,
        retryable: bool = False,
    ) -> RpcError:
        """Create one stable response lifecycle error."""
        data: dict[str, Any] = {"reason_code": reason_code}
        if retryable:
            data["retryable"] = True
        return RpcError(RPC_LIFECYCLE_ERROR, message, data=data)

    async def stop(self, params: IdentityParams) -> dict[str, Any]:
        """Stop the Runner from any non-terminal state."""
        async with self._lock:
            self._check_identity(params)
            endpoint_hook = self._detach_endpoint_locked()
            if self.state != RunnerState.STOPPED:
                self.state = RunnerState.STOPPED
                attempts = self._outbound.fence_attempts()
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
            response_scope = self._response_scope_for_operation_locked(
                params.to_handle,
            )
            if params.to_handle in self._response_checkpoint_pending:
                raise self._response_error(
                    "RESPONSE_BUSY",
                    "response route checkpoint is pending",
                    retryable=True,
                )
            task = asyncio.current_task()
            if task is None:
                raise RuntimeError("outbound attempt requires an asyncio task")
            try:
                attempt = self._outbound.reserve_send(
                    params,
                    task,
                    response_handle=(
                        params.to_handle if response_scope else None
                    ),
                )
            except OutboundStateError as exc:
                raise self._outbound_error_from_state(exc) from exc
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
            result = await self._prepare_runner_delivery_result(
                attempt,
                raw_result,
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
            response_scope = self._response_scope_for_operation_locked(
                params.to_handle,
            )
            if params.to_handle in self._response_checkpoint_pending:
                raise self._response_error(
                    "RESPONSE_BUSY",
                    "response route checkpoint is pending",
                    retryable=True,
                )
            task = asyncio.current_task()
            if task is None:
                raise RuntimeError("outbound attempt requires an asyncio task")
            try:
                attempt = self._outbound.reserve_reaction(
                    params,
                    task,
                    response_handle=(
                        params.to_handle if response_scope else None
                    ),
                )
            except OutboundStateError as exc:
                raise self._outbound_error_from_state(exc) from exc
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
            result = await self._prepare_runner_delivery_result(
                attempt,
                raw_result,
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

    async def _finish_outbound_attempt(
        self,
        attempt: OutboundAttempt,
        result: OutboundResult,
        *,
        send_params: SendParams | None = None,
        defer_completion: bool = False,
    ) -> OutboundResult:
        """Commit a terminal attempt result at one lifecycle boundary."""
        async with self._lock:
            await self._expire_lease_if_needed_async()
            staged = self._outbound.stage_result(
                attempt,
                result,
                send_params=send_params,
                defer_completion=defer_completion,
                lifecycle_state=self.state.value,
                now=asyncio.get_running_loop().time(),
            )
            return staged

    async def _prepare_runner_delivery_result(
        self,
        attempt: OutboundAttempt,
        raw_result: object,
    ) -> OutboundResult:
        """Persist response resources before an ACK can be published."""
        internal = (
            raw_result
            if isinstance(raw_result, RunnerDeliveryResult)
            else RunnerDeliveryResult(outbound_result=raw_result)
        )
        result = self._outbound.parse_result(
            internal.outbound_result,
            attempt.delivery_id,
        )
        if attempt.response_handle is None or not internal.resource_refs:
            return result
        response_handle = attempt.response_handle
        async with self._lock:
            try:
                snapshot = self._response_routes.record_delivery(
                    response_handle,
                    internal.resource_refs,
                )
            except ResponseStateError as exc:
                raise self._response_error_from_state(exc) from exc
            self._response_checkpoint_pending.add(response_handle)
        try:
            await self._put_response_snapshot(snapshot)
        except Exception:
            return self._outbound.unknown_result(
                attempt.delivery_id,
                "PLATFORM_RESULT_UNKNOWN",
            )
        finally:
            await self._clear_response_checkpoint_pending_resilient(
                response_handle,
            )
        return result

    async def _clear_response_checkpoint_pending_resilient(
        self,
        response_handle: str,
    ) -> None:
        """Clear checkpoint admission despite repeated cancellation."""
        cleanup = asyncio.create_task(
            self._clear_response_checkpoint_pending(response_handle),
        )
        while not cleanup.done():
            try:
                await asyncio.shield(cleanup)
            except asyncio.CancelledError:
                continue
        cleanup.result()

    async def _clear_response_checkpoint_pending(
        self,
        response_handle: str,
    ) -> None:
        """Clear one response checkpoint admission marker."""
        async with self._lock:
            self._response_checkpoint_pending.discard(response_handle)

    def _outbound_response_publication(
        self,
        attempt: OutboundAttempt,
        result: dict[str, Any],
    ) -> RpcResponsePublication:
        """Keep an outbound result retractable until its response is sent."""
        return RpcResponsePublication(
            result=result,
            on_prepare=lambda: self._prepare_outbound_publication(attempt),
            on_published=lambda: self._publish_outbound_publication(attempt),
            on_write_failed=lambda: self._rollback_outbound_publication(
                attempt,
            ),
            on_write_deferred=lambda: self._defer_outbound_publication(
                attempt,
            ),
            on_aborted=lambda reason_code: self._finish_outbound_unknown(
                attempt,
                reason_code,
            ),
        )

    async def _prepare_outbound_publication(
        self,
        attempt: OutboundAttempt,
    ) -> dict[str, Any]:
        """Choose the private result before writer visibility."""
        await self._lock.acquire()
        attempt.publication_lock_held = True
        try:
            self._expire_lease_and_revoke()
            return self._outbound.prepare_publication(
                attempt,
                lifecycle_state=self.state.value,
                now=asyncio.get_running_loop().time(),
            ).to_mapping()
        except BaseException:
            self._release_outbound_publication_lock(attempt)
            raise

    def _publish_outbound_publication(
        self,
        attempt: OutboundAttempt,
    ) -> Any:
        """Finalize one result after output accepted its complete frame."""
        if not attempt.publication_lock_held:
            if attempt.publication_result is None:
                return None
            return self._publish_deferred_outbound_publication(attempt)
        try:
            self._publish_outbound_publication_locked(attempt)
        finally:
            self._release_outbound_publication_lock(attempt)
        return None

    async def _publish_deferred_outbound_publication(
        self,
        attempt: OutboundAttempt,
    ) -> None:
        """Finalize a late accepted frame without blocking lifecycle work."""
        async with self._lock:
            self._publish_outbound_publication_locked(attempt)

    def _publish_outbound_publication_locked(
        self,
        attempt: OutboundAttempt,
    ) -> None:
        """Expose one accepted result and its ordering effects atomically."""
        self._outbound.publish(attempt)

    def _defer_outbound_publication(
        self,
        attempt: OutboundAttempt,
    ) -> None:
        """Release lifecycle work while HANDLE acceptance remains pending."""
        self._detach_deferred_outbound_attempt_locked(attempt)
        self._release_outbound_publication_lock(attempt)

    def _detach_deferred_outbound_attempt_locked(
        self,
        attempt: OutboundAttempt,
    ) -> None:
        """Stop lifecycle waits while keeping prepared ordering private."""
        self._outbound.detach_deferred(attempt)

    def _rollback_outbound_publication(
        self,
        attempt: OutboundAttempt,
    ) -> Any:
        """Retract a result when output rejected its complete frame."""
        if not attempt.publication_lock_held:
            if attempt.publication_result is None:
                return None
            return self._rollback_deferred_outbound_publication(attempt)
        self._rollback_outbound_publication_locked(attempt)
        return None

    async def _rollback_deferred_outbound_publication(
        self,
        attempt: OutboundAttempt,
    ) -> None:
        """Rollback a late rejected frame without blocking lifecycle work."""
        async with self._lock:
            self._rollback_outbound_publication_locked(attempt)

    def _rollback_outbound_publication_locked(
        self,
        attempt: OutboundAttempt,
    ) -> None:
        """Rollback one prepared publication while holding the state lock."""
        try:
            self._outbound.rollback_publication(attempt)
        finally:
            self._release_outbound_publication_lock(attempt)

    def _release_outbound_publication_lock(
        self,
        attempt: OutboundAttempt,
    ) -> None:
        """Release the lifecycle lock retained across writer.write."""
        if not attempt.publication_lock_held:
            return
        attempt.publication_lock_held = False
        self._lock.release()

    async def _finish_outbound_unknown_resilient(
        self,
        attempt: OutboundAttempt,
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
        attempt: OutboundAttempt,
        reason_code: str,
    ) -> None:
        """Retain an attempted delivery ID after an uncertain outcome."""
        async with self._lock:
            self._outbound.finish_unknown(attempt, reason_code)

    async def _wait_for_outbound_attempts(
        self,
        attempts: list[OutboundAttempt],
        deadline: float,
    ) -> list[OutboundAttempt]:
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
        attempt: OutboundAttempt,
        reason_code: str,
        *,
        cancel: bool,
    ) -> None:
        """Fence one unfinished attempt without waiting for its handler."""
        self._outbound.force_unknown(
            attempt,
            reason_code,
            cancel=cancel,
        )

    @staticmethod
    def _expire_outbound_drain(attempt: OutboundAttempt) -> None:
        """Fence and interrupt one drain cohort at its absolute deadline."""
        if attempt.done.is_set():
            return
        if attempt.forced_reason is None:
            attempt.forced_reason = "DRAIN_TIMEOUT"
        if not attempt.task.done():
            attempt.task.cancel()

    def _outbound_error_from_state(self, exc: OutboundStateError) -> RpcError:
        """Convert a synchronous outbound violation to the wire error."""
        return RpcError(
            RPC_LIFECYCLE_ERROR,
            exc.message,
            data={"reason_code": exc.reason_code},
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
            endpoint_handler = self._endpoint_handler
            result = {
                "status": {
                    "register": "registered",
                    "update": "updated",
                }[operation],
                "generation": self.generation,
                "readiness": params.readiness,
            }
        if endpoint_handler is not None:
            hook_result = endpoint_handler(operation, params)
            if hasattr(hook_result, "__await__"):
                await hook_result
        return result

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
        self._expire_lease_and_revoke()

    def _expire_lease_and_revoke(self) -> None:
        """Synchronously fence expiry at lifecycle or writer boundaries."""
        had_endpoint = self.endpoint is not None
        was_failed = self.state is RunnerState.FAILED
        self._expire_lease_if_needed()
        if self.state is RunnerState.FAILED and not was_failed:
            attempts = self._outbound.fence_attempts()
            for attempt in attempts:
                self._force_outbound_unknown_locked(
                    attempt,
                    "LEASE_EXPIRED",
                    cancel=False,
                )
            if had_endpoint:
                self._schedule_endpoint_hook(self._detach_endpoint_locked())

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
        peer.register_method(
            "channel.response.finish",
            lambda params, _: self.response_finish(params),
        )


def __getattr__(name: str) -> Any:
    """Resolve pre-refactor Core host exports without a circular import."""
    if name in {"CoreLifecycleAdapter", "HostStateStore"}:
        from . import host

        return getattr(host, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "LifecycleController",
    "CoreLifecycleAdapter",
    "FixtureSecretHandleConsumer",
    "HostStateStore",
    "ResponseOutcome",
    "RPC_AUTH_ERROR",
    "RPC_FENCING_ERROR",
    "RPC_LIFECYCLE_ERROR",
    "RPC_CAPABILITY_ERROR",
    "RunnerState",
]
