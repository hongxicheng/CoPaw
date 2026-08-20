# -*- coding: utf-8 -*-
"""Synchronous outbound delivery and publication state."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any

from .errors import ProtocolValidationError
from .models import (
    DeliveryState,
    OutboundOperation,
    OutboundResult,
    ReactionParams,
    SendParams,
    StreamType,
)


@dataclass(frozen=True)
class OutboundStateError(Exception):
    """Describe one stable outbound ordering violation."""

    reason_code: str
    message: str


@dataclass(frozen=True)
class OutboundTargetSnapshot:
    """Return immutable target ordering state."""

    delivery_id: str
    operation: OutboundOperation
    to_handle: str
    stream_type: StreamType | None
    sequence: int | None
    ended: bool
    pending_delivery_id: str | None


@dataclass(frozen=True)
class OutboundAttemptSnapshot:
    """Return immutable in-flight publication state."""

    delivery_id: str
    forced_reason: str | None
    drain_deadline: float | None
    provisional: bool
    publication_prepared: bool
    publication_lock_held: bool


@dataclass
class OutboundAttempt:
    """Track one immutable platform-side attempt across lifecycle changes."""

    delivery_id: str
    epoch: int
    task: asyncio.Task[Any]
    target: "_OutboundTarget | None" = None
    forced_reason: str | None = None
    drain_deadline: float | None = None
    terminal_result: OutboundResult | None = None
    send_params: SendParams | None = None
    provisional: bool = False
    publication_result: OutboundResult | None = None
    publication_send_params: SendParams | None = None
    publication_lock_held: bool = False
    response_handle: str | None = None
    lifecycle_detached: bool = False
    drain_timer: asyncio.TimerHandle | None = None
    done: asyncio.Event = field(default_factory=asyncio.Event)


@dataclass
class _OutboundTarget:
    """Retain one acknowledged outbound ordering target."""

    delivery_id: str
    operation: OutboundOperation
    to_handle: str
    stream_type: StreamType | None
    sequence: int | None
    ended: bool = False
    pending_delivery_id: str | None = None


class OutboundDeliveryState:
    """Apply delivery, target, and publication transitions synchronously."""

    def __init__(self) -> None:
        self._delivery_states: dict[str, DeliveryState] = {}
        self._targets: dict[str, _OutboundTarget] = {}
        self._attempts: dict[str, OutboundAttempt] = {}
        self._epoch = 0

    def reserve_send(
        self,
        params: SendParams,
        task: asyncio.Task[Any],
        *,
        response_handle: str | None,
    ) -> OutboundAttempt:
        """Validate ordering and reserve one send delivery ID."""
        if params.delivery_id in self._delivery_states:
            raise OutboundStateError(
                "OUTBOUND_ORDER_VIOLATION",
                "duplicate delivery_id",
            )
        target = self._target_for_send(params)
        return self._reserve(
            params.delivery_id,
            task,
            target=target,
            response_handle=response_handle,
        )

    def reserve_reaction(
        self,
        params: ReactionParams,
        task: asyncio.Task[Any],
        *,
        response_handle: str | None,
    ) -> OutboundAttempt:
        """Validate the reaction target and reserve its delivery ID."""
        if params.delivery_id in self._delivery_states:
            raise OutboundStateError(
                "OUTBOUND_ORDER_VIOLATION",
                "duplicate delivery_id",
            )
        target = self._targets.get(params.target_delivery_id)
        if target is None:
            raise OutboundStateError(
                "OUTBOUND_TARGET_UNKNOWN",
                "outbound target is unknown",
            )
        if target.pending_delivery_id is not None:
            raise OutboundStateError(
                "OUTBOUND_ORDER_VIOLATION",
                "outbound target is busy",
            )
        if (
            target.operation
            not in {
                OutboundOperation.MESSAGE_CREATE,
                OutboundOperation.STREAM_START,
            }
            or target.to_handle != params.to_handle
        ):
            raise OutboundStateError(
                "OUTBOUND_ORDER_VIOLATION",
                "invalid reaction target",
            )
        return self._reserve(
            params.delivery_id,
            task,
            response_handle=response_handle,
        )

    def stage_result(
        self,
        attempt: OutboundAttempt,
        result: OutboundResult,
        *,
        send_params: SendParams | None,
        defer_completion: bool,
        lifecycle_state: str,
        now: float,
    ) -> OutboundResult:
        """Fence and stage or commit one platform result."""
        result = self.fence_result(
            attempt,
            result,
            lifecycle_state=lifecycle_state,
            now=now,
        )
        if defer_completion:
            attempt.terminal_result = result
            attempt.send_params = send_params
            attempt.provisional = True
            return result
        self.commit(attempt, result, send_params)
        return result

    def prepare_publication(
        self,
        attempt: OutboundAttempt,
        *,
        lifecycle_state: str,
        now: float,
    ) -> OutboundResult:
        """Choose the private result immediately before writer visibility."""
        if self._attempts.get(attempt.delivery_id) is not attempt:
            return self.unknown_result(
                attempt.delivery_id,
                attempt.forced_reason or "LIFECYCLE_FENCED",
            )
        result = attempt.terminal_result
        if result is None:
            result = self.unknown_result(
                attempt.delivery_id,
                attempt.forced_reason or "PLATFORM_RESULT_UNKNOWN",
            )
        result = self.fence_result(
            attempt,
            result,
            lifecycle_state=lifecycle_state,
            now=now,
        )
        attempt.publication_result = result
        attempt.publication_send_params = attempt.send_params
        attempt.provisional = False
        attempt.terminal_result = None
        attempt.send_params = None
        return result

    def publish(self, attempt: OutboundAttempt) -> bool:
        """Commit one accepted response publication."""
        result = attempt.publication_result
        if result is None:
            return False
        send_params = attempt.publication_send_params
        self._delivery_states[attempt.delivery_id] = result.state
        if (
            send_params is not None
            and result.state is DeliveryState.ACKNOWLEDGED
        ):
            self._record_send(send_params, attempt.target)
        self._clear_publication(attempt)
        self._complete(attempt)
        return True

    def detach_deferred(self, attempt: OutboundAttempt) -> bool:
        """Stop lifecycle waits while prepared ordering remains private."""
        if self._attempts.get(attempt.delivery_id) is not attempt:
            return False
        attempt.lifecycle_detached = True
        self._clear_timer(attempt)
        attempt.done.set()
        return True

    def commit(
        self,
        attempt: OutboundAttempt,
        result: OutboundResult,
        send_params: SendParams | None,
    ) -> None:
        """Make one terminal result and ordering effects visible."""
        self._delivery_states[attempt.delivery_id] = result.state
        if (
            send_params is not None
            and result.state is DeliveryState.ACKNOWLEDGED
        ):
            self._record_send(send_params, attempt.target)
        attempt.provisional = False
        attempt.terminal_result = None
        attempt.send_params = None
        self._complete(attempt)

    def rollback_publication(self, attempt: OutboundAttempt) -> bool:
        """Rollback one response frame rejected before acceptance."""
        if attempt.publication_result is None:
            return False
        self._delivery_states[attempt.delivery_id] = DeliveryState.UNKNOWN
        self._clear_publication(attempt)
        self._complete(attempt)
        return True

    @staticmethod
    def _clear_publication(attempt: OutboundAttempt) -> None:
        """Discard one attempt-private prepared publication."""
        attempt.publication_result = None
        attempt.publication_send_params = None

    def fence_result(
        self,
        attempt: OutboundAttempt,
        result: OutboundResult,
        *,
        lifecycle_state: str,
        now: float,
    ) -> OutboundResult:
        """Apply lifecycle, drain, and epoch fencing before commit."""
        if attempt.forced_reason is not None:
            return self.unknown_result(
                attempt.delivery_id,
                attempt.forced_reason,
            )
        if (
            attempt.drain_deadline is not None
            and now >= attempt.drain_deadline
        ):
            return self.unknown_result(
                attempt.delivery_id,
                "DRAIN_TIMEOUT",
            )
        if lifecycle_state not in {"active", "quiescing"}:
            return self.unknown_result(
                attempt.delivery_id,
                "LIFECYCLE_FENCED",
            )
        if lifecycle_state == "active" and attempt.epoch != self._epoch:
            return self.unknown_result(
                attempt.delivery_id,
                "LIFECYCLE_FENCED",
            )
        return result

    def finish_unknown(
        self,
        attempt: OutboundAttempt,
        reason_code: str,
    ) -> bool:
        """Retain an attempted delivery ID after an uncertain outcome."""
        if self._attempts.get(attempt.delivery_id) is not attempt:
            return False
        self._delivery_states[attempt.delivery_id] = DeliveryState.UNKNOWN
        if attempt.forced_reason is None:
            attempt.forced_reason = reason_code
        self._complete(attempt)
        return True

    def fence_attempts(self) -> list[OutboundAttempt]:
        """Close the current admission epoch and snapshot in-flight work."""
        self._epoch += 1
        return [
            attempt
            for attempt in self._attempts.values()
            if not attempt.lifecycle_detached
        ]

    def force_unknown(
        self,
        attempt: OutboundAttempt,
        reason_code: str,
        *,
        cancel: bool,
    ) -> bool:
        """Fence one unfinished attempt without waiting for its handler."""
        if self._attempts.get(attempt.delivery_id) is not attempt:
            return False
        was_provisional = attempt.provisional
        attempt.forced_reason = reason_code
        attempt.provisional = False
        attempt.terminal_result = None
        attempt.send_params = None
        self._delivery_states[attempt.delivery_id] = DeliveryState.UNKNOWN
        self._complete(attempt)
        if (cancel or was_provisional) and not attempt.task.done():
            attempt.task.cancel()
        return True

    def _complete(self, attempt: OutboundAttempt) -> None:
        """Release in-flight target and timer state."""
        if (
            attempt.target is not None
            and attempt.target.pending_delivery_id == attempt.delivery_id
        ):
            attempt.target.pending_delivery_id = None
        self._attempts.pop(attempt.delivery_id, None)
        self._clear_timer(attempt)
        attempt.done.set()

    def delivery_state(self, delivery_id: str) -> DeliveryState | None:
        """Return one immutable delivery state value."""
        return self._delivery_states.get(delivery_id)

    def target_snapshot(
        self,
        delivery_id: str,
    ) -> OutboundTargetSnapshot | None:
        """Return an immutable copy of one target."""
        target = self._targets.get(delivery_id)
        if target is None:
            return None
        return OutboundTargetSnapshot(
            delivery_id=target.delivery_id,
            operation=target.operation,
            to_handle=target.to_handle,
            stream_type=target.stream_type,
            sequence=target.sequence,
            ended=target.ended,
            pending_delivery_id=target.pending_delivery_id,
        )

    def attempt_snapshot(
        self,
        delivery_id: str,
    ) -> OutboundAttemptSnapshot | None:
        """Return an immutable copy of one in-flight attempt."""
        attempt = self._attempts.get(delivery_id)
        if attempt is None:
            return None
        return OutboundAttemptSnapshot(
            delivery_id=attempt.delivery_id,
            forced_reason=attempt.forced_reason,
            drain_deadline=attempt.drain_deadline,
            provisional=attempt.provisional,
            publication_prepared=attempt.publication_result is not None,
            publication_lock_held=attempt.publication_lock_held,
        )

    def set_drain_deadline(
        self,
        delivery_id: str,
        deadline: float,
    ) -> bool:
        """Set the absolute drain fence for one tracked attempt."""
        attempt = self._attempts.get(delivery_id)
        if attempt is None:
            return False
        attempt.drain_deadline = deadline
        return True

    def inflight_attempts(self) -> tuple[OutboundAttempt, ...]:
        """Return a stable tuple of currently tracked attempts."""
        return tuple(
            attempt
            for attempt in self._attempts.values()
            if not attempt.lifecycle_detached
        )

    def has_inflight_response(self, response_handle: str) -> bool:
        """Return whether delivery/publication still uses one response."""
        return any(
            attempt.response_handle == response_handle
            for attempt in self._attempts.values()
        )

    @staticmethod
    def parse_result(value: object, delivery_id: str) -> OutboundResult:
        """Validate a handler result and bind it to the request ID."""
        result = OutboundResult.from_mapping(value)
        if result.delivery_id != delivery_id:
            raise ProtocolValidationError(
                "outbound result delivery_id does not match request",
                path=("delivery_id",),
                reason_code="SCHEMA_MISMATCH",
            )
        return result

    @staticmethod
    def unknown_result(
        delivery_id: str,
        reason_code: str,
    ) -> OutboundResult:
        """Create one stable unknown attempt result."""
        return OutboundResult(
            delivery_id=delivery_id,
            state=DeliveryState.UNKNOWN,
            reason_code=reason_code,
        )

    def _target_for_send(self, params: SendParams) -> _OutboundTarget | None:
        """Validate stream target ordering for one send operation."""
        if params.target_delivery_id is None:
            return None
        target = self._targets.get(params.target_delivery_id)
        if target is None:
            raise OutboundStateError(
                "OUTBOUND_TARGET_UNKNOWN",
                "outbound target is unknown",
            )
        if target.pending_delivery_id is not None:
            raise OutboundStateError(
                "OUTBOUND_ORDER_VIOLATION",
                "outbound target is busy",
            )
        if (
            target.operation is not OutboundOperation.STREAM_START
            or target.to_handle != params.to_handle
            or target.ended
        ):
            raise OutboundStateError(
                "OUTBOUND_ORDER_VIOLATION",
                "invalid outbound target",
            )
        if target.sequence is None:
            raise RuntimeError("stream target requires a sequence")
        if params.sequence != target.sequence + 1:
            raise OutboundStateError(
                "OUTBOUND_ORDER_VIOLATION",
                "non-contiguous sequence",
            )
        if (
            params.stream_type is not None
            and params.stream_type is not target.stream_type
        ):
            raise OutboundStateError(
                "OUTBOUND_ORDER_VIOLATION",
                "stream type mismatch",
            )
        return target

    def _reserve(
        self,
        delivery_id: str,
        task: asyncio.Task[Any],
        *,
        target: _OutboundTarget | None = None,
        response_handle: str | None = None,
    ) -> OutboundAttempt:
        """Occupy one delivery ID before platform side effects start."""
        attempt = OutboundAttempt(
            delivery_id=delivery_id,
            epoch=self._epoch,
            task=task,
            target=target,
            response_handle=response_handle,
        )
        self._delivery_states[delivery_id] = DeliveryState.SENDING
        self._attempts[delivery_id] = attempt
        if target is not None:
            target.pending_delivery_id = delivery_id
        return attempt

    def _record_send(
        self,
        params: SendParams,
        target: _OutboundTarget | None,
    ) -> None:
        """Record one successfully acknowledged outbound operation."""
        if params.operation in {
            OutboundOperation.MESSAGE_CREATE,
            OutboundOperation.STREAM_START,
        }:
            self._targets[params.delivery_id] = _OutboundTarget(
                delivery_id=params.delivery_id,
                operation=params.operation,
                to_handle=params.to_handle,
                stream_type=params.stream_type,
                sequence=params.sequence,
            )
        if target is not None:
            target.sequence = params.sequence
            if params.operation is OutboundOperation.STREAM_END:
                target.ended = True

    @staticmethod
    def _clear_timer(attempt: OutboundAttempt) -> None:
        """Cancel and remove one absolute drain timer."""
        if attempt.drain_timer is not None:
            attempt.drain_timer.cancel()
            attempt.drain_timer = None


__all__ = [
    "OutboundAttempt",
    "OutboundAttemptSnapshot",
    "OutboundDeliveryState",
    "OutboundStateError",
    "OutboundTargetSnapshot",
]
