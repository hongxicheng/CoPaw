# -*- coding: utf-8 -*-
"""Reliable event and delivery primitives for the Channel prototype."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable

from .models import (
    DeliveryState,
    DeliveryUpdateParams,
    EventBatchAck,
    EventBatchParams,
    InboundEvent,
    RejectedEvent,
)


class DeliveryStateConflictError(ValueError):
    """Report an attempted non-monotonic delivery transition."""

    reason_code = "DELIVERY_STATE_CONFLICT"

    def __init__(
        self,
        delivery_id: str,
        current: DeliveryState | None,
        next_state: DeliveryState,
    ) -> None:
        current_value = current.value if current is not None else "absent"
        super().__init__(
            f"{self.reason_code}: delivery {delivery_id} cannot transition "
            f"from {current_value} to {next_state.value}",
        )


@dataclass(frozen=True)
class RetryPolicy:
    """Bounded deterministic exponential retry policy."""

    initial_delay: float = 0.1
    max_delay: float = 5.0
    max_attempts: int = 5

    def __post_init__(self) -> None:
        """Reject policies that cannot make progress."""
        if self.initial_delay <= 0:
            raise ValueError("initial_delay must be positive")
        if self.max_delay < self.initial_delay:
            raise ValueError(
                "max_delay must not be smaller than initial_delay",
            )
        if self.max_attempts <= 0:
            raise ValueError("max_attempts must be positive")

    def delay_for(self, attempt: int) -> float:
        """Return the bounded delay before a one-based retry attempt."""
        if attempt <= 0:
            raise ValueError("attempt must be positive")
        return min(self.initial_delay * (2 ** (attempt - 1)), self.max_delay)


@dataclass
class InboundInbox:
    """Small durable-boundary stand-in for Core Inbox deduplication."""

    persisted_event_ids: set[str] = field(default_factory=set)
    admitted_event_ids: list[str] = field(default_factory=list)

    def accept_batch(self, batch: EventBatchParams) -> EventBatchAck:
        """Persist each new event before classifying it as accepted."""
        accepted: list[str] = []
        duplicates: list[str] = []
        rejected: list[RejectedEvent] = list(batch.invalid_events)
        for event in batch.events:
            if event.event_id in self.persisted_event_ids:
                duplicates.append(event.event_id)
                continue
            self.persisted_event_ids.add(event.event_id)
            self.admitted_event_ids.append(event.event_id)
            accepted.append(event.event_id)
        return EventBatchAck(
            batch_id=batch.batch_id,
            accepted_event_ids=tuple(accepted),
            duplicate_event_ids=tuple(duplicates),
            rejected_events=tuple(rejected),
        )

    def snapshot(self) -> dict[str, object]:
        """Return JSON-compatible state for a restart simulation."""
        return {
            "persisted_event_ids": sorted(self.persisted_event_ids),
            "admitted_event_ids": list(self.admitted_event_ids),
        }

    @classmethod
    def from_snapshot(cls, value: object) -> "InboundInbox":
        """Restore a restart simulation from a JSON-compatible snapshot."""
        if not isinstance(value, dict):
            raise ValueError("inbox snapshot must be an object")
        persisted = value.get("persisted_event_ids", [])
        admitted = value.get("admitted_event_ids", [])
        if (
            not isinstance(persisted, list)
            or not isinstance(admitted, list)
            or any(not isinstance(item, str) for item in persisted)
            or any(not isinstance(item, str) for item in admitted)
        ):
            raise ValueError("inbox snapshot contains invalid event IDs")
        return cls(set(persisted), list(admitted))


@dataclass
class OutboundDeliveryLedger:
    """Minimal immutable-ID ledger used by the reliability prototype."""

    states: dict[str, DeliveryState] = field(default_factory=dict)
    history: dict[str, list[DeliveryState]] = field(default_factory=dict)

    def request(self, delivery_id: str) -> DeliveryState:
        """Create a delivery in the requested state once."""
        current = self.states.get(delivery_id)
        if current is None:
            self._record(delivery_id, DeliveryState.REQUESTED)
            return DeliveryState.REQUESTED
        if current is not DeliveryState.REQUESTED:
            raise DeliveryStateConflictError(
                delivery_id,
                current,
                DeliveryState.REQUESTED,
            )
        return current

    def apply(self, update: DeliveryUpdateParams) -> DeliveryState:
        """Apply a monotonic delivery update without replacing its ID."""
        current = self.states.get(update.delivery_id)
        if current is update.state and current is not DeliveryState.REQUESTED:
            return current
        terminal_states = {
            DeliveryState.ACKNOWLEDGED,
            DeliveryState.FAILED,
            DeliveryState.TIMEOUT,
            DeliveryState.UNKNOWN,
        }
        valid_transition = (
            current is DeliveryState.REQUESTED
            and update.state
            in {
                DeliveryState.SENDING,
                *terminal_states,
            }
        ) or (
            current is DeliveryState.SENDING
            and update.state in terminal_states
        )
        if not valid_transition:
            raise DeliveryStateConflictError(
                update.delivery_id,
                current,
                update.state,
            )
        self._record(update.delivery_id, update.state)
        return update.state

    def _record(self, delivery_id: str, state: DeliveryState) -> None:
        self.states[delivery_id] = state
        self.history.setdefault(delivery_id, []).append(state)

    def snapshot(self) -> dict[str, object]:
        """Return JSON-compatible state for a restart simulation."""
        return {
            "states": {key: value.value for key, value in self.states.items()},
            "history": {
                key: [item.value for item in values]
                for key, values in self.history.items()
            },
        }

    @classmethod
    def from_snapshot(cls, value: object) -> "OutboundDeliveryLedger":
        """Restore a restart simulation from a JSON-compatible snapshot."""
        if not isinstance(value, dict):
            raise ValueError("delivery snapshot must be an object")
        raw_states = value.get("states", {})
        raw_history = value.get("history", {})
        if not isinstance(raw_states, dict) or not isinstance(
            raw_history,
            dict,
        ):
            raise ValueError("delivery snapshot must contain objects")
        states = {
            key: DeliveryState(item)
            for key, item in raw_states.items()
            if isinstance(key, str) and isinstance(item, str)
        }
        history = {
            key: [DeliveryState(item) for item in items]
            for key, items in raw_history.items()
            if isinstance(key, str) and isinstance(items, list)
        }
        if len(states) != len(raw_states) or len(history) != len(raw_history):
            raise ValueError("delivery snapshot contains invalid states")
        return cls(states=states, history=history)


def events_for_retry(
    batch: EventBatchParams,
    ack: EventBatchAck | None,
    *,
    attempt: int,
    policy: RetryPolicy,
) -> tuple[tuple[InboundEvent, ...], float | None]:
    """Return retryable events and their next backoff delay."""
    if attempt >= policy.max_attempts:
        return (), None
    if ack is None:
        return batch.events, policy.delay_for(attempt)
    if ack.batch_id is not None and ack.batch_id != batch.batch_id:
        raise ValueError("ACK batch_id does not match the submitted batch")
    rejected = {item.event_id: item.retryable for item in ack.rejected_events}
    acknowledged = set(ack.accepted_event_ids)
    acknowledged.update(ack.duplicate_event_ids)
    retry_events = tuple(
        event
        for event in batch.events
        if rejected.get(event.event_id, False)
        or (
            event.event_id not in acknowledged
            and event.event_id not in rejected
        )
    )
    if not retry_events:
        return (), None
    return retry_events, policy.delay_for(attempt)


def delivery_is_safe_to_retry(state: DeliveryState) -> bool:
    """Return whether a delivery result permits an automatic retry."""
    return state in {DeliveryState.FAILED, DeliveryState.TIMEOUT}


def delivery_updates(
    delivery_id: str,
    *,
    channel_key: str,
    instance_id: str,
    generation: int,
    states: Iterable[DeliveryState],
) -> tuple[DeliveryUpdateParams, ...]:
    """Build a deterministic sequence of ledger updates for tests."""
    return tuple(
        DeliveryUpdateParams(
            channel_key=channel_key,
            instance_id=instance_id,
            generation=generation,
            delivery_id=delivery_id,
            state=state,
        )
        for state in states
    )


__all__ = [
    "DeliveryStateConflictError",
    "InboundInbox",
    "OutboundDeliveryLedger",
    "RetryPolicy",
    "delivery_is_safe_to_retry",
    "delivery_updates",
    "events_for_retry",
]
