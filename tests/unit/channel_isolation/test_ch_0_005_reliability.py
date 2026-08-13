# -*- coding: utf-8 -*-
"""Tests for CH-0-005 reliable events, ACKs, and delivery semantics."""

from __future__ import annotations

import asyncio

import pytest

from qwenpaw.channel_protocol import (
    CoreLifecycleAdapter,
    DeliveryState,
    DeliveryUpdateParams,
    EventBatchAck,
    EventBatchParams,
    HelloParams,
    InboundInbox,
    LifecycleController,
    LeaseParams,
    OutboundDeliveryLedger,
    PrepareParams,
    ProtocolValidationError,
    RetryPolicy,
    RpcPeer,
    delivery_is_safe_to_retry,
    events_for_retry,
)


def _event(event_id: str = "event-1") -> dict[str, object]:
    """Return a stable inbound event fixture."""
    return {
        "event_id": event_id,
        "channel_key": "voice",
        "instance_id": "instance-1",
        "generation": 7,
        "conversation": {"id": "chat-1", "type": "dm", "thread_id": None},
        "sender_id": "display-id",
        "acl_sender_id": "stable-id",
        "sender_name": "Alice",
        "content_parts": [{"type": "text", "text": "hello"}],
        "metadata": {},
    }


def _batch(*event_ids: str) -> EventBatchParams:
    """Return a batch containing the requested event IDs."""
    return EventBatchParams.from_mapping(
        {
            "batch_id": "generation-7-batch-1",
            "events": [_event(event_id) for event_id in event_ids],
        },
    )


def test_event_batch_and_ack_are_closed_and_round_trip() -> None:
    """Event batches and ACKs retain stable per-event classifications."""
    batch = _batch("event-1", "event-2")
    assert batch.to_mapping()["batch_id"] == "generation-7-batch-1"
    ack = EventBatchAck.from_mapping(
        {
            "batch_id": batch.batch_id,
            "accepted_event_ids": ["event-1"],
            "duplicate_event_ids": ["event-2"],
            "rejected_events": [
                {
                    "event_id": "event-3",
                    "reason_code": "TEMPORARY_UNAVAILABLE",
                    "retryable": True,
                },
            ],
        },
    )
    assert ack.rejected_events[0].retryable is True
    with pytest.raises(ProtocolValidationError):
        EventBatchAck.from_mapping(
            {
                "accepted_event_ids": [],
                "duplicate_event_ids": [],
                "rejected_events": [],
                "extra": True,
            },
        )


def test_malformed_event_does_not_poison_valid_events() -> None:
    """A malformed event becomes a permanent per-event rejection."""
    batch = EventBatchParams.from_mapping(
        {
            "batch_id": "batch-1",
            "events": [
                _event("valid"),
                {**_event("invalid"), "content_parts": "not-an-array"},
            ],
        },
    )
    assert [event.event_id for event in batch.events] == ["valid"]
    assert batch.invalid_events[0].event_id == "invalid"
    assert batch.invalid_events[0].retryable is False


def test_all_invalid_events_can_return_rejected_ack() -> None:
    """A batch with stable identity can reject every event individually."""
    batch = EventBatchParams.from_mapping(
        {
            "batch_id": "batch-1",
            "events": [
                {**_event("invalid"), "content_parts": "not-an-array"},
            ],
        },
    )
    ack = InboundInbox().accept_batch(batch)
    assert not ack.accepted_event_ids
    assert ack.rejected_events[0].event_id == "invalid"


def test_inbox_persists_before_ack_and_deduplicates_after_restart() -> None:
    """Duplicate retries do not admit the same event twice."""
    inbox = InboundInbox()
    first = inbox.accept_batch(_batch("event-1"))
    assert first.accepted_event_ids == ("event-1",)
    restored = InboundInbox.from_snapshot(inbox.snapshot())
    duplicate = restored.accept_batch(_batch("event-1"))
    assert duplicate.duplicate_event_ids == ("event-1",)
    assert restored.admitted_event_ids == ["event-1"]


def test_retry_policy_retries_unacknowledged_and_retryable_events() -> None:
    """ACK loss and retryable rejection share the same batch ID."""
    policy = RetryPolicy(initial_delay=0.25, max_delay=0.75, max_attempts=4)
    batch = _batch("accepted", "retry", "permanent", "missing")
    ack = EventBatchAck.from_mapping(
        {
            "batch_id": batch.batch_id,
            "accepted_event_ids": ["accepted"],
            "duplicate_event_ids": [],
            "rejected_events": [
                {
                    "event_id": "retry",
                    "reason_code": "TEMPORARY_UNAVAILABLE",
                    "retryable": True,
                },
                {
                    "event_id": "permanent",
                    "reason_code": "CONFIG_INVALID",
                    "retryable": False,
                },
            ],
        },
    )
    retry, delay = events_for_retry(batch, ack, attempt=1, policy=policy)
    assert [event.event_id for event in retry] == ["retry", "missing"]
    assert delay == 0.25
    lost_ack, lost_delay = events_for_retry(
        batch,
        None,
        attempt=2,
        policy=policy,
    )
    assert [event.event_id for event in lost_ack] == [
        "accepted",
        "retry",
        "permanent",
        "missing",
    ]
    assert lost_delay == 0.5
    assert events_for_retry(
        batch,
        ack,
        attempt=4,
        policy=policy,
    ) == ((), None)


def test_delivery_ledger_preserves_unknown_and_restart_state() -> None:
    """Unknown results are retained and never implicitly acknowledged."""
    ledger = OutboundDeliveryLedger()
    assert ledger.request("delivery-1") is DeliveryState.REQUESTED
    update = DeliveryUpdateParams.from_mapping(
        {
            "channel_key": "voice",
            "instance_id": "instance-1",
            "generation": 7,
            "delivery_id": "delivery-1",
            "state": "unknown",
            "retryable": False,
        },
    )
    assert ledger.apply(update) is DeliveryState.UNKNOWN
    restored = OutboundDeliveryLedger.from_snapshot(ledger.snapshot())
    late_ack = DeliveryUpdateParams(
        channel_key="voice",
        instance_id="instance-1",
        generation=7,
        delivery_id="delivery-1",
        state=DeliveryState.ACKNOWLEDGED,
    )
    assert restored.apply(late_ack) is DeliveryState.UNKNOWN
    assert not delivery_is_safe_to_retry(DeliveryState.UNKNOWN)
    assert delivery_is_safe_to_retry(DeliveryState.TIMEOUT)
    retry_update = DeliveryUpdateParams(
        channel_key="voice",
        instance_id="instance-1",
        generation=7,
        delivery_id="delivery-2",
        state=DeliveryState.FAILED,
    )
    ledger.request("delivery-2")
    assert ledger.apply(retry_update) is DeliveryState.FAILED
    assert (
        ledger.apply(
            DeliveryUpdateParams(
                channel_key="voice",
                instance_id="instance-1",
                generation=7,
                delivery_id="delivery-2",
                state=DeliveryState.SENDING,
            ),
        )
        is DeliveryState.SENDING
    )


@pytest.mark.asyncio
async def test_event_batch_rpc_acks_after_core_inbox_admission() -> None:
    """A mock Core returns per-event ACKs only from its active generation."""

    class MemoryTransport:
        """Minimal in-memory transport for the RPC smoke test."""

        def __init__(self) -> None:
            self.inbox: asyncio.Queue[str | None] = asyncio.Queue()
            self.peer: MemoryTransport | None = None
            self.closed = False

        async def send(self, message: str) -> None:
            """Deliver one message to the peer."""
            if self.closed or self.peer is None or self.peer.closed:
                raise ConnectionError("transport closed")
            await self.peer.inbox.put(message)

        async def receive(self) -> str:
            """Receive one message from the peer."""
            message = await self.inbox.get()
            if message is None:
                raise ConnectionError("transport closed")
            return message

        async def aclose(self) -> None:
            """Close the transport and wake the peer."""
            if not self.closed:
                self.closed = True
                if self.peer is not None:
                    await self.peer.inbox.put(None)

    left = MemoryTransport()
    right = MemoryTransport()
    left.peer = right
    right.peer = left
    controller = LifecycleController(
        channel_key="voice",
        instance_id="instance-1",
        environment_spec_id="ches1_" + "1" * 64,
        environment_id="ches1_" + "1" * 64 + ".install1_" + "2" * 32,
        generation=7,
        capabilities=(),
    )
    adapter = CoreLifecycleAdapter(controller)
    core = RpcPeer(left)
    runner = RpcPeer(right)
    adapter.register_rpc_methods(core)
    controller.accept_hello(
        HelloParams.from_mapping(
            {
                "protocol_min": 1,
                "protocol_max": 1,
                "qwenpaw_version": "0.1",
                "channel_key": "voice",
                "instance_id": "instance-1",
                "environment_spec_id": "ches1_" + "1" * 64,
                "environment_id": "ches1_"
                + "1" * 64
                + ".install1_"
                + "2" * 32,
                "lock_sha256": "0" * 64,
                "python_abi": "cp313-cp313",
                "platform_tag": "macosx_11_0_arm64",
                "capabilities": [],
            },
        ),
    )
    await controller.prepare(
        PrepareParams.from_mapping(
            {
                "channel_key": "voice",
                "instance_id": "instance-1",
                "generation": 7,
                "host_context": {},
                "capabilities": [],
            },
        ),
    )
    lease = LeaseParams(
        channel_key="voice",
        instance_id="instance-1",
        generation=7,
        lease_token="token",
        lease_ttl_ms=1000,
    )
    await controller.activate(lease)
    await controller.commit(lease)
    await asyncio.gather(core.start(), runner.start())
    first = await runner.call("event.batch", _batch("event-1").to_mapping())
    second = await runner.call("event.batch", _batch("event-1").to_mapping())
    assert first["accepted_event_ids"] == ["event-1"]
    assert second["duplicate_event_ids"] == ["event-1"]
    await asyncio.gather(core.aclose(), runner.aclose())
