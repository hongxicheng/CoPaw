# -*- coding: utf-8 -*-
"""Tests for CH-0-005 reliable events, ACKs, and delivery semantics."""

from __future__ import annotations

import asyncio
import json

import pytest

from qwenpaw.channel_protocol import (
    CoreLifecycleAdapter,
    DeliveryState,
    DeliveryUpdateParams,
    EventBatchAck,
    EventBatchParams,
    HelloParams,
    InboundInbox,
    InboundEvent,
    LifecycleController,
    LeaseParams,
    OutboundDeliveryLedger,
    PrepareParams,
    ProtocolValidationError,
    RetryPolicy,
    RpcLimits,
    RpcPeer,
    RpcTimeoutError,
    delivery_is_safe_to_retry,
    events_for_retry,
)


_ENVIRONMENT_SPEC_ID = f"ches1_{'1' * 64}"
_ENVIRONMENT_ID = f"{_ENVIRONMENT_SPEC_ID}.install1_{'2' * 32}"


class MemoryTransport:
    """Minimal in-memory transport for reliability RPC tests."""

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


class DropFirstResponseTransport(MemoryTransport):
    """Discard the first JSON-RPC response after Core handles a request."""

    def __init__(self) -> None:
        super().__init__()
        self.dropped_response = False

    async def send(self, message: str) -> None:
        """Drop one response while still reporting a successful write."""
        value = json.loads(message)
        is_response = "id" in value and "method" not in value
        if is_response and not self.dropped_response:
            self.dropped_response = True
            return
        await super().send(message)


async def _active_rpc_pair(
    *,
    core_transport: MemoryTransport | None = None,
    inbound_inbox: InboundInbox | None = None,
    runner_limits: RpcLimits | None = None,
) -> tuple[CoreLifecycleAdapter, RpcPeer, RpcPeer]:
    """Create an active mock Core and Runner pair."""
    left = core_transport or MemoryTransport()
    right = MemoryTransport()
    left.peer = right
    right.peer = left
    controller = LifecycleController(
        channel_key="voice",
        instance_id="instance-1",
        environment_spec_id=_ENVIRONMENT_SPEC_ID,
        environment_id=_ENVIRONMENT_ID,
        generation=7,
        capabilities=(),
    )
    adapter = CoreLifecycleAdapter(
        controller,
        inbound_inbox=inbound_inbox,
    )
    core = RpcPeer(left)
    runner = RpcPeer(right, limits=runner_limits)
    adapter.register_rpc_methods(core)
    controller.accept_hello(
        HelloParams.from_mapping(
            {
                "protocol_min": 1,
                "protocol_max": 1,
                "qwenpaw_version": "0.1",
                "channel_key": "voice",
                "instance_id": "instance-1",
                "environment_spec_id": _ENVIRONMENT_SPEC_ID,
                "environment_id": _ENVIRONMENT_ID,
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
    return adapter, core, runner


def _event(event_id: str = "event-1") -> dict[str, object]:
    """Return a stable inbound event fixture."""
    return {
        "event_id": event_id,
        "channel_key": "voice",
        "instance_id": "instance-1",
        "generation": 7,
        "event_kind": "message.query",
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


@pytest.mark.parametrize(
    "field,value",
    [
        ("generation", 6),
        ("channel_key", "other"),
        ("instance_id", "other-instance"),
    ],
)
def test_event_batch_rejects_mixed_identity(
    field: str,
    value: object,
) -> None:
    """A batch cannot mix generation or instance identity."""
    second = {**_event("event-2"), field: value}
    with pytest.raises(ProtocolValidationError):
        EventBatchParams.from_mapping(
            {
                "batch_id": "batch-1",
                "events": [_event("event-1"), second],
            },
        )


def test_event_kind_is_required_and_round_trips() -> None:
    """Every event carries a stable, non-empty kind."""
    event = _event()
    assert (
        EventBatchParams.from_mapping(
            {"batch_id": "batch-1", "events": [event]},
        )
        .events[0]
        .to_mapping()["event_kind"]
        == "message.query"
    )
    for value in (None, ""):
        invalid = event.copy()
        invalid["event_kind"] = value
        with pytest.raises(ProtocolValidationError):
            InboundEvent.from_mapping(invalid)


def test_event_batch_rejects_duplicate_event_ids() -> None:
    """A legal and malformed duplicate ID cannot form one ACK."""
    duplicate = {**_event("event-1"), "content_parts": "invalid"}
    for events in (
        [_event("event-1"), _event("event-1")],
        [_event("event-1"), duplicate],
    ):
        with pytest.raises(ProtocolValidationError):
            EventBatchParams.from_mapping(
                {"batch_id": "batch-1", "events": events},
            )


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
    _, core, runner = await _active_rpc_pair()
    first = await runner.call("event.batch", _batch("event-1").to_mapping())
    second = await runner.call("event.batch", _batch("event-1").to_mapping())
    assert first["accepted_event_ids"] == ["event-1"]
    assert second["duplicate_event_ids"] == ["event-1"]
    await asyncio.gather(core.aclose(), runner.aclose())


@pytest.mark.asyncio
async def test_lost_ack_and_core_runner_restart_are_idempotent() -> None:
    """A lost ACK survives both peer restarts without double admission."""
    dropping_transport = DropFirstResponseTransport()
    adapter, core, runner = await _active_rpc_pair(
        core_transport=dropping_transport,
        runner_limits=RpcLimits(request_timeout=0.01),
    )
    batch = _batch("event-1")
    with pytest.raises(RpcTimeoutError):
        await runner.call("event.batch", batch.to_mapping())
    assert dropping_transport.dropped_response
    assert adapter.inbound_inbox.admitted_event_ids == ["event-1"]
    snapshot = adapter.inbound_inbox.snapshot()
    await asyncio.gather(core.aclose(), runner.aclose())

    restored = InboundInbox.from_snapshot(snapshot)
    new_adapter, new_core, new_runner = await _active_rpc_pair(
        inbound_inbox=restored,
    )
    retried = await new_runner.call("event.batch", batch.to_mapping())
    assert retried["batch_id"] == batch.batch_id
    assert retried["duplicate_event_ids"] == ["event-1"]
    assert new_adapter.inbound_inbox.admitted_event_ids == ["event-1"]
    await asyncio.gather(new_core.aclose(), new_runner.aclose())
