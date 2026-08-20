# -*- coding: utf-8 -*-
# pylint: disable=protected-access
"""Tests for CH-0-004 request-scoped response lifecycle."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
import inspect
import json
from typing import Any

import pytest

from qwenpaw.channel_protocol import (
    CoreLifecycleAdapter,
    HelloParams,
    JSONRPC_INVALID_PARAMS,
    LeaseParams,
    LifecycleController,
    PrepareParams,
    ResponseFinishParams,
    ResponseOutcome,
    RpcError,
    RpcPeer,
    SendParams,
)
from qwenpaw.channel_protocol.response_lifecycle import (
    ResponseCleanupState,
    ResponseResourceRef,
    ResponseRouteKind,
    ResponseRouteSnapshot,
    RunnerDeliveryResult,
)


_ENVIRONMENT_SPEC_ID = "ches1_" + "1" * 64
_ENVIRONMENT_ID = _ENVIRONMENT_SPEC_ID + ".install1_" + "2" * 32


class MemoryTransport:
    """Small in-memory full-duplex transport for response tests."""

    def __init__(self) -> None:
        self.inbox: asyncio.Queue[str | None] = asyncio.Queue()
        self.peer: MemoryTransport | None = None
        self.closed = False
        self.block_delivery_id: str | None = None
        self.response_started = asyncio.Event()
        self.release_response = asyncio.Event()

    async def send(
        self,
        message: str,
        *,
        prepare_write: Callable[[], str | Awaitable[str]] | None = None,
        on_write_succeeded: Callable[[], None] | None = None,
        on_write_failed: Callable[[], None] | None = None,
        on_write_deferred: Callable[[], None] | None = None,
    ) -> None:
        """Deliver one message to the peer."""
        _ = on_write_failed, on_write_deferred
        if self.closed or self.peer is None or self.peer.closed:
            raise ConnectionError("transport closed")
        if prepare_write is not None:
            message = prepare_write()
            if inspect.isawaitable(message):
                message = await message
        payload = json.loads(message)
        result = payload.get("result", {})
        if (
            self.block_delivery_id is not None
            and isinstance(result, dict)
            and result.get("delivery_id") == self.block_delivery_id
        ):
            self.response_started.set()
            await self.release_response.wait()
        self.peer.inbox.put_nowait(message)
        if on_write_succeeded is not None:
            on_write_succeeded()

    async def receive(self) -> str:
        """Receive one message from the peer."""
        message = await self.inbox.get()
        if message is None:
            raise ConnectionError("transport closed")
        return message

    async def aclose(self) -> None:
        """Close this side and wake its peer."""
        if self.closed:
            return
        self.closed = True
        if self.peer is not None:
            await self.peer.inbox.put(None)


class Clock:
    """Deterministic millisecond clock for response tests."""

    def __init__(self) -> None:
        self.now = 1000

    def __call__(self) -> int:
        """Return the current fake time."""
        return self.now


def _identity() -> dict[str, object]:
    """Return the fixed response-test identity."""
    return {
        "channel_key": "voice",
        "instance_id": "instance-1",
        "generation": 7,
    }


def _hello() -> HelloParams:
    """Return a compatible response-capable Runner hello."""
    return HelloParams.from_mapping(
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
            "capabilities": ["response_lifecycle"],
        },
    )


def _controller(
    clock: Clock,
    *,
    max_response_routes: int = 1024,
) -> LifecycleController:
    """Create one response-capable lifecycle controller."""
    return LifecycleController(
        channel_key="voice",
        instance_id="instance-1",
        generation=7,
        environment_spec_id=_ENVIRONMENT_SPEC_ID,
        environment_id=_ENVIRONMENT_ID,
        capabilities=("response_lifecycle",),
        max_response_routes=max_response_routes,
        clock_ms=clock,
        response_clock_ms=clock,
    )


async def _activate(controller: LifecycleController) -> None:
    """Activate one response-capable controller."""
    controller.accept_hello(_hello())
    await controller.prepare(
        PrepareParams.from_mapping(
            {
                **_identity(),
                "host_context": {},
                "capabilities": ["response_lifecycle"],
            },
        ),
    )
    lease = LeaseParams.from_mapping(
        {
            **_identity(),
            "lease_token": "response-lease",
            "lease_ttl_ms": 100,
        },
    )
    await controller.activate(lease)
    await controller.commit(lease)


def _finish(handle: str, outcome: str = "completed") -> ResponseFinishParams:
    """Build one response.finish request."""
    return ResponseFinishParams.from_mapping(
        {
            **_identity(),
            "response_handle": handle,
            "outcome": outcome,
        },
    )


@pytest.mark.asyncio
async def test_aggregate_finish_is_idempotent_and_closes_route() -> None:
    """Finish closes a route once and fences later platform calls."""
    calls: list[str] = []

    async def finish_handler(
        params: ResponseFinishParams,
        _: ResponseRouteSnapshot,
    ) -> None:
        calls.append(params.outcome.value)

    controller = _controller(Clock())
    controller._response_finish_handler = finish_handler
    await _activate(controller)
    await controller.open_response_route("response-1")
    params = _finish("response-1")
    assert (await controller.response_finish(params))["state"] == "closed"
    assert (await controller.response_finish(params))["state"] == "closed"
    assert calls == ["completed"]
    snapshot = await controller.response_route_snapshot("response-1")
    assert snapshot is not None
    assert snapshot.kind is ResponseRouteKind.TERMINAL
    with pytest.raises(RpcError) as closed:
        await controller.send(
            SendParams.from_mapping(
                {
                    **_identity(),
                    "delivery_id": "late",
                    "to_handle": "response-1",
                    "content_parts": [{"type": "text", "text": "late"}],
                },
            ),
        )
    assert closed.value.data["reason_code"] == "RESPONSE_CLOSED"


@pytest.mark.asyncio
async def test_pending_cleanup_is_not_gc_eligible() -> None:
    """Cleanup-pending receipts survive completed-receipt GC."""
    clock = Clock()
    controller = _controller(clock, max_response_routes=1)

    async def failing_handler(
        _: ResponseFinishParams,
        __: ResponseRouteSnapshot,
    ) -> None:
        raise RuntimeError("cleanup failed")

    controller._response_finish_handler = failing_handler
    await _activate(controller)
    await controller.open_response_route("pending")
    with pytest.raises(RpcError):
        await controller.response_finish(_finish("pending"))
    await controller.gc_response_routes()
    snapshot = await controller.response_route_snapshot("pending")
    assert snapshot is not None
    assert snapshot.cleanup_state is ResponseCleanupState.PENDING
    with pytest.raises(RpcError) as limited:
        await controller.open_response_route("second")
    assert limited.value.data["reason_code"] == "RESPONSE_SCOPE_LIMIT"


@pytest.mark.asyncio
async def test_complete_receipt_expires_only_after_cleanup() -> None:
    """Completed receipts are retained until their completion-based TTL."""
    clock = Clock()
    controller = _controller(clock)
    await _activate(controller)
    await controller.open_response_route("expired")
    await controller.response_finish(_finish("expired"))
    clock.now += 24 * 60 * 60 * 1000
    await controller.gc_response_routes()
    controller.lease_expires_at_ms = clock.now + 100
    assert await controller.response_route_snapshot("expired") is None

    with pytest.raises(RpcError) as unknown:
        await controller.response_finish(_finish("expired"))
    assert unknown.value.data["reason_code"] == "RESPONSE_HANDLE_UNKNOWN"


@pytest.mark.asyncio
async def test_gc_does_not_classify_expired_handle_for_generic_send() -> None:
    """Receipt GC does not create a second historical handle fence."""
    clock = Clock()
    sends: list[str] = []

    async def send_handler(params: SendParams) -> dict[str, str]:
        sends.append(params.to_handle)
        return {
            "delivery_id": params.delivery_id,
            "state": "acknowledged",
        }

    controller = _controller(clock)
    controller._send_handler = send_handler
    await _activate(controller)
    await controller.open_response_route("expired-send")
    await controller.response_finish(_finish("expired-send"))
    clock.now += 24 * 60 * 60 * 1000
    await controller.gc_response_routes()
    controller.lease_expires_at_ms = clock.now + 100
    result = await controller.send(
        SendParams.from_mapping(
            {
                **_identity(),
                "delivery_id": "post-gc",
                "to_handle": "expired-send",
                "content_parts": [{"type": "text", "text": "late"}],
            },
        ),
    )
    assert result["state"] == "acknowledged"
    assert sends == ["expired-send"]


@pytest.mark.asyncio
async def test_receipt_gc_commits_after_checkpoint_delete() -> None:
    """A failed durable delete cannot remove the in-memory closed fence."""
    clock = Clock()
    fail_delete = True

    async def checkpoint_delete(_: str, __: int) -> None:
        if fail_delete:
            raise RuntimeError("checkpoint delete unavailable")

    controller = _controller(clock)
    controller._response_checkpoint_delete = checkpoint_delete
    await _activate(controller)
    await controller.open_response_route("gc-response")
    await controller.response_finish(_finish("gc-response"))
    clock.now += 24 * 60 * 60 * 1000
    with pytest.raises(RuntimeError):
        await controller.gc_response_routes()
    assert await controller.response_route_snapshot("gc-response") is not None
    fail_delete = False
    await controller.gc_response_routes()
    assert await controller.response_route_snapshot("gc-response") is None


@pytest.mark.asyncio
async def test_duplicate_open_rechecks_checkpoint() -> None:
    """A duplicate open can confirm the same desired checkpoint again."""
    started = asyncio.Event()
    release = asyncio.Event()
    writes = 0

    async def checkpoint_put(
        _: ResponseRouteSnapshot,
        __: bool,
    ) -> None:
        nonlocal writes
        writes += 1
        started.set()
        await release.wait()

    controller = _controller(Clock())
    controller._response_checkpoint_put = checkpoint_put
    await _activate(controller)
    first = asyncio.create_task(controller.open_response_route("duplicate"))
    await asyncio.wait_for(started.wait(), timeout=1.0)
    with pytest.raises(RpcError) as busy:
        await controller.open_response_route("duplicate")
    assert busy.value.data["reason_code"] == "RESPONSE_BUSY"
    release.set()
    await first
    await controller.open_response_route("duplicate")
    assert writes == 2


@pytest.mark.asyncio
async def test_response_finish_is_busy_until_delivery_settles() -> None:
    """Finish cannot race an in-flight delivery into route closure."""
    started = asyncio.Event()
    release = asyncio.Event()

    async def send_handler(_: SendParams) -> dict[str, str]:
        started.set()
        await release.wait()
        return {"delivery_id": "busy-message", "state": "acknowledged"}

    controller = _controller(Clock())
    controller._send_handler = send_handler
    await _activate(controller)
    await controller.open_response_route("busy-response")
    send_task = asyncio.create_task(
        controller.send(
            SendParams.from_mapping(
                {
                    **_identity(),
                    "delivery_id": "busy-message",
                    "to_handle": "busy-response",
                    "content_parts": [{"type": "text", "text": "busy"}],
                },
            ),
        ),
    )
    await asyncio.wait_for(started.wait(), timeout=1.0)
    with pytest.raises(RpcError) as busy:
        await controller.response_finish(_finish("busy-response"))
    assert busy.value.data["reason_code"] == "RESPONSE_BUSY"
    release.set()
    await send_task
    assert (await controller.response_finish(_finish("busy-response")))[
        "state"
    ] == "closed"


@pytest.mark.asyncio
async def test_rpc_publication_keeps_response_finish_busy() -> None:
    """A platform ACK remains in flight until its RPC response publishes."""
    left = MemoryTransport()
    right = MemoryTransport()
    left.peer = right
    right.peer = left
    right.block_delivery_id = "publication-message"
    controller = _controller(Clock())
    core = RpcPeer(left)
    runner = RpcPeer(right)
    CoreLifecycleAdapter(controller).register_rpc_methods(core)
    controller.register_rpc_methods(runner)
    await asyncio.gather(core.start(), runner.start())
    await runner.call("runner.hello", _hello().to_mapping())
    await core.call(
        "channel.prepare",
        {
            **_identity(),
            "host_context": {},
            "capabilities": ["response_lifecycle"],
        },
    )
    lease = {**_identity(), "lease_token": "rpc", "lease_ttl_ms": 100}
    await core.call("channel.activate", lease)
    await core.call("channel.commit", lease)
    await controller.open_response_route("publication-response")
    send_task = asyncio.create_task(
        core.call(
            "channel.send",
            {
                **_identity(),
                "delivery_id": "publication-message",
                "to_handle": "publication-response",
                "content_parts": [{"type": "text", "text": "reply"}],
            },
        ),
    )
    await asyncio.wait_for(right.response_started.wait(), timeout=1.0)
    finish_task = asyncio.create_task(
        controller.response_finish(_finish("publication-response")),
    )
    await asyncio.sleep(0)
    assert not finish_task.done()
    right.release_response.set()
    assert (await send_task)["state"] == "acknowledged"
    assert (await finish_task)["state"] == "closed"
    await asyncio.gather(core.aclose(), runner.aclose())


@pytest.mark.asyncio
async def test_resource_snapshot_failure_returns_unknown_delivery() -> None:
    """A side effect cannot be acknowledged before its refs persist."""
    writes = 0

    async def checkpoint_put(
        _: ResponseRouteSnapshot,
        __: bool,
    ) -> None:
        nonlocal writes
        writes += 1
        if writes > 1:
            raise RuntimeError("checkpoint unavailable")

    async def send_handler(_: SendParams) -> RunnerDeliveryResult:
        return RunnerDeliveryResult(
            outbound_result={
                "delivery_id": "resource-send",
                "state": "acknowledged",
            },
            resource_refs=(
                ResponseResourceRef.create(
                    "feishu.delivery",
                    "resource-send",
                    {"message_id": "msg-1"},
                ),
            ),
        )

    controller = _controller(Clock())
    controller._response_checkpoint_put = checkpoint_put
    controller._send_handler = send_handler
    await _activate(controller)
    await controller.open_response_route("resource-response")
    result = await controller.send(
        SendParams.from_mapping(
            {
                **_identity(),
                "delivery_id": "resource-send",
                "to_handle": "resource-response",
                "content_parts": [{"type": "text", "text": "side effect"}],
            },
        ),
    )
    assert result["state"] == "unknown"


@pytest.mark.asyncio
async def test_pending_cleanup_restores_resources_after_failure() -> None:
    """A restart retries cleanup from the one durable pending snapshot."""
    durable: dict[str, ResponseRouteSnapshot] = {}
    fail_complete = True
    cleanup_resources: list[tuple[ResponseResourceRef, ...]] = []

    async def checkpoint_put(
        snapshot: ResponseRouteSnapshot,
        _: bool,
    ) -> None:
        if (
            fail_complete
            and snapshot.cleanup_state is ResponseCleanupState.COMPLETE
        ):
            raise RuntimeError("completion checkpoint unavailable")
        durable[snapshot.response_handle] = snapshot

    async def send_handler(_: SendParams) -> RunnerDeliveryResult:
        resource = ResponseResourceRef.create(
            "feishu.delivery",
            "cleanup-message",
            {"message_id": "message-1"},
        )
        return RunnerDeliveryResult(
            outbound_result={
                "delivery_id": "cleanup-message",
                "state": "acknowledged",
            },
            resource_refs=(resource,),
        )

    async def finish_handler(
        _: ResponseFinishParams,
        snapshot: ResponseRouteSnapshot,
    ) -> None:
        cleanup_resources.append(snapshot.resource_refs)

    first = _controller(Clock())
    first._response_checkpoint_put = checkpoint_put
    first._send_handler = send_handler
    first._response_finish_handler = finish_handler
    await _activate(first)
    await first.open_response_route("cleanup-response")
    await first.send(
        SendParams.from_mapping(
            {
                **_identity(),
                "delivery_id": "cleanup-message",
                "to_handle": "cleanup-response",
                "content_parts": [{"type": "text", "text": "reply"}],
            },
        ),
    )
    with pytest.raises(RpcError) as failed:
        await first.response_finish(_finish("cleanup-response"))
    assert failed.value.data["reason_code"] == "RESPONSE_FINISH_FAILED"
    pending = durable["cleanup-response"]
    assert pending.cleanup_state is ResponseCleanupState.PENDING

    fail_complete = False
    restored = _controller(Clock())
    restored._response_checkpoint_put = checkpoint_put
    restored._response_finish_handler = finish_handler
    await _activate(restored)
    await restored.restore_response_routes((pending,))
    await restored.resume_response_cleanups()
    completed = durable["cleanup-response"]
    assert completed.cleanup_state is ResponseCleanupState.COMPLETE
    assert cleanup_resources == [pending.resource_refs, pending.resource_refs]


@pytest.mark.asyncio
async def test_finish_task_waiter_cancel_does_not_cancel_cleanup() -> None:
    """One cancelled waiter cannot cancel a shared cleanup task."""
    started = asyncio.Event()
    release = asyncio.Event()
    calls = 0

    async def finish_handler(
        _: ResponseFinishParams,
        __: ResponseRouteSnapshot,
    ) -> None:
        nonlocal calls
        calls += 1
        started.set()
        await release.wait()

    controller = _controller(Clock())
    controller._response_finish_handler = finish_handler
    await _activate(controller)
    await controller.open_response_route("shared")
    first = asyncio.create_task(controller.response_finish(_finish("shared")))
    second = asyncio.create_task(controller.response_finish(_finish("shared")))
    await asyncio.wait_for(started.wait(), timeout=1.0)
    first.cancel()
    with pytest.raises(asyncio.CancelledError):
        await first
    release.set()
    assert (await second)["state"] == "closed"
    assert calls == 1
    assert not controller._response_finish_tasks


@pytest.mark.asyncio
async def test_response_finish_is_registered_as_reliable_rpc() -> None:
    """The Core request path validates and dispatches response.finish."""
    left = MemoryTransport()
    right = MemoryTransport()
    left.peer = right
    right.peer = left
    controller = _controller(Clock())
    core = RpcPeer(left)
    runner = RpcPeer(right)
    CoreLifecycleAdapter(controller).register_rpc_methods(core)
    controller.register_rpc_methods(runner)
    await asyncio.gather(core.start(), runner.start())
    await runner.call("runner.hello", _hello().to_mapping())
    await core.call(
        "channel.prepare",
        {
            **_identity(),
            "host_context": {},
            "capabilities": ["response_lifecycle"],
        },
    )
    lease = {**_identity(), "lease_token": "rpc", "lease_ttl_ms": 100}
    await core.call("channel.activate", lease)
    await core.call("channel.commit", lease)
    await controller.open_response_route("rpc-response")
    assert await core.call(
        "channel.response.finish",
        {
            **_identity(),
            "response_handle": "rpc-response",
            "outcome": "completed",
        },
    ) == {
        "response_handle": "rpc-response",
        "outcome": "completed",
        "state": "closed",
    }
    await asyncio.gather(core.aclose(), runner.aclose())


@pytest.mark.asyncio
async def test_response_finish_missing_identity_is_invalid_params() -> None:
    """Raw response.finish identity failures retain JSON-RPC classification."""
    controller = _controller(Clock())
    left = MemoryTransport()
    right = MemoryTransport()
    left.peer = right
    right.peer = left
    core = RpcPeer(left)
    runner = RpcPeer(right)
    CoreLifecycleAdapter(controller).register_rpc_methods(core)
    controller.register_rpc_methods(runner)
    await asyncio.gather(core.start(), runner.start())
    await runner.call("runner.hello", _hello().to_mapping())
    await core.call(
        "channel.prepare",
        {
            **_identity(),
            "host_context": {},
            "capabilities": ["response_lifecycle"],
        },
    )
    lease = {**_identity(), "lease_token": "rpc", "lease_ttl_ms": 100}
    await core.call("channel.activate", lease)
    await core.call("channel.commit", lease)
    for missing_field in ("channel_key", "instance_id", "generation"):
        params: dict[str, Any] = {
            **_identity(),
            "response_handle": "missing",
            "outcome": "completed",
        }
        params.pop(missing_field)
        with pytest.raises(RpcError) as invalid:
            await core.call("channel.response.finish", params)
        assert invalid.value.code == JSONRPC_INVALID_PARAMS
        assert invalid.value.data["path"] == [missing_field]
    await asyncio.gather(core.aclose(), runner.aclose())


def test_snapshot_is_closed_and_deterministic() -> None:
    """Aggregate snapshots reject open-ended state and sort collections."""
    ref = ResponseResourceRef.create(
        "feishu.delivery",
        "delivery-1",
        {"message_id": "message-1"},
    )
    snapshot = ResponseRouteSnapshot(
        response_handle="response-1",
        kind=ResponseRouteKind.TERMINAL,
        version=2,
        resource_refs=(ref,),
        outcome=ResponseOutcome.FAILED,
        cleanup_state=ResponseCleanupState.PENDING,
        closed_at_ms=1000,
    )
    assert (
        ResponseRouteSnapshot.from_mapping(snapshot.to_mapping()) == snapshot
    )
    with pytest.raises(ValueError):
        ResponseRouteSnapshot.from_mapping(
            {**snapshot.to_mapping(), "extra": 1},
        )
