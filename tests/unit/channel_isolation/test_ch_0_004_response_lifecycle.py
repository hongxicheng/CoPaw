# -*- coding: utf-8 -*-
"""Tests for CH-0-004 request-scoped response lifecycle."""

# pylint: disable=protected-access

from __future__ import annotations

import asyncio
import inspect
from collections.abc import Awaitable, Callable
from typing import Any

import pytest

from qwenpaw.channel_protocol import (
    CoreLifecycleAdapter,
    HelloParams,
    LeaseParams,
    LifecycleController,
    PrepareParams,
    ReactionParams,
    ResponseFinishParams,
    ResponseOutcome,
    RpcError,
    RpcPeer,
    SendParams,
)


_ENVIRONMENT_SPEC_ID = "ches1_" + "1" * 64
_ENVIRONMENT_ID = _ENVIRONMENT_SPEC_ID + ".install1_" + "2" * 32


class MemoryTransport:
    """Small in-memory full-duplex transport for response tests."""

    def __init__(self) -> None:
        self.inbox: asyncio.Queue[str | None] = asyncio.Queue()
        self.peer: MemoryTransport | None = None
        self.closed = False

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
            "capabilities": ["reaction", "response_lifecycle"],
        },
    )


def _controller(
    clock: Clock,
    *,
    max_response_scopes: int = 1024,
) -> LifecycleController:
    """Create one response-capable lifecycle controller."""
    return LifecycleController(
        channel_key="voice",
        instance_id="instance-1",
        generation=7,
        environment_spec_id=_ENVIRONMENT_SPEC_ID,
        environment_id=_ENVIRONMENT_ID,
        capabilities=("reaction", "response_lifecycle"),
        max_response_scopes=max_response_scopes,
        clock_ms=clock,
    )


async def _activate(
    controller: LifecycleController,
    capabilities: list[str],
) -> None:
    """Activate one controller with the requested capability subset."""
    controller.accept_hello(_hello())
    await controller.prepare(
        PrepareParams.from_mapping(
            {
                **_identity(),
                "host_context": {},
                "capabilities": capabilities,
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


async def _active_rpc_pair() -> tuple[LifecycleController, RpcPeer, RpcPeer]:
    """Create an active Core and Runner pair for response RPC tests."""
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
    lease = LeaseParams.from_mapping(
        {
            **_identity(),
            "lease_token": "response-rpc",
            "lease_ttl_ms": 100,
        },
    )
    await core.call("channel.activate", lease.to_mapping())
    await core.call("channel.commit", lease.to_mapping())
    return controller, core, runner


@pytest.mark.asyncio
async def test_response_finish_is_idempotent_and_closes_scope() -> None:
    """Finish closes a route once and prevents later platform calls."""
    calls: list[str] = []

    async def finish_handler(params: ResponseFinishParams) -> None:
        calls.append(params.outcome.value)

    controller = _controller(Clock())
    controller._response_finish_handler = finish_handler
    await _activate(controller, ["reaction", "response_lifecycle"])
    await controller.open_response_scope("response-1")
    sent = await controller.send(
        SendParams.from_mapping(
            {
                **_identity(),
                "delivery_id": "response-message-1",
                "to_handle": "response-1",
                "content_parts": [{"type": "text", "text": "done"}],
            },
        ),
    )
    assert sent["state"] == "acknowledged"
    params = ResponseFinishParams.from_mapping(
        {
            **_identity(),
            "response_handle": "response-1",
            "outcome": "completed",
        },
    )
    assert (await controller.response_finish(params))["state"] == "closed"
    assert (await controller.response_finish(params))["state"] == "closed"
    assert calls == ["completed"]
    with pytest.raises(RpcError) as closed:
        await controller.send(
            SendParams.from_mapping(
                {
                    **_identity(),
                    "delivery_id": "response-message-2",
                    "to_handle": "response-1",
                    "content_parts": [{"type": "text", "text": "late"}],
                },
            ),
        )
    assert closed.value.data["reason_code"] == "RESPONSE_CLOSED"
    with pytest.raises(RpcError) as closed_reaction:
        await controller.reaction(
            ReactionParams.from_mapping(
                {
                    **_identity(),
                    "delivery_id": "response-reaction-1",
                    "to_handle": "response-1",
                    "target_delivery_id": "response-message-1",
                    "reaction": "completed",
                },
            ),
        )
    assert closed_reaction.value.data["reason_code"] == "RESPONSE_CLOSED"
    with pytest.raises(RpcError) as conflict:
        await controller.response_finish(
            ResponseFinishParams.from_mapping(
                {
                    **_identity(),
                    "response_handle": "response-1",
                    "outcome": "failed",
                },
            ),
        )
    assert conflict.value.data["reason_code"] == "RESPONSE_OUTCOME_CONFLICT"
    await controller.discard_response_scope("response-1")


@pytest.mark.asyncio
async def test_response_scope_restore_and_driver_gc_are_bounded() -> None:
    """Driver restoration keeps tombstones closed until explicit GC."""
    controller = _controller(Clock(), max_response_scopes=1)
    await _activate(controller, ["response_lifecycle"])
    await controller.restore_response_scope(
        "restored-response",
        ResponseOutcome.COMPLETED,
    )
    with pytest.raises(RpcError) as closed:
        await controller.send(
            SendParams.from_mapping(
                {
                    **_identity(),
                    "delivery_id": "restored-send",
                    "to_handle": "restored-response",
                    "content_parts": [{"type": "text", "text": "late"}],
                },
            ),
        )
    assert closed.value.data["reason_code"] == "RESPONSE_CLOSED"
    with pytest.raises(RpcError) as limited:
        await controller.open_response_scope("second-response")
    assert limited.value.data["reason_code"] == "RESPONSE_SCOPE_LIMIT"
    await controller.discard_response_scope("restored-response")
    await controller.open_response_scope("second-response")


@pytest.mark.asyncio
async def test_response_finish_is_busy_until_delivery_settles() -> None:
    """A finish cannot race an in-flight delivery into route closure."""
    started = asyncio.Event()
    release = asyncio.Event()

    async def send_handler(_: SendParams) -> dict[str, str]:
        started.set()
        await release.wait()
        return {"delivery_id": "busy-message", "state": "acknowledged"}

    controller = _controller(Clock())
    controller._send_handler = send_handler
    await _activate(controller, ["response_lifecycle"])
    await controller.open_response_scope("busy-response")
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
        await controller.response_finish(
            ResponseFinishParams.from_mapping(
                {
                    **_identity(),
                    "response_handle": "busy-response",
                    "outcome": "completed",
                },
            ),
        )
    assert busy.value.data["reason_code"] == "RESPONSE_BUSY"
    release.set()
    assert (await send_task)["state"] == "acknowledged"
    result = await controller.response_finish(
        ResponseFinishParams.from_mapping(
            {
                **_identity(),
                "response_handle": "busy-response",
                "outcome": "completed",
            },
        ),
    )
    assert result["state"] == "closed"


@pytest.mark.asyncio
async def test_response_finish_cleanup_failure_is_retryable() -> None:
    """A failed Driver cleanup stays closed and can be retried safely."""
    attempts = 0

    async def finish_handler(_: ResponseFinishParams) -> None:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise RuntimeError("platform cleanup failed")

    controller = _controller(Clock())
    controller._response_finish_handler = finish_handler
    await _activate(controller, ["response_lifecycle"])
    await controller.open_response_scope("retry-response")
    params = ResponseFinishParams.from_mapping(
        {
            **_identity(),
            "response_handle": "retry-response",
            "outcome": "failed",
        },
    )
    with pytest.raises(RpcError) as failed:
        await controller.response_finish(params)
    assert failed.value.data["reason_code"] == "RESPONSE_FINISH_FAILED"
    snapshot = controller._response_scopes.snapshot("retry-response")
    assert snapshot is not None
    assert snapshot.outcome is ResponseOutcome.FAILED
    assert (await controller.response_finish(params))["state"] == "closed"
    assert attempts == 2


@pytest.mark.asyncio
async def test_response_finish_can_close_an_empty_response() -> None:
    """A response with no outbound delivery still has a terminal scope."""
    controller = _controller(Clock())
    await _activate(controller, ["response_lifecycle"])
    await controller.open_response_scope("empty-response")
    result = await controller.response_finish(
        ResponseFinishParams.from_mapping(
            {
                **_identity(),
                "response_handle": "empty-response",
                "outcome": "cancelled",
            },
        ),
    )
    assert result == {
        "response_handle": "empty-response",
        "outcome": "cancelled",
        "state": "closed",
    }


@pytest.mark.asyncio
async def test_response_finish_is_registered_as_a_reliable_rpc() -> None:
    """The Core request path validates and dispatches response.finish."""
    controller, core, runner = await _active_rpc_pair()
    await controller.open_response_scope("rpc-response")
    params: dict[str, Any] = {
        **_identity(),
        "response_handle": "rpc-response",
        "outcome": "completed",
    }
    expected = {
        "response_handle": "rpc-response",
        "outcome": "completed",
        "state": "closed",
    }
    assert await core.call("channel.response.finish", params) == expected
    assert await core.call("channel.response.finish", params) == expected
    await asyncio.gather(core.aclose(), runner.aclose())
