# -*- coding: utf-8 -*-
# pylint: disable=protected-access
"""Tests for the CH-0-007 Feishu active-connection prototype."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
import inspect
import io
import json
import logging
from pathlib import Path
import subprocess
import sys
import time
from types import SimpleNamespace
from typing import Any
from unittest.mock import patch

import pytest

from qwenpaw.app.channels.feishu.driver import FeishuDriver
from qwenpaw.app.channels.feishu import platform as platform_module
from qwenpaw.app.channels.feishu.platform import (
    FeishuDeliveryError,
    LarkOapiPlatform,
)
from qwenpaw.channel_protocol import (
    FixtureSecretHandleConsumer,
    FramedTransport,
    HelloParams,
    HostContext,
    HostStateStore,
    PrepareParams,
    ProtocolValidationError,
    RpcError,
    RpcPeer,
)


PROCESS_FIXTURE = Path(__file__).with_name("runner.py")


class MemoryTransport:
    """In-memory full-duplex transport for a fixture Runner session."""

    def __init__(self) -> None:
        self.inbox: asyncio.Queue[str | None] = asyncio.Queue()
        self.peer: MemoryTransport | None = None
        self.closed = False
        self.messages: list[str] = []

    async def send(
        self,
        message: str,
        *,
        prepare_write: Callable[[], str | Awaitable[str]] | None = None,
        on_write_succeeded: Callable[[], None] | None = None,
        on_write_failed: Callable[[], None] | None = None,
        on_write_deferred: Callable[[], None] | None = None,
    ) -> None:
        """Deliver one complete protocol message."""
        _ = on_write_failed, on_write_deferred
        if self.closed or self.peer is None or self.peer.closed:
            raise ConnectionError("transport closed")
        if prepare_write is not None:
            message = prepare_write()
            if inspect.isawaitable(message):
                message = await message
        self.messages.append(message)
        self.peer.inbox.put_nowait(message)
        if on_write_succeeded is not None:
            on_write_succeeded()

    async def receive(self) -> str:
        """Receive one complete protocol message."""
        message = await self.inbox.get()
        if message is None:
            raise ConnectionError("transport closed")
        return message

    async def aclose(self) -> None:
        """Close this transport and wake the linked peer."""
        if self.closed:
            return
        self.closed = True
        if self.peer is not None:
            await self.peer.inbox.put(None)


def _transport_pair() -> tuple[MemoryTransport, MemoryTransport]:
    left = MemoryTransport()
    right = MemoryTransport()
    left.peer = right
    right.peer = left
    return left, right


@dataclass(frozen=True)
class FixtureIdentity:
    """Task-local runtime identity without formal bootstrap changes."""

    channel_key: str
    instance_id: str
    environment_spec_id: str
    environment_id: str
    generation: int
    lock_sha256: str
    python_abi: str
    platform_tag: str
    qwenpaw_version: str
    capabilities: tuple[str, ...]


def _identity(instance_id: str, generation: int = 1) -> FixtureIdentity:
    environment_spec_id = f"ches1_{'1' * 64}"
    return FixtureIdentity(
        channel_key="feishu",
        instance_id=instance_id,
        environment_spec_id=environment_spec_id,
        environment_id=f"{environment_spec_id}.install1_{'2' * 32}",
        generation=generation,
        lock_sha256="3" * 64,
        python_abi="cp313-cp313",
        platform_tag="macosx_11_0_arm64",
        qwenpaw_version="0.1",
        capabilities=(
            "approval_card",
            "host_state",
            "media",
            "reaction",
            "streaming",
        ),
    )


class FakePlatform:
    """Driver seam used for protocol and isolation behavior tests."""

    def __init__(self, name: str) -> None:
        self.name = name
        self.config: dict[str, Any] = {}
        self.secret_names: set[str] = set()
        self.connect_count = 0
        self.sent: list[dict[str, Any]] = []
        self.fail_prepare = False
        self.send_error: FeishuDeliveryError | None = None
        self._on_message: Callable[[object], Awaitable[None]] | None = None
        self._on_card: Callable[[object], Awaitable[None]] | None = None
        self._disconnected = asyncio.Event()

    async def prepare(
        self,
        config: dict[str, Any],
        secret: dict[str, str],
        media_work_dir: Path,
    ) -> None:
        """Record only non-secret assertions at the Driver seam."""
        if self.fail_prepare:
            raise RuntimeError("invalid credentials")
        assert media_work_dir.is_absolute()
        assert secret["app_secret"]
        assert secret["encrypt_key"]
        assert secret["verification_token"]
        self.secret_names = set(secret)
        self.config = dict(config)

    async def connect(
        self,
        on_message: Callable[[object], Awaitable[None]],
        on_card: Callable[[object], Awaitable[None]],
    ) -> None:
        """Establish one deterministic active connection."""
        self.connect_count += 1
        self._on_message = on_message
        self._on_card = on_card
        self._disconnected = asyncio.Event()

    async def wait_disconnected(self) -> None:
        """Wait until the test disconnects this platform."""
        await self._disconnected.wait()

    async def disconnect(self) -> None:
        """Release the fake active connection."""
        self._disconnected.set()

    async def close(self) -> None:
        """Release fake platform resources."""
        await self.disconnect()

    async def emit_message(self, value: object) -> None:
        """Deliver one normalized platform event."""
        assert self._on_message is not None
        await self._on_message(value)

    async def emit_card(self, value: object) -> None:
        """Deliver one normalized card callback."""
        assert self._on_card is not None
        await self._on_card(value)

    def force_disconnect(self) -> None:
        """Simulate a platform connection loss."""
        self._disconnected.set()

    async def send_message(
        self,
        receive_id_type: str,
        receive_id: str,
        content_parts: tuple[dict[str, Any], ...],
    ) -> str:
        """Record one Driver outbound call."""
        if self.send_error is not None:
            raise self.send_error
        self.sent.append(
            {
                "kind": "message",
                "receive_id_type": receive_id_type,
                "receive_id": receive_id,
                "content_parts": [dict(part) for part in content_parts],
            },
        )
        return f"{self.name}-message-{len(self.sent)}"

    async def send_approval(
        self,
        receive_id_type: str,
        receive_id: str,
        approval: Mapping[str, str],
        fallback_text: str,
    ) -> str:
        """Record one approval card call."""
        self.sent.append(
            {
                "kind": "approval",
                "receive_id_type": receive_id_type,
                "receive_id": receive_id,
                "approval": dict(approval),
                "fallback_text": fallback_text,
            },
        )
        return f"{self.name}-approval-{len(self.sent)}"

    async def start_stream(
        self,
        receive_id_type: str,
        receive_id: str,
        text: str,
    ) -> dict[str, str]:
        """Record one CardKit stream creation."""
        target = {
            "message_id": f"{self.name}-stream-message",
            "card_id": f"{self.name}-stream-card",
        }
        self.sent.append(
            {
                "kind": "stream.start",
                "receive_id_type": receive_id_type,
                "receive_id": receive_id,
                "text": text,
                **target,
            },
        )
        return target

    async def update_stream(
        self,
        target: Mapping[str, str],
        text: str,
        sequence: int,
        *,
        final: bool,
    ) -> bool:
        """Record one CardKit stream update."""
        self.sent.append(
            {
                "kind": "stream.end" if final else "stream.delta",
                "target": dict(target),
                "text": text,
                "sequence": sequence,
            },
        )
        return True

    async def add_reaction(
        self,
        message_id: str,
        emoji_type: str = "DONE",
    ) -> bool:
        """Record the legacy completed reaction."""
        self.sent.append(
            {
                "kind": "reaction",
                "message_id": message_id,
                "emoji_type": emoji_type,
            },
        )
        return True


class MockHost:
    """Core-side Phase 0 Host with bounded instance state and Inbox."""

    def __init__(
        self,
        peer: RpcPeer,
        identity: FixtureIdentity,
        *,
        state_store: HostStateStore | None = None,
    ) -> None:
        self.peer = peer
        self.identity = identity
        self.events: list[Any] = []
        self.state_store = state_store or HostStateStore()
        self.hello = asyncio.Event()
        self.reject_unmentioned_groups = False
        self.fail_state_put = False
        self.frames: list[str] = []
        self.transports: tuple[MemoryTransport, MemoryTransport] | None = None
        peer.register_method("runner.hello", self._runner_hello)
        peer.register_method("event.batch", self._event_batch)
        peer.register_method("host.state.get", self._state_get)
        peer.register_method("host.state.put", self._state_put)
        peer.register_method("host.state.delete", self._state_delete)

    async def _runner_hello(self, params: Any, _: object) -> dict[str, Any]:
        assert params.instance_id == self.identity.instance_id
        self.hello.set()
        return {
            "protocol_version": 1,
            "capabilities": list(self.identity.capabilities),
        }

    async def _event_batch(self, params: Any, _: object) -> dict[str, Any]:
        accepted: list[str] = []
        rejected: list[dict[str, Any]] = []
        for event in params.events:
            is_group = bool(event.metadata.get("is_group"))
            mentioned = bool(event.metadata.get("bot_mentioned"))
            if (
                self.reject_unmentioned_groups
                and event.event_kind == "message"
                and is_group
                and not mentioned
            ):
                rejected.append(
                    {
                        "event_id": event.event_id,
                        "reason_code": "MENTION_REQUIRED",
                        "retryable": False,
                    },
                )
                continue
            self.events.append(event)
            accepted.append(event.event_id)
        return {
            "batch_id": params.batch_id,
            "accepted_event_ids": accepted,
            "duplicate_event_ids": [],
            "rejected_events": rejected,
        }

    async def _state_get(self, params: Any, _: object) -> dict[str, Any]:
        value = await self.state_store.get(params.key)
        if value is None:
            return {"found": False, "key": params.key}
        schema_version, stored = value
        return {
            "found": True,
            "key": params.key,
            "schema_version": schema_version,
            "value": stored,
        }

    async def _state_put(self, params: Any, _: object) -> dict[str, Any]:
        if self.fail_state_put:
            raise ProtocolValidationError(
                "fixture state write rejected",
                reason_code="STATE_LIMIT_EXCEEDED",
            )
        await self.state_store.put(
            params.key,
            params.schema_version or 1,
            params.value,
        )
        return {"status": "stored", "key": params.key}

    async def _state_delete(self, params: Any, _: object) -> dict[str, Any]:
        await self.state_store.delete(params.key)
        return {"status": "deleted", "key": params.key}


async def _run_driver_session(
    driver: FeishuDriver,
    transport: MemoryTransport,
    identity: FixtureIdentity,
    consumer: FixtureSecretHandleConsumer,
) -> None:
    peer = RpcPeer(transport)
    driver.bind(peer, identity)
    controller = driver.create_lifecycle_controller(
        identity,
        secret_handle_consumer=consumer,
    )
    controller.register_rpc_methods(peer)
    driver.attach_lifecycle(controller)
    await peer.start()
    try:
        hello = HelloParams(
            protocol_min=1,
            protocol_max=1,
            qwenpaw_version=identity.qwenpaw_version,
            channel_key=identity.channel_key,
            instance_id=identity.instance_id,
            environment_spec_id=identity.environment_spec_id,
            environment_id=identity.environment_id,
            lock_sha256=identity.lock_sha256,
            python_abi=identity.python_abi,
            platform_tag=identity.platform_tag,
            capabilities=identity.capabilities,
        )
        controller.accept_hello(hello)
        await peer.call("runner.hello", hello.to_mapping())
        await peer.wait_closed()
    finally:
        await driver.stop()
        await peer.aclose()


async def _start_session(
    identity: FixtureIdentity,
    platform: FakePlatform,
    media_dir: Path,
    *,
    state_store: HostStateStore | None = None,
) -> tuple[RpcPeer, MockHost, FeishuDriver, asyncio.Task[None]]:
    core_transport, runner_transport = _transport_pair()
    core = RpcPeer(core_transport)
    host = MockHost(core, identity, state_store=state_store)
    host.transports = (core_transport, runner_transport)
    driver = FeishuDriver(
        platform_factory=lambda: platform,
        reconnect_initial_delay=0.001,
        reconnect_max_delay=0.005,
        connect_timeout=1.0,
    )
    handle = f"secret-{identity.instance_id}"
    consumer = FixtureSecretHandleConsumer(
        {
            (handle, identity.generation): {
                "app_secret": f"app-secret-{identity.instance_id}",
                "encrypt_key": f"encrypt-{identity.instance_id}",
                "verification_token": f"token-{identity.instance_id}",
            },
        },
        driver.consume_secret,
    )
    await core.start()
    session = asyncio.create_task(
        _run_driver_session(
            driver,
            runner_transport,
            identity,
            consumer,
        ),
    )
    await asyncio.wait_for(host.hello.wait(), timeout=1.0)
    await core.call(
        "channel.prepare",
        PrepareParams(
            channel_key="feishu",
            instance_id=identity.instance_id,
            generation=identity.generation,
            host_context=HostContext(
                media_work_dir=str(media_dir),
                config_snapshot={
                    "app_id": f"app-{identity.instance_id}",
                    "domain": "feishu",
                    "bot_prefix": "[Bot]",
                    "share_session_in_group": False,
                },
                secret_handle=handle,
            ),
            capabilities=identity.capabilities,
        ).to_mapping(),
    )
    lease = {
        "channel_key": "feishu",
        "instance_id": identity.instance_id,
        "generation": identity.generation,
        "lease_token": f"lease-{identity.instance_id}",
        "lease_ttl_ms": 60_000,
    }
    await core.call("channel.activate", lease)
    await core.call("channel.commit", lease)
    return core, host, driver, session


async def _close_session(
    core: RpcPeer,
    session: asyncio.Task[None],
    identity: FixtureIdentity,
) -> None:
    await core.call(
        "channel.stop",
        {
            "channel_key": "feishu",
            "instance_id": identity.instance_id,
            "generation": identity.generation,
        },
    )
    await core.aclose()
    await asyncio.wait_for(session, timeout=1.0)


def _message(
    event_id: str,
    *,
    chat_id: str = "oc-chat",
    chat_type: str = "p2p",
    sender: str = "ou-user",
    mentioned: bool = False,
) -> dict[str, Any]:
    return {
        "event_id": event_id,
        "message_id": f"msg-{event_id}",
        "chat_id": chat_id,
        "chat_type": chat_type,
        "sender_open_id": sender,
        "sender_name": "Alice",
        "content_parts": [{"type": "text", "text": "hello"}],
        "bot_mentioned": mentioned,
    }


def test_driver_entrypoint_does_not_import_legacy_channel() -> None:
    """The production Runner entrypoint has no Core Channel dependency."""
    result = subprocess.run(
        [
            sys.executable,
            "-I",
            "-c",
            (
                "import sys; "
                "from qwenpaw.app.channels.feishu.driver "
                "import FeishuDriver; "
                "print(FeishuDriver.__name__); "
                "print('qwenpaw.app.channels.feishu.channel' in sys.modules)"
            ),
        ],
        capture_output=True,
        check=False,
        timeout=5,
    )
    assert result.returncode == 0
    assert result.stdout.splitlines() == [b"FeishuDriver", b"False"]


@pytest.mark.asyncio
async def test_driver_ingress_core_mention_gate_and_checkpoint_failure(
    tmp_path: Path,
) -> None:
    """Ingress keeps Core-owned mention policy and non-blocking checkpoint."""
    identity = _identity("ingress")
    platform = FakePlatform("ingress")
    core, host, driver, session = await _start_session(
        identity,
        platform,
        tmp_path,
    )
    host.reject_unmentioned_groups = True
    host.fail_state_put = True
    await platform.emit_message(_message("dm", chat_id="oc-dm"))
    await platform.emit_message(
        _message(
            "group-no-mention",
            chat_id="oc-rejected",
            chat_type="group",
            sender="ou-rejected",
        ),
    )
    await platform.emit_message(
        _message(
            "group-mentioned",
            chat_id="oc-group",
            chat_type="group",
            mentioned=True,
        ),
    )
    await platform.emit_card(
        {
            "event_id": "card-action",
            "action": "approve",
            "request_id": "approval-1",
            "operator_open_id": "ou-approver",
            "session_ctx": {
                "chat_id": "oc-group",
                "chat_type": "group",
                "receive_id": "oc-group",
                "receive_id_type": "chat_id",
            },
        },
    )
    assert [event.event_id for event in host.events] == [
        "dm",
        "group-mentioned",
        "card-action",
    ]
    assert host.events[0].conversation["type"] == "dm"
    event = host.events[1]
    assert event.acl_sender_id == "ou-user"
    assert event.sender_id == "Alice#user"
    assert event.metadata["bot_mentioned"] is True
    assert host.events[2].event_kind == "approval_action"
    assert host.events[2].content_parts[0]["text"] == (
        "/approval approve approval-1"
    )
    typing_ids = [
        item["message_id"]
        for item in platform.sent
        if item["kind"] == "reaction" and item["emoji_type"] == "Typing"
    ]
    assert typing_ids == ["msg-dm", "msg-group-mentioned"]
    checkpoint = driver._receive_state_value()
    assert "oc-rejected" not in checkpoint
    assert "ou-rejected" not in checkpoint
    await _close_session(core, session, identity)


@pytest.mark.asyncio
async def test_driver_outbound_card_stream_reaction_and_unknown_boundary(
    tmp_path: Path,
) -> None:
    """Outbound operations preserve cards and uncertainty classification."""
    identity = _identity("outbound")
    platform = FakePlatform("outbound")
    core, _, _, session = await _start_session(identity, platform, tmp_path)
    common = {
        "channel_key": "feishu",
        "instance_id": identity.instance_id,
        "generation": identity.generation,
        "to_handle": "feishu:chat_id:oc-outbound",
    }
    created = await core.call(
        "channel.send",
        {
            **common,
            "delivery_id": "delivery-create",
            "operation": "message.create",
            "content_parts": [{"type": "text", "text": "answer"}],
        },
    )
    approval = await core.call(
        "channel.send",
        {
            **common,
            "delivery_id": "delivery-approval",
            "operation": "message.create",
            "content_parts": [{"type": "text", "text": "Approve?"}],
            "approval": {
                "request_id": "approval-1",
                "tool_name": "shell",
                "severity": "high",
            },
        },
    )
    started = await core.call(
        "channel.send",
        {
            **common,
            "delivery_id": "delivery-stream",
            "operation": "stream.start",
            "stream_type": "message",
            "sequence": 0,
            "accumulated_text": "A",
        },
    )
    delta = await core.call(
        "channel.send",
        {
            **common,
            "delivery_id": "delivery-delta",
            "operation": "stream.delta",
            "target_delivery_id": "delivery-stream",
            "stream_type": "message",
            "sequence": 1,
            "accumulated_text": "AB",
        },
    )
    ended = await core.call(
        "channel.send",
        {
            **common,
            "delivery_id": "delivery-end",
            "operation": "stream.end",
            "target_delivery_id": "delivery-stream",
            "stream_type": "message",
            "sequence": 2,
            "accumulated_text": "ABC",
        },
    )
    reaction = await core.call(
        "channel.reaction",
        {
            **common,
            "delivery_id": "delivery-reaction",
            "target_delivery_id": "delivery-create",
            "reaction": "completed",
        },
    )
    platform.send_error = FeishuDeliveryError(
        "PARTIAL_SEND_UNKNOWN",
        side_effect_possible=True,
    )
    unknown = await core.call(
        "channel.send",
        {
            **common,
            "delivery_id": "delivery-unknown",
            "operation": "message.create",
            "content_parts": [
                {"type": "text", "text": "first"},
                {"type": "file", "file_url": str(tmp_path / "missing")},
            ],
        },
    )
    for result in (created, approval, started, delta, ended, reaction):
        assert result["state"] == "acknowledged"
    assert unknown == {
        "delivery_id": "delivery-unknown",
        "state": "unknown",
        "reason_code": "PARTIAL_SEND_UNKNOWN",
        "retryable": False,
    }
    assert [item["kind"] for item in platform.sent] == [
        "message",
        "approval",
        "stream.start",
        "stream.delta",
        "stream.end",
        "reaction",
    ]
    await _close_session(core, session, identity)


@pytest.mark.asyncio
async def test_two_agents_share_environment_without_state_or_secret_crosstalk(
    tmp_path: Path,
) -> None:
    """Two default instances isolate every Runner-owned value."""
    identity_a = _identity("agent-a")
    identity_b = _identity("agent-b")
    platform_a = FakePlatform("a")
    platform_b = FakePlatform("b")
    core_a, host_a, _, session_a = await _start_session(
        identity_a,
        platform_a,
        tmp_path / "a",
    )
    core_b, host_b, _, session_b = await _start_session(
        identity_b,
        platform_b,
        tmp_path / "b",
    )
    assert identity_a.environment_id == identity_b.environment_id
    assert platform_a.config["app_id"] == "app-agent-a"
    assert platform_b.config["app_id"] == "app-agent-b"
    assert platform_a.secret_names == {
        "app_secret",
        "encrypt_key",
        "verification_token",
    }
    assert host_a.transports is not None
    wire = "".join(
        message
        for transport in host_a.transports
        for message in transport.messages
    )
    for secret in (
        "app-secret-agent-a",
        "encrypt-agent-a",
        "token-agent-a",
    ):
        assert secret not in wire
    await platform_a.emit_message(
        _message("agent-a", chat_id="oc-agent-a", chat_type="group"),
    )
    assert len(host_a.events) == 1
    assert host_b.events == []
    result_a, result_b = await asyncio.gather(
        core_a.call(
            "channel.send",
            {
                "channel_key": "feishu",
                "instance_id": identity_a.instance_id,
                "generation": 1,
                "delivery_id": "delivery-a",
                "to_handle": "feishu:chat_id:oc-agent-a",
                "operation": "message.create",
                "content_parts": [{"type": "text", "text": "A"}],
            },
        ),
        core_b.call(
            "channel.send",
            {
                "channel_key": "feishu",
                "instance_id": identity_b.instance_id,
                "generation": 1,
                "delivery_id": "delivery-b",
                "to_handle": "feishu:chat_id:oc-agent-b",
                "operation": "message.create",
                "content_parts": [{"type": "text", "text": "B"}],
            },
        ),
    )
    assert result_a["state"] == "acknowledged"
    assert result_b["state"] == "acknowledged"
    assert platform_a.sent[-1]["receive_id"] == "oc-agent-a"
    assert platform_b.sent[-1]["receive_id"] == "oc-agent-b"
    await asyncio.gather(
        _close_session(core_a, session_a, identity_a),
        _close_session(core_b, session_b, identity_b),
    )


@pytest.mark.asyncio
async def test_platform_reconnect_and_runner_eof_cleanup(
    tmp_path: Path,
) -> None:
    """Platform loss reconnects while Core EOF stops the Runner cleanly."""
    identity = _identity("reconnect")
    platform = FakePlatform("reconnect")
    core, _, _, session = await _start_session(identity, platform, tmp_path)
    assert platform.connect_count == 1
    platform.force_disconnect()
    for _ in range(100):
        if platform.connect_count >= 2:
            break
        await asyncio.sleep(0.001)
    assert platform.connect_count >= 2
    await core.aclose()
    await asyncio.wait_for(session, timeout=1.0)


@pytest.mark.asyncio
async def test_receive_checkpoint_is_bounded(tmp_path: Path) -> None:
    """Receive routing state stays below Host State per-value limits."""
    identity = _identity("bounded")
    platform = FakePlatform("bounded")
    core, _, driver, session = await _start_session(
        identity,
        platform,
        tmp_path,
    )
    for index in range(400):
        event = driver._normalize_message(
            _message(
                f"bounded-{index}",
                chat_id=f"oc-{index:04d}-{'x' * 80}",
                chat_type="group",
                sender=f"ou-{index:04d}-{'y' * 80}",
            ),
        )
        await driver._remember_receive_target(event)
    await driver._persist_receive_ids()
    encoded = json.dumps(
        driver._receive_state_value(),
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")
    assert len(driver._receive_ids) <= 256
    assert len(encoded) <= 60 * 1024
    await _close_session(core, session, identity)


class _Builder:
    """Minimal fluent SDK request builder used at the production seam."""

    def __init__(self, value: dict[str, Any] | None = None) -> None:
        self.value = value or {}

    @classmethod
    def builder(cls) -> "_Builder":
        return cls()

    def __getattr__(self, name: str) -> Callable[[Any], "_Builder"]:
        def setter(value: Any) -> "_Builder":
            self.value[name] = value
            return self

        return setter

    def build(self) -> dict[str, Any]:
        return dict(self.value)


class _SdkResponse:
    """Configurable lark-oapi response object."""

    def __init__(
        self,
        *,
        success: bool = True,
        data: Any = None,
        file_data: bytes | None = None,
    ) -> None:
        self._success = success
        self.data = data
        self.file = io.BytesIO(file_data) if file_data is not None else None
        self.code = 0 if success else 1
        self.msg = "ok" if success else "failed"

    def success(self) -> bool:
        return self._success


def _sdk_message(
    event_id: str,
    message_id: str,
    content: str,
    *,
    message_type: str = "text",
    app_id: str = "app-sdk",
) -> Any:
    message = SimpleNamespace(
        message_id=message_id,
        chat_id="oc-sdk",
        chat_type="group",
        message_type=message_type,
        content=content,
        mentions=[],
        thread_id="",
        parent_id="",
    )
    sender = SimpleNamespace(
        sender_id=SimpleNamespace(open_id="ou-sdk"),
        name="SDK User",
        nickname="",
    )
    return SimpleNamespace(
        header=SimpleNamespace(
            event_id=event_id,
            app_id=app_id,
            create_time=str(int(time.time() * 1000)),
        ),
        event=SimpleNamespace(message=message, sender=sender),
    )


@pytest.mark.asyncio
async def test_production_sdk_callback_is_fast_and_observes_failure() -> None:
    """The sync SDK callback returns promptly and observes Future errors."""
    platform = LarkOapiPlatform()
    platform._config = {"app_id": "app-sdk"}
    platform._runner_loop = asyncio.get_running_loop()
    errors: list[str] = []

    async def failing(_value: object) -> None:
        await asyncio.sleep(0.05)
        errors.append("failed")
        raise RuntimeError("fixture failure")

    value = _sdk_message("evt-fast", "msg-fast", '{"text":"hello"}')
    started = time.monotonic()
    platform._on_message_sync(value, failing)
    assert time.monotonic() - started < 0.02
    await asyncio.sleep(0.08)
    assert errors == ["failed"]


@pytest.mark.asyncio
async def test_production_sdk_callback_failure_is_logged(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A scheduled SDK callback failure is consumed and diagnosed."""
    platform = LarkOapiPlatform()
    platform._config = {"app_id": "app-sdk"}
    platform._runner_loop = asyncio.get_running_loop()

    async def failing(_value: object) -> None:
        raise RuntimeError("fixture callback failure")

    caplog.set_level(logging.ERROR)
    with caplog.at_level(
        logging.ERROR,
        logger="qwenpaw.app.channels.feishu.platform",
    ):
        platform._on_message_sync(
            _sdk_message("evt-log", "msg-log", '{"text":"hello"}'),
            failing,
        )
        for _ in range(20):
            if caplog.records:
                break
            await asyncio.sleep(0.001)
        assert any(
            record.getMessage() == "Feishu message callback failed"
            for record in caplog.records
        )


@pytest.mark.asyncio
async def test_production_sdk_media_download_keeps_legacy_paths(
    tmp_path: Path,
) -> None:
    """The production adapter preserves legacy image and file naming."""
    platform = LarkOapiPlatform()
    platform._client = SimpleNamespace(
        im=SimpleNamespace(
            v1=SimpleNamespace(
                message_resource=SimpleNamespace(
                    aget=lambda request: asyncio.sleep(
                        0,
                        result=_SdkResponse(
                            file_data={
                                "image-a": b"\x89PNG\r\n\x1a\nA",
                                "image-b": b"\x89PNG\r\n\x1a\nB",
                                "file-a": b"file-A",
                                "file-b": b"file-B",
                            }[request["file_key"]],
                        ),
                    ),
                ),
            ),
        ),
    )
    platform._media_work_dir = tmp_path
    post = json.dumps(
        {
            "content": [
                [
                    {"tag": "img", "image_key": "image-a"},
                    {"tag": "img", "image_key": "image-b"},
                    {"tag": "media", "file_key": "file-a"},
                    {"tag": "media", "file_key": "file-b"},
                ],
            ],
        },
    )
    with patch(
        "qwenpaw.app.channels.feishu.platform.GetMessageResourceRequest",
        _Builder,
    ):
        _, _, parts = await platform._parse_message_content(
            "post",
            post,
            "msg-post",
        )
    paths = [
        Path(part.get("image_url") or part.get("file_url")) for part in parts
    ]
    assert paths[0].name == "msg-post_image-a.png"
    assert paths[1].name == "msg-post_image-b.png"
    assert paths[2].name == "msg-post_file.bin"
    assert paths[3].name == "msg-post_file.bin"
    assert paths[0].read_bytes().endswith(b"A")
    assert paths[1].read_bytes().endswith(b"B")
    assert paths[2] == paths[3]
    assert paths[3].read_bytes() == b"file-B"


@pytest.mark.asyncio
async def test_production_sdk_media_failure_preserves_inbound_event(
    tmp_path: Path,
) -> None:
    """A media download failure emits the legacy diagnostic text."""
    platform = LarkOapiPlatform()
    platform._media_work_dir = tmp_path
    platform._client = SimpleNamespace(
        im=SimpleNamespace(
            v1=SimpleNamespace(
                message_resource=SimpleNamespace(
                    aget=lambda _request: asyncio.sleep(
                        0,
                        result=_SdkResponse(success=False),
                    ),
                ),
            ),
        ),
    )
    with patch(
        "qwenpaw.app.channels.feishu.platform.GetMessageResourceRequest",
        _Builder,
    ):
        text, hints, parts = await platform._parse_message_content(
            "image",
            '{"image_key":"missing"}',
            "msg-missing",
        )
    assert text is None
    assert hints == ["[image: download failed]"]
    assert parts == []


@pytest.mark.asyncio
async def test_production_sdk_file_uri_vectors(tmp_path: Path) -> None:
    """The production adapter reuses the shared cross-platform URI parser."""
    path = tmp_path / "空 格.txt"
    path.write_bytes(b"payload")
    platform = LarkOapiPlatform()
    assert await platform._read_locator(path.as_uri()) == b"payload"
    assert await platform._read_locator(str(path)) == b"payload"
    with patch(
        "qwenpaw.app.channels.feishu.platform.file_url_to_local_path",
        return_value=str(path),
    ) as parser:
        assert await platform._read_locator("file:///C:/Temp/test.txt") == (
            b"payload"
        )
    parser.assert_called_once_with("file:///C:/Temp/test.txt")


@pytest.mark.asyncio
async def test_production_sdk_partial_success_becomes_unknown(
    tmp_path: Path,
) -> None:
    """A later media failure after text send reports an uncertain result."""
    platform = LarkOapiPlatform()
    platform._config = {"bot_prefix": ""}

    async def sent_text(
        _receive_type: str,
        _receive_id: str,
        _body: str,
    ) -> list[str]:
        return ["msg-text"]

    async def failed_file(
        _receive_type: str,
        _receive_id: str,
        _part: Mapping[str, Any],
    ) -> str:
        raise FeishuDeliveryError(
            "LOCAL_FILE_UNAVAILABLE",
            side_effect_possible=False,
        )

    platform._send_text = sent_text
    platform._send_file_part = failed_file
    with pytest.raises(FeishuDeliveryError) as captured:
        await platform.send_message(
            "chat_id",
            "oc-partial",
            (
                {"type": "text", "text": "already sent"},
                {"type": "file", "file_url": str(tmp_path / "missing")},
            ),
        )
    assert captured.value.side_effect_possible is True
    assert captured.value.retryable is False


@pytest.mark.asyncio
async def test_production_sdk_send_card_stream_and_reaction_boundaries(
    tmp_path: Path,
) -> None:
    """The production adapter invokes each existing lark-oapi API family."""
    calls: list[str] = []
    message_counter = 0

    async def create_message(_request: object) -> _SdkResponse:
        nonlocal message_counter
        message_counter += 1
        calls.append("message.create")
        return _SdkResponse(
            data=SimpleNamespace(message_id=f"message-{message_counter}"),
        )

    async def create_image(_request: object) -> _SdkResponse:
        calls.append("image.create")
        return _SdkResponse(data=SimpleNamespace(image_key="image-key"))

    async def create_file(_request: object) -> _SdkResponse:
        calls.append("file.create")
        return _SdkResponse(data=SimpleNamespace(file_key="file-key"))

    async def create_card(_request: object) -> _SdkResponse:
        calls.append("card.create")
        return _SdkResponse(data=SimpleNamespace(card_id="card-id"))

    async def update_card(_request: object) -> _SdkResponse:
        calls.append("card.update")
        return _SdkResponse()

    async def finalize_card(_request: object) -> _SdkResponse:
        calls.append("card.finalize")
        return _SdkResponse()

    async def create_reaction(_request: object) -> _SdkResponse:
        calls.append("reaction.create")
        return _SdkResponse()

    platform = LarkOapiPlatform()
    platform._config = {"bot_prefix": "[Bot]"}
    platform._client = SimpleNamespace(
        im=SimpleNamespace(
            v1=SimpleNamespace(
                message=SimpleNamespace(acreate=create_message),
                image=SimpleNamespace(acreate=create_image),
                file=SimpleNamespace(acreate=create_file),
                message_reaction=SimpleNamespace(acreate=create_reaction),
            ),
        ),
        cardkit=SimpleNamespace(
            v1=SimpleNamespace(
                card=SimpleNamespace(
                    acreate=create_card,
                    asettings=finalize_card,
                ),
                card_element=SimpleNamespace(acontent=update_card),
            ),
        ),
    )
    image_path = tmp_path / "image.png"
    file_path = tmp_path / "file.txt"
    image_path.write_bytes(b"image")
    file_path.write_bytes(b"file")
    builder_names = (
        "CreateMessageRequest",
        "CreateMessageRequestBody",
        "CreateImageRequest",
        "CreateImageRequestBody",
        "CreateFileRequest",
        "CreateFileRequestBody",
        "CreateMessageReactionRequest",
        "CreateMessageReactionRequestBody",
        "Emoji",
        "CreateCardRequest",
        "CreateCardRequestBody",
        "ContentCardElementRequest",
        "ContentCardElementRequestBody",
        "SettingsCardRequest",
        "SettingsCardRequestBody",
    )
    patches = [
        patch.object(platform_module, name, _Builder) for name in builder_names
    ]
    for item in patches:
        item.start()
    try:
        message_id = await platform.send_message(
            "chat_id",
            "oc-sdk",
            (
                {"type": "text", "text": "hello"},
                {"type": "image", "image_url": image_path.as_uri()},
                {
                    "type": "file",
                    "file_url": file_path.as_uri(),
                    "filename": "file.txt",
                },
            ),
        )
        approval_id = await platform.send_approval(
            "chat_id",
            "oc-sdk",
            {
                "request_id": "approval-sdk",
                "tool_name": "shell",
                "severity": "high",
            },
            "Approve?",
        )
        stream = await platform.start_stream("chat_id", "oc-sdk", "...")
        assert await platform.update_stream(
            stream,
            "partial",
            1,
            final=False,
        )
        assert await platform.update_stream(
            stream,
            "complete",
            2,
            final=True,
        )
        assert await platform.add_reaction(message_id, "DONE")
    finally:
        for item in reversed(patches):
            item.stop()
    assert message_id == "message-3"
    assert approval_id == "message-4"
    assert calls == [
        "message.create",
        "image.create",
        "message.create",
        "file.create",
        "message.create",
        "message.create",
        "card.create",
        "message.create",
        "card.update",
        "card.update",
        "card.finalize",
        "reaction.create",
    ]


@pytest.mark.asyncio
async def test_production_sdk_active_connection_auth_failure(
    tmp_path: Path,
) -> None:
    """A controlled SDK WebSocket authentication failure aborts connect."""

    class AuthFailureClient:
        def __init__(self, *_args: object, **_kwargs: object) -> None:
            self._conn = None

        async def _connect(self) -> None:
            raise RuntimeError("invalid credentials")

        async def _disconnect(self) -> None:
            return None

        async def _ping_loop(self) -> None:
            return None

    class ClientBuilder(_Builder):
        def build(self) -> Any:
            return SimpleNamespace(_config=object())

    class DispatcherBuilder(_Builder):
        @classmethod
        def builder(cls, *_args: object) -> "DispatcherBuilder":
            return cls()

        def build(self) -> Any:
            return SimpleNamespace()

    fake_sdk = SimpleNamespace(
        Client=ClientBuilder,
        EventDispatcherHandler=DispatcherBuilder,
        LogLevel=SimpleNamespace(INFO="info"),
        FEISHU_DOMAIN="https://open.feishu.cn",
        LARK_DOMAIN="https://open.larksuite.com",
        ws=SimpleNamespace(Client=AuthFailureClient),
    )
    platform = LarkOapiPlatform()
    token_manager = SimpleNamespace(
        get_self_tenant_token=lambda _config: "fixture-token",
    )
    with (
        patch.object(platform_module, "lark", fake_sdk),
        patch.object(platform_module, "TokenManager", token_manager),
    ):
        await platform.prepare(
            {"app_id": "app-sdk", "domain": "feishu"},
            {
                "app_secret": "sdk-secret",
                "encrypt_key": "sdk-encrypt",
                "verification_token": "sdk-token",
            },
            tmp_path,
        )

        async def noop(_value: object) -> None:
            return None

        with pytest.raises(RuntimeError, match="connection failed"):
            await platform.connect(noop, noop)
        await platform.close()


@pytest.mark.asyncio
async def test_production_sdk_prepare_rejects_invalid_credentials(
    tmp_path: Path,
) -> None:
    """Prepare performs the required read-only platform auth probe."""

    class ClientBuilder(_Builder):
        def build(self) -> Any:
            return SimpleNamespace(_config=object())

    fake_sdk = SimpleNamespace(
        Client=ClientBuilder,
        LogLevel=SimpleNamespace(INFO="info"),
        FEISHU_DOMAIN="https://open.feishu.cn",
        LARK_DOMAIN="https://open.larksuite.com",
    )
    platform = LarkOapiPlatform()

    def reject_credentials(_config: object) -> str:
        raise RuntimeError("invalid credentials")

    token_manager = SimpleNamespace(
        get_self_tenant_token=reject_credentials,
    )
    with (
        patch.object(platform_module, "lark", fake_sdk),
        patch.object(platform_module, "TokenManager", token_manager),
    ):
        with pytest.raises(RuntimeError, match="invalid credentials"):
            await platform.prepare(
                {"app_id": "app-invalid", "domain": "feishu"},
                {
                    "app_secret": "invalid-secret",
                    "encrypt_key": "",
                    "verification_token": "",
                },
                tmp_path,
            )
    assert platform._client is None
    assert platform._http_client is None
    assert not platform._secret


@pytest.mark.asyncio
async def test_invalid_credentials_and_secret_snapshot_rejection(
    tmp_path: Path,
) -> None:
    """Authentication fails without placing secrets in config snapshots."""
    identity = _identity("auth")
    platform = FakePlatform("auth")
    platform.fail_prepare = True
    core_transport, runner_transport = _transport_pair()
    core = RpcPeer(core_transport)
    host = MockHost(core, identity)
    driver = FeishuDriver(platform_factory=lambda: platform)
    handle = "secret-auth"
    consumer = FixtureSecretHandleConsumer(
        {
            (handle, 1): {
                "app_secret": "app-secret-auth",
                "encrypt_key": "encrypt-auth",
                "verification_token": "token-auth",
            },
        },
        driver.consume_secret,
    )
    await core.start()
    session = asyncio.create_task(
        _run_driver_session(driver, runner_transport, identity, consumer),
    )
    await asyncio.wait_for(host.hello.wait(), timeout=1.0)
    with pytest.raises(RpcError) as captured:
        await core.call(
            "channel.prepare",
            PrepareParams(
                channel_key="feishu",
                instance_id=identity.instance_id,
                generation=1,
                host_context=HostContext(
                    media_work_dir=str(tmp_path),
                    config_snapshot={"app_id": "app-auth", "domain": "feishu"},
                    secret_handle=handle,
                ),
                capabilities=identity.capabilities,
            ).to_mapping(),
        )
    assert captured.value.data == {"reason_code": "PLATFORM_AUTH_FAILED"}
    await core.aclose()
    await asyncio.wait_for(session, timeout=1.0)


@pytest.mark.asyncio
async def test_driver_rejects_secret_fields_in_config_snapshot(
    tmp_path: Path,
) -> None:
    """Secret values cannot cross the JSON config snapshot boundary."""
    driver = FeishuDriver(platform_factory=lambda: FakePlatform("secret"))
    await driver.consume_secret(
        {
            "app_secret": "app-secret",
            "encrypt_key": "encrypt-secret",
            "verification_token": "token-secret",
        },
    )
    with pytest.raises(ValueError, match="contains secret fields"):
        await driver.prepare(
            HostContext(
                media_work_dir=str(tmp_path),
                config_snapshot={
                    "app_id": "app-secret",
                    "domain": "feishu",
                    "encrypt_key": "must-not-cross-rpc",
                },
            ),
        )
    assert driver._secret is None


def _identity_mapping(identity: FixtureIdentity) -> dict[str, Any]:
    return {
        "channel_key": identity.channel_key,
        "instance_id": identity.instance_id,
        "environment_spec_id": identity.environment_spec_id,
        "environment_id": identity.environment_id,
        "generation": identity.generation,
        "lock_sha256": identity.lock_sha256,
        "python_abi": identity.python_abi,
        "platform_tag": identity.platform_tag,
        "qwenpaw_version": identity.qwenpaw_version,
        "capabilities": list(identity.capabilities),
    }


@pytest.mark.asyncio
async def test_distinct_runner_processes_and_restart_restore_core_reply(
    tmp_path: Path,
) -> None:
    """Two fixture processes isolate state and restart restores routing."""
    identity_a = _identity("process-a")
    identity_b = _identity("process-b")
    state_a = HostStateStore()
    state_b = HostStateStore()

    async def start_process(
        identity: FixtureIdentity,
        event: dict[str, Any],
        state: HostStateStore,
    ) -> tuple[asyncio.subprocess.Process, RpcPeer, MockHost]:
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            "-I",
            str(PROCESS_FIXTURE),
            json.dumps(_identity_mapping(identity), separators=(",", ":")),
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        assert process.stdin is not None
        assert process.stdout is not None
        core = RpcPeer(FramedTransport(process.stdout, process.stdin))
        host = MockHost(core, identity, state_store=state)
        await core.start()
        await asyncio.wait_for(host.hello.wait(), timeout=2.0)
        await core.call(
            "channel.prepare",
            PrepareParams(
                channel_key="feishu",
                instance_id=identity.instance_id,
                generation=identity.generation,
                host_context=HostContext(
                    media_work_dir=str(tmp_path / identity.instance_id),
                    config_snapshot={
                        "app_id": f"app-{identity.instance_id}",
                        "domain": "feishu",
                        "fixture_event": event,
                    },
                    secret_handle=f"secret-{identity.instance_id}",
                ),
                capabilities=identity.capabilities,
            ).to_mapping(),
        )
        lease = {
            "channel_key": "feishu",
            "instance_id": identity.instance_id,
            "generation": identity.generation,
            "lease_token": f"lease-{identity.instance_id}",
            "lease_ttl_ms": 60_000,
        }
        await core.call("channel.activate", lease)
        await core.call("channel.commit", lease)
        return process, core, host

    process_a, core_a, host_a = await start_process(
        identity_a,
        _message("process-a", chat_id="oc-process-a", chat_type="group"),
        state_a,
    )
    process_b, core_b, host_b = await start_process(
        identity_b,
        _message("process-b", chat_id="oc-process-b", chat_type="group"),
        state_b,
    )
    for _ in range(100):
        if host_a.events and host_b.events:
            break
        await asyncio.sleep(0.01)
    assert process_a.pid != process_b.pid
    assert host_a.events[0].conversation["id"] == "oc-process-a"
    assert host_b.events[0].conversation["id"] == "oc-process-b"
    process_a.kill()
    await asyncio.wait_for(process_a.wait(), timeout=2.0)
    await core_a.aclose()
    restarted_identity = _identity("process-a", generation=2)
    restarted, restarted_core, _ = await start_process(
        restarted_identity,
        {},
        state_a,
    )
    session_key = (
        f"{f'app-{restarted_identity.instance_id}'[-4:]}_"
        f"{'oc-process-a'[-8:]}"
    )
    result = await restarted_core.call(
        "channel.send",
        {
            "channel_key": "feishu",
            "instance_id": restarted_identity.instance_id,
            "generation": 2,
            "delivery_id": "delivery-restart",
            "to_handle": f"feishu:sw:{session_key}",
            "operation": "message.create",
            "content_parts": [{"type": "text", "text": "reply"}],
        },
    )
    assert result["state"] == "acknowledged"
    for process, core, identity in (
        (restarted, restarted_core, restarted_identity),
        (process_b, core_b, identity_b),
    ):
        await core.call(
            "channel.stop",
            {
                "channel_key": "feishu",
                "instance_id": identity.instance_id,
                "generation": identity.generation,
            },
        )
        await core.aclose()
        await asyncio.wait_for(process.wait(), timeout=2.0)
        assert process.returncode == 0
