# -*- coding: utf-8 -*-
"""Focused CH-0-009 Voice Runner-owned ingress tests."""
# pylint: disable=protected-access

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
import inspect
import json
from pathlib import Path
import re
import socket
import sys
from typing import Any
from urllib.parse import parse_qs, urlparse

import aiohttp
from aiohttp import web
import pytest
from twilio.base.exceptions import TwilioRestException
from twilio.request_validator import RequestValidator

from qwenpaw.app.channels.voice.driver import VoiceDriver
from qwenpaw.app.channels.voice.platform import (
    TunnelInfo,
    VoicePlatform,
    VoicePlatformError,
)
from qwenpaw.app.channels.voice.runner_twilio_manager import (
    RunnerTwilioManager,
    TwilioAuthenticationError,
    VoiceWebhookSnapshot,
)
from qwenpaw.channel_protocol import (
    EndpointParams,
    EventBatchAck,
    EventBatchParams,
    FixtureSecretHandleConsumer,
    FramedTransport,
    HelloParams,
    HostContext,
    IdentityParams,
    LeaseParams,
    OutboundOperation,
    PrepareParams,
    QuiesceParams,
    RetryPolicy,
    RpcError,
    RpcLimits,
    RpcPeer,
    RunnerState,
    SendParams,
)
from qwenpaw.channel_protocol.rpc import RpcResponsePublication


PROCESS_FIXTURE = Path(__file__).with_name("runner.py")
AUTH_TOKEN = "fixture-twilio-auth-token"
PUBLIC_BASE_URL = "https://voice.test"


@dataclass(frozen=True)
class FixtureIdentity:
    """Provide one immutable Voice Runner identity."""

    channel_key: str = "voice"
    instance_id: str = "chinst1_fixture-voice"
    environment_spec_id: str = "ches1_fixture-voice"
    environment_id: str = "chenv1_fixture-voice"
    generation: int = 1
    qwenpaw_version: str = "0.0.test"
    lock_sha256: str = "1" * 64
    python_abi: str = "cp313-cp313"
    platform_tag: str = "macosx_11_0_arm64"
    capabilities: tuple[str, ...] = ("ingress_endpoint",)


class FakeTunnel:
    """Record tunnel lifecycle without external network access."""

    def __init__(self) -> None:
        self.start_ports: list[int] = []
        self.stop_count = 0
        self.start_errors: list[BaseException] = []
        self.stop_started = asyncio.Event()
        self.stop_attempts: asyncio.Queue[int] = asyncio.Queue()
        self.stop_gate: asyncio.Event | None = None

    async def start(self, local_port: int) -> TunnelInfo:
        self.start_ports.append(local_port)
        if self.start_errors:
            raise self.start_errors.pop(0)
        return TunnelInfo(
            public_url=PUBLIC_BASE_URL,
            public_wss_url="wss://voice.test",
        )

    async def stop(self) -> None:
        self.stop_count += 1
        self.stop_started.set()
        self.stop_attempts.put_nowait(self.stop_count)
        if self.stop_gate is not None:
            await self.stop_gate.wait()


class FakeRunnerTwilioManager:
    """Record webhook changes without calling Twilio."""

    def __init__(self) -> None:
        self.previous = VoiceWebhookSnapshot(
            voice_url="https://legacy.test/voice/incoming",
            voice_method="GET",
            status_callback="https://legacy.test/voice/status",
            status_callback_method="GET",
        )
        self.current = self.previous
        self.applied: list[VoiceWebhookSnapshot] = []
        self.fetch_count = 0
        self.fetch_errors: list[BaseException] = []
        self.apply_errors_before: list[BaseException] = []
        self.apply_errors_after: list[BaseException] = []

    async def fetch_voice_webhook(
        self,
        phone_number_sid: str,
    ) -> VoiceWebhookSnapshot:
        assert phone_number_sid
        self.fetch_count += 1
        if self.fetch_errors:
            raise self.fetch_errors.pop(0)
        return self.current

    async def apply_voice_webhook(
        self,
        phone_number_sid: str,
        snapshot: VoiceWebhookSnapshot,
    ) -> None:
        assert phone_number_sid
        if self.apply_errors_before:
            raise self.apply_errors_before.pop(0)
        self.current = snapshot
        self.applied.append(snapshot)
        if self.apply_errors_after:
            raise self.apply_errors_after.pop(0)


class MockHost:
    """Persist events and endpoints without receiving native frames."""

    def __init__(self) -> None:
        self.endpoints: list[tuple[str, EndpointParams | IdentityParams]] = []
        self.events: list[Any] = []
        self.event_received = asyncio.Event()
        self.batch_started = asyncio.Event()
        self.release_batch = asyncio.Event()
        self.block_batches = False
        self.backpressure_remaining = 0
        self.after_ack: Any = None
        self.endpoint: EndpointParams | None = None
        self.endpoint_failures: list[str] = []

    async def call(
        self,
        method: str,
        params: object,
        *,
        timeout: float | None = None,
    ) -> dict[str, Any]:
        """Implement Core methods used by the Voice Driver."""
        _ = timeout
        if method in {
            "ingress.endpoint.register",
            "ingress.endpoint.update",
        }:
            endpoint = EndpointParams.from_mapping(params)
            self.endpoint = endpoint
            self.endpoints.append((method, endpoint))
            if self.endpoint_failures:
                raise RuntimeError(self.endpoint_failures.pop(0))
            return {
                "status": method.rsplit(".", 1)[-1],
                "generation": endpoint.generation,
                "readiness": endpoint.readiness,
            }
        if method == "ingress.endpoint.unregister":
            identity = IdentityParams.from_mapping(params)
            self.endpoint = None
            self.endpoints.append((method, identity))
            return {
                "status": "unregistered",
                "generation": identity.generation,
            }
        if method != "event.batch":
            raise AssertionError(f"unexpected method: {method}")
        batch = EventBatchParams.from_mapping(params)
        self.batch_started.set()
        if self.block_batches:
            await self.release_batch.wait()
        if self.backpressure_remaining:
            self.backpressure_remaining -= 1
            raise RpcError(
                -32010,
                "INGRESS_BACKPRESSURE",
                data={"reason_code": "INGRESS_BACKPRESSURE"},
            )
        self.events.extend(batch.events)
        self.event_received.set()
        callback = self.after_ack
        if callback is not None:
            asyncio.create_task(callback(tuple(batch.events)))
        return EventBatchAck(
            batch_id=batch.batch_id,
            accepted_event_ids=tuple(event.event_id for event in batch.events),
        ).to_mapping()


class MemoryTransport:
    """Provide one in-memory full-duplex transport for real RPC peers."""

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
        """Deliver one complete message to the linked peer."""
        _ = on_write_failed, on_write_deferred
        if self.closed or self.peer is None or self.peer.closed:
            raise ConnectionError("transport closed")
        if prepare_write is not None:
            message = prepare_write()
            if inspect.isawaitable(message):
                message = await message
        self.peer.inbox.put_nowait(message)
        if on_write_succeeded is not None:
            result = on_write_succeeded()
            if inspect.isawaitable(result):
                await result

    async def receive(self) -> str:
        """Receive one complete message from the linked peer."""
        message = await self.inbox.get()
        if message is None:
            raise ConnectionError("transport closed")
        return message

    async def aclose(self) -> None:
        """Close this side and wake the linked peer."""
        if self.closed:
            return
        self.closed = True
        if self.peer is not None:
            await self.peer.inbox.put(None)


def _memory_transport_pair() -> tuple[MemoryTransport, MemoryTransport]:
    """Create one linked in-memory transport pair."""
    left = MemoryTransport()
    right = MemoryTransport()
    left.peer = right
    right.peer = left
    return left, right


@dataclass
class Session:
    """Retain one task-local Voice Runner session."""

    driver: VoiceDriver
    controller: Any
    identity: FixtureIdentity
    host: Any
    lease: LeaseParams
    tunnel: FakeTunnel
    twilio: FakeRunnerTwilioManager

    async def stop(self) -> None:
        """Stop the session without leaking listener tasks."""
        await self.controller.stop(self.identity_params())
        await self.controller.stop(self.identity_params())
        await asyncio.sleep(0)

    def identity_params(self) -> IdentityParams:
        return IdentityParams(
            channel_key=self.identity.channel_key,
            instance_id=self.identity.instance_id,
            generation=self.identity.generation,
        )


async def _session(
    *,
    config: dict[str, Any] | None = None,
    commit: bool = True,
    host: Any | None = None,
    identity: FixtureIdentity | None = None,
    retry_policy: RetryPolicy | None = None,
    platform_kwargs: dict[str, Any] | None = None,
    twilio: FakeRunnerTwilioManager | None = None,
) -> Session:
    identity = identity or FixtureIdentity()
    host = host or MockHost()
    tunnel = FakeTunnel()
    twilio = twilio or FakeRunnerTwilioManager()

    def platform_factory() -> VoicePlatform:
        return VoicePlatform(
            tunnel_factory=lambda: tunnel,
            twilio_manager_factory=lambda _sid, _token: twilio,
            **(platform_kwargs or {}),
        )

    driver = VoiceDriver(
        platform_factory=platform_factory,
        retry_policy=retry_policy,
        event_batch_timeout_s=1.0,
    )
    driver.bind(host, identity)
    handle = f"secret-{identity.instance_id}-{identity.generation}"
    consumer = FixtureSecretHandleConsumer(
        {
            (handle, identity.generation): {
                "twilio_auth_token": AUTH_TOKEN,
            },
        },
        driver.consume_secret,
    )
    controller = driver.create_lifecycle_controller(
        identity,
        secret_handle_consumer=consumer,
    )
    driver.attach_lifecycle(controller)
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
    await controller.prepare(
        PrepareParams(
            channel_key=identity.channel_key,
            instance_id=identity.instance_id,
            generation=identity.generation,
            host_context=HostContext(
                config_snapshot={
                    "twilio_account_sid": "AC-fixture",
                    "phone_number_sid": "PN-fixture",
                    "ingress_host": "127.0.0.1",
                    "ingress_port": 0,
                    **(config or {}),
                },
                secret_handle=handle,
            ),
            capabilities=identity.capabilities,
        ),
    )
    lease = LeaseParams(
        channel_key=identity.channel_key,
        instance_id=identity.instance_id,
        generation=identity.generation,
        lease_token=f"lease-{identity.generation}",
        lease_ttl_ms=60_000,
    )
    await controller.activate(lease)
    if commit:
        await controller.commit(lease)
    return Session(
        driver,
        controller,
        identity,
        host,
        lease,
        tunnel,
        twilio,
    )


def _local_url(session: Session, path: str) -> str:
    platform = session.driver.platform
    assert platform is not None
    assert platform.listen_port is not None
    return f"http://127.0.0.1:{platform.listen_port}{path}"


def _signature(path: str, form: dict[str, str]) -> str:
    validator = RequestValidator(AUTH_TOKEN)
    return validator.compute_signature(
        f"{PUBLIC_BASE_URL}{path}",
        form,
    )


async def _incoming_token(
    client: aiohttp.ClientSession,
    session: Session,
    call_sid: str,
) -> str:
    form = {"CallSid": call_sid, "From": "+15550001111"}
    response = await client.post(
        _local_url(session, "/voice/incoming"),
        data=form,
        headers={
            "X-Twilio-Signature": _signature("/voice/incoming", form),
            "x-forwarded-proto": "https",
            "x-forwarded-host": "voice.test",
        },
    )
    body = await response.text()
    assert response.status == 200
    match = re.search(r'url="([^"]+)"', body)
    assert match is not None
    query = parse_qs(urlparse(match.group(1)).query)
    return query["token"][0]


async def _connect_call(
    client: aiohttp.ClientSession,
    session: Session,
    call_sid: str,
) -> aiohttp.ClientWebSocketResponse:
    token = await _incoming_token(client, session, call_sid)
    websocket = await client.ws_connect(
        _local_url(session, f"/voice/ws?token={token}"),
    )
    await websocket.send_json(
        {
            "type": "setup",
            "callSid": call_sid,
            "from": "+15550001111",
            "to": "+15550002222",
        },
    )
    await _wait_until(
        lambda: any(
            event.event_kind == "call.started"
            and event.metadata["platform_session_id"] == call_sid
            for event in session.host.events
        ),
    )
    return websocket


async def _wait_until(predicate: Any, *, timeout: float = 2.0) -> None:
    """Wait for one deterministic asynchronous condition."""

    async def wait() -> None:
        while not predicate():
            await asyncio.sleep(0.005)

    await asyncio.wait_for(wait(), timeout=timeout)


def _reserve_port() -> int:
    """Reserve and release one explicit loopback test port."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
        server.bind(("127.0.0.1", 0))
        return int(server.getsockname()[1])


@pytest.mark.asyncio
async def test_runner_twilio_manager_snapshots_exact_webhook_fields() -> None:
    """Production wrapper reads and writes the rollback field set."""

    class Resource:
        voice_url = "https://legacy.test/incoming"
        voice_method = "GET"
        status_callback = "https://legacy.test/status"
        status_callback_method = "GET"

    class PhoneNumber:
        def __init__(self) -> None:
            self.updates: list[dict[str, str]] = []

        def fetch(self) -> Resource:
            return Resource()

        def update(self, **kwargs: str) -> None:
            self.updates.append(kwargs)

    phone_number = PhoneNumber()

    class Client:
        @staticmethod
        def incoming_phone_numbers(phone_number_sid: str) -> PhoneNumber:
            assert phone_number_sid == "PN-fixture"
            return phone_number

    manager = RunnerTwilioManager("AC-fixture", AUTH_TOKEN)
    manager._client = Client()
    snapshot = await manager.fetch_voice_webhook("PN-fixture")
    assert snapshot == VoiceWebhookSnapshot(
        voice_url="https://legacy.test/incoming",
        voice_method="GET",
        status_callback="https://legacy.test/status",
        status_callback_method="GET",
    )
    await manager.apply_voice_webhook("PN-fixture", snapshot)
    assert phone_number.updates == [snapshot.to_update_kwargs()]


@pytest.mark.asyncio
async def test_runner_twilio_manager_classifies_auth_probe_failure() -> None:
    """Credential and phone-number probe rejection is stable."""

    class PhoneNumber:
        @staticmethod
        def fetch() -> None:
            raise TwilioRestException(
                401,
                "/IncomingPhoneNumbers/PN-fixture",
                "Unauthorized",
            )

    class Client:
        @staticmethod
        def incoming_phone_numbers(_phone_number_sid: str) -> PhoneNumber:
            return PhoneNumber()

    manager = RunnerTwilioManager("AC-fixture", AUTH_TOKEN)
    manager._client = Client()
    with pytest.raises(TwilioAuthenticationError):
        await manager.fetch_voice_webhook("PN-fixture")


@pytest.mark.asyncio
async def test_prepare_probes_twilio_authentication() -> None:
    """Standby fails before commit when Twilio rejects the probe."""
    twilio = FakeRunnerTwilioManager()
    twilio.fetch_errors.append(
        TwilioAuthenticationError("Twilio authentication failed"),
    )
    with pytest.raises(RpcError) as exc_info:
        await _session(commit=False, twilio=twilio)
    assert exc_info.value.data["reason_code"] == "PLATFORM_AUTH_FAILED"
    assert twilio.fetch_count == 1
    assert not twilio.applied


@pytest.mark.asyncio
async def test_standby_has_no_listener_or_external_side_effects() -> None:
    """Standby consumes secret without exposing or configuring Voice."""
    session = await _session(commit=False)
    try:
        platform = session.driver.platform
        assert platform is not None
        assert platform.listen_port is None
        assert session.tunnel.start_ports == []
        assert session.twilio.fetch_count == 1
        assert session.twilio.applied == []
        serialized = json.dumps(session.host.endpoints)
        assert AUTH_TOKEN not in serialized
        assert "twilio_auth_token" not in serialized
    finally:
        await session.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize("explicit", [False, True])
async def test_commit_registers_dynamic_or_explicit_endpoint(
    explicit: bool,
) -> None:
    """Only commit binds and reports the actual Runner endpoint."""
    expected_port = _reserve_port() if explicit else 0
    session = await _session(config={"ingress_port": expected_port})
    try:
        platform = session.driver.platform
        assert platform is not None
        if explicit:
            assert platform.listen_port == expected_port
        else:
            assert platform.listen_port is not None
            assert platform.listen_port > 0
        states = [
            endpoint.readiness
            for _, endpoint in session.host.endpoints
            if isinstance(endpoint, EndpointParams)
        ]
        assert states == ["starting", "ready"]
        endpoint = session.host.endpoint
        assert endpoint is not None
        assert endpoint.generation == session.identity.generation
        assert endpoint.public_base_url == PUBLIC_BASE_URL
        assert endpoint.path == "/voice"
        assert endpoint.auth_required is True
    finally:
        await session.stop()


@pytest.mark.asyncio
async def test_startup_failure_rolls_back_endpoint_and_resources() -> None:
    """A startup failure restores ownership and removes local ingress."""
    session = await _session(commit=False)
    session.tunnel.start_errors.append(RuntimeError("injected tunnel error"))
    with pytest.raises(RuntimeError, match="injected tunnel error"):
        await session.controller.commit(session.lease)
    platform = session.driver.platform
    assert platform is not None
    assert session.controller.state is RunnerState.FAILED
    assert platform.listen_port is None
    assert session.host.endpoint is None
    assert session.tunnel.stop_count == 1
    await session.stop()


@pytest.mark.asyncio
async def test_commit_publication_controls_admission_and_rollback() -> None:
    """Only a published commit response admits formal Voice traffic."""
    session = await _session(commit=False)
    controller = session.controller
    publication = await controller._rpc_commit(session.lease)
    assert isinstance(publication, RpcResponsePublication)
    platform = session.driver.platform
    assert platform is not None
    assert platform.health_snapshot()["accepting"] is False
    assert session.host.endpoint is not None
    assert session.host.endpoint.readiness == "ready"
    publication.on_published()
    assert platform.health_snapshot()["accepting"] is True
    await session.stop()

    aborted = await _session(commit=False)
    replacement = VoiceWebhookSnapshot(
        voice_url="https://replacement.test/voice/incoming",
        status_callback="https://replacement.test/voice/status",
    )
    assert aborted.twilio.fetch_count == 1
    aborted.twilio.current = replacement
    publication = await aborted.controller._rpc_commit(aborted.lease)
    assert isinstance(publication, RpcResponsePublication)
    assert aborted.twilio.fetch_count == 2
    await publication.on_aborted("REQUEST_CANCELLED")
    aborted_platform = aborted.driver.platform
    assert aborted_platform is not None
    assert aborted.controller.state is RunnerState.FAILED
    assert aborted.twilio.current == replacement
    assert aborted.host.endpoint is None
    assert aborted_platform.listen_port is None
    await aborted.stop()


@pytest.mark.asyncio
async def test_rollback_preserves_newer_webhook_owner() -> None:
    """An old generation cannot overwrite a newer webhook owner."""
    session = await _session(commit=False)
    publication = await session.controller._rpc_commit(session.lease)
    assert isinstance(publication, RpcResponsePublication)
    replacement = VoiceWebhookSnapshot(
        voice_url="https://generation-c.test/voice/incoming",
        voice_method="POST",
        status_callback="https://generation-c.test/voice/status",
        status_callback_method="POST",
    )
    session.twilio.current = replacement
    await publication.on_aborted("REQUEST_CANCELLED")
    platform = session.driver.platform
    assert platform is not None
    diagnostics = platform.health_snapshot()
    assert session.twilio.current == replacement
    assert diagnostics["webhook_transaction_state"] == (
        "rollback_skipped_owner_changed"
    )
    assert diagnostics["webhook_rollback_reason"] == ("WEBHOOK_OWNER_CHANGED")
    assert session.host.endpoint is None
    assert platform.listen_port is None
    await session.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("failure_stage", "expected_state", "expected_reason"),
    [
        (
            "ownership",
            "rollback_unknown",
            "WEBHOOK_OWNERSHIP_UNKNOWN",
        ),
        (
            "restore",
            "rollback_failed",
            "WEBHOOK_RESTORE_FAILED",
        ),
    ],
)
async def test_rollback_failure_is_failed_and_non_ready(
    failure_stage: str,
    expected_state: str,
    expected_reason: str,
) -> None:
    """Rollback uncertainty remains diagnosed after local cleanup."""
    session = await _session(commit=False)
    publication = await session.controller._rpc_commit(session.lease)
    assert isinstance(publication, RpcResponsePublication)
    if failure_stage == "ownership":
        session.twilio.fetch_errors.append(
            RuntimeError("ownership fetch failed"),
        )
    else:
        session.twilio.apply_errors_before.append(
            RuntimeError("restore failed"),
        )
    await publication.on_aborted("REQUEST_CANCELLED")
    platform = session.driver.platform
    assert platform is not None
    diagnostics = platform.health_snapshot()
    assert session.controller.state is RunnerState.FAILED
    assert diagnostics["webhook_transaction_state"] == expected_state
    assert diagnostics["webhook_rollback_reason"] == expected_reason
    assert diagnostics["accepting"] is False
    assert session.host.endpoint is None
    assert platform.listen_port is None
    await session.stop()


@pytest.mark.asyncio
async def test_signature_twiml_token_ttl_and_atomic_single_use() -> None:
    """Webhook auth mints one expiring, atomically consumed WS token."""
    now = [10.0]
    tokens = iter(("single-use", "expired"))
    session = await _session(
        platform_kwargs={
            "clock": lambda: now[0],
            "token_factory": lambda: next(tokens),
            "token_ttl_s": 1.0,
        },
    )
    try:
        async with aiohttp.ClientSession() as client:
            invalid = await client.post(
                _local_url(session, "/voice/incoming"),
                data={"CallSid": "CA-invalid"},
            )
            assert invalid.status == 403
            token = await _incoming_token(client, session, "CA-single")
            assert token == "single-use"

            async def connect() -> int:
                try:
                    websocket = await client.ws_connect(
                        _local_url(
                            session,
                            f"/voice/ws?token={token}",
                        ),
                    )
                except aiohttp.WSServerHandshakeError as exc:
                    return exc.status
                await websocket.close()
                return 101

            values = await asyncio.gather(connect(), connect())
            assert sorted(values) == [101, 403]
            expired = await _incoming_token(client, session, "CA-expired")
            now[0] = 12.0
            with pytest.raises(aiohttp.WSServerHandshakeError) as exc_info:
                await client.ws_connect(
                    _local_url(
                        session,
                        f"/voice/ws?token={expired}",
                    ),
                )
            assert exc_info.value.status == 403
    finally:
        await session.stop()


@pytest.mark.asyncio
async def test_signature_fallback_preserves_non_default_host_port() -> None:
    """Direct Host authority retains its port during signature validation."""
    session = await _session()
    try:
        form = {"CallSid": "CA-port", "From": "+15550001111"}
        validator = RequestValidator(AUTH_TOKEN)
        signature = validator.compute_signature(
            "https://voice.test:8443/voice/incoming",
            form,
        )
        async with aiohttp.ClientSession() as client:
            response = await client.post(
                _local_url(session, "/voice/incoming"),
                data=form,
                headers={
                    "Host": "voice.test:8443",
                    "X-Twilio-Signature": signature,
                    "x-forwarded-proto": "https",
                },
            )
            assert response.status == 200
    finally:
        await session.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize("barrier", ["token", "handshake"])
async def test_stop_accepting_fences_provisional_websocket(
    monkeypatch: pytest.MonkeyPatch,
    barrier: str,
) -> None:
    """A started token or handshake admission cannot cross its fence."""
    session = await _session()
    release = asyncio.Event()
    started = asyncio.Event()
    connect_task: asyncio.Task[Any] | None = None
    try:
        async with aiohttp.ClientSession() as client:
            token = await _incoming_token(client, session, "CA-provisional")
            platform = session.driver.platform
            assert platform is not None
            if barrier == "token":
                consume_token = platform._consume_token

                async def blocked_consume(value: str) -> Any:
                    started.set()
                    await release.wait()
                    return await consume_token(value)

                monkeypatch.setattr(
                    platform,
                    "_consume_token",
                    blocked_consume,
                )
            else:
                prepare = web.WebSocketResponse.prepare

                async def blocked_prepare(
                    websocket: web.WebSocketResponse,
                    request: web.Request,
                ) -> Any:
                    result = await prepare(websocket, request)
                    started.set()
                    await release.wait()
                    return result

                monkeypatch.setattr(
                    web.WebSocketResponse,
                    "prepare",
                    blocked_prepare,
                )
            connect_task = asyncio.create_task(
                client.ws_connect(
                    _local_url(session, f"/voice/ws?token={token}"),
                ),
            )
            await asyncio.wait_for(started.wait(), timeout=1.0)
            await _wait_until(
                lambda: platform.health_snapshot()[
                    "provisional_connection_count"
                ]
                == 1,
            )
            await platform.stop_accepting()
            release.set()
            result = await asyncio.gather(
                connect_task,
                return_exceptions=True,
            )
            websocket = result[0]
            if isinstance(websocket, aiohttp.ClientWebSocketResponse):
                message = await websocket.receive(timeout=1.0)
                assert message.type in {
                    aiohttp.WSMsgType.CLOSE,
                    aiohttp.WSMsgType.CLOSED,
                    aiohttp.WSMsgType.ERROR,
                }
                await websocket.close()
            assert platform.health_snapshot()["connection_count"] == 0
            assert (
                platform.health_snapshot()["provisional_connection_count"] == 0
            )
    finally:
        release.set()
        if connect_task is not None and not connect_task.done():
            connect_task.cancel()
            await asyncio.gather(connect_task, return_exceptions=True)
        await session.stop()


@pytest.mark.asyncio
async def test_global_connection_limit_rejects_and_recovers() -> None:
    """The live plus provisional admission budget has a hard limit."""
    session = await _session(platform_kwargs={"max_connections": 1})
    try:
        async with aiohttp.ClientSession() as client:
            first = await _connect_call(client, session, "CA-limit-first")
            token = await _incoming_token(client, session, "CA-limit-next")
            with pytest.raises(aiohttp.WSServerHandshakeError) as exc_info:
                await client.ws_connect(
                    _local_url(session, f"/voice/ws?token={token}"),
                )
            assert exc_info.value.status == 503
            platform = session.driver.platform
            assert platform is not None
            assert platform.health_snapshot()["connection_overload_total"] == 1
            await first.close()
            await _wait_until(
                lambda: platform.health_snapshot()["connection_count"] == 0,
            )
            recovered = await client.ws_connect(
                _local_url(session, f"/voice/ws?token={token}"),
            )
            assert platform.health_snapshot()["connection_count"] == 1
            await recovered.close()
    finally:
        await session.stop()


@pytest.mark.asyncio
async def test_duplicate_call_sid_does_not_mutate_active_binding() -> None:
    """A rejected duplicate setup cannot replace the active call binding."""
    session = await _session()
    try:
        async with aiohttp.ClientSession() as client:
            first = await _connect_call(client, session, "CA-duplicate")
            token = await _incoming_token(client, session, "CA-duplicate")
            second = await client.ws_connect(
                _local_url(session, f"/voice/ws?token={token}"),
            )
            await second.send_json(
                {
                    "type": "setup",
                    "callSid": "CA-duplicate",
                    "from": "+15550003333",
                    "to": "+15550004444",
                },
            )
            message = await second.receive(timeout=2.0)
            assert message.type in {
                aiohttp.WSMsgType.CLOSE,
                aiohttp.WSMsgType.CLOSED,
            }
            await first.send_json(
                {"type": "prompt", "voicePrompt": "still active"},
            )
            await _wait_until(
                lambda: any(
                    event.event_kind == "message.query"
                    for event in session.host.events
                ),
            )
            started = [
                event
                for event in session.host.events
                if event.event_kind == "call.started"
            ]
            assert len(started) == 1
            assert started[0].metadata["voice_payload"] == {
                "from": "***1111",
                "to": "***2222",
            }
            await first.close()
            await second.close()
    finally:
        await session.stop()


@pytest.mark.asyncio
async def test_voice_events_direct_session_reply_and_status_close() -> None:
    """Voice events use event.batch and Core replies use channel.send."""
    session = await _session()
    release_reply = asyncio.Event()

    async def direct_session(events: tuple[Any, ...]) -> None:
        query = next(
            (event for event in events if event.event_kind == "message.query"),
            None,
        )
        if query is None:
            return
        await release_reply.wait()
        result = await session.controller.send(
            SendParams(
                channel_key=session.identity.channel_key,
                instance_id=session.identity.instance_id,
                generation=session.identity.generation,
                delivery_id="delivery-query-1",
                to_handle=query.metadata["session_binding"],
                operation=OutboundOperation.MESSAGE_CREATE,
                content_parts=({"type": "text", "text": "Core reply"},),
            ),
        )
        assert result["state"] == "acknowledged"

    session.host.after_ack = direct_session
    try:
        async with aiohttp.ClientSession() as client:
            websocket = await _connect_call(client, session, "CA-events")
            await websocket.send_json(
                {"type": "prompt", "voicePrompt": "Hello Core"},
            )
            await websocket.send_json(
                {
                    "type": "interrupt",
                    "utteranceUntilInterrupt": "Core rep",
                },
            )
            await websocket.send_json({"type": "dtmf", "digit": "5"})
            await _wait_until(lambda: len(session.host.events) >= 4)
            release_reply.set()
            reply = await websocket.receive_json(timeout=2.0)
            assert reply == {
                "type": "text",
                "token": "Core reply",
                "last": True,
            }
            form = {"CallSid": "CA-events", "CallStatus": "completed"}
            response = await client.post(
                _local_url(session, "/voice/status-callback"),
                data=form,
                headers={
                    "X-Twilio-Signature": _signature(
                        "/voice/status-callback",
                        form,
                    ),
                    "x-forwarded-proto": "https",
                    "x-forwarded-host": "voice.test",
                },
            )
            assert response.status == 204
            await _wait_until(
                lambda: any(
                    event.event_kind == "call.closed"
                    for event in session.host.events
                ),
            )
            kinds = [event.event_kind for event in session.host.events]
            assert kinds == [
                "call.started",
                "message.query",
                "call.interrupted",
                "dtmf",
                "call.closed",
            ]
            assert [
                event.metadata["sequence"] for event in session.host.events
            ] == [1, 2, 3, 4, 5]
            started = session.host.events[0]
            assert started.metadata["voice_payload"] == {
                "from": "***1111",
                "to": "***2222",
            }
            assert session.host.events[1].content_parts == (
                {"type": "text", "text": "Hello Core"},
            )
            await websocket.close()
    finally:
        await session.stop()


@pytest.mark.asyncio
async def test_runner_and_core_backpressure_are_separate() -> None:
    """Runner queue pressure and Core ACK pressure have separate counters."""
    host = MockHost()
    host.backpressure_remaining = 1
    session = await _session(
        host=host,
        retry_policy=RetryPolicy(
            max_attempts=3,
            initial_delay=0.001,
            max_delay=0.001,
        ),
    )
    try:
        async with aiohttp.ClientSession() as client:
            websocket = await _connect_call(client, session, "CA-retry")
            assert (
                session.driver.diagnostics()["core_batch_backpressure_total"]
                == 1
            )
            assert session.driver.diagnostics()["core_batch_retry_total"] == 1
            assert (
                session.driver.diagnostics()["runner_backpressure_total"] == 0
            )
            await websocket.close()
    finally:
        await session.stop()

    blocked = MockHost()
    blocked.block_batches = True
    overloaded = await _session(
        host=blocked,
        platform_kwargs={"event_queue_size": 1},
    )
    try:
        async with aiohttp.ClientSession() as client:
            token = await _incoming_token(client, overloaded, "CA-pressure")
            websocket = await client.ws_connect(
                _local_url(overloaded, f"/voice/ws?token={token}"),
            )
            await websocket.send_json(
                {
                    "type": "setup",
                    "callSid": "CA-pressure",
                    "from": "+1",
                    "to": "+2",
                },
            )
            await blocked.batch_started.wait()
            await websocket.send_json(
                {"type": "prompt", "voicePrompt": "queued"},
            )
            await websocket.send_json(
                {"type": "dtmf", "digit": "2"},
            )
            await _wait_until(
                lambda: overloaded.driver.diagnostics()[
                    "runner_backpressure_total"
                ]
                == 1,
            )
            blocked.release_batch.set()
            await websocket.close()
    finally:
        blocked.release_batch.set()
        await overloaded.stop()


@pytest.mark.asyncio
async def test_real_rpc_pending_limit_retries_voice_event() -> None:
    """Peer capacity pressure cannot drop a Voice event before its ACK."""
    runner_transport, core_transport = _memory_transport_pair()
    runner_peer = RpcPeer(
        runner_transport,
        limits=RpcLimits(max_pending_requests=1, request_timeout=1.0),
    )
    core_peer = RpcPeer(core_transport)
    core_host = ProcessHost(core_peer)
    hold_started = asyncio.Event()
    release_hold = asyncio.Event()

    async def hold_pending(
        _params: object,
        _request: object,
    ) -> dict[str, Any]:
        hold_started.set()
        await release_hold.wait()
        return {"released": True}

    core_peer.register_method("test.hold", hold_pending)
    await asyncio.gather(runner_peer.start(), core_peer.start())
    session: Session | None = None
    pending: asyncio.Task[Any] | None = None
    try:
        session = await _session(
            host=runner_peer,
            retry_policy=RetryPolicy(
                max_attempts=20,
                initial_delay=0.01,
                max_delay=0.01,
            ),
        )
        pending = asyncio.create_task(runner_peer.call("test.hold", {}))
        await asyncio.wait_for(hold_started.wait(), timeout=1.0)
        async with aiohttp.ClientSession() as client:
            token = await _incoming_token(
                client,
                session,
                "CA-rpc-capacity",
            )
            websocket = await client.ws_connect(
                _local_url(session, f"/voice/ws?token={token}"),
            )
            await websocket.send_json(
                {
                    "type": "setup",
                    "callSid": "CA-rpc-capacity",
                    "from": "+1",
                    "to": "+2",
                },
            )
            await _wait_until(
                lambda: session.driver.diagnostics()[
                    "core_batch_backpressure_total"
                ]
                >= 1,
            )
            release_hold.set()
            await asyncio.wait_for(pending, timeout=1.0)
            await _wait_until(
                lambda: any(
                    event.event_kind == "call.started"
                    for event in core_host.events
                ),
            )
            started = [
                event
                for event in core_host.events
                if event.event_kind == "call.started"
            ]
            assert len(started) == 1
            assert session.driver.diagnostics()["core_batch_retry_total"] >= 1
            await websocket.close()
    finally:
        release_hold.set()
        if pending is not None and not pending.done():
            pending.cancel()
            await asyncio.gather(pending, return_exceptions=True)
        if session is not None:
            await session.stop()
        await asyncio.gather(runner_peer.aclose(), core_peer.aclose())


@pytest.mark.asyncio
async def test_half_close_unknown_send_and_old_generation_fencing() -> None:
    """Disconnect is ordered; ambiguous writes and stale sends are fenced."""
    session = await _session()
    try:
        async with aiohttp.ClientSession() as client:
            websocket = await _connect_call(
                client,
                session,
                "CA-half-close",
            )
            await websocket.send_json(
                {"type": "prompt", "voicePrompt": "before close"},
            )
            await websocket.close()
            await _wait_until(
                lambda: len(session.host.events) >= 3,
            )
            assert [event.event_kind for event in session.host.events] == [
                "call.started",
                "message.query",
                "call.closed",
            ]

        platform = session.driver.platform
        assert platform is not None

        async def ambiguous_send(_target: str, _text: str) -> None:
            raise VoicePlatformError(
                "PLATFORM_RESULT_UNKNOWN",
                side_effect_possible=True,
            )

        platform.send_text = ambiguous_send  # type: ignore[method-assign]
        result = await session.controller.send(
            SendParams(
                channel_key=session.identity.channel_key,
                instance_id=session.identity.instance_id,
                generation=session.identity.generation,
                delivery_id="delivery-unknown",
                to_handle="ambiguous",
                operation=OutboundOperation.MESSAGE_CREATE,
                content_parts=({"type": "text", "text": "unknown"},),
            ),
        )
        assert result == {
            "delivery_id": "delivery-unknown",
            "state": "unknown",
            "reason_code": "PLATFORM_RESULT_UNKNOWN",
            "retryable": False,
        }
        await session.stop()
        with pytest.raises(RpcError) as exc_info:
            await session.controller.send(
                SendParams(
                    channel_key=session.identity.channel_key,
                    instance_id=session.identity.instance_id,
                    generation=session.identity.generation,
                    delivery_id="delivery-stale",
                    to_handle="stale",
                    operation=OutboundOperation.MESSAGE_CREATE,
                    content_parts=({"type": "text", "text": "stale"},),
                ),
            )
        assert exc_info.value.data["reason_code"] == (
            "INVALID_STATE_TRANSITION"
        )
    finally:
        await session.stop()


@pytest.mark.asyncio
async def test_quiesce_sends_end_unregisters_and_obeys_deadline() -> None:
    """Quiesce rejects calls, ends active calls, and bounds blocked ACKs."""
    session = await _session()
    try:
        async with aiohttp.ClientSession() as client:
            websocket = await _connect_call(client, session, "CA-quiesce")
            await session.controller.quiesce(
                QuiesceParams(
                    channel_key=session.identity.channel_key,
                    instance_id=session.identity.instance_id,
                    generation=session.identity.generation,
                    drain_timeout_ms=500,
                ),
            )
            frames: list[dict[str, Any]] = []
            async for message in websocket:
                if message.type == aiohttp.WSMsgType.TEXT:
                    frames.append(json.loads(message.data))
            assert {frame["type"] for frame in frames} == {"end"}
            assert session.host.endpoint is None
            assert any(
                method == "ingress.endpoint.unregister"
                for method, _ in session.host.endpoints
            )
    finally:
        await session.stop()

    blocked_host = MockHost()
    blocked_host.block_batches = True
    blocked = await _session(host=blocked_host)
    try:
        async with aiohttp.ClientSession() as client:
            token = await _incoming_token(client, blocked, "CA-blocked")
            websocket = await client.ws_connect(
                _local_url(blocked, f"/voice/ws?token={token}"),
            )
            await websocket.send_json(
                {
                    "type": "setup",
                    "callSid": "CA-blocked",
                    "from": "+1",
                    "to": "+2",
                },
            )
            await blocked_host.batch_started.wait()
            start = asyncio.get_running_loop().time()
            await blocked.controller.quiesce(
                QuiesceParams(
                    channel_key=blocked.identity.channel_key,
                    instance_id=blocked.identity.instance_id,
                    generation=blocked.identity.generation,
                    drain_timeout_ms=100,
                ),
            )
            elapsed = asyncio.get_running_loop().time() - start
            assert elapsed < 0.5
            await websocket.close()
    finally:
        blocked_host.release_batch.set()
        await blocked.stop()


@pytest.mark.asyncio
async def test_stop_retries_cleanup_skipped_by_quiesce_deadline() -> None:
    """A later stop releases handles retained after an exhausted deadline."""
    session = await _session()
    platform = session.driver.platform
    assert platform is not None
    runner = platform._runner
    assert runner is not None

    class CountingRunner:
        def __init__(self) -> None:
            self.cleanup_count = 0

        async def cleanup(self) -> None:
            self.cleanup_count += 1
            await runner.cleanup()

    counting_runner = CountingRunner()
    platform._runner = counting_runner
    try:
        await session.controller.quiesce(
            QuiesceParams(
                channel_key=session.identity.channel_key,
                instance_id=session.identity.instance_id,
                generation=session.identity.generation,
                drain_timeout_ms=0,
            ),
        )
        assert session.tunnel.stop_count == 0
        assert counting_runner.cleanup_count == 0
        assert platform._tunnel is session.tunnel
        assert platform._runner is counting_runner
        assert platform.cleanup_complete is False
        await session.stop()
        assert session.tunnel.stop_count == 1
        assert counting_runner.cleanup_count == 1
        assert platform._tunnel is None
        assert platform._runner is None
        assert platform.cleanup_complete is True
    finally:
        await session.stop()


@pytest.mark.asyncio
async def test_concurrent_stop_retries_expired_quiesce_cleanup() -> None:
    """Stop retries after joining expired quiesce cleanup."""
    session = await _session()
    platform = session.driver.platform
    assert platform is not None
    assert platform._runner is not None
    stop_gate = asyncio.Event()
    session.tunnel.stop_gate = stop_gate
    quiesce_task = asyncio.create_task(
        session.controller.quiesce(
            QuiesceParams(
                channel_key=session.identity.channel_key,
                instance_id=session.identity.instance_id,
                generation=session.identity.generation,
                drain_timeout_ms=100,
            ),
        ),
    )
    stop_task: asyncio.Task[Any] | None = None
    try:
        first_attempt = await asyncio.wait_for(
            session.tunnel.stop_attempts.get(),
            timeout=1.0,
        )
        assert first_attempt == 1
        stop_task = asyncio.create_task(
            session.controller.stop(session.identity_params()),
        )
        await quiesce_task
        second_attempt = await asyncio.wait_for(
            session.tunnel.stop_attempts.get(),
            timeout=1.0,
        )
        assert second_attempt == 2
        assert not stop_task.done()
        assert session.tunnel.stop_count == 2
        assert platform._tunnel is session.tunnel
        assert platform.cleanup_complete is False

        stop_gate.set()
        await asyncio.wait_for(stop_task, timeout=1.0)
        assert session.tunnel.stop_count == 2
        assert platform._tunnel is None
        assert platform._runner is None
        assert platform.cleanup_complete is True
        assert session.driver.platform is None
    finally:
        stop_gate.set()
        if not quiesce_task.done():
            await quiesce_task
        if stop_task is not None and not stop_task.done():
            await stop_task
        await session.stop()


class ProcessHost:
    """Provide Core-side handlers for the subprocess Runner proof."""

    def __init__(self, peer: RpcPeer) -> None:
        self.peer = peer
        self.events: list[Any] = []
        self.endpoint: EndpointParams | None = None
        self.endpoint_ready = asyncio.Event()
        peer.register_method("runner.hello", self._runner_hello)
        peer.register_method("event.batch", self._event_batch)
        peer.register_method(
            "ingress.endpoint.register",
            self._endpoint_register,
        )
        peer.register_method(
            "ingress.endpoint.update",
            self._endpoint_register,
        )
        peer.register_method(
            "ingress.endpoint.unregister",
            self._endpoint_unregister,
        )

    async def _runner_hello(
        self,
        params: HelloParams,
        request: Any,
    ) -> dict[str, Any]:
        _ = request
        return {
            "protocol_version": 1,
            "capabilities": list(params.capabilities),
        }

    async def _event_batch(
        self,
        params: EventBatchParams,
        request: Any,
    ) -> dict[str, Any]:
        _ = request
        self.events.extend(params.events)
        return EventBatchAck(
            batch_id=params.batch_id,
            accepted_event_ids=tuple(
                event.event_id for event in params.events
            ),
        ).to_mapping()

    async def _endpoint_register(
        self,
        params: EndpointParams,
        request: Any,
    ) -> dict[str, Any]:
        _ = request
        self.endpoint = params
        if self.endpoint.readiness == "ready":
            self.endpoint_ready.set()
        return {
            "status": "registered",
            "generation": self.endpoint.generation,
            "readiness": self.endpoint.readiness,
        }

    async def _endpoint_unregister(
        self,
        params: IdentityParams,
        request: Any,
    ) -> dict[str, Any]:
        _ = request
        self.endpoint = None
        return {
            "status": "unregistered",
            "generation": params.generation,
        }


def _identity_mapping(identity: FixtureIdentity) -> dict[str, Any]:
    return {
        "channel_key": identity.channel_key,
        "instance_id": identity.instance_id,
        "environment_spec_id": identity.environment_spec_id,
        "environment_id": identity.environment_id,
        "generation": identity.generation,
        "qwenpaw_version": identity.qwenpaw_version,
        "lock_sha256": identity.lock_sha256,
        "python_abi": identity.python_abi,
        "platform_tag": identity.platform_tag,
        "capabilities": list(identity.capabilities),
    }


@pytest.mark.asyncio
async def test_subprocess_separates_stdio_from_voice_ingress() -> None:
    """Native HTTP/WS frames never traverse the framed stdio pipe."""
    identity = FixtureIdentity(instance_id="chinst1_process-voice")
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
    transport = FramedTransport(process.stdout, process.stdin)
    peer = RpcPeer(transport)
    host = ProcessHost(peer)
    await peer.start()
    try:
        await peer.call(
            "channel.prepare",
            PrepareParams(
                channel_key=identity.channel_key,
                instance_id=identity.instance_id,
                generation=identity.generation,
                host_context=HostContext(
                    config_snapshot={
                        "twilio_account_sid": "AC-process",
                        "phone_number_sid": "PN-process",
                        "ingress_host": "127.0.0.1",
                        "ingress_port": 0,
                    },
                    secret_handle=f"secret-{identity.instance_id}",
                ),
                capabilities=identity.capabilities,
            ).to_mapping(),
        )
        lease = LeaseParams(
            channel_key=identity.channel_key,
            instance_id=identity.instance_id,
            generation=identity.generation,
            lease_token="lease-process",
            lease_ttl_ms=60_000,
        )
        await peer.call("channel.activate", lease.to_mapping())
        await peer.call("channel.commit", lease.to_mapping())
        await host.endpoint_ready.wait()
        endpoint = host.endpoint
        assert endpoint is not None
        local_base = f"http://127.0.0.1:{endpoint.port}"
        async with aiohttp.ClientSession() as client:
            form = {"CallSid": "CA-process", "From": "+1"}
            response = await client.post(
                f"{local_base}/voice/incoming",
                data=form,
                headers={
                    "X-Twilio-Signature": _signature(
                        "/voice/incoming",
                        form,
                    ),
                    "x-forwarded-proto": "https",
                    "x-forwarded-host": "voice.test",
                },
            )
            body = await response.text()
            match = re.search(r'url="([^"]+)"', body)
            assert match is not None
            token = parse_qs(urlparse(match.group(1)).query)["token"][0]
            websocket = await client.ws_connect(
                f"{local_base}/voice/ws?token={token}",
            )
            await websocket.send_json(
                {
                    "type": "setup",
                    "callSid": "CA-process",
                    "from": "+1",
                    "to": "+2",
                },
            )
            await websocket.send_json(
                {"type": "prompt", "voicePrompt": "process query"},
            )
            await _wait_until(
                lambda: any(
                    event.event_kind == "message.query"
                    for event in host.events
                ),
            )
            query = next(
                event
                for event in host.events
                if event.event_kind == "message.query"
            )
            result = await peer.call(
                "channel.send",
                SendParams(
                    channel_key=identity.channel_key,
                    instance_id=identity.instance_id,
                    generation=identity.generation,
                    delivery_id="process-delivery",
                    to_handle=query.metadata["session_binding"],
                    operation=OutboundOperation.MESSAGE_CREATE,
                    content_parts=({"type": "text", "text": "process reply"},),
                ).to_mapping(),
            )
            assert result["state"] == "acknowledged"
            assert await websocket.receive_json(timeout=2.0) == {
                "type": "text",
                "token": "process reply",
                "last": True,
            }
            await websocket.close()
        await peer.call(
            "channel.stop",
            IdentityParams(
                channel_key=identity.channel_key,
                instance_id=identity.instance_id,
                generation=identity.generation,
            ).to_mapping(),
        )
    finally:
        await peer.aclose()
        if process.returncode is None:
            process.terminate()
        await process.wait()
        assert process.stderr is not None
        stderr = (await process.stderr.read()).decode(errors="replace")
        assert AUTH_TOKEN not in stderr
