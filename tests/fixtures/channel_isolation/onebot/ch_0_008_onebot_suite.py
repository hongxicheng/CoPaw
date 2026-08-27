# -*- coding: utf-8 -*-
"""Focused CH-0-008 OneBot Runner-owned ingress tests."""
# pylint: disable=protected-access

from __future__ import annotations

import asyncio
from dataclasses import dataclass
import ipaddress
import json
from pathlib import Path
import socket
import sys
from typing import Any

import aiohttp
import pytest

from qwenpaw.app.channels.onebot.driver import OneBotDriver
from qwenpaw.app.channels.onebot.platform import OneBotPlatform
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
    RpcPeer,
    SendParams,
)


PROCESS_FIXTURE = Path(__file__).with_name("runner.py")


@dataclass(frozen=True)
class FixtureIdentity:
    """Provide the immutable identity expected by a task-local Driver."""

    channel_key: str = "onebot"
    instance_id: str = "chinst1_fixture-onebot"
    environment_spec_id: str = "ches1_fixture-onebot"
    environment_id: str = "chenv1_fixture-onebot"
    generation: int = 1
    qwenpaw_version: str = "0.0.test"
    lock_sha256: str = "1" * 64
    python_abi: str = "cp313-cp313"
    platform_tag: str = "macosx_11_0_arm64"
    capabilities: tuple[str, ...] = ("ingress_endpoint",)


class MockHost:
    """Receive endpoint and event RPCs without sharing platform sockets."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, object, float | None]] = []
        self.endpoints: list[tuple[str, EndpointParams | IdentityParams]] = []
        self.events: list[Any] = []
        self.event_received = asyncio.Event()
        self.batch_started = asyncio.Event()
        self.release_batch = asyncio.Event()
        self.block_batches = False
        self.backpressure_remaining = 0
        self.endpoint_update_failures: list[str] = []

    async def call(
        self,
        method: str,
        params: object,
        *,
        timeout: float | None = None,
    ) -> dict[str, Any]:
        """Implement the Core methods used by the OneBot Driver."""
        self.calls.append((method, params, timeout))
        if method in {
            "ingress.endpoint.register",
            "ingress.endpoint.update",
        }:
            endpoint = EndpointParams.from_mapping(params)
            if method == "ingress.endpoint.update" and (
                self.endpoint_update_failures
            ):
                failure = self.endpoint_update_failures.pop(0)
                if failure == "after_apply":
                    self.endpoints.append((method, endpoint))
                raise RuntimeError(f"injected endpoint failure: {failure}")
            self.endpoints.append((method, endpoint))
            return {
                "status": method.rsplit(".", 1)[-1],
                "generation": endpoint.generation,
                "readiness": endpoint.readiness,
            }
        if method == "ingress.endpoint.unregister":
            identity = IdentityParams.from_mapping(params)
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
        return EventBatchAck(
            batch_id=batch.batch_id,
            accepted_event_ids=tuple(event.event_id for event in batch.events),
        ).to_mapping()


@dataclass
class Session:
    """Retain one active task-local Runner session."""

    driver: OneBotDriver
    controller: Any
    identity: FixtureIdentity
    host: MockHost
    lease: LeaseParams

    async def stop(self) -> None:
        """Stop the session without leaking listener tasks."""
        await self.controller.stop(self.identity_params())
        await asyncio.sleep(0)

    def identity_params(self) -> IdentityParams:
        return IdentityParams(
            channel_key=self.identity.channel_key,
            instance_id=self.identity.instance_id,
            generation=self.identity.generation,
        )


def _platform_factory(
    *,
    event_task_hard_cap: int = 500,
    watchdog_interval: float = 0.02,
) -> OneBotPlatform:
    return OneBotPlatform(
        event_task_hard_cap=event_task_hard_cap,
        watchdog_interval=watchdog_interval,
        api_timeout=1.0,
    )


async def _session(
    *,
    config: dict[str, Any] | None = None,
    access_token: str = "",
    commit: bool = True,
    event_task_hard_cap: int = 500,
    host: MockHost | None = None,
    identity: FixtureIdentity | None = None,
    retry_policy: RetryPolicy | None = None,
) -> Session:
    identity = identity or FixtureIdentity()
    host = host or MockHost()
    driver = OneBotDriver(
        platform_factory=lambda: _platform_factory(
            event_task_hard_cap=event_task_hard_cap,
        ),
        retry_policy=retry_policy,
    )
    driver.bind(host, identity)
    handle = f"secret-{identity.instance_id}-{identity.generation}"
    consumer = FixtureSecretHandleConsumer(
        {
            (handle, identity.generation): {
                "access_token": access_token,
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
        protocol_version=1,
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
                    "ws_host": "127.0.0.1",
                    "ws_port": 0,
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
    return Session(driver, controller, identity, host, lease)


def _ws_url(session: Session) -> str:
    platform = session.driver.platform
    assert platform is not None
    assert platform.listen_port is not None
    return f"ws://127.0.0.1:{platform.listen_port}/ws"


def _endpoint_ws_url(endpoint: EndpointParams) -> str:
    host = f"[{endpoint.host}]" if ":" in endpoint.host else endpoint.host
    return f"ws://{host}:{endpoint.port}{endpoint.path}"


async def _wait_until(predicate: Any, *, timeout: float = 2.0) -> None:
    """Wait for a deterministic asynchronous condition."""

    async def wait() -> None:
        while not predicate():
            await asyncio.sleep(0.005)

    await asyncio.wait_for(wait(), timeout=timeout)


def _message(
    message_id: int,
    text: str,
    *,
    user_id: int = 12345,
    group_id: int | None = None,
    segments: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    value: dict[str, Any] = {
        "post_type": "message",
        "message_type": "group" if group_id is not None else "private",
        "message_id": message_id,
        "user_id": user_id,
        "self_id": 99999,
        "sender": {"nickname": f"user-{user_id}"},
        "message": segments
        if segments is not None
        else [{"type": "text", "data": {"text": text}}],
    }
    if group_id is not None:
        value["group_id"] = group_id
    return value


@pytest.mark.asyncio
async def test_candidate_waits_for_commit_and_registers_dynamic_port() -> None:
    """A candidate neither binds nor accepts before the unique commit."""
    session = await _session(commit=False)
    try:
        platform = session.driver.platform
        assert platform is not None
        assert platform.listen_port is None
        assert session.host.endpoints == []

        await session.controller.commit(session.lease)

        assert platform.listen_port is not None
        method, endpoint = session.host.endpoints[-1]
        assert method == "ingress.endpoint.register"
        assert isinstance(endpoint, EndpointParams)
        assert endpoint.port == platform.listen_port
        assert endpoint.readiness == "ready"
        assert endpoint.bound_externally is False
        assert endpoint.auth_required is False
    finally:
        await session.stop()


@pytest.mark.asyncio
async def test_reverse_ws_reports_message_and_accepts_core_reply() -> None:
    """Native frames stay on the platform socket around stable DTOs."""
    session = await _session()
    try:
        async with aiohttp.ClientSession() as client:
            async with client.ws_connect(_ws_url(session)) as websocket:
                await websocket.send_json(_message(101, "hello"))
                await asyncio.wait_for(
                    session.host.event_received.wait(),
                    timeout=1.0,
                )
                event = session.host.events[-1]
                assert event.event_id == "onebot:message:101"
                assert event.sender_id == "12345"
                assert event.acl_sender_id == "12345"
                assert event.sender_name == "user-12345"
                assert event.conversation == {
                    "id": "12345",
                    "type": "dm",
                    "thread_id": None,
                }
                assert event.content_parts == (
                    {"type": "text", "text": "hello"},
                )
                raw_frames = [
                    params
                    for method, params, _ in session.host.calls
                    if method == "event.batch"
                ]
                assert len(raw_frames) == 1
                assert "post_type" not in json.dumps(raw_frames[0])

                send = asyncio.create_task(
                    session.controller.send(
                        SendParams(
                            channel_key=session.identity.channel_key,
                            instance_id=session.identity.instance_id,
                            generation=session.identity.generation,
                            delivery_id="delivery-1",
                            to_handle="12345",
                            operation=OutboundOperation.MESSAGE_CREATE,
                            content_parts=({"type": "text", "text": "reply"},),
                        ),
                    ),
                )
                action = json.loads(await websocket.receive_str())
                assert action["action"] == "send_private_msg"
                assert action["params"]["user_id"] == 12345
                await websocket.send_json(
                    {
                        "retcode": 0,
                        "data": {"message_id": 202},
                        "echo": action["echo"],
                    },
                )
                result = await send
                assert result["state"] == "acknowledged"
    finally:
        await session.stop()


@pytest.mark.asyncio
async def test_external_binding_requires_token_and_reports_auth() -> None:
    """Non-loopback listeners reject missing tokens and report exposure."""
    missing = await _session(config={"ws_host": "0.0.0.0"})
    try:
        method, endpoint = missing.host.endpoints[-1]
        assert method == "ingress.endpoint.register"
        assert isinstance(endpoint, EndpointParams)
        assert endpoint.bound_externally is True
        assert endpoint.auth_required is True
        async with aiohttp.ClientSession() as client:
            response = await client.get(_ws_url(missing))
            assert response.status == 401
            await response.release()
    finally:
        await missing.stop()

    protected = await _session(
        config={"ws_host": "0.0.0.0"},
        access_token="secret-token",
    )
    try:
        async with aiohttp.ClientSession() as client:
            with pytest.raises(aiohttp.WSServerHandshakeError):
                await client.ws_connect(_ws_url(protected))
            websocket = await client.ws_connect(
                _ws_url(protected),
                headers={"Authorization": "Bearer secret-token"},
            )
            await websocket.close()
    finally:
        await protected.stop()


@pytest.mark.asyncio
async def test_loopback_token_is_enforced_and_reported() -> None:
    """A configured loopback token is distinct from listener exposure."""
    session = await _session(access_token="secret-token")
    try:
        _, endpoint = session.host.endpoints[-1]
        assert isinstance(endpoint, EndpointParams)
        assert endpoint.bound_externally is False
        assert endpoint.auth_required is True
        async with aiohttp.ClientSession() as client:
            with pytest.raises(aiohttp.WSServerHandshakeError):
                await client.ws_connect(_ws_url(session))
            websocket = await client.ws_connect(
                _ws_url(session),
                headers={"Authorization": "Token secret-token"},
            )
            await websocket.close()
    finally:
        await session.stop()


@pytest.mark.asyncio
async def test_outbound_file_preserves_surrounding_content_order() -> None:
    """A file operation cannot overtake text on either side of it."""
    identity = FixtureIdentity(
        capabilities=("ingress_endpoint", "media"),
    )
    session = await _session(identity=identity)
    try:
        async with aiohttp.ClientSession() as client:
            async with client.ws_connect(_ws_url(session)) as websocket:
                send = asyncio.create_task(
                    session.controller.send(
                        SendParams(
                            channel_key=session.identity.channel_key,
                            instance_id=session.identity.instance_id,
                            generation=session.identity.generation,
                            delivery_id="delivery-ordered-parts",
                            to_handle="12345",
                            operation=OutboundOperation.MESSAGE_CREATE,
                            content_parts=(
                                {"type": "text", "text": "before"},
                                {
                                    "type": "file",
                                    "file_url": "https://files.test/a.txt",
                                    "filename": "a.txt",
                                },
                                {"type": "text", "text": "after"},
                            ),
                        ),
                    ),
                )

                before = json.loads(
                    await asyncio.wait_for(
                        websocket.receive_str(),
                        timeout=1.0,
                    ),
                )
                assert before["action"] == "send_private_msg"
                assert before["params"]["message"] == [
                    {"type": "text", "data": {"text": "before"}},
                ]
                await websocket.send_json(
                    {"retcode": 0, "data": {}, "echo": before["echo"]},
                )

                file_action = json.loads(
                    await asyncio.wait_for(
                        websocket.receive_str(),
                        timeout=1.0,
                    ),
                )
                assert file_action["action"] == "upload_private_file"
                assert file_action["params"]["file"] == (
                    "https://files.test/a.txt"
                )
                await websocket.send_json(
                    {
                        "retcode": 0,
                        "data": {},
                        "echo": file_action["echo"],
                    },
                )

                after = json.loads(
                    await asyncio.wait_for(
                        websocket.receive_str(),
                        timeout=1.0,
                    ),
                )
                assert after["action"] == "send_private_msg"
                assert after["params"]["message"] == [
                    {"type": "text", "data": {"text": "after"}},
                ]
                await websocket.send_json(
                    {"retcode": 0, "data": {}, "echo": after["echo"]},
                )

                result = await send
                assert result["state"] == "acknowledged"
    finally:
        await session.stop()


@pytest.mark.asyncio
async def test_dynamic_rebind_updates_core_to_actual_port() -> None:
    """Watchdog recovery updates Core after a dynamic port changes."""
    session = await _session()
    try:
        platform = session.driver.platform
        assert platform is not None
        first_port = platform.listen_port
        assert first_port is not None
        site = platform._site
        assert site is not None
        await site.stop()
        await _wait_until(
            lambda: any(
                method == "ingress.endpoint.update"
                and isinstance(endpoint, EndpointParams)
                and endpoint.readiness == "ready"
                for method, endpoint in session.host.endpoints
            ),
        )
        assert platform.listen_port is not None
        _, endpoint = session.host.endpoints[-1]
        assert isinstance(endpoint, EndpointParams)
        assert endpoint.readiness == "ready"
        assert endpoint.port == platform.listen_port
        assert session.driver.diagnostics()["platform_rebind_total"] == 1
    finally:
        await session.stop()


@pytest.mark.asyncio
async def test_endpoint_update_failures_are_reconciled() -> None:
    """Transient and response-lost updates cannot stop recovery."""
    host = MockHost()
    session = await _session(host=host)
    try:
        platform = session.driver.platform
        assert platform is not None
        site = platform._site
        assert site is not None
        host.endpoint_update_failures.extend(
            ["before_apply", "after_apply"],
        )

        await site.stop()
        await _wait_until(
            lambda: not host.endpoint_update_failures
            and platform.health_snapshot()["accepting"] is True
            and any(
                method == "ingress.endpoint.update"
                and isinstance(endpoint, EndpointParams)
                and endpoint.readiness == "ready"
                and endpoint.port == platform.listen_port
                for method, endpoint in host.endpoints
            ),
        )

        watchdog = platform._watchdog_task
        assert watchdog is not None
        assert watchdog.done() is False
        _, endpoint = host.endpoints[-1]
        assert isinstance(endpoint, EndpointParams)
        async with aiohttp.ClientSession() as client:
            websocket = await client.ws_connect(_endpoint_ws_url(endpoint))
            await websocket.close()
    finally:
        await session.stop()


@pytest.mark.asyncio
async def test_port_conflict_recovers_with_endpoint_update() -> None:
    """A conflicted explicit port becomes ready after the owner releases it."""
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.bind(("127.0.0.1", 0))
    listener.listen()
    port = int(listener.getsockname()[1])
    session: Session | None = None
    try:
        session = await _session(config={"ws_port": port})
        platform = session.driver.platform
        assert platform is not None
        assert platform.listen_port is None
        method, endpoint = session.host.endpoints[-1]
        assert method == "ingress.endpoint.register"
        assert isinstance(endpoint, EndpointParams)
        assert endpoint.readiness == "degraded"
        assert endpoint.port == port

        listener.close()
        await _wait_until(
            lambda: any(
                method == "ingress.endpoint.update"
                and isinstance(endpoint, EndpointParams)
                and endpoint.readiness == "ready"
                and endpoint.port == port
                for method, endpoint in session.host.endpoints
            ),
        )
        assert platform.listen_port == port
        assert session.driver.diagnostics()["platform_rebind_total"] == 1
    finally:
        listener.close()
        if session is not None:
            await session.stop()


def _ipv6_loopback_available() -> bool:
    try:
        with socket.socket(socket.AF_INET6, socket.SOCK_STREAM) as listener:
            listener.bind(("::1", 0))
    except OSError:
        return False
    return True


@pytest.mark.asyncio
@pytest.mark.parametrize("host", ["localhost", "127.0.0.1", "::1"])
async def test_dynamic_loopback_reports_one_actual_socket(host: str) -> None:
    """A dynamic endpoint identifies one actual loopback listener."""
    if host == "::1" and not _ipv6_loopback_available():
        pytest.skip("IPv6 loopback is unavailable")
    session = await _session(config={"ws_host": host})
    try:
        platform = session.driver.platform
        assert platform is not None
        site = platform._site
        assert site is not None
        server = site._server
        assert server is not None
        ports = {
            int(bound_socket.getsockname()[1])
            for bound_socket in server.sockets or ()
        }
        assert ports == {platform.listen_port}
        assert len(server.sockets or ()) == 1

        _, endpoint = session.host.endpoints[-1]
        assert isinstance(endpoint, EndpointParams)
        assert ipaddress.ip_address(endpoint.host).is_loopback
        assert endpoint.port == platform.listen_port
        async with aiohttp.ClientSession() as client:
            websocket = await client.ws_connect(_endpoint_ws_url(endpoint))
            await websocket.close()
    finally:
        await session.stop()


@pytest.mark.asyncio
async def test_runner_cap_and_core_batch_pressure_are_distinct() -> None:
    """Runner overload and protocol backpressure have separate counters."""
    blocked_host = MockHost()
    blocked_host.block_batches = True
    blocked = await _session(
        host=blocked_host,
        event_task_hard_cap=1,
    )
    try:
        async with aiohttp.ClientSession() as client:
            async with client.ws_connect(_ws_url(blocked)) as websocket:
                await websocket.send_json(_message(301, "first"))
                await asyncio.wait_for(
                    blocked_host.batch_started.wait(),
                    timeout=1.0,
                )
                await websocket.send_json(_message(302, "second"))
                await _wait_until(
                    lambda: blocked.driver.diagnostics()[
                        "runner_event_dropped_total"
                    ]
                    == 1,
                )
                diagnostics = blocked.driver.diagnostics()
                assert diagnostics["runner_event_dropped_total"] == 1
                assert diagnostics["core_batch_backpressure_total"] == 0
                blocked_host.release_batch.set()
    finally:
        blocked_host.release_batch.set()
        await blocked.stop()

    pressure_host = MockHost()
    pressure_host.backpressure_remaining = 1
    pressured = await _session(
        host=pressure_host,
        retry_policy=RetryPolicy(
            initial_delay=0.001,
            max_delay=0.002,
            max_attempts=3,
        ),
    )
    try:
        async with aiohttp.ClientSession() as client:
            async with client.ws_connect(_ws_url(pressured)) as websocket:
                await websocket.send_json(_message(303, "retry"))
                await asyncio.wait_for(
                    pressure_host.event_received.wait(),
                    timeout=1.0,
                )
        diagnostics = pressured.driver.diagnostics()
        assert diagnostics["runner_event_dropped_total"] == 0
        assert diagnostics["core_batch_backpressure_total"] == 1
        assert diagnostics["core_batch_retry_total"] == 1
        batch_calls = [
            call for call in pressure_host.calls if call[0] == "event.batch"
        ]
        assert len(batch_calls) == 2
        assert batch_calls[0][1] == batch_calls[1][1]
    finally:
        await pressured.stop()


@pytest.mark.asyncio
async def test_quoted_api_roundtrip_does_not_block_ws_reader() -> None:
    """Echo-dependent quote expansion leaves the native read loop free."""
    session = await _session(config={"require_mention": True})
    try:
        async with aiohttp.ClientSession() as client:
            async with client.ws_connect(_ws_url(session)) as websocket:
                await websocket.send_json(
                    _message(
                        401,
                        "",
                        group_id=777,
                        segments=[
                            {"type": "reply", "data": {"id": "88"}},
                            {"type": "at", "data": {"qq": "99999"}},
                            {
                                "type": "text",
                                "data": {"text": "current"},
                            },
                        ],
                    ),
                )
                action = json.loads(await websocket.receive_str())
                assert action["action"] == "get_msg"

                await websocket.send_json(_message(402, "second"))
                await _wait_until(
                    lambda: any(
                        event.event_id == "onebot:message:402"
                        for event in session.host.events
                    ),
                )
                await websocket.send_json(
                    {
                        "retcode": 0,
                        "data": {
                            "message": [
                                {
                                    "type": "file",
                                    "data": {
                                        "file": "quote.txt",
                                        "file_id": "quoted-file",
                                        "name": "quote.txt",
                                    },
                                },
                            ],
                        },
                        "echo": action["echo"],
                    },
                )
                file_action = json.loads(await websocket.receive_str())
                assert file_action["action"] == "get_group_file_url"
                assert file_action["params"] == {
                    "group_id": 777,
                    "file_id": "quoted-file",
                }
                await websocket.send_json(
                    {
                        "retcode": 0,
                        "data": {
                            "url": "https://files.test/quote.txt",
                        },
                        "echo": file_action["echo"],
                    },
                )
                await _wait_until(
                    lambda: any(
                        event.event_id == "onebot:message:401"
                        for event in session.host.events
                    ),
                )
        quoted = next(
            event
            for event in session.host.events
            if event.event_id == "onebot:message:401"
        )
        assert quoted.content_parts == (
            {"type": "text", "text": "[Quoted message]"},
            {
                "type": "text",
                "text": "[Quoted file message: quote.txt]",
            },
            {
                "type": "file",
                "file_url": "https://files.test/quote.txt",
                "filename": "quote.txt",
            },
            {"type": "text", "text": "[Current message]"},
            {"type": "text", "text": "current"},
        )
        event_calls = [
            call for call in session.host.calls if call[0] == "event.batch"
        ]
        assert event_calls
        assert all(call[2] == 60.0 for call in event_calls)
    finally:
        await session.stop()


def _unused_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


@pytest.mark.asyncio
async def test_quiesce_releases_listener_before_outbound_drain() -> None:
    """Quiesce rejects new sockets while an existing echo can finish."""
    port = _unused_port()
    session = await _session(config={"ws_port": port})
    send: asyncio.Task[dict[str, Any]] | None = None
    quiesce: asyncio.Task[dict[str, Any]] | None = None
    try:
        async with aiohttp.ClientSession() as client:
            async with client.ws_connect(_ws_url(session)) as websocket:
                send = asyncio.create_task(
                    session.controller.send(
                        SendParams(
                            channel_key=session.identity.channel_key,
                            instance_id=session.identity.instance_id,
                            generation=session.identity.generation,
                            delivery_id="delivery-quiesce-drain",
                            to_handle="12345",
                            operation=OutboundOperation.MESSAGE_CREATE,
                            content_parts=(
                                {"type": "text", "text": "pending"},
                            ),
                        ),
                    ),
                )
                action = json.loads(await websocket.receive_str())
                quiesce = asyncio.create_task(
                    session.controller.quiesce(
                        QuiesceParams(
                            channel_key=session.identity.channel_key,
                            instance_id=session.identity.instance_id,
                            generation=session.identity.generation,
                            drain_timeout_ms=5_000,
                        ),
                    ),
                )
                platform = session.driver.platform
                assert platform is not None
                await _wait_until(
                    lambda: platform.listen_port is None
                    and platform.health_snapshot()["accepting"] is False,
                    timeout=1.0,
                )

                await websocket.send_json(
                    {"retcode": 0, "data": {}, "echo": action["echo"]},
                )
                result = await send
                assert result["state"] == "acknowledged"
                await asyncio.wait_for(quiesce, timeout=5.0)
    finally:
        for task in (send, quiesce):
            if task is not None and not task.done():
                task.cancel()
        await asyncio.gather(
            *(task for task in (send, quiesce) if task is not None),
            return_exceptions=True,
        )
        await session.stop()


@pytest.mark.asyncio
async def test_explicit_port_handoff_disconnect_and_shutdown() -> None:
    """Quiesce hands one explicit port to the committed replacement."""
    port = _unused_port()
    first = await _session(config={"ws_port": port})
    second = await _session(
        config={"ws_port": port},
        commit=False,
        identity=FixtureIdentity(
            instance_id=first.identity.instance_id,
            generation=2,
        ),
    )
    websocket: aiohttp.ClientWebSocketResponse | None = None
    try:
        platform = second.driver.platform
        assert platform is not None
        assert platform.listen_port is None
        client = aiohttp.ClientSession()
        websocket = await client.ws_connect(_ws_url(first))
        await first.controller.quiesce(
            QuiesceParams(
                channel_key=first.identity.channel_key,
                instance_id=first.identity.instance_id,
                generation=first.identity.generation,
                drain_timeout_ms=1_000,
            ),
        )
        message = await websocket.receive(timeout=1.0)
        assert message.type in {
            aiohttp.WSMsgType.CLOSE,
            aiohttp.WSMsgType.CLOSED,
        }
        await second.controller.commit(second.lease)
        assert platform.listen_port == port
        assert any(
            method == "ingress.endpoint.unregister"
            for method, _ in first.host.endpoints
        )
        await client.close()
    finally:
        if websocket is not None and not websocket.closed:
            await websocket.close()
        await first.stop()
        await second.stop()


class HangingConnection:
    """Model a client that never completes its close handshake."""

    def __init__(self) -> None:
        self.close_started = asyncio.Event()
        self.force_closed = False

    async def close(self) -> None:
        self.close_started.set()
        await asyncio.Event().wait()

    def force_close(self) -> None:
        self.force_closed = True


class RecordingTransport:
    """Record forced transport closure after the shared deadline."""

    def __init__(self) -> None:
        self.closed = False

    def close(self) -> None:
        self.closed = True


@pytest.mark.asyncio
async def test_quiesce_deadline_forces_unresponsive_connections() -> None:
    """The wire drain deadline also bounds platform connection cleanup."""
    session = await _session()
    platform = session.driver.platform
    assert platform is not None
    first = HangingConnection()
    second = HangingConnection()
    first_transport = RecordingTransport()
    second_transport = RecordingTransport()
    platform_any: Any = platform
    platform_any._connections.update({first, second})
    platform_any._connection_transports.update(
        {
            first: first_transport,
            second: second_transport,
        },
    )
    started = asyncio.get_running_loop().time()
    try:
        await session.controller.quiesce(
            QuiesceParams(
                channel_key=session.identity.channel_key,
                instance_id=session.identity.instance_id,
                generation=session.identity.generation,
                drain_timeout_ms=50,
            ),
        )
    finally:
        await session.stop()
    elapsed = asyncio.get_running_loop().time() - started

    assert elapsed < 0.2
    assert first.close_started.is_set()
    assert second.close_started.is_set()
    assert first_transport.closed is True
    assert second_transport.closed is True


@pytest.mark.asyncio
async def test_shutdown_is_bounded_concurrent_and_cancellation_safe() -> None:
    """One total deadline settles multiple unresponsive connections."""
    platform = OneBotPlatform(
        watchdog_interval=60.0,
        shutdown_timeout=0.05,
    )
    await platform.prepare(
        {"ws_host": "127.0.0.1", "ws_port": 0},
        {"access_token": ""},
    )
    await platform.start(
        lambda _: asyncio.sleep(0),
        lambda _operation, _endpoint: asyncio.sleep(0),
    )
    first = HangingConnection()
    second = HangingConnection()
    first_transport = RecordingTransport()
    second_transport = RecordingTransport()
    platform_any: Any = platform
    platform_any._connections.update({first, second})
    platform_any._connection_transports.update(
        {
            first: first_transport,
            second: second_transport,
        },
    )
    pending: asyncio.Future[
        dict[str, Any]
    ] = asyncio.get_running_loop().create_future()
    platform_any._pending_calls["pending"] = pending

    close = asyncio.create_task(platform.close())
    await asyncio.wait_for(
        asyncio.gather(
            first.close_started.wait(),
            second.close_started.wait(),
        ),
        timeout=0.2,
    )
    started = asyncio.get_running_loop().time()
    close.cancel()
    with pytest.raises(asyncio.CancelledError):
        await close
    elapsed = asyncio.get_running_loop().time() - started

    assert elapsed < 0.2
    assert first_transport.closed is True
    assert second_transport.closed is True
    assert pending.cancelled()
    assert platform.listen_port is None
    assert platform.health_snapshot()["connection_count"] == 0


class ProcessHost:
    """Core-side RPC handlers for the subprocess Runner proof."""

    def __init__(self, peer: RpcPeer) -> None:
        self.hello = asyncio.Event()
        self.endpoint_ready = asyncio.Event()
        self.event_received = asyncio.Event()
        self.endpoint: EndpointParams | None = None
        self.events: list[Any] = []
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
        _: object,
    ) -> dict[str, Any]:
        self.hello.set()
        return {
            "protocol_version": 1,
            "capabilities": list(params.capabilities),
        }

    async def _event_batch(
        self,
        params: EventBatchParams,
        _: object,
    ) -> dict[str, Any]:
        self.events.extend(params.events)
        self.event_received.set()
        return EventBatchAck(
            batch_id=params.batch_id,
            accepted_event_ids=tuple(
                event.event_id for event in params.events
            ),
        ).to_mapping()

    async def _endpoint_register(
        self,
        params: EndpointParams,
        _: object,
    ) -> dict[str, Any]:
        self.endpoint = params
        if params.readiness == "ready":
            self.endpoint_ready.set()
        return {
            "status": "registered",
            "generation": params.generation,
            "readiness": params.readiness,
        }

    async def _endpoint_unregister(
        self,
        params: IdentityParams,
        _: object,
    ) -> dict[str, Any]:
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
async def test_subprocess_separates_stdio_from_platform_ingress() -> None:
    """External WebSocket traffic never traverses the framed stdio pipe."""
    identity = FixtureIdentity(instance_id="chinst1_process-onebot")
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
    host = ProcessHost(core)
    await core.start()
    websocket: aiohttp.ClientWebSocketResponse | None = None
    try:
        await asyncio.wait_for(host.hello.wait(), timeout=2.0)
        await core.call(
            "channel.prepare",
            PrepareParams(
                channel_key=identity.channel_key,
                instance_id=identity.instance_id,
                generation=identity.generation,
                host_context=HostContext(
                    config_snapshot={
                        "ws_host": "127.0.0.1",
                        "ws_port": 0,
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
        await core.call("channel.activate", lease.to_mapping())
        await core.call("channel.commit", lease.to_mapping())
        await asyncio.wait_for(host.endpoint_ready.wait(), timeout=2.0)
        assert host.endpoint is not None
        assert host.endpoint.port > 0

        client = aiohttp.ClientSession()
        websocket = await client.ws_connect(
            f"ws://127.0.0.1:{host.endpoint.port}/ws",
        )
        await websocket.send_json(_message(901, "process ingress"))
        await asyncio.wait_for(host.event_received.wait(), timeout=2.0)
        assert host.events[0].event_id == "onebot:message:901"

        send = asyncio.create_task(
            core.call(
                "channel.send",
                SendParams(
                    channel_key=identity.channel_key,
                    instance_id=identity.instance_id,
                    generation=identity.generation,
                    delivery_id="process-delivery",
                    to_handle="12345",
                    operation=OutboundOperation.MESSAGE_CREATE,
                    content_parts=({"type": "text", "text": "process reply"},),
                ).to_mapping(),
            ),
        )
        action = json.loads(await websocket.receive_str())
        assert action["action"] == "send_private_msg"
        await websocket.send_json(
            {
                "retcode": 0,
                "data": {"message_id": 902},
                "echo": action["echo"],
            },
        )
        result = await send
        assert result["state"] == "acknowledged"
        await core.call(
            "channel.stop",
            IdentityParams(
                channel_key=identity.channel_key,
                instance_id=identity.instance_id,
                generation=identity.generation,
            ).to_mapping(),
        )
        await client.close()
    finally:
        if websocket is not None and not websocket.closed:
            await websocket.close()
        await core.aclose()
        try:
            await asyncio.wait_for(process.wait(), timeout=2.0)
        except asyncio.TimeoutError:
            process.terminate()
            await process.wait()
        if process.returncode != 0:
            assert process.stderr is not None
            stderr = await process.stderr.read()
            raise AssertionError(stderr.decode(errors="replace"))
