# -*- coding: utf-8 -*-
"""Task-local fixture Runner for the production VoiceDriver."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Mapping
import inspect
import json
import sys
from typing import Any

from qwenpaw.app.channels.voice.driver import VoiceDriver
from qwenpaw.app.channels.voice.platform import TunnelInfo, VoicePlatform
from qwenpaw.app.channels.voice.runner_twilio_manager import (
    VoiceWebhookSnapshot,
)
from qwenpaw.channel_protocol import (
    FixtureSecretHandleConsumer,
    HelloParams,
    RpcPeer,
    encode_frame,
)


AUTH_TOKEN = "fixture-twilio-auth-token"


class StdioTransport:
    """Cross-platform framed stdio adapter used only by this fixture."""

    def __init__(self) -> None:
        self._closed = False
        self._write_lock = asyncio.Lock()

    async def send(
        self,
        message: str,
        *,
        prepare_write: Callable[[], str | Awaitable[str]] | None = None,
        on_write_succeeded: Callable[[], None] | None = None,
        on_write_failed: Callable[[], None] | None = None,
        on_write_deferred: Callable[[], None] | None = None,
    ) -> None:
        """Write one complete protocol frame."""
        _ = on_write_deferred
        async with self._write_lock:
            if prepare_write is not None:
                message = prepare_write()
                if inspect.isawaitable(message):
                    message = await message
            try:
                await asyncio.to_thread(self._write, encode_frame(message))
            except Exception:
                if on_write_failed is not None:
                    result = on_write_failed()
                    if inspect.isawaitable(result):
                        await result
                raise
            if on_write_succeeded is not None:
                result = on_write_succeeded()
                if inspect.isawaitable(result):
                    await result

    async def receive(self) -> str:
        """Read one complete protocol frame."""
        if self._closed:
            raise ConnectionError("transport closed")
        return await asyncio.to_thread(self._read)

    async def aclose(self) -> None:
        """Stop accepting new reads."""
        self._closed = True

    @staticmethod
    def _write(frame: bytes) -> None:
        sys.stdout.buffer.write(frame)
        sys.stdout.buffer.flush()

    @staticmethod
    def _read() -> str:
        header = sys.stdin.buffer.readline()
        if not header:
            raise ConnectionError("stdin closed")
        if not header.lower().startswith(b"content-length:"):
            raise ValueError("invalid Content-Length header")
        length = int(header.split(b":", 1)[1].strip())
        if sys.stdin.buffer.readline() != b"\r\n":
            raise ValueError("invalid frame separator")
        body = sys.stdin.buffer.read(length)
        if len(body) != length:
            raise ConnectionError("truncated frame")
        return body.decode("utf-8")


class FixtureTunnel:
    """Expose a deterministic public URL without a network tunnel."""

    async def start(self, local_port: int) -> TunnelInfo:
        assert local_port > 0
        return TunnelInfo(
            public_url="https://voice.test",
            public_wss_url="wss://voice.test",
        )

    async def stop(self) -> None:
        return None


class FixtureRunnerTwilioManager:
    """Keep webhook state inside the fixture process."""

    def __init__(self) -> None:
        self.current = VoiceWebhookSnapshot()

    async def fetch_voice_webhook(
        self,
        phone_number_sid: str,
    ) -> VoiceWebhookSnapshot:
        assert phone_number_sid
        return self.current

    async def apply_voice_webhook(
        self,
        phone_number_sid: str,
        snapshot: VoiceWebhookSnapshot,
    ) -> None:
        assert phone_number_sid
        self.current = snapshot


class FixtureIdentity:
    """Attribute-based identity loaded before the Runner session."""

    def __init__(self, value: Mapping[str, Any]) -> None:
        for name, item in value.items():
            setattr(
                self,
                name,
                tuple(item) if name == "capabilities" else item,
            )


async def _run_session(
    driver: VoiceDriver,
    identity: FixtureIdentity,
) -> None:
    transport = StdioTransport()
    peer = RpcPeer(transport)
    driver.bind(peer, identity)
    handle = f"secret-{identity.instance_id}"
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
    controller.register_rpc_methods(peer)
    driver.attach_lifecycle(controller)
    await peer.start()
    try:
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
        await peer.call("runner.hello", hello.to_mapping())
        await peer.wait_closed()
    finally:
        await driver.stop()
        await peer.aclose()


async def _main() -> None:
    identity = FixtureIdentity(json.loads(sys.argv[1]))
    tunnel = FixtureTunnel()
    twilio = FixtureRunnerTwilioManager()
    driver = VoiceDriver(
        platform_factory=lambda: VoicePlatform(
            tunnel_factory=lambda: tunnel,
            twilio_manager_factory=lambda _sid, _token: twilio,
            shutdown_timeout_s=1.0,
        ),
    )
    await _run_session(driver, identity)


if __name__ == "__main__":
    asyncio.run(_main())
