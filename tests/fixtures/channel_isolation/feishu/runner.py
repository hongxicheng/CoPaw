# -*- coding: utf-8 -*-
"""Task-local fixture Runner for the production FeishuDriver."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Mapping
import inspect
import json
import sys
from typing import Any

from qwenpaw.app.channels.feishu.driver import FeishuDriver
from qwenpaw.channel_protocol import FixtureSecretHandleConsumer, encode_frame


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
                    on_write_failed()
                raise
            if on_write_succeeded is not None:
                on_write_succeeded()

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


class FixturePlatform:
    """Deterministic platform boundary for subprocess isolation tests."""

    def __init__(self) -> None:
        self._config: dict[str, Any] = {}
        self._on_message: Callable[[object], Awaitable[None]] | None = None
        self._disconnected = asyncio.Event()
        self._emit_task: asyncio.Task[None] | None = None
        self._connected = False

    async def prepare(
        self,
        config: dict[str, Any],
        secret: dict[str, str],
        media_work_dir: Any,
    ) -> None:
        """Validate fixture inputs without logging secret values."""
        assert secret["app_secret"].startswith("fixture-secret-")
        assert secret["encrypt_key"].startswith("fixture-encrypt-")
        assert secret["verification_token"].startswith("fixture-token-")
        assert media_work_dir.is_absolute()
        self._config = dict(config)

    async def connect(
        self,
        on_message: Callable[[object], Awaitable[None]],
        on_card: Callable[[object], Awaitable[None]],
    ) -> None:
        """Start one active fixture connection."""
        _ = on_card
        self._on_message = on_message
        self._connected = True
        self._disconnected = asyncio.Event()
        event = self._config.get("fixture_event")
        if isinstance(event, Mapping) and event:
            self._emit_task = asyncio.create_task(self._emit(dict(event)))

    async def _emit(self, event: dict[str, Any]) -> None:
        await asyncio.sleep(0)
        if self._on_message is not None:
            await self._on_message(event)

    async def wait_disconnected(self) -> None:
        """Wait until the fixture is stopped."""
        await self._disconnected.wait()

    async def disconnect(self) -> None:
        """Stop the active fixture connection."""
        self._connected = False
        self._disconnected.set()
        if self._emit_task is not None:
            await asyncio.gather(self._emit_task, return_exceptions=True)

    async def close(self) -> None:
        """Release fixture resources."""
        await self.disconnect()

    async def send_message(
        self,
        receive_id_type: str,
        receive_id: str,
        content_parts: tuple[dict[str, Any], ...],
        *,
        reply_message_id: str = "",
    ) -> str:
        """Return a deterministic message identifier."""
        assert receive_id_type in {"chat_id", "open_id"}
        assert receive_id
        assert content_parts
        _ = reply_message_id
        return f"fixture-message-{receive_id}"

    async def health_snapshot(self) -> dict[str, Any]:
        """Return deterministic local connection health."""
        return {"connected": self._connected}

    async def send_approval(
        self,
        receive_id_type: str,
        receive_id: str,
        approval: Mapping[str, str],
        fallback_text: str,
    ) -> str:
        """Return a deterministic approval identifier."""
        _ = receive_id_type, approval, fallback_text
        return f"fixture-approval-{receive_id}"

    async def start_stream(
        self,
        receive_id_type: str,
        receive_id: str,
        text: str,
    ) -> dict[str, str]:
        """Return a deterministic streaming target."""
        _ = receive_id_type, receive_id, text
        return {
            "message_id": "fixture-stream-message",
            "card_id": "fixture-stream-card",
        }

    async def update_stream(
        self,
        target: Mapping[str, str],
        text: str,
        sequence: int,
        *,
        final: bool,
    ) -> bool:
        """Accept a fixture streaming update."""
        _ = target, text, sequence, final
        return True

    async def add_reaction(
        self,
        message_id: str,
        emoji_type: str = "DONE",
    ) -> bool:
        """Accept the legacy completed reaction."""
        return bool(message_id and emoji_type == "DONE")


async def _run_session(
    driver: FeishuDriver,
    transport: StdioTransport,
    identity: Any,
    secret_handle_consumer: Any,
) -> None:
    from qwenpaw.channel_protocol import HelloParams, RpcPeer

    peer = RpcPeer(transport)
    driver.bind(peer, identity)
    controller = driver.create_lifecycle_controller(
        identity,
        secret_handle_consumer=secret_handle_consumer,
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


class FixtureIdentity:
    """Attribute-based identity used by the task-local Runner."""

    def __init__(self, value: Mapping[str, Any]) -> None:
        for name, item in value.items():
            setattr(
                self,
                name,
                tuple(item) if name == "capabilities" else item,
            )


async def _main() -> None:
    identity = FixtureIdentity(json.loads(sys.argv[1]))
    driver = FeishuDriver(
        platform_factory=FixturePlatform,
        reconnect_initial_delay=0.001,
        reconnect_max_delay=0.005,
        connect_timeout=1.0,
    )
    handle = f"secret-{identity.instance_id}"
    consumer = FixtureSecretHandleConsumer(
        {
            (handle, identity.generation): {
                "app_secret": f"fixture-secret-{identity.instance_id}",
                "encrypt_key": f"fixture-encrypt-{identity.instance_id}",
                "verification_token": (
                    f"fixture-token-{identity.instance_id}"
                ),
            },
        },
        driver.consume_secret,
    )
    await _run_session(
        driver,
        StdioTransport(),
        identity,
        consumer,
    )


if __name__ == "__main__":
    asyncio.run(_main())
