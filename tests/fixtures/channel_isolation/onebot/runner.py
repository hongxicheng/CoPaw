# -*- coding: utf-8 -*-
"""Task-local fixture Runner for the production OneBotDriver."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Mapping
import inspect
import json
import sys
from typing import Any

from qwenpaw.app.channels.onebot.driver import OneBotDriver
from qwenpaw.app.channels.onebot.platform import OneBotPlatform
from qwenpaw.channel_protocol import (
    FixtureSecretHandleConsumer,
    HelloParams,
    RpcPeer,
    encode_frame,
)


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
    driver: OneBotDriver,
    identity: FixtureIdentity,
) -> None:
    transport = StdioTransport()
    peer = RpcPeer(transport)
    driver.bind(peer, identity)
    handle = f"secret-{identity.instance_id}"
    consumer = FixtureSecretHandleConsumer(
        {
            (handle, identity.generation): {
                "access_token": "",
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
    driver = OneBotDriver(
        platform_factory=lambda: OneBotPlatform(
            watchdog_interval=0.05,
            api_timeout=1.0,
        ),
    )
    await _run_session(driver, identity)


if __name__ == "__main__":
    asyncio.run(_main())
