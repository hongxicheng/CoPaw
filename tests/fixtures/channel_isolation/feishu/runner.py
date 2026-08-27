# -*- coding: utf-8 -*-
"""Task-local fixture Runner for the production FeishuDriver."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Mapping
from typing import Any

from qwenpaw.app.channels.feishu.driver import FeishuDriver
from qwenpaw.channel_protocol import FixtureSecretHandleConsumer


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


class FixtureFeishuDriver(FeishuDriver):
    """Inject deterministic platform and secret seams into the artifact."""

    def __init__(self) -> None:
        super().__init__(
            platform_factory=FixturePlatform,
            reconnect_initial_delay=0.001,
            reconnect_max_delay=0.005,
            connect_timeout=1.0,
        )

    def create_lifecycle_spec(
        self,
        identity: Any,
        *,
        secret_handle_consumer: Any | None,
    ) -> Any:
        """Supply fixture secrets without owning protocol identity."""
        if secret_handle_consumer is not None:
            raise RuntimeError("bootstrap secret consumer must be empty")
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
            self.consume_secret,
        )
        return super().create_lifecycle_spec(
            identity,
            secret_handle_consumer=consumer,
        )
