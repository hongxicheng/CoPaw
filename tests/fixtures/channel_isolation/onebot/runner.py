# -*- coding: utf-8 -*-
"""Task-local fixture Runner for the production OneBotDriver."""

from __future__ import annotations

from typing import Any

from qwenpaw.app.channels.onebot.driver import OneBotDriver
from qwenpaw.app.channels.onebot.platform import OneBotPlatform
from qwenpaw.channel_protocol import FixtureSecretHandleConsumer


class FixtureOneBotDriver(OneBotDriver):
    """Inject deterministic platform and secret seams into the artifact."""

    def __init__(self) -> None:
        super().__init__(
            platform_factory=lambda: OneBotPlatform(
                watchdog_interval=0.05,
                api_timeout=1.0,
            ),
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
                    "access_token": "",
                },
            },
            self.consume_secret,
        )
        return super().create_lifecycle_spec(
            identity,
            secret_handle_consumer=consumer,
        )
