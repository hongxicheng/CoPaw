# -*- coding: utf-8 -*-
"""Task-local fixture Runner for the production VoiceDriver."""

from __future__ import annotations

from typing import Any

from qwenpaw.app.channels.voice.driver import VoiceDriver
from qwenpaw.app.channels.voice.platform import TunnelInfo, VoicePlatform
from qwenpaw.app.channels.voice.runner_twilio_manager import (
    VoiceWebhookSnapshot,
)
from qwenpaw.channel_protocol import (
    FixtureSecretHandleConsumer,
)


AUTH_TOKEN = "fixture-twilio-auth-token"


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


class FixtureVoiceDriver(VoiceDriver):
    """Inject deterministic platform and secret seams into the artifact."""

    def __init__(self) -> None:
        tunnel = FixtureTunnel()
        twilio = FixtureRunnerTwilioManager()
        super().__init__(
            platform_factory=lambda: VoicePlatform(
                tunnel_factory=lambda: tunnel,
                twilio_manager_factory=lambda _sid, _token: twilio,
                shutdown_timeout_s=1.0,
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
                    "twilio_auth_token": AUTH_TOKEN,
                },
            },
            self.consume_secret,
        )
        return super().create_lifecycle_spec(
            identity,
            secret_handle_consumer=consumer,
        )
