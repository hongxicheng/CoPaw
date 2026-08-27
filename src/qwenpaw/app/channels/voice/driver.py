# -*- coding: utf-8 -*-
"""Runner-owned Voice ChannelDriver."""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Mapping
import logging
from typing import Any

from ....channel_protocol import (
    DeliveryState,
    EndpointParams,
    EventBatchAck,
    EventBatchParams,
    HostContext,
    IdentityParams,
    InboundEvent,
    OutboundOperation,
    PlatformAuthenticationError,
    RetryPolicy,
    RpcClosedError,
    RpcError,
    RpcTimeoutError,
    SendParams,
    VoiceEvent,
    VoiceEventKind,
    events_for_retry,
)
from ....channel_protocol.lifecycle import LifecycleController, RunnerState
from ....channel_protocol.runner_host import RunnerLifecycleSpec
from ....channel_protocol.rpc import RpcResponsePublication
from .platform import (
    VoiceEndpoint,
    VoiceNativeEvent,
    VoicePlatform,
    VoicePlatformError,
)


logger = logging.getLogger(__name__)

_EVENT_BATCH_TIMEOUT_S = 60.0


class _VoiceLifecycleController(LifecycleController):
    """Invoke Voice Driver work at frozen lifecycle boundaries."""

    def __init__(self, driver: "VoiceDriver", **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._driver = driver
        self.host_context: HostContext | None
        self.endpoint: EndpointParams | None

    async def prepare(self, params: Any) -> dict[str, Any]:
        result = await super().prepare(params)
        host_context = self.host_context
        if host_context is None:
            raise RuntimeError("prepared host context is unavailable")
        try:
            await self._driver.prepare(host_context)
        except PlatformAuthenticationError as exc:
            self.host_context = None
            self.effective_capabilities = frozenset()
            self.state = RunnerState.FAILED
            raise self._secret_handle_error(
                "PLATFORM_AUTH_FAILED",
            ) from exc
        except Exception:
            self.host_context = None
            self.effective_capabilities = frozenset()
            self.state = RunnerState.FAILED
            raise
        return result

    async def commit(self, params: Any) -> dict[str, Any]:
        result = await self._commit(params, defer_publication=False)
        if isinstance(result, RpcResponsePublication):
            raise RuntimeError("direct Voice commit cannot be deferred")
        return result

    def _rpc_commit(
        self,
        params: Any,
    ) -> Any:
        """Return a commit bound to JSON-RPC response publication."""
        return self._commit(params, defer_publication=True)

    async def _commit(
        self,
        params: Any,
        *,
        defer_publication: bool,
    ) -> dict[str, Any] | RpcResponsePublication:
        result = await super().commit(params)
        try:
            await self._driver.commit()
        except BaseException:
            await self._abort_commit("STARTUP_FAILED")
            raise
        if defer_publication:
            return self._commit_publication(result)
        self._driver.confirm_commit()
        return result

    def _commit_publication(
        self,
        result: dict[str, Any],
    ) -> RpcResponsePublication:
        """Retain rollback ownership until the commit response is accepted."""

        def on_published() -> None:
            self._driver.confirm_commit()

        publication: RpcResponsePublication

        def on_write_failed() -> Any:
            self._mark_commit_failed("COMMIT_RESPONSE_WRITE_FAILED")
            if publication.deferred:
                return self._abort_commit(
                    "COMMIT_RESPONSE_WRITE_FAILED",
                )
            return None

        publication = RpcResponsePublication(
            result=result,
            on_prepare=lambda: result,
            on_published=on_published,
            on_write_failed=on_write_failed,
            on_write_deferred=lambda: None,
            on_aborted=self._abort_commit,
        )
        return publication

    def _mark_commit_failed(self, reason_code: str) -> None:
        """Synchronously fence a response that failed publication."""
        self.state = RunnerState.FAILED
        self.lease_token = None
        self.lease_expires_at_ms = None
        self._driver.mark_commit_failed(reason_code)

    async def _abort_commit(self, reason_code: str) -> None:
        """Fence lifecycle state and await Voice startup compensation."""
        self._mark_commit_failed(reason_code)
        await self._driver.abort_commit(reason_code)

    async def withdraw_voice_endpoint(self, params: IdentityParams) -> None:
        """Detach and synchronously publish Voice endpoint withdrawal."""
        async with self._lock:
            self._check_identity(params)
            had_endpoint = self.endpoint is not None
            self.endpoint = None
            endpoint_handler = self._endpoint_handler if had_endpoint else None
        if endpoint_handler is None:
            return
        result = endpoint_handler("unregister", None)
        if hasattr(result, "__await__"):
            await result

    async def quiesce(self, params: Any) -> dict[str, Any]:
        deadline = (
            asyncio.get_running_loop().time() + params.drain_timeout_ms / 1000
        )
        result = await super().quiesce(params)
        await self._driver.quiesce(deadline)
        return result

    async def stop(self, params: Any) -> dict[str, Any]:
        result = await super().stop(params)
        await self._driver.stop()
        return result

    async def health(self, params: Any) -> dict[str, Any]:
        result = await super().health(params)
        result["diagnostics"] = self._driver.diagnostics()
        return result

    async def generation_status(self, params: Any) -> dict[str, Any]:
        return await LifecycleController.health(self, params)

    def register_rpc_methods(self, peer: Any) -> None:
        """Register lifecycle RPCs with commit publication fencing."""
        peer.register_method(
            "channel.prepare",
            lambda params, _: self.prepare(params),
        )
        peer.register_method(
            "channel.activate",
            lambda params, _: self.activate(params),
        )
        peer.register_method(
            "channel.commit",
            lambda params, _: self._rpc_commit(params),
        )
        peer.register_method(
            "channel.lease_renew",
            lambda params, _: self.lease_renew(params),
        )
        peer.register_method(
            "channel.quiesce",
            lambda params, _: self.quiesce(params),
        )
        peer.register_method(
            "channel.health",
            lambda params, _: self.health(params),
        )
        peer.register_method(
            "channel.generation_status",
            lambda params, _: self.generation_status(params),
        )
        peer.register_method(
            "channel.stop",
            lambda params, _: self.stop(params),
        )
        peer.register_method(
            "channel.send",
            lambda params, _: self.send(
                params,
                defer_response_publication=True,
            ),
        )
        peer.register_method(
            "channel.reaction",
            lambda params, _: self.reaction(
                params,
                defer_response_publication=True,
            ),
        )
        peer.register_method(
            "channel.response.finish",
            lambda params, _: self.response_finish(params),
        )


class VoiceDriver:
    """Bridge Twilio ConversationRelay to the Channel protocol."""

    def __init__(
        self,
        *,
        platform_factory: Callable[[], VoicePlatform] | None = None,
        retry_policy: RetryPolicy | None = None,
        event_batch_timeout_s: float = _EVENT_BATCH_TIMEOUT_S,
    ) -> None:
        if event_batch_timeout_s <= 0:
            raise ValueError(
                "Voice event_batch_timeout_s must be positive",
            )
        self._platform_factory = platform_factory or VoicePlatform
        self._retry_policy = retry_policy or RetryPolicy()
        self._event_batch_timeout_s = event_batch_timeout_s
        self._peer: Any = None
        self._identity: Any = None
        self._lifecycle: LifecycleController | None = None
        self._platform: VoicePlatform | None = None
        self._secret: dict[str, str] = {}
        self._prepared = False
        self._core_batch_backpressure_total = 0
        self._core_batch_timeout_total = 0
        self._core_batch_retry_total = 0

    @property
    def platform(self) -> VoicePlatform | None:
        """Return the bound platform for health and focused tests."""
        return self._platform

    def bind(self, peer: Any, identity: Any) -> None:
        """Bind one Runner RPC session and immutable identity."""
        self._peer = peer
        self._identity = identity

    def create_lifecycle_spec(
        self,
        identity: Any,
        *,
        secret_handle_consumer: Any | None,
    ) -> RunnerLifecycleSpec:
        """Describe lifecycle hooks without carrying source identity."""
        return RunnerLifecycleSpec(
            controller_class=_VoiceLifecycleController,
            args=(self,),
            kwargs={
                "channel_key": identity.channel_key,
                "instance_id": identity.instance_id,
                "environment_spec_id": identity.environment_spec_id,
                "environment_id": identity.environment_id,
                "qwenpaw_version": identity.qwenpaw_version,
                "lock_sha256": identity.lock_sha256,
                "python_abi": identity.python_abi,
                "platform_tag": identity.platform_tag,
                "generation": identity.generation,
                "capabilities": identity.capabilities,
                "send_handler": self.send,
                "secret_handle_consumer": secret_handle_consumer,
                "endpoint_handler": self._publish_endpoint,
            },
        )

    def attach_lifecycle(self, controller: LifecycleController) -> None:
        """Retain lifecycle state for ingress admission fencing."""
        self._lifecycle = controller

    async def consume_secret(self, value: object) -> None:
        """Consume the Twilio auth token from an opaque handle."""
        if not isinstance(value, Mapping):
            raise PlatformAuthenticationError("invalid Voice secret")
        auth_token = value.get("twilio_auth_token", "")
        if not isinstance(auth_token, str) or not auth_token:
            raise PlatformAuthenticationError(
                "invalid Twilio auth token",
            )
        self._secret = {"twilio_auth_token": auth_token}

    async def prepare(self, host_context: HostContext) -> None:
        """Prepare config without binding or external side effects."""
        secret = self._secret
        try:
            config = dict(host_context.config_snapshot)
            if "twilio_auth_token" in config:
                raise ValueError(
                    "Voice config snapshot contains twilio_auth_token",
                )
            platform = self._platform_factory()
            try:
                await platform.prepare(config, dict(secret))
            except ValueError as exc:
                if "auth_token" in str(exc):
                    raise PlatformAuthenticationError(
                        "Twilio authentication failed",
                    ) from exc
                raise
            self._platform = platform
            self._prepared = True
        finally:
            self._secret = {}
            secret.clear()

    async def commit(self) -> None:
        """Start Runner-owned ingress only after generation commit."""
        platform = self._platform
        if not self._prepared or platform is None:
            raise RuntimeError("Voice Driver is not prepared")
        identity = self._require_identity()
        await platform.start(
            self._handle_native_event,
            self._handle_endpoint_change,
            identity.generation,
        )

    def confirm_commit(self) -> None:
        """Publish provisional Voice ingress after response acceptance."""
        platform = self._platform
        if platform is not None:
            platform.confirm_startup()

    def mark_commit_failed(self, reason_code: str) -> None:
        """Fence provisional Voice ingress before compensation."""
        platform = self._platform
        if platform is not None:
            platform.mark_startup_failed(reason_code)

    async def abort_commit(self, reason_code: str) -> None:
        """Await provisional Voice compensation with diagnostics."""
        platform = self._platform
        if platform is not None:
            await platform.abort_startup(reason_code)

    async def quiesce(self, deadline: float) -> None:
        """Stop new calls and drain existing Voice resources."""
        platform = self._platform
        if platform is not None:
            await platform.close(deadline=deadline)

    async def stop(self) -> None:
        """Release Driver resources idempotently."""
        platform = self._platform
        if platform is not None:
            await platform.close()
            if platform.cleanup_complete and self._platform is platform:
                self._platform = None
        self._prepared = False

    def diagnostics(self) -> dict[str, Any]:
        """Return separate Runner and Core pressure diagnostics."""
        snapshot: dict[str, Any] = {
            "core_batch_backpressure_total": (
                self._core_batch_backpressure_total
            ),
            "core_batch_timeout_total": self._core_batch_timeout_total,
            "core_batch_retry_total": self._core_batch_retry_total,
        }
        platform = self._platform
        if platform is not None:
            snapshot.update(platform.health_snapshot())
        return snapshot

    async def _handle_endpoint_change(
        self,
        operation: str,
        endpoint: VoiceEndpoint | None,
    ) -> None:
        """Apply one platform endpoint through lifecycle fencing."""
        lifecycle = self._require_lifecycle()
        identity = self._require_identity()
        if operation == "unregister":
            if not isinstance(lifecycle, _VoiceLifecycleController):
                raise RuntimeError(
                    "Voice lifecycle controller is unavailable",
                )
            await lifecycle.withdraw_voice_endpoint(
                IdentityParams(
                    channel_key=identity.channel_key,
                    instance_id=identity.instance_id,
                    generation=identity.generation,
                ),
            )
            return
        if endpoint is None:
            raise RuntimeError("Voice endpoint update is unavailable")
        params = EndpointParams(
            channel_key=identity.channel_key,
            instance_id=identity.instance_id,
            generation=identity.generation,
            protocol="http+ws",
            host=endpoint.host,
            port=endpoint.port,
            path="/voice",
            public_base_url=endpoint.public_base_url,
            readiness=endpoint.readiness,
            bound_externally=endpoint.bound_externally,
            auth_required=endpoint.auth_required,
        )
        if operation == "register":
            await lifecycle.endpoint_register(params)
            return
        await lifecycle.endpoint_update(params)

    async def _publish_endpoint(
        self,
        operation: str,
        endpoint: EndpointParams | None,
    ) -> None:
        """Publish endpoint state to the Core-owned registry."""
        peer = self._require_peer()
        identity = self._require_identity()
        if operation == "unregister":
            platform = self._platform
            if platform is not None:
                await platform.stop_accepting()
            params = IdentityParams(
                channel_key=identity.channel_key,
                instance_id=identity.instance_id,
                generation=identity.generation,
            )
            await peer.call(
                "ingress.endpoint.unregister",
                params.to_mapping(),
            )
            return
        if endpoint is None:
            raise RuntimeError("Voice endpoint update is unavailable")
        await peer.call(
            f"ingress.endpoint.{operation}",
            endpoint.to_mapping(),
        )

    async def _handle_native_event(self, native: VoiceNativeEvent) -> None:
        """Normalize one ordered Voice event and submit it reliably."""
        if not self._can_consume():
            return
        identity = self._require_identity()
        voice_event = VoiceEvent.from_mapping(
            {
                "event_id": native.event_id,
                "event_kind": native.event_kind,
                "channel_key": identity.channel_key,
                "instance_id": identity.instance_id,
                "generation": native.generation,
                "connection_id": native.connection_id,
                "sequence": native.sequence,
                "session_binding": native.session_binding,
                "platform_session_id": native.platform_session_id,
                "payload": self._redacted_payload(native),
            },
        )
        await self._submit_event(self._inbound_event(voice_event))

    @staticmethod
    def _redacted_payload(native: VoiceNativeEvent) -> dict[str, Any]:
        """Remove raw phone numbers from call.started payloads."""
        payload = dict(native.payload)
        if native.event_kind != VoiceEventKind.CALL_STARTED.value:
            return payload
        return {
            "from": VoiceDriver._redact_number(
                str(payload.get("from", "")),
            ),
            "to": VoiceDriver._redact_number(
                str(payload.get("to", "")),
            ),
        }

    @staticmethod
    def _redact_number(value: str) -> str:
        """Retain only a short non-secret phone-number suffix."""
        suffix = value[-4:] if value else ""
        return f"***{suffix}"

    @staticmethod
    def _inbound_event(voice_event: VoiceEvent) -> InboundEvent:
        """Embed the frozen Voice DTO in the generic event envelope."""
        content_parts: tuple[dict[str, Any], ...] = ()
        if voice_event.event_kind is VoiceEventKind.MESSAGE_QUERY:
            text = voice_event.payload.get("text", "")
            if isinstance(text, str) and text:
                content_parts = ({"type": "text", "text": text},)
        return InboundEvent(
            event_id=voice_event.event_id,
            event_kind=voice_event.event_kind.value,
            channel_key=voice_event.channel_key,
            instance_id=voice_event.instance_id,
            generation=voice_event.generation,
            conversation={
                "id": voice_event.session_binding,
                "type": "dm",
                "thread_id": None,
            },
            sender_id=voice_event.platform_session_id,
            acl_sender_id=voice_event.platform_session_id,
            sender_name="",
            content_parts=content_parts,
            metadata={
                "connection_id": voice_event.connection_id,
                "sequence": voice_event.sequence,
                "session_binding": voice_event.session_binding,
                "platform_session_id": voice_event.platform_session_id,
                "voice_payload": dict(voice_event.payload),
            },
        )

    async def _submit_event(self, event: InboundEvent) -> None:
        """Submit one event with bounded ACK retry semantics."""
        peer = self._require_peer()
        batch_id = f"voice:g{event.generation}:batch:{event.event_id}"
        events = (event,)
        for attempt in range(1, self._retry_policy.max_attempts + 1):
            if not self._can_consume():
                return
            batch = EventBatchParams(batch_id=batch_id, events=events)
            acknowledgement: EventBatchAck | None = None
            try:
                result = await peer.call(
                    "event.batch",
                    batch.to_mapping(),
                    timeout=self._event_batch_timeout_s,
                )
                acknowledgement = EventBatchAck.from_mapping(result)
                self._record_ack_backpressure(acknowledgement)
            except RpcTimeoutError:
                self._core_batch_timeout_total += 1
            except RpcError as exc:
                if self._rpc_reason(exc) not in {
                    "INGRESS_BACKPRESSURE",
                    "RPC_BACKPRESSURE",
                    "TEMPORARY_UNAVAILABLE",
                }:
                    raise
                self._core_batch_backpressure_total += 1
            except RpcClosedError:
                return
            retry_events, delay = events_for_retry(
                batch,
                acknowledgement,
                attempt=attempt,
                policy=self._retry_policy,
            )
            if not retry_events or delay is None:
                return
            self._core_batch_retry_total += 1
            events = retry_events
            await asyncio.sleep(delay)

    def _record_ack_backpressure(self, ack: EventBatchAck) -> None:
        """Count retryable Core batch-pressure rejections separately."""
        self._core_batch_backpressure_total += sum(
            1
            for rejected in ack.rejected_events
            if rejected.reason_code == "INGRESS_BACKPRESSURE"
        )

    async def send(self, params: SendParams) -> dict[str, Any]:
        """Send one Core text response to ConversationRelay."""
        if params.operation is not OutboundOperation.MESSAGE_CREATE:
            return self._failed_result(
                params.delivery_id,
                "OUTBOUND_OPERATION_UNSUPPORTED",
            )
        text_parts = [
            str(part.get("text", ""))
            for part in params.content_parts
            if part.get("type") == "text" and part.get("text")
        ]
        if not text_parts:
            return self._failed_result(
                params.delivery_id,
                "OUTBOUND_CONTENT_UNSUPPORTED",
            )
        platform = self._platform
        if platform is None:
            return self._failed_result(
                params.delivery_id,
                "INGRESS_CONNECTION_UNKNOWN",
                retryable=True,
            )
        try:
            await platform.send_text(
                params.to_handle,
                "\n".join(text_parts),
            )
        except VoicePlatformError as exc:
            if exc.side_effect_possible:
                return self._unknown_result(
                    params.delivery_id,
                    exc.reason_code,
                )
            return self._failed_result(
                params.delivery_id,
                exc.reason_code,
                retryable=True,
            )
        return self._acknowledged_result(params.delivery_id)

    def _can_consume(self) -> bool:
        lifecycle = self._lifecycle
        return lifecycle is not None and lifecycle.state is RunnerState.ACTIVE

    def _require_peer(self) -> Any:
        if self._peer is None:
            raise RuntimeError("Voice RPC peer is unavailable")
        return self._peer

    def _require_identity(self) -> Any:
        if self._identity is None:
            raise RuntimeError("Voice identity is unavailable")
        return self._identity

    def _require_lifecycle(self) -> LifecycleController:
        if self._lifecycle is None:
            raise RuntimeError("Voice lifecycle is unavailable")
        return self._lifecycle

    @staticmethod
    def _rpc_reason(exc: RpcError) -> str:
        data = exc.data
        if isinstance(data, Mapping):
            return str(data.get("reason_code") or "")
        return ""

    @staticmethod
    def _acknowledged_result(delivery_id: str) -> dict[str, Any]:
        return {
            "delivery_id": delivery_id,
            "state": DeliveryState.ACKNOWLEDGED.value,
            "retryable": False,
        }

    @staticmethod
    def _failed_result(
        delivery_id: str,
        reason_code: str,
        *,
        retryable: bool = False,
    ) -> dict[str, Any]:
        return {
            "delivery_id": delivery_id,
            "state": DeliveryState.FAILED.value,
            "reason_code": reason_code,
            "retryable": retryable,
        }

    @staticmethod
    def _unknown_result(
        delivery_id: str,
        reason_code: str,
    ) -> dict[str, Any]:
        return {
            "delivery_id": delivery_id,
            "state": DeliveryState.UNKNOWN.value,
            "reason_code": reason_code,
            "retryable": False,
        }


__all__ = ["VoiceDriver"]
