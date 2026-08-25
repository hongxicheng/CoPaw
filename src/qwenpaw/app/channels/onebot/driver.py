# -*- coding: utf-8 -*-
"""Runner-owned OneBot ChannelDriver."""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Mapping
import logging
import re
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
    events_for_retry,
)
from ....channel_protocol.lifecycle import LifecycleController, RunnerState
from .platform import (
    OneBotEndpoint,
    OneBotPlatform,
    OneBotPlatformError,
)


logger = logging.getLogger(__name__)

_EVENT_BATCH_TIMEOUT = 60.0
_CQ_PATTERN = re.compile(r"\[CQ:(?P<type>\w+),(?P<data>[^\]]*)\]")
_CQ_UNESCAPE_REPLACEMENTS = (
    ("&#44;", ","),
    ("&#91;", "["),
    ("&#93;", "]"),
    ("&#38;", "&"),
    ("&amp;", "&"),
)


class _OneBotLifecycleController(LifecycleController):
    """Invoke OneBot Driver hooks at frozen lifecycle boundaries."""

    def __init__(self, driver: "OneBotDriver", **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._driver = driver
        self.host_context: HostContext | None

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
        result = await super().commit(params)
        try:
            await self._driver.commit()
        except Exception:
            self.state = RunnerState.FAILED
            await self._driver.stop()
            raise
        return result

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


class OneBotDriver:
    """Bridge OneBot native operations to the Channel protocol."""

    def __init__(
        self,
        *,
        platform_factory: Callable[[], OneBotPlatform] | None = None,
        retry_policy: RetryPolicy | None = None,
        event_batch_timeout: float = _EVENT_BATCH_TIMEOUT,
    ) -> None:
        if event_batch_timeout <= 0:
            raise ValueError("event_batch_timeout must be positive")
        self._platform_factory = platform_factory or OneBotPlatform
        self._retry_policy = retry_policy or RetryPolicy()
        self._event_batch_timeout = event_batch_timeout
        self._peer: Any = None
        self._identity: Any = None
        self._lifecycle: LifecycleController | None = None
        self._platform: OneBotPlatform | None = None
        self._secret: dict[str, str] = {}
        self._config: dict[str, Any] = {}
        self._self_id: str | None = None
        self._prepared = False
        self._core_batch_backpressure_total = 0
        self._core_batch_timeout_total = 0
        self._core_batch_retry_total = 0

    @property
    def platform(self) -> OneBotPlatform | None:
        """Return the bound platform for health and focused tests."""
        return self._platform

    def bind(self, peer: Any, identity: Any) -> None:
        """Bind one Runner RPC session and immutable identity."""
        self._peer = peer
        self._identity = identity

    def create_lifecycle_controller(
        self,
        identity: Any,
        *,
        secret_handle_consumer: Any | None,
    ) -> LifecycleController:
        """Create the protocol lifecycle controller for this Driver."""
        return _OneBotLifecycleController(
            self,
            channel_key=identity.channel_key,
            instance_id=identity.instance_id,
            environment_spec_id=identity.environment_spec_id,
            environment_id=identity.environment_id,
            generation=identity.generation,
            capabilities=identity.capabilities,
            qwenpaw_version=identity.qwenpaw_version,
            send_handler=self.send,
            secret_handle_consumer=secret_handle_consumer,
            endpoint_handler=self._publish_endpoint,
        )

    def attach_lifecycle(self, controller: LifecycleController) -> None:
        """Retain lifecycle state for ingress admission fencing."""
        self._lifecycle = controller

    async def consume_secret(self, value: object) -> None:
        """Consume the optional OneBot access token from an opaque handle."""
        if not isinstance(value, Mapping):
            raise PlatformAuthenticationError("invalid OneBot secret")
        token = value.get("access_token", "")
        if not isinstance(token, str):
            raise PlatformAuthenticationError("invalid OneBot access token")
        self._secret = {"access_token": token}

    async def prepare(self, host_context: HostContext) -> None:
        """Prepare config without binding or consuming formal ingress."""
        secret = self._secret
        try:
            config = dict(host_context.config_snapshot)
            if "access_token" in config:
                raise ValueError(
                    "OneBot config snapshot contains access_token",
                )
            platform = self._platform_factory()
            await platform.prepare(config, dict(secret))
            self._platform = platform
            self._config = config
            self._prepared = True
        finally:
            self._secret = {}
            secret.clear()

    async def commit(self) -> None:
        """Bind the platform listener only after generation commit."""
        platform = self._platform
        if not self._prepared or platform is None:
            raise RuntimeError("OneBot Driver is not prepared")
        await platform.start(
            self._handle_platform_event,
            self._handle_endpoint_change,
        )

    async def quiesce(self, deadline: float) -> None:
        """Stop admitting native connections and events."""
        platform = self._platform
        if platform is not None:
            await platform.close(deadline=deadline)

    async def stop(self) -> None:
        """Release Driver resources idempotently."""
        platform = self._platform
        self._platform = None
        if platform is not None:
            await platform.close()
        self._prepared = False

    def diagnostics(self) -> dict[str, Any]:
        """Return separate Runner and Core batch pressure counters."""
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
        endpoint: OneBotEndpoint,
    ) -> None:
        """Apply one platform bind result through lifecycle fencing."""
        lifecycle = self._require_lifecycle()
        identity = self._require_identity()
        params = EndpointParams(
            channel_key=identity.channel_key,
            instance_id=identity.instance_id,
            generation=identity.generation,
            protocol="ws",
            host=endpoint.host,
            port=endpoint.port,
            path="/ws",
            public_base_url=None,
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
            raise RuntimeError("OneBot endpoint update is unavailable")
        await peer.call(
            f"ingress.endpoint.{operation}",
            endpoint.to_mapping(),
        )

    async def _handle_platform_event(self, value: Mapping[str, Any]) -> None:
        """Normalize and submit one native OneBot event."""
        if not self._can_consume():
            return
        post_type = value.get("post_type")
        if post_type == "meta_event":
            self_id = value.get("self_id")
            if self_id is not None:
                self._self_id = str(self_id)
            return
        if post_type != "message":
            return
        event = await self._normalize_message(value)
        if event is not None:
            await self._submit_event(event)

    async def _normalize_message(
        self,
        value: Mapping[str, Any],
    ) -> InboundEvent | None:
        """Convert one OneBot message report into a stable event DTO."""
        identity = self._require_identity()
        message_type = str(value.get("message_type") or "private")
        user_id = str(value.get("user_id") or "")
        group_id = str(value.get("group_id") or "")
        message_id = str(value.get("message_id") or "")
        if not user_id or not message_id:
            raise ValueError("OneBot message requires user_id and message_id")
        event_self_id = value.get("self_id")
        if event_self_id is not None:
            self._self_id = str(event_self_id)
        segments = self._normalize_segments(value.get("message", []))
        content_parts, bot_mentioned = self._parse_segments(segments)
        reply_message_id = self._reply_message_id(segments)
        if not content_parts and reply_message_id is None:
            return None
        is_group = message_type == "group"
        if (
            is_group
            and bool(self._config.get("require_mention"))
            and not bot_mentioned
        ):
            return None
        if reply_message_id is not None:
            quoted_segments = await self._quoted_segments(reply_message_id)
            quoted_parts, _ = self._parse_segments(quoted_segments)
            quoted_parts = await self._resolve_file_urls(
                quoted_parts,
                message_type,
                value,
                quoted_segments,
            )
            content_parts = await self._resolve_file_urls(
                content_parts,
                message_type,
                value,
                segments,
            )
            content_parts = self._with_quoted_context(
                quoted_parts,
                content_parts,
            )
        else:
            content_parts = await self._resolve_file_urls(
                content_parts,
                message_type,
                value,
                segments,
            )
        if not content_parts:
            return None
        sender = value.get("sender", {})
        sender_mapping = sender if isinstance(sender, Mapping) else {}
        sender_name = str(
            sender_mapping.get("card")
            or sender_mapping.get("nickname")
            or user_id,
        )
        conversation_id = group_id if is_group else user_id
        if not conversation_id:
            raise ValueError("OneBot group message requires group_id")
        to_handle = f"group:{group_id}" if is_group else user_id
        metadata = {
            "message_type": message_type,
            "message_id": message_id,
            "sender_id": user_id,
            "user_name": sender_name,
            "group_id": group_id if is_group else "",
            "is_group": is_group,
            "bot_mentioned": bot_mentioned,
            "onebot_to_handle": to_handle,
        }
        return InboundEvent(
            event_id=f"onebot:message:{message_id}",
            event_kind="message",
            channel_key=identity.channel_key,
            instance_id=identity.instance_id,
            generation=identity.generation,
            conversation={
                "id": conversation_id,
                "type": "group" if is_group else "dm",
                "thread_id": None,
            },
            sender_id=user_id,
            acl_sender_id=user_id,
            sender_name=sender_name,
            content_parts=tuple(content_parts),
            metadata=metadata,
        )

    async def _submit_event(self, event: InboundEvent) -> None:
        """Submit one stable event with bounded ACK retry semantics."""
        peer = self._require_peer()
        batch_id = f"onebot:g{event.generation}:batch:{event.event_id}"
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
                    timeout=self._event_batch_timeout,
                )
                acknowledgement = EventBatchAck.from_mapping(result)
                self._record_ack_backpressure(acknowledgement)
            except RpcTimeoutError:
                self._core_batch_timeout_total += 1
            except RpcError as exc:
                if self._rpc_reason(exc) != "INGRESS_BACKPRESSURE":
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
        """Send one Core message through the active OneBot connection."""
        if params.operation is not OutboundOperation.MESSAGE_CREATE:
            return self._failed_result(
                params.delivery_id,
                "OUTBOUND_OPERATION_UNSUPPORTED",
            )
        platform = self._platform
        if platform is None:
            return self._failed_result(
                params.delivery_id,
                "INGRESS_CONNECTION_UNKNOWN",
                retryable=True,
            )
        try:
            await platform.send_message(
                params.to_handle,
                params.content_parts,
            )
        except OneBotPlatformError as exc:
            if exc.side_effect_possible:
                return self._unknown_result(
                    params.delivery_id,
                    exc.reason_code,
                )
            return self._failed_result(
                params.delivery_id,
                exc.reason_code,
                retryable=(exc.reason_code == "INGRESS_CONNECTION_UNKNOWN"),
            )
        return self._acknowledged_result(params.delivery_id)

    async def _quoted_segments(self, message_id: str) -> list[dict[str, Any]]:
        """Fetch a quoted message without blocking the WebSocket reader."""
        platform = self._require_platform()
        api_message_id: str | int = message_id
        try:
            api_message_id = int(message_id)
        except ValueError:
            pass
        try:
            result = await platform.call_api(
                "get_msg",
                {"message_id": api_message_id},
            )
        except OneBotPlatformError:
            logger.warning(
                f"onebot: failed to fetch quoted message {message_id}",
            )
            return []
        data = result.get("data")
        mapping = data if isinstance(data, Mapping) else {}
        segments = self._normalize_segments(mapping.get("message"))
        raw_segments = self._normalize_segments(mapping.get("raw_message"))
        if (
            raw_segments
            and self._segment_types(segments) == ["text"]
            and self._segment_types(raw_segments) != ["text"]
        ):
            return raw_segments
        return segments

    async def _resolve_file_urls(
        self,
        parts: list[dict[str, Any]],
        message_type: str,
        event: Mapping[str, Any],
        segments: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Resolve OneBot file identifiers through the echo API."""
        file_segments = [
            segment for segment in segments if segment.get("type") == "file"
        ]
        file_index = 0
        resolved: list[dict[str, Any]] = []
        for part in parts:
            if part.get("type") != "file":
                resolved.append(part)
                continue
            source = (
                file_segments[file_index]
                if file_index < len(file_segments)
                else {}
            )
            file_index += 1
            source_data = source.get("data", {})
            source_mapping = (
                source_data if isinstance(source_data, Mapping) else {}
            )
            file_id = str(source_mapping.get("file_id") or "")
            current_url = str(part.get("file_url") or "")
            if current_url.startswith(("http://", "https://", "file://")):
                resolved.append(part)
                continue
            if not file_id:
                resolved.append(part)
                continue
            try:
                if message_type == "group":
                    result = await self._require_platform().call_api(
                        "get_group_file_url",
                        {
                            "group_id": int(event.get("group_id") or 0),
                            "file_id": file_id,
                        },
                    )
                else:
                    result = await self._require_platform().call_api(
                        "get_private_file_url",
                        {"file_id": file_id},
                    )
            except OneBotPlatformError:
                resolved.append(part)
                continue
            result_data = result.get("data")
            result_mapping = (
                result_data if isinstance(result_data, Mapping) else {}
            )
            real_url = str(result_mapping.get("url") or "")
            if not real_url:
                resolved.append(part)
                continue
            updated = dict(part)
            updated["file_url"] = real_url
            resolved.append(updated)
        return resolved

    def _parse_segments(  # pylint: disable=too-many-branches
        self,
        segments: list[dict[str, Any]],
    ) -> tuple[list[dict[str, Any]], bool]:
        """Parse native OneBot segments into protocol content locators."""
        parts: list[dict[str, Any]] = []
        bot_mentioned = False
        for segment in segments:
            segment_type = str(segment.get("type") or "")
            data = segment.get("data", {})
            mapping = data if isinstance(data, Mapping) else {}
            if segment_type == "text":
                text = str(mapping.get("text") or "").strip()
                if text:
                    parts.append({"type": "text", "text": text})
            elif segment_type == "image":
                locator = str(mapping.get("url") or mapping.get("file") or "")
                if locator:
                    parts.append({"type": "image", "image_url": locator})
            elif segment_type == "record":
                locator = str(mapping.get("url") or mapping.get("file") or "")
                if locator:
                    parts.append({"type": "audio", "data": locator})
            elif segment_type == "video":
                locator = str(mapping.get("url") or mapping.get("file") or "")
                if locator:
                    parts.append({"type": "video", "video_url": locator})
            elif segment_type == "file":
                locator = str(mapping.get("url") or mapping.get("file") or "")
                filename = str(
                    mapping.get("name") or mapping.get("file") or "file",
                )
                if locator or mapping.get("file_id"):
                    parts.append(
                        {
                            "type": "file",
                            "file_url": locator or filename,
                            "filename": filename,
                        },
                    )
            elif segment_type == "at":
                target = str(mapping.get("qq") or "")
                if self._self_id is not None and target == self._self_id:
                    bot_mentioned = True
        return parts, bot_mentioned

    @staticmethod
    def _normalize_segments(value: object) -> list[dict[str, Any]]:
        """Normalize array or CQ-code messages into native segments."""
        if isinstance(value, list):
            return [dict(item) for item in value if isinstance(item, Mapping)]
        if not isinstance(value, str):
            return []
        segments: list[dict[str, Any]] = []
        cursor = 0
        for match in _CQ_PATTERN.finditer(value):
            if match.start() > cursor:
                text = value[cursor : match.start()].strip()
                if text:
                    segments.append(
                        {"type": "text", "data": {"text": text}},
                    )
            data: dict[str, str] = {}
            for item in match.group("data").split(","):
                key, separator, raw = item.partition("=")
                if separator and key:
                    data[key] = OneBotDriver._unescape_cq(raw)
            segments.append({"type": match.group("type"), "data": data})
            cursor = match.end()
        if cursor < len(value):
            text = value[cursor:].strip()
            if text:
                segments.append({"type": "text", "data": {"text": text}})
        if not segments and value.strip():
            segments.append(
                {"type": "text", "data": {"text": value.strip()}},
            )
        return segments

    @staticmethod
    def _unescape_cq(value: str) -> str:
        """Decode OneBot CQ parameter escaping."""
        result = value
        for escaped, decoded in _CQ_UNESCAPE_REPLACEMENTS:
            result = result.replace(escaped, decoded)
        return result

    @staticmethod
    def _reply_message_id(
        segments: list[dict[str, Any]],
    ) -> str | None:
        """Return the directly quoted OneBot message identifier."""
        for segment in segments:
            if segment.get("type") != "reply":
                continue
            data = segment.get("data", {})
            mapping = data if isinstance(data, Mapping) else {}
            message_id = mapping.get("id")
            if message_id is not None and str(message_id):
                return str(message_id)
        return None

    @staticmethod
    def _segment_types(segments: list[dict[str, Any]]) -> list[str]:
        return [str(segment.get("type") or "") for segment in segments]

    @staticmethod
    def _with_quoted_context(
        quoted: list[dict[str, Any]],
        current: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Preserve the legacy quoted-content marker format."""
        if not quoted:
            return current
        quoted_text = OneBotDriver._all_text(quoted)
        current_text = OneBotDriver._all_text(current)
        if quoted_text is not None and current_text is not None:
            text = f"[Quoted message]\n{'\n'.join(quoted_text)}"
            if current_text:
                text = (
                    f"{text}\n\n[Current message]\n"
                    f"{'\n'.join(current_text)}"
                )
            return [{"type": "text", "text": text}]
        merged: list[dict[str, Any]] = [
            {"type": "text", "text": "[Quoted message]"},
        ]
        for part in quoted:
            annotation = OneBotDriver._quoted_annotation(part)
            if annotation:
                merged.append({"type": "text", "text": annotation})
            merged.append(part)
        if current:
            merged.append(
                {"type": "text", "text": "[Current message]"},
            )
            merged.extend(current)
        return merged

    @staticmethod
    def _all_text(parts: list[dict[str, Any]]) -> list[str] | None:
        """Return text values only when every part is textual."""
        texts: list[str] = []
        for part in parts:
            if part.get("type") != "text":
                return None
            text = str(part.get("text") or "").strip()
            if text:
                texts.append(text)
        return texts

    @staticmethod
    def _quoted_annotation(part: Mapping[str, Any]) -> str | None:
        """Return the existing marker for a quoted media locator."""
        content_type = part.get("type")
        if content_type == "image":
            return "[Quoted image message]"
        if content_type == "audio":
            return "[Quoted voice message]"
        if content_type == "video":
            return "[Quoted video message]"
        if content_type == "file":
            filename = str(part.get("filename") or "file")
            return f"[Quoted file message: {filename}]"
        return None

    def _can_consume(self) -> bool:
        lifecycle = self._lifecycle
        return lifecycle is not None and lifecycle.state is RunnerState.ACTIVE

    def _require_peer(self) -> Any:
        if self._peer is None:
            raise RuntimeError("OneBot RPC peer is unavailable")
        return self._peer

    def _require_identity(self) -> Any:
        if self._identity is None:
            raise RuntimeError("OneBot identity is unavailable")
        return self._identity

    def _require_lifecycle(self) -> LifecycleController:
        if self._lifecycle is None:
            raise RuntimeError("OneBot lifecycle is unavailable")
        return self._lifecycle

    def _require_platform(self) -> OneBotPlatform:
        if self._platform is None:
            raise RuntimeError("OneBot platform is unavailable")
        return self._platform

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


__all__ = ["OneBotDriver"]
