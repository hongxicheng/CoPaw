# -*- coding: utf-8 -*-
# pylint: disable=protected-access
"""Runner-owned Feishu ChannelDriver."""

from __future__ import annotations

import asyncio
from collections import OrderedDict
from collections.abc import Callable, Mapping
from dataclasses import dataclass
import json
import logging
from pathlib import Path
from typing import Any, Protocol

from ....channel_protocol import (
    DeliveryState,
    EventBatchAck,
    EventBatchParams,
    HostContext,
    HostStateParams,
    InboundEvent,
    OutboundOperation,
    PlatformAuthenticationError,
    ReactionParams,
    SendParams,
)
from ....channel_protocol.lifecycle import LifecycleController, RunnerState
from .platform import FeishuDeliveryError, LarkOapiPlatform
from .utils import sender_display_string, short_session_id_from_full_id


logger = logging.getLogger(__name__)

_RECEIVE_STATE_KEY = "feishu.receive_ids"
_RECEIVE_STATE_VERSION = 1
_RECEIVE_STATE_MAX_ENTRIES = 256
_RECEIVE_STATE_MAX_BYTES = 60 * 1024


class FeishuPlatform(Protocol):
    """Platform operations consumed by the Feishu Driver."""

    async def prepare(
        self,
        config: dict[str, Any],
        secret: dict[str, str],
        media_work_dir: Path,
    ) -> None:
        ...

    async def connect(
        self,
        on_message: Callable[[object], Any],
        on_card: Callable[[object], Any],
    ) -> None:
        ...

    async def wait_disconnected(self) -> None:
        ...

    async def disconnect(self) -> None:
        ...

    async def close(self) -> None:
        ...

    async def send_message(
        self,
        receive_id_type: str,
        receive_id: str,
        content_parts: tuple[dict[str, Any], ...],
    ) -> str:
        ...

    async def send_approval(
        self,
        receive_id_type: str,
        receive_id: str,
        approval: Mapping[str, str],
        fallback_text: str,
    ) -> str:
        ...

    async def start_stream(
        self,
        receive_id_type: str,
        receive_id: str,
        text: str,
    ) -> dict[str, str]:
        ...

    async def update_stream(
        self,
        target: Mapping[str, str],
        text: str,
        sequence: int,
        *,
        final: bool,
    ) -> bool:
        ...

    async def add_reaction(
        self,
        message_id: str,
        emoji_type: str = "DONE",
    ) -> bool:
        ...


@dataclass(frozen=True)
class _ReceiveTarget:
    """One persisted Feishu receive target."""

    receive_id_type: str
    receive_id: str


class _FeishuLifecycleController(LifecycleController):
    """Invoke Driver hooks at frozen lifecycle boundaries."""

    def __init__(self, driver: "FeishuDriver", **kwargs: Any) -> None:
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
        result = await super().quiesce(params)
        await self._driver.quiesce()
        return result

    async def stop(self, params: Any) -> dict[str, Any]:
        result = await super().stop(params)
        await self._driver.stop()
        return result


class FeishuDriver:
    """Bridge Feishu platform operations to the Channel protocol."""

    def __init__(
        self,
        *,
        platform_factory: Callable[[], FeishuPlatform] | None = None,
        reconnect_initial_delay: float = 1.0,
        reconnect_max_delay: float = 60.0,
        connect_timeout: float = 30.0,
    ) -> None:
        self._platform_factory = platform_factory or LarkOapiPlatform
        self._reconnect_initial_delay = reconnect_initial_delay
        self._reconnect_max_delay = reconnect_max_delay
        self._connect_timeout = connect_timeout
        self._platform: FeishuPlatform | None = None
        self._peer: Any = None
        self._identity: Any = None
        self._lifecycle: LifecycleController | None = None
        self._secret: dict[str, str] | None = None
        self._config: dict[str, Any] = {}
        self._receive_ids: OrderedDict[str, _ReceiveTarget] = OrderedDict()
        self._delivery_targets: dict[str, dict[str, str]] = {}
        self._connection_task: asyncio.Task[None] | None = None
        self._connection_ready = asyncio.Event()
        self._stop_event = asyncio.Event()
        self._prepared = False
        self._connected = False
        self._last_error = ""

    def bind(self, peer: Any, identity: Any) -> None:
        """Bind one task-local Runner session."""
        self._peer = peer
        self._identity = identity

    def create_lifecycle_controller(
        self,
        identity: Any,
        *,
        secret_handle_consumer: Any | None,
    ) -> LifecycleController:
        """Create the protocol lifecycle controller for this Driver."""
        return _FeishuLifecycleController(
            self,
            channel_key=identity.channel_key,
            instance_id=identity.instance_id,
            environment_spec_id=identity.environment_spec_id,
            environment_id=identity.environment_id,
            generation=identity.generation,
            capabilities=identity.capabilities,
            qwenpaw_version=identity.qwenpaw_version,
            send_handler=self.send,
            reaction_handler=self.reaction,
            secret_handle_consumer=secret_handle_consumer,
        )

    def attach_lifecycle(self, controller: LifecycleController) -> None:
        """Retain lifecycle state for active-ingress fencing."""
        self._lifecycle = controller

    async def consume_secret(self, value: object) -> None:
        """Consume all Feishu secrets from one opaque handle."""
        if not isinstance(value, Mapping):
            raise PlatformAuthenticationError("invalid Feishu secret")
        app_secret = value.get("app_secret")
        if not isinstance(app_secret, str) or not app_secret:
            raise PlatformAuthenticationError("invalid Feishu app_secret")
        secret: dict[str, str] = {"app_secret": app_secret}
        for name in ("encrypt_key", "verification_token"):
            item = value.get(name, "")
            if not isinstance(item, str):
                raise PlatformAuthenticationError("invalid Feishu secret")
            secret[name] = item
        self._secret = secret

    async def prepare(self, host_context: HostContext) -> None:
        """Initialize the platform from non-secret config and host context."""
        secret = self._secret
        if secret is None:
            raise PlatformAuthenticationError("Feishu secret is unavailable")
        try:
            config = dict(host_context.config_snapshot)
            forbidden = {
                "app_secret",
                "encrypt_key",
                "verification_token",
            }.intersection(config)
            if forbidden:
                raise ValueError(
                    "Feishu config snapshot contains secret fields",
                )
            app_id = config.get("app_id")
            if not isinstance(app_id, str) or not app_id:
                raise PlatformAuthenticationError(
                    "Feishu app_id is required",
                )
            domain = config.get("domain", "feishu")
            if domain not in {"feishu", "lark"}:
                raise ValueError("invalid Feishu domain")
            media_work_dir = host_context.media_work_dir
            if not isinstance(media_work_dir, str) or not media_work_dir:
                raise ValueError("Feishu media_work_dir is required")
            media_path = Path(media_work_dir)
            if not media_path.is_absolute():
                raise ValueError("Feishu media_work_dir must be absolute")
            platform = self._platform_factory()
            await platform.prepare(config, dict(secret), media_path)
            self._platform = platform
            self._config = config
            await self._restore_receive_ids()
            self._prepared = True
        except (PlatformAuthenticationError, ValueError):
            raise
        except Exception as exc:
            raise PlatformAuthenticationError(
                "Feishu platform prepare failed",
            ) from exc
        finally:
            self._secret = None
            secret.clear()

    async def commit(self) -> None:
        """Start platform consumption after generation commit."""
        if not self._prepared or self._platform is None:
            raise RuntimeError("Feishu Driver is not prepared")
        if self._connection_task is not None:
            return
        self._stop_event.clear()
        self._connection_ready.clear()
        self._connection_task = asyncio.create_task(self._connection_loop())
        try:
            await asyncio.wait_for(
                self._connection_ready.wait(),
                timeout=self._connect_timeout,
            )
        except asyncio.TimeoutError as exc:
            await self._stop_connection()
            raise RuntimeError("Feishu platform connection timed out") from exc
        if self._last_error and not self._connected:
            raise RuntimeError("Feishu platform connection failed")

    async def quiesce(self) -> None:
        """Stop admitting platform events."""
        await self._stop_connection()

    async def stop(self) -> None:
        """Release Driver resources idempotently."""
        await self._stop_connection()
        platform = self._platform
        self._platform = None
        if platform is not None:
            await platform.close()
        self._prepared = False

    async def _stop_connection(self) -> None:
        self._stop_event.set()
        platform = self._platform
        if platform is not None:
            try:
                await platform.disconnect()
            except Exception:
                logger.debug("Feishu disconnect failed", exc_info=True)
        task = self._connection_task
        self._connection_task = None
        if task is not None and task is not asyncio.current_task():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        self._connected = False

    async def _connection_loop(self) -> None:
        delay = self._reconnect_initial_delay
        while not self._stop_event.is_set():
            platform = self._platform
            if platform is None:
                return
            try:
                await platform.connect(
                    self._handle_platform_message,
                    self._handle_card_action,
                )
                self._connected = True
                self._last_error = ""
                self._connection_ready.set()
                await platform.wait_disconnected()
                self._connected = False
                delay = self._reconnect_initial_delay
            except Exception as exc:
                self._connected = False
                self._last_error = type(exc).__name__
                self._connection_ready.set()
                logger.warning(
                    "Feishu connection failed: %s",
                    type(exc).__name__,
                )
            if self._stop_event.is_set():
                break
            await asyncio.sleep(delay)
            delay = min(delay * 2, self._reconnect_max_delay)

    def health_snapshot(self) -> dict[str, Any]:
        """Return non-secret Driver health details."""
        return {
            "prepared": self._prepared,
            "connected": self._connected,
            "last_error": self._last_error,
        }

    async def send(  # pylint: disable=too-many-return-statements
        self,
        params: SendParams,
    ) -> dict[str, Any]:
        """Map one platform-neutral outbound operation to Feishu."""
        platform = self._require_platform()
        try:
            target = await self._resolve_receive_target(params.to_handle)
            if params.operation is OutboundOperation.MESSAGE_CREATE:
                if params.approval is not None:
                    message_id = await platform.send_approval(
                        target.receive_id_type,
                        target.receive_id,
                        params.approval.to_mapping(),
                        self._fallback_text(params.content_parts),
                    )
                else:
                    message_id = await platform.send_message(
                        target.receive_id_type,
                        target.receive_id,
                        params.content_parts,
                    )
                self._delivery_targets[params.delivery_id] = {
                    "message_id": message_id,
                }
            elif params.operation is OutboundOperation.STREAM_START:
                stream_target = await platform.start_stream(
                    target.receive_id_type,
                    target.receive_id,
                    self._stream_text(params),
                )
                self._delivery_targets[params.delivery_id] = stream_target
            else:
                stream_target = self._delivery_targets.get(
                    params.target_delivery_id or "",
                )
                if stream_target is None:
                    return self._failed_result(
                        params.delivery_id,
                        "OUTBOUND_TARGET_UNKNOWN",
                    )
                final = params.operation is OutboundOperation.STREAM_END
                updated = await platform.update_stream(
                    stream_target,
                    self._stream_text(params),
                    params.sequence or 0,
                    final=final,
                )
                if not updated:
                    return self._failed_result(
                        params.delivery_id,
                        "PLATFORM_SEND_FAILED",
                    )
            return self._acknowledged_result(params.delivery_id)
        except FeishuDeliveryError as exc:
            logger.warning(
                "Feishu delivery failed: %s",
                exc.reason_code,
            )
            if exc.side_effect_possible:
                return self._unknown_result(
                    params.delivery_id,
                    exc.reason_code,
                )
            return self._failed_result(
                params.delivery_id,
                exc.reason_code,
                retryable=exc.retryable,
            )
        except KeyError:
            return self._failed_result(
                params.delivery_id,
                "OUTBOUND_TARGET_UNKNOWN",
            )
        except Exception:
            logger.warning("Feishu delivery result is unknown", exc_info=True)
            return self._unknown_result(
                params.delivery_id,
                "PLATFORM_RESULT_UNKNOWN",
            )

    async def reaction(self, params: ReactionParams) -> dict[str, Any]:
        """Add the legacy DONE reaction to a sent platform message."""
        target = self._delivery_targets.get(params.target_delivery_id)
        message_id = target.get("message_id", "") if target else ""
        if not message_id:
            return self._failed_result(
                params.delivery_id,
                "OUTBOUND_TARGET_UNKNOWN",
            )
        try:
            if await self._require_platform().add_reaction(
                message_id,
                "DONE",
            ):
                return self._acknowledged_result(params.delivery_id)
            return self._failed_result(
                params.delivery_id,
                "PLATFORM_SEND_FAILED",
            )
        except Exception:
            return self._unknown_result(
                params.delivery_id,
                "PLATFORM_RESULT_UNKNOWN",
            )

    async def _handle_platform_message(self, value: object) -> None:
        if not self._can_consume():
            return
        event = self._normalize_message(value)
        acknowledgement = await self._submit_event(event)
        delivered = (
            event.event_id in acknowledgement.accepted_event_ids
            or event.event_id in acknowledgement.duplicate_event_ids
        )
        if not delivered:
            rejected = next(
                (
                    item
                    for item in acknowledgement.rejected_events
                    if item.event_id == event.event_id
                ),
                None,
            )
            if rejected is not None:
                logger.warning(
                    "Feishu event rejected reason=%s retryable=%s",
                    rejected.reason_code,
                    rejected.retryable,
                )
            return
        await self._remember_receive_target(event)
        message_id = str(event.metadata.get("feishu_message_id") or "")
        if message_id:
            try:
                await self._require_platform().add_reaction(
                    message_id,
                    "Typing",
                )
            except Exception:
                logger.debug("Feishu typing reaction failed", exc_info=True)
        try:
            await self._persist_receive_ids()
        except Exception:
            logger.warning(
                "Feishu receive target checkpoint failed",
                exc_info=True,
            )

    async def _handle_card_action(self, value: object) -> None:
        if not self._can_consume():
            return
        await self._submit_event(self._normalize_card_action(value))

    def _can_consume(self) -> bool:
        lifecycle = self._lifecycle
        return lifecycle is not None and lifecycle.state is RunnerState.ACTIVE

    async def _submit_event(self, event: InboundEvent) -> EventBatchAck:
        if self._peer is None:
            raise RuntimeError("Feishu RPC peer is unavailable")
        batch = EventBatchParams(
            batch_id=f"batch-{event.event_id}",
            events=(event,),
        )
        result = await self._peer.call("event.batch", batch.to_mapping())
        return EventBatchAck.from_mapping(result)

    def _normalize_message(self, value: object) -> InboundEvent:
        data = self._mapping(value, "Feishu message")
        identity = self._require_identity()
        event_id = self._required_text(data, "event_id")
        message_id = self._required_text(data, "message_id")
        chat_id = self._required_text(data, "chat_id")
        chat_type = str(data.get("chat_type") or "p2p")
        sender_open_id = self._required_text(data, "sender_open_id")
        sender_name = str(data.get("sender_name") or "")
        thread_id = str(data.get("thread_id") or "") or None
        is_group = chat_type == "group"
        receive_id = chat_id if is_group else sender_open_id
        receive_type = "chat_id" if is_group else "open_id"
        sender_display = sender_display_string(sender_name, sender_open_id)
        metadata: dict[str, Any] = {
            "feishu_message_id": message_id,
            "feishu_chat_id": chat_id,
            "feishu_chat_type": chat_type,
            "feishu_sender_id": sender_open_id,
            "feishu_receive_id": receive_id,
            "feishu_receive_id_type": receive_type,
            "is_group": is_group,
            "user_name": sender_name,
        }
        if thread_id:
            metadata["feishu_thread_id"] = thread_id
            metadata[
                "feishu_sender_id"
            ] = f"thread:{short_session_id_from_full_id(thread_id)}"
        elif is_group and bool(self._config.get("share_session_in_group")):
            metadata["feishu_sender_id"] = "group"
        if bool(data.get("bot_mentioned")):
            metadata["bot_mentioned"] = True
        raw_parts = data.get("content_parts")
        if not isinstance(raw_parts, list) or not raw_parts:
            raise ValueError("Feishu message has no content parts")
        parts = tuple(
            dict(part) for part in raw_parts if isinstance(part, Mapping)
        )
        if not parts:
            raise ValueError("Feishu message has no valid content parts")
        return InboundEvent(
            event_id=event_id,
            event_kind="message",
            channel_key=identity.channel_key,
            instance_id=identity.instance_id,
            generation=identity.generation,
            conversation={
                "id": chat_id,
                "type": "group" if is_group else "dm",
                "thread_id": thread_id,
            },
            sender_id=sender_display,
            acl_sender_id=sender_open_id,
            sender_name=sender_name,
            content_parts=parts,
            metadata=metadata,
        )

    def _normalize_card_action(self, value: object) -> InboundEvent:
        data = self._mapping(value, "Feishu card action")
        identity = self._require_identity()
        action = self._required_text(data, "action")
        request_id = self._required_text(data, "request_id")
        operator = self._required_text(data, "operator_open_id")
        session = self._mapping(
            data.get("session_ctx", {}),
            "Feishu card session",
        )
        chat_id = str(session.get("chat_id") or operator)
        chat_type = str(session.get("chat_type") or "p2p")
        is_group = chat_type == "group"
        return InboundEvent(
            event_id=self._required_text(data, "event_id"),
            event_kind="approval_action",
            channel_key=identity.channel_key,
            instance_id=identity.instance_id,
            generation=identity.generation,
            conversation={
                "id": chat_id,
                "type": "group" if is_group else "dm",
                "thread_id": None,
            },
            sender_id=operator,
            acl_sender_id=operator,
            sender_name="",
            content_parts=(
                {
                    "type": "text",
                    "text": f"/approval {action} {request_id}",
                },
            ),
            metadata={
                "approval_action": action,
                "approval_request_id": request_id,
                "from_card_action": True,
                "feishu_receive_id": str(
                    session.get("receive_id") or chat_id,
                ),
                "feishu_receive_id_type": str(
                    session.get("receive_id_type") or "open_id",
                ),
                "is_group": is_group,
            },
        )

    async def _remember_receive_target(self, event: InboundEvent) -> None:
        receive_id = str(event.metadata.get("feishu_receive_id") or "")
        receive_type = str(
            event.metadata.get("feishu_receive_id_type") or "open_id",
        )
        if not receive_id:
            return
        target = _ReceiveTarget(receive_type, receive_id)
        for key in (
            str(event.conversation["id"]),
            event.acl_sender_id,
            self._session_key(event),
        ):
            self._receive_ids.pop(key, None)
            self._receive_ids[key] = target
        self._trim_receive_ids()

    def _session_key(self, event: InboundEvent) -> str:
        if event.conversation["type"] == "group":
            app_id = str(self._config.get("app_id") or "")
            suffix = app_id[-4:] if len(app_id) >= 4 else app_id
            conversation_id = str(event.conversation["id"])
            return f"{suffix}_{short_session_id_from_full_id(conversation_id)}"
        return short_session_id_from_full_id(event.acl_sender_id)

    async def _restore_receive_ids(self) -> None:
        if self._peer is None or self._identity is None:
            return
        try:
            result = await self._peer.call(
                "host.state.get",
                HostStateParams(
                    channel_key=self._identity.channel_key,
                    instance_id=self._identity.instance_id,
                    generation=self._identity.generation,
                    key=_RECEIVE_STATE_KEY,
                ).to_mapping(),
            )
        except Exception:
            logger.warning(
                "Feishu receive target restore failed",
                exc_info=True,
            )
            return
        if not isinstance(result, Mapping) or not result.get("found"):
            return
        value = result.get("value")
        if not isinstance(value, Mapping):
            return
        restored: OrderedDict[str, _ReceiveTarget] = OrderedDict()
        for key, item in value.items():
            if not isinstance(key, str) or not isinstance(item, Mapping):
                continue
            receive_id = item.get("receive_id")
            receive_type = item.get("receive_id_type")
            if isinstance(receive_id, str) and isinstance(receive_type, str):
                restored[key] = _ReceiveTarget(receive_type, receive_id)
        self._receive_ids = restored
        self._trim_receive_ids()

    async def _persist_receive_ids(self) -> None:
        if self._peer is None or self._identity is None:
            return
        value = self._receive_state_value()
        await self._peer.call(
            "host.state.put",
            HostStateParams(
                channel_key=self._identity.channel_key,
                instance_id=self._identity.instance_id,
                generation=self._identity.generation,
                key=_RECEIVE_STATE_KEY,
                schema_version=_RECEIVE_STATE_VERSION,
                value=value,
            ).to_mapping(),
        )

    def _trim_receive_ids(self) -> None:
        while len(self._receive_ids) > _RECEIVE_STATE_MAX_ENTRIES:
            self._receive_ids.popitem(last=False)
        while self._receive_ids:
            encoded = json.dumps(
                self._receive_state_value(),
                ensure_ascii=False,
                separators=(",", ":"),
            ).encode("utf-8")
            if len(encoded) <= _RECEIVE_STATE_MAX_BYTES:
                break
            self._receive_ids.popitem(last=False)

    def _receive_state_value(self) -> dict[str, dict[str, str]]:
        return {
            key: {
                "receive_id_type": target.receive_id_type,
                "receive_id": target.receive_id,
            }
            for key, target in self._receive_ids.items()
        }

    async def _resolve_receive_target(self, to_handle: str) -> _ReceiveTarget:
        value = to_handle.strip()
        prefixes = {
            "feishu:chat_id:": "chat_id",
            "feishu:open_id:": "open_id",
        }
        for prefix, receive_type in prefixes.items():
            if value.startswith(prefix):
                return _ReceiveTarget(receive_type, value[len(prefix) :])
        if value.startswith("oc_"):
            return _ReceiveTarget("chat_id", value)
        if value.startswith("ou_"):
            return _ReceiveTarget("open_id", value)
        key = value.removeprefix("feishu:sw:")
        target = self._receive_ids.get(key)
        if target is None and "#" in key:
            suffix = key.split("#", 1)[-1]
            for item in reversed(self._receive_ids.values()):
                if item.receive_id.endswith(suffix):
                    target = item
                    break
        if target is None:
            raise KeyError("Feishu receive target is unavailable")
        if key in self._receive_ids:
            self._receive_ids.move_to_end(key)
        return target

    def _require_platform(self) -> FeishuPlatform:
        if self._platform is None:
            raise RuntimeError("Feishu platform is unavailable")
        return self._platform

    def _require_identity(self) -> Any:
        if self._identity is None:
            raise RuntimeError("Feishu Driver identity is unavailable")
        return self._identity

    @staticmethod
    def _mapping(value: object, label: str) -> Mapping[str, Any]:
        if not isinstance(value, Mapping):
            raise ValueError(f"{label} must be an object")
        return value

    @staticmethod
    def _required_text(data: Mapping[str, Any], name: str) -> str:
        value = data.get(name)
        if not isinstance(value, str) or not value:
            raise ValueError(f"Feishu {name} is required")
        return value

    @staticmethod
    def _fallback_text(parts: tuple[dict[str, Any], ...]) -> str:
        return "\n".join(
            str(part.get("text") or "")
            for part in parts
            if part.get("type") == "text"
        ).strip()

    def _stream_text(self, params: SendParams) -> str:
        text = params.accumulated_text or ""
        prefix = str(self._config.get("bot_prefix") or "")
        if (
            params.stream_type is not None
            and params.stream_type.value == "reasoning"
        ):
            return f"{prefix}  💭 {text}" if prefix else f"💭 {text}"
        return f"{prefix}  {text}" if prefix and text else text

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


__all__ = ["FeishuDriver"]
