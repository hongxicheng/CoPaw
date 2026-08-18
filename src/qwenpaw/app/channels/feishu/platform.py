# -*- coding: utf-8 -*-
# pylint: disable=protected-access
"""Runner-safe Feishu platform implementation backed by lark-oapi."""

from __future__ import annotations

import asyncio
import base64
from collections import OrderedDict
from collections.abc import Awaitable, Callable, Mapping
from concurrent.futures import Future
from contextlib import suppress
from email.utils import parsedate_to_datetime
import io
import json
import logging
from pathlib import Path
import re
import sys
import threading
import time
import types
from typing import Any
import uuid

import httpx

from ..utils import file_url_to_local_path
from .card_templates import (
    build_tool_guard_approval_card,
    build_tool_guard_resolved_card,
    build_tool_guard_toast,
    parse_tool_guard_action_value,
)
from .constants import (
    FEISHU_FILE_MAX_BYTES,
    FEISHU_NICKNAME_CACHE_MAX,
    FEISHU_PROCESSED_IDS_MAX,
    FEISHU_STALE_MSG_THRESHOLD_MS,
    FEISHU_STREAM_ELEMENT_ID,
)
from .utils import (
    build_interactive_content_chunks,
    detect_file_ext,
    extract_interactive_text,
    extract_json_key,
    extract_post_image_keys,
    extract_post_media_file_keys,
    extract_post_text,
    normalize_feishu_md,
)


logger = logging.getLogger(__name__)


def _declare_namespace_shim(_name: str) -> None:
    return None


_PKG_RESOURCES_MISSING = object()
_original_pkg_resources: Any = sys.modules.get(
    "pkg_resources",
    _PKG_RESOURCES_MISSING,
)
_pkg_resources_shim: types.ModuleType | None = None
_pkg_resources_module: Any = None
_declare_namespace_patched = False

try:
    import pkg_resources as _pkg_resources_module  # type: ignore
except ImportError:  # pragma: no cover - setuptools>=82
    _pkg_resources_shim = types.ModuleType("pkg_resources")
    _pkg_resources_shim.declare_namespace = (  # type: ignore[attr-defined]
        _declare_namespace_shim
    )
    sys.modules["pkg_resources"] = _pkg_resources_shim
else:
    if not hasattr(_pkg_resources_module, "declare_namespace"):
        setattr(
            _pkg_resources_module,
            "declare_namespace",
            _declare_namespace_shim,
        )
        _declare_namespace_patched = True

try:
    import lark_oapi as lark
    import lark_oapi.ws.client as lark_ws
    from lark_oapi.api.contact.v3 import GetUserRequest
    from lark_oapi.api.im.v1 import (
        CreateFileRequest,
        CreateFileRequestBody,
        CreateImageRequest,
        CreateImageRequestBody,
        CreateMessageRequest,
        CreateMessageRequestBody,
        CreateMessageReactionRequest,
        CreateMessageReactionRequestBody,
        Emoji,
        GetMessageRequest,
        GetMessageResourceRequest,
    )
    from lark_oapi.api.cardkit.v1 import (
        ContentCardElementRequest,
        ContentCardElementRequestBody,
        CreateCardRequest,
        CreateCardRequestBody,
        SettingsCardRequest,
        SettingsCardRequestBody,
    )
    from lark_oapi.event.callback.model.p2_card_action_trigger import (
        P2CardActionTriggerResponse,
    )
    from lark_oapi.core.token import TokenManager
finally:
    if (
        _pkg_resources_shim is not None
        and sys.modules.get("pkg_resources") is _pkg_resources_shim
    ):
        if _original_pkg_resources is _PKG_RESOURCES_MISSING:
            del sys.modules["pkg_resources"]
        else:
            sys.modules["pkg_resources"] = _original_pkg_resources
    if _declare_namespace_patched and _pkg_resources_module is not None:
        if (
            getattr(_pkg_resources_module, "declare_namespace", None)
            is _declare_namespace_shim
        ):
            delattr(_pkg_resources_module, "declare_namespace")


class FeishuDeliveryError(RuntimeError):
    """Describe a determinate or uncertain platform delivery failure."""

    def __init__(
        self,
        reason_code: str,
        *,
        side_effect_possible: bool,
        retryable: bool = False,
    ) -> None:
        self.reason_code = reason_code
        self.side_effect_possible = side_effect_possible
        self.retryable = retryable
        super().__init__(reason_code)


class LarkOapiPlatform:
    """Own Feishu SDK clients, callbacks, media, and platform sends."""

    def __init__(self) -> None:
        self._config: dict[str, Any] = {}
        self._secret: dict[str, str] = {}
        self._media_work_dir: Path | None = None
        self._client: Any = None
        self._http_client: httpx.AsyncClient | None = None
        self._runner_loop: asyncio.AbstractEventLoop | None = None
        self._ws_loop: asyncio.AbstractEventLoop | None = None
        self._ws_client: Any = None
        self._ws_thread: threading.Thread | None = None
        self._ws_started = threading.Event()
        self._ws_error: BaseException | None = None
        self._disconnected = asyncio.Event()
        self._closed = False
        self._clock_offset = 0
        self._bot_open_id = ""
        self._processed_ids: OrderedDict[str, None] = OrderedDict()
        self._nickname_cache: OrderedDict[str, str] = OrderedDict()

    async def prepare(
        self,
        config: dict[str, Any],
        secret: dict[str, str],
        media_work_dir: Path,
    ) -> None:
        """Build the production SDK client from validated Runner inputs."""
        app_id = str(config.get("app_id") or "")
        app_secret = str(secret.get("app_secret") or "")
        if not app_id or not app_secret:
            raise ValueError("Feishu credentials are required")
        self._config = dict(config)
        self._secret = {
            "app_secret": app_secret,
            "encrypt_key": str(secret.get("encrypt_key") or ""),
            "verification_token": str(
                secret.get("verification_token") or "",
            ),
        }
        self._media_work_dir = media_work_dir
        self._media_work_dir.mkdir(parents=True, exist_ok=True)
        self._http_client = httpx.AsyncClient(timeout=30.0)
        self._client = (
            lark.Client.builder()
            .app_id(app_id)
            .app_secret(app_secret)
            .domain(self._sdk_domain())
            .log_level(lark.LogLevel.INFO)
            .build()
        )
        try:
            await self._probe_credentials()
        except Exception:
            await self._discard_prepared_resources()
            raise

    async def _probe_credentials(self) -> None:
        """Validate credentials through the SDK tenant-token flow."""
        if self._client is None:
            raise RuntimeError("Feishu authentication client is unavailable")
        token = await asyncio.to_thread(
            TokenManager.get_self_tenant_token,
            self._client._config,
        )
        if not token:
            raise RuntimeError("Feishu credentials were rejected")

    async def _discard_prepared_resources(self) -> None:
        """Release partially prepared clients after authentication failure."""
        if self._http_client is not None:
            await self._http_client.aclose()
        self._http_client = None
        self._client = None
        self._secret.clear()

    async def connect(
        self,
        on_message: Callable[[object], Awaitable[None]],
        on_card: Callable[[object], Awaitable[None]],
    ) -> None:
        """Start one SDK WebSocket connection in its dedicated thread."""
        if self._ws_thread is not None and self._ws_thread.is_alive():
            return
        try:
            self._bot_open_id = await self._fetch_bot_open_id()
        except Exception:
            logger.warning("Feishu bot identity lookup failed", exc_info=True)
        self._closed = False
        self._runner_loop = asyncio.get_running_loop()
        self._disconnected = asyncio.Event()
        self._ws_error = None
        self._ws_started.clear()
        handler = (
            lark.EventDispatcherHandler.builder(
                self._secret.get("encrypt_key", ""),
                self._secret.get("verification_token", ""),
            )
            .register_p2_im_message_receive_v1(
                lambda value: self._on_message_sync(value, on_message),
            )
            .register_p2_im_message_reaction_created_v1(
                lambda _value: None,
            )
            .register_p2_im_message_reaction_deleted_v1(
                lambda _value: None,
            )
            .register_p2_card_action_trigger(
                lambda value: self._on_card_sync(value, on_card),
            )
            .build()
        )
        self._ws_thread = threading.Thread(
            target=self._run_ws,
            args=(handler,),
            daemon=True,
        )
        self._ws_thread.start()
        started = await asyncio.to_thread(self._ws_started.wait, 30.0)
        if not started:
            await self.disconnect()
            raise TimeoutError("Feishu WebSocket connection timed out")
        if self._ws_error is not None:
            error = self._ws_error
            await self.disconnect()
            raise RuntimeError("Feishu WebSocket connection failed") from error

    async def wait_disconnected(self) -> None:
        """Wait until the SDK connection terminates."""
        await self._disconnected.wait()

    async def disconnect(self) -> None:
        """Stop the current SDK connection without discarding config."""
        self._closed = True
        loop = self._ws_loop
        client = self._ws_client
        if loop is not None and not loop.is_closed():
            if client is not None and hasattr(client, "_disconnect"):
                with suppress(asyncio.CancelledError, Exception):
                    future = asyncio.run_coroutine_threadsafe(
                        client._disconnect(),
                        loop,
                    )
                    await asyncio.wrap_future(future)
            with suppress(RuntimeError):
                loop.call_soon_threadsafe(loop.stop)
        thread = self._ws_thread
        if thread is not None and thread is not threading.current_thread():
            await asyncio.to_thread(thread.join, 5.0)
        self._ws_thread = None
        self._ws_client = None
        self._disconnected.set()

    async def close(self) -> None:
        """Release all platform resources."""
        await self.disconnect()
        await self._discard_prepared_resources()

    def _on_message_sync(
        self,
        value: object,
        callback: Callable[[object], Awaitable[None]],
    ) -> None:
        """Fast-ACK SDK callback that schedules Runner-owned work."""
        if self._closed or not self._event_matches_instance(value):
            return
        if self._event_is_stale(value):
            return
        self._schedule_callback(
            self._dispatch_native_message(value, callback),
            "message",
        )

    def _on_card_sync(
        self,
        value: object,
        callback: Callable[[object], Awaitable[None]],
    ) -> Any:
        """Schedule card ingestion and return the SDK response promptly."""
        if self._closed or not self._event_matches_instance(value):
            return P2CardActionTriggerResponse({})
        native = self._native_card(value)
        parsed = parse_tool_guard_action_value(native.get("action_value"))
        if parsed is None:
            return P2CardActionTriggerResponse({})
        native.update(parsed)
        self._schedule_callback(callback(native), "card")
        operator = str(native.get("operator_open_id") or "")
        display = self._nickname_cache.get(operator, "")
        if not display and operator:
            display = operator[-6:]
        resolved = build_tool_guard_resolved_card(
            tool_name=str(parsed.get("tool_name") or "tool"),
            action=str(parsed.get("action") or ""),
            operator_display=display,
            body_text=str(parsed.get("body") or ""),
        )
        toast = build_tool_guard_toast(
            str(parsed.get("action") or ""),
            str(parsed.get("tool_name") or "tool"),
        )
        return P2CardActionTriggerResponse(
            {
                "toast": toast,
                "card": {
                    "type": "raw",
                    "data": json.loads(resolved),
                },
            },
        )

    def _schedule_callback(
        self,
        coroutine: Awaitable[None],
        event_kind: str,
    ) -> None:
        loop = self._runner_loop
        if loop is None or not loop.is_running():
            if hasattr(coroutine, "close"):
                coroutine.close()  # type: ignore[attr-defined]
            logger.warning(
                "Feishu %s callback loop is unavailable",
                event_kind,
            )
            return
        future = asyncio.run_coroutine_threadsafe(coroutine, loop)
        future.add_done_callback(
            lambda item: self._consume_callback_future(item, event_kind),
        )

    @staticmethod
    def _consume_callback_future(
        future: Future[None],
        event_kind: str,
    ) -> None:
        try:
            future.result()
        except Exception:
            logger.exception("Feishu %s callback failed", event_kind)

    def _run_ws(self, handler: Any) -> None:
        """Run one lark-oapi connection in a private event loop."""
        runner_loop = self._runner_loop
        if runner_loop is None:
            self._ws_error = RuntimeError("Runner loop is unavailable")
            self._ws_started.set()
            return
        loop = asyncio.new_event_loop()
        self._ws_loop = loop
        try:
            asyncio.set_event_loop(loop)
            lark_ws.loop = loop
            self._ws_client = lark.ws.Client(
                str(self._config.get("app_id") or ""),
                self._secret.get("app_secret", ""),
                event_handler=handler,
                log_level=lark.LogLevel.INFO,
                domain=self._sdk_domain(),
                auto_reconnect=False,
            )

            async def receive_message_loop() -> None:
                try:
                    while True:
                        connection = self._ws_client._conn
                        if connection is None:
                            return
                        message = await connection.recv()
                        loop.create_task(
                            self._ws_client._handle_message(message),
                        )
                except Exception:
                    logger.debug(
                        "Feishu WebSocket receive loop ended",
                        exc_info=True,
                    )
                finally:
                    with suppress(Exception):
                        await self._ws_client._disconnect()
                    runner_loop.call_soon_threadsafe(
                        self._disconnected.set,
                    )
                    loop.call_soon(loop.stop)

            self._ws_client._receive_message_loop = receive_message_loop
            loop.run_until_complete(self._ws_client._connect())
            loop.create_task(self._ws_client._ping_loop())
            self._ws_started.set()
            loop.run_forever()
        except BaseException as exc:
            self._ws_error = exc
            self._ws_started.set()
        finally:
            if self._ws_client is not None:
                with suppress(Exception):
                    loop.run_until_complete(self._ws_client._disconnect())
            pending = [
                task for task in asyncio.all_tasks(loop) if not task.done()
            ]
            for task in pending:
                task.cancel()
            if pending:
                with suppress(Exception):
                    loop.run_until_complete(
                        asyncio.gather(*pending, return_exceptions=True),
                    )
            loop.close()
            self._ws_loop = None
            runner_loop.call_soon_threadsafe(self._disconnected.set)

    async def _dispatch_native_message(
        self,
        value: object,
        callback: Callable[[object], Awaitable[None]],
    ) -> None:
        native = self._native_message(value)
        message_id = str(native.get("message_id") or "")
        if not message_id or message_id in self._processed_ids:
            return
        self._processed_ids[message_id] = None
        while len(self._processed_ids) > FEISHU_PROCESSED_IDS_MAX:
            self._processed_ids.popitem(last=False)
        sender_id = str(native.get("sender_open_id") or "")
        if not sender_id or sender_id == self._bot_open_id:
            return
        sender_name = str(native.get("sender_name") or "")
        if not sender_name:
            sender_name = await self._fetch_sender_name(sender_id)
        native["sender_name"] = sender_name
        text_parts: list[str] = []
        content_parts: list[dict[str, Any]] = []
        message_type = str(native.get("message_type") or "text")
        main_text, hints, parts = await self._parse_message_content(
            message_type,
            str(native.get("content") or ""),
            message_id,
        )
        if message_type == "text" and main_text:
            for key in native.get("bot_mention_keys", []):
                main_text = main_text.replace(str(key), "")
            main_text = main_text.strip() or None
        if main_text:
            text_parts.append(main_text)
        text_parts.extend(hints)
        content_parts.extend(parts)
        parent_id = str(native.get("parent_id") or "")
        if parent_id:
            await self._append_quoted_message(
                parent_id,
                text_parts,
                content_parts,
            )
        if not text_parts and not content_parts:
            text_parts.append(f"[{message_type}]")
        if text_parts:
            content_parts.insert(
                0,
                {"type": "text", "text": "\n".join(text_parts)},
            )
        native["content_parts"] = content_parts
        await callback(native)

    async def _parse_message_content(
        self,
        message_type: str,
        content: str,
        message_id: str,
    ) -> tuple[str | None, list[str], list[dict[str, Any]]]:
        text: str | None = None
        hints: list[str] = []
        parts: list[dict[str, Any]] = []
        if message_type == "text":
            text = extract_json_key(content, "text")
        elif message_type == "post":
            text = extract_post_text(content)
            for image_key in extract_post_image_keys(content):
                try:
                    locator = await self._download_resource(
                        message_id,
                        image_key,
                        "image",
                        "image.jpg",
                    )
                    parts.append({"type": "image", "image_url": locator})
                except Exception:
                    logger.warning(
                        "Feishu image download failed",
                        exc_info=True,
                    )
                    hints.append("[image: download failed]")
            for file_key in extract_post_media_file_keys(content):
                try:
                    locator = await self._download_resource(
                        message_id,
                        file_key,
                        "file",
                        "file.bin",
                    )
                    parts.append({"type": "file", "file_url": locator})
                except Exception:
                    logger.warning(
                        "Feishu file download failed",
                        exc_info=True,
                    )
                    hints.append("[media: download failed]")
        elif message_type == "interactive":
            text = extract_interactive_text(content)
        elif message_type in {"image", "file", "media", "audio"}:
            key = extract_json_key(
                content,
                "image_key",
                "file_key",
                "imageKey",
                "fileKey",
            )
            if not key:
                return None, [f"[{message_type}: missing key]"], []
            filename = extract_json_key(
                content,
                "file_name",
                "fileName",
            ) or {
                "image": "image.jpg",
                "media": "video.mp4",
                "audio": "audio.opus",
            }.get(message_type, "file.bin")
            resource_type = "image" if message_type == "image" else "file"
            try:
                locator = await self._download_resource(
                    message_id,
                    key,
                    resource_type,
                    filename,
                )
            except Exception:
                logger.warning(
                    "Feishu %s download failed",
                    message_type,
                    exc_info=True,
                )
                label = "video" if message_type == "media" else message_type
                return None, [f"[{label}: download failed]"], []
            content_type = {
                "image": "image",
                "audio": "audio",
                "media": "video",
            }.get(message_type, "file")
            locator_name = (
                "data" if content_type == "audio" else f"{content_type}_url"
            )
            part = {
                "type": content_type,
                locator_name: locator,
                "filename": filename,
            }
            if content_type == "audio":
                part["format"] = Path(filename).suffix.lstrip(".") or "opus"
            parts.append(part)
        if text:
            text = text.strip() or None
        return text, hints, parts

    async def _append_quoted_message(
        self,
        parent_id: str,
        text_parts: list[str],
        content_parts: list[dict[str, Any]],
    ) -> None:
        result = await self._fetch_quoted_message(parent_id)
        if result is None:
            return
        message_type, content = result
        text, hints, parts = await self._parse_message_content(
            message_type,
            content,
            parent_id,
        )
        label = {
            "text": "message",
            "post": "message",
            "image": "image",
            "file": "file",
            "media": "video",
            "audio": "audio",
            "interactive": "interactive card",
        }.get(message_type, message_type)
        quoted = f"[quoted {label}: {text}]" if text else f"[quoted {label}]"
        text_parts.insert(0, quoted)
        for hint in reversed(hints):
            text_parts.insert(1, f"[quoted {hint.strip('[]')}]")
        content_parts.extend(parts)

    async def _fetch_quoted_message(
        self,
        parent_id: str,
    ) -> tuple[str, str] | None:
        if self._client is None:
            return None
        try:
            request = GetMessageRequest.builder().message_id(parent_id).build()
            request.add_query("card_msg_content_type", "user_card_content")
            response = await self._client.im.v1.message.aget(request)
            if not response.success():
                return None
            items = response.data.items if response.data else []
            if not items:
                return None
            message = items[0]
            body = getattr(message, "body", None)
            return (
                str(getattr(message, "msg_type", "") or ""),
                str(getattr(body, "content", "") or ""),
            )
        except Exception:
            logger.debug("Feishu quoted message fetch failed", exc_info=True)
            return None

    async def _fetch_sender_name(self, open_id: str) -> str:
        if open_id in self._nickname_cache:
            name = self._nickname_cache.pop(open_id)
            self._nickname_cache[open_id] = name
            return name
        if self._client is None:
            return ""
        try:
            request = (
                GetUserRequest.builder()
                .user_id(open_id)
                .user_id_type("open_id")
                .build()
            )
            response = await asyncio.to_thread(
                self._client.contact.v3.user.get,
                request,
            )
            user = response.data.user if response.success() else None
            for field in ("name", "en_name", "nickname"):
                value = getattr(user, field, "") if user is not None else ""
                if isinstance(value, str) and value.strip():
                    name = value.strip()
                    self._nickname_cache[open_id] = name
                    while (
                        len(self._nickname_cache) > FEISHU_NICKNAME_CACHE_MAX
                    ):
                        self._nickname_cache.popitem(last=False)
                    return name
        except Exception:
            logger.debug("Feishu sender name lookup failed", exc_info=True)
        return ""

    async def _fetch_bot_open_id(self) -> str:
        """Fetch bot identity and update the server clock offset."""
        if self._client is None or self._http_client is None:
            return ""
        try:
            token = TokenManager.get_self_tenant_token(self._client._config)
            if not token:
                return ""
            response = await self._http_client.get(
                f"{self._sdk_domain()}/open-apis/bot/v3/info",
                headers={
                    "Authorization": f"Bearer {token}",
                    "Content-Type": "application/json",
                },
                timeout=10.0,
            )
            date_value = response.headers.get("Date")
            if date_value:
                server_ms = int(
                    parsedate_to_datetime(date_value).timestamp() * 1000,
                )
                self._clock_offset = server_ms - int(time.time() * 1000)
            data = response.json()
            if data.get("code", -1) != 0:
                return ""
            return str((data.get("bot") or {}).get("open_id") or "")
        except Exception:
            logger.debug("Feishu bot identity lookup failed", exc_info=True)
            return ""

    async def _download_resource(
        self,
        message_id: str,
        resource_key: str,
        resource_type: str,
        filename: str,
    ) -> str:
        if self._client is None or GetMessageResourceRequest is None:
            raise RuntimeError("Feishu media client is unavailable")
        media_dir = self._media_work_dir
        if media_dir is None:
            raise RuntimeError("Feishu media_work_dir is unavailable")
        request = (
            GetMessageResourceRequest.builder()
            .message_id(message_id)
            .file_key(resource_key)
            .type(resource_type)
            .build()
        )
        response = await self._client.im.v1.message_resource.aget(request)
        if not response.success() or response.file is None:
            raise RuntimeError("Feishu media download failed")
        data = response.file.read()
        if not data:
            raise RuntimeError("Feishu media download was empty")
        if resource_type == "image":
            safe_key = (
                "".join(
                    character
                    for character in resource_key
                    if character.isalnum() or character in "-_."
                )
                or "img"
            )
            extension = detect_file_ext(data, default="jpg")
            path = media_dir / f"{message_id}_{safe_key}.{extension}"
        else:
            safe_name = Path(filename).name or "file.bin"
            if safe_name in {"file.bin", "video.mp4"}:
                extension = detect_file_ext(data, default="bin")
                safe_name = f"file.{extension}"
            path = media_dir / f"{message_id}_{safe_name}"
        await asyncio.to_thread(path.write_bytes, data)
        return str(path)

    async def send_message(
        self,
        receive_id_type: str,
        receive_id: str,
        content_parts: tuple[dict[str, Any], ...],
    ) -> str:
        """Send text and media with legacy Feishu ordering and rendering."""
        text_parts: list[str] = []
        media_parts: list[Mapping[str, Any]] = []
        for part in content_parts:
            if part.get("type") == "text":
                text_parts.append(str(part.get("text") or ""))
            else:
                media_parts.append(part)
        body = "\n".join(text_parts).strip()
        prefix = str(self._config.get("bot_prefix") or "")
        if prefix and body:
            body = f"{prefix}  {body}"
        last_message_id = ""
        side_effect_possible = False
        try:
            if body:
                message_ids = await self._send_text(
                    receive_id_type,
                    receive_id,
                    body,
                )
                if message_ids:
                    last_message_id = message_ids[-1]
                    side_effect_possible = True
            for part in media_parts:
                if part.get("type") == "image":
                    message_id = await self._send_image_part(
                        receive_id_type,
                        receive_id,
                        part,
                    )
                else:
                    message_id = await self._send_file_part(
                        receive_id_type,
                        receive_id,
                        part,
                    )
                last_message_id = message_id
                side_effect_possible = True
        except FeishuDeliveryError as exc:
            raise FeishuDeliveryError(
                exc.reason_code,
                side_effect_possible=(
                    side_effect_possible or exc.side_effect_possible
                ),
                retryable=exc.retryable,
            ) from exc
        if not last_message_id:
            raise FeishuDeliveryError(
                "EMPTY_OUTBOUND_CONTENT",
                side_effect_possible=False,
            )
        return last_message_id

    async def send_approval(
        self,
        receive_id_type: str,
        receive_id: str,
        approval: Mapping[str, str],
        fallback_text: str,
    ) -> str:
        content = build_tool_guard_approval_card(
            request_id=str(approval.get("request_id") or ""),
            tool_name=str(approval.get("tool_name") or "tool"),
            severity=str(approval.get("severity") or "medium"),
            body_text=fallback_text,
            session_ctx={
                "chat_id": (
                    receive_id if receive_id_type == "chat_id" else ""
                ),
                "chat_type": (
                    "group" if receive_id_type == "chat_id" else "p2p"
                ),
                "receive_id": receive_id,
                "receive_id_type": receive_id_type,
                "is_group": receive_id_type == "chat_id",
            },
        )
        return await self._create_message(
            receive_id_type,
            receive_id,
            "interactive",
            content,
        )

    async def _send_text(
        self,
        receive_id_type: str,
        receive_id: str,
        body: str,
    ) -> list[str]:
        if re.search(r"^\s*\|", body, re.MULTILINE):
            message_ids = []
            try:
                for chunk in build_interactive_content_chunks(body):
                    message_ids.append(
                        await self._create_message(
                            receive_id_type,
                            receive_id,
                            "interactive",
                            chunk,
                        ),
                    )
            except FeishuDeliveryError as exc:
                raise FeishuDeliveryError(
                    exc.reason_code,
                    side_effect_possible=(
                        bool(message_ids) or exc.side_effect_possible
                    ),
                    retryable=exc.retryable,
                ) from exc
            return message_ids
        post = {
            "zh_cn": {
                "content": [
                    [{"tag": "md", "text": normalize_feishu_md(body)}],
                ],
            },
        }
        return [
            await self._create_message(
                receive_id_type,
                receive_id,
                "post",
                json.dumps(post, ensure_ascii=False),
            ),
        ]

    async def _send_image_part(
        self,
        receive_id_type: str,
        receive_id: str,
        part: Mapping[str, Any],
    ) -> str:
        if self._client is None:
            raise FeishuDeliveryError(
                "PLATFORM_UNAVAILABLE",
                side_effect_possible=False,
            )
        data = await self._read_locator(str(part.get("image_url") or ""))
        request = (
            CreateImageRequest.builder()
            .request_body(
                CreateImageRequestBody.builder()
                .image_type("message")
                .image(io.BytesIO(data))
                .build(),
            )
            .build()
        )
        try:
            response = await self._client.im.v1.image.acreate(request)
        except Exception as exc:
            raise FeishuDeliveryError(
                "IMAGE_UPLOAD_UNKNOWN",
                side_effect_possible=True,
            ) from exc
        image_key = (
            getattr(response.data, "image_key", "")
            if response.success() and response.data
            else ""
        )
        if not image_key:
            raise FeishuDeliveryError(
                "IMAGE_UPLOAD_FAILED",
                side_effect_possible=False,
            )
        try:
            return await self._create_message(
                receive_id_type,
                receive_id,
                "image",
                json.dumps({"image_key": image_key}, ensure_ascii=False),
            )
        except FeishuDeliveryError as exc:
            raise FeishuDeliveryError(
                exc.reason_code,
                side_effect_possible=True,
                retryable=exc.retryable,
            ) from exc

    async def _send_file_part(
        self,
        receive_id_type: str,
        receive_id: str,
        part: Mapping[str, Any],
    ) -> str:
        locator = str(
            part.get("file_url")
            or part.get("video_url")
            or part.get("data")
            or "",
        )
        data = await self._read_locator(locator)
        if len(data) > FEISHU_FILE_MAX_BYTES:
            raise FeishuDeliveryError(
                "FILE_TOO_LARGE",
                side_effect_possible=False,
            )
        filename = str(
            part.get("filename") or Path(locator).name or "file.bin",
        )
        suffix = Path(filename).suffix.lower().lstrip(".")
        file_type = "stream"
        if suffix in {
            "pdf",
            "doc",
            "docx",
            "xls",
            "xlsx",
            "ppt",
            "pptx",
        }:
            file_type = {"docx": "doc", "xlsx": "xls", "pptx": "ppt"}.get(
                suffix,
                suffix,
            )
        elif suffix in {"ogg", "opus"}:
            file_type = "opus"
        request = (
            CreateFileRequest.builder()
            .request_body(
                CreateFileRequestBody.builder()
                .file_type(file_type)
                .file_name(Path(filename).name)
                .file(io.BytesIO(data))
                .build(),
            )
            .build()
        )
        try:
            response = await self._client.im.v1.file.acreate(request)
        except Exception as exc:
            raise FeishuDeliveryError(
                "FILE_UPLOAD_UNKNOWN",
                side_effect_possible=True,
            ) from exc
        file_key = (
            getattr(response.data, "file_key", "")
            if response.success() and response.data
            else ""
        )
        if not file_key:
            raise FeishuDeliveryError(
                "FILE_UPLOAD_FAILED",
                side_effect_possible=False,
            )
        try:
            return await self._create_message(
                receive_id_type,
                receive_id,
                "audio" if file_type == "opus" else "file",
                json.dumps({"file_key": file_key}, ensure_ascii=False),
            )
        except FeishuDeliveryError as exc:
            raise FeishuDeliveryError(
                exc.reason_code,
                side_effect_possible=True,
                retryable=exc.retryable,
            ) from exc

    async def _read_locator(self, locator: str) -> bytes:
        if locator.startswith("data:") and "base64," in locator:
            try:
                return base64.b64decode(locator.split("base64,", 1)[1])
            except Exception as exc:
                raise FeishuDeliveryError(
                    "INVALID_DATA_LOCATOR",
                    side_effect_possible=False,
                ) from exc
        local_path = file_url_to_local_path(locator)
        if local_path is not None:
            path = Path(local_path)
            if not path.is_file():
                raise FeishuDeliveryError(
                    "LOCAL_FILE_UNAVAILABLE",
                    side_effect_possible=False,
                )
            return await asyncio.to_thread(path.read_bytes)
        if locator.startswith(("http://", "https://")):
            if self._http_client is None:
                raise FeishuDeliveryError(
                    "HTTP_CLIENT_UNAVAILABLE",
                    side_effect_possible=False,
                )
            response = await self._http_client.get(locator)
            if response.status_code >= 400:
                raise FeishuDeliveryError(
                    "REMOTE_MEDIA_UNAVAILABLE",
                    side_effect_possible=False,
                )
            return response.content
        raise FeishuDeliveryError(
            "UNSUPPORTED_MEDIA_LOCATOR",
            side_effect_possible=False,
        )

    async def _create_message(
        self,
        receive_id_type: str,
        receive_id: str,
        message_type: str,
        content: str,
    ) -> str:
        request = (
            CreateMessageRequest.builder()
            .receive_id_type(receive_id_type)
            .request_body(
                CreateMessageRequestBody.builder()
                .receive_id(receive_id)
                .msg_type(message_type)
                .content(content)
                .build(),
            )
            .build()
        )
        try:
            response = await self._client.im.v1.message.acreate(request)
        except Exception as exc:
            raise FeishuDeliveryError(
                "MESSAGE_RESULT_UNKNOWN",
                side_effect_possible=True,
            ) from exc
        if not response.success():
            raise FeishuDeliveryError(
                "MESSAGE_REJECTED",
                side_effect_possible=False,
                retryable=True,
            )
        message_id = getattr(response.data, "message_id", "")
        if not message_id:
            raise FeishuDeliveryError(
                "MESSAGE_RESULT_UNKNOWN",
                side_effect_possible=True,
            )
        return str(message_id)

    async def start_stream(
        self,
        receive_id_type: str,
        receive_id: str,
        text: str,
    ) -> dict[str, str]:
        card_json = {
            "schema": "2.0",
            "config": {"streaming_mode": True},
            "body": {
                "elements": [
                    {
                        "tag": "markdown",
                        "content": text or "...",
                        "element_id": FEISHU_STREAM_ELEMENT_ID,
                    },
                ],
            },
        }
        request = (
            CreateCardRequest.builder()
            .request_body(
                CreateCardRequestBody.builder()
                .type("card_json")
                .data(json.dumps(card_json, ensure_ascii=False))
                .build(),
            )
            .build()
        )
        try:
            response = await self._client.cardkit.v1.card.acreate(request)
        except Exception as exc:
            raise FeishuDeliveryError(
                "STREAM_CREATE_UNKNOWN",
                side_effect_possible=True,
            ) from exc
        card_id = (
            getattr(response.data, "card_id", "")
            if response.success() and response.data
            else ""
        )
        if not card_id:
            raise FeishuDeliveryError(
                "STREAM_CREATE_FAILED",
                side_effect_possible=False,
            )
        try:
            message_id = await self._create_message(
                receive_id_type,
                receive_id,
                "interactive",
                json.dumps(
                    {"type": "card", "data": {"card_id": card_id}},
                    ensure_ascii=False,
                ),
            )
        except FeishuDeliveryError as exc:
            raise FeishuDeliveryError(
                exc.reason_code,
                side_effect_possible=True,
                retryable=exc.retryable,
            ) from exc
        return {"card_id": str(card_id), "message_id": message_id}

    async def update_stream(
        self,
        target: Mapping[str, str],
        text: str,
        sequence: int,
        *,
        final: bool,
    ) -> bool:
        request = (
            ContentCardElementRequest.builder()
            .card_id(target["card_id"])
            .element_id(FEISHU_STREAM_ELEMENT_ID)
            .request_body(
                ContentCardElementRequestBody.builder()
                .content(normalize_feishu_md(text) if final else text)
                .sequence(sequence)
                .uuid(str(uuid.uuid4()))
                .build(),
            )
            .build()
        )
        try:
            response = await self._client.cardkit.v1.card_element.acontent(
                request,
            )
        except Exception as exc:
            raise FeishuDeliveryError(
                "STREAM_UPDATE_UNKNOWN",
                side_effect_possible=True,
            ) from exc
        if not response.success() or not final:
            return bool(response.success())
        preview = text.strip()
        if len(preview) > 80:
            preview = f"{preview[:77]}..."
        settings = json.dumps(
            {
                "config": {
                    "streaming_mode": False,
                    "summary": {"content": preview or "completed"},
                },
            },
            ensure_ascii=False,
        )
        finalize = (
            SettingsCardRequest.builder()
            .card_id(target["card_id"])
            .request_body(
                SettingsCardRequestBody.builder()
                .settings(settings)
                .sequence(sequence + 1)
                .uuid(str(uuid.uuid4()))
                .build(),
            )
            .build()
        )
        try:
            final_response = await self._client.cardkit.v1.card.asettings(
                finalize,
            )
        except Exception as exc:
            raise FeishuDeliveryError(
                "STREAM_FINALIZE_UNKNOWN",
                side_effect_possible=True,
            ) from exc
        if not final_response.success():
            raise FeishuDeliveryError(
                "STREAM_FINALIZE_PARTIAL",
                side_effect_possible=True,
            )
        return True

    async def add_reaction(
        self,
        message_id: str,
        emoji_type: str = "DONE",
    ) -> bool:
        return await self._add_reaction(message_id, emoji_type)

    async def _add_reaction(
        self,
        message_id: str,
        emoji_type: str,
    ) -> bool:
        if not message_id or self._client is None:
            return False
        request = (
            CreateMessageReactionRequest.builder()
            .message_id(message_id)
            .request_body(
                CreateMessageReactionRequestBody.builder()
                .reaction_type(
                    Emoji.builder().emoji_type(emoji_type).build(),
                )
                .build(),
            )
            .build()
        )
        try:
            response = await self._client.im.v1.message_reaction.acreate(
                request,
            )
            return bool(response.success())
        except Exception:
            logger.debug("Feishu reaction failed", exc_info=True)
            return False

    def _native_message(self, value: object) -> dict[str, Any]:
        event = getattr(value, "event", None)
        message = getattr(event, "message", None)
        sender = getattr(event, "sender", None)
        sender_id = getattr(sender, "sender_id", None)
        mentions = getattr(message, "mentions", None) or []
        mention_keys: list[str] = []
        content = str(getattr(message, "content", "") or "")
        mentioned = "@_all" in content
        for mention in mentions:
            mention_id = getattr(mention, "id", None)
            if (
                self._bot_open_id
                and getattr(mention_id, "open_id", "") == self._bot_open_id
            ):
                mentioned = True
                key = str(getattr(mention, "key", "") or "")
                if key:
                    mention_keys.append(key)
        header = getattr(value, "header", None)
        return {
            "event_id": str(
                getattr(header, "event_id", "")
                or getattr(message, "message_id", ""),
            ),
            "message_id": str(getattr(message, "message_id", "") or ""),
            "chat_id": str(getattr(message, "chat_id", "") or ""),
            "chat_type": str(getattr(message, "chat_type", "p2p") or "p2p"),
            "message_type": str(
                getattr(message, "message_type", "text") or "text",
            ),
            "content": content,
            "sender_open_id": str(
                getattr(sender_id, "open_id", "") or "",
            ),
            "sender_name": str(
                getattr(sender, "name", "")
                or getattr(sender, "nickname", "")
                or "",
            ),
            "thread_id": str(getattr(message, "thread_id", "") or ""),
            "parent_id": str(getattr(message, "parent_id", "") or ""),
            "bot_mentioned": mentioned,
            "bot_mention_keys": mention_keys,
        }

    @staticmethod
    def _native_card(value: object) -> dict[str, Any]:
        event = getattr(value, "event", None)
        action = getattr(event, "action", None)
        operator = getattr(event, "operator", None)
        header = getattr(value, "header", None)
        action_value = getattr(action, "value", None) or {}
        action_name = str(action_value.get("action") or "")
        request_id = str(action_value.get("request_id") or "")
        operator_id = str(getattr(operator, "open_id", "") or "")
        fallback_id = f"card-{request_id}-{operator_id}-{action_name}"
        return {
            "event_id": str(
                getattr(header, "event_id", "") or fallback_id,
            ),
            "operator_open_id": operator_id,
            "action_value": action_value,
        }

    def _event_matches_instance(self, value: object) -> bool:
        app_id = str(
            getattr(getattr(value, "header", None), "app_id", "") or "",
        )
        expected = str(self._config.get("app_id") or "")
        return not app_id or app_id == expected

    def _event_is_stale(self, value: object) -> bool:
        create_time = getattr(
            getattr(value, "header", None),
            "create_time",
            None,
        )
        if not create_time:
            return False
        now_ms = int(time.time() * 1000) + self._clock_offset
        try:
            return now_ms - int(create_time) > FEISHU_STALE_MSG_THRESHOLD_MS
        except (TypeError, ValueError):
            return False

    def _sdk_domain(self) -> str:
        domain = str(self._config.get("domain") or "feishu")
        return lark.LARK_DOMAIN if domain == "lark" else lark.FEISHU_DOMAIN


__all__ = ["FeishuDeliveryError", "LarkOapiPlatform"]
