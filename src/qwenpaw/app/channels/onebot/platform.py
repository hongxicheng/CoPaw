# -*- coding: utf-8 -*-
"""Runner-safe OneBot v11 reverse WebSocket platform boundary."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Coroutine, Mapping
from dataclasses import dataclass
import hmac
import ipaddress
import json
import logging
import socket
from typing import Any
import uuid

import aiohttp
from aiohttp import web


logger = logging.getLogger(__name__)

_AUTH_SCHEMES = frozenset({"bearer", "token"})
_DEFAULT_EVENT_TASK_HARD_CAP = 500
_DEFAULT_SHUTDOWN_TIMEOUT = 5.0
_DEFAULT_WS_HOST = "127.0.0.1"


class OneBotPlatformError(RuntimeError):
    """Report a platform operation with a classified side-effect risk."""

    def __init__(
        self,
        reason_code: str,
        *,
        side_effect_possible: bool = False,
    ) -> None:
        self.reason_code = reason_code
        self.side_effect_possible = side_effect_possible
        super().__init__(reason_code)


@dataclass(frozen=True)
class OneBotEndpoint:
    """Describe the actual reverse WebSocket listener state."""

    host: str
    port: int
    readiness: str
    bound_externally: bool
    auth_required: bool


EventHandler = Callable[
    [Mapping[str, Any]],
    Coroutine[Any, Any, None],
]
EndpointHandler = Callable[[str, OneBotEndpoint], Awaitable[None]]


def _normalize_host(value: object) -> str:
    """Normalize a bind host while preserving the loopback default."""
    if not isinstance(value, str):
        raise ValueError("OneBot ws_host must be a string")
    return value.strip().strip("[]") or _DEFAULT_WS_HOST


def _is_external_host(host: str) -> bool:
    """Return whether a listener is exposed beyond loopback."""
    normalized = host.strip().lower()
    if normalized == "localhost":
        return False
    try:
        address = ipaddress.ip_address(normalized)
    except ValueError:
        return True
    return not address.is_loopback


def _probe_host(host: str) -> str:
    """Map wildcard bind addresses to a local health-probe address."""
    normalized = host.strip().lower()
    if normalized in {"0.0.0.0", ""}:
        return "127.0.0.1"
    if normalized == "::":
        return "::1"
    return host


async def _resolve_dynamic_bind_host(host: str, port: int) -> str:
    """Resolve a dynamic hostname to one deterministic socket address."""
    if port != 0:
        return host
    try:
        ipaddress.ip_address(host.split("%", 1)[0])
        return host
    except ValueError:
        pass
    try:
        results = await asyncio.get_running_loop().getaddrinfo(
            host,
            0,
            type=socket.SOCK_STREAM,
        )
    except socket.gaierror as exc:
        raise ValueError(f"OneBot ws_host cannot be resolved: {host}") from exc
    candidates: set[tuple[int, str]] = set()
    for family, _, _, _, address in results:
        if family not in {socket.AF_INET, socket.AF_INET6}:
            continue
        resolved = str(address[0])
        if family == socket.AF_INET6 and len(address) > 3 and address[3]:
            resolved = f"{resolved}%{address[3]}"
        priority = 0 if family == socket.AF_INET else 1
        candidates.add((priority, resolved))
    if not candidates:
        raise ValueError(f"OneBot ws_host has no bind address: {host}")
    return min(candidates)[1]


def _extract_auth_token(value: str) -> str:
    """Extract a supported OneBot Authorization header token."""
    scheme, _, token = value.strip().partition(" ")
    if scheme.lower() not in _AUTH_SCHEMES:
        return ""
    return token.strip()


def _tokens_match(provided: str, expected: str) -> bool:
    """Compare access tokens without content-dependent timing."""
    return hmac.compare_digest(
        provided.encode("utf-8"),
        expected.encode("utf-8"),
    )


def _log_remote(request: web.Request) -> str:
    """Return a single-line remote address for diagnostics."""
    return (
        (request.remote or "unknown")
        .replace("\r", "")
        .replace(
            "\n",
            "",
        )
    )


class OneBotPlatform:
    """Own OneBot listener, native WebSockets, and echo-based APIs."""

    def __init__(
        self,
        *,
        event_task_hard_cap: int = _DEFAULT_EVENT_TASK_HARD_CAP,
        watchdog_interval: float = 10.0,
        api_timeout: float = 15.0,
        shutdown_timeout: float = _DEFAULT_SHUTDOWN_TIMEOUT,
    ) -> None:
        if event_task_hard_cap <= 0:
            raise ValueError("event_task_hard_cap must be positive")
        if watchdog_interval <= 0:
            raise ValueError("watchdog_interval must be positive")
        if api_timeout <= 0:
            raise ValueError("api_timeout must be positive")
        if shutdown_timeout <= 0:
            raise ValueError("shutdown_timeout must be positive")
        self._event_task_hard_cap = event_task_hard_cap
        self._watchdog_interval = watchdog_interval
        self._api_timeout = api_timeout
        self._shutdown_timeout = shutdown_timeout
        self._ws_host = _DEFAULT_WS_HOST
        self._bind_host = _DEFAULT_WS_HOST
        self._ws_port = 6199
        self._access_token = ""
        self._bound_externally = False
        self._auth_required = False
        self._app: web.Application | None = None
        self._runner: web.AppRunner | None = None
        self._site: web.TCPSite | None = None
        self._connections: set[web.WebSocketResponse] = set()
        self._connection_transports: dict[
            web.WebSocketResponse,
            asyncio.BaseTransport,
        ] = {}
        self._pending_calls: dict[str, asyncio.Future[dict[str, Any]]] = {}
        self._event_tasks: set[asyncio.Task[None]] = set()
        self._watchdog_task: asyncio.Task[None] | None = None
        self._event_handler: EventHandler | None = None
        self._endpoint_handler: EndpointHandler | None = None
        self._last_endpoint: OneBotEndpoint | None = None
        self._desired_endpoint: OneBotEndpoint | None = None
        self._desired_endpoint_operation: str | None = None
        self._endpoint_publication_pending = False
        self._accepting = False
        self._stopping = False
        self._prepared = False
        self._runner_event_dropped_total = 0
        self._platform_rebind_total = 0

    @property
    def listen_port(self) -> int | None:
        """Return the actual bound port, if a listener exists."""
        if self._site is None:
            return None
        server = getattr(self._site, "_server", None)
        if server is None:
            return None
        ports = {
            int(bound_socket.getsockname()[1])
            for bound_socket in server.sockets or ()
        }
        if len(ports) != 1:
            return None
        return ports.pop()

    @property
    def endpoint(self) -> OneBotEndpoint | None:
        """Return the most recently reported endpoint."""
        return self._last_endpoint

    async def prepare(
        self,
        config: Mapping[str, Any],
        secret: Mapping[str, str],
    ) -> None:
        """Validate non-secret config and consume the access token."""
        host = _normalize_host(config.get("ws_host", _DEFAULT_WS_HOST))
        port = config.get("ws_port", 6199)
        if (
            not isinstance(port, int)
            or isinstance(port, bool)
            or not 0 <= port <= 65535
        ):
            raise ValueError("OneBot ws_port must be between 0 and 65535")
        access_token = secret.get("access_token", "")
        if not isinstance(access_token, str):
            raise ValueError("OneBot access_token must be a string")
        self._ws_host = host
        self._bind_host = await _resolve_dynamic_bind_host(host, port)
        self._ws_port = port
        self._access_token = access_token.strip()
        self._bound_externally = _is_external_host(host)
        self._auth_required = self._bound_externally or bool(
            self._access_token,
        )
        self._prepared = True

    async def start(
        self,
        event_handler: EventHandler,
        endpoint_handler: EndpointHandler,
    ) -> None:
        """Bind the listener after commit and start health recovery."""
        if not self._prepared:
            raise RuntimeError("OneBot platform is not prepared")
        if self._watchdog_task is not None:
            return
        self._event_handler = event_handler
        self._endpoint_handler = endpoint_handler
        self._stopping = False
        await self._start_ws_server("register")
        self._watchdog_task = asyncio.create_task(self._watchdog_loop())

    async def quiesce(self) -> None:
        """Stop admitting new platform traffic and release the listener."""
        await self.close()

    async def stop_accepting(self) -> None:
        """Release the listener while preserving established WebSockets."""
        self._stopping = True
        self._accepting = False
        site = self._site
        server = getattr(site, "_server", None)
        if server is not None:
            server.close()
        task = self._watchdog_task
        self._watchdog_task = None
        if task is not None and task is not asyncio.current_task():
            task.cancel()
        if site is not None:
            try:
                await site.stop()
            except Exception:
                logger.debug("onebot: site stop failed", exc_info=True)
        if task is not None and task is not asyncio.current_task():
            await asyncio.gather(task, return_exceptions=True)

    async def close(self, *, deadline: float | None = None) -> None:
        """Release all listener resources idempotently."""
        self._stopping = True
        self._accepting = False
        loop = asyncio.get_running_loop()
        shutdown_deadline = loop.time() + self._shutdown_timeout
        if deadline is not None:
            shutdown_deadline = min(shutdown_deadline, deadline)
        cleanup = asyncio.create_task(
            self._close_resources(shutdown_deadline),
        )
        try:
            await asyncio.shield(cleanup)
        except asyncio.CancelledError:
            await cleanup
            raise

    def health_snapshot(self) -> dict[str, Any]:
        """Return non-secret listener diagnostics."""
        return {
            "prepared": self._prepared,
            "listening": self.listen_port is not None,
            "accepting": self._accepting,
            "listen_port": self.listen_port,
            "connection_count": len(self._connections),
            "runner_event_dropped_total": (self._runner_event_dropped_total),
            "platform_rebind_total": self._platform_rebind_total,
        }

    async def _start_ws_server(self, operation: str) -> None:
        """Attempt one bind and report either ready or degraded state."""
        self._accepting = False
        self._app = web.Application()
        self._app.router.add_get("/ws", self._handle_ws_connection)
        self._app.router.add_get("/ws/", self._handle_ws_connection)
        self._runner = web.AppRunner(self._app)
        await self._runner.setup()
        self._site = web.TCPSite(
            self._runner,
            self._bind_host,
            self._ws_port,
        )
        try:
            await self._site.start()
        except OSError:
            await self._cleanup_failed_bind()
            await self._report_endpoint(operation, "degraded")
            logger.warning(
                f"onebot: bind failed for "
                f"{self._bind_host}:{self._ws_port}; watchdog will retry",
            )
            return
        if self.listen_port is None:
            await self._cleanup_failed_bind()
            raise OneBotPlatformError("INGRESS_ENDPOINT_AMBIGUOUS")
        await self._report_endpoint(operation, "ready")
        logger.info(
            f"onebot: reverse WS server listening on "
            f"{self._bind_host}:{self.listen_port}",
        )

    async def _cleanup_failed_bind(self) -> None:
        """Discard a failed aiohttp bind attempt."""
        self._site = None
        runner = self._runner
        self._runner = None
        self._app = None
        if runner is not None:
            try:
                await runner.cleanup()
            except Exception:
                logger.debug(
                    "onebot: failed bind cleanup failed",
                    exc_info=True,
                )

    async def _report_endpoint(
        self,
        operation: str,
        readiness: str,
    ) -> None:
        """Publish the actual listener state before accepting traffic."""
        port = self.listen_port
        if port is None:
            port = self._ws_port
        endpoint = OneBotEndpoint(
            host=self._bind_host,
            port=port,
            readiness=readiness,
            bound_externally=self._bound_externally,
            auth_required=self._auth_required,
        )
        self._desired_endpoint_operation = operation
        self._desired_endpoint = endpoint
        self._endpoint_publication_pending = True
        await self._publish_desired_endpoint()

    async def _publish_desired_endpoint(self) -> None:
        """Publish and acknowledge the latest desired endpoint state."""
        handler = self._endpoint_handler
        if handler is None:
            raise RuntimeError("OneBot endpoint handler is unavailable")
        operation = self._desired_endpoint_operation
        endpoint = self._desired_endpoint
        if not self._endpoint_publication_pending:
            return
        if operation is None or endpoint is None:
            raise RuntimeError("OneBot desired endpoint is unavailable")
        await handler(operation, endpoint)
        if (
            self._desired_endpoint_operation != operation
            or self._desired_endpoint is not endpoint
        ):
            return
        self._last_endpoint = endpoint
        self._endpoint_publication_pending = False
        if (
            endpoint.readiness == "ready"
            and not self._stopping
            and self.listen_port == endpoint.port
        ):
            self._accepting = True

    async def _close_resources(self, deadline: float) -> None:
        """Close all platform resources within one absolute deadline."""
        task = self._watchdog_task
        self._watchdog_task = None
        if task is not None and task is not asyncio.current_task():
            task.cancel()
            await self._wait_tasks_until((task,), deadline)
        await self._stop_ws_server(deadline)
        event_tasks = tuple(self._event_tasks)
        for event_task in event_tasks:
            event_task.cancel()
        await self._wait_tasks_until(event_tasks, deadline)
        self._event_tasks.clear()

    async def _stop_ws_server(self, deadline: float | None = None) -> None:
        """Stop the listener and settle native waiters within a deadline."""
        self._accepting = False
        if deadline is None:
            deadline = (
                asyncio.get_running_loop().time() + self._shutdown_timeout
            )
        connections = tuple(self._connections)
        transports = {
            connection: self._connection_transports.get(connection)
            for connection in connections
        }
        site = self._site
        runner = self._runner
        server = getattr(site, "_server", None)
        if server is not None:
            server.close()
        for future in tuple(self._pending_calls.values()):
            if not future.done():
                future.cancel()
        self._pending_calls.clear()
        close_tasks = tuple(
            asyncio.create_task(connection.close())
            for connection in connections
        )
        try:
            await self._wait_tasks_until(close_tasks, deadline)
            if site is not None:
                await self._run_cleanup_until(
                    site.stop(),
                    deadline,
                    "site stop",
                )
            if runner is not None:
                await self._run_cleanup_until(
                    runner.cleanup(),
                    deadline,
                    "runner cleanup",
                )
        finally:
            for connection in connections:
                try:
                    connection.force_close()
                except Exception:
                    logger.debug(
                        "onebot: WebSocket force close failed",
                        exc_info=True,
                    )
                transport = transports.get(connection)
                if transport is not None:
                    transport.close()
                self._connections.discard(connection)
                self._connection_transports.pop(connection, None)
            if self._site is site:
                self._site = None
            if self._runner is runner:
                self._runner = None
            self._app = None

    async def _run_cleanup_until(
        self,
        awaitable: Awaitable[Any],
        deadline: float,
        operation: str,
    ) -> None:
        """Run one cleanup operation without exceeding the deadline."""
        task = asyncio.ensure_future(awaitable)
        await self._wait_tasks_until((task,), deadline)
        if not task.done() or task.cancelled():
            logger.warning(f"onebot: {operation} timed out")
            return
        try:
            task.result()
        except Exception:
            logger.debug(f"onebot: {operation} failed", exc_info=True)

    async def _wait_tasks_until(
        self,
        tasks: tuple[asyncio.Task[Any], ...],
        deadline: float,
    ) -> None:
        """Wait for tasks concurrently until one shared deadline."""
        if not tasks:
            return
        remaining = max(
            0.0,
            deadline - asyncio.get_running_loop().time(),
        )
        done, pending = await asyncio.wait(tasks, timeout=remaining)
        for task in done:
            self._consume_cleanup_task(task)
        for task in pending:
            task.cancel()
            task.add_done_callback(self._consume_cleanup_task)

    @staticmethod
    def _consume_cleanup_task(task: asyncio.Task[Any]) -> None:
        """Consume a late cleanup result after its caller deadline."""
        if task.cancelled():
            return
        try:
            task.exception()
        except Exception:
            return

    async def _watchdog_loop(self) -> None:
        """Recover a missing or unhealthy listener with endpoint updates."""
        while not self._stopping:
            await asyncio.sleep(self._watchdog_interval)
            if self._stopping:
                return
            try:
                await self._watchdog_iteration()
            except Exception:
                logger.warning(
                    "onebot: watchdog recovery attempt failed",
                    exc_info=True,
                )

    async def _watchdog_iteration(self) -> None:
        """Reconcile endpoint publication or perform one listener rebind."""
        if await self._is_server_healthy():
            if self._endpoint_publication_pending:
                await self._publish_desired_endpoint()
            return
        self._accepting = False
        if (
            self._last_endpoint is not None
            or self._desired_endpoint is not None
        ):
            try:
                await self._report_endpoint("update", "degraded")
            except Exception:
                logger.warning(
                    "onebot: degraded endpoint update failed",
                    exc_info=True,
                )
        await self._stop_ws_server()
        if self._stopping:
            return
        try:
            await self._start_ws_server("update")
        finally:
            if self._site is not None and self.listen_port is not None:
                self._platform_rebind_total += 1

    async def _is_server_healthy(self) -> bool:
        """Return whether the current TCP listener accepts connections."""
        port = self.listen_port
        if self._site is None or port is None:
            return False
        try:
            _, writer = await asyncio.wait_for(
                asyncio.open_connection(_probe_host(self._bind_host), port),
                timeout=3.0,
            )
            writer.close()
            await writer.wait_closed()
            return True
        except (OSError, asyncio.TimeoutError):
            return False

    def _token_authorized(self, request: web.Request) -> bool:
        """Validate the OneBot Authorization header."""
        provided = _extract_auth_token(
            request.headers.get("Authorization", ""),
        )
        return bool(provided) and _tokens_match(
            provided,
            self._access_token,
        )

    async def _handle_ws_connection(
        self,
        request: web.Request,
    ) -> web.StreamResponse:
        """Handle one OneBot reverse WebSocket connection."""
        if not self._accepting:
            return web.Response(status=503, text="Unavailable")
        if self._auth_required and not self._access_token:
            logger.error(
                f"onebot: rejected connection from {_log_remote(request)}: "
                f"ws_host={self._ws_host} requires access_token",
            )
            return web.Response(status=401, text="Unauthorized")
        if self._access_token and not self._token_authorized(request):
            logger.warning(
                f"onebot: rejected connection from "
                f"{_log_remote(request)} (bad token)",
            )
            return web.Response(status=401, text="Unauthorized")

        connection = web.WebSocketResponse()
        await connection.prepare(request)
        self._connections.add(connection)
        transport = request.transport
        if transport is not None:
            self._connection_transports[connection] = transport
        try:
            async for message in connection:
                if message.type == aiohttp.WSMsgType.TEXT:
                    self._handle_text_frame(message.data)
                elif message.type in {
                    aiohttp.WSMsgType.ERROR,
                    aiohttp.WSMsgType.CLOSE,
                }:
                    break
        except Exception:
            logger.exception("onebot: WebSocket connection failed")
        finally:
            self._connections.discard(connection)
            self._connection_transports.pop(connection, None)
        return connection

    def _handle_text_frame(self, value: str) -> None:
        """Route native echo responses and platform events separately."""
        try:
            data = json.loads(value)
        except json.JSONDecodeError:
            logger.warning(f"onebot: invalid JSON: {value[:200]}")
            return
        if not isinstance(data, Mapping):
            logger.warning("onebot: native WebSocket payload is not an object")
            return
        native = dict(data)
        if "echo" in native:
            self._handle_api_response(native)
            return
        handler = self._event_handler
        if handler is None:
            return
        self._spawn_event_task(handler(native))

    def _spawn_event_task(
        self,
        coro: Coroutine[Any, Any, None],
    ) -> None:
        """Schedule native event work without blocking echo responses."""
        if len(self._event_tasks) >= self._event_task_hard_cap:
            self._runner_event_dropped_total += 1
            coro.close()
            logger.warning(
                f"onebot: event task cap "
                f"{self._event_task_hard_cap} reached; event dropped",
            )
            return
        task = asyncio.create_task(coro)
        self._event_tasks.add(task)
        task.add_done_callback(self._event_task_done)

    def _event_task_done(self, task: asyncio.Task[None]) -> None:
        """Release a completed event task and consume its failure."""
        self._event_tasks.discard(task)
        if task.cancelled():
            return
        try:
            task.result()
        except Exception:
            logger.exception("onebot: platform event handler failed")

    async def call_api(
        self,
        action: str,
        params: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Call a OneBot action through the reverse WebSocket echo path."""
        if not self._connections:
            raise OneBotPlatformError("INGRESS_CONNECTION_UNKNOWN")
        echo = str(uuid.uuid4())
        future: asyncio.Future[
            dict[str, Any]
        ] = asyncio.get_running_loop().create_future()
        self._pending_calls[echo] = future
        payload = json.dumps(
            {
                "action": action,
                "params": dict(params),
                "echo": echo,
            },
            ensure_ascii=False,
        )
        sent = False
        for connection in tuple(self._connections):
            try:
                await connection.send_str(payload)
                sent = True
                break
            except Exception:
                continue
        if not sent:
            self._pending_calls.pop(echo, None)
            raise OneBotPlatformError("INGRESS_CONNECTION_UNKNOWN")
        try:
            result = await asyncio.wait_for(
                future,
                timeout=self._api_timeout,
            )
        except asyncio.TimeoutError as exc:
            raise OneBotPlatformError(
                "PLATFORM_RESULT_UNKNOWN",
                side_effect_possible=True,
            ) from exc
        finally:
            self._pending_calls.pop(echo, None)
        if result.get("retcode") != 0:
            raise OneBotPlatformError("PLATFORM_API_FAILED")
        return result

    def _handle_api_response(self, data: dict[str, Any]) -> None:
        """Resolve an echo waiter without blocking the WebSocket reader."""
        echo = data.get("echo")
        if not isinstance(echo, str):
            return
        future = self._pending_calls.get(echo)
        if future is not None and not future.done():
            future.set_result(data)

    async def send_message(
        self,
        to_handle: str,
        content_parts: tuple[dict[str, Any], ...],
    ) -> None:
        """Encode stable content locators into OneBot v11 operations."""
        is_group, target = self._resolve_target(to_handle)
        action = "send_group_msg" if is_group else "send_private_msg"
        target_key = "group_id" if is_group else "user_id"
        segments: list[dict[str, Any]] = []

        async def flush_segments() -> None:
            if not segments:
                return
            await self.call_api(
                action,
                {target_key: target, "message": list(segments)},
            )
            segments.clear()

        for part in content_parts:
            content_type = part.get("type")
            if content_type == "text":
                text = str(part.get("text") or "").strip()
                if text:
                    segments.append(
                        {"type": "text", "data": {"text": text}},
                    )
            elif content_type in {"image", "video", "audio"}:
                locator_key = (
                    "data"
                    if content_type == "audio"
                    else f"{content_type}_url"
                )
                locator = str(part.get(locator_key) or "")
                if locator:
                    segment_type = (
                        "record" if content_type == "audio" else content_type
                    )
                    segments.append(
                        {
                            "type": segment_type,
                            "data": {"file": locator},
                        },
                    )
            elif content_type == "file":
                await flush_segments()
                await self._send_file(is_group, target, part)
        await flush_segments()

    async def _send_file(
        self,
        is_group: bool,
        target: int,
        part: Mapping[str, Any],
    ) -> None:
        """Send one file locator through the OneBot upload action."""
        locator = str(part.get("file_url") or "")
        if not locator:
            return
        name = str(part.get("filename") or "file")
        action = "upload_group_file" if is_group else "upload_private_file"
        target_key = "group_id" if is_group else "user_id"
        await self.call_api(
            action,
            {target_key: target, "file": locator, "name": name},
        )

    @staticmethod
    def _resolve_target(to_handle: str) -> tuple[bool, int]:
        """Resolve the existing OneBot group/private target syntax."""
        is_group = to_handle.startswith("group:")
        value = to_handle.removeprefix("group:") if is_group else to_handle
        try:
            return is_group, int(value)
        except ValueError as exc:
            raise OneBotPlatformError("OUTBOUND_TARGET_UNKNOWN") from exc


__all__ = [
    "OneBotEndpoint",
    "OneBotPlatform",
    "OneBotPlatformError",
]
