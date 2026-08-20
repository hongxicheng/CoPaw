# -*- coding: utf-8 -*-
"""Persist request-scoped Feishu response routes."""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Mapping
from dataclasses import dataclass
import hashlib
import json
import time
from typing import Any

from ....channel_protocol import (
    HostStateParams,
    ResponseFinishParams,
    ResponseOutcome,
)
from ....channel_protocol.lifecycle import LifecycleController


RESPONSE_HANDLE_PREFIX = "feishu:reply:"
RESPONSE_ROUTE_KEY_PREFIX = "feishu.response_routes."
RESPONSE_ROUTE_SCHEMA_VERSION = 1
RESPONSE_ROUTE_SHARD_COUNT = 16
RESPONSE_ROUTE_MAX_SHARD_BYTES = 48 * 1024
RESPONSE_ROUTE_MAX_ENTRIES = 512
RESPONSE_TOMBSTONE_TTL_MS = 24 * 60 * 60 * 1000


class FeishuResponseRouteError(RuntimeError):
    """Report invalid or unavailable persisted response routing."""


class FeishuResponseRouteCapacityError(FeishuResponseRouteError):
    """Report a bounded response route store capacity failure."""


@dataclass(frozen=True)
class FeishuResponseTarget:
    """One request-scoped Feishu delivery target."""

    receive_id_type: str
    receive_id: str
    thread_message_id: str = ""


@dataclass(frozen=True)
class _ResponseRoute:
    """One active route or durable closed tombstone."""

    target: FeishuResponseTarget | None
    outcome: ResponseOutcome | None = None
    cleanup_complete: bool = False
    closed_at_ms: int | None = None

    @property
    def active(self) -> bool:
        """Return whether this route can still target Feishu."""
        return self.outcome is None


class FeishuResponseRouteStore:
    """Own one bounded, deterministically sharded route checkpoint."""

    def __init__(
        self,
        *,
        shard_count: int = RESPONSE_ROUTE_SHARD_COUNT,
        max_shard_bytes: int = RESPONSE_ROUTE_MAX_SHARD_BYTES,
        max_entries: int = RESPONSE_ROUTE_MAX_ENTRIES,
        tombstone_ttl_ms: int = RESPONSE_TOMBSTONE_TTL_MS,
        clock_ms: Callable[[], int] | None = None,
    ) -> None:
        if shard_count <= 0:
            raise ValueError("shard_count must be positive")
        if max_shard_bytes <= 0:
            raise ValueError("max_shard_bytes must be positive")
        if max_entries <= 0:
            raise ValueError("max_entries must be positive")
        if tombstone_ttl_ms < 0:
            raise ValueError("tombstone_ttl_ms must not be negative")
        self.shard_count = shard_count
        self.max_shard_bytes = max_shard_bytes
        self.max_entries = max_entries
        self.tombstone_ttl_ms = tombstone_ttl_ms
        self._clock_ms = clock_ms or (lambda: time.time_ns() // 1_000_000)
        self._peer: Any = None
        self._identity: Any = None
        self._routes: dict[str, _ResponseRoute] = {}
        self._discard_pending: set[str] = set()
        self._lock = asyncio.Lock()

    def bind(self, peer: Any, identity: Any) -> None:
        """Bind the store to one Runner generation and Host RPC peer."""
        self._peer = peer
        self._identity = identity

    @staticmethod
    def response_handle(event_id: str) -> str:
        """Derive one fixed-length opaque handle from a platform event."""
        digest = hashlib.sha256(event_id.encode("utf-8")).hexdigest()
        return f"{RESPONSE_HANDLE_PREFIX}{digest}"

    async def restore(self, lifecycle: LifecycleController) -> None:
        """Restore active routes and unexpired tombstones before commit."""
        restored: dict[str, _ResponseRoute] = {}
        async with self._lock:
            for shard in range(self.shard_count):
                value = await self._read_shard(shard)
                for handle, route in self._decode_shard(shard, value).items():
                    if handle in restored:
                        raise FeishuResponseRouteError(
                            f"duplicate response route {handle}",
                        )
                    restored[handle] = route
            now_ms = self._clock_ms()
            live_count = sum(
                not self._expired(route, now_ms) for route in restored.values()
            )
            if live_count > self.max_entries:
                raise FeishuResponseRouteCapacityError(
                    "persisted response route capacity is exceeded",
                )
            self._routes = restored
        for handle, route in restored.items():
            if self._expired(route, now_ms):
                continue
            await lifecycle.restore_response_scope(
                handle,
                route.outcome,
                cleanup_complete=route.cleanup_complete,
            )

    async def admit_active(
        self,
        response_handle: str,
        target: FeishuResponseTarget,
    ) -> None:
        """Persist one active route before its event is submitted."""
        route = _ResponseRoute(target=target)
        async with self._lock:
            existing = self._routes.get(response_handle)
            if existing is not None:
                if existing == route:
                    return
                raise FeishuResponseRouteError(
                    "response handle already has a different route",
                )
            if len(self._routes) >= self.max_entries:
                raise FeishuResponseRouteCapacityError(
                    "response route capacity is exhausted",
                )
            shard = self._shard_for(response_handle)
            updated = self._shard_routes(shard)
            updated[response_handle] = route
            await self._write_shard(shard, updated)
            self._routes[response_handle] = route

    async def resolve(self, response_handle: str) -> FeishuResponseTarget:
        """Resolve an active opaque handle without deciding admission."""
        async with self._lock:
            route = self._routes.get(response_handle)
            if route is None or not route.active or route.target is None:
                raise KeyError("Feishu response target is unavailable")
            return route.target

    async def rollback_active(self, response_handle: str) -> None:
        """Durably remove one event route rejected before Core admission."""
        async with self._lock:
            route = self._routes.get(response_handle)
            if route is None:
                return
            if not route.active:
                raise FeishuResponseRouteError(
                    "a closed response route cannot be rolled back",
                )
            shard = self._shard_for(response_handle)
            updated = self._shard_routes(shard)
            updated.pop(response_handle, None)
            await self._write_shard(shard, updated)
            self._routes.pop(response_handle, None)

    async def begin_finish(self, params: ResponseFinishParams) -> None:
        """Persist the durable close linearization point."""
        async with self._lock:
            existing = self._routes.get(params.response_handle)
            if existing is None:
                raise FeishuResponseRouteError(
                    "response route is unavailable during finish",
                )
            if not existing.active:
                if existing.outcome is not params.outcome:
                    raise FeishuResponseRouteError(
                        "response route outcome conflicts with finish",
                    )
                return
            tombstone = _ResponseRoute(
                target=None,
                outcome=params.outcome,
                cleanup_complete=False,
                closed_at_ms=self._clock_ms(),
            )
            shard = self._shard_for(params.response_handle)
            updated = self._shard_routes(shard)
            updated[params.response_handle] = tombstone
            await self._write_shard(shard, updated)
            self._routes[params.response_handle] = tombstone

    async def complete_finish(self, params: ResponseFinishParams) -> None:
        """Persist completion after Driver-owned resources are released."""
        async with self._lock:
            existing = self._routes.get(params.response_handle)
            if existing is None or existing.active:
                raise FeishuResponseRouteError(
                    "response tombstone is unavailable during cleanup",
                )
            if existing.outcome is not params.outcome:
                raise FeishuResponseRouteError(
                    "response route outcome conflicts with cleanup",
                )
            if existing.cleanup_complete:
                return
            completed = _ResponseRoute(
                target=None,
                outcome=existing.outcome,
                cleanup_complete=True,
                closed_at_ms=existing.closed_at_ms,
            )
            shard = self._shard_for(params.response_handle)
            updated = self._shard_routes(shard)
            updated[params.response_handle] = completed
            await self._write_shard(shard, updated)
            self._routes[params.response_handle] = completed

    async def gc_expired(self, lifecycle: LifecycleController) -> None:
        """Delete expired completed tombstones before discarding scopes."""
        now_ms = self._clock_ms()
        removed: list[str] = []
        async with self._lock:
            expired_by_shard: dict[int, list[str]] = {}
            for handle, route in self._routes.items():
                if self._expired(route, now_ms):
                    expired_by_shard.setdefault(
                        self._shard_for(handle),
                        [],
                    ).append(handle)
            for shard, handles in expired_by_shard.items():
                updated = self._shard_routes(shard)
                for handle in handles:
                    updated.pop(handle, None)
                await self._write_shard(shard, updated)
                for handle in handles:
                    self._routes.pop(handle, None)
                    removed.append(handle)
            self._discard_pending.update(removed)
            pending = tuple(self._discard_pending)
        for handle in pending:
            try:
                await lifecycle.discard_response_scope(handle)
            except Exception:
                continue
            async with self._lock:
                self._discard_pending.discard(handle)

    async def snapshot(self) -> dict[str, dict[str, object]]:
        """Return a test and diagnostics projection of persisted routes."""
        async with self._lock:
            return {
                handle: self._route_value(route)
                for handle, route in self._routes.items()
            }

    def state_key(self, shard: int) -> str:
        """Return one deterministic Host State shard key."""
        if shard < 0 or shard >= self.shard_count:
            raise ValueError("response route shard is out of range")
        return f"{RESPONSE_ROUTE_KEY_PREFIX}{shard:02x}"

    def shard_for_handle(self, response_handle: str) -> int:
        """Expose deterministic placement for tests and diagnostics."""
        return self._shard_for(response_handle)

    async def _read_shard(self, shard: int) -> object:
        if self._peer is None or self._identity is None:
            raise FeishuResponseRouteError("response route store is unbound")
        result = await self._peer.call(
            "host.state.get",
            HostStateParams(
                channel_key=self._identity.channel_key,
                instance_id=self._identity.instance_id,
                generation=self._identity.generation,
                key=self.state_key(shard),
            ).to_mapping(),
        )
        if not isinstance(result, Mapping) or not result.get("found"):
            return {}
        if result.get("schema_version") != RESPONSE_ROUTE_SCHEMA_VERSION:
            raise FeishuResponseRouteError(
                "unsupported Feishu response route schema",
            )
        return result.get("value")

    async def _write_shard(
        self,
        shard: int,
        routes: Mapping[str, _ResponseRoute],
    ) -> None:
        if self._peer is None or self._identity is None:
            raise FeishuResponseRouteError("response route store is unbound")
        value = {
            handle: self._route_value(route)
            for handle, route in sorted(routes.items())
        }
        encoded = json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
        ).encode("utf-8")
        if len(encoded) > self.max_shard_bytes:
            raise FeishuResponseRouteCapacityError(
                "response route shard capacity is exhausted",
            )
        params = HostStateParams(
            channel_key=self._identity.channel_key,
            instance_id=self._identity.instance_id,
            generation=self._identity.generation,
            key=self.state_key(shard),
            schema_version=RESPONSE_ROUTE_SCHEMA_VERSION,
            value=value,
        )
        if value:
            await self._peer.call("host.state.put", params.to_mapping())
        else:
            await self._peer.call("host.state.delete", params.to_mapping())

    def _decode_shard(
        self,
        shard: int,
        value: object,
    ) -> dict[str, _ResponseRoute]:
        if not isinstance(value, Mapping):
            raise FeishuResponseRouteError(
                "Feishu response route shard must be an object",
            )
        result: dict[str, _ResponseRoute] = {}
        for handle, item in value.items():
            if not isinstance(handle, str) or not isinstance(item, Mapping):
                raise FeishuResponseRouteError(
                    "Feishu response route entry is invalid",
                )
            if self._shard_for(handle) != shard:
                raise FeishuResponseRouteError(
                    "Feishu response route is in the wrong shard",
                )
            result[handle] = self._route_from_value(item)
        return result

    @staticmethod
    def _route_from_value(value: Mapping[str, object]) -> _ResponseRoute:
        state = value.get("state")
        if state == "active":
            receive_type = value.get("receive_id_type")
            receive_id = value.get("receive_id")
            thread_message_id = value.get("thread_message_id", "")
            if (
                not isinstance(receive_type, str)
                or not receive_type
                or not isinstance(receive_id, str)
                or not receive_id
                or not isinstance(thread_message_id, str)
            ):
                raise FeishuResponseRouteError(
                    "Feishu active response route is invalid",
                )
            return _ResponseRoute(
                target=FeishuResponseTarget(
                    receive_type,
                    receive_id,
                    thread_message_id,
                ),
            )
        if state != "closed":
            raise FeishuResponseRouteError(
                "Feishu response route state is invalid",
            )
        outcome_value = value.get("outcome")
        cleanup_complete = value.get("cleanup_complete")
        closed_at_ms = value.get("closed_at_ms")
        try:
            outcome = ResponseOutcome(outcome_value)
        except (TypeError, ValueError) as exc:
            raise FeishuResponseRouteError(
                "Feishu response route outcome is invalid",
            ) from exc
        if not isinstance(cleanup_complete, bool):
            raise FeishuResponseRouteError(
                "Feishu response route cleanup state is invalid",
            )
        if not isinstance(closed_at_ms, int) or closed_at_ms < 0:
            raise FeishuResponseRouteError(
                "Feishu response route close time is invalid",
            )
        return _ResponseRoute(
            target=None,
            outcome=outcome,
            cleanup_complete=cleanup_complete,
            closed_at_ms=closed_at_ms,
        )

    @staticmethod
    def _route_value(route: _ResponseRoute) -> dict[str, object]:
        if route.active:
            if route.target is None:
                raise FeishuResponseRouteError(
                    "active response route has no target",
                )
            value: dict[str, object] = {
                "state": "active",
                "receive_id_type": route.target.receive_id_type,
                "receive_id": route.target.receive_id,
            }
            if route.target.thread_message_id:
                value["thread_message_id"] = route.target.thread_message_id
            return value
        if route.outcome is None or route.closed_at_ms is None:
            raise FeishuResponseRouteError(
                "closed response route has incomplete state",
            )
        return {
            "state": "closed",
            "outcome": route.outcome.value,
            "cleanup_complete": route.cleanup_complete,
            "closed_at_ms": route.closed_at_ms,
        }

    def _shard_routes(self, shard: int) -> dict[str, _ResponseRoute]:
        return {
            handle: route
            for handle, route in self._routes.items()
            if self._shard_for(handle) == shard
        }

    def _shard_for(self, response_handle: str) -> int:
        digest = hashlib.sha256(response_handle.encode("utf-8")).digest()
        return int.from_bytes(digest[:8], "big") % self.shard_count

    def _expired(self, route: _ResponseRoute, now_ms: int) -> bool:
        return bool(
            not route.active
            and route.cleanup_complete
            and route.closed_at_ms is not None
            and now_ms - route.closed_at_ms >= self.tombstone_ttl_ms,
        )


__all__ = [
    "FeishuResponseRouteCapacityError",
    "FeishuResponseRouteError",
    "FeishuResponseRouteStore",
    "FeishuResponseTarget",
    "RESPONSE_HANDLE_PREFIX",
    "RESPONSE_ROUTE_MAX_ENTRIES",
]
