# -*- coding: utf-8 -*-
"""Persist versioned Feishu response route aggregate snapshots."""

from __future__ import annotations

import asyncio
from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
import hashlib
import json
from typing import Any

from ....channel_protocol import HostStateParams
from ....channel_protocol.errors import (
    ProtocolValidationError,
    RpcCancelledError,
    RpcClosedError,
    RpcError,
    RpcTimeoutError,
)
from ....channel_protocol.response_lifecycle import (
    ResponseCheckpointUnknownError,
    ResponseRouteSnapshot,
)


RESPONSE_HANDLE_PREFIX = "feishu:reply:"
RESPONSE_ROUTE_KEY_PREFIX = "feishu.response_routes."
RESPONSE_ROUTE_STATE_SCHEMA_VERSION = 1
RESPONSE_ROUTE_SHARD_COUNT = 16
RESPONSE_ROUTE_MAX_SHARD_BYTES = 48 * 1024
RESPONSE_ROUTE_MAX_ENTRIES = 512


class FeishuResponseRouteError(RuntimeError):
    """Report invalid or unavailable response route checkpoint data."""


class FeishuResponseRouteCapacityError(FeishuResponseRouteError):
    """Report a bounded response route checkpoint failure."""


class _ShardSettlement(StrEnum):
    """Describe the result known for the latest shard mutation."""

    CONFIRMED = "confirmed"
    REJECTED = "rejected"
    UNKNOWN = "unknown"


@dataclass
class _ShardState:
    """Track desired and confirmed state for one durable shard."""

    durable: dict[str, ResponseRouteSnapshot]
    desired: dict[str, ResponseRouteSnapshot]
    dirty: bool = False
    settlement: _ShardSettlement = _ShardSettlement.CONFIRMED


class FeishuResponseRouteCheckpoint:
    """Load and persist aggregate snapshots without owning transitions."""

    def __init__(
        self,
        *,
        shard_count: int = RESPONSE_ROUTE_SHARD_COUNT,
        max_shard_bytes: int = RESPONSE_ROUTE_MAX_SHARD_BYTES,
        max_entries: int = RESPONSE_ROUTE_MAX_ENTRIES,
    ) -> None:
        if shard_count <= 0:
            raise ValueError("shard_count must be positive")
        if max_shard_bytes <= 0:
            raise ValueError("max_shard_bytes must be positive")
        if max_entries <= 0:
            raise ValueError("max_entries must be positive")
        self.shard_count = shard_count
        self.max_shard_bytes = max_shard_bytes
        self.max_entries = max_entries
        self._peer: Any = None
        self._identity: Any = None
        self._shards = {
            shard: _ShardState(durable={}, desired={})
            for shard in range(shard_count)
        }
        self._lock = asyncio.Lock()

    def bind(self, peer: Any, identity: Any) -> None:
        """Bind the checkpoint to one Runner generation and Host peer."""
        self._peer = peer
        self._identity = identity

    @staticmethod
    def response_handle(event_id: str) -> str:
        """Derive one fixed-length opaque handle from a platform event."""
        digest = hashlib.sha256(event_id.encode("utf-8")).hexdigest()
        return f"{RESPONSE_HANDLE_PREFIX}{digest}"

    async def load(self) -> tuple[ResponseRouteSnapshot, ...]:
        """Load all persisted snapshots before lifecycle restoration."""
        async with self._lock:
            restored: dict[str, ResponseRouteSnapshot] = {}
            for shard in range(self.shard_count):
                value = await self._read_shard(shard)
                decoded = self._decode_shard(shard, value)
                overlap = set(restored).intersection(decoded)
                if overlap:
                    handle = sorted(overlap)[0]
                    raise FeishuResponseRouteError(
                        f"duplicate response route {handle}",
                    )
                restored.update(decoded)
            if len(restored) > self.max_entries:
                raise FeishuResponseRouteCapacityError(
                    "persisted response route capacity is exceeded",
                )
            for shard in range(self.shard_count):
                decoded = {
                    handle: snapshot
                    for handle, snapshot in restored.items()
                    if self.shard_for_handle(handle) == shard
                }
                self._shards[shard] = _ShardState(
                    durable=decoded.copy(),
                    desired=decoded.copy(),
                )
            return self._ordered_snapshots()

    async def put(
        self,
        snapshot: ResponseRouteSnapshot,
        provisional: bool = False,
    ) -> None:
        """Persist at least the requested aggregate snapshot version."""
        async with self._lock:
            shard = self.shard_for_handle(snapshot.response_handle)
            state = self._shards[shard]
            previous_desired = state.desired.copy()
            previous_dirty = state.dirty
            previous_settlement = state.settlement
            current = state.desired.get(snapshot.response_handle)
            if current is not None:
                if current.version > snapshot.version:
                    return
                if current.version == snapshot.version:
                    if current != snapshot:
                        raise FeishuResponseRouteError(
                            "response snapshot version conflicts",
                        )
                    if not state.dirty:
                        return
            elif len(self._all_desired()) >= self.max_entries:
                raise FeishuResponseRouteCapacityError(
                    "response route capacity is exhausted",
                )
            state.desired[snapshot.response_handle] = snapshot
            state.dirty = True
            try:
                await self._persist_shard(shard)
            except BaseException:
                if (
                    provisional
                    and state.settlement is _ShardSettlement.REJECTED
                ):
                    state.desired = previous_desired
                    state.dirty = previous_dirty
                    state.settlement = previous_settlement
                raise

    async def delete(self, response_handle: str, version: int) -> None:
        """Delete a snapshot unless a newer version is already durable."""
        async with self._lock:
            shard = self.shard_for_handle(response_handle)
            state = self._shards[shard]
            desired = state.desired.get(response_handle)
            durable = state.durable.get(response_handle)
            current = desired or durable
            if current is not None and current.version > version:
                return
            if desired is None and durable is None and not state.dirty:
                return
            state.desired.pop(response_handle, None)
            state.dirty = True
            await self._persist_shard(shard)

    async def snapshot(self) -> dict[str, dict[str, object]]:
        """Return the desired projection for tests and diagnostics."""
        async with self._lock:
            return {
                item.response_handle: item.to_mapping()
                for item in self._ordered_snapshots()
            }

    def state_key(self, shard: int) -> str:
        """Return one deterministic Host State shard key."""
        if shard < 0 or shard >= self.shard_count:
            raise ValueError("response route shard is out of range")
        return f"{RESPONSE_ROUTE_KEY_PREFIX}{shard:02x}"

    def shard_for_handle(self, response_handle: str) -> int:
        """Return deterministic placement for tests and diagnostics."""
        digest = hashlib.sha256(response_handle.encode("utf-8")).digest()
        return int.from_bytes(digest[:8], "big") % self.shard_count

    async def _read_shard(self, shard: int) -> object:
        if self._peer is None or self._identity is None:
            raise FeishuResponseRouteError(
                "response route checkpoint is unbound",
            )
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
        if result.get("schema_version") != RESPONSE_ROUTE_STATE_SCHEMA_VERSION:
            raise FeishuResponseRouteError(
                "unsupported Feishu response route schema",
            )
        return result.get("value")

    async def _write_shard(
        self,
        shard: int,
        snapshots: Mapping[str, ResponseRouteSnapshot],
    ) -> None:
        if self._peer is None or self._identity is None:
            raise FeishuResponseRouteError(
                "response route checkpoint is unbound",
            )
        value = {
            handle: snapshot.to_mapping()
            for handle, snapshot in sorted(snapshots.items())
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
            schema_version=RESPONSE_ROUTE_STATE_SCHEMA_VERSION,
            value=value,
        )
        method = "host.state.put" if value else "host.state.delete"
        await self._peer.call(method, params.to_mapping())

    async def _persist_shard(self, shard: int) -> None:
        """Persist desired shard state and settle its local mirror."""
        state = self._shards[shard]
        try:
            await self._write_shard(shard, state.desired)
        except BaseException as exc:
            state.settlement = self._settlement_for(exc)
            state.dirty = True
            if (
                state.settlement is _ShardSettlement.UNKNOWN
                and not isinstance(exc, asyncio.CancelledError)
            ):
                raise ResponseCheckpointUnknownError(
                    "response route checkpoint settlement is unknown",
                ) from exc
            raise
        state.durable = state.desired.copy()
        state.dirty = False
        state.settlement = _ShardSettlement.CONFIRMED

    @staticmethod
    def _settlement_for(exc: BaseException) -> _ShardSettlement:
        """Classify whether a failed mutation has a known remote result."""
        if isinstance(
            exc,
            (
                RpcTimeoutError,
                RpcClosedError,
                RpcCancelledError,
                ConnectionError,
                TimeoutError,
                asyncio.CancelledError,
            ),
        ):
            return _ShardSettlement.UNKNOWN
        if isinstance(
            exc,
            (
                FeishuResponseRouteError,
                ProtocolValidationError,
                RpcError,
                TypeError,
                ValueError,
            ),
        ):
            return _ShardSettlement.REJECTED
        return _ShardSettlement.UNKNOWN

    def _all_desired(self) -> dict[str, ResponseRouteSnapshot]:
        """Return the current desired projection across all shards."""
        return {
            handle: snapshot
            for state in self._shards.values()
            for handle, snapshot in state.desired.items()
        }

    def _decode_shard(
        self,
        shard: int,
        value: object,
    ) -> dict[str, ResponseRouteSnapshot]:
        if not isinstance(value, Mapping):
            raise FeishuResponseRouteError(
                "Feishu response route shard must be an object",
            )
        result: dict[str, ResponseRouteSnapshot] = {}
        for handle, item in value.items():
            if not isinstance(handle, str):
                raise FeishuResponseRouteError(
                    "Feishu response route handle is invalid",
                )
            try:
                snapshot = ResponseRouteSnapshot.from_mapping(item)
            except ValueError as exc:
                raise FeishuResponseRouteError(
                    "Feishu response route snapshot is invalid",
                ) from exc
            if snapshot.response_handle != handle:
                raise FeishuResponseRouteError(
                    "Feishu response route handle does not match",
                )
            if self.shard_for_handle(handle) != shard:
                raise FeishuResponseRouteError(
                    "Feishu response route is in the wrong shard",
                )
            result[handle] = snapshot
        return result

    def _ordered_snapshots(self) -> tuple[ResponseRouteSnapshot, ...]:
        desired = self._all_desired()
        return tuple(desired[handle] for handle in sorted(desired))


__all__ = [
    "FeishuResponseRouteCapacityError",
    "FeishuResponseRouteCheckpoint",
    "FeishuResponseRouteError",
    "RESPONSE_HANDLE_PREFIX",
    "RESPONSE_ROUTE_MAX_ENTRIES",
]
