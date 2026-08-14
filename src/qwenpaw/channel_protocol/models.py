# -*- coding: utf-8 -*-
"""Versioned JSON-RPC envelopes and Channel protocol DTOs."""

from __future__ import annotations

import ipaddress
import ntpath
import posixpath
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, Mapping

from .errors import ProtocolValidationError
from .identifiers import validate_channel_key, validate_digest


JSONRPC_VERSION = "2.0"
PROTOCOL_VERSION = 1
MAX_METHOD_LENGTH = 128
MAX_SECRET_HANDLE_LENGTH = 256


def _error(
    message: str,
    *,
    path: tuple[str | int, ...] = (),
    reason_code: str = "INVALID_PARAMS",
) -> ProtocolValidationError:
    """Create a stable DTO validation error."""
    return ProtocolValidationError(
        message,
        path=path,
        reason_code=reason_code,
    )


def _object(
    value: object,
    *,
    path: tuple[str | int, ...] = (),
) -> dict[str, Any]:
    """Require a JSON object and copy it to a mutable mapping."""
    if not isinstance(value, Mapping):
        raise _error("value must be an object", path=path)
    if any(not isinstance(key, str) for key in value):
        raise _error("object keys must be strings", path=path)
    return dict(value)


def _closed(
    value: Mapping[str, Any],
    allowed: set[str],
    *,
    path: tuple[str | int, ...] = (),
) -> None:
    """Reject fields not declared by a v1 closed DTO."""
    unknown = sorted(set(value) - allowed)
    if unknown:
        raise _error(
            f"unknown field: {unknown[0]}",
            path=path + (unknown[0],),
            reason_code="SCHEMA_MISMATCH",
        )


def _required(
    value: Mapping[str, Any],
    name: str,
    *,
    path: tuple[str | int, ...] = (),
) -> Any:
    """Read a required field."""
    if name not in value:
        raise _error(
            f"missing required field: {name}",
            path=path + (name,),
            reason_code="SCHEMA_MISMATCH",
        )
    return value[name]


def _string(
    value: object,
    name: str,
    *,
    path: tuple[str | int, ...] = (),
    non_empty: bool = True,
) -> str:
    """Require a JSON string."""
    if not isinstance(value, str) or (non_empty and not value):
        raise _error(
            f"{name} must be a non-empty string"
            if non_empty
            else f"{name} must be a string",
            path=path + (name,),
        )
    return value


def _integer(
    value: object,
    name: str,
    *,
    path: tuple[str | int, ...] = (),
    minimum: int | None = None,
) -> int:
    """Require a JSON integer, excluding booleans."""
    if not isinstance(value, int) or isinstance(value, bool):
        raise _error(f"{name} must be an integer", path=path + (name,))
    if minimum is not None and value < minimum:
        raise _error(
            f"{name} must be at least {minimum}",
            path=path + (name,),
        )
    return value


def _boolean(
    value: object,
    name: str,
    *,
    path: tuple[str | int, ...] = (),
) -> bool:
    """Require a JSON boolean."""
    if not isinstance(value, bool):
        raise _error(f"{name} must be a boolean", path=path + (name,))
    return value


def _number(
    value: object,
    name: str,
    *,
    path: tuple[str | int, ...] = (),
    minimum: int | float | None = None,
) -> int | float:
    """Require a finite JSON number."""
    import math

    if (
        not isinstance(value, (int, float))
        or isinstance(value, bool)
        or not math.isfinite(value)
    ):
        raise _error(f"{name} must be a finite number", path=path + (name,))
    if minimum is not None and value < minimum:
        raise _error(
            f"{name} must be at least {minimum}",
            path=path + (name,),
        )
    return value


def _list(
    value: object,
    name: str,
    *,
    path: tuple[str | int, ...] = (),
) -> list[Any]:
    """Require a JSON array."""
    if not isinstance(value, list):
        raise _error(f"{name} must be an array", path=path + (name,))
    return value


def _absolute_path(value: object, name: str) -> str:
    """Require an absolute POSIX or Windows path."""
    result = _string(value, name)
    if not (posixpath.isabs(result) or ntpath.isabs(result)):
        raise _error(
            f"{name} must be an absolute path",
            path=(name,),
        )
    return result


def _optional_string(
    value: Mapping[str, Any],
    name: str,
    *,
    path: tuple[str | int, ...] = (),
    non_empty: bool = True,
) -> str | None:
    """Read an optional nullable string."""
    if name not in value or value[name] is None:
        return None
    return _string(
        value[name],
        name,
        path=path,
        non_empty=non_empty,
    )


def _optional_integer(
    value: Mapping[str, Any],
    name: str,
    *,
    path: tuple[str | int, ...] = (),
    minimum: int | None = None,
) -> int | None:
    """Read an optional nullable integer."""
    if name not in value or value[name] is None:
        return None
    return _integer(value[name], name, path=path, minimum=minimum)


def _request_id(value: object) -> str | int:
    """Validate a JSON-RPC request identifier."""
    if isinstance(value, bool) or not isinstance(value, (str, int)):
        raise _error("request id must be a string or integer", path=("id",))
    if isinstance(value, str) and not value:
        raise _error("request id must not be empty", path=("id",))
    return value


@dataclass(frozen=True)
class RpcErrorObject:
    """JSON-RPC error object with a stable string reason in data."""

    code: int
    message: str
    data: object = None

    def to_mapping(self) -> dict[str, Any]:
        """Encode the error object."""
        result: dict[str, Any] = {
            "code": self.code,
            "message": self.message,
        }
        if self.data is not None:
            result["data"] = self.data
        return result

    @classmethod
    def from_mapping(cls, value: object) -> "RpcErrorObject":
        """Validate a JSON-RPC error object."""
        data = _object(value, path=("error",))
        _closed(data, {"code", "message", "data"}, path=("error",))
        code = _integer(data.get("code"), "code", path=("error",))
        message = _string(data.get("message"), "message", path=("error",))
        return cls(code=code, message=message, data=data.get("data"))


@dataclass(frozen=True)
class RpcRequest:
    """JSON-RPC request envelope."""

    id: str | int
    method: str
    params: Mapping[str, Any] | list[Any] | None = None

    def to_mapping(self) -> dict[str, Any]:
        """Encode the request envelope."""
        result: dict[str, Any] = {
            "jsonrpc": JSONRPC_VERSION,
            "id": self.id,
            "method": self.method,
        }
        if self.params is not None:
            result["params"] = self.params
        return result

    @classmethod
    def from_mapping(cls, value: object) -> "RpcRequest":
        """Validate a request envelope."""
        data = _object(value)
        _closed(data, {"jsonrpc", "id", "method", "params"})
        if data.get("jsonrpc") != JSONRPC_VERSION:
            raise _error(
                "jsonrpc must be '2.0'",
                path=("jsonrpc",),
                reason_code="PROTOCOL_MISMATCH",
            )
        if "id" not in data:
            raise _error(
                "request must contain id",
                path=("id",),
                reason_code="SCHEMA_MISMATCH",
            )
        request_id = _request_id(data["id"])
        method = _string(data.get("method"), "method")
        if len(method) > MAX_METHOD_LENGTH:
            raise _error("method is too long", path=("method",))
        params = data.get("params")
        if params is not None and not isinstance(params, (Mapping, list)):
            raise _error("params must be an object or array", path=("params",))
        return cls(id=request_id, method=method, params=params)


@dataclass(frozen=True)
class RpcNotification:
    """JSON-RPC notification envelope."""

    method: str
    params: Mapping[str, Any] | list[Any] | None = None

    def to_mapping(self) -> dict[str, Any]:
        """Encode the notification envelope."""
        result: dict[str, Any] = {
            "jsonrpc": JSONRPC_VERSION,
            "method": self.method,
        }
        if self.params is not None:
            result["params"] = self.params
        return result

    @classmethod
    def from_mapping(cls, value: object) -> "RpcNotification":
        """Validate a notification envelope."""
        data = _object(value)
        _closed(data, {"jsonrpc", "method", "params"})
        if data.get("jsonrpc") != JSONRPC_VERSION:
            raise _error(
                "jsonrpc must be '2.0'",
                path=("jsonrpc",),
                reason_code="PROTOCOL_MISMATCH",
            )
        method = _string(data.get("method"), "method")
        params = data.get("params")
        if params is not None and not isinstance(params, (Mapping, list)):
            raise _error("params must be an object or array", path=("params",))
        return cls(method=method, params=params)


@dataclass(frozen=True)
class RpcResponse:
    """JSON-RPC response envelope."""

    id: str | int
    result: object = None
    error: RpcErrorObject | None = None

    def __post_init__(self) -> None:
        """Require exactly one of result or error semantically."""
        if self.error is not None and self.result is not None:
            raise ValueError("response cannot contain both result and error")

    def to_mapping(self) -> dict[str, Any]:
        """Encode the response envelope."""
        result: dict[str, Any] = {
            "jsonrpc": JSONRPC_VERSION,
            "id": self.id,
        }
        if self.error is not None:
            result["error"] = self.error.to_mapping()
        else:
            result["result"] = self.result
        return result

    @classmethod
    def from_mapping(cls, value: object) -> "RpcResponse":
        """Validate a response envelope."""
        data = _object(value)
        _closed(data, {"jsonrpc", "id", "result", "error"})
        if data.get("jsonrpc") != JSONRPC_VERSION:
            raise _error(
                "jsonrpc must be '2.0'",
                path=("jsonrpc",),
                reason_code="PROTOCOL_MISMATCH",
            )
        if "id" not in data:
            raise _error("response must contain id", path=("id",))
        response_id = _request_id(data["id"])
        has_result = "result" in data
        has_error = "error" in data
        if has_result == has_error:
            raise _error(
                "response must contain exactly one result or error",
                reason_code="SCHEMA_MISMATCH",
            )
        return cls(
            id=response_id,
            result=data.get("result"),
            error=(
                RpcErrorObject.from_mapping(data["error"])
                if has_error
                else None
            ),
        )


RpcMessage = RpcRequest | RpcResponse | RpcNotification


def parse_rpc_message(value: object) -> RpcMessage:
    """Parse one JSON-RPC request, response, or notification mapping."""
    data = _object(value)
    if "method" in data and "id" in data:
        return RpcRequest.from_mapping(data)
    if "method" in data:
        return RpcNotification.from_mapping(data)
    return RpcResponse.from_mapping(data)


@dataclass(frozen=True)
class HelloParams:
    """Runner identity and protocol capability handshake."""

    protocol_min: int
    protocol_max: int
    qwenpaw_version: str
    channel_key: str
    instance_id: str
    environment_spec_id: str
    environment_id: str
    lock_sha256: str
    python_abi: str
    platform_tag: str
    capabilities: tuple[str, ...] = ()

    def to_mapping(self) -> dict[str, Any]:
        """Encode handshake parameters."""
        return {
            "protocol_min": self.protocol_min,
            "protocol_max": self.protocol_max,
            "qwenpaw_version": self.qwenpaw_version,
            "channel_key": self.channel_key,
            "instance_id": self.instance_id,
            "environment_spec_id": self.environment_spec_id,
            "environment_id": self.environment_id,
            "lock_sha256": self.lock_sha256,
            "python_abi": self.python_abi,
            "platform_tag": self.platform_tag,
            "capabilities": list(self.capabilities),
        }

    @classmethod
    def from_mapping(cls, value: object) -> "HelloParams":
        """Validate handshake parameters."""
        data = _object(value)
        allowed = {
            "protocol_min",
            "protocol_max",
            "qwenpaw_version",
            "channel_key",
            "instance_id",
            "environment_spec_id",
            "environment_id",
            "lock_sha256",
            "python_abi",
            "platform_tag",
            "capabilities",
        }
        _closed(data, allowed)
        protocol_min = _integer(
            data.get("protocol_min"),
            "protocol_min",
            minimum=1,
        )
        protocol_max = _integer(
            data.get("protocol_max"),
            "protocol_max",
            minimum=protocol_min,
        )
        qwenpaw_version = _string(
            data.get("qwenpaw_version"),
            "qwenpaw_version",
        )
        channel_key = validate_channel_key(
            _string(data.get("channel_key"), "channel_key"),
        )
        instance_id = _string(data.get("instance_id"), "instance_id")
        environment_spec_id = _string(
            data.get("environment_spec_id"),
            "environment_spec_id",
        )
        environment_id = _string(data.get("environment_id"), "environment_id")
        lock_sha256 = validate_digest(
            _string(data.get("lock_sha256"), "lock_sha256"),
            name="Lock digest",
        )
        python_abi = _string(data.get("python_abi"), "python_abi")
        platform_tag = _string(data.get("platform_tag"), "platform_tag")
        capabilities = _capabilities(data.get("capabilities"))
        return cls(
            protocol_min=protocol_min,
            protocol_max=protocol_max,
            qwenpaw_version=qwenpaw_version,
            channel_key=channel_key,
            instance_id=instance_id,
            environment_spec_id=environment_spec_id,
            environment_id=environment_id,
            lock_sha256=lock_sha256,
            python_abi=python_abi,
            platform_tag=platform_tag,
            capabilities=capabilities,
        )


def _capabilities(value: object) -> tuple[str, ...]:
    """Validate a sorted, unique capability list."""
    values = _list(value, "capabilities")
    if any(not isinstance(item, str) or not item for item in values):
        raise _error("capabilities must contain non-empty strings")
    if len(set(values)) != len(values) or list(values) != sorted(values):
        raise _error("capabilities must be sorted and unique")
    return tuple(values)


@dataclass(frozen=True)
class HostContext:
    """Core-owned host context passed during prepare."""

    media_work_dir: str | None = None
    config_snapshot: Mapping[str, Any] = field(default_factory=dict)
    secret_handle: str | None = field(default=None, repr=False)

    def to_mapping(self) -> dict[str, Any]:
        """Encode host context without secret values."""
        result: dict[str, Any] = {
            "config_snapshot": dict(self.config_snapshot),
        }
        if self.media_work_dir is not None:
            result["media_work_dir"] = self.media_work_dir
        if self.secret_handle is not None:
            result["secret_handle"] = self.secret_handle
        return result

    @classmethod
    def from_mapping(cls, value: object) -> "HostContext":
        """Validate host context and absolute media path."""
        data = _object(value, path=("host_context",))
        _closed(
            data,
            {"media_work_dir", "config_snapshot", "secret_handle"},
            path=("host_context",),
        )
        media_work_dir = None
        if "media_work_dir" in data and data["media_work_dir"] is not None:
            media_work_dir = _absolute_path(
                data["media_work_dir"],
                "media_work_dir",
            )
        config_snapshot = data.get("config_snapshot", {})
        if not isinstance(config_snapshot, Mapping):
            raise _error(
                "config_snapshot must be an object",
                path=("config_snapshot",),
            )
        secret_handle = _optional_string(data, "secret_handle")
        if secret_handle is not None:
            if len(secret_handle) > MAX_SECRET_HANDLE_LENGTH:
                raise _error(
                    "secret_handle exceeds the maximum length",
                    path=("secret_handle",),
                )
            if any(
                ord(character) < 32 or ord(character) == 127
                for character in secret_handle
            ):
                raise _error(
                    "secret_handle must not contain control characters",
                    path=("secret_handle",),
                )
        return cls(
            media_work_dir=media_work_dir,
            config_snapshot=dict(config_snapshot),
            secret_handle=secret_handle,
        )


@dataclass(frozen=True)
class IdentityParams:
    """Common instance and generation identity for control methods."""

    channel_key: str
    instance_id: str
    generation: int

    def to_mapping(self) -> dict[str, Any]:
        """Encode control identity."""
        return {
            "channel_key": self.channel_key,
            "instance_id": self.instance_id,
            "generation": self.generation,
        }

    @classmethod
    def from_mapping(cls, value: object) -> "IdentityParams":
        """Validate control identity."""
        data = _object(value)
        _closed(data, {"channel_key", "instance_id", "generation"})
        return cls(
            channel_key=validate_channel_key(
                _string(data.get("channel_key"), "channel_key"),
            ),
            instance_id=_string(data.get("instance_id"), "instance_id"),
            generation=_integer(
                data.get("generation"),
                "generation",
                minimum=1,
            ),
        )


@dataclass(frozen=True)
class PrepareParams(IdentityParams):
    """Prepare a candidate Runner for a generation."""

    host_context: HostContext = field(default_factory=HostContext)
    capabilities: tuple[str, ...] = ()

    @classmethod
    def from_mapping(cls, value: object) -> "PrepareParams":
        """Validate prepare parameters."""
        data = _object(value)
        _closed(
            data,
            {
                "channel_key",
                "instance_id",
                "generation",
                "host_context",
                "capabilities",
            },
        )
        identity = IdentityParams.from_mapping(
            {
                key: data[key]
                for key in ("channel_key", "instance_id", "generation")
            },
        )
        return cls(
            **identity.__dict__,
            host_context=HostContext.from_mapping(
                data.get("host_context", {}),
            ),
            capabilities=_capabilities(data.get("capabilities", [])),
        )

    def to_mapping(self) -> dict[str, Any]:
        """Encode prepare parameters."""
        return {
            **super().to_mapping(),
            "host_context": self.host_context.to_mapping(),
            "capabilities": list(self.capabilities),
        }


@dataclass(frozen=True)
class LeaseParams(IdentityParams):
    """Activate or renew a generation lease."""

    lease_token: str
    lease_ttl_ms: int

    @classmethod
    def from_mapping(cls, value: object) -> "LeaseParams":
        """Validate lease parameters."""
        data = _object(value)
        _closed(
            data,
            {
                "channel_key",
                "instance_id",
                "generation",
                "lease_token",
                "lease_ttl_ms",
            },
        )
        identity = IdentityParams.from_mapping(
            {
                key: data[key]
                for key in ("channel_key", "instance_id", "generation")
            },
        )
        return cls(
            **identity.__dict__,
            lease_token=_string(data.get("lease_token"), "lease_token"),
            lease_ttl_ms=_integer(
                data.get("lease_ttl_ms"),
                "lease_ttl_ms",
                minimum=1,
            ),
        )

    def to_mapping(self) -> dict[str, Any]:
        """Encode lease parameters."""
        return {
            **super().to_mapping(),
            "lease_token": self.lease_token,
            "lease_ttl_ms": self.lease_ttl_ms,
        }


@dataclass(frozen=True)
class EndpointParams(IdentityParams):
    """Runner-owned ingress endpoint registration DTO."""

    protocol: str
    host: str
    port: int
    path: str
    public_base_url: str | None
    readiness: str
    bound_externally: bool
    auth_required: bool
    quiescing: bool = False

    @classmethod
    def from_mapping(cls, value: object) -> "EndpointParams":
        """Validate an endpoint registration/update DTO."""
        data = _object(value)
        allowed = {
            "channel_key",
            "instance_id",
            "generation",
            "protocol",
            "host",
            "port",
            "path",
            "public_base_url",
            "readiness",
            "bound_externally",
            "auth_required",
            "quiescing",
        }
        _closed(data, allowed)
        identity = IdentityParams.from_mapping(
            {
                key: data[key]
                for key in ("channel_key", "instance_id", "generation")
            },
        )
        protocol = _string(data.get("protocol"), "protocol")
        host = _string(data.get("host"), "host")
        port = _integer(data.get("port"), "port", minimum=0)
        path = _string(data.get("path"), "path")
        if not path.startswith("/"):
            raise _error("path must start with '/'", path=("path",))
        readiness = _string(data.get("readiness"), "readiness")
        if readiness not in {"starting", "ready", "degraded", "stopped"}:
            raise _error("invalid endpoint readiness", path=("readiness",))
        _boolean(
            data.get("bound_externally"),
            "bound_externally",
        )
        auth_required = _boolean(data.get("auth_required"), "auth_required")
        bound_externally = is_external_host(host)
        if bound_externally and not auth_required:
            raise _error(
                "externally bound endpoint must require authentication",
                reason_code="AUTH_FAILED",
            )
        return cls(
            **identity.__dict__,
            protocol=protocol,
            host=host,
            port=port,
            path=path,
            public_base_url=_optional_string(data, "public_base_url"),
            readiness=readiness,
            bound_externally=bound_externally,
            auth_required=auth_required,
            quiescing=_boolean(data.get("quiescing", False), "quiescing"),
        )

    def to_mapping(self) -> dict[str, Any]:
        """Encode endpoint DTO."""
        return {
            **super().to_mapping(),
            "protocol": self.protocol,
            "host": self.host,
            "port": self.port,
            "path": self.path,
            "public_base_url": self.public_base_url,
            "readiness": self.readiness,
            "bound_externally": self.bound_externally,
            "auth_required": self.auth_required,
            "quiescing": self.quiescing,
        }


def is_external_host(host: str) -> bool:
    """Return whether an endpoint host is exposed beyond loopback."""
    normalized = host.strip().lower()
    if normalized == "localhost":
        return False
    try:
        return not ipaddress.ip_address(normalized.strip("[]")).is_loopback
    except ValueError:
        return True


def validate_content_part(value: object) -> dict[str, Any]:
    """Validate one platform-independent Content locator."""
    data = _object(value)
    content_type = _string(data.get("type"), "type")
    fields = {
        "text": {"type", "text"},
        "image": {"type", "image_url", "filename", "mime_type"},
        "video": {"type", "video_url", "filename", "mime_type"},
        "file": {"type", "file_url", "filename", "mime_type"},
        "audio": {"type", "data", "format", "filename", "mime_type"},
    }
    if content_type not in fields:
        raise _error("unsupported content type", path=("type",))
    _closed(data, fields[content_type])
    if content_type == "text":
        _string(data.get("text"), "text", non_empty=False)
    else:
        locator_name = (
            "data" if content_type == "audio" else f"{content_type}_url"
        )
        _string(data.get(locator_name), locator_name)
        for name in ("filename", "mime_type", "format"):
            if name in data and data[name] is not None:
                _string(data[name], name)
    return data


def _conversation(value: object) -> dict[str, Any]:
    """Validate the stable conversation identity carried by an event."""
    data = _object(value, path=("conversation",))
    _closed(
        data,
        {"id", "type", "thread_id"},
        path=("conversation",),
    )
    result = {
        "id": _string(data.get("id"), "id", path=("conversation",)),
        "type": _string(
            data.get("type"),
            "type",
            path=("conversation",),
        ),
        "thread_id": _optional_string(
            data,
            "thread_id",
            path=("conversation",),
        ),
    }
    return result


@dataclass(frozen=True)
class InboundEvent:
    """Stable platform-independent event submitted by a Runner."""

    event_id: str
    channel_key: str
    instance_id: str
    generation: int
    conversation: Mapping[str, Any]
    sender_id: str
    acl_sender_id: str
    sender_name: str
    content_parts: tuple[dict[str, Any], ...]
    event_kind: str
    metadata: Mapping[str, Any] = field(default_factory=dict)

    @classmethod
    def from_mapping(cls, value: object) -> "InboundEvent":
        """Validate one stable inbound event."""
        data = _object(value)
        _closed(
            data,
            {
                "event_id",
                "event_kind",
                "channel_key",
                "instance_id",
                "generation",
                "conversation",
                "sender_id",
                "acl_sender_id",
                "sender_name",
                "content_parts",
                "metadata",
            },
        )
        content_parts = _list(data.get("content_parts"), "content_parts")
        metadata = data.get("metadata", {})
        if not isinstance(metadata, Mapping):
            raise _error("metadata must be an object", path=("metadata",))
        event_kind = _string(data.get("event_kind"), "event_kind")
        return cls(
            event_id=_string(data.get("event_id"), "event_id"),
            event_kind=event_kind,
            channel_key=validate_channel_key(
                _string(data.get("channel_key"), "channel_key"),
            ),
            instance_id=_string(data.get("instance_id"), "instance_id"),
            generation=_integer(
                data.get("generation"),
                "generation",
                minimum=1,
            ),
            conversation=_conversation(data.get("conversation")),
            sender_id=_string(data.get("sender_id"), "sender_id"),
            acl_sender_id=_string(
                data.get("acl_sender_id"),
                "acl_sender_id",
            ),
            sender_name=_string(
                data.get("sender_name"),
                "sender_name",
                non_empty=False,
            ),
            content_parts=tuple(
                validate_content_part(item) for item in content_parts
            ),
            metadata=dict(metadata),
        )

    def to_mapping(self) -> dict[str, Any]:
        """Encode one stable inbound event."""
        result = {
            "event_id": self.event_id,
            "channel_key": self.channel_key,
            "instance_id": self.instance_id,
            "generation": self.generation,
            "conversation": dict(self.conversation),
            "sender_id": self.sender_id,
            "acl_sender_id": self.acl_sender_id,
            "sender_name": self.sender_name,
            "content_parts": list(self.content_parts),
            "metadata": dict(self.metadata),
        }
        result["event_kind"] = self.event_kind
        return result


@dataclass(frozen=True)
class EventBatchParams:
    """Reliable batch of inbound events submitted by a Runner."""

    batch_id: str
    events: tuple[InboundEvent, ...]
    invalid_events: tuple[RejectedEvent, ...] = ()
    identity: IdentityParams | None = None

    @classmethod
    def from_mapping(cls, value: object) -> "EventBatchParams":
        """Validate a non-empty event batch and its shared identity."""
        data = _object(value)
        _closed(data, {"batch_id", "events"})
        raw_events = _list(data.get("events"), "events")
        if not raw_events:
            raise _error("events must not be empty", path=("events",))
        events: list[InboundEvent] = []
        invalid_events: list[RejectedEvent] = []
        identity: IdentityParams | None = None
        seen_event_ids: set[str] = set()
        for item in raw_events:
            raw_item = item if isinstance(item, Mapping) else {}
            item_identity = IdentityParams.from_mapping(
                {
                    key: raw_item.get(key)
                    for key in (
                        "channel_key",
                        "instance_id",
                        "generation",
                    )
                },
            )
            if identity is None:
                identity = item_identity
            elif item_identity != identity:
                raise _error(
                    "all events in a batch must share identity",
                    reason_code="SCHEMA_MISMATCH",
                )
            try:
                event = InboundEvent.from_mapping(item)
                if event.event_id in seen_event_ids:
                    raise _error(
                        "event_id must be unique within a batch",
                        path=("event_id",),
                        reason_code="SCHEMA_MISMATCH",
                    )
                events.append(event)
                seen_event_ids.add(event.event_id)
            except ProtocolValidationError as exc:
                event_id = raw_item.get("event_id")
                if not isinstance(event_id, str) or not event_id:
                    raise
                if event_id in seen_event_ids:
                    raise _error(
                        "event_id must be unique within a batch",
                        path=("event_id",),
                        reason_code="SCHEMA_MISMATCH",
                    ) from exc
                invalid_events.append(
                    RejectedEvent(
                        event_id=event_id,
                        reason_code=exc.reason_code,
                        retryable=False,
                    ),
                )
                seen_event_ids.add(event_id)
        event_tuple = tuple(events)
        return cls(
            batch_id=_string(data.get("batch_id"), "batch_id"),
            events=event_tuple,
            invalid_events=tuple(invalid_events),
            identity=identity,
        )

    def to_mapping(self) -> dict[str, Any]:
        """Encode a reliable event batch."""
        return {
            "batch_id": self.batch_id,
            "events": [event.to_mapping() for event in self.events],
        }


@dataclass(frozen=True)
class RejectedEvent:
    """Per-event rejection returned in a reliable batch ACK."""

    event_id: str
    reason_code: str
    retryable: bool

    @classmethod
    def from_mapping(cls, value: object) -> "RejectedEvent":
        """Validate a rejected event result."""
        data = _object(value)
        _closed(data, {"event_id", "reason_code", "retryable"})
        return cls(
            event_id=_string(data.get("event_id"), "event_id"),
            reason_code=_string(data.get("reason_code"), "reason_code"),
            retryable=_boolean(data.get("retryable"), "retryable"),
        )

    def to_mapping(self) -> dict[str, Any]:
        """Encode a rejected event result."""
        return {
            "event_id": self.event_id,
            "reason_code": self.reason_code,
            "retryable": self.retryable,
        }


@dataclass(frozen=True)
class EventBatchAck:
    """Per-event ACK returned after Core persistence and deduplication."""

    accepted_event_ids: tuple[str, ...] = ()
    duplicate_event_ids: tuple[str, ...] = ()
    rejected_events: tuple[RejectedEvent, ...] = ()
    batch_id: str | None = None

    @classmethod
    def from_mapping(cls, value: object) -> "EventBatchAck":
        """Validate an event batch ACK."""
        data = _object(value)
        _closed(
            data,
            {
                "batch_id",
                "accepted_event_ids",
                "duplicate_event_ids",
                "rejected_events",
            },
        )
        accepted = _list(
            data.get("accepted_event_ids", []),
            "accepted_event_ids",
        )
        duplicates = _list(
            data.get("duplicate_event_ids", []),
            "duplicate_event_ids",
        )
        if any(not isinstance(item, str) or not item for item in accepted):
            raise _error(
                "accepted_event_ids must contain non-empty strings",
                path=("accepted_event_ids",),
            )
        if any(not isinstance(item, str) or not item for item in duplicates):
            raise _error(
                "duplicate_event_ids must contain non-empty strings",
                path=("duplicate_event_ids",),
            )
        if set(accepted).intersection(duplicates):
            raise _error(
                "accepted and duplicate event IDs must be disjoint",
                reason_code="SCHEMA_MISMATCH",
            )
        rejected = _list(data.get("rejected_events", []), "rejected_events")
        rejected_ids = [
            item.get("event_id")
            for item in rejected
            if isinstance(item, Mapping)
        ]
        if set(accepted).intersection(rejected_ids) or set(
            duplicates,
        ).intersection(
            rejected_ids,
        ):
            raise _error(
                "ACK event classifications must be disjoint",
                reason_code="SCHEMA_MISMATCH",
            )
        return cls(
            batch_id=_optional_string(data, "batch_id"),
            accepted_event_ids=tuple(accepted),
            duplicate_event_ids=tuple(duplicates),
            rejected_events=tuple(
                RejectedEvent.from_mapping(item) for item in rejected
            ),
        )

    def to_mapping(self) -> dict[str, Any]:
        """Encode an event batch ACK."""
        result = {
            "accepted_event_ids": list(self.accepted_event_ids),
            "duplicate_event_ids": list(self.duplicate_event_ids),
            "rejected_events": [
                item.to_mapping() for item in self.rejected_events
            ],
        }
        if self.batch_id is not None:
            result["batch_id"] = self.batch_id
        return result


class DeliveryState(StrEnum):
    """Stable outbound delivery ledger states."""

    REQUESTED = "requested"
    SENDING = "sending"
    ACKNOWLEDGED = "acknowledged"
    FAILED = "failed"
    TIMEOUT = "timeout"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class DeliveryUpdateParams(IdentityParams):
    """Runner result update for an immutable outbound delivery."""

    delivery_id: str
    state: DeliveryState
    reason_code: str | None = None
    retryable: bool = False

    @classmethod
    def from_mapping(cls, value: object) -> "DeliveryUpdateParams":
        """Validate a delivery ledger update."""
        data = _object(value)
        _closed(
            data,
            {
                "channel_key",
                "instance_id",
                "generation",
                "delivery_id",
                "state",
                "reason_code",
                "retryable",
            },
        )
        identity = IdentityParams.from_mapping(
            {
                key: data[key]
                for key in ("channel_key", "instance_id", "generation")
            },
        )
        state_value = _string(data.get("state"), "state")
        try:
            state = DeliveryState(state_value)
        except ValueError as exc:
            raise _error(
                "unsupported delivery state",
                path=("state",),
            ) from exc
        return cls(
            **identity.__dict__,
            delivery_id=_string(data.get("delivery_id"), "delivery_id"),
            state=state,
            reason_code=_optional_string(data, "reason_code"),
            retryable=_boolean(data.get("retryable", False), "retryable"),
        )

    def to_mapping(self) -> dict[str, Any]:
        """Encode a delivery ledger update."""
        result = {
            **super().to_mapping(),
            "delivery_id": self.delivery_id,
            "state": self.state.value,
            "retryable": self.retryable,
        }
        if self.reason_code is not None:
            result["reason_code"] = self.reason_code
        return result


class OutboundOperation(StrEnum):
    """Platform-independent outbound operations carried by channel.send."""

    MESSAGE_CREATE = "message.create"
    MESSAGE_UPDATE = "message.update"
    STREAM_START = "stream.start"
    STREAM_DELTA = "stream.delta"
    STREAM_END = "stream.end"


class StreamType(StrEnum):
    """Stable stream categories produced by Core event aggregation."""

    REASONING = "reasoning"
    MESSAGE = "message"


class ApprovalSeverity(StrEnum):
    """Stable tool-approval severity values."""

    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


@dataclass(frozen=True)
class ApprovalCardParams:
    """Platform-independent tool approval card semantics."""

    request_id: str
    tool_name: str
    severity: ApprovalSeverity

    @classmethod
    def from_mapping(cls, value: object) -> "ApprovalCardParams":
        """Validate a closed approval card object."""
        data = _object(value, path=("approval",))
        _closed(
            data,
            {"request_id", "tool_name", "severity"},
            path=("approval",),
        )
        severity_value = _string(
            data.get("severity"),
            "severity",
            path=("approval",),
        )
        try:
            severity = ApprovalSeverity(severity_value)
        except ValueError as exc:
            raise _error(
                "unsupported approval severity",
                path=("approval", "severity"),
            ) from exc
        return cls(
            request_id=_string(
                data.get("request_id"),
                "request_id",
                path=("approval",),
            ),
            tool_name=_string(
                data.get("tool_name"),
                "tool_name",
                path=("approval",),
            ),
            severity=severity,
        )

    def to_mapping(self) -> dict[str, Any]:
        """Encode platform-independent approval semantics."""
        return {
            "request_id": self.request_id,
            "tool_name": self.tool_name,
            "severity": self.severity.value,
        }


def _reject_operation_fields(
    data: Mapping[str, Any],
    names: tuple[str, ...],
    operation: OutboundOperation,
) -> None:
    """Reject fields that are not valid for one outbound operation."""
    for name in names:
        if name in data:
            raise _error(
                f"{name} is not valid for {operation.value}",
                path=(name,),
                reason_code="SCHEMA_MISMATCH",
            )


def _require_send_parts(parts: tuple[dict[str, Any], ...]) -> None:
    """Require one outbound operation to carry content parts."""
    if not parts:
        raise _error(
            "content_parts must not be empty",
            path=("content_parts",),
        )


def _require_send_target(target_delivery_id: str | None) -> None:
    """Require one outbound operation to reference an existing target."""
    if target_delivery_id is None:
        raise _error(
            "target_delivery_id is required",
            path=("target_delivery_id",),
        )


def _require_send_sequence(sequence: int | None) -> None:
    """Require a positive sequence for one outbound update."""
    if sequence is None or sequence < 1:
        raise _error(
            "sequence must be at least 1",
            path=("sequence",),
        )


def _require_send_stream(
    stream_type: StreamType | None,
    accumulated_text: str | None,
) -> None:
    """Require stable stream identity and accumulated text."""
    if stream_type is None:
        raise _error(
            "stream_type is required",
            path=("stream_type",),
        )
    if accumulated_text is None:
        raise _error(
            "accumulated_text is required",
            path=("accumulated_text",),
        )


def _parse_outbound_operation(data: Mapping[str, Any]) -> OutboundOperation:
    """Parse the operation with message.create as the v1 default."""
    value = data.get("operation", OutboundOperation.MESSAGE_CREATE.value)
    try:
        return OutboundOperation(_string(value, "operation"))
    except ValueError as exc:
        raise _error(
            "unsupported outbound operation",
            path=("operation",),
        ) from exc


def _parse_stream_type(data: Mapping[str, Any]) -> StreamType | None:
    """Parse an optional stable stream type."""
    if "stream_type" not in data or data["stream_type"] is None:
        return None
    value = _string(data["stream_type"], "stream_type")
    try:
        return StreamType(value)
    except ValueError as exc:
        raise _error(
            "unsupported stream type",
            path=("stream_type",),
        ) from exc


def _validate_send_fields(
    data: Mapping[str, Any],
    operation: OutboundOperation,
    parts: tuple[dict[str, Any], ...],
    target_delivery_id: str | None,
    stream_type: StreamType | None,
    sequence: int | None,
    accumulated_text: str | None,
) -> None:
    """Validate the field combination for one outbound operation."""
    if operation is OutboundOperation.MESSAGE_CREATE:
        _require_send_parts(parts)
        _reject_operation_fields(
            data,
            (
                "target_delivery_id",
                "stream_type",
                "sequence",
                "accumulated_text",
            ),
            operation,
        )
        return
    if operation is OutboundOperation.MESSAGE_UPDATE:
        _require_send_sequence(sequence)
        _require_send_parts(parts)
        _require_send_target(target_delivery_id)
        _reject_operation_fields(
            data,
            ("stream_type", "accumulated_text", "approval"),
            operation,
        )
        return
    _require_send_stream(stream_type, accumulated_text)
    if operation is OutboundOperation.STREAM_START:
        if sequence != 0:
            raise _error(
                "stream.start sequence must be 0",
                path=("sequence",),
            )
        _reject_operation_fields(
            data,
            ("content_parts", "target_delivery_id", "approval"),
            operation,
        )
        return
    _require_send_sequence(sequence)
    _require_send_target(target_delivery_id)
    _reject_operation_fields(
        data,
        ("content_parts", "approval"),
        operation,
    )


@dataclass(frozen=True)
class SendParams(IdentityParams):
    """Platform-independent outbound operation DTO."""

    delivery_id: str
    to_handle: str
    operation: OutboundOperation
    content_parts: tuple[dict[str, Any], ...] = ()
    target_delivery_id: str | None = None
    stream_type: StreamType | None = None
    sequence: int | None = None
    accumulated_text: str | None = None
    approval: ApprovalCardParams | None = None

    @classmethod
    def from_mapping(cls, value: object) -> "SendParams":
        """Validate outbound content and delivery identity."""
        data = _object(value)
        _closed(
            data,
            {
                "channel_key",
                "instance_id",
                "generation",
                "delivery_id",
                "to_handle",
                "operation",
                "content_parts",
                "target_delivery_id",
                "stream_type",
                "sequence",
                "accumulated_text",
                "approval",
            },
        )
        identity = IdentityParams.from_mapping(
            {
                key: data[key]
                for key in ("channel_key", "instance_id", "generation")
            },
        )
        operation = _parse_outbound_operation(data)
        raw_parts = _list(data.get("content_parts", []), "content_parts")
        parts = tuple(validate_content_part(item) for item in raw_parts)
        target_delivery_id = _optional_string(data, "target_delivery_id")
        sequence = _optional_integer(data, "sequence", minimum=0)
        accumulated_text = _optional_string(
            data,
            "accumulated_text",
            non_empty=False,
        )
        stream_type = _parse_stream_type(data)
        approval = None
        if "approval" in data and data["approval"] is not None:
            approval = ApprovalCardParams.from_mapping(data["approval"])
        _validate_send_fields(
            data,
            operation,
            parts,
            target_delivery_id,
            stream_type,
            sequence,
            accumulated_text,
        )
        return cls(
            **identity.__dict__,
            delivery_id=_string(data.get("delivery_id"), "delivery_id"),
            to_handle=_string(data.get("to_handle"), "to_handle"),
            operation=operation,
            content_parts=parts,
            target_delivery_id=target_delivery_id,
            stream_type=stream_type,
            sequence=sequence,
            accumulated_text=accumulated_text,
            approval=approval,
        )

    def to_mapping(self) -> dict[str, Any]:
        """Encode outbound message DTO."""
        result: dict[str, Any] = {
            **super().to_mapping(),
            "delivery_id": self.delivery_id,
            "to_handle": self.to_handle,
            "operation": self.operation.value,
        }
        if self.content_parts:
            result["content_parts"] = [
                dict(part) for part in self.content_parts
            ]
        if self.target_delivery_id is not None:
            result["target_delivery_id"] = self.target_delivery_id
        if self.stream_type is not None:
            result["stream_type"] = self.stream_type.value
        if self.sequence is not None:
            result["sequence"] = self.sequence
        if self.accumulated_text is not None:
            result["accumulated_text"] = self.accumulated_text
        if self.approval is not None:
            result["approval"] = self.approval.to_mapping()
        return result


@dataclass(frozen=True)
class ReactionParams(IdentityParams):
    """Platform-independent reaction operation DTO."""

    delivery_id: str
    to_handle: str
    target_delivery_id: str
    reaction: str

    @classmethod
    def from_mapping(cls, value: object) -> "ReactionParams":
        """Validate the v1 completed reaction operation."""
        data = _object(value)
        _closed(
            data,
            {
                "channel_key",
                "instance_id",
                "generation",
                "delivery_id",
                "to_handle",
                "target_delivery_id",
                "reaction",
            },
        )
        identity = IdentityParams.from_mapping(
            {
                key: data[key]
                for key in ("channel_key", "instance_id", "generation")
            },
        )
        reaction = _string(data.get("reaction"), "reaction")
        if reaction != "completed":
            raise _error(
                "unsupported reaction",
                path=("reaction",),
            )
        return cls(
            **identity.__dict__,
            delivery_id=_string(data.get("delivery_id"), "delivery_id"),
            to_handle=_string(data.get("to_handle"), "to_handle"),
            target_delivery_id=_string(
                data.get("target_delivery_id"),
                "target_delivery_id",
            ),
            reaction=reaction,
        )

    def to_mapping(self) -> dict[str, Any]:
        """Encode a platform-independent reaction operation."""
        return {
            **super().to_mapping(),
            "delivery_id": self.delivery_id,
            "to_handle": self.to_handle,
            "target_delivery_id": self.target_delivery_id,
            "reaction": self.reaction,
        }


class VoiceEventKind(StrEnum):
    """Stable Voice event kinds crossing the process boundary."""

    CALL_STARTED = "call.started"
    MESSAGE_QUERY = "message.query"
    CALL_INTERRUPTED = "call.interrupted"
    DTMF = "dtmf"
    CALL_CLOSED = "call.closed"


@dataclass(frozen=True)
class VoiceEvent:
    """Stable Voice event DTO extracted from ConversationRelay messages."""

    event_id: str
    event_kind: VoiceEventKind
    channel_key: str
    instance_id: str
    generation: int
    connection_id: str
    sequence: int
    session_binding: str
    platform_session_id: str
    payload: Mapping[str, Any] = field(default_factory=dict)

    @classmethod
    def from_mapping(cls, value: object) -> "VoiceEvent":
        """Validate a Voice event DTO."""
        data = _object(value)
        _closed(
            data,
            {
                "event_id",
                "event_kind",
                "channel_key",
                "instance_id",
                "generation",
                "connection_id",
                "sequence",
                "session_binding",
                "platform_session_id",
                "payload",
            },
        )
        payload = data.get("payload", {})
        if not isinstance(payload, Mapping):
            raise _error("payload must be an object", path=("payload",))
        event_kind_value = _string(data.get("event_kind"), "event_kind")
        try:
            event_kind = VoiceEventKind(event_kind_value)
        except ValueError as exc:
            raise _error(
                "unsupported Voice event kind",
                path=("event_kind",),
            ) from exc
        return cls(
            event_id=_string(data.get("event_id"), "event_id"),
            event_kind=event_kind,
            channel_key=validate_channel_key(
                _string(data.get("channel_key"), "channel_key"),
            ),
            instance_id=_string(data.get("instance_id"), "instance_id"),
            generation=_integer(
                data.get("generation"),
                "generation",
                minimum=1,
            ),
            connection_id=_string(data.get("connection_id"), "connection_id"),
            sequence=_integer(data.get("sequence"), "sequence", minimum=1),
            session_binding=_string(
                data.get("session_binding"),
                "session_binding",
            ),
            platform_session_id=_string(
                data.get("platform_session_id"),
                "platform_session_id",
            ),
            payload=dict(payload),
        )

    def to_mapping(self) -> dict[str, Any]:
        """Encode Voice event DTO."""
        return {
            "event_id": self.event_id,
            "event_kind": self.event_kind.value,
            "channel_key": self.channel_key,
            "instance_id": self.instance_id,
            "generation": self.generation,
            "connection_id": self.connection_id,
            "sequence": self.sequence,
            "session_binding": self.session_binding,
            "platform_session_id": self.platform_session_id,
            "payload": dict(self.payload),
        }


@dataclass(frozen=True)
class CancelParams:
    """Notification parameters for request cancellation."""

    request_id: str | int
    reason: str | None = None

    @classmethod
    def from_mapping(cls, value: object) -> "CancelParams":
        """Validate cancellation parameters."""
        data = _object(value)
        _closed(data, {"request_id", "reason"})
        if "request_id" not in data:
            raise _error(
                "missing required field: request_id",
                path=("request_id",),
            )
        return cls(
            request_id=_request_id(data["request_id"]),
            reason=_optional_string(data, "reason"),
        )

    def to_mapping(self) -> dict[str, Any]:
        """Encode cancellation parameters."""
        result: dict[str, Any] = {"request_id": self.request_id}
        if self.reason is not None:
            result["reason"] = self.reason
        return result


@dataclass(frozen=True)
class QuiesceParams(IdentityParams):
    """Stop new work and drain a bounded amount of existing work."""

    drain_timeout_ms: int

    @classmethod
    def from_mapping(cls, value: object) -> "QuiesceParams":
        """Validate quiesce parameters."""
        data = _object(value)
        _closed(
            data,
            {"channel_key", "instance_id", "generation", "drain_timeout_ms"},
        )
        identity = IdentityParams.from_mapping(
            {
                key: data[key]
                for key in ("channel_key", "instance_id", "generation")
            },
        )
        return cls(
            **identity.__dict__,
            drain_timeout_ms=_integer(
                data.get("drain_timeout_ms"),
                "drain_timeout_ms",
                minimum=0,
            ),
        )

    def to_mapping(self) -> dict[str, Any]:
        """Encode quiesce parameters."""
        return {
            **super().to_mapping(),
            "drain_timeout_ms": self.drain_timeout_ms,
        }


@dataclass(frozen=True)
class HostStateParams(IdentityParams):
    """Identify a versioned, instance-scoped host state key."""

    key: str
    schema_version: int = 1
    value: object = None

    @classmethod
    def from_mapping(cls, value: object) -> "HostStateParams":
        """Validate host state request parameters."""
        data = _object(value)
        _closed(
            data,
            {
                "channel_key",
                "instance_id",
                "generation",
                "key",
                "schema_version",
                "value",
            },
        )
        identity = IdentityParams.from_mapping(
            {
                key: data[key]
                for key in ("channel_key", "instance_id", "generation")
            },
        )
        key = _string(data.get("key"), "key")
        if "/" in key or "\\" in key or key.startswith("."):
            raise _error(
                "host state key is not a safe relative key",
                path=("key",),
            )
        schema_version = _integer(
            data.get("schema_version", 1),
            "schema_version",
            minimum=1,
        )
        return cls(
            **identity.__dict__,
            key=key,
            schema_version=schema_version,
            value=data.get("value"),
        )

    def to_mapping(self) -> dict[str, Any]:
        """Encode host state request parameters."""
        result = {
            **super().to_mapping(),
            "key": self.key,
            "schema_version": self.schema_version,
        }
        if self.value is not None:
            result["value"] = self.value
        return result


@dataclass(frozen=True)
class VoiceSetup:
    """ConversationRelay setup message fields needed by the protocol."""

    platform_session_id: str
    from_number: str
    to_number: str

    @classmethod
    def from_mapping(cls, value: object) -> "VoiceSetup":
        """Validate a ConversationRelay setup message."""
        data = _object(value)
        if data.get("type") != "setup":
            raise _error(
                "setup message must have type 'setup'",
                path=("type",),
            )
        return cls(
            platform_session_id=_string(data.get("callSid"), "callSid"),
            from_number=_string(data.get("from", ""), "from", non_empty=False),
            to_number=_string(data.get("to", ""), "to", non_empty=False),
        )


_VOICE_TERMINAL_STATUSES = frozenset(
    {"completed", "busy", "no-answer", "canceled", "failed"},
)


@dataclass(frozen=True)
class VoiceStatusCallback:
    """Idempotent status callback fields for closing a Voice session."""

    platform_session_id: str
    status: str

    @property
    def is_terminal(self) -> bool:
        """Return whether the callback closes the platform session."""
        return self.status in _VOICE_TERMINAL_STATUSES

    @classmethod
    def from_mapping(cls, value: object) -> "VoiceStatusCallback":
        """Validate a Twilio status callback form mapping."""
        data = _object(value)
        return cls(
            platform_session_id=_string(data.get("CallSid"), "CallSid"),
            status=_string(data.get("CallStatus"), "CallStatus"),
        )


def voice_event_from_setup(
    setup: VoiceSetup,
    *,
    event_id: str,
    channel_key: str,
    instance_id: str,
    generation: int,
    connection_id: str,
    session_binding: str,
    sequence: int = 1,
) -> VoiceEvent:
    """Build the first stable Voice event from a setup message."""
    return VoiceEvent(
        event_id=event_id,
        event_kind=VoiceEventKind.CALL_STARTED,
        channel_key=validate_channel_key(channel_key),
        instance_id=instance_id,
        generation=generation,
        connection_id=connection_id,
        sequence=sequence,
        session_binding=session_binding,
        platform_session_id=setup.platform_session_id,
        payload={
            "from": setup.from_number,
            "to": setup.to_number,
        },
    )


def voice_event_from_status_callback(
    callback: VoiceStatusCallback,
    *,
    event_id: str,
    channel_key: str,
    instance_id: str,
    generation: int,
    connection_id: str,
    session_binding: str,
    sequence: int,
) -> VoiceEvent:
    """Build an idempotent close event from a terminal callback."""
    if not callback.is_terminal:
        raise _error(
            "status callback is not terminal",
            reason_code="TEMPORARY_UNAVAILABLE",
        )
    return VoiceEvent(
        event_id=event_id,
        event_kind=VoiceEventKind.CALL_CLOSED,
        channel_key=validate_channel_key(channel_key),
        instance_id=instance_id,
        generation=generation,
        connection_id=connection_id,
        sequence=sequence,
        session_binding=session_binding,
        platform_session_id=callback.platform_session_id,
        payload={"status": callback.status},
    )


@dataclass(frozen=True)
class GenerationStatus:
    """Read-only lifecycle status returned by a Runner."""

    state: str
    generation: int
    lease_expires_at_ms: int | None = None
    consuming: bool = False

    def to_mapping(self) -> dict[str, Any]:
        """Encode status data."""
        return {
            "state": self.state,
            "generation": self.generation,
            "lease_expires_at_ms": self.lease_expires_at_ms,
            "consuming": self.consuming,
        }


METHOD_SCHEMAS: dict[str, type[Any]] = {
    "channel.prepare": PrepareParams,
    "runner.hello": HelloParams,
    "channel.activate": LeaseParams,
    "channel.commit": LeaseParams,
    "channel.lease_renew": LeaseParams,
    "channel.quiesce": QuiesceParams,
    "channel.health": IdentityParams,
    "channel.generation_status": IdentityParams,
    "channel.stop": IdentityParams,
    "channel.send": SendParams,
    "channel.reaction": ReactionParams,
    "event.batch": EventBatchParams,
    "delivery.update": DeliveryUpdateParams,
    "ingress.endpoint.register": EndpointParams,
    "ingress.endpoint.update": EndpointParams,
    "ingress.endpoint.unregister": IdentityParams,
    "request.cancel": CancelParams,
    "host.state.get": HostStateParams,
    "host.state.put": HostStateParams,
    "host.state.delete": HostStateParams,
}


def validate_method_params(method: str, params: object) -> Any:
    """Validate params for a registered method, if one has a DTO."""
    dto_type = METHOD_SCHEMAS.get(method)
    if dto_type is None:
        if params is None:
            return None
        return _object(params)
    return dto_type.from_mapping(params)
