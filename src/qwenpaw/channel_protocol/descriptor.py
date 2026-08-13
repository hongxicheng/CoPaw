# -*- coding: utf-8 -*-
"""Closed Channel descriptor v1 model and cross-field validation."""

from __future__ import annotations

from collections.abc import Collection, Mapping
from dataclasses import dataclass
from decimal import Decimal
import re
from typing import Any, cast, Literal, TypeAlias
from urllib.parse import urlsplit

from .canonical import (
    canonical_json,
    domain_sha256,
    normalize_string,
    parse_json_value,
)
from .errors import DescriptorValidationError, PathPart, validation_error
from .identifiers import (
    condition_set_sha256,
    validate_channel_key,
    validate_digest,
    validate_platform_tag,
    validate_python_abi,
)
from .requirements import canonicalize_requirements


LocalizedText: TypeAlias = str | dict[str, str]
SourceKind: TypeAlias = Literal["builtin", "plugin"]
ProcessMode: TypeAlias = Literal["in_process", "runner_process"]
DispatchMode: TypeAlias = Literal["manager_queue", "direct_session"]
IngressOwner: TypeAlias = Literal["none", "runner_owned", "core_owned"]

_TOP_LEVEL_FIELDS = frozenset(
    {
        "schema_version",
        "channel_key",
        "source_kind",
        "process_mode",
        "dispatch_mode",
        "ingress_owner",
        "label",
        "description",
        "icon",
        "doc_url",
        "plugin_metadata",
        "entrypoint",
        "config_fields",
        "core_requirements",
        "isolated_requirements",
        "condition_fields",
        "supported_python_abis",
        "supported_platform_tags",
        "capabilities",
        "bot_identity_fields",
        "environment_passthrough_allowlist",
    },
)
_PLUGIN_METADATA_FIELDS = frozenset(
    {"plugin_id", "version", "artifact_sha256"},
)
_ENTRYPOINT_FIELDS = frozenset({"scope", "module", "qualname"})
_CONFIG_FIELD_FIELDS = frozenset(
    {
        "name",
        "label",
        "help",
        "placeholder",
        "type",
        "required",
        "nullable",
        "default",
        "allowed_values",
        "secret",
        "condition",
    },
)
_IDENTITY_FIELD_FIELDS = frozenset({"name", "normalization"})
_FIELD_NAME_PATTERN = re.compile(r"^[a-z][a-z0-9_]*$")
_DOTTED_NAME_PATTERN = re.compile(
    r"^[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*$",
)
_LOCALE_PATTERN = re.compile(r"^[a-z]{2,8}(?:-[A-Za-z0-9]{1,8})*$")
_CAPABILITY_PATTERN = re.compile(r"^[a-z][a-z0-9_.-]*$")
_ENVIRONMENT_NAME_PATTERN = re.compile(r"^[A-Z][A-Z0-9_]*$")

_SOURCE_KINDS = frozenset({"builtin", "plugin"})
_PROCESS_MODES = frozenset({"in_process", "runner_process"})
_DISPATCH_MODES = frozenset({"manager_queue", "direct_session"})
_INGRESS_OWNERS = frozenset({"none", "runner_owned", "core_owned"})
_ENTRYPOINT_SCOPES = frozenset({"core", "runner"})
_CONFIG_FIELD_TYPES = frozenset(
    {"text", "password", "number", "switch", "select"},
)
_IDENTITY_NORMALIZATIONS = frozenset(
    {"strip", "strip_trailing_slash"},
)
_CAPABILITIES = frozenset(
    {
        "streaming",
        "typing",
        "reaction",
        "media",
        "approval_card",
        "server_side_idempotency",
        "exactly-once-visible",
        "ingress_endpoint",
        "checkpoint",
        "host_state",
    },
)

DESCRIPTOR_DOMAIN = "qwenpaw.channel.descriptor.v1"


def _error(message: str, *path: PathPart) -> DescriptorValidationError:
    return validation_error(message, path=path)


def _require_mapping(value: Any, *path: PathPart) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise _error("Expected an object", *path)
    return value


def _require_list(value: Any, *path: PathPart) -> list[Any]:
    if not isinstance(value, list):
        raise _error("Expected an array", *path)
    return value


def _closed_object(
    value: Any,
    expected: frozenset[str],
    *path: PathPart,
) -> Mapping[str, Any]:
    mapping = _require_mapping(value, *path)
    if any(not isinstance(key, str) for key in mapping):
        raise _error("Object field names must be strings", *path)
    keys = set(mapping)
    if keys != expected:
        missing = sorted(expected - keys)
        unknown = sorted(keys - expected)
        details = f"missing={missing}, unknown={unknown}"
        raise _error(f"Object fields do not match v1 shape: {details}", *path)
    return mapping


def _string(value: Any, *path: PathPart, nonempty: bool = False) -> str:
    if not isinstance(value, str):
        raise _error("Expected a string", *path)
    normalized = normalize_string(value, path=tuple(path))
    if nonempty and not normalized:
        raise _error("String must not be empty", *path)
    return normalized


def _boolean(value: Any, *path: PathPart) -> bool:
    if not isinstance(value, bool):
        raise _error("Expected a boolean", *path)
    return value


def _enum(
    value: Any,
    allowed: Collection[str],
    *path: PathPart,
) -> str:
    normalized = _string(value, *path)
    if normalized not in allowed:
        raise _error("Value is not in the closed enum", *path)
    return normalized


def _http_url(value: str, *path: PathPart) -> str:
    if not value:
        return value
    try:
        parts = urlsplit(value)
        port = parts.port
    except ValueError as exc:
        raise _error("HTTP(S) URL is invalid", *path) from exc
    if parts.scheme not in {"http", "https"} or not parts.hostname:
        raise _error("Expected an absolute HTTP(S) URL", *path)
    if port is not None and not 0 < port < 65536:
        raise _error("HTTP(S) URL port is invalid", *path)
    return value


def _localized_text(
    value: Any,
    *path: PathPart,
    url: bool = False,
) -> LocalizedText:
    if isinstance(value, str):
        normalized = _string(value, *path)
        return _http_url(normalized, *path) if url else normalized
    mapping = _require_mapping(value, *path)
    if not mapping:
        raise _error("LocalizedText map must not be empty", *path)
    output: dict[str, str] = {}
    for locale, text in mapping.items():
        normalized_locale = _string(locale, *path, "locale", nonempty=True)
        if not _LOCALE_PATTERN.fullmatch(normalized_locale):
            raise _error("Locale key is not canonical BCP-47", *path, locale)
        if normalized_locale in output:
            raise _error("LocalizedText contains a duplicate locale", *path)
        normalized_text = _string(text, *path, normalized_locale)
        output[normalized_locale] = (
            _http_url(normalized_text, *path, normalized_locale)
            if url
            else normalized_text
        )
    return output


def resolve_localized_text(value: LocalizedText, locale: str) -> str:
    """Resolve LocalizedText with the descriptor v1 fallback order."""
    if isinstance(value, str):
        return value
    exact = value.get(locale)
    if exact is not None:
        return exact
    primary = locale.split("-", 1)[0].lower()
    primary_value = value.get(primary)
    if primary_value is not None:
        return primary_value
    english = value.get("en")
    if english is not None:
        return english
    return value[sorted(value)[0]]


def _canonical_scalar(value: Any, *path: PathPart) -> Any:
    if value is None or isinstance(value, (str, bool, int, Decimal)):
        canonical_json(value)
        return normalize_string(value) if isinstance(value, str) else value
    raise _error("Expected a canonical JSON scalar", *path)


def _compatible_value(field_type: str, value: Any) -> bool:
    if value is None:
        return True
    if field_type in {"text", "password"}:
        return isinstance(value, str)
    if field_type == "number":
        return isinstance(value, (int, Decimal)) and not isinstance(
            value,
            bool,
        )
    if field_type == "switch":
        return isinstance(value, bool)
    return isinstance(value, (str, bool, int, Decimal)) and not isinstance(
        value,
        float,
    )


def _unique_scalars(
    values: list[Any],
    *,
    field_type: str,
    path: tuple[PathPart, ...],
) -> tuple[Any, ...]:
    canonical: dict[bytes, Any] = {}
    for index, raw in enumerate(values):
        value = _canonical_scalar(raw, *path, index)
        if not _compatible_value(field_type, value):
            raise _error("Value is incompatible with field type", *path, index)
        encoded = canonical_json(value)
        if encoded in canonical:
            raise _error("allowed_values contains a duplicate", *path, index)
        canonical[encoded] = value
    return tuple(canonical.values())


@dataclass(frozen=True)
class PluginMetadata:
    """Stable owner metadata for a plugin Channel descriptor."""

    plugin_id: str
    version: str
    artifact_sha256: str

    def to_mapping(self) -> dict[str, str]:
        """Return the closed canonical JSON object."""
        return {
            "plugin_id": self.plugin_id,
            "version": self.version,
            "artifact_sha256": self.artifact_sha256,
        }


@dataclass(frozen=True)
class Entrypoint:
    """Static import path interpreted only after descriptor validation."""

    scope: Literal["core", "runner"]
    module: str
    qualname: str

    def to_mapping(self) -> dict[str, str]:
        """Return the closed canonical JSON object."""
        return {
            "scope": self.scope,
            "module": self.module,
            "qualname": self.qualname,
        }


@dataclass(frozen=True)
class ConfigField:
    """One scalar UI/configuration projection field."""

    name: str
    label: LocalizedText
    help: LocalizedText
    placeholder: LocalizedText
    type: Literal["text", "password", "number", "switch", "select"]
    required: bool
    nullable: bool
    default: Any
    allowed_values: tuple[Any, ...]
    secret: bool
    condition: bool

    def to_mapping(self) -> dict[str, Any]:
        """Return the closed canonical JSON object."""
        return {
            "name": self.name,
            "label": self.label,
            "help": self.help,
            "placeholder": self.placeholder,
            "type": self.type,
            "required": self.required,
            "nullable": self.nullable,
            "default": self.default,
            "allowed_values": list(self.allowed_values),
            "secret": self.secret,
            "condition": self.condition,
        }


@dataclass(frozen=True)
class BotIdentityField:
    """One descriptor-declared bot identity field reference."""

    name: str
    normalization: Literal["strip", "strip_trailing_slash"]

    def to_mapping(self) -> dict[str, str]:
        """Return the closed canonical JSON object."""
        return {"name": self.name, "normalization": self.normalization}


def _parse_plugin_metadata(
    value: Any,
    *,
    source_kind: str,
    process_mode: str,
) -> PluginMetadata | None:
    if source_kind == "builtin":
        if value is not None:
            raise _error(
                "Builtin plugin_metadata must be null",
                "plugin_metadata",
            )
        return None
    mapping = _closed_object(
        value,
        _PLUGIN_METADATA_FIELDS,
        "plugin_metadata",
    )
    plugin_id = _string(
        mapping["plugin_id"],
        "plugin_metadata",
        "plugin_id",
        nonempty=True,
    )
    version = _string(
        mapping["version"],
        "plugin_metadata",
        "version",
        nonempty=True,
    )
    artifact = _string(
        mapping["artifact_sha256"],
        "plugin_metadata",
        "artifact_sha256",
    )
    if process_mode == "runner_process":
        validate_digest(artifact, name="Plugin artifact digest")
    elif artifact and not re.fullmatch(r"[0-9a-f]{64}", artifact):
        raise _error(
            "Legacy plugin artifact digest is not canonical",
            "plugin_metadata",
            "artifact_sha256",
        )
    return PluginMetadata(plugin_id, version, artifact)


def _parse_entrypoint(value: Any, process_mode: str) -> Entrypoint:
    mapping = _closed_object(value, _ENTRYPOINT_FIELDS, "entrypoint")
    scope = _enum(
        mapping["scope"],
        _ENTRYPOINT_SCOPES,
        "entrypoint",
        "scope",
    )
    module = _string(
        mapping["module"],
        "entrypoint",
        "module",
        nonempty=True,
    )
    qualname = _string(
        mapping["qualname"],
        "entrypoint",
        "qualname",
        nonempty=True,
    )
    if not module.isascii() or not _DOTTED_NAME_PATTERN.fullmatch(module):
        raise _error("Entrypoint module is invalid", "entrypoint", "module")
    if not qualname.isascii() or not _DOTTED_NAME_PATTERN.fullmatch(qualname):
        raise _error(
            "Entrypoint qualname is invalid",
            "entrypoint",
            "qualname",
        )
    expected_scope = "core" if process_mode == "in_process" else "runner"
    if scope != expected_scope:
        raise _error(
            "Entrypoint scope does not match process mode",
            "entrypoint",
            "scope",
        )
    return Entrypoint(
        scope=cast(Literal["core", "runner"], scope),
        module=module,
        qualname=qualname,
    )


def _parse_config_field(value: Any, index: int) -> ConfigField:
    path = ("config_fields", index)
    mapping = _closed_object(value, _CONFIG_FIELD_FIELDS, *path)
    name = _string(mapping["name"], *path, "name")
    if not _FIELD_NAME_PATTERN.fullmatch(name):
        raise _error("Config field name is invalid", *path, "name")
    label = _localized_text(mapping["label"], *path, "label")
    help_text = _localized_text(mapping["help"], *path, "help")
    placeholder = _localized_text(
        mapping["placeholder"],
        *path,
        "placeholder",
    )
    field_type = _enum(mapping["type"], _CONFIG_FIELD_TYPES, *path, "type")
    required = _boolean(mapping["required"], *path, "required")
    nullable = _boolean(mapping["nullable"], *path, "nullable")
    secret = _boolean(mapping["secret"], *path, "secret")
    condition = _boolean(mapping["condition"], *path, "condition")
    if required and nullable:
        raise _error("Required fields cannot be nullable", *path)
    default = _canonical_scalar(mapping["default"], *path, "default")
    if not _compatible_value(field_type, default):
        raise _error(
            "Default is incompatible with field type",
            *path,
            "default",
        )
    allowed = _unique_scalars(
        _require_list(mapping["allowed_values"], *path, "allowed_values"),
        field_type=field_type,
        path=(*path, "allowed_values"),
    )
    if secret and (default is not None or allowed):
        raise _error(
            "Secret fields require null default and empty allowed_values",
            *path,
        )
    if secret and condition:
        raise _error("Secret fields cannot be condition fields", *path)
    if required and (default == "" or any(item == "" for item in allowed)):
        raise _error(
            "Required fields cannot default to or allow an empty string",
            *path,
        )
    if not nullable and any(item is None for item in allowed):
        raise _error(
            "Non-nullable fields cannot allow null",
            *path,
            "allowed_values",
        )
    if default is not None and allowed:
        encoded_allowed = {canonical_json(item) for item in allowed}
        if canonical_json(default) not in encoded_allowed:
            raise _error("Default must be present in allowed_values", *path)
    return ConfigField(
        name=name,
        label=label,
        help=help_text,
        placeholder=placeholder,
        type=cast(
            Literal["text", "password", "number", "switch", "select"],
            field_type,
        ),
        required=required,
        nullable=nullable,
        default=default,
        allowed_values=allowed,
        secret=secret,
        condition=condition,
    )


def _parse_identity_fields(
    values: Any,
    config_by_name: Mapping[str, ConfigField],
) -> tuple[BotIdentityField, ...]:
    output: list[BotIdentityField] = []
    names: set[str] = set()
    for index, value in enumerate(
        _require_list(values, "bot_identity_fields"),
    ):
        path = ("bot_identity_fields", index)
        mapping = _closed_object(value, _IDENTITY_FIELD_FIELDS, *path)
        name = _string(mapping["name"], *path, "name", nonempty=True)
        normalization = _enum(
            mapping["normalization"],
            _IDENTITY_NORMALIZATIONS,
            *path,
            "normalization",
        )
        if name not in config_by_name:
            raise _error(
                "Identity field does not reference config_fields",
                *path,
            )
        if name in names:
            raise _error("Identity field name is duplicated", *path)
        names.add(name)
        output.append(
            BotIdentityField(
                name,
                cast(
                    Literal["strip", "strip_trailing_slash"],
                    normalization,
                ),
            ),
        )
    return tuple(
        sorted(output, key=lambda item: (item.name, item.normalization)),
    )


def _sorted_string_set(
    value: Any,
    *,
    path: str,
    validator: Any = None,
) -> tuple[str, ...]:
    output: set[str] = set()
    for index, item in enumerate(_require_list(value, path)):
        text = _string(item, path, index, nonempty=True)
        if validator is not None:
            validator(text)
        output.add(text)
    return tuple(sorted(output))


def _validate_environment_name(value: str) -> None:
    if not _ENVIRONMENT_NAME_PATTERN.fullmatch(value):
        raise _error(
            "Environment variable name is invalid",
            "environment_passthrough_allowlist",
        )


def _parse_conditions(
    value: Any,
    config_fields: tuple[ConfigField, ...],
    config_by_name: Mapping[str, ConfigField],
) -> tuple[str, ...]:
    condition_fields = _sorted_string_set(
        value,
        path="condition_fields",
    )
    condition_names = {
        field.name for field in config_fields if field.condition
    }
    if set(condition_fields) != condition_names:
        raise _error(
            "condition_fields must match condition config fields",
            "condition_fields",
        )
    for name in condition_fields:
        field = config_by_name[name]
        if not field.allowed_values:
            raise _error(
                "Condition field requires finite allowed_values",
                "config_fields",
                name,
            )
        if field.type == "password":
            raise _error(
                "Password fields cannot be conditions",
                "config_fields",
                name,
            )
        for allowed in field.allowed_values:
            if isinstance(allowed, Decimal):
                raise _error(
                    "Decimal condition values are not allowed",
                    "config_fields",
                    name,
                )
            if not (
                allowed is None
                or isinstance(allowed, (str, bool))
                or (isinstance(allowed, int) and not isinstance(allowed, bool))
            ):
                raise _error(
                    "Condition value is outside the finite v1 domain",
                    "config_fields",
                    name,
                )
    return condition_fields


def _parse_targets(
    mapping: Mapping[str, Any],
    *,
    process_mode: str,
    allowed_platform_tags: Collection[str],
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    python_abis = _sorted_string_set(
        mapping["supported_python_abis"],
        path="supported_python_abis",
        validator=validate_python_abi,
    )
    platform_tags = _sorted_string_set(
        mapping["supported_platform_tags"],
        path="supported_platform_tags",
        validator=lambda item: validate_platform_tag(
            item,
            allowed_platform_tags=allowed_platform_tags,
        ),
    )
    if process_mode == "runner_process" and (
        not python_abis or not platform_tags
    ):
        raise _error("runner_process requires at least one ABI and platform")
    return python_abis, platform_tags


def _parse_capabilities(
    value: Any,
    *,
    ingress_owner: str,
    process_mode: str,
) -> tuple[str, ...]:
    capabilities: list[str] = []
    seen: set[str] = set()
    for index, item in enumerate(_require_list(value, "capabilities")):
        capability = _string(item, "capabilities", index, nonempty=True)
        if not _CAPABILITY_PATTERN.fullmatch(capability):
            raise _error("Capability ID is invalid", "capabilities", index)
        if capability not in _CAPABILITIES:
            raise _error(
                "Capability ID is not registered",
                "capabilities",
                index,
            )
        if capability in seen:
            raise _error("Capability ID is duplicated", "capabilities", index)
        seen.add(capability)
        capabilities.append(capability)
    has_ingress = "ingress_endpoint" in seen
    if ingress_owner == "none" and has_ingress:
        raise _error("ingress_owner=none forbids ingress_endpoint")
    if ingress_owner != "none" and not has_ingress:
        raise _error("Ingress owner requires ingress_endpoint capability")
    if ingress_owner == "runner_owned" and process_mode != "runner_process":
        raise _error("runner_owned ingress requires runner_process")
    if (
        "exactly-once-visible" in seen
        and "server_side_idempotency" not in seen
    ):
        raise _error(
            "exactly-once-visible requires server_side_idempotency",
            "capabilities",
        )
    return tuple(sorted(capabilities))


@dataclass(frozen=True)
class ChannelDescriptor:
    """Canonical, closed Channel descriptor schema version 1."""

    schema_version: int
    channel_key: str
    source_kind: SourceKind
    process_mode: ProcessMode
    dispatch_mode: DispatchMode
    ingress_owner: IngressOwner
    label: LocalizedText
    description: LocalizedText
    icon: str
    doc_url: LocalizedText
    plugin_metadata: PluginMetadata | None
    entrypoint: Entrypoint
    config_fields: tuple[ConfigField, ...]
    core_requirements: tuple[str, ...]
    isolated_requirements: tuple[str, ...]
    condition_fields: tuple[str, ...]
    supported_python_abis: tuple[str, ...]
    supported_platform_tags: tuple[str, ...]
    capabilities: tuple[str, ...]
    bot_identity_fields: tuple[BotIdentityField, ...]
    environment_passthrough_allowlist: tuple[str, ...]

    @classmethod
    def from_json(
        cls,
        data: str | bytes,
        *,
        allowed_platform_tags: Collection[str],
    ) -> "ChannelDescriptor":
        """Parse exact JSON and validate a descriptor without imports."""
        value = parse_json_value(data)
        if not isinstance(value, Mapping):
            raise _error("Descriptor JSON must contain an object")
        return cls.from_mapping(
            value,
            allowed_platform_tags=allowed_platform_tags,
        )

    @classmethod
    def from_mapping(
        cls,
        value: Mapping[str, Any],
        *,
        allowed_platform_tags: Collection[str],
    ) -> "ChannelDescriptor":
        """Validate and canonicalize a descriptor without importing code."""
        mapping = _closed_object(value, _TOP_LEVEL_FIELDS)
        schema_version = mapping["schema_version"]
        if (
            not isinstance(schema_version, int)
            or isinstance(
                schema_version,
                bool,
            )
            or schema_version != 1
        ):
            raise _error("schema_version must be integer 1", "schema_version")
        source_kind = _enum(
            mapping["source_kind"],
            _SOURCE_KINDS,
            "source_kind",
        )
        process_mode = _enum(
            mapping["process_mode"],
            _PROCESS_MODES,
            "process_mode",
        )
        legacy = source_kind == "plugin" and process_mode == "in_process"
        channel_key = _string(
            mapping["channel_key"],
            "channel_key",
            nonempty=True,
        )
        if not legacy:
            validate_channel_key(channel_key)
        dispatch_mode = _enum(
            mapping["dispatch_mode"],
            _DISPATCH_MODES,
            "dispatch_mode",
        )
        ingress_owner = _enum(
            mapping["ingress_owner"],
            _INGRESS_OWNERS,
            "ingress_owner",
        )
        label = _localized_text(mapping["label"], "label")
        if not legacy:
            if not isinstance(label, dict):
                raise _error("New descriptor label must be localized", "label")
            if not label.get("en") or not label.get("zh"):
                raise _error("Label requires non-empty en and zh", "label")
        description = _localized_text(mapping["description"], "description")
        icon = _http_url(_string(mapping["icon"], "icon"), "icon")
        doc_url = _localized_text(mapping["doc_url"], "doc_url", url=True)
        plugin_metadata = _parse_plugin_metadata(
            mapping["plugin_metadata"],
            source_kind=source_kind,
            process_mode=process_mode,
        )
        entrypoint = _parse_entrypoint(mapping["entrypoint"], process_mode)

        config_fields = tuple(
            _parse_config_field(item, index)
            for index, item in enumerate(
                _require_list(mapping["config_fields"], "config_fields"),
            )
        )
        config_by_name = {field.name: field for field in config_fields}
        if len(config_by_name) != len(config_fields):
            raise _error("Config field names must be unique", "config_fields")

        core_requirements = canonicalize_requirements(
            _require_list(mapping["core_requirements"], "core_requirements"),
        )
        isolated_requirements = canonicalize_requirements(
            _require_list(
                mapping["isolated_requirements"],
                "isolated_requirements",
            ),
        )
        if process_mode == "in_process" and isolated_requirements:
            raise _error(
                "in_process descriptors cannot have isolated requirements",
                "isolated_requirements",
            )

        condition_fields = _parse_conditions(
            mapping["condition_fields"],
            config_fields,
            config_by_name,
        )
        python_abis, platform_tags = _parse_targets(
            mapping,
            process_mode=process_mode,
            allowed_platform_tags=allowed_platform_tags,
        )
        capabilities = _parse_capabilities(
            mapping["capabilities"],
            ingress_owner=ingress_owner,
            process_mode=process_mode,
        )

        identity_fields = _parse_identity_fields(
            mapping["bot_identity_fields"],
            config_by_name,
        )
        passthrough = _sorted_string_set(
            mapping["environment_passthrough_allowlist"],
            path="environment_passthrough_allowlist",
            validator=_validate_environment_name,
        )
        return cls(
            schema_version=1,
            channel_key=channel_key,
            source_kind=cast(SourceKind, source_kind),
            process_mode=cast(ProcessMode, process_mode),
            dispatch_mode=cast(DispatchMode, dispatch_mode),
            ingress_owner=cast(IngressOwner, ingress_owner),
            label=label,
            description=description,
            icon=icon,
            doc_url=doc_url,
            plugin_metadata=plugin_metadata,
            entrypoint=entrypoint,
            config_fields=config_fields,
            core_requirements=core_requirements,
            isolated_requirements=isolated_requirements,
            condition_fields=condition_fields,
            supported_python_abis=python_abis,
            supported_platform_tags=platform_tags,
            capabilities=capabilities,
            bot_identity_fields=identity_fields,
            environment_passthrough_allowlist=passthrough,
        )

    def to_mapping(self) -> dict[str, Any]:
        """Return the canonical descriptor object used for its digest."""
        return {
            "schema_version": self.schema_version,
            "channel_key": self.channel_key,
            "source_kind": self.source_kind,
            "process_mode": self.process_mode,
            "dispatch_mode": self.dispatch_mode,
            "ingress_owner": self.ingress_owner,
            "label": self.label,
            "description": self.description,
            "icon": self.icon,
            "doc_url": self.doc_url,
            "plugin_metadata": (
                self.plugin_metadata.to_mapping()
                if self.plugin_metadata is not None
                else None
            ),
            "entrypoint": self.entrypoint.to_mapping(),
            "config_fields": [
                field.to_mapping() for field in self.config_fields
            ],
            "core_requirements": list(self.core_requirements),
            "isolated_requirements": list(self.isolated_requirements),
            "condition_fields": list(self.condition_fields),
            "supported_python_abis": list(self.supported_python_abis),
            "supported_platform_tags": list(self.supported_platform_tags),
            "capabilities": list(self.capabilities),
            "bot_identity_fields": [
                field.to_mapping() for field in self.bot_identity_fields
            ],
            "environment_passthrough_allowlist": list(
                self.environment_passthrough_allowlist,
            ),
        }

    def canonical_bytes(self) -> bytes:
        """Return the canonical descriptor bytes."""
        return canonical_json(self.to_mapping())

    def digest(self) -> str:
        """Return the domain-separated descriptor SHA-256 digest."""
        return domain_sha256(DESCRIPTOR_DOMAIN, self.to_mapping())

    def condition_set(
        self,
        effective_config: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Extract and validate conditions from schema-expanded config."""
        if not isinstance(effective_config, Mapping):
            raise _error("Effective config must be an object")
        fields = {field.name: field for field in self.config_fields}
        conditions: dict[str, Any] = {}
        for name in self.condition_fields:
            if name not in effective_config:
                raise _error("Effective config is missing a condition", name)
            value = effective_config[name]
            field = fields[name]
            encoded = canonical_json(value)
            allowed = {canonical_json(item) for item in field.allowed_values}
            if encoded not in allowed:
                raise _error("Condition value is not allowed", name)
            conditions[name] = value
        condition_set_sha256(conditions)
        return conditions

    def bot_identity(
        self,
        effective_config: Mapping[str, Any],
    ) -> tuple[tuple[str, str], ...] | None:
        """Build the in-memory comparable identity declared by descriptor."""
        if not isinstance(effective_config, Mapping):
            raise _error("Effective config must be an object")
        identity: list[tuple[str, str]] = []
        for field in self.bot_identity_fields:
            if field.name not in effective_config:
                raise _error(
                    "Effective config is missing an identity field",
                    field.name,
                )
            raw = effective_config[field.name]
            normalized = "" if raw is None else str(raw).strip()
            if field.normalization == "strip_trailing_slash":
                normalized = normalized.rstrip("/")
            if not normalized:
                return None
            identity.append((field.name, normalized))
        return tuple(identity) if identity else None
