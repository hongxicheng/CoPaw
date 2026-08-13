# -*- coding: utf-8 -*-
"""Pure Channel identity and directory-key value models."""

from __future__ import annotations

from collections.abc import Collection, Mapping
from dataclasses import dataclass
import hashlib
import re
import secrets
from typing import Any

from packaging.tags import sys_tags

from .canonical import canonical_json, domain_sha256, normalize_string
from .errors import validation_error


_CHANNEL_KEY_PATTERN = re.compile(
    r"^[a-z0-9](?:[a-z0-9_-]{0,62}[a-z0-9])?$",
)
_PYTHON_ABI_PATTERN = re.compile(r"^[a-z0-9_]+-[a-z0-9_]+$")
_PLATFORM_TAG_PATTERN = re.compile(r"^[a-z0-9]+(?:_[a-z0-9]+)*$")
_DIGEST_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_INSTANCE_ID_PATTERN = re.compile(r"^chi1_[0-9a-f]{64}$")
_ENVIRONMENT_SPEC_ID_PATTERN = re.compile(r"^ches1_[0-9a-f]{64}$")
_INSTALLATION_ID_PATTERN = re.compile(r"^install1_[0-9a-f]{32}$")
_ENVIRONMENT_ID_PATTERN = re.compile(
    r"^(ches1_[0-9a-f]{64})\.(install1_[0-9a-f]{32})$",
)
_DIR_KEY_PATTERN = re.compile(r"^dir1_[0-9a-f]{32}$")
_PLATFORM_ALIASES = frozenset({"windows", "darwin", "macos", "x86_64"})

INSTANCE_DOMAIN = "qwenpaw.channel.instance.v1"
CONDITION_SET_DOMAIN = "qwenpaw.channel.conditions.v1"
ENVIRONMENT_SPEC_DOMAIN = "qwenpaw.channel.environment-spec.v1"


def validate_channel_key(value: str) -> str:
    """Validate a canonical v1 builtin or isolated Channel key."""
    if not isinstance(value, str) or not _CHANNEL_KEY_PATTERN.fullmatch(value):
        raise validation_error("Channel key is not canonical")
    return value


def validate_agent_id(value: str) -> str:
    """Validate the non-empty, case-sensitive committed Agent ID."""
    if not isinstance(value, str) or not value:
        raise validation_error("Agent ID must be a non-empty string")
    return normalize_string(value)


def validate_digest(value: str, *, name: str = "digest") -> str:
    """Validate a complete lowercase SHA-256 hexadecimal digest."""
    if not isinstance(value, str) or not _DIGEST_PATTERN.fullmatch(value):
        raise validation_error(f"{name} must be 64 lowercase hex characters")
    return value


def validate_python_abi(value: str) -> str:
    """Validate the canonical interpreter-ABI pair."""
    if not isinstance(value, str) or not _PYTHON_ABI_PATTERN.fullmatch(value):
        raise validation_error("Python ABI is not canonical")
    return value


def current_python_abi() -> str:
    """Return the current interpreter's first concrete PEP 425 ABI."""
    for tag in sys_tags():
        if tag.abi != "none":
            return validate_python_abi(f"{tag.interpreter}-{tag.abi}")
    raise validation_error("Current interpreter has no concrete ABI tag")


def validate_platform_tag(
    value: str,
    *,
    allowed_platform_tags: Collection[str],
) -> str:
    """Validate syntax and release-target registry membership."""
    if not isinstance(value, str) or not _PLATFORM_TAG_PATTERN.fullmatch(
        value,
    ):
        raise validation_error("Platform tag is not canonical")
    if value in _PLATFORM_ALIASES:
        raise validation_error("Product platform aliases are not PEP 425 tags")
    if value not in allowed_platform_tags:
        raise validation_error("Platform tag is not in the release registry")
    return value


def condition_set_sha256(condition_set: Mapping[str, Any]) -> str:
    """Hash a validated condition set with its v1 domain."""
    if not isinstance(condition_set, Mapping):
        raise validation_error("Condition set must be an object")
    validated: dict[str, Any] = {}
    for name, value in condition_set.items():
        if not isinstance(name, str):
            raise validation_error("Condition field names must be strings")
        normalized_name = normalize_string(name)
        if normalized_name in validated:
            raise validation_error(
                "Condition set has duplicate normalized field names",
            )
        if isinstance(value, bool) or value is None:
            validated[normalized_name] = value
        elif isinstance(value, int):
            canonical_json(value)
            validated[normalized_name] = value
        elif isinstance(value, str):
            validated[normalized_name] = normalize_string(value)
        else:
            raise validation_error(
                "Condition values must be string, boolean, null, or integer",
            )
    return domain_sha256(CONDITION_SET_DOMAIN, validated)


@dataclass(frozen=True)
class InstanceIdentity:
    """Deterministic identity for one Agent and Channel pair."""

    agent_id: str
    channel_key: str
    instance_id: str

    @classmethod
    def create(cls, *, agent_id: str, channel_key: str) -> "InstanceIdentity":
        """Create a deterministic instance identity."""
        agent_id = validate_agent_id(agent_id)
        channel_key = validate_channel_key(channel_key)
        payload = {"agent_id": agent_id, "channel_key": channel_key}
        digest = domain_sha256(INSTANCE_DOMAIN, payload)
        return cls(
            agent_id=agent_id,
            channel_key=channel_key,
            instance_id=f"chi1_{digest}",
        )

    @classmethod
    def parse(
        cls,
        *,
        agent_id: str,
        channel_key: str,
        instance_id: str,
    ) -> "InstanceIdentity":
        """Validate a supplied instance ID against its logical inputs."""
        if not isinstance(
            instance_id,
            str,
        ) or not _INSTANCE_ID_PATTERN.fullmatch(
            instance_id,
        ):
            raise validation_error("Instance ID is not canonical")
        expected = cls.create(agent_id=agent_id, channel_key=channel_key)
        if instance_id != expected.instance_id:
            raise validation_error("Instance ID does not match its payload")
        return expected


@dataclass(frozen=True)
class EnvironmentSpecIdentity:
    """Deterministic identity for an immutable environment specification."""

    channel_key: str
    lock_sha256: str
    python_abi: str
    platform_tag: str
    condition_set_sha256: str
    environment_spec_id: str

    @classmethod
    def create(
        cls,
        *,
        channel_key: str,
        lock_sha256: str,
        python_abi: str,
        platform_tag: str,
        condition_set: Mapping[str, Any],
        allowed_platform_tags: Collection[str],
    ) -> "EnvironmentSpecIdentity":
        """Create a deterministic environment specification identity."""
        channel_key = validate_channel_key(channel_key)
        lock_sha256 = validate_digest(lock_sha256, name="Lock digest")
        python_abi = validate_python_abi(python_abi)
        platform_tag = validate_platform_tag(
            platform_tag,
            allowed_platform_tags=allowed_platform_tags,
        )
        conditions_digest = condition_set_sha256(condition_set)
        payload = {
            "channel_key": channel_key,
            "condition_set_sha256": conditions_digest,
            "lock_sha256": lock_sha256,
            "platform_tag": platform_tag,
            "python_abi": python_abi,
        }
        digest = domain_sha256(ENVIRONMENT_SPEC_DOMAIN, payload)
        return cls(
            channel_key=channel_key,
            lock_sha256=lock_sha256,
            python_abi=python_abi,
            platform_tag=platform_tag,
            condition_set_sha256=conditions_digest,
            environment_spec_id=f"ches1_{digest}",
        )

    def validate_id(self, value: str) -> str:
        """Validate a supplied environment spec ID against this model."""
        if not isinstance(
            value,
            str,
        ) or not _ENVIRONMENT_SPEC_ID_PATTERN.fullmatch(
            value,
        ):
            raise validation_error("Environment spec ID is not canonical")
        if value != self.environment_spec_id:
            raise validation_error(
                "Environment spec ID does not match its payload",
            )
        return value


@dataclass(frozen=True)
class InstallationIdentity:
    """Random immutable installation identity persisted in install.json."""

    installation_id: str

    @classmethod
    def create(cls) -> "InstallationIdentity":
        """Generate a new 128-bit installation identity."""
        return cls(installation_id=f"install1_{secrets.token_hex(16)}")

    @classmethod
    def parse(cls, value: str) -> "InstallationIdentity":
        """Validate a persisted installation identity."""
        if not isinstance(
            value,
            str,
        ) or not _INSTALLATION_ID_PATTERN.fullmatch(
            value,
        ):
            raise validation_error("Installation ID is not canonical")
        return cls(installation_id=value)


@dataclass(frozen=True)
class EnvironmentIdentity:
    """Identity of one installation of an environment specification."""

    environment_spec_id: str
    installation_id: str
    environment_id: str

    @classmethod
    def create(
        cls,
        *,
        environment_spec_id: str,
        installation: InstallationIdentity | None = None,
    ) -> "EnvironmentIdentity":
        """Create an environment ID for a new installation."""
        if not isinstance(
            environment_spec_id,
            str,
        ) or not _ENVIRONMENT_SPEC_ID_PATTERN.fullmatch(environment_spec_id):
            raise validation_error("Environment spec ID is not canonical")
        installation = installation or InstallationIdentity.create()
        parsed = InstallationIdentity.parse(installation.installation_id)
        environment_id = f"{environment_spec_id}.{parsed.installation_id}"
        return cls(
            environment_spec_id=environment_spec_id,
            installation_id=parsed.installation_id,
            environment_id=environment_id,
        )

    @classmethod
    def parse(cls, value: str) -> "EnvironmentIdentity":
        """Parse and validate a complete environment identity."""
        if not isinstance(value, str):
            raise validation_error("Environment ID must be a string")
        match = _ENVIRONMENT_ID_PATTERN.fullmatch(value)
        if match is None:
            raise validation_error("Environment ID is not canonical")
        return cls(
            environment_spec_id=match.group(1),
            installation_id=match.group(2),
            environment_id=value,
        )


def dir_key(logical_id: str) -> str:
    """Return the cross-platform directory key for one logical ID."""
    if not isinstance(logical_id, str) or not logical_id:
        raise validation_error("Logical ID must be a non-empty string")
    try:
        logical_id_bytes = logical_id.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise validation_error(
            "Logical ID must contain only Unicode scalar values",
        ) from exc
    digest = hashlib.sha256(logical_id_bytes).hexdigest()[:32]
    return f"dir1_{digest}"


@dataclass(frozen=True)
class DirectoryIdentity:
    """Validated mapping between a logical ID and its directory manifest."""

    logical_id: str
    directory_key: str

    @classmethod
    def validate(
        cls,
        *,
        logical_id: str,
        directory_key: str,
        manifest_logical_id: str,
    ) -> "DirectoryIdentity":
        """Reject malformed keys and short-key manifest collisions."""
        if not isinstance(
            directory_key,
            str,
        ) or not _DIR_KEY_PATTERN.fullmatch(
            directory_key,
        ):
            raise validation_error("Directory key is not canonical")
        expected = dir_key(logical_id)
        if directory_key != expected:
            raise validation_error("Directory key does not match logical ID")
        if manifest_logical_id != logical_id:
            raise validation_error("Directory manifest logical ID collision")
        return cls(logical_id=logical_id, directory_key=directory_key)
