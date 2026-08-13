# -*- coding: utf-8 -*-
"""Tests for the CH-0-002 closed Channel descriptor v1 model."""

from __future__ import annotations

from copy import deepcopy
from decimal import Decimal
import json
import subprocess
import sys
from typing import Any

import pytest

from qwenpaw.channel_protocol import DescriptorValidationError
from qwenpaw.channel_protocol.descriptor import (
    ChannelDescriptor,
    resolve_localized_text,
)


PLATFORM = "macosx_11_0_arm64"
ALLOWED_PLATFORMS = {PLATFORM, "manylinux_2_28_x86_64", "win_amd64"}
EXPECTED_DIGEST = (
    "8b05ef521e5f2ae268f90f704dd36f1fe1e8eb958182c1c0220ffe6405e7cdb8"
)


def test_value_model_imports_no_channel_implementation_modules() -> None:
    """The pure package does not load Registry, Channel, or platform SDKs."""
    script = (
        "import json, sys; "
        "import qwenpaw.channel_protocol; "
        "print(json.dumps(sorted(name for name in sys.modules "
        "if name.startswith('qwenpaw.app.channels.'))))"
    )
    result = subprocess.run(
        [sys.executable, "-c", script],
        check=True,
        capture_output=True,
        text=True,
    )

    assert json.loads(result.stdout) == []


def _field(
    name: str,
    *,
    field_type: str = "text",
    required: bool = False,
    nullable: bool = False,
    default: Any = None,
    allowed_values: list[Any] | None = None,
    secret: bool = False,
    condition: bool = False,
    label: Any = "",
    help_text: Any = "",
) -> dict[str, Any]:
    """Build one complete config field object for descriptor tests."""
    return {
        "name": name,
        "label": label,
        "help": help_text,
        "placeholder": "",
        "type": field_type,
        "required": required,
        "nullable": nullable,
        "default": default,
        "allowed_values": allowed_values or [],
        "secret": secret,
        "condition": condition,
    }


def _descriptor() -> dict[str, Any]:
    """Build an intentionally non-canonical producer form of the fixture."""
    return {
        "schema_version": 1,
        "channel_key": "fixture",
        "source_kind": "builtin",
        "process_mode": "runner_process",
        "dispatch_mode": "manager_queue",
        "ingress_owner": "none",
        "label": {"zh": "示例", "en": "Fixture\u0301"},
        "description": {"zh": "", "en": ""},
        "icon": "",
        "doc_url": {
            "zh": "https://example.com/zh",
            "en": "https://example.com/en",
        },
        "plugin_metadata": None,
        "entrypoint": {
            "qualname": "FixtureDriver",
            "scope": "runner",
            "module": "qwenpaw.fixture",
        },
        "config_fields": [
            _field(
                "region",
                field_type="select",
                required=True,
                default="eu",
                allowed_values=["eu", "us"],
                condition=True,
                label={"zh": "示例", "en": "Fixture\u0301"},
                help_text="Line\nhelp",
            ),
            _field(
                "bot_token",
                field_type="password",
                required=True,
                secret=True,
                label={"zh": "令牌", "en": "Token"},
            ),
            _field(
                "url",
                label={"zh": "地址", "en": "URL"},
            ),
        ],
        "core_requirements": ["Requests >= 2", "requests>=2"],
        "isolated_requirements": [
            'Fixture[FOO,bar] >= 1.0 ; python_version >= "3.11"',
        ],
        "condition_fields": ["region"],
        "supported_python_abis": ["cp313-cp313"],
        "supported_platform_tags": [PLATFORM],
        "capabilities": ["streaming", "media"],
        "bot_identity_fields": [
            {"name": "url", "normalization": "strip_trailing_slash"},
            {"name": "bot_token", "normalization": "strip"},
        ],
        "environment_passthrough_allowlist": ["HTTPS_PROXY"],
    }


def _validate(value: dict[str, Any]) -> ChannelDescriptor:
    return ChannelDescriptor.from_mapping(
        value,
        allowed_platform_tags=ALLOWED_PLATFORMS,
    )


def test_complete_descriptor_fixture_matches_design_digest() -> None:
    """The full Unicode, Requirement, and collection fixture is stable."""
    descriptor = _validate(_descriptor())

    assert descriptor.digest() == EXPECTED_DIGEST
    assert descriptor.core_requirements == ("requests>=2",)
    assert descriptor.isolated_requirements == (
        'fixture[bar,foo]>=1.0 ; python_version >= "3.11"',
    )
    assert [item.name for item in descriptor.bot_identity_fields] == [
        "bot_token",
        "url",
    ]
    assert descriptor.capabilities == ("media", "streaming")
    assert b"Fixture\xcc\x81" not in descriptor.canonical_bytes()
    assert "Fixturé" in descriptor.canonical_bytes().decode("utf-8")


def test_descriptor_json_decoder_preserves_decimal_numbers() -> None:
    """JSON descriptor decimals remain exact and encode without exponent."""
    value = _descriptor()
    value["config_fields"].append(
        _field(
            "ratio",
            field_type="number",
            default=Decimal("1.2300"),
        ),
    )
    canonical = _validate(value).canonical_bytes()

    parsed = ChannelDescriptor.from_json(
        canonical,
        allowed_platform_tags=ALLOWED_PLATFORMS,
    )
    ratio = next(item for item in parsed.config_fields if item.name == "ratio")
    assert ratio.default == Decimal("1.23")


def test_descriptor_does_not_import_entrypoint() -> None:
    """Static validation treats entrypoint as data and imports no module."""
    module = "qwenpaw.fixture.never_imported"
    value = _descriptor()
    value["entrypoint"]["module"] = module

    assert module not in sys.modules
    assert _validate(value).entrypoint.module == module
    assert module not in sys.modules


def test_secret_identity_normalization_is_core_memory_only() -> None:
    """Secret references normalize in memory and never enter the digest."""
    descriptor = _validate(_descriptor())
    before = descriptor.digest()
    identity = descriptor.bot_identity(
        {"bot_token": "  secret-token  ", "url": " https://bot.example/// "},
    )

    assert identity == (
        ("bot_token", "secret-token"),
        ("url", "https://bot.example"),
    )
    assert descriptor.digest() == before
    assert b"secret-token" not in descriptor.canonical_bytes()
    assert (
        descriptor.bot_identity(
            {"bot_token": "", "url": "https://bot"},
        )
        is None
    )
    with pytest.raises(DescriptorValidationError):
        descriptor.bot_identity({"bot_token": "secret"})


def test_identity_normalization_preserves_falsey_scalars() -> None:
    """Identity conversion does not treat zero or false as missing values."""
    value = _descriptor()
    value["config_fields"].extend(
        [
            _field("enabled", field_type="switch", default=False),
            _field("port", field_type="number", default=0),
        ],
    )
    value["bot_identity_fields"].extend(
        [
            {"name": "port", "normalization": "strip"},
            {"name": "enabled", "normalization": "strip"},
        ],
    )
    descriptor = _validate(value)

    assert descriptor.bot_identity(
        {
            "bot_token": "secret",
            "enabled": False,
            "port": 0,
            "url": "https://bot.example/",
        },
    ) == (
        ("bot_token", "secret"),
        ("enabled", "False"),
        ("port", "0"),
        ("url", "https://bot.example"),
    )


def test_condition_set_uses_only_declared_fields() -> None:
    """Unrelated effective config cannot affect the condition set."""
    descriptor = _validate(_descriptor())

    assert descriptor.condition_set(
        {"region": "eu", "bot_token": "secret", "other": "ignored"},
    ) == {"region": "eu"}
    with pytest.raises(DescriptorValidationError):
        descriptor.condition_set({"region": "asia"})
    with pytest.raises(DescriptorValidationError):
        descriptor.condition_set({})


def test_localized_text_fallback_and_url_rules() -> None:
    """Locale lookup follows exact, primary, English, then sorted fallback."""
    descriptor = _validate(_descriptor())

    assert resolve_localized_text(descriptor.label, "zh-CN") == "示例"
    assert resolve_localized_text(descriptor.label, "fr-FR") == "Fixturé"
    assert resolve_localized_text({"ja": "日本語", "zh": "中文"}, "fr") == ("日本語")
    value = _descriptor()
    value["doc_url"] = {"en": "file:///tmp/docs"}
    with pytest.raises(DescriptorValidationError):
        _validate(value)
    value = _descriptor()
    value["label"] = {}
    with pytest.raises(DescriptorValidationError):
        _validate(value)


@pytest.mark.parametrize(
    ("field", "invalid"),
    [
        ("source_kind", "external"),
        ("process_mode", "thread"),
        ("dispatch_mode", "direct"),
        ("ingress_owner", "host"),
    ],
)
def test_descriptor_rejects_each_closed_enum(field: str, invalid: str) -> None:
    """Every top-level enum is closed in schema version 1."""
    value = _descriptor()
    value[field] = invalid

    with pytest.raises(DescriptorValidationError) as raised:
        _validate(value)

    assert raised.value.code == "descriptor_invalid"


@pytest.mark.parametrize(
    "mutate",
    [
        lambda value: value["entrypoint"].update({"scope": "worker"}),
        lambda value: value["config_fields"][0].update({"type": "slider"}),
        lambda value: value["bot_identity_fields"][0].update(
            {"normalization": "lower"},
        ),
    ],
)
def test_descriptor_rejects_each_nested_closed_enum(mutate: Any) -> None:
    """Entrypoint, field, and identity enums are closed in v1."""
    value = _descriptor()
    mutate(value)

    with pytest.raises(DescriptorValidationError) as raised:
        _validate(value)

    assert raised.value.code == "descriptor_invalid"


@pytest.mark.parametrize(
    "mutate",
    [
        lambda value: value.update({"unknown": True}),
        lambda value: value["entrypoint"].update({"unknown": True}),
        lambda value: value["config_fields"][0].update({"unknown": True}),
        lambda value: value["bot_identity_fields"][0].update(
            {"unknown": True},
        ),
    ],
)
def test_descriptor_rejects_unknown_fields(mutate: Any) -> None:
    """The descriptor and every nested record use closed objects."""
    value = _descriptor()
    mutate(value)

    with pytest.raises(DescriptorValidationError):
        _validate(value)

    plugin = _descriptor()
    plugin["source_kind"] = "plugin"
    plugin["plugin_metadata"] = {
        "plugin_id": "fixture-owner",
        "version": "1.0.0",
        "artifact_sha256": "a" * 64,
        "unknown": True,
    }
    with pytest.raises(DescriptorValidationError):
        _validate(plugin)


def test_config_field_required_nullable_secret_and_allowed_value_rules() -> (
    None
):
    """Enforce field-level null, default, secret, and scalar rules."""
    cases = []
    required_nullable = _descriptor()
    required_nullable["config_fields"][0]["nullable"] = True
    cases.append(required_nullable)
    secret_default = _descriptor()
    secret_default["config_fields"][1]["default"] = "secret"
    cases.append(secret_default)
    secret_allowed = _descriptor()
    secret_allowed["config_fields"][1]["allowed_values"] = ["secret"]
    cases.append(secret_allowed)
    duplicate_allowed = _descriptor()
    duplicate_allowed["config_fields"][0]["allowed_values"] = ["eu", "eu"]
    cases.append(duplicate_allowed)
    required_empty = _descriptor()
    required_empty["config_fields"][1]["default"] = ""
    cases.append(required_empty)
    required_allows_empty = _descriptor()
    required_allows_empty["config_fields"][1]["allowed_values"] = [""]
    required_allows_empty["config_fields"][1]["secret"] = False
    cases.append(required_allows_empty)
    wrong_number = _descriptor()
    wrong_number["config_fields"].append(
        _field("number", field_type="number", default="one"),
    )
    cases.append(wrong_number)

    for value in cases:
        with pytest.raises(DescriptorValidationError):
            _validate(value)


def test_condition_domain_must_be_finite_and_nonsecret() -> None:
    """Every declared condition has a finite supported scalar domain."""
    no_domain = _descriptor()
    no_domain["config_fields"][0]["allowed_values"] = []
    decimal_domain = _descriptor()
    decimal_domain["config_fields"][0]["type"] = "number"
    decimal_domain["config_fields"][0]["default"] = Decimal("1.5")
    decimal_domain["config_fields"][0]["allowed_values"] = [Decimal("1.5")]
    mismatch = _descriptor()
    mismatch["condition_fields"] = []

    for value in (no_domain, decimal_domain, mismatch):
        with pytest.raises(DescriptorValidationError):
            _validate(value)


def test_identity_reference_and_duplicate_name_rules() -> None:
    """Identity declarations reference fields and use each name once."""
    missing = _descriptor()
    missing["bot_identity_fields"][0]["name"] = "missing"
    duplicate = _descriptor()
    duplicate["bot_identity_fields"][0]["name"] = "bot_token"

    for value in (missing, duplicate):
        with pytest.raises(DescriptorValidationError):
            _validate(value)


def test_capability_and_ingress_combinations() -> None:
    """Ingress and exactly-once capability cross-rules are enforced."""
    ingress_missing = _descriptor()
    ingress_missing["ingress_owner"] = "runner_owned"
    ingress_unowned = _descriptor()
    ingress_unowned["capabilities"].append("ingress_endpoint")
    exactly_once = _descriptor()
    exactly_once["capabilities"].append("exactly-once-visible")
    unknown = _descriptor()
    unknown["capabilities"].append("future_capability")
    duplicate = _descriptor()
    duplicate["capabilities"].append("media")

    for value in (
        ingress_missing,
        ingress_unowned,
        exactly_once,
        unknown,
        duplicate,
    ):
        with pytest.raises(DescriptorValidationError):
            _validate(value)

    valid = _descriptor()
    valid["capabilities"].extend(
        ["server_side_idempotency", "exactly-once-visible"],
    )
    assert "exactly-once-visible" in _validate(valid).capabilities

    core_owned = _descriptor()
    core_owned["ingress_owner"] = "core_owned"
    core_owned["capabilities"].append("ingress_endpoint")
    assert _validate(core_owned).ingress_owner == "core_owned"

    runner_owned = _descriptor()
    runner_owned["ingress_owner"] = "runner_owned"
    runner_owned["capabilities"].append("ingress_endpoint")
    assert _validate(runner_owned).ingress_owner == "runner_owned"

    direct_session = _descriptor()
    direct_session["dispatch_mode"] = "direct_session"
    assert _validate(direct_session).dispatch_mode == "direct_session"


@pytest.mark.parametrize(
    "capability",
    [
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
    ],
)
def test_capability_registry_accepts_every_v1_value(capability: str) -> None:
    """Every frozen capability ID has a valid descriptor combination."""
    value = _descriptor()
    value["capabilities"] = [capability]
    if capability == "exactly-once-visible":
        value["capabilities"].append("server_side_idempotency")
    if capability == "ingress_endpoint":
        value["ingress_owner"] = "runner_owned"

    assert capability in _validate(value).capabilities


def test_target_and_environment_allowlist_rules() -> None:
    """Runner targets require registry membership and env names are closed."""
    no_target = _descriptor()
    no_target["supported_platform_tags"] = []
    unregistered = _descriptor()
    unregistered["supported_platform_tags"] = ["manylinux_2_17_aarch64"]
    bad_env = _descriptor()
    bad_env["environment_passthrough_allowlist"] = ["HTTPS_PROXY=secret"]

    for value in (no_target, unregistered, bad_env):
        with pytest.raises(DescriptorValidationError):
            _validate(value)


def test_in_process_requirement_and_entrypoint_rules() -> None:
    """In-process descriptors cannot declare an isolated environment."""
    value = _descriptor()
    value["process_mode"] = "in_process"
    value["entrypoint"]["scope"] = "core"
    with pytest.raises(DescriptorValidationError):
        _validate(value)

    value["isolated_requirements"] = []
    value["supported_python_abis"] = []
    value["supported_platform_tags"] = []
    assert _validate(value).entrypoint.scope == "core"


def test_legacy_plugin_accepts_noncanonical_historical_key() -> None:
    """Only the explicit legacy profile may preserve a historical key."""
    value = _descriptor()
    value.update(
        {
            "channel_key": "Legacy.Plugin Key",
            "source_kind": "plugin",
            "process_mode": "in_process",
            "label": "Legacy",
            "plugin_metadata": {
                "plugin_id": "legacy-owner",
                "version": "1.0",
                "artifact_sha256": "",
            },
            "isolated_requirements": [],
            "supported_python_abis": [],
            "supported_platform_tags": [],
        },
    )
    value["entrypoint"]["scope"] = "core"

    assert _validate(value).channel_key == "Legacy.Plugin Key"


def test_isolated_plugin_requires_canonical_artifact_digest() -> None:
    """An isolated Plugin carries a verified artifact identity."""
    value = _descriptor()
    value["source_kind"] = "plugin"
    plugin_metadata = {
        "plugin_id": "fixture-owner",
        "version": "1.0.0",
        "artifact_sha256": "a" * 64,
    }
    value["plugin_metadata"] = plugin_metadata

    descriptor = _validate(value)
    assert descriptor.plugin_metadata is not None
    assert descriptor.plugin_metadata.artifact_sha256 == "a" * 64

    plugin_metadata["artifact_sha256"] = ""
    with pytest.raises(DescriptorValidationError):
        _validate(value)


def test_requirement_policy_is_deliberately_not_classified_in_ch_0_002() -> (
    None
):
    """Core requirements receive syntax normalization, not package policy."""
    value = _descriptor()
    value["core_requirements"] = ["discord-py>=2", "Requests>=2"]

    descriptor = _validate(value)
    assert descriptor.core_requirements == (
        "discord-py>=2",
        "requests>=2",
    )


def test_descriptor_failure_has_stable_code_and_diagnostic_path() -> None:
    """The exception code is stable while local path remains diagnostic."""
    value = deepcopy(_descriptor())
    value["bot_identity_fields"][0]["name"] = "missing"

    with pytest.raises(DescriptorValidationError) as raised:
        _validate(value)

    assert raised.value.code == "descriptor_invalid"
    assert raised.value.path[:1] == ("bot_identity_fields",)
