# -*- coding: utf-8 -*-
"""Tests for CH-0-002 Channel identity and directory-key models."""

from __future__ import annotations

from decimal import Decimal

import pytest

from qwenpaw.channel_protocol import (
    DescriptorValidationError,
    DirectoryIdentity,
    EnvironmentIdentity,
    EnvironmentSpecIdentity,
    InstallationIdentity,
    InstanceIdentity,
    condition_set_sha256,
    current_python_abi,
    dir_key,
    validate_channel_key,
    validate_platform_tag,
    validate_python_abi,
)


EMPTY_CONDITION_DIGEST = (
    "dc4e5b494b66d21b82ac92cf406a37d007c80b7d5b986203d5b8d3094d1d051f"
)
LOCK_DIGEST = "0" * 64
PLATFORMS = {
    "macosx_11_0_arm64",
    "manylinux_2_28_x86_64",
    "win_amd64",
}


def test_instance_id_fixed_vector_and_single_instance_ownership() -> None:
    """The default vector is stable and Agent identity scopes instances."""
    default = InstanceIdentity.create(
        agent_id="default",
        channel_key="feishu",
    )
    other = InstanceIdentity.create(
        agent_id="Default",
        channel_key="feishu",
    )

    assert default.instance_id == (
        "chi1_00aaff7d5548053ae2a51a6bc5e64a3b2e5198a311dcd98be9916162b3e63b17"
    )
    assert default == InstanceIdentity.create(
        agent_id="default",
        channel_key="feishu",
    )
    assert default.instance_id != other.instance_id
    state_by_instance = {
        default.instance_id: "default-state",
        other.instance_id: "other-state",
    }
    assert len(state_by_instance) == 2
    assert (
        InstanceIdentity.create(
            agent_id="default",
            channel_key="telegram",
        ).instance_id
        != default.instance_id
    )
    assert (
        InstanceIdentity.parse(
            agent_id="default",
            channel_key="feishu",
            instance_id=default.instance_id,
        )
        == default
    )


@pytest.mark.parametrize(
    "value",
    ["", "UPPER", " leading", "trailing-", "bad.key", "a" * 65],
)
def test_channel_key_rejects_noncanonical_input(value: str) -> None:
    """V1 keys are rejected rather than trimmed or case-folded."""
    with pytest.raises(DescriptorValidationError):
        validate_channel_key(value)


@pytest.mark.parametrize(
    "value",
    ["cp311-cp311", "cp312-cp312", "cp313-cp313"],
)
def test_python_abi_matrix(value: str) -> None:
    """The required Python ABI examples are valid canonical pairs."""
    assert validate_python_abi(value) == value


def test_current_python_abi_is_canonical() -> None:
    """Runtime ABI discovery uses the first concrete sys_tags ABI."""
    assert validate_python_abi(current_python_abi()) == current_python_abi()


@pytest.mark.parametrize("value", sorted(PLATFORMS))
def test_platform_tag_registry_matrix(value: str) -> None:
    """Target-platform shapes require explicit registry membership."""
    assert (
        validate_platform_tag(
            value,
            allowed_platform_tags=PLATFORMS,
        )
        == value
    )


@pytest.mark.parametrize(
    "value",
    ["windows", "darwin", "macos", "x86_64", "linux-x86_64", "WIN_AMD64"],
)
def test_platform_tag_rejects_alias_or_noncanonical_input(value: str) -> None:
    """Product aliases and unregistered tags cannot identify environments."""
    with pytest.raises(DescriptorValidationError):
        validate_platform_tag(
            value,
            allowed_platform_tags={value, *PLATFORMS},
        )


def test_platform_tag_rejects_canonical_nonmember() -> None:
    """Canonical syntax cannot bypass release registry membership."""
    with pytest.raises(DescriptorValidationError):
        validate_platform_tag(
            "manylinux_2_17_aarch64",
            allowed_platform_tags=PLATFORMS,
        )


def test_environment_spec_fixed_vector_and_change_dimensions() -> None:
    """Every semantic environment input changes the deterministic spec ID."""
    base = EnvironmentSpecIdentity.create(
        channel_key="feishu",
        lock_sha256=LOCK_DIGEST,
        python_abi="cp313-cp313",
        platform_tag="macosx_11_0_arm64",
        condition_set={},
        allowed_platform_tags=PLATFORMS,
    )

    assert base.condition_set_sha256 == EMPTY_CONDITION_DIGEST
    assert base.environment_spec_id == (
        "ches1_5c705f48418202bdafc20672ae0ccb7c1b178a389389ee6bbd9a8ec7"
        "c59264c1"
    )
    variants = {
        EnvironmentSpecIdentity.create(
            channel_key="telegram",
            lock_sha256=LOCK_DIGEST,
            python_abi="cp313-cp313",
            platform_tag="macosx_11_0_arm64",
            condition_set={},
            allowed_platform_tags=PLATFORMS,
        ).environment_spec_id,
        EnvironmentSpecIdentity.create(
            channel_key="feishu",
            lock_sha256="1" * 64,
            python_abi="cp313-cp313",
            platform_tag="macosx_11_0_arm64",
            condition_set={},
            allowed_platform_tags=PLATFORMS,
        ).environment_spec_id,
        EnvironmentSpecIdentity.create(
            channel_key="feishu",
            lock_sha256=LOCK_DIGEST,
            python_abi="cp312-cp312",
            platform_tag="macosx_11_0_arm64",
            condition_set={},
            allowed_platform_tags=PLATFORMS,
        ).environment_spec_id,
        EnvironmentSpecIdentity.create(
            channel_key="feishu",
            lock_sha256=LOCK_DIGEST,
            python_abi="cp313-cp313",
            platform_tag="win_amd64",
            condition_set={},
            allowed_platform_tags=PLATFORMS,
        ).environment_spec_id,
        EnvironmentSpecIdentity.create(
            channel_key="feishu",
            lock_sha256=LOCK_DIGEST,
            python_abi="cp313-cp313",
            platform_tag="macosx_11_0_arm64",
            condition_set={"region": "eu"},
            allowed_platform_tags=PLATFORMS,
        ).environment_spec_id,
    }
    assert base.environment_spec_id not in variants
    assert len(variants) == 5


def test_condition_set_rejects_unbounded_value_types() -> None:
    """Condition digests accept only the finite v1 scalar value types."""
    assert condition_set_sha256({}) == EMPTY_CONDITION_DIGEST
    with pytest.raises(DescriptorValidationError):
        condition_set_sha256({"decimal": Decimal("1.5")})
    with pytest.raises(DescriptorValidationError):
        condition_set_sha256({"array": ["value"]})


def test_installation_repair_creates_distinct_immutable_environment_ids() -> (
    None
):
    """Repair keeps the spec and changes its 128-bit installation identity."""
    spec_id = EnvironmentSpecIdentity.create(
        channel_key="feishu",
        lock_sha256=LOCK_DIGEST,
        python_abi="cp313-cp313",
        platform_tag="macosx_11_0_arm64",
        condition_set={},
        allowed_platform_tags=PLATFORMS,
    ).environment_spec_id
    first_install = InstallationIdentity.create()
    second_install = InstallationIdentity.create()
    first = EnvironmentIdentity.create(
        environment_spec_id=spec_id,
        installation=first_install,
    )
    second = EnvironmentIdentity.create(
        environment_spec_id=spec_id,
        installation=second_install,
    )

    assert first.environment_spec_id == second.environment_spec_id
    assert first.installation_id != second.installation_id
    assert first.environment_id != second.environment_id
    assert EnvironmentIdentity.parse(first.environment_id) == first
    assert InstallationIdentity.parse(first.installation_id) == first_install


def test_directory_identity_detects_manifest_collision() -> None:
    """Never trust a short directory key without the full manifest ID."""
    logical_id = "chi1_" + "a" * 64
    directory_key = dir_key(logical_id)

    assert directory_key.startswith("dir1_")
    assert len(directory_key) == 37
    assert (
        DirectoryIdentity.validate(
            logical_id=logical_id,
            directory_key=directory_key,
            manifest_logical_id=logical_id,
        ).logical_id
        == logical_id
    )
    with pytest.raises(DescriptorValidationError):
        DirectoryIdentity.validate(
            logical_id=logical_id,
            directory_key=directory_key,
            manifest_logical_id="chi1_" + "b" * 64,
        )


def test_directory_key_hashes_exact_logical_id_utf8_bytes() -> None:
    """Directory keys do not normalize or reinterpret logical ID bytes."""
    assert dir_key("é") != dir_key("e\u0301")
    with pytest.raises(DescriptorValidationError):
        dir_key("\ud800")


def test_identifier_failures_share_stable_error_code() -> None:
    """Pure identity validation exposes the descriptor_invalid code."""
    with pytest.raises(DescriptorValidationError) as raised:
        InstanceIdentity.create(agent_id="", channel_key="feishu")

    assert raised.value.code == "descriptor_invalid"
