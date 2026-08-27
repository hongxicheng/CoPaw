# -*- coding: utf-8 -*-
"""Build task-local Channel artifacts for subprocess Runner proofs."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
import shutil
import sys
from typing import Any


_REPOSITORY_ROOT = Path(__file__).parents[3]
_SOURCE_ROOT = _REPOSITORY_ROOT / "src" / "qwenpaw"
_BOOTSTRAP = (
    _SOURCE_ROOT / "channel_protocol" / "runner_bootstrap.py"
).resolve()
_DESCRIPTOR_TEMPLATE = (
    Path(__file__).with_name("bootstrap_code") / "channel.json"
)
_ALLOWED_PLATFORM_TAGS = [
    "macosx_11_0_arm64",
    "manylinux_2_28_x86_64",
    "win_amd64",
]


def _hash_code_root(code_root: Path) -> str:
    """Hash one fixture artifact with the bootstrap algorithm."""
    digest = hashlib.sha256()
    files = sorted(
        path
        for path in code_root.rglob("*")
        if path.is_file() and "__pycache__" not in path.parts
    )
    for path in files:
        relative = path.relative_to(code_root).as_posix().encode("utf-8")
        digest.update(len(relative).to_bytes(8, "big"))
        digest.update(relative)
        content = path.read_bytes()
        digest.update(len(content).to_bytes(8, "big"))
        digest.update(content)
    return digest.hexdigest()


class RunnerArtifact:
    """Hold one validated fixture artifact and its launch manifest."""

    def __init__(
        self,
        *,
        code_root: Path,
        manifest_path: Path,
        source_revision: str,
    ) -> None:
        self.code_root = code_root
        self.manifest_path = manifest_path
        self.source_revision = source_revision

    def command(self, identity: dict[str, Any]) -> tuple[str, ...]:
        """Write non-source identity and return the isolated command."""
        identity_path = self.manifest_path.with_name(
            f"launch-{identity['instance_id']}.json",
        )
        identity_path.write_text(
            json.dumps(identity, separators=(",", ":")),
            encoding="utf-8",
        )
        return (
            sys.executable,
            "-I",
            str(_BOOTSTRAP),
            "--code-root",
            str(self.code_root),
            "--manifest",
            str(self.manifest_path),
            "--launch-identity",
            str(identity_path.resolve()),
        )


def build_runner_artifact(
    directory: Path,
    *,
    channel_key: str,
    runner_source: Path,
    entrypoint: str,
    capabilities: tuple[str, ...],
    ingress_owner: str,
) -> RunnerArtifact:
    """Copy one Channel and fixture entrypoint into an isolated root."""
    code_root = directory / f"{channel_key}-artifact"
    channel_target = code_root / "qwenpaw" / "app" / "channels" / channel_key
    channel_target.parent.mkdir(parents=True)
    ignore = shutil.ignore_patterns("__pycache__", "*.pyc")
    shutil.copytree(
        _SOURCE_ROOT / "app" / "channels" / channel_key,
        channel_target,
        ignore=ignore,
    )
    shutil.copy2(
        _SOURCE_ROOT / "app" / "__init__.py",
        channel_target.parents[1],
    )
    shutil.copy2(
        _SOURCE_ROOT / "app" / "channels" / "__init__.py",
        channel_target.parent,
    )
    shutil.copy2(
        _SOURCE_ROOT / "app" / "channels" / "utils.py",
        channel_target.parent,
    )
    shutil.copy2(runner_source, code_root / "fixture_runner.py")
    descriptor = json.loads(
        _DESCRIPTOR_TEMPLATE.read_text(encoding="utf-8"),
    )
    descriptor["channel_key"] = channel_key
    descriptor["ingress_owner"] = ingress_owner
    descriptor["entrypoint"] = {
        "scope": "runner",
        "module": "fixture_runner",
        "qualname": entrypoint,
    }
    descriptor["capabilities"] = sorted(capabilities)
    descriptor["environment_passthrough_allowlist"] = []
    descriptor_path = code_root / "channel.json"
    descriptor_path.write_text(
        json.dumps(descriptor, separators=(",", ":")),
        encoding="utf-8",
    )
    source_revision = _hash_code_root(code_root)
    manifest = {
        "schema_version": 1,
        "source_revision": source_revision,
        "descriptor_path": str(descriptor_path.resolve()),
        "descriptor_sha256": hashlib.sha256(
            descriptor_path.read_bytes(),
        ).hexdigest(),
        "allowed_platform_tags": _ALLOWED_PLATFORM_TAGS,
    }
    manifest_path = directory / f"{channel_key}-manifest.json"
    manifest_path.write_text(
        json.dumps(manifest, separators=(",", ":")),
        encoding="utf-8",
    )
    return RunnerArtifact(
        code_root=code_root.resolve(),
        manifest_path=manifest_path.resolve(),
        source_revision=source_revision,
    )
