# -*- coding: utf-8 -*-
"""Tests for CH-0-006 Runner bootstrap and log separation."""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time
from typing import Any
import venv

import pytest


BOOTSTRAP = (
    Path(__file__).parents[3]
    / "src"
    / "qwenpaw"
    / "channel_protocol"
    / "runner_bootstrap.py"
).resolve()
FIXTURE_CODE_ROOT = (
    Path(__file__).parents[2]
    / "fixtures"
    / "channel_isolation"
    / "bootstrap_code"
).resolve()


def _hash_code_root(code_root: Path) -> str:
    """Reproduce the bootstrap code artifact digest in the test harness."""
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


def _write_manifest(
    directory: Path,
    *,
    code_root: Path,
    qualname: str = "smoke",
    **overrides: Any,
) -> Path:
    """Write the closed task-local bootstrap manifest fixture."""
    manifest: dict[str, Any] = {
        "schema_version": 1,
        "channel_key": "feishu",
        "code_root_sha256": _hash_code_root(code_root),
        "entrypoint": {
            "module": "runner_entrypoint",
            "qualname": qualname,
        },
    }
    manifest.update(overrides)
    path = directory / "bootstrap-manifest.json"
    path.write_text(json.dumps(manifest), encoding="utf-8")
    return path.resolve()


def _command(*, code_root: Path, manifest: Path) -> list[str]:
    """Return the required absolute isolated bootstrap command."""
    return [
        sys.executable,
        "-I",
        str(BOOTSTRAP),
        "--code-root",
        str(code_root.resolve()),
        "--manifest",
        str(manifest.resolve()),
    ]


def _read_frame(data: bytes) -> dict[str, Any]:
    """Decode the one fixture protocol frame."""
    header, body = data.split(b"\r\n\r\n", 1)
    name, length = header.split(b":", 1)
    assert name.lower() == b"content-length"
    assert int(length.strip()) == len(body)
    value = json.loads(body.decode("utf-8"))
    assert isinstance(value, dict)
    return value


def test_rejects_non_isolated_python_before_channel_import(
    tmp_path: Path,
) -> None:
    """Missing -I fails before the fake Feishu SDK can be imported."""
    manifest = _write_manifest(tmp_path, code_root=FIXTURE_CODE_ROOT)
    result = subprocess.run(
        _command(code_root=FIXTURE_CODE_ROOT, manifest=manifest)[0:1]
        + _command(code_root=FIXTURE_CODE_ROOT, manifest=manifest)[2:],
        capture_output=True,
        check=False,
        timeout=5,
    )

    assert result.returncode == 2
    assert result.stdout == b""
    assert b'"error":"PYTHON_NOT_ISOLATED"' in result.stderr
    assert b"feishu-sdk" not in result.stderr


@pytest.mark.parametrize(
    ("code_root", "manifest_value", "reason"),
    [
        (Path("relative-root"), None, "INVALID_BOOTSTRAP_ARGUMENT"),
        (None, {"schema_version": 2}, "MANIFEST_INVALID"),
        (None, {"code_root_sha256": "0" * 64}, "CODE_ROOT_MISMATCH"),
        (
            None,
            {"entrypoint": {"module": "json", "qualname": "dumps"}},
            "ENTRYPOINT_OUTSIDE_CODE_ROOT",
        ),
    ],
)
def test_rejects_code_root_and_manifest_before_channel_import(
    tmp_path: Path,
    code_root: Path | None,
    manifest_value: dict[str, Any] | None,
    reason: str,
) -> None:
    """Invalid trust inputs fail without importing the Channel SDK."""
    if manifest_value is None:
        manifest = _write_manifest(tmp_path, code_root=FIXTURE_CODE_ROOT)
    else:
        manifest = _write_manifest(
            tmp_path,
            code_root=FIXTURE_CODE_ROOT,
            **manifest_value,
        )
    command = _command(code_root=FIXTURE_CODE_ROOT, manifest=manifest)
    if code_root is not None:
        command[4] = str(code_root)
    result = subprocess.run(
        command,
        capture_output=True,
        check=False,
        timeout=5,
    )

    assert result.returncode == 2
    assert result.stdout == b""
    assert f'"error":"{reason}"'.encode() in result.stderr
    assert b"feishu-sdk" not in result.stderr


def test_isolated_bootstrap_uses_only_explicit_code_root_and_splits_logs(
    tmp_path: Path,
) -> None:
    """SDK, print, logging, and FD 1 output never enter the protocol."""
    manifest = _write_manifest(tmp_path, code_root=FIXTURE_CODE_ROOT)
    ambient_root = tmp_path / "ambient"
    ambient_root.mkdir()
    (ambient_root / "runner_entrypoint.py").write_text(
        "raise RuntimeError('ambient import used')\n",
        encoding="utf-8",
    )
    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(ambient_root)
    environment["PYTHONHOME"] = str(tmp_path / "missing-python-home")
    environment["QWENPAW_BOOTSTRAP_LEAK"] = "secret-leak"

    result = subprocess.run(
        _command(code_root=FIXTURE_CODE_ROOT, manifest=manifest),
        cwd=ambient_root,
        env=environment,
        capture_output=True,
        check=False,
        timeout=5,
    )

    assert result.returncode == 0
    assert result.stdout.startswith(b"Content-Length: ")
    assert result.stdout.count(b"Content-Length: ") == 1
    assert b"feishu-sdk" not in result.stdout
    assert b"runner-print" not in result.stdout
    assert b"runner-logging" not in result.stdout
    assert b"runner-fd1" not in result.stdout
    payload = _read_frame(result.stdout)
    assert payload["sdk_ready"] is True
    assert payload["pythonpath"] is None
    assert payload["pythonhome"] is None
    assert payload["unexpected_env"] is None
    assert payload["isolated"] is True
    assert payload["ignore_environment"] is True
    assert payload["no_user_site"] is True
    assert Path(payload["entrypoint_file"]) == (
        FIXTURE_CODE_ROOT / "runner_entrypoint.py"
    )
    assert payload["sys_path"][0] == str(FIXTURE_CODE_ROOT)
    assert str(ambient_root) not in payload["sys_path"]
    assert str(BOOTSTRAP.parents[2]) not in payload["sys_path"]
    assert b"feishu-sdk-print" in result.stderr
    assert b"feishu-sdk-fd1" in result.stderr
    assert b"runner-print" in result.stderr
    assert b"runner-logging" in result.stderr
    assert b"runner-fd1" in result.stderr


@pytest.mark.asyncio
async def test_continuous_stderr_drain_prevents_log_backpressure(
    tmp_path: Path,
) -> None:
    """A draining parent lets high-volume logs and protocol both finish."""
    manifest = _write_manifest(
        tmp_path,
        code_root=FIXTURE_CODE_ROOT,
        qualname="noisy",
    )
    process = await asyncio.create_subprocess_exec(
        *_command(code_root=FIXTURE_CODE_ROOT, manifest=manifest),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await asyncio.wait_for(
        process.communicate(),
        timeout=10,
    )

    assert process.returncode == 0
    assert _read_frame(stdout) == {"logs_written": 16 * 1024 * 1024}
    assert len(stderr) >= 16 * 1024 * 1024


@pytest.mark.asyncio
async def test_undrained_stderr_applies_backpressure(
    tmp_path: Path,
) -> None:
    """Without a log reader, the Runner blocks before protocol progress."""
    manifest = _write_manifest(
        tmp_path,
        code_root=FIXTURE_CODE_ROOT,
        qualname="noisy",
    )
    process = await asyncio.create_subprocess_exec(
        *_command(code_root=FIXTURE_CODE_ROOT, manifest=manifest),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    assert process.stdout is not None
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(process.stdout.read(1), timeout=0.2)

    process.kill()
    await asyncio.wait_for(process.communicate(), timeout=5)


def test_descendant_cannot_inherit_private_protocol_handle(
    tmp_path: Path,
) -> None:
    """Uncontrolled descendants cannot keep protocol stdout alive."""
    manifest = _write_manifest(
        tmp_path,
        code_root=FIXTURE_CODE_ROOT,
        qualname="spawn_descendant",
    )
    start = time.monotonic()
    result = subprocess.run(
        _command(code_root=FIXTURE_CODE_ROOT, manifest=manifest),
        capture_output=True,
        check=False,
        timeout=1.0,
    )
    elapsed = time.monotonic() - start

    assert result.returncode == 0
    payload = _read_frame(result.stdout)
    assert isinstance(payload["descendant_pid"], int)
    assert elapsed < 1.0


def test_frozen_style_bootstrap_artifact_needs_no_installed_qwenpaw(
    tmp_path: Path,
) -> None:
    """A copied read-only bootstrap works without importable QwenPaw."""
    bootstrap_copy = tmp_path / "application" / "runner_bootstrap.py"
    bootstrap_copy.parent.mkdir()
    shutil.copy2(BOOTSTRAP, bootstrap_copy)
    bootstrap_copy.chmod(0o444)
    manifest = _write_manifest(tmp_path, code_root=FIXTURE_CODE_ROOT)
    environment_root = tmp_path / "dependency-environment"
    venv.EnvBuilder(with_pip=False).create(environment_root)
    interpreter = environment_root / (
        "Scripts/python.exe" if os.name == "nt" else "bin/python"
    )
    environment = {
        name: value
        for name, value in os.environ.items()
        if name
        in {
            "COMSPEC",
            "PATH",
            "PATHEXT",
            "SYSTEMDRIVE",
            "SYSTEMROOT",
            "TEMP",
            "TMP",
            "TMPDIR",
            "WINDIR",
        }
    }
    result = subprocess.run(
        [
            str(interpreter),
            "-I",
            str(bootstrap_copy.resolve()),
            "--code-root",
            str(FIXTURE_CODE_ROOT),
            "--manifest",
            str(manifest),
        ],
        env=environment,
        cwd=tmp_path,
        capture_output=True,
        check=False,
        timeout=5,
    )

    assert result.returncode == 0
    assert _read_frame(result.stdout)["sdk_ready"] is True
    assert b"Traceback" not in result.stderr
