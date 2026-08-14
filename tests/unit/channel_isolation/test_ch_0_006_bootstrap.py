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
import threading
import time
from typing import Any
import venv

import packaging
import pytest

from qwenpaw.channel_protocol import (
    FrameLimitError,
    FrameTimeoutError,
    FrameWriteError,
    FramedTransport,
    FramingLimits,
    encode_frame,
)
from qwenpaw.channel_protocol.runner_bootstrap import (
    _open_protocol_transport,
)


BOOTSTRAP = (
    Path(__file__).parents[3]
    / "src"
    / "qwenpaw"
    / "channel_protocol"
    / "runner_bootstrap.py"
).resolve()
PROTOCOL_SOURCE_ROOT = BOOTSTRAP.parent
FIXTURE_SOURCE_ROOT = (
    Path(__file__).parents[2]
    / "fixtures"
    / "channel_isolation"
    / "bootstrap_code"
).resolve()
PROTOCOL_OUTPUT_PROBE = FIXTURE_SOURCE_ROOT / "protocol_output_probe.py"
ALLOWED_PLATFORM_TAGS = [
    "macosx_11_0_arm64",
    "manylinux_2_28_x86_64",
    "win_amd64",
]


def _copy_code_root(directory: Path) -> Path:
    """Build one explicit trusted source root for the Runner fixture."""
    code_root = directory / "code-root"
    ignore = shutil.ignore_patterns("__pycache__", "*.pyc")
    shutil.copytree(FIXTURE_SOURCE_ROOT, code_root, ignore=ignore)
    protocol_target = code_root / "qwenpaw" / "channel_protocol"
    protocol_target.parent.mkdir(parents=True)
    shutil.copytree(
        PROTOCOL_SOURCE_ROOT,
        protocol_target,
        ignore=ignore,
    )
    return code_root.resolve()


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


def _descriptor(code_root: Path) -> dict[str, Any]:
    """Read the mutable descriptor fixture from one copied source root."""
    value = json.loads(
        (code_root / "channel.json").read_text(encoding="utf-8"),
    )
    assert isinstance(value, dict)
    return value


def _write_descriptor(code_root: Path, value: object) -> Path:
    """Replace one copied descriptor before its manifest is generated."""
    path = code_root / "channel.json"
    path.write_text(json.dumps(value), encoding="utf-8")
    return path.resolve()


def _write_manifest(
    directory: Path,
    *,
    code_root: Path,
    **overrides: Any,
) -> Path:
    """Write the closed task-local integrity manifest fixture."""
    descriptor_path = code_root / "channel.json"
    manifest: dict[str, Any] = {
        "schema_version": 1,
        "code_root_sha256": _hash_code_root(code_root),
        "descriptor_path": str(descriptor_path.resolve()),
        "descriptor_sha256": hashlib.sha256(
            descriptor_path.read_bytes(),
        ).hexdigest(),
        "allowed_platform_tags": ALLOWED_PLATFORM_TAGS,
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


def _environment(result_path: Path) -> dict[str, str]:
    """Build an inherited environment with declared and ambient values."""
    environment = os.environ.copy()
    environment["QWENPAW_FIXTURE_RESULT"] = str(result_path)
    environment["HTTPS_PROXY"] = "http://proxy.example:8443"
    environment["TELEGRAM_HTTP_PROXY"] = "http://telegram.example:8080"
    environment["SSL_CERT_FILE"] = str(result_path.parent / "ca.pem")
    environment["QWENPAW_BOOTSTRAP_LEAK"] = "secret-leak"
    environment["PYTHONPATH"] = str(result_path.parent / "ambient")
    environment["PYTHONHOME"] = str(result_path.parent / "missing-home")
    return environment


def _read_result(path: Path) -> dict[str, Any]:
    """Read one driver construction result."""
    value = json.loads(path.read_text(encoding="utf-8"))
    assert isinstance(value, dict)
    return value


def _decode_frames(data: bytes) -> list[str]:
    """Decode complete frames emitted by FramedTransport."""
    messages: list[str] = []
    remaining = data
    while remaining:
        header, separator, body = remaining.partition(b"\r\n\r\n")
        assert separator
        name, value = header.split(b":", 1)
        assert name.lower() == b"content-length"
        length = int(value.strip())
        payload = body[:length]
        assert len(payload) == length
        messages.append(payload.decode("utf-8"))
        remaining = body[length:]
    return messages


def _read_pipe(fd: int, expected_bytes: int) -> bytes:
    """Drain an OS pipe until every expected frame byte arrives."""
    data = bytearray()
    try:
        while len(data) < expected_bytes:
            chunk = os.read(fd, expected_bytes - len(data))
            if not chunk:
                break
            data.extend(chunk)
    finally:
        os.close(fd)
    return bytes(data)


class _PartialWriteHandle:
    """Record bytes while accepting only a small prefix per write."""

    def __init__(self) -> None:
        self.data = bytearray()
        self.closed = False

    def write(self, data: bytes) -> int:
        """Accept at most three bytes to exercise complete writes."""
        if self.closed:
            raise BrokenPipeError("handle is closed")
        accepted = min(3, len(data))
        self.data.extend(data[:accepted])
        return accepted

    def close(self) -> None:
        """Mark the fake handle closed."""
        self.closed = True


class _BrokenWriteHandle:
    """Fail every synchronous pipe write."""

    def write(self, _data: bytes) -> int:
        """Raise the OS error exposed by a closed peer."""
        raise BrokenPipeError("peer closed")

    def close(self) -> None:
        """Match the synchronous handle interface."""


class _BlockedWriteHandle:
    """Block a synchronous write until close interrupts it."""

    def __init__(self) -> None:
        self.started = threading.Event()
        self.release = threading.Event()
        self.closed = False

    def write(self, _data: bytes) -> int:
        """Wait for close, then expose the interrupted pipe write."""
        self.started.set()
        self.release.wait(timeout=1)
        if self.closed:
            raise BrokenPipeError("handle is closed")
        return 0

    def close(self) -> None:
        """Interrupt the blocked fake write."""
        self.closed = True
        self.release.set()


def _set_entrypoint(
    code_root: Path,
    *,
    module: str = "runner_entrypoint",
    qualname: str,
) -> None:
    """Change only the copied descriptor entrypoint."""
    descriptor = _descriptor(code_root)
    descriptor["entrypoint"]["module"] = module
    descriptor["entrypoint"]["qualname"] = qualname
    _write_descriptor(code_root, descriptor)


def test_rejects_non_isolated_python_before_channel_import(
    tmp_path: Path,
) -> None:
    """Missing -I fails before the fake Feishu SDK can be imported."""
    code_root = _copy_code_root(tmp_path)
    manifest = _write_manifest(tmp_path, code_root=code_root)
    command = _command(code_root=code_root, manifest=manifest)
    environment = _environment(tmp_path / "result.json")
    environment.pop("PYTHONHOME")
    result = subprocess.run(
        command[0:1] + command[2:],
        env=environment,
        capture_output=True,
        check=False,
        timeout=5,
    )

    assert result.returncode == 2
    assert result.stdout == b""
    assert b'"error":"PYTHON_NOT_ISOLATED"' in result.stderr
    assert b"feishu-sdk" not in result.stderr


@pytest.mark.parametrize(
    ("manifest_value", "reason"),
    [
        ({"schema_version": 2}, "MANIFEST_INVALID"),
        ({"code_root_sha256": "0" * 64}, "CODE_ROOT_MISMATCH"),
        ({"descriptor_sha256": "0" * 64}, "DESCRIPTOR_MISMATCH"),
    ],
)
def test_rejects_invalid_integrity_manifest_before_channel_import(
    tmp_path: Path,
    manifest_value: dict[str, Any],
    reason: str,
) -> None:
    """Invalid trust inputs fail without importing the Channel SDK."""
    code_root = _copy_code_root(tmp_path)
    manifest = _write_manifest(
        tmp_path,
        code_root=code_root,
        **manifest_value,
    )
    result = subprocess.run(
        _command(code_root=code_root, manifest=manifest),
        env=_environment(tmp_path / "result.json"),
        capture_output=True,
        check=False,
        timeout=5,
    )

    assert result.returncode == 2
    assert result.stdout == b""
    assert f'"error":"{reason}"'.encode() in result.stderr
    assert b"feishu-sdk" not in result.stderr


@pytest.mark.parametrize("mode", ["invalid_scope", "core_process"])
def test_rejects_non_runner_descriptor_before_channel_import(
    tmp_path: Path,
    mode: str,
) -> None:
    """Descriptor scope and process mode are validated before import."""
    code_root = _copy_code_root(tmp_path)
    descriptor = _descriptor(code_root)
    descriptor["entrypoint"]["scope"] = "core"
    expected = "DESCRIPTOR_INVALID"
    if mode == "core_process":
        descriptor["process_mode"] = "in_process"
        expected = "DESCRIPTOR_NOT_RUNNER"
    _write_descriptor(code_root, descriptor)
    manifest = _write_manifest(tmp_path, code_root=code_root)
    result_path = tmp_path / "result.json"
    result = subprocess.run(
        _command(code_root=code_root, manifest=manifest),
        env=_environment(result_path),
        capture_output=True,
        check=False,
        timeout=5,
    )

    assert result.returncode == 2
    assert result.stdout == b""
    assert f'"error":"{expected}"'.encode() in result.stderr
    assert b"feishu-sdk" not in result.stderr
    assert not result_path.exists()


def test_descriptor_entrypoint_must_resolve_inside_code_root(
    tmp_path: Path,
) -> None:
    """A validated descriptor cannot import an external module."""
    code_root = _copy_code_root(tmp_path)
    _set_entrypoint(code_root, module="json", qualname="JSONDecoder")
    manifest = _write_manifest(tmp_path, code_root=code_root)
    result = subprocess.run(
        _command(code_root=code_root, manifest=manifest),
        env=_environment(tmp_path / "result.json"),
        capture_output=True,
        check=False,
        timeout=5,
    )

    assert result.returncode == 2
    assert result.stdout == b""
    assert b'"error":"ENTRYPOINT_OUTSIDE_CODE_ROOT"' in result.stderr
    assert b"feishu-sdk" not in result.stderr


def test_descriptor_constructs_driver_and_controls_environment(
    tmp_path: Path,
) -> None:
    """The descriptor alone selects the driver and passthrough variables."""
    code_root = _copy_code_root(tmp_path)
    manifest = _write_manifest(tmp_path, code_root=code_root)
    ambient_root = tmp_path / "ambient"
    ambient_root.mkdir()
    (ambient_root / "runner_entrypoint.py").write_text(
        "raise RuntimeError('ambient import used')\n",
        encoding="utf-8",
    )
    result_path = tmp_path / "result.json"

    result = subprocess.run(
        _command(code_root=code_root, manifest=manifest),
        cwd=ambient_root,
        env=_environment(result_path),
        capture_output=True,
        check=False,
        timeout=5,
    )

    assert result.returncode == 0
    assert result.stdout == b""
    payload = _read_result(result_path)
    assert payload["driver_constructed"] is True
    assert payload["sdk_ready"] is True
    assert payload["pythonpath"] is None
    assert payload["pythonhome"] is None
    assert payload["unexpected_env"] is None
    assert payload["https_proxy"] == "http://proxy.example:8443"
    assert payload["telegram_proxy"] == "http://telegram.example:8080"
    assert payload["ssl_cert_file"] == str(tmp_path / "ca.pem")
    assert payload["isolated"] is True
    assert payload["ignore_environment"] is True
    assert payload["no_user_site"] is True
    assert Path(payload["entrypoint_file"]) == (
        code_root / "runner_entrypoint.py"
    )
    assert Path(payload["descriptor_file"]) == (
        code_root / "qwenpaw" / "channel_protocol" / "descriptor.py"
    )
    assert payload["sys_path"][0] == str(code_root)
    assert str(ambient_root) not in payload["sys_path"]
    assert str(BOOTSTRAP.parents[2]) not in payload["sys_path"]
    assert b"feishu-sdk-print" in result.stderr
    assert b"feishu-sdk-fd1" in result.stderr
    assert b"runner-print" in result.stderr
    assert b"runner-logging" in result.stderr
    assert b"runner-fd1" in result.stderr


def test_private_protocol_handle_emits_only_framed_output() -> None:
    """Production-order stdout isolation keeps frames and logs separate."""
    result = subprocess.run(
        [sys.executable, str(PROTOCOL_OUTPUT_PROBE)],
        cwd=FIXTURE_SOURCE_ROOT,
        capture_output=True,
        check=False,
        timeout=5,
    )

    assert result.returncode == 0
    assert _decode_frames(result.stdout) == ['{"probe":"ready"}']
    assert b"feishu-sdk-print" in result.stderr
    assert b"feishu-sdk-fd1" in result.stderr
    assert b"probe-print" in result.stderr
    assert b"probe-fd1" in result.stderr


def test_rejects_legacy_callback_entrypoint_without_calling_it(
    tmp_path: Path,
) -> None:
    """The removed callback-to-int contract cannot masquerade as a driver."""
    code_root = _copy_code_root(tmp_path)
    _set_entrypoint(code_root, qualname="legacy_callback")
    manifest = _write_manifest(tmp_path, code_root=code_root)
    result_path = tmp_path / "result.json"
    result = subprocess.run(
        _command(code_root=code_root, manifest=manifest),
        env=_environment(result_path),
        capture_output=True,
        check=False,
        timeout=5,
    )

    assert result.returncode == 2
    assert result.stdout == b""
    assert b'"error":"ENTRYPOINT_INVALID"' in result.stderr
    assert not result_path.exists()


@pytest.mark.asyncio
async def test_continuous_stderr_drain_prevents_log_backpressure(
    tmp_path: Path,
) -> None:
    """A draining parent lets high-volume driver logs finish."""
    code_root = _copy_code_root(tmp_path)
    _set_entrypoint(code_root, qualname="NoisyDriver")
    manifest = _write_manifest(tmp_path, code_root=code_root)
    result_path = tmp_path / "result.json"
    process = await asyncio.create_subprocess_exec(
        *_command(code_root=code_root, manifest=manifest),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        env=_environment(result_path),
    )
    stdout, stderr = await asyncio.wait_for(
        process.communicate(),
        timeout=10,
    )

    assert process.returncode == 0
    assert stdout == b""
    assert _read_result(result_path) == {"logs_written": 16 * 1024 * 1024}
    assert len(stderr) >= 16 * 1024 * 1024


@pytest.mark.asyncio
async def test_undrained_stderr_applies_backpressure(
    tmp_path: Path,
) -> None:
    """Without a log reader, the Runner blocks before construction ends."""
    code_root = _copy_code_root(tmp_path)
    _set_entrypoint(code_root, qualname="NoisyDriver")
    manifest = _write_manifest(tmp_path, code_root=code_root)
    result_path = tmp_path / "result.json"
    process = await asyncio.create_subprocess_exec(
        *_command(code_root=code_root, manifest=manifest),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        env=_environment(result_path),
    )
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(process.wait(), timeout=0.2)

    assert not result_path.exists()
    process.kill()
    await asyncio.wait_for(process.communicate(), timeout=5)


def test_descendant_inherits_fd1_but_not_private_protocol_handle(
    tmp_path: Path,
) -> None:
    """A descendant can log without keeping protocol stdout alive."""
    code_root = _copy_code_root(tmp_path)
    _set_entrypoint(code_root, qualname="DescendantDriver")
    manifest = _write_manifest(tmp_path, code_root=code_root)
    result_path = tmp_path / "result.json"
    start = time.monotonic()
    result = subprocess.run(
        _command(code_root=code_root, manifest=manifest),
        env=_environment(result_path),
        capture_output=True,
        check=False,
        timeout=1.0,
    )
    elapsed = time.monotonic() - start

    assert result.returncode == 0
    assert result.stdout == b""
    payload = _read_result(result_path)
    assert isinstance(payload["descendant_pid"], int)
    assert b"descendant-fd1" in result.stderr
    assert elapsed < 1.0


def _copy_packaging_to_environment(
    interpreter: Path,
) -> None:
    """Install only the Protocol SDK's third-party parser dependency."""
    purelib = subprocess.check_output(
        [
            str(interpreter),
            "-I",
            "-c",
            "import sysconfig; print(sysconfig.get_paths()['purelib'])",
        ],
        text=True,
    ).strip()
    source = Path(packaging.__file__).resolve().parent
    shutil.copytree(source, Path(purelib) / "packaging")


def test_frozen_style_bootstrap_artifact_needs_no_installed_qwenpaw(
    tmp_path: Path,
) -> None:
    """A copied bootstrap loads QwenPaw only from explicit code_root."""
    code_root = _copy_code_root(tmp_path)
    bootstrap_copy = tmp_path / "application" / "runner_bootstrap.py"
    bootstrap_copy.parent.mkdir()
    shutil.copy2(BOOTSTRAP, bootstrap_copy)
    bootstrap_copy.chmod(0o444)
    manifest = _write_manifest(tmp_path, code_root=code_root)
    environment_root = tmp_path / "dependency-environment"
    venv.EnvBuilder(with_pip=False).create(environment_root)
    interpreter = environment_root / (
        "Scripts/python.exe" if os.name == "nt" else "bin/python"
    )
    _copy_packaging_to_environment(interpreter)
    probe_code = (
        "import importlib.util; "
        "found = importlib.util.find_spec('qwenpaw'); "
        "raise SystemExit(found is not None)"
    )
    probe = subprocess.run(
        [
            str(interpreter),
            "-I",
            "-c",
            probe_code,
        ],
        capture_output=True,
        check=False,
        timeout=5,
    )
    assert probe.returncode == 0

    result_path = tmp_path / "result.json"
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
    environment.update(_environment(result_path))
    result = subprocess.run(
        [
            str(interpreter),
            "-I",
            str(bootstrap_copy.resolve()),
            "--code-root",
            str(code_root),
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
    assert result.stdout == b""
    payload = _read_result(result_path)
    assert payload["driver_constructed"] is True
    assert Path(payload["descriptor_file"]) == (
        code_root / "qwenpaw" / "channel_protocol" / "descriptor.py"
    )
    assert b"Traceback" not in result.stderr


@pytest.mark.asyncio
async def test_protocol_adapter_uses_framed_transport_single_writer() -> None:
    """Concurrent and large messages remain complete on the real pipe."""
    read_fd, write_fd = os.pipe()
    os.set_inheritable(write_fd, False)
    handle = os.fdopen(write_fd, "wb", buffering=0)
    transport = await _open_protocol_transport(FramedTransport, handle)
    messages = [f'{{"index":{index}}}' for index in range(16)]
    messages.append(json.dumps({"payload": "x" * (128 * 1024)}))
    expected_bytes = sum(len(encode_frame(value)) for value in messages)
    reader = asyncio.create_task(
        asyncio.to_thread(_read_pipe, read_fd, expected_bytes),
    )

    await asyncio.gather(*(transport.send(value) for value in messages))
    await transport.aclose()
    data = await reader

    assert _decode_frames(data) == messages


@pytest.mark.asyncio
async def test_windows_adapter_completes_partial_sync_writes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The Windows path avoids Proactor and completes every sync write."""
    handle = _PartialWriteHandle()
    monkeypatch.setattr(os, "name", "nt")
    transport = await _open_protocol_transport(FramedTransport, handle)
    message = '{"platform":"windows"}'

    await transport.send(message)
    await transport.aclose()

    assert bytes(handle.data) == encode_frame(message)
    assert handle.closed


@pytest.mark.asyncio
async def test_windows_adapter_preserves_framing_failures(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The Windows adapter retains broken-pipe and timeout semantics."""
    monkeypatch.setattr(os, "name", "nt")
    broken = await _open_protocol_transport(
        FramedTransport,
        _BrokenWriteHandle(),
    )
    with pytest.raises(FrameWriteError):
        await broken.send("{}")
    await broken.aclose()

    blocked_handle = _BlockedWriteHandle()
    blocked = await _open_protocol_transport(
        FramedTransport,
        blocked_handle,
        limits=FramingLimits(write_timeout=0.02),
    )
    with pytest.raises(FrameTimeoutError):
        await blocked.send("{}")
    assert blocked_handle.started.wait(timeout=1)
    assert blocked.is_closed
    await blocked.aclose()


@pytest.mark.asyncio
async def test_protocol_adapter_reuses_framing_limits_and_failures() -> None:
    """Oversize, broken pipe, and write timeout keep CH-0-003 errors."""
    limited_read_fd, limited_write_fd = os.pipe()
    limited_handle = os.fdopen(limited_write_fd, "wb", buffering=0)
    limited = await _open_protocol_transport(
        FramedTransport,
        limited_handle,
        limits=FramingLimits(max_frame_bytes=8),
    )
    with pytest.raises(FrameLimitError):
        await limited.send("x" * 9)
    await limited.aclose()
    os.close(limited_read_fd)

    broken_read_fd, broken_write_fd = os.pipe()
    os.close(broken_read_fd)
    broken_handle = os.fdopen(broken_write_fd, "wb", buffering=0)
    broken = await _open_protocol_transport(
        FramedTransport,
        broken_handle,
    )
    with pytest.raises(FrameWriteError):
        await broken.send("{}")
    await broken.aclose()

    blocked_read_fd, blocked_write_fd = os.pipe()
    blocked_handle = os.fdopen(blocked_write_fd, "wb", buffering=0)
    blocked = await _open_protocol_transport(
        FramedTransport,
        blocked_handle,
        limits=FramingLimits(write_timeout=0.02),
    )
    with pytest.raises(FrameTimeoutError):
        await blocked.send("x" * (512 * 1024))
    assert blocked.is_closed
    await blocked.aclose()
    os.close(blocked_read_fd)
