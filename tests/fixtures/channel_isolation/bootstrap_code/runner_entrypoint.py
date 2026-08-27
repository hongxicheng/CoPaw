# -*- coding: utf-8 -*-
"""ChannelDriver fixtures for the standalone Runner bootstrap."""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path
import subprocess
import sys
from typing import Any

import fake_feishu_sdk
from qwenpaw.channel_protocol import LifecycleController, RunnerLifecycleSpec


def _write_result(value: dict[str, Any]) -> None:
    """Persist one fixture result outside the trusted code root."""
    result_path = os.environ.get("QWENPAW_FIXTURE_RESULT")
    if result_path is None:
        raise RuntimeError("QWENPAW_FIXTURE_RESULT is required")
    Path(result_path).write_text(
        json.dumps(value, separators=(",", ":")),
        encoding="utf-8",
    )


class FixtureDriver:
    """Record construction, environment isolation, and output routing."""

    def __init__(self) -> None:
        print("runner-print")
        logging.warning("runner-logging")
        os.write(1, b"runner-fd1\n")
        descriptor_module = sys.modules["qwenpaw.channel_protocol.descriptor"]
        descriptor_file = descriptor_module.__file__
        if descriptor_file is None:
            raise RuntimeError("Descriptor module has no source file")
        _write_result(
            {
                "driver_constructed": True,
                "sdk_ready": fake_feishu_sdk.SDK_READY,
                "pythonpath": os.environ.get("PYTHONPATH"),
                "pythonhome": os.environ.get("PYTHONHOME"),
                "unexpected_env": os.environ.get(
                    "QWENPAW_BOOTSTRAP_LEAK",
                ),
                "https_proxy": os.environ.get("HTTPS_PROXY"),
                "telegram_proxy": os.environ.get(
                    "TELEGRAM_HTTP_PROXY",
                ),
                "ssl_cert_file": os.environ.get("SSL_CERT_FILE"),
                "isolated": bool(sys.flags.isolated),
                "ignore_environment": bool(sys.flags.ignore_environment),
                "no_user_site": bool(sys.flags.no_user_site),
                "entrypoint_file": str(Path(__file__).resolve()),
                "descriptor_file": str(
                    Path(descriptor_file).resolve(),
                ),
                "sys_path": list(sys.path),
            },
        )


class _FixtureLifecycleController(LifecycleController):
    """Record that prepare crossed the real bootstrap protocol path."""

    async def prepare(self, params: Any) -> dict[str, Any]:
        result = await super().prepare(params)
        result_path = os.environ.get("QWENPAW_FIXTURE_RESULT")
        if result_path is None:
            raise RuntimeError("QWENPAW_FIXTURE_RESULT is required")
        path = Path(result_path)
        payload = json.loads(path.read_text(encoding="utf-8"))
        payload["prepare_called"] = True
        _write_result(payload)
        return result


class LifecycleFixtureDriver(FixtureDriver):
    """Expose the Phase 0 lifecycle contract to the real bootstrap."""

    source_revision = "f" * 64

    def bind(self, peer: Any, identity: Any) -> None:
        """Retain the trusted session inputs supplied by bootstrap."""
        self.peer = peer
        self.identity = identity

    def create_lifecycle_spec(
        self,
        identity: Any,
        *,
        secret_handle_consumer: Any | None,
    ) -> RunnerLifecycleSpec:
        """Describe a controller without carrying protocol source."""
        if secret_handle_consumer is not None:
            raise RuntimeError("fixture does not accept a secret consumer")
        return RunnerLifecycleSpec(
            controller_class=_FixtureLifecycleController,
            args=(),
            kwargs={
                "channel_key": identity.channel_key,
                "instance_id": identity.instance_id,
                "environment_spec_id": identity.environment_spec_id,
                "environment_id": identity.environment_id,
                "qwenpaw_version": identity.qwenpaw_version,
                "lock_sha256": identity.lock_sha256,
                "python_abi": identity.python_abi,
                "platform_tag": identity.platform_tag,
                "generation": identity.generation,
                "capabilities": identity.capabilities,
            },
        )

    def attach_lifecycle(self, controller: LifecycleController) -> None:
        """Retain the controller used by the bootstrap session."""
        self.controller = controller


class NoisyDriver:
    """Fill stderr beyond normal pipe capacity during construction."""

    def __init__(self) -> None:
        block = b"log-backpressure-" + b"x" * (64 * 1024)
        total = 16 * 1024 * 1024
        remaining = total
        while remaining > 0:
            written = os.write(2, block[: min(len(block), remaining)])
            remaining -= written
        _write_result({"logs_written": total})


class DescendantDriver:
    """Spawn a descendant that uses inherited ordinary stdout."""

    def __init__(self) -> None:
        child_code = (
            "import os, time; "
            "os.write(1, b'descendant-fd1\\n'); "
            "os.close(1); os.close(2); time.sleep(10)"
        )
        process = subprocess.Popen(  # pylint: disable=consider-using-with
            [sys.executable, "-I", "-c", child_code],
            close_fds=False,
            stdin=subprocess.DEVNULL,
        )
        _write_result({"descendant_pid": process.pid})


def legacy_callback(_value: object) -> int:
    """Expose the removed callback contract as a negative fixture."""
    _write_result({"legacy_callback_invoked": True})
    return 0
