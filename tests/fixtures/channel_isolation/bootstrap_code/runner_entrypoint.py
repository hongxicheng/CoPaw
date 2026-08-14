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
            "os.close(1); os.close(2); time.sleep(1.5)"
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
