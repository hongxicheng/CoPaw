# -*- coding: utf-8 -*-
"""Channel entrypoint fixtures for the standalone Runner bootstrap."""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path
import subprocess
import sys
from collections.abc import Callable

import fake_feishu_sdk


def _send(write_protocol: Callable[[str], None], value: object) -> None:
    """Send one compact JSON fixture message."""
    write_protocol(json.dumps(value, separators=(",", ":")))


def smoke(write_protocol: Callable[[str], None]) -> int:
    """Exercise Python, logging, SDK, and native stdout redirection."""
    print("runner-print")
    logging.warning("runner-logging")
    os.write(1, b"runner-fd1\n")
    _send(
        write_protocol,
        {
            "sdk_ready": fake_feishu_sdk.SDK_READY,
            "pythonpath": os.environ.get("PYTHONPATH"),
            "pythonhome": os.environ.get("PYTHONHOME"),
            "unexpected_env": os.environ.get("QWENPAW_BOOTSTRAP_LEAK"),
            "isolated": bool(sys.flags.isolated),
            "ignore_environment": bool(sys.flags.ignore_environment),
            "no_user_site": bool(sys.flags.no_user_site),
            "entrypoint_file": str(Path(__file__).resolve()),
            "sys_path": list(sys.path),
        },
    )
    return 0


def noisy(write_protocol: Callable[[str], None]) -> int:
    """Fill stderr beyond normal pipe capacity before writing a frame."""
    block = b"log-backpressure-" + b"x" * (64 * 1024)
    remaining = 16 * 1024 * 1024
    while remaining > 0:
        written = os.write(2, block[: min(len(block), remaining)])
        remaining -= written
    _send(write_protocol, {"logs_written": 16 * 1024 * 1024})
    return 0


def spawn_descendant(write_protocol: Callable[[str], None]) -> int:
    """Spawn an uncontrolled descendant with inheritance enabled."""
    # The descendant intentionally outlives the Runner fixture process.
    process = subprocess.Popen(  # pylint: disable=consider-using-with
        [
            sys.executable,
            "-I",
            "-c",
            "import time; time.sleep(1.5)",
        ],
        close_fds=False,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    _send(write_protocol, {"descendant_pid": process.pid})
    return 0
