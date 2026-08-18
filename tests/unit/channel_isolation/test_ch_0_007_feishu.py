# -*- coding: utf-8 -*-
"""Run CH-0-007 tests outside legacy global SDK substitutes."""

from pathlib import Path
import subprocess
import sys


SUITE = (
    Path(__file__).parents[2]
    / "fixtures"
    / "channel_isolation"
    / "feishu"
    / "ch_0_007_feishu_suite.py"
)


def test_ch_0_007_in_isolated_pytest_process() -> None:
    """Verify the production adapter with its real dependency imports."""
    result = subprocess.run(
        [
            sys.executable,
            "-I",
            "-m",
            "pytest",
            "--confcutdir",
            str(SUITE.parent),
            str(SUITE),
            "-q",
        ],
        capture_output=True,
        check=False,
        timeout=60,
    )
    assert result.returncode == 0, result.stdout.decode(
        errors="replace",
    ) + result.stderr.decode(errors="replace")
