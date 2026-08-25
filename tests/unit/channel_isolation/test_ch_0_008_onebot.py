# -*- coding: utf-8 -*-
"""Run CH-0-008 tests outside legacy Channel imports."""

from pathlib import Path
import subprocess
import sys


SUITE = (
    Path(__file__).parents[2]
    / "fixtures"
    / "channel_isolation"
    / "onebot"
    / "ch_0_008_onebot_suite.py"
)


def test_ch_0_008_in_isolated_pytest_process() -> None:
    """Verify the production OneBot Driver in an isolated test process."""
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
