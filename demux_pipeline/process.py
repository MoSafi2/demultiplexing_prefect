from __future__ import annotations

import shutil
import subprocess
import sys
from typing import Sequence

from prefect import get_run_logger


def require_executable(exe: str) -> None:
    if shutil.which(exe) is None:
        raise SystemExit(
            f"Missing required executable on PATH: {exe}. "
            f"Please install it and ensure it is available on your PATH."
        )


def run_command(cmd: Sequence[str], *, capture_err_tail: int | None = None) -> None:
    logger = get_run_logger()

    proc = subprocess.Popen(
        list(cmd),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    stdout, stderr = proc.communicate()

    # Print tool output "as-is" (preserve newlines/formatting) instead of
    # threading it through structured Prefect logs.
    if stdout:
        sys.stdout.write(stdout)
        sys.stdout.flush()
    if stderr:
        sys.stderr.write(stderr)
        sys.stderr.flush()

    err_tail: list[str] = []
    if capture_err_tail and capture_err_tail > 0 and stderr:
        err_tail = stderr.splitlines()[-capture_err_tail:]

    if proc.returncode != 0:
        msg = f"Command failed: {' '.join(cmd)}"
        if err_tail:
            msg = f"{msg}\n{'\n'.join(err_tail)}"
        raise RuntimeError(msg)
