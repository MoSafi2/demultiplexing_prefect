from __future__ import annotations

import shutil
import subprocess
import sys
import time
from typing import Sequence

from prefect import get_run_logger
from demux_pipeline.observability import get_observer


def require_executable(exe: str) -> None:
    if shutil.which(exe) is None:
        raise SystemExit(
            f"Missing required executable on PATH: {exe}. "
            f"Please install it and ensure it is available on your PATH."
        )


def run_command(
    cmd: Sequence[str],
    *,
    capture_err_tail: int | None = None,
    step: str | None = None,
    tool: str | None = None,
    sample: str | None = None,
) -> None:
    logger = get_run_logger()
    obs = get_observer()

    cmd_str = " ".join(cmd)
    start = time.monotonic()

    if obs:
        obs.event({
            "type": "command_started",
            "run_name": obs.run_name,
            "step": step,
            "tool": tool,
            "sample": sample,
            "cmd": list(cmd),
            "cmd_str": cmd_str,
        })

    proc = subprocess.Popen(
        list(cmd),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    stdout, stderr = proc.communicate()
    duration_ms = int((time.monotonic() - start) * 1000)

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
        if obs:
            obs.event({
                "type": "command_failed",
                "run_name": obs.run_name,
                "step": step,
                "tool": tool,
                "sample": sample,
                "cmd": list(cmd),
                "cmd_str": cmd_str,
                "returncode": proc.returncode,
                "duration_ms": duration_ms,
                "stderr_tail": err_tail,
            })
        msg = f"Command failed: {' '.join(cmd)}"
        if err_tail:
            msg = f"{msg}\n{'\n'.join(err_tail)}"
        raise RuntimeError(msg)

    if obs:
        obs.event({
            "type": "command_finished",
            "run_name": obs.run_name,
            "step": step,
            "tool": tool,
            "sample": sample,
            "cmd": list(cmd),
            "cmd_str": cmd_str,
            "returncode": proc.returncode,
            "duration_ms": duration_ms,
            "stderr_tail": err_tail,
        })
