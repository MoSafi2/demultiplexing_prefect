"""Tests for run_command: success, failure, stderr capture, observer events."""
from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]


def _load_module(name: str, relative_path: str):
    path = REPO_ROOT / relative_path
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


def _load_process():
    if "process" in sys.modules and hasattr(sys.modules["process"], "run_command"):
        return sys.modules["process"]
    _load_module("observability", "demux_pipeline/observability.py")
    return _load_module("process", "demux_pipeline/process.py")


def _mock_popen(returncode: int = 0, stdout: str = "", stderr: str = ""):
    proc = MagicMock()
    proc.returncode = returncode
    proc.communicate.return_value = (stdout, stderr)
    return proc


def _call(process_mod, cmd, **kwargs):
    """Call run_command with get_run_logger patched out."""
    mock_logger = MagicMock()
    with patch.object(process_mod, "get_run_logger", return_value=mock_logger):
        process_mod.run_command(cmd, **kwargs)


def test_run_command_success(tmp_path):
    process_mod = _load_process()
    proc = _mock_popen(returncode=0)
    with patch.object(process_mod.subprocess, "Popen", return_value=proc):
        _call(process_mod, ["echo", "hello"])  # must not raise


def test_run_command_failure_raises(tmp_path):
    process_mod = _load_process()
    proc = _mock_popen(returncode=1)
    with patch.object(process_mod.subprocess, "Popen", return_value=proc):
        with pytest.raises(RuntimeError, match="echo"):
            _call(process_mod, ["echo", "fail"])


def test_run_command_stderr_tail_in_error(tmp_path):
    process_mod = _load_process()
    stderr = "\n".join(f"line{i}" for i in range(10))
    proc = _mock_popen(returncode=1, stderr=stderr)
    with patch.object(process_mod.subprocess, "Popen", return_value=proc):
        with pytest.raises(RuntimeError) as exc_info:
            _call(process_mod, ["mytool"], capture_err_tail=3)
    msg = str(exc_info.value)
    assert "line9" in msg
    assert "line8" in msg
    assert "line7" in msg
    # lines outside the tail are NOT included
    assert "line0" not in msg


def test_run_command_emits_observer_events(tmp_path):
    process_mod = _load_process()
    obs_mod = sys.modules["observability"]

    events_file = tmp_path / "events.jsonl"
    summary_file = tmp_path / "run_summary.json"
    observer = obs_mod.Observer(
        run_name="t", events_file=events_file, summary_file=summary_file
    )
    obs_mod.set_observer(observer)
    try:
        proc = _mock_popen(returncode=0)
        with patch.object(process_mod.subprocess, "Popen", return_value=proc):
            _call(process_mod, ["mytool"], step="qc", tool="mytool", sample="s1")

        events = obs_mod.read_events(events_file)
        types = [e["type"] for e in events]
        assert "command_started" in types
        assert "command_finished" in types
    finally:
        obs_mod.set_observer(None)


def test_run_command_emits_failure_event(tmp_path):
    process_mod = _load_process()
    obs_mod = sys.modules["observability"]

    events_file = tmp_path / "events.jsonl"
    summary_file = tmp_path / "run_summary.json"
    observer = obs_mod.Observer(
        run_name="t", events_file=events_file, summary_file=summary_file
    )
    obs_mod.set_observer(observer)
    try:
        proc = _mock_popen(returncode=1, stderr="bad error")
        with patch.object(process_mod.subprocess, "Popen", return_value=proc):
            with pytest.raises(RuntimeError):
                _call(process_mod, ["mytool"], step="qc", tool="mytool")

        events = obs_mod.read_events(events_file)
        types = [e["type"] for e in events]
        assert "command_failed" in types
    finally:
        obs_mod.set_observer(None)
