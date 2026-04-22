"""Tests for run_command: success, failure, stderr capture, observer events."""
from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]


def _load_process():
    if str(REPO_ROOT) not in sys.path:
        sys.path.insert(0, str(REPO_ROOT))
    from demux_pipeline import observability as obs_mod
    from demux_pipeline import process as process_mod

    return process_mod, obs_mod


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
    process_mod, _ = _load_process()
    proc = _mock_popen(returncode=0)
    with patch.object(process_mod.subprocess, "Popen", return_value=proc):
        _call(process_mod, ["echo", "hello"])  # must not raise


def test_run_command_success_stays_quiet(capsys):
    process_mod, _ = _load_process()
    proc = _mock_popen(returncode=0, stdout="hello\n", stderr="warning\n")
    with patch.object(process_mod.subprocess, "Popen", return_value=proc):
        _call(process_mod, ["echo", "hello"])

    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == ""


def test_run_command_failure_raises(tmp_path):
    process_mod, _ = _load_process()
    proc = _mock_popen(returncode=1)
    with patch.object(process_mod.subprocess, "Popen", return_value=proc):
        with pytest.raises(RuntimeError, match="echo"):
            _call(process_mod, ["echo", "fail"])


def test_run_command_failure_prints_raw_output(capsys):
    process_mod, _ = _load_process()
    proc = _mock_popen(returncode=1, stdout="partial out\n", stderr="bad error\n")
    with patch.object(process_mod.subprocess, "Popen", return_value=proc):
        with pytest.raises(RuntimeError, match="echo"):
            _call(process_mod, ["echo", "fail"])

    captured = capsys.readouterr()
    assert captured.out == "partial out\n"
    assert captured.err == "bad error\n"


def test_run_command_stderr_tail_in_error(tmp_path):
    process_mod, _ = _load_process()
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
    process_mod, obs_mod = _load_process()

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
        obs_mod.reset_observer()


def test_run_command_emits_failure_event(tmp_path):
    process_mod, obs_mod = _load_process()

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
        obs_mod.reset_observer()
