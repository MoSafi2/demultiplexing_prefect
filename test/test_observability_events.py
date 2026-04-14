from __future__ import annotations

import importlib.util
import json
import os
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def _load_repo_module(name: str, relative_path: str):
    path = REPO_ROOT / relative_path
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


def test_events_jsonl_append_and_summary(tmp_path: Path) -> None:
    obs = _load_repo_module("observability", "demux_pipeline/observability.py")

    events = tmp_path / "events.jsonl"
    summary = tmp_path / "run_summary.json"
    ctx = obs.RunContext(
        run_name="t",
        outdir=str(tmp_path),
        mode="demux",
        qc_tool="falco",
        contamination_tool=None,
        thread_budget=1,
        started_at=obs.utc_now_iso(),
        inputs={"x": 1},
    )

    obs.append_event(events, {"type": "asset_created", "path": str(tmp_path / "a.txt")})
    obs.append_event(
        events,
        {"type": "command_finished", "step": "qc", "duration_ms": 12, "returncode": 0},
    )

    out = obs.finalize_run_summary(events_file=events, summary_file=summary, context=ctx)

    assert summary.exists()
    loaded = json.loads(summary.read_text(encoding="utf-8"))
    assert loaded["counts"]["events"] >= 2
    assert loaded["counts"]["assets"] == 1
    assert loaded["durations_by_step"]["qc"]["total_ms"] == 12
    assert out["context"]["run_name"] == "t"


def test_create_run_table_emits_artifact(tmp_path: Path) -> None:
    obs = _load_repo_module("observability", "demux_pipeline/observability.py")

    summary = {
        "context": {
            "run_name": "unit_test",
            "mode": "demux",
            "qc_tool": "falco",
            "contamination_tool": None,
            "outdir": "/tmp/out",
            "started_at": "2026-01-01T00:00:00+00:00",
            "thread_budget": 4,
            "inputs": {"bcl_dir": "/tmp/bcl", "samplesheet": "/tmp/SampleSheet.csv"},
        },
        "counts": {"events": 3, "assets": 2, "commands": 2, "phases": 2, "failures": 0},
        "durations_by_step": {
            "qc": {"count": 2, "total_ms": 4000, "max_ms": 2500},
        },
        "assets": ["/tmp/out/falco/s1_R1", "/tmp/out/falco/s1_R2"],
    }

    table_calls: list[dict] = []

    def _fake_table(**kwargs):
        table_calls.append(kwargs)
        return "id"

    setattr(obs, "create_table_artifact", _fake_table)
    previous = os.environ.get("DEMUX_ENABLE_PREFECT_ARTIFACTS")
    os.environ["DEMUX_ENABLE_PREFECT_ARTIFACTS"] = "1"
    try:
        obs.create_run_table(summary)
    finally:
        if previous is None:
            os.environ.pop("DEMUX_ENABLE_PREFECT_ARTIFACTS", None)
        else:
            os.environ["DEMUX_ENABLE_PREFECT_ARTIFACTS"] = previous

    assert len(table_calls) == 1
    call = table_calls[0]
    assert call["key"] == "pipeline-summary"
    rows = call["table"]

    # all rows have section/key/value columns
    assert all("section" in r and "key" in r and "value" in r for r in rows)

    sections = [r["section"] for r in rows]
    assert "context" in sections
    assert "counts" in sections
    assert "durations_by_step" in sections
    assert "assets" in sections

    # context run_name row
    ctx_row = next(r for r in rows if r["section"] == "context" and r["key"] == "run_name")
    assert ctx_row["value"] == "unit_test"

    # duration row includes formatted timing
    dur_row = next(r for r in rows if r["section"] == "durations_by_step" and r["key"] == "qc")
    assert "4.0s" in dur_row["value"]
    assert "2.5s" in dur_row["value"]

    # assets appear as value rows
    asset_values = [r["value"] for r in rows if r["section"] == "assets"]
    assert "/tmp/out/falco/s1_R1" in asset_values


def test_create_run_table_skips_artifact_when_disabled() -> None:
    obs = _load_repo_module("observability", "demux_pipeline/observability.py")
    table_calls: list[dict] = []

    def _fake_table(**kwargs):
        table_calls.append(kwargs)
        return "id"

    setattr(obs, "create_table_artifact", _fake_table)
    previous = os.environ.get("DEMUX_ENABLE_PREFECT_ARTIFACTS")
    os.environ.pop("DEMUX_ENABLE_PREFECT_ARTIFACTS", None)
    try:
        obs.create_run_table({"context": {}, "counts": {}, "durations_by_step": {}, "assets": []})
    finally:
        if previous is None:
            os.environ.pop("DEMUX_ENABLE_PREFECT_ARTIFACTS", None)
        else:
            os.environ["DEMUX_ENABLE_PREFECT_ARTIFACTS"] = previous

    assert table_calls == []


def test_observer_records_events_and_assets(tmp_path: Path) -> None:
    obs = _load_repo_module("observability", "demux_pipeline/observability.py")
    events = tmp_path / "events.jsonl"
    summary = tmp_path / "run_summary.json"
    observer = obs.Observer(run_name="unit_test", events_file=events, summary_file=summary)
    observer.event({"type": "phase_started", "phase": "qc", "run_name": "unit_test"})
    observer.asset_created(
        path=tmp_path / "sample.txt",
        step="qc",
        tool="falco",
        kind="report",
        sample="s1",
    )
    loaded = obs.read_events(events)
    assert len(loaded) == 2
    assert loaded[0]["type"] == "phase_started"
    assert loaded[1]["type"] == "asset_created"
    assert loaded[1]["run_name"] == "unit_test"
    assert loaded[1]["sample"] == "s1"


def test_observer_finalize_summary(tmp_path: Path) -> None:
    obs = _load_repo_module("observability", "demux_pipeline/observability.py")
    events = tmp_path / "events.jsonl"
    summary = tmp_path / "run_summary.json"
    ctx = obs.RunContext(
        run_name="unit_test",
        outdir=str(tmp_path),
        mode="demux",
        qc_tool="falco",
        contamination_tool=None,
        thread_budget=1,
        started_at=obs.utc_now_iso(),
        inputs={},
    )
    observer = obs.Observer(run_name="unit_test", events_file=events, summary_file=summary)
    observer.asset_created(path=tmp_path / "a.txt", step="qc", tool="falco", kind="report")
    out = observer.finalize_summary(context=ctx)
    assert out["counts"]["assets"] == 1
    assert summary.exists()
    loaded = json.loads(summary.read_text(encoding="utf-8"))
    assert loaded["context"]["run_name"] == "unit_test"
