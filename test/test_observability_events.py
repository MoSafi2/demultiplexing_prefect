from __future__ import annotations

import importlib.util
import json
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
        mode="qc",
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


def test_publish_prefect_observability_artifacts_best_effort(tmp_path: Path) -> None:
    obs = _load_repo_module("observability", "demux_pipeline/observability.py")

    events = tmp_path / "events.jsonl"
    summary = tmp_path / "run_summary.json"
    multiqc = tmp_path / "multiqc_report.html"
    events.write_text('{"type":"asset_created","path":"x"}\n', encoding="utf-8")
    summary.write_text(
        json.dumps(
            {
                "context": {"mode": "qc", "qc_tool": "falco", "thread_budget": 1},
                "counts": {"events": 1, "assets": 1, "commands": 0, "phases": 0, "failures": 0},
            }
        ),
        encoding="utf-8",
    )
    multiqc.write_text("<html></html>", encoding="utf-8")

    md_calls: list[dict] = []
    link_calls: list[dict] = []

    def _fake_md(**kwargs):
        md_calls.append(kwargs)
        return "id"

    def _fake_link(**kwargs):
        link_calls.append(kwargs)
        return "id"

    setattr(obs, "create_markdown_artifact", _fake_md)
    setattr(obs, "create_link_artifact", _fake_link)

    obs.publish_prefect_observability_artifacts(
        run_name="unit_test",
        summary_file=summary,
        events_file=events,
        extra_paths=[multiqc],
    )

    assert len(md_calls) == 1
    assert "key" not in md_calls[0]
    assert "Pipeline run: unit_test" in md_calls[0]["markdown"]
    assert len(link_calls) == 3
    link_texts = {c["link_text"] for c in link_calls}
    assert {"events.jsonl", "run_summary.json", "multiqc_report.html"} <= link_texts


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


def test_observer_finalize_and_publish_prefect_artifacts(tmp_path: Path) -> None:
    obs = _load_repo_module("observability", "demux_pipeline/observability.py")
    events = tmp_path / "events.jsonl"
    summary = tmp_path / "run_summary.json"
    report = tmp_path / "multiqc_report.html"
    report.write_text("<html></html>", encoding="utf-8")
    ctx = obs.RunContext(
        run_name="unit_test",
        outdir=str(tmp_path),
        mode="qc",
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

    md_calls: list[dict] = []
    link_calls: list[dict] = []

    def _fake_md(**kwargs):
        md_calls.append(kwargs)
        return "id"

    def _fake_link(**kwargs):
        link_calls.append(kwargs)
        return "id"

    setattr(obs, "create_markdown_artifact", _fake_md)
    setattr(obs, "create_link_artifact", _fake_link)
    observer.publish_prefect_artifacts(extra_paths=[report])
    assert len(md_calls) == 1
    assert "Pipeline run: unit_test" in md_calls[0]["markdown"]
    assert len(link_calls) == 3
