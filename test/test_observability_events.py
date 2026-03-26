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


def test_emit_prefect_asset_events_from_local_log_dedupes_and_payload(tmp_path: Path) -> None:
    obs = _load_repo_module("observability", "demux_pipeline/observability.py")

    events = tmp_path / "events.jsonl"
    p1 = tmp_path / "a" / "x.txt"
    p2 = tmp_path / "b" / "y.txt"
    p1.parent.mkdir(parents=True, exist_ok=True)
    p2.parent.mkdir(parents=True, exist_ok=True)
    p1.write_text("x", encoding="utf-8")
    p2.write_text("y", encoding="utf-8")
    events.write_text(
        "\n".join(
            [
                json.dumps({"type": "asset_created", "path": str(p1), "kind": "report", "tool": "qc", "step": "qc"}),
                json.dumps({"type": "asset_created", "path": str(p1), "kind": "report", "tool": "qc", "step": "qc"}),
                json.dumps({"type": "asset_created", "path": str(p2), "kind": "output", "tool": "multiqc", "step": "multiqc"}),
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    event_calls: list[dict] = []

    def _fake_emit_event(**kwargs):
        event_calls.append(kwargs)
        return None

    setattr(obs, "emit_event", _fake_emit_event)
    obs.emit_prefect_asset_events_from_local_log(events_file=events, run_name="unit_test")

    assert len(event_calls) == 2
    for call in event_calls:
        assert call["event"] == "asset.created"
        rid = call["resource"]["prefect.resource.id"]
        assert rid.startswith("prefect.asset.pipeline.unit_test.")
        assert call["resource"]["prefect.resource.role"] == "asset"
        assert call["payload"]["run_name"] == "unit_test"


def test_emit_prefect_asset_events_from_local_log_is_best_effort(tmp_path: Path) -> None:
    obs = _load_repo_module("observability", "demux_pipeline/observability.py")
    events = tmp_path / "events.jsonl"
    p = tmp_path / "x.txt"
    p.write_text("x", encoding="utf-8")
    events.write_text(
        json.dumps({"type": "asset_created", "path": str(p), "kind": "report"}) + "\n",
        encoding="utf-8",
    )

    def _boom(**kwargs):
        raise RuntimeError("boom")

    setattr(obs, "emit_event", _boom)
    obs.emit_prefect_asset_events_from_local_log(events_file=events, run_name="unit_test")
