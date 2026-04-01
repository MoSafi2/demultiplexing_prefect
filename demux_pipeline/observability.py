from __future__ import annotations

import json
import os
import re
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import fcntl  # type: ignore
from prefect.artifacts import create_table_artifact


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _epoch_ms() -> int:
    return int(time.time() * 1000)


def slugify_run_name(raw: str, *, max_len: int = 80) -> str:
    s = raw.strip()
    if not s:
        return ""
    s = re.sub(r"[^A-Za-z0-9_.-]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("._-")
    if len(s) > max_len:
        s = s[:max_len].rstrip("._-")
    return s


def default_run_name(*, mode: str, qc_tool: str) -> str:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{stamp}_{mode}_{qc_tool}"


def tracking_dir(outdir: Path, run_name: str) -> Path:
    return Path(outdir) / ".pipeline" / run_name


def events_path(outdir: Path, run_name: str) -> Path:
    return tracking_dir(outdir, run_name) / "events.jsonl"


def summary_path(outdir: Path, run_name: str) -> Path:
    return tracking_dir(outdir, run_name) / "run_summary.json"


# ---------------------------------------------------------------------------
# Run context
# ---------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class RunContext:
    run_name: str
    outdir: str
    mode: str
    qc_tool: str
    thread_budget: int
    contamination_tool: str | None = None
    started_at: str = ""
    inputs: dict[str, Any] | None = None

    def as_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["inputs"] = d["inputs"] or {}
        return d


# ---------------------------------------------------------------------------
# Observer — lifecycle and event logging only
# ---------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class Observer:
    run_name: str
    events_file: Path
    summary_file: Path

    def event(self, payload: dict[str, Any]) -> None:
        append_event(self.events_file, payload)

    def phase_started(self, phase: str) -> None:
        self.event({"type": "phase_started", "phase": phase, "run_name": self.run_name})

    def phase_finished(self, phase: str) -> None:
        self.event({"type": "phase_finished", "phase": phase, "run_name": self.run_name})

    def pipeline_started(self, context: RunContext) -> None:
        self.event({"type": "pipeline_started", "context": context.as_dict()})

    def pipeline_finished(self) -> None:
        self.event({"type": "pipeline_finished", "run_name": self.run_name})

    def asset_created(
        self,
        *,
        path: Path | str,
        step: str,
        tool: str,
        kind: str,
        sample: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        payload: dict[str, Any] = {
            "type": "asset_created",
            "run_name": self.run_name,
            "step": step,
            "tool": tool,
            "kind": kind,
            "path": str(path),
        }
        if sample is not None:
            payload["sample"] = sample
        if metadata is not None:
            payload["metadata"] = metadata
        self.event(payload)

    def finalize_summary(self, *, context: RunContext) -> dict[str, Any]:
        return finalize_run_summary(
            events_file=self.events_file,
            summary_file=self.summary_file,
            context=context,
        )


# ---------------------------------------------------------------------------
# Module-level observer — set once by the flow, read by infrastructure code
# ---------------------------------------------------------------------------

_current_observer: Observer | None = None


def set_observer(obs: Observer) -> None:
    global _current_observer
    _current_observer = obs


def get_observer() -> Observer | None:
    return _current_observer


def reset_observer() -> None:
    global _current_observer
    _current_observer = None


def init_run_tracking(
    outdir: Path,
    run_name: str,
    mode: str,
    qc_tool: str,
    contamination_tool: str | None,
    thread_budget: int,
    bcl_dir: Path | None,
    samplesheet: Path | None,
) -> tuple[RunContext, Observer]:
    """Create RunContext + Observer and start pipeline tracking."""
    events_file = events_path(outdir, run_name)
    summary_file = summary_path(outdir, run_name)

    ctx = RunContext(
        run_name=run_name,
        outdir=str(outdir),
        mode=mode,
        qc_tool=qc_tool,
        contamination_tool=contamination_tool,
        thread_budget=thread_budget,
        started_at=utc_now_iso(),
        inputs={
            "bcl_dir": str(bcl_dir) if bcl_dir else None,
            "samplesheet": str(samplesheet) if samplesheet else None,
        },
    )
    observer = Observer(
        run_name=run_name, events_file=events_file, summary_file=summary_file
    )
    set_observer(observer)
    observer.pipeline_started(ctx)
    return ctx, observer


def record_asset(
    path: Path | str,
    *,
    step: str,
    tool: str,
    kind: str,
    sample: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> None:
    """Emit an asset_created event via the current observer if one is set."""
    obs = get_observer()
    if obs:
        obs.asset_created(
            path=path,
            step=step,
            tool=tool,
            kind=kind,
            sample=sample,
            metadata=metadata,
        )


# ---------------------------------------------------------------------------
# Event log
# ---------------------------------------------------------------------------

def append_event(path: Path, event: dict[str, Any]) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    payload = dict(event)
    payload.setdefault("ts", utc_now_iso())
    payload.setdefault("ts_epoch_ms", _epoch_ms())

    line = json.dumps(payload, sort_keys=True, ensure_ascii=False)
    with path.open("a", encoding="utf-8") as f:
        try:
            fcntl.flock(f.fileno(), fcntl.LOCK_EX)
        except Exception:
            pass
        f.write(line + "\n")
        f.flush()
        os.fsync(f.fileno())
        try:
            fcntl.flock(f.fileno(), fcntl.LOCK_UN)
        except Exception:
            pass


def read_events(path: Path) -> list[dict[str, Any]]:
    path = Path(path)
    if not path.exists():
        return []
    out: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            out.append(json.loads(line))
        except Exception:
            continue
    return out


# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------

def finalize_run_summary(
    *, events_file: Path, summary_file: Path, context: RunContext
) -> dict[str, Any]:
    evs = read_events(events_file)
    assets = [e for e in evs if e.get("type") == "asset_created"]
    commands = [
        e for e in evs if e.get("type") in ("command_finished", "command_failed")
    ]
    phases = [e for e in evs if e.get("type") in ("phase_started", "phase_finished")]

    asset_index: dict[str, dict[str, Any]] = {}
    for a in assets:
        p = str(a.get("path", ""))
        if not p:
            continue
        asset_index[p] = a

    by_step: dict[str, list[dict[str, Any]]] = {}
    for cmd in commands:
        by_step.setdefault(str(cmd.get("step", "")), []).append(cmd)

    durations_by_step: dict[str, dict[str, Any]] = {}
    for step, items in by_step.items():
        durs = [
            int(i.get("duration_ms", 0) or 0)
            for i in items
            if i.get("duration_ms") is not None
        ]
        if not durs:
            continue
        durations_by_step[step] = {
            "count": len(durs),
            "total_ms": sum(durs),
            "max_ms": max(durs),
        }

    summary: dict[str, Any] = {
        "context": context.as_dict(),
        "counts": {
            "events": len(evs),
            "assets": len(asset_index),
            "commands": len(commands),
            "phases": len(phases),
            "failures": sum(1 for e in evs if e.get("type") == "command_failed"),
        },
        "assets": sorted(asset_index.keys()),
        "durations_by_step": durations_by_step,
    }

    summary_file = Path(summary_file)
    summary_file.parent.mkdir(parents=True, exist_ok=True)
    summary_file.write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return summary


# ---------------------------------------------------------------------------
# Prefect artifact — single table at end of run
# ---------------------------------------------------------------------------

def create_run_table(summary: dict[str, Any]) -> None:
    """Publish one Prefect table artifact with the full run summary."""
    try:
        ctx = summary.get("context", {})
        counts = summary.get("counts", {})
        durations = summary.get("durations_by_step", {})
        assets: list[str] = summary.get("assets", [])
        run_name = ctx.get("run_name", "")

        rows: list[dict[str, str]] = []

        # context
        for key in ("run_name", "mode", "qc_tool", "contamination_tool",
                    "outdir", "started_at", "thread_budget"):
            rows.append({"section": "context", "key": key, "value": str(ctx.get(key, ""))})
        inputs = ctx.get("inputs") or {}
        for k, v in inputs.items():
            rows.append({"section": "context.inputs", "key": k, "value": str(v) if v is not None else ""})

        # counts
        for key in ("assets", "commands", "events", "failures", "phases"):
            rows.append({"section": "counts", "key": key, "value": str(counts.get(key, 0))})

        # durations
        for phase in ("demux", "qc", "contamination", "multiqc"):
            d = durations.get(phase)
            if d is None:
                continue
            rows.append({
                "section": "durations_by_step",
                "key": phase,
                "value": (
                    f"count={d['count']}  "
                    f"total={round(d['total_ms'] / 1000, 1)}s  "
                    f"max={round(d['max_ms'] / 1000, 1)}s"
                ),
            })

        # assets
        for path in assets:
            rows.append({"section": "assets", "key": "", "value": path})

        create_table_artifact(
            table=rows,
            key="pipeline-summary",
            description=(
                f"run={run_name}  mode={ctx.get('mode', '')}  "
                f"qc={ctx.get('qc_tool', '')}  "
                f"assets={counts.get('assets', 0)}  failures={counts.get('failures', 0)}"
            ),
        )
    except Exception:
        pass
