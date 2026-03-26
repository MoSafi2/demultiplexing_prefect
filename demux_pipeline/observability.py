from __future__ import annotations

import json
import os
import re
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable
from prefect.artifacts import create_link_artifact, create_markdown_artifact
import fcntl  # type: ignore


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
        self.event(
            {"type": "phase_finished", "phase": phase, "run_name": self.run_name}
        )

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

    def publish_prefect_artifacts(
        self, *, extra_paths: Iterable[Path] | None = None
    ) -> None:
        publish_prefect_observability_artifacts(
            run_name=self.run_name,
            summary_file=self.summary_file,
            events_file=self.events_file,
            extra_paths=extra_paths,
        )


def append_event(path: Path, event: dict[str, Any]) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    payload = dict(event)
    payload.setdefault("ts", utc_now_iso())
    payload.setdefault("ts_epoch_ms", _epoch_ms())

    line = json.dumps(payload, sort_keys=True, ensure_ascii=False)
    with path.open("a", encoding="utf-8") as f:
        if fcntl is not None:
            try:
                fcntl.flock(f.fileno(), fcntl.LOCK_EX)
            except Exception:
                pass
        f.write(line + "\n")
        f.flush()
        os.fsync(f.fileno())
        if fcntl is not None:
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
            # tolerate partial/corrupt lines (e.g. abrupt termination)
            continue
    return out


def _group_by(
    items: Iterable[dict[str, Any]], key: str
) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for it in items:
        k = str(it.get(key, ""))
        grouped.setdefault(k, []).append(it)
    return grouped


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

    durations_by_step: dict[str, dict[str, Any]] = {}
    for step, items in _group_by(commands, "step").items():
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


def publish_prefect_observability_artifacts(
    *,
    run_name: str,
    summary_file: Path,
    events_file: Path,
    extra_paths: Iterable[Path] | None = None,
) -> None:
    """
    Best-effort bridge to Prefect dashboard artifacts.

    This function intentionally never raises to avoid changing pipeline outcomes.
    """
    if create_markdown_artifact is None or create_link_artifact is None:
        return

    summary_file = Path(summary_file)
    events_file = Path(events_file)

    summary_data: dict[str, Any] = {}
    try:
        if summary_file.exists():
            summary_data = json.loads(summary_file.read_text(encoding="utf-8"))
    except Exception:
        summary_data = {}

    counts = summary_data.get("counts", {}) if isinstance(summary_data, dict) else {}
    context = summary_data.get("context", {}) if isinstance(summary_data, dict) else {}
    md_lines = [
        f"## Pipeline run: {run_name}",
        "",
        "### Counts",
        f"- events: {counts.get('events', 0)}",
        f"- assets: {counts.get('assets', 0)}",
        f"- commands: {counts.get('commands', 0)}",
        f"- phases: {counts.get('phases', 0)}",
        f"- failures: {counts.get('failures', 0)}",
    ]
    if context:
        md_lines.extend(
            [
                "",
                "### Context",
                f"- mode: {context.get('mode', '')}",
                f"- qc_tool: {context.get('qc_tool', '')}",
                f"- contamination_tool: {context.get('contamination_tool', '')}",
                f"- thread_budget: {context.get('thread_budget', '')}",
            ]
        )
    markdown_payload = "\n".join(md_lines)

    try:
        create_markdown_artifact(
            markdown=markdown_payload,
            description=f"Pipeline observability summary for {run_name}",
        )
    except Exception:
        pass

    link_targets: list[tuple[str, Path]] = [
        ("events.jsonl", events_file),
        ("run_summary.json", summary_file),
    ]
    for p in extra_paths or []:
        pp = Path(p)
        link_targets.append((pp.name, pp))

    seen: set[str] = set()
    for link_text, path in link_targets:
        if not path.exists():
            continue
        resolved = str(path.resolve())
        if resolved in seen:
            continue
        seen.add(resolved)
        try:
            create_link_artifact(
                link=path.resolve().as_uri(),
                link_text=link_text,
                description=f"Pipeline output for {run_name}",
            )
        except Exception:
            pass
