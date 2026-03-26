from __future__ import annotations

from pathlib import Path
from typing import List, Literal

from prefect import flow, get_run_logger, task

from models import Sample
from qc import qc_phase, run_multiqc
from contamination import contamination_phase
from demux import BCL_CONVERT_OUTDIR_NAME, demux_bcl, _samples_from_fastq_dir
from observability import (
    RunContext,
    append_event,
    default_run_name,
    emit_prefect_asset_events_from_local_log,
    events_path as _events_path,
    finalize_run_summary,
    publish_prefect_observability_artifacts,
    slugify_run_name,
    summary_path as _summary_path,
    utc_now_iso,
)


def _split_phase_budgets(thread_budget: int) -> tuple[int, int]:
    """
    Split a global budget into QC + contamination budgets.

    Policy: favor contamination.
    - qc_budget = max(1, floor(thread_budget / 3))
    - contamination_budget = thread_budget - qc_budget
    """
    if thread_budget < 1:
        raise SystemExit("thread budget must be at least 1")
    qc_budget = max(1, thread_budget // 3)
    contamination_budget = thread_budget - qc_budget
    if thread_budget >= 2:
        contamination_budget = max(1, contamination_budget)
    return qc_budget, contamination_budget


@task
def _run_qc_phase(
    samples: List[Sample],
    qc_tool: str,
    outdir: Path,
    thread_budget: int,
    events_file: str | None = None,
    run_name: str | None = None,
) -> None:
    qc_phase(
        samples,
        qc_tool,
        outdir,
        thread_budget,
        events_file=events_file,
        run_name=run_name,
    )


@task
def _run_contamination_phase(
    samples: List[Sample],
    contamination_tool: Literal["kraken", "kraken_bracken", "fastq_screen"],
    outdir: Path,
    thread_budget: int,
    events_file: str | None = None,
    run_name: str | None = None,
    kraken_db: Path | None = None,
    bracken_db: Path | None = None,
    fastq_screen_conf: Path | None = None,
    read_length: int = 150,
) -> None:
    contamination_phase(
        samples,
        contamination_tool,
        outdir,
        thread_budget,
        events_file=events_file,
        run_name=run_name,
        kraken_db=kraken_db,
        bracken_db=bracken_db,
        fastq_screen_conf=fastq_screen_conf,
        read_length=read_length,
    )


def _run_qc_and_optional_contamination(
    *,
    samples: List[Sample],
    qc_tool: str,
    outdir: Path,
    thread_budget: int,
    events_file: Path,
    run_name: str,
    contamination_tool: Literal["kraken", "kraken_bracken", "fastq_screen"] | None,
    kraken_db: Path | None = None,
    bracken_db: Path | None = None,
    fastq_screen_conf: Path | None = None,
    read_length: int = 150,
) -> None:
    logger = get_run_logger()
    if not contamination_tool:
        append_event(
            events_file,
            {"type": "phase_started", "phase": "qc", "run_name": run_name},
        )
        qc_phase(
            samples,
            qc_tool,
            outdir,
            thread_budget,
            events_file=str(events_file),
            run_name=run_name,
        )
        append_event(
            events_file,
            {"type": "phase_finished", "phase": "qc", "run_name": run_name},
        )
        return

    if thread_budget == 1:
        logger.info(
            "thread_budget=1 with contamination enabled; running QC and contamination sequentially."
        )
        append_event(
            events_file,
            {"type": "phase_started", "phase": "qc", "run_name": run_name},
        )
        qc_phase(
            samples,
            qc_tool,
            outdir,
            thread_budget,
            events_file=str(events_file),
            run_name=run_name,
        )
        append_event(
            events_file,
            {"type": "phase_finished", "phase": "qc", "run_name": run_name},
        )
        append_event(
            events_file,
            {"type": "phase_started", "phase": "contamination", "run_name": run_name},
        )
        contamination_phase(
            samples,
            contamination_tool,
            outdir,
            thread_budget,
            events_file=str(events_file),
            run_name=run_name,
            kraken_db=kraken_db,
            bracken_db=bracken_db,
            fastq_screen_conf=fastq_screen_conf,
            read_length=read_length,
        )
        append_event(
            events_file,
            {"type": "phase_finished", "phase": "contamination", "run_name": run_name},
        )
        return

    qc_budget, contamination_budget = _split_phase_budgets(thread_budget)
    logger.info(
        "Concurrent phases: qc_budget=%s, contamination_budget=%s (global_budget=%s)",
        qc_budget,
        contamination_budget,
        thread_budget,
    )
    qc_future = _run_qc_phase.submit(
        samples=samples,
        qc_tool=qc_tool,
        outdir=outdir,
        thread_budget=qc_budget,
        events_file=str(events_file),
        run_name=run_name,
    )
    contamination_future = _run_contamination_phase.submit(
        samples=samples,
        contamination_tool=contamination_tool,
        outdir=outdir,
        thread_budget=contamination_budget,
        events_file=str(events_file),
        run_name=run_name,
        kraken_db=kraken_db,
        bracken_db=bracken_db,
        fastq_screen_conf=fastq_screen_conf,
        read_length=read_length,
    )
    qc_future.result()
    contamination_future.result()


# -------------------------
# Sample loading tasks
# -------------------------
def load_samples_from_manifest(manifest_tsv: Path) -> List[Sample]:
    logger = get_run_logger()
    manifest_tsv = Path(manifest_tsv)

    if not manifest_tsv.exists() or not manifest_tsv.is_file():
        raise SystemExit(f"Manifest TSV not found: {manifest_tsv}")

    samples: List[Sample] = []
    with manifest_tsv.open("r") as f:
        for line_num, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            parts = line.split("\t")
            if len(parts) != 2:
                raise SystemExit(
                    f"Invalid manifest line {line_num}: {line!r} (expected 2 columns)"
                )
            sample_name, fq_path_str = parts
            fq_path = Path(fq_path_str)
            if not fq_path.exists():
                raise SystemExit(f"FASTQ path not found: {fq_path}")
            samples.append(Sample(name=sample_name, r1=fq_path))

    if not samples:
        logger.warning("No samples loaded from manifest: %s", manifest_tsv)

    return sorted(samples, key=lambda s: s.name)


def load_samples_from_fastq_dir(in_fastq_dir: Path) -> List[Sample]:
    in_fastq_dir = Path(in_fastq_dir)
    if not in_fastq_dir.exists() or not in_fastq_dir.is_dir():
        raise SystemExit(f"FASTQ directory not found: {in_fastq_dir}")
    return _samples_from_fastq_dir(in_fastq_dir)


def _discover_samples(
    manifest_tsv: Path | None = None,
    in_fastq_dir: Path | None = None,
    demux_dir: Path | None = None,
) -> List[Sample]:
    if manifest_tsv:
        return load_samples_from_manifest(manifest_tsv)
    elif in_fastq_dir:
        return load_samples_from_fastq_dir(in_fastq_dir)
    elif demux_dir:
        return _samples_from_fastq_dir(demux_dir, include_undetermined=False)
    else:
        raise SystemExit("No input provided for sample discovery.")


@flow(name="qc-only", log_prints=True, flow_run_name="{run_name}")
def _qc_only_pipeline_flow(
    *,
    qc_tool: str = "falco",
    thread_budget: int = 4,
    outdir: Path | str,
    run_name: str | None = None,
    manifest_tsv: Path | None = None,
    in_fastq_dir: Path | None = None,
    contamination_tool: (
        Literal["kraken", "kraken_bracken", "fastq_screen"] | None
    ) = None,
    kraken_db: Path | None = None,
    bracken_db: Path | None = None,
    fastq_screen_conf: Path | None = None,
    read_length: int = 150,
) -> None:
    outdir_path = Path(outdir)
    resolved = slugify_run_name(run_name or "") or default_run_name(
        mode="qc", qc_tool=qc_tool
    )
    events_file = _events_path(outdir_path, resolved)
    summary_file = _summary_path(outdir_path, resolved)
    ctx = RunContext(
        run_name=resolved,
        outdir=str(outdir_path),
        mode="qc",
        qc_tool=qc_tool,
        contamination_tool=contamination_tool,
        thread_budget=thread_budget,
        started_at=utc_now_iso(),
        inputs={
            "manifest_tsv": str(manifest_tsv) if manifest_tsv else None,
            "in_fastq_dir": str(in_fastq_dir) if in_fastq_dir else None,
        },
    )
    append_event(events_file, {"type": "pipeline_started", "context": ctx.as_dict()})

    samples = _discover_samples(manifest_tsv=manifest_tsv, in_fastq_dir=in_fastq_dir)

    if not samples:
        raise SystemExit("No samples found to process.")

    logger = get_run_logger()
    logger.info("run_name=%s tracking=%s", resolved, events_file.parent)

    _run_qc_and_optional_contamination(
        samples=samples,
        qc_tool=qc_tool,
        outdir=outdir_path,
        thread_budget=thread_budget,
        events_file=events_file,
        run_name=resolved,
        contamination_tool=contamination_tool,
        kraken_db=kraken_db,
        bracken_db=bracken_db,
        fastq_screen_conf=fastq_screen_conf,
        read_length=read_length,
    )

    append_event(events_file, {"type": "phase_started", "phase": "multiqc", "run_name": resolved})
    run_multiqc(
        outdir_path,
        [],
        include_contamination=bool(contamination_tool),
        events_file=str(events_file),
        run_name=resolved,
    )
    append_event(events_file, {"type": "phase_finished", "phase": "multiqc", "run_name": resolved})
    append_event(events_file, {"type": "pipeline_finished", "run_name": resolved})
    finalize_run_summary(events_file=events_file, summary_file=summary_file, context=ctx)
    append_event(
        events_file,
        {
            "type": "asset_created",
            "run_name": resolved,
            "step": "tracking",
            "tool": "pipeline",
            "kind": "events_jsonl",
            "path": str(events_file),
        },
    )
    append_event(
        events_file,
        {
            "type": "asset_created",
            "run_name": resolved,
            "step": "tracking",
            "tool": "pipeline",
            "kind": "run_summary_json",
            "path": str(summary_file),
        },
    )
    emit_prefect_asset_events_from_local_log(events_file=events_file, run_name=resolved)
    publish_prefect_observability_artifacts(
        run_name=resolved,
        summary_file=summary_file,
        events_file=events_file,
        extra_paths=[outdir_path / "multiqc" / "multiqc_report.html"],
    )


def qc_only_pipeline(**kwargs):  # type: ignore[no-untyped-def]
    qc_tool = kwargs.get("qc_tool", "falco")
    raw_name = kwargs.get("run_name")
    resolved = slugify_run_name(raw_name or "") or default_run_name(
        mode="qc", qc_tool=qc_tool
    )
    kwargs["run_name"] = resolved
    return _qc_only_pipeline_flow(**kwargs)


# -------------------------
# Main pipeline flow
# -------------------------
# Demux uses bcl-convert as a single subprocess; it may use more CPU threads internally
# than `thread_budget`. The budget applies to QC and contamination parallel stages.


@flow(name="demux-qc", log_prints=True, flow_run_name="{run_name}")
def _demux_qc_pipeline_flow(
    *,
    bcl_dir: Path,
    samplesheet: Path,
    qc_tool: str = "falco",
    thread_budget: int = 4,
    outdir: Path | str,
    run_name: str | None = None,
    contamination_tool: (
        Literal["kraken", "kraken_bracken", "fastq_screen"] | None
    ) = None,
    kraken_db: Path | None = None,
    bracken_db: Path | None = None,
    fastq_screen_conf: Path | None = None,
    read_length: int = 150,
) -> None:
    outdir_path = Path(outdir)
    resolved = slugify_run_name(run_name or "") or default_run_name(
        mode="demux", qc_tool=qc_tool
    )
    events_file = _events_path(outdir_path, resolved)
    summary_file = _summary_path(outdir_path, resolved)
    ctx = RunContext(
        run_name=resolved,
        outdir=str(outdir_path),
        mode="demux",
        qc_tool=qc_tool,
        contamination_tool=contamination_tool,
        thread_budget=thread_budget,
        started_at=utc_now_iso(),
        inputs={
            "bcl_dir": str(bcl_dir),
            "samplesheet": str(samplesheet),
        },
    )
    append_event(events_file, {"type": "pipeline_started", "context": ctx.as_dict()})
    logger = get_run_logger()
    logger.info("run_name=%s tracking=%s", resolved, events_file.parent)

    # Demultiplex first
    append_event(events_file, {"type": "phase_started", "phase": "demux", "run_name": resolved})
    demux_bcl(
        bcl_dir=bcl_dir,
        samplesheet=samplesheet,
        outdir=outdir_path,
        events_file=str(events_file),
        run_name=resolved,
    )
    append_event(events_file, {"type": "phase_finished", "phase": "demux", "run_name": resolved})
    samples = _discover_samples(demux_dir=outdir_path / BCL_CONVERT_OUTDIR_NAME)

    if not samples:
        raise SystemExit("No samples found after demux.")

    _run_qc_and_optional_contamination(
        samples=samples,
        qc_tool=qc_tool,
        outdir=outdir_path,
        thread_budget=thread_budget,
        events_file=events_file,
        run_name=resolved,
        contamination_tool=contamination_tool,
        kraken_db=kraken_db,
        bracken_db=bracken_db,
        fastq_screen_conf=fastq_screen_conf,
        read_length=read_length,
    )

    append_event(events_file, {"type": "phase_started", "phase": "multiqc", "run_name": resolved})
    run_multiqc(
        outdir_path,
        [],
        include_contamination=bool(contamination_tool),
        events_file=str(events_file),
        run_name=resolved,
    )
    append_event(events_file, {"type": "phase_finished", "phase": "multiqc", "run_name": resolved})
    append_event(events_file, {"type": "pipeline_finished", "run_name": resolved})
    finalize_run_summary(events_file=events_file, summary_file=summary_file, context=ctx)
    append_event(
        events_file,
        {
            "type": "asset_created",
            "run_name": resolved,
            "step": "tracking",
            "tool": "pipeline",
            "kind": "events_jsonl",
            "path": str(events_file),
        },
    )
    append_event(
        events_file,
        {
            "type": "asset_created",
            "run_name": resolved,
            "step": "tracking",
            "tool": "pipeline",
            "kind": "run_summary_json",
            "path": str(summary_file),
        },
    )
    emit_prefect_asset_events_from_local_log(events_file=events_file, run_name=resolved)
    publish_prefect_observability_artifacts(
        run_name=resolved,
        summary_file=summary_file,
        events_file=events_file,
        extra_paths=[outdir_path / "multiqc" / "multiqc_report.html"],
    )


def demux_qc_pipeline(**kwargs):  # type: ignore[no-untyped-def]
    qc_tool = kwargs.get("qc_tool", "falco")
    raw_name = kwargs.get("run_name")
    resolved = slugify_run_name(raw_name or "") or default_run_name(
        mode="demux", qc_tool=qc_tool
    )
    kwargs["run_name"] = resolved
    return _demux_qc_pipeline_flow(**kwargs)
