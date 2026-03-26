from __future__ import annotations

from pathlib import Path
from typing import List, Literal

from prefect import flow, get_run_logger
from prefect.task_runners import ThreadPoolTaskRunner

from models import Sample
from qc import run_multiqc, submit_qc_tasks
from contamination import submit_contamination_tasks
from demux import BCL_CONVERT_OUTDIR_NAME, demux_bcl, _samples_from_fastq_dir
from observability import (
    Observer,
    RunContext,
    default_run_name,
    events_path as _events_path,
    slugify_run_name,
    summary_path as _summary_path,
    utc_now_iso,
)


def _allocate_sample_parallelism(
    thread_budget: int, num_samples: int
) -> tuple[int, int]:
    """
    Allocate concurrent sample tasks (C) and threads per tool invocation (T) so that
    C * T <= thread_budget.
    """
    if thread_budget < 1:
        raise SystemExit("thread budget must be at least 1")
    if num_samples < 1:
        raise SystemExit("no samples provided")
    max_workers = min(num_samples, max(1, thread_budget))
    per_task_threads = max(1, thread_budget // max_workers)
    return max_workers, per_task_threads


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


def _discover_samples(
    manifest_tsv: Path | None = None,
    in_fastq_dir: Path | None = None,
    demux_dir: Path | None = None,
) -> List[Sample]:
    if manifest_tsv:
        return load_samples_from_manifest(manifest_tsv)
    elif in_fastq_dir:
        in_fastq_dir = Path(in_fastq_dir)
        if not in_fastq_dir.exists() or not in_fastq_dir.is_dir():
            raise SystemExit(f"FASTQ directory not found: {in_fastq_dir}")
        return _samples_from_fastq_dir(in_fastq_dir)
    elif demux_dir:
        return _samples_from_fastq_dir(demux_dir, include_undetermined=False)
    else:
        raise SystemExit("No input provided for sample discovery.")


@flow(name="demux-pipeline", log_prints=True)
def demux_pipeline(
    *,
    # Demux inputs — omit both to run QC-only mode
    bcl_dir: Path | None = None,
    samplesheet: Path | None = None,
    # QC-only inputs (used when bcl_dir is None)
    manifest_tsv: Path | None = None,
    in_fastq_dir: Path | None = None,
    # Common
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
    mode = "demux" if bcl_dir else "qc"
    outdir_path = Path(outdir)
    resolved = slugify_run_name(run_name or "") or default_run_name(
        mode=mode, qc_tool=qc_tool
    )

    events_file = _events_path(outdir_path, resolved)
    summary_file = _summary_path(outdir_path, resolved)
    ctx = RunContext(
        run_name=resolved,
        outdir=str(outdir_path),
        mode=mode,
        qc_tool=qc_tool,
        contamination_tool=contamination_tool,
        thread_budget=thread_budget,
        started_at=utc_now_iso(),
        inputs={
            "bcl_dir": str(bcl_dir) if bcl_dir else None,
            "samplesheet": str(samplesheet) if samplesheet else None,
            "manifest_tsv": str(manifest_tsv) if manifest_tsv else None,
            "in_fastq_dir": str(in_fastq_dir) if in_fastq_dir else None,
        },
    )
    observer = Observer(
        run_name=resolved, events_file=events_file, summary_file=summary_file
    )
    observer.pipeline_started(ctx)
    logger = get_run_logger()
    logger.info("run_name=%s tracking=%s", resolved, events_file.parent)

    # --- Stage 1: Demux (optional) ---
    if bcl_dir:
        observer.phase_started("demux")
        demux_bcl(
            bcl_dir=bcl_dir,
            samplesheet=samplesheet,
            outdir=outdir_path,
            observer=observer,
        )
        observer.phase_finished("demux")
        samples = _discover_samples(demux_dir=outdir_path / BCL_CONVERT_OUTDIR_NAME)
    else:
        samples = _discover_samples(
            manifest_tsv=manifest_tsv, in_fastq_dir=in_fastq_dir
        )

    if not samples:
        raise SystemExit("No samples found.")

    max_workers, per_task_threads = _allocate_sample_parallelism(
        thread_budget, len(samples)
    )
    logger.info(
        "max_workers=%s per_task_threads=%s (budget=%s)",
        max_workers,
        per_task_threads,
        thread_budget,
    )

    # --- Stages 2 & 3: QC + Contamination (concurrent) ---
    with ThreadPoolTaskRunner(max_workers=max_workers):
        observer.phase_started("qc")
        qc_futures = submit_qc_tasks(
            samples, qc_tool, outdir_path, per_task_threads, observer=observer
        )

        contam_futures = None
        if contamination_tool:
            observer.phase_started("contamination")
            contam_futures = submit_contamination_tasks(
                samples,
                contamination_tool,
                outdir_path,
                per_task_threads,
                observer=observer,
                kraken_db=kraken_db,
                bracken_db=bracken_db,
                fastq_screen_conf=fastq_screen_conf,
                read_length=read_length,
            )

        qc_futures.result()
        observer.phase_finished("qc")
        if contam_futures is not None:
            contam_futures.result()
            observer.phase_finished("contamination")

    # --- Stage 4: MultiQC ---
    observer.phase_started("multiqc")
    run_multiqc(
        outdir_path,
        [],
        include_contamination=bool(contamination_tool),
        observer=observer,
    )
    observer.phase_finished("multiqc")

    observer.pipeline_finished()
    observer.finalize_summary(context=ctx)
    observer.asset_created(
        path=events_file, step="tracking", tool="pipeline", kind="events_jsonl"
    )
    observer.asset_created(
        path=summary_file, step="tracking", tool="pipeline", kind="run_summary_json"
    )
    observer.publish_prefect_artifacts(
        extra_paths=[outdir_path / "multiqc" / "multiqc_report.html"]
    )
