from __future__ import annotations

from pathlib import Path
from typing import Any, Iterable, List

from prefect import flow, get_run_logger
from prefect.task_runners import ThreadPoolTaskRunner

from demux_pipeline.models import Sample
from demux_pipeline.qc import run_multiqc, submit_qc_tasks
from demux_pipeline.contamination import submit_contamination_tasks
from demux_pipeline.demux import (
    BCL_CONVERT_OUTDIR_NAME,
    _samples_from_fastq_dir,
    _write_samples_tsv,
    demux_bcl,
)
from demux_pipeline.observability import (
    create_run_table,
    default_run_name,
    init_run_tracking,
    reset_observer,
    slugify_run_name,
)


def _normalize_tools(
    tools: str | Iterable[str] | None,
    *,
    default: str | None = None,
) -> list[str]:
    if tools is None:
        return [default] if default else []
    if isinstance(tools, str):
        parts = [p.strip().lower() for p in tools.split(",")]
    else:
        parts = [str(p).strip().lower() for p in tools]
    normalized = [p for p in parts if p]
    if not normalized and default:
        return [default]
    deduped: list[str] = []
    seen: set[str] = set()
    for tool in normalized:
        if tool not in seen:
            deduped.append(tool)
            seen.add(tool)
    return deduped


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


def _discover_samples(
    demux_dir: Path | None = None,
) -> List[Sample]:
    if demux_dir:
        return _samples_from_fastq_dir(demux_dir, include_undetermined=False)
    else:
        raise SystemExit("No demultiplexed output provided for sample discovery.")


def _write_discovered_manifest(samples: list[Sample], outdir: Path) -> Path:
    manifest_path = outdir / "samples.tsv"
    _write_samples_tsv(samples, manifest_path)
    return manifest_path


def _resolve_run_name(
    *,
    run_name: str | None = None,
    bcl_dir: Path | None = None,
    qc_tool: str = "falco",
    **_: Any,
) -> str:
    mode = "demux"
    qc_tools = _normalize_tools(qc_tool, default="falco")
    return slugify_run_name(run_name or "") or default_run_name(
        mode=mode, qc_tool="+".join(qc_tools)
    )


@flow(name="demux-pipeline", flow_run_name=_resolve_run_name, log_prints=True)
def demux_pipeline(
    *,
    bcl_dir: Path,
    samplesheet: Path,
    # Common
    qc_tool: str | list[str] = "falco",
    thread_budget: int = 4,
    outdir: Path | str,
    run_name: str | None = None,
    contamination_tool: str | list[str] | None = None,
    kraken_db: Path | None = None,
    bracken_db: Path | None = None,
    fastq_screen_conf: Path | None = None,
    read_length: int = 150,
) -> None:
    mode = "demux"
    qc_tools = _normalize_tools(qc_tool, default="falco")
    contamination_tools = [
        t for t in _normalize_tools(contamination_tool) if t != "none"
    ]
    qc_label = "+".join(qc_tools)
    contamination_label = (
        "+".join(contamination_tools) if contamination_tools else None
    )
    outdir_path = Path(outdir)
    resolved = slugify_run_name(run_name or "") or default_run_name(
        mode=mode, qc_tool=qc_label
    )

    ctx, observer = init_run_tracking(
        outdir_path,
        resolved,
        mode,
        qc_label,
        contamination_label,
        thread_budget,
        bcl_dir,
        samplesheet,
    )
    logger = get_run_logger()
    logger.info("run_name=%s tracking=%s", resolved, observer.events_file.parent)

    try:
        # --- Stage 1: Demux ---
        observer.phase_started("demux")
        demux_bcl(
            bcl_dir=bcl_dir,
            samplesheet=samplesheet,
            outdir=outdir_path,
        )
        observer.phase_finished("demux")
        samples = _discover_samples(demux_dir=outdir_path / BCL_CONVERT_OUTDIR_NAME)

        if not samples:
            raise SystemExit("No samples found.")
        samples_manifest = _write_discovered_manifest(samples, outdir_path)
        logger.info("samples manifest written to %s", samples_manifest)

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
            qc_futures = [
                submit_qc_tasks(samples, tool, outdir_path, per_task_threads)
                for tool in qc_tools
            ]
            futures = qc_futures[0]
            for future_list in qc_futures[1:]:
                futures.extend(future_list)

            contam_futures = None
            if contamination_tools:
                observer.phase_started("contamination")
                contam_batches = [
                    submit_contamination_tasks(
                        samples,
                        tool,
                        outdir_path,
                        per_task_threads,
                        kraken_db=kraken_db,
                        bracken_db=bracken_db,
                        fastq_screen_conf=fastq_screen_conf,
                        read_length=read_length,
                    )
                    for tool in contamination_tools
                ]
                contam_futures = contam_batches[0]
                for future_list in contam_batches[1:]:
                    contam_futures.extend(future_list)

            if contam_futures is not None:
                futures.extend(contam_futures)

            futures.result()
            observer.phase_finished("qc")
            if contam_futures is not None:
                observer.phase_finished("contamination")

        # --- Stage 4: MultiQC ---
        observer.phase_started("multiqc")
        run_multiqc(
            outdir_path,
            include_contamination=bool(contamination_tools),
        )
        observer.phase_finished("multiqc")

        observer.pipeline_finished()
        summary = observer.finalize_summary(context=ctx)
        create_run_table(summary)
    finally:
        # Avoid leaking observer state when multiple runs happen in one process.
        reset_observer()
