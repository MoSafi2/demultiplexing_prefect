from __future__ import annotations

from pathlib import Path
from typing import Any, Callable, List, Literal, Tuple, cast

from prefect import flow, get_run_logger
from prefect.futures import PrefectFutureList
from prefect.task_runners import ThreadPoolTaskRunner

from models import Sample
from qc import run_multiqc, submit_qc_tasks
from contamination import submit_contamination_tasks
from demux import BCL_CONVERT_OUTDIR_NAME, demux_bcl, _samples_from_fastq_dir


def _allocate_sample_parallelism(
    thread_budget: int, num_samples: int
) -> Tuple[int, int]:
    """
    Allocate concurrent sample tasks (C) and threads per tool invocation (T) so that
    C * T <= thread_budget (with integer floor division for T).

    Used for fastp, falco concurrency, kraken/fastq_screen, and as the base T for
    fastqc before per-sample caps (paired: min(T, 2)).
    """
    if thread_budget < 1:
        raise SystemExit("thread budget must be at least 1")
    n = num_samples
    if n < 1:
        raise SystemExit("internal error: allocate parallelism with zero samples")
    max_concurrent = min(n, max(1, thread_budget))
    per_task_threads = max(1, thread_budget // max_concurrent)
    return max_concurrent, per_task_threads


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


def _run_mapped_phase(
    *,
    phase_name: str,
    thread_budget: int,
    sample_count: int,
    submit_map: Callable[[int], PrefectFutureList],
) -> None:
    logger = get_run_logger()
    max_workers, per_task_threads = _allocate_sample_parallelism(thread_budget, sample_count)
    logger.info(
        "%s phase: max concurrent samples=%s, per-sample tool threads=%s (budget=%s)",
        phase_name,
        max_workers,
        per_task_threads,
        thread_budget,
    )
    runner = ThreadPoolTaskRunner(max_workers=max_workers)

    @flow(log_prints=True)
    def _phase_flow() -> None:
        submit_map(per_task_threads).result()

    _phase_flow.with_options(task_runner=cast(Any, runner))()


def _run_qc_phase(
    samples: List[Sample],
    qc_tool: str,
    outdir_path: Path,
    thread_budget: int,
) -> None:
    _run_mapped_phase(
        phase_name="QC",
        thread_budget=thread_budget,
        sample_count=len(samples),
        submit_map=lambda per_task_threads: submit_qc_tasks(
            samples=samples,
            qc_tool=qc_tool,
            outdir=outdir_path,
            per_task_threads=per_task_threads,
        ),
    )


def _run_contamination_phase(
    samples: List[Sample],
    contamination_tool: Literal["kraken", "kraken_bracken", "fastq_screen"],
    outdir_path: Path,
    thread_budget: int,
    kraken_db: Path | None = None,
    bracken_db: Path | None = None,
    fastq_screen_conf: Path | None = None,
    read_length: int = 150,
) -> None:
    _run_mapped_phase(
        phase_name="Contamination",
        thread_budget=thread_budget,
        sample_count=len(samples),
        submit_map=lambda per_task_threads: submit_contamination_tasks(
            samples=samples,
            contamination_tool=contamination_tool,
            outdir=outdir_path,
            per_task_threads=per_task_threads,
            kraken_db=kraken_db,
            bracken_db=bracken_db,
            fastq_screen_conf=fastq_screen_conf,
            read_length=read_length,
        ),
    )


@flow(name="qc-only", log_prints=True)
def qc_only_pipeline(
    *,
    qc_tool: str = "falco",
    thread_budget: int = 4,
    outdir: Path | str,
    manifest_tsv: Path | None = None,
    in_fastq_dir: Path | None = None,
    contamination_tool: Literal["kraken", "kraken_bracken", "fastq_screen"] | None = None,
    kraken_db: Path | None = None,
    bracken_db: Path | None = None,
    fastq_screen_conf: Path | None = None,
    read_length: int = 150,
) -> None:
    outdir_path = Path(outdir)
    samples = _discover_samples(manifest_tsv=manifest_tsv, in_fastq_dir=in_fastq_dir)

    if not samples:
        raise SystemExit("No samples found to process.")

    _run_qc_phase(samples, qc_tool, outdir_path, thread_budget)
    if contamination_tool:
        _run_contamination_phase(
            samples,
            contamination_tool,
            outdir_path,
            thread_budget,
            kraken_db=kraken_db,
            bracken_db=bracken_db,
            fastq_screen_conf=fastq_screen_conf,
            read_length=read_length,
        )
    run_multiqc(outdir_path, [], include_contamination=bool(contamination_tool))


# -------------------------
# Main pipeline flow
# -------------------------
# Demux uses bcl-convert as a single subprocess; it may use more CPU threads internally
# than `thread_budget`. The budget applies to QC and contamination parallel stages.


@flow(name="demux-qc", log_prints=True)
def demux_qc_pipeline(
    *,
    bcl_dir: Path,
    samplesheet: Path,
    qc_tool: str = "falco",
    thread_budget: int = 4,
    outdir: Path | str,
    contamination_tool: Literal["kraken", "kraken_bracken", "fastq_screen"] | None = None,
    kraken_db: Path | None = None,
    bracken_db: Path | None = None,
    fastq_screen_conf: Path | None = None,
    read_length: int = 150,
) -> None:
    outdir_path = Path(outdir)

    # Demultiplex first
    demux_bcl(bcl_dir=bcl_dir, samplesheet=samplesheet, outdir=outdir_path)
    samples = _discover_samples(demux_dir=outdir_path / BCL_CONVERT_OUTDIR_NAME)

    if not samples:
        raise SystemExit("No samples found after demux.")

    _run_qc_phase(samples, qc_tool, outdir_path, thread_budget)
    if contamination_tool:
        _run_contamination_phase(
            samples,
            contamination_tool,
            outdir_path,
            thread_budget,
            kraken_db=kraken_db,
            bracken_db=bracken_db,
            fastq_screen_conf=fastq_screen_conf,
            read_length=read_length,
        )
    run_multiqc(outdir_path, [], include_contamination=bool(contamination_tool))
