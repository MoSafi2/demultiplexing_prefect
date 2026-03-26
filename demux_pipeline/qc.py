from __future__ import annotations

import subprocess
import shutil
from pathlib import Path
from typing import Any, Tuple, cast

from prefect import task, get_run_logger, flow
from prefect.futures import PrefectFutureList
from prefect.task_runners import ThreadPoolTaskRunner

# Lets MultiQC pick up Bracken `-w` reports (see multiqc_config.yaml).
MULTIQC_PROJECT_CONFIG = Path(__file__).resolve().parent / "multiqc_config.yaml"

from demux import BCL_CONVERT_OUTDIR_NAME
from models import Sample


def _ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def _allocate_sample_parallelism(thread_budget: int, num_samples: int) -> Tuple[int, int]:
    """
    Allocate concurrent sample tasks (C) and threads per tool invocation (T) so that
    C * T <= thread_budget.

    This controls:
    - **max_workers**: how many samples run concurrently
    - **per_task_threads**: how many threads each tool invocation should use
    """
    if thread_budget < 1:
        raise SystemExit("thread budget must be at least 1")
    if num_samples < 1:
        raise SystemExit("no samples provided")

    max_workers = min(num_samples, max(1, thread_budget))
    per_task_threads = max(1, thread_budget // max_workers)
    return max_workers, per_task_threads


def _run(cmd: list[str]) -> None:
    logger = get_run_logger()

    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
    )

    for line in proc.stdout or []:
        logger.info(line.rstrip())

    for line in proc.stderr or []:
        # Many QC CLIs (fastqc, fastp, multiqc, falco) write normal progress to stderr.
        logger.info(line.rstrip())

    proc.wait()

    if proc.returncode != 0:
        raise RuntimeError(f"Command failed: {' '.join(cmd)}")


@task
def run_multiqc(
    outdir: Path,
    _qc_tasks: list[Any],
    *,
    include_contamination: bool = False,
) -> None:
    """
    Collect QC results into a single MultiQC report.

    This is intentionally tolerant:
    - if `multiqc` is missing from PATH, we log a warning and skip.
    - we only pass input directories that actually exist under `outdir`.
    - contamination outputs are included only when `include_contamination` is true.
    """
    logger = get_run_logger()

    if shutil.which("multiqc") is None:
        logger.warning("multiqc not found on PATH; skipping multiqc collection.")
        return

    multiqc_out = outdir / "multiqc"
    if multiqc_out.exists():
        shutil.rmtree(multiqc_out)
    _ensure_dir(multiqc_out)

    # Feed multiqc only directories that exist to avoid errors.
    # (multiqc can still detect supported modules under these.)
    candidate_dirs = [
        outdir / BCL_CONVERT_OUTDIR_NAME,
        outdir / "fastqc",
        outdir / "fastp",
        outdir / "falco",
    ]
    if include_contamination:
        candidate_dirs.append(outdir / "contamination")
    inputs = [str(p) for p in candidate_dirs if p.exists()]
    if not inputs:
        logger.warning(
            "No QC output directories found under %s; skipping multiqc.", outdir
        )
        return

    cmd: list[str] = ["multiqc"]
    if MULTIQC_PROJECT_CONFIG.is_file():
        cmd.extend(["-c", str(MULTIQC_PROJECT_CONFIG)])
    cmd.extend(["-o", str(multiqc_out), *inputs])
    logger.info("multiqc: %s", " ".join(cmd))
    _run(cmd)


@task(tags=["qc"])
def run_fastqc(sample: Sample, outdir: Path, threads: int) -> None:
    logger = get_run_logger()
    fastqc_dir = outdir / "fastqc"
    _ensure_dir(fastqc_dir)
    if threads < 1:
        raise ValueError("run_fastqc threads must be >= 1")
    for fastq_path in sample.get_paths():
        cmd = [
            "fastqc",
            "--threads",
            str(threads),
            "--outdir",
            str(fastqc_dir),
            str(fastq_path),
        ]

        logger.info("fastqc: %s", " ".join(cmd))
        _run(cmd)


@task(tags=["qc"])
def run_fastp(sample: Sample, outdir: Path, threads: int) -> Path:
    logger = get_run_logger()

    fastp_dir = outdir / "fastp"
    tmp_dir = outdir / "fastp_passthrough"
    _ensure_dir(fastp_dir)
    _ensure_dir(tmp_dir)

    html_path = fastp_dir / f"{sample.name}.html"
    json_path = fastp_dir / f"{sample.name}.json"

    if sample.paired:
        out_r1 = tmp_dir / f"{sample.name}_R1.fastq.gz"
        out_r2 = tmp_dir / f"{sample.name}_R2.fastq.gz"

        cmd = [
            "fastp",
            "-i",
            str(sample.r1),
            "-I",
            str(sample.r2),
            "-o",
            str(out_r1),
            "-O",
            str(out_r2),
            "--thread",
            str(threads),
            "--html",
            str(html_path),
            "--json",
            str(json_path),
            # ---- disable all modifications ----
            "--disable_length_filtering",
            "--disable_adapter_trimming",
            "--disable_quality_filtering",
            "--disable_trim_poly_g",
        ]
    else:
        out_r1 = tmp_dir / f"{sample.name}.fastq.gz"

        cmd = [
            "fastp",
            "-i",
            str(sample.r1),
            "-o",
            str(out_r1),
            "--thread",
            str(threads),
            "--html",
            str(html_path),
            "--json",
            str(json_path),
            # ---- disable all modifications ----
            "--disable_length_filtering",
            "--disable_adapter_trimming",
            "--disable_quality_filtering",
            "--disable_trim_poly_g",
        ]

    logger.info("fastp (QC-only): %s", " ".join(cmd))
    _run(cmd)

    return out_r1


@task(tags=["qc"])
def run_falco(sample: Sample, outdir: Path) -> None:
    logger = get_run_logger()

    for read, path in [("R1", sample.r1), ("R2", sample.r2)]:
        if path is None:
            continue

        falco_dir = outdir / "falco" / f"{sample.name}_{read}"
        _ensure_dir(falco_dir)

        cmd = [
            "falco",
            "--outdir",
            str(falco_dir),
            str(path),
        ]

        logger.info("falco: %s", " ".join(cmd))
        _run(cmd)


def submit_qc_tasks(
    samples: list[Sample], qc_tool: str, outdir: Path, per_task_threads: int
) -> PrefectFutureList:
    """Submit mapped QC tasks for all samples."""
    qc_tool_norm = qc_tool.lower().strip()
    n = len(samples)
    outdirs = [outdir] * n
    dispatch = {
        "fastqc": lambda: run_fastqc.map(
            sample=samples,
            outdir=outdirs,
            threads=[min(per_task_threads, 2 if s.paired else 1) for s in samples],
        ),
        "fastp": lambda: run_fastp.map(
            sample=samples,
            outdir=outdirs,
            threads=[per_task_threads] * n,
        ),
        "falco": lambda: run_falco.map(sample=samples, outdir=outdirs),
    }
    if qc_tool_norm not in dispatch:
        raise SystemExit(f"Unknown QC tool: {qc_tool}")
    return dispatch[qc_tool_norm]()


@flow(name="qc_phase", log_prints=True)
def qc_phase(
    samples: list[Sample],
    qc_tool: str,
    outdir: Path,
    thread_budget: int,
) -> PrefectFutureList:
    """
    Run QC in parallel under a thread budget.

    Returns a `PrefectFutureList` of the mapped QC tasks so callers can await
    completion by calling `.result()` on the returned object.
    """
    logger = get_run_logger()
    max_workers, per_task_threads = _allocate_sample_parallelism(
        thread_budget, len(samples)
    )
    logger.info(
        "QC phase: max concurrent samples=%s, per-sample tool threads=%s (budget=%s)",
        max_workers,
        per_task_threads,
        thread_budget,
    )
    runner = ThreadPoolTaskRunner(max_workers=max_workers)

    @flow(log_prints=True)
    def _qc_submit() -> PrefectFutureList:
        return submit_qc_tasks(samples, qc_tool, outdir, per_task_threads)

    return _qc_submit.with_options(task_runner=cast(Any, runner))()
