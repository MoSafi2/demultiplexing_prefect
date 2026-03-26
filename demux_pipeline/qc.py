from __future__ import annotations

import shutil
from pathlib import Path
from typing import Any  # used by run_multiqc signature (_qc_tasks: list[Any])

from prefect import task, get_run_logger
from prefect.futures import PrefectFutureList

# Lets MultiQC pick up Bracken `-w` reports (see multiqc_config.yaml).
MULTIQC_PROJECT_CONFIG = Path(__file__).resolve().parent / "multiqc_config.yaml"

from demux import BCL_CONVERT_OUTDIR_NAME
from models import Sample
from process import run_command
from observability import record_asset

def _ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


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
    run_command(cmd, step="multiqc", tool="multiqc", capture_err_tail=80)
    record_asset(multiqc_out, step="multiqc", tool="multiqc", kind="directory")
    report = multiqc_out / "multiqc_report.html"
    if report.exists():
        record_asset(report, step="multiqc", tool="multiqc", kind="report_html")


@task(tags=["qc"])
def run_fastqc(
    sample: Sample,
    outdir: Path,
    threads: int,
) -> None:
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
        run_command(cmd, step="qc", tool="fastqc", sample=sample.name, capture_err_tail=80)

    record_asset(fastqc_dir, step="qc", tool="fastqc", kind="directory", sample=sample.name)


@task(tags=["qc"])
def run_fastp(
    sample: Sample,
    outdir: Path,
    threads: int,
) -> Path:
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
    run_command(cmd, step="qc", tool="fastp", sample=sample.name, capture_err_tail=80)

    for p, kind in [(html_path, "report_html"), (json_path, "report_json"), (out_r1, "fastq")]:
        record_asset(p, step="qc", tool="fastp", kind=kind, sample=sample.name)
    if sample.paired:
        record_asset(out_r2, step="qc", tool="fastp", kind="fastq", sample=sample.name)

    return out_r1


@task(tags=["qc"])
def run_falco(
    sample: Sample,
    outdir: Path,
) -> None:
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
        run_command(cmd, step="qc", tool="falco", sample=sample.name, capture_err_tail=80)
        record_asset(
            falco_dir, step="qc", tool="falco", kind="directory",
            sample=sample.name, metadata={"read": read},
        )


def submit_qc_tasks(
    samples: list[Sample],
    qc_tool: str,
    outdir: Path,
    per_task_threads: int,
) -> PrefectFutureList:
    """Submit mapped QC tasks for all samples."""
    n = len(samples)
    tool = qc_tool.lower().strip()
    if tool == "fastqc":
        return run_fastqc.map(
            sample=samples,
            outdir=[outdir] * n,
            threads=[min(per_task_threads, 2 if s.paired else 1) for s in samples],
        )
    elif tool == "fastp":
        return run_fastp.map(
            sample=samples,
            outdir=[outdir] * n,
            threads=[per_task_threads] * n,
        )
    elif tool == "falco":
        return run_falco.map(
            sample=samples,
            outdir=[outdir] * n,
        )
    else:
        raise SystemExit(f"Unknown QC tool: {qc_tool}")


