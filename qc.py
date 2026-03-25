from __future__ import annotations

import subprocess
import shutil
from pathlib import Path
from typing import Any, Optional

from prefect import flow, task, get_run_logger  # type: ignore[import-not-found]

from .models import Sample


def _ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def _run(cmd: list[str]) -> None:
    # Keep output in Prefect logs for easier debugging.
    subprocess.run(cmd, check=True)


@task
def run_multiqc(outdir: Path, _qc_tasks: list[Any]) -> None:
    """
    Collect QC results into a single MultiQC report.

    This is intentionally tolerant:
    - if `multiqc` is missing from PATH, we log a warning and skip.
    - we only pass input directories that actually exist under `outdir`.
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
        outdir / "fastqc",
        outdir / "fastp",
        outdir / "falco",
        outdir / "contamination",
    ]
    inputs = [str(p) for p in candidate_dirs if p.exists()]
    if not inputs:
        logger.warning("No QC output directories found under %s; skipping multiqc.", outdir)
        return

    cmd = ["multiqc", "-o", str(multiqc_out), *inputs]
    logger.info("multiqc: %s", " ".join(cmd))
    _run(cmd)


@task
def run_fastqc(sample_name: str, r1: Path, r2: Optional[Path], outdir: Path, threads: int) -> None:
    logger = get_run_logger()
    fastqc_dir = outdir / "fastqc"
    _ensure_dir(fastqc_dir)

    cmd = [
        "fastqc",
        "--threads",
        str(threads),
        "--outdir",
        str(fastqc_dir),
        str(r1),
    ]
    if r2 is not None:
        cmd.append(str(r2))

    logger.info("fastqc: %s", " ".join(cmd))
    _run(cmd)


@task
def run_fastp(sample_name: str, r1: Path, r2: Optional[Path], outdir: Path, threads: int) -> None:
    logger = get_run_logger()
    fastp_dir = outdir / "fastp"
    trimmed_dir = outdir / "fastp_trimmed"
    _ensure_dir(fastp_dir)
    _ensure_dir(trimmed_dir)

    html_path = fastp_dir / f"{sample_name}.html"
    json_path = fastp_dir / f"{sample_name}.json"

    if r2 is None:
        out_r1 = trimmed_dir / f"{sample_name}.trimmed.fastq.gz"
        cmd = [
            "fastp",
            "-i",
            str(r1),
            "-o",
            str(out_r1),
            "--thread",
            str(threads),
            "--html",
            str(html_path),
            "--json",
            str(json_path),
            "--report_title",
            sample_name,
        ]
    else:
        out_r1 = trimmed_dir / f"{sample_name}_R1.trimmed.fastq.gz"
        out_r2 = trimmed_dir / f"{sample_name}_R2.trimmed.fastq.gz"
        cmd = [
            "fastp",
            "-i",
            str(r1),
            "-I",
            str(r2),
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
            "--report_title",
            sample_name,
            "--detect_adapter_for_pe",
        ]

    logger.info("fastp: %s", " ".join(cmd))
    _run(cmd)


@task
def run_falco(sample_name: str, r1: Path, r2: Optional[Path], outdir: Path) -> None:
    """
    Run `falco` (FastQC emulation) for a sample.

    falco writes fixed filenames (e.g. `fastqc_report.html`) into the output directory,
    so we create a per-sample folder to avoid collisions between samples.
    """

    logger = get_run_logger()
    falco_root = outdir / "falco" / sample_name
    _ensure_dir(falco_root)

    cmd: list[str] = [
        "falco",
        "--outdir",
        str(falco_root),
        str(r1),
    ]
    if r2 is not None:
        cmd.append(str(r2))

    logger.info("falco: %s", " ".join(cmd))
    _run(cmd)


@flow(name="fastq-qc", log_prints=True)
def fastq_qc_flow(
    samples: list[Sample],
    outdir: Path | str,
    mode: str = "fastqc",
    threads: int = 4,
) -> None:
    """
    Run FASTQ QC for each sample.

    Outputs:
      - outdir/fastqc/
      - outdir/fastp/
      - outdir/fastp_trimmed/
      - outdir/falco/<sample_name>/ (when mode includes `falco`)
      - outdir/multiqc/ (MultiQC summary report)
    """

    outdir = Path(outdir)
    _ensure_dir(outdir)

    mode_norm = (mode or "fastqc").lower()
    run_fastqc_flag = mode_norm == "fastqc"
    run_fastp_flag = mode_norm == "fastp"
    run_falco_flag = mode_norm == "falco"

    # Capture per-sample task futures so MultiQC will only run once all of them complete.
    qc_tasks: list[Any] = []

    for sample in samples:
        if run_fastqc_flag:
            qc_tasks.append(run_fastqc(sample.name, sample.r1, sample.r2, outdir, threads))
        if run_fastp_flag:
            qc_tasks.append(run_fastp(sample.name, sample.r1, sample.r2, outdir, threads))
        if run_falco_flag:
            qc_tasks.append(run_falco(sample.name, sample.r1, sample.r2, outdir))

    # Single collection step at the end, across all generated reports.
    run_multiqc(outdir, qc_tasks)
