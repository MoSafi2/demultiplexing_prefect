from __future__ import annotations

import subprocess
import shutil
from pathlib import Path
from typing import Any

from prefect import task, get_run_logger  # type: ignore[import-not-found]

try:
    from .models import Sample
except ImportError:  # pragma: no cover
    from models import Sample


def _ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


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


# TODO: Improve this function to collect QC for also bcl_convert output directories.
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

    cmd = ["multiqc", "-o", str(multiqc_out), *inputs]
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
