from __future__ import annotations

import shutil
from pathlib import Path
from typing import Any  # used by run_multiqc signature (_qc_tasks: list[Any])

from prefect import task, get_run_logger
from prefect.futures import PrefectFutureList

# Lets MultiQC pick up Bracken `-w` reports (see multiqc_config.yaml).
MULTIQC_PROJECT_CONFIG = Path(__file__).resolve().parent / "multiqc_config.yaml"

from demux_pipeline.demux import DEMUX_FASTQ_OUTDIR_NAME, parse_fastq
from demux_pipeline.models import Sample
from demux_pipeline.process import run_command
from demux_pipeline.observability import record_asset

def _ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def _sample_project_dir(root: Path, sample: Sample) -> Path:
    if not sample.project:
        return root
    return root.parent / DEMUX_FASTQ_OUTDIR_NAME / sample.project / "qc" / root.name


def _project_names_from_demux_output(outdir: Path) -> list[str]:
    demux_root = outdir / DEMUX_FASTQ_OUTDIR_NAME
    if not demux_root.exists():
        return []
    projects = []
    for path in sorted(demux_root.iterdir()):
        if path.is_dir() and any(
            p.is_file() and parse_fastq(p) for p in path.rglob("*")
        ):
            projects.append(path.name)
    return projects


@task
def run_multiqc(
    outdir: Path,
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
        outdir / DEMUX_FASTQ_OUTDIR_NAME,
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

    project_names = _project_names_from_demux_output(outdir)
    if not project_names:
        return

    for project in project_names:
        project_inputs = [
            str(tool_root)
            for tool in ("fastqc", "fastp", "falco", "contamination")
            for tool_root in [outdir / DEMUX_FASTQ_OUTDIR_NAME / project / "qc" / tool]
            if tool_root.exists()
        ]
        if not project_inputs:
            continue
        project_out = outdir / DEMUX_FASTQ_OUTDIR_NAME / project / "qc" / "multiqc"
        if project_out.exists():
            shutil.rmtree(project_out)
        _ensure_dir(project_out)
        project_cmd: list[str] = ["multiqc"]
        if MULTIQC_PROJECT_CONFIG.is_file():
            project_cmd.extend(["-c", str(MULTIQC_PROJECT_CONFIG)])
        project_cmd.extend(["-o", str(project_out), *project_inputs])
        logger.info("multiqc project %s: %s", project, " ".join(project_cmd))
        run_command(project_cmd, step="multiqc", tool="multiqc", capture_err_tail=80)
        record_asset(
            project_out,
            step="multiqc",
            tool="multiqc",
            kind="directory",
            metadata={"project": project},
        )
        project_report = project_out / "multiqc_report.html"
        if project_report.exists():
            record_asset(
                project_report,
                step="multiqc",
                tool="multiqc",
                kind="report_html",
                metadata={"project": project},
            )


@task(tags=["qc"])
def run_fastqc(
    sample: Sample,
    outdir: Path,
    threads: int,
) -> None:
    logger = get_run_logger()
    fastqc_dir = _sample_project_dir(outdir / "fastqc", sample)
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

    fastp_dir = _sample_project_dir(outdir / "fastp", sample)
    tmp_dir = _sample_project_dir(outdir / "fastp_passthrough", sample)
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

    logger.info("fastp (QC stage): %s", " ".join(cmd))
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

        falco_dir = _sample_project_dir(outdir / "falco", sample) / f"{sample.name}_{read}"
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
