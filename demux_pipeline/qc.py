from __future__ import annotations

import shutil
from pathlib import Path
from typing import Any, Callable, Tuple, cast

from prefect import task, get_run_logger, flow
from prefect.futures import PrefectFutureList
from prefect.task_runners import ThreadPoolTaskRunner

# Lets MultiQC pick up Bracken `-w` reports (see multiqc_config.yaml).
MULTIQC_PROJECT_CONFIG = Path(__file__).resolve().parent / "multiqc_config.yaml"

from demux import BCL_CONVERT_OUTDIR_NAME
from models import Sample
from process import run_command
from observability import append_event

QCSubmitter = Callable[[list[Sample], Path, int], PrefectFutureList]


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


@task
def run_multiqc(
    outdir: Path,
    _qc_tasks: list[Any],
    *,
    include_contamination: bool = False,
    events_file: str | None = None,
    run_name: str | None = None,
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
    run_command(
        cmd,
        events_file=events_file,
        run_name=run_name,
        step="multiqc",
        tool="multiqc",
        capture_err_tail=80,
    )

    if events_file:
        ev_path = Path(events_file)
        append_event(
            ev_path,
            {
                "type": "asset_created",
                "run_name": run_name,
                "step": "multiqc",
                "tool": "multiqc",
                "kind": "directory",
                "path": str(multiqc_out),
            },
        )
        report = multiqc_out / "multiqc_report.html"
        if report.exists():
            append_event(
                ev_path,
                {
                    "type": "asset_created",
                    "run_name": run_name,
                    "step": "multiqc",
                    "tool": "multiqc",
                    "kind": "report_html",
                    "path": str(report),
                },
            )


@task(tags=["qc"])
def run_fastqc(
    sample: Sample,
    outdir: Path,
    threads: int,
    events_file: str | None = None,
    run_name: str | None = None,
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
        run_command(
            cmd,
            events_file=events_file,
            run_name=run_name,
            step="qc",
            tool="fastqc",
            sample=sample.name,
            capture_err_tail=80,
        )

    if events_file:
        ev_path = Path(events_file)
        append_event(
            ev_path,
            {
                "type": "asset_created",
                "run_name": run_name,
                "step": "qc",
                "tool": "fastqc",
                "kind": "directory",
                "path": str(fastqc_dir),
                "sample": sample.name,
            },
        )


@task(tags=["qc"])
def run_fastp(
    sample: Sample,
    outdir: Path,
    threads: int,
    events_file: str | None = None,
    run_name: str | None = None,
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
    run_command(
        cmd,
        events_file=events_file,
        run_name=run_name,
        step="qc",
        tool="fastp",
        sample=sample.name,
        capture_err_tail=80,
    )

    if events_file:
        ev_path = Path(events_file)
        for p, kind in [
            (html_path, "report_html"),
            (json_path, "report_json"),
            (out_r1, "fastq"),
        ]:
            append_event(
                ev_path,
                {
                    "type": "asset_created",
                    "run_name": run_name,
                    "step": "qc",
                    "tool": "fastp",
                    "kind": kind,
                    "path": str(p),
                    "sample": sample.name,
                },
            )
        if sample.paired:
            append_event(
                ev_path,
                {
                    "type": "asset_created",
                    "run_name": run_name,
                    "step": "qc",
                    "tool": "fastp",
                    "kind": "fastq",
                    "path": str(out_r2),
                    "sample": sample.name,
                },
            )

    return out_r1


@task(tags=["qc"])
def run_falco(
    sample: Sample,
    outdir: Path,
    events_file: str | None = None,
    run_name: str | None = None,
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
        run_command(
            cmd,
            events_file=events_file,
            run_name=run_name,
            step="qc",
            tool="falco",
            sample=sample.name,
            capture_err_tail=80,
        )
        if events_file:
            ev_path = Path(events_file)
            append_event(
                ev_path,
                {
                    "type": "asset_created",
                    "run_name": run_name,
                    "step": "qc",
                    "tool": "falco",
                    "kind": "directory",
                    "path": str(falco_dir),
                    "sample": sample.name,
                    "metadata": {"read": read},
                },
            )


def _submit_fastqc(
    samples: list[Sample],
    outdir: Path,
    per_task_threads: int,
    *,
    events_file: str | None,
    run_name: str | None,
) -> PrefectFutureList:
    n = len(samples)
    return run_fastqc.map(
        sample=samples,
        outdir=[outdir] * n,
        threads=[min(per_task_threads, 2 if s.paired else 1) for s in samples],
        events_file=[events_file] * n,
        run_name=[run_name] * n,
    )


def _submit_fastp(
    samples: list[Sample],
    outdir: Path,
    per_task_threads: int,
    *,
    events_file: str | None,
    run_name: str | None,
) -> PrefectFutureList:
    n = len(samples)
    return run_fastp.map(
        sample=samples,
        outdir=[outdir] * n,
        threads=[per_task_threads] * n,
        events_file=[events_file] * n,
        run_name=[run_name] * n,
    )


def _submit_falco(
    samples: list[Sample],
    outdir: Path,
    _per_task_threads: int,
    *,
    events_file: str | None,
    run_name: str | None,
) -> PrefectFutureList:
    n = len(samples)
    return run_falco.map(
        sample=samples,
        outdir=[outdir] * n,
        events_file=[events_file] * n,
        run_name=[run_name] * n,
    )


QC_TOOL_REGISTRY: dict[str, Any] = {
    "fastqc": _submit_fastqc,
    "fastp": _submit_fastp,
    "falco": _submit_falco,
}


def submit_qc_tasks(
    samples: list[Sample],
    qc_tool: str,
    outdir: Path,
    per_task_threads: int,
    *,
    events_file: str | None,
    run_name: str | None,
) -> PrefectFutureList:
    """Submit mapped QC tasks for all samples."""
    qc_tool_norm = qc_tool.lower().strip()
    submitter = QC_TOOL_REGISTRY.get(qc_tool_norm)
    if submitter is None:
        raise SystemExit(f"Unknown QC tool: {qc_tool}")
    return submitter(
        samples,
        outdir,
        per_task_threads,
        events_file=events_file,
        run_name=run_name,
    )


@flow(name="qc_submit", log_prints=True)
def _qc_submit_flow(
    samples: list[Sample],
    qc_tool: str,
    outdir: Path,
    per_task_threads: int,
    *,
    events_file: str | None,
    run_name: str | None,
) -> PrefectFutureList:
    return submit_qc_tasks(
        samples,
        qc_tool,
        outdir,
        per_task_threads,
        events_file=events_file,
        run_name=run_name,
    )


@flow(name="qc_phase", log_prints=True)
def qc_phase(
    samples: list[Sample],
    qc_tool: str,
    outdir: Path,
    thread_budget: int,
    *,
    events_file: str | None = None,
    run_name: str | None = None,
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
    return _qc_submit_flow.with_options(task_runner=cast(Any, runner))(
        samples=samples,
        qc_tool=qc_tool,
        outdir=outdir,
        per_task_threads=per_task_threads,
        events_file=events_file,
        run_name=run_name,
    )
