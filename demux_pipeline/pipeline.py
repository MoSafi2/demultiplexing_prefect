from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterable, List
import subprocess

from prefect import flow, get_run_logger
from prefect.task_runners import ThreadPoolTaskRunner

from demux_pipeline.models import Sample
from demux_pipeline.qc import (
    _project_names_from_demux_output,
    run_multiqc,
    submit_qc_tasks,
)
from demux_pipeline.contamination import submit_contamination_tasks
from demux_pipeline.demux import (
    AVITI_AUX_OUTDIR_NAME,
    DEMUX_FASTQ_OUTDIR_NAME,
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


def _get_projects_samples(samples: list[Sample]) -> dict[Path, list[Path]]:
    pp = list(set([x.r1.parent for x in samples]))
    pp = {p: [] for p in pp}
    for sample in samples:
        if sample.r1:
            pp[sample.r1.parent].append(sample.r1)
        if sample.r2:
            pp[sample.r2.parent].append(sample.r2)
    return pp


def _hash_paths(paths: list[Path]) -> list[str]:
    paths = [Path(p) for p in paths]

    result = subprocess.run(
        ["md5sum", "--binary", *map(str, paths)],
        check=True,
        capture_output=True,
        text=True,
    )

    lines = []
    for line, path in zip(result.stdout.splitlines(), paths):
        digest = line.split()[0]
        lines.append(f"{digest}  {path.name}")

    return lines


def write_sample_hashes(samples: list[Sample]):
    samples_dict = _get_projects_samples(samples)
    for path, project_samples in samples_dict.items():
        hashes = _hash_paths(project_samples)
        with open(path/"md5.txt", "w") as f:
            f.write("\n".join(hashes))


def _resolve_run_name(
    *,
    run_name: str | None = None,
    input_dir: Path | None = None,
    qc_tool: str = "falco",
    **_: Any,
) -> str:
    mode = "demux"
    qc_tools = _normalize_tools(qc_tool, default="falco")
    return slugify_run_name(run_name or "") or default_run_name(
        mode=mode, qc_tool="+".join(qc_tools)
    )


def write_output_contract(
    *,
    outdir: Path,
    artifact_path: Path,
) -> Path:
    qc_dir = next(
        (
            str(p)
            for p in (outdir / "falco", outdir / "fastqc", outdir / "fastp")
            if p.exists()
        ),
        None,
    )
    contamination_dir = outdir / "contamination"
    multiqc_report = outdir / "multiqc" / "multiqc_report.html"
    project_names = _project_names_from_demux_output(outdir)
    project_qc_dirs = {
        project: str(qc_dir)
        for project in project_names
        for qc_dir in [outdir / DEMUX_FASTQ_OUTDIR_NAME / project / "qc"]
        if qc_dir.exists()
    }
    project_contamination_dirs = {
        project: str(contam_dir)
        for project in project_names
        for contam_dir in [
            outdir / DEMUX_FASTQ_OUTDIR_NAME / project / "qc" / "contamination"
        ]
        if contam_dir.exists()
    }
    project_multiqc_reports = {
        project: str(report)
        for project in project_names
        for report in [
            outdir
            / DEMUX_FASTQ_OUTDIR_NAME
            / project
            / "qc"
            / "multiqc"
            / "multiqc_report.html"
        ]
        if report.exists()
    }
    summaries = sorted((outdir / ".pipeline").glob("*/run_summary.json"))
    payload = {
        "outdir": str(outdir),
        "outputs": {
            "aviti_auxiliary_dir": (
                str(outdir / AVITI_AUX_OUTDIR_NAME)
                if (outdir / AVITI_AUX_OUTDIR_NAME).exists()
                else None
            ),
            "samples_tsv": str(outdir / "samples.tsv"),
            "qc_dir": qc_dir,
            "project_qc_dirs": project_qc_dirs,
            "contamination_dir": str(contamination_dir) if contamination_dir.exists() else None,
            "project_contamination_dirs": project_contamination_dirs,
            "multiqc_report": str(multiqc_report) if multiqc_report.exists() else None,
            "project_multiqc_reports": project_multiqc_reports,
            "run_summary": str(summaries[-1]) if summaries else None,
        },
    }
    artifact_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return artifact_path


@flow(name="demux-pipeline", flow_run_name=_resolve_run_name, log_prints=True)
def demux_pipeline(
    *,
    input_dir: Path,
    samplesheet: Path,
    platform: str = "illumina",
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
    output_contract_file: Path | None = None,
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
        input_dir,
        samplesheet,
    )
    logger = get_run_logger()
    logger.info("run_name=%s tracking=%s", resolved, observer.events_file.parent)

    try:
        # --- Stage 1: Demux ---
        observer.phase_started("demux")
        demux_bcl(
            input_dir=input_dir,
            samplesheet=samplesheet,
            platform=platform,
            outdir=outdir_path,
            extra_args=["--num-threads", str(thread_budget)] if platform == "aviti" else None,
        )
        observer.phase_finished("demux")
        samples = _discover_samples(demux_dir=outdir_path / DEMUX_FASTQ_OUTDIR_NAME)

        if not samples:
            raise SystemExit("No samples found.")
        samples_manifest = _write_discovered_manifest(samples, outdir_path)
        logger.info("samples manifest written to %s", samples_manifest)
        
        # Hash Fastq files, and write hashes to project folder
        write_sample_hashes(samples)

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
                        tool, # type: ignore
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
        if output_contract_file is not None:
            write_output_contract(
                outdir=outdir_path,
                artifact_path=output_contract_file,
            )
    finally:
        # Avoid leaking observer state when multiple runs happen in one process.
        reset_observer()
