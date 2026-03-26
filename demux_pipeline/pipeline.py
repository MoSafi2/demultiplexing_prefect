from __future__ import annotations

from pathlib import Path
from typing import List, Literal

from prefect import flow, get_run_logger, task

from models import Sample
from qc import qc_phase, run_multiqc
from contamination import contamination_phase
from demux import BCL_CONVERT_OUTDIR_NAME, demux_bcl, _samples_from_fastq_dir


def _split_phase_budgets(thread_budget: int) -> tuple[int, int]:
    """
    Split a global budget into QC + contamination budgets.

    Policy: favor contamination.
    - qc_budget = max(1, floor(thread_budget / 3))
    - contamination_budget = thread_budget - qc_budget
    """
    if thread_budget < 1:
        raise SystemExit("thread budget must be at least 1")
    qc_budget = max(1, thread_budget // 3)
    contamination_budget = thread_budget - qc_budget
    if thread_budget >= 2:
        contamination_budget = max(1, contamination_budget)
    return qc_budget, contamination_budget


@task
def _run_qc_phase(
    samples: List[Sample],
    qc_tool: str,
    outdir: Path,
    thread_budget: int,
) -> None:
    qc_phase(samples, qc_tool, outdir, thread_budget)


@task
def _run_contamination_phase(
    samples: List[Sample],
    contamination_tool: Literal["kraken", "kraken_bracken", "fastq_screen"],
    outdir: Path,
    thread_budget: int,
    kraken_db: Path | None = None,
    bracken_db: Path | None = None,
    fastq_screen_conf: Path | None = None,
    read_length: int = 150,
) -> None:
    contamination_phase(
        samples,
        contamination_tool,
        outdir,
        thread_budget,
        kraken_db=kraken_db,
        bracken_db=bracken_db,
        fastq_screen_conf=fastq_screen_conf,
        read_length=read_length,
    )


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


@flow(name="qc-only", log_prints=True)
def qc_only_pipeline(
    *,
    qc_tool: str = "falco",
    thread_budget: int = 4,
    outdir: Path | str,
    manifest_tsv: Path | None = None,
    in_fastq_dir: Path | None = None,
    contamination_tool: (
        Literal["kraken", "kraken_bracken", "fastq_screen"] | None
    ) = None,
    kraken_db: Path | None = None,
    bracken_db: Path | None = None,
    fastq_screen_conf: Path | None = None,
    read_length: int = 150,
) -> None:
    logger = get_run_logger()
    outdir_path = Path(outdir)
    samples = _discover_samples(manifest_tsv=manifest_tsv, in_fastq_dir=in_fastq_dir)

    if not samples:
        raise SystemExit("No samples found to process.")

    if contamination_tool:
        if thread_budget == 1:
            logger.info(
                "thread_budget=1 with contamination enabled; running QC and contamination sequentially."
            )
            qc_phase(samples, qc_tool, outdir_path, thread_budget)
            contamination_phase(
                samples,
                contamination_tool,
                outdir_path,
                thread_budget,
                kraken_db=kraken_db,
                bracken_db=bracken_db,
                fastq_screen_conf=fastq_screen_conf,
                read_length=read_length,
            )
        else:
            qc_budget, contamination_budget = _split_phase_budgets(thread_budget)
            logger.info(
                "Concurrent phases: qc_budget=%s, contamination_budget=%s (global_budget=%s)",
                qc_budget,
                contamination_budget,
                thread_budget,
            )
            qc_future = _run_qc_phase.submit(
                samples=samples,
                qc_tool=qc_tool,
                outdir=outdir_path,
                thread_budget=qc_budget,
            )
            contamination_future = _run_contamination_phase.submit(
                samples=samples,
                contamination_tool=contamination_tool,
                outdir=outdir_path,
                thread_budget=contamination_budget,
                kraken_db=kraken_db,
                bracken_db=bracken_db,
                fastq_screen_conf=fastq_screen_conf,
                read_length=read_length,
            )
            qc_future.result()
            contamination_future.result()
    else:
        qc_phase(samples, qc_tool, outdir_path, thread_budget)
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
    contamination_tool: (
        Literal["kraken", "kraken_bracken", "fastq_screen"] | None
    ) = None,
    kraken_db: Path | None = None,
    bracken_db: Path | None = None,
    fastq_screen_conf: Path | None = None,
    read_length: int = 150,
) -> None:
    logger = get_run_logger()
    outdir_path = Path(outdir)

    # Demultiplex first
    demux_bcl(bcl_dir=bcl_dir, samplesheet=samplesheet, outdir=outdir_path)
    samples = _discover_samples(demux_dir=outdir_path / BCL_CONVERT_OUTDIR_NAME)

    if not samples:
        raise SystemExit("No samples found after demux.")

    if contamination_tool:
        if thread_budget == 1:
            logger.info(
                "thread_budget=1 with contamination enabled; running QC and contamination sequentially."
            )
            qc_phase(samples, qc_tool, outdir_path, thread_budget)
            contamination_phase(
                samples,
                contamination_tool,
                outdir_path,
                thread_budget,
                kraken_db=kraken_db,
                bracken_db=bracken_db,
                fastq_screen_conf=fastq_screen_conf,
                read_length=read_length,
            )
        else:
            qc_budget, contamination_budget = _split_phase_budgets(thread_budget)
            logger.info(
                "Concurrent phases: qc_budget=%s, contamination_budget=%s (global_budget=%s)",
                qc_budget,
                contamination_budget,
                thread_budget,
            )
            qc_future = _run_qc_phase.submit(
                samples=samples,
                qc_tool=qc_tool,
                outdir=outdir_path,
                thread_budget=qc_budget,
            )
            contamination_future = _run_contamination_phase.submit(
                samples=samples,
                contamination_tool=contamination_tool,
                outdir=outdir_path,
                thread_budget=contamination_budget,
                kraken_db=kraken_db,
                bracken_db=bracken_db,
                fastq_screen_conf=fastq_screen_conf,
                read_length=read_length,
            )
            qc_future.result()
            contamination_future.result()
    else:
        qc_phase(samples, qc_tool, outdir_path, thread_budget)
    run_multiqc(outdir_path, [], include_contamination=bool(contamination_tool))
