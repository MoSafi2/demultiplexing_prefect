from __future__ import annotations

import re
import shutil
from pathlib import Path
from typing import Any, Optional

from prefect import flow, get_run_logger, task  # type: ignore[import-not-found]

from .qc import run_falco, run_fastp, run_fastqc, run_multiqc
from .models import Sample
from .contamination import run_fastq_screen, run_kraken2
from .demux import demux_bcl_to_fastqs_task


FASTQ_R1R2_RE = re.compile(r"^(?P<sample>.+?)_R(?P<read>[12])\.fastq\.gz$")


@task(name="load_samples_from_manifest")
def load_samples_from_manifest_task(manifest_tsv: Path) -> list[Sample]:
    """
    Load samples from a TSV written by `demux.flow.demux_bcl_to_fastqs`.

    Format:
      sample_name<TAB>r1<TAB>r2(optional)
    """
    manifest_tsv = Path(manifest_tsv)
    logger = get_run_logger()

    if not manifest_tsv.exists() or not manifest_tsv.is_file():
        raise SystemExit(f"Manifest TSV does not exist or is not a file: {manifest_tsv}")

    samples: list[Sample] = []
    with manifest_tsv.open("r") as f:
        for line_num, raw_line in enumerate(f, start=1):
            line = raw_line.strip()
            if not line:
                continue

            parts = line.split("\t")
            if len(parts) not in (2, 3):
                raise SystemExit(
                    f"Invalid manifest TSV line {line_num} (expected 2 or 3 columns): {raw_line!r}"
                )

            sample_name = parts[0]
            r1_path = Path(parts[1])
            if not r1_path.exists():
                raise SystemExit(f"Manifest TSV r1 FASTQ not found: {r1_path}")

            r2_path: Optional[Path] = None
            if len(parts) == 3:
                r2_path = Path(parts[2])
                if not r2_path.exists():
                    raise SystemExit(f"Manifest TSV r2 FASTQ not found: {r2_path}")

            samples.append(Sample(name=sample_name, r1=r1_path, r2=r2_path))

    if not samples:
        logger.warning("Manifest contained no samples: %s", manifest_tsv)

    return sorted(samples, key=lambda s: s.name)


@task(name="load_samples_from_fastq_dir")
def load_samples_from_fastq_dir_task(in_fastq_dir: Path) -> list[Sample]:
    """
    Discover samples under a directory of FASTQs with names like:
      <sample>_R1.fastq.gz
      <sample>_R2.fastq.gz (optional)
    """
    in_fastq_dir = Path(in_fastq_dir)
    logger = get_run_logger()

    if not in_fastq_dir.exists() or not in_fastq_dir.is_dir():
        raise SystemExit(f"--in-fastq-dir must exist and be a directory: {in_fastq_dir}")

    discovered: dict[str, dict[int, list[Path]]] = {}
    fastqs = list(in_fastq_dir.rglob("*.fastq.gz"))
    if not fastqs:
        raise SystemExit(f"No .fastq.gz files found under {in_fastq_dir}")

    for fq in fastqs:
        m = FASTQ_R1R2_RE.match(fq.name)
        if m is None:
            continue

        sample_name = m.group("sample")
        read = int(m.group("read"))
        discovered.setdefault(sample_name, {}).setdefault(read, []).append(fq)

    if not discovered:
        raise SystemExit(
            f"Found {len(fastqs)} .fastq.gz files under {in_fastq_dir}, but none matched "
            "expected names like *_R1.fastq.gz (and optional *_R2.fastq.gz)."
        )

    samples: list[Sample] = []
    for sample_name in sorted(discovered.keys()):
        r1_paths = discovered[sample_name].get(1, [])
        if not r1_paths:
            continue

        r1_paths = sorted(r1_paths)
        if len(r1_paths) > 1:
            logger.warning(
                "Multiple R1 files found for sample %s; using %s",
                sample_name,
                r1_paths[0],
            )
        r1_path = r1_paths[0]

        r2_paths = discovered[sample_name].get(2)
        if r2_paths:
            r2_paths = sorted(r2_paths)
            if len(r2_paths) > 1:
                logger.warning(
                    "Multiple R2 files found for sample %s; using %s",
                    sample_name,
                    r2_paths[0],
                )
            r2_path: Optional[Path] = r2_paths[0]
        else:
            r2_path = None

        samples.append(Sample(name=sample_name, r1=r1_path, r2=r2_path))

    if not samples:
        raise SystemExit("No samples could be discovered from the provided FASTQ directory.")

    return samples


@flow(name="unified-demux-qc-contamination", log_prints=True)
def unified_demux_qc_contamination_pipeline(
    *,
    mode: str,
    qc_tool: str = "falco",
    threads: int = 4,
    outdir: Path | str,
    # demux inputs
    bcl_dir: Path | None = None,
    samplesheet: Path | None = None,
    # qc inputs:
    #   - when mode == demux_qc: this is an optional manifest TSV output override
    #   - when mode == qc: this is a required manifest TSV input (if `in_fastq_dir` is not set)
    manifest_tsv: Path | None = None,
    in_fastq_dir: Path | None = None,
    # contamination inputs:
    contamination_tool: str | None = None,
    kraken_db: Path | None = None,
    fastq_screen_conf: Path | None = None,
) -> None:
    """
    Unified pipeline:
      optional demux -> QC -> optional contamination -> MULTIQC (once at the end).

    Output layout (flat relative to `outdir`):
      - `outdir/demux_fastq/` (when demux is enabled)
      - `outdir/qc_inputs/` (manifest from demux, when demux is enabled)
      - `outdir/fastqc|fastp|fastp_trimmed|falco/`
      - `outdir/contamination/` (when contamination enabled)
      - `outdir/multiqc/`
    """
    logger = get_run_logger()
    outdir_path = Path(outdir)

    # Keep QC + contamination outputs at the top-level of `outdir` for a flat layout.
    qc_base_dir = outdir_path

    mode_norm = (mode or "").lower().strip()
    if mode_norm in {"demux_qc", "demux-qc", "demuxqc"}:
        stage = "demux_qc"
    elif mode_norm in {"qc"}:
        stage = "qc"
    else:
        raise SystemExit(f"Unknown --mode value: {mode} (expected demux_qc or QC)")

    qc_tool_norm = (qc_tool or "").lower().strip()
    if qc_tool_norm not in {"fastqc", "fastp", "falco"}:
        raise SystemExit(f"Unknown --qc-tool: {qc_tool} (expected fastqc|fastp|falco)")

    if stage == "demux_qc":
        if bcl_dir is None or samplesheet is None:
            raise SystemExit("--mode demux_qc requires --bcl_dir and --samplesheet.")

        samples_future = demux_bcl_to_fastqs_task(
            bcl_dir=bcl_dir,
            samplesheet=samplesheet,
            outdir=outdir_path,
            manifest_tsv=manifest_tsv,
        )
        samples = samples_future.result()
    else:
        if (manifest_tsv is None and in_fastq_dir is None) or (manifest_tsv and in_fastq_dir):
            raise SystemExit("For --mode qc, provide exactly one of --manifest-tsv or --in-fastq-dir.")

        if manifest_tsv is not None:
            samples_future = load_samples_from_manifest_task(manifest_tsv=manifest_tsv)
        else:
            samples_future = load_samples_from_fastq_dir_task(in_fastq_dir=in_fastq_dir)

        samples = samples_future.result()

    if not samples:
        raise SystemExit("No samples to process.")

    # ---- QC tasks (no nested flows) ----
    qc_tasks: list[Any] = []
    for sample in samples:
        if qc_tool_norm == "fastqc":
            qc_tasks.append(run_fastqc(sample.name, sample.r1, sample.r2, qc_base_dir, threads))
        elif qc_tool_norm == "fastp":
            qc_tasks.append(run_fastp(sample.name, sample.r1, sample.r2, qc_base_dir, threads))
        else:
            qc_tasks.append(run_falco(sample.name, sample.r1, sample.r2, qc_base_dir))

    # ---- Optional contamination tasks (no nested flows) ----
    cont_tasks: list[Any] = []
    if contamination_tool:
        tool_norm = contamination_tool.lower().strip()
        if tool_norm not in {"kraken", "fastq_screen"}:
            raise SystemExit("--contamination-tool must be one of kraken|fastq_screen.")

        if tool_norm == "kraken":
            if kraken_db is None:
                raise SystemExit("--kraken-db is required when --contamination-tool kraken.")
            if shutil.which("kraken2") is None:
                raise SystemExit("Missing required executable on PATH: kraken2.")
            kraken_db_path = Path(kraken_db)
            if not kraken_db_path.exists():
                raise SystemExit(f"Kraken DB path does not exist: {kraken_db_path}")
        else:
            if fastq_screen_conf is None:
                raise SystemExit("--fastq-screen-conf is required when --contamination-tool fastq_screen.")
            if shutil.which("fastq_screen") is None:
                raise SystemExit("Missing required executable on PATH: fastq_screen.")
            fastq_screen_conf_path = Path(fastq_screen_conf)
            if not fastq_screen_conf_path.exists():
                raise SystemExit(f"fastq_screen config file does not exist: {fastq_screen_conf_path}")

        for sample in samples:
            if tool_norm == "kraken":
                cont_tasks.append(
                    run_kraken2(
                        sample_name=sample.name,
                        r1=sample.r1,
                        r2=sample.r2,
                        outdir=qc_base_dir,
                        threads=threads,
                        kraken_db=kraken_db_path,
                    )
                )
            else:
                cont_tasks.append(
                    run_fastq_screen(
                        sample_name=sample.name,
                        r1=sample.r1,
                        r2=sample.r2,
                        outdir=qc_base_dir,
                        threads=threads,
                        fastq_screen_conf=fastq_screen_conf_path,
                    )
                )

        logger.info("Contamination enabled (%s) for %d sample(s).", tool_norm, len(samples))

    # ---- MULTIQC once at the end ----
    all_tasks = qc_tasks + cont_tasks
    run_multiqc(qc_base_dir, all_tasks)

