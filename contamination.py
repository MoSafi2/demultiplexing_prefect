from __future__ import annotations

import shutil
import subprocess
from pathlib import Path
from typing import Any, Optional

from prefect import flow, get_run_logger, task  # type: ignore[import-not-found]

from .qc import run_multiqc
from .models import Sample


def _ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def _run(cmd: list[str]) -> None:
    # Keep stdout/stderr in Prefect logs for easier debugging.
    subprocess.run(cmd, check=True)


def _require_executable(exe: str) -> None:
    if shutil.which(exe) is None:
        raise SystemExit(
            f"Missing required executable on PATH: {exe}. "
            f"Please install it and ensure it is available on your PATH."
        )


@task
def run_kraken2(
    sample_name: str,
    r1: Path,
    r2: Optional[Path],
    outdir: Path,
    threads: int,
    kraken_db: Path,
) -> None:
    logger = get_run_logger()
    kraken_root = outdir / "contamination" / "kraken"
    sample_dir = kraken_root / sample_name
    _ensure_dir(sample_dir)

    report_path = sample_dir / f"{sample_name}.kraken2.report"
    # Avoid large outputs: MultiQC uses --report, not the classification output.
    output_path = Path("/dev/null")

    cmd: list[str] = [
        "kraken2",
        "--db",
        str(kraken_db),
        "--threads",
        str(threads),
        "--report",
        str(report_path),
        "--output",
        str(output_path),
    ]
    if r2 is None:
        cmd.append(str(r1))
    else:
        cmd += ["--paired", str(r1), str(r2)]

    logger.info("kraken2: %s", " ".join(cmd))
    _run(cmd)


@task
def run_fastq_screen(
    sample_name: str,
    r1: Path,
    r2: Optional[Path],
    outdir: Path,
    threads: int,
    fastq_screen_conf: Path,
) -> None:
    logger = get_run_logger()
    fastq_screen_root = outdir / "contamination" / "fastq_screen"
    sample_dir = fastq_screen_root / sample_name
    _ensure_dir(sample_dir)

    # FastQ Screen writes *_screen.txt into the output directory.
    cmd: list[str] = [
        "fastq_screen",
        "-conf",
        str(fastq_screen_conf),
        "--outdir",
        str(sample_dir),
        "--threads",
        str(threads),
        "--force",
    ]
    if r2 is None:
        cmd.append(str(r1))
    else:
        cmd += [str(r1), str(r2)]

    logger.info("fastq_screen: %s", " ".join(cmd))
    _run(cmd)

    # Rename to a stable filename so MultiQC can reliably associate reports with samples.
    screen_files = list(sample_dir.rglob("*_screen.txt"))
    if not screen_files:
        raise SystemExit(
            f"fastq_screen produced no *_screen.txt files under {sample_dir}"
        )

    preferred = [
        p
        for p in screen_files
        if p.name.startswith(sample_name) and p.name.endswith("_screen.txt")
    ]
    chosen = sorted(preferred)[0] if preferred else sorted(screen_files)[0]

    dest = sample_dir / f"{sample_name}_screen.txt"
    if chosen != dest:
        if dest.exists():
            dest.unlink()
        chosen.rename(dest)


@flow(name="contamination", log_prints=True)
def contamination_flow(
    *,
    samples: list[Sample],
    outdir: Path | str,
    tool: str,
    threads: int = 4,
    kraken_db: Path | None = None,
    fastq_screen_conf: Path | None = None,
) -> None:
    """
    Run contamination screening after FASTQ QC.

    Outputs are written under:
      - outdir/contamination/kraken/<sample_name>/*.kraken2.report
      - outdir/contamination/fastq_screen/<sample_name>/<sample_name>_screen.txt

    This is designed so `QC.flow.run_multiqc()` can pick them up.
    """

    logger = get_run_logger()
    outdir = Path(outdir)
    _ensure_dir(outdir)

    tool_norm = (tool or "").lower().strip()
    if tool_norm not in {"kraken", "fastq_screen"}:
        raise SystemExit(
            f"Unknown contamination tool: {tool_norm} (expected kraken|fastq_screen)."
        )

    if tool_norm == "kraken":
        _require_executable("kraken2")
        if kraken_db is None:
            raise SystemExit(
                "--kraken-db is required when --contamination-tool is kraken."
            )
        kraken_db = Path(kraken_db)
        if not kraken_db.exists():
            raise SystemExit(f"Kraken DB path does not exist: {kraken_db}")
    else:
        _require_executable("fastq_screen")
        if fastq_screen_conf is None:
            raise SystemExit(
                "--fastq-screen-conf is required when --contamination-tool is fastq_screen."
            )
        fastq_screen_conf = Path(fastq_screen_conf)
        if not fastq_screen_conf.exists():
            raise SystemExit(
                f"fastq_screen config file does not exist: {fastq_screen_conf}"
            )

    cont_tasks: list[Any] = []
    for sample in samples:
        if tool_norm == "kraken":
            cont_tasks.append(
                run_kraken2(
                    sample_name=sample.name,
                    r1=sample.r1,
                    r2=sample.r2,
                    outdir=outdir,
                    threads=threads,
                    kraken_db=kraken_db,
                )
            )
        else:
            cont_tasks.append(
                run_fastq_screen(
                    sample_name=sample.name,
                    r1=sample.r1,
                    r2=sample.r2,
                    outdir=outdir,
                    threads=threads,
                    fastq_screen_conf=fastq_screen_conf,
                )
            )

    logger.info("Collecting MultiQC after contamination (%s).", tool_norm)
    run_multiqc(outdir, cont_tasks)
