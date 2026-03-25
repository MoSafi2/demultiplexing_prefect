from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

from prefect import get_run_logger, task  # type: ignore[import-not-found]


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
    fastq_path: Path,
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
    cmd.append(str(fastq_path))

    logger.info("kraken2: %s", " ".join(cmd))
    _run(cmd)


@task
def run_fastq_screen(
    sample_name: str,
    fastq_path: Path,
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
    cmd.append(str(fastq_path))

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


