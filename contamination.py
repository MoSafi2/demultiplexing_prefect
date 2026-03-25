from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

from prefect import get_run_logger, task  # type: ignore[import-not-found]
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
        logger.error(line.rstrip())

    proc.wait()

    if proc.returncode != 0:
        raise RuntimeError(f"Command failed: {' '.join(cmd)}")


def _require_executable(exe: str) -> None:
    if shutil.which(exe) is None:
        raise SystemExit(
            f"Missing required executable on PATH: {exe}. "
            f"Please install it and ensure it is available on your PATH."
        )


@task(tags=["contamination"])
def run_kraken2(
    sample: Sample,
    outdir: Path,
    threads: int,
    kraken_db: Path,
) -> dict:
    logger = get_run_logger()
    _require_executable("kraken2")

    root = outdir / "contamination" / "kraken"
    sample_dir = root / sample.name
    _ensure_dir(sample_dir)

    report_path = sample_dir / f"{sample.name}.kraken.report"
    output_path = sample_dir / f"{sample.name}.kraken.out"

    cmd = [
        "kraken2",
        "--db",
        str(kraken_db),
        "--threads",
        str(threads),
        "--report",
        str(report_path),
        "--output",
        str(output_path),
        "--confidence",
        "0.1",
    ]

    if sample.paired:
        cmd.extend(["--paired", str(sample.r1), str(sample.r2)])
    else:
        cmd.append(str(sample.r1))

    logger.info("kraken2: %s", " ".join(cmd))
    _run(cmd)

    if not report_path.exists():
        raise RuntimeError(f"Missing Kraken report: {report_path}")

    return {
        "sample": sample.name,
        "report": str(report_path),
        "output": str(output_path),
    }


@task(tags=["contamination"])
def run_bracken(
    kraken_result: dict,
    outdir: Path,
    bracken_db: Path,
    read_length: int,
    level: str = "S",  # species level
) -> dict:
    logger = get_run_logger()
    _require_executable("bracken")

    sample_name = kraken_result["sample"]
    kraken_report = Path(kraken_result["report"])

    root = outdir / "contamination" / "bracken"
    sample_dir = root / sample_name
    _ensure_dir(sample_dir)

    output_path = sample_dir / f"{sample_name}.bracken.txt"
    report_path = sample_dir / f"{sample_name}.bracken.report"

    cmd = [
        "bracken",
        "-d",
        str(bracken_db),
        "-i",
        str(kraken_report),
        "-o",
        str(output_path),
        "-w",
        str(report_path),
        "-r",
        str(read_length),
        "-l",
        level,
    ]

    logger.info("bracken: %s", " ".join(cmd))
    _run(cmd)

    if not output_path.exists():
        raise RuntimeError(f"Missing Bracken output: {output_path}")

    return {
        "sample": sample_name,
        "bracken": str(output_path),
        "report": str(report_path),
    }


@task(tags=["contamination"])
def run_kraken_bracken(
    sample: Sample,
    outdir: Path,
    kraken_db: Path,
    bracken_db: Path,
    threads: int,
    read_length: int,
) -> dict:
    """
    Convenience wrapper to run Kraken2 followed by Bracken.
    """

    kraken_res = run_kraken2.fn(  # call underlying function directly
        sample=sample,
        outdir=outdir,
        threads=threads,
        kraken_db=kraken_db,
    )

    bracken_res = run_bracken.fn(
        kraken_result=kraken_res,
        outdir=outdir,
        bracken_db=bracken_db,
        read_length=read_length,
    )

    return {
        "sample": sample.name,
        "kraken": kraken_res,
        "bracken": bracken_res,
    }


@task(tags=["contamination"])
def run_fastq_screen(
    sample: Sample,
    outdir: Path,
    threads: int,
    fastq_screen_conf: Path,
) -> dict:
    logger = get_run_logger()
    _require_executable("fastq_screen")

    root = outdir / "contamination" / "fastq_screen"
    sample_dir = root / sample.name
    _ensure_dir(sample_dir)

    inputs = [str(sample.r1)]
    if sample.r2:
        inputs.append(str(sample.r2))

    cmd = [
        "fastq_screen",
        "-conf",
        str(fastq_screen_conf),
        "--outdir",
        str(sample_dir),
        "--threads",
        str(threads),
        "--force",
        "--basename",
        sample.name,
        *inputs,
    ]

    logger.info("fastq_screen: %s", " ".join(cmd))
    _run(cmd)

    output = sample_dir / f"{sample.name}_screen.txt"

    if not output.exists():
        raise RuntimeError(f"Missing FastQ Screen output: {output}")

    return {
        "sample": sample.name,
        "screen": str(output),
    }
