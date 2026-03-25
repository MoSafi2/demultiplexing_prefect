from __future__ import annotations

from collections import defaultdict
import re
import shutil
import subprocess
from pathlib import Path

from prefect import get_run_logger, task  # type: ignore[import-not-found]
from models import Sample


FASTQ_RE = re.compile(
    r"""^(?P<sample>[A-Za-z0-9_.-]+?)(?:_S\d+)?(?:_L(?P<lane>\d{3}))?_R(?P<read>[12])
    (?:_(?P<chunk>\d{3}))?\.(?P<ext>fastq|fq)(?:\.gz)?$""",
    re.VERBOSE | re.IGNORECASE,
)


def parse_fastq(path: Path):
    m = FASTQ_RE.match(path.name)
    if not m:
        return None

    return {
        "sample": m.group("sample"),
        "read": int(m.group("read")),
        "lane": int(m.group("lane")) if m.group("lane") else None,
        "chunk": int(m.group("chunk")) if m.group("chunk") else 0,
    }


def _group_fastqs(
    root: Path, *, recursive: bool = True, include_undetermined: bool = False
) -> dict[tuple[str, int], dict[str, Path]]:
    iterator = root.rglob("*") if recursive else root.glob("*")
    grouped: dict[tuple[str, int], dict[str, Path]] = defaultdict(dict)
    for path in iterator:
        if not path.is_file():
            continue
        if not include_undetermined:
            # Skip undetermined reads
            if any("undetermined" in part.lower() for part in path.parts):
                continue
        parsed = parse_fastq(path)
        if not parsed:
            continue
        read_key = f"R{parsed['read']}"
        grouped[parsed["sample"], parsed["chunk"]][read_key] = path
    return grouped


def _samples_from_fastq_dir(
    root: Path,
    *,
    recursive: bool = True,
    include_undetermined: bool = False,
) -> list[Sample]:
    """
    Discover FASTQ files and return a list of Sample objects.

    Behavior:
    - Walks directory (recursive by default)
    - Groups by (sample, chunk)
    - Each chunk becomes an independent Sample
    - Supports SE and PE
    - Optionally filters Undetermined reads
    """

    grouped = _group_fastqs(
        root, recursive=recursive, include_undetermined=include_undetermined
    )
    samples: list[Sample] = []

    for (sample, chunk), reads in sorted(
        grouped.items(), key=lambda x: (x[0][0], x[0][1])
    ):
        if "R1" not in reads:
            # skip incomplete units
            continue
        samples.append(Sample(name=sample, r1=reads["R1"], r2=reads.get("R2")))

    return samples


def _write_samples_tsv(samples: list[Sample], path: Path) -> None:
    with path.open("w") as f:
        for sample in samples:
            f.write(f"{sample.name}\t{sample.r1}\t{sample.r2}\n")


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
        # bcl-convert and similar tools often log progress on stderr.
        logger.info(line.rstrip())

    proc.wait()

    if proc.returncode != 0:
        raise RuntimeError(f"Command failed: {' '.join(cmd)}")


def _resolve_bcl_convert_binary() -> str:
    # Illumina docs commonly show `bcl-convert`, but some installs ship `bcl_convert`.
    for candidate in ("bcl-convert", "bcl_convert"):
        if shutil.which(candidate) is not None:
            return candidate
    raise SystemExit(
        "Missing required binary on PATH: bcl-convert (or bcl_convert). "
        "Please install BCL Convert and ensure it is available on your PATH."
    )


@task(name="demux_bcl", log_prints=True)
def demux_bcl(
    *,
    bcl_dir: Path,
    samplesheet: Path,
    outdir: Path | str,
    extra_args: list[str] | None = None,
) -> None:
    """
    Implementation used by the `demux_bcl_to_fastqs_task`.
    """
    logger = get_run_logger()
    outdir = Path(outdir)

    if not bcl_dir.exists() or not bcl_dir.is_dir():
        raise SystemExit(f"Expected --bcl_dir to be an existing directory: {bcl_dir}")
    if not samplesheet.exists() or not samplesheet.is_file():
        raise SystemExit(
            f"Expected --samplesheet to be an existing file: {samplesheet}"
        )

    bcl_bin = _resolve_bcl_convert_binary()

    cmd = [
        bcl_bin,
        "--bcl-input-directory",
        str(bcl_dir),
        "--output-directory",
        str(outdir),
        "--sample-sheet",
        str(samplesheet),
    ]
    logger.info("bcl-convert: %s", " ".join(cmd))
    _run(cmd)


@task(name="write_fastq_manifest")
def write_fastq_manifest(
    input_dir: Path, output_path: Path, include_undetermined: bool = False
) -> None:
    samples = _samples_from_fastq_dir(
        input_dir, include_undetermined=include_undetermined
    )
    _write_samples_tsv(samples, output_path)
