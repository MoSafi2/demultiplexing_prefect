from __future__ import annotations

from collections import defaultdict
import re
import shutil
from pathlib import Path

from prefect import get_run_logger, task  # type: ignore[import-not-found]
from demux_pipeline.models import Sample
from demux_pipeline.process import run_command
from demux_pipeline.observability import record_asset


# Parent `--outdir` contains this subdirectory with all bcl-convert artifacts (FASTQs, Reports, etc.).
BCL_CONVERT_OUTDIR_NAME = "output"

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
            r2 = str(sample.r2) if sample.r2 is not None else ""
            f.write(f"{sample.name}\t{sample.r1}\t{r2}\n")


def _resolve_bcl_convert_binary() -> str:
    """Prefer ./bcl-convert (cwd), then copy next to this module, then PATH."""
    project_root = Path(__file__).resolve().parent
    for local in (Path.cwd() / "bcl-convert", project_root / "bcl-convert"):
        if local.is_file():
            return str(local.resolve())
    # Illumina docs commonly show `bcl-convert`, but some installs ship `bcl_convert`.
    for candidate in ("bcl-convert", "bcl_convert"):
        found = shutil.which(candidate)
        if found is not None:
            return found
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
    force: bool = True,
) -> None:
    """
    Implementation used by the `demux_bcl_to_fastqs_task`.
    """
    logger = get_run_logger()
    outdir = Path(outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    bcl_output = outdir / BCL_CONVERT_OUTDIR_NAME

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
        str(bcl_output),
        "--sample-sheet",
        str(samplesheet),
        # bcl-convert expects an explicit true/false after this flag (not a bare switch).
        "--no-lane-splitting",
        "true",
        "--bcl-sampleproject-subdirectories",
        "true",
    ]
    if force:
        cmd.append("--force")
    if extra_args:
        cmd.extend(extra_args)
    logger.info("bcl-convert: %s", " ".join(cmd))
    run_command(cmd, capture_err_tail=80, step="demux", tool="bcl-convert")
    record_asset(
        bcl_output,
        step="demux",
        tool="bcl-convert",
        kind="directory",
        metadata={"source": "bcl-convert --output-directory"},
    )


@task(name="write_fastq_manifest")
def write_fastq_manifest(
    input_dir: Path, output_path: Path, include_undetermined: bool = False
) -> None:
    samples = _samples_from_fastq_dir(
        input_dir, include_undetermined=include_undetermined
    )
    _write_samples_tsv(samples, output_path)
