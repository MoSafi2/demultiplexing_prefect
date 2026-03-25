from __future__ import annotations

import argparse
import re
import shutil
import subprocess
from pathlib import Path
from typing import Optional

from prefect import flow, get_run_logger, task  # type: ignore[import-not-found]

from QC.flow import fastq_qc_flow
from QC.models import Sample
from contamination.flow import contamination_flow


_FASTQ_WITH_LANE_RE = re.compile(
    r"^(?P<sample>.+?)_S\d+_L(?P<lane>\d{3})_R(?P<read>[12])_001\.fastq\.gz$",
)
_FASTQ_NO_LANE_RE = re.compile(
    r"^(?P<sample>.+?)_S\d+_R(?P<read>[12])_001\.fastq\.gz$",
)


def _ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def _run(cmd: list[str]) -> None:
    subprocess.run(cmd, check=True)


def _resolve_bcl_convert_binary() -> str:
    # Illumina docs commonly show `bcl-convert`, but some installs ship `bcl_convert`.
    for candidate in ("bcl-convert", "bcl_convert"):
        if shutil.which(candidate) is not None:
            return candidate
    raise SystemExit(
        "Missing required binary on PATH: bcl-convert (or bcl_convert). "
        "Please install BCL Convert and ensure it is available on your PATH."
    )


def _parse_bcl_fastq_filename(path: Path) -> tuple[str, int] | None:
    """
    Extract `(sample_name, read)` from BCL Convert output filenames.

    Expected patterns (examples):
      - `SampleID_S1_L001_R1_001.fastq.gz`
      - `SampleID_S1_R1_001.fastq.gz` (when lane splitting is disabled)
    """

    m = _FASTQ_WITH_LANE_RE.match(path.name)
    if m is not None:
        sample = m.group("sample")
        read = int(m.group("read"))
        return sample, read

    m = _FASTQ_NO_LANE_RE.match(path.name)
    if m is not None:
        sample = m.group("sample")
        read = int(m.group("read"))
        return sample, read

    return None


def _concat_gzip_members(output_path: Path, input_paths: list[Path]) -> None:
    """
    Concatenate gzipped files by concatenating gzip members.

    Most gzip implementations can read concatenated members transparently.
    """

    if not input_paths:
        raise ValueError(f"Cannot concat empty input list into {output_path}")

    if output_path.exists():
        output_path.unlink()

    tmp_path = output_path.with_suffix(output_path.suffix + ".tmp")
    try:
        with tmp_path.open("wb") as out_f:
            for p in input_paths:
                with p.open("rb") as in_f:
                    shutil.copyfileobj(in_f, out_f)
        tmp_path.replace(output_path)
    finally:
        if tmp_path.exists():
            tmp_path.unlink()


def _discover_fastqs(demux_fastq_dir: Path) -> dict[str, dict[int, list[Path]]]:
    """
    Return mapping: sample_name -> {1: [r1 paths], 2: [r2 paths]}.
    """

    fastqs = list(demux_fastq_dir.rglob("*.fastq.gz"))
    if not fastqs:
        raise SystemExit(f"No .fastq.gz files found under {demux_fastq_dir}")

    discovered: dict[str, dict[int, list[Path]]] = {}

    for fq in fastqs:
        parsed = _parse_bcl_fastq_filename(fq)
        if parsed is None:
            continue
        sample, read = parsed
        discovered.setdefault(sample, {}).setdefault(read, []).append(fq)

    if not discovered:
        raise SystemExit(
            f"Found {len(fastqs)} .fastq.gz files under {demux_fastq_dir}, "
            "but none matched the expected BCL Convert filename patterns "
            "(e.g. *_S#_L###_R1_001.fastq.gz)."
        )

    # Sort members so concatenation is deterministic (by filename).
    for sample in discovered:
        for read in (1, 2):
            if read in discovered[sample]:
                discovered[sample][read] = sorted(
                    discovered[sample][read], key=lambda p: p.name
                )

    return discovered


def _prepare_samples_from_fastqs(
    demux_fastq_dir: Path,
    combined_fastq_dir: Path,
) -> list[Sample]:
    discovered = _discover_fastqs(demux_fastq_dir)

    _ensure_dir(combined_fastq_dir)

    samples: list[Sample] = []
    for sample_name in sorted(discovered.keys()):
        r1_members = discovered[sample_name].get(1, [])
        if not r1_members:
            continue

        r2_members = discovered[sample_name].get(2)

        r1_out = combined_fastq_dir / f"{sample_name}_R1.fastq.gz"
        _concat_gzip_members(r1_out, r1_members)

        if r2_members:
            r2_out = combined_fastq_dir / f"{sample_name}_R2.fastq.gz"
            _concat_gzip_members(r2_out, r2_members)
            samples.append(Sample(name=sample_name, r1=r1_out, r2=r2_out))
        else:
            samples.append(Sample(name=sample_name, r1=r1_out, r2=None))

    if not samples:
        raise SystemExit("No samples could be prepared from discovered FASTQs.")

    return samples


def _write_samples_tsv(samples: list[Sample], path: Path) -> None:
    """
    Write QC manifest TSV in the format:
      sample_name<TAB>r1<TAB>r2(optional)
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        for s in samples:
            if s.r2 is not None:
                f.write(f"{s.name}\t{str(s.r1)}\t{str(s.r2)}\n")
            else:
                f.write(f"{s.name}\t{str(s.r1)}\n")


@flow(name="bcl-demux-fastqs", log_prints=True)
def demux_bcl_to_fastqs(
    bcl_dir: Path,
    samplesheet: Path,
    outdir: Path | str,
    manifest_tsv: Path | None = None,
) -> list[Sample]:
    """
    Demultiplex BCL input with bcl-convert and produce merged per-sample FASTQs.

    Outputs:
      - `${outdir}/demux_fastq/` : raw bcl-convert output
      - `${outdir}/demux_fastq/combined/` : merged per-sample FASTQs
      - manifest TSV : by default `${outdir}/qc_inputs/samples.tsv`
    """
    return _demux_bcl_to_fastqs_impl(
        bcl_dir=Path(bcl_dir),
        samplesheet=Path(samplesheet),
        outdir=Path(outdir),
        manifest_tsv=manifest_tsv,
    )


def _demux_bcl_to_fastqs_impl(
    *,
    bcl_dir: Path,
    samplesheet: Path,
    outdir: Path,
    manifest_tsv: Path | None,
) -> list[Sample]:
    """
    Implementation shared by both the public `demux_bcl_to_fastqs` flow
    and the unified-pipeline `demux_bcl_to_fastqs_task`.
    """
    logger = get_run_logger()

    if not bcl_dir.exists() or not bcl_dir.is_dir():
        raise SystemExit(f"Expected --bcl_dir to be an existing directory: {bcl_dir}")
    if not samplesheet.exists() or not samplesheet.is_file():
        raise SystemExit(f"Expected --samplesheet to be an existing file: {samplesheet}")

    if manifest_tsv is None:
        manifest_tsv = outdir / "qc_inputs" / "samples.tsv"
    manifest_tsv = Path(manifest_tsv)

    bcl_bin = _resolve_bcl_convert_binary()
    demux_fastq_dir = outdir / "demux_fastq"
    combined_fastq_dir = demux_fastq_dir / "combined"

    _ensure_dir(demux_fastq_dir)
    _ensure_dir(combined_fastq_dir)

    # If users re-run, we want a clean set of merged per-sample FASTQs.
    if combined_fastq_dir.exists():
        for p in combined_fastq_dir.glob("*.fastq.gz"):
            p.unlink(missing_ok=True)

    cmd = [
        bcl_bin,
        "--bcl-input-directory",
        str(bcl_dir),
        "--output-directory",
        str(demux_fastq_dir),
        "--sample-sheet",
        str(samplesheet),
        "-f",
    ]
    logger.info("bcl-convert: %s", " ".join(cmd))
    _run(cmd)

    logger.info("Preparing samples from demux FASTQs under %s", demux_fastq_dir)
    samples = _prepare_samples_from_fastqs(demux_fastq_dir, combined_fastq_dir=combined_fastq_dir)

    logger.info("Writing QC manifest TSV (%d sample(s)) to %s", len(samples), manifest_tsv)
    _write_samples_tsv(samples, manifest_tsv)

    return samples


@task(name="demux_bcl_to_fastqs_task", log_prints=True)
def demux_bcl_to_fastqs_task(
    bcl_dir: Path,
    samplesheet: Path,
    outdir: Path | str,
    manifest_tsv: Path | None = None,
) -> list[Sample]:
    """Task wrapper around :func:`_demux_bcl_to_fastqs_impl`."""
    return _demux_bcl_to_fastqs_impl(
        bcl_dir=Path(bcl_dir),
        samplesheet=Path(samplesheet),
        outdir=Path(outdir),
        manifest_tsv=manifest_tsv,
    )


@flow(name="bcl-demux-qc", log_prints=True)
def demux_bcl_to_qc(
    bcl_dir: Path,
    samplesheet: Path,
    outdir: Path | str,
    mode: str,
    threads: int = 4,
) -> None:
    """
    Demultiplex BCL input with bcl-convert and feed resulting FASTQs into the QC pipeline.

    Output layout:
      - `${outdir}/demux_fastq/` : bcl-convert output + a `combined/` subdir with merged per-sample FASTQs
      - `${outdir}/qc/` : reports from `QC.flow.fastq_qc_flow`
    """
    logger = get_run_logger()
    outdir = Path(outdir)

    samples = demux_bcl_to_fastqs(bcl_dir=bcl_dir, samplesheet=samplesheet, outdir=outdir)

    qc_outdir = outdir / "qc"
    logger.info("Running QC (%s) for %d sample(s) into %s", mode, len(samples), qc_outdir)
    fastq_qc_flow(samples=samples, outdir=qc_outdir, mode=mode, threads=threads)


def run_demux_qc_pipeline(
    *,
    bcl_dir: Path,
    samplesheet: Path,
    outdir: Path | str,
    mode: str,
    threads: int = 4,
    manifest_tsv: Path | None = None,
    contamination_tool: str | None = None,
    kraken_db: Path | None = None,
    fastq_screen_conf: Path | None = None,
) -> None:
    """
    Plain Python helper that runs the full demux+QC pipeline (no CLI parsing).

    This is useful when you want to orchestrate in-process from your own code.
    """
    outdir_path = Path(outdir)
    samples = demux_bcl_to_fastqs(
        bcl_dir=bcl_dir,
        samplesheet=samplesheet,
        outdir=outdir_path,
        manifest_tsv=manifest_tsv,
    )
    qc_outdir = outdir_path / "qc"
    fastq_qc_flow(samples=samples, outdir=qc_outdir, mode=mode, threads=threads)

    if contamination_tool:
        contamination_flow(
            samples=samples,
            outdir=qc_outdir,
            tool=contamination_tool,
            threads=threads,
            kraken_db=kraken_db,
            fastq_screen_conf=fastq_screen_conf,
        )
