#!/usr/bin/env python3
"""Smoke test for demux->QC flow using mocked demultiplexing."""

from __future__ import annotations

import argparse
import gzip
import shutil
from pathlib import Path
from unittest.mock import patch

def write_tiny_fastq_gz(path: Path, read_name: str = "smoke_read") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    block = f"@{read_name}/1\nACGTACGT\n+\nIIIIIIII\n".encode("ascii")
    with gzip.open(path, "wb") as f:
        f.write(block)


def _parse_modes(raw: str) -> list[str]:
    modes = [m.strip().lower() for m in raw.split(",") if m.strip()]
    allowed = {"fastqc", "fastp", "falco"}
    unknown = set(modes) - allowed
    if unknown:
        raise SystemExit(
            f"Unknown --modes value(s): {', '.join(sorted(unknown))}. Allowed: {', '.join(sorted(allowed))}"
        )
    return modes


def main(argv: list[str] | None = None) -> None:
    from demux_pipeline.pipeline import demux_pipeline

    parser = argparse.ArgumentParser(
        description="Run demux_pipeline with synthetic demux output (smoke test).",
    )
    parser.add_argument(
        "--outdir",
        type=Path,
        required=True,
        help="Base directory; each QC tool writes to a subdirectory.",
    )
    parser.add_argument(
        "--modes",
        default="falco",
        help="Comma-separated QC tools to run: fastqc, fastp, falco (default: falco).",
    )
    parser.add_argument(
        "--single",
        action="store_true",
        help="Run only the first mode from --modes.",
    )
    parser.add_argument(
        "--threads",
        type=int,
        default=2,
        help="Global thread budget for parallel QC steps (default: 2).",
    )
    args = parser.parse_args(argv)

    modes = _parse_modes(args.modes)
    if args.single:
        modes = modes[:1]
    if not modes:
        raise SystemExit("No modes to run after parsing --modes.")

    args.outdir.mkdir(parents=True, exist_ok=True)

    for qc_tool in modes:
        run_dir = args.outdir / f"qc_only_{qc_tool}"
        if run_dir.exists():
            shutil.rmtree(run_dir)
        run_dir.mkdir(parents=True, exist_ok=True)

        fastq_dir = run_dir / "input"
        fq_path = fastq_dir / "smoke_S1_L001_R1_001.fastq.gz"
        write_tiny_fastq_gz(fq_path)

        outdir = run_dir / "out"
        print(
            f"Smoke: demux_pipeline qc_tool={qc_tool!r} outdir={outdir}", flush=True
        )
        bcl_dir = run_dir / "bcl"
        samplesheet = run_dir / "SampleSheet.csv"
        bcl_dir.mkdir(parents=True, exist_ok=True)
        samplesheet.write_text("dummy\n", encoding="utf-8")

        def _mock_demux(**_) -> None:
            demux_out = outdir / "output"
            write_tiny_fastq_gz(demux_out / "smoke_S1_L001_R1_001.fastq.gz")

        with patch("demux_pipeline.pipeline.demux_bcl", side_effect=_mock_demux):
            demux_pipeline(
                qc_tool=qc_tool,
                thread_budget=args.threads,
                outdir=outdir,
                bcl_dir=bcl_dir,
                samplesheet=samplesheet,
            )


if __name__ == "__main__":
    main()
