from __future__ import annotations

import argparse
import sys
from pathlib import Path

from pipeline import unified_demux_qc_contamination_pipeline


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Unified demux -> QC -> optional contamination -> MULTIQC Prefect pipeline.",
    )

    parser.add_argument(
        "--mode",
        required=True,
        choices=["demux", "qc"],
        help="Pipeline stage: `demux` (run demux) or `qc` (skip demux; QC only).",
    )
    parser.add_argument(
        "--qc-tool",
        required=False,
        default="falco",
        choices=["fastqc", "fastp", "falco"],
        help="Which QC program to run: fastqc|fastp|falco. Default: falco.",
    )
    parser.add_argument(
        "--threads", required=False, default=4, type=int, help="Threads for fastp/QC."
    )
    parser.add_argument(
        "--outdir", required=True, type=Path, help="Root output directory."
    )

    # demux inputs (required when --mode demux)
    parser.add_argument(
        "--bcl_dir", required=False, type=Path, help="BCL run folder for bcl-convert."
    )
    parser.add_argument(
        "--samplesheet",
        required=False,
        type=Path,
        help="Sample sheet passed to bcl-convert as --sample-sheet.",
    )
    # manifest TSV:
    # - output override for demux mode (required when --mode demux)
    # - input manifest for QC-only mode
    parser.add_argument(
        "--manifest-tsv",
        required=False,
        type=Path,
        default=None,
        help="Manifest TSV path. In demux mode it overrides the output path; in QC mode it is the input manifest.",
    )
    # QC inputs for QC-only mode
    parser.add_argument(
        "--in-fastq-dir",
        required=False,
        type=Path,
        default=None,
        help="Directory containing *_R1.fastq.gz and optional *_R2.fastq.gz files (required when --mode qc).",
    )

    # optional contamination
    parser.add_argument(
        "--contamination-tool",
        required=False,
        choices=["kraken", "fastq_screen"],
        default=None,
        help="Optional contamination tool to run after QC (default: disabled).",
    )
    parser.add_argument(
        "--kraken-db", required=False, type=Path, default=None, help="Kraken2 DB path."
    )
    parser.add_argument(
        "--fastq-screen-conf",
        required=False,
        type=Path,
        default=None,
        help="Path to FastQ Screen config file.",
    )

    return parser


def _validate_args(parser: argparse.ArgumentParser) -> argparse.Namespace:
    """Validate command line arguments."""
    
    args = parser.parse_args()
    if args.mode.lower() == "demux":
        if args.bcl_dir is None or args.samplesheet is None:
            parser.error("--mode demux requires --bcl_dir and --samplesheet.")
    else:
        # QC-only mode
        if (args.manifest_tsv is None and args.in_fastq_dir is None) or (
            args.manifest_tsv and args.in_fastq_dir
        ):
            parser.error(
                "In --mode QC, provide exactly one of --manifest-tsv or --in-fastq-dir."
            )

    if args.contamination_tool == "kraken" and args.kraken_db is None:
        parser.error("--kraken-db is required when --contamination-tool kraken.")
    if args.contamination_tool == "fastq_screen" and args.fastq_screen_conf is None:
        parser.error(
            "--fastq-screen-conf is required when --contamination-tool fastq_screen."
        )

    return args


def main(argv: list[str]) -> None:
    parser = _build_parser()
    args = _validate_args(parser)

    unified_demux_qc_contamination_pipeline(
        mode=args.mode,
        qc_tool=args.qc_tool,
        threads=args.threads,
        outdir=args.outdir,
        bcl_dir=args.bcl_dir,
        samplesheet=args.samplesheet,
        manifest_tsv=args.manifest_tsv,
        in_fastq_dir=args.in_fastq_dir,
        contamination_tool=args.contamination_tool,
        kraken_db=args.kraken_db,
        fastq_screen_conf=args.fastq_screen_conf,
    )


if __name__ == "__main__":
    main(sys.argv[1:])
