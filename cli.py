from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Iterable

# This repo's pipeline modules use "flat" imports like `from models import Sample`
# (i.e., they expect `demux_pipeline/` to be on `PYTHONPATH`).
#
# The top-level `cli.py` makes that implicit so users can run:
#   pixi run python cli.py ...
DEMUX_PIPELINE_DIR = Path(__file__).resolve().parent / "demux_pipeline"
if str(DEMUX_PIPELINE_DIR) not in sys.path:
    sys.path.insert(0, str(DEMUX_PIPELINE_DIR))

from demux_pipeline.pipeline import demux_pipeline  # noqa: E402

ALLOWED_QC_TOOLS = {"fastqc", "fastp", "falco"}
ALLOWED_CONTAMINATION_TOOLS = {"kraken", "kraken_bracken", "fastq_screen", "none"}


def _parse_tool_csv(
    raw: str | None,
    *,
    allowed: Iterable[str],
    flag_name: str,
) -> list[str]:
    if raw is None:
        return []
    allowed_set = {s.lower() for s in allowed}
    values = [token.strip().lower() for token in str(raw).split(",")]
    values = [v for v in values if v]
    if not values:
        return []
    bad = [v for v in values if v not in allowed_set]
    if bad:
        raise argparse.ArgumentTypeError(
            f"Unknown value(s) for {flag_name}: {', '.join(bad)}. "
            f"Allowed: {', '.join(sorted(allowed_set))}."
        )
    deduped: list[str] = []
    seen: set[str] = set()
    for v in values:
        if v not in seen:
            deduped.append(v)
            seen.add(v)
    return deduped


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
        required=True,
        default="falco",
        help=(
            "Which QC program(s) to run: fastqc, fastp, falco. "
            "Use comma-separated values to run multiple (e.g. fastqc,fastp). "
            "Default: falco."
        ),
    )

    parser.add_argument(
        "--threads",
        required=False,
        default=4,
        type=int,
        help=(
            "Global CPU thread budget for parallel QC and contamination steps. "
            "Concurrency and per-sample tool thread counts are chosen so this limit "
            "is not exceeded during mapped work."
        ),
    )

    parser.add_argument(
        "--outdir",
        required=True,
        type=Path,
        help="Root output directory.",
    )

    parser.add_argument(
        "--run-name",
        required=False,
        default=None,
        help=(
            "Optional user-friendly label for this pipeline run. "
            "Used for Prefect flow run naming and pipeline tracking artifacts."
        ),
    )

    # demux inputs (required when --mode demux)
    parser.add_argument(
        "--bcl_dir",
        required=False,
        type=Path,
        help="BCL run folder for bcl-convert.",
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
        help=(
            "Manifest TSV path. In demux mode it overrides the output path; in QC mode it is the input manifest."
        ),
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
        default=None,
        help=(
            "Optional contamination tool(s) after QC: kraken (Kraken2 only), "
            "kraken_bracken (Kraken2 then Bracken; same DB path as kraken after bracken-build), "
            "fastq_screen, or none. Use comma-separated values to run multiple tools "
            "(default: disabled)."
        ),
    )

    parser.add_argument(
        "--kraken-db",
        required=False,
        type=Path,
        default=None,
        help=(
            "Kraken2 DB path (required for --contamination-tool kraken; "
            "for kraken_bracken use this or --bracken-db, typically the same directory)."
        ),
    )

    parser.add_argument(
        "--bracken-db",
        required=False,
        type=Path,
        default=None,
        help=(
            "Kraken2 DB directory used by Bracken (-d; run bracken-build there after "
            "installing a Kraken2 index). For kraken_bracken, omit if --kraken-db is set "
            "(same path)."
        ),
    )

    parser.add_argument(
        "--fastq-screen-conf",
        required=False,
        type=Path,
        default=None,
        help="Path to FastQ Screen config file.",
    )

    return parser


def _validate_args(
    parser: argparse.ArgumentParser, argv: list[str] | None = None
) -> argparse.Namespace:
    """Validate command line arguments."""
    args = parser.parse_args(argv)

    try:
        qc_tools = _parse_tool_csv(
            args.qc_tool,
            allowed=ALLOWED_QC_TOOLS,
            flag_name="--qc-tool",
        )
    except argparse.ArgumentTypeError as exc:
        parser.error(str(exc))
    if not qc_tools:
        parser.error("--qc-tool requires at least one value.")
    args.qc_tool = ",".join(qc_tools)

    try:
        contamination_tools = _parse_tool_csv(
            args.contamination_tool,
            allowed=ALLOWED_CONTAMINATION_TOOLS,
            flag_name="--contamination-tool",
        )
    except argparse.ArgumentTypeError as exc:
        parser.error(str(exc))
    if "none" in contamination_tools and len(contamination_tools) > 1:
        parser.error(
            "--contamination-tool value 'none' cannot be combined with other tools."
        )
    args.contamination_tool = ",".join(contamination_tools) if contamination_tools else None

    if args.mode.lower() == "demux":
        if args.bcl_dir is None or args.samplesheet is None:
            parser.error("--mode demux requires --bcl_dir and --samplesheet.")
    elif args.mode.lower() == "qc":
        # QC-only mode
        if (args.manifest_tsv is None and args.in_fastq_dir is None) or (
            args.manifest_tsv and args.in_fastq_dir
        ):
            parser.error(
                "In --mode QC, provide exactly one of --manifest-tsv or --in-fastq-dir."
            )

    contamination_set = set(contamination_tools)
    if "kraken" in contamination_set:
        if args.kraken_db is None:
            parser.error("--kraken-db is required when --contamination-tool kraken.")

    if "kraken_bracken" in contamination_set:
        if args.bracken_db is None and args.kraken_db is None:
            parser.error(
                "--bracken-db or --kraken-db is required when --contamination-tool "
                "kraken_bracken (both point at the same Kraken2 directory after bracken-build)."
            )
        if args.bracken_db is None:
            args.bracken_db = args.kraken_db

    if "fastq_screen" in contamination_set and args.fastq_screen_conf is None:
        parser.error(
            "--fastq-screen-conf is required when --contamination-tool fastq_screen."
        )

    if args.threads < 1:
        parser.error("--threads must be at least 1.")

    return args


def main(argv: list[str]) -> None:
    parser = _build_parser()
    args = _validate_args(parser)

    demux_pipeline(
        bcl_dir=args.bcl_dir if args.mode == "demux" else None,
        samplesheet=args.samplesheet if args.mode == "demux" else None,
        manifest_tsv=args.manifest_tsv if args.mode == "qc" else None,
        in_fastq_dir=args.in_fastq_dir if args.mode == "qc" else None,
        qc_tool=args.qc_tool,
        thread_budget=args.threads,
        outdir=args.outdir,
        run_name=args.run_name,
        contamination_tool=(
            None
            if args.contamination_tool in (None, "none")
            else args.contamination_tool
        ),
        kraken_db=args.kraken_db,
        bracken_db=args.bracken_db,
        fastq_screen_conf=args.fastq_screen_conf,
    )


if __name__ == "__main__":
    main(sys.argv[1:])
