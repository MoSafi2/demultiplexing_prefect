from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from demux_pipeline.pipeline import demux_pipeline

ALLOWED_QC_TOOLS = {"fastqc", "fastp", "falco"}
ALLOWED_CONTAMINATION_TOOLS = {"kraken", "kraken_bracken", "fastq_screen", "none"}


@dataclass(frozen=True, slots=True)
class PipelineRunConfig:
    bcl_dir: Path
    samplesheet: Path
    qc_tool: str
    thread_budget: int
    outdir: Path
    run_name: str | None
    contamination_tool: str | None
    kraken_db: Path | None
    bracken_db: Path | None
    fastq_screen_conf: Path | None
    output_contract_file: Path | None


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

    parser.add_argument(
        "--bcl_dir",
        required=True,
        type=Path,
        help="BCL run folder for bcl-convert.",
    )
    parser.add_argument(
        "--samplesheet",
        required=True,
        type=Path,
        help="Sample sheet passed to bcl-convert as --sample-sheet.",
    )

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
    parser.add_argument(
        "--output-contract-file",
        required=False,
        type=Path,
        default=None,
        help=(
            "Optional JSON file path to write template output contract "
            "(samples_tsv, qc_dir, contamination_dir, multiqc_report, run_summary)."
        ),
    )

    return parser


def _validate_args(
    parser: argparse.ArgumentParser, argv: list[str] | None = None
) -> argparse.Namespace:
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


def build_run_config(args: argparse.Namespace) -> PipelineRunConfig:
    return PipelineRunConfig(
        bcl_dir=args.bcl_dir,
        samplesheet=args.samplesheet,
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
        output_contract_file=args.output_contract_file,
    )


def parse_and_build_config(argv: list[str] | None = None) -> PipelineRunConfig:
    parser = _build_parser()
    args = _validate_args(parser, argv)
    return build_run_config(args)


def run_with_config(config: PipelineRunConfig) -> None:
    demux_pipeline(
        bcl_dir=config.bcl_dir,
        samplesheet=config.samplesheet,
        qc_tool=config.qc_tool,
        thread_budget=config.thread_budget,
        outdir=config.outdir,
        run_name=config.run_name,
        contamination_tool=config.contamination_tool,
        kraken_db=config.kraken_db,
        bracken_db=config.bracken_db,
        fastq_screen_conf=config.fastq_screen_conf,
        output_contract_file=config.output_contract_file,
    )


def main(argv: list[str] | None = None) -> None:
    run_with_config(parse_and_build_config(argv))


if __name__ == "__main__":
    main(sys.argv[1:])
