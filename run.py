#!/usr/bin/env python3
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from cli import parse_and_build_config, run_with_config


def _param(params: dict[str, Any], key: str, default: Any = None) -> Any:
    value = params.get(key, default)
    return default if value is None else value


def _build_argv_from_ctx(ctx: Any) -> list[str]:
    params = dict(getattr(ctx, "params", {}) or {})
    argv: list[str] = [
        "--outdir",
        str(_param(params, "outdir")),
        "--bcl_dir",
        str(_param(params, "bcl_dir")),
        "--samplesheet",
        str(_param(params, "samplesheet")),
    ]

    optional_map = {
        "run_name": "--run-name",
        "qc_tool": "--qc-tool",
        "contamination_tool": "--contamination-tool",
        "threads": "--threads",
        "kraken_db": "--kraken-db",
        "bracken_db": "--bracken-db",
        "fastq_screen_conf": "--fastq-screen-conf",
    }
    for param_name, flag in optional_map.items():
        value = _param(params, param_name, None)
        if value not in (None, ""):
            argv.extend([flag, str(value)])

    return argv


def infer_outputs(outdir: Path) -> dict[str, str | None]:
    qc_dir = None
    for candidate in (outdir / "falco", outdir / "fastqc", outdir / "fastp"):
        if candidate.exists():
            qc_dir = str(candidate)
            break

    contamination_dir = outdir / "contamination"
    multiqc_report = outdir / "multiqc" / "multiqc_report.html"
    summaries = sorted((outdir / ".pipeline").glob("*/run_summary.json"))

    return {
        "samples_tsv": str(outdir / "samples.tsv"),
        "qc_dir": qc_dir,
        "contamination_dir": (
            str(contamination_dir) if contamination_dir.exists() else None
        ),
        "multiqc_report": str(multiqc_report) if multiqc_report.exists() else None,
        "run_summary": str(summaries[-1]) if summaries else None,
    }


def write_output_contract(
    outdir: Path,
    outputs: dict[str, str | None],
    artifact_dir: Path | None = None,
) -> Path:
    artifact_root = artifact_dir if artifact_dir is not None else Path.cwd()
    artifact = artifact_root / "template_outputs.json"
    payload = {
        "outdir": str(outdir),
        "outputs": outputs,
    }
    artifact.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return artifact


def main(ctx: Any | None = None) -> None:
    if ctx is None:
        raise SystemExit("run.py expects BPM context argument `ctx`.")
    argv = _build_argv_from_ctx(ctx)
    config = parse_and_build_config(argv)
    run_with_config(config)
    outputs = infer_outputs(Path(config.outdir))
    write_output_contract(Path(config.outdir), outputs)


if __name__ == "__main__":
    # Local debugging helper: run from shell with regular CLI flags.
    config = parse_and_build_config()
    run_with_config(config)
    outputs = infer_outputs(Path(config.outdir))
    write_output_contract(Path(config.outdir), outputs)
