from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest  # type: ignore[import-not-found]

REPO_ROOT = Path(__file__).resolve().parents[1]


def _import_cli():
    if "demux_pipeline.cli" in sys.modules and hasattr(
        sys.modules["demux_pipeline.cli"], "_validate_args"
    ):
        return sys.modules["demux_pipeline.cli"]
    if str(REPO_ROOT) not in sys.path:
        sys.path.insert(0, str(REPO_ROOT))
    path = REPO_ROOT / "demux_pipeline" / "cli.py"
    spec = importlib.util.spec_from_file_location("demux_pipeline.cli", path)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules["demux_pipeline.cli"] = mod
    spec.loader.exec_module(mod)
    return mod


def _base_args(tmp_path: Path) -> list[str]:
    return [
        "--bcl_dir",
        str(tmp_path / "bcl"),
        "--samplesheet",
        str(tmp_path / "SampleSheet.csv"),
        "--outdir",
        str(tmp_path / "out"),
        "--qc-tool",
        "falco",
    ]


def test_validate_args_parses_and_dedupes_multi_tool_flags(tmp_path: Path) -> None:
    cli_mod = _import_cli()
    parser = cli_mod._build_parser()
    args = cli_mod._validate_args(
        parser,
        _base_args(tmp_path)
        + [
            "--qc-tool",
            "fastqc,fastp,fastqc",
            "--contamination-tool",
            "kraken,fastq_screen,kraken",
            "--kraken-db",
            str(tmp_path / "db"),
            "--fastq-screen-conf",
            str(tmp_path / "fastq_screen.conf"),
        ],
    )
    assert args.qc_tool == "fastqc,fastp"
    assert args.contamination_tool == "kraken,fastq_screen"


def test_validate_args_rejects_none_combined_with_other_tool(tmp_path: Path) -> None:
    cli_mod = _import_cli()
    parser = cli_mod._build_parser()
    with pytest.raises(SystemExit):
        cli_mod._validate_args(
            parser,
            _base_args(tmp_path)
            + ["--contamination-tool", "none,kraken", "--kraken-db", str(tmp_path / "db")],
        )


def test_validate_args_requires_tool_specific_dependencies_in_multi_mode(
    tmp_path: Path,
) -> None:
    cli_mod = _import_cli()
    parser = cli_mod._build_parser()
    with pytest.raises(SystemExit):
        cli_mod._validate_args(
            parser,
            _base_args(tmp_path)
            + ["--contamination-tool", "kraken,fastq_screen"],
        )


def test_validate_args_allows_none_only_for_contamination(tmp_path: Path) -> None:
    cli_mod = _import_cli()
    parser = cli_mod._build_parser()
    args = cli_mod._validate_args(
        parser,
        _base_args(tmp_path) + ["--contamination-tool", "none"],
    )
    assert args.contamination_tool == "none"


def test_build_run_config_maps_validated_args(tmp_path: Path) -> None:
    cli_mod = _import_cli()
    parser = cli_mod._build_parser()
    args = cli_mod._validate_args(
        parser,
        _base_args(tmp_path)
        + [
            "--qc-tool",
            "fastqc,fastp",
            "--contamination-tool",
            "none",
            "--threads",
            "3",
        ],
    )
    cfg = cli_mod.build_run_config(args)
    assert cfg.bcl_dir == tmp_path / "bcl"
    assert cfg.samplesheet == tmp_path / "SampleSheet.csv"
    assert cfg.qc_tool == "fastqc,fastp"
    assert cfg.thread_budget == 3
    assert cfg.contamination_tool is None
    assert cfg.output_contract_file is None


def test_build_run_config_maps_output_contract_file(tmp_path: Path) -> None:
    cli_mod = _import_cli()
    parser = cli_mod._build_parser()
    contract_path = tmp_path / "template_outputs.json"
    args = cli_mod._validate_args(
        parser,
        _base_args(tmp_path) + ["--output-contract-file", str(contract_path)],
    )
    cfg = cli_mod.build_run_config(args)
    assert cfg.output_contract_file == contract_path
