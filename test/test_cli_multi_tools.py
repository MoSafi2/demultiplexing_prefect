from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest  # type: ignore[import-not-found]

REPO_ROOT = Path(__file__).resolve().parents[1]


def _import_cli():
    if "cli" in sys.modules and hasattr(sys.modules["cli"], "_validate_args"):
        return sys.modules["cli"]
    if str(REPO_ROOT) not in sys.path:
        sys.path.insert(0, str(REPO_ROOT))
    path = REPO_ROOT / "cli.py"
    spec = importlib.util.spec_from_file_location("cli", path)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules["cli"] = mod
    spec.loader.exec_module(mod)
    return mod


def _base_qc_args(tmp_path: Path) -> list[str]:
    return [
        "--mode",
        "qc",
        "--manifest-tsv",
        str(tmp_path / "manifest.tsv"),
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
        _base_qc_args(tmp_path)
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
            _base_qc_args(tmp_path)
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
            _base_qc_args(tmp_path)
            + ["--contamination-tool", "kraken,fastq_screen"],
        )


def test_validate_args_allows_none_only_for_contamination(tmp_path: Path) -> None:
    cli_mod = _import_cli()
    parser = cli_mod._build_parser()
    args = cli_mod._validate_args(
        parser,
        _base_qc_args(tmp_path) + ["--contamination-tool", "none"],
    )
    assert args.contamination_tool == "none"
