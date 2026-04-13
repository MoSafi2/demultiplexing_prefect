from __future__ import annotations

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def test_pyproject_exposes_console_script() -> None:
    text = (REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8")
    assert 'name = "demux-pipeline"' in text
    assert 'demux-pipeline = "demux_pipeline.cli:main"' in text


def test_pixi_exposes_cli_task() -> None:
    text = (REPO_ROOT / "pixi.toml").read_text(encoding="utf-8")
    assert "[tasks]" in text
    assert 'demux-pipeline = "python -m demux_pipeline.cli"' in text


def test_generic_runner_uses_cli_entrypoint() -> None:
    text = (REPO_ROOT / "run.sh").read_text(encoding="utf-8")
    assert "require_cmd bcl-convert" in text
    assert "require_cmd pixi" in text
    assert 'declare -a CMD=(pixi run demux-pipeline "$@")' in text
    assert 'CMD+=(--output-contract-file "$OUTPUT_CONTRACT_FILE")' in text


def test_bpm_template_files_removed() -> None:
    assert not (REPO_ROOT / "run.sh.j2").exists()
    assert not (REPO_ROOT / "template_config.yaml").exists()
