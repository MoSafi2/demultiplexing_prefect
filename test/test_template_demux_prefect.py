from __future__ import annotations

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
TEMPLATE_DIR = REPO_ROOT


def test_template_shell_runner_exists() -> None:
    path = TEMPLATE_DIR / "run.sh.j2"
    assert path.exists()


def test_template_shell_runner_uses_pixi_cli_interface() -> None:
    text = (TEMPLATE_DIR / "run.sh.j2").read_text(encoding="utf-8")
    assert "require_cmd bcl-convert" in text
    assert "require_cmd pixi" in text
    assert "pixi run python demux_pipeline/cli.py" in text
    assert '--output-contract-file "template_outputs.json"' in text
    assert "pixi run python - <<'PY'" not in text


def test_template_shell_runner_maps_required_flags() -> None:
    text = (TEMPLATE_DIR / "run.sh.j2").read_text(encoding="utf-8")
    assert '--outdir "{{ ctx.params.outdir }}"' in text
    assert '--bcl_dir "{{ ctx.params.bcl_dir }}"' in text
    assert '--samplesheet "{{ ctx.params.samplesheet }}"' in text
    assert '--qc-tool "{{ ctx.params.get(\'qc_tool\', \'falco\') }}"' in text
    assert '--contamination-tool "{{ ctx.params.get(\'contamination_tool\', \'none\') }}"' in text
    assert '--threads "{{ ctx.params.get(\'threads\', 4) }}"' in text


def test_template_descriptor_has_no_publish_block() -> None:
    descriptor = (TEMPLATE_DIR / "template_config.yaml").read_text(encoding="utf-8")
    assert "publish:" not in descriptor


def test_template_descriptor_runs_shell_entry() -> None:
    descriptor = (TEMPLATE_DIR / "template_config.yaml").read_text(encoding="utf-8")
    assert "run.sh.j2 -> run.sh" in descriptor
    assert 'entry: "run.sh"' in descriptor
    assert "run.py -> run.py" not in descriptor
    assert 'entry: "run.py"' not in descriptor
