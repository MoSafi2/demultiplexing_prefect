from __future__ import annotations

import importlib.util
import json
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
TEMPLATE_DIR = REPO_ROOT


def _load_run_module():
    path = TEMPLATE_DIR / "run.py"
    spec = importlib.util.spec_from_file_location("template_demux_prefect_run", path)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class _Ctx:
    def __init__(self, params: dict[str, object]) -> None:
        self.params = params


def test_template_run_builds_argv_demux_mode() -> None:
    mod = _load_run_module()
    ctx = _Ctx(
        {
            "outdir": "/tmp/out",
            "bcl_dir": "/tmp/bcl",
            "samplesheet": "/tmp/SampleSheet.csv",
            "qc_tool": "falco",
            "threads": 4,
        }
    )
    argv = mod._build_argv_from_ctx(ctx)
    assert "--outdir" in argv and "/tmp/out" in argv
    assert "--bcl_dir" in argv and "/tmp/bcl" in argv
    assert "--samplesheet" in argv and "/tmp/SampleSheet.csv" in argv


def test_infer_outputs_returns_expected_keys(tmp_path: Path) -> None:
    mod = _load_run_module()
    outdir = tmp_path / "out"
    (outdir / "fastqc").mkdir(parents=True)
    (outdir / "contamination").mkdir(parents=True)
    (outdir / "multiqc").mkdir(parents=True)
    (outdir / "multiqc" / "multiqc_report.html").write_text("ok", encoding="utf-8")
    (outdir / ".pipeline" / "runA").mkdir(parents=True)
    (outdir / ".pipeline" / "runA" / "run_summary.json").write_text(
        "{}",
        encoding="utf-8",
    )
    outputs = mod.infer_outputs(outdir)
    assert outputs["samples_tsv"] == str(outdir / "samples.tsv")
    assert outputs["qc_dir"] == str(outdir / "fastqc")
    assert outputs["contamination_dir"] == str(outdir / "contamination")
    assert outputs["multiqc_report"] == str(outdir / "multiqc" / "multiqc_report.html")
    assert outputs["run_summary"] == str(outdir / ".pipeline" / "runA" / "run_summary.json")


def test_write_output_contract_writes_json(tmp_path: Path) -> None:
    mod = _load_run_module()
    outdir = tmp_path / "out"
    outdir.mkdir(parents=True)
    outputs = {
        "samples_tsv": str(outdir / "samples.tsv"),
        "qc_dir": None,
        "contamination_dir": None,
        "multiqc_report": None,
        "run_summary": None,
    }
    artifact = mod.write_output_contract(outdir, outputs, artifact_dir=tmp_path)
    payload = json.loads(artifact.read_text(encoding="utf-8"))
    assert payload["outdir"] == str(outdir)
    assert payload["outputs"]["samples_tsv"] == str(outdir / "samples.tsv")


def test_template_descriptor_has_no_publish_block() -> None:
    descriptor = (TEMPLATE_DIR / "template_config.yaml").read_text(encoding="utf-8")
    assert "publish:" not in descriptor
