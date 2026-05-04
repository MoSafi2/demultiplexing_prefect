from __future__ import annotations

from pathlib import Path
import json

from demux_pipeline.models import Sample
from demux_pipeline import qc
from demux_pipeline.pipeline import write_output_contract


def _touch(path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("", encoding="utf-8")
    return path


def test_sample_project_dir_uses_project_when_present(tmp_path: Path) -> None:
    sample = Sample(
        name="s1",
        r1=tmp_path / "output" / "project-a" / "s1_R1_001.fastq.gz",
        project="project-a",
    )

    assert qc._sample_project_dir(tmp_path / "fastqc", sample) == (
        tmp_path / "output" / "project-a" / "qc" / "fastqc"
    )


def test_sample_project_dir_keeps_legacy_flat_layout_without_project(tmp_path: Path) -> None:
    sample = Sample(name="s1", r1=tmp_path / "output" / "s1_R1_001.fastq.gz")

    assert qc._sample_project_dir(tmp_path / "fastqc", sample) == tmp_path / "fastqc"


def test_project_names_from_demux_output_uses_fastq_project_dirs(tmp_path: Path) -> None:
    outdir = tmp_path / "out"
    _touch(outdir / "output" / "project-a" / "s1_S1_R1_001.fastq.gz")
    _touch(outdir / "output" / "project-b" / "s2_S1_R1_001.fastq.gz")
    _touch(outdir / "output" / "Reports" / "Demultiplex_Stats.csv")
    _touch(outdir / "output" / "top_level_S1_R1_001.fastq.gz")

    assert qc._project_names_from_demux_output(outdir) == ["project-a", "project-b"]


def test_output_contract_exports_project_multiqc_reports(tmp_path: Path) -> None:
    outdir = tmp_path / "out"
    _touch(outdir / "output" / "project-a" / "s1_S1_R1_001.fastq.gz")
    report = _touch(
        outdir / "output" / "project-a" / "qc" / "multiqc" / "multiqc_report.html"
    )
    contract = tmp_path / "contract.json"

    write_output_contract(outdir=outdir, artifact_path=contract)

    payload = json.loads(contract.read_text(encoding="utf-8"))
    assert payload["outputs"]["project_multiqc_reports"] == {
        "project-a": str(report)
    }
