"""Smoke checks for `qc_only_pipeline` (mocked CLIs for speed)."""

from __future__ import annotations

import gzip
import importlib.util
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch
from unittest.mock import ANY

import pytest  # type: ignore[import-not-found]

REPO_ROOT = Path(__file__).resolve().parents[1]


def _load_repo_module(name: str, relative_path: str):
    path = REPO_ROOT / relative_path
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


def _import_pipeline():
    """Load `pipeline.py` without going through `import demux` package shadowing."""
    if "pipeline" in sys.modules and hasattr(
        sys.modules["pipeline"], "qc_only_pipeline"
    ):
        return sys.modules["pipeline"]
    _load_repo_module("models", "demux_pipeline/models.py")
    _load_repo_module("process", "demux_pipeline/process.py")
    _load_repo_module("demux", "demux_pipeline/demux.py")
    _load_repo_module("qc", "demux_pipeline/qc.py")
    _load_repo_module("contamination", "demux_pipeline/contamination.py")
    return _load_repo_module("pipeline", "demux_pipeline/pipeline.py")


def _tiny_gz(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(path, "wb") as f:
        f.write(b"@r/1\nACGT\n+\nIIII\n")


def test_qc_only_pipeline_smoke_mocked(tmp_path: Path) -> None:
    pipeline_mod = _import_pipeline()
    fq = tmp_path / "sample_S1_L001_R1_001.fastq.gz"
    _tiny_gz(fq)
    manifest = tmp_path / "manifest.tsv"
    manifest.write_text(f"sample\t{fq}\n", encoding="utf-8")
    outdir = tmp_path / "out"

    with patch.object(
        pipeline_mod, "qc_phase", MagicMock()
    ) as run_qc_phase, patch.object(
        pipeline_mod, "run_multiqc", MagicMock()
    ) as run_mq, patch.object(
        pipeline_mod.Observer, "publish_prefect_artifacts", MagicMock()
    ) as publish_obs:
        pipeline_mod.qc_only_pipeline(
            qc_tool="falco",
            thread_budget=1,
            outdir=outdir,
            run_name="unit_test",
            manifest_tsv=manifest,
        )

    run_qc_phase.assert_called_once()
    run_mq.assert_called_once()
    publish_obs.assert_called_once()


def test_qc_only_pipeline_contamination_sequential(tmp_path: Path) -> None:
    pipeline_mod = _import_pipeline()
    fq = tmp_path / "sample_S1_L001_R1_001.fastq.gz"
    _tiny_gz(fq)
    manifest = tmp_path / "manifest.tsv"
    manifest.write_text(f"sample\t{fq}\n", encoding="utf-8")
    outdir = tmp_path / "out"

    with patch.object(pipeline_mod, "qc_phase", MagicMock()) as run_qc_phase, patch.object(
        pipeline_mod, "contamination_phase", MagicMock()
    ) as run_contam_phase, patch.object(
        pipeline_mod, "run_multiqc", MagicMock()
    ) as run_mq, patch.object(
        pipeline_mod.Observer, "publish_prefect_artifacts", MagicMock()
    ):
        pipeline_mod.qc_only_pipeline(
            qc_tool="falco",
            thread_budget=1,
            outdir=outdir,
            run_name="unit_test",
            manifest_tsv=manifest,
            contamination_tool="kraken",
            kraken_db=tmp_path / "db",
        )

    run_qc_phase.assert_called_once()
    run_contam_phase.assert_called_once()
    run_mq.assert_called_once_with(
        outdir,
        [],
        include_contamination=True,
        observer=ANY,
    )


def test_qc_only_pipeline_contamination_parallel_submits_both(tmp_path: Path) -> None:
    pipeline_mod = _import_pipeline()
    fq = tmp_path / "sample_S1_L001_R1_001.fastq.gz"
    _tiny_gz(fq)
    manifest = tmp_path / "manifest.tsv"
    manifest.write_text(f"sample\t{fq}\n", encoding="utf-8")
    outdir = tmp_path / "out"

    class _Future:
        def result(self) -> None:
            return None

    with patch.object(pipeline_mod._run_qc_phase, "submit", return_value=_Future()) as qc_submit, patch.object(
        pipeline_mod._run_contamination_phase, "submit", return_value=_Future()
    ) as contam_submit, patch.object(
        pipeline_mod, "run_multiqc", MagicMock()
    ) as run_mq, patch.object(
        pipeline_mod.Observer, "publish_prefect_artifacts", MagicMock()
    ):
        pipeline_mod.qc_only_pipeline(
            qc_tool="falco",
            thread_budget=4,
            outdir=outdir,
            run_name="unit_test",
            manifest_tsv=manifest,
            contamination_tool="kraken",
            kraken_db=tmp_path / "db",
        )

    qc_submit.assert_called_once()
    contam_submit.assert_called_once()
    run_mq.assert_called_once_with(
        outdir,
        [],
        include_contamination=True,
        observer=ANY,
    )


def test_flows_define_flow_run_name_template() -> None:
    pipeline_mod = _import_pipeline()
    assert getattr(pipeline_mod._qc_only_pipeline_flow, "flow_run_name", None) == "{run_name}"
    assert getattr(pipeline_mod._demux_qc_pipeline_flow, "flow_run_name", None) == "{run_name}"
