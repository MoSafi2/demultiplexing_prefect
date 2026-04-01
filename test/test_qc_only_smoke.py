"""Smoke checks for `demux_pipeline` (mocked CLIs for speed)."""

from __future__ import annotations

import gzip
import importlib.util
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

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
    if "pipeline" in sys.modules and hasattr(sys.modules["pipeline"], "demux_pipeline"):
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


class _Futures:
    """Minimal stand-in for PrefectFutureList returned by .map()."""
    def __init__(self) -> None:
        self._items: list[object] = []

    def extend(self, items) -> None:
        self._items.extend(list(items))

    def __iter__(self):
        return iter(self._items)

    def result(self) -> None:
        return None


def test_qc_only_pipeline_smoke_mocked(tmp_path: Path) -> None:
    pipeline_mod = _import_pipeline()
    fq = tmp_path / "sample_S1_L001_R1_001.fastq.gz"
    _tiny_gz(fq)
    manifest = tmp_path / "manifest.tsv"
    manifest.write_text(f"sample\t{fq}\n", encoding="utf-8")
    outdir = tmp_path / "out"

    with patch.object(
        pipeline_mod, "submit_qc_tasks", return_value=_Futures()
    ) as mock_qc, patch.object(
        pipeline_mod, "run_multiqc", MagicMock()
    ) as mock_mq:
        pipeline_mod.demux_pipeline(
            qc_tool="falco",
            thread_budget=1,
            outdir=outdir,
            run_name="unit_test",
            manifest_tsv=manifest,
        )

    mock_qc.assert_called_once()
    mock_mq.assert_called_once()


def test_pipeline_with_contamination(tmp_path: Path) -> None:
    pipeline_mod = _import_pipeline()
    fq = tmp_path / "sample_S1_L001_R1_001.fastq.gz"
    _tiny_gz(fq)
    manifest = tmp_path / "manifest.tsv"
    manifest.write_text(f"sample\t{fq}\n", encoding="utf-8")
    outdir = tmp_path / "out"

    with patch.object(
        pipeline_mod, "submit_qc_tasks", return_value=_Futures()
    ) as mock_qc, patch.object(
        pipeline_mod, "submit_contamination_tasks", return_value=_Futures()
    ) as mock_contam, patch.object(
        pipeline_mod, "run_multiqc", MagicMock()
    ) as mock_mq:
        pipeline_mod.demux_pipeline(
            qc_tool="falco",
            thread_budget=4,
            outdir=outdir,
            run_name="unit_test",
            manifest_tsv=manifest,
            contamination_tool="kraken",
            kraken_db=tmp_path / "db",
        )

    mock_qc.assert_called_once()
    mock_contam.assert_called_once()
    mock_mq.assert_called_once_with(
        outdir,
        include_contamination=True,
    )


def test_pipeline_skips_contamination_when_not_requested(tmp_path: Path) -> None:
    pipeline_mod = _import_pipeline()
    fq = tmp_path / "sample_S1_L001_R1_001.fastq.gz"
    _tiny_gz(fq)
    manifest = tmp_path / "manifest.tsv"
    manifest.write_text(f"sample\t{fq}\n", encoding="utf-8")
    outdir = tmp_path / "out"

    with patch.object(
        pipeline_mod, "submit_qc_tasks", return_value=_Futures()
    ) as mock_qc, patch.object(
        pipeline_mod, "submit_contamination_tasks", MagicMock()
    ) as mock_contam, patch.object(
        pipeline_mod, "run_multiqc", MagicMock()
    ) as mock_mq:
        pipeline_mod.demux_pipeline(
            qc_tool="falco",
            thread_budget=2,
            outdir=outdir,
            run_name="unit_test",
            manifest_tsv=manifest,
        )

    mock_qc.assert_called_once()
    mock_contam.assert_not_called()
    mock_mq.assert_called_once_with(
        outdir,
        include_contamination=False,
    )


def test_pipeline_dispatches_multiple_qc_and_contamination_tools(tmp_path: Path) -> None:
    pipeline_mod = _import_pipeline()
    fq = tmp_path / "sample_S1_L001_R1_001.fastq.gz"
    _tiny_gz(fq)
    manifest = tmp_path / "manifest.tsv"
    manifest.write_text(f"sample\t{fq}\n", encoding="utf-8")
    outdir = tmp_path / "out"

    with patch.object(
        pipeline_mod, "submit_qc_tasks", side_effect=[_Futures(), _Futures()]
    ) as mock_qc, patch.object(
        pipeline_mod, "submit_contamination_tasks", side_effect=[_Futures(), _Futures()]
    ) as mock_contam, patch.object(
        pipeline_mod, "run_multiqc", MagicMock()
    ) as mock_mq:
        pipeline_mod.demux_pipeline(
            qc_tool="fastqc,fastp",
            thread_budget=4,
            outdir=outdir,
            run_name="unit_test",
            manifest_tsv=manifest,
            contamination_tool="kraken,fastq_screen",
            kraken_db=tmp_path / "db",
            fastq_screen_conf=tmp_path / "fastq_screen.conf",
        )

    assert mock_qc.call_count == 2
    assert [c.args[1] for c in mock_qc.call_args_list] == ["fastqc", "fastp"]
    assert mock_contam.call_count == 2
    assert [c.args[1] for c in mock_contam.call_args_list] == [
        "kraken",
        "fastq_screen",
    ]
    mock_mq.assert_called_once_with(
        outdir,
        include_contamination=True,
    )


def test_pipeline_is_a_prefect_flow() -> None:
    pipeline_mod = _import_pipeline()
    flow_fn = pipeline_mod.demux_pipeline
    # Prefect @flow wraps the function; it exposes `.name` on the flow object
    assert hasattr(flow_fn, "name")
    assert flow_fn.name == "demux-pipeline"
