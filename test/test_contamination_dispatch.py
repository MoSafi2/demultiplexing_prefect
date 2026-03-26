from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from unittest.mock import patch


REPO_ROOT = Path(__file__).resolve().parents[1]


def _load_repo_module(name: str, relative_path: str):
    path = REPO_ROOT / relative_path
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


def _import_contamination_and_models():
    if "contamination" in sys.modules and hasattr(
        sys.modules["contamination"], "submit_contamination_tasks"
    ):
        return sys.modules["contamination"], sys.modules["models"]
    models_mod = _load_repo_module("models", "demux_pipeline/models.py")
    _load_repo_module("process", "demux_pipeline/process.py")
    contamination_mod = _load_repo_module("contamination", "demux_pipeline/contamination.py")
    return contamination_mod, models_mod


def _sample(models_mod, tmp_path: Path):
    fastq = tmp_path / "s_R1.fastq.gz"
    fastq.write_text("x", encoding="utf-8")
    return models_mod.Sample(name="s", r1=fastq)


def test_submit_contamination_tasks_dispatch_kraken(tmp_path: Path) -> None:
    contamination_mod, models_mod = _import_contamination_and_models()
    sample = _sample(models_mod, tmp_path)
    with patch.object(contamination_mod.run_kraken2, "map", return_value="ok") as run_map:
        result = contamination_mod.submit_contamination_tasks(
            samples=[sample],
            contamination_tool="kraken",
            outdir=tmp_path / "out",
            per_task_threads=2,
            kraken_db=tmp_path / "kraken_db",
        )
    assert result == "ok"
    run_map.assert_called_once()


def test_submit_contamination_tasks_dispatch_kraken_bracken(tmp_path: Path) -> None:
    contamination_mod, models_mod = _import_contamination_and_models()
    sample = _sample(models_mod, tmp_path)
    with patch.object(
        contamination_mod.run_kraken_bracken, "map", return_value="ok"
    ) as run_map:
        result = contamination_mod.submit_contamination_tasks(
            samples=[sample],
            contamination_tool="kraken_bracken",
            outdir=tmp_path / "out",
            per_task_threads=2,
            bracken_db=tmp_path / "bracken_db",
        )
    assert result == "ok"
    run_map.assert_called_once()


def test_submit_contamination_tasks_dispatch_fastq_screen(tmp_path: Path) -> None:
    contamination_mod, models_mod = _import_contamination_and_models()
    sample = _sample(models_mod, tmp_path)
    conf = tmp_path / "fastq_screen.conf"
    conf.write_text("# config\n", encoding="utf-8")
    with patch.object(
        contamination_mod.run_fastq_screen, "map", return_value="ok"
    ) as run_map:
        result = contamination_mod.submit_contamination_tasks(
            samples=[sample],
            contamination_tool="fastq_screen",
            outdir=tmp_path / "out",
            per_task_threads=2,
            fastq_screen_conf=conf,
        )
    assert result == "ok"
    run_map.assert_called_once()
