"""Tests for the Sample dataclass."""
from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]


def _load_module(name: str, relative_path: str):
    path = REPO_ROOT / relative_path
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


models = _load_module("models", "demux_pipeline/models.py")
Sample = models.Sample


def test_single_end_not_paired():
    s = Sample(name="s1", r1=Path("/tmp/r1.fastq.gz"))
    assert not s.paired


def test_paired_end_is_paired():
    s = Sample(name="s1", r1=Path("/tmp/r1.fastq.gz"), r2=Path("/tmp/r2.fastq.gz"))
    assert s.paired


def test_get_paths_single_end():
    r1 = Path("/tmp/r1.fastq.gz")
    s = Sample(name="s1", r1=r1)
    assert s.get_paths() == [r1]


def test_get_paths_paired_end():
    r1 = Path("/tmp/r1.fastq.gz")
    r2 = Path("/tmp/r2.fastq.gz")
    s = Sample(name="s1", r1=r1, r2=r2)
    assert s.get_paths() == [r1, r2]


def test_sample_is_immutable():
    s = Sample(name="s1", r1=Path("/tmp/r1.fastq.gz"))
    try:
        s.name = "other"  # type: ignore[misc]
        raise AssertionError("frozen dataclass should raise on assignment")
    except Exception as exc:
        assert not isinstance(exc, AssertionError)
