from pathlib import Path
import sys
import importlib.util

import pytest  # type: ignore[import-not-found]


# Avoid a naming collision: this directory is `test/demux/`, so importing `demux`
# would resolve to the test package instead of the repo's `demux.py`.
REPO_ROOT = Path(__file__).resolve().parents[2]
DEMUX_PY = REPO_ROOT / "demux.py"

sys.path.insert(0, str(REPO_ROOT))

spec = importlib.util.spec_from_file_location("prefect_demux", DEMUX_PY)
assert spec is not None and spec.loader is not None
demux_mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(demux_mod)
parse_fastq = demux_mod.parse_fastq


@pytest.mark.parametrize(
    "filename, expected",
    [
        # Typical Illumina bcl-convert output (sample index + lane + read + chunk).
        ("NA12878_S1_L001_R1_001.fastq.gz", {"sample": "NA12878", "read": 1, "lane": 1, "chunk": 1}),
        ("NA12878_S1_L001_R2_001.fastq.gz", {"sample": "NA12878", "read": 2, "lane": 1, "chunk": 1}),
        ("NA12878_S1_L002_R1_002.fastq.gz", {"sample": "NA12878", "read": 1, "lane": 2, "chunk": 2}),
        ("patient.01_S1_L001_R1_001.fastq.gz", {"sample": "patient.01", "read": 1, "lane": 1, "chunk": 1}),
        ("sample-1_S3_L001_R1_001.fq.gz", {"sample": "sample-1", "read": 1, "lane": 1, "chunk": 1}),
        ("sample-1_S3_L001_R2_001.fq", {"sample": "sample-1", "read": 2, "lane": 1, "chunk": 1}),
        # Common variants seen when some demux metadata is omitted/merged.
        ("NA12878_L001_R1_001.fastq.gz", {"sample": "NA12878", "read": 1, "lane": 1, "chunk": 1}),  # no `_S#`
        ("NA12878_S1_R2_001.fastq.gz", {"sample": "NA12878", "read": 2, "lane": None, "chunk": 1}),  # no `_L###`
        ("NA12878_S1_L001_R1_001.FASTQ.GZ", {"sample": "NA12878", "read": 1, "lane": 1, "chunk": 1}),  # case-insensitive extension

        # Realistic Illumina names: `sampleName_S#_R#_001.fastq.gz` (lane omitted).
        ("LV7010476801_S2_R1_001.fastq.gz", {"sample": "LV7010476801", "read": 1, "lane": None, "chunk": 1}),
        ("LV7011561401_S1_R1_001.fastq.gz", {"sample": "LV7011561401", "read": 1, "lane": None, "chunk": 1}),
        ("LV7012234584_S7_R1_001.fastq.gz", {"sample": "LV7012234584", "read": 1, "lane": None, "chunk": 1}),
        ("LV7008799804_S5_R1_001.fastq.gz", {"sample": "LV7008799804", "read": 1, "lane": None, "chunk": 1}),
        ("LV7011545591_S6_R1_001.fastq.gz", {"sample": "LV7011545591", "read": 1, "lane": None, "chunk": 1}),
        ("LV7012229417_S4_R1_001.fastq.gz", {"sample": "LV7012229417", "read": 1, "lane": None, "chunk": 1}),
        ("LV7013375608_S3_R1_001.fastq.gz", {"sample": "LV7013375608", "read": 1, "lane": None, "chunk": 1}),
        ("Undetermined_S0_R1_001.fastq.gz", {"sample": "Undetermined", "read": 1, "lane": None, "chunk": 1}),
    ],
)
def test_parse_fastq_illumina_filenames(filename: str, expected: dict[str, object]) -> None:
    assert parse_fastq(Path(filename)) == expected


@pytest.mark.parametrize(
    "filename",
    [
        "NA12878_S1_L001_R3_001.fastq.gz",  # read must be 1 or 2
        "NA12878_S1_L001_R1_001.fastq.zip",  # unsupported extension
        "NA12878_S1_L001_R1_001",  # missing extension
    ],
)
def test_parse_fastq_invalid_returns_none(filename: str) -> None:
    assert parse_fastq(Path(filename)) is None


def _touch(path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("", encoding="utf-8")
    return path


def test_group_fastqs_skips_undetermined_by_default(tmp_path: Path) -> None:
    good_r1 = _touch(tmp_path / "LV7011561401_S1_R1_001.fastq.gz")
    good_r2 = _touch(tmp_path / "LV7011561401_S1_R2_001.fastq.gz")
    _touch(tmp_path / "Undetermined_S0_R1_001.fastq.gz")
    _touch(tmp_path / "nested" / "Undetermined" / "NA12878_S1_L001_R1_001.fastq.gz")

    grouped = demux_mod._group_fastqs(tmp_path)
    assert grouped == {("LV7011561401", 1): {"R1": good_r1, "R2": good_r2}}

    grouped_including = demux_mod._group_fastqs(tmp_path, include_undetermined=True)
    assert ("Undetermined", 1) in grouped_including


def test_group_fastqs_recursive_flag(tmp_path: Path) -> None:
    top = _touch(tmp_path / "NA12878_S1_R1_001.fastq.gz")
    nested = _touch(tmp_path / "deep" / "NA12878_S1_R2_001.fastq.gz")

    grouped_nonrec = demux_mod._group_fastqs(tmp_path, recursive=False, include_undetermined=True)
    assert grouped_nonrec == {("NA12878", 1): {"R1": top}}

    grouped_rec = demux_mod._group_fastqs(tmp_path, recursive=True, include_undetermined=True)
    assert grouped_rec[("NA12878", 1)] == {"R1": top, "R2": nested}


def test_samples_from_fastq_dir_builds_samples_and_skips_incomplete(tmp_path: Path) -> None:
    # complete PE unit
    r1 = _touch(tmp_path / "sampleA_S1_L001_R1_001.fastq.gz")
    r2 = _touch(tmp_path / "sampleA_S1_L001_R2_001.fastq.gz")

    # incomplete unit (R2 only) -> skipped
    _touch(tmp_path / "sampleB_S1_R2_001.fastq.gz")

    # second chunk -> becomes separate Sample
    r1_2 = _touch(tmp_path / "sampleA_S1_L001_R1_002.fastq.gz")

    samples = demux_mod._samples_from_fastq_dir(tmp_path, include_undetermined=True)
    assert [s.name for s in samples] == ["sampleA", "sampleA"]
    assert samples[0].r1 == r1 and samples[0].r2 == r2
    assert samples[1].r1 == r1_2 and samples[1].r2 is None


def test_write_samples_tsv(tmp_path: Path) -> None:
    from models import Sample

    out = tmp_path / "samples.tsv"
    s1 = Sample(name="s1", r1=Path("/tmp/s1_R1.fastq.gz"), r2=None)
    s2 = Sample(name="s2", r1=Path("/tmp/s2_R1.fastq.gz"), r2=Path("/tmp/s2_R2.fastq.gz"))

    demux_mod._write_samples_tsv([s1, s2], out)
    assert out.read_text(encoding="utf-8") == (
        "s1\t/tmp/s1_R1.fastq.gz\tNone\n"
        "s2\t/tmp/s2_R1.fastq.gz\t/tmp/s2_R2.fastq.gz\n"
    )

