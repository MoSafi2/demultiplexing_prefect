from pathlib import Path
import sys
import importlib.util
from unittest.mock import patch

import pytest  # type: ignore[import-not-found]


# Avoid a naming collision: this directory is `test/demux/`, so importing `demux`
# would resolve to the test package instead of the repo's `demux.py`.
REPO_ROOT = Path(__file__).resolve().parents[2]
DEMUX_PIPELINE_DIR = REPO_ROOT / "demux_pipeline"
DEMUX_PY = DEMUX_PIPELINE_DIR / "demux.py"

sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(DEMUX_PIPELINE_DIR))

spec = importlib.util.spec_from_file_location("prefect_demux", DEMUX_PY)
assert spec is not None and spec.loader is not None
demux_mod = importlib.util.module_from_spec(spec)
sys.modules["prefect_demux"] = demux_mod
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
    assert grouped == {(None, "LV7011561401", 1): {"R1": good_r1, "R2": good_r2}}

    grouped_including = demux_mod._group_fastqs(tmp_path, include_undetermined=True)
    assert (None, "Undetermined", 1) in grouped_including


def test_group_fastqs_recursive_flag(tmp_path: Path) -> None:
    top = _touch(tmp_path / "NA12878_S1_R1_001.fastq.gz")
    nested = _touch(tmp_path / "deep" / "NA12878_S1_R2_001.fastq.gz")

    grouped_nonrec = demux_mod._group_fastqs(tmp_path, recursive=False, include_undetermined=True)
    assert grouped_nonrec == {(None, "NA12878", 1): {"R1": top}}

    grouped_rec = demux_mod._group_fastqs(tmp_path, recursive=True, include_undetermined=True)
    assert grouped_rec[(None, "NA12878", 1)] == {"R1": top}


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


def test_samples_from_fastq_dir_preserves_project_folders(tmp_path: Path) -> None:
    r1_a = _touch(tmp_path / "project-a" / "shared_S1_R1_001.fastq.gz")
    r1_b = _touch(tmp_path / "project-b" / "shared_S1_R1_001.fastq.gz")

    samples = demux_mod._samples_from_fastq_dir(tmp_path, include_undetermined=True)

    assert [(s.project, s.name, s.r1) for s in samples] == [
        ("project-a", "shared", r1_a),
        ("project-b", "shared", r1_b),
    ]


def test_samples_from_fastq_dir_ignores_project_qc_fastqs(tmp_path: Path) -> None:
    raw = _touch(tmp_path / "project-a" / "shared_S1_R1_001.fastq.gz")
    _touch(tmp_path / "project-a" / "qc" / "fastp_passthrough" / "shared_R1.fastq.gz")

    samples = demux_mod._samples_from_fastq_dir(tmp_path, include_undetermined=True)

    assert [(s.project, s.name, s.r1) for s in samples] == [
        ("project-a", "shared", raw),
    ]


def test_write_samples_tsv(tmp_path: Path) -> None:
    from demux_pipeline.models import Sample

    out = tmp_path / "samples.tsv"
    s1 = Sample(name="s1", r1=Path("/tmp/s1_R1.fastq.gz"), r2=None)
    s2 = Sample(
        name="s2",
        r1=Path("/tmp/s2_R1.fastq.gz"),
        r2=Path("/tmp/s2_R2.fastq.gz"),
        project="p1",
    )

    demux_mod._write_samples_tsv([s1, s2], out)
    assert out.read_text(encoding="utf-8") == (
        "s1\t/tmp/s1_R1.fastq.gz\t\t\n"
        "s2\t/tmp/s2_R1.fastq.gz\t/tmp/s2_R2.fastq.gz\tp1\n"
    )


def test_sample_ordinals_from_manifest_handles_illumina_data_section(tmp_path: Path) -> None:
    manifest = tmp_path / "SampleSheet.csv"
    manifest.write_text(
        "[Header]\n"
        "IEMFileVersion,4\n"
        "[Data]\n"
        "Sample_ID,Sample_Project\n"
        "sampleA,project-a\n"
        "sampleB,project-b\n",
        encoding="utf-8",
    )

    assert demux_mod._sample_ordinals_from_manifest(manifest) == {
        ("project-a", "sampleA"): 1,
        ("project-b", "sampleB"): 2,
    }


def test_normalize_aviti_output_rewrites_samples_tree_into_output_contract(tmp_path: Path) -> None:
    staged = tmp_path / "staged"
    samples_root = staged / "Samples"
    _touch(samples_root / "project-a" / "sampleA" / "sampleA_R1.fastq.gz")
    _touch(samples_root / "project-a" / "sampleA" / "sampleA_R2.fastq.gz")
    _touch(samples_root / "sampleB" / "sampleB_R1.fastq.gz")
    _touch(samples_root / "Unassigned" / "Unassigned_R1.fastq.gz")

    manifest = tmp_path / "RunManifest.csv"
    manifest.write_text(
        "sample_id,project\n"
        "sampleA,project-a\n"
        "sampleB,\n",
        encoding="utf-8",
    )

    outdir = tmp_path / "out" / demux_mod.DEMUX_FASTQ_OUTDIR_NAME
    demux_mod.normalize_aviti_output(
        staged_output=staged,
        demux_output=outdir,
        manifest_path=manifest,
    )

    assert (outdir / "project-a" / "sampleA_S1_R1_001.fastq.gz").exists()
    assert (outdir / "project-a" / "sampleA_S1_R2_001.fastq.gz").exists()
    assert (outdir / "sampleB_S2_R1_001.fastq.gz").exists()
    assert (outdir / "Undetermined_S0_R1_001.fastq.gz").exists()


def test_demux_bcl_constructs_aviti_command(tmp_path: Path) -> None:
    input_dir = tmp_path / "run"
    input_dir.mkdir()
    _touch(input_dir / "RunManifest.csv")
    _touch(input_dir / "RunParameters.json")
    manifest = tmp_path / "manifest.csv"
    manifest.write_text("sample_id\nsampleA\n", encoding="utf-8")

    commands: list[list[str]] = []

    def _fake_run(cmd, **_kwargs):
        commands.append(cmd)

    with patch.object(demux_mod, "get_run_logger"), patch.object(
        demux_mod, "_resolve_bases2fastq_binary", return_value="/usr/bin/bases2fastq"
    ), patch.object(demux_mod, "run_command", side_effect=_fake_run), patch.object(
        demux_mod, "record_asset"
    ), patch.object(
        demux_mod, "normalize_aviti_output"
    ):
        demux_mod.demux_bcl.fn(
            input_dir=input_dir,
            samplesheet=manifest,
            outdir=tmp_path / "out",
            platform="aviti",
            extra_args=["--num-threads", "4"],
        )

    assert commands == [[
        "/usr/bin/bases2fastq",
        str(input_dir),
        str(tmp_path / "out" / ".demux_native" / "bases2fastq"),
        "--run-manifest",
        str(manifest),
        "--num-threads",
        "4",
    ]]
