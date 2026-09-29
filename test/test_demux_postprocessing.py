from __future__ import annotations

import gzip
import hashlib
from pathlib import Path
from unittest.mock import patch

import pytest

from demux_pipeline.demux import demux_bcl
from demux_pipeline.pipeline import (
    _discover_samples,
    _get_projects_samples,
    write_sample_hashes,
)


READS_R_ONLY = ("R1", "R2")
READS_WITH_INDEX = ("R1", "R2", "I1", "I2")


def write_fastq_gz(path: Path, read: str) -> None:
    """Create a tiny valid FASTQ.gz file."""
    path.parent.mkdir(parents=True, exist_ok=True)

    sequences = {
        "R1": "ACGTACGTACGT",
        "R2": "TGCATGCATGCA",
        "I1": "AACCGGTTAACC",
        "I2": "TTGGCCAATTGG",
    }

    sequence = sequences[read]
    quality = "I" * len(sequence)

    content = (
        f"@test_{read}\n"
        f"{sequence}\n"
        "+\n"
        f"{quality}\n"
    )

    with gzip.open(path, "wt") as handle:
        handle.write(content)


def create_input_run(input_dir: Path) -> None:
    """
    Create the minimum fake input directory required by demux_bcl().
    The actual contents are irrelevant because the external demultiplexer
    is mocked.
    """
    input_dir.mkdir(parents=True, exist_ok=True)


def create_samplesheet(path: Path) -> None:
    """
    Minimal samplesheet understood by the AVITI post-processing code.

    The same file is accepted by the mocked Illumina path.
    """
    path.write_text(
        "[Data]\n"
        "Sample_ID,Sample_Project\n"
        "SampleA,Project1\n",
        encoding="utf-8",
    )


def create_illumina_demux_output(
    output_dir: Path,
    reads: tuple[str, ...],
) -> None:
    """
    Simulate exactly what bcl-convert should have produced.

    Unlike the previous test, this happens INSIDE the mocked external
    bcl-convert invocation, so demux_bcl() itself is exercised.
    """
    project_dir = output_dir / "Project1"
    project_dir.mkdir(parents=True, exist_ok=True)

    for read in reads:
        write_fastq_gz(
            project_dir / f"SampleA_S1_{read}_001.fastq.gz",
            read,
        )


def create_aviti_native_output(
    staged_output: Path,
    reads: tuple[str, ...],
) -> None:
    """
    Simulate the native Bases2Fastq output BEFORE AVITI normalization.

    This is deliberately different from the final normalized layout.
    The test therefore exercises:

        native output
            -> finalize_aviti_outputs()
            -> normalized output
    """
    samples_dir = staged_output / "Samples" / "Project1" / "SampleA"
    samples_dir.mkdir(parents=True, exist_ok=True)

    for read in reads:
        write_fastq_gz(
            samples_dir / f"SampleA_{read}_001.fastq.gz",
            read,
        )

    # Also create a non-FASTQ auxiliary file. This verifies that the
    # existing AVITI auxiliary-output handling is still exercised.
    aux_dir = staged_output / "run_info"
    aux_dir.mkdir(parents=True, exist_ok=True)
    (aux_dir / "bases2fastq.log").write_text(
        "fake bases2fastq output\n",
        encoding="utf-8",
    )


def md5(path: Path) -> str:
    digest = hashlib.md5()

    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)

    return digest.hexdigest()


def assert_md5_contains(
    project_dir: Path,
    expected_paths: list[Path],
) -> None:
    md5_file = project_dir / "md5.txt"

    assert md5_file.is_file()

    lines = {
        line.strip()
        for line in md5_file.read_text().splitlines()
        if line.strip()
    }

    expected = {
        f"{md5(path)}  {path.name}"
        for path in expected_paths
    }

    assert lines == expected


@pytest.mark.parametrize(
    ("platform", "reads"),
    [
        ("illumina", READS_R_ONLY),
        ("illumina", READS_WITH_INDEX),
        ("aviti", READS_R_ONLY),
        ("aviti", READS_WITH_INDEX),
    ],
)
def test_demux_postprocessing_md5_and_discovery(
    tmp_path: Path,
    platform: str,
    reads: tuple[str, ...],
) -> None:
    """
    Test:

        external demultiplexer
          ↓
        platform-specific post-processing
          ↓
        normalized FASTQ files
          ↓
        sample discovery
          ↓
        MD5

    This test deliberately does NOT start at an already-normalized FASTQ
    directory.
    """
    input_dir = tmp_path / "input"
    outdir = tmp_path / "output"
    samplesheet = tmp_path / "SampleSheet.csv"

    create_input_run(input_dir)
    create_samplesheet(samplesheet)

    if platform == "illumina":

        def fake_run_command(cmd, **kwargs):
            create_illumina_demux_output(
                outdir / "output",
                reads,
            )

    else:

        def fake_run_command(cmd, **kwargs):
            create_aviti_native_output(
                outdir / ".demux_native" / "bases2fastq",
                reads,
            )

    with patch(
        "demux_pipeline.demux.run_command",
        side_effect=fake_run_command,
    ), patch(
        "demux_pipeline.demux._resolve_bcl_convert_binary",
        return_value="fake-bcl-convert",
    ), patch(
        "demux_pipeline.demux._resolve_bases2fastq_binary",
        return_value="fake-bases2fastq",
    ):
        demux_bcl(
            input_dir=input_dir,
            samplesheet=samplesheet,
            outdir=outdir,
            platform=platform,
            extra_args=None,
        )

    demux_dir = outdir / "output"

    # ---------------------------------------------------------------
    # 1. Verify that post-processing produced the expected FASTQs.
    # ---------------------------------------------------------------

    project_dir = demux_dir / "Project1"

    expected_paths = [
        project_dir / f"SampleA_S1_{read}_001.fastq.gz"
        for read in reads
    ]

    for path in expected_paths:
        assert path.is_file(), (
            f"{platform}: expected post-processed FASTQ missing: {path}"
        )

    # I1/I2 must not appear in cases where they were not produced.
    if reads == READS_R_ONLY:
        assert not (
            project_dir / "SampleA_S1_I1_001.fastq.gz"
        ).exists()
        assert not (
            project_dir / "SampleA_S1_I2_001.fastq.gz"
        ).exists()

    # ---------------------------------------------------------------
    # 2. AVITI-specific: native output must have been cleaned up.
    # ---------------------------------------------------------------

    if platform == "aviti":
        native_root = outdir / ".demux_native"

        assert not native_root.exists(), (
            "AVITI native staging directory should be removed after "
            "successful finalization."
        )

        # Auxiliary outputs should survive in the AVITI auxiliary area.
        aux_log = outdir / "bases2fastq" / "run_info" / "bases2fastq.log"

        assert aux_log.is_file()

    # ---------------------------------------------------------------
    # 3. Sample discovery happens AFTER post-processing.
    # ---------------------------------------------------------------

    samples = _discover_samples(demux_dir)

    assert len(samples) == 1

    sample = samples[0]

    assert sample.name == "SampleA"
    assert sample.r1 == project_dir / "SampleA_S1_R1_001.fastq.gz"
    assert sample.r2 == project_dir / "SampleA_S1_R2_001.fastq.gz"

    discovered = {path.name for path in sample.get_paths()}

    assert discovered == {
        f"SampleA_S1_{read}_001.fastq.gz"
        for read in reads
    }

    if "I1" in reads:
        assert any(
            path.name == "SampleA_S1_I1_001.fastq.gz"
            for path in sample.get_paths()
        )

    if "I2" in reads:
        assert any(
            path.name == "SampleA_S1_I2_001.fastq.gz"
            for path in sample.get_paths()
        )

    # ---------------------------------------------------------------
    # 4. MD5 must operate on the POST-PROCESSING files.
    # ---------------------------------------------------------------

    write_sample_hashes(samples)

    assert_md5_contains(
        project_dir,
        expected_paths,
    )


@pytest.mark.parametrize("platform", ["illumina", "aviti"])
def test_i_reads_are_not_silently_dropped_during_postprocessing(
    tmp_path: Path,
    platform: str,
) -> None:
    """
    A focused regression test for the original bug of I1/I2 reads being dropped.

    If Bases2Fastq/bcl-convert produces I1/I2, they must still exist
    after demux_bcl() returns.
    """
    input_dir = tmp_path / "input"
    outdir = tmp_path / "output"
    samplesheet = tmp_path / "SampleSheet.csv"

    create_input_run(input_dir)
    create_samplesheet(samplesheet)

    if platform == "illumina":

        def fake_run_command(cmd, **kwargs):
            create_illumina_demux_output(
                outdir / "output",
                READS_WITH_INDEX,
            )

    else:

        def fake_run_command(cmd, **kwargs):
            create_aviti_native_output(
                outdir / ".demux_native" / "bases2fastq",
                READS_WITH_INDEX,
            )

    with patch(
        "demux_pipeline.demux.run_command",
        side_effect=fake_run_command,
    ), patch(
        "demux_pipeline.demux._resolve_bcl_convert_binary",
        return_value="fake-bcl-convert",
    ), patch(
        "demux_pipeline.demux._resolve_bases2fastq_binary",
        return_value="fake-bases2fastq",
    ):
        demux_bcl(
            input_dir=input_dir,
            samplesheet=samplesheet,
            outdir=outdir,
            platform=platform,
        )

    output_fastqs = list(
        (outdir / "output").rglob("*.fastq.gz")
    )

    output_names = {path.name for path in output_fastqs}

    assert output_names == {
        "SampleA_S1_R1_001.fastq.gz",
        "SampleA_S1_R2_001.fastq.gz",
        "SampleA_S1_I1_001.fastq.gz",
        "SampleA_S1_I2_001.fastq.gz",
    }


@pytest.mark.parametrize("platform", ["illumina", "aviti"])
def test_postprocessed_fastqs_have_same_content_as_demux_output(
    tmp_path: Path,
    platform: str,
) -> None:
    """
    Verify that the normalization step doesn't modify FASTQ contents.

    This is particularly important for AVITI because normalize_aviti_output()
    hardlinks/copies the native FASTQs into their final names.
    """
    input_dir = tmp_path / "input"
    outdir = tmp_path / "output"
    samplesheet = tmp_path / "SampleSheet.csv"

    create_input_run(input_dir)
    create_samplesheet(samplesheet)

    source_hashes = {}

    if platform == "illumina":

        def fake_run_command(cmd, **kwargs):
            create_illumina_demux_output(
                outdir / "output",
                READS_WITH_INDEX,
            )

            for path in (outdir / "output").rglob("*.fastq.gz"):
                source_hashes[path.name] = md5(path)

    else:

        def fake_run_command(cmd, **kwargs):
            native = outdir / ".demux_native" / "bases2fastq"

            create_aviti_native_output(
                native,
                READS_WITH_INDEX,
            )

            for path in native.rglob("*.fastq.gz"):
                source_hashes[path.name] = md5(path)

    with patch(
        "demux_pipeline.demux.run_command",
        side_effect=fake_run_command,
    ), patch(
        "demux_pipeline.demux._resolve_bcl_convert_binary",
        return_value="fake-bcl-convert",
    ), patch(
        "demux_pipeline.demux._resolve_bases2fastq_binary",
        return_value="fake-bases2fastq",
    ):
        demux_bcl(
            input_dir=input_dir,
            samplesheet=samplesheet,
            outdir=outdir,
            platform=platform,
        )

    final_fastqs = list(
        (outdir / "output").rglob("*.fastq.gz")
    )

    assert len(final_fastqs) == 4

    for path in final_fastqs:
        assert path.name in source_hashes
        assert md5(path) == source_hashes[path.name]