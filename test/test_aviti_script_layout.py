from __future__ import annotations

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def test_aviti_simulated_script_downloads_and_runs_cli() -> None:
    text = (REPO_ROOT / "scripts" / "run_aviti_simulated.sh").read_text(
        encoding="utf-8"
    )
    assert "download_file() {" in text
    assert "ensure_tar_archive() {" in text
    assert 'curl --fail --location --retry 3 --retry-delay 2 --output "${dest}" "${url}"' in text
    assert "https://bases2fastq-release.s3.amazonaws.com/bases2fastq-latest.tar.gz" in text
    assert "https://element-public-data.s3.amazonaws.com/bases2fastq-share/bases2fastq-v2/" in text
    assert 'SIM_PROJECT_NAME="${SIM_PROJECT_NAME:-custom-aviti-project}"' in text
    assert "write_custom_project_manifest() {" in text
    assert 'CUSTOM_RUN_MANIFEST="${OUTDIR}/RunManifest.custom.csv"' in text
    assert 'export PATH="${INSTALL_DIR}:${PATH}"' in text
    assert 'rm -rf "${OUTDIR}"' in text
    assert 'tar -xf "${SIM_ARCHIVE}" -C "${SAMPLES_DIR}"' in text
    assert 'exec pixi run demux-pipeline \\' in text
    assert '--platform aviti \\' in text
    assert '--samplesheet "${CUSTOM_RUN_MANIFEST}" \\' in text
