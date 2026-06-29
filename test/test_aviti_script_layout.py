from __future__ import annotations

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def test_aviti_simulated_script_downloads_and_runs_cli() -> None:
    text = (REPO_ROOT / "scripts" / "run_aviti_simulated.sh").read_text(
        encoding="utf-8"
    )
    assert "https://bases2fastq-release.s3.amazonaws.com/bases2fastq-latest.tar.gz" in text
    assert "http://element-public-data.s3.amazonaws.com/bases2fastq-share/bases2fastq-v2/" in text
    assert 'export PATH="${INSTALL_DIR}:${PATH}"' in text
    assert 'rm -rf "${OUTDIR}"' in text
    assert 'exec pixi run demux-pipeline \\' in text
    assert '--platform aviti \\' in text
    assert '--samplesheet "${SIM_RUN_DIR}/RunManifest.csv" \\' in text
