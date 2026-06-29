from __future__ import annotations

import shutil
from pathlib import Path

from prefect import get_run_logger, task  # type: ignore[import-not-found]

from demux_pipeline.aviti_demux import (
    AVITI_AUX_OUTDIR_NAME,
    AVITI_NATIVE_OUTDIR,
    _resolve_bases2fastq_binary,
    _sample_ordinals_from_manifest,
    build_aviti_demux_command,
    copy_aviti_auxiliary_outputs,
    finalize_aviti_outputs,
    normalize_aviti_output,
    verify_aviti_outputs,
)
from demux_pipeline.illumina_demux import (
    DEMUX_FASTQ_OUTDIR_NAME,
    _group_fastqs,
    _resolve_bcl_convert_binary,
    _samples_from_fastq_dir,
    _write_samples_tsv,
    build_illumina_demux_command,
    parse_fastq,
)
from demux_pipeline.observability import record_asset
from demux_pipeline.process import run_command


@task(name="demux_bcl", log_prints=True)
def demux_bcl(
    *,
    input_dir: Path,
    samplesheet: Path,
    outdir: Path | str,
    platform: str = "illumina",
    extra_args: list[str] | None = None,
    force: bool = True,
) -> None:
    logger = get_run_logger()
    outdir = Path(outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    demux_output = outdir / DEMUX_FASTQ_OUTDIR_NAME

    if not input_dir.exists() or not input_dir.is_dir():
        raise SystemExit(f"Expected --input-dir to be an existing directory: {input_dir}")
    if not samplesheet.exists() or not samplesheet.is_file():
        raise SystemExit(f"Expected --samplesheet to be an existing file: {samplesheet}")

    platform_name = platform.lower().strip()
    if platform_name == "illumina":
        cmd = build_illumina_demux_command(
            demux_bin=_resolve_bcl_convert_binary(),
            input_dir=input_dir,
            demux_output=demux_output,
            samplesheet=samplesheet,
            extra_args=extra_args,
            force=force,
        )
        logger.info("bcl-convert: %s", " ".join(cmd))
        run_command(cmd, capture_err_tail=80, step="demux", tool="bcl-convert")
        record_asset(
            demux_output,
            step="demux",
            tool="bcl-convert",
            kind="directory",
            metadata={"source": "bcl-convert --output-directory"},
        )
        return

    if platform_name == "aviti":
        native_root = outdir / AVITI_NATIVE_OUTDIR
        aux_output = outdir / AVITI_AUX_OUTDIR_NAME
        native_root.parent.mkdir(parents=True, exist_ok=True)
        if native_root.exists():
            shutil.rmtree(native_root)
        cmd = build_aviti_demux_command(
            demux_bin=_resolve_bases2fastq_binary(),
            input_dir=input_dir,
            staged_output=native_root,
            samplesheet=samplesheet,
            extra_args=extra_args,
        )
        logger.info("bases2fastq: %s", " ".join(cmd))
        run_command(cmd, capture_err_tail=80, step="demux", tool="bases2fastq")
        record_asset(
            native_root,
            step="demux",
            tool="bases2fastq",
            kind="directory",
            metadata={"source": "bases2fastq native output"},
        )
        finalize_aviti_outputs(
            staged_output=native_root,
            demux_output=demux_output,
            aux_output=aux_output,
            manifest_path=samplesheet,
        )
        record_asset(
            aux_output,
            step="demux",
            tool="bases2fastq",
            kind="directory",
            metadata={"source": "bases2fastq auxiliary outputs"},
        )
        record_asset(
            demux_output,
            step="demux",
            tool="bases2fastq",
            kind="directory",
            metadata={"source": "normalized AVITI output"},
        )
        return

    raise SystemExit(f"Unknown platform: {platform}")
