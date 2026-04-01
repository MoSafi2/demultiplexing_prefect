# Prefect-based pipeline for demultiplexing + QC

## Install

This project uses `pixi`:

```bash
pixi install
```

## Requirements

System tools must be on your `PATH` (depending on what you run):

* `bcl-convert` (required for `--mode demux`)

## Output directories

All outputs go under `--outdir`:

* `outdir/output/` (demux mode only; bcl-convert output directory)
* `outdir/fastqc/` (FastQC reports)
* `outdir/fastp/` (Fastp HTML + JSON)
* `outdir/fastp_passthrough/` (Fastp FASTQ outputs; this pipeline disables trimming/filtering)
* `outdir/falco/<sample>_<R1|R2>/` (Falco output; for `--qc-tool falco`)
* `outdir/contamination/` (optional; Kraken/Bracken or FastQ Screen outputs)
* `outdir/multiqc/` (MultiQC summary; created only if `multiqc` is available on PATH)

## Usage

Run:

```bash
pixi run python cli.py --mode {demux|qc} --qc-tool <tool[,tool...]> --outdir OUTDIR --threads N --contamination-tool <tool[,tool...]> ...
```

`--qc-tool` accepts one or more of: `fastqc`, `fastp`, `falco`.

`--contamination-tool` accepts one or more of: `kraken`, `kraken_bracken`, `fastq_screen`, `none`.

* Use comma-separated values to run multiple tools in one run (for example `--qc-tool fastqc,fastp`).
* `none` must be used alone (`--contamination-tool none`) and disables contamination.
* If `kraken` is selected, pass `--kraken-db`.
* If `kraken_bracken` is selected, pass `--bracken-db` or `--kraken-db`.
* If `fastq_screen` is selected, pass `--fastq-screen-conf`.

### Demux + QC (`--mode demux`)

```bash
pixi run python cli.py \
  --mode demux \
  --qc-tool fastqc \
  --outdir ./demux_qc_out \
  --threads 4 \
  --bcl_dir /path/to/BCL_RUN_FOLDER \
  --samplesheet /path/to/SampleSheet.csv
```

Optional contamination screening (runs after the QC tool):

```bash
pixi run python cli.py \
  --mode demux \
  --qc-tool fastqc \
  --outdir ./demux_qc_out \
  --threads 4 \
  --bcl_dir /path/to/BCL_RUN_FOLDER \
  --samplesheet /path/to/SampleSheet.csv \
  --contamination-tool kraken_bracken \
  --kraken-db /path/to/kraken-db
```

Run multiple QC and contamination tools in one invocation:

```bash
pixi run python cli.py \
  --mode demux \
  --qc-tool fastqc,fastp \
  --outdir ./demux_qc_out \
  --threads 4 \
  --bcl_dir /path/to/BCL_RUN_FOLDER \
  --samplesheet /path/to/SampleSheet.csv \
  --contamination-tool kraken,fastq_screen \
  --kraken-db /path/to/kraken-db \
  --fastq-screen-conf /path/to/fastq_screen.conf
```

FastQ Screen:

```bash
pixi run python cli.py \
  --mode demux \
  --qc-tool fastqc \
  --outdir ./demux_qc_out \
  --threads 4 \
  --bcl_dir /path/to/BCL_RUN_FOLDER \
  --samplesheet /path/to/SampleSheet.csv \
  --contamination-tool fastq_screen \
  --fastq-screen-conf /path/to/fastq_screen.conf
```

Note: in demux mode, this pipeline does *not* write a QC manifest TSV; it discovers samples by scanning `outdir/output/` after bcl-convert finishes.

### QC only (`--mode qc`)

You must provide exactly one of:

* `--manifest-tsv` (single-end only, 2 columns)
* `--in-fastq-dir` (supports SE and PE based on FASTQ filename patterns)

## Smoke test

`run_qc_smoke_test.py` generates tiny synthetic FASTQ.gz files and runs the QC-only flow using real binaries.

```bash
pixi run python run_qc_smoke_test.py --outdir ./qc_smoke_out
```

Run multiple tools:

```bash
pixi run python run_qc_smoke_test.py \
  --modes fastqc,fastp,falco \
  --threads 2 \
  --outdir ./qc_smoke_out
```

Outputs for each mode go under:

* `OUTDIR/qc_only_<qc_tool>/input/`
* `OUTDIR/qc_only_<qc_tool>/manifest.tsv`
* `OUTDIR/qc_only_<qc_tool>/out/` (where `fastqc/`, `fastp/`, `falco/`, and `multiqc/` are written)
