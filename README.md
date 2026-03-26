# Prefect FASTQ QC (fastqc + fastp + falco)

Small Prefect 2 pipeline that runs a single QC tool (`fastqc` or `fastp` or `falco`) on FASTQ files, with an optional contamination screening stage (Kraken2/Bracken or FastQ Screen), and then generates a MultiQC report.

## Install

This project uses `pixi`:

```bash
pixi install
```

## Requirements

System tools must be on your `PATH` (depending on what you run):

* `fastqc` (for `--qc-tool fastqc`)
* `fastp` (for `--qc-tool fastp`)
* `falco` (for `--qc-tool falco`)
* `multiqc` (optional; if missing the pipeline still runs QC/contamination)
* `bcl-convert` (required for `--mode demux`)
* optional contamination tools:
  * `kraken2` (for `--contamination-tool kraken`)
  * `bracken` (for `--contamination-tool kraken_bracken`)
  * `fastq_screen` (for `--contamination-tool fastq_screen`)
  * `bowtie2` (only needed if you want to build a FastQ Screen index yourself)

## Important: import layout

The pipeline modules use "flat" imports (for example `from models import Sample`), so they expect `demux_pipeline/` to be on `PYTHONPATH` when imported as modules.

The main CLI is now the top-level `cli.py`, which makes this `PYTHONPATH` implicit for normal CLI usage. The smoke-test helper (`run_qc_smoke_test.py`) also adjusts `sys.path` when executed, so no `PYTHONPATH` is required for smoke tests.

## Output directories

All outputs go under `--outdir`:

* `outdir/output/` (demux mode only; bcl-convert output directory)
* `outdir/fastqc/` (FastQC reports)
* `outdir/fastp/` (Fastp HTML + JSON)
* `outdir/fastp_passthrough/` (Fastp FASTQ outputs; this pipeline disables trimming/filtering)
* `outdir/falco/<sample>_<R1|R2>/` (Falco output; for `--qc-tool falco`)
* `outdir/contamination/` (optional; Kraken/Bracken or FastQ Screen outputs)
* `outdir/multiqc/` (MultiQC summary; created only if `multiqc` is available on PATH)

## Usage (Unified CLI)

The CLI lives here:

* `cli.py`

Run:

```bash
pixi run python cli.py --mode {demux|qc} --qc-tool {fastqc|fastp|falco} --outdir OUTDIR --threads N ...
```

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

#### QC-only from a manifest TSV

The manifest is tab-separated with exactly 2 columns per non-empty line:

`sample_name<TAB>path_to_fastq.gz`

Example:

```tsv
sample1	/path/to/sample1_R1.fastq.gz
sample2	/path/to/sample2.fastq.gz
```

Run:

```bash
pixi run python cli.py \
  --mode qc \
  --qc-tool falco \
  --outdir ./qc_out \
  --threads 4 \
  --manifest-tsv ./samples.tsv
```

If you need paired-end (R1 + R2) processing, use `--in-fastq-dir` instead.

#### QC-only from a FASTQ directory

```bash
pixi run python cli.py \
  --mode qc \
  --qc-tool fastp \
  --outdir ./qc_out \
  --threads 4 \
  --in-fastq-dir /path/to/fastqs
```

This directory is scanned recursively and FASTQs are grouped into samples based on an Illumina-ish filename pattern (expects `*_R1*` plus an optional matching `*_R2*`). Incomplete chunks are skipped, and paths containing `undetermined` are skipped by default.

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
