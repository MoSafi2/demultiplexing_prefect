# Prefect FASTQ QC (fastqc + fastp + falco)

This repository contains a small, easy-to-read Prefect 2 pipeline that runs `fastqc`, `fastp`, and/or `falco` on FASTQ files.

## Requirements

- System tools on your `PATH` (depending on `--mode`):
  - `fastqc` (for `--qc-tool fastqc`)
  - `fastp` (for `--qc-tool fastp`)
  - `falco` (for `--qc-tool falco`)
  - (optional) `kraken2` (for `--contamination-tool kraken`)
  - (optional) `fastq_screen` (for `--contamination-tool fastq_screen`)
- Python 3.10+
- Pixi installed (to manage the Python environment)

## Install

From this directory (creates a Pixi environment):

```bash
pixi install
```

## Usage

All outputs are written under `--outdir`:

- `outdir/fastqc/` (FastQC reports)
- `outdir/fastp/` (Fastp HTML + JSON reports)
- `outdir/fastp_trimmed/` (trimmed FASTQ files)
- `outdir/falco/<sample_name>/` (Falco output; for `--qc-tool falco`)
- `outdir/contamination/` (optional; `kraken2` or `fastq_screen` outputs)
- `outdir/multiqc/` (MultiQC summary report; if `multiqc` is available on PATH)

The unified CLI uses two knobs:

- `--mode` selects the pipeline stage:
  - `demux_qc`: run demux + QC
  - `QC`: skip demux; run QC only
- `--qc-tool` selects exactly one QC program:
  - `fastqc`: only `fastqc`
  - `fastp`: only `fastp`
  - `falco`: only `falco` (default in `run_pipeline.py`)

## Pipeline Diagram

```mermaid
flowchart TD
  subgraph DemuxPlusQC[Demux + QC (`--mode demux_qc`)]
    A[BCL run folder] --> B[bcl-convert (demux) + merge FASTQs]
    B --> C[combined FASTQs per sample]
    C --> D[QC input manifest `samples.tsv`]
  end

  subgraph QConly[QC only (`--mode QC`)]
    D2[manifest TSV (`--manifest-tsv`) OR FASTQ dir (`--in-fastq-dir`)] --> E[QC tool (choose one): fastqc / fastp / falco]
  end

  D --> E[QC tool (choose one): fastqc / fastp / falco]
  E --> F[Optional contamination (kraken2 or fastq_screen)]
  F --> G[MultiQC summary (if `multiqc` is on PATH)]
```

Notes:

- `--contamination-tool` is optional and runs after the FASTQ QC step.
- If `multiqc` is not installed, the pipeline will still run the underlying QC tools.

## Smoke Test

This repository includes a small smoke-test script that generates tiny synthetic FASTQ files and runs the pipeline using the real `fastqc`/`fastp`/`falco` binaries.

```bash
pixi run python run_qc_smoke_test.py --outdir ./qc_smoke_out
```

Run multiple modes:

```bash
pixi run python run_qc_smoke_test.py --modes fastqc,fastp,falco --outdir ./qc_smoke_out
```

Single-end (do not generate/use R2):

```bash
pixi run python run_qc_smoke_test.py --single --outdir ./qc_smoke_out
```

Mode outputs are written under `--outdir/<mode>/` and the generated inputs are under `--outdir/inputs/`.

### Single-end

```bash
pixi run python run_qc.py \
  --sample-name sample1 \
  --r1 /path/to/sample1_R1.fastq.gz \
  --outdir ./qc_out \
  --threads 4 \
  --mode fastqc
```

### Paired-end

```bash
pixi run python run_qc.py \
  --sample-name sample1 \
  --r1 /path/to/sample1_R1.fastq.gz \
  --r2 /path/to/sample1_R2.fastq.gz \
  --outdir ./qc_out \
  --threads 4 \
  --mode fastp
```

### Multiple samples via TSV

Create a tab-separated manifest `samples.tsv` with columns:

`sample_name<TAB>r1<TAB>r2(optional)`

Example:

```tsv
sample1\t/path/to/sample1_R1.fastq.gz\t/path/to/sample1_R2.fastq.gz
sample2\t/path/to/sample2_R1.fastq.gz
```

Run:

```bash
pixi run python run_qc.py \
  --samples-tsv samples.tsv \
  --outdir ./qc_out \
  --threads 4 \
  --mode falco
```

### Demux only (BCL -> FASTQs)

This runs `bcl-convert` on your Illumina BCL run folder, then merges per-sample FASTQs.

```bash
pixi run python run_demux.py \
  --bcl_dir /path/to/BCL_RUN_FOLDER \
  --samplesheet /path/to/SampleSheet.csv \
  --outdir ./demux_out
```

It also writes a QC input manifest TSV for later QC runs:

- default: `./demux_out/qc_inputs/samples.tsv`

You can override the manifest path with `--manifest-tsv`.

### Full pipeline (demux + QC)

If you want both steps in one go:

```bash
pixi run python run_pipeline.py \
  --mode demux_qc \
  --qc-tool fastqc \
  --bcl_dir /path/to/BCL_RUN_FOLDER \
  --samplesheet /path/to/SampleSheet.csv \
  --outdir ./demux_qc_out \
  --threads 4
```

Optional contamination screening can run after the FASTQ QC step:

```bash
pixi run python run_pipeline.py \
  --mode demux_qc \
  --qc-tool fastqc \
  --bcl_dir /path/to/BCL_RUN_FOLDER \
  --samplesheet /path/to/SampleSheet.csv \
  --outdir ./demux_qc_out \
  --threads 4 \
  --contamination-tool kraken \
  --kraken-db /path/to/kraken-db
```

Or using FastQ Screen:

```bash
pixi run python run_pipeline.py \
  --mode demux_qc \
  --qc-tool fastqc \
  --bcl_dir /path/to/BCL_RUN_FOLDER \
  --samplesheet /path/to/SampleSheet.csv \
  --outdir ./demux_qc_out \
  --threads 4 \
  --contamination-tool fastq_screen \
  --fastq-screen-conf /path/to/fastq_screen.conf
```

Outputs:

- `demux_qc_out/demux_fastq/` : raw `bcl-convert` FASTQs
- `demux_qc_out/demux_fastq/combined/` : merged per-sample `*_R1.fastq.gz` (and `*_R2.fastq.gz` when present)
- `demux_qc_out/qc_inputs/samples.tsv` : manifest TSV used as input to `run_pipeline.py --mode QC`
- `demux_qc_out/` : QC reports under `fastqc/`, `fastp/`, `fastp_trimmed/`, `falco/`, and `multiqc/` (depending on `--qc-tool` and installed tools)
- `demux_qc_out/contamination/` : optional contamination reports (included in `demux_qc_out/multiqc/`)

### QC only (from a manifest TSV)

```bash
pixi run python run_pipeline.py \
  --mode QC \
  --qc-tool fastqc \
  --manifest-tsv /path/to/demux_out/qc_inputs/samples.tsv \
  --outdir ./qc_out \
  --threads 4
```

### QC only (from an existing FASTQ dir)

```bash
pixi run python run_pipeline.py \
  --mode QC \
  --qc-tool fastqc \
  --in-fastq-dir /path/to/demux_out/demux_fastq/combined \
  --outdir ./qc_out \
  --threads 4
```

### Full pipeline from Python (no CLI)

```python
from pathlib import Path

from pipeline import unified_demux_qc_contamination_pipeline

unified_demux_qc_contamination_pipeline(
    mode="demux",
    qc_tool="fastqc",
    bcl_dir=Path("/path/to/BCL_RUN_FOLDER"),
    samplesheet=Path("/path/to/SampleSheet.csv"),
    outdir=Path("./demux_qc_out"),
    threads=4,
)
```
