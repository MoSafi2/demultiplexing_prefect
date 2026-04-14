# demux-pipeline

Standalone demultiplexing, QC, and contamination CLI for Illumina runs.

This repository is now CLI-first rather than BPM-template-first so Linkar and other
orchestrators can call it directly without adding another environment wrapper.

## What changed

- The public interface is the `demux-pipeline` CLI.
- The repository now includes a standard `pyproject.toml` console entry point.
- `pixi.toml` remains the only environment definition for local development and runtime.
- BPM-specific template files were removed from the root package boundary.
- `run.sh` is now a generic convenience runner that forwards to `pixi run demux-pipeline`.

## Install

This project uses `pixi`:

```bash
pixi install
```

You can also install it as a normal Python package if you want the console script:

```bash
pip install -e .
```

## Requirements

System tools must be on your `PATH` depending on the mode you run:

- `bcl-convert` for demultiplexing
- `fastqc`, `fastp`, `falco`, `multiqc`, `kraken2`, `bracken`, `fastq_screen`, `bowtie2`
  according to the QC and contamination tools you select

## CLI usage

Preferred invocation:

```bash
pixi run demux-pipeline \
  --qc-tool falco \
  --outdir ./demux_qc_out \
  --threads 4 \
  --bcl_dir /path/to/BCL_RUN_FOLDER \
  --samplesheet /path/to/SampleSheet.csv
```

Equivalent entry points:

```bash
python -m demux_pipeline.cli ...
./run.sh ...
```

`--qc-tool` accepts one or more of: `fastqc`, `fastp`, `falco`.

`--contamination-tool` accepts one or more of: `kraken`, `kraken_bracken`, `fastq_screen`, `none`.

- Use comma-separated values to run multiple tools in one run, for example `--qc-tool fastqc,fastp`.
- `none` must be used alone and disables contamination.
- If `kraken` is selected, pass `--kraken-db`.
- If `kraken_bracken` is selected, pass `--bracken-db` or `--kraken-db`.
- If `fastq_screen` is selected, pass `--fastq-screen-conf`.
- Optional: pass `--output-contract-file PATH` to write a machine-readable output contract JSON for Linkar or other orchestrators.

## Examples

Demux plus QC:

```bash
pixi run demux-pipeline \
  --qc-tool fastqc \
  --outdir ./demux_qc_out \
  --threads 4 \
  --bcl_dir /path/to/BCL_RUN_FOLDER \
  --samplesheet /path/to/SampleSheet.csv
```

Optional contamination screening:

```bash
pixi run demux-pipeline \
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
pixi run demux-pipeline \
  --qc-tool fastqc,fastp \
  --outdir ./demux_qc_out \
  --threads 4 \
  --bcl_dir /path/to/BCL_RUN_FOLDER \
  --samplesheet /path/to/SampleSheet.csv \
  --contamination-tool kraken,fastq_screen \
  --kraken-db /path/to/kraken-db \
  --fastq-screen-conf /path/to/fastq_screen.conf
```

## Output contract

When invoked with `--output-contract-file`, the CLI writes a JSON document with:

- `samples_tsv`
- `qc_dir`
- `contamination_dir`
- `multiqc_report`
- `run_summary`

This keeps the orchestration boundary small: Linkar can bind params and consume outputs
without owning the internal demultiplexing logic.

## Prefect artifacts

Prefect table artifact publishing is disabled by default for local CLI runs and tests to
avoid ephemeral server startup and shutdown noise.

Enable it explicitly when you want Prefect UI artifacts:

```bash
DEMUX_ENABLE_PREFECT_ARTIFACTS=1 pixi run demux-pipeline ...
```

## Output directories

All outputs go under `--outdir`:

- `outdir/output/` for `bcl-convert` output
- `outdir/fastqc/` for FastQC reports
- `outdir/fastp/` for Fastp HTML and JSON
- `outdir/fastp_passthrough/` for Fastp FASTQ outputs
- `outdir/falco/<sample>_<R1|R2>/` for Falco output
- `outdir/contamination/` for optional contamination outputs
- `outdir/multiqc/` for MultiQC summaries

The pipeline also writes `outdir/samples.tsv` after demultiplexing and sample discovery.

## Linkar integration advice

For Linkar, prefer calling this repository directly from a template:

```bash
git clone ...
cd demultiplexing_prefect
pixi run demux-pipeline ...
```

or call the generic wrapper:

```bash
OUTPUT_CONTRACT_FILE=template_outputs.json ./run.sh ...
```

That lets `demultiplexing_prefect` own its dependencies and runtime contract while Linkar
stays focused on param binding, run layout, and downstream output wiring.

## Tests

Run the unit tests:

```bash
pixi run test
```

Run the QC smoke test:

```bash
pixi run python test/run_qc_smoke_test.py --outdir ./qc_smoke_out
```
