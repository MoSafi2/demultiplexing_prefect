# Changelog

This changelog is a retrospective summary of the most important repository milestones.
The repository does not currently contain historical git tags, so versions before
`0.1.0` are documented here as inferred project milestones rather than confirmed
published releases.

## 0.1.0 - 2026-06-29

- Converted the repository into a standalone `demux-pipeline` CLI with a packaged
  `pyproject.toml` entry point, replacing the earlier BPM-template-oriented shape.
- Added AVITI support through `bases2fastq`, including run-manifest handling,
  normalized FASTQ output, copied run metrics/logs, and final-shape cleanup of
  staging files.
- Added project-aware output layout so FASTQs, QC outputs, contamination outputs,
  and project MultiQC reports are grouped under per-project directories.
- Added machine-readable output contract generation for orchestrators such as Linkar.
- Added sample hashing and stronger run artifact tracking/observability.
- Split demultiplexing implementation into `illumina_demux.py`, `aviti_demux.py`,
  and a small dispatcher `demux.py`.

## 0.0.4 - 2026-05-04

- Added project-grouped QC output handling and exported project-specific paths in
  the output contract.
- Fixed FASTQ project detection and placed contamination outputs under project QC
  directories.
- Reduced noisy successful subprocess logging and improved local test ergonomics.

## 0.0.3 - 2026-04-13

- Added multi-tool execution for QC and contamination via comma-separated CLI values.
- Simplified the CLI surface by removing earlier mode-driven paths and making the
  command line more direct.
- Reworked the runtime around orchestrator-friendly invocation and clearer README
  guidance.

## 0.0.2 - 2026-03-26

- Added concurrency improvements so QC and contamination can run in parallel with a
  shared thread budget.
- Introduced observability and artifact recording for pipeline runs.
- Added Kraken/Bracken contamination support and broader QC/contamination pipeline
  integration.

## 0.0.1 - 2026-03-25

- Introduced the first Prefect-based demultiplexing and QC pipeline implementation.
- Established the initial Illumina demultiplexing flow, repository structure, and
  development environment.
