# demux_prefect

<!-- AGENT_METADATA_START -->
## Agent Metadata

```yaml
id: demux_prefect
kind: template
descriptor: templates/demux_prefect/template_config.yaml
run_entry: run.sh
required_params:
  - outdir
  - bcl_dir
  - samplesheet
publish_keys:
  - samples_tsv
  - qc_dir
  - contamination_dir
  - multiqc_report
  - run_summary
output_contract_file: template_outputs.json
```
<!-- AGENT_METADATA_END -->

Minimal template interface around the existing Prefect demux pipeline.

## Parameters

- `outdir`: output root
- `bcl_dir`, `samplesheet`: required demultiplexing inputs
- `qc_tool`: defaults to `falco`
- `contamination_tool`: defaults to `none`
- `threads`: defaults to `4`
- `run_name`: optional

## Behavior

- `run.sh` validates required host commands (`pixi`, `bcl-convert`) and executes the pipeline via `pixi run python demux_pipeline/cli.py`.
- The host system does not need Python directly; Python usage is encapsulated in `pixi run`.
- `run.sh` passes `--output-contract-file template_outputs.json` so contract generation happens in the pipeline/CLI layer.

## Published outputs

- This template does not use BPM resolver functions.
- `demux_pipeline/cli.py` writes `template_outputs.json` when invoked with `--output-contract-file`, and `run.sh` only forwards template inputs.
- Output keys:
  - `samples_tsv`: `${outdir}/samples.tsv`
  - `qc_dir`: first existing directory among `${outdir}/falco`, `${outdir}/fastqc`, `${outdir}/fastp`
  - `contamination_dir`: `${outdir}/contamination` when present (else `null`)
  - `multiqc_report`: `${outdir}/multiqc/multiqc_report.html` when present (else `null`)
  - `run_summary`: latest `${outdir}/.pipeline/*/run_summary.json` when present (else `null`)
