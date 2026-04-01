# demux_prefect

<!-- AGENT_METADATA_START -->
## Agent Metadata

```yaml
id: demux_prefect
kind: template
descriptor: templates/demux_prefect/template_config.yaml
run_entry: run.py
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

- `run.py` reuses the same parser/validation/execution path as `cli.py`.
- No subprocess CLI invocation is used; it imports and calls Python functions directly.

## Published outputs

- This template does not use BPM resolver functions.
- `run.py` infers outputs from `outdir` and writes `template_outputs.json` in the rendered run directory.
- Output keys:
  - `samples_tsv`: `${outdir}/samples.tsv`
  - `qc_dir`: first existing directory among `${outdir}/falco`, `${outdir}/fastqc`, `${outdir}/fastp`
  - `contamination_dir`: `${outdir}/contamination` when present (else `null`)
  - `multiqc_report`: `${outdir}/multiqc/multiqc_report.html` when present (else `null`)
  - `run_summary`: latest `${outdir}/.pipeline/*/run_summary.json` when present (else `null`)
