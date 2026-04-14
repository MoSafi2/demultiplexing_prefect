#!/usr/bin/env bash
set -euo pipefail

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "ERROR: required command not found in PATH: $1" >&2
    exit 127
  fi
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

require_cmd bcl-convert
require_cmd pixi

declare -a CMD=(pixi run demux-pipeline "$@")

if [[ -n "${OUTPUT_CONTRACT_FILE:-}" ]]; then
  CMD+=(--output-contract-file "$OUTPUT_CONTRACT_FILE")
fi

echo "Running: ${CMD[*]}"
exec "${CMD[@]}"
