#!/usr/bin/env bash
set -euo pipefail

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "ERROR: required command not found in PATH: $1" >&2
    exit 127
  fi
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

DATA_ROOT="${REPO_ROOT}/data/elembio/bases2fastq"
DOWNLOAD_DIR="${DATA_ROOT}/downloads"
INSTALL_DIR="${DATA_ROOT}/install"
SAMPLES_DIR="${DATA_ROOT}/samples"

BASES2FASTQ_URL="https://bases2fastq-release.s3.amazonaws.com/bases2fastq-latest.tar.gz"
BASES2FASTQ_ARCHIVE="${DOWNLOAD_DIR}/bases2fastq-latest.tar.gz"
BASES2FASTQ_BIN="${INSTALL_DIR}/bases2fastq"

SIM_NAME="20230404-bases2fastq-sim-151-151-9-9"
SIM_URL="http://element-public-data.s3.amazonaws.com/bases2fastq-share/bases2fastq-v2/${SIM_NAME}.tar.gz"
SIM_ARCHIVE="${DOWNLOAD_DIR}/${SIM_NAME}.tar.gz"
SIM_RUN_DIR="${SAMPLES_DIR}/${SIM_NAME}"

QC_TOOL="${QC_TOOL:-falco}"
THREADS="${THREADS:-4}"
OUTDIR="${OUTDIR:-${REPO_ROOT}/data/runs/aviti-simulated}"
OUTPUT_CONTRACT_FILE="${OUTPUT_CONTRACT_FILE:-${OUTDIR}/template_outputs.json}"

require_cmd curl
require_cmd tar
require_cmd pixi

mkdir -p "${DOWNLOAD_DIR}" "${INSTALL_DIR}" "${SAMPLES_DIR}"

if [[ ! -f "${BASES2FASTQ_ARCHIVE}" ]]; then
  echo "Downloading Bases2Fastq archive..."
  curl -L "${BASES2FASTQ_URL}" -o "${BASES2FASTQ_ARCHIVE}"
fi

if [[ ! -x "${BASES2FASTQ_BIN}" ]]; then
  echo "Extracting Bases2Fastq binary..."
  rm -rf "${INSTALL_DIR}"
  mkdir -p "${INSTALL_DIR}"
  tar -xzf "${BASES2FASTQ_ARCHIVE}" -C "${INSTALL_DIR}"
  chmod +x "${BASES2FASTQ_BIN}"
fi

if [[ ! -f "${SIM_ARCHIVE}" ]]; then
  echo "Downloading simulated AVITI run..."
  curl -L "${SIM_URL}" -o "${SIM_ARCHIVE}"
fi

if [[ ! -d "${SIM_RUN_DIR}" ]]; then
  echo "Extracting simulated AVITI run..."
  tar -xzf "${SIM_ARCHIVE}" -C "${SAMPLES_DIR}"
fi

if [[ ! -f "${SIM_RUN_DIR}/RunManifest.csv" ]]; then
  echo "ERROR: missing simulated RunManifest.csv: ${SIM_RUN_DIR}/RunManifest.csv" >&2
  exit 1
fi

if [[ ! -f "${SIM_RUN_DIR}/RunParameters.json" ]]; then
  echo "ERROR: missing simulated RunParameters.json: ${SIM_RUN_DIR}/RunParameters.json" >&2
  exit 1
fi

export PATH="${INSTALL_DIR}:${PATH}"

rm -rf "${OUTDIR}"
mkdir -p "${OUTDIR}"

cd "${REPO_ROOT}"

exec pixi run demux-pipeline \
  --platform aviti \
  --qc-tool "${QC_TOOL}" \
  --threads "${THREADS}" \
  --input-dir "${SIM_RUN_DIR}" \
  --samplesheet "${SIM_RUN_DIR}/RunManifest.csv" \
  --outdir "${OUTDIR}" \
  --output-contract-file "${OUTPUT_CONTRACT_FILE}"
