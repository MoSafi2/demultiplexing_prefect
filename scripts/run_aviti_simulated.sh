#!/usr/bin/env bash
set -euo pipefail

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "ERROR: required command not found in PATH: $1" >&2
    exit 127
  fi
}

download_file() {
  local url="$1"
  local dest="$2"

  curl --fail --location --retry 3 --retry-delay 2 --output "${dest}" "${url}"
}

ensure_tar_archive() {
  local path="$1"
  local url="$2"
  local label="$3"

  if [[ -f "${path}" ]] && tar -tf "${path}" >/dev/null 2>&1; then
    return 0
  fi

  rm -f "${path}"
  echo "Downloading ${label}..."
  download_file "${url}" "${path}"

  if ! tar -tf "${path}" >/dev/null 2>&1; then
    echo "ERROR: downloaded ${label} is not a valid tar archive: ${path}" >&2
    rm -f "${path}"
    exit 1
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
SIM_URL="https://element-public-data.s3.amazonaws.com/bases2fastq-share/bases2fastq-v2/${SIM_NAME}.tar.gz"
SIM_ARCHIVE="${DOWNLOAD_DIR}/${SIM_NAME}.tar.gz"
SIM_RUN_DIR="${SAMPLES_DIR}/${SIM_NAME}"

QC_TOOL="${QC_TOOL:-falco}"
THREADS="${THREADS:-4}"
OUTDIR="${OUTDIR:-${REPO_ROOT}/data/runs/aviti-simulated}"
OUTPUT_CONTRACT_FILE="${OUTPUT_CONTRACT_FILE:-${OUTDIR}/template_outputs.json}"
SIM_PROJECT_NAME="${SIM_PROJECT_NAME:-custom-aviti-project}"

write_custom_project_manifest() {
  local src="$1"
  local dest="$2"
  local project_name="$3"

  python3 - "${src}" "${dest}" "${project_name}" <<'PY'
import csv
import re
import sys
from pathlib import Path

src = Path(sys.argv[1])
dest = Path(sys.argv[2])
project_name = sys.argv[3]

section_header_re = re.compile(r"^\[(?P<name>[^\]]+)\]\s*(?:,.*)?$")


def normalize(value: str) -> str:
    return re.sub(r"[^a-z0-9]", "", value.strip().lower())


lines = src.read_text(encoding="utf-8", errors="replace").splitlines()
output: list[str] = []
in_samples = False
header_done = False
project_index: int | None = None
header_width = 0

for line in lines:
    stripped = line.strip()
    match = section_header_re.match(stripped)
    if match:
        in_samples = normalize(match.group("name")) == "samples"
        header_done = False
        project_index = None
        header_width = 0
        output.append(line)
        continue

    if not in_samples or not stripped or stripped.startswith("#"):
        output.append(line)
        continue

    row = next(csv.reader([line]))
    if not header_done:
        normalized = [normalize(field) for field in row]
        if "project" in normalized:
            project_index = normalized.index("project")
        else:
            project_index = len(row)
            row.append("Project")
        header_width = len(row)
        header_done = True
    else:
        if project_index is None:
            raise RuntimeError("sample project column index was not initialized")
        if len(row) < header_width:
            row.extend([""] * (header_width - len(row)))
        row[project_index] = project_name

    output.append(",".join(row))

dest.write_text("\n".join(output) + "\n", encoding="utf-8")
PY
}

require_cmd curl
require_cmd tar
require_cmd pixi
require_cmd python3

mkdir -p "${DOWNLOAD_DIR}" "${INSTALL_DIR}" "${SAMPLES_DIR}"

ensure_tar_archive "${BASES2FASTQ_ARCHIVE}" "${BASES2FASTQ_URL}" "Bases2Fastq archive"

if [[ ! -x "${BASES2FASTQ_BIN}" ]]; then
  echo "Extracting Bases2Fastq binary..."
  rm -rf "${INSTALL_DIR}"
  mkdir -p "${INSTALL_DIR}"
  tar -xf "${BASES2FASTQ_ARCHIVE}" -C "${INSTALL_DIR}"
  chmod +x "${BASES2FASTQ_BIN}"
fi

ensure_tar_archive "${SIM_ARCHIVE}" "${SIM_URL}" "simulated AVITI run"

if [[ ! -d "${SIM_RUN_DIR}" ]]; then
  echo "Extracting simulated AVITI run..."
  tar -xf "${SIM_ARCHIVE}" -C "${SAMPLES_DIR}"
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

CUSTOM_RUN_MANIFEST="${OUTDIR}/RunManifest.custom.csv"
write_custom_project_manifest \
  "${SIM_RUN_DIR}/RunManifest.csv" \
  "${CUSTOM_RUN_MANIFEST}" \
  "${SIM_PROJECT_NAME}"

cd "${REPO_ROOT}"

exec pixi run demux-pipeline \
  --platform aviti \
  --qc-tool "${QC_TOOL}" \
  --threads "${THREADS}" \
  --input-dir "${SIM_RUN_DIR}" \
  --samplesheet "${CUSTOM_RUN_MANIFEST}" \
  --outdir "${OUTDIR}" \
  --output-contract-file "${OUTPUT_CONTRACT_FILE}"
