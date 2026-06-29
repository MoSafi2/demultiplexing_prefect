from __future__ import annotations

from collections import defaultdict
import csv
import re
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from prefect import get_run_logger, task  # type: ignore[import-not-found]
from demux_pipeline.models import Sample
from demux_pipeline.process import run_command
from demux_pipeline.observability import record_asset


DEMUX_FASTQ_OUTDIR_NAME = "output"
AVITI_NATIVE_OUTDIR = Path(".demux_native") / "bases2fastq"
AVITI_AUX_OUTDIR_NAME = "bases2fastq"
AVITI_SAMPLES_DIR_NAME = "Samples"

ILLUMINA_SAMPLE_ID_KEYS = ("sample_id", "sampleid", "sample_name", "samplename")
ILLUMINA_PROJECT_KEYS = ("sample_project", "sampleproject", "project")

FASTQ_RE = re.compile(
    r"""^(?P<sample>[A-Za-z0-9_.-]+?)(?:_S\d+)?(?:_L(?P<lane>\d{3}))?_R(?P<read>[12])
    (?:_(?P<chunk>\d{3}))?\.(?P<ext>fastq|fq)(?:\.gz)?$""",
    re.VERBOSE | re.IGNORECASE,
)
FASTQ_READ_RE = re.compile(
    r"(?i)^(?P<stem>.+?)(?:_S\d+)?(?:_L\d{3})?_R(?P<read>[12])(?:_(?P<chunk>\d{3}))?\.(?:fastq|fq)(?:\.gz)?$"
)


@dataclass(frozen=True, slots=True)
class ManifestSampleEntry:
    sample_name: str
    project: str | None


@dataclass(frozen=True, slots=True)
class NativeFastqGroup:
    sample_name: str
    project: str | None
    reads: dict[int, list[Path]]


def parse_fastq(path: Path):
    m = FASTQ_RE.match(path.name)
    if not m:
        return None

    return {
        "sample": m.group("sample"),
        "read": int(m.group("read")),
        "lane": int(m.group("lane")) if m.group("lane") else None,
        "chunk": int(m.group("chunk")) if m.group("chunk") else 0,
    }


def _group_fastqs(
    root: Path, *, recursive: bool = True, include_undetermined: bool = False
) -> dict[tuple[str | None, str, int], dict[str, Path]]:
    iterator = root.rglob("*") if recursive else root.glob("*")
    paths = [path for path in iterator if path.is_file()]
    grouped: dict[tuple[str | None, str, int], dict[str, Path]] = defaultdict(dict)
    for path in paths:
        if _is_under_qc_dir(root, path):
            continue
        if not include_undetermined:
            # Skip undetermined reads
            if any("undetermined" in part.lower() for part in path.parts):
                continue
        parsed = parse_fastq(path)
        if not parsed:
            continue
        read_key = f"R{parsed['read']}"
        project = _project_from_fastq_path(root, path)
        grouped[project, parsed["sample"], parsed["chunk"]][read_key] = path
    return grouped


def _is_under_qc_dir(root: Path, path: Path) -> bool:
    try:
        rel = path.relative_to(root)
    except ValueError:
        return False
    return "qc" in rel.parts


def _project_from_fastq_path(root: Path, path: Path) -> str | None:
    try:
        rel = path.relative_to(root)
    except ValueError:
        return None
    return rel.parts[0] if len(rel.parts) > 1 else None


def _samples_from_fastq_dir(
    root: Path,
    *,
    recursive: bool = True,
    include_undetermined: bool = False,
) -> list[Sample]:
    """
    Discover FASTQ files and return a list of Sample objects.

    Behavior:
    - Walks directory (recursive by default)
    - Groups by (sample, chunk)
    - Each chunk becomes an independent Sample
    - Supports SE and PE
    - Optionally filters Undetermined reads
    """

    grouped = _group_fastqs(
        root, recursive=recursive, include_undetermined=include_undetermined
    )
    samples: list[Sample] = []

    for (project, sample, chunk), reads in sorted(
        grouped.items(), key=lambda x: (x[0][0] or "", x[0][1], x[0][2])
    ):
        if "R1" not in reads:
            # skip incomplete units
            continue
        samples.append(
            Sample(name=sample, r1=reads["R1"], r2=reads.get("R2"), project=project)
        )

    return samples


def _write_samples_tsv(samples: list[Sample], path: Path) -> None:
    with path.open("w") as f:
        for sample in samples:
            r2 = str(sample.r2) if sample.r2 is not None else ""
            project = sample.project or ""
            f.write(f"{sample.name}\t{sample.r1}\t{r2}\t{project}\n")


def _resolve_local_binary(*candidates: str) -> str | None:
    project_root = Path(__file__).resolve().parent
    for candidate in candidates:
        local_names = {candidate}
        if candidate == "bcl_convert":
            local_names.add("bcl-convert")
        for local_name in local_names:
            for local in (Path.cwd() / local_name, project_root / local_name):
                if local.is_file():
                    return str(local.resolve())
        found = shutil.which(candidate)
        if found is not None:
            return found
    return None


def _resolve_bcl_convert_binary() -> str:
    """Prefer local copies, then PATH."""
    found = _resolve_local_binary("bcl-convert", "bcl_convert")
    if found is not None:
        return found
    raise SystemExit(
        "Missing required binary on PATH: bcl-convert (or bcl_convert). "
        "Please install BCL Convert and ensure it is available on your PATH."
    )


def _resolve_bases2fastq_binary() -> str:
    found = _resolve_local_binary("bases2fastq")
    if found is not None:
        return found
    raise SystemExit(
        "Missing required binary on PATH: bases2fastq. "
        "Please install Bases2Fastq and ensure it is available on your PATH."
    )


def _normalize_manifest_key(name: str) -> str:
    return re.sub(r"[^a-z0-9]", "", name.lower())


SECTION_HEADER_RE = re.compile(r"^\[(?P<name>[^\]]+)\]\s*(?:,.*)?$")


def _manifest_section_name(line: str) -> str | None:
    match = SECTION_HEADER_RE.match(line.strip())
    if not match:
        return None
    return _normalize_manifest_key(match.group("name"))


def _manifest_section_rows(lines: list[str], *section_names: str) -> list[str] | None:
    target_names = {_normalize_manifest_key(name) for name in section_names}
    collecting = False
    matched_target = False
    rows: list[str] = []

    for line in lines:
        stripped = line.strip()
        section_name = _manifest_section_name(stripped)
        if section_name is not None:
            if collecting:
                break
            collecting = section_name in target_names
            if collecting:
                matched_target = True
            continue
        if not stripped or stripped.startswith("#"):
            continue
        if collecting:
            rows.append(line)

    if matched_target:
        return rows
    return None


def _manifest_fallback_rows(lines: list[str]) -> list[str]:
    rows: list[str] = []
    for line in lines:
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or _manifest_section_name(stripped):
            continue
        rows.append(line)
    return rows


def _read_manifest_entries(manifest_path: Path) -> list[ManifestSampleEntry]:
    lines = manifest_path.read_text(encoding="utf-8", errors="replace").splitlines()
    if not lines:
        return []
    csv_rows = _manifest_section_rows(lines, "Samples", "Data")
    if csv_rows is None:
        csv_rows = _manifest_fallback_rows(lines)
    csv_text = "\n".join(csv_rows).strip()
    if not csv_text:
        return []
    reader = csv.DictReader(csv_text.splitlines())
    if reader.fieldnames is None:
        return []
    sample_key = None
    project_key = None
    normalized = {_normalize_manifest_key(name): name for name in reader.fieldnames}
    for candidate in ILLUMINA_SAMPLE_ID_KEYS:
        if candidate in normalized:
            sample_key = normalized[candidate]
            break
    for candidate in ILLUMINA_PROJECT_KEYS:
        if candidate in normalized:
            project_key = normalized[candidate]
            break
    if sample_key is None:
        raise RuntimeError(
            f"Manifest {manifest_path} does not contain a recognizable sample column."
        )

    entries: list[ManifestSampleEntry] = []
    for row in reader:
        sample_name = (row.get(sample_key) or "").strip()
        if not sample_name:
            continue
        if sample_name.lower() in {"undetermined", "unassigned"}:
            continue
        project = (row.get(project_key) or "").strip() if project_key else ""
        entries.append(
            ManifestSampleEntry(
                sample_name=sample_name,
                project=project or None,
            )
        )
    return entries


def _sample_ordinals_from_manifest(manifest_path: Path) -> dict[tuple[str | None, str], int]:
    ordinals: dict[tuple[str | None, str], int] = {}
    next_index = 1
    for entry in _read_manifest_entries(manifest_path):
        key = (entry.project, entry.sample_name)
        if key in ordinals:
            continue
        ordinals[key] = next_index
        next_index += 1
    return ordinals


def _guess_native_fastq_read(path: Path) -> tuple[int | None, int]:
    parsed = parse_fastq(path)
    if parsed:
        return parsed["read"], parsed["chunk"]
    match = FASTQ_READ_RE.match(path.name)
    if match:
        chunk = int(match.group("chunk")) if match.group("chunk") else 1
        return int(match.group("read")), chunk
    return None, 1


def _native_sample_identity(samples_root: Path, fastq_path: Path) -> tuple[str | None, str]:
    rel = fastq_path.relative_to(samples_root)
    parent_parts = rel.parent.parts
    parsed = parse_fastq(fastq_path)
    parsed_name = parsed["sample"] if parsed else None

    if len(parent_parts) >= 2:
        return parent_parts[0], parent_parts[1]
    if len(parent_parts) == 1:
        return None, parent_parts[0]
    if parsed_name:
        return None, parsed_name
    raise RuntimeError(f"Unable to infer sample identity from {fastq_path}")


def _group_native_aviti_fastqs(samples_root: Path) -> list[NativeFastqGroup]:
    grouped: dict[tuple[str | None, str], dict[int, list[Path]]] = defaultdict(
        lambda: defaultdict(list)
    )
    for path in sorted(samples_root.rglob("*")):
        if not path.is_file():
            continue
        read, _chunk = _guess_native_fastq_read(path)
        if read not in (1, 2):
            continue
        project, sample_name = _native_sample_identity(samples_root, path)
        grouped[(project, sample_name)][read].append(path)

    result: list[NativeFastqGroup] = []
    for (project, sample_name), reads in sorted(
        grouped.items(), key=lambda item: ((item[0][0] or ""), item[0][1])
    ):
        result.append(
            NativeFastqGroup(
                sample_name=sample_name,
                project=project,
                reads={read: sorted(paths) for read, paths in reads.items()},
            )
        )
    return result


def _safe_fastq_name(name: str) -> str:
    safe = re.sub(r"[^A-Za-z0-9_.-]", "_", name)
    return safe.strip("_") or "sample"


def _link_or_copy(src: Path, dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    if dest.exists():
        dest.unlink()
    try:
        dest.hardlink_to(src)
    except OSError:
        shutil.copy2(src, dest)


def _normalize_undetermined_fastqs(
    *,
    demux_output: Path,
    unassigned_paths: Iterable[Path],
) -> None:
    grouped: dict[int, list[Path]] = defaultdict(list)
    for path in unassigned_paths:
        read, _chunk = _guess_native_fastq_read(path)
        if read in (1, 2):
            grouped[read].append(path)
    for read, paths in grouped.items():
        for chunk_index, src in enumerate(sorted(paths), start=1):
            dest = demux_output / f"Undetermined_S0_R{read}_{chunk_index:03d}.fastq.gz"
            _link_or_copy(src, dest)


def _is_fastq_artifact(path: Path) -> bool:
    name = path.name.lower()
    return (
        name.endswith(".fastq")
        or name.endswith(".fq")
        or name.endswith(".fastq.gz")
        or name.endswith(".fq.gz")
    )


def copy_aviti_auxiliary_outputs(
    *,
    staged_output: Path,
    destination_root: Path,
) -> None:
    if destination_root.exists():
        shutil.rmtree(destination_root)
    destination_root.mkdir(parents=True, exist_ok=True)

    for src in sorted(staged_output.rglob("*")):
        if not src.is_file() or _is_fastq_artifact(src):
            continue
        dest = destination_root / src.relative_to(staged_output)
        _link_or_copy(src, dest)


def normalize_aviti_output(
    *,
    staged_output: Path,
    demux_output: Path,
    manifest_path: Path,
) -> None:
    samples_root = staged_output / AVITI_SAMPLES_DIR_NAME
    if not samples_root.is_dir():
        raise RuntimeError(
            f"Bases2Fastq output is missing the expected {AVITI_SAMPLES_DIR_NAME}/ directory: {samples_root}"
        )
    if demux_output.exists():
        shutil.rmtree(demux_output)
    demux_output.mkdir(parents=True, exist_ok=True)

    manifest_ordinals = _sample_ordinals_from_manifest(manifest_path)
    fallback_next = max(manifest_ordinals.values(), default=0) + 1
    unassigned_paths: list[Path] = []

    for group in _group_native_aviti_fastqs(samples_root):
        sample_name = group.sample_name
        if sample_name.lower() in {"unassigned", "undetermined"}:
            for paths in group.reads.values():
                unassigned_paths.extend(paths)
            continue

        key = (group.project, sample_name)
        ordinal = manifest_ordinals.get(key)
        if ordinal is None:
            fallback_keys = ((None, sample_name), (group.project, sample_name))
            for fallback_key in fallback_keys:
                ordinal = manifest_ordinals.get(fallback_key)
                if ordinal is not None:
                    break
        if ordinal is None:
            ordinal = fallback_next
            manifest_ordinals[key] = ordinal
            fallback_next += 1

        sample_dir = demux_output / group.project if group.project else demux_output
        normalized_name = _safe_fastq_name(sample_name)
        max_chunks = max((len(paths) for paths in group.reads.values()), default=0)
        for chunk_index in range(max_chunks):
            for read in (1, 2):
                paths = group.reads.get(read, [])
                if chunk_index >= len(paths):
                    continue
                dest = sample_dir / (
                    f"{normalized_name}_S{ordinal}_R{read}_{chunk_index + 1:03d}.fastq.gz"
                )
                _link_or_copy(paths[chunk_index], dest)

    _normalize_undetermined_fastqs(
        demux_output=demux_output,
        unassigned_paths=unassigned_paths,
    )


def _validate_aviti_input_dir(input_dir: Path) -> None:
    required = ("RunManifest.csv", "RunParameters.json")
    missing = [name for name in required if not (input_dir / name).exists()]
    if missing:
        raise SystemExit(
            "Expected --input-dir to be an AVITI run directory containing: "
            + ", ".join(required)
            + f". Missing: {', '.join(missing)}"
        )


@task(name="demux_bcl", log_prints=True)
def demux_bcl(
    *,
    input_dir: Path,
    samplesheet: Path,
    outdir: Path | str,
    platform: str = "illumina",
    extra_args: list[str] | None = None,
    force: bool = True,
) -> None:
    """
    Implementation used by the `demux_bcl_to_fastqs_task`.
    """
    logger = get_run_logger()
    outdir = Path(outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    demux_output = outdir / DEMUX_FASTQ_OUTDIR_NAME

    if not input_dir.exists() or not input_dir.is_dir():
        raise SystemExit(f"Expected --input-dir to be an existing directory: {input_dir}")
    if not samplesheet.exists() or not samplesheet.is_file():
        raise SystemExit(
            f"Expected --samplesheet to be an existing file: {samplesheet}"
        )

    platform_name = platform.lower().strip()
    if platform_name == "illumina":
        demux_bin = _resolve_bcl_convert_binary()
        cmd = [
            demux_bin,
            "--bcl-input-directory",
            str(input_dir),
            "--output-directory",
            str(demux_output),
            "--sample-sheet",
            str(samplesheet),
            "--no-lane-splitting",
            "true",
            "--bcl-sampleproject-subdirectories",
            "true",
        ]
        if force:
            cmd.append("--force")
        if extra_args:
            cmd.extend(extra_args)
        logger.info("bcl-convert: %s", " ".join(cmd))
        run_command(cmd, capture_err_tail=80, step="demux", tool="bcl-convert")
        record_asset(
            demux_output,
            step="demux",
            tool="bcl-convert",
            kind="directory",
            metadata={"source": "bcl-convert --output-directory"},
        )
        return

    if platform_name == "aviti":
        _validate_aviti_input_dir(input_dir)
        native_root = outdir / AVITI_NATIVE_OUTDIR
        aux_output = outdir / AVITI_AUX_OUTDIR_NAME
        native_root.parent.mkdir(parents=True, exist_ok=True)
        if native_root.exists():
            shutil.rmtree(native_root)
        demux_bin = _resolve_bases2fastq_binary()
        cmd = [
            demux_bin,
            str(input_dir),
            str(native_root),
            "--run-manifest",
            str(samplesheet),
        ]
        if extra_args:
            cmd.extend(extra_args)
        logger.info("bases2fastq: %s", " ".join(cmd))
        run_command(cmd, capture_err_tail=80, step="demux", tool="bases2fastq")
        record_asset(
            native_root,
            step="demux",
            tool="bases2fastq",
            kind="directory",
            metadata={"source": "bases2fastq native output"},
        )
        copy_aviti_auxiliary_outputs(
            staged_output=native_root,
            destination_root=aux_output,
        )
        record_asset(
            aux_output,
            step="demux",
            tool="bases2fastq",
            kind="directory",
            metadata={"source": "bases2fastq auxiliary outputs"},
        )
        normalize_aviti_output(
            staged_output=native_root,
            demux_output=demux_output,
            manifest_path=samplesheet,
        )
        record_asset(
            demux_output,
            step="demux",
            tool="bases2fastq",
            kind="directory",
            metadata={"source": "normalized AVITI output"},
        )
        return

    raise SystemExit(f"Unknown platform: {platform}")
