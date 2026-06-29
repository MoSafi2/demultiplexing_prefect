from __future__ import annotations

from collections import defaultdict
import csv
from dataclasses import dataclass
from pathlib import Path
import re
import shutil

from demux_pipeline.illumina_demux import _resolve_local_binary, parse_fastq


AVITI_NATIVE_OUTDIR = Path(".demux_native") / "bases2fastq"
AVITI_AUX_OUTDIR_NAME = "bases2fastq"
AVITI_SAMPLES_DIR_NAME = "Samples"

ILLUMINA_SAMPLE_ID_KEYS = ("sample_id", "sampleid", "sample_name", "samplename")
ILLUMINA_PROJECT_KEYS = ("sample_project", "sampleproject", "project")

FASTQ_READ_RE = re.compile(
    r"(?i)^(?P<stem>.+?)(?:_S\d+)?(?:_L\d{3})?_R(?P<read>[12])(?:_(?P<chunk>\d{3}))?\.(?:fastq|fq)(?:\.gz)?$"
)
SECTION_HEADER_RE = re.compile(r"^\[(?P<name>[^\]]+)\]\s*(?:,.*)?$")


@dataclass(frozen=True, slots=True)
class ManifestSampleEntry:
    sample_name: str
    project: str | None


@dataclass(frozen=True, slots=True)
class NativeFastqGroup:
    sample_name: str
    project: str | None
    reads: dict[int, list[Path]]


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

    return rows if matched_target else None


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

    normalized = {_normalize_manifest_key(name): name for name in reader.fieldnames}
    sample_key = next(
        (normalized[candidate] for candidate in ILLUMINA_SAMPLE_ID_KEYS if candidate in normalized),
        None,
    )
    project_key = next(
        (normalized[candidate] for candidate in ILLUMINA_PROJECT_KEYS if candidate in normalized),
        None,
    )
    if sample_key is None:
        raise RuntimeError(
            f"Manifest {manifest_path} does not contain a recognizable sample column."
        )

    entries: list[ManifestSampleEntry] = []
    for row in reader:
        sample_name = (row.get(sample_key) or "").strip()
        if not sample_name or sample_name.lower() in {"undetermined", "unassigned"}:
            continue
        project = (row.get(project_key) or "").strip() if project_key else ""
        entries.append(ManifestSampleEntry(sample_name=sample_name, project=project or None))
    return entries


def _sample_ordinals_from_manifest(manifest_path: Path) -> dict[tuple[str | None, str], int]:
    ordinals: dict[tuple[str | None, str], int] = {}
    next_index = 1
    for entry in _read_manifest_entries(manifest_path):
        key = (entry.project, entry.sample_name)
        if key not in ordinals:
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


def _is_fastq_artifact(path: Path) -> bool:
    name = path.name.lower()
    return (
        name.endswith(".fastq")
        or name.endswith(".fq")
        or name.endswith(".fastq.gz")
        or name.endswith(".fq.gz")
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


def build_aviti_demux_command(
    *,
    demux_bin: str | None = None,
    input_dir: Path,
    staged_output: Path,
    samplesheet: Path,
    extra_args: list[str] | None = None,
) -> list[str]:
    _validate_aviti_input_dir(input_dir)
    cmd = [
        demux_bin or _resolve_bases2fastq_binary(),
        str(input_dir),
        str(staged_output),
        "--run-manifest",
        str(samplesheet),
    ]
    if extra_args:
        cmd.extend(extra_args)
    return cmd


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


def _planned_normalized_aviti_fastqs(
    *,
    staged_output: Path,
    manifest_path: Path,
) -> dict[Path, Path]:
    samples_root = staged_output / AVITI_SAMPLES_DIR_NAME
    if not samples_root.is_dir():
        raise RuntimeError(
            f"Bases2Fastq output is missing the expected {AVITI_SAMPLES_DIR_NAME}/ directory: {samples_root}"
        )

    manifest_ordinals = _sample_ordinals_from_manifest(manifest_path)
    fallback_next = max(manifest_ordinals.values(), default=0) + 1
    planned: dict[Path, Path] = {}
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
            for fallback_key in ((None, sample_name), (group.project, sample_name)):
                ordinal = manifest_ordinals.get(fallback_key)
                if ordinal is not None:
                    break
        if ordinal is None:
            ordinal = fallback_next
            manifest_ordinals[key] = ordinal
            fallback_next += 1

        sample_prefix = Path(group.project) if group.project else Path()
        normalized_name = _safe_fastq_name(sample_name)
        max_chunks = max((len(paths) for paths in group.reads.values()), default=0)
        for chunk_index in range(max_chunks):
            for read in (1, 2):
                paths = group.reads.get(read, [])
                if chunk_index >= len(paths):
                    continue
                rel_dest = sample_prefix / (
                    f"{normalized_name}_S{ordinal}_R{read}_{chunk_index + 1:03d}.fastq.gz"
                )
                planned[rel_dest] = paths[chunk_index]

    grouped_unassigned: dict[int, list[Path]] = defaultdict(list)
    for path in unassigned_paths:
        read, _chunk = _guess_native_fastq_read(path)
        if read in (1, 2):
            grouped_unassigned[read].append(path)
    for read, paths in grouped_unassigned.items():
        for chunk_index, src in enumerate(sorted(paths), start=1):
            planned[Path(f"Undetermined_S0_R{read}_{chunk_index:03d}.fastq.gz")] = src

    return planned


def verify_aviti_outputs(
    *,
    staged_output: Path,
    demux_output: Path,
    aux_output: Path,
    manifest_path: Path,
) -> None:
    expected_fastqs = _planned_normalized_aviti_fastqs(
        staged_output=staged_output,
        manifest_path=manifest_path,
    )
    actual_fastqs = {
        path.relative_to(demux_output): path
        for path in demux_output.rglob("*")
        if path.is_file() and _is_fastq_artifact(path)
    }
    if set(actual_fastqs) != set(expected_fastqs):
        missing = sorted(str(path) for path in set(expected_fastqs) - set(actual_fastqs))
        extra = sorted(str(path) for path in set(actual_fastqs) - set(expected_fastqs))
        raise RuntimeError(
            "AVITI normalized FASTQ verification failed. "
            f"Missing: {missing or 'none'}. Extra: {extra or 'none'}."
        )
    for rel_path, src in expected_fastqs.items():
        if actual_fastqs[rel_path].stat().st_size != src.stat().st_size:
            raise RuntimeError(
                f"AVITI normalized FASTQ size mismatch for {rel_path}: "
                f"{actual_fastqs[rel_path].stat().st_size} != {src.stat().st_size}"
            )

    expected_aux = {
        path.relative_to(staged_output): path
        for path in staged_output.rglob("*")
        if path.is_file() and not _is_fastq_artifact(path)
    }
    actual_aux = {
        path.relative_to(aux_output): path
        for path in aux_output.rglob("*")
        if path.is_file()
    }
    if set(actual_aux) != set(expected_aux):
        missing = sorted(str(path) for path in set(expected_aux) - set(actual_aux))
        extra = sorted(str(path) for path in set(actual_aux) - set(expected_aux))
        raise RuntimeError(
            "AVITI auxiliary artifact verification failed. "
            f"Missing: {missing or 'none'}. Extra: {extra or 'none'}."
        )
    for rel_path, src in expected_aux.items():
        if actual_aux[rel_path].stat().st_size != src.stat().st_size:
            raise RuntimeError(
                f"AVITI auxiliary artifact size mismatch for {rel_path}: "
                f"{actual_aux[rel_path].stat().st_size} != {src.stat().st_size}"
            )


def normalize_aviti_output(
    *,
    staged_output: Path,
    demux_output: Path,
    manifest_path: Path,
) -> None:
    if demux_output.exists():
        shutil.rmtree(demux_output)
    demux_output.mkdir(parents=True, exist_ok=True)
    for rel_dest, src in _planned_normalized_aviti_fastqs(
        staged_output=staged_output,
        manifest_path=manifest_path,
    ).items():
        _link_or_copy(src, demux_output / rel_dest)


def finalize_aviti_outputs(
    *,
    staged_output: Path,
    demux_output: Path,
    aux_output: Path,
    manifest_path: Path,
) -> None:
    copy_aviti_auxiliary_outputs(
        staged_output=staged_output,
        destination_root=aux_output,
    )
    normalize_aviti_output(
        staged_output=staged_output,
        demux_output=demux_output,
        manifest_path=manifest_path,
    )
    verify_aviti_outputs(
        staged_output=staged_output,
        demux_output=demux_output,
        aux_output=aux_output,
        manifest_path=manifest_path,
    )
    shutil.rmtree(staged_output.parent)
