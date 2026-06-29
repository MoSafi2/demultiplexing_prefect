from __future__ import annotations

from collections import defaultdict
import re
import shutil
from pathlib import Path

from demux_pipeline.models import Sample


DEMUX_FASTQ_OUTDIR_NAME = "output"

FASTQ_RE = re.compile(
    r"""^(?P<sample>[A-Za-z0-9_.-]+?)(?:_S\d+)?(?:_L(?P<lane>\d{3}))?_R(?P<read>[12])
    (?:_(?P<chunk>\d{3}))?\.(?P<ext>fastq|fq)(?:\.gz)?$""",
    re.VERBOSE | re.IGNORECASE,
)


def parse_fastq(path: Path):
    match = FASTQ_RE.match(path.name)
    if not match:
        return None

    return {
        "sample": match.group("sample"),
        "read": int(match.group("read")),
        "lane": int(match.group("lane")) if match.group("lane") else None,
        "chunk": int(match.group("chunk")) if match.group("chunk") else 0,
    }


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


def _group_fastqs(
    root: Path, *, recursive: bool = True, include_undetermined: bool = False
) -> dict[tuple[str | None, str, int], dict[str, Path]]:
    iterator = root.rglob("*") if recursive else root.glob("*")
    paths = [path for path in iterator if path.is_file()]
    grouped: dict[tuple[str | None, str, int], dict[str, Path]] = defaultdict(dict)
    for path in paths:
        if _is_under_qc_dir(root, path):
            continue
        if not include_undetermined and any(
            "undetermined" in part.lower() for part in path.parts
        ):
            continue
        parsed = parse_fastq(path)
        if not parsed:
            continue
        read_key = f"R{parsed['read']}"
        project = _project_from_fastq_path(root, path)
        grouped[project, parsed["sample"], parsed["chunk"]][read_key] = path
    return grouped


def _samples_from_fastq_dir(
    root: Path,
    *,
    recursive: bool = True,
    include_undetermined: bool = False,
) -> list[Sample]:
    grouped = _group_fastqs(
        root, recursive=recursive, include_undetermined=include_undetermined
    )
    samples: list[Sample] = []

    for (project, sample, _chunk), reads in sorted(
        grouped.items(), key=lambda item: (item[0][0] or "", item[0][1], item[0][2])
    ):
        if "R1" not in reads:
            continue
        samples.append(
            Sample(name=sample, r1=reads["R1"], r2=reads.get("R2"), project=project)
        )

    return samples


def _write_samples_tsv(samples: list[Sample], path: Path) -> None:
    with path.open("w") as handle:
        for sample in samples:
            r2 = str(sample.r2) if sample.r2 is not None else ""
            project = sample.project or ""
            handle.write(f"{sample.name}\t{sample.r1}\t{r2}\t{project}\n")


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
    found = _resolve_local_binary("bcl-convert", "bcl_convert")
    if found is not None:
        return found
    raise SystemExit(
        "Missing required binary on PATH: bcl-convert (or bcl_convert). "
        "Please install BCL Convert and ensure it is available on your PATH."
    )


def build_illumina_demux_command(
    *,
    demux_bin: str | None = None,
    input_dir: Path,
    demux_output: Path,
    samplesheet: Path,
    extra_args: list[str] | None = None,
    force: bool = True,
) -> list[str]:
    cmd = [
        demux_bin or _resolve_bcl_convert_binary(),
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
    return cmd
