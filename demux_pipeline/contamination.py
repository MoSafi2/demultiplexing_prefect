from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Callable, Literal, Tuple, cast

from prefect import flow, get_run_logger, task  # type: ignore[import-not-found]
from prefect.futures import PrefectFutureList
from prefect.task_runners import ThreadPoolTaskRunner
from models import Sample
from process import require_executable, run_command
from observability import append_event

ContamSubmitter = Callable[
    [list[Sample], Path, int, Path | None, Path | None, Path | None, int],
    PrefectFutureList,
]


def _ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def _allocate_sample_parallelism(thread_budget: int, num_samples: int) -> Tuple[int, int]:
    """
    Allocate concurrent sample tasks (C) and threads per tool invocation (T) so that
    C * T <= thread_budget.
    """
    if thread_budget < 1:
        raise SystemExit("thread budget must be at least 1")
    if num_samples < 1:
        raise SystemExit("no samples provided")

    max_workers = min(num_samples, max(1, thread_budget))
    per_task_threads = max(1, thread_budget // max_workers)
    return max_workers, per_task_threads


def _bowtie2_index_exists(index_prefix: Path) -> bool:
    """True if Bowtie2 index files exist for basename ``index_prefix``."""
    if not index_prefix.name:
        return False
    return any(index_prefix.parent.glob(f"{index_prefix.name}.*.bt2"))


def _write_resolved_fastq_screen_conf(src: Path, dest: Path) -> None:
    """
    Write a copy of the FastQ Screen conf with relative DATABASE paths made absolute.

    Relative paths resolve to ``src.parent / path`` first; if no index is found there,
    ``Path.cwd() / path`` is used so configs that reference the working directory still work.
    """
    src = src.resolve()
    lines_out: list[str] = []
    for line in src.read_text(encoding="utf-8", errors="replace").splitlines():
        stripped = line.lstrip()
        if not stripped or stripped.startswith("#"):
            lines_out.append(line)
            continue
        parts = line.split()
        if len(parts) >= 3 and parts[0] == "DATABASE":
            raw = Path(parts[2])
            if not raw.is_absolute():
                cand_conf = (src.parent / raw).resolve()
                cand_cwd = (Path.cwd() / raw).resolve()
                if _bowtie2_index_exists(cand_conf):
                    resolved = cand_conf
                elif _bowtie2_index_exists(cand_cwd):
                    resolved = cand_cwd
                else:
                    resolved = cand_conf
                parts[2] = str(resolved)
            lines_out.append(" ".join(parts))
            continue
        lines_out.append(line)
    _ensure_dir(dest.parent)
    dest.write_text("\n".join(lines_out) + "\n", encoding="utf-8")


def _fastq_screen_report_path(out_dir: Path, fastq: Path) -> Path:
    """Match FastQ Screen's output naming (Perl outfile + _screen.txt suffix)."""
    stem = fastq.name
    if stem.endswith(".gz"):
        stem = stem[:-3]
    m = re.match(r"(?i)^(.*)\.(txt|seq|fastq|fq)$", stem)
    if m:
        stem = m.group(1)
    return out_dir / f"{stem}_screen.txt"


@task(tags=["contamination"])
def run_kraken2(
    sample: Sample,
    outdir: Path,
    threads: int,
    kraken_db: Path,
    events_file: str | None = None,
    run_name: str | None = None,
) -> dict:
    return _run_kraken2_impl(
        sample=sample,
        outdir=outdir,
        threads=threads,
        kraken_db=kraken_db,
        events_file=events_file,
        run_name=run_name,
    )


@task(tags=["contamination"])
def run_bracken(
    kraken_result: dict,
    outdir: Path,
    bracken_db: Path,
    read_length: int,
    level: str = "S",  # species level
    events_file: str | None = None,
    run_name: str | None = None,
) -> dict:
    return _run_bracken_impl(
        kraken_result=kraken_result,
        outdir=outdir,
        bracken_db=bracken_db,
        read_length=read_length,
        level=level,
        events_file=events_file,
        run_name=run_name,
    )


@task(tags=["contamination"])
def run_kraken_bracken(
    sample: Sample,
    outdir: Path,
    kraken_db: Path,
    bracken_db: Path,
    threads: int,
    read_length: int,
    events_file: str | None = None,
    run_name: str | None = None,
) -> dict:
    """Run Kraken2 then Bracken for a single sample."""
    kraken_res = _run_kraken2_impl(
        sample=sample,
        outdir=outdir,
        threads=threads,
        kraken_db=kraken_db,
        events_file=events_file,
        run_name=run_name,
    )
    bracken_res = _run_bracken_impl(
        kraken_result=kraken_res,
        outdir=outdir,
        bracken_db=bracken_db,
        read_length=read_length,
        events_file=events_file,
        run_name=run_name,
    )

    return {
        "sample": sample.name,
        "kraken": kraken_res,
        "bracken": bracken_res,
    }


def _run_kraken2_impl(
    *,
    sample: Sample,
    outdir: Path,
    threads: int,
    kraken_db: Path,
    events_file: str | None = None,
    run_name: str | None = None,
) -> dict:
    logger = get_run_logger()
    require_executable("kraken2")

    root = outdir / "contamination" / "kraken"
    sample_dir = root / sample.name
    _ensure_dir(sample_dir)

    report_path = sample_dir / f"{sample.name}.kraken.report"
    output_path = sample_dir / f"{sample.name}.kraken.out"

    cmd = [
        "kraken2",
        "--db",
        str(kraken_db),
        "--threads",
        str(threads),
        "--report",
        str(report_path),
        "--output",
        str(output_path),
        "--confidence",
        "0.1",
    ]

    if sample.paired:
        cmd.extend(["--paired", str(sample.r1), str(sample.r2)])
    else:
        cmd.append(str(sample.r1))

    logger.info("kraken2: %s", " ".join(cmd))
    run_command(
        cmd,
        capture_err_tail=80,
        events_file=events_file,
        run_name=run_name,
        step="contamination",
        tool="kraken2",
        sample=sample.name,
    )

    if not report_path.exists():
        raise RuntimeError(f"Missing Kraken report: {report_path}")

    if events_file:
        for p, kind in [(report_path, "report"), (output_path, "output")]:
            append_event(
                events_file,
                {
                    "type": "asset_created",
                    "run_name": run_name,
                    "step": "contamination",
                    "tool": "kraken2",
                    "kind": kind,
                    "path": str(p),
                    "sample": sample.name,
                },
            )

    return {
        "sample": sample.name,
        "report": str(report_path),
        "output": str(output_path),
    }


def _run_bracken_impl(
    *,
    kraken_result: dict,
    outdir: Path,
    bracken_db: Path,
    read_length: int,
    level: str = "S",
    events_file: str | None = None,
    run_name: str | None = None,
) -> dict:
    logger = get_run_logger()
    require_executable("bracken")

    sample_name = kraken_result["sample"]
    kraken_report = Path(kraken_result["report"])

    root = outdir / "contamination" / "bracken"
    sample_dir = root / sample_name
    _ensure_dir(sample_dir)

    output_path = sample_dir / f"{sample_name}.bracken.txt"
    report_path = sample_dir / f"{sample_name}.bracken.report"

    kmer_distrib = bracken_db / f"database{read_length}mers.kmer_distrib"
    if not kraken_report.is_file():
        raise RuntimeError(
            f"Kraken report for Bracken input is missing: {kraken_report}. "
            "Run Kraken2 first (--contamination-tool kraken) or place reports under "
            f"{kraken_report.parent} before using kraken_bracken."
        )
    if not kmer_distrib.is_file():
        raise RuntimeError(
            f"Bracken k-mer distribution file missing: {kmer_distrib}. "
            f"Run `bracken-build -d {bracken_db} -t <threads> -k <kmer> -l {read_length}` "
            "so the read length matches the pipeline's --read-length "
            "(same Kraken DB path as -d / --kraken-db)."
        )

    cmd = [
        "bracken",
        "-d",
        str(bracken_db),
        "-i",
        str(kraken_report),
        "-o",
        str(output_path),
        "-w",
        str(report_path),
        "-r",
        str(read_length),
        "-l",
        level,
    ]

    logger.info("bracken: %s", " ".join(cmd))
    run_command(
        cmd,
        capture_err_tail=80,
        events_file=events_file,
        run_name=run_name,
        step="contamination",
        tool="bracken",
        sample=sample_name,
    )

    if not output_path.exists():
        raise RuntimeError(
            f"Missing Bracken output: {output_path}. "
            "The packaged `bracken` script often exits 0 even when it prints an error "
            "(missing report, wrong -r read length vs bracken-build, etc.); check Prefect logs "
            "above for Bracken messages."
        )

    if events_file:
        for p, kind in [(output_path, "output"), (report_path, "report")]:
            append_event(
                events_file,
                {
                    "type": "asset_created",
                    "run_name": run_name,
                    "step": "contamination",
                    "tool": "bracken",
                    "kind": kind,
                    "path": str(p),
                    "sample": sample_name,
                },
            )

    return {
        "sample": sample_name,
        "bracken": str(output_path),
        "report": str(report_path),
    }


@task(tags=["contamination"])
def run_fastq_screen(
    sample: Sample,
    outdir: Path,
    threads: int,
    fastq_screen_conf: Path,
    events_file: str | None = None,
    run_name: str | None = None,
) -> dict:
    logger = get_run_logger()
    require_executable("fastq_screen")

    root = outdir / "contamination" / "fastq_screen"
    sample_dir = root / sample.name
    _ensure_dir(sample_dir)

    resolved_conf = sample_dir / "resolved_fastq_screen.conf"
    _write_resolved_fastq_screen_conf(Path(fastq_screen_conf), resolved_conf)

    inputs = [str(sample.r1)]
    if sample.r2:
        inputs.append(str(sample.r2))

    cmd = [
        "fastq_screen",
        "-conf",
        str(resolved_conf),
        "--outdir",
        str(sample_dir),
        "--threads",
        str(threads),
        "--force",
        *inputs,
    ]

    logger.info("fastq_screen: %s", " ".join(cmd))
    run_command(
        cmd,
        capture_err_tail=80,
        events_file=events_file,
        run_name=run_name,
        step="contamination",
        tool="fastq_screen",
        sample=sample.name,
    )

    reports = [_fastq_screen_report_path(sample_dir, Path(f)) for f in inputs]
    missing = [p for p in reports if not p.exists()]
    if missing:
        raise RuntimeError(
            "Missing FastQ Screen output(s): "
            + ", ".join(str(p) for p in missing)
        )

    if events_file:
        append_event(
            events_file,
            {
                "type": "asset_created",
                "run_name": run_name,
                "step": "contamination",
                "tool": "fastq_screen",
                "kind": "config",
                "path": str(resolved_conf),
                "sample": sample.name,
            },
        )
        for p in reports:
            append_event(
                events_file,
                {
                    "type": "asset_created",
                    "run_name": run_name,
                    "step": "contamination",
                    "tool": "fastq_screen",
                    "kind": "report",
                    "path": str(p),
                    "sample": sample.name,
                },
            )

    return {
        "sample": sample.name,
        "screen": str(reports[0]),
        "screens": [str(p) for p in reports],
    }


def _resolve_kraken_bracken_dbs(
    kraken_db: Path | None, bracken_db: Path | None
) -> tuple[Path, Path]:
    if kraken_db is not None:
        kraken_path = Path(kraken_db)
        bracken_path = Path(bracken_db) if bracken_db is not None else kraken_path
        return kraken_path, bracken_path
    if bracken_db is not None:
        bracken_path = Path(bracken_db)
        return bracken_path, bracken_path
    raise SystemExit(
        "kraken_bracken requires kraken_db and/or bracken_db "
        "(same Kraken2 directory after bracken-build)."
    )


def _submit_kraken(
    *,
    samples: list[Sample],
    outdir: Path,
    per_task_threads: int,
    kraken_db: Path | None,
    bracken_db: Path | None,
    fastq_screen_conf: Path | None,
    read_length: int,
    events_file: str | None,
    run_name: str | None,
) -> PrefectFutureList:
    _ = bracken_db, fastq_screen_conf, read_length
    if not kraken_db:
        raise SystemExit("Kraken requires kraken_db")
    n = len(samples)
    return run_kraken2.map(
        sample=samples,
        outdir=[outdir] * n,
        threads=[per_task_threads] * n,
        kraken_db=[Path(kraken_db)] * n,
        events_file=[events_file] * n,
        run_name=[run_name] * n,
    )


def _submit_kraken_bracken(
    *,
    samples: list[Sample],
    outdir: Path,
    per_task_threads: int,
    kraken_db: Path | None,
    bracken_db: Path | None,
    fastq_screen_conf: Path | None,
    read_length: int,
    events_file: str | None,
    run_name: str | None,
) -> PrefectFutureList:
    _ = fastq_screen_conf
    kraken_path, bracken_path = _resolve_kraken_bracken_dbs(kraken_db, bracken_db)
    n = len(samples)
    return run_kraken_bracken.map(
        sample=samples,
        outdir=[outdir] * n,
        threads=[per_task_threads] * n,
        kraken_db=[kraken_path] * n,
        bracken_db=[bracken_path] * n,
        read_length=[read_length] * n,
        events_file=[events_file] * n,
        run_name=[run_name] * n,
    )


def _submit_fastq_screen(
    *,
    samples: list[Sample],
    outdir: Path,
    per_task_threads: int,
    kraken_db: Path | None,
    bracken_db: Path | None,
    fastq_screen_conf: Path | None,
    read_length: int,
    events_file: str | None,
    run_name: str | None,
) -> PrefectFutureList:
    _ = kraken_db, bracken_db, read_length
    if not fastq_screen_conf:
        raise SystemExit("fastq_screen requires config file")
    n = len(samples)
    return run_fastq_screen.map(
        sample=samples,
        outdir=[outdir] * n,
        threads=[per_task_threads] * n,
        fastq_screen_conf=[Path(fastq_screen_conf)] * n,
        events_file=[events_file] * n,
        run_name=[run_name] * n,
    )


CONTAM_TOOL_REGISTRY: dict[str, Any] = {
    "kraken": _submit_kraken,
    "kraken_bracken": _submit_kraken_bracken,
    "fastq_screen": _submit_fastq_screen,
}


def submit_contamination_tasks(
    samples: list[Sample],
    contamination_tool: Literal["kraken", "kraken_bracken", "fastq_screen"],
    outdir: Path,
    per_task_threads: int,
    *,
    events_file: str | None = None,
    run_name: str | None = None,
    kraken_db: Path | None = None,
    bracken_db: Path | None = None,
    fastq_screen_conf: Path | None = None,
    read_length: int = 150,
) -> PrefectFutureList:
    """Submit mapped contamination tasks for all samples."""
    tool = contamination_tool.lower().strip()
    submitter = CONTAM_TOOL_REGISTRY.get(tool)
    if submitter is None:
        raise SystemExit(f"Unknown contamination tool: {contamination_tool}")
    return submitter(
        samples=samples,
        outdir=outdir,
        per_task_threads=per_task_threads,
        kraken_db=kraken_db,
        bracken_db=bracken_db,
        fastq_screen_conf=fastq_screen_conf,
        read_length=read_length,
        events_file=events_file,
        run_name=run_name,
    )


@flow(name="contamination_submit", log_prints=True)
def _contamination_submit_flow(
    samples: list[Sample],
    contamination_tool: Literal["kraken", "kraken_bracken", "fastq_screen"],
    outdir: Path,
    per_task_threads: int,
    *,
    events_file: str | None = None,
    run_name: str | None = None,
    kraken_db: Path | None = None,
    bracken_db: Path | None = None,
    fastq_screen_conf: Path | None = None,
    read_length: int = 150,
) -> PrefectFutureList:
    return submit_contamination_tasks(
        samples=samples,
        contamination_tool=contamination_tool,
        outdir=outdir,
        per_task_threads=per_task_threads,
        events_file=events_file,
        run_name=run_name,
        kraken_db=kraken_db,
        bracken_db=bracken_db,
        fastq_screen_conf=fastq_screen_conf,
        read_length=read_length,
    )


@flow(name="contamination_phase", log_prints=True)
def contamination_phase(
    samples: list[Sample],
    contamination_tool: Literal["kraken", "kraken_bracken", "fastq_screen"],
    outdir: Path,
    thread_budget: int,
    *,
    events_file: str | None = None,
    run_name: str | None = None,
    kraken_db: Path | None = None,
    bracken_db: Path | None = None,
    fastq_screen_conf: Path | None = None,
    read_length: int = 150,
) -> PrefectFutureList:
    """
    Run contamination analysis in parallel under a thread budget.

    Returns a `PrefectFutureList` of mapped tasks so callers can await
    completion by calling `.result()` on the returned object.
    """
    logger = get_run_logger()
    max_workers, per_task_threads = _allocate_sample_parallelism(
        thread_budget, len(samples)
    )
    logger.info(
        "Contamination phase: max concurrent samples=%s, per-sample tool threads=%s (budget=%s)",
        max_workers,
        per_task_threads,
        thread_budget,
    )
    runner = ThreadPoolTaskRunner(max_workers=max_workers)
    return _contamination_submit_flow.with_options(task_runner=cast(Any, runner))(
        samples=samples,
        contamination_tool=contamination_tool,
        outdir=outdir,
        per_task_threads=per_task_threads,
        events_file=events_file,
        run_name=run_name,
        kraken_db=kraken_db,
        bracken_db=bracken_db,
        fastq_screen_conf=fastq_screen_conf,
        read_length=read_length,
    )
