@pytest.mark.parametrize(
    ("platform", "reads"),
    [
        ("illumina", ("R1", "R2")),
        ("illumina", ("R1", "R2", "I1", "I2")),
        ("aviti", ("R1", "R2")),
        ("aviti", ("R1", "R2", "I1", "I2")),
    ],
)
@pytest.mark.parametrize("qc_tool", ["fastqc", "fastp", "falco"])

def test_post_demux_qc_receives_all_reads(
    tmp_path: Path,
    platform: str,
    reads: tuple[str, ...],
    qc_tool: str,
) -> None:
    """
    Full chain:

        demux executable
            ↓
        platform-specific post-processing
            ↓
        _discover_samples()
            ↓
        submit_qc_tasks()
            ↓
        QC tool

    The external demux and QC executable are mocked, but the pipeline
    plumbing and post-processing are real.
    """
    from unittest.mock import patch

    from demux_pipeline.qc import submit_qc_tasks

    input_dir = tmp_path / "input"
    outdir = tmp_path / "output"
    samplesheet = tmp_path / "SampleSheet.csv"

    create_input_run(input_dir)
    create_samplesheet(samplesheet)

    if platform == "illumina":

        def fake_demux_command(cmd, **kwargs):
            create_illumina_demux_output(
                outdir / "output",
                reads,
            )

    else:

        def fake_demux_command(cmd, **kwargs):
            create_aviti_native_output(
                outdir / ".demux_native" / "bases2fastq",
                reads,
            )

    with patch(
        "demux_pipeline.demux.run_command",
        side_effect=fake_demux_command,
    ), patch(
        "demux_pipeline.demux._resolve_bcl_convert_binary",
        return_value="fake-bcl-convert",
    ), patch(
        "demux_pipeline.demux._resolve_bases2fastq_binary",
        return_value="fake-bases2fastq",
    ):
        demux_bcl(
            input_dir=input_dir,
            samplesheet=samplesheet,
            outdir=outdir,
            platform=platform,
        )

    # IMPORTANT:
    # Discover from the actual post-processed output.
    samples = _discover_samples(outdir / "output")

    assert len(samples) == 1

    sample = samples[0]

    assert {
        path.name
        for path in sample.get_paths()
    } == {
        f"SampleA_S1_{read}_001.fastq.gz"
        for read in reads
    }

    # ---------------------------------------------------------------
    # Mock only the external QC executable.
    # The real submit_qc_tasks() and QC task are exercised.
    # ---------------------------------------------------------------

    def fake_qc_command(cmd, **kwargs):
        tool = kwargs.get("tool")

        if tool == "fastqc":
            outdir_index = cmd.index("--outdir") + 1
            qc_dir = Path(cmd[outdir_index])
            input_fastq = Path(cmd[-1])

            qc_dir.mkdir(parents=True, exist_ok=True)

            stem = input_fastq.name.removesuffix(".fastq.gz")
            (qc_dir / f"{stem}_fastqc.html").write_text(
                "<html>fake FastQC report</html>",
                encoding="utf-8",
            )

        elif tool == "falco":
            outdir_index = cmd.index("--outdir") + 1
            qc_dir = Path(cmd[outdir_index])

            qc_dir.mkdir(parents=True, exist_ok=True)

            (qc_dir / "fastqc_report.html").write_text(
                "<html>fake Falco report</html>",
                encoding="utf-8",
            )

        elif tool == "fastp":
            html_index = cmd.index("--html") + 1
            json_index = cmd.index("--json") + 1

            html_path = Path(cmd[html_index])
            json_path = Path(cmd[json_index])

            html_path.parent.mkdir(parents=True, exist_ok=True)

            html_path.write_text(
                "<html>fake fastp report</html>",
                encoding="utf-8",
            )

            json_path.write_text(
                "{}\n",
                encoding="utf-8",
            )

            # Create all output FASTQs requested by fastp.
            if "-O" in cmd:
                out_r1 = Path(cmd[cmd.index("-o") + 1])
                out_r2 = Path(cmd[cmd.index("-O") + 1])

                out_r1.parent.mkdir(parents=True, exist_ok=True)
                write_fastq_gz(out_r1, "R1")
                write_fastq_gz(out_r2, "R2")

            else:
                out_r1 = Path(cmd[cmd.index("-o") + 1])

                out_r1.parent.mkdir(parents=True, exist_ok=True)

                input_path = Path(cmd[cmd.index("-i") + 1])

                read = next(
                    read
                    for read in ("R1", "R2", "I1", "I2")
                    if f"_{read}_" in input_path.name
                )

                write_fastq_gz(out_r1, read)

        else:
            raise AssertionError(f"Unexpected QC tool: {tool}")

    with patch(
        "demux_pipeline.qc.run_command",
        side_effect=fake_qc_command,
    ):
        futures = submit_qc_tasks(
            [sample],
            qc_tool,
            outdir,
            per_task_threads=1,
        )

        futures.result()

    # ---------------------------------------------------------------
    # Verify that every read reached the QC implementation.
    # ---------------------------------------------------------------

    if qc_tool == "fastqc":
        qc_dir = outdir / "fastqc" / "Project1" / "SampleA"

        assert qc_dir.exists()

        for read in reads:
            report = (
                qc_dir
                / f"SampleA_S1_{read}_001_fastqc.html"
            )

            assert report.is_file(), (
                f"{platform}/{qc_tool}: "
                f"{read} did not reach FastQC"
            )

    elif qc_tool == "falco":
        qc_root = outdir / "falco" / "Project1"

        for read in reads:
            read_dir = qc_root / f"SampleA_{read}"

            assert read_dir.is_dir(), (
                f"{platform}/{qc_tool}: "
                f"{read} did not reach Falco"
            )

    elif qc_tool == "fastp":
        qc_dir = outdir / "fastp" / "Project1"

        assert (qc_dir / "SampleA.html").is_file()
        assert (qc_dir / "SampleA.json").is_file()

        # Once fastp is extended to process additional_reads, these
        # reports prove I1/I2 reached fastp as well.
        if "I1" in reads:
            assert (qc_dir / "SampleA_I1.html").is_file()
            assert (qc_dir / "SampleA_I1.json").is_file()

        if "I2" in reads:
            assert (qc_dir / "SampleA_I2.html").is_file()
            assert (qc_dir / "SampleA_I2.json").is_file()