from dashboard.pipeline_runner import run_pipeline


def test_run_pipeline_truncates_output(tmp_path, monkeypatch):
    scripts_dir = tmp_path / "scripts"
    scripts_dir.mkdir(parents=True)
    pipeline_py = scripts_dir / "pipeline.py"
    # Write a noisy script that would exceed the cap if unbounded.
    pipeline_py.write_text(
        "import sys\nfor i in range(5000):\n    print('line-' + str(i))\n", encoding="utf-8"
    )

    # Ensure the subprocess uses the same interpreter
    monkeypatch.setenv("PYTHONPATH", "")
    result = run_pipeline(tmp_path)

    # Should succeed, report truncation, and keep recent output only.
    assert result.returncode == 0
    assert "(truncated output)" in result.output
    assert "line-4999" in result.output
