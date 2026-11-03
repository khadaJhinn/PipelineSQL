import asyncio
import hashlib
import json
import os
import subprocess
import sys
from datetime import date
from pathlib import Path

import duckdb
import pandas as pd
import pytest

from scripts.archive import iter_records
from scripts.ingest.rte import ingest
from scripts.load.database import load_processed
from scripts.transform.clean import transform_archive


def test_cli_help_works_with_windows_console_encoding():
    result = subprocess.run(
        [sys.executable, "app.py", "--help"],
        env={**os.environ, "PYTHONIOENCODING": "cp1252"},
        capture_output=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr.decode("cp1252")
    assert "Pipeline RTE" in result.stdout.decode("cp1252")


async def test_end_to_end_and_raw_is_never_modified(tmp_path, api):
    rows = [
        {
            "perimetre": "France",
            "date_heure": "2026-10-01T00:00:00+00:00",
            "consommation": 40000,
        },
        {
            "perimetre": "France",
            "date_heure": "2026-10-01T00:15:00+00:00",
            "consommation": None,
            "prevision_j1": 41000,
        },
    ]
    settings = api.settings
    api.respond({"total_count": 2, "results": rows})
    raw = await ingest(settings, date(2026, 10, 1), date(2026, 10, 2))
    before = {file.name: hashlib.sha256(file.read_bytes()).hexdigest() for file in raw.iterdir()}
    processed = transform_archive(raw, tmp_path / "processed")
    excel = load_processed(processed, settings.database)
    after = {file.name: hashlib.sha256(file.read_bytes()).hexdigest() for file in raw.iterdir()}
    assert before == after
    assert len(pd.read_excel(raw / "brut.xlsx")) == 2
    assert len(pd.read_parquet(processed / "consommation.parquet")) == 1
    assert len(pd.read_excel(excel)) == 1
    with duckdb.connect(str(settings.database)) as connection:
        assert connection.execute(
            "SELECT consommation_mw FROM electricity_consumption"
        ).fetchone() == (40000,)
    quality = json.loads((processed / "quality.json").read_text())
    assert quality["unmeasured_rows"] == 1
    page = next(raw.glob("2026*.jsonl"))
    page.write_bytes(b"{}")
    with pytest.raises(ValueError, match="modifiée"):
        list(iter_records(raw))


async def test_actual_cli_run_uses_configured_data_directory_and_produces_reports(api, tmp_path):
    api.respond(
        {
            "total_count": 1,
            "results": [
                {
                    "perimetre": "France",
                    "date_heure": "2026-10-01T00:00:00+00:00",
                    "consommation": 40000,
                }
            ],
        }
    )
    environment = {
        **os.environ,
        "PYTHONIOENCODING": "utf-8",
        "DATA_DIR": str(tmp_path / "cli"),
        "DATABASE_PATH": str(tmp_path / "cli/test.duckdb"),
        "RTE_API_BASE": api.settings.api_base,
    }
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "app.py",
        "run",
        "--start",
        "2026-10-01",
        "--end",
        "2026-10-02",
        "--batch-size",
        "1",
        env=environment,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await process.communicate()
    assert process.returncode == 0, stderr.decode("utf-8")
    result = json.loads(stdout)
    report = json.loads(Path(result["pipeline_report"]).read_text(encoding="utf-8"))
    assert report["clean_rows"] == 1
    assert report["transformation_reused"] is False
    assert report["parquet_batch_size"] == 1
    assert Path(result["database"]).is_relative_to(tmp_path)
    analysis = json.loads(Path(result["analysis_report"]).read_text(encoding="utf-8"))
    assert len(analysis["queries"]) == 7
