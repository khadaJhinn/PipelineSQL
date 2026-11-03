import hashlib
import json
import sqlite3
from dataclasses import replace
from datetime import date

import httpx
import pandas as pd
import pytest

from scripts.config import Settings
from scripts.ingest.rte import ingest
from scripts.load.sqlite import load_processed
from scripts.transform.clean import read_archive, transform_archive


async def test_end_to_end_and_raw_is_never_modified(tmp_path):
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
    settings = replace(Settings(), data_dir=tmp_path, database=tmp_path / "test.sqlite")
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(200, json={"total_count": 2, "results": rows})
        )
    ) as client:
        raw = await ingest(settings, date(2026, 10, 1), date(2026, 10, 2), client=client)
    before = {file.name: hashlib.sha256(file.read_bytes()).hexdigest() for file in raw.iterdir()}
    processed = transform_archive(raw, tmp_path / "processed")
    excel = load_processed(processed, settings.database)
    after = {file.name: hashlib.sha256(file.read_bytes()).hexdigest() for file in raw.iterdir()}
    assert before == after
    assert len(pd.read_excel(raw / "brut.xlsx")) == 2
    assert len(pd.read_parquet(processed / "consommation.parquet")) == 1
    assert len(pd.read_excel(excel)) == 1
    with sqlite3.connect(settings.database) as connection:
        assert connection.execute(
            "SELECT consommation_mw FROM electricity_consumption"
        ).fetchone() == (40000,)
    quality = json.loads((processed / "quality.json").read_text())
    assert quality["unmeasured_rows"] == 1
    page = next(raw.glob("2026*.json"))
    page.write_bytes(b"{}")
    with pytest.raises(ValueError, match="modifiée"):
        read_archive(raw)
