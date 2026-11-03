import json

import duckdb
import pandas as pd
import pytest

from scripts.load.database import load_frame, load_processed
from scripts.transform.clean import transform_records


def make_frame(consumption=42000):
    return transform_records(
        [
            {
                "date_heure": "2026-10-01T12:00:00+00:00",
                "perimetre": "France",
                "consommation": consumption,
            }
        ],
        "test",
    )[0]


def make_archive(tmp_path, stamp="2026-10-02T00:00:00+00:00"):
    archive = tmp_path / stamp.replace(":", "")
    archive.mkdir()
    (archive / "manifest.json").write_text(
        json.dumps({"status": "complete", "retrieved_at": stamp, "raw_rows": 1})
    )
    return archive


def test_upsert_replay_and_transaction_rollback(tmp_path):
    database = tmp_path / "test.duckdb"
    archive = make_archive(tmp_path)
    frame = make_frame()
    load_frame(frame, database, "run1", archive)
    load_frame(frame, database, "run1", archive)
    revised = make_frame(43000)
    assert load_frame(revised, database, "run2", archive).iloc[0]["consommation_mw"] == 43000
    invalid = make_frame()
    invalid.loc[0, "consommation_mw"] = -1
    with pytest.raises(duckdb.ConstraintException):
        load_frame(invalid, database, "failed", archive)
    with duckdb.connect(str(database)) as connection:
        assert connection.execute("SELECT COUNT(*) FROM electricity_consumption").fetchone() == (1,)
        assert connection.execute(
            "SELECT consommation_mw FROM electricity_consumption"
        ).fetchone() == (43000,)
        assert connection.execute(
            "SELECT COUNT(*) FROM pipeline_runs WHERE run_id='failed'"
        ).fetchone() == (0,)


def test_old_archive_cannot_overwrite_recent_revision(tmp_path):
    database = tmp_path / "test.duckdb"
    old = make_archive(tmp_path)
    recent = make_archive(tmp_path, "2026-10-03T00:00:00+00:00")
    load_frame(make_frame(45000), database, "new", recent)
    exported = load_frame(make_frame(42000), database, "old", old)
    assert exported.iloc[0]["consommation_mw"] == 45000


def test_excel_is_read_from_sql_and_loading_can_be_repeated(tmp_path):
    archive = make_archive(tmp_path)
    processed = tmp_path / "processed"
    processed.mkdir()
    (processed / "quality.json").write_text(json.dumps({"source_run": str(archive)}))
    frame = make_frame()
    frame.to_parquet(processed / "consommation.parquet", index=False)
    database = tmp_path / "test.duckdb"
    excel = load_processed(processed, database)
    load_processed(processed, database)
    actual = pd.read_excel(excel)
    assert len(actual) == 1
    assert actual.iloc[0]["consommation_mw"] == 42000
    assert actual.iloc[0]["date_heure_utc"] == frame.iloc[0]["date_heure_utc"]


def test_loading_parquet_is_batched_and_does_not_use_pandas_read_parquet(tmp_path, monkeypatch):
    archive = make_archive(tmp_path)
    processed = tmp_path / "processed"
    processed.mkdir()
    frame = pd.concat([make_frame()] * 25, ignore_index=True)
    frame["date_heure_utc"] = pd.date_range("2026-10-01", periods=25, freq="15min", tz="UTC").map(
        lambda stamp: stamp.isoformat()
    )
    frame.to_parquet(processed / "consommation.parquet", index=False)
    (processed / "quality.json").write_text(json.dumps({"source_run": str(archive)}))

    def forbidden(*args, **kwargs):
        raise AssertionError("Lecture complète du Parquet interdite")

    monkeypatch.setattr(pd, "read_parquet", forbidden)
    excel = load_processed(processed, tmp_path / "test.duckdb", batch_size=3)
    assert len(pd.read_excel(excel)) == 25
    report = json.loads((processed / "load_report.json").read_text())
    assert report["batch_size"] == 3
    assert report["sql_rows_exported"] == 25


def test_failure_in_later_batch_rolls_back_all_previous_batches(tmp_path):
    archive = make_archive(tmp_path)
    processed = tmp_path / "processed"
    processed.mkdir()
    frame = pd.concat([make_frame(), make_frame()], ignore_index=True)
    frame.loc[1, "date_heure_utc"] = "2026-10-01T12:15:00+00:00"
    frame.loc[1, "consommation_mw"] = -1
    frame.to_parquet(processed / "consommation.parquet", index=False)
    (processed / "quality.json").write_text(json.dumps({"source_run": str(archive)}))
    database = tmp_path / "test.duckdb"
    with pytest.raises(duckdb.ConstraintException):
        load_processed(processed, database, batch_size=1)
    with duckdb.connect(str(database)) as connection:
        assert connection.execute("SELECT COUNT(*) FROM electricity_consumption").fetchone() == (0,)
        assert connection.execute("SELECT COUNT(*) FROM pipeline_runs").fetchone() == (0,)
