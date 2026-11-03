import csv
import json

import pytest

from scripts.analyze import analyze, read_queries
from scripts.load.database import load_frame
from scripts.transform.clean import transform_records


def test_all_seven_analyses_and_real_explain_analyze(tmp_path):
    archive = tmp_path / "archive"
    archive.mkdir()
    (archive / "manifest.json").write_text(
        json.dumps(
            {"status": "complete", "raw_rows": 4, "retrieved_at": "2026-11-02T00:00:00+00:00"}
        )
    )
    records = [
        {
            "perimetre": "France",
            "date_heure": f"2026-10-0{day}T{hour}:00:00+00:00",
            "consommation": value,
        }
        for day, hour, value in [
            (1, "00", 40000),
            (1, "01", 50000),
            (2, "00", 45000),
            (2, "01", 55000),
        ]
    ]
    database = tmp_path / "test.duckdb"
    frame, _ = transform_records(records, "test")
    load_frame(frame, database, "run", archive)
    report_path = analyze(database, tmp_path / "reports", batch_size=2)
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert len(read_queries()) == 7
    assert len(report["queries"]) == 7
    assert report["database_rows"] == 4
    benchmark = report["index_benchmark"]
    assert benchmark["before"]["rows"] == benchmark["after"]["rows"] == 1
    assert benchmark["before"]["median_ms"] > 0
    assert "Total Time" in benchmark["after"]["plan"]
    with (report_path.parent / "statistiques_journalieres.csv").open(
        encoding="utf-8-sig", newline=""
    ) as source:
        daily = list(csv.DictReader(source, delimiter=";"))
    assert [float(row["consommation_moyenne_mw"]) for row in daily] == [45000, 50000]
    # Une relance garde les mesures et réévalue le même index.
    analyze(database, report_path.parent, 2)


def test_analyze_does_not_create_an_empty_database(tmp_path):
    database = tmp_path / "missing.duckdb"
    with pytest.raises(FileNotFoundError):
        analyze(database, tmp_path / "reports")
    assert not database.exists()
