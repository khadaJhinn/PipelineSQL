import hashlib
import json
from datetime import UTC, datetime, timedelta

import pandas as pd
import pyarrow.parquet as pq
import pytest

from scripts.transform.clean import transform_archive


def archive(tmp_path, rows):
    raw = tmp_path / "raw" / "snapshot"
    raw.mkdir(parents=True)
    content = "".join(json.dumps(row) + "\n" for row in rows).encode()
    (raw / "data.jsonl").write_bytes(content)
    manifest = {
        "status": "complete",
        "dataset": "test",
        "raw_rows": len(rows),
        "start": "2026-10-01",
        "end_exclusive": "2026-11-01",
        "retrieved_at": "2026-11-02T00:00:00+00:00",
        "pages": [
            {"file": "data.jsonl", "rows": len(rows), "sha256": hashlib.sha256(content).hexdigest()}
        ],
    }
    (raw / "manifest.json").write_text(json.dumps(manifest))
    return raw


def rows(count):
    return [
        {
            "perimetre": "France",
            "consommation": 42000 + i,
            "date_heure": (
                datetime(2026, 10, 1, tzinfo=UTC) + timedelta(minutes=15 * i)
            ).isoformat(),
        }
        for i in range(count)
    ]


def test_parquet_written_by_batches_with_global_dedup_and_null_schema(tmp_path):
    values = rows(51)
    values[0]["nucleaire"] = None
    values[-1]["nucleaire"] = 30000
    raw = archive(tmp_path, [*reversed(values), values[0]])
    output = transform_archive(raw, tmp_path / "processed", batch_size=7)
    quality = json.loads((output / "quality.json").read_text())
    assert quality["max_batch_rows"] == 7
    assert quality["clean_rows"] == 51
    assert quality["duplicate_rows"] == 1
    parquet = pq.ParquetFile(output / "consommation.parquet")
    assert parquet.num_row_groups == 8
    assert all(parquet.metadata.row_group(i).num_rows <= 7 for i in range(8))
    frame = pd.read_parquet(output / "consommation.parquet")
    assert frame.date_heure_utc.is_monotonic_increasing
    assert frame.nucleaire_mw.iloc[-1] == 30000
    assert frame.nucleaire_mw.iloc[:50].isna().all()
    assert transform_archive(raw, tmp_path / "processed", 7) == output


def test_conflicting_values_across_batches_fail_without_completed_output(tmp_path):
    values = rows(3)
    raw = archive(tmp_path, [*values, {**values[0], "consommation": 1}])
    with pytest.raises(ValueError, match="contradictoires"):
        transform_archive(raw, tmp_path / "processed", batch_size=2)
    assert not (tmp_path / "processed/snapshot/quality.json").exists()
    assert not (tmp_path / "processed/snapshot/.dedup.sqlite").exists()


def test_batch_of_only_forecasts_is_skipped(tmp_path):
    values = [{**row, "consommation": None} for row in rows(2)] + rows(2)
    raw = archive(tmp_path, values)
    output = transform_archive(raw, tmp_path / "processed", batch_size=2)
    quality = json.loads((output / "quality.json").read_text())
    assert quality["unmeasured_rows"] == 2
    assert quality["clean_rows"] == 2


def test_tampered_parquet_is_rejected_on_replay(tmp_path):
    raw = archive(tmp_path, rows(2))
    output = transform_archive(raw, tmp_path / "processed", batch_size=1)
    (output / "consommation.parquet").write_bytes(b"changed")
    with pytest.raises(ValueError, match="Parquet"):
        transform_archive(raw, tmp_path / "processed", 1)


def test_negative_zero_and_zero_are_identical_across_batches(tmp_path):
    value = rows(1)[0]
    raw = archive(tmp_path, [{**value, "solaire": -0.0}, {**value, "solaire": 0}])
    output = transform_archive(raw, tmp_path / "processed", batch_size=1)
    quality = json.loads((output / "quality.json").read_text())
    assert quality["clean_rows"] == 1
    assert quality["duplicate_rows"] == 1
