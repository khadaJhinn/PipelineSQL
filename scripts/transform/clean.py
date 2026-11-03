"""Mesures nationales validées, unités explicites, UTC et heure française."""

import json
import math
import sqlite3
from contextlib import closing
from datetime import UTC, datetime
from itertools import islice
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from scripts.archive import file_hash, iter_records, read_manifest
from scripts.metrics import measure

METRICS = {
    "consommation": "consommation_mw",
    "prevision_j1": "prevision_j1_mw",
    "prevision_j": "prevision_j_mw",
    "fioul": "fioul_mw",
    "charbon": "charbon_mw",
    "gaz": "gaz_mw",
    "nucleaire": "nucleaire_mw",
    "eolien": "eolien_mw",
    "solaire": "solaire_mw",
    "hydraulique": "hydraulique_mw",
    "bioenergies": "bioenergies_mw",
    "pompage": "pompage_mw",
    "ech_physiques": "echanges_physiques_mw",
    "taux_co2": "taux_co2_g_kwh",
}
PRODUCTION = [
    "fioul_mw",
    "charbon_mw",
    "gaz_mw",
    "nucleaire_mw",
    "eolien_mw",
    "solaire_mw",
    "hydraulique_mw",
    "bioenergies_mw",
]
COLUMNS = [
    "date_heure_utc",
    "date_heure_paris",
    "perimetre",
    "nature",
    *METRICS.values(),
    "production_totale_mw",
    "source_dataset",
]


def number(value, field: str) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        raise ValueError(f"Valeur booléenne interdite : {field}.")
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Valeur numérique invalide pour {field} : {value!r}.") from exc
    if not math.isfinite(result):
        raise ValueError(f"Valeur non finie pour {field}.")
    return result


def transform_records(
    records: list[dict], dataset: str, allow_empty: bool = False
) -> tuple[pd.DataFrame, dict]:
    cleaned = []
    skipped = 0
    for record in records:
        if record.get("perimetre") != "France":
            raise ValueError("Le jeu de données doit contenir uniquement le périmètre France.")
        # Les prévisions sans télémesure sont conservées dans l'archive brute.
        if record.get("consommation") is None:
            skipped += 1
            continue
        try:
            stamp = datetime.fromisoformat(record["date_heure"])
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError("Horodatage de mesure invalide.") from exc
        if stamp.tzinfo is None:
            raise ValueError("Un fuseau horaire est obligatoire.")
        stamp = stamp.astimezone(UTC)
        row = {
            "date_heure_utc": stamp.isoformat(),
            "date_heure_paris": stamp.astimezone(ZoneInfo("Europe/Paris")).isoformat(),
            "perimetre": "France",
            "nature": record.get("nature") or "Données temps réel",
            "source_dataset": dataset,
            **{target: number(record.get(source), source) for source, target in METRICS.items()},
        }
        if row["consommation_mw"] < 0:
            raise ValueError("Une consommation négative est invalide.")
        # Les totaux RTE incluent leurs sous-filières : ne jamais les additionner deux fois.
        values = [row[field] for field in PRODUCTION]
        row["production_totale_mw"] = (
            sum(values) if all(value is not None for value in values) else None
        )
        cleaned.append(row)
    if not cleaned and not allow_empty:
        raise ValueError("Aucune consommation mesurée exploitable dans cette archive.")
    frame = pd.DataFrame(cleaned, columns=COLUMNS)
    keys = ["date_heure_utc", "perimetre"]
    # Des doublons strictement identiques sont sûrs ; un conflit doit rester visible.
    unique = frame.drop_duplicates()
    if unique.duplicated(keys).any():
        raise ValueError("Mesures contradictoires pour un même horodatage.")
    frame = unique.sort_values(keys).reset_index(drop=True)
    for column in [*METRICS.values(), "production_totale_mw"]:
        frame[column] = frame[column].astype("float64")
    return frame, {
        "raw_rows": len(records),
        "unmeasured_rows": skipped,
        "duplicate_rows": len(cleaned) - len(frame),
        "clean_rows": len(frame),
    }


def record_batches(records, batch_size: int):
    if batch_size < 1:
        raise ValueError("La taille de lot doit être positive.")
    iterator = iter(records)
    while batch := list(islice(iterator, batch_size)):
        yield batch


def transform_archive(run_dir: Path, processed_root: Path, batch_size: int = 1000) -> Path:
    """Nettoie par lots, déduplique sur disque puis écrit des row groups Parquet."""
    if batch_size < 1:
        raise ValueError("La taille de lot doit être positive.")
    manifest = read_manifest(run_dir)
    manifest_sha = file_hash(run_dir / "manifest.json")
    output = processed_root / run_dir.name
    output.mkdir(parents=True, exist_ok=True)
    parquet = output / "consommation.parquet"
    report_path = output / "quality.json"
    if report_path.exists():
        previous = json.loads(report_path.read_text(encoding="utf-8"))
        if previous.get("source_manifest_sha256") != manifest_sha:
            raise ValueError("Le dossier transformé existe pour une autre version de source.")
        if previous["parquet_sha256"] != file_hash(parquet):
            raise ValueError("Le fichier Parquet a été modifié.")
        for _ in iter_records(run_dir, manifest):
            pass
        return output
    report = {
        "raw_rows": 0,
        "unmeasured_rows": 0,
        "duplicate_rows": 0,
        "clean_rows": 0,
        "batch_size": batch_size,
        "max_batch_rows": 0,
    }
    stage = output / ".dedup.sqlite"
    temporary = output / ".consommation.partial.parquet"
    numeric = {*METRICS.values(), "production_totale_mw"}
    schema = pa.schema(
        [(name, pa.float64() if name in numeric else pa.string()) for name in COLUMNS]
    )
    with measure() as metrics:
        try:
            with closing(sqlite3.connect(stage)) as connection:
                connection.execute("DROP TABLE IF EXISTS measures")
                connection.execute("""CREATE TABLE measures (
                    stamp TEXT, area TEXT, payload TEXT, PRIMARY KEY(stamp, area))""")
                with connection:
                    for batch in record_batches(iter_records(run_dir, manifest), batch_size):
                        frame, counters = transform_records(batch, manifest["dataset"], True)
                        report["max_batch_rows"] = max(report["max_batch_rows"], len(batch))
                        for key in ("raw_rows", "unmeasured_rows", "duplicate_rows"):
                            report[key] += counters[key]
                        for row in frame.itertuples(index=False, name=None):
                            values = [None if pd.isna(value) else value for value in row]
                            payload = json.dumps(values, ensure_ascii=False, allow_nan=False)
                            prior = connection.execute(
                                "SELECT payload FROM measures WHERE stamp=? AND area=?",
                                (values[0], values[2]),
                            ).fetchone()
                            if prior:
                                if prior[0] != payload:
                                    raise ValueError("Mesures contradictoires entre deux lots.")
                                report["duplicate_rows"] += 1
                            else:
                                connection.execute(
                                    "INSERT INTO measures VALUES (?, ?, ?)",
                                    (values[0], values[2], payload),
                                )
                                report["clean_rows"] += 1
                if not report["clean_rows"]:
                    raise ValueError("Aucune consommation mesurée exploitable dans cette archive.")
                cursor = connection.execute("SELECT payload FROM measures ORDER BY stamp, area")
                with pq.ParquetWriter(temporary, schema, compression="zstd") as writer:
                    while rows := cursor.fetchmany(batch_size):
                        records = [
                            dict(zip(COLUMNS, json.loads(row[0]), strict=True)) for row in rows
                        ]
                        writer.write_table(pa.Table.from_pylist(records, schema=schema))
            temporary.replace(parquet)
        finally:
            stage.unlink(missing_ok=True)
            temporary.unlink(missing_ok=True)
    report.update(
        source_run=str(run_dir.resolve()),
        dataset=manifest["dataset"],
        start=manifest["start"],
        end_exclusive=manifest["end_exclusive"],
        source_manifest_sha256=manifest_sha,
        parquet_sha256=file_hash(parquet),
        metrics=metrics,
    )
    temporary_report = report_path.with_suffix(".tmp")
    temporary_report.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    temporary_report.replace(report_path)
    return output
