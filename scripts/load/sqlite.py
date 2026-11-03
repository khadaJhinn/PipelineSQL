"""Chargement transactionnel, upsert et export des valeurs réellement présentes en SQL."""

import json
import sqlite3
from contextlib import closing
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import pandas as pd

from scripts.config import ROOT
from scripts.excel import write_excel
from scripts.transform.clean import COLUMNS


def load_frame(frame: pd.DataFrame, database: Path, run_id: str, archive: Path) -> pd.DataFrame:
    if frame.empty or list(frame.columns) != COLUMNS:
        raise ValueError("Tableau transformé vide ou schéma incorrect.")
    manifest = json.loads((archive / "manifest.json").read_text(encoding="utf-8"))
    if manifest.get("status") != "complete":
        raise ValueError("Le chargement nécessite une archive complète.")
    database.parent.mkdir(parents=True, exist_ok=True)
    with closing(sqlite3.connect(database, timeout=30)) as connection:
        connection.executescript((ROOT / "sql/001_schema.sql").read_text(encoding="utf-8"))
        with connection:
            connection.execute(
                """INSERT INTO pipeline_runs VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(run_id) DO UPDATE SET loaded_at = excluded.loaded_at""",
                (
                    run_id,
                    str(archive.resolve()),
                    manifest["retrieved_at"],
                    manifest["raw_rows"],
                    len(frame),
                    datetime.now(UTC).isoformat(),
                ),
            )
            columns = [*COLUMNS, "source_retrieved_at", "run_id"]
            placeholders = ", ".join("?" for _ in columns)
            assignments = ", ".join(
                f"{name} = excluded.{name}"
                for name in columns
                if name not in ("date_heure_utc", "perimetre")
            )
            # Les noms SQL proviennent exclusivement de COLUMNS, jamais de l'API.
            query = (
                f"INSERT INTO electricity_consumption ({', '.join(columns)}) "
                f"VALUES ({placeholders}) "
                f"ON CONFLICT(date_heure_utc, perimetre) DO UPDATE SET {assignments} "
                "WHERE excluded.source_retrieved_at >= electricity_consumption.source_retrieved_at"
            )
            records = [
                tuple(None if pd.isna(value) else value for value in row)
                + (manifest["retrieved_at"], run_id)
                for row in frame.itertuples(index=False, name=None)
            ]
            connection.executemany(query, records)
            # Relecture des seules clés de cette exécution, y compris en cas de chevauchement.
            connection.execute(
                "CREATE TEMP TABLE incoming_keys (date_heure_utc TEXT, perimetre TEXT)"
            )
            connection.executemany(
                "INSERT INTO incoming_keys VALUES (?, ?)",
                list(frame[["date_heure_utc", "perimetre"]].itertuples(index=False, name=None)),
            )
            selected = ", ".join(f"e.{column}" for column in COLUMNS)
            exported = pd.read_sql_query(
                f"SELECT {selected} FROM electricity_consumption e "
                "JOIN incoming_keys k USING (date_heure_utc, perimetre) "
                "ORDER BY e.date_heure_utc, e.perimetre",
                connection,
            )
            if len(exported) != len(frame):
                raise ValueError("Échec de vérification du nombre de lignes SQL.")
    return exported


def load_processed(processed: Path, database: Path) -> Path:
    quality = json.loads((processed / "quality.json").read_text(encoding="utf-8"))
    frame = pd.read_parquet(processed / "consommation.parquet")
    exported = load_frame(frame, database, processed.name, Path(quality["source_run"]))
    # Le fichier définitif est remplacé seulement après un export complet.
    target = processed / "consommation_sql.xlsx"
    temporary = processed / f".{uuid4().hex}.xlsx"
    try:
        write_excel(exported, temporary)
        temporary.replace(target)
    finally:
        temporary.unlink(missing_ok=True)
    report = {
        "database": str(database.resolve()),
        "sql_rows_exported": len(exported),
        "excel": target.name,
        "loaded_at": datetime.now(UTC).isoformat(),
    }
    (processed / "load_report.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    return target
