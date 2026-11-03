"""DuckDB : insertion Parquet par lots, transaction et Excel relu en SQL."""

import json
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from scripts.archive import file_hash
from scripts.config import ROOT
from scripts.excel import write_records_excel
from scripts.metrics import measure
from scripts.transform.clean import COLUMNS


def connect(database: Path):
    database.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(database), config={"memory_limit": "256MB", "threads": 2})


@contextmanager
def transaction(connection):
    connection.execute("BEGIN TRANSACTION")
    try:
        yield
    except BaseException:
        connection.execute("ROLLBACK")
        raise
    else:
        connection.execute("COMMIT")


def insert_batches(connection, batches, run_id: str, archive: Path, clean_rows: int) -> None:
    manifest = json.loads((archive / "manifest.json").read_text(encoding="utf-8"))
    if manifest.get("status") != "complete" or clean_rows < 1:
        raise ValueError("Le chargement nécessite une archive complète avec des mesures.")
    connection.execute((ROOT / "sql/001_schema.sql").read_text(encoding="utf-8"))
    columns = [*COLUMNS, "source_retrieved_at", "run_id"]
    assignments = ", ".join(
        f"{name}=excluded.{name}" for name in columns if name not in ("date_heure_utc", "perimetre")
    )
    names = ", ".join(COLUMNS)
    with transaction(connection):
        connection.execute(
            """INSERT INTO pipeline_runs VALUES (?, ?, ?, ?, ?, ?)
               ON CONFLICT(run_id) DO NOTHING""",
            [
                run_id,
                str(archive.resolve()),
                manifest["retrieved_at"],
                manifest["raw_rows"],
                clean_rows,
                datetime.now(UTC),
            ],
        )
        connection.execute(
            """CREATE TEMP TABLE incoming_keys (
                date_heure_utc VARCHAR, perimetre VARCHAR,
                PRIMARY KEY(date_heure_utc, perimetre))"""
        )
        received = 0
        for batch in batches:
            table = pa.Table.from_batches([batch]) if isinstance(batch, pa.RecordBatch) else batch
            if table.column_names != COLUMNS:
                raise ValueError("Schéma Parquet incorrect.")
            connection.register("incoming_batch", table)
            try:
                connection.execute(
                    f"INSERT INTO electricity_consumption ({', '.join(columns)}) "
                    f"SELECT {names}, CAST(? AS TIMESTAMPTZ), ? FROM incoming_batch "
                    f"ON CONFLICT(date_heure_utc, perimetre) DO UPDATE SET {assignments} "
                    "WHERE excluded.source_retrieved_at >= "
                    "electricity_consumption.source_retrieved_at",
                    [manifest["retrieved_at"], run_id],
                )
                connection.execute(
                    "INSERT INTO incoming_keys SELECT date_heure_utc, perimetre FROM incoming_batch"
                )
                received += len(table)
            finally:
                connection.unregister("incoming_batch")
        if received != clean_rows:
            raise ValueError("Le nombre de lignes chargé ne correspond pas au Parquet.")
        actual = connection.execute(
            "SELECT COUNT(*) FROM electricity_consumption JOIN incoming_keys "
            "USING (date_heure_utc, perimetre)"
        ).fetchone()[0]
        if actual != clean_rows:
            raise ValueError("Échec de vérification du nombre de lignes SQL.")


def select_loaded(connection):
    selected = ", ".join(f"e.{name}" for name in COLUMNS)
    return connection.execute(
        f"SELECT {selected} FROM electricity_consumption e "
        "JOIN incoming_keys USING (date_heure_utc, perimetre) "
        "ORDER BY e.date_heure_utc, e.perimetre"
    )


def sql_records(connection, batch_size: int):
    cursor = select_loaded(connection)
    while batch := cursor.fetchmany(batch_size):
        for row in batch:
            yield dict(zip(COLUMNS, row, strict=True))


def load_frame(frame: pd.DataFrame, database: Path, run_id: str, archive: Path) -> pd.DataFrame:
    """Petit adaptateur pour les tests ; le pipeline utilise exclusivement load_processed."""
    if frame.empty or list(frame.columns) != COLUMNS:
        raise ValueError("Tableau transformé vide ou schéma incorrect.")
    with connect(database) as connection:
        insert_batches(
            connection,
            [pa.Table.from_pandas(frame, preserve_index=False)],
            run_id,
            archive,
            len(frame),
        )
        return select_loaded(connection).fetchdf()


def load_processed(processed: Path, database: Path, batch_size: int = 1000) -> Path:
    if batch_size < 1:
        raise ValueError("La taille de lot doit être positive.")
    quality = json.loads((processed / "quality.json").read_text(encoding="utf-8"))
    parquet_path = processed / "consommation.parquet"
    if quality.get("parquet_sha256") and file_hash(parquet_path) != quality["parquet_sha256"]:
        raise ValueError("Le fichier Parquet a été modifié.")
    archive = Path(quality["source_run"])
    if quality.get("source_manifest_sha256"):
        if file_hash(archive / "manifest.json") != quality["source_manifest_sha256"]:
            raise ValueError("Le manifeste source a été modifié.")
    parquet = pq.ParquetFile(parquet_path)
    count = parquet.metadata.num_rows
    if quality.get("clean_rows", count) != count:
        raise ValueError("Le rapport qualité ne correspond pas au Parquet.")
    target = processed / "consommation_sql.xlsx"
    temporary = processed / f".{uuid4().hex}.xlsx"
    with measure() as metrics:
        with connect(database) as connection:
            insert_batches(
                connection,
                parquet.iter_batches(batch_size=batch_size),
                processed.name,
                archive,
                count,
            )
            # La transaction SQL est validée avant la relecture destinée à Excel.
            try:
                exported = write_records_excel(
                    lambda: sql_records(connection, batch_size), temporary, COLUMNS
                )
                temporary.replace(target)
            finally:
                temporary.unlink(missing_ok=True)
    report = {
        "database": str(database.resolve()),
        "engine": "duckdb",
        "sql_rows_exported": exported,
        "excel": target.name,
        "loaded_at": datetime.now(UTC).isoformat(),
        "metrics": metrics,
        "batch_size": batch_size,
    }
    report_path = processed / "load_report.json"
    temporary_report = report_path.with_suffix(".tmp")
    temporary_report.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    temporary_report.replace(report_path)
    return target
