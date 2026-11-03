"""Exécute les analyses et mesure un index réel avec EXPLAIN ANALYZE."""

import csv
import json
import re
from datetime import UTC, datetime
from pathlib import Path
from statistics import median
from time import perf_counter

from scripts.config import ROOT
from scripts.load.database import connect
from scripts.metrics import measure

INDEX_NAME = "idx_consommation_mw"
INDEX_QUERY = (
    "SELECT COUNT(*), MIN(date_heure_utc), MAX(date_heure_utc) "
    "FROM electricity_consumption WHERE consommation_mw = ?"
)


def read_queries() -> dict[str, str]:
    source = (ROOT / "sql/analyses.sql").read_text(encoding="utf-8")
    return dict(re.findall(r"-- analyse: ([a-z_]+)\n(.*?)(?=-- analyse: |\Z)", source, re.S))


def index_benchmark(connection, repetitions: int = 31) -> dict:
    value = connection.execute(
        "SELECT MAX(consommation_mw) FROM electricity_consumption"
    ).fetchone()[0]
    if value is None:
        raise ValueError("La base ne contient aucune mesure.")

    def profile():
        plan = connection.execute("EXPLAIN ANALYZE " + INDEX_QUERY, [value]).fetchone()[1]
        expected = connection.execute(INDEX_QUERY, [value]).fetchall()
        for _ in range(3):
            connection.execute(INDEX_QUERY, [value]).fetchall()
        times = []
        for _ in range(repetitions):
            started = perf_counter()
            actual = connection.execute(INDEX_QUERY, [value]).fetchall()
            times.append((perf_counter() - started) * 1000)
            if actual != expected:
                raise ValueError("Résultats SQL non déterministes pendant le benchmark.")
        return {
            "median_ms": median(times),
            "plan": plan,
            "rows": len(expected),
            "matched_rows": expected[0][0],
        }, expected

    # Cet index est créé par ce projet. Les autres index ne sont jamais modifiés.
    connection.execute(f"DROP INDEX IF EXISTS {INDEX_NAME}")
    before, expected = profile()
    connection.execute((ROOT / "sql/002_indexes.sql").read_text(encoding="utf-8"))
    after, actual = profile()
    if expected != actual:
        raise ValueError("L'index a changé les résultats.")
    return {
        "index": INDEX_NAME,
        "column": "consommation_mw",
        "query": INDEX_QUERY,
        "parameter_mw": value,
        "repetitions": repetitions,
        "before": before,
        "after": after,
        "ratio_before_after": before["median_ms"] / after["median_ms"],
        "index_scan_selected": "Index Scan" in after["plan"],
        "method": "Même requête, mêmes données, médiane après 3 échauffements, cache chaud.",
        "limitation": "Sur un petit volume, le coût fixe peut masquer tout gain de l'index.",
    }


def analyze(database: Path, output: Path, batch_size: int = 1000) -> Path:
    if batch_size < 1:
        raise ValueError("La taille de lot doit être positive.")
    if not database.is_file():
        raise FileNotFoundError(database)
    output.mkdir(parents=True, exist_ok=True)
    results = {}
    with measure() as metrics:
        with connect(database) as connection:
            count, first, last = connection.execute(
                "SELECT COUNT(*), MIN(date_heure_utc), MAX(date_heure_utc) "
                "FROM electricity_consumption"
            ).fetchone()
            for name, query in read_queries().items():
                started = perf_counter()
                cursor = connection.execute(query)
                fields = [field[0] for field in cursor.description]
                rows = 0
                with (output / f"{name}.csv").open("w", encoding="utf-8-sig", newline="") as target:
                    writer = csv.writer(target, delimiter=";")
                    writer.writerow(fields)
                    while batch := cursor.fetchmany(batch_size):
                        writer.writerows(batch)
                        rows += len(batch)
                results[name] = {"rows": rows, "seconds": perf_counter() - started}
            benchmark = index_benchmark(connection)
    report = {
        "engine": "duckdb",
        "database": str(database.resolve()),
        "measured_at": datetime.now(UTC).isoformat(),
        "database_rows": count,
        "first_utc": first,
        "last_utc": last,
        "queries": results,
        "index_benchmark": benchmark,
        "metrics": metrics,
    }
    path = output / "analysis_report.json"
    path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    (output / "index_explain_analyze.txt").write_text(
        "AVANT INDEX\n"
        + benchmark["before"]["plan"]
        + "\nAPRÈS INDEX\n"
        + benchmark["after"]["plan"],
        encoding="utf-8",
    )
    return path
