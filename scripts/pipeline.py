"""Lancement complet ou étape par étape depuis la racine du dépôt."""

import argparse
import asyncio
import json
import logging
import sys
from datetime import date
from pathlib import Path

import aiohttp
import duckdb

from scripts.analyze import analyze
from scripts.config import Settings
from scripts.ingest.rte import ingest
from scripts.load.database import load_processed
from scripts.metrics import measure
from scripts.publish.prepare import prepare_hub
from scripts.transform.clean import transform_archive


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Pipeline RTE : API > Excel > Parquet > SQL > Excel"
    )
    commands = parser.add_subparsers(dest="command", required=True)
    run = commands.add_parser("run", help="Exécuter toute la chaîne")
    collect = commands.add_parser("ingest", help="Collecter et archiver les données brutes")
    for command in (run, collect):
        command.add_argument(
            "--start", type=date.fromisoformat, help="Premier jour français inclus"
        )
        command.add_argument("--end", type=date.fromisoformat, help="Dernier jour français EXCLU")
        command.add_argument(
            "--days", type=int, default=7, help="Défaut : 7 derniers jours mesurés"
        )
        command.add_argument("--refresh", action="store_true", help="Créer un nouveau snapshot")
        command.add_argument("--resume", type=Path, help="Reprendre un checkpoint JSON")
    clean = commands.add_parser("transform", help="Nettoyer une archive existante")
    clean.add_argument("--raw", type=Path, required=True, help="Dossier raw d'une exécution")
    load = commands.add_parser("load", help="Charger un Parquet existant et exporter depuis SQL")
    load.add_argument("--processed", type=Path, required=True)
    analysis = commands.add_parser("analyze", help="Exécuter les analyses SQL et mesurer l'index")
    analysis.add_argument("--output", type=Path, help="Dossier de rapports")
    prepare = commands.add_parser("prepare-hub", help="Préparer les fichiers pour le dépôt manuel")
    prepare.add_argument("--processed", type=Path, required=True)
    prepare.add_argument("--output", type=Path, help="Dossier local du dataset à déposer")
    for command in (run, load, analysis):
        command.add_argument("--database", type=Path, help="Chemin DuckDB (défaut : configuration)")
    for command in (run, clean, load, analysis):
        command.add_argument("--batch-size", type=int, help="Lignes par lot (défaut : 1000)")
    return parser


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s : %(message)s")
    parser = build_parser()
    arguments = sys.argv[1:] if argv is None else argv
    args = parser.parse_args(arguments or ["run"])
    result = {}
    try:
        settings = Settings.from_env()
        batch_size = getattr(args, "batch_size", None)
        batch_size = settings.batch_size if batch_size is None else batch_size
        if batch_size < 1:
            raise ValueError("La taille de lot doit être positive.")
        if args.command in ("run", "ingest"):
            with measure() as ingestion_metrics:
                raw = asyncio.run(
                    ingest(
                        settings,
                        args.start,
                        args.end,
                        args.days,
                        refresh=args.refresh,
                        resume=args.resume,
                    )
                )
            result["raw"] = str(raw)
            result["raw_excel"] = str(raw / "brut.xlsx")
        if args.command in ("run", "transform"):
            raw = raw if args.command == "run" else args.raw.resolve()
            transformed_before = (
                settings.data_dir / "processed" / raw.name / "quality.json"
            ).exists()
            with measure() as transformation_metrics:
                processed = transform_archive(raw, settings.data_dir / "processed", batch_size)
            result["processed"] = str(processed)
            result["parquet"] = str(processed / "consommation.parquet")
        if args.command in ("run", "load"):
            processed = processed if args.command == "run" else args.processed.resolve()
            database = (args.database or settings.database).resolve()
            result["sql_excel"] = str(load_processed(processed, database, batch_size))
            result["database"] = str(database)
        if args.command in ("run", "analyze"):
            database = (args.database or settings.database).resolve()
            output = (
                settings.data_dir / "reports" / processed.name
                if args.command == "run"
                else args.output or settings.data_dir / "reports/latest"
            )
            result["analysis_report"] = str(analyze(database, output, batch_size))
        if args.command == "run":
            quality = json.loads((processed / "quality.json").read_text(encoding="utf-8"))
            loaded = json.loads((processed / "load_report.json").read_text(encoding="utf-8"))
            analyzed = json.loads(Path(result["analysis_report"]).read_text(encoding="utf-8"))
            report = {
                "run_id": processed.name,
                "raw_rows": quality["raw_rows"],
                "clean_rows": quality["clean_rows"],
                "raw_bytes": sum(path.stat().st_size for path in raw.iterdir() if path.is_file()),
                "parquet_bytes": (processed / "consommation.parquet").stat().st_size,
                "batch_size": batch_size,
                "ingestion": ingestion_metrics,
                "transformation": transformation_metrics,
                "transformation_reused": transformed_before,
                "parquet_batch_size": quality["batch_size"],
                "loading": loaded["metrics"],
                "analysis": analyzed["metrics"],
                "note": "RSS du processus entier, échantillonné toutes les 20 ms. "
                "Une archive complète déjà présente est réutilisée sauf --refresh.",
            }
            report_path = processed / "pipeline_report.json"
            report_path.write_text(
                json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8"
            )
            result["pipeline_report"] = str(report_path)
        if args.command == "prepare-hub":
            processed = args.processed.resolve()
            output = args.output or settings.data_dir / "publish" / processed.name
            result["manual_upload_folder"] = str(prepare_hub(processed, output))
    except (ValueError, OSError, aiohttp.ClientError, TimeoutError, duckdb.Error, KeyError) as exc:
        parser.exit(1, f"Échec du pipeline : {exc}\n")
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
