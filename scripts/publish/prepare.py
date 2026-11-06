"""Prépare uniquement des fichiers locaux ; aucun appel réseau ou jeton Hub."""

import json
import shutil
from pathlib import Path
from string import Template
from uuid import uuid4

import pyarrow.compute as pc
import pyarrow.parquet as pq

from scripts.archive import file_hash, read_manifest


def prepare_hub(processed: Path, output: Path) -> Path:
    quality = json.loads((processed / "quality.json").read_text(encoding="utf-8"))
    archive = Path(quality["source_run"])
    manifest = read_manifest(archive)
    parquet = processed / "consommation.parquet"
    digest = file_hash(parquet)
    if quality.get("parquet_sha256") != digest:
        raise ValueError("Le fichier Parquet ne correspond pas au rapport qualité.")
    if quality.get("source_manifest_sha256") != file_hash(archive / "manifest.json"):
        raise ValueError("Le manifeste source a été modifié.")
    source = pq.ParquetFile(parquet)
    if source.metadata.num_rows != quality["clean_rows"]:
        raise ValueError("Nombre de mesures incohérent.")
    first = last = None
    for batch in source.iter_batches(batch_size=1000, columns=["date_heure_utc"]):
        bounds = pc.min_max(batch.column(0)).as_py()
        first = min(first, bounds["min"]) if first else bounds["min"]
        last = max(last, bounds["max"]) if last else bounds["max"]
    if first is None:
        raise ValueError("Impossible de préparer un dataset vide.")
    historical = manifest["dataset"] == "eco2mix-national-cons-def"
    values = {
        "run_id": processed.name,
        "source_url": manifest["source"],
        "source_dataset": manifest["dataset"],
        "source_page": f"https://odre.opendatasoft.com/explore/dataset/{manifest['dataset']}/",
        "source_name": (
            "éCO2mix national consolidé et définitif" if historical else manifest["dataset"]
        ),
        "source_description": (
            "Données consolidées et définitives issues des comptages, complétées par des "
            "forfaits. La consommation et les filières sont mesurées à la demi-heure ; "
            "les prévisions sont au quart d'heure. Les lignes intermédiaires sans "
            "consommation mesurée sont exclues du Parquet."
            if historical
            else "Données temps réel issues de télémesures, forfaits et estimations, "
            "au quart d'heure. Elles sont provisoires et peuvent être corrigées ; "
            "l'historique de cette source est limité."
        ),
        "retrieved_at": manifest["retrieved_at"],
        "start": manifest["start"],
        "end_exclusive": manifest["end_exclusive"],
        "first_utc": first,
        "last_utc": last,
        "parquet_sha256": digest,
        "invalid_local_time_rows": quality.get("invalid_local_time_rows", 0),
        **{
            key: quality[key]
            for key in ("raw_rows", "unmeasured_rows", "duplicate_rows", "clean_rows")
        },
    }
    card = Template(Path(__file__).with_name("dataset_card.md").read_text(encoding="utf-8"))
    documents = {
        "README.md": card.substitute(values),
        "provenance.json": json.dumps(values, ensure_ascii=False, indent=2),
    }
    target = output / "data/consommation.parquet"
    target.parent.mkdir(parents=True, exist_ok=True)
    # Ne pas écraser une carte modifiée manuellement ni un dataset différent.
    for name, content in documents.items():
        destination = output / name
        if destination.exists() and destination.read_text(encoding="utf-8") != content:
            raise FileExistsError(f"Fichier existant différent : {destination}")
    if target.exists() and file_hash(target) != digest:
        raise FileExistsError(f"Dataset existant différent : {target}")
    if not target.exists():
        temporary = target.with_name(f".{uuid4().hex}.parquet")
        try:
            shutil.copyfile(parquet, temporary)
            if file_hash(temporary) != digest:
                raise ValueError("Échec de vérification de la copie du dataset.")
            temporary.replace(target)
        finally:
            temporary.unlink(missing_ok=True)
    for name, content in documents.items():
        destination = output / name
        if not destination.exists():
            with destination.open("x", encoding="utf-8") as stream:
                stream.write(content)
    return output
