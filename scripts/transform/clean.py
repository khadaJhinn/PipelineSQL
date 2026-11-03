"""Mesures nationales validées, unités explicites, UTC et heure française."""

import hashlib
import json
import math
from datetime import UTC, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd

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


def transform_records(records: list[dict], dataset: str) -> tuple[pd.DataFrame, dict]:
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
    if not cleaned:
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


def read_archive(run_dir: Path) -> tuple[list[dict], dict]:
    manifest = json.loads((run_dir / "manifest.json").read_text(encoding="utf-8"))
    if manifest.get("status") != "complete":
        raise ValueError("Une archive incomplète ne peut pas être transformée.")
    records = []
    for page in manifest["pages"]:
        path = (run_dir / page["file"]).resolve()
        if path.parent != run_dir.resolve():
            raise ValueError("Chemin de page hors de l'archive.")
        content = path.read_bytes()
        if hashlib.sha256(content).hexdigest() != page["sha256"]:
            raise ValueError(f"Archive brute modifiée : {path.name}.")
        rows = json.loads(content)["results"]
        if len(rows) != page["rows"]:
            raise ValueError("Nombre de lignes incohérent avec le manifeste.")
        records.extend(rows)
    if len(records) != manifest["raw_rows"]:
        raise ValueError("Archive incomplète : nombre de lignes incohérent.")
    return records, manifest


def transform_archive(run_dir: Path, processed_root: Path) -> Path:
    records, manifest = read_archive(run_dir)
    frame, report = transform_records(records, manifest["dataset"])
    output = processed_root / run_dir.name
    output.mkdir(parents=True, exist_ok=False)
    frame.to_parquet(output / "consommation.parquet", index=False)
    report.update(
        source_run=str(run_dir.resolve()),
        dataset=manifest["dataset"],
        start=manifest["start"],
        end_exclusive=manifest["end_exclusive"],
    )
    (output / "quality.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    return output
