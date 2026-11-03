"""Lecture des archives JSONL et des anciennes archives JSON, en mémoire bornée."""

import hashlib
import json
from collections.abc import Iterator
from pathlib import Path


def file_hash(path: Path) -> str:
    with path.open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def read_manifest(run_dir: Path) -> dict:
    manifest = json.loads((run_dir / "manifest.json").read_text(encoding="utf-8"))
    if manifest.get("status") != "complete":
        raise ValueError("Une archive incomplète ne peut pas être transformée.")
    if sum(page["rows"] for page in manifest["pages"]) != manifest["raw_rows"]:
        raise ValueError("Nombre de lignes incohérent dans le manifeste.")
    return manifest


def iter_records(run_dir: Path, manifest: dict | None = None) -> Iterator[dict]:
    manifest = read_manifest(run_dir) if manifest is None else manifest
    for page in manifest["pages"]:
        path = (run_dir / page["file"]).resolve()
        if path.parent != run_dir.resolve():
            raise ValueError("Chemin de page hors de l'archive.")
        if file_hash(path) != page["sha256"]:
            raise ValueError(f"Archive brute modifiée : {path.name}.")
        count = 0
        if path.suffix == ".jsonl":
            with path.open(encoding="utf-8") as source:
                for line in source:
                    record = json.loads(line)
                    if not isinstance(record, dict):
                        raise ValueError("Une ligne brute doit être un objet JSON.")
                    count += 1
                    yield record
        else:
            # Anciennes pages API limitées à 100 lignes : compatibilité sans réécriture.
            records = json.loads(path.read_bytes())["results"]
            for record in records:
                count += 1
                yield record
        if count != page["rows"]:
            raise ValueError("Nombre de lignes incohérent avec le manifeste.")
