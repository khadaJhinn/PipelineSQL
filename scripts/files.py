"""Écritures atomiques, avec reprise des verrouillages temporaires sous Windows."""

import json
import os
from pathlib import Path
from time import sleep
from uuid import uuid4


def replace_file(source: Path, target: Path) -> None:
    for attempt in range(6):
        try:
            source.replace(target)
            return
        except PermissionError:
            if attempt == 5:
                raise
            sleep(0.05 * (attempt + 1))


def atomic_json(path: Path, value: dict) -> None:
    temporary = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    try:
        with temporary.open("x", encoding="utf-8") as stream:
            json.dump(value, stream, ensure_ascii=False, indent=2)
            stream.flush()
            os.fsync(stream.fileno())
        replace_file(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)
