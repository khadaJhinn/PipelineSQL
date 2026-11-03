import json
from pathlib import Path

from scripts.files import atomic_json


def test_checkpoint_replacement_retries_a_temporary_windows_lock(tmp_path, monkeypatch):
    target = tmp_path / "checkpoint.json"
    target.write_text('{"status": "old"}')
    original = Path.replace
    attempts = []

    def replace(source, destination):
        attempts.append(1)
        if len(attempts) < 3:
            raise PermissionError("Fichier temporairement verrouillé")
        return original(source, destination)

    monkeypatch.setattr(Path, "replace", replace)
    monkeypatch.setattr("scripts.files.sleep", lambda seconds: None)
    atomic_json(target, {"status": "complete"})
    assert len(attempts) == 3
    assert json.loads(target.read_text()) == {"status": "complete"}
    assert list(tmp_path.glob("*.tmp")) == []
