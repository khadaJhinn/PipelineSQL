import json
from datetime import date

import pytest

from scripts.archive import file_hash
from scripts.ingest.rte import ingest
from scripts.publish.prepare import prepare_hub
from scripts.transform.clean import transform_archive


async def prepare_source(api):
    api.respond(
        {
            "total_count": 2,
            "results": [
                {
                    "perimetre": "France",
                    "date_heure": "2026-10-01T00:00:00+00:00",
                    "consommation": 40000,
                },
                {
                    "perimetre": "France",
                    "date_heure": "2026-10-01T00:15:00+00:00",
                    "consommation": None,
                },
            ],
        }
    )
    raw = await ingest(api.settings, date(2026, 10, 1), date(2026, 10, 2))
    return transform_archive(raw, api.settings.data_dir / "processed", batch_size=1)


async def test_preparation_is_local_and_reproducible(api, tmp_path):
    processed = await prepare_source(api)
    count = len(api.requests)
    output = tmp_path / "manual"
    prepare_hub(processed, output)
    assert prepare_hub(processed, output) == output
    assert len(api.requests) == count
    assert file_hash(output / "data/consommation.parquet") == file_hash(
        processed / "consommation.parquet"
    )
    card = (output / "README.md").read_text(encoding="utf-8")
    assert "$" not in card
    assert "v1.0.0" in card
    assert "Mesures nettoyées : **1**" in card
    assert "consolidé et définitif" in card
    assert "demi-heure" in card
    assert "eco2mix-national-cons-def/" in card
    assert str(tmp_path) not in card
    provenance = json.loads((output / "provenance.json").read_text(encoding="utf-8"))
    assert provenance["clean_rows"] == 1


async def test_existing_manual_card_is_never_overwritten(api, tmp_path):
    processed = await prepare_source(api)
    output = prepare_hub(processed, tmp_path / "manual")
    (output / "README.md").write_text("Ma documentation", encoding="utf-8")
    with pytest.raises(FileExistsError):
        prepare_hub(processed, output)
    assert (output / "README.md").read_text(encoding="utf-8") == "Ma documentation"


async def test_tampered_data_cannot_be_prepared(api, tmp_path):
    processed = await prepare_source(api)
    (processed / "consommation.parquet").write_bytes(b"modified")
    with pytest.raises(ValueError, match="Parquet"):
        prepare_hub(processed, tmp_path / "manual")
