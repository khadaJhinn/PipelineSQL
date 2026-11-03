import asyncio
import hashlib
import json
from dataclasses import replace
from datetime import date

import aiohttp
import pytest
from aiohttp import web

from scripts.ingest.rte import RteClient, day_bounds, ingest


async def test_pagination_jsonl_checkpoint_and_excel(api):
    def handler(request):
        offset = int(request.query["offset"])
        rows = [
            {"date_heure": str(i + offset), "consommation": 40000 + i}
            for i in range(100 if offset == 0 else 1)
        ]
        return web.json_response({"total_count": 101, "results": rows})

    api.handler = handler
    run = await ingest(api.settings, date(2026, 10, 1), date(2026, 10, 2))
    manifest = json.loads((run / "manifest.json").read_text())
    assert manifest["status"] == "complete"
    assert manifest["raw_rows"] == 101
    assert [page["file"] for page in manifest["pages"]] == [
        "2026-10-01_00000.jsonl",
        "2026-10-01_00100.jsonl",
    ]
    for page in manifest["pages"]:
        raw = (run / page["file"]).read_bytes()
        assert page["sha256"] == hashlib.sha256(raw).hexdigest()
        assert len(raw.decode().splitlines()) == page["rows"]
    assert (run / "brut.xlsx").is_file()
    checkpoint = next((api.settings.data_dir / "checkpoints").glob("*.json"))
    assert json.loads(checkpoint.read_text())["status"] == "complete"


async def test_retry_on_rate_limit(monkeypatch, api):
    calls = []

    async def no_sleep(delay):
        calls.append(delay)

    monkeypatch.setattr("scripts.ingest.rte.asyncio.sleep", no_sleep)
    api.respond(status=429)
    api.respond({"results": []})
    async with aiohttp.ClientSession() as client:
        await RteClient(replace(api.settings, retries=3), client).request({})
    assert calls == [1]


async def test_interruption_resumes_without_redownloading_saved_page(api):
    api.respond({"total_count": 101, "results": [{"consommation": 42}] * 100})
    api.respond(status=503)
    with pytest.raises(aiohttp.ClientResponseError):
        await ingest(api.settings, date(2026, 10, 1), date(2026, 10, 2))
    checkpoint = next((api.settings.data_dir / "checkpoints").glob("*.json"))
    state = json.loads(checkpoint.read_text())
    assert state["status"] == "interrupted"
    run = api.settings.data_dir / "raw" / state["run_id"]
    first_page = run / "2026-10-01_00000.jsonl"
    original = first_page.read_bytes()
    assert not (run / "manifest.json").exists()
    api.requests.clear()
    api.respond({"total_count": 101, "results": [{"consommation": 43}]})
    resumed = await ingest(api.settings, resume=checkpoint)
    assert resumed == run
    assert [int(request["offset"]) for request in api.requests] == [100]
    assert first_page.read_bytes() == original
    assert json.loads((run / "manifest.json").read_text())["raw_rows"] == 101


def test_french_day_includes_dst_change():
    assert day_bounds(date(2026, 10, 25)) == (
        "2026-10-24T22:00:00+00:00",
        "2026-10-25T23:00:00+00:00",
    )


async def test_concurrency_is_limited(api):
    active = maximum = 0

    async def handler(request):
        nonlocal active, maximum
        active += 1
        maximum = max(maximum, active)
        await asyncio.sleep(0.01)
        active -= 1
        return web.json_response({"total_count": 1, "results": [{"consommation": 42}]})

    api.handler = handler
    await ingest(replace(api.settings, concurrency=2), date(2026, 10, 1), date(2026, 10, 6))
    assert maximum == 2


async def test_changed_total_fails_and_keeps_checkpoint(api):
    api.respond({"total_count": 101, "results": [{"consommation": 42}] * 100})
    api.respond({"total_count": 102, "results": [{"consommation": 42}] * 2})
    with pytest.raises(ValueError, match="changé"):
        await ingest(api.settings, date(2026, 10, 1), date(2026, 10, 2))


async def test_completed_run_is_reused_and_refresh_preserves_original(api):
    api.handler = lambda request: web.json_response(
        {"total_count": 1, "results": [{"consommation": 42}]}
    )
    first = await ingest(api.settings, date(2026, 10, 1), date(2026, 10, 2))
    original = {p.name: p.read_bytes() for p in first.iterdir()}
    request_count = len(api.requests)
    assert await ingest(api.settings, date(2026, 10, 1), date(2026, 10, 2)) == first
    assert len(api.requests) == request_count
    second = await ingest(api.settings, date(2026, 10, 1), date(2026, 10, 2), refresh=True)
    assert first != second
    assert first.name.endswith("v001") and second.name.endswith("v002")
    assert original == {p.name: p.read_bytes() for p in first.iterdir()}


async def test_corrupted_checkpoint_page_is_rejected(api):
    api.respond({"total_count": 101, "results": [{"consommation": 42}] * 100})
    api.respond(status=400)
    with pytest.raises(aiohttp.ClientResponseError):
        await ingest(api.settings, date(2026, 10, 1), date(2026, 10, 2))
    page = next((api.settings.data_dir / "raw").glob("*/*.jsonl"))
    page.write_text("{}\n")
    with pytest.raises(ValueError, match="modifiée"):
        await ingest(api.settings, date(2026, 10, 1), date(2026, 10, 2))
