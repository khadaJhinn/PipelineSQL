import hashlib
import json
from dataclasses import replace
from datetime import date

import httpx
import pytest

from scripts.config import Settings
from scripts.ingest.rte import RteClient, day_bounds, ingest


async def test_pagination_preserves_bytes_and_excel(tmp_path):
    content = []

    def handler(request):
        offset = int(request.url.params["offset"])
        count = 100 if offset == 0 else 1
        rows = [{"date_heure": str(i), "consommation": 40000 + i} for i in range(count)]
        raw = json.dumps({"total_count": 101, "results": rows}).encode()
        content.append(raw)
        return httpx.Response(200, content=raw)

    settings = replace(Settings(), data_dir=tmp_path)
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        run = await ingest(settings, date(2026, 10, 1), date(2026, 10, 2), client=client)
    manifest = json.loads((run / "manifest.json").read_text())
    assert manifest["status"] == "complete"
    assert manifest["raw_rows"] == 101
    for page, original in zip(manifest["pages"], content, strict=True):
        assert (run / page["file"]).read_bytes() == original
        assert page["sha256"] == hashlib.sha256(original).hexdigest()
    assert (run / "brut.xlsx").is_file()


async def test_retry_on_rate_limit(monkeypatch):
    calls = []

    async def no_sleep(delay):
        calls.append(delay)

    monkeypatch.setattr("scripts.ingest.rte.asyncio.sleep", no_sleep)
    responses = iter([httpx.Response(429), httpx.Response(200, json={"results": []})])
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: next(responses))
    ) as client:
        await RteClient(Settings(), client).request({})
    assert calls == [1]


async def test_failed_run_cannot_be_marked_complete(tmp_path):
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(400))
    ) as client:
        with pytest.raises(httpx.HTTPStatusError):
            await ingest(
                replace(Settings(), data_dir=tmp_path),
                date(2026, 10, 1),
                date(2026, 10, 2),
                client=client,
            )
    manifest = next(tmp_path.glob("raw/*/manifest.json"))
    assert json.loads(manifest.read_text())["status"] == "failed"


def test_french_day_includes_dst_change():
    start, end = day_bounds(date(2026, 10, 25))
    assert start == "2026-10-24T22:00:00+00:00"
    assert end == "2026-10-25T23:00:00+00:00"
