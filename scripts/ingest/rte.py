"""API publique ODRÉ v2.1 : requêtes concurrentes bornées, pagination et reprises."""

import asyncio
import hashlib
import json
import logging
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from uuid import uuid4
from zoneinfo import ZoneInfo

import httpx

from scripts.config import Settings
from scripts.excel import records_frame, write_excel

LOGGER = logging.getLogger(__name__)
PARIS = ZoneInfo("Europe/Paris")
PAGE_SIZE = 100


def day_bounds(day: date) -> tuple[str, str]:
    """Bornes UTC d'un jour français, y compris aux changements d'heure."""
    start = datetime.combine(day, time.min, PARIS).astimezone(UTC)
    end = datetime.combine(day + timedelta(days=1), time.min, PARIS).astimezone(UTC)
    return start.isoformat(), end.isoformat()


class RteClient:
    def __init__(self, settings: Settings, client: httpx.AsyncClient):
        if settings.concurrency < 1 or settings.retries < 0:
            raise ValueError("Concurrence >= 1 et reprises >= 0 requises.")
        self.settings = settings
        self.client = client
        self.semaphore = asyncio.Semaphore(settings.concurrency)

    async def request(self, params: dict) -> httpx.Response:
        for attempt in range(self.settings.retries + 1):
            try:
                async with self.semaphore:
                    response = await self.client.get(self.settings.records_url, params=params)
                response.raise_for_status()
                return response
            except (httpx.TransportError, httpx.HTTPStatusError) as exc:
                if isinstance(exc, httpx.HTTPStatusError):
                    if exc.response.status_code not in (408, 429, 500, 502, 503, 504):
                        raise
                if attempt == self.settings.retries:
                    raise
                delay = 2.0**attempt
                if isinstance(exc, httpx.HTTPStatusError):
                    retry_after = exc.response.headers.get("Retry-After", "")
                    try:
                        delay = min(60, max(delay, float(retry_after)))
                    except ValueError:
                        pass
                LOGGER.warning("Échec API, nouvelle tentative dans %.1f s", delay)
                await asyncio.sleep(delay)
        raise RuntimeError("Nombre de tentatives épuisé.")

    async def latest_measured_day(self) -> date:
        response = await self.request(
            {
                "where": "perimetre = 'France' AND consommation IS NOT NULL",
                "order_by": "date_heure DESC",
                "limit": 1,
            }
        )
        records = response.json()["results"]
        if not records:
            raise ValueError("Aucune consommation mesurée disponible auprès de RTE.")
        return datetime.fromisoformat(records[0]["date_heure"]).astimezone(PARIS).date()

    async def collect_day(self, day: date, run_dir: Path) -> list[dict]:
        start, end = day_bounds(day)
        params = {
            "where": f"perimetre = 'France' AND date_heure >= '{start}' AND date_heure < '{end}'",
            "order_by": "date_heure ASC",
            "limit": PAGE_SIZE,
            "offset": 0,
        }
        pages = []
        expected_total = None
        while True:
            response = await self.request(params)
            payload = response.json()
            rows = payload["results"]
            total = payload["total_count"]
            if not isinstance(rows, list) or not isinstance(total, int) or total < 0:
                raise ValueError("Réponse API invalide.")
            if expected_total is not None and total != expected_total:
                raise ValueError("Les données API ont changé pendant la pagination : relancer.")
            expected_total = total
            if total > 10_000:
                raise ValueError("Plus de 10 000 lignes sur un jour : pagination non supportée.")
            if len(rows) > PAGE_SIZE or params["offset"] + len(rows) > total:
                raise ValueError("Pagination incohérente : trop de lignes retournées.")
            name = f"{day.isoformat()}_{params['offset']:05d}.json"
            # Le corps JSON reste inchangé ; chaque archive est créée une seule fois.
            with (run_dir / name).open("xb") as output:
                output.write(response.content)
            pages.append(
                {
                    "file": name,
                    "sha256": hashlib.sha256(response.content).hexdigest(),
                    "rows": len(rows),
                    "params": dict(params),
                }
            )
            params["offset"] += len(rows)
            if params["offset"] >= total:
                break
            if not rows:
                raise ValueError("Pagination interrompue avant la fin des résultats.")
        return pages


async def ingest(
    settings: Settings,
    start: date | None = None,
    end: date | None = None,
    days: int = 7,
    client: httpx.AsyncClient | None = None,
) -> Path:
    """Période française [start, end[ ; défaut : 7 derniers jours avec mesures."""
    if not 1 <= days <= 366:
        raise ValueError("days doit être compris entre 1 et 366.")
    if (start is None) != (end is None):
        raise ValueError("Fournir start et end ensemble.")
    if client is None:
        async with httpx.AsyncClient(timeout=30, follow_redirects=True) as owned_client:
            return await ingest(settings, start, end, days, owned_client)
    api = RteClient(settings, client)
    if start is None:
        end = await api.latest_measured_day() + timedelta(days=1)
        start = end - timedelta(days=days)
    if not 1 <= (end - start).days <= 366:
        raise ValueError("La période doit couvrir de 1 à 366 jours.")
    run_id = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ") + "_" + uuid4().hex[:12]
    run_dir = settings.data_dir / "raw" / run_id
    run_dir.mkdir(parents=True, exist_ok=False)
    manifest = {
        "run_id": run_id,
        "source": settings.records_url,
        "dataset": settings.dataset,
        "retrieved_at": datetime.now(UTC).isoformat(),
        "start": start.isoformat(),
        "end_exclusive": end.isoformat(),
        "timezone": "Europe/Paris",
    }
    tasks = [
        asyncio.create_task(api.collect_day(start + timedelta(days=index), run_dir))
        for index in range((end - start).days)
    ]
    try:
        page_groups = await asyncio.gather(*tasks)
        pages = [page for group in page_groups for page in group]
        records = [
            record
            for page in pages
            for record in json.loads((run_dir / page["file"]).read_bytes())["results"]
        ]
        if not records:
            raise ValueError("Aucune donnée disponible pour cette période.")
        await asyncio.to_thread(write_excel, records_frame(records), run_dir / "brut.xlsx")
        manifest.update(status="complete", pages=pages, raw_rows=len(records))
    except BaseException:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        manifest["status"] = "failed"
        raise
    finally:
        with (run_dir / "manifest.json").open("x", encoding="utf-8") as output:
            json.dump(manifest, output, ensure_ascii=False, indent=2)
    LOGGER.info("Archive brute : %s (%d lignes)", run_dir, len(records))
    return run_dir
