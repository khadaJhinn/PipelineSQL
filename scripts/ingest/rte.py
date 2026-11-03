"""Collecte aiohttp, JSON Lines déterministe et checkpoints reprenables."""

import asyncio
import hashlib
import json
import logging
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import aiohttp
from filelock import FileLock

from scripts.archive import file_hash, iter_records, read_manifest
from scripts.config import Settings
from scripts.excel import write_records_excel

LOGGER = logging.getLogger(__name__)
PARIS = ZoneInfo("Europe/Paris")
PAGE_SIZE = 100


def day_bounds(day: date) -> tuple[str, str]:
    start = datetime.combine(day, time.min, PARIS).astimezone(UTC)
    end = datetime.combine(day + timedelta(days=1), time.min, PARIS).astimezone(UTC)
    return start.isoformat(), end.isoformat()


def save_state(path: Path, state: dict) -> None:
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    temporary.replace(path)


class RteClient:
    def __init__(self, settings: Settings, client: aiohttp.ClientSession):
        if settings.concurrency < 1 or settings.retries < 0:
            raise ValueError("Concurrence >= 1 et reprises >= 0 requises.")
        self.settings = settings
        self.client = client
        self.semaphore = asyncio.Semaphore(settings.concurrency)

    async def request(self, params: dict) -> dict:
        for attempt in range(self.settings.retries + 1):
            try:
                async with self.semaphore:
                    async with self.client.get(
                        self.settings.records_url, params=params
                    ) as response:
                        response.raise_for_status()
                        return await response.json()
            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                if isinstance(exc, aiohttp.ClientResponseError):
                    if exc.status not in (408, 429, 500, 502, 503, 504):
                        raise
                if attempt == self.settings.retries:
                    raise
                delay = 2.0**attempt
                if isinstance(exc, aiohttp.ClientResponseError) and exc.headers:
                    try:
                        delay = min(60, max(delay, float(exc.headers.get("Retry-After", ""))))
                    except ValueError:
                        pass
                LOGGER.warning("Échec API, nouvelle tentative dans %.1f s", delay)
                await asyncio.sleep(delay)
        raise RuntimeError("Nombre de tentatives épuisé.")

    async def latest_measured_day(self) -> date:
        payload = await self.request(
            {
                "where": "perimetre = 'France' AND consommation IS NOT NULL",
                "order_by": "date_heure DESC",
                "limit": 1,
            }
        )
        if not payload["results"]:
            raise ValueError("Aucune consommation mesurée disponible auprès de RTE.")
        return datetime.fromisoformat(payload["results"][0]["date_heure"]).astimezone(PARIS).date()

    async def collect_day(self, day: date, run_dir: Path, state: dict, checkpoint: Path) -> None:
        start, end = day_bounds(day)
        params = {
            "where": f"perimetre = 'France' AND date_heure >= '{start}' AND date_heure < '{end}'",
            "order_by": "date_heure ASC",
            "limit": PAGE_SIZE,
            "offset": 0,
        }
        while True:
            name = f"{day.isoformat()}_{params['offset']:05d}.jsonl"
            previous = state["pages"].get(name)
            if previous:
                if file_hash(run_dir / name) != previous["sha256"]:
                    raise ValueError(f"Archive brute modifiée : {name}.")
                rows_count = previous["rows"]
                total = previous["total_count"]
            else:
                payload = await self.request(params)
                rows = payload["results"]
                total = payload["total_count"]
                if not isinstance(rows, list) or type(total) is not int or total < 0:
                    raise ValueError("Réponse API invalide.")
                same_day = [
                    page
                    for page in state["pages"].values()
                    if page["file"].startswith(day.isoformat())
                ]
                if same_day and total != same_day[0]["total_count"]:
                    raise ValueError(
                        "Les données API ont changé pendant la pagination : utiliser --refresh."
                    )
                if total > 10_000 or len(rows) > PAGE_SIZE or params["offset"] + len(rows) > total:
                    raise ValueError("Pagination incohérente ou limite API dépassée.")
                if not rows and params["offset"] < total:
                    raise ValueError("Pagination interrompue avant la fin des résultats.")
                content = "".join(
                    json.dumps(row, ensure_ascii=False, sort_keys=True, allow_nan=False) + "\n"
                    for row in rows
                ).encode("utf-8")
                destination = run_dir / name
                if destination.exists():
                    # Crash après l'écriture et avant le checkpoint : ne pas écraser.
                    if file_hash(destination) != hashlib.sha256(content).hexdigest():
                        raise ValueError("Page orpheline différente : utiliser --refresh.")
                else:
                    temporary = destination.with_suffix(".part")
                    temporary.write_bytes(content)
                    temporary.replace(destination)
                rows_count = len(rows)
                state["pages"][name] = {
                    "file": name,
                    "sha256": hashlib.sha256(content).hexdigest(),
                    "rows": rows_count,
                    "total_count": total,
                    "params": dict(params),
                }
                save_state(checkpoint, state)
            params["offset"] += rows_count
            if params["offset"] >= total:
                return
            if rows_count == 0:
                raise ValueError("Pagination interrompue.")


async def ingest(
    settings: Settings,
    start: date | None = None,
    end: date | None = None,
    days: int = 7,
    client: aiohttp.ClientSession | None = None,
    refresh: bool = False,
    resume: Path | None = None,
) -> Path:
    """Même période = même snapshot ; --refresh en crée un nouveau, --resume le reprend."""
    if not 1 <= days <= 366:
        raise ValueError("days doit être compris entre 1 et 366.")
    if (start is None) != (end is None):
        raise ValueError("Fournir start et end ensemble.")
    if resume:
        if start or end or refresh:
            raise ValueError("--resume ne se combine pas avec --start, --end ou --refresh.")
        saved = json.loads(resume.read_text(encoding="utf-8"))
        if saved["source"] != settings.records_url:
            raise ValueError("Le checkpoint ne correspond pas à la source configurée.")
        start, end = date.fromisoformat(saved["start"]), date.fromisoformat(saved["end_exclusive"])
    if client is None:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as session:
            return await ingest(
                settings,
                start if not resume else None,
                end if not resume else None,
                days,
                session,
                refresh,
                resume,
            )
    api = RteClient(settings, client)
    if start is None:
        end = await api.latest_measured_day() + timedelta(days=1)
        start = end - timedelta(days=days)
    if not 1 <= (end - start).days <= 366:
        raise ValueError("La période doit couvrir de 1 à 366 jours.")
    identity = f"{settings.records_url}|{start}|{end}"
    key = hashlib.sha256(identity.encode()).hexdigest()[:16]
    checkpoint_root = settings.data_dir / "checkpoints"
    checkpoint_root.mkdir(parents=True, exist_ok=True)
    checkpoint = checkpoint_root / f"{key}.json"
    if resume and resume.resolve() != checkpoint.resolve():
        raise ValueError("Checkpoint hors du dossier de données configuré.")
    with FileLock(str(checkpoint.with_suffix(".lock")), timeout=0):
        state = json.loads(checkpoint.read_text(encoding="utf-8")) if checkpoint.exists() else None
        if state and state["status"] == "complete" and not refresh:
            run_dir = settings.data_dir / "raw" / state["run_id"]
            read_manifest(run_dir)
            return run_dir
        if state is None or refresh:
            generation = state["generation"] + 1 if state else 1
            run_id = f"{settings.dataset}_{start}_{end}_{key}_v{generation:03d}"
            run_dir = settings.data_dir / "raw" / run_id
            run_dir.mkdir(parents=True, exist_ok=False)
            state = {
                "format_version": 2,
                "run_id": run_id,
                "generation": generation,
                "source": settings.records_url,
                "dataset": settings.dataset,
                "retrieved_at": datetime.now(UTC).isoformat(),
                "start": start.isoformat(),
                "end_exclusive": end.isoformat(),
                "timezone": "Europe/Paris",
                "status": "running",
                "pages": {},
            }
            save_state(checkpoint, state)
        run_dir = settings.data_dir / "raw" / state["run_id"]
        tasks = [
            asyncio.create_task(
                api.collect_day(start + timedelta(days=i), run_dir, state, checkpoint)
            )
            for i in range((end - start).days)
        ]
        try:
            await asyncio.gather(*tasks)
            pages = sorted(state["pages"].values(), key=lambda page: page["file"])
            raw_rows = sum(page["rows"] for page in pages)
            if not raw_rows:
                raise ValueError("Aucune donnée disponible pour cette période.")
            manifest = {key: value for key, value in state.items() if key != "pages"}
            manifest.update(status="complete", pages=pages, raw_rows=raw_rows)
            if not (run_dir / "brut.xlsx").exists():
                await asyncio.to_thread(
                    write_records_excel,
                    lambda: iter_records(run_dir, manifest),
                    run_dir / "brut.xlsx",
                )
            manifest["excel_sha256"] = file_hash(run_dir / "brut.xlsx")
            # Le manifeste final est écrit une seule fois. Le checkpoint mutable reste ailleurs.
            if not (run_dir / "manifest.json").exists():
                with (run_dir / "manifest.json").open("x", encoding="utf-8") as output:
                    json.dump(manifest, output, ensure_ascii=False, indent=2)
            state.update(status="complete", raw_rows=raw_rows)
            save_state(checkpoint, state)
        except BaseException:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            state["status"] = "interrupted"
            save_state(checkpoint, state)
            raise
    LOGGER.info("Archive brute : %s (%d lignes)", run_dir, raw_rows)
    return run_dir
