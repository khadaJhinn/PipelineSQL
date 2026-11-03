"""Configuration locale sans identifiants dans le code."""

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]


@dataclass(frozen=True)
class Settings:
    data_dir: Path = ROOT / "data"
    database: Path = ROOT / "data/electricity.sqlite"
    api_base: str = "https://odre.opendatasoft.com/api/explore/v2.1/catalog/datasets"
    dataset: str = "eco2mix-national-tr"
    concurrency: int = 4
    retries: int = 3

    @property
    def records_url(self) -> str:
        return f"{self.api_base.rstrip('/')}/{self.dataset}/records"

    @classmethod
    def from_env(cls) -> "Settings":
        load_dotenv(ROOT / ".env")
        database = Path(os.getenv("SQLITE_PATH", "data/electricity.sqlite"))
        return cls(
            database=database if database.is_absolute() else ROOT / database,
            api_base=os.getenv("RTE_API_BASE", cls.api_base),
            dataset=os.getenv("RTE_DATASET", cls.dataset),
        )
