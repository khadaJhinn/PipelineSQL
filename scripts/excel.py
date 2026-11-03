"""Exports Excel : dates explicites et chaînes conservées comme du texte."""

from pathlib import Path

import pandas as pd


def write_excel(frame: pd.DataFrame, path: Path) -> None:
    """Évite la limite Excel par feuille et l'interprétation des chaînes en formules."""
    if path.exists():
        raise FileExistsError(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        for index, offset in enumerate(range(0, max(len(frame), 1), 1_000_000), start=1):
            frame.iloc[offset : offset + 1_000_000].to_excel(
                writer, sheet_name=f"donnees_{index}", index=False
            )
            sheet = writer.sheets[f"donnees_{index}"]
            sheet.freeze_panes = "A2"
            sheet.auto_filter.ref = sheet.dimensions
            for row in sheet.iter_rows():
                for cell in row:
                    if isinstance(cell.value, str):
                        cell.data_type = "s"


def records_frame(records: list[dict]) -> pd.DataFrame:
    """Garde les valeurs numériques JSON et les champs absents dans Excel."""
    return pd.DataFrame(records, dtype=object)
