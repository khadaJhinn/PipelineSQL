"""Exports Excel en écriture continue, sans charger le classeur en mémoire."""

import json
from collections.abc import Callable, Iterable
from pathlib import Path
from uuid import uuid4

import pandas as pd
from openpyxl import Workbook
from openpyxl.cell import WriteOnlyCell


def write_records_excel(
    records: Callable[[], Iterable[dict]], path: Path, columns: list[str] | None = None
) -> int:
    """Deux passages bornés : découverte des colonnes, puis écriture des lignes."""
    if path.exists():
        raise FileExistsError(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    if columns is None:
        columns = list(dict.fromkeys(key for row in records() for key in row))
    workbook = Workbook(write_only=True)
    temporary = path.with_name(f".{uuid4().hex}.xlsx")
    count = 0
    sheet = None
    try:
        for record in records():
            if count % 1_000_000 == 0:
                sheet = workbook.create_sheet(f"donnees_{count // 1_000_000 + 1}")
                sheet.freeze_panes = "A2"
                sheet.append(columns)
            values = []
            for name in columns:
                value = record.get(name)
                if isinstance(value, (dict, list)):
                    value = json.dumps(value, ensure_ascii=False)
                if isinstance(value, str):
                    cell = WriteOnlyCell(sheet, value=value)
                    cell.data_type = "s"
                    values.append(cell)
                else:
                    values.append(value)
            sheet.append(values)
            count += 1
        if sheet is None:
            workbook.create_sheet("donnees_1").append(columns)
        workbook.save(temporary)
        temporary.replace(path)
    finally:
        workbook.close()
        temporary.unlink(missing_ok=True)
    return count


def write_excel(frame: pd.DataFrame, path: Path) -> None:
    write_records_excel(
        lambda: (
            dict(zip(frame.columns, row, strict=True))
            for row in frame.itertuples(index=False, name=None)
        ),
        path,
        list(frame.columns),
    )


def records_frame(records: list[dict]) -> pd.DataFrame:
    return pd.DataFrame(records, dtype=object)
