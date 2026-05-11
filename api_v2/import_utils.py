from __future__ import annotations

import re
from pathlib import Path
from typing import Iterable, List, Optional, Sequence

import pandas as pd


DATE_TOKEN_RE = re.compile(r"(\d{4})[-_]?([01]\d)[-_]?([0-3]\d)")
UID_KEY_CANDIDATES = ("uid", "cid", "id", "baseid", "cif_user_id", "user_id", "c_customer_no")


def normalize_text(value: object) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    if text == "":
        return None
    if text.lower() in {"nan", "none", "null"}:
        return None
    return text


def normalize_for_key(value: object) -> Optional[str]:
    text = normalize_text(value)
    if text is None:
        return None
    return text.lower()


def parse_iso_datetime(value: object) -> Optional[str]:
    text = normalize_text(value)
    if text is None:
        return None
    ts = pd.to_datetime(text, errors="coerce")
    if pd.isna(ts):
        return None
    return ts.isoformat()


def first_existing_value(row: pd.Series, candidates: Iterable[str]) -> object:
    for col in candidates:
        if col in row:
            return row.get(col)
    return None


def extract_uid_key(row: pd.Series, candidates: Iterable[str] = UID_KEY_CANDIDATES) -> Optional[str]:
    return normalize_for_key(first_existing_value(row, candidates))


def extract_sort_key(root: Path, file_path: Path) -> tuple:
    rel = str(file_path.relative_to(root)) if root.is_dir() else file_path.name
    match = DATE_TOKEN_RE.search(rel)
    if match is None:
        return (9999, 99, 99, rel)
    return (int(match.group(1)), int(match.group(2)), int(match.group(3)), rel)


def normalize_col_name(value: object) -> str:
    return str(value).replace("\ufeff", "").strip().lower()


def list_csv_files(path_like: str) -> List[Path]:
    source = Path(path_like)
    if source.is_file():
        return [source]
    if not source.exists() or not source.is_dir():
        raise FileNotFoundError(f"Input path not found: {source}")

    files = [p for p in source.rglob("*.csv") if p.is_file()]
    files.sort(key=lambda p: extract_sort_key(source, p))
    return files


def read_csv_chunks(csv_path: Path, chunk_size: int) -> Iterable[pd.DataFrame]:
    chunks = pd.read_csv(
        csv_path,
        dtype=str,
        keep_default_na=False,
        na_filter=False,
        chunksize=chunk_size,
        encoding="utf-8-sig",
    )
    for chunk in chunks:
        chunk.columns = [normalize_col_name(col) for col in chunk.columns]
        yield chunk


def execute_write(session, write_batch, rows: Sequence[dict]) -> None:
    write_fn = getattr(session, "execute_write", None)
    if callable(write_fn):
        write_fn(write_batch, rows)
    else:
        session.write_transaction(write_batch, rows)  # type: ignore[attr-defined]
