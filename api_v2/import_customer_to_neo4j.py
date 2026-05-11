from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import pandas as pd
from dotenv import load_dotenv
from neo4j import GraphDatabase, Session, Transaction


load_dotenv()

DEFAULT_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
DEFAULT_USER = os.getenv("NEO4J_USER", "neo4j")
DEFAULT_PASSWORD = os.getenv("NEO4J_PASSWORD", "kBXwIuxLTvgxnbGD")
DEFAULT_DB = "dev"

# v2 数据目录，支持目录递归读取或单文件导入。
CUSTOMER_LOG_ROOT = os.getenv("CUSTOMER_LOG_ROOT", "")

READ_CHUNK_SIZE = 20000
BATCH_SIZE = 5000
PROGRESS_INTERVAL = 50000

TIME_COLUMNS = ("log_create_date",)
UID_COLUMNS = ("baseid",)
IDENTITY_COLUMNS = ("identity_no",)
PHONE_COLUMNS = ("mobile_phone",)

CONSTRAINT_QUERIES = [
    "CREATE CONSTRAINT uid_key IF NOT EXISTS FOR (n:uid) REQUIRE n.key IS UNIQUE",
    "CREATE CONSTRAINT identity_no_key IF NOT EXISTS FOR (n:identity_no) REQUIRE n.key IS UNIQUE",
    "CREATE CONSTRAINT phone_num_key IF NOT EXISTS FOR (n:phone_num) REQUIRE n.key IS UNIQUE",
]


BATCH_QUERY = """
UNWIND $rows AS row
WITH row
ORDER BY row.ts
WITH row, datetime(row.ts) AS row_ts
MERGE (u:uid {key: row.uid})
ON CREATE SET
    u.created_at = row_ts,
    u.updated_at = row_ts,
    u.last_update_ts = row_ts,
    u.latest_identity = row.identity_no,
    u.latest_phone = row.phone_num,
    u.logout_flag = false
ON MATCH SET
    u.updated_at = CASE
        WHEN u.updated_at IS NULL OR row_ts > u.updated_at THEN row_ts
        ELSE u.updated_at
    END,
    u.last_update_ts = CASE
        WHEN u.last_update_ts IS NULL OR row_ts > u.last_update_ts THEN row_ts
        ELSE u.last_update_ts
    END,
    u.logout_flag = coalesce(u.logout_flag, false)
WITH u, row, row_ts,
     coalesce(u.latest_identity, '__NULL__') <> coalesce(row.identity_no, '__NULL__') AS identity_changed,
     coalesce(u.latest_phone, '__NULL__') <> coalesce(row.phone_num, '__NULL__') AS phone_changed
FOREACH (_ IN CASE WHEN identity_changed THEN [1] ELSE [] END |
    SET u.latest_identity = row.identity_no
)
FOREACH (_ IN CASE WHEN phone_changed THEN [1] ELSE [] END |
    SET u.latest_phone = row.phone_num
)
FOREACH (_ IN CASE WHEN identity_changed AND row.identity_no IS NOT NULL THEN [1] ELSE [] END |
    MERGE (i:identity_no {key: row.identity_no})
    ON CREATE SET
        i.created_at = row_ts,
        i.updated_at = row_ts,
        i.logout_count = 0
    ON MATCH SET
        i.updated_at = CASE
            WHEN i.updated_at IS NULL OR row_ts > i.updated_at THEN row_ts
            ELSE i.updated_at
        END,
        i.logout_count = coalesce(i.logout_count, 0)
    MERGE (u)-[r:HAS_IDENTITY]->(i)
    ON CREATE SET
        r.first_seen_ts = row_ts,
        r.last_seen_ts = row_ts,
        r.event_count = 1
    ON MATCH SET
        r.last_seen_ts = CASE
            WHEN r.last_seen_ts IS NULL OR row_ts > r.last_seen_ts THEN row_ts
            ELSE r.last_seen_ts
        END,
        r.event_count = coalesce(r.event_count, 0) + 1
)
FOREACH (_ IN CASE WHEN phone_changed AND row.phone_num IS NOT NULL THEN [1] ELSE [] END |
    MERGE (p:phone_num {key: row.phone_num})
    ON CREATE SET
        p.created_at = row_ts,
        p.updated_at = row_ts,
        p.logout_count = 0
    ON MATCH SET
        p.updated_at = CASE
            WHEN p.updated_at IS NULL OR row_ts > p.updated_at THEN row_ts
            ELSE p.updated_at
        END,
        p.logout_count = coalesce(p.logout_count, 0)
    MERGE (u)-[r:HAS_PHONE]->(p)
    ON CREATE SET
        r.first_seen_ts = row_ts,
        r.last_seen_ts = row_ts,
        r.event_count = 1
    ON MATCH SET
        r.last_seen_ts = CASE
            WHEN r.last_seen_ts IS NULL OR row_ts > r.last_seen_ts THEN row_ts
            ELSE r.last_seen_ts
        END,
        r.event_count = coalesce(r.event_count, 0) + 1
)
"""


DATE_TOKEN_RE = re.compile(r"(\d{4})[-_]?([01]\d)[-_]?([0-3]\d)")


def normalize_text(value: object) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    if text == "":
        return None
    lowered = text.lower()
    if lowered in {"nan", "none", "null"}:
        return None
    return text


def normalize_col_name(value: object) -> str:
    return str(value).replace("\ufeff", "").strip().lower()


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


def transform_row(row: pd.Series) -> Optional[Dict[str, Optional[str]]]:
    uid = normalize_for_key(first_existing_value(row, UID_COLUMNS))
    ts = parse_iso_datetime(first_existing_value(row, TIME_COLUMNS))
    if uid is None or ts is None:
        return None

    return {
        "uid": uid,
        "identity_no": normalize_for_key(first_existing_value(row, IDENTITY_COLUMNS)),
        "phone_num": normalize_for_key(first_existing_value(row, PHONE_COLUMNS)),
        "ts": ts,
    }


def extract_sort_key(root: Path, file_path: Path) -> tuple:
    rel = str(file_path.relative_to(root)) if root.is_dir() else file_path.name
    match = DATE_TOKEN_RE.search(rel)
    if match is None:
        return (9999, 99, 99, rel)
    return (int(match.group(1)), int(match.group(2)), int(match.group(3)), rel)


def list_customer_csv_files(path_like: str) -> List[Path]:
    source = Path(path_like)
    if source.is_file():
        return [source]
    if not source.exists() or not source.is_dir():
        raise FileNotFoundError(f"Input path not found: {source}")

    files = [p for p in source.rglob("*.csv") if p.is_file()]
    files.sort(key=lambda p: extract_sort_key(source, p))
    return files


def create_constraints(session: Session) -> None:
    for query in CONSTRAINT_QUERIES:
        session.run(query)


def write_batch(tx: Transaction, rows: List[Dict[str, Optional[str]]]) -> None:
    tx.run(BATCH_QUERY, rows=rows)


def execute_write(session: Session, rows: List[Dict[str, Optional[str]]]) -> None:
    write_fn = getattr(session, "execute_write", None)
    if callable(write_fn):
        write_fn(write_batch, rows)
    else:
        session.write_transaction(write_batch, rows)  # type: ignore[attr-defined]


def import_customer_logs(customer_log_root: str, database: str) -> Dict[str, int]:
    files = list_customer_csv_files(customer_log_root)
    if not files:
        raise FileNotFoundError(f"No CSV files found under: {customer_log_root}")

    driver = GraphDatabase.driver(DEFAULT_URI, auth=(DEFAULT_USER, DEFAULT_PASSWORD))

    total_rows = 0
    skipped_rows = 0
    written_rows = 0
    skipped_missing_uid = 0
    skipped_missing_ts = 0
    pending_rows: List[Dict[str, Optional[str]]] = []

    try:
        with driver.session(database=database) as session:
            create_constraints(session)

            for csv_path in files:
                csv_iter = pd.read_csv(
                    csv_path,
                    dtype=str,
                    keep_default_na=False,
                    na_filter=False,
                    chunksize=READ_CHUNK_SIZE,
                    encoding="utf-8-sig",
                )

                for chunk in csv_iter:
                    # Align importer behavior with web pre-validation: case-insensitive headers.
                    chunk.columns = [normalize_col_name(col) for col in chunk.columns]
                    for _, row in chunk.iterrows():
                        total_rows += 1
                        uid = normalize_for_key(first_existing_value(row, UID_COLUMNS))
                        ts = parse_iso_datetime(first_existing_value(row, TIME_COLUMNS))
                        if uid is None or ts is None:
                            skipped_rows += 1
                            if uid is None:
                                skipped_missing_uid += 1
                            if ts is None:
                                skipped_missing_ts += 1
                            continue

                        record = {
                            "uid": uid,
                            "identity_no": normalize_for_key(first_existing_value(row, IDENTITY_COLUMNS)),
                            "phone_num": normalize_for_key(first_existing_value(row, PHONE_COLUMNS)),
                            "ts": ts,
                        }

                        pending_rows.append(record)

                        if len(pending_rows) >= BATCH_SIZE:
                            execute_write(session, pending_rows)
                            written_rows += len(pending_rows)
                            if written_rows % PROGRESS_INTERVAL == 0:
                                print(f"[customer] written={written_rows} total={total_rows} skipped={skipped_rows}")
                            pending_rows = []

            if pending_rows:
                execute_write(session, pending_rows)
                written_rows += len(pending_rows)

    finally:
        driver.close()

    return {
        "files": len(files),
        "total_rows": total_rows,
        "written_rows": written_rows,
        "skipped_rows": skipped_rows,
        "skipped_missing_uid": skipped_missing_uid,
        "skipped_missing_ts": skipped_missing_ts,
    }


def main() -> None:
    if CUSTOMER_LOG_ROOT == "":
        raise ValueError("CUSTOMER_LOG_ROOT is required")
    result = import_customer_logs(CUSTOMER_LOG_ROOT, DEFAULT_DB)
    print(
        "[customer] done "
        f"files={result['files']} total={result['total_rows']} "
        f"written={result['written_rows']} skipped={result['skipped_rows']}"
    )


if __name__ == "__main__":
    main()
