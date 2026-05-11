from __future__ import annotations

import os
from pathlib import Path
from typing import Dict, List, Optional

from dotenv import load_dotenv
from neo4j import GraphDatabase, Session, Transaction

from import_utils import (
    execute_write,
    extract_sort_key,
    normalize_for_key,
    parse_iso_datetime,
    read_csv_chunks,
)


load_dotenv()

DEFAULT_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
DEFAULT_USER = os.getenv("NEO4J_USER", "neo4j")
DEFAULT_PASSWORD = os.getenv("NEO4J_PASSWORD", "kBXwIuxLTvgxnbGD")
DEFAULT_DB = "dev"

LINKMAN_ROOTS = [
    os.getenv("FIRST_LINKMAN_ROOT", ""),
    os.getenv("SECOND_LINKMAN_ROOT", ""),
    os.getenv("FIRST_LINKMAN_DERIVED_ROOT", ""),
    os.getenv("SECOND_LINKMAN_DERIVED_ROOT", ""),
]

READ_CHUNK_SIZE = 20000
BATCH_SIZE = 5000
PROGRESS_INTERVAL = 50000

CONSTRAINT_QUERIES = [
    "CREATE CONSTRAINT uid_key IF NOT EXISTS FOR (n:uid) REQUIRE n.key IS UNIQUE",
    "CREATE CONSTRAINT phone_num_key IF NOT EXISTS FOR (n:phone_num) REQUIRE n.key IS UNIQUE",
]


BATCH_QUERY = """
UNWIND $rows AS row
WITH row, datetime(row.ts) AS row_ts
MERGE (u:uid {key: row.uid})
ON CREATE SET u.created_at = row_ts, u.updated_at = row_ts, u.logout_flag = false
ON MATCH SET
    u.updated_at = CASE
        WHEN u.updated_at IS NULL OR row_ts > u.updated_at THEN row_ts
        ELSE u.updated_at
    END,
    u.logout_flag = coalesce(u.logout_flag, false)
MERGE (p:phone_num {key: row.phone_num})
ON CREATE SET p.created_at = row_ts, p.updated_at = row_ts, p.logout_count = 0
ON MATCH SET
    p.updated_at = CASE
        WHEN p.updated_at IS NULL OR row_ts > p.updated_at THEN row_ts
        ELSE p.updated_at
    END,
    p.logout_count = coalesce(p.logout_count, 0)
MERGE (u)-[r:HAS_CONTACT_PHONE]->(p)
ON CREATE SET r.first_seen_ts = row_ts, r.last_seen_ts = row_ts, r.event_count = 1
ON MATCH SET
    r.last_seen_ts = CASE
        WHEN r.last_seen_ts IS NULL OR row_ts > r.last_seen_ts THEN row_ts
        ELSE r.last_seen_ts
    END,
    r.event_count = coalesce(r.event_count, 0) + 1
"""


def list_linkman_csv_files(roots: List[str]) -> List[Path]:
    files: List[Path] = []
    pseudo_root = Path("/")
    for root in roots:
        if not root:
            continue
        p = Path(root)
        if not p.exists() or not p.is_dir():
            continue
        files.extend([f for f in p.rglob("*.csv") if f.is_file()])
    files.sort(key=lambda x: extract_sort_key(pseudo_root, x))
    return files


def pick_time(row) -> Optional[str]:
    for col in ("create_date", "modify_date", "ts"):
        if col in row:
            ts = parse_iso_datetime(row.get(col))
            if ts is not None:
                return ts
    return None


def pick_phone(row) -> Optional[str]:
    for col in ("mobile_phone", "second_mobile_phone", "phone_num"):
        if col in row:
            value = normalize_for_key(row.get(col))
            if value is not None:
                return value
    return None


def transform_row(row) -> Optional[Dict[str, Optional[str]]]:
    uid = normalize_for_key(row.get("cid"))
    phone = pick_phone(row)
    ts = pick_time(row)
    if uid is None or phone is None or ts is None:
        return None
    return {"uid": uid, "phone_num": phone, "ts": ts}


def create_constraints(session: Session) -> None:
    for query in CONSTRAINT_QUERIES:
        session.run(query)


def write_batch(tx: Transaction, rows: List[Dict[str, Optional[str]]]) -> None:
    tx.run(BATCH_QUERY, rows=rows)


def import_linkman(roots: List[str], database: str) -> Dict[str, int]:
    files = list_linkman_csv_files(roots)
    if not files:
        raise FileNotFoundError(f"No CSV files found under roots: {roots}")

    driver = GraphDatabase.driver(DEFAULT_URI, auth=(DEFAULT_USER, DEFAULT_PASSWORD))
    total_rows = 0
    skipped_rows = 0
    written_rows = 0
    pending_rows: List[Dict[str, Optional[str]]] = []

    try:
        with driver.session(database=database) as session:
            create_constraints(session)

            for csv_path in files:
                for chunk in read_csv_chunks(csv_path, READ_CHUNK_SIZE):
                    for _, row in chunk.iterrows():
                        total_rows += 1
                        rec = transform_row(row)
                        if rec is None:
                            skipped_rows += 1
                            continue
                        pending_rows.append(rec)

                        if len(pending_rows) >= BATCH_SIZE:
                            execute_write(session, write_batch, pending_rows)
                            written_rows += len(pending_rows)
                            if written_rows % PROGRESS_INTERVAL == 0:
                                print(f"[linkman] written={written_rows} total={total_rows} skipped={skipped_rows}")
                            pending_rows = []

            if pending_rows:
                execute_write(session, write_batch, pending_rows)
                written_rows += len(pending_rows)
    finally:
        driver.close()

    return {
        "files": len(files),
        "total_rows": total_rows,
        "written_rows": written_rows,
        "skipped_rows": skipped_rows,
    }


def main() -> None:
    if not any(LINKMAN_ROOTS):
        raise ValueError("At least one linkman root is required")
    result = import_linkman(LINKMAN_ROOTS, DEFAULT_DB)
    print(
        "[linkman] done "
        f"files={result['files']} total={result['total_rows']} "
        f"written={result['written_rows']} skipped={result['skipped_rows']}"
    )


if __name__ == "__main__":
    main()
