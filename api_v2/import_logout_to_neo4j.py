from __future__ import annotations

import os
from typing import Dict, List, Optional

from dotenv import load_dotenv
from neo4j import GraphDatabase, Session, Transaction

from import_utils import (
    execute_write,
    extract_uid_key,
    first_existing_value,
    list_csv_files,
    normalize_for_key,
    parse_iso_datetime,
    read_csv_chunks,
)


load_dotenv()

DEFAULT_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
DEFAULT_USER = os.getenv("NEO4J_USER", "neo4j")
DEFAULT_PASSWORD = os.getenv("NEO4J_PASSWORD", "kBXwIuxLTvgxnbGD")
DEFAULT_DB = "dev"

LOGOUT_ROOT = os.getenv("LOGOUT_ROOT", "")

READ_CHUNK_SIZE = 20000
BATCH_SIZE = 5000
PROGRESS_INTERVAL = 50000

TIME_COLUMNS = ("create_date",)
UID_COLUMNS = ("cid",)
IDENTITY_COLUMNS = ("identity_no",)
PHONE_COLUMNS = ("mobile_phone",)

CONSTRAINT_QUERIES = [
    "CREATE CONSTRAINT uid_key IF NOT EXISTS FOR (n:uid) REQUIRE n.key IS UNIQUE",
    "CREATE CONSTRAINT identity_no_key IF NOT EXISTS FOR (n:identity_no) REQUIRE n.key IS UNIQUE",
    "CREATE CONSTRAINT phone_num_key IF NOT EXISTS FOR (n:phone_num) REQUIRE n.key IS UNIQUE",
]


BATCH_QUERY = """
UNWIND $rows AS row
WITH row, datetime(row.ts) AS row_ts
MERGE (u:uid {key: row.uid})
ON CREATE SET
    u.created_at = row_ts,
    u.updated_at = row_ts,
    u.logout_flag = true
ON MATCH SET
    u.updated_at = CASE
        WHEN u.updated_at IS NULL OR row_ts > u.updated_at THEN row_ts
        ELSE u.updated_at
    END,
    u.logout_flag = true
FOREACH (_ IN CASE WHEN row.identity_no IS NULL THEN [] ELSE [1] END |
    MERGE (i:identity_no {key: row.identity_no})
    ON CREATE SET
        i.created_at = row_ts,
        i.updated_at = row_ts,
        i.logout_count = 1
    ON MATCH SET
        i.updated_at = CASE
            WHEN i.updated_at IS NULL OR row_ts > i.updated_at THEN row_ts
            ELSE i.updated_at
        END,
        i.logout_count = coalesce(i.logout_count, 0) + 1
)
FOREACH (_ IN CASE WHEN row.phone_num IS NULL THEN [] ELSE [1] END |
    MERGE (p:phone_num {key: row.phone_num})
    ON CREATE SET
        p.created_at = row_ts,
        p.updated_at = row_ts,
        p.logout_count = 1
    ON MATCH SET
        p.updated_at = CASE
            WHEN p.updated_at IS NULL OR row_ts > p.updated_at THEN row_ts
            ELSE p.updated_at
        END,
        p.logout_count = coalesce(p.logout_count, 0) + 1
)
"""


def transform_row(row) -> Optional[Dict[str, Optional[str]]]:
    uid = extract_uid_key(row, UID_COLUMNS)
    ts = parse_iso_datetime(first_existing_value(row, TIME_COLUMNS))
    if uid is None or ts is None:
        return None
    return {
        "uid": uid,
        "identity_no": normalize_for_key(first_existing_value(row, IDENTITY_COLUMNS)),
        "phone_num": normalize_for_key(first_existing_value(row, PHONE_COLUMNS)),
        "ts": ts,
    }


def create_constraints(session: Session) -> None:
    for query in CONSTRAINT_QUERIES:
        session.run(query)


def write_batch(tx: Transaction, rows: List[Dict[str, Optional[str]]]) -> None:
    tx.run(BATCH_QUERY, rows=rows)


def import_logout(logout_root: str, database: str) -> Dict[str, int]:
    files = list_csv_files(logout_root)
    if not files:
        raise FileNotFoundError(f"No CSV files found under: {logout_root}")

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
                                print(f"[logout] written={written_rows} total={total_rows} skipped={skipped_rows}")
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
    if LOGOUT_ROOT == "":
        raise ValueError("LOGOUT_ROOT is required")
    result = import_logout(LOGOUT_ROOT, DEFAULT_DB)
    print(
        "[logout] done "
        f"files={result['files']} total={result['total_rows']} "
        f"written={result['written_rows']} skipped={result['skipped_rows']}"
    )


if __name__ == "__main__":
    main()
