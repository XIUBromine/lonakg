from __future__ import annotations

import os
from typing import Dict, List, Optional

from dotenv import load_dotenv
from neo4j import GraphDatabase, Session, Transaction

from import_utils import (
    execute_write,
    list_csv_files,
    normalize_for_key,
    normalize_text,
    parse_iso_datetime,
    read_csv_chunks,
)


load_dotenv()

DEFAULT_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
DEFAULT_USER = os.getenv("NEO4J_USER", "neo4j")
DEFAULT_PASSWORD = os.getenv("NEO4J_PASSWORD", "kBXwIuxLTvgxnbGD")
DEFAULT_DB = "dev"

CONSUMER_CASE_ROOT = os.getenv("CONSUMER_CASE_ROOT", "")

READ_CHUNK_SIZE = 20000
BATCH_SIZE = 5000
PROGRESS_INTERVAL = 50000

CONSTRAINT_QUERIES = [
    "CREATE CONSTRAINT complaint_case_key IF NOT EXISTS FOR (n:complaint_case) REQUIRE n.key IS UNIQUE",
    "CREATE CONSTRAINT identity_no_key IF NOT EXISTS FOR (n:identity_no) REQUIRE n.key IS UNIQUE",
    "CREATE CONSTRAINT phone_num_key IF NOT EXISTS FOR (n:phone_num) REQUIRE n.key IS UNIQUE",
]


BATCH_QUERY = """
UNWIND $rows AS row
WITH row, datetime(row.ts) AS row_ts
MERGE (c:complaint_case {key: row.case_key})
ON CREATE SET c.created_at = row_ts, c.updated_at = row_ts, c.source = 'consumer_protection_case'
ON MATCH SET
    c.updated_at = CASE
        WHEN c.updated_at IS NULL OR row_ts > c.updated_at THEN row_ts
        ELSE c.updated_at
    END
FOREACH (_ IN CASE WHEN row.identity_no IS NULL THEN [] ELSE [1] END |
    MERGE (i:identity_no {key: row.identity_no})
    ON CREATE SET i.created_at = row_ts, i.updated_at = row_ts, i.logout_count = 0
    ON MATCH SET
        i.updated_at = CASE
            WHEN i.updated_at IS NULL OR row_ts > i.updated_at THEN row_ts
            ELSE i.updated_at
        END,
        i.logout_count = coalesce(i.logout_count, 0)
    MERGE (c)-[r:C_HAS_IDENTITY]->(i)
    ON CREATE SET r.first_seen_ts = row_ts, r.last_seen_ts = row_ts, r.event_count = 1
    ON MATCH SET
        r.last_seen_ts = CASE
            WHEN r.last_seen_ts IS NULL OR row_ts > r.last_seen_ts THEN row_ts
            ELSE r.last_seen_ts
        END,
        r.event_count = coalesce(r.event_count, 0) + 1
)
FOREACH (_ IN CASE WHEN row.register_phone IS NULL THEN [] ELSE [1] END |
    MERGE (p:phone_num {key: row.register_phone})
    ON CREATE SET p.created_at = row_ts, p.updated_at = row_ts, p.logout_count = 0
    ON MATCH SET
        p.updated_at = CASE
            WHEN p.updated_at IS NULL OR row_ts > p.updated_at THEN row_ts
            ELSE p.updated_at
        END,
        p.logout_count = coalesce(p.logout_count, 0)
    MERGE (c)-[r:C_REGISTER_PHONE]->(p)
    ON CREATE SET r.first_seen_ts = row_ts, r.last_seen_ts = row_ts, r.event_count = 1
    ON MATCH SET
        r.last_seen_ts = CASE
            WHEN r.last_seen_ts IS NULL OR row_ts > r.last_seen_ts THEN row_ts
            ELSE r.last_seen_ts
        END,
        r.event_count = coalesce(r.event_count, 0) + 1
)
FOREACH (_ IN CASE WHEN row.contact_phone IS NULL THEN [] ELSE [1] END |
    MERGE (p:phone_num {key: row.contact_phone})
    ON CREATE SET p.created_at = row_ts, p.updated_at = row_ts, p.logout_count = 0
    ON MATCH SET
        p.updated_at = CASE
            WHEN p.updated_at IS NULL OR row_ts > p.updated_at THEN row_ts
            ELSE p.updated_at
        END,
        p.logout_count = coalesce(p.logout_count, 0)
    MERGE (c)-[r:C_CONTACT_PHONE]->(p)
    ON CREATE SET r.first_seen_ts = row_ts, r.last_seen_ts = row_ts, r.event_count = 1
    ON MATCH SET
        r.last_seen_ts = CASE
            WHEN r.last_seen_ts IS NULL OR row_ts > r.last_seen_ts THEN row_ts
            ELSE r.last_seen_ts
        END,
        r.event_count = coalesce(r.event_count, 0) + 1
)
"""


def transform_row(row) -> Optional[Dict[str, Optional[str]]]:
    ts = parse_iso_datetime(row.get("d_update"))
    case_id = normalize_text(row.get("c_id"))
    if ts is None or case_id is None:
        return None

    return {
        "ts": ts,
        "case_key": f"consumer:{case_id}",
        "identity_no": normalize_for_key(row.get("c_identity_no")),
        "register_phone": normalize_for_key(row.get("c_register_phone_no")),
        "contact_phone": normalize_for_key(row.get("c_contact_phone_no")),
    }


def create_constraints(session: Session) -> None:
    for query in CONSTRAINT_QUERIES:
        session.run(query)


def write_batch(tx: Transaction, rows: List[Dict[str, Optional[str]]]) -> None:
    tx.run(BATCH_QUERY, rows=rows)


def import_consumer_case(root: str, database: str) -> Dict[str, int]:
    files = list_csv_files(root)
    if not files:
        raise FileNotFoundError(f"No CSV files found under: {root}")

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
                                print(f"[consumer_case] written={written_rows} total={total_rows} skipped={skipped_rows}")
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
    if CONSUMER_CASE_ROOT == "":
        raise ValueError("CONSUMER_CASE_ROOT is required")
    result = import_consumer_case(CONSUMER_CASE_ROOT, DEFAULT_DB)
    print(
        "[consumer_case] done "
        f"files={result['files']} total={result['total_rows']} "
        f"written={result['written_rows']} skipped={result['skipped_rows']}"
    )


if __name__ == "__main__":
    main()
