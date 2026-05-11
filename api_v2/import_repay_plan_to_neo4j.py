from __future__ import annotations

import os
from typing import Dict, List, Optional

from dotenv import load_dotenv
from neo4j import GraphDatabase, Session, Transaction

from import_utils import (
    execute_write,
    first_existing_value,
    list_csv_files,
    normalize_text,
    parse_iso_datetime,
    read_csv_chunks,
)


load_dotenv()

DEFAULT_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
DEFAULT_USER = os.getenv("NEO4J_USER", "neo4j")
DEFAULT_PASSWORD = os.getenv("NEO4J_PASSWORD", "kBXwIuxLTvgxnbGD")
DEFAULT_DB = "dev"

REPAY_PLAN_ROOT = os.getenv("REPAY_PLAN_ROOT", "")

READ_CHUNK_SIZE = 20000
BATCH_SIZE = 5000
PROGRESS_INTERVAL = 50000

TIME_COLUMNS = ("modify_date",)
ORDER_ID_COLUMNS = ("order_id",)
PLAN_ID_COLUMNS = ("id",)

CONSTRAINT_QUERIES = [
    "CREATE CONSTRAINT order_key IF NOT EXISTS FOR (n:order) REQUIRE n.key IS UNIQUE",
    "CREATE CONSTRAINT repay_plan_key IF NOT EXISTS FOR (n:repay_plan) REQUIRE n.key IS UNIQUE",
]


BATCH_QUERY = """
UNWIND $rows AS row
WITH row, datetime(row.ts) AS row_ts
MERGE (o:order {key: row.order_id})
ON CREATE SET o.created_at = row_ts, o.updated_at = row_ts
ON MATCH SET
    o.updated_at = CASE
        WHEN o.updated_at IS NULL OR row_ts > o.updated_at THEN row_ts
        ELSE o.updated_at
    END
MERGE (rp:repay_plan {key: row.repay_plan_id})
ON CREATE SET
    rp.created_at = row_ts,
    rp.updated_at = row_ts,
    rp.due_date = CASE WHEN row.current_repay_date IS NULL THEN NULL ELSE datetime(row.current_repay_date) END,
    rp.repayed_date = CASE WHEN row.repayed_date IS NULL THEN NULL ELSE datetime(row.repayed_date) END,
    rp.early_mark = row.early_repay_mark,
    rp.overdue_mark = row.overdue_mark,
    rp.overdue_days = row.overdue_days,
    rp.status = row.current_repay_status
ON MATCH SET
    rp.updated_at = CASE
        WHEN rp.updated_at IS NULL OR row_ts > rp.updated_at THEN row_ts
        ELSE rp.updated_at
    END,
    rp.due_date = CASE
        WHEN row.current_repay_date IS NULL THEN rp.due_date
        WHEN rp.due_date IS NULL OR datetime(row.current_repay_date) > rp.due_date THEN datetime(row.current_repay_date)
        ELSE rp.due_date
    END,
    rp.repayed_date = CASE
        WHEN row.repayed_date IS NULL THEN rp.repayed_date
        WHEN rp.repayed_date IS NULL OR datetime(row.repayed_date) > rp.repayed_date THEN datetime(row.repayed_date)
        ELSE rp.repayed_date
    END,
    rp.early_mark = coalesce(row.early_repay_mark, rp.early_mark),
    rp.overdue_mark = coalesce(row.overdue_mark, rp.overdue_mark),
    rp.overdue_days = coalesce(row.overdue_days, rp.overdue_days),
    rp.status = coalesce(row.current_repay_status, rp.status)
MERGE (o)-[r:REPAY_PLAN]->(rp)
ON CREATE SET r.first_seen_ts = row_ts, r.last_seen_ts = row_ts, r.event_count = 1
ON MATCH SET
    r.last_seen_ts = CASE
        WHEN r.last_seen_ts IS NULL OR row_ts > r.last_seen_ts THEN row_ts
        ELSE r.last_seen_ts
    END,
    r.event_count = coalesce(r.event_count, 0) + 1
"""


def transform_row(row) -> Optional[Dict[str, Optional[str]]]:
    ts = parse_iso_datetime(first_existing_value(row, TIME_COLUMNS))
    order_id = normalize_text(first_existing_value(row, ORDER_ID_COLUMNS))
    repay_plan_id = normalize_text(first_existing_value(row, PLAN_ID_COLUMNS))

    if ts is None or order_id is None or repay_plan_id is None:
        return None

    return {
        "ts": ts,
        "order_id": order_id,
        "repay_plan_id": repay_plan_id,
        "current_repay_date": parse_iso_datetime(row.get("current_repay_date")),
        "repayed_date": parse_iso_datetime(row.get("repayed_date")),
        "early_repay_mark": normalize_text(row.get("early_repay_mark")),
        "overdue_mark": normalize_text(row.get("overdue_mark")),
        "overdue_days": normalize_text(row.get("overdue_days")),
        "current_repay_status": normalize_text(row.get("current_repay_status")),
    }


def create_constraints(session: Session) -> None:
    for query in CONSTRAINT_QUERIES:
        session.run(query)


def write_batch(tx: Transaction, rows: List[Dict[str, Optional[str]]]) -> None:
    tx.run(BATCH_QUERY, rows=rows)


def import_repay_plan(root: str, database: str) -> Dict[str, int]:
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
                                print(f"[repay_plan] written={written_rows} total={total_rows} skipped={skipped_rows}")
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
    if REPAY_PLAN_ROOT == "":
        raise ValueError("REPAY_PLAN_ROOT is required")
    result = import_repay_plan(REPAY_PLAN_ROOT, DEFAULT_DB)
    print(
        "[repay_plan] done "
        f"files={result['files']} total={result['total_rows']} "
        f"written={result['written_rows']} skipped={result['skipped_rows']}"
    )


if __name__ == "__main__":
    main()
