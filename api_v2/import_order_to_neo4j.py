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
    normalize_text,
    parse_iso_datetime,
    read_csv_chunks,
)


load_dotenv()

DEFAULT_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
DEFAULT_USER = os.getenv("NEO4J_USER", "neo4j")
DEFAULT_PASSWORD = os.getenv("NEO4J_PASSWORD", "kBXwIuxLTvgxnbGD")
DEFAULT_DB = "dev"

ORDER_ROOT = os.getenv("ORDER_ROOT", "")

READ_CHUNK_SIZE = 20000
BATCH_SIZE = 5000
PROGRESS_INTERVAL = 50000

TIME_COLUMNS = ("create_date",)
ORDER_ID_COLUMNS = ("id",)
UID_COLUMNS = ("user_id",)

CONSTRAINT_QUERIES = [
    "CREATE CONSTRAINT uid_key IF NOT EXISTS FOR (n:uid) REQUIRE n.key IS UNIQUE",
    "CREATE CONSTRAINT identity_no_key IF NOT EXISTS FOR (n:identity_no) REQUIRE n.key IS UNIQUE",
    "CREATE CONSTRAINT phone_num_key IF NOT EXISTS FOR (n:phone_num) REQUIRE n.key IS UNIQUE",
    "CREATE CONSTRAINT card_no_key IF NOT EXISTS FOR (n:card_no) REQUIRE n.key IS UNIQUE",
    "CREATE CONSTRAINT order_key IF NOT EXISTS FOR (n:order) REQUIRE n.key IS UNIQUE",
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
MERGE (o:order {key: row.order_id})
ON CREATE SET o.created_at = row_ts, o.updated_at = row_ts, o.order_status = row.order_status
ON MATCH SET
    o.updated_at = CASE
        WHEN o.updated_at IS NULL OR row_ts > o.updated_at THEN row_ts
        ELSE o.updated_at
    END,
    o.order_status = coalesce(row.order_status, o.order_status)
MERGE (u)-[rpo:PLACED_ORDER]->(o)
ON CREATE SET rpo.first_seen_ts = row_ts, rpo.last_seen_ts = row_ts, rpo.event_count = 1
ON MATCH SET
    rpo.last_seen_ts = CASE
        WHEN rpo.last_seen_ts IS NULL OR row_ts > rpo.last_seen_ts THEN row_ts
        ELSE rpo.last_seen_ts
    END,
    rpo.event_count = coalesce(rpo.event_count, 0) + 1
FOREACH (_ IN CASE WHEN row.apply_ident_no IS NULL THEN [] ELSE [1] END |
    MERGE (i:identity_no {key: row.apply_ident_no})
    ON CREATE SET i.created_at = row_ts, i.updated_at = row_ts, i.logout_count = 0
    ON MATCH SET
        i.updated_at = CASE
            WHEN i.updated_at IS NULL OR row_ts > i.updated_at THEN row_ts
            ELSE i.updated_at
        END,
        i.logout_count = coalesce(i.logout_count, 0)
    MERGE (o)-[r:ORDER_APPLY_IDENTITY]->(i)
    ON CREATE SET r.first_seen_ts = row_ts, r.last_seen_ts = row_ts, r.event_count = 1
    ON MATCH SET
        r.last_seen_ts = CASE
            WHEN r.last_seen_ts IS NULL OR row_ts > r.last_seen_ts THEN row_ts
            ELSE r.last_seen_ts
        END,
        r.event_count = coalesce(r.event_count, 0) + 1
)
FOREACH (_ IN CASE WHEN row.apply_loan_tel IS NULL THEN [] ELSE [1] END |
    MERGE (p:phone_num {key: row.apply_loan_tel})
    ON CREATE SET p.created_at = row_ts, p.updated_at = row_ts, p.logout_count = 0
    ON MATCH SET
        p.updated_at = CASE
            WHEN p.updated_at IS NULL OR row_ts > p.updated_at THEN row_ts
            ELSE p.updated_at
        END,
        p.logout_count = coalesce(p.logout_count, 0)
    MERGE (o)-[r:ORDER_APPLY_PHONE]->(p)
    ON CREATE SET r.first_seen_ts = row_ts, r.last_seen_ts = row_ts, r.event_count = 1
    ON MATCH SET
        r.last_seen_ts = CASE
            WHEN r.last_seen_ts IS NULL OR row_ts > r.last_seen_ts THEN row_ts
            ELSE r.last_seen_ts
        END,
        r.event_count = coalesce(r.event_count, 0) + 1
)
FOREACH (_ IN CASE WHEN row.apply_card_no IS NULL THEN [] ELSE [1] END |
    MERGE (c:card_no {key: row.apply_card_no})
    ON CREATE SET c.created_at = row_ts, c.updated_at = row_ts
    ON MATCH SET
        c.updated_at = CASE
            WHEN c.updated_at IS NULL OR row_ts > c.updated_at THEN row_ts
            ELSE c.updated_at
        END
    MERGE (o)-[r:ORDER_APPLY_CARD]->(c)
    ON CREATE SET r.first_seen_ts = row_ts, r.last_seen_ts = row_ts, r.event_count = 1
    ON MATCH SET
        r.last_seen_ts = CASE
            WHEN r.last_seen_ts IS NULL OR row_ts > r.last_seen_ts THEN row_ts
            ELSE r.last_seen_ts
        END,
        r.event_count = coalesce(r.event_count, 0) + 1
)
FOREACH (_ IN CASE WHEN row.repay_card_no IS NULL THEN [] ELSE [1] END |
    MERGE (c:card_no {key: row.repay_card_no})
    ON CREATE SET c.created_at = row_ts, c.updated_at = row_ts
    ON MATCH SET
        c.updated_at = CASE
            WHEN c.updated_at IS NULL OR row_ts > c.updated_at THEN row_ts
            ELSE c.updated_at
        END
    MERGE (o)-[r:ORDER_REPAY_CARD]->(c)
    ON CREATE SET r.first_seen_ts = row_ts, r.last_seen_ts = row_ts, r.event_count = 1
    ON MATCH SET
        r.last_seen_ts = CASE
            WHEN r.last_seen_ts IS NULL OR row_ts > r.last_seen_ts THEN row_ts
            ELSE r.last_seen_ts
        END,
        r.event_count = coalesce(r.event_count, 0) + 1
)
FOREACH (_ IN CASE WHEN row.apply_card_no IS NULL OR row.apply_bank_mobile IS NULL THEN [] ELSE [1] END |
    MERGE (c:card_no {key: row.apply_card_no})
    ON CREATE SET c.created_at = row_ts, c.updated_at = row_ts
    ON MATCH SET
        c.updated_at = CASE
            WHEN c.updated_at IS NULL OR row_ts > c.updated_at THEN row_ts
            ELSE c.updated_at
        END
    MERGE (p:phone_num {key: row.apply_bank_mobile})
    ON CREATE SET p.created_at = row_ts, p.updated_at = row_ts, p.logout_count = 0
    ON MATCH SET
        p.updated_at = CASE
            WHEN p.updated_at IS NULL OR row_ts > p.updated_at THEN row_ts
            ELSE p.updated_at
        END,
        p.logout_count = coalesce(p.logout_count, 0)
    MERGE (c)-[r:CARD_BIND_PHONE]->(p)
    ON CREATE SET r.first_seen_ts = row_ts, r.last_seen_ts = row_ts, r.event_count = 1
    ON MATCH SET
        r.last_seen_ts = CASE
            WHEN r.last_seen_ts IS NULL OR row_ts > r.last_seen_ts THEN row_ts
            ELSE r.last_seen_ts
        END,
        r.event_count = coalesce(r.event_count, 0) + 1
)
FOREACH (_ IN CASE WHEN row.repay_card_no IS NULL OR row.repay_bank_mobile IS NULL THEN [] ELSE [1] END |
    MERGE (c:card_no {key: row.repay_card_no})
    ON CREATE SET c.created_at = row_ts, c.updated_at = row_ts
    ON MATCH SET
        c.updated_at = CASE
            WHEN c.updated_at IS NULL OR row_ts > c.updated_at THEN row_ts
            ELSE c.updated_at
        END
    MERGE (p:phone_num {key: row.repay_bank_mobile})
    ON CREATE SET p.created_at = row_ts, p.updated_at = row_ts, p.logout_count = 0
    ON MATCH SET
        p.updated_at = CASE
            WHEN p.updated_at IS NULL OR row_ts > p.updated_at THEN row_ts
            ELSE p.updated_at
        END,
        p.logout_count = coalesce(p.logout_count, 0)
    MERGE (c)-[r:CARD_BIND_PHONE]->(p)
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
    order_id = normalize_text(first_existing_value(row, ORDER_ID_COLUMNS))
    uid = extract_uid_key(row, UID_COLUMNS)
    ts = parse_iso_datetime(first_existing_value(row, TIME_COLUMNS))

    if order_id is None or uid is None or ts is None:
        return None

    return {
        "order_id": order_id,
        "uid": uid,
        "order_status": normalize_text(row.get("order_status")),
        "apply_loan_tel": normalize_for_key(row.get("apply_loan_tel")),
        "apply_ident_no": normalize_for_key(row.get("apply_ident_no")),
        "apply_card_no": normalize_for_key(row.get("apply_card_no")),
        "apply_bank_mobile": normalize_for_key(row.get("apply_bank_mobile")),
        "repay_card_no": normalize_for_key(row.get("repay_card_no")),
        "repay_bank_mobile": normalize_for_key(row.get("repay_bank_mobile")),
        "ts": ts,
    }


def create_constraints(session: Session) -> None:
    for query in CONSTRAINT_QUERIES:
        session.run(query)


def write_batch(tx: Transaction, rows: List[Dict[str, Optional[str]]]) -> None:
    tx.run(BATCH_QUERY, rows=rows)


def import_order(order_root: str, database: str) -> Dict[str, int]:
    files = list_csv_files(order_root)
    if not files:
        raise FileNotFoundError(f"No CSV files found under: {order_root}")

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
                                print(f"[order] written={written_rows} total={total_rows} skipped={skipped_rows}")
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
    if ORDER_ROOT == "":
        raise ValueError("ORDER_ROOT is required")
    result = import_order(ORDER_ROOT, DEFAULT_DB)
    print(
        "[order] done "
        f"files={result['files']} total={result['total_rows']} "
        f"written={result['written_rows']} skipped={result['skipped_rows']}"
    )


if __name__ == "__main__":
    main()
