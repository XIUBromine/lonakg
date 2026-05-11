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

LOGIN_ROOT = os.getenv("LOGIN_ROOT", "")

READ_CHUNK_SIZE = 20000
BATCH_SIZE = 5000
PROGRESS_INTERVAL = 50000

TIME_COLUMNS = ("login_time", "create_date", "modify_date", "ts")
UID_COLUMNS = ("cif_user_id", "cid", "baseid", "user_id", "id")

INVALID_DEVICE_VALUES = {
    "unknown",
    "unk",
    "none",
    "null",
    "nan",
    "-",
    "--",
    "0",
    "00000000",
    "0000000000000000",
    "00000000-0000-0000-0000-000000000000",
}

CONSTRAINT_QUERIES = [
    "CREATE CONSTRAINT uid_key IF NOT EXISTS FOR (n:uid) REQUIRE n.key IS UNIQUE",
    "CREATE CONSTRAINT phone_num_key IF NOT EXISTS FOR (n:phone_num) REQUIRE n.key IS UNIQUE",
    "CREATE CONSTRAINT device_no_key IF NOT EXISTS FOR (n:device_no) REQUIRE n.key IS UNIQUE",
    "CREATE CONSTRAINT td_device_id_key IF NOT EXISTS FOR (n:td_device_id) REQUIRE n.key IS UNIQUE",
    "CREATE CONSTRAINT remote_ip_key IF NOT EXISTS FOR (n:remote_ip) REQUIRE n.key IS UNIQUE",
]


BATCH_QUERY = """
UNWIND $rows AS row
WITH row, datetime(row.ts) AS row_ts
MERGE (u:uid {key: row.uid})
ON CREATE SET
    u.created_at = row_ts,
    u.updated_at = row_ts,
    u.last_login_ts = row_ts,
    u.logout_flag = false
ON MATCH SET
    u.updated_at = CASE
        WHEN u.updated_at IS NULL OR row_ts > u.updated_at THEN row_ts
        ELSE u.updated_at
    END,
    u.last_login_ts = CASE
        WHEN u.last_login_ts IS NULL OR row_ts > u.last_login_ts THEN row_ts
        ELSE u.last_login_ts
    END,
    u.logout_flag = coalesce(u.logout_flag, false)
FOREACH (_ IN CASE WHEN row.phone_num IS NULL THEN [] ELSE [1] END |
    MERGE (p:phone_num {key: row.phone_num})
    ON CREATE SET p.created_at = row_ts, p.updated_at = row_ts, p.logout_count = 0
    ON MATCH SET
        p.updated_at = CASE
            WHEN p.updated_at IS NULL OR row_ts > p.updated_at THEN row_ts
            ELSE p.updated_at
        END,
        p.logout_count = coalesce(p.logout_count, 0)
    MERGE (u)-[r:LOGIN_WITH_PHONE]->(p)
    ON CREATE SET r.first_seen_ts = row_ts, r.last_seen_ts = row_ts, r.event_count = 1
    ON MATCH SET
        r.last_seen_ts = CASE
            WHEN r.last_seen_ts IS NULL OR row_ts > r.last_seen_ts THEN row_ts
            ELSE r.last_seen_ts
        END,
        r.event_count = coalesce(r.event_count, 0) + 1
)
FOREACH (_ IN CASE WHEN row.device_no IS NULL THEN [] ELSE [1] END |
    MERGE (d:device_no {key: row.device_no})
    ON CREATE SET d.created_at = row_ts, d.updated_at = row_ts
    ON MATCH SET
        d.updated_at = CASE
            WHEN d.updated_at IS NULL OR row_ts > d.updated_at THEN row_ts
            ELSE d.updated_at
        END
    MERGE (u)-[r:LOGIN_WITH_DEVICE]->(d)
    ON CREATE SET r.first_seen_ts = row_ts, r.last_seen_ts = row_ts, r.event_count = 1
    ON MATCH SET
        r.last_seen_ts = CASE
            WHEN r.last_seen_ts IS NULL OR row_ts > r.last_seen_ts THEN row_ts
            ELSE r.last_seen_ts
        END,
        r.event_count = coalesce(r.event_count, 0) + 1
)
FOREACH (_ IN CASE WHEN row.td_device_id IS NULL THEN [] ELSE [1] END |
    MERGE (td:td_device_id {key: row.td_device_id})
    ON CREATE SET td.created_at = row_ts, td.updated_at = row_ts
    ON MATCH SET
        td.updated_at = CASE
            WHEN td.updated_at IS NULL OR row_ts > td.updated_at THEN row_ts
            ELSE td.updated_at
        END
    MERGE (u)-[r:LOGIN_WITH_TD_DEVICE]->(td)
    ON CREATE SET r.first_seen_ts = row_ts, r.last_seen_ts = row_ts, r.event_count = 1
    ON MATCH SET
        r.last_seen_ts = CASE
            WHEN r.last_seen_ts IS NULL OR row_ts > r.last_seen_ts THEN row_ts
            ELSE r.last_seen_ts
        END,
        r.event_count = coalesce(r.event_count, 0) + 1
)
FOREACH (_ IN CASE WHEN row.remote_ip IS NULL THEN [] ELSE [1] END |
    MERGE (ip:remote_ip {key: row.remote_ip})
    ON CREATE SET ip.created_at = row_ts, ip.updated_at = row_ts
    ON MATCH SET
        ip.updated_at = CASE
            WHEN ip.updated_at IS NULL OR row_ts > ip.updated_at THEN row_ts
            ELSE ip.updated_at
        END
    MERGE (u)-[r:LOGIN_WITH_IP]->(ip)
    ON CREATE SET r.first_seen_ts = row_ts, r.last_seen_ts = row_ts, r.event_count = 1
    ON MATCH SET
        r.last_seen_ts = CASE
            WHEN r.last_seen_ts IS NULL OR row_ts > r.last_seen_ts THEN row_ts
            ELSE r.last_seen_ts
        END,
        r.event_count = coalesce(r.event_count, 0) + 1
)
"""


def normalize_device(value: object) -> Optional[str]:
    key = normalize_for_key(value)
    if key is None:
        return None
    if key in INVALID_DEVICE_VALUES:
        return None
    return key


def transform_row(row) -> Optional[Dict[str, Optional[str]]]:
    uid = extract_uid_key(row, UID_COLUMNS)
    ts = parse_iso_datetime(first_existing_value(row, TIME_COLUMNS))
    if uid is None or ts is None:
        return None
    return {
        "uid": uid,
        "phone_num": normalize_for_key(row.get("phone_num")),
        "device_no": normalize_device(row.get("device_no")),
        "td_device_id": normalize_for_key(row.get("td_device_id")),
        "remote_ip": normalize_for_key(row.get("remote_ip")),
        "ts": ts,
    }


def create_constraints(session: Session) -> None:
    for query in CONSTRAINT_QUERIES:
        session.run(query)


def write_batch(tx: Transaction, rows: List[Dict[str, Optional[str]]]) -> None:
    tx.run(BATCH_QUERY, rows=rows)


def import_login(login_root: str, database: str) -> Dict[str, int]:
    files = list_csv_files(login_root)
    if not files:
        raise FileNotFoundError(f"No CSV files found under: {login_root}")

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
                for chunk in read_csv_chunks(csv_path, READ_CHUNK_SIZE):
                    for _, row in chunk.iterrows():
                        total_rows += 1
                        uid = extract_uid_key(row, UID_COLUMNS)
                        ts = parse_iso_datetime(first_existing_value(row, TIME_COLUMNS))
                        if uid is None or ts is None:
                            skipped_rows += 1
                            if uid is None:
                                skipped_missing_uid += 1
                            if ts is None:
                                skipped_missing_ts += 1
                            continue
                        rec = {
                            "uid": uid,
                            "phone_num": normalize_for_key(row.get("phone_num")),
                            "device_no": normalize_device(row.get("device_no")),
                            "td_device_id": normalize_for_key(row.get("td_device_id")),
                            "remote_ip": normalize_for_key(row.get("remote_ip")),
                            "ts": ts,
                        }
                        pending_rows.append(rec)

                        if len(pending_rows) >= BATCH_SIZE:
                            execute_write(session, write_batch, pending_rows)
                            written_rows += len(pending_rows)
                            if written_rows % PROGRESS_INTERVAL == 0:
                                print(f"[login] written={written_rows} total={total_rows} skipped={skipped_rows}")
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
        "skipped_missing_uid": skipped_missing_uid,
        "skipped_missing_ts": skipped_missing_ts,
    }


def main() -> None:
    if LOGIN_ROOT == "":
        raise ValueError("LOGIN_ROOT is required")
    result = import_login(LOGIN_ROOT, DEFAULT_DB)
    print(
        "[login] done "
        f"files={result['files']} total={result['total_rows']} "
        f"written={result['written_rows']} skipped={result['skipped_rows']}"
    )


if __name__ == "__main__":
    main()
