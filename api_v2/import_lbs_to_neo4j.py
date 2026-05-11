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

GPS_ROOT = os.getenv("GPS_ROOT", "")

READ_CHUNK_SIZE = 20000
BATCH_SIZE = 5000
PROGRESS_INTERVAL = 50000

TIME_COLUMNS = ("create_date", "modify_date", "ts")
UID_COLUMNS = ("cid", "cif_user_id", "user_id", "baseid", "id")
GEO_COLUMNS = ("geo_code", "adcode", "geo", "location_code")

CONSTRAINT_QUERIES = [
    "CREATE CONSTRAINT uid_key IF NOT EXISTS FOR (n:uid) REQUIRE n.key IS UNIQUE",
    "CREATE CONSTRAINT geo_code_key IF NOT EXISTS FOR (n:geo_code) REQUIRE n.key IS UNIQUE",
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
MERGE (g:geo_code {key: row.geo_code})
ON CREATE SET g.created_at = row_ts, g.updated_at = row_ts
ON MATCH SET
    g.updated_at = CASE
        WHEN g.updated_at IS NULL OR row_ts > g.updated_at THEN row_ts
        ELSE g.updated_at
    END
MERGE (u)-[r:LOCATED_AT]->(g)
ON CREATE SET r.first_seen_ts = row_ts, r.last_seen_ts = row_ts, r.event_count = 1
ON MATCH SET
    r.last_seen_ts = CASE
        WHEN r.last_seen_ts IS NULL OR row_ts > r.last_seen_ts THEN row_ts
        ELSE r.last_seen_ts
    END,
    r.event_count = coalesce(r.event_count, 0) + 1
"""


def transform_row(row) -> Optional[Dict[str, Optional[str]]]:
    uid = extract_uid_key(row, UID_COLUMNS)
    ts = parse_iso_datetime(first_existing_value(row, TIME_COLUMNS))
    geo_code = normalize_for_key(first_existing_value(row, GEO_COLUMNS))
    if uid is None or ts is None or geo_code is None:
        return None
    return {"uid": uid, "geo_code": geo_code, "ts": ts}


def create_constraints(session: Session) -> None:
    for query in CONSTRAINT_QUERIES:
        session.run(query)


def write_batch(tx: Transaction, rows: List[Dict[str, Optional[str]]]) -> None:
    tx.run(BATCH_QUERY, rows=rows)


def import_lbs_gps(gps_root: str, database: str) -> Dict[str, int]:
    files = list_csv_files(gps_root)
    if not files:
        raise FileNotFoundError(f"No CSV files found under: {gps_root}")

    driver = GraphDatabase.driver(DEFAULT_URI, auth=(DEFAULT_USER, DEFAULT_PASSWORD))
    total_rows = 0
    skipped_rows = 0
    written_rows = 0
    skipped_missing_uid = 0
    skipped_missing_ts = 0
    skipped_missing_geo = 0
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
                        geo_code = normalize_for_key(first_existing_value(row, GEO_COLUMNS))
                        if uid is None or ts is None or geo_code is None:
                            skipped_rows += 1
                            if uid is None:
                                skipped_missing_uid += 1
                            if ts is None:
                                skipped_missing_ts += 1
                            if geo_code is None:
                                skipped_missing_geo += 1
                            continue
                        rec = {"uid": uid, "geo_code": geo_code, "ts": ts}
                        pending_rows.append(rec)

                        if len(pending_rows) >= BATCH_SIZE:
                            execute_write(session, write_batch, pending_rows)
                            written_rows += len(pending_rows)
                            if written_rows % PROGRESS_INTERVAL == 0:
                                print(f"[gps] written={written_rows} total={total_rows} skipped={skipped_rows}")
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
        "skipped_missing_geo": skipped_missing_geo,
    }


def main() -> None:
    if GPS_ROOT == "":
        raise ValueError("GPS_ROOT is required")
    result = import_lbs_gps(GPS_ROOT, DEFAULT_DB)
    print(
        "[gps] done "
        f"files={result['files']} total={result['total_rows']} "
        f"written={result['written_rows']} skipped={result['skipped_rows']}"
    )


if __name__ == "__main__":
    main()
