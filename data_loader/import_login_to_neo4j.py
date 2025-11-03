from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
from neo4j import GraphDatabase, Session, Transaction


from dotenv import load_dotenv
import os

# 从.env文件加载Neo4j连接信息
load_dotenv()
NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "123456")

CSV_FILENAME = "test/登录信息.csv"
BATCH_SIZE = 5000
PROGRESS_INTERVAL = 5000
SALT_SUFFIX = ":bank_salt_v2"

RENAME_MAP = {
    "cif_user_id": "uid",
    "phone_num": "phone_num",
    "device_no": "device_no",
    "remote_ip": "remote_ip",
    "td_device_id": "td_device_id",
    "login_time": "login_time",
}

CONSTRAINT_QUERIES = [
    "CREATE CONSTRAINT uid_key IF NOT EXISTS FOR (n:uid) REQUIRE n.uid_key IS UNIQUE",
    "CREATE CONSTRAINT phone_num_key IF NOT EXISTS FOR (n:phone_num) REQUIRE n.key IS UNIQUE",
    "CREATE CONSTRAINT device_no_key IF NOT EXISTS FOR (n:device_no) REQUIRE n.key IS UNIQUE",
    "CREATE CONSTRAINT td_device_id_key IF NOT EXISTS FOR (n:td_device_id) REQUIRE n.key IS UNIQUE",
    "CREATE CONSTRAINT remote_ip_key IF NOT EXISTS FOR (n:remote_ip) REQUIRE n.key IS UNIQUE",
]

BATCH_QUERY = """
UNWIND $rows AS row
MERGE (u:uid {uid_key: row.uid_key})
FOREACH (_ IN CASE WHEN row.phone_num_key IS NULL THEN [] ELSE [1] END |
    MERGE (phone:phone_num {key: row.phone_num_key})
    MERGE (u)-[:login_phone_num {event_time: datetime(row.login_time)}]->(phone)
)
FOREACH (_ IN CASE WHEN row.device_no_key IS NULL THEN [] ELSE [1] END |
    MERGE (device:device_no {key: row.device_no_key})
    MERGE (u)-[:login_device_no {event_time: datetime(row.login_time)}]->(device)
)
FOREACH (_ IN CASE WHEN row.td_device_id_key IS NULL THEN [] ELSE [1] END |
    MERGE (td:td_device_id {key: row.td_device_id_key})
    MERGE (u)-[:login_td_device_id {event_time: datetime(row.login_time)}]->(td)
)
FOREACH (_ IN CASE WHEN row.remote_ip_key IS NULL THEN [] ELSE [1] END |
    MERGE (ip:remote_ip {key: row.remote_ip_key})
    MERGE (u)-[:login_remote_ip {event_time: datetime(row.login_time)}]->(ip)
)
"""


def normalize_for_key(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    normalized = value.strip().lower()
    return normalized or None


# def build_key(value: Optional[str]) -> Optional[str]:
#     normalized = normalize_for_key(value)
#     if normalized is None:
#         return None
#     salted = normalized + SALT_SUFFIX
#     return hashlib.sha256(salted.encode("utf-8")).hexdigest()

def build_key(value: Optional[str]) -> Optional[str]:
    return normalize_for_key(value)

def parse_login_time(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    cleaned = value.strip()
    if not cleaned:
        return None
    timestamp = pd.to_datetime(cleaned, errors="coerce")
    if pd.isna(timestamp):
        return None
    return timestamp.isoformat()


def transform_row(row: pd.Series) -> Optional[Dict[str, Optional[str]]]:
    uid_key = build_key(row.get("uid"))
    login_time = parse_login_time(row.get("login_time"))

    if uid_key is None or login_time is None:
        return None

    return {
        "uid_key": uid_key,
        "phone_num_key": build_key(row.get("phone_num")),
        "device_no_key": build_key(row.get("device_no")),
        "td_device_id_key": build_key(row.get("td_device_id")),
        "remote_ip_key": build_key(row.get("remote_ip")),
        "login_time": login_time,
    }


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


def main() -> None:
    csv_path = Path(CSV_FILENAME)
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV file not found: {csv_path.resolve()}")

    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

    total_rows = 0
    skipped_rows = 0
    written_rows = 0
    pending_rows: List[Dict[str, Optional[str]]] = []

    try:
        with driver.session() as session:
            create_constraints(session)

            csv_iterator = pd.read_csv(
                csv_path,
                dtype=str,
                keep_default_na=False,
                na_filter=False,
                chunksize=1000,
                encoding="utf-8-sig",
            )

            for chunk in csv_iterator:
                chunk = chunk.rename(columns=RENAME_MAP)
                for _, row in chunk.iterrows():
                    total_rows += 1
                    record = transform_row(row)
                    if record is None:
                        skipped_rows += 1
                        continue

                    pending_rows.append(record)

                    if len(pending_rows) == BATCH_SIZE:
                        execute_write(session, pending_rows)
                        written_rows += len(pending_rows)
                        if written_rows % PROGRESS_INTERVAL == 0:
                            print(f"已成功写入 {written_rows} 条...")
                        pending_rows = []

            if pending_rows:
                execute_write(session, pending_rows)
                written_rows += len(pending_rows)
                if written_rows % PROGRESS_INTERVAL == 0:
                    print(f"已成功写入 {written_rows} 条...")

    finally:
        driver.close()

    print(f"✅ 导入完成，共读取 {total_rows} 条，成功写入 {written_rows} 条，跳过 {skipped_rows} 条。")


if __name__ == "__main__":
    main()
