from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
from neo4j import GraphDatabase, Session, Transaction


# 填写你的 Neo4j 连接信息
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "12345678"

CSV_FILENAME = "test/lbs_gps信息.csv"
BATCH_SIZE = 5000
PROGRESS_INTERVAL = 5000
SALT_SUFFIX = ":bank_salt_v2"

RENAME_MAP = {
    "cid": "uid",
    "geo_code": "geo_code",
    "create_date": "create_time",
}

CONSTRAINT_QUERIES = [
    "CREATE CONSTRAINT uid_key IF NOT EXISTS FOR (n:uid) REQUIRE n.uid_key IS UNIQUE",
    "CREATE CONSTRAINT geo_code_key IF NOT EXISTS FOR (n:geo_code) REQUIRE n.key IS UNIQUE",
]

BATCH_QUERY = """
UNWIND $rows AS row
MERGE (u:uid {uid_key: row.uid_key})
FOREACH (_ IN CASE WHEN row.geo_code_key IS NULL THEN [] ELSE [1] END |
    MERGE (geo:geo_code {key: row.geo_code_key})
    MERGE (u)-[:gps_geo_code {create_time: datetime(row.create_time)}]->(geo)
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

def parse_create_time(value: Optional[str]) -> Optional[str]:
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
    create_time = parse_create_time(row.get("create_time"))
    geo_code_key = build_key(row.get("geo_code"))

    if uid_key is None or create_time is None or geo_code_key is None:
        return None

    return {
        "uid_key": uid_key,
        "geo_code_key": geo_code_key,
        "create_time": create_time,
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
                chunksize=10000,
                encoding="utf-8-sig",
            )

            for chunk in csv_iterator:
                chunk = chunk.rename(columns=RENAME_MAP)
                if "create_time" not in chunk.columns:
                    chunk["create_time"] = ""
                chunk["create_time"] = (
                    chunk["create_time"].astype(str).str.replace(" ", "T", regex=False)
                )

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
                pending_rows = []

    finally:
        driver.close()

    print(f"✅ 导入完成，共读取 {total_rows} 条，成功写入 {written_rows} 条，跳过 {skipped_rows} 条。")


if __name__ == "__main__":
    main()
