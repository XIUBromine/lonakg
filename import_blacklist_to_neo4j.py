from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Dict, Optional

import pandas as pd
from neo4j import GraphDatabase, Session


# 填写你的 Neo4j 连接信息
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "12345678"

CSV_FILENAME = "黑名单.csv"
BATCH_SIZE = 500
SALT_SUFFIX = ":bank_salt_v2"

RENAME_MAP = {
    "c_reason_list": "reason",
    "c_mobile_phone": "phone_num",
    "c_identity_no": "identity_no",
}

UPDATE_QUERIES = [
    """
    MATCH (p:phone_num {key: $phone_key})
    SET p.status = 'blacklisted',
        p.reason = $reason
    """,
    """
    MATCH (i:identity_no {key: $identity_key})
    SET i.status = 'blacklisted',
        i.reason = $reason
    """,
]


def normalize_text(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    cleaned = str(value).strip()
    return cleaned or None


# def build_key(value: Optional[str]) -> Optional[str]:
#     cleaned = normalize_text(value)
#     if cleaned is None:
#         return None
#     salted = cleaned.lower() + SALT_SUFFIX
#     return hashlib.sha256(salted.encode("utf-8")).hexdigest()

# 不要重复加密
def build_key(value: Optional[str]) -> Optional[str]:
    return normalize_text(value)

def transform_row(row: pd.Series) -> Optional[Dict[str, Optional[str]]]:
    reason = normalize_text(row.get("reason"))
    phone_key = build_key(row.get("phone_num"))
    identity_key = build_key(row.get("identity_no"))

    if reason is None or (phone_key is None and identity_key is None):
        return None

    return {
        "reason": reason,
        "phone_key": phone_key,
        "identity_key": identity_key,
    }


def update_entry(session: Session, data: Dict[str, Optional[str]]) -> int:
    updated = 0
    if data["phone_key"] is not None:
        result = session.run(
            UPDATE_QUERIES[0],
            phone_key=data["phone_key"],
            reason=data["reason"],
        )
        if result.consume().counters.properties_set:
            updated += 1

    if data["identity_key"] is not None:
        result = session.run(
            UPDATE_QUERIES[1],
            identity_key=data["identity_key"],
            reason=data["reason"],
        )
        if result.consume().counters.properties_set:
            updated += 1

    return updated


def main() -> None:
    csv_path = Path(CSV_FILENAME)
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV file not found: {csv_path.resolve()}")

    df = pd.read_csv(
        csv_path,
        dtype=str,
        keep_default_na=False,
        na_filter=False,
        encoding="utf-8-sig",
    ).rename(columns=RENAME_MAP)

    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

    total_rows = len(df)
    updated_count = 0
    processed_rows = 0

    try:
        with driver.session() as session:
            for _, row in df.iterrows():
                processed_rows += 1
                data = transform_row(row)

                if data is None:
                    continue

                updated_count += update_entry(session, data)

                if processed_rows % BATCH_SIZE == 0:
                    print(f"已更新 {processed_rows} 条...")

    finally:
        driver.close()

    print(f"✅ 黑名单导入完成，共读取 {total_rows} 条，成功更新 {updated_count} 条。")


if __name__ == "__main__":
    main()
