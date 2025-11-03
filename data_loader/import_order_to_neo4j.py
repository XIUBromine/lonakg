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

CSV_FILENAME = "test/订单order信息.csv"
# CSV_FILENAME = "订单order信息.csv"
BATCH_SIZE = 5000
PROGRESS_INTERVAL = 5000
SALT_SUFFIX = ":bank_salt_v2"

RENAME_MAP = {
    "id": "order_id",
    "user_id": "uid",
    "apply_loan_tel": "phone_num",
    "apply_ident_no": "identity_no",
    "apply_card_no": "card_no",
    "apply_bank_mobile": "card_phone_num",
    "repay_card_no": "repay_card_no",
    "repay_bank_mobile": "repay_card_phone_num",
    "apply_loan_date": "apply_time",
    "sign_date": "sign_time",
    "order_status": "order_status",
}

CONSTRAINT_QUERIES = [
    "CREATE CONSTRAINT uid_key IF NOT EXISTS FOR (n:uid) REQUIRE n.uid_key IS UNIQUE",
    "CREATE CONSTRAINT phone_num_key IF NOT EXISTS FOR (n:phone_num) REQUIRE n.key IS UNIQUE",
    "CREATE CONSTRAINT identity_no_key IF NOT EXISTS FOR (n:identity_no) REQUIRE n.key IS UNIQUE",
    "CREATE CONSTRAINT card_no_key IF NOT EXISTS FOR (n:card_no) REQUIRE n.key IS UNIQUE",
]

BATCH_QUERY = """
UNWIND $rows AS row
MERGE (u:uid {uid_key: row.uid_key})
WITH row, u

// 1. UID → 申请手机号 (保留)
FOREACH (_ IN CASE WHEN row.phone_num_key IS NULL THEN [] ELSE [1] END |
    MERGE (p_phone:phone_num {key: row.phone_num_key})
    MERGE (u)-[r1:order_apply_phone_num]->(p_phone)
    SET r1.order_id = row.order_id,
        r1.order_status = row.order_status,
        r1.apply_time = CASE WHEN row.apply_time IS NULL THEN NULL ELSE datetime(row.apply_time) END,
        r1.sign_time = CASE WHEN row.sign_time IS NULL THEN NULL ELSE datetime(row.sign_time) END
)

// 2. UID → 身份证号 (保留)
FOREACH (_ IN CASE WHEN row.identity_no_key IS NULL THEN [] ELSE [1] END |
    MERGE (iden:identity_no {key: row.identity_no_key})
    MERGE (u)-[r2:order_apply_identity_no]->(iden)
    SET r2.order_id = row.order_id,
        r2.order_status = row.order_status,
        r2.apply_time = CASE WHEN row.apply_time IS NULL THEN NULL ELSE datetime(row.apply_time) END,
        r2.sign_time = CASE WHEN row.sign_time IS NULL THEN NULL ELSE datetime(row.sign_time) END
)

// 3. UID → 申请银行卡 (保留)
FOREACH (_ IN CASE WHEN row.card_no_key IS NULL THEN [] ELSE [1] END |
    MERGE (card_apply:card_no {key: row.card_no_key})
    MERGE (u)-[r3:order_apply_card_no]->(card_apply)
    SET r3.order_id = row.order_id,
        r3.order_status = row.order_status,
        r3.apply_time = CASE WHEN row.apply_time IS NULL THEN NULL ELSE datetime(row.apply_time) END,
        r3.sign_time = CASE WHEN row.sign_time IS NULL THEN NULL ELSE datetime(row.sign_time) END
)

// 4. UID → 银行卡绑定手机号 (修改：关系方向改为UID→手机号)
FOREACH (_ IN CASE WHEN row.card_phone_num_key IS NULL THEN [] ELSE [1] END |
    MERGE (card_apply:card_no {key: row.card_no_key})
    MERGE (phone_card_apply:phone_num {key: row.card_phone_num_key})
    MERGE (u)-[r4:order_apply_card_phone_num]->(phone_card_apply)
    SET r4.order_id = row.order_id,
        r4.order_status = row.order_status,
        r4.apply_time = CASE WHEN row.apply_time IS NULL THEN NULL ELSE datetime(row.apply_time) END,
        r4.sign_time = CASE WHEN row.sign_time IS NULL THEN NULL ELSE datetime(row.sign_time) END
)

// 5. UID → 还款银行卡 (保留)
FOREACH (_ IN CASE WHEN row.repay_card_no_key IS NULL THEN [] ELSE [1] END |
    MERGE (card_repay:card_no {key: row.repay_card_no_key})
    MERGE (u)-[r5:order_repay_card_no]->(card_repay)
    SET r5.order_id = row.order_id,
        r5.order_status = row.order_status,
        r5.apply_time = CASE WHEN row.apply_time IS NULL THEN NULL ELSE datetime(row.apply_time) END,
        r5.sign_time = CASE WHEN row.sign_time IS NULL THEN NULL ELSE datetime(row.sign_time) END
)

// 6. UID → 还款银行卡绑定手机号 (修改：关系方向改为UID→手机号)
FOREACH (_ IN CASE WHEN row.repay_card_phone_key IS NULL THEN [] ELSE [1] END |
    MERGE (card_repay:card_no {key: row.repay_card_no_key})
    MERGE (phone_repay:phone_num {key: row.repay_card_phone_key})
    MERGE (u)-[r6:order_repay_card_phone_num]->(phone_repay)
    SET r6.order_id = row.order_id,
        r6.order_status = row.order_status,
        r6.apply_time = CASE WHEN row.apply_time IS NULL THEN NULL ELSE datetime(row.apply_time) END,
        r6.sign_time = CASE WHEN row.sign_time IS NULL THEN NULL ELSE datetime(row.sign_time) END
)
"""


def normalize_text(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    cleaned = str(value).strip()
    return cleaned or None


def normalize_for_key(value: Optional[str]) -> Optional[str]:
    normalized = normalize_text(value)
    if normalized is None:
        return None
    return normalized.lower()


# def build_key(value: Optional[str]) -> Optional[str]:
#     normalized = normalize_for_key(value)
#     if normalized is None:
#         return None
#     salted = normalized + SALT_SUFFIX
#     return hashlib.sha256(salted.encode("utf-8")).hexdigest()

def build_key(value: Optional[str]) -> Optional[str]:
    return normalize_for_key(value)

def parse_datetime(value: Optional[str]) -> Optional[str]:
    cleaned = normalize_text(value)
    if cleaned is None:
        return None
    timestamp = pd.to_datetime(cleaned, errors="coerce")
    if pd.isna(timestamp):
        return None
    return timestamp.isoformat()


def transform_row(row: pd.Series) -> Optional[Dict[str, Optional[str]]]:
    order_id = normalize_text(row.get("order_id"))
    uid_key = build_key(row.get("uid"))

    if order_id is None or uid_key is None:
        return None

    apply_time = parse_datetime(row.get("apply_time"))
    sign_time = parse_datetime(row.get("sign_time"))

    return {
        "order_id": order_id,
        "uid_key": uid_key,
        "order_status": normalize_text(row.get("order_status")),
        "apply_time": apply_time,
        "sign_time": sign_time,
        "phone_num_key": build_key(row.get("phone_num")),
        "identity_no_key": build_key(row.get("identity_no")),
        "card_no_key": build_key(row.get("card_no")),
        "card_phone_num_key": build_key(row.get("card_phone_num")),
        "repay_card_no_key": build_key(row.get("repay_card_no")),
        "repay_card_phone_key": build_key(row.get("repay_card_phone_num")),
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

            time_columns = ["apply_time", "sign_time"]

            for chunk in csv_iterator:
                chunk = chunk.rename(columns=RENAME_MAP)

                for col in time_columns:
                    if col in chunk.columns:
                        chunk[col] = chunk[col].astype(str).str.replace(" ", "T", regex=False)

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
