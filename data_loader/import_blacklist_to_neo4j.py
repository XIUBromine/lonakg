from __future__ import annotations
import hashlib
from pathlib import Path
from typing import Dict, Optional

import pandas as pd
from neo4j import GraphDatabase, Session
from dotenv import load_dotenv
import os

# 从.env文件加载Neo4j连接信息
load_dotenv()
NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "123456")

CSV_FILENAME = "黑名单.csv"
CSV_FILENAME = "test/黑名单.csv"
BATCH_SIZE = 500
SALT_SUFFIX = ":bank_salt_v2"

RENAME_MAP = {
    "id": "uid",  # 新增：将id映射为uid
    "c_reason_list": "reason",
    "c_mobile_phone": "phone_num",
    "c_identity_no": "identity_no",
}

UPDATE_QUERIES = [
    # 更新手机号为黑名单
    """
    MATCH (p:phone_num {key: $phone_key})
    SET p.status = 'blacklisted',
        p.reason = $reason
    """,
    # 更新身份证号为黑名单
    """
    MATCH (i:identity_no {key: $identity_key})
    SET i.status = 'blacklisted',
        i.reason = $reason
    """,
    # 新增：直接更新UID为黑名单
    """
    MATCH (u:uid {uid_key: $uid_key})
    SET u.status = 'blacklisted',
        u.blacklist_reason = $reason
    """
]


def normalize_text(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    cleaned = str(value).strip()
    return cleaned or None


def build_key(value: Optional[str]) -> Optional[str]:
    return normalize_text(value)


def transform_row(row: pd.Series) -> Optional[Dict[str, Optional[str]]]:
    reason = normalize_text(row.get("reason"))
    phone_key = build_key(row.get("phone_num"))
    identity_key = build_key(row.get("identity_no"))
    uid_key = build_key(row.get("uid"))  # 修改：现在使用映射后的uid字段

    # 修改条件：只要有reason和至少一个标识符（uid、phone或identity）就处理
    if reason is None or (uid_key is None and phone_key is None and identity_key is None):
        return None

    return {
        "reason": reason,
        "phone_key": phone_key,
        "identity_key": identity_key,
        "uid_key": uid_key,
    }


def update_entry(session: Session, data: Dict[str, Optional[str]]) -> int:
    updated = 0

    # 优先更新UID（直接标记用户）
    if data["uid_key"] is not None:
        result = session.run(
            UPDATE_QUERIES[2],  # UID更新查询
            uid_key=data["uid_key"],
            reason=data["reason"],
        )
        if result.consume().counters.properties_set:
            updated += 1

    # 更新手机号节点
    if data["phone_key"] is not None:
        result = session.run(
            UPDATE_QUERIES[0],
            phone_key=data["phone_key"],
            reason=data["reason"],
        )
        if result.consume().counters.properties_set:
            updated += 1

    # 更新身份证号节点
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
    ).rename(columns=RENAME_MAP)  # 这里会进行列名映射

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
                    print(f"已处理 {processed_rows} 条...")

    finally:
        driver.close()

    print(f"✅ 黑名单导入完成，共读取 {total_rows} 条，成功更新 {updated_count} 个属性。")


if __name__ == "__main__":
    main()