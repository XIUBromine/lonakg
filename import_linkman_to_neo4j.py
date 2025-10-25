from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import pandas as pd
from neo4j import GraphDatabase, Session, Transaction


# 填写你的 Neo4j 连接信息
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "12345678"

# NEO4J_URI="bolt://mini.fffu.fun:7687"
# NEO4J_USER = "neo4j"
# NEO4J_PASSWORD="kBXwIuxLTvgxnbGD"

CSV_FILENAMES = [
    "第一联系人.csv",
    "其它联系人.csv",
    "手机号关联第一联系人衍生.csv",
    "手机号关联其他联系人衍生.csv",
]

BATCH_SIZE = 5000
PROGRESS_INTERVAL = 5000
SALT_SUFFIX = ":bank_salt_v2"

RENAME_MAP = {
    "cid": "uid",
}

CONSTRAINT_QUERIES = [
    "CREATE CONSTRAINT uid_key IF NOT EXISTS FOR (n:uid) REQUIRE n.uid_key IS UNIQUE",
    "CREATE CONSTRAINT phone_num_key IF NOT EXISTS FOR (n:phone_num) REQUIRE n.key IS UNIQUE",
]

BATCH_QUERY = """
UNWIND $rows AS row
MERGE (u:uid {uid_key: row.uid_key})
FOREACH (_ IN CASE WHEN row.phone_num_key IS NULL THEN [] ELSE [1] END |
    MERGE (phone:phone_num {key: row.phone_num_key})
    MERGE (u)-[:linkman_phone_num {modify_time: datetime(row.modify_time)}]->(phone)
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

# 不要重复加密
def build_key(value: Optional[str]) -> Optional[str]:
    return normalize_for_key(value)

def parse_modify_time(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    cleaned = value.strip()
    if not cleaned:
        return None
    timestamp = pd.to_datetime(cleaned, errors="coerce")
    if pd.isna(timestamp):
        return None
    return timestamp.isoformat()


def load_dataframes(filenames: List[str]) -> Iterable[pd.DataFrame]:
    for name in filenames:
        path = Path(name)
        if not path.exists():
            raise FileNotFoundError(f"CSV file not found: {path.resolve()}")
        yield pd.read_csv(
            path,
            dtype=str,
            keep_default_na=False,
            na_filter=False,
            encoding="utf-8-sig",
        )


def prepare_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(columns=RENAME_MAP)

    phone_cols = [col for col in ("mobile_phone", "second_mobile_phone") if col in df.columns]
    if phone_cols:
        phone_series = (
            df[phone_cols]
            .replace({"": pd.NA})
            .bfill(axis=1)
            .iloc[:, 0]
            .fillna("")
        )
    else:
        phone_series = pd.Series([""] * len(df), index=df.index, dtype=object)
    df["phone_num"] = phone_series.astype(str)

    modify_cols = [col for col in ("modify_date", "create_date") if col in df.columns]
    if modify_cols:
        modify_series = (
            df[modify_cols]
            .replace({"": pd.NA})
            .bfill(axis=1)
            .iloc[:, 0]
            .fillna("")
        )
    else:
        modify_series = pd.Series([""] * len(df), index=df.index, dtype=object)
    df["modify_time"] = modify_series.astype(str).str.replace(" ", "T", regex=False)

    drop_cols = phone_cols + modify_cols
    if drop_cols:
        df = df.drop(columns=drop_cols, errors="ignore")

    return df


def transform_row(row: pd.Series) -> Optional[Dict[str, Optional[str]]]:
    uid_key = build_key(row.get("uid"))
    modify_time = parse_modify_time(row.get("modify_time"))

    if uid_key is None or modify_time is None:
        return None

    return {
        "uid_key": uid_key,
        "phone_num_key": build_key(row.get("phone_num")),
        "modify_time": modify_time,
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
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

    total_rows = 0
    skipped_rows = 0
    written_rows = 0
    pending_rows: List[Dict[str, Optional[str]]] = []

    try:
        with driver.session() as session:
            create_constraints(session)

            for df in load_dataframes(CSV_FILENAMES):
                prepared_df = prepare_dataframe(df)

                for _, row in prepared_df.iterrows():
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
