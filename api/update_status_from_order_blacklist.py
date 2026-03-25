from __future__ import annotations

import argparse
import csv
import logging
import os
import time
from pathlib import Path
from typing import Dict, Iterable, List, Optional

from dotenv import load_dotenv
from neo4j import GraphDatabase


load_dotenv()

NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "kBXwIuxLTvgxnbGD")
DEFAULT_DB = "prod"

def build_logger(log_file: Optional[Path]) -> logging.Logger:
    logger = logging.getLogger("status_backfill")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    if log_file is not None:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger


def norm(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    v = str(value).strip()
    if v == "" or v.lower() in {"nan", "none", "null"}:
        return None
    return v


def safe_int(value: Optional[str]) -> Optional[int]:
    v = norm(value)
    if v is None:
        return None
    try:
        return int(float(v))
    except Exception:
        return None


def choose_order_file(primary: Path, fallback: Path) -> Path:
    if primary.exists():
        return primary
    if fallback.exists():
        return fallback
    raise FileNotFoundError(f"Neither order file exists: {primary} / {fallback}")


def batched(items: Iterable[Dict[str, object]], batch_size: int) -> Iterable[List[Dict[str, object]]]:
    batch: List[Dict[str, object]] = []
    for item in items:
        batch.append(item)
        if len(batch) >= batch_size:
            yield batch
            batch = []
    if batch:
        yield batch


def iter_order_rows(order_file: Path):
    with order_file.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            order_key = norm(row.get("id") or row.get("order_id"))
            order_status = norm(row.get("order_status"))
            if order_key is None or order_status is None:
                continue
            yield {
                "order_key": order_key,
                "order_status": order_status,
            }


def iter_blacklist_rows(blacklist_file: Path):
    with blacklist_file.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            uid = norm(row.get("c_customer_no"))
            phone = norm(row.get("c_mobile_phone"))
            identity = norm(row.get("c_identity_no"))
            reason = norm(row.get("c_reason_list"))
            begin_date = norm(row.get("d_begin_date"))
            banned_days = safe_int(row.get("n_banned_days"))
            duration = safe_int(row.get("n_duration"))

            if uid is None and phone is None and identity is None:
                continue

            yield {
                "uid": uid,
                "phone": phone,
                "identity": identity,
                "reason": reason,
                "begin_date": begin_date,
                "banned_days": banned_days,
                "duration": duration,
            }


def update_order_status(session, rows: List[Dict[str, object]]) -> None:
    query = """
    UNWIND $rows AS row
    MATCH (o:order {key: row.order_key})
    SET o.status = row.order_status
    """
    session.run(query, rows=rows)


def update_blacklist_status(session, rows: List[Dict[str, object]]) -> None:
    query = """
    UNWIND $rows AS row
    OPTIONAL MATCH (u:uid {key: row.uid})
    OPTIONAL MATCH (p:phone_num {key: row.phone})
    OPTIONAL MATCH (i:identity_no {key: row.identity})

    FOREACH (_ IN CASE WHEN u IS NULL THEN [] ELSE [1] END |
        SET u.status = 'BLACKLISTED'
    )

    FOREACH (_ IN CASE WHEN p IS NULL THEN [] ELSE [1] END |
        SET p.status = 'BLACKLISTED'
    )

    FOREACH (_ IN CASE WHEN i IS NULL THEN [] ELSE [1] END |
        SET i.status = 'BLACKLISTED'
    )
    """
    session.run(query, rows=rows)


def process_order_file(session, order_file: Path, batch_size: int, progress_interval: int, logger: logging.Logger) -> int:
    total = 0
    start = time.time()
    for batch in batched(iter_order_rows(order_file), batch_size):
        update_order_status(session, batch)
        total += len(batch)
        if total % progress_interval == 0:
            elapsed = time.time() - start
            speed = total / elapsed if elapsed > 0 else 0.0
            logger.info("order status progress=%d speed=%.2f rows/s", total, speed)
    elapsed = time.time() - start
    logger.info("order status done total=%d elapsed=%.2fs speed=%.2f rows/s", total, elapsed, total / elapsed if elapsed > 0 else 0.0)
    return total


def process_blacklist_file(session, blacklist_file: Path, batch_size: int, progress_interval: int, logger: logging.Logger) -> int:
    total = 0
    start = time.time()
    for batch in batched(iter_blacklist_rows(blacklist_file), batch_size):
        update_blacklist_status(session, batch)
        total += len(batch)
        if total % progress_interval == 0:
            elapsed = time.time() - start
            speed = total / elapsed if elapsed > 0 else 0.0
            logger.info("blacklist status progress=%d speed=%.2f rows/s", total, speed)
    elapsed = time.time() - start
    logger.info("blacklist status done total=%d elapsed=%.2fs speed=%.2f rows/s", total, elapsed, total / elapsed if elapsed > 0 else 0.0)
    return total


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backfill status attributes for order/uid/phone_num/identity_no")
    parser.add_argument("--database", default=DEFAULT_DB, help="Neo4j database name")
    parser.add_argument("--uri", default=NEO4J_URI)
    parser.add_argument("--user", default=NEO4J_USER)
    parser.add_argument("--password", default=NEO4J_PASSWORD)
    parser.add_argument("--order-file", default="/data/aiimport_1119/订单order.csv", help="Order CSV path")
    parser.add_argument("--order-file-fallback", default="/data/aiimport_1119/订单order信息.csv", help="Fallback order CSV path")
    parser.add_argument("--blacklist-file", default="/data/aiimport_1119/黑名单log.csv", help="Blacklist CSV path")
    parser.add_argument("--batch-size", type=int, default=5000)
    parser.add_argument("--progress-interval", type=int, default=50000)
    parser.add_argument("--log-file", default="api/update_status_from_order_blacklist.log")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    log_file = Path(args.log_file) if args.log_file else None
    logger = build_logger(log_file)

    order_primary = Path(args.order_file)
    order_fallback = Path(args.order_file_fallback)
    blacklist_file = Path(args.blacklist_file)

    order_file = choose_order_file(order_primary, order_fallback)
    if not blacklist_file.exists():
        raise FileNotFoundError(f"blacklist file not found: {blacklist_file}")

    logger.info("start backfill database=%s", args.database)
    logger.info("order_file=%s", order_file)
    logger.info("blacklist_file=%s", blacklist_file)
    logger.info("batch_size=%d progress_interval=%d", args.batch_size, args.progress_interval)

    driver = GraphDatabase.driver(args.uri, auth=(args.user, args.password))
    start = time.time()
    try:
        with driver.session(database=args.database) as session:
            order_count = process_order_file(
                session,
                order_file=order_file,
                batch_size=max(1, args.batch_size),
                progress_interval=max(1, args.progress_interval),
                logger=logger,
            )

            blacklist_count = process_blacklist_file(
                session,
                blacklist_file=blacklist_file,
                batch_size=max(1, args.batch_size),
                progress_interval=max(1, args.progress_interval),
                logger=logger,
            )

        elapsed = time.time() - start
        logger.info(
            "all done order_rows=%d blacklist_rows=%d total_elapsed=%.2fs",
            order_count,
            blacklist_count,
            elapsed,
        )
    finally:
        driver.close()


if __name__ == "__main__":
    main()
