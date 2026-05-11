from __future__ import annotations

import argparse
import os
from pathlib import Path
from typing import Callable, Dict, List, Tuple

from dotenv import load_dotenv
from neo4j import GraphDatabase

from import_blacklist_log_to_neo4j import import_blacklist_log
from import_company_log_to_neo4j import import_company_log
from import_complaint_to_neo4j import import_complaint
from import_consumer_protection_case_to_neo4j import import_consumer_case
from import_customer_to_neo4j import import_customer_logs
from import_lbs_to_neo4j import import_lbs_gps
from import_linkman_to_neo4j import import_linkman
from import_login_to_neo4j import import_login
from import_logout_to_neo4j import import_logout
from import_order_to_neo4j import import_order
from import_repay_plan_to_neo4j import import_repay_plan


load_dotenv()

DEFAULT_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
DEFAULT_USER = os.getenv("NEO4J_USER", "neo4j")
DEFAULT_PASSWORD = os.getenv("NEO4J_PASSWORD", "kBXwIuxLTvgxnbGD")
DEFAULT_DB = "dev"


def create_database_if_needed(database: str) -> None:
    driver = GraphDatabase.driver(DEFAULT_URI, auth=(DEFAULT_USER, DEFAULT_PASSWORD))
    try:
        with driver.session(database="system") as session:
            session.run(f"CREATE DATABASE `{database}` IF NOT EXISTS")
    finally:
        driver.close()


def ensure_dir(path: Path) -> None:
    if not path.exists() or not path.is_dir():
        raise FileNotFoundError(f"Input directory not found: {path}")


def run_imports(base_root: Path, database: str, blacklist_max_ts: str) -> Dict[str, Dict[str, int]]:
    steps: List[Tuple[str, Callable[[], Dict[str, int]]]] = [
        ("import_customer_to_neo4j", lambda: import_customer_logs(str(base_root / "customer_log"), database)),
        ("import_logout_to_neo4j", lambda: import_logout(str(base_root / "logout"), database)),
        ("import_login_to_neo4j", lambda: import_login(str(base_root / "login"), database)),
        ("import_lbs_to_neo4j", lambda: import_lbs_gps(str(base_root / "lbs_gps"), database)),
        (
            "import_linkman_to_neo4j",
            lambda: import_linkman(
                [
                    str(base_root / "first_linkman"),
                    str(base_root / "second_linkman"),
                    str(base_root / "first_linkman_derived"),
                    str(base_root / "second_linkman_derived"),
                ],
                database,
            ),
        ),
        ("import_order_to_neo4j", lambda: import_order(str(base_root / "order"), database)),
        ("import_repay_plan_to_neo4j", lambda: import_repay_plan(str(base_root / "repay_plan"), database)),
        ("import_company_log_to_neo4j", lambda: import_company_log(str(base_root / "company_log"), database)),
        ("import_complaint_to_neo4j", lambda: import_complaint(str(base_root / "complaint"), database)),
        (
            "import_consumer_protection_case_to_neo4j",
            lambda: import_consumer_case(str(base_root / "consumer_protection_case"), database),
        ),
        (
            "import_blacklist_log_to_neo4j",
            lambda: import_blacklist_log(str(base_root / "blacklist_log"), database, blacklist_max_ts),
        ),
    ]

    summary: Dict[str, Dict[str, int]] = {}
    for step_name, fn in steps:
        print(f"[full_import] start {step_name}")
        stats = fn()
        summary[step_name] = stats
        print(
            f"[full_import] done {step_name} "
            f"files={stats.get('files', 0)} total={stats.get('total_rows', 0)} "
            f"written={stats.get('written_rows', 0)} skipped={stats.get('skipped_rows', 0)}"
        )
    return summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Import full graph from data_processed_v2 in fixed order")
    parser.add_argument(
        "--input-root",
        default="/data/processed_v2",
        help="Root path for data_processed_v2",
    )
    parser.add_argument(
        "--database",
        default=DEFAULT_DB,
        help="Target Neo4j database name",
    )
    parser.add_argument(
        "--create-db",
        action="store_true",
        help="Create target database in Neo4j system database before import",
    )
    parser.add_argument(
        "--blacklist-max-ts",
        default="2025-11-20T00:00:00",
        help="Exclude blacklist rows whose timestamp >= this value",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    input_root = Path(args.input_root)
    ensure_dir(input_root)

    database = args.database
    if args.create_db:
        create_database_if_needed(database)
        print(f"[full_import] database ensured: {database}")

    summary = run_imports(input_root, database, args.blacklist_max_ts)
    print(f"[full_import] all steps finished. database={database}")
    for step_name, stats in summary.items():
        print(
            f"[full_import] {step_name}: files={stats.get('files', 0)} "
            f"total={stats.get('total_rows', 0)} written={stats.get('written_rows', 0)} "
            f"skipped={stats.get('skipped_rows', 0)}"
        )


if __name__ == "__main__":
    main()