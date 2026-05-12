import argparse
import os
from pathlib import Path
from typing import Iterable, List, Tuple

from neo4j import GraphDatabase

from rule_based_risk import RuleBasedKHopRiskEngine


DIR_MODEL = Path(__file__).parent
DEFAULT_INPUT = DIR_MODEL / "output/device_count_sort.txt"
DEFAULT_OUTPUT = DIR_MODEL / "output/mul-uid_device_risks.txt"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Batch uid risk by device_no list"
    )
    parser.add_argument("--input", default=str(DEFAULT_INPUT))
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT))
    parser.add_argument("--k", type=int, default=2)
    parser.add_argument("--database", default=os.getenv("NEO4J_DATABASE", "prod"))
    parser.add_argument("--uri", default=os.getenv("NEO4J_URI", "bolt://localhost:7687"))
    parser.add_argument("--user", default=os.getenv("NEO4J_USER", "neo4j"))
    parser.add_argument(
        "--password", default=os.getenv("NEO4J_PASSWORD", "kBXwIuxLTvgxnbGD")
    )
    return parser.parse_args()


def _strip_quotes(value: str) -> str:
    value = value.strip()
    if len(value) >= 2 and value[0] == '"' and value[-1] == '"':
        return value[1:-1]
    return value


def read_devices(input_path: Path) -> Iterable[Tuple[str, int]]:
    with open(input_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            parts = line.split("\t")
            if len(parts) < 2:
                continue
            device_key = _strip_quotes(parts[0])
            try:
                uid_count = int(parts[1])
            except ValueError:
                uid_count = 0
            yield device_key, uid_count


def fetch_device_uids(driver, database: str, device_key: str) -> List[str]:
    query = """
    MATCH (u:uid)-[]-(d:device_no {key: $key})
    RETURN DISTINCT u.key AS uid_key
    """
    with driver.session(database=database) as session:
        rows = session.run(query, key=device_key)
        return [row["uid_key"] for row in rows if row["uid_key"]]


def append_results(output_path: Path, rows: List[Tuple[str, str, float]]) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "a", encoding="utf-8") as f:
        for device_key, uid_key, risk in rows:
            f.write(f"{device_key}\t{uid_key}\t{risk}\n")


def main() -> None:
    args = parse_args()
    input_path = Path(args.input)
    output_path = Path(args.output)

    if not input_path.exists():
        print(f"Input file not found: {input_path}")
        return

    engine = RuleBasedKHopRiskEngine(
        uri=args.uri,
        user=args.user,
        password=args.password,
        database=args.database,
    )

    try:
        for device_key, uid_count in read_devices(input_path):
            if not device_key:
                continue

            print(f"Start device {device_key}, expected uids: {uid_count}", flush=True)

            uid_keys = fetch_device_uids(engine.driver, args.database, device_key)
            results: List[Tuple[str, str, float]] = []
            processed = 0

            for uid_key in uid_keys:
                try:
                    risk = engine.compute_risk(node_type="uid", node_key=uid_key, k=args.k)
                except Exception:
                    risk = None
                results.append((device_key, uid_key, risk))
                processed += 1

                if len(results) >= 20:
                    append_results(output_path, results)
                    results.clear()
                    print(
                        f"Device {device_key}: processed {processed}/{len(uid_keys)} uids",
                        flush=True,
                    )

            if results:
                append_results(output_path, results)
                print(
                    f"Device {device_key}: processed {processed}/{len(uid_keys)} uids",
                    flush=True,
                )
            print(
                f"Device {device_key}: expected {uid_count}, got {len(uid_keys)} uids, written {len(results)} rows"
            )
    finally:
        engine.close()


if __name__ == "__main__":
    main()
