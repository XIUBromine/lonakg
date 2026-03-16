from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
import time
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

from dotenv import load_dotenv
from neo4j import GraphDatabase

from stream_kg_api import CONSTRAINT_QUERIES, process_event_in_session


load_dotenv()

DEFAULT_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
DEFAULT_USER = os.getenv("NEO4J_USER", "neo4j")
DEFAULT_PASSWORD = os.getenv("NEO4J_PASSWORD", "kBXwIuxLTvgxnbGD")
DEFAULT_DB = "prod"

MONTH_FILE_RE = re.compile(r"^\d{4}-\d{2}\.jsonl$")


def build_logger(log_file: Optional[Path], level: int = logging.INFO) -> logging.Logger:
    logger = logging.getLogger("import_events_prod")
    logger.setLevel(level)
    logger.handlers.clear()

    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    if log_file is not None:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger


def list_monthly_jsonl(events_dir: Path) -> List[Path]:
    files = [p for p in events_dir.iterdir() if p.is_file() and MONTH_FILE_RE.match(p.name)]
    files.sort(key=lambda p: p.name)
    return files


def load_checkpoint(checkpoint_file: Path) -> Dict[str, object]:
    if not checkpoint_file.exists():
        return {}
    try:
        with checkpoint_file.open("r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            return data
        return {}
    except Exception:
        return {}


def save_checkpoint(checkpoint_file: Path, current_file: str, line_no: int, counters: Dict[str, int]) -> None:
    checkpoint_file.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "current_file": current_file,
        "line_no": line_no,
        "counters": counters,
        "updated_at": int(time.time()),
    }
    tmp_file = checkpoint_file.with_suffix(checkpoint_file.suffix + ".tmp")
    with tmp_file.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=True)
    tmp_file.replace(checkpoint_file)


def should_skip_file(file_name: str, checkpoint_file_name: Optional[str]) -> bool:
    if checkpoint_file_name is None:
        return False
    return file_name < checkpoint_file_name


def maybe_resume_line_start(file_name: str, checkpoint_file_name: Optional[str], checkpoint_line_no: int) -> int:
    if checkpoint_file_name is None:
        return 1
    if file_name == checkpoint_file_name:
        return max(1, checkpoint_line_no + 1)
    return 1


def create_constraints(driver, database: str, logger: logging.Logger) -> None:
    with driver.session(database=database) as session:
        for query in CONSTRAINT_QUERIES:
            session.run(query)
    logger.info("constraints ensured for database=%s", database)


def process_file(
    session,
    file_path: Path,
    start_line: int,
    logger: logging.Logger,
    counters: Dict[str, int],
    progress_interval: int,
    checkpoint_every: int,
    checkpoint_file: Path,
    stop_on_error: bool,
) -> Tuple[bool, int]:
    processed_in_file = 0
    file_start = time.time()

    with file_path.open("r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, start=1):
            if line_no < start_line:
                continue

            text = line.strip()
            if text == "":
                counters["skipped_empty"] += 1
                continue

            counters["total_lines"] += 1
            processed_in_file += 1

            try:
                event = json.loads(text)
            except Exception as exc:
                counters["json_errors"] += 1
                logger.error("json parse error file=%s line=%d err=%s", file_path.name, line_no, exc)
                if stop_on_error:
                    save_checkpoint(checkpoint_file, file_path.name, line_no, counters)
                    return False, line_no
                continue

            try:
                process_event_in_session(session, event)
                counters["success"] += 1
            except Exception as exc:
                counters["event_errors"] += 1
                logger.error("event process error file=%s line=%d err=%s", file_path.name, line_no, exc)
                if stop_on_error:
                    save_checkpoint(checkpoint_file, file_path.name, line_no, counters)
                    return False, line_no

            if processed_in_file % checkpoint_every == 0:
                save_checkpoint(checkpoint_file, file_path.name, line_no, counters)

            if counters["total_lines"] % progress_interval == 0:
                elapsed = time.time() - counters["start_ts"]
                speed = counters["total_lines"] / elapsed if elapsed > 0 else 0.0
                logger.info(
                    "progress total=%d success=%d json_errors=%d event_errors=%d speed=%.2f lines/s current=%s:%d",
                    counters["total_lines"],
                    counters["success"],
                    counters["json_errors"],
                    counters["event_errors"],
                    speed,
                    file_path.name,
                    line_no,
                )

    file_elapsed = time.time() - file_start
    logger.info(
        "file done file=%s processed=%d elapsed=%.2fs avg_speed=%.2f lines/s",
        file_path.name,
        processed_in_file,
        file_elapsed,
        processed_in_file / file_elapsed if file_elapsed > 0 else 0.0,
    )

    save_checkpoint(checkpoint_file, file_path.name, 10**12, counters)
    return True, processed_in_file


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Bulk import monthly event jsonl files into Neo4j prod database")
    parser.add_argument("--events-dir", default="/data/processed/events", help="Directory containing YYYY-MM.jsonl files")
    parser.add_argument("--uri", default=DEFAULT_URI, help="Neo4j bolt URI")
    parser.add_argument("--user", default=DEFAULT_USER, help="Neo4j username")
    parser.add_argument("--password", default=DEFAULT_PASSWORD, help="Neo4j password")
    parser.add_argument("--database", default=DEFAULT_DB, help="Neo4j database name")
    parser.add_argument("--log-file", default="api/import_events_prod.log", help="Path to log file")
    parser.add_argument("--checkpoint-file", default="api/import_events_prod.checkpoint.json", help="Path to checkpoint file")
    parser.add_argument("--progress-interval", type=int, default=50000, help="Progress log interval in lines")
    parser.add_argument("--checkpoint-every", type=int, default=10000, help="Checkpoint save interval in lines")
    parser.add_argument("--stop-on-error", action="store_true", help="Stop immediately on first error")
    parser.add_argument("--init-constraints", action="store_true", help="Create constraints before importing")
    parser.add_argument("--no-resume", action="store_true", help="Ignore checkpoint and start from beginning")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    events_dir = Path(args.events_dir)
    log_file = Path(args.log_file)
    checkpoint_file = Path(args.checkpoint_file)
    logger = build_logger(log_file)

    if not events_dir.exists() or not events_dir.is_dir():
        raise FileNotFoundError(f"events dir not found: {events_dir}")

    files = list_monthly_jsonl(events_dir)
    if not files:
        logger.warning("no monthly jsonl files found in %s", events_dir)
        return

    checkpoint = {} if args.no_resume else load_checkpoint(checkpoint_file)
    checkpoint_file_name = checkpoint.get("current_file") if isinstance(checkpoint.get("current_file"), str) else None
    checkpoint_line_no = int(checkpoint.get("line_no", 0)) if str(checkpoint.get("line_no", "0")).isdigit() else 0

    counters: Dict[str, int] = {
        "total_lines": 0,
        "success": 0,
        "json_errors": 0,
        "event_errors": 0,
        "skipped_empty": 0,
        "start_ts": int(time.time()),
    }

    if isinstance(checkpoint.get("counters"), dict) and not args.no_resume:
        old_counters = checkpoint["counters"]
        for key in ("total_lines", "success", "json_errors", "event_errors", "skipped_empty"):
            if key in old_counters and isinstance(old_counters[key], int):
                counters[key] = old_counters[key]

    logger.info("import start events_dir=%s database=%s file_count=%d", events_dir, args.database, len(files))
    if checkpoint_file_name and not args.no_resume:
        logger.info("resume from checkpoint file=%s line=%d", checkpoint_file_name, checkpoint_line_no)

    driver = GraphDatabase.driver(args.uri, auth=(args.user, args.password))

    try:
        if args.init_constraints:
            create_constraints(driver, args.database, logger)

        with driver.session(database=args.database) as session:
            for file_path in files:
                if should_skip_file(file_path.name, checkpoint_file_name):
                    logger.info("skip completed file=%s", file_path.name)
                    continue

                start_line = maybe_resume_line_start(file_path.name, checkpoint_file_name, checkpoint_line_no)
                logger.info("processing file=%s start_line=%d", file_path.name, start_line)

                ok, _ = process_file(
                    session=session,
                    file_path=file_path,
                    start_line=start_line,
                    logger=logger,
                    counters=counters,
                    progress_interval=max(1, args.progress_interval),
                    checkpoint_every=max(1, args.checkpoint_every),
                    checkpoint_file=checkpoint_file,
                    stop_on_error=args.stop_on_error,
                )

                checkpoint_file_name = file_path.name
                checkpoint_line_no = 10**12

                if not ok:
                    logger.error("stopped on error at file=%s", file_path.name)
                    return

        elapsed = time.time() - counters["start_ts"]
        logger.info(
            "import finished total=%d success=%d json_errors=%d event_errors=%d empty=%d elapsed=%.2fs avg_speed=%.2f lines/s",
            counters["total_lines"],
            counters["success"],
            counters["json_errors"],
            counters["event_errors"],
            counters["skipped_empty"],
            elapsed,
            counters["total_lines"] / elapsed if elapsed > 0 else 0.0,
        )

    finally:
        driver.close()


if __name__ == "__main__":
    main()
