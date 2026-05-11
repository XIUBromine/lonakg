#!/usr/bin/env python3
"""List CSV headers under /data/processed_v3.

Directory layout:
  /data/processed_v3/<dataset>/<YYYY-MM>/<YYYY-MM-DD>.csv

Prints one header per dataset for later column validation.
"""

from __future__ import annotations

import argparse
import os
import sys
from typing import Iterable, List, Optional, Tuple


def find_first_csv(dataset_path: str) -> Optional[str]:
    """Return the first CSV file path under a dataset folder, or None."""
    for month in sorted(os.listdir(dataset_path)):
        month_path = os.path.join(dataset_path, month)
        if not os.path.isdir(month_path):
            continue
        for name in sorted(os.listdir(month_path)):
            if not name.lower().endswith(".csv"):
                continue
            return os.path.join(month_path, name)
    return None


def iter_csv_files(root: str) -> Iterable[Tuple[str, str]]:
    """Yield one (dataset, file_path) per dataset under root."""
    for dataset in sorted(os.listdir(root)):
        dataset_path = os.path.join(root, dataset)
        if not os.path.isdir(dataset_path):
            continue
        first_csv = find_first_csv(dataset_path)
        if first_csv:
            yield dataset, first_csv


def read_header(file_path: str) -> List[str]:
    """Read the first line and split CSV header."""
    with open(file_path, "r", encoding="utf-8") as f:
        line = f.readline()
    return [col.strip() for col in line.rstrip("\n").split(",")]


def main() -> int:
    parser = argparse.ArgumentParser(
        description="List one CSV header per dataset under /data/processed_v3."
    )
    parser.add_argument(
        "--root",
        default="/data/processed_v3",
        help="Root directory containing dataset folders (default: /data/processed_v3)",
    )
    args = parser.parse_args()

    root = args.root
    if not os.path.isdir(root):
        print(f"Root directory not found: {root}")
        return 2

    for dataset, file_path in iter_csv_files(root):
        try:
            header = read_header(file_path)
        except Exception as exc:
            print(f"[ERROR] {dataset} {file_path}: {exc}")
            continue
        print(f"{dataset}\t{file_path}\t{','.join(header)}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
