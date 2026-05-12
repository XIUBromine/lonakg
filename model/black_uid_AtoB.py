# 对比得出只存在blacklog文件的黑名单节点

from __future__ import annotations
from pathlib import Path
from typing import Iterable, Iterator, Set

import pandas as pd


BATCH_SIZE = 100


def iter_uid_from_csv(csv_path: Path, batch_size: int = BATCH_SIZE) -> Iterator[str]:
    reader = pd.read_csv(
        csv_path,
        dtype=str,
        keep_default_na=False,
        na_filter=False,
        encoding="utf-8-sig",
        usecols=[0],
        chunksize=batch_size,
    )
    for chunk in reader:
        uid_col = chunk.columns[0]
        series = chunk[uid_col].astype(str).str.strip()
        series = series[(series != "") & (series.str.lower() != "nan") & (series.str.lower() != "none")]
        for uid in series.tolist():
            yield uid


def append_batch(output_txt: Path, batch: Iterable[str]) -> None:
    lines = list(batch)
    if not lines:
        return
    with output_txt.open("a", encoding="utf-8") as f:
        f.write("\n".join(lines))
        f.write("\n")


def write_unique_uids_from_csv(csv_path: Path, output_txt: Path, batch_size: int = BATCH_SIZE) -> Set[str]:
    output_txt.parent.mkdir(parents=True, exist_ok=True)
    output_txt.write_text("", encoding="utf-8")

    seen: Set[str] = set()
    buffer: list[str] = []
    for uid in iter_uid_from_csv(csv_path, batch_size=batch_size):
        if uid in seen:
            continue
        seen.add(uid)
        buffer.append(uid)
        if len(buffer) >= batch_size:
            append_batch(output_txt, buffer)
            buffer.clear()

    if buffer:
        append_batch(output_txt, buffer)

    return seen


def read_uid_set_from_txt(txt_path: Path) -> Set[str]:
    if not txt_path.exists():
        return set()
    lines = [line.strip() for line in txt_path.read_text(encoding="utf-8").splitlines()]
    return {line for line in lines if line}


def write_uids_to_txt_in_batches(uids: Iterable[str], output_txt: Path, batch_size: int = BATCH_SIZE) -> None:
    output_txt.parent.mkdir(parents=True, exist_ok=True)
    output_txt.write_text("", encoding="utf-8")

    buffer: list[str] = []
    for uid in uids:
        buffer.append(uid)
        if len(buffer) >= batch_size:
            append_batch(output_txt, buffer)
            buffer.clear()

    if buffer:
        append_batch(output_txt, buffer)


def resolve_input_file(root: Path, filename: str) -> Path:
    candidate_dirs = [
        Path("/data/aiimport_1119"),
        root / "data/aiimport_1119",
    ]
    for base_dir in candidate_dirs:
        candidate = base_dir / filename
        if candidate.exists():
            return candidate
    checked = ", ".join(str(d / filename) for d in candidate_dirs)
    raise FileNotFoundError(f"文件不存在，已检查: {checked}")


def main() -> None:
    root = Path(__file__).resolve().parents[1]
    log_csv = resolve_input_file(root, "黑名单log.csv")
    blacklist_csv = resolve_input_file(root, "黑名单.csv")
    blacklist_txt = root / "model/output/black_uid_1120.txt"
    log_txt = root / "model/output/black_uid_0323.txt"
    output_txt = root / "model/output/black_uid_1120_0323.txt"

    blacklist_uids = write_unique_uids_from_csv(
        blacklist_csv,
        blacklist_txt,
        batch_size=BATCH_SIZE,
    )
    log_uids = write_unique_uids_from_csv(
        log_csv,
        log_txt,
        batch_size=BATCH_SIZE,
    )

    # 按要求基于两个输出文件做对比
    blacklist_uid_set = read_uid_set_from_txt(blacklist_txt)
    log_uid_set = read_uid_set_from_txt(log_txt)
    only_in_log = sorted(log_uid_set - blacklist_uid_set)
    write_uids_to_txt_in_batches(only_in_log, output_txt, batch_size=BATCH_SIZE)

    print(f"黑名单CSV客户ID数: {len(blacklist_uids)}")
    print(f"黑名单log CSV客户ID数: {len(log_uids)}")
    print(f"黑名单TXT客户ID数: {len(blacklist_uid_set)}")
    print(f"黑名单log TXT客户ID数: {len(log_uid_set)}")
    print(f"仅在log中的客户ID数: {len(only_in_log)}")
    print(f"输出文件: {blacklist_txt}")
    print(f"输出文件: {log_txt}")
    print(f"输出文件: {output_txt}")


if __name__ == "__main__":
    main()
