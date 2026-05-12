import argparse
from pathlib import Path
import csv
import sqlite3
from typing import Dict, Iterable, Optional, Tuple


DIR_MODEL = Path(__file__).parent
DEFAULT_INPUT = DIR_MODEL / "output/mul-uid_device_risks.txt"
DEFAULT_OUTPUT = DIR_MODEL / "output/device_analysis.txt"
CSV_FILENAME = Path("/data/aiimport_1119/登录信息.csv")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Summarize per-device uid risk stats with channel info"
    )
    parser.add_argument("--input", default=str(DEFAULT_INPUT))
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT))
    parser.add_argument("--channel-csv", default=str(CSV_FILENAME))
    parser.add_argument("--channel-encoding", default="utf-8-sig")
    parser.add_argument(
        "--channel-mode",
        default="sqlite",
        choices=["sqlite", "memory"],
    )
    parser.add_argument(
        "--channel-db",
        default=str(DIR_MODEL / "output/device_channel_index.sqlite"),
    )
    return parser.parse_args()


def _strip_quotes(value: str) -> str:
    value = value.strip()
    if len(value) >= 2 and value[0] == '"' and value[-1] == '"':
        return value[1:-1]
    return value


def _find_field(fieldnames: Iterable[str], candidates: Iterable[str]) -> Optional[str]:
    normalized = []
    for name in fieldnames:
        if not name:
            continue
        normalized.append((name, name.strip().lower()))

    for candidate in candidates:
        candidate_norm = candidate.strip().lower()
        for name, name_norm in normalized:
            if name_norm == candidate_norm:
                return name

    for candidate in candidates:
        for name, _ in normalized:
            if candidate in name:
                return name
    return None


def _iter_channel_rows(
    csv_path: Path, encoding: str
) -> Iterable[Tuple[Optional[str], Optional[str]]]:
    encodings = [encoding]
    if encoding == "utf-8-sig":
        encodings.append("gbk")

    for candidate_encoding in encodings:
        try:
            with open(csv_path, "r", encoding=candidate_encoding, newline="") as f:
                reader = csv.DictReader(f)
                if not reader.fieldnames:
                    print(f"Channel CSV header missing: {csv_path}")
                    return

                device_field = _find_field(reader.fieldnames, ["device_no", "设备号"])
                channel_field = _find_field(reader.fieldnames, ["app_channel", "渠道"])

                if not device_field or not channel_field:
                    print(
                        "Required columns not found in channel CSV. "
                        f"device_field={device_field}, channel_field={channel_field}"
                    )
                    return

                for row in reader:
                    device_raw = row.get(device_field, "")
                    channel_raw = row.get(channel_field, "")
                    device = _strip_quotes(device_raw).lower()
                    channel = channel_raw.strip()
                    yield device, channel
            break
        except UnicodeDecodeError:
            continue


def load_device_channels(csv_path: Path, encoding: str) -> Dict[str, str]:
    if not csv_path.exists():
        print(f"Channel CSV not found: {csv_path}")
        return {}

    device_channels: Dict[str, str] = {}
    for device, channel in _iter_channel_rows(csv_path, encoding):
        if not device or not channel:
            continue
        if device not in device_channels:
            device_channels[device] = channel

    return device_channels


def build_channel_index(
    csv_path: Path, encoding: str, db_path: Path
) -> Optional[sqlite3.Connection]:
    if not csv_path.exists():
        print(f"Channel CSV not found: {csv_path}")
        return None

    db_path.parent.mkdir(parents=True, exist_ok=True)
    if db_path.exists():
        db_path.unlink()

    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS device_channel ("
            "device_no TEXT PRIMARY KEY, app_channel TEXT)"
        )

        batch = []
        for device, channel in _iter_channel_rows(csv_path, encoding):
            if not device or not channel:
                continue
            batch.append((device, channel))
            if len(batch) >= 5000:
                conn.executemany(
                    "INSERT OR IGNORE INTO device_channel "
                    "(device_no, app_channel) VALUES (?, ?)",
                    batch,
                )
                conn.commit()
                batch.clear()

        if batch:
            conn.executemany(
                "INSERT OR IGNORE INTO device_channel "
                "(device_no, app_channel) VALUES (?, ?)",
                batch,
            )
            conn.commit()

        return conn
    except Exception:
        conn.close()
        raise


def lookup_channel(
    device_key: str,
    channel_map: Optional[Dict[str, str]],
    channel_conn: Optional[sqlite3.Connection],
) -> str:
    if channel_map is not None:
        return channel_map.get(device_key, "UNKNOWN")

    if channel_conn is None:
        return "UNKNOWN"

    row = channel_conn.execute(
        "SELECT app_channel FROM device_channel WHERE device_no = ?",
        (device_key,),
    ).fetchone()
    return row[0] if row and row[0] else "UNKNOWN"


def _parse_line(line: str) -> Optional[Tuple[str, float]]:
    line = line.strip()
    if not line:
        return None
    parts = line.split()
    if len(parts) < 3:
        return None

    device_key = _strip_quotes(parts[0]).lower()
    risk_raw = parts[2].strip()
    try:
        risk = float(risk_raw)
    except ValueError:
        return None
    return device_key, risk


def write_summary_row(
    f_out,
    device_key: str,
    channel: str,
    min_risk: Optional[float],
    max_risk: Optional[float],
    sum_risk: float,
    count: int,
) -> None:
    if not device_key:
        return

    if count > 0 and min_risk is not None and max_risk is not None:
        avg = sum_risk / count
        f_out.write(
            f"{device_key}\t{channel}\t{max_risk:.6f}\t{min_risk:.6f}\t{avg:.6f}\n"
        )
    else:
        f_out.write(f"{device_key}\t{channel}\tNA\tNA\tNA\n")


def main() -> None:
    args = parse_args()
    input_path = Path(args.input)
    output_path = Path(args.output)
    channel_csv = Path(args.channel_csv)

    if not input_path.exists():
        print(f"Input file not found: {input_path}")
        return

    channel_map: Optional[Dict[str, str]] = None
    channel_conn: Optional[sqlite3.Connection] = None
    if args.channel_mode == "memory":
        channel_map = load_device_channels(channel_csv, args.channel_encoding)
        if not channel_map:
            print("Channel map is empty. Missing or invalid channel CSV.")
    else:
        channel_conn = build_channel_index(
            channel_csv, args.channel_encoding, Path(args.channel_db)
        )
        if channel_conn is None:
            print("Channel index not available. Missing or invalid channel CSV.")

    output_path.parent.mkdir(parents=True, exist_ok=True)

    current_device = None
    min_risk = None
    max_risk = None
    sum_risk = 0.0
    count = 0

    with open(input_path, "r", encoding="utf-8") as f_in, open(
        output_path, "w", encoding="utf-8"
    ) as f_out:
        for line in f_in:
            parsed = _parse_line(line)
            if not parsed:
                continue
            device_key, risk = parsed

            if current_device is None:
                current_device = device_key

            if device_key != current_device:
                channel = lookup_channel(current_device, channel_map, channel_conn)
                write_summary_row(
                    f_out,
                    current_device,
                    channel,
                    min_risk,
                    max_risk,
                    sum_risk,
                    count,
                )
                current_device = device_key
                min_risk = None
                max_risk = None
                sum_risk = 0.0
                count = 0

            if min_risk is None or risk < min_risk:
                min_risk = risk
            if max_risk is None or risk > max_risk:
                max_risk = risk
            sum_risk += risk
            count += 1

        if current_device is not None:
            channel = lookup_channel(current_device, channel_map, channel_conn)
            write_summary_row(
                f_out,
                current_device,
                channel,
                min_risk,
                max_risk,
                sum_risk,
                count,
            )

    if channel_conn is not None:
        channel_conn.close()

    print(f"Summary written to: {output_path}")


if __name__ == "__main__":
    main()
