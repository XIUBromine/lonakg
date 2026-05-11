from __future__ import annotations

import csv
import os
import shutil
import time
import tempfile
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

from dotenv import load_dotenv
from flask import Flask, jsonify, render_template, request, send_from_directory
from neo4j import GraphDatabase
from werkzeug.utils import secure_filename

from import_blacklist_log_to_neo4j import (
    BLACKLIST_MAX_TS,
    DEFAULT_DB,
    DEFAULT_PASSWORD,
    DEFAULT_URI,
    DEFAULT_USER,
    import_blacklist_log,
)
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

MAX_CONTENT_LENGTH = int(os.getenv("MAX_UPLOAD_MB", "200")) * 1024 * 1024
ALLOWED_EXTENSIONS = {"csv"}
SAMPLES_DIR = os.getenv("SAMPLES_DIR", "/data/samples")


@dataclass(frozen=True)
class DatasetRoute:
    """Dataset routing metadata used by both UI and API dispatching."""

    key: str
    label: str
    importer: str


@dataclass(frozen=True)
class ColumnValidationRule:
    """Column-level validation rule for uploaded CSV files."""

    required_all: Tuple[str, ...] = ()
    required_any_groups: Tuple[Tuple[str, ...], ...] = ()


# NOTE: Linkman family datasets are all routed to import_linkman.
DATASET_ROUTES: List[DatasetRoute] = [
    DatasetRoute("customer_log", "customer_log -> import_customer_logs", "import_customer_logs"),
    DatasetRoute("logout", "logout -> import_logout", "import_logout"),
    DatasetRoute("login", "login -> import_login", "import_login"),
    DatasetRoute("lbs_gps", "lbs_gps -> import_lbs_gps", "import_lbs_gps"),
    DatasetRoute("first_linkman", "first_linkman -> import_linkman", "import_linkman"),
    DatasetRoute("second_linkman", "second_linkman -> import_linkman", "import_linkman"),
    DatasetRoute("first_linkman_derived", "first_linkman_derived -> import_linkman", "import_linkman"),
    DatasetRoute("second_linkman_derived", "second_linkman_derived -> import_linkman", "import_linkman"),
    DatasetRoute("order", "order -> import_order", "import_order"),
    DatasetRoute("repay_plan", "repay_plan -> import_repay_plan", "import_repay_plan"),
    DatasetRoute("company_log", "company_log -> import_company_log", "import_company_log"),
    DatasetRoute("complaint", "complaint -> import_complaint", "import_complaint"),
    DatasetRoute("consumer_protection_case", "consumer_protection_case -> import_consumer_case", "import_consumer_case"),
    DatasetRoute("blacklist_log", "blacklist_log -> import_blacklist_log", "import_blacklist_log"),
]

DATASET_KEYS = {item.key for item in DATASET_ROUTES}

DATASET_COLUMN_RULES: Dict[str, ColumnValidationRule] = {
    "blacklist_log": ColumnValidationRule(
        required_all=(
            "c_customer_no",
            "c_reason_list",
            "c_mobile_phone",
            "c_identity_no",
            "d_begin_date",
            "n_banned_days",
            "n_duration",
        )
    ),
    "company_log": ColumnValidationRule(
        required_all=(
            "id",
            "companyid",
            "cid",
            "company_name",
            "company_address",
            "tel_phone",
            "create_date",
            "modify_date",
        )
    ),
    "complaint": ColumnValidationRule(
        required_all=(
            "c_id",
            "c_contact_phone_no",
            "c_register_phone_no",
            "c_customer_name",
            "c_identity_no",
            "d_create",
            "d_update",
        )
    ),
    "consumer_protection_case": ColumnValidationRule(
        required_all=(
            "c_id",
            "c_contact_phone_no",
            "c_register_phone_no",
            "c_customer_name",
            "c_identity_no",
            "d_create",
            "d_update",
        )
    ),
    "customer_log": ColumnValidationRule(
        required_all=(
            "id",
            "baseid",
            "customer_name",
            "identity_no",
            "mobile_phone",
            "marital_status",
            "status",
            "apply_amount_channel",
            "declared_degree",
            "modify_date",
            "create_date",
            "issue_channel",
            "regist_channel",
            "loan_apply_date",
            "log_create_date",
            "log_modify_date",
        )
    ),
    "first_linkman": ColumnValidationRule(
        required_all=(
            "id",
            "cid",
            "linkman_name",
            "mobile_phone",
            "relation_ship",
            "create_date",
            "modify_date",
        )
    ),
    "first_linkman_derived": ColumnValidationRule(
        required_all=(
            "id",
            "cid",
            "linkman_name",
            "mobile_phone",
            "relation_ship",
            "create_date",
            "modify_date",
        )
    ),
    "lbs_gps": ColumnValidationRule(
        required_all=(
            "id",
            "cid",
            "logitude",
            "latitude",
            "city_code",
            "city_name",
            "provice_code",
            "provice_name",
            "area_code",
            "area_name",
            "address",
            "geo_code",
            "current_flag",
            "create_date",
            "modify_date",
        )
    ),
    "login": ColumnValidationRule(
        required_all=(
            "phone_num",
            "cif_user_id",
            "channel_name",
            "login_time",
            "device_type",
            "device_no",
            "remote_ip",
            "app_channel",
            "bundle_id",
            "app_version",
            "os_name",
            "os_version",
            "td_device_id",
            "province",
            "city",
            "last_active_channel",
            "date",
        )
    ),
    "logout": ColumnValidationRule(
        required_all=(
            "id",
            "cid",
            "identity_no",
            "mobile_phone",
            "create_date",
            "modify_date",
        )
    ),
    "order": ColumnValidationRule(
        required_all=(
            "id",
            "channel_no",
            "loan_channel",
            "contract_no",
            "user_id",
            "apply_loan_name",
            "apply_loan_tel",
            "apply_ident_no",
            "apply_loan_date",
            "sign_date",
            "repay_day",
            "apply_card_no",
            "apply_bank",
            "apply_bank_mobile",
            "repay_card_no",
            "repay_bank",
            "repay_bank_mobile",
            "order_begin_date",
            "order_end_date",
            "firsttime_repay_date",
            "loan_date",
            "overdue_date",
            "order_status",
            "settle_date",
            "create_date",
            "modify_date",
        )
    ),
    "repay_plan": ColumnValidationRule(
        required_all=(
            "id",
            "order_id",
            "contract_no",
            "current_repay_date",
            "repayed_date",
            "early_repay_mark",
            "overdue_mark",
            "overdue_days",
            "current_repay_status",
            "create_date",
            "modify_date",
        )
    ),
    "second_linkman": ColumnValidationRule(
        required_all=(
            "id",
            "cid",
            "second_linkman_name",
            "second_mobile_phone",
            "relation_ship",
            "create_date",
            "modify_date",
        )
    ),
    "second_linkman_derived": ColumnValidationRule(
        required_all=(
            "id",
            "cid",
            "second_linkman_name",
            "second_mobile_phone",
            "relation_ship",
            "create_date",
            "modify_date",
        )
    ),
}


app = Flask(__name__, template_folder="templates")
app.config["MAX_CONTENT_LENGTH"] = MAX_CONTENT_LENGTH


def _normalize_col(name: str) -> str:
    return name.strip().lower()


def _read_csv_columns(file_path: Path) -> Set[str]:
    encodings = ("utf-8-sig", "utf-8", "gbk")
    for encoding in encodings:
        try:
            with file_path.open("r", encoding=encoding, newline="") as csv_file:
                reader = csv.reader(csv_file)
                header = next(reader, None)
                if not header:
                    return set()
                return {_normalize_col(col) for col in header if col is not None}
        except UnicodeDecodeError:
            continue
    raise ValueError(f"CSV 编码无法识别: {file_path.name}")


def _validate_file_columns(dataset: str, file_path: Path) -> None:
    rule = DATASET_COLUMN_RULES.get(dataset)
    if rule is None:
        return

    columns = _read_csv_columns(file_path)
    missing_all = [col for col in rule.required_all if _normalize_col(col) not in columns]
    if missing_all:
        raise ValueError(
            f"文件 {file_path.name} 缺少必需列: {', '.join(missing_all)}"
        )

    for group in rule.required_any_groups:
        normalized_group = [_normalize_col(col) for col in group]
        if not any(col in columns for col in normalized_group):
            raise ValueError(
                f"文件 {file_path.name} 至少需要包含以下列之一: {', '.join(group)}"
            )


def _validate_uploaded_files(dataset: str, files: List[Path]) -> None:
    for file_path in files:
        _validate_file_columns(dataset, file_path)


def _rule_to_hint(rule: ColumnValidationRule) -> Dict[str, object]:
    required_any = [list(group) for group in rule.required_any_groups]
    hint_parts: List[str] = []
    if rule.required_all:
        hint_parts.append(f"必须包含: {', '.join(rule.required_all)}")
    for group in rule.required_any_groups:
        hint_parts.append(f"至少包含其一: {' / '.join(group)}")
    return {
        "required_all": list(rule.required_all),
        "required_any_groups": required_any,
        "hint": "；".join(hint_parts) if hint_parts else "无列校验规则",
    }


def _build_column_rules_payload() -> Dict[str, Dict[str, object]]:
    return {dataset: _rule_to_hint(rule) for dataset, rule in DATASET_COLUMN_RULES.items()}


def _database_options_from_env() -> List[str]:
    raw = os.getenv("NEO4J_DATABASE_OPTIONS") or os.getenv("NEO4J_DATABASES") or ""
    return [item.strip() for item in raw.split(",") if item.strip()]


def _database_options_from_neo4j() -> List[str]:
    driver = GraphDatabase.driver(DEFAULT_URI, auth=(DEFAULT_USER, DEFAULT_PASSWORD))
    try:
        with driver.session(database="system") as session:
            records = session.run("SHOW DATABASES YIELD name RETURN name ORDER BY name")
            return [str(record["name"]) for record in records if record.get("name")]
    finally:
        driver.close()


def _fetch_graph_summary(database: str) -> Dict[str, object]:
    driver = GraphDatabase.driver(DEFAULT_URI, auth=(DEFAULT_USER, DEFAULT_PASSWORD))
    try:
        with driver.session(database=database) as session:
            node_count = session.run("MATCH (n) RETURN count(n) AS cnt").single()["cnt"]
            rel_count = session.run("MATCH ()-[r]->() RETURN count(r) AS cnt").single()["cnt"]
            labels = session.run("CALL db.labels() YIELD label RETURN label ORDER BY label").values()
            rel_types = session.run(
                "CALL db.relationshipTypes() YIELD relationshipType RETURN relationshipType ORDER BY relationshipType"
            ).values()
    finally:
        driver.close()

    return {
        "node_count": int(node_count or 0),
        "relationship_count": int(rel_count or 0),
        "labels": [row[0] for row in labels],
        "relationship_types": [row[0] for row in rel_types],
    }


def _build_database_options() -> List[str]:
    options: List[str] = []
    try:
        options = _database_options_from_neo4j()
    except Exception:  # noqa: BLE001
        options = _database_options_from_env()

    # 至少包含默认库，且默认库放第一个
    if DEFAULT_DB in options:
        options = [DEFAULT_DB] + [x for x in options if x != DEFAULT_DB]
    elif options:
        options = [DEFAULT_DB] + options
    else:
        options = [DEFAULT_DB]
    return options

def _allowed_file(filename: str) -> bool:
    if "." not in filename:
        return False
    return filename.rsplit(".", 1)[-1].lower() in ALLOWED_EXTENSIONS


def _build_run_dir(dataset: str) -> Path:
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    run_id = uuid.uuid4().hex[:8]
    run_dir = Path(tempfile.gettempdir()) / "kg_import_runtime" / dataset / f"{timestamp}_{run_id}"
    run_dir.mkdir(parents=True, exist_ok=True)
    return run_dir


def _save_uploaded_files(dataset: str) -> List[Path]:
    files = request.files.getlist("files")
    if not files:
        raise ValueError("缺少上传文件，字段名请使用 files")

    run_dir = _build_run_dir(dataset)
    saved_files: List[Path] = []
    for incoming in files:
        original_name = incoming.filename or ""
        safe_name = secure_filename(original_name)
        if not safe_name:
            raise ValueError(f"文件名无效: {original_name}")
        if not _allowed_file(safe_name):
            raise ValueError(f"仅支持 CSV 文件: {safe_name}")

        target = run_dir / safe_name
        incoming.save(target)
        saved_files.append(target)

    return saved_files


def _run_import(dataset: str, file_root: Path, database: str, blacklist_max_ts: Optional[str]) -> Dict[str, int]:
    """Dispatches one dataset upload to the corresponding importer."""
    if dataset == "customer_log":
        return import_customer_logs(str(file_root), database)
    if dataset == "logout":
        return import_logout(str(file_root), database)
    if dataset == "login":
        return import_login(str(file_root), database)
    if dataset == "lbs_gps":
        return import_lbs_gps(str(file_root), database)
    if dataset in {
        "first_linkman",
        "second_linkman",
        "first_linkman_derived",
        "second_linkman_derived",
    }:
        return import_linkman([str(file_root)], database)
    if dataset == "order":
        return import_order(str(file_root), database)
    if dataset == "repay_plan":
        return import_repay_plan(str(file_root), database)
    if dataset == "company_log":
        return import_company_log(str(file_root), database)
    if dataset == "complaint":
        return import_complaint(str(file_root), database)
    if dataset == "consumer_protection_case":
        return import_consumer_case(str(file_root), database)
    if dataset == "blacklist_log":
        return import_blacklist_log(str(file_root), database, blacklist_max_ts)
    raise ValueError(f"不支持的数据集: {dataset}")


@app.get("/")
def index() -> str:
    """Renders a simple upload UI for daily CSV incremental import."""
    return render_template(
        "incremental_import.html",
        datasets=DATASET_ROUTES,
        column_rules=_build_column_rules_payload(),
        database_options=_build_database_options(),
        default_database=DEFAULT_DB,
        default_blacklist_max_ts=BLACKLIST_MAX_TS or "",
        max_upload_mb=MAX_CONTENT_LENGTH // (1024 * 1024),
    )


@app.get("/api/datasets")
def api_datasets() -> object:
    """Returns dataset -> importer mapping for UI or API clients."""
    payload = [
        {
            "dataset": item.key,
            "mapping": item.label,
            "importer": item.importer,
            "column_rule": _rule_to_hint(DATASET_COLUMN_RULES.get(item.key, ColumnValidationRule())),
        }
        for item in DATASET_ROUTES
    ]
    return jsonify({"ok": True, "datasets": payload})


@app.get("/api/graph/summary")
def api_graph_summary() -> object:
    database = (request.args.get("database") or DEFAULT_DB).strip() or DEFAULT_DB
    try:
        summary = _fetch_graph_summary(database)
        return jsonify({"ok": True, "database": database, "summary": summary})
    except Exception as exc:  # noqa: BLE001
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.get("/samples/<path:filename>")
def download_sample(filename: str):
    safe_name = os.path.basename(filename)
    if not safe_name.endswith(".csv"):
        return jsonify({"ok": False, "error": "sample 文件名无效"}), 400
    dataset = safe_name[:-4]
    if dataset not in DATASET_KEYS:
        return jsonify({"ok": False, "error": "sample 数据集无效"}), 404
    if not os.path.isdir(SAMPLES_DIR):
        return jsonify({"ok": False, "error": "sample 目录不存在"}), 404
    sample_path = os.path.join(SAMPLES_DIR, safe_name)
    if not os.path.isfile(sample_path):
        return jsonify({"ok": False, "error": "sample 文件不存在"}), 404
    return send_from_directory(SAMPLES_DIR, safe_name, as_attachment=True)


@app.post("/api/import")
def api_import() -> object:
    """
    Upload endpoint for incremental import.

    Request (multipart/form-data):
    - dataset: one of DATASET_KEYS
    - files: one or more CSV files
    - database: optional, default is DEFAULT_DB
    - blacklist_max_ts: optional, only used by blacklist_log
    """
    started = time.time()
    dataset = (request.form.get("dataset") or "").strip()
    database = (request.form.get("database") or DEFAULT_DB).strip() or DEFAULT_DB
    blacklist_max_ts = (request.form.get("blacklist_max_ts") or "").strip() or None

    if dataset not in DATASET_KEYS:
        return jsonify({"ok": False, "error": f"dataset 不合法: {dataset}"}), 400

    run_root: Optional[Path] = None
    try:
        saved_files = _save_uploaded_files(dataset)
        _validate_uploaded_files(dataset, saved_files)
        run_root = saved_files[0].parent
        stats = _run_import(dataset, run_root, database, blacklist_max_ts)
    except ValueError as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400
    except Exception as exc:  # noqa: BLE001
        return jsonify({"ok": False, "error": str(exc)}), 500
    finally:
        if run_root is not None:
            shutil.rmtree(run_root, ignore_errors=True)

    elapsed_sec = round(time.time() - started, 3)
    return jsonify(
        {
            "ok": True,
            "dataset": dataset,
            "database": database,
            "files": [p.name for p in saved_files],
            "saved_dir": None,
            "storage_mode": "temp_cleaned",
            "elapsed_sec": elapsed_sec,
            "stats": stats,
        }
    )


if __name__ == "__main__":
    host = os.getenv("WEB_IMPORT_HOST", "0.0.0.0")
    port = int(os.getenv("WEB_IMPORT_PORT", "18081"))
    debug = os.getenv("WEB_IMPORT_DEBUG", "false").lower() == "true"

    app.run(host=host, port=port, debug=debug)
