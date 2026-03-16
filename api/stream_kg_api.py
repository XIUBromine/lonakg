from __future__ import annotations

import json
import math
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from dotenv import load_dotenv
from flask import Flask, jsonify, request
from neo4j import GraphDatabase


load_dotenv()

NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "kBXwIuxLTvgxnbGD")
NEO4J_DATABASE = os.getenv("NEO4J_DATABASE", "dev")

app = Flask(__name__)

driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))


CONSTRAINT_QUERIES = [
    "CREATE CONSTRAINT uid_key IF NOT EXISTS FOR (n:uid) REQUIRE n.key IS UNIQUE",
    "CREATE CONSTRAINT identity_no_key IF NOT EXISTS FOR (n:identity_no) REQUIRE n.key IS UNIQUE",
    "CREATE CONSTRAINT phone_num_key IF NOT EXISTS FOR (n:phone_num) REQUIRE n.key IS UNIQUE",
    "CREATE CONSTRAINT device_no_key IF NOT EXISTS FOR (n:device_no) REQUIRE n.key IS UNIQUE",
    "CREATE CONSTRAINT td_device_id_key IF NOT EXISTS FOR (n:td_device_id) REQUIRE n.key IS UNIQUE",
    "CREATE CONSTRAINT remote_ip_key IF NOT EXISTS FOR (n:remote_ip) REQUIRE n.key IS UNIQUE",
    "CREATE CONSTRAINT geo_code_key IF NOT EXISTS FOR (n:geo_code) REQUIRE n.key IS UNIQUE",
    "CREATE CONSTRAINT card_no_key IF NOT EXISTS FOR (n:card_no) REQUIRE n.key IS UNIQUE",
    "CREATE CONSTRAINT order_key IF NOT EXISTS FOR (n:order) REQUIRE n.key IS UNIQUE",
]


def normalize_scalar(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    if isinstance(value, str):
        text = value.strip()
        if text == "":
            return None
        lower = text.lower()
        if lower in {"nan", "none", "null"}:
            return None
        return text
    if isinstance(value, (int, bool)):
        return str(value)
    return str(value)


def deep_normalize(value: Any) -> Any:
    if isinstance(value, dict):
        return {k: deep_normalize(v) for k, v in value.items()}
    if isinstance(value, list):
        return [deep_normalize(v) for v in value]
    normalized = normalize_scalar(value)
    return normalized if normalized is not None else None


def required(value: Optional[str], field_name: str) -> str:
    if value is None:
        raise ValueError(f"Missing required field: {field_name}")
    return value


def merge_counter_edge(
    tx,
    src_label: str,
    src_key: str,
    rel_type: str,
    dst_label: str,
    dst_key: str,
    ts: str,
) -> None:
    query = f"""
    MERGE (s:{src_label} {{key: $src_key}})
    ON CREATE SET s.created_at = datetime($ts), s.updated_at = datetime($ts)
    ON MATCH SET s.updated_at = datetime($ts)
    MERGE (d:{dst_label} {{key: $dst_key}})
    ON CREATE SET d.created_at = datetime($ts), d.updated_at = datetime($ts)
    ON MATCH SET d.updated_at = datetime($ts)
    MERGE (s)-[r:{rel_type}]->(d)
    ON CREATE SET r.first_seen_ts = datetime($ts), r.last_seen_ts = datetime($ts), r.event_count = 1
    ON MATCH SET r.last_seen_ts = datetime($ts), r.event_count = coalesce(r.event_count, 0) + 1
    """
    tx.run(query, src_key=src_key, dst_key=dst_key, ts=ts)


def init_constraints() -> None:
    with driver.session(database=NEO4J_DATABASE) as session:
        for query in CONSTRAINT_QUERIES:
            session.run(query)


def handle_customer_update(tx, event: Dict[str, Any]) -> Dict[str, Any]:
    ts = required(normalize_scalar(event.get("ts")), "ts")
    data = event.get("data") or {}
    uid = required(normalize_scalar(data.get("baseid")), "data.baseid")
    identity_no = normalize_scalar(data.get("identity_no"))
    mobile_phone = normalize_scalar(data.get("mobile_phone"))

    query = """
    MERGE (u:uid {key: $uid})
    ON CREATE SET u.created_at = datetime($ts), u.updated_at = datetime($ts), u.logout_flag = false
    ON MATCH SET u.updated_at = datetime($ts)
    WITH u, $identity_no AS identity_no, $mobile_phone AS mobile_phone, $ts AS ts
    WITH u, identity_no, mobile_phone, ts,
         coalesce(u.latest_identity, '__NULL__') <> coalesce(identity_no, '__NULL__') AS identity_changed,
         coalesce(u.latest_phone, '__NULL__') <> coalesce(mobile_phone, '__NULL__') AS phone_changed
    FOREACH (_ IN CASE WHEN identity_changed OR phone_changed THEN [1] ELSE [] END |
        SET u.last_update_ts = datetime(ts),
            u.updated_at = datetime(ts)
    )
    FOREACH (_ IN CASE WHEN identity_changed THEN [1] ELSE [] END |
        SET u.latest_identity = identity_no
    )
    FOREACH (_ IN CASE WHEN phone_changed THEN [1] ELSE [] END |
        SET u.latest_phone = mobile_phone
    )
    FOREACH (_ IN CASE WHEN identity_changed AND identity_no IS NOT NULL THEN [1] ELSE [] END |
        MERGE (i:identity_no {key: identity_no})
        ON CREATE SET i.created_at = datetime(ts), i.updated_at = datetime(ts), i.logout_count = 0
        ON MATCH SET i.updated_at = datetime(ts)
        MERGE (u)-[r:HAS_IDENTITY]->(i)
        ON CREATE SET r.first_seen_ts = datetime(ts), r.last_seen_ts = datetime(ts), r.event_count = 1
        ON MATCH SET r.last_seen_ts = datetime(ts), r.event_count = coalesce(r.event_count, 0) + 1
    )
    FOREACH (_ IN CASE WHEN phone_changed AND mobile_phone IS NOT NULL THEN [1] ELSE [] END |
        MERGE (p:phone_num {key: mobile_phone})
        ON CREATE SET p.created_at = datetime(ts), p.updated_at = datetime(ts), p.logout_count = 0
        ON MATCH SET p.updated_at = datetime(ts)
        MERGE (u)-[r:HAS_PHONE]->(p)
        ON CREATE SET r.first_seen_ts = datetime(ts), r.last_seen_ts = datetime(ts), r.event_count = 1
        ON MATCH SET r.last_seen_ts = datetime(ts), r.event_count = coalesce(r.event_count, 0) + 1
    )
    RETURN identity_changed, phone_changed
    """
    record = tx.run(query, uid=uid, identity_no=identity_no, mobile_phone=mobile_phone, ts=ts).single()

    identity_changed = bool(record["identity_changed"]) if record else False
    phone_changed = bool(record["phone_changed"]) if record else False

    return {
        "event_type": "customer_update",
        "uid": uid,
        "deduplicated": (not identity_changed) and (not phone_changed),
        "identity_changed": identity_changed,
        "phone_changed": phone_changed,
    }


def handle_login(tx, event: Dict[str, Any]) -> Dict[str, Any]:
    ts = required(normalize_scalar(event.get("ts")), "ts")
    data = event.get("data") or {}
    uid = required(normalize_scalar(data.get("cif_user_id")), "data.cif_user_id")

    tx.run(
        """
        MERGE (u:uid {key: $uid})
        ON CREATE SET u.created_at = datetime($ts), u.updated_at = datetime($ts), u.logout_flag = false
        ON MATCH SET u.updated_at = datetime($ts)
        SET u.last_login_ts = datetime($ts)
        """,
        uid=uid,
        ts=ts,
    )

    merged_targets = 0
    phone_num = normalize_scalar(data.get("phone_num"))
    device_no = normalize_scalar(data.get("device_no"))
    td_device_id = normalize_scalar(data.get("td_device_id"))
    remote_ip = normalize_scalar(data.get("remote_ip"))

    if phone_num is not None:
        merge_counter_edge(tx, "uid", uid, "LOGIN_WITH_PHONE", "phone_num", phone_num, ts)
        merged_targets += 1
    if device_no is not None:
        merge_counter_edge(tx, "uid", uid, "LOGIN_WITH_DEVICE", "device_no", device_no, ts)
        merged_targets += 1
    if td_device_id is not None:
        merge_counter_edge(tx, "uid", uid, "LOGIN_WITH_TD_DEVICE", "td_device_id", td_device_id, ts)
        merged_targets += 1
    if remote_ip is not None:
        merge_counter_edge(tx, "uid", uid, "LOGIN_WITH_IP", "remote_ip", remote_ip, ts)
        merged_targets += 1

    return {"event_type": "login", "uid": uid, "merged_targets": merged_targets}


def handle_gps(tx, event: Dict[str, Any]) -> Dict[str, Any]:
    ts = required(normalize_scalar(event.get("ts")), "ts")
    data = event.get("data") or {}
    uid = required(normalize_scalar(data.get("cid")), "data.cid")
    geo_code = required(normalize_scalar(data.get("geo_code")), "data.geo_code")

    merge_counter_edge(tx, "uid", uid, "LOCATED_AT", "geo_code", geo_code, ts)
    return {"event_type": "gps", "uid": uid, "geo_code": geo_code}


def handle_contact_edit(tx, event: Dict[str, Any]) -> Dict[str, Any]:
    ts = required(normalize_scalar(event.get("ts")), "ts")
    data = event.get("data") or {}
    uid = required(normalize_scalar(data.get("cid")), "data.cid")
    contact_phone = required(normalize_scalar(data.get("mobile_phone")), "data.mobile_phone")

    merge_counter_edge(tx, "uid", uid, "HAS_CONTACT_PHONE", "phone_num", contact_phone, ts)
    return {"event_type": "contact_edit", "uid": uid, "contact_phone": contact_phone}


def merge_card_bind_phone(tx, card_key: Optional[str], phone_key: Optional[str], ts: str) -> bool:
    if card_key is None or phone_key is None:
        return False
    query = """
    MERGE (c:card_no {key: $card_key})
    ON CREATE SET c.created_at = datetime($ts), c.updated_at = datetime($ts)
    ON MATCH SET c.updated_at = datetime($ts)
    MERGE (p:phone_num {key: $phone_key})
    ON CREATE SET p.created_at = datetime($ts), p.updated_at = datetime($ts), p.logout_count = coalesce(p.logout_count, 0)
    ON MATCH SET p.updated_at = datetime($ts)
    MERGE (c)-[r:CARD_BIND_PHONE]->(p)
    ON CREATE SET r.first_seen_ts = datetime($ts), r.last_seen_ts = datetime($ts), r.event_count = 1
    ON MATCH SET r.last_seen_ts = datetime($ts), r.event_count = coalesce(r.event_count, 0) + 1
    """
    tx.run(query, card_key=card_key, phone_key=phone_key, ts=ts)
    return True


def handle_order(tx, event: Dict[str, Any]) -> Dict[str, Any]:
    ts = required(normalize_scalar(event.get("ts")), "ts")
    data = event.get("data") or {}

    order_key = required(normalize_scalar(data.get("order_id")), "data.order_id")
    uid = required(normalize_scalar(data.get("user_id")), "data.user_id")

    apply_loan_tel = normalize_scalar(data.get("apply_loan_tel"))
    apply_ident_no = normalize_scalar(data.get("apply_ident_no"))
    apply_card_no = normalize_scalar(data.get("apply_card_no"))
    apply_bank_mobile = normalize_scalar(data.get("apply_bank_mobile"))
    repay_card_no = normalize_scalar(data.get("repay_card_no"))
    repay_bank_mobile = normalize_scalar(data.get("repay_bank_mobile"))
    order_status = normalize_scalar(data.get("order_status"))

    tx.run(
        """
        MERGE (u:uid {key: $uid})
        ON CREATE SET u.created_at = datetime($ts), u.updated_at = datetime($ts), u.logout_flag = false
        ON MATCH SET u.updated_at = datetime($ts)
        MERGE (o:order {key: $order_key})
        ON CREATE SET o.created_at = datetime($ts)
        SET o.updated_at = datetime($ts),
            o.order_status = coalesce($order_status, o.order_status)
        MERGE (u)-[r:PLACED_ORDER]->(o)
        ON CREATE SET r.first_seen_ts = datetime($ts), r.last_seen_ts = datetime($ts), r.event_count = 1
        ON MATCH SET r.last_seen_ts = datetime($ts), r.event_count = coalesce(r.event_count, 0) + 1
        """,
        uid=uid,
        order_key=order_key,
        ts=ts,
        order_status=order_status,
    )

    linked_fields = 0

    def merge_order_field(rel_type: str, dst_label: str, dst_key: Optional[str]) -> None:
        nonlocal linked_fields
        if dst_key is None:
            return
        query = f"""
        MATCH (o:order {{key: $order_key}})
        MERGE (d:{dst_label} {{key: $dst_key}})
        ON CREATE SET d.created_at = datetime($ts), d.updated_at = datetime($ts)
        ON MATCH SET d.updated_at = datetime($ts)
        MERGE (o)-[r:{rel_type}]->(d)
        ON CREATE SET r.first_seen_ts = datetime($ts), r.last_seen_ts = datetime($ts), r.event_count = 1
        ON MATCH SET r.last_seen_ts = datetime($ts), r.event_count = coalesce(r.event_count, 0) + 1
        """
        tx.run(query, order_key=order_key, dst_key=dst_key, ts=ts)
        linked_fields += 1

    merge_order_field("ORDER_APPLY_PHONE", "phone_num", apply_loan_tel)
    merge_order_field("ORDER_APPLY_IDENTITY", "identity_no", apply_ident_no)
    merge_order_field("ORDER_APPLY_CARD", "card_no", apply_card_no)
    merge_order_field("ORDER_REPAY_CARD", "card_no", repay_card_no)

    bind_count = 0
    if merge_card_bind_phone(tx, apply_card_no, apply_bank_mobile, ts):
        bind_count += 1
    if merge_card_bind_phone(tx, repay_card_no, repay_bank_mobile, ts):
        bind_count += 1

    return {
        "event_type": "order",
        "uid": uid,
        "order_key": order_key,
        "linked_fields": linked_fields,
        "card_phone_links": bind_count,
    }


def handle_risk_assessment(_tx, event: Dict[str, Any]) -> Dict[str, Any]:
    ts = required(normalize_scalar(event.get("ts")), "ts")
    id_type = normalize_scalar(event.get("id_type"))
    id_value = normalize_scalar(event.get("id_value"))
    return {
        "event_type": "risk_assessment",
        "ts": ts,
        "id_type": id_type,
        "id_value": id_value,
        "updated": False,
        "message": "risk_assessment currently skipped by design",
    }


def handle_logout(tx, event: Dict[str, Any]) -> Dict[str, Any]:
    ts = required(normalize_scalar(event.get("ts")), "ts")
    data = event.get("data") or {}
    uid = required(normalize_scalar(data.get("cid")), "data.cid")
    identity_no = normalize_scalar(data.get("identity_no"))
    mobile_phone = normalize_scalar(data.get("mobile_phone"))

    tx.run(
        """
        MERGE (u:uid {key: $uid})
        ON CREATE SET u.created_at = datetime($ts), u.updated_at = datetime($ts), u.logout_flag = true
        ON MATCH SET u.updated_at = datetime($ts)
        SET u.logout_flag = true
        """,
        uid=uid,
        ts=ts,
    )

    if identity_no is not None:
        tx.run(
            """
            MERGE (i:identity_no {key: $identity_no})
            ON CREATE SET i.created_at = datetime($ts), i.logout_count = 1
            ON MATCH SET i.logout_count = coalesce(i.logout_count, 0) + 1
            SET i.updated_at = datetime($ts)
            """,
            identity_no=identity_no,
            ts=ts,
        )

    if mobile_phone is not None:
        tx.run(
            """
            MERGE (p:phone_num {key: $mobile_phone})
            ON CREATE SET p.created_at = datetime($ts), p.logout_count = 1
            ON MATCH SET p.logout_count = coalesce(p.logout_count, 0) + 1
            SET p.updated_at = datetime($ts)
            """,
            mobile_phone=mobile_phone,
            ts=ts,
        )

    return {
        "event_type": "logout",
        "uid": uid,
        "identity_updated": identity_no is not None,
        "phone_updated": mobile_phone is not None,
    }


EVENT_HANDLERS = {
    "customer_update": handle_customer_update,
    "login": handle_login,
    "gps": handle_gps,
    "contact_edit": handle_contact_edit,
    "order": handle_order,
    "risk_assessment": handle_risk_assessment,
    "logout": handle_logout,
}


def process_event_in_session(session, event: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(event, dict):
        raise ValueError("event must be an object")

    event = deep_normalize(event)
    event_type = required(normalize_scalar(event.get("type")), "type")
    handler = EVENT_HANDLERS.get(event_type)
    if handler is None:
        raise ValueError(f"Unsupported event type: {event_type}")

    return session.execute_write(handler, event)


def process_event(event: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(event, dict):
        raise ValueError("event must be an object")

    with driver.session(database=NEO4J_DATABASE) as session:
        result = process_event_in_session(session, event)
    return result


@app.route("/health", methods=["GET"])
def health() -> Any:
    try:
        with driver.session(database=NEO4J_DATABASE) as session:
            session.run("RETURN 1 AS ok").single()
        return jsonify({"status": "ok", "database": NEO4J_DATABASE})
    except Exception as exc:  # pragma: no cover
        return jsonify({"status": "error", "message": str(exc)}), 500


@app.route("/event", methods=["POST"])
def ingest_event() -> Any:
    payload = request.get_json(silent=True)
    if payload is None:
        return jsonify({"error": "Request body must be valid JSON"}), 400

    try:
        result = process_event(payload)
        return jsonify({"ok": True, "result": result})
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400


@app.route("/events", methods=["POST"])
def ingest_events() -> Any:
    payload = request.get_json(silent=True)
    if payload is None or not isinstance(payload, dict):
        return jsonify({"error": "Request body must be a JSON object"}), 400

    events = payload.get("events")
    if not isinstance(events, list):
        return jsonify({"error": "events must be a list"}), 400

    results: List[Dict[str, Any]] = []
    errors: List[Dict[str, Any]] = []
    stop_on_error = bool(payload.get("stop_on_error", False))

    for idx, event in enumerate(events):
        try:
            result = process_event(event)
            results.append({"index": idx, "result": result})
        except Exception as exc:
            errors.append({"index": idx, "error": str(exc)})
            if stop_on_error:
                break

    code = 200 if not errors else 207
    return (
        jsonify(
            {
                "ok": len(errors) == 0,
                "processed": len(results),
                "failed": len(errors),
                "results": results,
                "errors": errors,
            }
        ),
        code,
    )


def parse_jsonl_file(file_path: Path, limit: Optional[int] = None) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    results: List[Dict[str, Any]] = []
    errors: List[Dict[str, Any]] = []

    with file_path.open("r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, start=1):
            text = line.strip()
            if text == "":
                continue
            try:
                event = json.loads(text)
                result = process_event(event)
                results.append({"line": line_no, "result": result})
            except Exception as exc:
                errors.append({"line": line_no, "error": str(exc)})

            if limit is not None and len(results) + len(errors) >= limit:
                break

    return results, errors


@app.route("/ingest_jsonl", methods=["POST"])
def ingest_jsonl() -> Any:
    payload = request.get_json(silent=True)
    if payload is None or not isinstance(payload, dict):
        return jsonify({"error": "Request body must be a JSON object"}), 400

    file_path_raw = normalize_scalar(payload.get("file_path"))
    if file_path_raw is None:
        return jsonify({"error": "file_path is required"}), 400

    limit_raw = payload.get("limit")
    limit: Optional[int]
    if limit_raw is None:
        limit = None
    else:
        try:
            limit = int(limit_raw)
            if limit <= 0:
                raise ValueError("limit must be > 0")
        except Exception:
            return jsonify({"error": "limit must be a positive integer"}), 400

    file_path = Path(file_path_raw)
    if not file_path.is_absolute():
        file_path = Path.cwd() / file_path

    if not file_path.exists():
        return jsonify({"error": f"File not found: {file_path}"}), 404

    try:
        results, errors = parse_jsonl_file(file_path, limit)
    except Exception as exc:
        return jsonify({"error": str(exc)}), 400

    code = 200 if not errors else 207
    return (
        jsonify(
            {
                "ok": len(errors) == 0,
                "file_path": str(file_path),
                "processed": len(results),
                "failed": len(errors),
                "results": results,
                "errors": errors,
            }
        ),
        code,
    )


@app.route("/init", methods=["POST"])
def init_graph() -> Any:
    try:
        init_constraints()
        return jsonify({"ok": True, "database": NEO4J_DATABASE, "constraints": len(CONSTRAINT_QUERIES)})
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route("/shutdown", methods=["POST"])
def shutdown() -> Any:
    try:
        driver.close()
        return jsonify({"ok": True})
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


if __name__ == "__main__":
    init_constraints()
    app.run(host="0.0.0.0", port=5001, debug=True)
