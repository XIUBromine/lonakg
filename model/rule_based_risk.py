from __future__ import annotations

import argparse
import math
import os
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

from neo4j import GraphDatabase


ALLOWED_NODE_TYPES = {
    "uid": "uid",
    "phone_num": "phone_num",
    "identity_no": "identity_no",
    "device_no": "device_no",
    "td_device_id": "td_device_id",
    "remote_ip": "remote_ip",
    "geo_code": "geo_code",
    "card_no": "card_no",
    "order": "order",
}


@dataclass
class RiskHyperParams:
    status_overdue_risk: float = 100
    blacklisted_risk: float = 100

    phone_main_weight: float = 1
    phone_contact_weight: float = 0.5
    identity_weight: float = 1
    card_weight: float = 1
    device_weight: float = 1
    td_device_weight: float = 1
    remote_ip_weight: float = 1
    geo_weight: float = 1

    # phone_main_weight: float = 1.2
    # phone_contact_weight: float = 0.7
    # identity_weight: float = 1.1
    # card_weight: float = 1.25
    # device_weight: float = 1.3
    # td_device_weight: float = 1.35
    # remote_ip_weight: float = 0.75
    # geo_weight: float = 0.55

    relation_type_weight: Dict[str, float] = field(
        default_factory=lambda: {
            "HAS_IDENTITY": 0.9,
            "HAS_PHONE": 0.8,
            "LOGIN_WITH_PHONE": 1.0,
            "LOGIN_WITH_DEVICE": 1.2,
            "LOGIN_WITH_TD_DEVICE": 1.3,
            "LOGIN_WITH_IP": 0.1,
            "LOCATED_AT": 0.25,
            "HAS_CONTACT_PHONE": 0.08,
            "PLACED_ORDER": 0.7,
            "ORDER_APPLY_PHONE": 0.95,
            "ORDER_APPLY_IDENTITY": 1.0,
            "ORDER_APPLY_CARD": 1.15,
            "ORDER_REPAY_CARD": 1.2,
            "CARD_BIND_PHONE": 1.1,
        }
    )

    # order_status_risk: Dict[str, float] = field(
    #     default_factory=lambda: {
    #         "OVERDUE": 1.0,
    #         "LOAN_REFUSE": 0.08,
    #         "REPAYING": 0.1,
    #         "REPAYED": 0,
    #         "SETTLED": 0,
    #         "EARLY_REPAYED": 0,
    #         "FAIL": 0,
    #         "CANCEL": 0,
    #     }
    # )

    ip_assoc_window_days: float = 7.0
    geo_assoc_window_days: float = 30.0

    ip_decay_days_to_tenth: float = 7.0
    geo_decay_days_to_tenth: float = 30.0
    default_decay_days_to_tenth: float = 730.0


class RuleBasedKHopRiskEngine:
    def __init__(
        self,
        uri: str,
        user: str,
        password: str,
        database: str = "prod",
        params: Optional[RiskHyperParams] = None,
    ) -> None:
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self.database = database
        self.params = params or RiskHyperParams()
        self._memo: Dict[Tuple[str, str, int], float] = {}
        self._assoc_cache: Dict[Tuple[str, str], float] = {}

    def close(self) -> None:
        self.driver.close()

    def compute_risk(self, node_type: str, node_key: str, k: int) -> float:
        if node_type not in ALLOWED_NODE_TYPES:
            raise ValueError(f"Unsupported node_type: {node_type}")
        if k < 0:
            raise ValueError("k must be >= 0")

        self._memo.clear()
        self._assoc_cache.clear()
        with self.driver.session(database=self.database) as session:
            return self._risk_recursive(session, node_type, node_key, k)

    def _risk_recursive(self, session, node_type: str, node_key: str, k: int) -> float:
        memo_key = (node_type, node_key, k)
        if memo_key in self._memo:
            return self._memo[memo_key]

        base = self._base_node_risk(session, node_type, node_key)
        if k == 0:
            self._memo[memo_key] = base
            return base

        neighbors = self._fetch_neighbors(session, node_type, node_key)
        if not neighbors:
            self._memo[memo_key] = base
            return base

        risk_sum = 0.0

        for n in neighbors:
            nbr_type = n["neighbor_type"]
            nbr_key = n["neighbor_key"]
            if nbr_type not in ALLOWED_NODE_TYPES or not nbr_key:
                continue

            nbr_risk = self._risk_recursive(session, nbr_type, nbr_key, k - 1)
            edge_w = self._edge_weight(
                rel_type=n["rel_type"],
                last_seen=n["last_seen"],
                src_updated_at=n["neighbor_updated_at"],
            )

            risk_sum += edge_w * nbr_risk

        risk = risk_sum
        self._memo[memo_key] = risk
        return risk

    def _base_node_risk(self, session, node_type: str, node_key: str) -> float:
        label = ALLOWED_NODE_TYPES[node_type]
        q = f"""
        MATCH (n:{label} {{key: $key}})
        RETURN coalesce(n.status, '') AS status
        """
        rec = session.run(q, key=node_key).single()
        if rec is None:
            return 0.0

        status = (rec["status"] or "").upper()

        status_risk = 0.0
        if status == "BLACKLISTED":
            status_risk = self.params.blacklisted_risk
        elif node_type == "order" and status == "OVERDUE":
            status_risk = self.params.status_overdue_risk
        elif self._is_connected_to_overdue_order(session, node_type, node_key):
            status_risk = self.params.status_overdue_risk

        assoc_risk = self._uid_association_score(session, node_type, node_key)
        return status_risk + assoc_risk

    def _fetch_neighbors(self, session, node_type: str, node_key: str) -> List[Dict[str, object]]:
        label = ALLOWED_NODE_TYPES[node_type]
        q = f"""
        MATCH (u:{label} {{key: $key}})-[r]-(v)
        RETURN labels(v)[0] AS neighbor_type,
               v.key AS neighbor_key,
             v.updated_at AS neighbor_updated_at,
               type(r) AS rel_type,
               r.last_seen_ts AS last_seen
        """
        out: List[Dict[str, object]] = []
        for rec in session.run(q, key=node_key):
            out.append(
                {
                    "neighbor_type": rec["neighbor_type"],
                    "neighbor_key": rec["neighbor_key"],
                    "neighbor_updated_at": rec["neighbor_updated_at"],
                    "rel_type": rec["rel_type"],
                    "last_seen": rec["last_seen"],
                }
            )
        return out

    def _edge_weight(
        self,
        rel_type: str,
        last_seen,
        src_updated_at,
    ) -> float:
        rel_w = self.params.relation_type_weight.get(rel_type, 0.6)
        temporal_w = self._relation_temporal_weight(
            rel_type=rel_type,
            last_seen=last_seen,
            src_updated_at=src_updated_at,
        )
        return rel_w * temporal_w

    def _relation_temporal_weight(self, rel_type: str, last_seen, src_updated_at) -> float:
        src_dt = self._to_datetime(src_updated_at)
        edge_dt = self._to_datetime(last_seen)
        if src_dt is None or edge_dt is None:
            return 1.0

        if (
            getattr(src_dt, "tzinfo", None) is None
            and getattr(edge_dt, "tzinfo", None) is not None
        ):
            edge_dt = edge_dt.replace(tzinfo=None)
        if (
            getattr(src_dt, "tzinfo", None) is not None
            and getattr(edge_dt, "tzinfo", None) is None
        ):
            src_dt = src_dt.replace(tzinfo=None)

        delta_days = max(0.0, (src_dt - edge_dt).total_seconds() / 86400.0)

        if rel_type == "LOGIN_WITH_IP":
            horizon = self.params.ip_decay_days_to_tenth
        elif rel_type == "LOCATED_AT":
            horizon = self.params.geo_decay_days_to_tenth
        else:
            horizon = self.params.default_decay_days_to_tenth

        horizon = max(1e-9, horizon)
        return math.pow(0.1, delta_days / horizon)

    def _to_datetime(self, value):
        if value is None:
            return None
        if hasattr(value, "to_native"):
            value = value.to_native()
        if isinstance(value, datetime):
            return value
        if isinstance(value, str):
            try:
                return datetime.fromisoformat(value.replace("Z", "+00:00"))
            except Exception:
                return None
        return None

    def _get_node_updated_at(self, session, node_type: str, node_key: str):
        label = ALLOWED_NODE_TYPES[node_type]
        q = f"""
        MATCH (n:{label} {{key: $key}})
        RETURN n.updated_at AS updated_at
        """
        rec = session.run(q, key=node_key).single()
        return rec["updated_at"] if rec else None

    def _is_connected_to_overdue_order(self, session, node_type: str, node_key: str) -> bool:
        label = ALLOWED_NODE_TYPES[node_type]
        q = f"""
        MATCH (n:{label} {{key: $key}})-[]-(o:order)
        WHERE toUpper(coalesce(o.status, '')) = 'OVERDUE'
        RETURN count(o) > 0 AS flag
        """
        rec = session.run(q, key=node_key).single()
        return bool(rec and rec["flag"])

    def _window_range(self, updated_at, window_days: Optional[float]) -> Tuple[Optional[str], Optional[str]]:
        dt = self._to_datetime(updated_at)
        if dt is None:
            return None, None
        end = dt.isoformat()
        if window_days is None:
            return None, end
        start = (dt - timedelta(days=window_days)).isoformat()
        return start, end

    def _uid_association_score(self, session, node_type: str, node_key: str) -> float:
        cache_key = (node_type, node_key)
        if cache_key in self._assoc_cache:
            return self._assoc_cache[cache_key]

        updated_at = self._get_node_updated_at(session, node_type, node_key)
        score = self._uid_association_weighted_count(session, node_type, node_key, updated_at)
        self._assoc_cache[cache_key] = score
        return score

    def _uid_association_weighted_count(
        self, session, node_type: str, node_key: str, updated_at
    ) -> float:
        def adjusted_uid_count(c: object) -> float:
            return max(float(c or 0) - 1.0, 0.0)

        if node_type == "phone_num":
            q = """
            MATCH (p:phone_num {key: $key})
            CALL (p) {
                MATCH (u:uid)-[:LOGIN_WITH_PHONE]->(p)
                RETURN DISTINCT u
                UNION
                MATCH (u:uid)-[:HAS_PHONE]->(p)
                RETURN DISTINCT u
                UNION
                MATCH (o:order)-[:ORDER_APPLY_PHONE]->(p)<-[:PLACED_ORDER]-(u:uid)
                RETURN DISTINCT u
            }
            WITH p, count(DISTINCT u) AS c_main
            OPTIONAL MATCH (u_contact:uid)-[:HAS_CONTACT_PHONE]->(p)
            RETURN c_main, count(DISTINCT u_contact) AS c_contact
            """
            rec = session.run(q, key=node_key).single()
            if rec is None:
                return 0.0
            return self.params.phone_main_weight * adjusted_uid_count(rec["c_main"]) + self.params.phone_contact_weight * adjusted_uid_count(rec["c_contact"])

        if node_type == "identity_no":
            q = """
            MATCH (i:identity_no {key: $key})
            CALL (i) {
                MATCH (u:uid)-[:HAS_IDENTITY]->(i)
                RETURN DISTINCT u
                UNION
                MATCH (o:order)-[:ORDER_APPLY_IDENTITY]->(i)<-[:PLACED_ORDER]-(u:uid)
                RETURN DISTINCT u
            }
            RETURN count(DISTINCT u) AS c_all
            """
            rec = session.run(q, key=node_key).single()
            if rec is None:
                return 0.0
            return self.params.identity_weight * adjusted_uid_count(rec["c_all"])

        if node_type == "card_no":
            q = """
            MATCH (c:card_no {key: $key})
            CALL (c) {
                MATCH (o1:order)-[:ORDER_APPLY_CARD]->(c)<-[:PLACED_ORDER]-(u:uid)
                RETURN DISTINCT u
                UNION
                MATCH (o2:order)-[:ORDER_REPAY_CARD]->(c)<-[:PLACED_ORDER]-(u:uid)
                RETURN DISTINCT u
            }
            RETURN count(DISTINCT u) AS c_all
            """
            rec = session.run(q, key=node_key).single()
            if rec is None:
                return 0.0
            return self.params.card_weight * adjusted_uid_count(rec["c_all"])

        if node_type == "device_no":
            q = "MATCH (u:uid)-[:LOGIN_WITH_DEVICE]->(:device_no {key:$key}) RETURN count(DISTINCT u) AS c"
            rec = session.run(q, key=node_key).single()
            return self.params.device_weight * adjusted_uid_count(rec["c"])

        if node_type == "td_device_id":
            q = "MATCH (u:uid)-[:LOGIN_WITH_TD_DEVICE]->(:td_device_id {key:$key}) RETURN count(DISTINCT u) AS c"
            rec = session.run(q, key=node_key).single()
            return self.params.td_device_weight * adjusted_uid_count(rec["c"])

        if node_type == "remote_ip":
            start, end = self._window_range(updated_at, self.params.ip_assoc_window_days)
            q = """
            MATCH (u:uid)-[r:LOGIN_WITH_IP]->(ip:remote_ip {key:$key})
                        WHERE r.last_seen_ts IS NOT NULL
                            AND ($start IS NULL OR r.last_seen_ts >= datetime($start))
                            AND ($end IS NULL OR r.last_seen_ts <= datetime($end))
            RETURN count(DISTINCT u) AS c
            """
            rec = session.run(q, key=node_key, start=start, end=end).single()
            return self.params.remote_ip_weight * adjusted_uid_count(rec["c"])

        if node_type == "geo_code":
            start, end = self._window_range(updated_at, self.params.geo_assoc_window_days)
            q = """
            MATCH (u:uid)-[r:LOCATED_AT]->(g:geo_code {key:$key})
                        WHERE r.last_seen_ts IS NOT NULL
                            AND ($start IS NULL OR r.last_seen_ts >= datetime($start))
                            AND ($end IS NULL OR r.last_seen_ts <= datetime($end))
            RETURN count(DISTINCT u) AS c
            """
            rec = session.run(q, key=node_key, start=start, end=end).single()
            return self.params.geo_weight * adjusted_uid_count(rec["c"])

        if node_type == "uid":
            return 0.0

        if node_type == "order":
            return 0.0

        return 0.0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Rule-based k-hop risk score on Neo4j graph"
    )
    parser.add_argument(
        "--node-type", choices=sorted(ALLOWED_NODE_TYPES.keys()), default="uid"
    )
    parser.add_argument("--node-key", default="20250921023199856279")
    parser.add_argument("--k", type=int, default=2)
    parser.add_argument("--database", default=os.getenv("NEO4J_DATABASE", "prod"))
    parser.add_argument(
        "--uri", default=os.getenv("NEO4J_URI", "bolt://localhost:7687")
    )
    parser.add_argument("--user", default=os.getenv("NEO4J_USER", "neo4j"))
    parser.add_argument(
        "--password", default=os.getenv("NEO4J_PASSWORD", "kBXwIuxLTvgxnbGD")
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    engine = RuleBasedKHopRiskEngine(
        uri=args.uri,
        user=args.user,
        password=args.password,
        database=args.database,
    )
    try:
        risk = engine.compute_risk(
            node_type=args.node_type, node_key=args.node_key, k=args.k
        )
        print(
            {
                "node_type": args.node_type,
                "node_key": args.node_key,
                "k": args.k,
                "risk": round(risk, 6),
            }
        )
    finally:
        engine.close()


if __name__ == "__main__":
    main()
