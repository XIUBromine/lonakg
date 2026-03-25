# Rule-Based K-Hop Risk Model

## 1. Design

For a target node $u$ at time $t$ and hop $k$:

$$
R(u,t,k) = \alpha \cdot R_{base}(u,t) + (1-\alpha) \cdot \frac{\sum_{v \in N(u)} w(u,v,t) \cdot R(v,t,k-1)}{\sum_{v \in N(u)} w(u,v,t)}
$$

where:

- $\alpha$ is `base_mix`.
- $R_{base}(u,t)$ combines node status and UID-association risk.
- $w(u,v,t)$ combines relation type weight, node type weight, temporal factor, and association boost.

Base case:

$$
R(u,t,0) = R_{base}(u,t)
$$

## 2. Covered Factors

- Node status:
  - `order.status` mapped to risk values.
  - `status=BLACKLISTED` on `uid/phone_num/identity_no` gives high base risk.
- Number of associated UIDs:
  - Different relation types use different coefficients, e.g. `LOGIN_WITH_PHONE` > `HAS_CONTACT_PHONE`.
- Temporal information:
  - Exponential recency decay from `last_seen_ts`.
  - Density term from `event_count`.
- Node/edge type information:
  - `relation_type_weight` and `node_type_weight` control propagation strength.
- Hyperparameters:
  - All key weights are in `RiskHyperParams` and can be tuned manually.

## 3. File

- `model/rule_based_risk.py`

## 4. Usage

```bash
python model/rule_based_risk.py \
  --node-type uid \
  --node-key 20250402023197173126 \
  --as-of 2025-04-02T17:17:12 \
  --k 2 \
  --database prod
```

## 5. Notes

- This is a pure rule-based model for online risk scoring.
- For strict anti-leakage, ensure graph temporal properties are updated incrementally in real time.
