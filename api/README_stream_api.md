# Stream KG API

## 1. Install

```bash
cd api
pip install -r requirements.txt
```

## 2. Run

```bash
cd api
python stream_kg_api.py
```

Default config:

- `NEO4J_URI=bolt://localhost:7687`
- `NEO4J_USER=neo4j`
- `NEO4J_PASSWORD=kBXwIuxLTvgxnbGD`
- `NEO4J_DATABASE=dev`

You can override them with environment variables.

## 3. Endpoints

- `GET /health` health check
- `POST /init` create constraints/indexes
- `POST /event` ingest one event
- `POST /events` ingest batch events
- `POST /ingest_jsonl` ingest jsonl file

## 4. Quick Examples

### 4.1 Create constraints

```bash
curl -X POST http://127.0.0.1:5001/init
```

### 4.2 Ingest one event

```bash
curl -X POST http://127.0.0.1:5001/event \
  -H "Content-Type: application/json" \
  -d '{"type":"login","ts":"2025-04-02T17:16:56","data":{"phone_num":"p1","cif_user_id":"u1","device_no":"d1","remote_ip":"1.2.3.4","td_device_id":"td1"}}'
```

### 4.3 Replay jsonl

```bash
curl -X POST http://127.0.0.1:5001/ingest_jsonl \
  -H "Content-Type: application/json" \
  -d '{"file_path":"api/example.jsonl","limit":1000}'
```

## 5. Notes

- `customer_update` has dedupe logic: if latest `(identity_no, mobile_phone)` did not change for a uid, no new identity/phone edge is created.
- `risk_assessment` is stored as supervision snapshots for training labels.

## 6. Bulk Import To prod

For full monthly jsonl import from `/data/processed/events` to Neo4j `prod`, use:

```bash
cd api
python import_events_prod.py --init-constraints
```

Recommended `nohup` background run:

```bash
cd /home/zqk/workplace/lonakg
nohup python api/import_events_prod.py \
  --events-dir /data/processed/events \
  --database prod \
  --log-file api/import_events_prod.log \
  --checkpoint-file api/import_events_prod.checkpoint.json \
  --progress-interval 50000 \
  --checkpoint-every 10000 \
  --init-constraints \
  > api/import_events_prod.nohup.out 2>&1 &
```

Resume behavior:

- By default, script resumes from `--checkpoint-file`.
- Use `--no-resume` to restart from the beginning.

Stop-on-error mode:

```bash
python api/import_events_prod.py --stop-on-error
```
