"""Offline preflight for the shipped sample: does not require Kafka or Spark."""
import json
from collections import Counter
from pathlib import Path

p = Path(__file__).resolve().parents[1] / "data/nexus/streaming/order_events.jsonl"
rows = [json.loads(line) for line in p.read_text(encoding="utf-8").splitlines() if line.strip()]
types = Counter(row["event_type"] for row in rows)
event_ids = Counter(row["event_id"] for row in rows)
assert len(rows) == 62, f"Expected 62, got {len(rows)}"
assert types["ORDER_UPDATED"] == 2
assert types["ORDER_CANCELLED"] == 1
assert types["ORDER_ERR"] == 1
assert sum(n-1 for n in event_ids.values() if n > 1) == 5
assert any("sales_channel" in row for row in rows)
assert any(row.get("order_id") is None for row in rows)
print(f"OK: {len(rows)} events, types={dict(types)}, extra duplicate IDs=5, and optional v2 fields found")
