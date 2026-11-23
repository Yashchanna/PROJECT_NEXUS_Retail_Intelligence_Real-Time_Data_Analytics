"""Replay the supplied NEXUS JSON events to Kafka in ingest order.

Works from host Python, a Docker container or Kubernetes. The producer publishes
historical sample events as a simulated live stream; it does not invent new orders.
"""
import argparse
import json
import os
import time
from pathlib import Path

from kafka import KafkaProducer


def load_events(path):
    with Path(path).open(encoding="utf-8") as handle:
        return [json.loads(line) for line in handle if line.strip()]


def create_producer(brokers):
    while True:
        try:
            return KafkaProducer(
                bootstrap_servers=brokers.split(","),
                key_serializer=lambda key: key.encode("utf-8"),
                value_serializer=lambda value: json.dumps(value).encode("utf-8"),
                acks="all",
                retries=5,
                request_timeout_ms=30000,
            )
        except Exception as exc:
            print(f"Kafka not ready ({exc}); retrying in 5 seconds", flush=True)
            time.sleep(5)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", default=os.getenv("NEXUS_EVENT_FILE", "data/nexus/streaming/order_events.jsonl"))
    parser.add_argument("--bootstrap", default=os.getenv("NEXUS_KAFKA_BOOTSTRAP", "localhost:9092"))
    parser.add_argument("--topic", default=os.getenv("NEXUS_KAFKA_TOPIC", "retail-order-events"))
    parser.add_argument("--delay", type=float, default=float(os.getenv("NEXUS_PUBLISH_DELAY", "0.6")))
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--loop", action="store_true", default=os.getenv("NEXUS_REPLAY_LOOP", "false").lower() == "true")
    parser.add_argument("--loop-pause", type=float, default=float(os.getenv("NEXUS_LOOP_PAUSE", "120")))
    parser.add_argument("--idle-after", action="store_true", default=os.getenv("NEXUS_IDLE_AFTER", "false").lower() == "true")
    args = parser.parse_args()

    events = load_events(args.file)
    if args.limit > 0:
        events = events[:args.limit]
    producer = create_producer(args.bootstrap)

    try:
        while True:
            for index, event in enumerate(events, 1):
                # Keep arrival order; sorting by event_time would hide late-arrival tests.
                key = event.get("order_id") or event.get("event_id") or "missing-order"
                producer.send(args.topic, key=key, value=event).get(timeout=30)
                print(f"[{index}/{len(events)}] {event.get('event_id')} {event.get('event_type')}", flush=True)
                if index < len(events) and args.delay > 0:
                    time.sleep(args.delay)
            producer.flush(timeout=30)
            print(f"Published {len(events)} historical sample events as simulated live messages.", flush=True)
            if args.loop:
                time.sleep(args.loop_pause)
            else:
                break
    finally:
        producer.flush(timeout=30)
        producer.close()

    if args.idle_after:
        # Demo Deployment remains running without republishing old event IDs on every restart.
        while True:
            time.sleep(60)


if __name__ == "__main__":
    main()
