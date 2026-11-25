# PROJECT NEXUS — Datasets Repository

This directory contains historical batch CSV files, incremental updates, schema evolution batches, and streaming JSONL event feeds.

## Directory Structure

```
Datasets/nexus/
├── batch/
│   ├── raw/
│   │   ├── customers/customers.csv        # Customer master records (26 rows)
│   │   ├── products/products.csv          # Catalog product items (23 rows)
│   │   └── orders/orders.csv              # Historical transaction records (181 rows)
│   ├── incremental/
│   │   └── orders_updates/orders_updates.csv  # Incremental order changes & status updates
│   ├── schema_evolution/
│   │   └── orders_v2/orders_v2.csv        # Evolved schema with discount_amount & sales_channel
│   └── data_dictionary.csv                # Data dictionary & column specifications
│
├── streaming/
│   ├── order_events.jsonl                 # 62 streaming order lifecycle events
│   ├── order_event.schema.json            # JSON schema validation definition
│   ├── order_events_kafka_keyed.txt       # Kafka keyed payload representation
│   └── micro_batches/                     # 5 micro-batch JSON files for stepped replay
│       ├── order_events_001.json
│       ├── order_events_002.json
│       ├── order_events_003.json
│       ├── order_events_004.json
│       └── order_events_005.json
└── ABOUT.txt                              # Dataset origin and provenance documentation
```

## Batch Datasets (230 Rows Total)
- **`customers.csv`**: `customer_id`, `customer_name`, `email`, `city`, `signup_date`
- **`products.csv`**: `product_id`, `product_name`, `category`, `unit_price`, `active`
- **`orders.csv`**: `order_id`, `customer_id`, `product_id`, `quantity`, `order_date`, `order_status`, `last_updated`

## Streaming Datasets (62 Events)
- Continuous order lifecycle events (`ORDER_CREATED`, `ORDER_UPDATED`, `ORDER_CANCELLED`, and error/test events).
- Micro-batch sequence preserves ingest-time ordering and synthetic late arrival for watermarking demonstrations.
