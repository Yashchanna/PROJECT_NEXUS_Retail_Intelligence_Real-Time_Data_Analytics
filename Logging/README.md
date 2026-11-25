# PROJECT NEXUS — Logging, Monitoring & Audit Lineage

This directory documents execution logs, audit reports, dead-letter quarantine tracking, and system lineage.

## Audit Architecture

1. **Bronze Ingestion Lineage:**
   - Every raw record is tagged with system metadata: `_ingestion_timestamp` and `_source_file`.
   - Streaming records capture Kafka broker offset and partition.

2. **Silver Dead-Letter Quarantine (`abfss://rejected@`):**
   - Corrupt or malformed records are isolated into quarantine tables rather than dropped.
   - Diagnostic `_reject_reason` codes include `INVALID_EMAIL`, `INVALID_QUANTITY`, `INVALID_CUSTOMER_ID`.
   - 70 records quarantined across customers (5), products (4), and orders (61).

3. **Deduplication Audit Summary:**
   - Tracks purged duplicate rows in `deduplication_summary`.
   - 14 duplicate extras purged across customers (2), products (2), and orders (10).

4. **Automated Delta Audit Table (`data_quality_report`):**
   - Appends run UUID, timestamp, check names, and PASS/FAIL status for all 10 automated test suites.

## Execution Evidence

See [`Execution_Evidence/README.md`](Execution_Evidence/README.md) for verified Databricks job run details and execution records.
