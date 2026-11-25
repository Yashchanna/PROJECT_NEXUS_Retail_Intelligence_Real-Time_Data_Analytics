# Delta Lake vs Apache Iceberg

| Aspect | Delta Lake | Apache Iceberg |
|---|---|---|
| Transactions | ACID transactions using transaction log | ACID transactions using metadata snapshots |
| Time travel | Version and timestamp history | Snapshot-based reads |
| Schema evolution | Supported | Supported |
| Partition evolution | Partition changes require planning | Hidden partitioning and partition evolution |
| Engine integration | Strong Spark/Databricks support | Broad multi-engine ecosystem |
| Maintenance | Vacuum, optimize and log maintenance | Snapshot expiry, rewrite and metadata maintenance |

## PROJECT NEXUS Choice

Delta Lake:
Azure Bronze, Silver, rejected datasets and Gold analytics,
because the project's managed execution environment is Databricks.

Apache Iceberg:
Local streaming aggregate tables, demonstrating MERGE,
snapshots, time travel and schema evolution.

The existing local Iceberg warehouse is not equivalent to an
Iceberg catalog deployed on Azure object storage.
