# Requirements Traceability

| ID | Requirement | Implementation status |
|---|---|---|
| BR-01 | Historical ingestion | Azure batch notebooks |
| BR-02 | Data validation | Silver and streaming transforms |
| BR-03 | Duplicate handling | Silver and event deduplication |
| BR-04 | Customer/product joins | Silver foreign-key checks and Gold |
| BR-05 | Sales measures | Gold fact and aggregates |
| BR-06 | Streaming ingestion | Kafka producer and consumer |
| BR-07 | Event-time processing | Spark windows and watermark |
| BR-08 | Lineage and curation | Bronze metadata and curated outputs |
| BR-09 | Restart and recovery | Local checkpoints and PVC |
| BR-10 | Azure and Databricks | Verified batch execution |
| BR-11 | Git and CI | Source/workflow available; CI run pending |
| BR-12 | Final demonstration | Documentation available; final demo pending |
| BR-13 | Apache Iceberg | Local tables and feature demonstrations |
| BR-14 | Kubernetes | Streaming manifests; batch Job demo pending |

## Outstanding Capstone Gaps

- Cloud-hosted Iceberg evidence if the reviewer requires it.
- Kubernetes batch Job/CronJob demonstration.
- GitHub Actions successful execution evidence.
- Final screenshots and project presentation.
- Automated reconciliation between historical and streamed order state.

The initial streaming-only requirements matrix is retained under
docs/requirements-matrix-streaming-initial.md.
