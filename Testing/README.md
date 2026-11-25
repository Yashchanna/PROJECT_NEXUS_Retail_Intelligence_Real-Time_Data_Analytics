# PROJECT NEXUS — Testing Suite

This directory contains test specifications, automated test scripts, and validation procedures for both the streaming engine and batch processing layers.

## Structure

- **`EventTransformsSpec.scala`**: ScalaTest specification covering:
  - Event payload parsing from JSON and Kafka envelopes.
  - Event deduplication logic by `event_id`.
  - Watermark thresholding and late-arriving event handling.
  - Tumbling window aggregation state transforms.
- **`check_sample.py`**: Python validation script verifying streaming test micro-batches and event schemas under `Datasets/nexus/streaming/`.
- **`CI_CD_README.md`**: Guide for continuous integration, GitHub Actions workflow execution, and automated test triggers.

## Running Tests

### 1. Streaming Engine Tests (sbt / ScalaTest)
Execute tests from the `Development/` or `Real_Time_Streaming/` directory:
```bash
cd Development
sbt test
```

### 2. Streaming Sample Data Verification
Execute the sample check script:
```bash
python Testing/check_sample.py
```

### 3. Batch Layer Quality & Reconciliation Suite
The 10-point Data Quality Audit executes in Azure Databricks:
- Notebook: `Data Ingestion/Databricks_Notebooks/04_Data_Quality_Checks.scala`
- Reconciles Bronze $\rightarrow$ Silver $\rightarrow$ Gold counts, checks uniqueness, and validates revenue aggregates.
