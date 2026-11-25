# Testing

## Streaming Unit Tests

ScalaTest:
`Real_Time_Streaming/src/test/scala/com/nexus/EventTransformsSpec.scala`

Run:

```bash
cd Real_Time_Streaming
sbt -batch clean test package
```

## Batch Quality Validation

`Data Ingestion/Databricks_Notebooks/04_Data_Quality_Checks.scala`

The notebook tests execute in Azure Databricks, not in the
root streaming sbt build.

## Evidence

Manual runtime tests have been performed during project development.
A successful GitHub Actions run must be verified after the first push.
