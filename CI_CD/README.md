# CI/CD

Existing workflow: .github/workflows/ci.yml

Automated steps:
- Java 17 and sbt setup
- Scala compilation and unit testing
- Python sample validation
- Docker Compose validation
- Producer and streaming image builds

CI does not automatically run Azure Databricks notebooks.

Verify the first GitHub Actions run after pushing source.
