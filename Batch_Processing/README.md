# Azure Batch Processing

## Notebook Execution Order

1. 01_Bronze_Ingestion.scala
2. 02_Silver_Transformation.scala
3. 03_Gold_Transformation.scala
4. 04_Data_Quality_Checks.scala

These are original Databricks notebook source exports.
They require a configured Databricks runtime and ADLS permissions.

## Processing

Bronze:
Explicit CSV schemas, ingestion metadata and Delta persistence.

Silver:
Standardization, validation, foreign-key integrity,
deduplication, rejected-record storage and duplicate audit.

Gold:
Customer, product and date dimensions; sales fact;
city, product and monthly aggregations; business KPIs.

Data Quality:
Reconciliation, integrity tests, revenue consistency,
and persisted Delta-based PASS/FAIL report.

## Verified Batch Counts

| Dataset | Bronze | Silver | Rejected | Duplicates |
|---|---:|---:|---:|---:|
| Customers | 42 | 35 | 5 | 2 |
| Products | 27 | 21 | 4 | 2 |
| Orders | 161 | 90 | 61 | 10 |
| Total | 230 | 146 | 70 | 14 |

Fact sales: 90 records.
Delivered orders: 51.
Revenue for delivered orders: INR 253,287.00.
Data Quality: 10 PASS.

The Databricks workflow completed all four dependent tasks.

## Implementation Limitations

The source notebooks use dataset-specific paths and demonstration
assertions. The sales fact uses current product catalog prices.
Do not interpret this as full historical transaction-price accounting.
