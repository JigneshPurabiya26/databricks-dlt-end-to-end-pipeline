# databricks-dlt-end-to-end-pipeline
End-to-end Delta Live Tables pipeline using Medallion Architecture (Bronze–Silver–Gold)

This project demonstrates a production-style end-to-end data pipeline built using **Databricks Delta Live Tables (DLT)** following the **Medallion Architecture**.

## Architecture Overview

- **Bronze Layer**
  - Streaming ingestion of raw data
  - Append-only tables

- **Silver Layer**
  - Data cleansing and standardization
  - SCD Type 1 handling using DLT auto CDC
  - Streaming views to expose incremental change feed

- **Gold Layer**
  - Analytics-ready fact and dimension tables
  - Built as streaming tables for incremental, deterministic computation
  - Sourced from Silver streaming views (not Silver tables)

## Key Concepts Covered

- Delta Live Tables (DLT)
- Medallion Architecture
- Streaming tables vs streaming views
- SCD Type 1 using `apply_changes` / `auto_cdc_flow`
- Incremental Gold layer design
- End-to-end pipeline execution and performance


## Tech Stack

- Databricks
- Delta Lake
- Delta Live Tables
- PySpark

---

This project focuses on **real-world lakehouse design patterns**, not just demo pipelines.

