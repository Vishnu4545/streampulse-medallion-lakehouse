# StreamPulse — Medallion Data Lakehouse Architecture

## Overview & Architecture Layers

StreamPulse implements a 3-tier Medallion Architecture (Bronze, Silver, Gold) designed to ingest, cleanse, validate, and serve high-throughput REST API event streams.

### 1. Bronze Layer (Raw Storage)
- Ingests raw JSON payloads directly from REST API event streams into Parquet/JSONB tables.
- Data is partitioned by `event_date` (`YYYY-MM-DD`).
- Preserves raw event payload immutability for full auditability and trace-back capability.

### 2. Data Quality Quarantine Gate
- Intercepts incoming raw data before Silver transformation.
- Evaluates event integrity against strict JSON Schema contracts.
- Failed or schema-invalid events are diverted into `bronze.quarantine_events` with detailed error tracking for asynchronous inspection and re-ingestion.

### 3. Silver Layer (Cleansed & Deduplicated)
- Utilizes PySpark for distributed transformations and windowing functions.
- Applies deterministic deduplication using:
  `ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY ingested_at DESC)`
- Enforces data type casting, null-handling, and string normalization.

### 4. Gold Layer (Analytics & Serving)
- Loads enriched datasets into PostgreSQL range-partitioned warehouse tables.
- Exposes analytical views and concurrent materialized views:
  `gold.mv_daily_kpi_metrics`
- Optimized for sub-second business intelligence dashboards and real-time operational metrics.

---

## Architecture Flow Diagram

```
+------------------+      +-------------------+      +---------------------------+
| REST API Stream  | ---> |   Bronze Layer    | ---> | Data Quality Gate         |
+------------------+      | (Raw Parquet/     |      | (Schema Validation Check) |
                          |  JSONB Partitioned|      +-------------+-------------+
                          |  by event_date)   |                    |
                          +-------------------+          +---------+---------+
                                                         |                   |
                                                    [ Valid ]          [ Invalid ]
                                                         |                   |
                                                         v                   v
                          +-------------------+      +-------+       +---------------+
                          |   Silver Layer    | <----+ PySpark|      | bronze.       |
                          | (Cleansed & PySpark|      | Clean |      | quarantine_   |
                          |  Deduplicated via |      +-------+       | events        |
                          |  ROW_NUMBER())    |                      +---------------+
                          +---------+---------+
                                    |
                                    v
                          +-------------------+
                          |    Gold Layer     |
                          | (PostgreSQL Range |
                          |  Partitioned &    |
                          |  mv_daily_kpi_    |
                          |  metrics)         |
                          +-------------------+
```

---

## Apache Airflow Fault-Tolerant Orchestration

The ingestion pipeline is scheduled and orchestrated end-to-end via Apache Airflow (`streampulse_medallion_dag`).

- **Idempotent Backfills**: All tasks accept explicit target execution dates (`ds` / `event_date`), ensuring strict deterministic re-runs without duplicate records or metrics corruption.
- **Exponential Retry Backoff**: Transient connection errors or API rate limits trigger dynamic retry backoffs (`retries=3`, `retry_delay=timedelta(seconds=30)` with exponential multiplier).
- **SLA & Failure Alerting**: Automated callbacks monitor DAG runtime limits (`sla=timedelta(minutes=15)`), alerting engineering channels immediately upon validation breaches or task execution failures.
