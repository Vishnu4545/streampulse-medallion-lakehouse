# StreamPulse (Medallion Data Lakehouse)

StreamPulse is a robust, production-grade Medallion Data Lakehouse platform designed for processing high-throughput REST API event streams. It establishes structured Bronze (Raw), Silver (Cleansed/Deduplicated), and Gold (Analytical Warehouse) pipeline layers to guarantee enterprise data quality, fault tolerance, and sub-second analytics.

## Tech Stack
- **Python**
- **SQL**
- **Apache Airflow**
- **PySpark**
- **PostgreSQL**

## Documentation & Architecture Links
- [Architecture](./ARCHITECTURE.md) — Detailed overview of Bronze, Quarantine, Silver, and Gold layers, ASCII flow diagram, and Airflow orchestration rules.
- [Documentation](./DOCUMENTATION.md) — Technical specifications including Event JSON Schema contracts, Data Quality validation rules (`DQ_001` - `DQ_005`), Airflow backfill operations, and PostgreSQL DDL/indexing.
- Pipeline implementation code: [`streampulse_pipeline.py`](./streampulse_pipeline.py)
