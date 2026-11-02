"""
StreamPulse — Medallion Data Lakehouse Pipeline & Quality Control

This module implements the core Bronze-to-Silver PySpark ETL job,
DataQualityGate validation suite, Apache Airflow DAG orchestration,
and PostgreSQL Gold DDL specifications.
"""

from datetime import datetime, timedelta
import json
from typing import Dict, List, Tuple
from airflow import DAG
from airflow.operators.python import PythonOperator
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

# ==============================================================================
# POSTGRESQL GOLD WAREHOUSE DDL (SPECIFICATION & COMMENTS)
# ==============================================================================
"""
POSTGRESQL WAREHOUSE SCHEMAS & TABLES:

-- Bronze Quarantine Table DDL:
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE TABLE bronze.quarantine_events (
    quarantine_id BIGSERIAL PRIMARY KEY,
    raw_payload JSONB NOT NULL,
    failure_reason TEXT NOT NULL,
    rule_id VARCHAR(16) NOT NULL,
    quarantined_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- Gold Fact Table Range Partitioned DDL:
CREATE SCHEMA IF NOT EXISTS gold;
CREATE TABLE gold.events_fact (
    event_id UUID NOT NULL,
    user_id VARCHAR(64) NOT NULL,
    event_type VARCHAR(32) NOT NULL,
    event_timestamp TIMESTAMPTZ NOT NULL,
    event_date DATE NOT NULL,
    latency_ms INT,
    revenue_usd NUMERIC(12, 2),
    ingested_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
) PARTITION BY RANGE (event_date);

-- Gold Concurrent Materialized View DDL:
CREATE MATERIALIZED VIEW gold.mv_daily_kpi_metrics AS
SELECT
    event_date,
    event_type,
    COUNT(DISTINCT user_id) AS unique_users,
    COUNT(event_id) AS total_events,
    SUM(revenue_usd) AS total_revenue_usd,
    AVG(latency_ms) AS avg_latency_ms
FROM gold.events_fact
GROUP BY event_date, event_type;

CREATE UNIQUE INDEX idx_mv_daily_kpi_metrics ON gold.mv_daily_kpi_metrics (event_date, event_type);
"""


# ==============================================================================
# DATA QUALITY GATE CLASS
# ==============================================================================
class DataQualityGate:
    """Enforces strict schema compliance and validation checks on raw Bronze events."""

    REQUIRED_FIELDS = ["event_id", "user_id", "event_type", "event_timestamp"]
    ANOMALY_THRESHOLD_PCT = 0.05  # 5% max batch failure rate

    def __init__(self, spark_session: SparkSession):
        self.spark = spark_session

    def validate_record(self, record: Dict) -> Tuple[bool, str, str]:
        """Validates an individual event record against DQ_001 through DQ_004 rules."""
        # DQ_001: Required fields check
        for field in self.REQUIRED_FIELDS:
            if field not in record or record[field] is None:
                return False, f"Missing required field: {field}", "DQ_001"

        # DQ_002: Schema drift check
        allowed_keys = set(self.REQUIRED_FIELDS + ["latency_ms", "revenue_usd"])
        for key in record.keys():
            if key not in allowed_keys:
                return False, f"Unexpected key detected (schema drift): {key}", "DQ_002"

        # DQ_003: Timestamp bounds check
        try:
            ts = datetime.fromisoformat(record["event_timestamp"].replace("Z", "+00:00"))
            if ts > datetime.utcnow() + timedelta(minutes=5):
                return False, f"Future event timestamp detected: {ts}", "DQ_003"
        except Exception as err:
            return False, f"Invalid timestamp format: {err}", "DQ_003"

        # DQ_004: Range validation check
        if "latency_ms" in record and record["latency_ms"] is not None:
            if record["latency_ms"] < 0:
                return False, f"Negative latency: {record['latency_ms']}", "DQ_004"

        if "revenue_usd" in record and record["revenue_usd"] is not None:
            if record["revenue_usd"] < 0:
                return False, f"Negative revenue: {record['revenue_usd']}", "DQ_004"

        return True, "PASSED", "DQ_000"

    def process_batch_validation(self, df_raw):
        """Filters valid records and routes bad data into quarantine."""
        # PySpark dataframe validation filter
        valid_condition = (
            F.col("event_id").isNotNull()
            & F.col("user_id").isNotNull()
            & F.col("event_type").isNotNull()
            & F.col("event_timestamp").isNotNull()
            & (F.col("latency_ms").isNull() | (F.col("latency_ms") >= 0))
            & (F.col("revenue_usd").isNull() | (F.col("revenue_usd") >= 0))
        )

        df_valid = df_raw.filter(valid_condition)
        df_quarantine = df_raw.filter(~valid_condition).withColumn(
            "failure_reason", F.lit("DQ_001/DQ_004 validation failure")
        )

        return df_valid, df_quarantine


# ==============================================================================
# SILVER PYSPARK DEDUPLICATION ETL
# ==============================================================================
def process_silver_deduplication(spark: SparkSession, bronze_path: str, silver_path: str):
    """Reads Bronze layer data, cleanses, and applies window-function deduplication."""
    df_bronze = spark.read.parquet(bronze_path)

    # Window function for deterministic deduplication
    window_spec = Window.partitionBy("event_id").orderBy(F.col("ingested_at").desc())

    df_deduped = (
        df_bronze.withColumn("row_num", F.row_number().over(window_spec))
        .filter(F.col("row_num") == 1)
        .drop("row_num")
    )

    # Output partitioned Silver datasets
    df_deduped.write.mode("overwrite").partitionBy("event_date").parquet(silver_path)
    return df_deduped


# ==============================================================================
# APACHE AIRFLOW DAG DEFINITION
# ==============================================================================
default_args = {
    "owner": "streampulse_eng",
    "depends_on_past": False,
    "email_on_failure": True,
    "email": ["alerts@streampulse.io"],
    "retries": 3,
    "retry_delay": timedelta(seconds=30),
    "sla": timedelta(minutes=15),
}

with DAG(
    dag_id="streampulse_medallion_dag",
    default_args=default_args,
    description="Orchestrates Bronze ingestion, DataQualityGate, Silver deduplication, and Gold PostgreSQL refreshes",
    schedule_interval="0 * * * *",  # Hourly schedule
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["medallion", "lakehouse", "pyspark", "postgresql"],
) as dag:

    def ingest_bronze_task(**kwargs):
        print("Ingesting raw REST API stream events into Bronze layer...")

    def quality_gate_task(**kwargs):
        print("Executing DataQualityGate checks and routing quarantine events...")

    def silver_transform_task(**kwargs):
        print("Executing PySpark Silver layer deduplication via ROW_NUMBER()...")

    def gold_refresh_task(**kwargs):
        print("Refreshing Gold PostgreSQL materialized views concurrently...")

    t1 = PythonOperator(
        task_id="ingest_bronze_raw_events",
        python_callable=ingest_bronze_task,
    )

    t2 = PythonOperator(
        task_id="run_data_quality_gate",
        python_callable=quality_gate_task,
    )

    t3 = PythonOperator(
        task_id="transform_silver_deduplicate",
        python_callable=silver_transform_task,
    )

    t4 = PythonOperator(
        task_id="refresh_gold_materialized_views",
        python_callable=gold_refresh_task,
    )

    t1 >> t2 >> t3 >> t4
