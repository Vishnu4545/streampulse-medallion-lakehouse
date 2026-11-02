# StreamPulse — Technical Documentation

## Event JSON Schema Contract

Incoming REST API events must adhere strictly to the following JSON schema specification:

| Field Name | Type | Required | Description / Constraints |
| :--- | :--- | :--- | :--- |
| `event_id` | String (UUIDv4) | Yes | Unique event identifier |
| `user_id` | String | Yes | Unique user identifier |
| `event_type` | String | Yes | Operational type (e.g., `click`, `purchase`, `view`) |
| `event_timestamp` | String (ISO 8601) | Yes | Timestamp of event occurrence |
| `latency_ms` | Integer | No | Processing latency in milliseconds (`>= 0`) |
| `revenue_usd` | Numeric / Float | No | Monetary transaction amount (`>= 0.00`) |

---

## Automated Data Quality Validation Rules

The `DataQualityGate` enforces five core validation checks before passing records to Silver:

1. **`DQ_001` Required Fields Check**: Validates non-null presence of mandatory fields (`event_id`, `user_id`, `event_type`, `event_timestamp`).
2. **`DQ_002` Schema Drift Check**: Ensures incoming payload keys conform strictly to known JSON contract properties, flagging unknown fields.
3. **`DQ_003` Timestamp Bounds Check**: Validates that `event_timestamp` falls within accepted lower/upper execution thresholds (`event_timestamp <= CURRENT_TIMESTAMP`).
4. **`DQ_004` Range Validation Check**: Confirms numerical values meet constraints (`latency_ms >= 0` and `revenue_usd >= 0`).
5. **`DQ_005` Batch Anomaly Threshold Check**: Rejects batch if failure rate exceeds 5% quarantine threshold.

---

## Airflow Orchestration & Backfill Operations

### DAG Execution & CLI Backfill Commands

```bash
# Unpause the StreamPulse Medallion Orchestrator DAG
airflow dags unpause streampulse_medallion_dag

# Trigger manual execution for a specific date
airflow dags trigger streampulse_medallion_dag --conf '{"event_date": "2026-11-01"}'

# Execute idempotent backfill across date range
airflow dags backfill \
  --start-date 2026-10-01 \
  --end-date 2026-10-31 \
  --reset-dagruns \
  streampulse_medallion_dag
```

---

## PostgreSQL Warehouse DDL & Indexing Strategy

```sql
-- Gold Layer Range Partitioned Table
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

-- Partition Definitions
CREATE TABLE gold.events_fact_y2026m11 PARTITION OF gold.events_fact
    FOR VALUES FROM ('2026-11-01') TO ('2026-12-01');

-- Composite B-Tree & BRIN Indexing Strategy
CREATE INDEX idx_events_fact_user_type ON gold.events_fact USING btree (user_id, event_type);
CREATE INDEX idx_events_fact_brin_ts ON gold.events_fact USING brin (event_timestamp);

-- Materialized View Definition & Concurrent Refresh Strategy
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

-- Concurrent Materialized View Refresh Command
REFRESH MATERIALIZED VIEW CONCURRENTLY gold.mv_daily_kpi_metrics;
```
