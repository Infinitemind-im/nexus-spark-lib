# NEXUS — Iteration 2 · Data Model
**New and Modified Database Tables · Version 0.5**
Mentis Consulting · April 2026 · Confidential

> **Routing additions V2.0.20–V2.0.23 (added 2026-04-29):** Four migrations close the CDM field routing gap within Iteration 2. `cdm_proposals` gains seven new advisory columns (`db_target_suggestion`, `es_role_suggestion`, `ts_role_suggestion`, `neo4j_role_suggestion`, `routing_overridden`, `routing_override_by`, `routing_override_at`) via V2.0.20. A new canonical table `nexus_system.cdm_field_routing` (primary key: `cdm_version, entity, cdm_field`) is created via V2.0.21 — it is the single source of truth for per-field AI store routing and replaces the manually maintained `FieldManifest`. `cdm_entity_storage_config` gains three provenance columns (`derived_from_cdm_version`, `auto_derived`, `derived_at`) via V2.0.22; rows are now auto-derived by the Airflow routing refresh task rather than hand-seeded by Dev 5. A one-time bootstrap script (V2.0.23) seeds `cdm_field_routing` from `nexus_cdm_ground_truth_v3_classified.json` for CDM version `v3`. See **`NEXUS-Iter2-SPEC-CDMFieldRouting-v0.1.md`** (in `architecture/`) for full DDL, migration ledger, and derivation logic.
>
> **See also (added 2026-04-27, numbering updated 2026-04-29):** The CDM-to-AIStores pipeline series adds the following tables under V2.0.24+ migrations (V2.0.20–V2.0.23 are now reserved for CDM Field Routing — see routing additions banner above). DDL in the spec named: `cdm_entity_materialization`, `golden_record_provenance`, `survivorship_rules`, `mention_review_queue`, `er_thresholds`, `er_review_queue`, `er_override_log`, `deterministic_id_columns`, `entity_blocking_rules`, `entity_min_identification`, `documents_index`, `mention_review_queue` (CDM-AIStores-Pipeline). `materialization_policy`, `materialization_cohorts`, `materialization_decision_log`, `materialization_signal`, `cost_model`, `materialization_recommendations`, `materialization_movement_log` (MaterializationPolicy + DevD4). `feature_definitions`, `reward_models`, `feature_importance_history` (MaterializationFeatureLearning). `golden_records_index`, `golden_record_redirects`, `golden_record_split_history` (DevD3). `entity_store_presence`, `cdm_entity_storage_config` (DevD5). `connector_poll_state`, `connector_backfill_handover`, `spark_stream_checkpoint_audit` (DevD1 / DevD2). `batch_job_checkpoints`, `backfill_cost_log` (DevD2). Sequencing of V2.0.20+ migration numbers to be assigned during sprint kickoff.
>
> **Revision v0.5 — Spark transformation stage**
> Two additions: (1) `nexus_system.entity_resolution_index` — new table mapping source identifiers to Golden Record IDs, written by `nexus-spark-transformer` and read during entity resolution; (2) `delta_checkpoint_threshold` column added to `connector_batch_state` (OQ-SP-03 resolved with configurable default 500k). Two new migrations: V2.0.18 and V2.0.19.
> Two new columns added to `business_metrics_raw`: `is_correction BOOLEAN DEFAULT FALSE` (immutable-append correction pattern) and `is_deletion BOOLEAN DEFAULT FALSE` (tombstone deletion pattern). Both required by TimescaleDB Writer v0.2. `connector_batch_state` table added as a new migration (V2.0.8) to track Airbyte batch history cursor state per connector. Migration ledger extended through V2.0.17 (CDM Mapper v2, CDM Validation v2, RHMA v2). OQ-DM-07 added for the V2.0.3–V2.0.8 numbering conflict with SprintPlan. OQ-DM-08 added for `is_deletion` column confirmation.
>
> **Revision v0.3 — Architecture review corrections applied**
> `nexus_system.identity_mapping` table specified here for the first time.
> Migration script added. OQ-DM-06 added for Iteration 1 seeding verification.

---

## Overview

Iteration 2 introduces five new tables across two schemas:

| Schema | Table | Purpose | New / Modified |
|---|---|---|---|
| `nexus_system` | `query_sessions` | Modified — extended for Query Engine | Modified |
| `nexus_system` | `dashboard_components` | Persistent dashboard chart components | **New** |
| `nexus_system` | `cdm_catalogue_cache_log` | Audit log for CDM catalogue cache operations | **New** |
| `nexus_ts` | `business_metrics_raw` | TimescaleDB hypertable — normalised CDM event rows (3-month retention) | **New** |
| `nexus_ts` | `metrics_weekly` | Continuous aggregate — weekly rollup (12-month retention) | **New** |
| `nexus_ts` | `metrics_monthly` | Continuous aggregate — monthly rollup (6-year retention) | **New** |
| `nexus_ts` | `metrics_yearly` | Continuous aggregate — yearly rollup (permanent) | **New** |
| `nexus_ts` | `platform_metrics` | TimescaleDB hypertable — NEXUS service metrics | **New** |

No Iteration 1 tables are modified in ways that break existing consumers. All changes are additive (new columns with defaults, new tables).

---

## Schema: nexus_system

### 1. query_sessions (MODIFIED)

The `nexus_system.query_sessions` table introduced in Iteration 1 for `nexus-m2-api` is extended to serve `nexus-query-api` as the primary query surface in Iteration 2. A `pipeline` discriminator column distinguishes session origin. **All new sessions default to `pipeline = 'query'`** (nexus-query-api). The value `'m2'` is deprecated — preserved for Iteration 1 RHMA session history only; `nexus-m2-api` is no longer a user-facing entrypoint (OQ-M6-01 resolved, April 2026).

**Additive changes only** — existing M2 query session records are unaffected.

```sql
-- Add new columns to nexus_system.query_sessions (Iteration 2 additions)

ALTER TABLE nexus_system.query_sessions
    ADD COLUMN IF NOT EXISTS pipeline          VARCHAR(20) NOT NULL DEFAULT 'query',
    -- "query" (Query Engine, Iteration 2 — default for all new sessions)
    -- "m2" (DEPRECATED — RHMA pipeline, Iteration 1 only; existing rows preserved, no new rows should use this value)

    ADD COLUMN IF NOT EXISTS user_role         VARCHAR(50),
    -- User role from X-User-Role header (used for persona-aware rendering)

    ADD COLUMN IF NOT EXISTS output_preference VARCHAR(20) NOT NULL DEFAULT 'auto',
    -- "auto" | "text" | "table" | "bar_chart" | "line_chart" | "pie_chart" | "report"

    ADD COLUMN IF NOT EXISTS output_type       VARCHAR(20),
    -- Resolved output type after rendering (set by query-executor on completion)

    ADD COLUMN IF NOT EXISTS context           JSONB NOT NULL DEFAULT '{}',
    -- { "user_role": "cfo", "current_view": "financial_dashboard", "time_zone": "..." }

    ADD COLUMN IF NOT EXISTS result            JSONB,
    -- Full RenderedOutput JSON (set by query-executor on completion)
    -- NULL for M2 pipeline sessions (those use response_text instead)

    ADD COLUMN IF NOT EXISTS query_plan        JSONB,
    -- CDMQueryPlan JSON (set by query-executor after planning)

    ADD COLUMN IF NOT EXISTS sources_queried   TEXT[],
    -- Array of source system identifiers actually queried

    ADD COLUMN IF NOT EXISTS sources_failed    JSONB,
    -- List of SourceFailure objects if partial result

    ADD COLUMN IF NOT EXISTS partial           BOOLEAN NOT NULL DEFAULT FALSE,
    -- True if at least one source failed but result was still returned

    ADD COLUMN IF NOT EXISTS cdm_version       VARCHAR(20);
    -- CDM version used for this query

-- New indexes for Query Engine access patterns
CREATE INDEX IF NOT EXISTS qs_tenant_pipeline_idx
    ON nexus_system.query_sessions (tenant_id, pipeline, created_at DESC);

CREATE INDEX IF NOT EXISTS qs_tenant_status_pipeline_idx
    ON nexus_system.query_sessions (tenant_id, status, pipeline)
    WHERE status IN ('planning', 'decomposing', 'executing');
    -- Partial index for active sessions only

-- Retention: sessions older than 30 days are deleted by cleanup DAG
-- (Airflow DAG: nexus_cleanup_query_sessions — Iteration 2)
```

**Full schema after modifications:**

```sql
CREATE TABLE nexus_system.query_sessions (
    -- Original columns (Iteration 1)
    session_id          TEXT            PRIMARY KEY,
    tenant_id           TEXT            NOT NULL,
    user_id             TEXT            NOT NULL,
    query_text          TEXT            NOT NULL,
    status              TEXT            NOT NULL DEFAULT 'pending',
    -- status values (Iteration 1): pending | processing | completed | failed | timeout
    -- status values (Iteration 2): planning | decomposing | executing | rendering | completed | failed | timeout
    response_text       TEXT,           -- M2 pipeline: NL response text
    sources             JSONB,          -- M2 pipeline: source references
    reasoning_trace     JSONB,          -- M2 pipeline: RHMA reasoning steps
    model_used          TEXT,
    error_code          TEXT,
    created_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    completed_at        TIMESTAMPTZ,
    processing_time_ms  INT,

    -- Added in Iteration 2
    pipeline            VARCHAR(20)     NOT NULL DEFAULT 'query',
    -- DEFAULT changed to 'query' in Iteration 2. Value 'm2' is DEPRECATED (legacy Iteration 1 RHMA sessions only).
    user_role           VARCHAR(50),
    output_preference   VARCHAR(20)     NOT NULL DEFAULT 'auto',
    output_type         VARCHAR(20),
    context             JSONB           NOT NULL DEFAULT '{}',
    result              JSONB,
    query_plan          JSONB,
    sources_queried     TEXT[],
    sources_failed      JSONB,
    partial             BOOLEAN         NOT NULL DEFAULT FALSE,
    cdm_version         VARCHAR(20)
);

ALTER TABLE nexus_system.query_sessions ENABLE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON nexus_system.query_sessions
    FOR ALL TO nexus_app
    USING (tenant_id = current_setting('nexus.current_tenant_id', true));
```

---

### 2. dashboard_components (NEW)

Stores persistent chart and table components that users save to their dashboards.

```sql
CREATE TABLE IF NOT EXISTS nexus_system.dashboard_components (
    component_id        UUID            PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id           VARCHAR(100)    NOT NULL,
    created_by          VARCHAR(200)    NOT NULL,   -- user_id from Okta
    title               VARCHAR(300)    NOT NULL,
    chart_spec          JSONB           NOT NULL,   -- ChartSpec or TableOutput JSON
    output_type         VARCHAR(20)     NOT NULL,   -- bar_chart | line_chart | pie_chart | table
    query_nl            TEXT            NOT NULL,   -- Original natural language question
    query_plan          JSONB           NOT NULL,   -- Full CDMQueryPlan — used for refresh
    refresh_schedule    VARCHAR(20),                -- "hourly" | "daily" | null
    last_refreshed_at   TIMESTAMPTZ,               -- Timestamp of last successful refresh
    cdm_version         VARCHAR(20)     NOT NULL,   -- CDM version when component was created
    source_session_id   TEXT            NOT NULL,   -- Session that created this component
    created_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ,

    -- Constraints
    CONSTRAINT dc_valid_output_type CHECK (
        output_type IN ('bar_chart', 'line_chart', 'pie_chart', 'table')
    ),
    CONSTRAINT dc_valid_refresh_schedule CHECK (
        refresh_schedule IS NULL OR refresh_schedule IN ('hourly', 'daily')
    ),
    CONSTRAINT dc_unique_session_save UNIQUE (source_session_id, tenant_id)
    -- Prevents saving the same query result twice as separate components
);

-- Row-level security
ALTER TABLE nexus_system.dashboard_components ENABLE ROW LEVEL SECURITY;
CREATE POLICY dc_tenant_isolation ON nexus_system.dashboard_components
    FOR ALL TO nexus_app
    USING (tenant_id = current_setting('nexus.current_tenant_id', true));

-- Indexes
CREATE INDEX dc_tenant_created_idx
    ON nexus_system.dashboard_components (tenant_id, created_at DESC);

CREATE INDEX dc_refresh_due_idx
    ON nexus_system.dashboard_components (refresh_schedule, last_refreshed_at)
    WHERE refresh_schedule IS NOT NULL;
-- Used by Airflow dashboard_refresh DAG for efficient "due for refresh" queries

CREATE INDEX dc_created_by_idx
    ON nexus_system.dashboard_components (tenant_id, created_by, created_at DESC);
-- Used by M6 "My Components" view
```

**Access patterns:**
- `nexus-query-api` (INSERT on save-dashboard, SELECT for ownership check)
- Airflow `dashboard_refresh` DAG (SELECT for due components, UPDATE last_refreshed_at)
- M6 CFO/CEO dashboard (SELECT for display, via nexus-query-api GET endpoints)

---

### 3. cdm_catalogue_cache_log (NEW)

Audit log for CDM catalogue cache build and invalidation events. Used for diagnostics and to track cache miss frequency.

```sql
CREATE TABLE IF NOT EXISTS nexus_system.cdm_catalogue_cache_log (
    log_id          UUID            PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id       VARCHAR(100)    NOT NULL,
    cdm_version     VARCHAR(20)     NOT NULL,
    event_type      VARCHAR(20)     NOT NULL,  -- "build" | "hit" | "miss" | "invalidate"
    entity_count    INT,                       -- Number of entities in catalogue (on build)
    duration_ms     INT,                       -- Build duration (on build/miss rebuild)
    triggered_by    VARCHAR(100),              -- Service that triggered the event
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),

    CONSTRAINT ccl_valid_event CHECK (
        event_type IN ('build', 'hit', 'miss', 'invalidate')
    )
);

-- No RLS — this is a platform-admin-only diagnostic table
-- Retention: 7 days (purged by cleanup DAG)

CREATE INDEX ccl_tenant_created_idx
    ON nexus_system.cdm_catalogue_cache_log (tenant_id, created_at DESC);

CREATE INDEX ccl_event_type_idx
    ON nexus_system.cdm_catalogue_cache_log (event_type, created_at DESC);
```

---

## Schema: nexus_ts (TimescaleDB)

The `nexus_ts` schema is new in Iteration 2. It must be created before tables are created:

```sql
CREATE SCHEMA IF NOT EXISTS nexus_ts;
GRANT USAGE ON SCHEMA nexus_ts TO nexus_app;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA nexus_ts TO nexus_app;
ALTER DEFAULT PRIVILEGES IN SCHEMA nexus_ts GRANT ALL ON TABLES TO nexus_app;
```

### 4. business_metrics — Four-Tier TimescaleDB Architecture

> **Issues 1 + 2 correction:** The original single-table design stored raw monetary values without currency normalisation (silently incorrect cross-source SUMs) and applied a 1-year retention policy with no rollup (data loss after 12 months). This section has been completely rewritten to reflect the four-tier continuous aggregate architecture and FX normalisation applied in the M3 AI Stores spec.

#### 4.1 Tier 0: Raw Hypertable (`nexus_ts.business_metrics_raw`)

Stores individual normalised measurements. All monetary values are converted to the tenant's base currency before INSERT. Raw rows are retained for 3 months then dropped — historical trends are preserved in the continuous aggregate tiers.

```sql
CREATE TABLE IF NOT EXISTS nexus_ts.business_metrics_raw (
    -- Time dimension (hypertable partition key)
    time                TIMESTAMPTZ         NOT NULL,
    -- Event timestamp from CDM entity; falls back to approved_at if null

    -- Tenant isolation
    tenant_id           VARCHAR(100)        NOT NULL,

    -- Metric identity
    metric_name         VARCHAR(200)        NOT NULL,
    -- Canonical metric name from EVENT_TO_METRIC_MAP
    -- Examples: "deal_closed.amount", "headcount.hired", "tickets.opened"

    -- Value — Issues 1 + 2 correction: always normalised to base currency
    normalised_value    DECIMAL(18, 4),
    -- NULL for event-count metrics (use COUNT aggregation)
    -- Non-null for monetary metrics: normalised to tenant base currency

    base_currency       CHAR(3)             NOT NULL DEFAULT 'EUR',
    -- Tenant base currency. Monetary values already converted before INSERT.

    -- Dimensions (flexible JSONB for ad-hoc slicing)
    dimensions          JSONB               NOT NULL DEFAULT '{}',
    -- Includes original_currency and fx_rate for auditability:
    -- {
    --   "source":            "salesforce",
    --   "region":            "BE",
    --   "original_currency": "USD",    ← source currency before normalisation
    --   "fx_rate":           "1.0823"  ← rate used (original → base)
    -- }

    -- Source attribution
    source_system       VARCHAR(100)        NOT NULL,
    cdm_entity_id       VARCHAR(200),       -- CDM entity ID for idempotency
    cdm_version         VARCHAR(20)         NOT NULL,
    ingested_at         TIMESTAMPTZ         NOT NULL DEFAULT NOW(),

    -- Immutable-append write pattern flags (data flow spec v0.1)
    is_correction       BOOLEAN             NOT NULL DEFAULT FALSE,
    -- TRUE on correction pair rows (reversal + corrected value).
    -- Aggregation queries filter WHERE is_correction IS FALSE OR is_correction IS NULL
    -- to get the effective current value.

    is_deletion         BOOLEAN             NOT NULL DEFAULT FALSE
    -- TRUE on tombstone rows inserted when source entity is permanently deleted.
    -- Aggregation queries filter WHERE is_deletion IS NOT TRUE.
);

-- Create hypertable (partitioned by time, 7-day chunks)
SELECT create_hypertable(
    'nexus_ts.business_metrics_raw',
    'time',
    chunk_time_interval => INTERVAL '7 days',
    if_not_exists       => TRUE
);

-- Unique constraint for idempotency (ON CONFLICT DO NOTHING in INSERTs)
ALTER TABLE nexus_ts.business_metrics_raw
    ADD CONSTRAINT bmr_idempotency_key
    UNIQUE (time, tenant_id, metric_name, cdm_entity_id);

-- Row-level security
ALTER TABLE nexus_ts.business_metrics_raw ENABLE ROW LEVEL SECURITY;
CREATE POLICY bmr_tenant_isolation ON nexus_ts.business_metrics_raw
    FOR ALL TO nexus_app
    USING (tenant_id = current_setting('nexus.current_tenant_id', true));

-- Indexes
CREATE INDEX bmr_tenant_metric_time_idx
    ON nexus_ts.business_metrics_raw (tenant_id, metric_name, time DESC);
CREATE INDEX bmr_tenant_source_time_idx
    ON nexus_ts.business_metrics_raw (tenant_id, source_system, time DESC);
CREATE INDEX bmr_dimensions_gin_idx
    ON nexus_ts.business_metrics_raw USING GIN (dimensions);

-- Retention: raw data kept for 3 months (historical trends in aggregates below)
SELECT add_retention_policy(
    'nexus_ts.business_metrics_raw',
    INTERVAL '3 months',
    if_not_exists => TRUE
);

-- Compression: compress chunks older than 7 days
ALTER TABLE nexus_ts.business_metrics_raw SET (
    timescaledb.compress,
    timescaledb.compress_orderby   = 'time DESC',
    timescaledb.compress_segmentby = 'tenant_id, metric_name'
);
SELECT add_compression_policy(
    'nexus_ts.business_metrics_raw',
    INTERVAL '7 days',
    if_not_exists => TRUE
);
```

#### 4.2 Tier 1: Weekly Aggregate (`nexus_ts.metrics_weekly`)

```sql
CREATE MATERIALIZED VIEW nexus_ts.metrics_weekly
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 week', time)  AS bucket,
    tenant_id,
    metric_name,
    source_system,
    base_currency,
    SUM(normalised_value)         AS total_value,
    COUNT(*)                      AS event_count,
    AVG(normalised_value)         AS avg_value
FROM nexus_ts.business_metrics_raw
GROUP BY bucket, tenant_id, metric_name, source_system, base_currency
WITH NO DATA;

SELECT add_continuous_aggregate_policy('nexus_ts.metrics_weekly',
    start_offset => INTERVAL '4 weeks',
    end_offset   => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour'
);

-- Retention: 12 months
SELECT add_retention_policy('nexus_ts.metrics_weekly',
    INTERVAL '12 months', if_not_exists => TRUE);
```

#### 4.3 Tier 2: Monthly Aggregate (`nexus_ts.metrics_monthly`)

```sql
CREATE MATERIALIZED VIEW nexus_ts.metrics_monthly
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 month', bucket) AS bucket,
    tenant_id,
    metric_name,
    source_system,
    base_currency,
    SUM(total_value)                AS total_value,
    SUM(event_count)                AS event_count,
    AVG(avg_value)                  AS avg_value
FROM nexus_ts.metrics_weekly
GROUP BY time_bucket('1 month', bucket), tenant_id, metric_name, source_system, base_currency
WITH NO DATA;

SELECT add_continuous_aggregate_policy('nexus_ts.metrics_monthly',
    start_offset => INTERVAL '3 months',
    end_offset   => INTERVAL '1 day',
    schedule_interval => INTERVAL '1 day'
);

-- Retention: 6 years
SELECT add_retention_policy('nexus_ts.metrics_monthly',
    INTERVAL '6 years', if_not_exists => TRUE);
```

#### 4.4 Tier 3: Yearly Aggregate (`nexus_ts.metrics_yearly`)

```sql
CREATE MATERIALIZED VIEW nexus_ts.metrics_yearly
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 year', bucket)  AS bucket,
    tenant_id,
    metric_name,
    source_system,
    base_currency,
    SUM(total_value)                AS total_value,
    SUM(event_count)                AS event_count,
    AVG(avg_value)                  AS avg_value
FROM nexus_ts.metrics_monthly
GROUP BY time_bucket('1 year', bucket), tenant_id, metric_name, source_system, base_currency
WITH NO DATA;

SELECT add_continuous_aggregate_policy('nexus_ts.metrics_yearly',
    start_offset => INTERVAL '2 years',
    end_offset   => INTERVAL '1 month',
    schedule_interval => INTERVAL '1 week'
);
-- No retention policy — yearly aggregates are permanent.
```

#### 4.5 Tier Selection (nexus-query-executor)

```python
def select_timescale_tier(date_range: DateRange) -> str:
    """
    Routes TimescaleDB queries to the appropriate tier based on query span.
    Using the most-aggregated tier that covers the requested granularity
    minimises I/O and improves query latency.
    """
    span = (date_range.to - date_range.from_).days
    if span <= 90:
        return "nexus_ts.business_metrics_raw"   # Raw — up to 3 months
    elif span <= 365:
        return "nexus_ts.metrics_weekly"          # Weekly aggregates — up to 12 months
    elif span <= 365 * 6:
        return "nexus_ts.metrics_monthly"         # Monthly — up to 6 years
    else:
        return "nexus_ts.metrics_yearly"          # Yearly — permanent archive
```

#### 4.6 Canonical Metric Names (defined in nexus-m3-writer `EVENT_TO_METRIC_MAP`)

| CDM event type | metric_name | value_field | Notes |
|---|---|---|---|
| `deal_closed` | `deal_closed.amount` | `amount` | FX normalised |
| `invoice_paid` | `invoice_paid.amount` | `amount` | FX normalised |
| `invoice_raised` | `invoice_raised.amount` | `amount` | FX normalised |
| `employee_hired` | `headcount.hired` | constant: 1.0 | No FX needed |
| `employee_left` | `headcount.attrition` | constant: 1.0 | No FX needed |
| `order_shipped` | `order_shipped.value` | `total_value` | FX normalised |
| `support_ticket_opened` | `tickets.opened` | constant: 1.0 | No FX needed |
| `support_ticket_closed` | `tickets.closed` | constant: 1.0 | No FX needed |

#### 4.7 Typical TimescaleDB Queries (issued by nexus-query-executor)

```sql
-- "Show me revenue trend over the last 12 months" → routes to metrics_weekly
SELECT
    bucket,
    SUM(total_value)   AS total_revenue,
    SUM(event_count)   AS deal_count
FROM nexus_ts.metrics_weekly
WHERE tenant_id   = $1
  AND metric_name = 'deal_closed.amount'
  AND bucket      >= NOW() - INTERVAL '12 months'
GROUP BY bucket
ORDER BY bucket;

-- "Compare Q1 2025 vs Q1 2026 revenue" → routes to metrics_monthly (cross-year)
SELECT
    CASE
        WHEN bucket < '2026-01-01' THEN 'Q1 2025'
        ELSE                            'Q1 2026'
    END                            AS period,
    SUM(total_value)                AS total_revenue
FROM nexus_ts.metrics_monthly
WHERE tenant_id   = $1
  AND metric_name = 'deal_closed.amount'
  AND bucket BETWEEN '2025-01-01' AND '2026-03-31'
GROUP BY period;

-- "Revenue by region last quarter" → routes to business_metrics_raw (< 90 days)
SELECT
    dimensions ->> 'region'        AS region,
    SUM(normalised_value)           AS revenue
FROM nexus_ts.business_metrics_raw
WHERE tenant_id   = $1
  AND metric_name = 'deal_closed.amount'
  AND time        >= NOW() - INTERVAL '3 months'
GROUP BY region
ORDER BY revenue DESC;
```

---

### 5. platform_metrics (NEW — TimescaleDB Hypertable)

Stores operational metrics from NEXUS services. Used for the platform health dashboard and Grafana dashboards.

```sql
CREATE TABLE IF NOT EXISTS nexus_ts.platform_metrics (
    time            TIMESTAMPTZ         NOT NULL,
    tenant_id       VARCHAR(100),
    -- NULL for platform-wide metrics (e.g. total Kafka lag across all tenants)

    service_name    VARCHAR(100)        NOT NULL,
    -- "nexus-m3-writer" | "nexus-query-executor" | "nexus-m1-worker" | ...

    metric_name     VARCHAR(200)        NOT NULL,
    -- "m3_write_duration_seconds.p95" | "query_execution_time_ms" | ...

    metric_value    DECIMAL(18, 4)      NOT NULL,

    labels          JSONB               NOT NULL DEFAULT '{}'
    -- { "store": "elasticsearch", "status": "success" }
);

SELECT create_hypertable(
    'nexus_ts.platform_metrics',
    'time',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists       => TRUE
);

CREATE INDEX pm_service_metric_time_idx
    ON nexus_ts.platform_metrics (service_name, metric_name, time DESC);

CREATE INDEX pm_tenant_time_idx
    ON nexus_ts.platform_metrics (tenant_id, time DESC)
    WHERE tenant_id IS NOT NULL;

SELECT add_retention_policy(
    'nexus_ts.platform_metrics',
    INTERVAL '90 days',
    if_not_exists => TRUE
);

SELECT add_compression_policy(
    'nexus_ts.platform_metrics',
    INTERVAL '1 day',
    if_not_exists => TRUE
);
```

**Who writes to platform_metrics:**
- `nexus-m3-writer` — M3 write latencies per store, write failure counts
- `nexus-query-executor` — Query execution time by backend, source failure rates
- `nexus-m1-worker` — Sync job duration, connector error rates
- Future: all NEXUS services (in Iteration 3, via a shared metrics emitter in `nexus_core`)

**Who reads platform_metrics:**
- M6 platform health dashboard (via nexus-query-api)
- Grafana (via TimescaleDB data source, direct query — no NEXUS API intermediary)

[CLARIFY: Should Grafana connect directly to TimescaleDB for platform metrics, or should there be a dedicated `/metrics/platform` API endpoint in nexus-query-api? Direct connection is simpler but grants Grafana broader DB access. Recommend direct connection with a read-only Grafana role in Iteration 2.]

---

## Airflow DAGs — New in Iteration 2

### nexus_cleanup_query_sessions

Purges query session records older than 30 days to prevent unbounded table growth.

```python
@dag(schedule="@daily", ...)
def nexus_cleanup_query_sessions():
    @task
    def delete_old_sessions():
        with get_system_connection() as conn:
            deleted = conn.execute("""
                DELETE FROM nexus_system.query_sessions
                WHERE created_at < NOW() - INTERVAL '30 days'
                RETURNING session_id
            """).rowcount
            logger.info(f"Deleted {deleted} old query sessions")
```

### nexus_cleanup_catalogue_cache_log

Purges CDM catalogue cache log entries older than 7 days.

```python
@dag(schedule="@daily", ...)
def nexus_cleanup_catalogue_cache_log():
    @task
    def delete_old_logs():
        with get_system_connection() as conn:
            conn.execute("""
                DELETE FROM nexus_system.cdm_catalogue_cache_log
                WHERE created_at < NOW() - INTERVAL '7 days'
            """)
```

### dashboard_refresh

Defined in full in Visual Outputs spec (section 7.3). Runs hourly.

---

## Schema: nexus_system (continued)

### 6. identity_mapping (SPECIFIED HERE — seeded in Iteration 1, enforced in Iteration 2)

Maps Okta `user_id` values to their corresponding identities in each connected source system. Used by `nexus-query-executor`'s Query Decomposer (Rule 6) to forward the correct source-system identity when issuing live queries via connector-worker, ensuring that source-system RBAC is applied correctly.

**Example:** Alice logs into NEXUS via Okta (user_id `okta|alice@corp.com`). Her Salesforce identity is `alice.smith@corp.com` (a different email format). Without this mapping, connector-worker would forward the Okta identity, which Salesforce would not recognise, and Alice would receive no data. With the mapping, connector-worker forwards `alice.smith@corp.com` and Salesforce applies Alice's own RBAC rules.

```sql
CREATE TABLE IF NOT EXISTS nexus_system.identity_mapping (
    mapping_id          UUID            PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id           VARCHAR(100)    NOT NULL,
    okta_user_id        VARCHAR(300)    NOT NULL,   -- Okta subject claim ("sub"), e.g. "okta|alice@corp.com"
    source_system       VARCHAR(100)    NOT NULL,   -- "salesforce" | "servicenow" | "odoo" | "postgresql"
    connector_id        UUID            NOT NULL    REFERENCES nexus_system.connectors(connector_id),
    source_identity     VARCHAR(300)    NOT NULL,   -- The identity string the source system recognises
    -- e.g. "alice.smith@corp.com" for Salesforce, "DOMAIN\\alice" for SQL Server
    identity_type       VARCHAR(50)     NOT NULL    DEFAULT 'email',
    -- "email" | "username" | "domain_user" | "api_user" | "service_account"
    is_active           BOOLEAN         NOT NULL    DEFAULT TRUE,
    created_at          TIMESTAMPTZ     NOT NULL    DEFAULT NOW(),
    updated_at          TIMESTAMPTZ,
    synced_from         VARCHAR(100),               -- "manual" | "okta_scim" | "connector_probe"

    CONSTRAINT im_unique_user_source UNIQUE (tenant_id, okta_user_id, connector_id)
    -- One mapping per user per connector — multiple connectors per source system are allowed
);

-- Row-level security: queries from nexus-query-executor are tenant-scoped
ALTER TABLE nexus_system.identity_mapping ENABLE ROW LEVEL SECURITY;
CREATE POLICY im_tenant_isolation ON nexus_system.identity_mapping
    FOR ALL TO nexus_app
    USING (tenant_id = current_setting('nexus.current_tenant_id', true));

-- Lookup index: primary access pattern is (tenant_id, okta_user_id, connector_id)
CREATE INDEX im_user_connector_idx
    ON nexus_system.identity_mapping (tenant_id, okta_user_id, connector_id)
    WHERE is_active = TRUE;

-- Index for connector-level audit (which users are mapped to a given connector)
CREATE INDEX im_connector_idx
    ON nexus_system.identity_mapping (connector_id, tenant_id)
    WHERE is_active = TRUE;
```

**How the Query Decomposer uses this table:**

When building a `SourceQuery` for a live-source backend, the Decomposer calls:

```python
async def resolve_source_identity(
    self,
    tenant_id:    str,
    okta_user_id: str,
    connector_id: str,
) -> str:
    """
    Returns the source-system identity string for the given user and connector.
    Falls back to the raw Okta user_id if no mapping exists — this will typically
    result in the source system denying the query, which is correct behaviour
    (the user has no confirmed identity in that source system).
    """
    async with get_tenant_scoped_connection(self.pool, tenant_id) as conn:
        row = await conn.fetchrow(
            """
            SELECT source_identity
            FROM   nexus_system.identity_mapping
            WHERE  tenant_id    = $1
              AND  okta_user_id = $2
              AND  connector_id = $3
              AND  is_active    = TRUE
            """,
            tenant_id, okta_user_id, str(connector_id),
        )
    if row:
        return row["source_identity"]
    # No mapping found — forward Okta identity as-is (source will enforce its own RBAC)
    logger.warning(
        f"No identity mapping for user={okta_user_id} connector={connector_id} "
        f"tenant={tenant_id} — forwarding Okta identity as fallback"
    )
    return okta_user_id
```

**Seeding approach for Iteration 2:**
- New connectors registered via `nexus-m1-api` (POST `/connectors`) trigger a prompt in M6 asking platform admins to map user identities
- Optionally, Okta SCIM integration can auto-populate mappings for tenants that use Okta as their IdP for source systems
- Data stewards can manually create/update mappings via a platform-admin endpoint (not exposed to regular business users)

**Who writes to identity_mapping:**
- `nexus-m4-api` (platform-admin endpoint, M6 admin console — Iteration 2)
- Airflow `okta_scim_sync` DAG (if enabled — future)

**Who reads identity_mapping:**
- `nexus-query-executor` (read-only, RLS-scoped, for every live-source query)

---

## Schema: nexus_system (continued)

### 7. connector_batch_state (NEW — data flow spec v0.1)

Tracks Airbyte batch history cursor state per connector. Written by the `nexus_batch_history_ingest` Airflow DAG (D1-06). Survives service restarts — the DAG resumes from `last_cursor_value` without reprocessing history.

```sql
CREATE TABLE IF NOT EXISTS nexus_system.connector_batch_state (
    connector_id        UUID            PRIMARY KEY
                        REFERENCES nexus_system.connectors(connector_id)
                        ON DELETE CASCADE,
    tenant_id           VARCHAR(100)    NOT NULL,
    years_back          INT             NOT NULL DEFAULT 5,
    batch_size          INT             NOT NULL DEFAULT 500,
    cursor_field        VARCHAR(200)    NOT NULL DEFAULT 'updated_at',
    -- Source field used for incremental extraction (e.g. updated_at, seq_id)
    last_cursor_value   TEXT,
    -- High-water mark of last committed cursor. NULL = not yet started.
    last_run_at         TIMESTAMPTZ,
    status              VARCHAR(20)     NOT NULL DEFAULT 'pending'
                        CHECK (status IN ('pending', 'running', 'completed', 'failed')),
    error_message       TEXT,
    created_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ,
    delta_checkpoint_threshold INT NOT NULL DEFAULT 500000
    -- OQ-SP-03 resolved: configurable per connector.
    -- When a batch job processes more rows than this threshold, Spark writes
    -- transformed records to Delta Lake staging before publishing to
    -- m1.int.transformed_records. 0 = always bypass Delta Lake.
);

-- No RLS needed — this is a platform-level table read only by Airflow service account.
-- Access via nexus_app role with GRANT SELECT, INSERT, UPDATE.

CREATE INDEX cbs_tenant_idx ON nexus_system.connector_batch_state (tenant_id);
CREATE INDEX cbs_status_idx ON nexus_system.connector_batch_state (status)
    WHERE status IN ('pending', 'running');
```

**Migration:** V2.0.8 `create_connector_batch_state` (replaces the previously undefined V2.0.8 slot — see OQ-DM-07 for numbering conflict with SprintPlan).

---

### 8. entity_resolution_index (NEW — Spark transformation stage)

Maps source-system identifiers to NEXUS Golden Record IDs (`cdm_entity_id`). Written by `nexus-spark-transformer` during entity resolution. Read by `nexus-spark-transformer` on every subsequent record to check if a source identifier has already been resolved. Also readable by the CDM Mapper to verify entity continuity.

This table is the persistence layer for Spark's entity resolution logic — without it, entity resolution would be stateless and unable to link records across connectors or across batch runs.

```sql
CREATE TABLE IF NOT EXISTS nexus_system.entity_resolution_index (
    resolution_id       UUID            PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id           VARCHAR(100)    NOT NULL,
    cdm_entity_id       VARCHAR(200)    NOT NULL,  -- Golden Record ID (stable across sources)
    entity_type         VARCHAR(100)    NOT NULL,  -- e.g. "party.customer", "party.employee"
    source_system       VARCHAR(100)    NOT NULL,
    connector_id        UUID            NOT NULL REFERENCES nexus_system.connectors(connector_id),
    source_table        VARCHAR(200)    NOT NULL,
    source_record_id    VARCHAR(500)    NOT NULL,  -- source PK (KUNNR, Account_Id, etc.)
    confidence          NUMERIC(4,3)    NOT NULL DEFAULT 1.000,
    -- 1.000 = exact match (same Golden Record confirmed by human)
    -- 0.80–0.999 = high-confidence automated match
    -- < 0.80 = low-confidence; human review recommended
    resolution_method   VARCHAR(50)     NOT NULL DEFAULT 'spark_auto',
    -- "spark_auto"   — Spark deterministic rule (exact PK match)
    -- "spark_fuzzy"  — Spark ML similarity match
    -- "human"        — manually confirmed by data steward
    resolved_at         TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    spark_job_id        VARCHAR(200),   -- for lineage: which Spark job created this row
    is_active           BOOLEAN         NOT NULL DEFAULT TRUE,

    CONSTRAINT eri_unique_source_record
        UNIQUE (tenant_id, connector_id, source_table, source_record_id)
);

ALTER TABLE nexus_system.entity_resolution_index ENABLE ROW LEVEL SECURITY;
CREATE POLICY eri_tenant_isolation ON nexus_system.entity_resolution_index
    FOR ALL TO nexus_app
    USING (tenant_id = current_setting('nexus.current_tenant_id', true));

-- Primary lookup: resolve a source record to its Golden Record ID
CREATE INDEX eri_source_lookup_idx
    ON nexus_system.entity_resolution_index
    (tenant_id, connector_id, source_table, source_record_id)
    WHERE is_active = TRUE;

-- Reverse lookup: find all source records for a given Golden Record ID
CREATE INDEX eri_golden_record_idx
    ON nexus_system.entity_resolution_index
    (tenant_id, cdm_entity_id)
    WHERE is_active = TRUE;

-- Entity type index for bulk resolution by type
CREATE INDEX eri_entity_type_idx
    ON nexus_system.entity_resolution_index
    (tenant_id, entity_type, resolved_at DESC);
```

**Who writes:**
- `nexus-spark-transformer` — inserts or upserts on natural key `(tenant_id, connector_id, source_table, source_record_id)` during every transformation run
- M4 data stewards (via admin endpoint) — when manually confirming or correcting a fuzzy match (`resolution_method = 'human'`)

**Who reads:**
- `nexus-spark-transformer` — checks for existing `cdm_entity_id` before assigning a new one
- `nexus-cdm-mapper` — verifies `cdm_entity_id` on `SparkTransformResult` is consistent with known mappings
- `nexus-query-executor` — used during two-phase query pattern to join results across source systems by Golden Record ID

**Migration:** V2.0.19 `create_entity_resolution_index`

---



## Migration Order

Migrations must be applied in this order (managed by Flyway or Alembic):

```
V2.0.0__add_nexus_ts_schema.sql
V2.0.1__create_business_metrics_hypertable.sql        (includes is_correction + is_deletion columns)
V2.0.2__create_platform_metrics_hypertable.sql
V2.0.3__alter_query_sessions_iteration2.sql
V2.0.4__create_dashboard_components.sql
V2.0.5__create_cdm_catalogue_cache_log.sql
V2.0.6__create_identity_mapping.sql                   (creates table + RLS policy + indexes)
V2.0.7__create_neo4j_indexes.cypher                   (applied separately via Neo4j migration runner)
V2.0.8__create_connector_batch_state.sql              (Airflow batch history cursor tracking — NEW)
V2.0.9__alter_cdm_proposals_natural_key.sql           (CDM Mapper v2 — classifier_version + input_digest)
V2.0.10__create_cdm_ground_truth.sql                  (CDM Mapper v2)
V2.0.11__create_cdm_ground_truth_runs.sql             (CDM Mapper v2)
V2.0.12__create_cdm_feedback.sql                      (CDM Mapper v2 — consumed by RLHF placeholder)
V2.0.13__create_cdm_validation_simulations.sql        (CDM Validation v2)
V2.0.14__create_cdm_validation_recommendations.sql    (CDM Validation v2)
V2.0.15__create_llm_audit_log.sql                     (CDM Validation v2 — shared with RHMA and Query Engine)
V2.0.16__create_agent_runs.sql                        (RHMA v2)
V2.0.17__alter_tenant_configs_rhma_budget.sql         (RHMA v2 — rhma_max_tokens_per_query column)
V2.0.18__alter_connector_batch_state_add_threshold.sql (Spark stage — delta_checkpoint_threshold column)
V2.0.19__create_entity_resolution_index.sql           (Spark stage — Golden Record ID mapping table)
V2.0.20__add_routing_columns_to_cdm_proposals.sql     (CDM Field Routing — db_target_suggestion + 6 columns)
V2.0.21__create_cdm_field_routing.sql                 (CDM Field Routing — canonical per-field routing table)
V2.0.22__extend_cdm_entity_storage_config.sql         (CDM Field Routing — provenance columns)
V2.0.23__bootstrap_routing_from_ground_truth_v3.py    (CDM Field Routing — one-time seed from classified ground truth)
```

**Ownership.** Schemas V2.0.0–V2.0.8 are defined in this document. V2.0.9–V2.0.12 in `NEXUS-Iter2-CDM-Mapper-v0.1.md`. V2.0.13–V2.0.15 in `NEXUS-Iter2-CDM-Validation-Workflow-v0.1.md`. V2.0.16–V2.0.17 in `NEXUS-Iter2-RHMA-v0.1.md`. V2.0.18–V2.0.19 defined in this document (Spark stage). V2.0.20–V2.0.23 in `NEXUS-Iter2-SPEC-CDMFieldRouting-v0.1.md` (CDM Field Routing). This document is the single source of truth for **ordering** only — DDL is not duplicated here.

> ⚠️ **OQ-DM-07 unresolved** — V2.0.3–V2.0.8 numbering conflicts between this document and `NEXUS-Iter2-SprintPlan-v0.3.md`. Tech Lead must pick one canonical numbering before Dev 1 authors any migration file. See OQ-DM-07 below.

---

## Neo4j Schema (Complementary)

The Neo4j constraint and index migrations referenced in the M3 AI Stores spec are collected here for completeness:

```cypher
-- Applied via nexus-m3-writer init job on first deployment

-- Uniqueness constraints
CREATE CONSTRAINT employee_id_unique IF NOT EXISTS
    FOR (e:Employee) REQUIRE (e.id, e.tenant_id) IS UNIQUE;

CREATE CONSTRAINT department_id_unique IF NOT EXISTS
    FOR (d:Department) REQUIRE (d.id, d.tenant_id) IS UNIQUE;

CREATE CONSTRAINT customer_id_unique IF NOT EXISTS
    FOR (c:Customer) REQUIRE (c.id, c.tenant_id) IS UNIQUE;

CREATE CONSTRAINT event_id_unique IF NOT EXISTS
    FOR (ev:Event) REQUIRE (ev.id, ev.tenant_id) IS UNIQUE;

-- Lookup indexes
CREATE INDEX employee_tenant_idx  IF NOT EXISTS FOR (e:Employee)   ON (e.tenant_id);
CREATE INDEX department_tenant_idx IF NOT EXISTS FOR (d:Department) ON (d.tenant_id);
CREATE INDEX customer_tenant_idx  IF NOT EXISTS FOR (c:Customer)   ON (c.tenant_id);
CREATE INDEX event_tenant_idx     IF NOT EXISTS FOR (ev:Event)     ON (ev.tenant_id);

-- Relationship property index (for date-range filtering on relationships)
CREATE INDEX reports_to_created_idx IF NOT EXISTS
    FOR ()-[r:REPORTS_TO]-() ON (r.created_at);
```

---

## Open Questions

| # | Question | Impact |
|---|---|---|
| OQ-DM-01 | Should `nexus_ts` be a separate PostgreSQL database or a schema within the existing `nexus_system` PostgreSQL instance? TimescaleDB extension must be installed at the database level. | Infrastructure setup — confirm with Platform team |
| OQ-DM-02 | The `result` JSONB column in `query_sessions` can be large (full chart spec + data rows). Should large results be stored in MinIO with only a MinIO path in the DB? Threshold recommendation: > 100KB. | Storage cost and query performance |
| OQ-DM-03 | Should `dashboard_components` support soft-delete (a `deleted_at` column) to allow undo, or hard-delete for simplicity? | M6 UX scope for Iteration 2 |
| OQ-DM-04 | The `cdm_catalogue_cache_log` is purely diagnostic — should it be optional (only written when `LOG_CDM_CACHE_EVENTS=true`)? | Kafka throughput for high-query tenants |
| OQ-DM-05 | `business_metrics` uses a JSONB `dimensions` column for flexible slicing. Should common dimensions (region, currency, source) be promoted to dedicated columns for query performance? | TimescaleDB query planner efficiency |
| OQ-DM-06 | `nexus_system.identity_mapping` — was this table created in an Iteration 1 migration? If so, V2.0.6 should use `IF NOT EXISTS` only. Confirm with Tech Lead before running Iteration 2 migrations. | Migration safety |
| OQ-DM-07 | Migration numbering V2.0.3–V2.0.8 conflicts between this document and `NEXUS-Iter2-SprintPlan-v0.3.md`. Tech Lead must pick one canonical numbering before Dev 1 authors any migration file. Recommendation: adopt SprintPlan v0.3 numbering. | Blocks D1-02 migration authoring |
| OQ-DM-08 | `is_deletion BOOLEAN DEFAULT FALSE` column added to `business_metrics_raw` alongside `is_correction`. Confirm these two new columns are present in the V2.0.1 hypertable DDL (or in a new V2.0.X ALTER TABLE migration). Dev 1 must confirm before TimescaleDB Writer D2C-03 begins. | TimescaleDB Writer D2C-03/05 |

---

*NEXUS Iteration 2 · Data Model Spec · v0.4 · Mentis Consulting · April 2026 · Confidential*
