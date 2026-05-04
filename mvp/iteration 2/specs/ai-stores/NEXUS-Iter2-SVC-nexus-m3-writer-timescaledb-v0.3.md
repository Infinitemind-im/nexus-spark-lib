# NEXUS — Iteration 2 · `nexus-m3-writer` · TimescaleDB Time-Series Handler
## Complete Specification — Manage · Fill · Update · Flexibility · LLM Recommendations

**Service:** `nexus-m3-writer` · **Module:** `nexus_m3_writer/stores/timescale_writer.py`
**Developer C task** · Supersedes v0.1 and v0.2
Mentis Consulting · Version 0.3 · April 2026 · Confidential

**Related docs:**
- `NEXUS-Iter2-SVC-nexus-m3-writer-stores-v0.1.md` — master service spec
- `NEXUS-Iter2-SPEC-M3-AIStores-v0.5.md` — architectural invariants
- `NEXUS-Iter2-SPEC-DataModel-v0.5.md` — schema DDL (must be updated with V2.0.2–V2.0.3)

---

## Version History

| Version | Key addition |
|---|---|
| v0.1 | Immutable-append pattern, schema DDL, three write paths (insert / correction / tombstone), backfill stub |
| v0.2 | Adaptive Metric Registry (replaces hardcoded map), Chunk Coordinator, three-mode write engine, progressive parallel backfill, TimescaleDB Auto-Config |
| **v0.3** | **Capture strategies (all_attributes / multi_metric / preserve_payload), Metric Recommendation Pipeline (LLM intake contract + inbox table + confidence router + NOTIFY activation), query flexibility design** |

Everything in v0.1 and v0.2 not explicitly superseded remains in force.

---

## 1 · Architecture Overview

```
┌────────────────────────────────────────────────────────────────────────┐
│                        EXTERNAL (out of scope)                         │
│   CDM schema → LLM pipeline → MetricRecommendation[] → submit()       │
└───────────────────────────────┬────────────────────────────────────────┘
                                │ writes to
                                ▼
┌────────────────────────────────────────────────────────────────────────┐
│                    nexus_ts.metric_recommendations                     │
│                (persistent inbox — auditable, replayable)              │
└───────────────────────────────┬────────────────────────────────────────┘
                                │ DB trigger → process_recommendations()
                                ▼
                   ┌────────────────────────┐
                   │   Confidence router    │
                   │  ≥ 0.90 → auto-promote │
                   │  0.60–0.89 → review    │
                   │  < 0.60 → candidate    │
                   └────────────┬───────────┘
                                │ INSERT + NOTIFY metric_registry_updated
                                ▼
┌────────────────────────────────────────────────────────────────────────┐
│                       nexus_ts.metric_registry                         │
│         (hot-reload via LISTEN/NOTIFY + 30s poll fallback)             │
└───────────────────────────────┬────────────────────────────────────────┘
                                │ in-memory cache
                                ▼
            ┌───────────────────────────────────────┐
            │           TimescaleWriter             │
            │                                       │
            │  Kafka entity  →  registry.lookup()   │
            │  ┌──────────────────────────────┐     │
            │  │  WriteMode selector          │     │
            │  │  REALTIME / CORRECTION /     │     │
            │  │  BACKFILL                    │     │
            │  └──────────┬───────────────────┘     │
            │             │                         │
            │  ┌──────────▼───────────────────┐     │
            │  │  Chunk Coordinator           │     │
            │  │  (decompress if needed)      │     │
            │  └──────────┬───────────────────┘     │
            │             │                         │
            │  ┌──────────▼───────────────────┐     │
            │  │  Capture Strategy executor   │     │
            │  │  explicit / all_attributes / │     │
            │  │  multi_metric / preserve_    │     │
            │  │  payload                     │     │
            │  └──────────┬───────────────────┘     │
            └─────────────┼─────────────────────────┘
                          │
                          ▼
         nexus_ts.business_metrics_raw  (hypertable)
                          │
              continuous aggregates (automatic)
                          │
         ┌────────────────┼────────────────┐
         ▼                ▼                ▼
   metrics_weekly   metrics_monthly  metrics_yearly
```

---

## 2 · What Gets Saved and Who Decides

TimescaleDB in Nexus stores **any time-series data derivable from CDM entities** — business metrics (revenue, orders), operational metrics (server CPU, request latency), sensor readings, HR events, or any other numeric measurement that evolves over time. It is not limited to financial data.

The decision chain has four layers:

| Layer | Decides | Owner |
|---|---|---|
| CDM schema | What fields and entity types exist | CDM team |
| Business / domain analyst | Which event types are metric-producing | Business analyst (sets `is_active` in registry) |
| `metric_registry.value_field` | Which field is the primary aggregatable number | Analyst or LLM recommendation |
| `metric_registry.capture_strategy` | How much context to preserve for future queries | Analyst or LLM recommendation |

**The critical constraint:** you can only GROUP BY or FILTER on dimensions that were saved at write time. A dimension not captured when the row was written is permanently unrecoverable from TimescaleDB — recovery requires re-reading from the CDM source and backfilling. This makes the `capture_strategy` decision high-stakes for long-lived deployments.

**The architectural answer to this constraint:** the default `capture_strategy` is `all_attributes`, which preserves all non-null scalar fields from the CDM entity as dimensions. You can always prune dimensions at query time; you can never recover dimensions you didn't save.

---

## 3 · Adaptive Metric Registry (updated from v0.2)

### 3.1 Database Table

```sql
-- Migration V2.0.2
CREATE TABLE IF NOT EXISTS nexus_ts.metric_registry (
    registry_id        BIGSERIAL      PRIMARY KEY,
    event_type         VARCHAR(300)   NOT NULL UNIQUE,
    -- e.g. "salesforce.opportunity.won", "device.sensor.reading", "hr.headcount.change"
    metric_name        VARCHAR(200)   NOT NULL,
    value_field        VARCHAR(300)   NOT NULL,
    -- JSONPath into CDM payload: "$.Amount", "$.temperature", "$.LineItems.length()"
    value_transform    VARCHAR(50)    NOT NULL DEFAULT 'identity',
    -- 'identity'     → field value as-is
    -- 'negate'       → multiply by -1 (returns, refunds, reductions)
    -- 'constant_1'   → ignore field value, emit 1.0 (headcount, event-count metrics)
    -- 'sum_children' → sum all leaf numeric values at value_field path

    capture_strategy   VARCHAR(20)    NOT NULL DEFAULT 'all_attributes',
    -- 'explicit'         → only capture fields in dimensions_map (v0.1 behaviour, not recommended)
    -- 'all_attributes'   → capture all non-null scalar CDM fields as dimensions (DEFAULT)
    -- 'multi_metric'     → one entity → multiple rows, one per entry in companion_metrics
    -- 'preserve_payload' → save full CDM payload in raw_payload for future metric derivation

    dimensions_map     JSONB          NOT NULL DEFAULT '{}',
    -- For 'explicit': whitelist of {dimension_name: CDM_field_path}
    -- For 'all_attributes': optional rename/alias map (applied on top of auto-capture)
    -- e.g. {"region": "$.BillingCountry", "segment": "$.Account.Type"}

    companion_metrics  JSONB          DEFAULT NULL,
    -- Only used when capture_strategy = 'multi_metric'
    -- Maps additional metric_names to their value_fields
    -- e.g. {"line_item_count": "$.LineItems.length()", "discount_amount": "$.DiscountAmount"}

    gapfill_strategy   VARCHAR(20)    NOT NULL DEFAULT 'none',
    -- 'none'        → gaps left as gaps (most metrics)
    -- 'locf'        → last observation carried forward (e.g. headcount, ARR)
    -- 'interpolate' → linear interpolation (e.g. sensor readings)
    -- Applied by the query tier, not the writer — stored here as metadata for the query layer

    is_active          BOOLEAN        NOT NULL DEFAULT TRUE,
    confidence         DECIMAL(4,3)   NOT NULL DEFAULT 1.000,
    -- 1.000 = human-confirmed; < 1.000 = LLM recommendation
    -- Writer only uses entries WHERE is_active = TRUE

    registered_at      TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
    registered_by      VARCHAR(100)   NOT NULL DEFAULT 'manual',
    -- 'manual' | 'llm-auto-promoted' | 'llm-human-approved' | 'migration' | 'import'
    notes              TEXT,
    source_cdm_version VARCHAR(20)
    -- CDM schema version this mapping was derived from
);

-- Seed with confirmed mappings (migration V2.0.2)
INSERT INTO nexus_ts.metric_registry
    (event_type, metric_name, value_field, capture_strategy, registered_by)
VALUES
    ('salesforce.opportunity.won',      'revenue_booked', '$.Amount',      'all_attributes', 'migration'),
    ('salesforce.order.created',        'order_value',    '$.TotalAmount', 'all_attributes', 'migration'),
    ('adventureworks.salesorderheader', 'revenue_booked', '$.TotalDue',    'all_attributes', 'migration')
ON CONFLICT DO NOTHING;

-- Hot-reload NOTIFY trigger (unchanged from v0.2)
CREATE OR REPLACE FUNCTION nexus_ts.notify_registry_change()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    PERFORM pg_notify('metric_registry_updated', row_to_json(NEW)::text);
    RETURN NEW;
END;
$$;

CREATE TRIGGER metric_registry_notify_trigger
    AFTER INSERT OR UPDATE ON nexus_ts.metric_registry
    FOR EACH ROW EXECUTE FUNCTION nexus_ts.notify_registry_change();
```

### 3.2 Capture Strategy Detail

**`all_attributes` (default, recommended)**

The writer extracts all non-null scalar fields from the CDM entity payload and merges them into the `dimensions` JSONB column. `dimensions_map` acts as an alias layer (rename specific keys) rather than a whitelist. No future questions are blocked by a missing dimension.

```python
# Pseudocode for all_attributes extraction
raw_dims = {k: v for k, v in entity.payload.items()
            if isinstance(v, (str, int, float, bool)) and v is not None}
aliases = registry_entry.dimensions_map  # rename map
dims = {aliases.get(k, k): v for k, v in raw_dims.items()}
# Always add: source, original_currency, fx_rate (from FX normalisation step)
dims.update({"source": entity.source_system,
             "original_currency": entity.currency,
             "fx_rate": fx_rate})
```

**`multi_metric` (for entities with multiple measurable values)**

One CDM entity writes N rows to `business_metrics_raw`, one per entry in `companion_metrics` plus the primary `value_field`. Each row has the same `time`, `tenant_id`, `cdm_entity_id`, `cdm_version`, and `dimensions`, but a different `metric_name` and `metric_value`. This is what enables "average deal size" queries (requires both `revenue_booked` and `deal_count` as separate rows so you can divide them at query time).

```python
# Primary row
rows = [build_row(entity, metric_name, value_field, dims)]
# Companion rows
if registry_entry.companion_metrics:
    for companion_name, companion_field in registry_entry.companion_metrics.items():
        companion_value = extract_jsonpath(entity.payload, companion_field)
        if companion_value is not None:
            rows.append(build_row(entity, companion_name, companion_field, dims,
                                  metric_value=companion_value))
await conn.executemany(_INSERT_SQL_NO_CONFLICT, [r.as_params() for r in rows])
```

**`preserve_payload` (for high-value entity types)**

Stores the full CDM entity payload in a `raw_payload JSONB` column alongside the extracted `metric_value`. Enables deriving new metrics from historical data without re-reading source systems. Storage overhead is modest after TimescaleDB columnar compression (repeated JSONB keys compress extremely well).

```sql
-- Additional column added to business_metrics_raw for preserve_payload rows
-- (added via ALTER in migration V2.0.3 — NULL for all other rows)
ALTER TABLE nexus_ts.business_metrics_raw
    ADD COLUMN IF NOT EXISTS raw_payload JSONB DEFAULT NULL;
```

**`explicit` (legacy, not recommended for new mappings)**

Only captures dimensions explicitly listed in `dimensions_map`. Preserved for compatibility with any existing integrations that rely on minimal dimension storage.

---

## 4 · Metric Recommendation Pipeline (NEW in v0.3)

### 4.1 Design Principle

The question of **what metrics to save** is a domain intelligence problem, not a TimescaleDB implementation problem. An LLM analyses CDM entity schemas and produces structured recommendations. The TimescaleDB writer's responsibility is to provide a clean, stable reception mechanism for those recommendations — a contract that decouples the intelligence (external) from the plumbing (internal).

The writer **never calls the LLM**. The LLM **never knows about TimescaleDB internals**. The `MetricRecommendation` schema is the entire shared surface.

### 4.2 The Contract — `MetricRecommendation`

This is the data structure the LLM pipeline must produce. It maps directly to a `metric_registry` row.

```python
from pydantic import BaseModel, Field, confloat
from typing import Literal

class MetricRecommendation(BaseModel):
    """
    The complete contract between the LLM recommendation pipeline
    and the TimescaleDB writer. The LLM produces this; the writer consumes it.
    Neither party needs to know about the other's internals.
    """

    # ── Identity ──────────────────────────────────────────────────────────────
    event_type:   str = Field(description="CDM event type, e.g. 'device.sensor.reading'")
    metric_name:  str = Field(description="Canonical metric name for storage + querying")

    # ── Extraction ────────────────────────────────────────────────────────────
    value_field:       str   = Field(description="JSONPath into CDM payload, e.g. '$.temperature'")
    value_transform:   Literal["identity", "negate", "constant_1", "sum_children"] = "identity"
    capture_strategy:  Literal["explicit", "all_attributes", "multi_metric", "preserve_payload"] = "all_attributes"
    dimensions_map:    dict  = Field(default_factory=dict,
                                     description="Rename/alias map, or explicit whitelist for 'explicit' strategy")
    companion_metrics: dict  = Field(default_factory=dict,
                                     description="Additional metric_name → value_field pairs for 'multi_metric' strategy")
    gapfill_strategy:  Literal["none", "locf", "interpolate"] = "none"

    # ── Provenance ────────────────────────────────────────────────────────────
    rationale:          str   = Field(description="Why this metric is useful — shown in admin review UI")
    confidence:         float = Field(ge=0.0, le=1.0,
                                      description="LLM confidence. ≥0.90=auto-promote, 0.60–0.89=human review, <0.60=candidate only")
    source_cdm_version: str   = Field(description="CDM schema version the LLM analysed")
    submitted_by:       str   = Field(default="unknown",
                                      description="Agent or model identifier, e.g. 'llm-nexus-cdm-analyser-v1'")
```

**Example recommendation from the LLM for an IoT sensor entity:**

```json
{
  "event_type":          "device.sensor.reading",
  "metric_name":         "sensor_temperature_celsius",
  "value_field":         "$.readings.temperature",
  "value_transform":     "identity",
  "capture_strategy":    "multi_metric",
  "dimensions_map":      {"device_id": "$.device.id", "location": "$.device.location.name"},
  "companion_metrics":   {
    "sensor_humidity_pct":  "$.readings.humidity",
    "sensor_pressure_hpa":  "$.readings.pressure"
  },
  "gapfill_strategy":    "locf",
  "rationale":           "IoT sensor entities carry three independent continuous measurements. Saving all three as companion metrics enables cross-metric correlation at query time (e.g. humidity vs temperature). LOCF gap-fill is appropriate because sensor readings are continuous physical states.",
  "confidence":          0.94,
  "source_cdm_version":  "3.2.1",
  "submitted_by":        "llm-nexus-cdm-analyser-v2"
}
```

### 4.3 Inbox Table — `metric_recommendations`

```sql
-- Migration V2.0.3
CREATE TABLE IF NOT EXISTS nexus_ts.metric_recommendations (
    rec_id               BIGSERIAL      PRIMARY KEY,
    event_type           VARCHAR(300)   NOT NULL,
    metric_name          VARCHAR(200)   NOT NULL,
    recommendation       JSONB          NOT NULL,
    -- Full MetricRecommendation payload — single source of truth for the row
    rationale            TEXT,
    confidence           DECIMAL(4,3)   NOT NULL,
    source_cdm_version   VARCHAR(20),
    submitted_by         VARCHAR(100)   NOT NULL DEFAULT 'unknown',
    submitted_at         TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
    status               VARCHAR(20)    NOT NULL DEFAULT 'pending',
    -- 'pending'    → awaiting routing (just arrived)
    -- 'promoted'   → copied to metric_registry (auto or human)
    -- 'rejected'   → human explicitly declined
    -- 'superseded' → a newer recommendation for same event_type replaced this one
    promoted_registry_id BIGINT         REFERENCES nexus_ts.metric_registry(registry_id),
    reviewed_by          VARCHAR(100),  -- NULL if auto-promoted
    reviewed_at          TIMESTAMPTZ,
    review_notes         TEXT           -- human override rationale (if any)
);

CREATE INDEX IF NOT EXISTS mr_event_type_status_idx
    ON nexus_ts.metric_recommendations (event_type, status, submitted_at DESC);

CREATE INDEX IF NOT EXISTS mr_pending_idx
    ON nexus_ts.metric_recommendations (status, confidence DESC)
    WHERE status = 'pending';

-- Trigger: fire process_recommendations() on every INSERT
CREATE OR REPLACE FUNCTION nexus_ts.on_recommendation_inserted()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    PERFORM pg_notify('metric_recommendation_received', row_to_json(NEW)::text);
    RETURN NEW;
END;
$$;

CREATE TRIGGER recommendation_notify_trigger
    AFTER INSERT ON nexus_ts.metric_recommendations
    FOR EACH ROW EXECUTE FUNCTION nexus_ts.on_recommendation_inserted();

GRANT SELECT, INSERT ON nexus_ts.metric_recommendations TO nexus_app;
```

### 4.4 Confidence-Based Routing

```python
@dataclass
class RoutingConfig:
    auto_promote_threshold: float = 0.90   # env: TIMESCALE_AUTO_PROMOTE_THRESHOLD
    review_threshold:       float = 0.60   # env: TIMESCALE_REVIEW_THRESHOLD

@dataclass
class RecommendationResult:
    total:          int
    auto_promoted:  int    # inserted to registry with is_active=TRUE
    queued_review:  int    # inserted to registry with is_active=FALSE
    candidates:     int    # stored in recommendations only
    skipped:        int    # already exists in registry for this event_type
    errors:         list[str]

async def process_recommendations(
    self,
    recommendations: list[MetricRecommendation],
    routing:         RoutingConfig | None = None,
) -> RecommendationResult:
    """
    Receives a list of MetricRecommendation objects and routes each one
    based on confidence. Called either:
      (a) by the LISTEN handler when 'metric_recommendation_received' fires, or
      (b) directly from the LLM pipeline via submit_recommendations().
    Idempotent: if a registry entry already exists for event_type, the
    recommendation is stored with status='superseded' (not a duplicate error).
    """
    routing = routing or RoutingConfig()
    result  = RecommendationResult(total=len(recommendations), ...)

    for rec in recommendations:
        # Store in inbox first (always — for audit trail)
        rec_id = await self._store_recommendation(rec)

        # Check if registry already has an active entry for this event_type
        existing = await self.registry.lookup(rec.event_type)
        if existing is not None:
            await self._mark_superseded(rec_id, existing.registry_id)
            result.skipped += 1
            continue

        if rec.confidence >= routing.auto_promote_threshold:
            # Auto-promote: write to registry, activate immediately
            reg_id = await self._promote_to_registry(
                rec, is_active=True, registered_by="llm-auto-promoted"
            )
            await self._update_rec_status(rec_id, "promoted", reg_id)
            result.auto_promoted += 1
            log.info("metric auto-promoted", event_type=rec.event_type,
                     metric_name=rec.metric_name, confidence=rec.confidence)

        elif rec.confidence >= routing.review_threshold:
            # Queue for human review: write to registry but inactive
            reg_id = await self._promote_to_registry(
                rec, is_active=False, registered_by="llm-human-approved"
            )
            await self._update_rec_status(rec_id, "promoted", reg_id)
            result.queued_review += 1
            log.info("metric queued for review", event_type=rec.event_type,
                     metric_name=rec.metric_name, confidence=rec.confidence)

        else:
            # Low confidence — stays in recommendations table only
            result.candidates += 1
            log.info("metric candidate stored", event_type=rec.event_type,
                     metric_name=rec.metric_name, confidence=rec.confidence)

    # NOTIFY fires automatically via DB trigger when metric_registry is updated
    # MetricRegistry LISTEN handler picks it up and reloads within < 30s
    return result
```

### 4.5 Human Review and Promotion

Metrics queued for review (`0.60 ≤ confidence < 0.90`) sit in `metric_registry` with `is_active = FALSE`. An admin activates them:

```python
async def activate_metric(
    self,
    event_type:    str,
    reviewed_by:   str,
    review_notes:  str | None = None,
    overrides:     dict | None = None,
    # overrides can adjust value_field, capture_strategy, dimensions_map, etc.
    # before activation — useful when the LLM got the structure right but
    # named a field slightly differently than expected
) -> None:
    """
    Sets is_active=TRUE in metric_registry for event_type.
    Applies any overrides to the registry row first.
    NOTIFY fires automatically → writer reloads within 30s.
    Updates the source recommendation row with reviewed_by and reviewed_at.
    """

async def reject_recommendation(
    self,
    rec_id:        int,
    reviewed_by:   str,
    review_notes:  str,
) -> None:
    """
    Marks recommendation as 'rejected' in metric_recommendations.
    Does NOT affect metric_registry — if the event_type was already
    in the registry (is_active=FALSE), it remains there until explicitly
    deleted by an admin.
    """
```

### 4.6 Zero-Deploy Activation Chain

The full path from LLM output to a live metric in TimescaleDB, with no service restart:

```
1. LLM pipeline calls submit_recommendations([MetricRecommendation(...)])
        │
        ▼
2. INSERT INTO nexus_ts.metric_recommendations
        │
        ▼ (DB trigger fires)
3. pg_notify('metric_recommendation_received', payload)
        │
        ▼
4. TimescaleWriter LISTEN handler calls process_recommendations()
        │
        ├─ confidence ≥ 0.90 ──▶ INSERT nexus_ts.metric_registry (is_active=TRUE)
        │                               │
        │                               ▼ (registry notify trigger fires)
        │                        pg_notify('metric_registry_updated', payload)
        │                               │
        │                               ▼
        │                        MetricRegistry LISTEN handler → reload in-memory dict
        │                               │
        │                               ▼  (within < 30s of original submission)
        │                        Next CDM entity with matching event_type
        │                               → row written to business_metrics_raw
        │
        └─ confidence < 0.90 ──▶ stored for review (no immediate effect on writes)
```

---

## 5 · Query Flexibility Design

### 5.1 The Write-Time Constraint

The aggregation tiers (weekly / monthly / yearly) are computed automatically by TimescaleDB from the raw rows in `business_metrics_raw`. The tiers are fully flexible — you can query by any combination of `time_bucket`, `metric_name`, and any key present in `dimensions`. **The constraint is: you can only GROUP BY or FILTER on dimensions that were saved at write time.**

| If you want to ask at query time… | You must save at write time… |
|---|---|
| Total revenue this month | `metric_value`, `time` |
| Revenue by region | + `dimensions.region` |
| Revenue by sales rep | + `dimensions.sales_rep_id` |
| Revenue by product category | + `dimensions.product_category` |
| FX impact on reported revenue | + `dimensions.original_currency`, `dimensions.fx_rate` |
| Average deal size | `revenue_booked` row + `deal_count` row (`multi_metric`) |
| Revenue by rep AND region AND product | + all three dimension fields |
| Any future question not yet known | → use `capture_strategy = 'all_attributes'` |

### 5.2 Tier Selection

The query executor selects the appropriate tier automatically based on the requested date range. The writer has no involvement in this decision.

```python
range_days <= 90    → nexus_ts.business_metrics_raw   # raw events, full resolution
range_days <= 365   → nexus_ts.metrics_weekly          # 1-week buckets
range_days <= 2190  → nexus_ts.metrics_monthly         # 1-month buckets
else                → nexus_ts.metrics_yearly           # 1-year buckets
```

### 5.3 Gap-Fill at Query Time

The `gapfill_strategy` field in `metric_registry` is metadata for the query layer, not the writer. For metrics that are not emitted on every day (e.g. headcount, ARR, sensor readings), the query executor should apply TimescaleDB's gap-fill functions:

```sql
-- LOCF: last observation carried forward (headcount, ARR, stock levels)
SELECT time_bucket_gapfill('1 week', time) AS bucket,
       tenant_id, metric_name,
       locf(SUM(metric_value)) AS value
FROM nexus_ts.business_metrics_raw
WHERE metric_name = 'headcount'
  AND time BETWEEN '2026-01-01' AND '2026-04-30'
GROUP BY 1, 2, 3;

-- Interpolate (sensor readings, prices)
SELECT time_bucket_gapfill('1 hour', time) AS bucket,
       dimensions->>'device_id' AS device,
       interpolate(AVG(metric_value)) AS temperature
FROM nexus_ts.business_metrics_raw
WHERE metric_name = 'sensor_temperature_celsius'
  AND time > NOW() - INTERVAL '24 hours'
GROUP BY 1, 2;
```

---

## 6 · Complete Interface

```python
class TimescaleWriter:

    # ── Lifecycle ──────────────────────────────────────────────────────────────

    async def start(self) -> None:
        """
        1. Opens asyncpg connection pool (TIMESCALEDB_DSN)
        2. Loads MetricRegistry from DB
        3. Starts LISTEN on 'metric_registry_updated' (hot-reload) and
           'metric_recommendation_received' (recommendation intake)
        4. Starts 30s poll fallback for both channels
        5. Runs TimescaleAutoConfig.calibrate() once
        6. Schedules calibrate() every 24h
        """

    async def stop(self) -> None:
        """Graceful shutdown: drain in-flight tasks, close pool."""

    # ── Primary write paths (called by Kafka consumer via Dev 5 shell) ─────────

    async def write(self, entity: CDMEntity) -> None:
        """
        Routes incoming entity to the appropriate write path.

        Lookup entity.event_type in MetricRegistry:
          - Not found → stage to unresolved_events + structured WARNING log
          - Found, entity.version == 1 → REALTIME insert
          - Found, entity.version > 1, correction_context present → CORRECTION (Strategy A)
          - Found, entity.version > 1, no correction_context → CORRECTION (Strategy B, DB lookup)

        Respects capture_strategy from registry entry:
          - all_attributes  → extract all scalar CDM fields as dimensions
          - explicit        → use only dimensions_map whitelist
          - multi_metric    → write N rows (primary + companion_metrics)
          - preserve_payload→ write row + store full payload in raw_payload column
        """

    async def delete(self, entity: CDMEntity) -> None:
        """Tombstone: append is_deletion=TRUE row. Never UPDATE or DELETE existing rows."""

    async def health_check(self) -> StoreHealth:
        """Connectivity + registry load status + last calibration time + pending recommendations count."""

    # ── Backfill (Progressive Parallel Backfill Engine) ────────────────────────

    async def backfill(self,
        tenant_id:   str,
        start:       datetime | None = None,
        end:         datetime | None = None,
        num_workers: int = 4,
        batch_size:  int = 500,
    ) -> BackfillResult:
        """
        Chunk-topology-aware parallel backfill.
        Workers aligned to chunk boundaries — no cross-worker contention.
        Decompresses compressed historical chunks before writing; recompresses on completion.
        Per-chunk: aggregate refresh + recompress.
        Fully resumable: ON CONFLICT DO NOTHING on unique key.
        """

    # ── Aggregate management ───────────────────────────────────────────────────

    async def refresh_aggregates_for_range(self,
        start_dt:  datetime,
        end_dt:    datetime,
        view_name: str | None = None,
    ) -> None:
        """
        Always run via asyncio.create_task() — never block Kafka consumer path.
        view_name=None refreshes all three tiers.
        """

    # ── Recommendation pipeline ────────────────────────────────────────────────

    async def submit_recommendations(self,
        recommendations: list[MetricRecommendation],
    ) -> RecommendationResult:
        """
        Entry point for the LLM pipeline. Stores all recommendations in
        metric_recommendations inbox, then calls process_recommendations().
        Idempotent: duplicate event_type submissions are marked 'superseded'.
        """

    async def process_recommendations(self,
        recommendations: list[MetricRecommendation],
        routing:         RoutingConfig | None = None,
    ) -> RecommendationResult:
        """
        Confidence-based router (see Section 4.4).
        Also called by the LISTEN handler when 'metric_recommendation_received' fires.
        """

    async def activate_metric(self,
        event_type:   str,
        reviewed_by:  str,
        review_notes: str | None = None,
        overrides:    dict | None = None,
    ) -> None:
        """
        Human-activates a registry entry that is is_active=FALSE (pending review).
        NOTIFY fires → writer reloads within 30s.
        """

    async def reject_recommendation(self,
        rec_id:       int,
        reviewed_by:  str,
        review_notes: str,
    ) -> None:
        """Marks recommendation as 'rejected'. Does not remove registry entry."""

    # ── Registry inspection ────────────────────────────────────────────────────

    async def get_registry_stats(self) -> RegistryStats:
        """
        Returns:
          - active_metrics: count of is_active=TRUE entries
          - pending_review: count of is_active=FALSE entries
          - candidates: count of recommendations with status='pending'
          - unresolved_event_types: distinct event_types in unresolved_events (last 24h)
        """

    async def flush_unresolved_events(self,
        event_type: str,
        new_state:  Literal["registered", "ignored"],
    ) -> int:
        """
        Updates resolution_state for all pending unresolved_events of this type.
        Called after a human adds a mapping to metric_registry.
        Returns count of updated rows.
        """
```

---

## 7 · Complete Data Model

### 7.1 Migration Sequence

| Migration | Tables / Changes |
|---|---|
| V2.0.0 | `nexus_ts` schema creation |
| V2.0.1 | `business_metrics_raw` hypertable + continuous aggregates + `is_correction` / `is_deletion` columns |
| **V2.0.2** | `metric_registry` (with `capture_strategy`, `companion_metrics`, `source_cdm_version`) + `unresolved_events` |
| **V2.0.3** | `metric_recommendations` inbox + `business_metrics_raw.raw_payload` column (nullable) |

### 7.2 All Tables in `nexus_ts`

| Table / View | Type | Purpose |
|---|---|---|
| `business_metrics_raw` | Hypertable | Raw time-series events — every metric row |
| `metrics_weekly` | Continuous Aggregate | 1-week SUM/COUNT, 12-month retention |
| `metrics_monthly` | Continuous Aggregate | 1-month SUM/COUNT, 6-year retention |
| `metrics_yearly` | Continuous Aggregate | 1-year SUM/COUNT, permanent |
| `platform_metrics` | Hypertable | Service-level operational metrics |
| `metric_registry` | Regular table | Live registry of active metric mappings |
| `metric_recommendations` | Regular table | LLM recommendation inbox |
| `unresolved_events` | Regular table | Staging for CDM entities with no registry match |

### 7.3 `business_metrics_raw` — Full DDL

```sql
CREATE TABLE IF NOT EXISTS nexus_ts.business_metrics_raw (
    time                  TIMESTAMPTZ    NOT NULL,
    tenant_id             VARCHAR(100)   NOT NULL,
    metric_name           VARCHAR(200)   NOT NULL,
    metric_value          DECIMAL(18,4),
    base_currency         VARCHAR(10)    NOT NULL DEFAULT 'EUR',
    dimensions            JSONB          NOT NULL DEFAULT '{}',
    source_system         VARCHAR(100)   NOT NULL,
    cdm_entity_id         VARCHAR(200),
    cdm_version           VARCHAR(20)    NOT NULL,
    materialization_level VARCHAR(10)    NOT NULL DEFAULT 'hot',
    is_correction         BOOLEAN        NOT NULL DEFAULT FALSE,
    is_deletion           BOOLEAN        NOT NULL DEFAULT FALSE,
    ingested_at           TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
    raw_payload           JSONB          DEFAULT NULL
    -- NULL for all rows except capture_strategy = 'preserve_payload'
);

SELECT create_hypertable('nexus_ts.business_metrics_raw', 'time',
    chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);

ALTER TABLE nexus_ts.business_metrics_raw
    ADD CONSTRAINT bmr_idempotency_key
    UNIQUE (time, tenant_id, metric_name, cdm_entity_id);

ALTER TABLE nexus_ts.business_metrics_raw ENABLE ROW LEVEL SECURITY;
CREATE POLICY bmr_tenant_isolation ON nexus_ts.business_metrics_raw
    FOR ALL TO nexus_app
    USING (tenant_id = current_setting('nexus.current_tenant_id', TRUE));

CREATE INDEX IF NOT EXISTS bmr_tenant_metric_idx
    ON nexus_ts.business_metrics_raw (tenant_id, metric_name, time DESC);
CREATE INDEX IF NOT EXISTS bmr_dimensions_gin_idx
    ON nexus_ts.business_metrics_raw USING GIN (dimensions);
CREATE INDEX IF NOT EXISTS bmr_raw_payload_gin_idx
    ON nexus_ts.business_metrics_raw USING GIN (raw_payload)
    WHERE raw_payload IS NOT NULL;

ALTER TABLE nexus_ts.business_metrics_raw SET (
    timescaledb.compress,
    timescaledb.compress_orderby   = 'time DESC',
    timescaledb.compress_segmentby = 'tenant_id, metric_name'
);

SELECT add_retention_policy('nexus_ts.business_metrics_raw',
    INTERVAL '3 months', if_not_exists => TRUE);
SELECT add_compression_policy('nexus_ts.business_metrics_raw',
    INTERVAL '7 days', if_not_exists => TRUE);
```

---

## 8 · Edge Cases (updated)

| Scenario | Behaviour |
|---|---|
| **LLM submits recommendation for event_type already in registry** | Stored in `metric_recommendations` with `status = 'superseded'`. Existing registry entry unchanged. `RecommendationResult.skipped` incremented. |
| **LLM submits duplicate recommendation (same event_type + metric_name, different payload)** | Both stored. Second is marked `superseded` if registry entry exists. If neither is in registry yet, both are processed; highest confidence wins. |
| **multi_metric: companion field is null on a specific entity instance** | Companion row is skipped for that instance only. Other companion rows and the primary row are written normally. |
| **preserve_payload: raw_payload exceeds 10KB** | Write proceeds but `raw_payload` is truncated with a `"_truncated": true` marker added to the JSONB. Structured WARNING log. |
| **Recommendation confidence exactly at threshold boundary** | `>= auto_promote_threshold` is inclusive. A recommendation with `confidence = 0.90` auto-promotes when threshold is `0.90`. |
| **Human activates a metric with an override on `value_field`** | Writer uses the overridden `value_field` from that point forward. Prior rows in `business_metrics_raw` are not affected (immutable append). |
| **all_attributes: CDM entity contains nested objects** | Only scalar (str, int, float, bool) leaf values are extracted. Nested objects and arrays are excluded from `dimensions` unless explicitly mapped via `dimensions_map`. The full object is available in `raw_payload` if `preserve_payload` is also used. |
| **Event type not in registry** | Stage to `unresolved_events` (payload trimmed to 10KB); emit `level=WARNING` structured log; return without error. Never raises — Kafka consumer must not retry on registry misses. |
| **Backfill write into compressed chunk** | `ChunkCoordinator.ensure_chunk_writable()` decompresses before write; recompresses after. Decompress failure → chunk added to `BackfillResult.chunks_failed`; backfill continues. |
| **Correction with no prior row** | Treated as a new insert. Both Strategy A and B behave consistently. |
| **Continuous aggregate refresh job in error state** | `TimescaleAutoConfig` detects via catalog, triggers manual refresh for last 7 days, emits `CRITICAL` log if that also fails. Scheduled policy is not disabled. |

---

## 9 · Open Questions

| OQ | Status | Question | Impact |
|---|---|---|---|
| **OQ-V2-01** | ❌ Open | Does the CDM UPDATE event schema include `correction_context.old_value` / `correction_context.new_value`? If yes, correction Strategy A applies universally. If no, all corrections use Strategy B (DB lookup). | Blocks D2C-04 design finalisation. Confirm with Developer B. |
| **OQ-V2-02** | ❌ Open | Maximum parallel backfill workers the DB can support? Recommend `num_workers ≤ floor(max_connections × 0.5)`. Confirm `max_connections` with Platform team before defaulting to 4. | Backfill throughput ceiling |
| **OQ-V2-03** | ❌ Open | Cap `unresolved_events.payload_snapshot` at 10KB (lossy) or full? | Storage cost for ERP payloads |
| **OQ-V2-04** | ❌ Open | Should `metric_registry` be tenant-scoped (same event_type can map to different metrics per tenant) or global (current)? | Schema change; confirm with product |
| **OQ-V2-05** | ❌ Open | For `multi_metric` strategy, should companion metric rows share the same `cdm_entity_id` as the primary row? If yes, they participate in the same correction and tombstone lifecycle. If no, they are independent. | Correction and tombstone semantics for companion metrics |
| **OQ-V2-06** | ❌ Open | What are the appropriate default confidence thresholds (`TIMESCALE_AUTO_PROMOTE_THRESHOLD` and `TIMESCALE_REVIEW_THRESHOLD`)? Suggested: 0.90 / 0.60. Should they be per-tenant or global? | Risk tolerance for auto-promoted metrics |
| **OQ-V2-07** | ❌ Open | When the LLM re-analyses a CDM entity schema after a CDM version bump, should it submit updated recommendations for existing metrics (potentially changing `value_field` or `capture_strategy`)? The current design marks these as `superseded` and leaves the existing registry entry unchanged. A migration workflow for updating live metrics is not yet specified. | CDM version upgrade path |
| **OQ-D5-01** | ✅ Resolved | Soft tombstone (`is_deletion=TRUE`) for hot→cold. Hard partition drop deferred post-Iter-3 as opt-in via `WriterConfig.use_hard_partition_drop = False`. |
| **OQ-DM-08** | ✅ Resolved | `is_correction` and `is_deletion` confirmed present in V2.0.1 DDL. |
| **EVENT_TO_METRIC_MAP** | ✅ Resolved | Replaced by `metric_registry` table. Three confirmed entries seeded in V2.0.2. Unknown event types staged to `unresolved_events`. |

---

## 10 · Implementation Phases (updated)

### Phase 1 — Setup (Weeks 1–2)

**D2C-01 · Connection layer, FX, row builder, registry, auto-config**
- `asyncpg` pool from `TIMESCALEDB_DSN`
- `MetricRegistry` with DB load + LISTEN on `metric_registry_updated` + 30s poll
- `TimescaleAutoConfig.calibrate()` — startup only
- Migrations V2.0.2 and V2.0.3
- LISTEN on `metric_recommendation_received` channel
- `submit_recommendations()` entry point (acceptance test: submit one recommendation, verify it routes correctly)

### Phase 2 — Implementation (Weeks 3–6)

**D2C-02 · Historical backfill (progressive parallel)**
- FX cache pre-warm for full date range
- `ChunkCoordinator.partition_for_workers()`
- N parallel workers (chunk-aligned)
- Per-chunk: aggregate refresh + recompress
- No longer blocked by incomplete map — unknown event types staged to `unresolved_events`

**D2C-03 · Real-time insert** — REALTIME mode; respects `capture_strategy` per registry entry

**D2C-04 · Metric correction** — Strategy A (CDM delta) or B (DB lookup); confirm OQ-V2-01 first

**D2C-05 · Tombstone delete** — unchanged from v0.1; OQ-V2-05 must be resolved for `multi_metric` tombstone behaviour

**D2C-06 · Aggregate refresh** — via `asyncio.create_task()`; optional `view_name` parameter

**D2C-08 · TimescaleAutoConfig full calibration** — interval + compression lag adjustment; 24h background schedule

**D2C-09 (new) · Recommendation pipeline integration test**
- Submit a `MetricRecommendation` with confidence = 0.95 → verify auto-promotion and writer reloads within 30s
- Submit with confidence = 0.75 → verify queued for review, not active
- Call `activate_metric()` → verify NOTIFY fires and writer picks up
- Submit a `multi_metric` recommendation → verify companion rows written correctly

### Phase 3 — Integration (Weeks 7–9)

**D2C-07 · Throughput + compression validation**
- Parallel backfill throughput: ≥ 1,000 rows/sec per worker
- Chunk decompress/write/recompress round-trip
- Registry hot-reload: add entry → active within 30s
- Calibration: seed abnormal data density → `calibrate()` adjusts interval
- `capture_strategy = 'all_attributes'` correctness: all CDM scalar fields present in `dimensions`
- `capture_strategy = 'multi_metric'` correctness: companion rows written, same `cdm_entity_id`
- `capture_strategy = 'preserve_payload'` correctness: `raw_payload` column populated, GIN index used

---

## 11 · Configuration Reference

| Environment Variable | Default | Purpose |
|---|---|---|
| `TIMESCALEDB_DSN` | — | PostgreSQL connection string (required) |
| `TIMESCALE_POOL_MIN` | `2` | Minimum connections in asyncpg pool |
| `TIMESCALE_POOL_MAX` | `10` | Maximum connections |
| `TIMESCALE_REGISTRY_RELOAD_SECS` | `30` | Poll interval for registry hot-reload fallback |
| `TIMESCALE_AUTO_PROMOTE_THRESHOLD` | `0.90` | Confidence threshold for auto-promotion to registry |
| `TIMESCALE_REVIEW_THRESHOLD` | `0.60` | Confidence threshold for queued-review (below = candidate only) |
| `TIMESCALE_BACKFILL_WORKERS` | `4` | Default parallel workers for backfill engine |
| `TIMESCALE_BACKFILL_BATCH_SIZE` | `500` | Rows per INSERT batch within a chunk |
| `TIMESCALE_AUTO_DISCOVER_METRICS` | `false` | Enable LLM-style auto-discovery from `unresolved_events` |
| `TIMESCALE_PRESERVE_PAYLOAD_MAX_KB` | `10` | Max size of `raw_payload` before truncation |
| `TIMESCALE_CALIBRATION_INTERVAL_HRS` | `24` | How often `TimescaleAutoConfig.calibrate()` runs |

---

*NEXUS Iteration 2 · nexus-m3-writer · TimescaleDB Handler · v0.3 · Mentis Consulting · April 2026 · Confidential*
*Supersedes v0.1 and v0.2. All sections of v0.1 and v0.2 not explicitly modified remain in force.*
