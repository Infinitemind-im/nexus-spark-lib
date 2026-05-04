# NEXUS — Iteration 2 · `nexus-m3-writer` · TimescaleDB Handler
## Managing, Filling, and Updating the Time-Series Store

**Service:** `nexus-m3-writer` · **Module:** `nexus_m3_writer/stores/timescale_writer.py`
**Developer C task** · Supersedes v0.1
Mentis Consulting · Version 0.2 · April 2026 · Confidential

**Related docs:**
- `NEXUS-Iter2-SVC-nexus-m3-writer-timescaledb-v0.1.md` — baseline spec (structure preserved; gaps addressed here)
- `NEXUS-Iter2-SVC-nexus-m3-writer-stores-v0.1.md` — master service spec
- `NEXUS-Iter2-SPEC-M3-AIStores-v0.5.md` — architectural invariants
- `NEXUS-Iter2-SPEC-DataModel-v0.5.md` — schema DDL

---

## What Changed in v0.2

v0.1 established the immutable-append pattern, schema, and basic write paths. Three problems were left open:

1. **The EVENT_TO_METRIC_MAP is a hardcoded, incomplete Python dict.** Adding a new metric type requires a code deploy. Unknown events are silently dropped. This is not acceptable at scale.

2. **Backfill ignores chunk topology.** TimescaleDB physically partitions data into chunks by time. Writing to a compressed chunk raises an error. Writing multiple workers to the same chunk creates contention. v0.1 specifies batch inserts but says nothing about how chunks are managed during that process.

3. **The writer cannot tune itself.** Optimal chunk interval, compression lag, and aggregate refresh frequency depend on data volume — which varies by tenant and changes over time. v0.1 sets these statically at DDL time.

v0.2 addresses all three by introducing:

| Component | Solves |
|---|---|
| **Adaptive Metric Registry** | Replaces hardcoded map; hot-reloadable; auto-discovers candidates |
| **Chunk Coordinator** | Makes all write paths chunk-topology-aware; handles decompress/recompress |
| **Three-Mode Write Engine** | `REALTIME`, `BACKFILL`, `CORRECTION` — each optimised independently |
| **Progressive Parallel Backfill** | Workers aligned to chunk boundaries; no contention; chunks compressed as they complete |
| **TimescaleDB Auto-Config** | Startup + periodic calibration of chunk interval, compression lag, aggregate schedule |

Everything in v0.1 that is not explicitly superseded remains in force.

---

## Architecture Overview

```
Kafka consumer (Dev 5)
        │
        ▼
┌───────────────────────────────────────────────────┐
│              TimescaleWriter.write()              │
│                                                   │
│  ┌──────────────────────────────────────────┐     │
│  │          Metric Registry Cache           │     │
│  │  (in-memory dict, hot-reload every 30s)  │     │
│  └──────────────┬───────────────────────────┘     │
│                 │ lookup event_type                │
│         ┌───────┴────────┐                        │
│      known?           unknown?                    │
│         │                 │                       │
│         ▼                 ▼                       │
│    WriteMode          stage to                    │
│    selector         unresolved_events             │
│         │             + structured log            │
│   ┌─────┴──────┐                                  │
│   │            │                                  │
│ REALTIME  CORRECTION                              │
│   │            │                                  │
│   └─────┬──────┘                                  │
│         ▼                                        │
│  ┌──────────────────┐                            │
│  │ Chunk Coordinator│                            │
│  │ - topology query │                            │
│  │ - decompress if  │                            │
│  │   needed         │                            │
│  └──────┬───────────┘                            │
│         ▼                                        │
│  asyncpg pool → TimescaleDB                      │
└───────────────────────────────────────────────────┘

backfill() call (separate invocation)
        │
        ▼
┌───────────────────────────────────────────────────┐
│           Progressive Backfill Engine             │
│                                                   │
│  FX cache pre-warm → chunk map query              │
│       │                                           │
│       ├── Worker 0 → chunk [t0, t1)               │
│       ├── Worker 1 → chunk [t1, t2)               │
│       ├── Worker N → chunk [tN, tN+1)             │
│       │                                           │
│  Each worker on completion:                       │
│    1. refresh_aggregates_for_range(chunk)         │
│    2. recompress_chunk(chunk)                     │
│    3. emit platform_metrics row                   │
└───────────────────────────────────────────────────┘

Auto-Config (startup + every 24h)
        │
        ▼
┌───────────────────────────────────────────────────┐
│         TimescaleAutoConfig.calibrate()           │
│                                                   │
│  - measure data density per day                   │
│  - check chunk size health                        │
│  - check aggregate refresh lag                    │
│  - adjust chunk_time_interval if drifted          │
│  - recreate missing policies                      │
│  - emit calibration result to platform_metrics    │
└───────────────────────────────────────────────────┘
```

---

## 1 · Adaptive Metric Registry

### 1.1 Why

The hardcoded `EVENT_TO_METRIC_MAP` in v0.1 has three failure modes. First, the map is incomplete — the business analyst has not confirmed all event types, which blocks backfill entirely. Second, adding a new source system (a third CRM, an ERP migration) requires a code deploy and a service restart. Third, unknown events are silently dropped as no-ops, making the gap invisible until someone queries a missing metric and notices the zero.

### 1.2 New Database Tables

```sql
-- ─── metric_registry: replaces the hardcoded EVENT_TO_METRIC_MAP ─────────────
CREATE TABLE IF NOT EXISTS nexus_ts.metric_registry (
    registry_id       BIGSERIAL     PRIMARY KEY,
    event_type        VARCHAR(300)  NOT NULL UNIQUE,
    -- e.g. "salesforce.opportunity.won", "hubspot.deal.closed_won"
    metric_name       VARCHAR(200)  NOT NULL,
    value_field       VARCHAR(300)  NOT NULL,
    -- JSONPath into the CDM payload: "$.Amount", "$.LineItems[*].Total"
    value_transform   VARCHAR(50)   NOT NULL DEFAULT 'identity',
    -- 'identity'    → use the field value as-is
    -- 'negate'      → multiply by -1 (returns/refunds)
    -- 'constant_1'  → ignore field value, emit 1.0 (headcount events)
    -- 'sum_children'→ sum all leaf numeric values at value_field path
    dimensions_map    JSONB         NOT NULL DEFAULT '{}',
    -- keys = dimension names, values = CDM field paths
    -- e.g. {"region": "$.BillingCountry", "segment": "$.Account.Type"}
    gapfill_strategy  VARCHAR(20)   NOT NULL DEFAULT 'none',
    -- 'none' | 'locf' (last-observation-carried-forward) | 'interpolate'
    -- Applied by the query tier, not the writer — stored here as metadata
    is_active         BOOLEAN       NOT NULL DEFAULT TRUE,
    confidence        DECIMAL(3,2)  NOT NULL DEFAULT 1.00,
    -- 1.00 = human-confirmed; < 1.00 = auto-discovered candidate
    -- Writer only uses entries WHERE is_active AND confidence = 1.00
    registered_at     TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    registered_by     VARCHAR(100)  NOT NULL DEFAULT 'manual',
    -- 'manual' | 'auto-discovery' | 'import' | 'migration'
    notes             TEXT
);

-- Seed with the three confirmed mappings from v0.1
INSERT INTO nexus_ts.metric_registry (event_type, metric_name, value_field, registered_by)
VALUES
    ('salesforce.opportunity.won',      'revenue_booked',  '$.Amount',     'migration'),
    ('salesforce.order.created',        'order_value',     '$.TotalAmount','migration'),
    ('adventureworks.salesorderheader', 'revenue_booked',  '$.TotalDue',   'migration')
ON CONFLICT DO NOTHING;

-- ─── unresolved_events: holds unknown event types instead of silently dropping ─
CREATE TABLE IF NOT EXISTS nexus_ts.unresolved_events (
    id               BIGSERIAL    PRIMARY KEY,
    received_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    event_type       VARCHAR(300) NOT NULL,
    tenant_id        VARCHAR(100) NOT NULL,
    cdm_entity_id    VARCHAR(200),
    payload_snapshot JSONB        NOT NULL,
    -- Trimmed to 10KB max to prevent bloat from large CDM payloads
    resolution_state VARCHAR(50)  NOT NULL DEFAULT 'pending',
    -- 'pending' | 'registered' | 'ignored' | 'auto_candidate_created'
    resolved_at      TIMESTAMPTZ,
    resolved_by      VARCHAR(100)
);

CREATE INDEX IF NOT EXISTS ue_event_type_state_idx
    ON nexus_ts.unresolved_events (event_type, resolution_state, received_at DESC);

-- Auto-expire resolved rows after 30 days (no TimescaleDB hypertable needed)
-- Cleaned by the daily maintenance job in nexus-m3-writer.
```

### 1.3 Hot-Reload Mechanism

The writer maintains an in-memory `MetricRegistry` instance. It does **not** query the database on every `write()` call.

```python
class MetricRegistry:
    """
    Thread-safe in-memory cache of metric_registry rows.
    Refreshes from DB every REGISTRY_RELOAD_INTERVAL_SECS (default: 30).
    Also subscribes to PostgreSQL NOTIFY on channel 'metric_registry_updated'
    for immediate invalidation when a human adds a mapping via admin API.
    """

    async def lookup(self, event_type: str) -> MetricConfig | None:
        """Returns None for unknown event types (never raises)."""

    async def _reload_from_db(self) -> None:
        """SELECT * FROM nexus_ts.metric_registry WHERE is_active AND confidence = 1.00"""

    async def _listen_for_notify(self) -> None:
        """LISTEN metric_registry_updated — triggers immediate reload on INSERT/UPDATE"""
```

The NOTIFY trigger fires on `metric_registry` INSERT and UPDATE:

```sql
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

This means a new metric mapping added via the admin API propagates to all running writer instances within milliseconds — no deploy, no restart.

### 1.4 Auto-Discovery (Optional, Off by Default)

When `TIMESCALE_AUTO_DISCOVER_METRICS=true` (default false), the writer runs a discovery pass on events in `unresolved_events`:

1. For each distinct `event_type` with ≥ 50 occurrences in the last 24h, extract all numeric leaf fields from `payload_snapshot`.
2. Score candidates: a field named `Amount`, `Total`, `Value`, `Price`, `Revenue`, `Cost`, `Quantity` scores 0.9; others score 0.5.
3. Insert into `metric_registry` with `is_active = FALSE`, `confidence = <score>`, `registered_by = 'auto-discovery'`.
4. Emit a structured log entry and (if Slack MCP is connected) post to `#nexus-metric-alerts`.

Candidates are never used for writes until a human sets `is_active = TRUE` and `confidence = 1.00`. This purely surfaces the gap to the team.

---

## 2 · Chunk Coordinator

### 2.1 Why

TimescaleDB stores hypertable data in physical sub-tables called chunks, each covering a fixed time interval (1 day in the current DDL). The compression policy compresses chunks older than 7 days. Two operational realities follow from this:

- **Backfill into old data** = writing into compressed chunks. An `INSERT` into a compressed chunk raises `ERROR: insert into a compressed chunk is not supported`. The writer must decompress, write, then recompress.
- **Parallel backfill workers** must each own a disjoint set of chunks. If two workers share a chunk, they serialize on the chunk lock, eliminating the throughput benefit.

### 2.2 Interface

```python
@dataclass
class ChunkInfo:
    chunk_name:    str           # e.g. "_hyper_1_42_chunk"
    chunk_schema:  str           # e.g. "_timescaledb_internal"
    range_start:   datetime
    range_end:     datetime
    is_compressed: bool
    size_bytes:    int

class ChunkCoordinator:

    async def get_chunk_map(self,
        start: datetime | None = None,
        end:   datetime | None = None,
    ) -> list[ChunkInfo]:
        """
        Queries timescaledb_information.chunks for business_metrics_raw.
        Optionally filtered to [start, end). Sorted by range_start ascending.
        """

    async def ensure_chunk_writable(self, target_time: datetime, conn) -> bool:
        """
        If the chunk that owns target_time is compressed, decompresses it.
        Returns True if decompression was performed (caller must recompress after writing).
        No-op and returns False for uncompressed or non-existent chunks.
        """

    async def recompress_chunk(self, chunk: ChunkInfo, conn) -> None:
        """
        SELECT compress_chunk(chunk_schema || '.' || chunk_name).
        Only called if ensure_chunk_writable returned True.
        """

    async def partition_for_workers(self,
        start: datetime,
        end:   datetime,
        num_workers: int,
    ) -> list[list[ChunkInfo]]:
        """
        Retrieves chunk map for [start, end), then splits into num_workers
        groups with approximately equal total size_bytes. Each group is a
        contiguous range of chunks assigned to one backfill worker.
        Chunks within a group never overlap with another group.
        """
```

### 2.3 Catalog Queries

```sql
-- Full chunk map with compression status
SELECT
    chunk_name,
    chunk_schema,
    range_start,
    range_end,
    is_compressed,
    pg_total_relation_size(chunk_schema || '.' || chunk_name) AS size_bytes
FROM timescaledb_information.chunks
WHERE hypertable_name = 'business_metrics_raw'
  AND hypertable_schema = 'nexus_ts'
  AND (:start IS NULL OR range_end   > :start)
  AND (:end   IS NULL OR range_start < :end)
ORDER BY range_start;

-- Decompress before writing
SELECT decompress_chunk(:chunk_schema || '.' || :chunk_name, if_compressed => TRUE);

-- Recompress after writing
SELECT compress_chunk(:chunk_schema || '.' || :chunk_name, if_not_compressed => TRUE);
```

---

## 3 · Three-Mode Write Engine

Every write path goes through a `WriteMode` selector. The mode governs batch size, FX resolution, aggregate refresh timing, and whether the Chunk Coordinator is invoked pre-write.

```python
class WriteMode(str, Enum):
    REALTIME   = "realtime"    # Kafka consumer path — single row, lowest latency
    BACKFILL   = "backfill"    # Historical fill — batch, high throughput
    CORRECTION = "correction"  # Immutable-append correction — two rows, atomic

class WriteModeConfig:
    REALTIME   = dict(batch_size=1,   fx_ttl_hours=24, refresh_strategy="async",   decompress=True)
    BACKFILL   = dict(batch_size=500, fx_ttl_hours=0,  refresh_strategy="deferred",decompress=True)
    # fx_ttl_hours=0 means: for backfill, use historical rate at event timestamp, not today's cached rate
    CORRECTION = dict(batch_size=2,   fx_ttl_hours=24, refresh_strategy="async",   decompress=True)
```

### 3.1 REALTIME mode (Kafka consumer path)

Called by `write()` for every entity consumed from `{tid}.m1.entity_routed`.

```python
async def _write_realtime(self, entity: CDMEntity) -> None:
    config = self.registry.lookup(entity.event_type)
    if config is None:
        await self._stage_unresolved(entity)
        return

    row = await self._build_row(entity, config, use_historical_fx=False)
    chunk_was_compressed = await self.chunk_coordinator.ensure_chunk_writable(row.time, conn)
    try:
        await conn.execute(_INSERT_SQL, *row.as_params(), False)  # is_correction=FALSE
    finally:
        if chunk_was_compressed:
            await self.chunk_coordinator.recompress_chunk(chunk, conn)

    # Non-blocking aggregate refresh: fire and forget
    asyncio.create_task(
        self.refresh_aggregates_for_range(row.time, row.time + timedelta(days=1))
    )
```

> **Note on realtime decompression:** For current-time events the target chunk is never compressed (compression lag = 7 days). The `ensure_chunk_writable` call is a no-op in steady state. The cost is one catalog lookup (~0.1ms). It is kept in the hot path as a safeguard against misconfigured compression policies.

### 3.2 BACKFILL mode (Progressive Parallel Backfill Engine)

See Section 4. `backfill()` does not call `_write_realtime` — it uses the dedicated backfill engine with parallel chunk workers.

### 3.3 CORRECTION mode

The v0.1 correction pattern had a design issue: it derived the reversal row from the incoming CDM entity, which carries the *new* (post-correction) value — not the original stored value. A reversal based on the new value produces an incorrect running sum.

v0.2 fixes this with two sub-strategies, selectable per event source:

**Strategy A — CDM carries delta (preferred).** The CDM UPDATE event includes `old_value` and `new_value` in a `correction_context` field. The writer uses `old_value` for the reversal and `new_value` for the corrected row. No database lookup needed.

```python
async def _write_correction_from_delta(
    self, entity: CDMEntity, config: MetricConfig
) -> None:
    ctx        = entity.correction_context     # {old_value, new_value, old_currency}
    now        = datetime.utcnow()
    old_value  = await self.fx.normalise(ctx.old_value, ctx.old_currency,
                                          entity.tenant_id, entity.source_ts)
    new_value  = await self.fx.normalise(ctx.new_value, entity.currency,
                                          entity.tenant_id, now)

    reversal   = TimescaleRow(time=now, metric_value=-old_value, is_correction=True, ...)
    corrected  = TimescaleRow(time=now, metric_value=+new_value, is_correction=True, ...)

    async with conn.transaction():
        await conn.executemany(_INSERT_SQL, [reversal.as_params(), corrected.as_params()])

    asyncio.create_task(
        self.refresh_aggregates_for_range(entity.source_ts, now)
    )
```

**Strategy B — DB lookup (fallback).** When `correction_context` is absent (older source systems), the writer queries the most recent non-correction row for `(tenant_id, metric_name, cdm_entity_id)` to derive the original value.

```sql
SELECT metric_value, base_currency
FROM nexus_ts.business_metrics_raw
WHERE tenant_id       = :tenant_id
  AND metric_name     = :metric_name
  AND cdm_entity_id   = :cdm_entity_id
  AND is_correction   = FALSE
  AND is_deletion     = FALSE
ORDER BY time DESC
LIMIT 1;
```

This query hits the compressed index efficiently (`bmr_tenant_metric_idx`). If no row is found, the correction is treated as a new insert.

> **[CLARIFY: OQ-V2-01]** Does the CDM schema for UPDATE events include `correction_context.old_value`? If yes, Strategy A is universally available. If not, Strategy B must be used for all sources — confirm with Developer B (CDM schema owner).

---

## 4 · Progressive Parallel Backfill

### 4.1 Design Principles

Three properties of TimescaleDB drive the backfill design:

1. **Physical chunk isolation.** Chunks are separate heap files. Concurrent writes to different chunks do not contend on the same locks. N workers writing to N disjoint chunks achieves near-linear throughput scaling up to the connection pool limit.

2. **Chunk-aligned compression.** Compression operates per chunk. Re-compressing a chunk after writing is a single SQL call (`compress_chunk`) and does not affect other chunks. This lets us compress completed historical chunks immediately, reclaiming storage while other workers are still writing.

3. **Continuous aggregate refresh is range-scoped.** `CALL refresh_continuous_aggregate(view, start, end)` re-materializes only the time buckets intersecting `[start, end)`. Refreshing per completed chunk keeps aggregates current incrementally, rather than a single blocking refresh at the end.

### 4.2 Backfill Engine Interface

```python
@dataclass
class BackfillResult:
    tenant_id:       str
    total_rows:      int
    inserted_rows:   int        # ON CONFLICT DO NOTHING — actual new rows
    skipped_rows:    int        # already present
    elapsed_secs:    float
    rows_per_sec:    float
    chunks_filled:   int
    chunks_failed:   list[str]  # chunk names that errored — safe to retry
    aggregate_views_refreshed: list[str]

class BackfillEngine:

    async def run(self,
        tenant_id:    str,
        start:        datetime,
        end:          datetime,
        num_workers:  int = 4,          # tunable; default 4
        batch_size:   int = 500,        # rows per INSERT batch within a chunk
    ) -> BackfillResult:
        ...
```

### 4.3 Execution Flow

```
Step 1 — Pre-warm FX cache
─────────────────────────
For each month in [start, end):
    For each (from_currency, to_currency) pair seen in EVENT_TO_METRIC_MAP:
        Fetch ECB rate for (currency_pair, month_first_day) → Redis cache
        Use TTL = 30 days for historical rates (they never change)
Goal: zero FX cache misses during the backfill hot path.

Step 2 — Build chunk partition map
──────────────────────────────────
chunks = await chunk_coordinator.get_chunk_map(start, end)
# chunks is a list of ChunkInfo sorted by range_start
# Any gaps (no chunk exists yet) are created automatically by TimescaleDB
# on first INSERT into that time range — no pre-creation needed.

worker_assignments = await chunk_coordinator.partition_for_workers(
    start, end, num_workers
)
# worker_assignments[i] = list of ChunkInfo for worker i
# Partitioned by total size_bytes for balanced load

Step 3 — Launch parallel workers
──────────────────────────────────
async with asyncio.TaskGroup() as tg:
    for i, chunk_list in enumerate(worker_assignments):
        tg.create_task(_backfill_worker(i, chunk_list, tenant_id, batch_size))

Step 4 — Per-worker execution (each chunk in the assigned list)
──────────────────────────────────────────────────────────────
for chunk in chunk_list:
    was_compressed = await chunk_coordinator.ensure_chunk_writable(chunk)

    # Fetch CDM events for this chunk's time range from source
    events = await cdm_source.fetch(tenant_id, chunk.range_start, chunk.range_end)

    # Build rows, resolve FX (all from pre-warmed cache — no network calls)
    rows = [await _build_row(e, registry.lookup(e.event_type), use_historical_fx=True)
            for e in events if registry.lookup(e.event_type) is not None]

    # Batch insert in chunks of batch_size
    for batch in _batches(rows, batch_size):
        await conn.executemany(_INSERT_SQL_NO_CONFLICT, batch)

    # Refresh aggregates for this chunk's range (blocks this worker only)
    await refresh_aggregates_for_range(chunk.range_start, chunk.range_end)

    # Re-compress now that this chunk is complete
    if was_compressed:
        await chunk_coordinator.recompress_chunk(chunk, conn)

    # Emit platform_metrics row
    await _emit_chunk_completion_metric(chunk, inserted_count, elapsed)

Step 5 — Final aggregate pass
──────────────────────────────────
# Tier 1 and 2 aggregates may have range_start/end offsets that
# exclude the most recent data. Run a full-range refresh to ensure
# no bucket is left stale after backfill.
for view in AGGREGATE_VIEWS:
    await refresh_aggregates_for_range(start, end, view_name=view)
```

### 4.4 Resumability

Each chunk is an independently committable unit. If a worker fails mid-chunk, the partially inserted rows are idempotent (`ON CONFLICT DO NOTHING` on the unique key). Restarting backfill with the same parameters re-runs all chunks; already-present rows are simply skipped. The `skipped_rows` counter in `BackfillResult` confirms this.

For very large tenants (> 5 years of history), the caller can invoke `backfill()` per quarter rather than for the entire range. The engine is fully composable with any date subdivision.

---

## 5 · TimescaleDB Auto-Config

### 5.1 Why

The DDL in v0.1 sets `chunk_time_interval = '1 day'` and `add_compression_policy(INTERVAL '7 days')`. These values are reasonable defaults but will become suboptimal as:

- Tenant data volume grows (small chunks = large number of file handles)
- Data arrives out of order (affects aggregate refresh `end_offset`)
- Retention requirements change per tenant (multi-tenant deployments)

Auto-Config solves this without manual DBA intervention.

### 5.2 Calibration Logic

```python
class TimescaleAutoConfig:
    """
    Runs at service startup and every 24 hours via asyncio background task.
    Results emitted to nexus_ts.platform_metrics for observability.
    Never performs destructive operations — only adjusts policies.
    """

    async def calibrate(self) -> CalibrationResult:

        # ── 1. Measure data density ──────────────────────────────────────────
        # Rows per day, averaged over last 30 days
        rows_per_day = await self._measure_daily_density()

        # ── 2. Evaluate chunk size health ────────────────────────────────────
        chunk_stats = await self._get_chunk_stats()
        # Target: chunk size between 100MB and 1GB (TimescaleDB recommendation)
        # If avg chunk < 50MB → increase chunk_time_interval
        # If avg chunk > 2GB  → decrease chunk_time_interval

        recommended_interval = self._recommend_chunk_interval(
            rows_per_day, avg_row_size_bytes=150  # measured from information_schema
        )
        current_interval = await self._get_current_chunk_interval()

        if abs((recommended_interval - current_interval).total_seconds()) > 3600:
            # More than 1 hour difference — apply change
            await self._set_chunk_interval(recommended_interval)
            log.info("chunk_interval adjusted",
                     old=current_interval, new=recommended_interval)

        # ── 3. Evaluate compression lag ──────────────────────────────────────
        # Target: compress after data is at least 2× the chunk interval old
        # (ensures no active inserts hit compressed chunks in normal operation)
        recommended_compression_lag = max(recommended_interval * 2, timedelta(days=7))
        await self._ensure_compression_policy(recommended_compression_lag)

        # ── 4. Evaluate aggregate refresh lag ────────────────────────────────
        job_stats = await self._get_aggregate_job_stats()
        for view_name, stats in job_stats.items():
            if stats.last_run_status == "Error":
                log.error("continuous_aggregate refresh failed",
                          view=view_name, error=stats.last_run_message)
                # Attempt immediate manual refresh for last 7 days
                await self.refresh_aggregates_for_range(
                    datetime.utcnow() - timedelta(days=7), datetime.utcnow(),
                    view_name=view_name
                )
            lag = stats.next_start - datetime.utcnow()
            if lag > stats.expected_interval * 2:
                log.warning("aggregate refresh overdue", view=view_name, lag=lag)

        # ── 5. Ensure all policies exist ─────────────────────────────────────
        # If a policy was accidentally dropped, recreate it.
        await self._ensure_retention_policy('nexus_ts.business_metrics_raw',
                                             INTERVAL '3 months')
        await self._ensure_retention_policy('nexus_ts.metrics_weekly',
                                             INTERVAL '12 months')
        await self._ensure_retention_policy('nexus_ts.metrics_monthly',
                                             INTERVAL '6 years')

        return CalibrationResult(
            rows_per_day=rows_per_day,
            chunk_interval_old=current_interval,
            chunk_interval_new=recommended_interval,
            compression_lag=recommended_compression_lag,
            policies_recreated=self._recreated_policies,
            timestamp=datetime.utcnow()
        )
```

### 5.3 Catalog Queries Used by Auto-Config

```sql
-- Daily row density
SELECT
    date_trunc('day', time) AS day,
    COUNT(*)                AS row_count
FROM nexus_ts.business_metrics_raw
WHERE time > NOW() - INTERVAL '30 days'
GROUP BY 1
ORDER BY 1 DESC;

-- Chunk size distribution
SELECT
    chunk_name,
    range_start,
    range_end,
    is_compressed,
    pg_size_pretty(pg_total_relation_size(chunk_schema || '.' || chunk_name)) AS human_size,
    pg_total_relation_size(chunk_schema || '.' || chunk_name)                 AS size_bytes
FROM timescaledb_information.chunks
WHERE hypertable_name   = 'business_metrics_raw'
  AND hypertable_schema = 'nexus_ts'
ORDER BY range_start DESC
LIMIT 60;

-- Current chunk interval
SELECT chunk_time_interval
FROM timescaledb_information.dimensions
WHERE hypertable_name = 'business_metrics_raw';

-- Continuous aggregate job status
SELECT
    j.application_name,
    js.last_run_started_at,
    js.last_run_status,
    js.last_run_duration,
    js.last_run_num_chunks_processed,
    js.next_start
FROM timescaledb_information.jobs j
JOIN timescaledb_information.job_stats js ON j.job_id = js.job_id
WHERE j.application_name LIKE 'Refresh Continuous%';

-- Check existing retention and compression policies
SELECT * FROM timescaledb_information.jobs
WHERE application_name IN ('Retention Policy', 'Compression Policy');
```

---

## 6 · Updated Interface

The public interface of `TimescaleWriter` expands slightly from v0.1:

```python
class TimescaleWriter:

    # ── Lifecycle ──────────────────────────────────────────────────────────────

    async def start(self) -> None:
        """
        Called by Dev 5's service shell on startup.
        1. Opens asyncpg connection pool
        2. Loads MetricRegistry from DB
        3. Starts registry hot-reload background task (LISTEN + 30s poll)
        4. Runs TimescaleAutoConfig.calibrate() once
        5. Schedules calibrate() to run every 24h
        """

    async def stop(self) -> None:
        """Graceful shutdown: drain in-flight tasks, close pool."""

    # ── Write paths ────────────────────────────────────────────────────────────

    async def write(self, entity: CDMEntity) -> None:
        """
        Routes to REALTIME insert or CORRECTION depending on entity state.
        Entity with version=1 or no prior row → _write_realtime()
        Entity with version>1 and correction_context present → _write_correction_from_delta()
        Entity with version>1 and no correction_context → _write_correction_db_lookup()
        """

    async def delete(self, entity: CDMEntity) -> None:
        """Tombstone: append is_deletion=TRUE row. Never UPDATE or DELETE."""

    async def health_check(self) -> StoreHealth:
        """TimescaleDB connectivity + registry load status + last calibration time."""

    # ── Backfill ───────────────────────────────────────────────────────────────

    async def backfill(self,
        tenant_id:   str,
        start:       datetime | None = None,  # defaults to earliest CDM event
        end:         datetime | None = None,  # defaults to NOW()
        num_workers: int = 4,
        batch_size:  int = 500,
    ) -> BackfillResult:
        """Progressive parallel backfill via BackfillEngine. Thread-safe."""

    # ── Aggregate management ───────────────────────────────────────────────────

    async def refresh_aggregates_for_range(self,
        start_dt:   datetime,
        end_dt:     datetime,
        view_name:  str | None = None,        # None = refresh all three tiers
    ) -> None:
        """Always run in a background task — never block the Kafka consumer path."""

    # ── Registry management ────────────────────────────────────────────────────

    async def get_registry_stats(self) -> RegistryStats:
        """Returns: total registered event types, unknown event count (last 24h)."""

    async def flush_unresolved_events(self,
        event_type:   str,
        new_state:    Literal["registered", "ignored"],
    ) -> int:
        """
        Called by admin API after a human adds a mapping to metric_registry.
        Updates resolution_state for all pending unresolved_events of this type.
        Returns count of updated rows.
        """
```

---

## 7 · Updated Data Model

New tables added in migration `V2.0.2`:

| Table | Purpose |
|---|---|
| `nexus_ts.metric_registry` | Replaces `EVENT_TO_METRIC_MAP`; hot-reloadable; auto-discovery candidates |
| `nexus_ts.unresolved_events` | Staging area for events with no registry entry; never silently dropped |

No changes to `business_metrics_raw` schema. The `is_correction` and `is_deletion` columns specified in v0.1 are confirmed present in `V2.0.1`.

Migration DDL:

```sql
-- V2.0.2 — Adaptive Metric Registry + Unresolved Events Staging

CREATE TABLE IF NOT EXISTS nexus_ts.metric_registry ( ... );   -- full DDL in Section 1.2
CREATE TABLE IF NOT EXISTS nexus_ts.unresolved_events ( ... );  -- full DDL in Section 1.2

-- Seed with v0.1 confirmed mappings
INSERT INTO nexus_ts.metric_registry ... ON CONFLICT DO NOTHING;

-- Notify trigger for hot-reload
CREATE OR REPLACE FUNCTION nexus_ts.notify_registry_change() ...;
CREATE TRIGGER metric_registry_notify_trigger ...;

-- Grant to nexus_app role
GRANT SELECT, INSERT, UPDATE ON nexus_ts.metric_registry    TO nexus_app;
GRANT SELECT, INSERT, UPDATE ON nexus_ts.unresolved_events  TO nexus_app;
```

---

## 8 · Resolved Open Questions

| OQ | Resolution |
|---|---|
| **OQ-D5-01** | **Resolved — soft tombstone in v0.2.** `is_deletion=TRUE` row appended for hot→cold. Hard partition drop deferred to post-Iter-3 as an opt-in via `WriterConfig.use_hard_partition_drop` (default `False`). |
| **EVENT_TO_METRIC_MAP** | **Resolved structurally.** The hardcoded map is replaced by `nexus_ts.metric_registry` (Section 1). The three confirmed entries are seeded in V2.0.2. Unknown types go to `unresolved_events` — not dropped. The business analyst can add mappings via admin API without a deploy. Backfill is no longer blocked on having a *complete* map; it processes whatever entries are confirmed and stages the rest. |
| **OQ-DM-08** | **Resolved.** `is_deletion` and `is_correction` columns are present in V2.0.1 DDL. |

---

## 9 · Open Questions

| OQ | Status | Question | Impact |
|---|---|---|---|
| **OQ-V2-01** | ❌ Open | Does the CDM UPDATE event schema include `correction_context.old_value` / `correction_context.new_value`? If yes, Strategy A (Section 3.3) applies universally. If no, all corrections use Strategy B (DB lookup). | Blocks D2C-04 design finalisation. Confirm with Developer B. |
| **OQ-V2-02** | ❌ Open | What is the maximum number of parallel backfill workers the database can support? Recommendation: `num_workers ≤ floor(max_connections * 0.5)`. Confirm `max_connections` with Platform team before setting the default of 4. | Backfill throughput ceiling |
| **OQ-V2-03** | ❌ Open | Should `unresolved_events.payload_snapshot` be capped at 10KB (lossy) or stored in full (could be large for ERP payloads)? | Storage cost for unresolved events |
| **OQ-V2-04** | ❌ Open | Should the `metric_registry` be tenant-scoped (different tenants can map the same event type to different metrics) or global? Current design is global — a `salesforce.opportunity.won` always maps to `revenue_booked`. Multi-tenant remapping would require adding `tenant_id` to the registry PK. | Schema change; confirm with product |
| **OQ-DM-01** | ❌ Open | Separate PostgreSQL database vs. schema within `nexus_system` for `nexus_ts`? | Infrastructure decision |
| **OQ-DM-05** | ❌ Open | Promote common dimensions (region, currency, source) from JSONB to dedicated columns? High-cardinality JSONB slows the query planner on `business_metrics_raw` joins. Consider for Iter-3 if query latency becomes a concern. | Performance |

---

## 10 · Edge Cases

| Scenario | Behaviour |
|---|---|
| **Event type not in registry** | Stage to `unresolved_events`; emit `level=WARNING` structured log with `event_type`, `tenant_id`, `cdm_entity_id`; return without error. Never raise — Kafka consumer must not retry on registry misses. |
| **FX rate unavailable for historical date** | Log `level=WARNING`; use most recent available rate for that currency pair; set `dimensions.fx_rate_estimated = true`. Insert proceeds — data integrity is preserved with a flag. |
| **Backfill write into compressed chunk** | `ChunkCoordinator.ensure_chunk_writable()` decompresses before write; recompresses after. If decompression fails (e.g. insufficient disk space), the chunk is skipped; the chunk name is added to `BackfillResult.chunks_failed` and backfill continues with the next chunk. |
| **Duplicate backfill invocation** | `ON CONFLICT DO NOTHING` on the unique key `(time, tenant_id, metric_name, cdm_entity_id)`. Idempotent. `skipped_rows` counter reports the collision count. |
| **Correction with no prior row** | Strategy A: insert as new row (no reversal). Strategy B: query returns no row → insert as new row. Both strategies behave consistently. |
| **Tombstone for already-tombstoned entity** | `ON CONFLICT DO NOTHING` on the unique key. The second tombstone call is a no-op. |
| **CalibrationResult recommends smaller chunk interval** | Auto-Config adjusts the interval for new chunks only — existing chunk sizes are never retroactively changed by TimescaleDB. This is correct behaviour: historic chunks remain at their original interval. |
| **Continuous aggregate refresh job in error state** | Auto-Config detects via `timescaledb_information.job_stats.last_run_status = 'Error'`, logs the error, and immediately triggers a manual refresh for the last 7 days. If the manual refresh also fails, emits `level=CRITICAL` log. Does not disable the scheduled policy. |
| **NOTIFY listener disconnected** | `MetricRegistry` falls back to the 30-second polling interval. The registry may be stale for up to 30 seconds after a new mapping is added. This is acceptable — the cost is staging a few events to `unresolved_events` during that window. |
| **`metric_value` is NULL** | Allowed for count-type metrics where `value_transform = 'constant_1'`. The row builder emits `metric_value = 1.0` regardless of the CDM field value. For all other transforms, a NULL `metric_value` in the CDM payload causes the row to be staged to `unresolved_events` with reason `null_metric_value`. |

---

## 11 · Implementation Phases (Updated)

All v0.1 task codes are preserved. New sub-tasks added.

### Phase 1 — Setup (Weeks 1–2)

**D2C-01 · Connection layer, FX, row builder** — unchanged from v0.1, plus:
- Implement `MetricRegistry` class with DB load + NOTIFY listener
- Implement `TimescaleAutoConfig.calibrate()` — startup calibration only (no interval adjustment yet)
- Create `nexus_ts.metric_registry` and `nexus_ts.unresolved_events` (migration V2.0.2)

### Phase 2 — Implementation (Weeks 3–6)

**D2C-02 · Historical backfill** — now implemented as `BackfillEngine` with:
- FX pre-warm
- `ChunkCoordinator.partition_for_workers()`
- Parallel chunk workers (Section 4.3)
- Per-chunk: refresh + recompress
- **No longer blocked by incomplete map** — stages unknown event types to `unresolved_events`

**D2C-03 · Real-time insert** — uses `_write_realtime()`; unknown types → `_stage_unresolved()`

**D2C-04 · Metric correction** — implements both Strategy A and Strategy B; selection based on presence of `correction_context` in entity. Confirm with Developer B (OQ-V2-01).

**D2C-05 · Tombstone delete** — unchanged from v0.1

**D2C-06 · Aggregate refresh** — `refresh_aggregates_for_range()` now accepts optional `view_name` parameter; runs via `asyncio.create_task()`

**D2C-08 (new) · TimescaleAutoConfig full calibration** — enable interval + compression lag adjustment; add 24h background schedule

### Phase 3 — Integration (Weeks 7–9)

**D2C-07 · Throughput + compression validation** — now includes:
- Parallel backfill throughput test (target: ≥ 1,000 rows/sec per worker)
- Chunk decompress/write/recompress round-trip test
- Registry hot-reload test (add entry to DB; confirm write picks it up within 30s)
- Calibration test: seed abnormal data density; confirm `calibrate()` adjusts interval
- `unresolved_events` test: send unknown event type; confirm staging + log output; add to registry; confirm `flush_unresolved_events()` updates state

---

*NEXUS Iteration 2 · nexus-m3-writer · TimescaleDB Handler · v0.2 · Mentis Consulting · April 2026 · Confidential*
*Supersedes v0.1. All sections of v0.1 not explicitly modified remain in force.*
