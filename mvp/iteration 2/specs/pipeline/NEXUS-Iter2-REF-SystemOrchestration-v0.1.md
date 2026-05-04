# Iteration 2 — System-Level Pipeline Orchestration

**Version:** v0.1 (draft)
**Date:** 2026-04-27
**Companion to:** `iter2-cdm-to-aistores-pipeline-v0.1.md`, `iter2-record-lifecycle-structured-walkthrough-v0.1.md`
**Scope:** Structured path. The operational mechanics behind the per-record lifecycle: the Spark jobs that actually run, how materialization levels are assigned and revisited, the Golden Record state machine, and the per-store routing matrix.

---

## 1. Overview

The pipeline spec defined the stages. The walkthrough traced one record through them. This document describes the **system as it runs**: the long-lived Spark applications, the scheduled DAGs, the classification heuristics, the state machine that governs Golden Record evolution, and the deterministic rules that decide which of the four stores (PostgreSQL canonical, Pinecone, Neo4j, TimescaleDB) each record lands in.

Three operational facts frame everything below.

The pipeline runs in **two regimes simultaneously**: a streaming regime that processes live source mutations within seconds, and a batch regime that handles initial loads, backfills, re-classifications, and periodic maintenance. Both regimes run on the same underlying Spark cluster but as separate applications with different lifecycles.

The **materialization level is per-entity-type per-tenant**, not per-record. All `Party` records in tenant Acme share a single classification. The system's job is to keep that classification right as usage and source data evolve, not to make per-record decisions.

The **Golden Record is the stable identity, not a content snapshot**. A Golden Record is created exactly once, and afterward only its provenance grows, shrinks, or is unwound by an explicit split. The state machine is small and the transitions are explicit.

---

## 2. The Spark Pipeline — What Actually Runs

### 2.1 Spark applications

Three independent Spark applications run in the cluster. Operational independence matters: a backfill job stalling cannot block live ingestion, and an ER reindex cannot starve the streaming path.

| Application | Type | Lifecycle | Owner |
|---|---|---|---|
| `spark-stream-transformer` | Structured Streaming | Long-lived `Deployment` | Data Intelligence |
| `spark-batch-jobs` | Spark on Kubernetes (per-job pods) | Triggered by Airflow | Data Intelligence |
| `spark-maintenance` | Spark on Kubernetes (cron-like) | Triggered by Airflow | Platform |

`spark-stream-transformer` consumes `m1.int.raw_records` directly from Kafka. It runs continuously, micro-batching every 30 seconds (configurable per tenant in `nexus_system.tenant_configs.spark_stream_trigger_seconds`). Its job graph is the per-record streaming pipeline: normalise, resolve, synthesise, publish `transformed_records`. Restart-safe by virtue of Kafka offset commits and Delta Lake idempotent writes.

`spark-batch-jobs` is a family of Airflow-triggered jobs. Each is a discrete Spark application launched per run, sized to its workload, terminated when complete. The catalogue is in §2.2.

`spark-maintenance` runs short, low-frequency jobs that don't fit either of the other lanes — statistics refresh, dead vector tombstone purge, continuous-aggregate gap detection.

### 2.2 The batch job catalogue

These are the operationally important Airflow DAGs that cooperate with the streaming path. Each is owned by `spark-batch-jobs` unless noted.

| Job | Cadence | Purpose | Trigger inputs | Outputs |
|---|---|---|---|---|
| `initial-load` | On-demand | Bulk ingestion of historical data when a connector is first registered | `nexus.connector.registered` event + admin trigger | Delta Lake raw + same downstream chain as streaming |
| `er-reindex` | Weekly + on threshold change | Re-evaluate all existing pairs in an entity type after threshold tuning or after large new corpus arrivals | `nexus.er.thresholds_changed` event or scheduled | Updates `entity_resolution_index`; emits `nexus.er.reindex_completed` |
| `survivorship-rebuild` | On survivorship rule change | Recompute `golden_record_provenance` for affected `(entity_type, attribute)` | `nexus.survivorship_rules_changed` | Provenance rewrites; emits `m1.entity_routed` for affected GRs (re-projects to stores) |
| `materialization-promotion-backfill` | On promotion event | Replay Delta Lake records for a newly-promoted entity type through the full pipeline | `nexus.materialization.changed` (warm→hot) | Same as streaming output |
| `materialization-demotion-cleanup` | Nightly + on demotion | Remove now-orphaned vectors and Neo4j elements for demoted entity types | Schedule + `nexus.materialization.changed` (hot→warm) | Pinecone tombstones; Neo4j `DETACH DELETE`; TimescaleDB rows retained until natural expiry |
| `m3-reconciliation` | Nightly | Detect records present in Delta Lake but missing from one or more stores | Schedule | Replays missing records via `entity_routed` |
| `cdm-version-migration` | On CDM version publish | Migrate provenance and re-embed when canonical attributes are renamed/split/merged | `nexus.cdm.version_published` | Provenance rewrites + Pinecone re-embeds |
| `materialization-recommend` | Daily | Compute the recommended materialization level per entity type from observed signals; stage proposals for governance | Schedule | Rows in `nexus_system.materialization_recommendations` |
| `entity-stats-refresh` | Daily | Recompute per-entity-type statistics used by ER thresholds and materialization heuristics | Schedule | Updates `nexus_system.entity_stats` |

### 2.3 Streaming pipeline as a Spark job graph

```
                  Kafka source
            (m1.int.raw_records)
                       │
                       ▼
       ┌───────────────────────────────┐
       │ readStream → micro-batch (30s)│
       └───────────────────────────────┘
                       │
                       ▼
       ┌───────────────────────────────┐
       │ join: cdm_entity_materialization (broadcast)│
       │ join: cdm_mappings (broadcast)              │
       │ join: tenants.base_currency (broadcast)     │
       └───────────────────────────────┘
                       │
              early filter: drop COLD
                       │
                       ▼
       ┌───────────────────────────────┐
       │ Stage 1 — normalise           │
       │  - type coercion              │
       │  - timestamp canonicalisation │
       │  - FX normalisation           │
       │  - blocking key computation   │
       └───────────────────────────────┘
                       │
                       ▼
       ┌───────────────────────────────┐
       │ Stage 2 — resolve             │
       │  Signal A: deterministic match│
       │  (LSH bucket join)            │
       │  Signal B: probabilistic      │
       │  Signal C: graph lift (REST)  │
       │  → assign cdm_entity_id       │
       └───────────────────────────────┘
                       │
                       ▼
       ┌───────────────────────────────┐
       │ Stage 3 — synthesise          │
       │  load survivorship rules      │
       │  compute per-attribute winner │
       │  upsert provenance rows       │
       │  compute provenance_hash      │
       └───────────────────────────────┘
                       │
                       ▼
       Kafka sink
       (m1.int.transformed_records)
```

The broadcast joins are over small, slow-changing reference tables — Spark caches them and refreshes on a schedule (every 5 minutes) or on a `nexus.cdm.version_published` event. The ER stages use Spark's stateful operations (`mapGroupsWithState`) keyed on `blocking_key` to maintain in-memory candidate buckets across micro-batches, with state expiry after 24 hours of inactivity.

### 2.4 Why streaming vs batch matters

A live CDC event takes the streaming path with end-to-end latency dominated by the 30-second micro-batch (per the walkthrough). A 50-million-row historical Salesforce backfill would saturate the streaming path and starve other tenants. It takes the `initial-load` batch path, which provisions a large Spark application sized to the workload, runs to completion, and tears down. Both paths share Stages 1–3 logic via a shared library (`nexus_spark_lib.transform`); only the ingestion source and the resource sizing differ.

---

## 3. Materialization Classification — How a Level Is Decided

### 3.1 What the level governs

`materialization_level ∈ {hot, warm, cold}` is set per `(tenant_id, cdm_entity_type)` in `nexus_system.cdm_entity_materialization`. It governs three things at once:

- **Streaming pipeline depth.** Hot entities run all three ER signals and full synthesis. Warm entities run Signal A only and skip synthesis. Cold entities are filtered out before Stage 1.
- **M3 projection.** Hot records project to all applicable stores. Warm records do not. Cold records do not.
- **Query path.** The Smart Query Engine treats hot entities as locally materialised, warm entities as Delta-Lake-only, and cold entities as source-live (deterministic-only resolution check applied at response time).

### 3.2 Initial assignment

When a new CDM entity type is approved by M4 governance, `nexus-discovery` proposes an initial level using a scoring function. The score is a sum over weighted heuristics; the bands are tenant-tunable but default as below.

| Heuristic | How measured | Weight | Notes |
|---|---|---|---|
| **Estimated row count** | Sum across all contributing sources from Discovery's statistical metadata | 25% | Log-scaled; below 100K rows → 1.0, above 100M → 0.0 |
| **Cross-source presence** | Count of distinct source systems mapping to this entity type | 25% | 1 source → 0.3; 2 sources → 0.7; 3+ → 1.0 |
| **Join centrality** | Count of relationship mappings in the CDM where this entity participates | 20% | Normalised; entities at the centre of the graph score high |
| **Text-heavy attributes** | Fraction of canonical attributes with `data_type = text` and `length > 64` | 10% | High-text entities benefit more from Pinecone |
| **Time-series potential** | Boolean: does the entity have a canonical timestamp + a numeric attribute or event nature? | 10% | Drives TimescaleDB value |
| **Tenant bias** | Tenant-configured prior in `tenant_configs.materialization_bias` | 10% | Allows "default to hot" for power tenants |

Score → level mapping (default thresholds, configurable per tenant):

| Score | Level |
|---|---|
| ≥ 0.70 | hot |
| 0.30 – 0.69 | warm |
| < 0.30 | cold |

The proposal is written to `nexus_system.materialization_recommendations` with `assigned_by = 'auto'`. A Tenant Admin can override before approval. The default policy for newly-published CDM entities is to **hold at `warm`** for a 7-day "warm-up" window even if the score crosses the hot threshold, to gather actual usage data before committing to full materialisation. Tenant Admins can lift the warm-up window per entity type. (See OQ-MAT-01 in parent spec — recommend keeping this default.)

### 3.3 Re-classification: signals and triggers

Once an entity type is in operation, four signals feed the daily `materialization-recommend` job:

- **Query frequency.** `nexus-query-executor` increments a per-(tenant, entity_type) counter on every query that touches the entity. The counter is windowed: `query_count_30d` rolls up into `cdm_entity_materialization` daily. High counts pull toward hot.
- **Source mutation rate.** `nexus-m1-worker` reports rows-extracted-per-day per entity type to `entity_stats`. High mutation rate combined with high query rate strengthens the hot case (stale data is queried frequently).
- **Cost signal.** Pinecone storage and embedding-call cost per entity type. If an entity is hot but has very low query frequency, the job recommends demotion.
- **Manual override staleness.** If a Tenant Admin froze a level manually, the job leaves it alone but flags it for periodic re-review at 90 days.

The job writes proposals to `materialization_recommendations`. Promotions of low impact (warm→hot for entities with < 1000 rows) auto-apply. Promotions of high impact (warm→hot for entities with > 10M rows, or any cold→hot direct jump) queue for Tenant Admin approval — they trigger a backfill job whose cost is non-trivial.

Default thresholds (configurable):

| Signal observed over 30 days | Direction | Action |
|---|---|---|
| ≥ 25 queries on a `warm` entity | warm → hot | Auto-apply if rows < 1M, else queue |
| ≥ 5 queries on a `cold` entity | cold → warm | Auto-apply |
| 0 queries on a `hot` entity for 90 days | hot → warm | Queue for review |
| 0 queries on a `warm` entity for 180 days | warm → cold | Auto-apply (frees Delta Lake older data per retention) |

### 3.4 Materialization state machine

```
              ┌────────────────────────────────────────┐
              │              cold                      │
              │  - cataloged in CDM only               │
              │  - not in Delta Lake                   │
              │  - retrieved on-demand by query layer  │
              └────────────────────────────────────────┘
                  │  ▲                   ▲
   first query    │  │ 0 queries 180d    │
   (≥5 / 30d)     │  │                   │
                  ▼  │                   │
              ┌────────────────────────────────────────┐    Tenant Admin
              │              warm                      │◀── manual override
              │  - cataloged                           │
              │  - in Delta Lake                       │
              │  - Signal A ER only                    │
              │  - no M3 projection                    │
              └────────────────────────────────────────┘
                  │  ▲
   ≥25 queries   │  │ 0 queries 90d
   on warm       │  │ (queue for review)
                  ▼  │
              ┌────────────────────────────────────────┐
              │              hot                       │
              │  - full ER (3 signals)                 │
              │  - synthesis + provenance              │
              │  - projection to applicable stores     │
              └────────────────────────────────────────┘

  Manual override: any → any, with backfill DAG triggered by upgrades.
```

Transitions emit `nexus.materialization.changed` carrying `(tenant_id, cdm_entity_type, old_level, new_level, triggered_by)`. Consumers: `m1-worker` (routing), `m3-writer` (cleanup on demotions), Spark broadcast cache (refresh), governance dashboards.

---

## 4. Entity Resolution at System Scale

### 4.1 Two regimes of ER

**Streaming ER** runs inside `spark-stream-transformer` Stage 2, per record, with stateful LSH buckets in Spark memory and a Neo4j REST call for Signal C lift. Latency is the dominant constraint; throughput is bounded by micro-batch size and the LSH bucket fan-out. This is what the walkthrough described.

**Batch ER** runs in the `er-reindex` job. It loads the full set of records of an entity type (or a partition by blocking key) and re-evaluates pairwise. Used when:

- Probabilistic match thresholds are tuned (existing matches may now be wrong)
- A new contributing source is connected (records from that source need to be matched against the existing Golden Record corpus)
- A large historical backfill completes (the deferred resolution from streaming is reconciled)
- The graph signal becomes more informative as the surrounding graph grows (records previously left in the review band may now be auto-resolvable)

Batch ER produces *delta* outcomes: new matches to apply, existing matches to undo, review-band items to surface or dismiss. The deltas are published as `entity_routed` events with explicit `operation = 'remerge' | 'unmerge'` so downstream stores process them with the same idempotent path as live mutations.

### 4.2 The Signal C feedback loop

Signal C is graph-based — it lifts a probabilistic match by finding shared neighbours in Neo4j. This creates a circular dependency: the graph that informs ER is itself written by the pipeline whose ER decisions shape it. Two safeguards prevent thrash.

**Asymmetric trust.** Signal C only *lifts* a score; it cannot *push down* a score below the auto-apply threshold. A bad graph link cannot prevent a good deterministic match.

**Confidence-weighted edges.** Every edge in Neo4j carries a `confidence` property derived from the underlying ER decisions. Signal C's lift is weighted by the confidence of the traversed edges. Low-confidence edges (under 0.85) contribute proportionally less to the lift. This bounds amplification of false positives.

### 4.3 The ER index as a denormalised lookup

`nexus_system.entity_resolution_index` is the hot-path lookup. It maps `(tenant_id, source_system, source_table, source_record_id) → cdm_entity_id`. Every downstream consumer that needs to know "what Golden Record does this source row resolve to?" reads from here, never from the provenance table.

The index is updated by:

- Spark Stage 2 (streaming, per record)
- `er-reindex` (batch, in bulk)
- Manual M4 actions (split/merge by data steward)

Reads are heavy (embedded join in many queries). Postgres index on `(tenant_id, source_system, source_table, source_record_id)` plus a covering index on `cdm_entity_id` for reverse lookups.

---

## 5. Golden Record State Machine

A Golden Record is a row in `nexus_system.golden_records_index` plus its provenance rows in `golden_record_provenance`. It has a small set of states and a small set of well-defined transitions. Every transition is reproducible from the event log.

### 5.1 States

| State | Meaning |
|---|---|
| `active` | The Golden Record represents a real entity; queryable everywhere |
| `provisional` | An ER review-band match created the record provisionally; pending steward decision |
| `superseded` | The record was merged into another Golden Record; resolves to the surviving ID via redirect |
| `tombstoned` | All contributing source records have been deleted; the record retains its ID for audit but is excluded from queries |

### 5.2 Transitions

```
                     CREATE
                       │
                       ▼
  ER review band ──▶ provisional ──── steward approve ───▶ active
                       │                                       │
                       │                                       │ ER finds it's the same
                       └── steward reject ──▶ tombstoned       │ as another GR (rare,
                                                               │ usually after a new
                                                               │ source connects)
                                                               ▼
                                                         (active GR_1)
                                                              │
                                                              ▼ MERGE
                                                  GR_1 absorbs GR_2:
                                                    GR_2 → superseded
                                                    GR_2.cdm_entity_id
                                                      redirects to GR_1

                  ▲ steward triggers SPLIT
                  │
            (active GR with bad merge)
                  │
                  ▼
            two new active GRs;
            old GR → tombstoned;
            DOC_MENTIONS edges reroute to mention review queue

  active ──── all source records deleted ───▶ tombstoned
```

### 5.3 Transition mechanics

**CREATE.** Stage 2 ER returns no match. Spark generates `cdm_entity_id` per the formula in the parent spec FR-M-05 (`gr:` + sha256 of `tenant_id || cdm_entity_type || canonical_blocking_key`). One row in `golden_records_index` (`state = 'active'`). N rows in `golden_record_provenance` (one per attribute the contributing record provided).

**UPDATE.** A new contributing record resolves to an existing `cdm_entity_id`. No new row in `golden_records_index`; only provenance changes. Survivorship rules decide whether the new record's values take any attribute slots (insert provenance rows) or yield to existing ones (no-op). The walkthrough showed this case.

**MERGE.** Two `active` Golden Records are determined to be the same entity. This is rare in streaming and common in batch ER (especially after a new source is connected). Mechanism: pick the surviving ID by deterministic rule (lower `created_at`, ties broken by lexical `cdm_entity_id`); update the loser's row to `state = 'superseded'`; insert a row in `nexus_system.golden_record_redirects` mapping `superseded_id → surviving_id`; rewrite all `entity_resolution_index` rows pointing at the loser to point at the survivor; emit `entity_routed` for both records (the survivor with `operation = 'remerge'`, the loser with `operation = 'supersede'`). M3 writers handle these explicitly: Pinecone tombstones the superseded vector; Neo4j detaches the superseded node and rewrites edges to the survivor; TimescaleDB rewrites `cdm_entity_id` on existing rows via a small migration.

**SPLIT.** Triggered manually from M4. A steward identifies that GR_X actually represents two entities — A and B — that should never have been merged. The steward indicates which contributing source records belong to A and which to B. The system creates a new Golden Record for B (B keeps the smaller cohort by convention to minimise downstream churn), reassigns provenance rows accordingly, marks the old GR_X as tombstoned (its ID is preserved for audit; redirects route incoming traffic to A as the "primary continuant"), and re-emits `entity_routed` for both. `DOC_MENTIONS` edges that pointed at GR_X are reattached to the new mention review queue with a special `source_change = 'split'` flag so the steward can re-evaluate per mention.

**TOMBSTONE.** All source records contributing to the Golden Record have been deleted. The record's `state` becomes `tombstoned`. Provenance rows are retained (audit). Pinecone vector tombstones. Neo4j node `DETACH DELETE`s. TimescaleDB rows get `is_deletion=TRUE`.

### 5.4 The redirect table

```sql
CREATE TABLE nexus_system.golden_record_redirects (
  superseded_cdm_entity_id  VARCHAR(48) PRIMARY KEY,
  surviving_cdm_entity_id   VARCHAR(48) NOT NULL,
  tenant_id                 UUID NOT NULL,
  reason                    VARCHAR(32) NOT NULL CHECK (reason IN ('merge','split_continuant')),
  redirected_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  redirected_by             VARCHAR(64) NOT NULL  -- 'spark_batch_er' | steward user_id
);
CREATE INDEX idx_grr_surviving ON nexus_system.golden_record_redirects(surviving_cdm_entity_id);
```

Read path: `nexus-query-executor` always resolves `cdm_entity_id` through this table before fetching from stores. A query referencing a stale `cdm_entity_id` (e.g. cached in a saved dashboard) transparently lands on the surviving record.

---

## 6. Per-Store Routing — Decision Matrix

Not every record goes to every store. The routing is governed by per-entity-type configuration in `nexus_system.cdm_entity_storage_config` (NEW table).

### 6.1 The configuration table

```sql
CREATE TABLE nexus_system.cdm_entity_storage_config (
  tenant_id           UUID NOT NULL,
  cdm_entity_type     VARCHAR(128) NOT NULL,
  embeddable          BOOLEAN NOT NULL DEFAULT FALSE,
  graph_persistent    BOOLEAN NOT NULL DEFAULT TRUE,
  metricable          BOOLEAN NOT NULL DEFAULT FALSE,
  metric_value_attr   VARCHAR(128),                    -- which attribute is the numeric metric
  metric_time_attr    VARCHAR(128),                    -- which attribute is the timestamp
  embed_attrs         VARCHAR(128)[] NOT NULL DEFAULT '{}',  -- which attributes feed embedding text
  PRIMARY KEY (tenant_id, cdm_entity_type)
);
```

Defaults are heuristic-driven at CDM publish time:

| Entity nature | embeddable | graph_persistent | metricable |
|---|---|---|---|
| Party (Customer, Vendor, Person) | ✓ | ✓ | ✗ |
| Product / SKU | ✓ | ✓ | ✗ |
| Transaction (Order, Invoice) | ✗ | ✓ | ✓ |
| Event (Login, Click, Action) | ✗ | ✓ | ✓ |
| Location | ✗ | ✓ | ✗ |
| Document | ✓ (via doc track) | ✓ | ✗ |
| Reference data (currency, status code) | ✗ | ✗ | ✗ |

`embed_attrs` defaults are inferred from canonical attributes of type `text` with length > 64. Example: for `Party`, `embed_attrs = {legal_name, industry, address}`. PII fields are excluded by `agent_core.PIIChecker` regardless of `embed_attrs`.

### 6.2 The routing decision tree

For each `entity_routed` event, `nexus-m3-writer` evaluates:

```
                 entity_routed received
                          │
                          ▼
               operation == 'remove' ?
                  yes ┌────┴────┐ no
                      ▼         ▼
              tombstone all   continue
              applicable
              stores
                                ▼
              load cdm_entity_storage_config(tenant, entity_type)
                                │
                                ▼
              ┌──────────────┬──────────────┬────────────────┐
              ▼              ▼              ▼                ▼
        embeddable?    graph_persistent?  metricable?    always
          yes/no          yes/no            yes/no
              │              │              │                │
              ▼              ▼              ▼                ▼
         Pinecone       Neo4j            TimescaleDB    PostgreSQL
         upsert         MERGE            INSERT         provenance
         (or skip       (or skip         (or skip       refresh
         if false)      if false)        if false)      (always)
              └──────────────┴──────────────┴────────────────┘
                                │
                                ▼
                        emit write_completed
                        with stores_written = [...]
                        and skipped_stores = [...]
```

`PostgreSQL canonical store` (provenance + golden_records_index) is always written for hot records. The provenance is the spine; the three AI stores are derived projections.

### 6.3 Examples

| Entity type | embeddable | graph | metricable | Stores written for one record |
|---|---|---|---|---|
| `Party` (Globex, the walkthrough case) | ✓ | ✓ | ✗ | PG + Pinecone + Neo4j |
| `Transaction.SalesOrder` | ✗ | ✓ | ✓ | PG + Neo4j + TimescaleDB |
| `Event.Login` | ✗ | ✗ | ✓ | PG + TimescaleDB |
| `Reference.Currency` | ✗ | ✗ | ✗ | PG provenance only |
| `Document.Contract` | ✓ | ✓ | ✗ | PG + Pinecone (chunks) + Neo4j (`Document` node + edges) |

### 6.4 Why a record skips a store

A record skipping a store at write time is a routine event, not a failure:

- A `Reference.Currency` record skips Pinecone, Neo4j, and TimescaleDB because it has no semantic content, no relationships worth materialising, and no time series.
- A `Transaction.SalesOrder` skips Pinecone because order numbers and amounts don't benefit from semantic search.
- An `Event.Login` skips Pinecone and Neo4j (the event itself is uninteresting as a graph node; what matters are the aggregate metrics).

The `skipped_stores` array on `write_completed` makes this explicit so observability can distinguish "skipped by config" from "failed to write".

### 6.5 What if storage config changes mid-flight?

Changing `embeddable` from false to true for an existing entity type is an event triggering `m3-reconciliation` to backfill embeddings for all records of that type from Delta Lake. Changing it from true to false triggers `materialization-demotion-cleanup`-style logic scoped to the store: all Pinecone vectors for that entity type are tombstoned. The change is applied by the same `nexus.materialization.changed` event family (renamed `nexus.storage_config.changed` for clarity).

---

## 7. Operational Concerns

### 7.1 Scaling

`spark-stream-transformer` scales by Spark Structured Streaming's executor allocation. Per-tenant resource isolation is by Kafka partition assignment; tenants with high volume get more partitions on `m1.raw_records`, and Spark naturally picks up parallelism from there.

`spark-batch-jobs` scales per-job. Each Airflow DAG declares the resources its Spark application requests (driver memory, executor count, executor memory); the cluster autoscales the underlying nodes.

`nexus-m3-writer` is KEDA-scaled per the existing spec (lag threshold 50 messages on `entity_routed`, min 1 max 6). The 30-second Spark micro-batch produces bursty load; the lag-based scaling handles bursts.

### 7.2 Observability

Each Spark application emits per-stage metrics to Grafana via the cluster's Prometheus exporter:

- `spark_stream_micro_batch_duration_seconds` per tenant per stage
- `spark_stream_records_processed_total` per tenant
- `spark_er_signal_outcomes_total{signal,outcome}` (signal ∈ {A,B,C}, outcome ∈ {match,no_match,skip})
- `spark_golden_record_transitions_total{transition}` (transition ∈ {create,update,merge,split,tombstone})
- `spark_batch_job_duration_seconds{job_name}`

The materialization recommendations job emits `nexus_materialization_proposals_pending_total{tenant,direction}` so unattended drift is visible to operators.

### 7.3 Failure modes specific to the system view

- **Spark micro-batch keeps failing on a poison record.** Spark's bad-record handler routes the offending record to a dead-letter Delta Lake location (`s3://nexus-deltalake/dlq/`) and continues the batch. A daily DAG `dlq-triage` surfaces these in M4.
- **ER reindex blocks streaming.** It can't, by design — they run in separate Spark applications on separate executor pools. A misconfigured reindex over-provisioned can starve cluster autoscaling for the streaming app, mitigated by a per-tenant cluster quota.
- **Backfill from materialization promotion is too large to complete in window.** The promotion DAG checkpoints progress; if the DAG fails partway, the replay resumes from the last checkpoint on next run. Until the backfill completes, the entity type is in a `promoting` substate and queries route to the source via the cold/warm fallback. The substate is recorded in `cdm_entity_materialization.transition_status`.

---

## 8. Open Questions

- **OQ-SYS-01.** Should `spark-stream-transformer` use a single application per cluster (all tenants share executors) or one application per tenant? Single is simpler and cheaper; per-tenant isolates blast radius. Recommend single for Iter 2, evaluate per-tenant for high-tier customers in Iter 3.
- **OQ-SYS-02.** The materialization recommendation thresholds (5 / 25 queries) are guesses. Need a benchmark on at least one pilot tenant before fixing defaults. Track override rates as a calibration signal (parent spec FR-S-02).
- **OQ-SYS-03.** The split operation in §5.3 is a one-shot reassign. Should it preserve a "split history" so the operation is reversible? Recommend yes — add `golden_record_split_history` table in v0.2.
- **OQ-SYS-04.** Storage config (§6.1) defaults are heuristic — should newly-published entity types default to "all stores" (then prune) or "none" (then enable)? Recommend prune from heuristic defaults; safer for cost.
- **OQ-SYS-05.** The redirect table in §5.4 grows monotonically. Compaction / pruning policy after long histories? Recommend retain forever (it's small relative to data) — flag for review at scale.

---

## 9. References

- `iter2-cdm-to-aistores-pipeline-v0.1.md` — parent spec; this document operationalises §3.0–§3.4.
- `iter2-record-lifecycle-structured-walkthrough-v0.1.md` — per-record trace; this document covers the system that makes that trace possible at scale.
- `NEXUS-Iter2-SPEC-ServiceTopology-v0.4.md` — Spark and Airflow service definitions.
- `NEXUS-Iter2-SPEC-DataModel-v0.5.md` — base for the new tables proposed here.
- C.1.2.md §5 (ETL) and §5 (Entity Resolution) — source of the materialization, ER, and survivorship concepts.
