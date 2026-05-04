# Iteration 2 — Database Backfill Pipeline

**Version:** v0.1 (draft)
**Date:** 2026-04-27
**Companion to:** `iter2-dev-overview-and-registers-v0.1.md`

---

## 📦 Codebase & Deployment

> **One repository, many deployments.** All NEXUS services live in the `nexus-platform` monorepo. You are contributing to a shared codebase — not an isolated repository. Shared libraries (`nexus_core` v2, `agent_core` v1, `nexus_spark_lib`) live in `libs/` and are imported across all services. Never duplicate logic that already exists there.

| | |
|---|---|
| **Deployed as** | Airflow DAGs (no standalone service process) |
| **Monorepo paths** | `libs/nexus_spark_lib/` · `dags/spark-batch-jobs/` |
| **Language / runtime** | Python 3.11 · PySpark 3.5 · Apache Airflow |
| **Iteration 2 owner** | Dev 2 (4 person-weeks) |
| **Key output** | `nexus_spark_lib` shared library consumed by `nexus-spark-transformer` (streaming) and all batch DAGs |

> ⚠️ **Note:** Batch Backfill does **not** deploy a long-running service. It ships a library (`nexus_spark_lib`) and a set of Airflow DAG definitions. The library is the primary deliverable — CDC Streaming and all batch jobs call into it.

---

## 1. Scope

The Batch Backfill component owns everything that ingests historical or replayed data in bulk, in contrast to CDC Streaming's live CDC stream. Concretely:

- The `nexus_spark_lib` shared library — the canonical implementation of Stages 1, 2, 3 of the pipeline that both CDC Streaming (streaming) and Batch Backfill (batch) call. Owning this library is what makes the two ingestion regimes structurally identical from Entity Resolution's perspective downstream.
- The `spark-batch-jobs` Airflow DAG family: `initial-load`, `m3-reconciliation`, `cdm-version-migration`, `materialization-promotion-backfill`, `materialization-demotion-cleanup` (the data-replay portion; M3 Writers owns the cleanup writes), and `er-reindex`.
- The connector handover protocol with CDC Streaming — the mechanism that ensures a backfilled connector and its CDC stream do not double-process the same source rows.
- Resumability / checkpointing for all batch jobs.
- Per-job sizing and Spark application lifecycle (launch, monitor, terminate).

This component does **not** own: connector configuration (CDC Streaming owns Debezium / Airbyte), the ER algorithm itself (Entity Resolution — Batch Backfill just calls the library), policy evaluation (Materialization Coordinator), or M3 writes (M3 Writers).

---

## 2. Dependencies

| Depends on | What for | When needed |
|---|---|---|
| Platform team (M5) | Spark on Kubernetes operator, Airflow, S3-compatible storage for Delta Lake | Week 0 |
| CDC Streaming | Connector cluster running, CDC topics defined, connector registration flow stable | Week 1 |
| Entity Resolution | Stage 2 + 3 algorithms (consumed via the shared library that The Batch Backfill component owns; Batch Backfill vendors Entity Resolution's code) | Week 1 |
| Materialization Coordinator | `cdm_entity_materialization`, `materialization_policy` schemas frozen | Week 0–1 |
| M3 Writers | None inbound; M3 Writers consumes Batch Backfill's output via Entity Resolution/Materialization Coordinator | n/a |

---

## 3. Functional Requirements (MoSCoW)

### 3.1 Must

- **FR-Dev 2-M-01.** Provide `nexus_spark_lib` as a Python wheel + Scala JAR with a stable API. Versioned. Used by D1's streaming application and Dev 2's batch jobs. The library exposes:
  - `nexus_spark_lib.transform.normalise(df, cdm_mapping_broadcast, fx_broadcast) -> df`
  - `nexus_spark_lib.transform.resolve(df, er_index_broadcast, neo4j_client) -> df` (calls Entity Resolution implementation internally)
  - `nexus_spark_lib.transform.synthesise(df, survivorship_broadcast) -> df` (calls Entity Resolution)
  - `nexus_spark_lib.transform.materialization_decide(df, policy_broadcast) -> df` (calls Materialization Coordinator)
  - `nexus_spark_lib.kafka.write_transformed_records(df, topic_pattern)`
- **FR-Dev 2-M-02.** Implement and operate the `initial-load` Airflow DAG, triggered by `nexus.connector.registered` events plus an admin trigger. Steps:
  1. Read `connector_backfill_handover.source_position_at_start` (recorded by the connector before the backfill begins).
  2. Read the source corpus via the existing connector worker (read-only).
  3. Write raw records to Delta Lake with `source_op = SNAPSHOT_READ`.
  4. Publish Kafka events on `m1.int.raw_records` (same shape as CDC Streaming's output) so the same downstream chain processes them.
  5. On completion, update `connector_backfill_handover.backfill_completed_at` and emit `nexus.backfill.batch_completed`. CDC Streaming then enables CDC starting from `source_position_at_start`.
- **FR-Dev 2-M-03.** Implement and operate `m3-reconciliation` (nightly DAG). Detects records in `entity_store_presence` where any applicable store flag is FALSE despite the entity being at `hot` materialization level, and replays them by re-emitting `entity_routed` events. This catches partial-write failures from D5 that did not auto-recover.
- **FR-Dev 2-M-04.** Implement and operate `cdm-version-migration` (triggered by `nexus.cdm.version_published`). Reads the version diff (renames, splits, merges of canonical attributes), rewrites `golden_record_provenance` rows for affected attributes, and re-emits `entity_routed` events for affected `cdm_entity_id`s so D5 re-projects them with the new schema. Re-embedding in Elasticsearch happens via D5; Batch Backfill's job is the orchestration.
- **FR-Dev 2-M-05.** Implement and operate `materialization-promotion-backfill` (triggered by `nexus.materialization.changed` warm→hot or cold→hot). Reads Delta Lake records for the affected cohort and re-emits them on `m1.raw_records` with `source_op = RELEVEL` so the streaming chain picks them up. For cold→hot, also coordinates with D1 to trigger a fresh extraction (cold records are not in Delta Lake).
- **FR-Dev 2-M-06.** Implement and operate `er-reindex` (weekly + on threshold change). Reads all records of an entity type from Delta Lake, runs full ER (including Signal C against the current Neo4j state), produces match deltas, and emits `entity_routed` events with `operation ∈ {UPSERT, MERGE, SUPERSEDE, REMERGE}` for affected GRs.
- **FR-Dev 2-M-07.** All batch jobs are **resumable**. Each job persists checkpoint metadata to `nexus_system.batch_job_checkpoints` (The Batch Backfill component owns) every N records (default 10K). On restart, the job resumes from the last checkpoint without reprocessing earlier records.
- **FR-Dev 2-M-08.** All batch jobs are **idempotent**. Re-running a completed job (or a partially completed job) produces no incremental side effects beyond what an interrupted previous run would have produced. Implementation: idempotent writes downstream + checkpoint-driven resume + deterministic record IDs.
- **FR-Dev 2-M-09.** Every batch job carries a `backfill_batch_id` (UUID generated at job start) on every Kafka message it emits. Downstream consumers (D3 specifically) use this to enable batch-mode behaviours and to scope acknowledgements.
- **FR-Dev 2-M-10.** The `connector_backfill_handover` protocol: when an `initial-load` starts, Dev 2 records the source position (LSN for Postgres, SCN for Oracle, version for SaaS APIs) and sets `status='running'`. When complete, sets `status='completed'`. D1 watches this table and starts CDC consumption only after `status='completed'`. Status transitions are emitted as Kafka events on `nexus.backfill.handover_changed` for observability.
- **FR-Dev 2-M-11.** Pre-flight check before launching `initial-load`: estimate the source corpus size from connector statistics; refuse to launch if estimated cost (Spark hours × executor count) exceeds the tenant's monthly cost budget. Surface as a recommendation that requires admin approval.

### 3.2 Should

- **FR-Dev 2-S-01.** Per-job parallelism tuning based on source row count: small corpus (< 100K) gets 4 executors, medium (100K–10M) gets 16, large (10M+) gets 64. Configurable per tenant.
- **FR-Dev 2-S-02.** Backfill scoping by date range — operator can request "backfill orders from 2023-01-01 onward, ignore older." Reduces cost when older data is genuinely useless.
- **FR-Dev 2-S-03.** A `dry-run` mode that runs the full job through Stages 1–3 but does not emit Kafka events. Used to estimate ER impact before committing.

### 3.3 Could

- **FR-Dev 2-C-01.** Adaptive batch sizing — micro-batches of varying size based on observed Spark application throughput.
- **FR-Dev 2-C-02.** Per-job cost estimation surfaced in the M4 admin UI as the job runs ("backfill is 47% complete; estimated remaining cost: $12.40").

### 3.4 Won't

- **FR-Dev 2-W-01.** Batch Backfill will not write to source systems. Read-only is a hard rule.
- **FR-Dev 2-W-02.** Batch Backfill will not have a separate Stages 1–3 implementation. The shared library is the only implementation; if a divergence is needed, it goes into the library.

---

## 4. Non-Functional Requirements

| ID | Requirement | Target |
|---|---|---|
| NFR-D2-01 | `initial-load` throughput | ≥ 50,000 records/second per Spark application on Iter 2 sizing |
| NFR-D2-02 | `m3-reconciliation` runtime | ≤ 4 hours nightly for a tenant with 100M records across all entity types |
| NFR-D2-03 | Batch job restart latency after crash | ≤ 5 minutes; Spark application relaunches and resumes from last checkpoint |
| NFR-D2-04 | Idempotency under double-execution | Running the same job twice produces the same final state byte-for-byte |
| NFR-D2-05 | Connector handover correctness | Zero double-processing of source rows in 100 simulated handover scenarios |
| NFR-D2-06 | Backfill cost predictability | Pre-flight estimate within ±15% of actual cost on observed runs |

---

## 5. Data Model Ownership

```sql
CREATE TABLE nexus_system.batch_job_checkpoints (
  job_id            UUID PRIMARY KEY,
  job_kind          VARCHAR(64) NOT NULL,           -- 'initial_load' | 'm3_reconciliation' | etc.
  tenant_id         UUID NOT NULL,
  scope             JSONB NOT NULL,                 -- e.g. {"connector_id": "c-sf-001"} or {"entity_type": "Party"}
  started_at        TIMESTAMPTZ NOT NULL,
  last_checkpoint_at TIMESTAMPTZ NOT NULL,
  records_processed BIGINT NOT NULL DEFAULT 0,
  records_total_estimate BIGINT,
  resume_token      JSONB,                          -- job-specific resumption state
  status            VARCHAR(16) NOT NULL CHECK (status IN ('running','succeeded','failed','aborted')),
  failure_reason    TEXT,
  ended_at          TIMESTAMPTZ
);

CREATE TABLE nexus_system.backfill_cost_log (
  job_id            UUID PRIMARY KEY,
  tenant_id         UUID NOT NULL,
  estimated_cost_usd NUMERIC(10,2) NOT NULL,
  actual_cost_usd   NUMERIC(10,2),
  spark_hours_actual NUMERIC(8,2),
  records_actual    BIGINT,
  recorded_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

`connector_backfill_handover` is co-owned with CDC Streaming — Batch Backfill inserts and updates it, CDC Streaming reads it. **Authoritative DDL (single source of truth):**

```sql
-- Owned by: Batch Backfill (writes). CDC Streaming reads to determine the LSN/offset at which to start CDC.
CREATE TABLE nexus_system.connector_backfill_handover (
  connector_id              VARCHAR(64) PRIMARY KEY,
  tenant_id                 UUID NOT NULL,
  backfill_started_at       TIMESTAMPTZ NOT NULL,
  source_position_at_start  VARCHAR(255) NOT NULL,    -- LSN / SCN / cursor / etag at backfill start
  backfill_completed_at     TIMESTAMPTZ,
  cdc_started_at            TIMESTAMPTZ,
  status                    VARCHAR(32) NOT NULL        -- 'running' | 'completed' | 'cdc_handover_done'
);
```

---

## 6. API / Kafka Contracts

### 6.1 Inbound (consumed)

- `nexus.connector.registered` — triggers `initial-load` DAG.
- `nexus.cdm.version_published` — triggers `cdm-version-migration` DAG.
- `nexus.materialization.changed` — triggers `materialization-promotion-backfill` (warm→hot or cold→hot directions).
- `nexus.er.thresholds_changed` — triggers `er-reindex` DAG.
- Operator-initiated triggers via Airflow API.

### 6.2 Outbound (produced)

- `m1.int.raw_records` — same topic CDC Streaming produces; Batch Backfill's events carry `backfill_batch_id` header. Schema identical to CDC Streaming.
- `nexus.backfill.batch_started` — bracket open; payload includes `job_id`, `job_kind`, `scope`, `estimated_records`.
- `nexus.backfill.batch_completed` — bracket close; payload includes `job_id`, `records_processed`, `duration`, `cost_actual_usd`.
- `nexus.backfill.handover_changed` — when handover state transitions (`running` → `completed` → `cdc_handover_done`).
- `nexus.batch_job.failed` — per-job failure event for alerting.

### 6.3 The shared library API

`nexus_spark_lib` is published to a private package registry as both Python wheel (for PySpark) and Scala JAR (for Spark applications written in Scala). The API surface is small and deliberately stable:

```python
# nexus_spark_lib/transform.py
def normalise(df: DataFrame,
              cdm_mapping: Broadcast[CdmMapping],
              fx_rates: Broadcast[FxRates]) -> DataFrame: ...

def resolve(df: DataFrame,
            er_index: Broadcast[ErIndexSnapshot],
            neo4j_session: Neo4jSession,
            mode: str = 'streaming') -> DataFrame:  # mode='streaming'|'batch'
    """Stage 2: 3-signal entity resolution. Implementation provided by Entity Resolution."""
    ...

def synthesise(df: DataFrame,
               survivorship: Broadcast[SurvivorshipRules]) -> DataFrame:
    """Stage 3: Golden Record synthesis. Implementation provided by Entity Resolution."""
    ...

def materialization_decide(df: DataFrame,
                           policy: Broadcast[MaterializationPolicy]) -> DataFrame:
    """Stage 0: policy evaluation. Implementation provided by Materialization Coordinator."""
    ...

def write_transformed_records(df: DataFrame, topic_pattern: str) -> StreamingQuery: ...
```

The library is versioned by SemVer. CDC Streaming and Batch Backfill pin a specific version per release; bumps are coordinated.

---

## 7. CRUD Handling — Batch Backfill's Slice

Backfills are predominantly "INSERT-shaped" — a `SNAPSHOT_READ` op for every source row. But several specific cases require Batch Backfill to handle other CRUD shapes.

**`SNAPSHOT_READ` (the dominant case).** For each source row read during backfill, Batch Backfill emits `m1.raw_records` with `source_op = SNAPSHOT_READ`. Entity Resolution treats this as `INSERT` for resolution, but with batch-mode optimisations (larger LSH bucket cache, deferred Signal C until the batch completes). The handover mechanism prevents CDC Streaming from processing the same row again.

**`RELEVEL` for promotion replay.** When Materialization Coordinator promotes a cohort, Batch Backfill's `materialization-promotion-backfill` reads Delta Lake records for the cohort and re-emits them with `source_op = RELEVEL`. Entity Resolution short-circuits ER (the resolution is already known and stored in `entity_resolution_index`); only Stages 0 and 4 effectively run. The point is to push the record through the projection pipeline so M3 Writers can populate the stores.

**`UPDATE` and `DELETE` during a long backfill.** A live source can mutate during a multi-hour backfill. The handover protocol means CDC starts only after backfill ends, so during the backfill, mutations accumulate in the Debezium connector's offset. When backfill completes, CDC is enabled and consumes the accumulated events. The order matters: a row that was `INSERT`ed at backfill time, then `UPDATE`d during the backfill, will be processed twice — once as `SNAPSHOT_READ` with the original value, once as `UPDATE` with the new value. Entity Resolution's idempotent UPSERT pattern means the final state is correct.

A row deleted during the backfill: the backfill emits `SNAPSHOT_READ` with the row's pre-delete state; CDC then emits `DELETE`. The order is correct (creation before deletion in Entity Resolution's processing) so the GR is created and then cleanly removed. The intermediate state (GR temporarily exists) is brief and acceptable.

**`er-reindex` and the MERGE / REMERGE / SUPERSEDE operations.** During an ER reindex, Batch Backfill's job may discover that two existing GRs should have been merged. The job emits two `entity_routed` events: one with `operation=MERGE` for the surviving GR, one with `operation=SUPERSEDE` for the loser. Entity Resolution owns the actual data manipulation (provenance reassignment, redirect insertion); Batch Backfill just orchestrates the events.

---

## 8. Hot/Warm/Cold Handling — Batch Backfill's Slice

Batch Backfill is the *workhorse* of tier movement. Every promotion, demotion, and reevaluation translates into a batch job.

**`materialization-promotion-backfill` (warm → hot or cold → hot).** This is the most common Batch Backfill job in steady state. For warm → hot:
1. Read the cohort's records from Delta Lake.
2. For each record, re-emit on `m1.raw_records` with `source_op = RELEVEL` and `materialization_level = 'hot'` in the headers.
3. The streaming chain picks them up; Stage 0 sees the new level; Stages 1–3 run; M3 Writers projects.

For cold → hot, the records are not in Delta Lake. Batch Backfill instead:
1. Coordinates with CDC Streaming to trigger a fresh extraction from source (via `nexus.connector.refresh_required`).
2. Once Delta Lake is populated by the extraction, the path is the same as warm → hot.

**`materialization-demotion-cleanup` (hot → warm or hot → cold).** Batch Backfill launches the data-side of cleanup:
1. Identify records in the cohort where `entity_store_presence` has any applicable store flag set to TRUE.
2. Emit `entity_routed` events with `operation = REMOVE` for each.
3. M3 Writers processes the REMOVE: tombstone in Elasticsearch (mark `deleted:true`), detach in Neo4j, append `is_deletion=TRUE` in TimescaleDB.
4. Track completion via `write_completed` events; declare cleanup done when all are accounted for.

**Per-record reevaluation (the daily DAG `materialization-policy-reevaluate`).** Materialization Coordinator owns the decision; The Batch Backfill component owns the execution. Materialization Coordinator's DAG identifies records whose level changed and writes a list to a Delta Lake staging table. Batch Backfill's executor reads the list and emits `RELEVEL` (for upgrades) or `REMOVE` (for downgrades) per record on `entity_routed`.

**Coordinating multiple movements.** A tenant with active decay rules and a fiscal close boost can have thousands of records moving simultaneously. Batch Backfill manages this via a global throttle in `tenant_configs.tier_movement_throughput_max` (default 1000 records/second per tenant). Above this rate, movements queue and are processed in priority order: manual overrides first, then learned-rule changes, then time-decay, then RLHF promotions.

**Oscillation cost mitigation.** A record cycling hot → warm → hot (boost expires, then re-promoted by RLHF) costs Elasticsearch re-embedding unless caught. Batch Backfill's executor retrieves the stored `provenance_hash` from the existing Elasticsearch document metadata before triggering a re-embed. If unchanged, M3 Writers is told to short-circuit via the provenance-hash comparison in the upsert algorithm. This was OQ-PROV-01 in the parent spec; Batch Backfill's executor is the enforcement point.

---

## 9. Acceptance Criteria

- **AC-D2-01.** Register a new Salesforce connector for tenant Acme; assert `initial-load` DAG launches, processes 100K source rows, emits the corresponding `m1.raw_records` events with `source_op = SNAPSHOT_READ`, and `connector_backfill_handover.status` transitions through `running → completed → cdc_handover_done`.
- **AC-D2-02.** Kill the `initial-load` Spark application halfway through a 1M-row backfill; relaunch; assert it resumes from the last checkpoint and produces no duplicate events.
- **AC-D2-03.** Run an `initial-load` and CDC simultaneously (failed handover scenario); assert D3 deduplicates correctly via `(source_system, source_record_id, source_op)` and the final state is identical to running them sequentially.
- **AC-D2-04.** Promote `Transaction.SalesOrder` from warm to hot for a tenant with 50K matching records; assert `materialization-promotion-backfill` completes within 30 minutes (NFR-D2-01); assert all 50K rows in `entity_store_presence` have `ts_present = TRUE`; assert no double-write.
- **AC-D2-05.** Demote `Party` from hot to warm for a tenant with 10K parties; assert all are tombstoned in Elasticsearch (metadata `deleted:true`) and detached in Neo4j; assert TimescaleDB rows persist (warm doesn't clean TimescaleDB).
- **AC-D2-06.** Run `er-reindex` on `Party` after lowering the auto-apply threshold from 0.92 to 0.85; assert previously review-band matches that now exceed the threshold are auto-applied as MERGEs and the survivor IDs are correctly published.
- **AC-D2-07.** Trigger `cdm-version-migration` after a CDM v3 publishes that splits `Party.address` into `Party.billing_address` and `Party.shipping_address`; assert provenance rows for `address` are migrated to the appropriate split, Elasticsearch vectors are re-embedded, and no records have stale schema references.
- **AC-D2-08.** Pre-flight check rejects an `initial-load` whose estimated cost exceeds the tenant budget; assert the recommendation queue receives the proposal with `requires_approval=true`.
- **AC-D2-09.** Run `m3-reconciliation` after deliberately corrupting Elasticsearch (delete a document outside the platform); assert the missing record is identified and re-emitted; D5 re-upserts; presence register reflects recovery.
- **AC-D2-10.** Run a backfill with `dry-run=true`; assert no Kafka events are emitted but the job logs ER outcomes and projection counts as if it had run.

---

## 10. Open Questions

- **OQ-D2-01.** Should `nexus_spark_lib` be one library shared between CDC Streaming and Batch Backfill, or split into a transform library (Stages 1–3) and a kafka-write library (different I/O)? Recommend single library for now; revisit if streaming and batch I/O patterns diverge enough.
- **OQ-D2-02.** For SaaS sources without a stable source position (no LSN/SCN), the handover protocol is best-effort. What's the fallback? Recommend D3 deduplication (already specified) with a per-source documented "expected duplicate rate during handover."
- **OQ-D2-03.** Cost model for pre-flight estimation — does it use the same `cost_model` table from the materialization feature-learning spec? Recommend yes, augmented with Spark-hour pricing.
- **OQ-D2-04.** `er-reindex` cadence for very large entity types — weekly may be too aggressive for a 100M-record `Party`. Recommend per-entity-type cadence based on observed match-change rate, falling out of v0.2.
- **OQ-D2-05.** Should `materialization-promotion-backfill` run *before* D4 flips the level, or *after*? Recommend after — the level is the formal commit, and queries during the backfill window correctly fall back to source via the `transition_status` flag.
- **OQ-D2-06.** Throttle interaction with multi-tenant fairness — if one tenant's tier movements saturate their throughput limit, do their normal CDC events also slow down? They share the Spark cluster but different applications. Recommend yes by design, but document the user-visible effect.

---

## 11. References

- `iter2-dev-overview-and-registers-v0.1.md` — cross-cutting contracts, esp. §5.2 (backfill differences) and §5.3 (handover).
- `iter2-dev-D1-cdc-streaming-ingestion-v0.1.md` — CDC Streaming spec; the handover counterparty.
- `iter2-system-pipeline-orchestration-v0.1.md` — §2.2 batch job catalogue, §7 operational concerns.
- `NEXUS-Iter2-SPEC-ServiceTopology-v0.4.md` — Spark on Kubernetes baseline.
- `iter2-materialization-policy-engine-v0.1.md` and `iter2-materialization-feature-learning-v0.1.md` — what triggers materialization-promotion-backfill at the policy layer.
