# Iteration 2 — Developer Coordination Overview and System Registers

**Version:** v0.1 (draft)
**Date:** 2026-04-27
**Audience:** All five developers working the CDM-to-AIStores pipeline
**Companion to:** the four pipeline drafts (`iter2-cdm-to-aistores-pipeline-v0.1.md`, `iter2-record-lifecycle-structured-walkthrough-v0.1.md`, `iter2-system-pipeline-orchestration-v0.1.md`, `iter2-materialization-policy-engine-v0.1.md`, `iter2-materialization-feature-learning-v0.1.md`)
**Companion to dev specs:** `iter2-dev-CDC Streaming-...`, `Batch Backfill-...`, `Entity Resolution-...`, `Materialization Coordinator-...`, `M3 Writers-...`

---

## 📦 Codebase & Deployment

> **One repository, many deployments.** All NEXUS services live in the `nexus-platform` monorepo. This document is the coordination reference for all five pipeline developers — it describes the shared registers and contracts that cross service boundaries. Every developer on the pipeline must read this before their individual spec.

| | |
|---|---|
| **This document covers** | Cross-service coordination — not a single deployment |
| **Register ownership** | See §4.3 — each register is owned by exactly one service |
| **All five pipeline devs** | Commit to the same monorepo (`nexus-platform`). Shared libraries are in `libs/`. Services are in `services/`. |
| **Monorepo layout** | `libs/nexus_core/` · `libs/agent_core/` · `libs/nexus_spark_lib/` · `services/nexus-m1-worker/` · `services/nexus-airbyte-stream-bridge/` · `services/nexus-spark-transformer/` · `services/nexus-m3-writer/` · `dags/` |

> ⚠️ **Integration rule:** If your service needs to read a register it does not own, query it via the documented API or direct SQL — never write to it. Cross-service writes are a pipeline correctness bug.

---

## 1. Purpose

This document is the seam where the five developer streams meet. It does three things:

It maps the five developer streams to the architecture so each developer knows what they own and what they consume from peers. It defines the **shared contracts** between developer pairs so integration is not invented at the eleventh hour. It catalogs the **system registers** — the bookkeeping tables the platform maintains so the query engine can answer "where is this record right now?" without scanning every store.

The dev specs in `CDC Streaming..M3 Writers` follow the same template: scope, dependencies, functional and non-functional requirements (MoSCoW), data model ownership, API/Kafka contracts, CRUD handling, hot/warm/cold handling specific to that developer, acceptance criteria, and open questions. This overview is the index they all reference back to.

---

## 2. The Five Developer Streams

| Stream | Owner role | Primary deliverable |
|---|---|---|
| **CDC Streaming** | Streaming engineer | `spark-stream-transformer` consuming CDC from sources, emitting normalised records |
| **Batch Backfill** | Batch engineer | `spark-batch-jobs` family for backfills, reconciliation, version migration |
| **Entity Resolution** | ER engineer | Three-signal entity resolution, Golden Record state machine, source-CRUD propagation |
| **Materialization Coordinator** | Tier engineer | Stage 0 policy evaluator, tier-movement DAGs, materialization registers and signals |
| **M3 Writers** | Store engineer | Three M3 writers, per-store CRUD, store presence register, cross-store reconciliation |

### 2.1 Dependency DAG

```
       (sources)
           │
   ┌───────┴───────┐
   ▼               ▼
  CDC Streaming              Batch Backfill
   │               │
   └───────┬───────┘
           ▼
          Entity Resolution
           │
           ▼
          Materialization Coordinator
           │
           ▼
          M3 Writers
           │
           ▼
   (Elasticsearch, Neo4j, TimescaleDB,
    presence register, query engine)
```

CDC Streaming and Batch Backfill produce structurally identical record streams (raw or transformed) on the same Kafka topics, so Entity Resolution cannot tell whether it is consuming live CDC or backfill output. This is intentional and is enforced by the `nexus_spark_lib` shared library that both CDC Streaming and Batch Backfill use.

Materialization Coordinator sits *between* synthesis and projection (not on the side) because materialization decisions affect what M3 Writers actually writes. Materialization Coordinator evaluates the policy after Entity Resolution has produced the final Golden Record state, then routes the resulting `entity_routed` event with the level annotation.

### 2.2 Phase gates and ordering

| Week | Gate | Required from |
|---|---|---|
| 0–1 | Library + DDL frozen | Batch Backfill (`nexus_spark_lib`), Entity Resolution (data model), Materialization Coordinator (policy table), M3 Writers (presence table) |
| 2–3 | Streaming end-to-end happy path | CDC Streaming, Entity Resolution, M3 Writers (stub Materialization Coordinator with always-hot) |
| 4–5 | Backfill happy path + reconciliation | Batch Backfill, Materialization Coordinator, M3 Writers |
| 6–7 | CRUD propagation (UPDATE/DELETE) end-to-end | CDC Streaming, Entity Resolution, M3 Writers |
| 8 | Tier movements live (promote/demote round-trip) | Materialization Coordinator, M3 Writers |
| 9 | Acceptance: full lifecycle including RLHF telemetry | All |

A developer can start their stream as soon as their inputs and outputs from the table below are stubbed. Stubs are mandatory in week 0 so no stream blocks on another.

---

## 3. Shared Contracts (Pairwise)

Every contract is a stable interface owned by one stream and consumed by another. Changes require notification of the consumer and a deprecation window of at least one week unless the consumer signs off sooner.

### 3.1 CDC Streaming → Entity Resolution and Batch Backfill → Entity Resolution

**Topic:** `m1.int.raw_records` (CDC Streaming, streaming) and same topic from Batch Backfill (batch — see Batch Backfill spec for partition strategy).

Both streams emit the same payload schema, so Entity Resolution has a single consumer loop. The payload includes `source_op ∈ {INSERT, UPDATE, DELETE, SNAPSHOT_READ}`, a `delta_pointer` to Delta Lake, and origin metadata (`tenant_id`, `connector_id`, `source_system`, `source_table`, `source_record_id`, `source_ts`, `ingest_offset`).

Entity Resolution must treat `SNAPSHOT_READ` (Batch Backfill's output for an initial-load row) as semantically equivalent to `INSERT` for resolution purposes, with one operational difference noted in §3.2.

### 3.2 Batch Backfill → Entity Resolution (backfill-specific)

Batch Backfill emits `nexus.backfill.batch_started` and `nexus.backfill.batch_completed` events bracketing each backfill. Entity Resolution uses the bracket to enable a "bulk ER" mode (LSH bucket caches grow larger, batch ER signal applies more aggressively) and to defer high-cost graph signal computations until the bracket ends, then run them once across the whole batch.

### 3.3 Entity Resolution → Materialization Coordinator

**Topic:** `m1.int.transformed_records`.

Payload includes the resolved `cdm_entity_id`, the `provenance_summary`, the `operation` (`UPSERT` | `RELEVEL` | `MERGE` | `SUPERSEDE` | `REMOVE`), and the originating `contributing_record`. Entity Resolution has already written all canonical-store rows (provenance, resolution index) before publishing.

The five operations let Materialization Coordinator know how to evaluate the policy and, more importantly, let M3 Writers know how to handle the change downstream. `RELEVEL` is published by Materialization Coordinator, not Entity Resolution, but the schema is shared.

### 3.4 Materialization Coordinator → M3 Writers

**Topic:** `{tid}.m1.entity_routed` (existing) with mandatory new headers `materialization_level` and `applied_rule_id`.

Materialization Coordinator reads `transformed_records`, evaluates policy, decides the level, and republishes as `entity_routed`. If the level is `warm` or `cold`, Materialization Coordinator publishes to `{tid}.m1.warm_recorded` or `{tid}.m1.cold_skipped` instead, and M3 Writers does not consume these.

Materialization Coordinator also emits two control topics:
- `nexus.materialization.changed` when a tier moves for an entity type / cohort
- `nexus.materialization.reevaluated` for record-level relevels triggered by the daily DAG

M3 Writers consumes both to know when to run promotion-backfill / demotion-cleanup.

### 3.5 M3 Writers → query engine (M2)

**Topic:** `nexus.m3.write_completed` / `nexus.m3.write_failed` (existing).

The completed event carries `stores_written: [...]` and `skipped_stores: [...]` so the query engine can update its presence register cache. The presence register itself is the authoritative read path for "where is this record?"

### 3.6 Entity Resolution ↔ Materialization Coordinator (tier policies inform ER)

Entity Resolution reads `cdm_entity_materialization` (Materialization Coordinator's table) to choose ER depth per record (full vs deterministic-only vs skip). Materialization Coordinator publishes `nexus.materialization.changed`; Entity Resolution refreshes its broadcast cache on receipt.

### 3.7 M3 Writers ↔ query engine and M3 Writers → Materialization Coordinator (telemetry)

M3 Writers owns the `entity_store_presence` register read by the query engine. M3 Writers also emits per-store signal events that Materialization Coordinator aggregates into `materialization_signal` for the RLHF loop. The two are separate concerns but share the same underlying observation point in m3-writer.

---

## 4. The System Registers

Six tables are referred to collectively as "the registers". Together, they answer every routing question the query engine asks. Each register has exactly one owner; reads are open, writes are restricted.

| Register | Owner | Purpose | Read path |
|---|---|---|---|
| `entity_resolution_index` | Entity Resolution | `(source_system, source_record_id) → cdm_entity_id` | hot lookup for any incoming source event |
| `golden_record_provenance` | Entity Resolution | per-attribute source pointer | query engine attribute resolution |
| `golden_records_index` | Entity Resolution | GR state (active / provisional / superseded / tombstoned) | every query that touches a GR ID |
| `golden_record_redirects` | Entity Resolution | `superseded_id → surviving_id` | query engine resolves stale GR refs transparently |
| `materialization_policy` + `cdm_entity_materialization` | Materialization Coordinator | what level each cohort or entity type is at | Spark Stage 0 and m1-worker routing |
| **`entity_store_presence`** (NEW) | M3 Writers | per-record confirmed write flags: `es_present`, `neo4j_present`, `ts_present` | query engine's store routing lookup |

### 4.1 Why per-record presence flags

The materialization level in `cdm_entity_materialization` tells you what level an entity *type* is at — it does not tell you whether a specific record has been confirmed written to each store. Three things make per-record presence necessary:

1. **Write lag during transitions.** When a cohort is being promoted, some records are written and some are not yet. The type-level `current_level='hot'` is already set but individual writes are in flight. The query engine needs to know per record, not per type.
2. **Per-store config asymmetry.** A `Transaction.SalesOrder` is `metricable=true` and `embeddable=false`. It is in TimescaleDB but not Elasticsearch, regardless of materialization level.
3. **Failure isolation.** M3 Writers can succeed on one store and fail on another. The query engine needs to know which stores actually confirmed the write.

`entity_store_presence` is owned by M3 Writers and upserted after each confirmed store write or tombstone.

```sql
CREATE TABLE nexus_system.entity_store_presence (
  tenant_id      UUID         NOT NULL,
  cdm_entity_id  VARCHAR(48)  NOT NULL,
  es_present     BOOLEAN      NOT NULL DEFAULT FALSE,
  neo4j_present  BOOLEAN      NOT NULL DEFAULT FALSE,
  ts_present     BOOLEAN      NOT NULL DEFAULT FALSE,
  updated_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
  PRIMARY KEY (tenant_id, cdm_entity_id)
);
CREATE INDEX idx_esp_missing_es    ON nexus_system.entity_store_presence(tenant_id) WHERE es_present    = FALSE;
CREATE INDEX idx_esp_missing_neo4j ON nexus_system.entity_store_presence(tenant_id) WHERE neo4j_present = FALSE;
CREATE INDEX idx_esp_missing_ts    ON nexus_system.entity_store_presence(tenant_id) WHERE ts_present    = FALSE;
```

### 4.2 The query engine's routing protocol

When the query engine receives a question, it follows a fixed protocol for each `cdm_entity_id` involved:

```
1. Resolve through golden_record_redirects (in case the ID is stale).
2. Read entity_store_presence row for the resolved ID.
3. For each store where the flag is TRUE, the query engine MAY query.
   For flags that are FALSE (or row is absent), the query engine falls back to source.
4. A missing row means the record has never been confirmed written — treat all flags as FALSE.
```

A stale TRUE flag is acceptable — the store returns zero rows and the engine falls back to source gracefully. A FALSE flag is always safe to act on.

### 4.3 Register update ownership matrix

| Register | Inserted by | Updated by | Tombstoned by |
|---|---|---|---|
| `entity_resolution_index` | Entity Resolution (Stage 2 ER) | Entity Resolution (re-merge), Materialization Coordinator indirect (via reevaluate trigger), data steward | data steward (split) |
| `golden_record_provenance` | Entity Resolution (Stage 3 synthesis) | Entity Resolution on source UPDATE | Entity Resolution on source DELETE |
| `golden_records_index` | Entity Resolution (CREATE) | Entity Resolution (state transition) | Entity Resolution (TOMBSTONE) |
| `golden_record_redirects` | Entity Resolution (MERGE) | never updated | data steward (SPLIT undo) |
| `materialization_policy` | Materialization Coordinator (admin / RLHF) | Materialization Coordinator (rule supersession is append-with-supersede) | Materialization Coordinator (retirement) |
| `cdm_entity_materialization` | Materialization Coordinator | Materialization Coordinator (level changes) | never |
| `entity_store_presence` | M3 Writers (after successful store write) | M3 Writers (flag flip on re-verification or tombstone) | never deleted — boolean flags (`es_present`, `neo4j_present`, `ts_present`) are set to FALSE |

A common bug to watch: Entity Resolution marking a Golden Record as `tombstoned` does not by itself remove it from the stores. Entity Resolution must publish a `REMOVE` operation; M3 Writers processes it; M3 Writers sets the relevant `entity_store_presence` flags to FALSE. Until M3 Writers's flag flip lands, the query engine will continue to consult the stores, which is fine because the stores have already had their tombstone writes by then.

---

## 5. The Two Ingestion Processes — Visible Differences

### 5.1 Real-time CDC (CDC Streaming)

CDC connections (Debezium for relational sources, Airbyte streaming for SaaS) emit row-level events as the source mutates. CDC Streaming's streaming Spark application consumes these events on `cdc.<source>.<tenant>.<table>` topics, normalises them, and emits `m1.raw_records`. Latency is bounded by the 30-second micro-batch (target p95 of `m3.write_completed` < 90 seconds end-to-end).

CDC events carry `op ∈ {c, u, d, r}` corresponding to create, update, delete, snapshot read. CDC Streaming maps these to `INSERT`, `UPDATE`, `DELETE`, `SNAPSHOT_READ` respectively.

### 5.2 Database backfill (Batch Backfill)

A backfill is triggered when a connector is registered, when a CDM version changes substantially, when a materialization tier promotion mandates re-projecting old records, or on operator demand. Batch Backfill launches a sized Spark application via Airflow (`initial-load`, `materialization-promotion-backfill`, `cdm-version-migration`, or `m3-reconciliation`), reads the source corpus through the connector, normalises, and emits to the same `m1.raw_records` topic with `source_op = SNAPSHOT_READ` (or `RELEVEL` for promotion replay).

The backfill payload is structurally identical to a CDC event so Entity Resolution, Materialization Coordinator, M3 Writers are unchanged. The only distinguishing metadata is a `backfill_batch_id` header on each Kafka message, which Entity Resolution uses for batch-mode ER (§3.2) and which M3 Writers uses to scope acknowledgements.

### 5.3 Coordination: not double-processing

The risk: Debezium starts CDC at offset T₀, but the source already has 10M historical rows that need backfilling. If both systems run, Entity Resolution sees each row twice (once as `SNAPSHOT_READ` from Batch Backfill, once as `INSERT` from CDC Streaming).

The mitigation is the **CDC start-after-backfill** pattern. Batch Backfill's `initial-load` job records the source's current LSN / SCN / position before reading. CDC Streaming's CDC connector is configured to start consuming from that position. The handover is recorded in `nexus_system.connector_backfill_handover` (owned by Batch Backfill; CDC Streaming reads it).

> **DDL:** See `NEXUS-Iter2-SPEC-Backfill-v0.1.md` §5 — that is the single authoritative DDL for this table.

For sources where this strict ordering is impossible (some SaaS APIs), Entity Resolution deduplicates by `(source_system, source_record_id, source_op, source_ts)` — `INSERT` on a record that already has an `entity_resolution_index` row is treated as `UPDATE` and re-runs synthesis idempotently.

---

## 6. CRUD Handling — The Whole Picture

This section is normative for the developer specs that follow. Each developer deals with one slice of CRUD; this overview shows the slices fit.

### 6.1 INSERT (and `SNAPSHOT_READ`)

Source emits new row → CDC or backfill puts it on `raw_records` → Entity Resolution ER produces (or absorbs into) a Golden Record → Entity Resolution writes provenance rows → Entity Resolution publishes `transformed_records` with `operation=UPSERT` → Materialization Coordinator evaluates policy → Materialization Coordinator publishes `entity_routed` with `materialization_level` → M3 Writers writes to applicable stores → M3 Writers upserts `entity_store_presence` row (flags set to TRUE for each successful store write) → M3 Writers publishes `write_completed`.

### 6.2 UPDATE

Source emits update → Entity Resolution looks up `entity_resolution_index` to find the existing `cdm_entity_id` → Entity Resolution re-runs Stage 2 ER (the values may have changed enough to break a previous match — see edge case EC-13.3 in the walkthrough) → Entity Resolution re-runs Stage 3 synthesis. Survivorship may now favour a different source for some attribute; the previous winner's `golden_record_provenance` row is replaced by the new winner's. Entity Resolution publishes `transformed_records` with `operation=UPSERT`. From Materialization Coordinator onward the path is identical to INSERT. M3 Writers's writes are idempotent so the change is in-place.

A subtlety: if Salesforce *was* the source for `industry` and the update removes the value (sets to null), the row in `golden_record_provenance` for that attribute is **deleted** (not updated to a null value). The next source in the priority order takes over if it has a value; otherwise the attribute disappears from the GR until any source re-supplies it.

### 6.3 DELETE — the hard case

Source emits delete on a single source record. This means: "this source no longer contributes to the corresponding Golden Record." It does **not** automatically mean "delete the Golden Record" because other sources may still contribute.

Entity Resolution's algorithm:

```
1. Look up entity_resolution_index for (source_system, source_table, source_record_id).
   If no row exists: the record was never resolved here; ignore (this should not happen
   in a healthy system but the handler must be safe).
2. Read the cdm_entity_id, then read all golden_record_provenance rows for that GR.
3. For each provenance row whose source_record_id matches the deleted source record:
   delete the provenance row.
4. If any attributes are now without provenance:
   re-run survivorship on those attributes from remaining contributing source records.
   - If a remaining source can provide the attribute: insert a new provenance row.
   - If no remaining source can: the attribute is now absent from the GR.
5. If after step 3 the GR has zero provenance rows (no source contributes anything anymore):
   transition the GR's state to 'tombstoned' in golden_records_index.
   Publish transformed_records with operation=REMOVE.
6. Otherwise:
   Publish transformed_records with operation=UPSERT (since the GR's effective value set has changed).
7. Delete the entity_resolution_index row for the deleted source record.
```

M3 Writers handles `operation=REMOVE` differently per store:

| Store | Action on REMOVE |
|---|---|
| Elasticsearch | Tombstone document (update with `deleted:true` metadata flag). Hard purge after 24h via maintenance job. |
| Neo4j | `MATCH ... DETACH DELETE` on the node; relationships are removed transitively. |
| TimescaleDB | Append a row with `is_deletion=TRUE`. The row stays (immutable append); query layer filters deletion rows. |
| PostgreSQL canonical | The provenance rows are already gone (Entity Resolution deleted them in step 3). The `golden_records_index` row stays in `tombstoned` state for audit. |

After M3 Writers completes, it sets the relevant `entity_store_presence` flags to FALSE and emits `write_completed` with `operation=REMOVE`. The query engine sees all flags as FALSE and stops querying the stores for that ID.

### 6.4 The MERGE and SPLIT cases

These are not source-driven CRUD — they are platform-driven reorganisations of the GR set. Entity Resolution owns them per the state machine in the system orchestration spec. M3 Writers handles them by treating them as a coordinated set of UPSERT and REMOVE operations on the affected `cdm_entity_id`s, with the redirect table updated by Entity Resolution before any store writes start so that any in-flight queries land on the surviving ID.

---

## 7. Hot/Warm/Cold Movement on Living DBs

This section is the heart of what makes the dev work different from a traditional batch ETL. Records do not enter a tier and stay there; they migrate constantly. Each developer handles a slice.

### 7.1 What "moving" means concretely

A record at hot is in Elasticsearch, Neo4j, TimescaleDB (subject to per-store config). Demoted to warm: removed from those stores, kept in Delta Lake. Re-promoted to hot: re-projected from Delta Lake. Each transition is an explicit set of M3 Writers operations on the relevant stores plus updates to `entity_store_presence` flags and `materialization_decision_log`.

The frequency matters. For decay-driven movements (a sales order ages from 90 days to 91), thousands of records may transition per day. For boost-driven movements (fiscal close starts), millions might transition in one window. The system needs to handle both without saturating.

### 7.2 Movement lifecycle (per record)

```
[hot]──demote──→[demoting]──cleanup_done──→[warm]──promote──→[promoting]──backfill_done──→[hot]
                    │                          │                              │
                    │                          ▼                              │
                    │                       [demoted]──promote──→ ...         │
                    │                                                         │
                    └─────query during transition──→ presence register      ──┘
                          flags FALSE on stores; query falls back
                          to source until backfill_done
```

The intermediate `demoting` / `promoting` states are not separate values in `materialization_level` — they are flags in `cdm_entity_materialization.transition_status`. Materialization Coordinator sets the type-level flag at the start of a movement and clears it when M3 Writers confirms the cleanup or backfill is done. Per-record progress is tracked through the `entity_store_presence` boolean flags: a flag flipped to FALSE signals that the store tombstone for that record is confirmed; a flag flipped back to TRUE signals that the re-projection is confirmed.

### 7.3 Materialization Coordinator and M3 Writers cooperation on movement

**Demotion of an entity type:**
1. Materialization Coordinator sets `cdm_entity_materialization.transition_status = 'demoting'` for the (tenant, entity_type).
2. Materialization Coordinator emits `nexus.materialization.changed`.
3. Materialization Coordinator launches `materialization-demotion-cleanup` DAG, which scans `entity_store_presence` for rows matching the cohort where any flag is TRUE and emits `entity_routed` events with `operation=REMOVE` for each.
4. M3 Writers processes the REMOVEs: tombstones in Elasticsearch (marks `deleted:true`), detaches from Neo4j, marks `is_deletion=TRUE` in TimescaleDB.
5. M3 Writers sets the relevant `entity_store_presence` flags to FALSE per row after each confirmed store tombstone.
6. When the DAG completes, Materialization Coordinator sets `transition_status = NULL` and `materialization_level = 'warm'`.

**Promotion of an entity type:**
1. Materialization Coordinator sets `transition_status = 'promoting'`.
2. Materialization Coordinator launches `materialization-promotion-backfill`, which reads Delta Lake records for the cohort and re-emits them on `m1.raw_records` with `source_op = RELEVEL`.
3. Entity Resolution re-resolves them (warm-tier records had only Signal A applied; now full ER runs).
4. Materialization Coordinator routes them through Stage 0 (now `hot`).
5. M3 Writers writes to applicable stores; sets the relevant `entity_store_presence` flags to TRUE per row after each confirmed store write.
6. When the DAG completes, Materialization Coordinator clears the transition flag and moves the level to `hot`.

**Per-record reevaluation (decay or boost expiring):**
The daily `materialization-policy-reevaluate` DAG identifies records whose level changed. Materialization Coordinator emits a `RELEVEL` event per record. M3 Writers either projects (if the record is now hot and was not before) or tombstones (if the record is now warm/cold and was hot). The `entity_store_presence` flags reflect the outcome of each write.

### 7.4 Handling oscillation

Time-related records oscillating between tiers (a "hot for 90 days, warm afterward" decay rule combined with "boost during fiscal close" creates frequent movement) is a real workload, not an edge case.

Three mechanisms keep oscillation cheap:

1. **Delta Lake as warm storage** means promotion is a re-projection, not a re-extraction. The expensive parts of Stages 1 and 2 do not redo work; canonical attributes are already in Delta Lake from prior runs.
2. **Elasticsearch idempotent upserts** mean a record promoted, demoted, and re-promoted within a week sees its document tombstoned then re-upserted; the embedding API is called only when the hash check shows the embedding text would actually be different.
3. **Neo4j MERGE** is cheap on existing nodes; demotion-cleanup `DETACH DELETE`s and promotion `MERGE`s. Edges follow the node, not the record's tier, so the graph topology is not needlessly rebuilt.

The query engine handles in-flight movements by trusting the presence register: during the `demoting` / `promoting` window, `entity_store_presence` flags flip to FALSE as each store tombstone is confirmed, and queries fall back to the source. The source path is slower but always correct. As soon as backfill completes and the flags flip back to TRUE, the fast path resumes.

### 7.5 The oscillation register

A new register, `materialization_movement_log` (owned by Materialization Coordinator), records every movement for both audit and RLHF telemetry:

```sql
CREATE TABLE nexus_system.materialization_movement_log (
  movement_id        BIGSERIAL PRIMARY KEY,
  tenant_id          UUID NOT NULL,
  scope              VARCHAR(128) NOT NULL,            -- entity_type or cohort_id
  cdm_entity_id      VARCHAR(48),                      -- NULL for entity-type-level moves
  from_level         VARCHAR(8) NOT NULL,
  to_level           VARCHAR(8) NOT NULL,
  trigger            VARCHAR(32) NOT NULL,             -- 'admin' | 'rlhf' | 'decay' | 'boost' | 'reevaluate'
  triggered_by_rule  UUID,
  started_at         TIMESTAMPTZ NOT NULL,
  completed_at       TIMESTAMPTZ,
  status             VARCHAR(16) NOT NULL              -- 'in_progress' | 'completed' | 'failed' | 'aborted'
) PARTITION BY RANGE (started_at);
```

The RLHF feature-learning loop (the previous spec) reads from this log to derive `oscillation_count_30d` and `time_at_hot_30d` features per entity type, which become inputs to the reward model.

---

## 8. What Each Dev Spec Will Add

Each of `CDC Streaming..M3 Writers` follows the same template and adds developer-specific detail beyond what this overview establishes. The skeleton:

- **Scope and dependencies.**
- **Functional Requirements (MoSCoW).** Specific to the developer's surface.
- **Non-Functional Requirements.** Throughput, latency, idempotency.
- **Data Model Ownership.** Tables this dev creates and owns.
- **API and Kafka Contracts.** Inbound and outbound, by name.
- **CRUD Handling.** What this dev does on INSERT / UPDATE / DELETE / RELEVEL.
- **Hot/Warm/Cold Handling.** How tier movements affect this dev's surface.
- **Acceptance Criteria.** Concrete tests that gate sign-off.
- **Effort estimate.** Rough person-weeks within Iter 2's 9-week window.
- **Open Questions.**

The overview document is the contract; the dev specs are the implementations of pieces of it.

---

## 9. References

- All four pipeline drafts in this folder (parent specs).
- `NEXUS-Iter2-SPEC-ServiceTopology-v0.4.md` — service-level contract baseline.
- `NEXUS-Iter2-SPEC-DataModel-v0.5.md` — base for the new tables added across CDC Streaming..M3 Writers.
- `NEXUS-Iter2-SprintPlan-v0.3.md` — phase-gate context (will need an addendum reflecting CDC Streaming..M3 Writers ordering).
