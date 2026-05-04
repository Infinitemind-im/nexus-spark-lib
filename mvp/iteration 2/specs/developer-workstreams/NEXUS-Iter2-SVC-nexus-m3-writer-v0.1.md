# NEXUS — Iteration 2 · `nexus-m3-writer` · M3 Writers and Store Presence Register

**Service:** `nexus-m3-writer`
**Version:** v0.1 (draft)
**Date:** 2026-04-27
**Companion to:** `iter2-dev-overview-and-registers-v0.1.md`

---

## 📦 Codebase & Deployment

> **One repository, many deployments.** All NEXUS services live in the `nexus-platform` monorepo. You are contributing a new service to a shared codebase. The three store handlers (Elasticsearch, Neo4j, TimescaleDB) are **modules within a single service process** — not three separate services. Each handler is one Python file; one team (Dev 5) owns the service; three developers (A, B, C) each own one handler file.

| | |
|---|---|
| **Deployed as** | `nexus-m3-writer` (**NEW** service — one process, three store handlers) |
| **Monorepo path** | `services/nexus-m3-writer/` |
| **Language / runtime** | Python 3.11 · asyncio |
| **Iteration 2 owner** | Dev 5 (5 person-weeks) — coordinates Developer A (ES), Developer B (Neo4j), Developer C (TimescaleDB) |
| **Handler modules** | `nexus_m3_writer/stores/elasticsearch_writer.py` · `nexus_m3_writer/stores/neo4j_writer.py` · `nexus_m3_writer/stores/timescale_writer.py` |

> ⚠️ **All three handler developers commit to the same service directory.** Dev 5 is the integration lead — they own the service shell, Kafka consumer loop, `entity_store_presence` writes, and acceptance testing. Developer A/B/C own their handler file and its unit tests.

---

## 1. Scope

The M3 Writers component owns the projection layer — the part of the platform that translates Golden Record state into actual writes to Elasticsearch, Neo4j, and TimescaleDB, and maintains the **store presence register** that the query engine consults to know where data actually lives.

Concretely:

- The `nexus-m3-writer` service (existing, extended). Three internal handlers — Elasticsearch, Neo4j, TimescaleDB — each idempotent, each handling its store's flavour of CRUD (upsert / merge / append).
- The `entity_store_presence` register: per-record per-store presence tracking with provenance hashes for staleness detection.
- The `cdm_entity_storage_config` register: per-entity-type per-tenant configuration of which stores are applicable (`embeddable`, `graph_persistent`, `metricable`).
- Per-store deletion semantics: tombstone-and-delete for Elasticsearch (Delete By Query with `deleted:true` filter), `DETACH DELETE` for Neo4j, `is_deletion=TRUE` append for TimescaleDB.
- Cross-store reconciliation: nightly scan of `entity_store_presence` against actual store contents; replay missing records.
- The query engine's read interface: a `presence_lookup(tenant_id, cdm_entity_id) → {elasticsearch: state, neo4j: state, timescaledb: state, postgres: state}` API that the query executor calls before issuing store queries.

This component does **not** own: ingestion (CDC Streaming/Batch Backfill), entity resolution (Entity Resolution), or materialization decisions (Materialization Coordinator). M3 Writers receives `entity_routed` events with an `operation` and a `materialization_level` and faithfully projects.

---

## 2. Dependencies

| Depends on | What for | When needed |
|---|---|---|
| Platform team (M5) | Elasticsearch index provisioned per tenant (`nexus_{tenant_slug}_{entity_type}`); Neo4j Aura with constraints; TimescaleDB extension installed; PostgreSQL `nexus_system` schema | Week 0 |
| Entity Resolution | `transformed_records` payload schema frozen, including all `operation` values | Week 1 |
| Materialization Coordinator | `entity_routed` payload schema frozen with `materialization_level` header | Week 1 |
| Batch Backfill | `m3-reconciliation` DAG framework that M3 Writers plugs into | Week 4 |
| Platform | OpenAI embeddings API access keys; embedding model version pinned | Week 2 |

---

## 3. Functional Requirements (MoSCoW)

### 3.1 Must

- **FR-Dev 5-M-01.** Implement the `nexus-m3-writer` service consuming `{tid}.m1.entity_routed` (consumer group `m3-writer-entities`). For each event, look up `cdm_entity_storage_config(tenant_id, cdm_entity_type)` to determine which of the three stores apply, then write in parallel to applicable stores. Per-store success/failure is independent.
- **FR-Dev 5-M-02.** Implement the **Elasticsearch writer** (see `NEXUS-Iter2-M3-Elasticsearch-Writer-v0.1.md` for full spec):
  - Document ID: `cdm_entity_id` (used as Elasticsearch `_id`).
  - Index name: `nexus_{tenant_slug}_{cdm_entity_type_lower}`.
  - Upsert via `_update` with `doc_as_upsert: true`. Payload: `dense_vector` (1536 dims, cosine similarity) + metadata (`tenant_id`, `cdm_entity_id`, `cdm_entity_type`, `contributing_sources`, `materialization_level`, `embedding_model_version`, `provenance_hash`, `deleted: false`).
  - Idempotency: re-upsert of same `_id` overwrites in place.
  - Embedding text construction: read `cdm_entity_storage_config.embed_attrs`, fetch the listed attributes' values from source via the connector worker, exclude PII per `agent_core.PIIChecker`, concatenate with a deterministic format, call `EmbeddingClient.embed()`. Discard the text after embedding returns.
  - Staleness short-circuit: if `provenance_hash` matches the existing document's metadata `provenance_hash` and `embedding_model_version` is unchanged, skip the embedding call and the upsert.
- **FR-Dev 5-M-03.** Implement the **Neo4j writer**:
  - Node MERGE on `(id, tenant_id)` composite. Properties: `id`, `tenant_id`, `connector_id`, `materialization_level`, `created_at`, `updated_at`. **No business field values.**
  - Relationship MERGE on `(start, end, type, source_fk)` composite. Properties: `source_fk`, `chunk_id` (for document-derived edges, optional), `materialization_level`, `connector_id`, `since`, `confidence`, `created_at`, `updated_at`. Same logical relationship from two different sources coexists as two edges per parent spec FR-M-07.
  - On UPDATE that changes a relationship: delete stale outbound edges with the same `source_fk` that are no longer present, MERGE new ones.
  - Idempotency: MERGE is idempotent by definition.
- **FR-Dev 5-M-04.** Implement the **TimescaleDB writer**:
  - INSERT into `nexus_ts.business_metrics_raw` with `ON CONFLICT (time, tenant_id, metric_name, cdm_entity_id) DO NOTHING`.
  - Required columns: `time`, `tenant_id`, `metric_name`, `normalised_value` (FX-normalised to base currency), `base_currency`, `dimensions` (JSONB), `source_system`, `cdm_entity_id`, `cdm_version`, `materialization_level`, `is_correction`, `is_deletion`.
  - Continuous aggregates (`metrics_weekly`, `metrics_monthly`, `metrics_yearly`) auto-maintained — M3 Writers does not manage them.
  - Corrections: append a reversal row + corrected row, never UPDATE.
  - Deletions: append a row with `is_deletion=TRUE`, never DELETE.
- **FR-Dev 5-M-05.** Handle each `operation` value from `entity_routed`:

| operation | Elasticsearch | Neo4j | TimescaleDB | Notes |
|---|---|---|---|---|
| `UPSERT` | upsert document | MERGE node + relationships | INSERT row | Standard happy path |
| `RELEVEL` | upsert document if newly hot, mark `deleted:true` if newly warm/cold | same | same (TimescaleDB rows persist on demotion) | Triggered by Materialization Coordinator |
| `MERGE` (survivor) | upsert with new `provenance_hash` | MERGE survivor; rewrite edges from loser to survivor | re-INSERT under survivor `cdm_entity_id` | Coordinated with SUPERSEDE |
| `SUPERSEDE` (loser) | mark `deleted:true` | DETACH DELETE loser node | INSERT `is_deletion=TRUE` row for loser | Coordinated with MERGE |
| `REMOVE` | mark `deleted:true` | DETACH DELETE | INSERT `is_deletion=TRUE` row | Source DELETE → empty GR |

- **FR-Dev 5-M-06.** Implement the **store presence register** (`entity_store_presence` table per the overview spec §4.1). After every successful store write or tombstone, atomically update the corresponding `entity_store_presence` row in the same transaction window as the store operation (best-effort; if the store write succeeds and the presence update fails, a follow-up reconciliation catches it).
- **FR-Dev 5-M-07.** Implement the `presence_lookup` API (gRPC + HTTP). Called by `nexus-query-executor` before issuing store queries. Returns the presence state for one or many `cdm_entity_id`s. Hot-path read; backed by the PostgreSQL `entity_store_presence` table with appropriate indexes; cached in Redis with 30-second TTL and invalidation on `nexus.m3.write_completed` events.
- **FR-Dev 5-M-08.** Implement per-store failure isolation. A failed write to one store does not block the other two. After processing all three:
  - All three succeeded → emit `nexus.m3.write_completed` with `stores_written=[...]`, `skipped_stores=[]`.
  - At least one failed (and was not deliberately skipped by config) → emit `nexus.m3.write_failed` with `failed_stores=[...]`. The Kafka offset is committed regardless.
  - Recovery is by topic replay (parent spec §7) — the `m3-reconciliation` DAG owned by Batch Backfill detects state drift and re-emits affected `entity_routed` events.
- **FR-Dev 5-M-09.** Implement the cleanup-on-demotion handler. When `entity_routed` arrives with `operation='RELEVEL'` and `materialization_level ∈ {'warm', 'cold'}`, and `entity_store_presence` shows the record currently `present` in some stores: mark `deleted:true` in Elasticsearch (hard-purged by the nightly deletion job), DETACH DELETE in Neo4j, append `is_deletion=TRUE` in TimescaleDB. **Exception:** TimescaleDB is *not* cleaned on hot-to-warm — historical metric data is preserved. Only on hot-to-cold is TimescaleDB tombstoned.
- **FR-Dev 5-M-10.** Maintain the `cdm_entity_storage_config` register. Default values seeded at CDM publish time per the heuristics in `iter2-system-pipeline-orchestration-v0.1.md` §6.1. Admin API for editing.
- **FR-Dev 5-M-11.** Implement the nightly cross-store reconciliation pass (executes within D2's `m3-reconciliation` DAG framework). For each tenant:
  1. Sample N `cdm_entity_id`s from `entity_store_presence` per store.
  2. Verify each is actually present in the corresponding store (Elasticsearch GET by `_id`, Neo4j MATCH, TimescaleDB SELECT).
  3. For drift detected: log to `nexus_system.entity_store_presence_drift_log`, re-emit `entity_routed` to repair.
  4. Update `last_verified_at` on confirmed-present rows.
- **FR-Dev 5-M-12.** Per-store metrics emission to Prometheus: `m3_writer_writes_total{store, outcome}`, `m3_writer_latency_seconds{store}`, `m3_writer_elasticsearch_embeddings_total`, `m3_writer_elasticsearch_short_circuits_total`, `entity_store_presence_drift_detected_total{store}`.

### 3.2 Should

- **FR-Dev 5-S-01.** Elasticsearch document deletion maintenance job: documents with `deleted:true` and tombstoned > 24 hours old are hard-deleted via the Elasticsearch Delete By Query API. Frees space and reduces index noise.
- **FR-Dev 5-S-02.** Neo4j relationship-property index on `source_fk` to accelerate per-source filtering at query time (the parent spec FR-M-07 use case).
- **FR-Dev 5-S-03.** A "trace mode" flag on `entity_routed` events — when `trace=true` is set, Dev 5 records a detailed log of every store call, every metadata write, every short-circuit. Used for debugging.

### 3.3 Could

- **FR-Dev 5-C-01.** Per-store circuit breaker — if a store fails > 50% of writes over a 5-minute window, pause writes to that store and emit `nexus.m3.store_circuit_open`. Resume on health recovery. Avoids Kafka backpressure when a downstream store is fundamentally broken.
- **FR-Dev 5-C-02.** Elasticsearch bulk upsert — accumulate up to 100 documents per Elasticsearch `_bulk` call to amortise the network round-trip cost. Tracked for high-volume tenants.

### 3.4 Won't

- **FR-Dev 5-W-01.** M3 Writers will not store business field values in any of its registers. Provenance hashes and store pointers only.
- **FR-Dev 5-W-02.** M3 Writers will not run entity resolution, survivorship, or any algorithmic content. The writer is mechanical.
- **FR-Dev 5-W-03.** M3 Writers will not call source systems for any reason except via the connector worker for embedding text construction (FR-Dev 5-M-02), and only for fields enumerated in `cdm_entity_storage_config.embed_attrs`.

---

## 4. Non-Functional Requirements

| ID | Requirement | Target |
|---|---|---|
| NFR-D5-01 | Write latency, three stores in parallel, p95 | ≤ 200 ms (Elasticsearch dominates with embedding call; ~60 ms with hash short-circuit) |
| NFR-D5-02 | Throughput | ≥ 200 records/second per replica; KEDA scales 1–6 replicas |
| NFR-D5-03 | `presence_lookup` p95 latency | ≤ 10 ms (Redis hit), ≤ 50 ms (Redis miss + PostgreSQL) |
| NFR-D5-04 | Idempotency | Re-delivery of any event produces no incremental store change beyond observability counters |
| NFR-D5-05 | Cross-store consistency post-reconciliation | ≥ 99.99% of records have `entity_store_presence` matching actual store state |
| NFR-D5-06 | Per-store failure isolation | A 30-minute Elasticsearch outage does not increase Neo4j or TimescaleDB write latencies by more than 10% |

---

## 5. Data Model Ownership

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

CREATE TABLE nexus_system.cdm_entity_storage_config (
  tenant_id           UUID NOT NULL,
  cdm_entity_type     VARCHAR(128) NOT NULL,
  embeddable          BOOLEAN NOT NULL DEFAULT FALSE,
  graph_persistent    BOOLEAN NOT NULL DEFAULT TRUE,
  metricable          BOOLEAN NOT NULL DEFAULT FALSE,
  metric_value_attr   VARCHAR(128),
  metric_time_attr    VARCHAR(128),
  metric_name_template VARCHAR(255),                    -- e.g. "{cdm_entity_type}.created" or "{source_table}.amount"
  embed_attrs         VARCHAR(128)[] NOT NULL DEFAULT '{}',
  pii_excluded_attrs  VARCHAR(128)[] NOT NULL DEFAULT '{}',
  updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (tenant_id, cdm_entity_type)
);

```

`entity_store_presence` has one row per record regardless of how many stores apply — 10M hot records = 10M rows. The partial indexes on FALSE values make the reconciliation scan efficient.

---

## 6. API / Kafka Contracts

### 6.1 Inbound Kafka

- `{tid}.m1.entity_routed` — primary input. Consumer group `m3-writer-entities`.
- `{tid}.m1.entity_removed` — explicit remove operation channel; same handler logic as `operation='REMOVE'` on `entity_routed` (legacy compatibility).
- `nexus.materialization.changed` — for cleanup-on-demotion of a whole entity type. M3 Writers schedules cleanup work via Batch Backfill's framework.
- `nexus.cdm.version_published` — for re-embedding when canonical attributes change. M3 Writers receives the migration plan from Batch Backfill's `cdm-version-migration` DAG.

### 6.2 Outbound Kafka

- `nexus.m3.write_completed` — payload per the lifecycle walkthrough §10:

```json
{
  "tenant_id":               "uuid",
  "cdm_entity_id":           "gr:...",
  "cdm_entity_type":         "Party",
  "operation":               "UPSERT",
  "stores_written":          ["elasticsearch","neo4j","timescaledb"],
  "skipped_stores":          [],
  "embedding_model_version": "openai/text-embedding-3-small@2025-01-15",
  "provenance_hash":         "sha256:...",
  "completed_at":            "iso8601",
  "trace_id":                "string"
}
```

- `nexus.m3.write_failed` — payload includes `failed_stores: [...]` with per-store error codes.
- `nexus.m3.short_circuit_skipped` — when the provenance-hash short-circuit fires; for observability only.
- `nexus.m3.store_circuit_open` / `close` — when per-store circuit breaker triggers.

### 6.3 HTTP / gRPC API

```
GET  /api/v1/m3/storage-config?tenant=...&entity_type=...
PUT  /api/v1/m3/storage-config                  # admin endpoint; modifies cdm_entity_storage_config
```

Presence lookup is no longer an HTTP API. `nexus-query-executor` queries `nexus_system.entity_store_presence` directly via the shared read-only PostgreSQL pool.

### 6.4 RLHF signal feedback (M3 Writers → Materialization Coordinator)

M3 Writers emits a signal event for each record write so Materialization Coordinator's RLHF loop has fine-grained data on materialization cost realisation:

- `nexus.m3.embedding_call_made` — per Elasticsearch embedding call (OpenAI embed → ES index), with cost.
- `nexus.m3.short_circuit_skipped` — per skipped embedding, with cost saved.

Materialization Coordinator's `materialization_signal` table aggregates these into the cost component of the reward function.

---

## 7. CRUD Handling — M3 Writers's Slice

M3 Writers receives operations already labelled by Entity Resolution and acts mechanically.

### 7.1 UPSERT

The standard path:
1. Look up `cdm_entity_storage_config` to determine applicable stores.
2. For each applicable store, call its writer in parallel.
3. Each writer is idempotent (Elasticsearch upsert via `doc_as_upsert: true`, Neo4j MERGE, TimescaleDB INSERT…ON CONFLICT).
4. Update `entity_store_presence` rows.
5. Emit `write_completed`.

### 7.2 RELEVEL — same as UPSERT for promotion

When `materialization_level` is `hot` and the record was previously not written (e.g. promoted from warm), RELEVEL behaves as UPSERT — just write to all applicable stores. The `entity_store_presence` row is upserted with applicable flags set to TRUE.

### 7.3 RELEVEL — cleanup for demotion

When `materialization_level` is `warm` or `cold` and the record was previously written:
1. Read `entity_store_presence` for the record to see which store flags are TRUE.
2. For each flag that is TRUE, issue the corresponding tombstone:
   - Elasticsearch: update document with `deleted: true` metadata flag (vector content unchanged; flag flipped). Hard-delete deferred to nightly deletion job.
   - Neo4j: `MATCH (n {id, tenant_id}) DETACH DELETE n`.
   - TimescaleDB **only on hot→cold**: append `is_deletion=TRUE` row. **Skip on hot→warm** — keep historical metrics.
3. Set the corresponding `entity_store_presence` flags to FALSE after each confirmed tombstone.
4. Emit `write_completed` with `operation=RELEVEL` and `stores_tombstoned=[...]`.

### 7.4 REMOVE — explicit removal

Entity Resolution has determined the GR has zero contributing sources. Same handling as RELEVEL-demotion-to-cold across all stores: tombstone Elasticsearch (mark `deleted: true`), detach Neo4j, append `is_deletion=TRUE` in TimescaleDB. Set all `entity_store_presence` flags to FALSE.

### 7.5 MERGE / SUPERSEDE (paired)

These arrive as two separate events on `entity_routed`. M3 Writers processes them independently but the order matters: SUPERSEDE first (tombstone the loser), then MERGE (upsert the survivor with the merged content).

If the order is reversed (MERGE arrives first), the survivor is upserted correctly but the loser's flags are still TRUE. The query engine sees both via `entity_store_presence` and resolves through `golden_record_redirects` to the survivor — slightly wasteful but correct. The follow-up SUPERSEDE cleans up the loser and flips its flags to FALSE.

To avoid this race in practice, Entity Resolution publishes the SUPERSEDE first with a small delay before publishing MERGE, and M3 Writers's consumer respects message ordering within a partition. Cross-partition ordering is not guaranteed but the redirect mechanism handles it.

### 7.6 The DELETE flow end-to-end

This is the question the user raised most pointedly. End-to-end:

1. Source (Salesforce) emits `op=d` on `001Hs00003xYZABC`.
2. CDC Streaming produces `m1.raw_records` with `source_op=DELETE`, `before_payload` populated.
3. Entity Resolution's algorithm (Entity Resolution spec §7.3) runs:
   - Look up `entity_resolution_index` to find `cdm_entity_id`.
   - Delete provenance rows for this source record.
   - Re-synthesise affected attributes; some may now have new winners or be absent.
   - If GR has zero provenance: `state='tombstoned'`, emit `operation=REMOVE`.
   - Else: emit `operation=UPSERT` (the GR's effective values changed).
4. M3 Writers receives the event and processes per §7.1 (UPSERT) or §7.4 (REMOVE).
5. For UPSERT, the *next* read against the GR sees the updated content. The Elasticsearch document is re-embedded if any text-relevant attributes changed (caught by `provenance_hash` mismatch). The Neo4j node is unchanged (it carries no business fields). TimescaleDB is unchanged (the GR persists; no metric event is emitted by a source DELETE).
6. For REMOVE, the GR is tombstoned everywhere.

The trickiest scenario: a source DELETE causes an attribute to lose its only contributor (e.g. Salesforce was the only source for `domain`, and the Salesforce record is deleted). The provenance row for `domain` is deleted. The next embedding text construction excludes `domain`. The new embedding is materially different from the old. `provenance_hash` mismatches. M3 Writers re-embeds and upserts. The Elasticsearch document is now correct.

---

## 8. Hot/Warm/Cold Handling — M3 Writers's Slice (the writes)

M3 Writers is the executor of every tier movement. The actions per movement:

| Movement | Elasticsearch | Neo4j | TimescaleDB | `entity_store_presence` |
|---|---|---|---|---|
| cold→warm | (no action) | (no action) | (no action) | (no row) |
| warm→hot (per-record promotion) | upsert | MERGE | INSERT | `es_present=TRUE, neo4j_present=TRUE, ts_present=TRUE` (per config) |
| hot→warm (per-record demotion) | tombstone | DETACH DELETE | (preserved) | applicable flags → FALSE |
| hot→cold | tombstone | DETACH DELETE | append `is_deletion=TRUE` | all flags → FALSE |
| warm→cold | (no action — was never written) | (no action) | (no action) | (no row) |

**The "data flows in/out/back again" reality.** A record may oscillate: hot for a quarter, warm during off-season, hot again for a fiscal close, warm afterward, cold after two years. Each transition is a few writes:

- Elasticsearch: mark `deleted: true` → re-upsert (cheap if `provenance_hash` unchanged because the embedding short-circuits; the cost is one upsert).
- Neo4j: detach → re-merge (always cheap; nodes carry no business data).
- TimescaleDB: tombstones accumulate but the time-series itself persists.

Total cost per oscillation per record: dominated by potential re-embedding. If `provenance_hash` is unchanged across the oscillation (the underlying contributing sources didn't change), the embedding cost is zero — M3 Writers detects the match and skips.

This is the answer to the user's concern about constant movement: the design makes oscillation cheap when the underlying truth is stable, and proportionally expensive when the truth changes (which is the case where you'd want to re-embed anyway).

### 8.1 The transition window in `entity_store_presence`

While Materialization Coordinator is running a `materialization-promotion-backfill` or `materialization-demotion-cleanup`, the cohort's records pass through intermediate states: some written, some pending. The query engine reads `entity_store_presence` per record and gets accurate per-record flag state — it does not assume cohort-level uniformity. Records mid-promotion have their flags still FALSE; the query engine falls back to source for those.

The Tenant Admin can see the progression in M4 by observing `entity_store_presence.updated_at` advancing across the cohort as each record's flags are flipped.

---

## 9. Acceptance Criteria

- **AC-D5-01.** Send an UPSERT event for a `Party` (embeddable + graph_persistent + not metricable). Assert document appears in Elasticsearch index `nexus_{tenant_slug}_party` with correct `_id` and metadata; assert node appears in Neo4j with `(id, tenant_id)` matching; assert no TimescaleDB row; assert `entity_store_presence` shows `es_present=TRUE, neo4j_present=TRUE, ts_present=FALSE`; assert `write_completed` emitted with `stores_written=["elasticsearch","neo4j"]`, `skipped_stores=["timescaledb"]`.
- **AC-D5-02.** Send the same UPSERT event 100 times rapidly. Assert the final state is identical to a single processing; assert `entity_store_presence.updated_at` equals the first write timestamp; assert no embedding call is made on any but the first (provenance hash unchanged).
- **AC-D5-03.** Send an UPSERT for a `Transaction.SalesOrder` (graph_persistent + metricable, not embeddable). Assert no Elasticsearch write; assert Neo4j MERGE; assert TimescaleDB INSERT; assert `entity_store_presence` shows `es_present=FALSE, neo4j_present=TRUE, ts_present=TRUE`.
- **AC-D5-04.** Send a REMOVE event for a `Party` previously written. Assert Elasticsearch document is tombstoned (metadata `deleted: true`); assert Neo4j node is `DETACH DELETE`d; assert no TimescaleDB action (Party isn't metricable); assert `entity_store_presence` shows all applicable flags as FALSE.
- **AC-D5-05.** Elasticsearch outage simulation: stop the Elasticsearch client mid-write. Assert subsequent events succeed in Neo4j and TimescaleDB; assert `nexus.m3.write_failed` is emitted with `failed_stores=["elasticsearch"]`. After Elasticsearch recovery, run `m3-reconciliation`; assert affected records are replayed; assert `es_present=TRUE` in `entity_store_presence` for all recovered records.
- **AC-D5-06.** Promote `Party` from warm to hot via D4. Assert all `Party` records in Delta Lake (per the cohort) are eventually present in Elasticsearch and Neo4j; assert all `entity_store_presence` rows show applicable flags TRUE after promotion completes; assert `write_completed` events fire for each.
- **AC-D5-07.** Demote `Party` from hot to warm. Assert all Elasticsearch documents tombstoned (`deleted: true`), all Neo4j nodes detach. Assert TimescaleDB is not touched (warm preserves metrics).
- **AC-D5-08.** Demote `Party` from hot to cold. Assert all stores tombstoned including TimescaleDB `is_deletion=TRUE` append.
- **AC-D5-09.** Send a MERGE+SUPERSEDE pair. Assert survivor's vector is updated, loser's vector is tombstoned; assert Neo4j survivor merge inherits loser's edges; assert `golden_record_redirects` is consulted by the query engine on subsequent reads of the loser's ID.
- **AC-D5-10.** Provenance-hash short-circuit test: send 50 UPSERTs for the same `cdm_entity_id` with no provenance changes. Assert `m3_writer_elasticsearch_short_circuits_total` increments 49 times (only the first call hits OpenAI).
- **AC-D5-11.** `entity_store_presence` batch lookup test: query for 1000 `cdm_entity_id`s via direct SQL; assert p95 ≤ 10 ms; assert all flag values match the underlying table.
- **AC-D5-12.** Cross-store reconciliation test: manually delete an Elasticsearch document outside the platform; run `m3-reconciliation`; assert drift is detected, logged, and the record is replayed; assert `entity_store_presence` is updated.

---

## 10. Open Questions

- **OQ-D5-01.** TimescaleDB cleanup on hot→cold — should it be a soft tombstone (`is_deletion=TRUE`) preserving the time series, or a hard partition drop? Recommend soft tombstone for v0.1; revisit at scale.
- **OQ-D5-02.** Elasticsearch embedding text format — single string with field separators, or a structured representation (some embedding models accept structured input)? Recommend single deterministic string for v0.1.
- **OQ-D5-03.** `presence_lookup` Redis cache TTL of 30 seconds — too long? It means a record demoted moments before a query may still appear `present` in cache. Tradeoff: query engine wastes a store call. Recommend 30 seconds for v0.1; instrument and revisit.
- **OQ-D5-04.** Neo4j edge MERGE on `(start, end, type, source_fk)` — this allows duplicate logical edges from different sources, which the parent spec explicitly wants for provenance. Confirm with M2 query engine team that traversal queries handle this correctly (filtering by `source_fk` per user access).
- **OQ-D5-05.** Embedding model version migration — when OpenAI releases a new model, do we rebuild all vectors at once, or lazily on next write? Recommend lazy via `embedding_model_version` mismatch detection in the short-circuit check.
- **OQ-D5-06.** Cross-tenant Elasticsearch — is there a single Elasticsearch cluster with one index per tenant, or one cluster per tenant? Recommend single cluster, one index per tenant per entity type (`nexus_{tenant_slug}_{entity_type}`) per `NEXUS-Iter2-M3-Elasticsearch-Writer-v0.1.md` convention.
- **OQ-D5-07.** Does `presence_lookup` need to reflect `golden_record_redirects` automatically (i.e. queries against superseded IDs return the survivor's presence)? Recommend yes — M3 Writers reads redirects in the lookup to make the query engine's life simpler. Cost is one extra PostgreSQL query per lookup.

---

## 11. References

- `iter2-dev-overview-and-registers-v0.1.md` — cross-cutting contracts, esp. §4 (registers), §6 (CRUD whole picture), §7 (movement).
- `iter2-cdm-to-aistores-pipeline-v0.1.md` — parent spec, §3.4 (per-store contracts).
- `iter2-record-lifecycle-structured-walkthrough-v0.1.md` — phases 8–10 trace M3 Writers.
- `iter2-system-pipeline-orchestration-v0.1.md` — §6 per-store routing matrix (origin of `cdm_entity_storage_config`).
- `NEXUS-Iter2-SPEC-M3-AIStores-v0.4.md` — original store contracts that M3 Writers extends.
- `NEXUS-Iter2-SPEC-M3-Elasticsearch-Writer-v0.1.md`, `NEXUS-Iter2-SPEC-M3-Neo4j-Writer-v0.1.md`, `NEXUS-Iter2-SPEC-M3-TimescaleDB-Writer-v0.1.md` — per-store specs that this dev spec consolidates.
