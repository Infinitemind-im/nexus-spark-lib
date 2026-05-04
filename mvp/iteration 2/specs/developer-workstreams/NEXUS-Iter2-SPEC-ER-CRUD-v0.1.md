# Iteration 2 — Entity Resolution and Source-CRUD Propagation

**Version:** v0.1 (draft)
**Date:** 2026-04-27
**Companion to:** `iter2-dev-overview-and-registers-v0.1.md`

---

## 📦 Codebase & Deployment

> **One repository, many deployments.** All NEXUS services live in the `nexus-platform` monorepo. You are contributing an algorithm library to a shared codebase — your code runs inside another service's process, not in a process you deploy yourself.

| | |
|---|---|
| **Deployed inside** | `nexus-spark-transformer` (Dev 1 deploys and operates this service) |
| **Monorepo path** | `libs/nexus_spark_lib/nexus_spark_lib/transform/resolve/` |
| **Language / runtime** | Python 3.11 · PySpark 3.5 (called as Spark job stages) |
| **Iteration 2 owner** | Dev 3 (6 person-weeks — the most complex individual stream) |
| **How your code ships** | Dev 3 commits ER logic to `libs/nexus_spark_lib/`. Dev 1 imports and calls it from `services/nexus-spark-transformer/`. There is no separate ER service process — the algorithm runs as stages within the Spark Transformer. |

> ⚠️ **Coordination point:** Dev 3 must freeze the `nexus_spark_lib.transform.resolve` API by Week 2 so Dev 1 can integrate it into the streaming Spark application. See §2 Dependencies.

---

## 1. Scope

The Entity Resolution component owns the platform's identity layer:

- The three-signal entity resolution algorithm (deterministic / probabilistic with LSH + similarity functions / graph-based transitive lift) implemented as the `nexus_spark_lib.transform.resolve` function called from CDC Streaming streaming and Batch Backfill batch.
- Stage 3 Golden Record synthesis: survivorship rule application, per-attribute provenance writes, provenance hash computation.
- The Golden Record state machine (active / provisional / superseded / tombstoned) and its five transitions (CREATE, UPDATE, MERGE, SPLIT, TOMBSTONE).
- **Source-CRUD propagation** — the algorithm that translates a source-side INSERT, UPDATE, or DELETE into the corresponding effect on the GR, including the hard case where a DELETE removes the only contributing source for an attribute and the GR's effective value set changes.
- The four canonical-store registers: `entity_resolution_index`, `golden_record_provenance`, `golden_records_index`, `golden_record_redirects`.

This component does **not** own: source extraction (CDC Streaming / Batch Backfill), policy evaluation (Materialization Coordinator), or any AI store writes (M3 Writers). Entity Resolution produces `transformed_records`; everything downstream is someone else's problem.

---

## 2. Dependencies

| Depends on | What for | When needed |
|---|---|---|
| Batch Backfill | `nexus_spark_lib` framework into which Entity Resolution plugs Stages 2 + 3 | Week 0–1 |
| Materialization Coordinator | `cdm_entity_materialization` to know ER depth per entity type | Week 1 |
| M3 Writers | None inbound from M3 Writers | n/a |
| Platform | Neo4j accessible from Spark executors for Signal C | Week 2 |
| Platform | PostgreSQL accessible with low latency for `entity_resolution_index` lookups | Week 0 |

---

## 3. Functional Requirements (MoSCoW)

### 3.1 Must

- **FR-Dev 3-M-01.** Implement the three-signal ER algorithm in `nexus_spark_lib.transform.resolve`:

  - **Signal A — Deterministic match** on the entity type's `deterministic_id_columns` (e.g. `tax_id`, `domain`, `duns_number`). A single match auto-applies with confidence 1.000.
  - **Signal B — Probabilistic match** on canonical attributes after LSH blocking. Per-attribute similarity functions per the parent spec §3.2 (Jaro-Winkler for names, Levenshtein-normalised for free-text, Soundex+Metaphone for phonetic name fallback, exact post-E.164 for phones, local-part Levenshtein × domain exact for emails). Combined score uses tenant-and-entity-type weights from `nexus_system.er_thresholds`.
  - **Signal C — Graph lift** for review-band matches: traverse Neo4j up to 2 hops, lift score by +0.05 per shared neighbour at depth 1, +0.02 at depth 2, capped at +0.10 total. Lifts are weighted by traversed edge confidence.
- **FR-Dev 3-M-02.** Match outcomes route as follows: above auto-apply threshold → write to `entity_resolution_index` and proceed to synthesis; review band → write to `er_review_queue` with `provisional=true` and proceed to synthesis with provisional flag; below review band → assign new `cdm_entity_id` (CREATE).
- **FR-Dev 3-M-03.** Materialization-aware ER depth: hot records run all three signals; warm records run Signal A only; cold records do not reach Dev 3 at all (filtered before publication of `transformed_records`). The level is read from the `materialization_level` field on the input event payload (set by D4-via-broadcast in Stage 0).
- **FR-Dev 3-M-04.** Implement Stage 3 synthesis: for each canonical attribute the contributing record provides, evaluate the survivorship rule from `nexus_system.survivorship_rules`. Insert / replace / delete `golden_record_provenance` rows accordingly. Compute `provenance_hash` as SHA-256 over the canonicalised provenance summary.
- **FR-Dev 3-M-05.** Implement Golden Record state transitions:

  - **CREATE.** No match → generate `cdm_entity_id` per parent spec FR-M-05 (`gr:` + sha256 of `tenant_id || cdm_entity_type || canonical_blocking_key`). Insert into `golden_records_index` with `state='active'`. Insert N provenance rows.
  - **UPDATE.** Match found → no row in `golden_records_index` changes. Provenance changes per survivorship.
  - **MERGE.** Two existing GRs determined to be the same. Pick survivor by min(`created_at`), tie-break lexically. Update loser's `state='superseded'`. Insert into `golden_record_redirects`. Rewrite all `entity_resolution_index` rows pointing at loser. Emit `transformed_records` with `operation='REMERGE'` for survivor, `operation='SUPERSEDE'` for loser.
  - **SPLIT.** Steward action only. Reassign provenance rows per the steward's partition. Mark old GR `tombstoned`. Insert redirect entries pointing both new IDs to a special "split-history" record.
  - **TOMBSTONE.** All contributing source records deleted (every provenance row gone). Update `state='tombstoned'`. Emit `operation='REMOVE'`.
- **FR-Dev 3-M-06.** Source-CRUD propagation algorithm (the user-flagged hard case). For an incoming `m1.raw_records` event with `source_op ∈ {INSERT, UPDATE, DELETE, SNAPSHOT_READ, RELEVEL}`:

  - **INSERT / SNAPSHOT_READ.** Run Signal A → B → C; resolve to existing GR (UPDATE) or create new (CREATE). Synthesise.
  - **UPDATE.** Look up `entity_resolution_index` to get `cdm_entity_id`. Compute attribute-level diff between `before_payload` and `after_payload`. If any ER-relevant attribute changed (the entity type's deterministic identifiers, or attributes weighted ≥ 0.20 in Signal B), re-run ER. Otherwise short-circuit — only synthesis re-runs. If re-run ER produces a different `cdm_entity_id`, the previous association is removed (effectively a SPLIT) and a new one made.
  - **DELETE.** Per the algorithm in §7. The provenance rows for this source record are deleted; affected attributes are re-synthesised from remaining sources; if the GR has zero provenance left, transition to `tombstoned`.
  - **RELEVEL.** Look up `cdm_entity_id` from `entity_resolution_index`. Skip ER (already resolved). Re-synthesise (the survivorship may have changed since last write). Emit `transformed_records` with `operation='UPSERT'`.
- **FR-Dev 3-M-07.** `transformed_records` payload includes the resolved `cdm_entity_id`, the `provenance_summary` (with attribute-source pointers and hash), the `operation` (`UPSERT` | `MERGE` | `SUPERSEDE` | `REMOVE` | `RELEVEL`), the originating `contributing_record`, and the `materialization_level` carried through from upstream.
- **FR-Dev 3-M-08.** All writes to `entity_resolution_index`, `golden_record_provenance`, `golden_records_index`, `golden_record_redirects` are idempotent. Re-delivery of any event produces no incremental side effects.
- **FR-Dev 3-M-09.** Survivorship is applied **deterministically**. Given the same set of contributing source records and the same rule config, the same `golden_record_provenance` rows result, regardless of the order events were processed. Achieved by: storing `observed_at` per provenance row and re-evaluating against the full set on every change rather than incrementally.
- **FR-Dev 3-M-10.** Survivorship rule changes (a row in `survivorship_rules` is updated) trigger D2's `survivorship-rebuild` DAG — The Entity Resolution component owns the *re-evaluation logic* called by that DAG, but the DAG itself is owned by D2.

### 3.2 Should

- **FR-Dev 3-S-01.** ER diagnostic mode: a per-record trace mode that records every signal evaluation, every score computation, every threshold check to a debug log table `nexus_system.er_trace`. Used by data stewards investigating bad merges.
- **FR-Dev 3-S-02.** Per-tenant per-entity-type ER threshold tuning recommendations based on observed override rates (parent spec FR-S-02). Dev 3 writes signal evidence; the actual recommendation surface is M4's UI.
- **FR-Dev 3-S-03.** A "merge candidate" queue in `nexus_system.er_review_queue` that surfaces pairs the system thinks might be the same entity but isn't sure enough to auto-apply. Stewards review and decide.

### 3.3 Could

- **FR-Dev 3-C-01.** ML-augmented Signal B — replace the linear weighted score with a small classifier trained on past steward decisions. Tracked for v0.3.
- **FR-Dev 3-C-02.** Approximate nearest neighbour (ANN) blocking via Elasticsearch kNN for very-high-cardinality entity types. Tracked for v0.3.

### 3.4 Won't

- **FR-Dev 3-W-01.** Entity Resolution will not split GRs autonomously. Splits are always steward-driven. The system never decides on its own to undo a merge.
- **FR-Dev 3-W-02.** Entity Resolution will not store any business field values outside provenance hashes and the deterministic identifier columns referenced by Signal A. The Virtual CDM principle is preserved.

---

## 4. Non-Functional Requirements

| ID | Requirement | Target |
|---|---|---|
| NFR-D3-01 | Stages 2 + 3 latency per record (streaming) | p95 ≤ 5 seconds |
| NFR-D3-02 | Throughput in streaming mode | ≥ 5,000 records/second per Spark executor |
| NFR-D3-03 | LSH blocking effectiveness | ≥ 99% of true matches are in the same bucket as their candidate |
| NFR-D3-04 | Idempotency under replay | Re-delivery of any event produces no observable downstream effect |
| NFR-D3-05 | Survivorship determinism | Two runs with the same inputs produce identical provenance row sets |
| NFR-D3-06 | DELETE propagation correctness | 100% of cases where a source DELETE leaves the GR in a stale state are caught by acceptance tests |
| NFR-D3-07 | MERGE consistency | Post-MERGE, no `entity_resolution_index` row points at the superseded GR; redirects route correctly |

---

## 5. Data Model Ownership

The Entity Resolution component owns the four canonical-store registers (defined across the parent specs; consolidated DDL here for the migration):

```sql
CREATE TABLE nexus_system.entity_resolution_index (
  cdm_entity_id        VARCHAR(48) NOT NULL,
  tenant_id            UUID NOT NULL,
  connector_id         VARCHAR(64) NOT NULL,
  source_system        VARCHAR(64) NOT NULL,
  source_table         VARCHAR(255) NOT NULL,
  source_record_id     VARCHAR(255) NOT NULL,
  confidence           NUMERIC(4,3) NOT NULL,
  resolution_method    VARCHAR(32) NOT NULL,           -- 'spark_deterministic' | 'spark_probabilistic' | 'spark_graph' | 'human' | 'merge_inheritance'
  resolved_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  provisional          BOOLEAN NOT NULL DEFAULT FALSE,
  PRIMARY KEY (tenant_id, connector_id, source_table, source_record_id)
);
CREATE INDEX idx_eri_cdm ON nexus_system.entity_resolution_index(tenant_id, cdm_entity_id);

CREATE TABLE nexus_system.golden_records_index (
  cdm_entity_id      VARCHAR(48) PRIMARY KEY,
  tenant_id          UUID NOT NULL,
  cdm_entity_type    VARCHAR(128) NOT NULL,
  state              VARCHAR(16) NOT NULL CHECK (state IN ('active','provisional','superseded','tombstoned')),
  created_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  state_changed_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  state_change_reason VARCHAR(64)
);
CREATE INDEX idx_gri_tenant_type_state ON nexus_system.golden_records_index(tenant_id, cdm_entity_type, state);

-- golden_record_provenance and golden_record_redirects DDL is in the parent
-- iter2-cdm-to-aistores-pipeline-v0.1.md §4.2 and iter2-system-pipeline-orchestration-v0.1.md §5.4

CREATE TABLE nexus_system.er_review_queue (
  pair_id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id        UUID NOT NULL,
  cdm_entity_type  VARCHAR(128) NOT NULL,
  candidate_a_id   VARCHAR(48) NOT NULL,            -- existing cdm_entity_id or new candidate
  candidate_b_id   VARCHAR(48) NOT NULL,
  combined_score   NUMERIC(4,3) NOT NULL,
  signal_breakdown JSONB NOT NULL,
  status           VARCHAR(16) NOT NULL DEFAULT 'pending'
                   CHECK (status IN ('pending','approved','rejected','dismissed_by_graph_lift')),
  reviewed_by      VARCHAR(64),
  reviewed_at      TIMESTAMPTZ,
  created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE nexus_system.er_override_log (
  log_id          BIGSERIAL PRIMARY KEY,
  tenant_id       UUID NOT NULL,
  cdm_entity_type VARCHAR(128) NOT NULL,
  override_kind   VARCHAR(32) NOT NULL,             -- 'auto_then_corrected' | 'manual_then_confirmed' | etc.
  original_method VARCHAR(32) NOT NULL,
  corrected_by    VARCHAR(64),
  observed_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE nexus_system.deterministic_id_columns (
  tenant_id        UUID NOT NULL,
  cdm_entity_type  VARCHAR(128) NOT NULL,
  attribute_name   VARCHAR(128) NOT NULL,
  PRIMARY KEY (tenant_id, cdm_entity_type, attribute_name)
);

CREATE TABLE nexus_system.entity_blocking_rules (
  tenant_id        UUID NOT NULL,
  cdm_entity_type  VARCHAR(128) NOT NULL,
  blocking_formula TEXT NOT NULL,                   -- e.g. "lower(left(legal_name,4)) || ':' || left(domain,6)"
  PRIMARY KEY (tenant_id, cdm_entity_type)
);

CREATE TABLE nexus_system.er_thresholds (
  tenant_id        UUID NOT NULL,
  cdm_entity_type  VARCHAR(128) NOT NULL,
  weights          JSONB NOT NULL,                  -- {"legal_name": 0.55, ...}
  auto_apply_threshold NUMERIC(4,3) NOT NULL,
  review_lower_bound NUMERIC(4,3) NOT NULL,
  PRIMARY KEY (tenant_id, cdm_entity_type)
);
```

---

## 6. API / Kafka Contracts

### 6.1 Inbound

`m1.int.raw_records` (from CDC Streaming + Batch Backfill) — see CDC Streaming spec §6.1.

### 6.2 Outbound: `m1.int.transformed_records`

```json
{
  "tenant_id":            "uuid",
  "cdm_entity_id":        "gr:...",
  "cdm_entity_type":      "Party",
  "operation":            "UPSERT|MERGE|SUPERSEDE|REMOVE|RELEVEL",
  "contributing_record": {
    "source_system":      "salesforce",
    "source_record_id":   "001Hs00003xYZABC",
    "source_table":       "Account",
    "source_op":          "INSERT|UPDATE|DELETE|SNAPSHOT_READ|RELEVEL",
    "source_ts":          "iso8601"
  },
  "provenance_summary": {
    "contributing_sources": ["sap_erp", "salesforce"],
    "attribute_provenance": {
      "legal_name":        "sap_erp:0000012847",
      "industry":          "salesforce:001Hs00003xYZABC",
      "...": "..."
    },
    "provenance_hash":     "sha256:..."
  },
  "materialization_level": "hot",
  "operation_metadata": {
    "merge_target":         "gr:other-id",            // only for MERGE
    "supersession_target":  "gr:survivor-id",         // only for SUPERSEDE
    "deletion_reason":      "all_sources_removed",    // only for REMOVE
    "split_partition_id":   "uuid"                    // only for split-related operations
  },
  "headers": {
    "schema_version":     "string",
    "backfill_batch_id":  "string|null",
    "trace_id":           "string"
  }
}
```

### 6.3 Outbound: control topics

- `nexus.er.review_queued` — when a pair lands in review band; consumed by M4 for the review UI.
- `nexus.gr.state_changed` — when a GR transitions state; consumed by Governance.
- `nexus.gr.merged` — explicit merge event; consumed by Governance and by the materialization recommendation log (a merge can shift cohort sizes).
- `nexus.gr.split` — explicit split event.

---

## 7. CRUD Handling — Entity Resolution's Slice (the core)

### 7.1 INSERT / SNAPSHOT_READ

Resolve → CREATE or UPDATE → synthesise → emit `UPSERT`. This is the path the lifecycle walkthrough traced.

### 7.2 UPDATE

```
1. Look up entity_resolution_index by (source_system, source_record_id) → cdm_entity_id.
2. Compute attribute-level diff between before_payload and after_payload.
3. If diff includes any ER-relevant attribute (deterministic IDs or weighted ≥ 0.20 in Signal B):
     re-run Signal A → B → C against the new values.
     If new resolution differs from existing cdm_entity_id:
         emit a SPLIT-shaped operation (this source migrates to the new GR;
         the old GR loses this source's provenance; if old GR now has zero
         provenance, transition to tombstoned).
4. Re-run synthesis with the new attribute values.
   For each attribute the source contributed:
     - if the survivorship rule selects this source's new value: replace the provenance row.
     - if a different source's value still wins: drop this source's provenance row for that attribute (if any).
5. Recompute provenance_hash.
6. Emit UPSERT.
```

The attribute-level diff is the optimisation that prevents every UPDATE from being expensive. Most updates (a `last_modified_at` bump, a status flag toggle) don't touch ER-relevant attributes and short-circuit at step 3.

### 7.3 DELETE — the algorithm in detail

This is the case the user flagged as the hard one. A source DELETE means "this source no longer contributes." It does **not** automatically mean "delete the Golden Record" because other sources may still contribute.

```
1. Look up entity_resolution_index by (source_system, source_record_id) → cdm_entity_id.
   If not found:
     log a warning (this should not happen in a healthy system) and ack.
2. Read all golden_record_provenance rows for cdm_entity_id.
3. Identify which provenance rows came from the deleted source record:
     SELECT * FROM golden_record_provenance
     WHERE cdm_entity_id = ?
       AND source_system = ?
       AND source_record_id = ?
4. For each such row: DELETE.
5. For each attribute_name affected by step 4 (i.e. attributes that this source
   used to win for):
     re-run survivorship for this attribute with the remaining contributing
     source records' values:
       - read all entity_resolution_index rows for cdm_entity_id
         to enumerate the surviving source records
       - for each surviving source, read its current attribute value
         (via Delta Lake — or via the connector for cold-tier records)
       - apply the survivorship rule
     if a winner exists:
       INSERT a new provenance row with the new winner's pointer.
     if no winner exists (no remaining source has this attribute):
       no row is inserted; the attribute is now absent from the GR.
6. After step 5, count provenance rows for the GR.
   If count == 0:
     UPDATE golden_records_index SET state='tombstoned',
       state_changed_at=NOW(),
       state_change_reason='all_sources_removed'
     WHERE cdm_entity_id = ?
     emit transformed_records with operation='REMOVE'.
   Else:
     recompute provenance_hash.
     emit transformed_records with operation='UPSERT'
       (the GR's effective value set has changed).
7. DELETE the entity_resolution_index row for the deleted source record.
```

A subtlety the implementation must get right: step 5's "current attribute value" for a remaining source must be the *current* value, not the value the source had when it was last seen. If Salesforce contributed `industry='Manufacturing'` at the same moment SAP contributed `industry='Heavy Industry'`, and Salesforce's record is later deleted, the new survivor under `source_priority` rules is SAP, but with SAP's *current* value, which may have changed since SAP first contributed. For sources at hot or warm level, the value is in Delta Lake. For sources at cold level, Entity Resolution must call the connector to fetch the live value (out-of-band; not on the streaming critical path — the DELETE handler queues this for a follow-up sweep).

### 7.4 RELEVEL

```
1. Look up entity_resolution_index → cdm_entity_id (if no row exists, treat as INSERT).
2. Skip ER (already resolved).
3. Re-run synthesis. Survivorship rules may have changed since the last write.
4. Emit UPSERT.
```

This is the cheapest path — no ER cost, just a re-write of provenance and a downstream re-projection.

### 7.5 MERGE (batch ER, rare in streaming)

```
1. Identify the pair: (cdm_entity_id_a, cdm_entity_id_b).
2. Pick survivor: smallest created_at; tie-break lexically on cdm_entity_id.
3. Begin transaction:
     - Reassign entity_resolution_index rows from loser to survivor.
     - For each loser provenance row, evaluate against survivor's existing
       provenance:
         if survivor doesn't have the attribute or the loser's source wins
         survivorship: insert into survivor, delete from loser.
         else: just delete from loser.
     - Update loser's golden_records_index.state = 'superseded',
       state_change_reason = 'merged_into:' || survivor_id.
     - Insert into golden_record_redirects (loser_id → survivor_id).
   Commit.
4. Recompute provenance_hash on survivor.
5. Emit transformed_records:
     - operation='REMERGE' for survivor (M3 Writers re-projects with new content).
     - operation='SUPERSEDE' for loser (M3 Writers tombstones the loser's previous
       store presence).
```

### 7.6 SPLIT (steward action only)

```
1. Steward provides: cdm_entity_id_to_split, partition_a_source_records,
   partition_b_source_records.
2. Validate the partition: every source record must be in exactly one partition.
3. Generate cdm_entity_id for partition_b (partition_a inherits the original ID).
4. Begin transaction:
     - Reassign entity_resolution_index rows for partition_b sources to the new ID.
     - Reassign golden_record_provenance rows for partition_b sources to the new ID.
     - Insert new golden_records_index row for partition_b ID, state='active'.
     - Mark the original GR as 'split-history-preserved' (state remains active for
       partition_a; the split is recorded in golden_record_split_history — a new
       table per OQ-SYS-03).
     - For DOC_MENTIONS edges to the original GR: re-evaluate by Stage 5d of the
       document pipeline (out of scope here; Entity Resolution emits a 'mention_reassessment_required'
       event for each affected document).
   Commit.
5. Recompute provenance_hash on both new GRs.
6. Emit transformed_records:
     - operation='UPSERT' for partition_a (GR content changed).
     - operation='UPSERT' for partition_b (new GR).
```

---

## 8. Hot/Warm/Cold Handling — Entity Resolution's Slice

Entity Resolution reads the `materialization_level` from each incoming event and applies the depth rule:

| Level | Stages run | Outputs |
|---|---|---|
| `hot` | 1 + 2 (full ER) + 3 | `transformed_records` event |
| `warm` | 1 + 2 (Signal A only) | `entity_resolution_index` row only; no synthesis; no `transformed_records` |
| `cold` | none (filtered upstream) | nothing |

When a record is promoted from warm to hot, Entity Resolution receives it as `RELEVEL` and runs full Stages 2 + 3 — Signals B and C, full synthesis. The Signal A result already in `entity_resolution_index` is the starting point; if Signals B + C agree, no change. If they discover the warm-level deterministic-only resolution was wrong (a fuzzy match would have caught it), Entity Resolution re-resolves and the previous association is corrected.

When a record is demoted from hot to warm, Entity Resolution does *not* delete provenance — the Delta Lake state and provenance survive. M3 Writers cleans up the AI store presence; Entity Resolution's data stays. Re-promotion can rehydrate from Entity Resolution's untouched provenance.

When a GR has contributing sources at multiple levels, the GR is "live" if any contributor is hot. Stage 3 synthesis runs whenever any contributing source updates, regardless of that source's level. The `materialization_level` on the *event* governs whether ER runs in full; the *GR* level is the highest level among its contributors.

---

## 9. Acceptance Criteria

- **AC-D3-01.** Two records ("Globex Corp." in Salesforce, "Globex Corporation" in SAP) arrive separately. Assert ER produces a single `cdm_entity_id`, with Signal A returning no match and Signal B auto-applying at score ≥ 0.92. Assert provenance shows SAP winning `legal_name` and `tax_id`, Salesforce winning `industry` and `domain`.
- **AC-D3-02.** A third record (Zendesk "Globex" with domain "globex.com") arrives. Signal A matches deterministically on `domain`. Assert merge into the same `cdm_entity_id` with confidence 1.000.
- **AC-D3-03.** UPDATE the Salesforce record's `legal_name` from "Globex Corp." to "Globex International Inc."; assert ER re-runs (legal_name is weighted ≥ 0.20); assert the new Jaro-Winkler against SAP's "Globex Corporation" drops below the threshold; assert SPLIT-shaped behaviour: Salesforce gets a fresh `cdm_entity_id`, original GR keeps SAP and Zendesk.
- **AC-D3-04.** DELETE the Salesforce record. Assert provenance rows for `industry`, `domain`, `primary_phone` (Salesforce-sourced) are deleted. Assert survivorship re-runs: SAP doesn't have `industry`, so the attribute disappears from the GR. Assert GR survives because SAP and Zendesk still contribute. Assert `transformed_records` emitted with `operation='UPSERT'`.
- **AC-D3-05.** DELETE all three contributing records over time. Assert the last DELETE transitions GR to `tombstoned` and emits `operation='REMOVE'`.
- **AC-D3-06.** Trigger an `er-reindex` after lowering the auto-apply threshold from 0.92 to 0.88. Assert previously review-band matches now auto-apply as MERGEs. Assert `golden_record_redirects` rows are inserted. Assert `entity_resolution_index` rows for losers are reassigned to survivors.
- **AC-D3-07.** Steward triggers a SPLIT on a GR with bad merge. Assert two new GRs are created (or one new ID assigned to the smaller partition); assert provenance is partitioned correctly; assert `mention_reassessment_required` events are emitted for documents that mention the original GR.
- **AC-D3-08.** Restart the streaming Spark application during ER processing of a 10K-record batch; assert no GR is duplicated, no provenance row is inserted twice, no `transformed_records` event is emitted twice (idempotency under replay).
- **AC-D3-09.** Run two independent ER processing orders for the same set of input records (different Kafka delivery orders); assert the final `golden_record_provenance` row set is identical (determinism).
- **AC-D3-10.** Demote `Party` from hot to warm. Send an UPDATE for an existing `Party` record. Assert Signal A only runs (FR-Dev 3-M-03). Assert no `transformed_records` event is emitted (warm-level updates do not trigger downstream).
- **AC-D3-11.** Promote `Party` back to hot. D2 re-emits records with `source_op=RELEVEL`. Assert Entity Resolution runs full ER + synthesis and emits `UPSERT`.

---

## 10. Open Questions

- **OQ-D3-01.** Should the cold-tier value fetch in DELETE handler step 5 be synchronous (block the streaming critical path) or queued? Recommend queued, with a follow-up `gr.attribute_resync_required` event consumed by a small reconciler.
- **OQ-D3-02.** When UPDATE causes an unwanted SPLIT (the rebrand case), what's the user-facing signal? Currently Entity Resolution splits silently. Recommend emitting `nexus.gr.unwanted_split_suspected` with confidence; M4 surfaces it for steward attention.
- **OQ-D3-03.** Provisional GRs from review-band matches — when do they auto-expire if the steward doesn't act? Recommend 30 days, configurable per tenant. Auto-expired provisional → remains as separate GR with `provisional=false`.
- **OQ-D3-04.** Signal C calls Neo4j synchronously per record. Cost concern at scale. Recommend a per-batch cache that pre-fetches one-hop neighbourhoods for all records in the batch, hitting Neo4j once.
- **OQ-D3-05.** `er_review_queue` UI surface — does M4 already plan a review workbench? Coordinate with M4 dev on the field set surfaced.
- **OQ-D3-06.** The MERGE inheritance via `golden_record_redirects` is transitive — A→B and B→C should resolve A→C. Implement at read time (recursive lookup) or at merge time (collapse chains)? Recommend collapse-at-merge-time for read efficiency.

---

## 11. References

- `iter2-dev-overview-and-registers-v0.1.md` — cross-cutting contracts.
- `iter2-cdm-to-aistores-pipeline-v0.1.md` — parent spec, §3.2 (ER), §3.3 (synthesis).
- `iter2-record-lifecycle-structured-walkthrough-v0.1.md` — phases 5–7 trace Entity Resolution's path.
- `iter2-system-pipeline-orchestration-v0.1.md` — §5 GR state machine.
- `NEXUS-Iter2-LIB-NexusCore-v0.3.md` — `CdmEntity` model that Entity Resolution's outputs conform to.
