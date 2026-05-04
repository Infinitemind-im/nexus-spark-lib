# Iteration 2 Spec — CDM-to-AIStores Pipeline (Best-of-Breed)

**Version:** v0.1 (draft)
**Date:** 2026-04-27
**Owner:** Architecture / Data Intelligence
**Scope:** The end-to-end pipeline that runs *after* a record has a stable CDM mapping and *before* it is queryable from Pinecone, Neo4j, and TimescaleDB. Covers structured records and documents.
**Status:** Draft for review. Supersedes the implicit M1→M3 seam currently distributed across `NEXUS-Iter2-SPEC-ServiceTopology-v0.4.md`, `NEXUS-Iter2-CDM-Mapper-v0.3.md`, and `NEXUS-Iter2-M3-AIStores-v0.4.md`.
**Source synthesis:** Best-of-breed reconciliation between the C.1.2 grant proposal (richer ETL / document / tier model) and the existing Iteration 2 specs (Virtual CDM principle, idempotent writers, Kafka topic shape). See "Reconciliation notes" at the bottom.

---

## 1. Overview

This spec closes the seam between the moment a CDM mapping is approved for an entity and the moment the corresponding records, plus their documents, are queryable from the three AI stores. It defines five pipeline stages, the services that own each, the events that connect them, the new data structures required, and the failure / replay semantics.

Three principles govern the design:

The **Virtual CDM principle** from `NEXUS-Iter2-M3-AIStores-v0.4.md` is preserved: no business field values are duplicated into Pinecone, Neo4j, or TimescaleDB. The three stores hold references and derived structures only; live business values are fetched from source by `nexus-query-executor` phase 2 at query time.

A **materialization model** governs *whether and how deeply* an entity is processed. Three materialization levels — hot, warm, cold — replace the implicit "everything flows through ETL" assumption. Only hot-level entities are pushed through the full pipeline. Warm-level entities are catalogued but not embedded or graph-linked. Cold-level entities are not processed at all and are retrieved from source on demand by the query layer.

A **two-track pipeline** runs in parallel: a structured-record track and a document track. Both feed the same three AI stores using the same event vocabulary, but their internal stages differ.

### 1.1 Naming reconciliation — the word "tier"

The codebase already uses "Tier 1 / Tier 2 / Tier 3" for CDM mapping confidence (`NEXUS-Iter2-CDM-Mapper-v0.3.md`). To avoid overloading the term, this spec uses **"materialization level"** (hot / warm / cold) for the new concept. Wherever a service config or table column references the new concept, the field name is `materialization_level`, never `tier`. Existing `tier` columns and code paths are left untouched.

### 1.2 Pipeline at a glance

```
              CDM published (schema approved by M4)
                          │
                          ▼
    ┌─────────────────────────────────────────────┐
    │  Stage 0 — Materialization Level Resolution │   (per entity, per tenant)
    └─────────────────────────────────────────────┘
                          │
            ┌─────────────┴─────────────┐
            ▼                           ▼
  Structured Track               Document Track
  ───────────────                ─────────────────
  Stage 1 — Normalise            Stage 1d — Parse
  Stage 2 — Resolve (3 signals)  Stage 2d — Classify
  Stage 3 — Synthesise Golden    Stage 3d — Chunk
  Stage 4 — Project to stores    Stage 4d — Extract entities
                                 Stage 5d — Mention-to-entity resolve
                                 Stage 6d — Project to stores
            └─────────────┬─────────────┘
                          ▼
              ┌──────────────────────┐
              │  Pinecone │ Neo4j │  │
              │  TimescaleDB         │
              └──────────────────────┘
                          │
                          ▼
                nexus.m3.write_completed
```

---

## 2. Functional Requirements (MoSCoW)

### 2.1 Must

- **FR-M-01.** Every CDM entity in a tenant's catalogue carries a `materialization_level ∈ {hot, warm, cold}`. The default for newly published entities is `warm`. The level is mutable and stored in `nexus_system.cdm_entity_materialization` (new table, see §4.1).
- **FR-M-02.** Only entities at `materialization_level = 'hot'` flow through Stages 1–4 of the structured track and Stages 1d–6d of the document track. `warm` entities are catalogued and written to Delta Lake by `nexus-m1-worker` but are not embedded, graph-linked, or projected to TimescaleDB. `cold` entities are catalogued only — no extraction beyond what `nexus-discovery` already performs.
- **FR-M-03.** Promotion (cold→warm, warm→hot) and demotion are triggered both manually (by a Tenant Admin via M4) and automatically by usage signals collected by `nexus-query-executor`. Auto-promotion thresholds are stored per tenant in `nexus_system.tenant_configs`.
- **FR-M-04.** The structured track must perform entity resolution using **three signals** (deterministic, probabilistic, graph-based), in that order, with Locality-Sensitive Hashing for blocking. The signals and their thresholds are specified in §3.2.
- **FR-M-05.** Every Golden Record assignment must be reproducible and idempotent. The Golden Record ID `cdm_entity_id` is computed as `sha256(tenant_id || cdm_entity_type || canonical_blocking_key)` truncated to 128 bits and prefixed with `gr:`. Ties are broken deterministically by the smallest source record id.
- **FR-M-06.** Every Golden Record must carry **per-attribute survivorship provenance** — for each canonical attribute, the source system, source record id, and timestamp from which the winning value originated. Provenance lives in `nexus_system.golden_record_provenance` (new table, see §4.2). The Virtual CDM principle is preserved: provenance points at source records, it does not duplicate values.
- **FR-M-07.** Every Neo4j edge must carry a `source_fk` property: the source-system-qualified foreign key (or extraction reference) that produced it. Format: `"{source_system}:{source_table}.{column}"` for structured sources, `"{source_system}:doc_extraction"` with an additional `chunk_id` property for document-derived edges.
- **FR-M-08.** The document track must implement six stages — parse, classify, chunk, extract, resolve, project. The track is owned by a new service `nexus-doc-processor` (see §6). Document chunks land in the existing `{tenant_id}-documents` Pinecone index (currently provisioned but unused per `NEXUS-Iter2-M3-AIStores-v0.4.md`).
- **FR-M-09.** A document mention can create a new Golden Record only when **all four** gating conditions are met (see §3.5). Otherwise the mention is stored as a `DOC_MENTIONS` edge to the unresolved-mention placeholder, queued in `nexus_system.mention_review_queue` (new table, see §4.4).
- **FR-M-10.** A document can contribute attributes to an existing Golden Record only when **both** of the following hold: (a) no structured source contributes that attribute at all; (b) the document satisfies the same four gating conditions as in FR-M-09. Contributed values are written to provenance as `attribute_source_kind = 'document'` and inherit the strict half of the visibility policy by default (see FR-M-11).
- **FR-M-11.** The Golden Record visibility policy is **permissive by default** — an attribute is visible to a user if they can read at least one of its contributing source records. A tenant administrator can override per-attribute to **strict** (must have access to *all* contributing sources). The policy is enforced at query time by `nexus-query-executor`, not at write time.
- **FR-M-12.** The pipeline emits exactly two terminal events per record: `nexus.m3.write_completed` on success across all three stores, `nexus.m3.write_failed` on unrecoverable failure to any store. Partial success does not roll back the Kafka offset; recovery is by topic replay (see §7).
- **FR-M-13.** Idempotency is preserved through all stages: re-delivering any inbound event produces the same store state. Stage-2 ER results, Stage-3 Golden Record assignments, and Stage-4 store writes all carry deterministic IDs; reruns are no-ops.

### 2.2 Should

- **FR-S-01.** Provide a "dry-run" mode for the entire pipeline driven by a `dry_run: true` flag on `{tid}.m1.entity_routed`. In dry-run, all stages execute but no writes are committed to PostgreSQL, Pinecone, Neo4j, or TimescaleDB. Used for tier promotion impact analysis and ER threshold tuning.
- **FR-S-02.** ER probabilistic match thresholds are tuned per tenant per entity type. The platform tracks the rate of overridden auto-merges (`nexus_system.er_override_log`) and surfaces a recommended threshold adjustment in the M4 governance UI when overrides exceed 5 percent over a rolling 30-day window.
- **FR-S-03.** The document classifier emits a `doc_type_confidence` along with `doc_type`. Documents below a configurable per-tenant confidence threshold (default 0.70) are queued for human classification rather than auto-routed.
- **FR-S-04.** Stage-4 store writes are wrapped by a per-record reconciliation marker. A nightly Airflow DAG (`m3-reconciliation`) detects records present in Delta Lake but missing from one of the three stores and replays them.

### 2.3 Could

- **FR-C-01.** Stage-3 Golden Record synthesis uses an LLM-assisted survivorship suggester for ambiguous cases (e.g., two sources both claim to be authoritative for `Party.legal_name`). The suggestion is advisory — actual survivorship rules remain deterministic per `nexus_system.survivorship_rules` (new table, §4.3).
- **FR-C-02.** Pinecone embeddings track their `embedding_model_version` and are auto-re-embedded on model change via the existing `nexus.cdm.version_published` topic.
- **FR-C-03.** A graph-based ER signal includes a transitivity propagation pass (if A↔B and B↔C, suggest A↔C) bounded by a per-tenant configurable depth (default 2).

### 2.4 Won't (this iteration)

- **FR-W-01.** No write-back to source systems. Pipeline is strictly read-only against tenant sources, consistent with `NEXUS-Module-Responsibilities.md` Rule 2.
- **FR-W-02.** No on-the-fly schema inference inside this pipeline. Schema discovery and CDM extension proposals remain owned by `nexus-cdm-mapper` and the M2 Structural Agent upstream of this seam.
- **FR-W-03.** No multi-tenant Golden Records. Every Golden Record is scoped to a single `tenant_id`.

---

## 3. Pipeline Stages — Detailed Specification

### 3.0 Stage 0 — Materialization Level Resolution

**Owner:** `nexus-m1-worker` (existing — extend the Op Router)
**Trigger:** Consumes `m1.int.cdm_entities_ready` (existing topic from `nexus-cdm-mapper`)
**Action:** For each record, look up the materialization level for `(tenant_id, cdm_entity_type)` in `nexus_system.cdm_entity_materialization`.

Routing decision:

| Level | Action |
|---|---|
| `hot` | Publish `{tid}.m1.entity_routed` (existing) — pipeline continues |
| `warm` | Write to Delta Lake only via existing path; publish `{tid}.m1.warm_recorded` (new); pipeline halts here |
| `cold` | Discard record after catalogue update; publish `{tid}.m1.cold_skipped` (new) for audit |

The two new topics are emitted for governance visibility (queue depth, replay support on level promotion) and are not consumed by `nexus-m3-writer`.

### 3.1 Stage 1 — Structured Track: Normalise

**Owner:** `nexus-spark-transformer` (existing)
**Status:** No change to current spec. Already performs type coercion, FX normalisation per `nexus_system.tenants.base_currency`, timestamp canonicalisation, and dedup. Output topic `m1.int.transformed_records` is unchanged.
**This spec strengthens:** Output payload must include `record_blocking_key` (a deterministic blocking key derived from the entity's canonical attributes) for downstream LSH efficiency. Blocking key formula per entity type lives in `nexus_system.entity_blocking_rules` (new table).

### 3.2 Stage 2 — Structured Track: Entity Resolution (3 signals)

**Owner:** `nexus-spark-transformer` (existing — extend the resolution stage)
**Inputs:** Output of Stage 1; existing `nexus_system.entity_resolution_index`; existing Neo4j (read-only, for graph signal)
**Output:** Updates to `entity_resolution_index` and to the new provenance tables; emits `m1.int.transformed_records` with `cdm_entity_id` populated (existing contract preserved)

Three signals are evaluated in sequence. A match short-circuits subsequent signals only when its confidence is above the auto-apply threshold.

**Signal A — Deterministic.** Exact match on any of the entity type's deterministic identifier columns (e.g. `tax_id`, `email_domain`, `duns_number`, `iso_country_code` for Location, `barcode` for Product). Identifier columns per entity type live in `nexus_system.deterministic_id_columns` (new table). A single deterministic match is auto-applied with confidence `1.000` regardless of probabilistic signal.

**Signal B — Probabilistic.** Pairwise comparison of canonical attributes. The candidate-pair set is reduced by Locality-Sensitive Hashing on `record_blocking_key`. Per-attribute similarity functions:

| Attribute kind | Function | Library |
|---|---|---|
| Names (legal_name, full_name) | Jaro-Winkler | `jellyfish` |
| Free-text (address lines, descriptions) | Levenshtein normalised | `python-Levenshtein` |
| Phonetic name fallback | Soundex + Metaphone (OR-merged) | `jellyfish` |
| Phone numbers (E.164-normalised first) | Exact post-normalisation | `phonenumbers` |
| Email | Local-part Levenshtein × domain exact | custom |

Per-attribute weights and the combined-score auto-apply / review / reject thresholds are tenant-and-entity-type-tuned, stored in `nexus_system.er_thresholds` (new table). Default starting thresholds: combined ≥ 0.92 auto-apply, 0.75–0.92 review queue, < 0.75 reject.

**Signal C — Graph-based.** For pairs in the review band after Signal B, traverse the Neo4j graph one or two hops from each candidate and compare the surrounding entity sets. A shared neighbour at depth 1 lifts the combined score by `+0.05`; a shared neighbour at depth 2 lifts by `+0.02`. Lifts are capped at `+0.10` total. If the lifted score crosses the auto-apply threshold, the match is auto-applied with `resolution_method = 'spark_graph'`.

Match outcomes route as follows:

| Outcome | Action | Topic |
|---|---|---|
| Auto-apply (≥ auto threshold) | Update `entity_resolution_index`; assign `cdm_entity_id` per FR-M-05 | (continues) |
| Review band | Insert into `nexus_system.er_review_queue` (new); assign provisional `cdm_entity_id` flagged `provisional=true` | `nexus.er.review_queued` |
| Reject | No merge; record stands alone with its own `cdm_entity_id` | (continues) |

**Materialization-aware ER depth.** ER signals run in full only on `hot` records. `warm` records run Signal A only (deterministic). `cold` records are not handled here at all — when retrieved on demand by the query executor, a deterministic-only check runs against existing Golden Records in the response path (out of scope for this spec, owned by `nexus-query-executor`).

### 3.3 Stage 3 — Structured Track: Golden Record Synthesis

**Owner:** `nexus-spark-transformer` (existing — extend with survivorship + provenance writer)
**Inputs:** Resolved record from Stage 2; existing Golden Record (if any) for `cdm_entity_id`
**Output:** Writes to `nexus_system.golden_record_provenance`; updates `nexus_system.golden_records_index`. **No business field values are stored** — only the source pointer per attribute (Virtual CDM principle preserved).

For each canonical attribute on the resolved entity, survivorship rules in `nexus_system.survivorship_rules` decide which source record's value wins. Default rule kinds:

| Rule kind | Description | Example |
|---|---|---|
| `most_recent` | Pick the source with the most recent `updated_at` | `Party.primary_phone` |
| `source_priority` | Pick the source with the highest priority for this attribute | `Party.legal_name` (ERP > CRM) |
| `most_complete` | Pick the source with the longest non-null value | `Party.address_line_2` |
| `first_observed` | Pick the source that first observed the value | `Party.created_at` |
| `manual_override` | Frozen by data steward — source can no longer change it | (any sensitive attr) |

Each surviving attribute produces one row in `golden_record_provenance` with `(cdm_entity_id, attribute_name, source_system, source_record_id, source_attr_value_hash, observed_at, rule_kind)`. The hash is for change detection only — the actual value is fetched live from source by `nexus-query-executor` phase 2.

### 3.4 Stage 4 — Structured Track: Projection to Stores

**Owner:** `nexus-m3-writer` (existing — extend handler set)
**Inputs:** `{tid}.m1.entity_routed` (existing) and `{tid}.m1.entity_removed` (existing)
**Outputs:** Pinecone, Neo4j, TimescaleDB writes; `nexus.m3.write_completed` / `nexus.m3.write_failed`

Per-store contracts are unchanged from `NEXUS-Iter2-M3-Elasticsearch-Writer-v0.1.md`, `NEXUS-Iter2-M3-Neo4j-Writer-v0.1.md`, and `NEXUS-Iter2-M3-TimescaleDB-Writer-v0.1.md`, with the following additions:

**Pinecone.**
- Vector ID format unchanged: `{tenant_id}::{entity_type}::{cdm_entity_id}`.
- New required metadata fields on every vector: `materialization_level`, `embedding_model_version`, `provenance_hash` (SHA-256 of the concatenated `golden_record_provenance` rows for this entity, used for staleness detection).
- The text fed to `embed()` is constructed from canonical attributes the user has potentially access to, never from PII-flagged columns (existing `agent_core.PIIChecker` integration).

**Neo4j.**
- Node MERGE on `(id, tenant_id)` — existing.
- **New: every relationship MERGE must set `source_fk` and `materialization_level` on the edge** (FR-M-07). Relationship MERGE key is extended to `(start_node, end_node, type, source_fk)` so that the same logical relationship coming from two different source systems coexists as two edges with distinct provenance, per the C.1.2 design.
- Document-derived edges (`DOC_MENTIONS`, `DOC_AUTHORED_BY`, `DOC_ATTACHED_TO`) carry `source_fk = "{source_system}:doc_extraction"` plus `chunk_id` and `extraction_confidence` properties.

**TimescaleDB.**
- Hypertable `nexus_ts.business_metrics_raw` and continuous aggregates (`metrics_weekly` 12mo / `metrics_monthly` 6yr / `metrics_yearly` permanent) unchanged.
- New required column on `business_metrics_raw`: `materialization_level VARCHAR(8) NOT NULL DEFAULT 'hot'`. Rows at `warm` are not written here at all (Stage 0 short-circuits); the column lets the query layer trace the provenance of any record that reached the metric layer.

### 3.5 Document Track — Stages 1d–6d

**Owner:** `nexus-doc-processor` (NEW service — see §6)
**Inputs:** Document arrival event `{tid}.m1.document_landed` (NEW topic, emitted by `nexus-m1-worker` for raw files in Delta Lake)
**Outputs:** Same Stage-4 store writes as the structured track + emits `{tid}.m1.entity_routed` for any new Golden Records the document creates.

**Stage 1d — Parse.** Apache Tika + Unstructured.io. Output: structured text representation preserving title, headings, paragraphs, tables, lists, OCR'd images, and file-level metadata (author, dates, tags). Stored in Delta Lake as `documents_parsed/{tenant_id}/{document_id}.parquet`.

**Stage 2d — Classify.** LLM-assisted classifier (called via `agent_core` — Rule 1 of `NEXUS-Module-Responsibilities.md` requires LLM access to be brokered through M2; this spec proposes an exception or relay, see Open Question OQ-DP-01). Output: `doc_type` from a tenant-extensible vocabulary, `doc_type_confidence`. Documents with confidence below threshold queue for human review (FR-S-03).

**Stage 3d — Chunk.** Semantic chunking with target 500–1500 tokens, respecting heading and paragraph boundaries. Each chunk inherits `document_id`, `tenant_id`, position index, parent heading, and `doc_type`. Stored in Delta Lake as `documents_chunked/{tenant_id}/{document_id}.parquet`.

**Stage 4d — Entity extraction.** LLM-assisted NER per chunk. Each mention produces `(mention_id, chunk_id, entity_type, surface_form, char_offsets, extraction_confidence)`. For specific high-value `doc_type`s (contract, MSA, signed_policy), type-specific extractors pull additional structured fields (effective_date, renewal_date, contract_value, parties).

**Stage 5d — Mention-to-entity resolution.** Run the same three signals as §3.2 but with the candidate side being existing Golden Records of the matching entity type. Outcome routing:

| Outcome | Action |
|---|---|
| Auto-apply | Create `DOC_MENTIONS` edge to the matched Golden Record |
| Review band | Insert into `nexus_system.mention_review_queue`; create `DOC_MENTIONS` to placeholder node |
| Reject + gating conditions met | Create new Golden Record from the mention (see below) |
| Reject + gating not met | Create `DOC_MENTIONS` to placeholder; mention remains unresolved |

**Gating conditions (FR-M-09).** A document mention may seed a new Golden Record only when **all four** are true:

1. The document's `doc_type` is in the tenant's `authoritative_doc_types` set (default: `contract`, `master_services_agreement`, `signed_policy`, `corporate_registry`).
2. The document's source path is in the tenant's `authoritative_source_directories` set (declared via M4 admin UI; e.g. `SharePoint:/Contracts/Signed/`).
3. The document's `created_at` is within the tenant's freshness window (default: 18 months).
4. The mention carries the entity type's minimum identifying attributes (per `nexus_system.entity_min_identification` — for `Party`: name AND (tax_id OR domain OR address)).

When all four hold, a Golden Record is created with `cdm_entity_id` per FR-M-05 and an initial provenance row marked `attribute_source_kind = 'document'`. The new Golden Record is published downstream as a normal `{tid}.m1.entity_routed` event.

**Stage 6d — Project to stores.** Same as §3.4. Document writes:

- Pinecone `{tenant_id}-documents` index — one vector per chunk; metadata includes `document_id`, `chunk_id`, `chunk_position`, `doc_type`, mentioned `cdm_entity_id`s, `materialization_level`.
- Neo4j — one `Document` node + `DOC_MENTIONS` / `DOC_ATTACHED_TO` / `DOC_AUTHORED_BY` edges with `source_fk` and `chunk_id` properties.
- PostgreSQL — `nexus_system.documents_index` row (document metadata only — no body, the Virtual CDM principle holds for documents too: chunk text lives in Delta Lake parquet only).

---

## 4. Data Model — New Tables

All tables live in the `nexus_system` schema. PostgreSQL DDL written for V2.0.20 migration (next available slot (V2.0.20+) per `NEXUS-Iter2-SPEC-DataModel-v0.5.md`).

### 4.1 `cdm_entity_materialization`

```sql
CREATE TABLE nexus_system.cdm_entity_materialization (
  tenant_id              UUID NOT NULL,
  cdm_entity_type        VARCHAR(128) NOT NULL,
  materialization_level  VARCHAR(8) NOT NULL CHECK (materialization_level IN ('hot','warm','cold')),
  assigned_by            VARCHAR(16) NOT NULL CHECK (assigned_by IN ('default','steward','auto')),
  last_promoted_at       TIMESTAMPTZ,
  last_demoted_at        TIMESTAMPTZ,
  query_count_30d        BIGINT NOT NULL DEFAULT 0,
  updated_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (tenant_id, cdm_entity_type)
);
```

### 4.2 `golden_record_provenance`

```sql
CREATE TABLE nexus_system.golden_record_provenance (
  cdm_entity_id            VARCHAR(48) NOT NULL,        -- "gr:..." per FR-M-05
  tenant_id                UUID NOT NULL,
  attribute_name           VARCHAR(128) NOT NULL,
  source_system            VARCHAR(64) NOT NULL,
  source_record_id         VARCHAR(255) NOT NULL,
  source_attr_value_hash   CHAR(64) NOT NULL,           -- SHA-256, change detection
  observed_at              TIMESTAMPTZ NOT NULL,
  rule_kind                VARCHAR(32) NOT NULL,
  attribute_source_kind    VARCHAR(16) NOT NULL DEFAULT 'structured'  -- 'structured' | 'document'
                           CHECK (attribute_source_kind IN ('structured','document')),
  document_id              VARCHAR(64),                 -- required when attribute_source_kind='document'
  chunk_id                 VARCHAR(64),                 -- required when attribute_source_kind='document'
  visibility_policy        VARCHAR(16) NOT NULL DEFAULT 'permissive'
                           CHECK (visibility_policy IN ('permissive','strict')),
  PRIMARY KEY (cdm_entity_id, attribute_name, source_system, source_record_id),
  CHECK (
    (attribute_source_kind = 'structured' AND document_id IS NULL AND chunk_id IS NULL)
    OR
    (attribute_source_kind = 'document' AND document_id IS NOT NULL AND chunk_id IS NOT NULL)
  )
);
CREATE INDEX idx_grp_tenant_entity ON nexus_system.golden_record_provenance(tenant_id, cdm_entity_id);
```

### 4.3 `survivorship_rules`

```sql
CREATE TABLE nexus_system.survivorship_rules (
  tenant_id        UUID NOT NULL,
  cdm_entity_type  VARCHAR(128) NOT NULL,
  attribute_name   VARCHAR(128) NOT NULL,
  rule_kind        VARCHAR(32) NOT NULL
                   CHECK (rule_kind IN ('most_recent','source_priority','most_complete','first_observed','manual_override')),
  rule_config      JSONB NOT NULL DEFAULT '{}',     -- e.g. {"priority":["sap","salesforce","zendesk"]}
  updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_by       VARCHAR(64),
  PRIMARY KEY (tenant_id, cdm_entity_type, attribute_name)
);
```

### 4.4 `mention_review_queue`

```sql
CREATE TABLE nexus_system.mention_review_queue (
  mention_id              VARCHAR(48) PRIMARY KEY,
  tenant_id               UUID NOT NULL,
  document_id             VARCHAR(64) NOT NULL,
  chunk_id                VARCHAR(64) NOT NULL,
  surface_form            TEXT NOT NULL,
  cdm_entity_type         VARCHAR(128) NOT NULL,
  candidate_cdm_entity_id VARCHAR(48),               -- best probabilistic match if any
  match_confidence        NUMERIC(4,3),
  status                  VARCHAR(16) NOT NULL DEFAULT 'pending'
                          CHECK (status IN ('pending','approved','rejected','promoted_to_new')),
  reviewed_by             VARCHAR(64),
  reviewed_at             TIMESTAMPTZ,
  created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_mrq_tenant_status ON nexus_system.mention_review_queue(tenant_id, status);
```

### 4.5 Supporting tables

`nexus_system.er_thresholds`, `nexus_system.er_review_queue`, `nexus_system.er_override_log`, `nexus_system.deterministic_id_columns`, `nexus_system.entity_blocking_rules`, `nexus_system.entity_min_identification`, `nexus_system.authoritative_doc_types`, `nexus_system.authoritative_source_directories`, `nexus_system.documents_index` — DDL omitted in this draft for brevity; columns implied by the FRs above. To be detailed in v0.2.

---

## 5. API / Event Contracts

### 5.1 New Kafka topics

| Topic | Producer | Consumer group(s) | Purpose |
|---|---|---|---|
| `{tid}.m1.warm_recorded` | `nexus-m1-worker` | governance only (Grafana metric) | Audit trail for warm-level skips |
| `{tid}.m1.cold_skipped` | `nexus-m1-worker` | governance only | Audit trail for cold-level skips |
| `{tid}.m1.document_landed` | `nexus-m1-worker` | `doc-processor` | Trigger document track |
| `nexus.er.review_queued` | `nexus-spark-transformer` | `m4-worker` | Surface ER review items in M4 UI |
| `nexus.doc.review_queued` | `nexus-doc-processor` | `m4-worker` | Surface mention/classification reviews |
| `nexus.materialization.changed` | `nexus-m4-api` | `m1-worker`, `m3-writer`, `doc-processor` | Promotion/demotion broadcast |

Existing topics (`{tid}.m1.entity_routed`, `{tid}.m1.entity_removed`, `nexus.cdm.version_published`, `nexus.m3.write_completed`, `nexus.m3.write_failed`) keep their schemas — only the payload metadata is extended (additional optional fields, no breaking changes).

### 5.2 Payload extensions (additive — backward compatible)

`{tid}.m1.entity_routed` payload gains:

```json
{
  "...": "(existing fields preserved)",
  "materialization_level": "hot",
  "provenance_summary": {
    "contributing_sources": ["salesforce","sap","zendesk"],
    "attribute_source_kinds": {"address": "document"}
  },
  "dry_run": false
}
```

`nexus.m3.write_completed` payload gains:

```json
{
  "...": "(existing fields preserved)",
  "stores_written": ["pinecone","neo4j","timescaledb"],
  "skipped_stores": [],
  "embedding_model_version": "openai/text-embedding-3-small@2025-01-15",
  "provenance_hash": "sha256:..."
}
```

### 5.3 No new HTTP endpoints in M3

All M3 ingress remains event-driven. New HTTP surfaces live in M4 admin (review queues, materialization-level overrides) and are out of scope for this spec.

---

## 6. New Service — `nexus-doc-processor`

| Property | Value |
|---|---|
| Module | M1 (data intelligence track) |
| Type | Event-driven worker, Kubernetes `Deployment` |
| Language | Python 3.12 + asyncio |
| Scaling | KEDA on `{tid}.m1.document_landed` lag, min 0 max 8 |
| LLM access | Brokered through `agent_core` per Rule 1 — but with a dedicated low-cost model lane (see OQ-DP-01) |
| Storage adjacencies | Reads/writes Delta Lake; reads `nexus_system.cdm_entity_materialization`, `authoritative_*` tables; writes `documents_index`, `mention_review_queue`, `golden_record_provenance` (document attribute contributions) |
| Replaceable in Iter 3 | Yes — designed so the six stages can be split into separate services if throughput requires |

**Sprint Plan integration.** Adds approximately one developer-week to the Iter 2 plan if scoped to "structured pipeline only" for v1. Recommended sequencing: ship structured track first (Stages 0–4), then layer the document track in a v0.2 of this spec. Coordinate with `NEXUS-Iter2-SprintPlan-v0.3.md`.

---

## 7. Failure Semantics and Recovery

The current "no DLQ in Iter 2 — failure event + full replay" decision from `NEXUS-Iter2-M3-AIStores-v0.4.md` is preserved. This spec adds two refinements:

**Per-store partial success.** `nexus-m3-writer` writes to all three stores in parallel. On any failure, the offset is committed (not rolled back), `nexus.m3.write_failed` is emitted with `failed_stores: [...]` populated, and a Grafana alert fires when message count on `write_failed` exceeds zero in any rolling 5-minute window. Replay strategy on store recovery: replay `{tid}.m1.entity_routed` from the earliest offset corresponding to the outage start, relying on idempotency.

**Document-track failure isolation.** Failure in document Stages 2d (classify) or 4d (extract) does not block Stages 1d and 3d (parse and chunk). Parsed and chunked content is persisted to Delta Lake even if downstream LLM stages fail; on recovery, the document track resumes from the last completed stage, identified by a marker column on `documents_index.last_completed_stage`.

---

## 8. Non-Functional Requirements

| ID | Requirement | Target |
|---|---|---|
| NFR-01 | End-to-end latency, structured track, hot tier, p95 | ≤ 90 seconds from `m1.entity_routed` to `m3.write_completed` for a record with all three store writes |
| NFR-02 | End-to-end latency, document track, hot tier, p95 (for a 50-page contract) | ≤ 6 minutes from `m1.document_landed` to all stores written |
| NFR-03 | Idempotency: duplicate-event injection rate | Repeated delivery of any event must produce zero net store change beyond observability counters |
| NFR-04 | Multi-tenant isolation | No cross-tenant data leakage in Pinecone, Neo4j, TimescaleDB; verified by the existing cross-tenant safety scanner in `agent_core` |
| NFR-05 | Survivorship determinism | Given the same inputs and rule config, Stage 3 produces the same `golden_record_provenance` rows on every run |
| NFR-06 | Materialization promotion impact | Promoting an entity from `warm` to `hot` must complete backfill within 30 minutes for ≤ 1M records |
| NFR-07 | LLM cost cap on document track | Per-tenant monthly budget enforced via `agent_core` token accounting; over-budget classifies as Stage 2d/4d failure and queues for review |

---

## 9. Edge Cases

The following are covered explicitly to remove ambiguity from the current implementation.

**EC-01 — Re-mapped CDM after data has flowed.** When `nexus.cdm.version_published` reflects an attribute rename or split, all surviving Golden Records' provenance rows for the affected attribute are migrated by an Airflow DAG (`m3-cdm-version-migration`). Pinecone vectors are re-embedded under the new model version. Neo4j edges are unaffected (relationships are not attribute-bound).

**EC-02 — ER false positive correction.** A user-flagged "this is not the same entity" decision in M4 results in a *split*. The Golden Record is unmerged into its contributing source records, each receiving a fresh `cdm_entity_id`. Document `DOC_MENTIONS` edges to the original Golden Record are re-routed to the mention review queue for steward decision.

**EC-03 — Source record deleted.** `{tid}.m1.entity_removed` removes the source pointer from `golden_record_provenance` for that source record. If no provenance rows remain for the Golden Record, the Golden Record itself is deleted (Pinecone tombstone + Neo4j `DETACH DELETE` + TimescaleDB `is_deletion=TRUE` row), preserving the existing per-store deletion semantics.

**EC-04 — Same document attached to two Golden Records that later split.** The document's `DOC_ATTACHED_TO` edges follow the split: each new Golden Record retains the edge with its corresponding `source_fk`. `DOC_MENTIONS` edges are re-evaluated by Stage 5d on the post-split graph.

**EC-05 — Materialization demotion mid-flight.** A demotion `hot → warm` while records are in flight does not abort in-flight writes — those writes complete and become "stranded warm with hot data". A reconciliation DAG removes them from Pinecone and Neo4j on the next nightly run; TimescaleDB rows are retained until retention naturally expires.

**EC-06 — Probabilistic match in review band, then auto-resolved by graph signal.** Signal C lifts the score above auto-apply threshold. The match is auto-applied with `resolution_method = 'spark_graph'` and the originally-queued `er_review_queue` row is auto-dismissed with `dismissed_reason = 'graph_lift'`.

**EC-07 — Document mentions an entity not yet in the platform but gating conditions partially met.** Under the four-condition rule (FR-M-09), partial satisfaction never creates a new Golden Record; the mention is queued for steward decision. Stewards can manually promote it.

**EC-08 — Survivorship rule changed retroactively.** Updating a row in `survivorship_rules` triggers an Airflow DAG (`m3-survivorship-rebuild`) that recomputes `golden_record_provenance` for the affected `(cdm_entity_type, attribute_name)`. The recomputation is bounded by tenant scope.

---

## 10. Open Questions (`[CLARIFY:]`)

- **OQ-DP-01.** Does `nexus-doc-processor` make LLM calls directly (violating Rule 1 of `NEXUS-Module-Responsibilities.md`), or does it relay through `nexus-m2-executor` for every chunk? Direct calls are simpler and cheaper but require an exception to the rule. Recommendation: define a sanctioned "Rule 1 exception" for cost-optimised batch LLM calls inside M1, gated by `agent_core` token accounting and PII redaction.
- **OQ-DP-02.** What is the chunk size policy when `doc_type = 'contract'`? Contracts often have semantically meaningful clauses below 500 tokens. Should a smaller chunk minimum (e.g. 200) apply per `doc_type`?
- **OQ-MAT-01.** Default starting `materialization_level` for newly-published CDM entities — `warm` (this spec's proposal) or `hot`? `warm` is conservative and protects costs; `hot` is more useful out of the box for small tenants.
- **OQ-MAT-02.** Auto-promotion thresholds — what query count over what window crosses `cold→warm` and `warm→hot`? Tenant-default proposed: 5 queries / 7 days for `cold→warm`; 25 queries / 30 days for `warm→hot`. Confirm.
- **OQ-VIS-01.** When `visibility_policy = 'strict'` excludes an attribute, does the response include a placeholder indicator ("hidden by policy") or omit the attribute silently? The illustration in C.1.2 suggests a placeholder; confirm the user-facing copy via `design:ux-copy` review.
- **OQ-PROV-01.** `provenance_hash` on Pinecone metadata is for staleness detection. When provenance changes but the embedding text would be identical (e.g. survivorship swap on a non-text attribute), do we re-embed anyway for hash consistency, or short-circuit when text is unchanged? Cost vs determinism tradeoff.
- **OQ-ER-01.** LSH parameter selection — band count and band width — needs benchmarking per typical tenant volume. Proposed default: 32 bands × 4 rows for blocking key length 128.
- **OQ-DOC-01.** `documents_index` table is mentioned but its column set is not yet detailed in this draft. To be fleshed out in v0.2.
- **OQ-DOC-02.** OCR quality threshold — when scanned PDFs return low-confidence OCR, does Stage 1d still chunk them or queue the document for human re-scan?

---

## 11. Reconciliation Notes — Where this spec lands relative to the two source bodies

This spec **adopts from C.1.2** the materialization-level model (renamed to avoid the existing "tier" overload), the three-signal entity resolution with explicit algorithmic detail, the document processing pipeline (six stages), the gated rules for document-driven Golden Record creation and attribute contribution, the edge-level `source_fk` provenance, and the permissive/strict Golden Record visibility policy.

This spec **preserves from current Iteration 2 specs** the Virtual CDM principle (no business field values duplicated into M3), the pre-assigned `cdm_entity_id` flowing from `nexus-spark-transformer` through `nexus-m3-writer`, the Kafka topic vocabulary (`{tid}.m1.entity_routed`, `nexus.m3.write_completed`, etc.), the per-store idempotency contracts, the multi-tenant `(id, tenant_id)` composite uniqueness in Neo4j, the TimescaleDB hypertable + continuous aggregate retention model, and the failure-event-plus-replay recovery pattern.

This spec **resolves naming collisions** by reserving "Tier 1/2/3" exclusively for CDM mapping confidence (existing meaning) and using "materialization level" (hot/warm/cold) for the new concept.

This spec **introduces** one new service (`nexus-doc-processor`) and four primary new tables (`cdm_entity_materialization`, `golden_record_provenance`, `survivorship_rules`, `mention_review_queue`) plus several supporting tables to be detailed in v0.2.

This spec **defers** the Frontend Angular-vs-React contradiction (out of scope here; tracked separately) and the Workflow Airflow-vs-Temporal split (also out of scope; current Temporal-for-business-processes choice stands).

---

## 12. References

- `NEXUS-Iter2-M3-AIStores-v0.4.md` — store contracts and the Virtual CDM principle this spec builds on.
- `NEXUS-Iter2-CDM-Mapper-v0.3.md` — Tier 1/2/3 confidence semantics that this spec preserves and disambiguates from the new materialization model.
- `NEXUS-Iter2-SPEC-ServiceTopology-v0.4.md` — existing Spark-transformer / M3-writer service shape.
- `NEXUS-Iter2-SPEC-DataModel-v0.5.md` — schema this spec extends with V2.0.20 migration.
- `NEXUS-Module-Responsibilities.md` — Rule 1 (LLM calls in M2 only) — relevant to OQ-DP-01.
- `iter2-gap-analysis-v0.1.md` and `iter2-review-and-plan-v0.1.md` — drafts that flagged this seam as underspecified.
- C.1.1.md and C.1.2.md (uploaded grant-format proposal) — source of the materialization, document, ER, and visibility concepts adopted here.
