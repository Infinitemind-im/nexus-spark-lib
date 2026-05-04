# NEXUS — Iteration 2 · `nexus-m3-writer` · M3 AI Stores Architecture
**Service:** `nexus-m3-writer`
Mentis Consulting · Version 0.5 · April 2026 · Confidential

> **Revision v0.5 — Architectural reference only.** The detailed per-store implementation (DDL, Cypher patterns, write algorithms, Python code) has been consolidated into `NEXUS-Iter2-SVC-nexus-m3-writer-stores-v0.1.md`. The service specification (identity, Kafka topics, write orchestration, scaling, security, observability) lives in `developer-workstreams/NEXUS-Iter2-SVC-nexus-m3-writer-v0.1.md`. This document defines the architectural invariants, functional requirements, and per-store contracts that both implementation documents must satisfy.
>
> **Revision v0.4 — topic-name cleanup.** Removed three stale references to `{tid}.m1.mapping.approved`; corrected to `{tid}.m1.entity_routed` (primary write path) or `{tid}.m4.mapping_approved` (catch-up trigger).
>
> **Revision v0.3 — architecture review corrections.** Primary continuous write path added; idempotency table corrected to reference `business_metrics_raw`.

---

## Overview

Module 3 transitions from a passive infrastructure layer (Iteration 1) to an actively populated knowledge base. In Iteration 1, M3 stores were provisioned and consumed by `nexus-m2-executor` as a side effect of query processing. In Iteration 2, a dedicated service — `nexus-m3-writer` — owns all writes to M3, separating the ingestion concern from the query-execution concern.

The three stores serve non-overlapping query patterns:

| Store | Technology | Query type served | Isolation model |
|---|---|---|---|
| Vector | Elasticsearch | Semantic similarity, kNN search, RAG context | Per-tenant index: `nexus_{tenant_slug}_{entity_type}` |
| Graph | Neo4j Aura | Relationship traversal, org hierarchy | `tenant_id` property on every node + query-level WHERE |
| Time-series | TimescaleDB | Trend analysis, time-bucket aggregation | RLS policy + `tenant_id` column on all hypertables |

**Architectural rule preserved from Iteration 1:** `nexus-m2-executor` still *reads* from M3 stores. `nexus-m3-writer` now owns all *writes*. No other service writes to M3.

---

## Virtual CDM Principle

Source data is never duplicated into AI stores. The three stores hold *references and derived structures only*:

- **Elasticsearch** stores vector embeddings (`dense_vector`, 1536 dims, cosine similarity) and a reference tuple (`tenant_id`, `cdm_entity_id`, `cdm_entity_type`, `contributing_sources`, `provenance_hash`, `materialization_level`, `embedding_model_version`). Field values are used transiently to generate the embedding and discarded immediately — they are not persisted in Elasticsearch document metadata.
- **Neo4j** stores entity IDs, `tenant_id`, and relationship edges. Business field values (`name`, `email`, `amount`, etc.) are never written to graph nodes.
- **TimescaleDB** stores pre-computed metric aggregates (metric name, normalised value, dimensions). Raw source records are never replicated.

All business data remains in the live source systems and is fetched on demand by `nexus-query-executor` during phase 2 of the two-phase query pattern.

A consequence of this rule: if a miscoded query bypasses OPA tenant isolation, the attacker receives only opaque ID pairs from Neo4j and metric hashes from TimescaleDB — no usable business data.

---

## Functional Requirements

| ID | Requirement | Priority |
|---|---|---|
| M3-FR-01 | `nexus-m3-writer` populates Elasticsearch with an embedding for every CDM entity arriving via `{tid}.m1.entity_routed`, regardless of whether individual field mappings are Tier 1 or Tier 2. Tier 2 and Tier 3 field mappings do not block the write — under Virtual CDM, only the reference tuple is stored, not field values. | Must |
| M3-FR-02 | `nexus-m3-writer` populates Neo4j nodes and edges for every CDM entity arriving via `{tid}.m1.entity_routed`. Node contents are `id` + `tenant_id` only (Virtual CDM). | Must |
| M3-FR-03 | `nexus-m3-writer` inserts TimescaleDB rows for every CDM entity that carries a timestamp and a numeric value mapping, arriving via `{tid}.m1.entity_routed`. | Must |
| M3-FR-04 | All writes are idempotent — reprocessing the same Kafka message produces no duplicate records. | Must |
| M3-FR-05 | Partial store failure does not block writes to the other two stores. | Must |
| M3-FR-06 | `nexus-m3-writer` operates one logical consumer per tenant (KEDA lag-based scaling). | Should |
| M3-FR-07 | The service exposes a `/health` endpoint that reports per-store connectivity status. | Must |
| M3-FR-08 | Elasticsearch index is refreshed (batch re-embed) when a new CDM version is published. | Should |
| M3-FR-09 | TimescaleDB `platform_metrics` table receives service-level metrics from all NEXUS services. | Could |
| M3-FR-10 | All writes carry `cdm_version` metadata for traceability. | Must |

## Non-Functional Requirements

| ID | Requirement | Target |
|---|---|---|
| M3-NFR-01 | P95 Elasticsearch upsert latency (including embedding call) | < 500ms per entity |
| M3-NFR-02 | P95 Neo4j MERGE latency per node | < 200ms |
| M3-NFR-03 | TimescaleDB insert throughput | ≥ 1,000 rows/second sustained |
| M3-NFR-04 | Kafka consumer lag (`{tid}.m1.entity_routed` — primary write path) | < 60 seconds steady state |
| M3-NFR-05 | Startup time | < 20s (connection pool init) |
| M3-NFR-06 | Memory footprint (single replica) | < 512Mi |
| M3-NFR-07 | Write failure must be logged with full trace; no silent data loss | Zero silent failures |

---

## 1. Elasticsearch — Architectural Invariants

**Index naming:** `nexus_{tenant_slug}_{cdm_entity_type_lower}` — one index per tenant per CDM entity type. Provisioned by `nexus_core.provisioning.onboard_tenant()` at tenant creation; never created by `nexus-m3-writer` at write time.

**Dimensions / metric:** 1536-dimensional dense vectors (OpenAI `text-embedding-3-small`), cosine similarity. Tenant isolation is at index level.

**Idempotency:** Every document uses `cdm_entity_id` as the Elasticsearch `_id`. Re-upserting the same entity replaces the document in place. Re-processing any `{tid}.m1.entity_routed` event is safe.

**PII exclusion:** Fields flagged `pii=true` in `nexus_system.schema_snapshots.column_profiles` are excluded from the transient embedding text via `agent_core.PIIChecker`.

**Staleness short-circuit:** If the incoming `provenance_hash` matches the stored document's metadata hash and `embedding_model_version` is unchanged, the embedding call and upsert are skipped.

**Deletion semantics:** Soft-delete via metadata flag `deleted:true` on demotion or Golden Record tombstone. Hard-purge by a nightly maintenance job (Delete By Query on `deleted:true` documents older than 24 hours).

**CDM version refresh:** Targeted re-embed for affected entity types on minor version publish; full index rebuild on major version change.

> **Implementation details:** See `NEXUS-Iter2-SVC-nexus-m3-writer-stores-v0.1.md` §Part 1 — Elasticsearch Handler for the full index schema/mapping JSON, upsert algorithm, kNN query contract, batch sizing, and retry policy.

---

## 2. Neo4j — Architectural Invariants

**Node schema:** Every node carries only `{ id, tenant_id, connector_id, source_ref }` — no business data properties. Business data stays in the live source and is fetched during phase 2 of the two-phase query pattern.

**Write pattern:** All writes use `MERGE` — never `CREATE`. The composite uniqueness key on `(id, tenant_id)` prevents cross-tenant collisions and ensures idempotency.

**Relationship schema:** Edges carry structural metadata only (`since`, `connector_id`, provenance attributes including `source_fk`). Business field values are never written to relationship properties.

**Tenant isolation:** Enforced at two levels — composite uniqueness constraints include `tenant_id`, and OPA pre-query checks block queries without a valid `tenant_id` claim.

**Org chart building:** `OrgChartBuilder` (inside `nexus-m3-writer`) issues a live service-level query to retrieve structural columns from HR source systems and writes `REPORTS_TO` edges. No PII fields are fetched. Triggered by `nexus.cdm.version_published`.

> **Implementation details:** See `NEXUS-Iter2-SVC-nexus-m3-writer-stores-v0.1.md` §Part 2 — Neo4j Graph Store Handler for the full DDL (constraints + indexes), all MERGE patterns, DETACH DELETE for deletions, and the `OrgChartBuilder` implementation.

---

## 3. TimescaleDB — Architectural Invariants

**Append-only design:** The store is immutable append-only. Corrections are written as new rows. Deletions are recorded as tombstone rows (`is_deletion = TRUE`).

**Four-tier aggregate architecture:**

| Tier | Table / View | Granularity | Retention |
|---|---|---|---|
| Raw | `business_metrics_raw` | Event-level | 3 months |
| Weekly | `metrics_weekly` | 1-week buckets | 12 months |
| Monthly | `metrics_monthly` | 1-month buckets | 6 years |
| Yearly | `metrics_yearly` | 1-year buckets | Permanent |

The query executor selects the coarsest tier that fully covers the requested date range. No application logic is needed beyond a range check — TimescaleDB serves the query from the appropriate materialised view automatically when the correct view is targeted.

**Currency normalisation:** All monetary values are normalised to the tenant's configured `base_currency` (stored in `nexus_system.tenants.base_currency`, default `EUR`) before insertion into `business_metrics_raw`. The original currency and FX rate are preserved in the `dimensions` JSONB column for auditability.

**Idempotency:** `INSERT … ON CONFLICT DO NOTHING` on the `(time, tenant_id, metric_name, cdm_entity_id)` unique constraint defined on `business_metrics_raw`.

**Asymmetric demotion rule:** Entities demoted from hot→warm or warm→cold retain their existing `business_metrics_raw` rows. The demotion does not trigger any TimescaleDB write. Only cold→archived transitions trigger a compression pass.

> **Implementation details:** See `NEXUS-Iter2-SVC-nexus-m3-writer-stores-v0.1.md` §Part 3 — TimescaleDB Time-Series Handler for the full DDL (hypertable + 3 continuous aggregates + `platform_metrics`), write paths, currency normalisation implementation, and query tier selection logic.

---

## 4. Idempotency Summary

| Store | Idempotency mechanism |
|---|---|
| Elasticsearch | Document `_id` = `cdm_entity_id` — `_update` with `doc_as_upsert:true` replaces in place |
| Neo4j | `MERGE` on `(id, tenant_id)` composite — no duplicates possible |
| TimescaleDB | `INSERT … ON CONFLICT DO NOTHING` on `(time, tenant_id, metric_name, cdm_entity_id)` |

---

## 5. Edge Cases

| Case | Handling |
|---|---|
| CDM entity has no timestamp field (for TimescaleDB) | Use `approved_at` as fallback time; log a warning |
| Entity type not in `EVENT_TO_METRIC_MAP` | Skip TimescaleDB insert — not an error |
| Elasticsearch index for tenant does not exist yet | Auto-create index on first write using the provisioned template; if template missing, publish `write_failed` event and alert Platform team |
| Neo4j `organizationnode` path refers to a parent that doesn't exist yet | Log warning; skip edge; reprocess on next CDM version publish |
| Embedding API (OpenAI) rate-limited | Retry with exponential backoff (max 3 attempts, 2/4/8s). After 3 failures, publish `write_failed` |
| TimescaleDB chunk not yet created for a historical timestamp | TimescaleDB creates chunks automatically — no action needed |
| Kafka consumer rebalance mid-batch | All three stores are idempotent — reprocessing the batch is safe |
| CDM entity with PII-only fields (all fields are PII-flagged) | Embed an empty-fields representation — entity shape is still useful for type-level similarity search |

---

## 6. Open Questions

| # | Question | Status | Impact |
|---|---|---|---|
| OQ-M3-01 | Elasticsearch cluster sizing for Iteration 2 — single node vs. 3-node cluster? Affects availability and shard allocation. See `NEXUS-Iter2-SVC-nexus-m3-writer-stores-v0.1.md` ES-OQ-01. | Open | Availability and query latency |
| OQ-M3-02 | Neo4j Aura: single shared instance with property-level tenant isolation vs. one Aura instance per tenant? | Open | Cost vs. hard isolation. Recommend shared for Iteration 2, revisit at 20+ tenants |
| OQ-M3-03 | Should the Org Chart Builder run as part of `nexus-m3-writer` or as a separate Airflow DAG? | **Resolved:** `OrgChartBuilder` lives inside `nexus-m3-writer` (phase D2B-05), triggered by `nexus.cdm.version_published`. | — |
| OQ-M3-04 | When a CDM entity is updated (new `{tid}.m4.mapping_approved` with same CDM ID), should the old Neo4j node's relationships be deleted and re-created, or only new ones merged? | Open | Data correctness for org chart hierarchy changes |
| OQ-M3-05 | Currency normalisation: should `amount` fields be normalised to EUR before insertion, or stored in source currency with a `currency` dimension? | **Resolved:** Normalised to `tenants.base_currency` (default `EUR`) before insert. Original currency + FX rate preserved in `dimensions` JSONB. | — |

---

## Related Documents

| Document | Role |
|---|---|
| `NEXUS-Iter2-SVC-nexus-m3-writer-stores-v0.1.md` | **Implementation spec** — full DDL, Cypher patterns, write algorithms, Python code for all three stores |
| `developer-workstreams/NEXUS-Iter2-SVC-nexus-m3-writer-v0.1.md` | **Service spec** — identity, Kafka topics, write orchestration, scaling, security, observability |
| `pipeline/NEXUS-Iter2-CDM-AIStores-Pipeline-v0.1.md` | Pipeline architecture that feeds `nexus-m3-writer` |
| `architecture/NEXUS-Iter2-SPEC-DataModel-v0.5.md` | `entity_store_presence` register and `nexus_ts` schema DDL |
| `developer-workstreams/NEXUS-Iter2-SPEC-PipelineRegisters-v0.1.md` | `entity_store_presence` register ownership and cross-store reconciliation |

---

*NEXUS Iteration 2 · M3 AI Stores Architecture · v0.5 · Mentis Consulting · April 2026 · Confidential*
