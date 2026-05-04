# AI Stores

Specifications for the three AI stores that back NEXUS's query capability: Elasticsearch (kNN vector search), Neo4j (property graph), and TimescaleDB (time-series). All three stores are written to by the single `nexus-m3-writer` service.

All stores enforce the **Virtual CDM principle**: only embeddings and reference tuples (`cdm_entity_id`, `tenant_id`, `cdm_entity_type`) are persisted here. No raw business field values are stored. Live values are retrieved from source systems at query time via a two-phase lookup.

---

## Files

**`NEXUS-Iter2-SPEC-M3-AIStores-v0.5.md`**
Architectural reference for all three AI stores. Defines the Virtual CDM principle, write contracts, idempotency rules, per-store architectural invariants, FRs/NFRs, edge cases, and open questions. Points to the master service spec and individual store specs for implementation detail.

**`NEXUS-Iter2-SVC-nexus-m3-writer-stores-v0.1.md`** — *Master service spec*
Covers the `nexus-m3-writer` service as a whole: scope, cross-cutting dependencies, Virtual CDM rule, data model ownership (`entity_store_presence`, `cdm_entity_storage_config`), Kafka contracts, write orchestration, CRUD operation matrix, hot/warm/cold movement matrix, presence lookup via direct SQL, NFRs, observability, acceptance criteria, and open questions. The service lead's primary reference — assign to the senior developer coordinating the three handlers.

**`NEXUS-Iter2-SVC-nexus-m3-writer-elasticsearch-v0.1.md`** — *Developer A task*
Elasticsearch handler implementation spec. Covers FRs/NFRs, index naming convention and full mapping JSON (1536-dim dense vector, cosine similarity), Kafka payload contracts, upsert algorithm with provenance-hash short-circuit, kNN query contract for the query engine, edge cases, implementation phases (D2A-01 through D2A-06), and open questions.

**`NEXUS-Iter2-SVC-nexus-m3-writer-neo4j-v0.1.md`** — *Developer B task*
Neo4j handler implementation spec. Covers the handler interface, schema DDL (constraints + indexes), node MERGE pattern (id + tenant_id composite, no business data), relationship MERGE on `(start, end, type, source_fk)` composite, DETACH DELETE for deletions, `OrgChartBuilder` for AdventureWorks HR hierarchy, tenant isolation enforcement, implementation phases (D2B-01 through D2B-06), and open questions.

**`NEXUS-Iter2-SVC-nexus-m3-writer-timescaledb-v0.1.md`** — *Developer C task*
TimescaleDB handler implementation spec. Covers the handler interface, key data decisions (immutable append, asymmetric hot→warm/hot→cold demotion rule, four-tier query selection), full DDL (hypertable + 3 continuous aggregates + `platform_metrics`), all write paths (`_insert`, `_correct`, `_tombstone`, `refresh_aggregates_for_range`), FX normalisation via `nexus_core.fx.FXService`, implementation phases (D2C-01 through D2C-07), and open questions.

---

*NEXUS Iteration 2 · Mentis Consulting · April 2026*
