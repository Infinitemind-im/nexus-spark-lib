# Developer Workstreams

Implementation specs for the five parallel CDM-to-AIStores pipeline development streams, plus the coordination overview, service delta specs, and new service specs. These are the documents developers work from day-to-day. Total estimated effort across all streams: 24 person-weeks in the 9-week Iteration 2 window.

Each stream spec follows the same template: scope, dependencies, functional requirements (MoSCoW), non-functional requirements, data model ownership, Kafka contracts, CRUD handling, hot/warm/cold behaviour, acceptance criteria, and open questions.

**Read `PipelineRegisters` first** — it defines the dependency DAG between streams and the shared registers that all streams write to or consume.

---

## Service Specs (new and delta)

**`NEXUS-Iter2-SVC-nexus-spark-transformer-v0.1.md`** — `nexus-spark-transformer` · NEW
Full implementation spec for the Spark transformation stage. Type coercion, FX normalisation, entity resolution, Golden Record assignment, Delta Lake checkpointing. Resolves OQ-SP-01/02/03.

**`NEXUS-Iter2-SVC-nexus-airbyte-stream-bridge-v0.1.md`** — `nexus-airbyte-stream-bridge` · NEW
Spec for the SaaS polling bridge (Salesforce, SAP, ServiceNow). Poll → delta-compute → Debezium-envelope emit. Note: this service was missing from the ServiceTopology service count table.

**`NEXUS-Iter2-SVC-nexus-m1-api-v0.1.md`** — `nexus-m1-api` · Iteration 2 delta
Confirms no breaking changes. One new endpoint: `POST /connectors/{id}/refresh` to reset `consecutive_failures` in the Airbyte bridge.

**`NEXUS-Iter2-SVC-nexus-schema-profiler-v0.1.md`** — `nexus-schema-profiler` · Iteration 2 delta
Confirms no breaking changes. New trigger: `nexus.cdm.schema_drift_detected` event fires on-demand profiling run. Coexistence with `nexus-spark-transformer` inline stats documented.

**`NEXUS-Iter2-SVC-nexus-m2-api-v0.1.md`** — `nexus-m2-api` · Iteration 2 delta
**Breaking role change.** `nexus-m2-api` is no longer the user-facing query entry point — `nexus-query-api` takes that surface. `nexus-m2-api` is now internal/programmatic only. Removed from Kong routing.

---

## Pipeline Workstream Specs

**`NEXUS-Iter2-SPEC-PipelineRegisters-v0.1.md`** — Coordination overview
The coordination document for all five streams. Contains the dependency DAG showing which stream hands off to the next, the pairwise handover contracts, and the detailed specs for the six shared system registers: `entity_resolution_index`, `golden_record_provenance`, `golden_records_index`, `golden_record_redirects`, `materialization_policy` + `cdm_entity_materialization`, and `entity_store_presence`. Also defines the end-to-end CRUD handling matrix and hot/warm/cold movement across the pipeline. Every developer should read this before their individual spec.

**`NEXUS-Iter2-SVC-nexus-m1-worker-CDCStreaming-v0.1.md`** — `nexus-m1-worker` (extended) · Dev 1 · 4 person-weeks
Covers the streaming ingestion layer: Debezium CDC for PostgreSQL and the Airbyte bridge for SaaS sources (Salesforce, SAP, ServiceNow), feeding into `spark-stream-transformer`. Defines how each source operation type (INSERT, UPDATE, DELETE, SNAPSHOT_READ, TRUNCATE) is normalised, how schema drift is detected and surfaced, and how per-tenant Kafka partitioning provides fairness guarantees. The connector handover protocol — the contract between this stream and ER-CRUD — is defined here.

**`NEXUS-Iter2-SPEC-Backfill-v0.1.md`** — Dev 2 · 4 person-weeks
Specifies the `nexus_spark_lib` shared library used by both the streaming and batch layers, and seven Airflow DAGs for initial loads and ongoing reconciliation: initial-load, m3-reconciliation, cdm-version-migration, materialization-promotion-backfill, materialization-demotion-cleanup, er-reindex, and connector-catchup. Includes checkpointing strategy (all jobs restartable from last committed offset) and pre-flight cost estimation to prevent runaway jobs.

**`NEXUS-Iter2-SPEC-ER-CRUD-v0.1.md`** — Dev 3 · 6 person-weeks
The most complex individual stream. Covers three-signal entity resolution (Jaro-Winkler, Levenshtein, Soundex+Metaphone) with LSH blocking to keep comparison counts tractable, and the Neo4j graph lift that persists resolution decisions as edges. Then specifies Stage 3 Golden Record synthesis — how survivorship rules (recency, completeness, source priority) deterministically pick field values when multiple source records contribute. Also defines the source-DELETE propagation algorithm that handles deletions without corrupting surviving Golden Records.

**`NEXUS-Iter2-SPEC-MaterializationCoordinator-v0.1.md`** — Dev 4 · 5 person-weeks
Implements Stage 0 of the pipeline: the policy evaluation step in Spark and the `m1-worker` that decides hot/warm/cold before a record reaches the AI stores. Specifies the five tier-movement Airflow DAGs, the RLHF training loop that feeds signals back to `MaterializationFeatureLearning`, and movement guardrails (rate limits, cool-down periods, manual-override respect). Also defines the M4 admin API for policy CRUD used by data stewards.

**`NEXUS-Iter2-SVC-nexus-m3-writer-v0.1.md`** — `nexus-m3-writer` · Dev 5 · 5 person-weeks
The consolidated implementation spec for all three AI store writers and the `entity_store_presence` register. For Elasticsearch: kNN upsert via `_update/doc_as_upsert`, index naming `nexus_{tenant_slug}_{entity_type}`, and deletion tombstones. For Neo4j: MERGE pattern with `(start, end, type, source_fk)` edge attributes. For TimescaleDB: append-only with `is_deletion = TRUE` for soft deletes. Covers cross-store reconciliation, drift detection, and the Prometheus metrics the query engine relies on.

---

*NEXUS Iteration 2 · Mentis Consulting · April 2026*
