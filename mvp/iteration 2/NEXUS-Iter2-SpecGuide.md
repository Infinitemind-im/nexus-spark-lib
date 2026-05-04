# NEXUS — Iteration 2 · Specification Guide

A plain-language description of every specification file in `specs/`. Use this as a starting point before diving into any individual document.

Mentis Consulting · April 2026 · Confidential

---

## Planning

**`NEXUS-Iter2-SprintPlan-v0.3.md`**
The single master task list for the entire iteration. It organises work across two parallel developer tracks (query engine: Dev 1–6; CDM-to-AIStores pipeline: Dev 1–5), defines the four phase gates (Weeks 1, 3, 5, 7), records all estimates, dependency chains, and open questions. This is the document a Tech Lead or PM checks daily to track progress.

---

## Architecture & Design

**`NEXUS-Iter2-ServiceTopology-v0.4.md`**
Defines the three-layer service model (ingestion → processing → query) and the seven inviolable architectural rules, including Rule 0 (Kong is the sole JWT decoder) and the rule that `nexus-query-api` is the only user-facing query surface. It owns the Kafka topic registry, consumer group assignments, network policies, and service account permissions. Any new service or topic must be registered here first.

**`NEXUS-Iter2-DataModel-v0.5.md`**
The canonical source of truth for every database table added or modified in Iteration 2. It contains the full DDL for all PostgreSQL and TimescaleDB tables, the Flyway migration ledger (V2.0.1–V2.0.19), and the deprecation notice for the `pipeline = 'm2'` column. When two specs disagree on a table definition, this document wins.

**`NEXUS-Iter2-ArchReview-v0.2.md`**
A two-pass cross-spec consistency review written after all Iteration 2 specifications existed. It records every conflict found, classifies each as critical or important, and tracks which were resolved versus which remain open. It is the authoritative record of inter-spec decisions — in particular the confirmation that `nexus-query-api` is the sole user-facing chat surface (OQ-M6-01 resolved) and the replacement of Pinecone with Elasticsearch throughout.

---

## CDM-to-AIStores Pipeline

**`NEXUS-Iter2-CDM-AIStores-Pipeline-v0.1.md`**
The parent architecture document for the end-to-end pipeline that moves data from a published CDM entity into the three AI stores (Elasticsearch, Neo4j, TimescaleDB). It describes the five processing stages, the parallel document track, CRUD handling for every source operation type, the Virtual CDM principle (no business field values stored in AI stores — only embeddings and reference tuples), and the gating logic that decides whether a record is materialised at all.

**`NEXUS-Iter2-RecordLifecycle-Structured-v0.1.md`**
A step-by-step walkthrough of a single Salesforce record travelling through the entire pipeline — from a source UPDATE event to a completed write in all three AI stores. It traces all 10 phases with timing (~31 seconds end-to-end), shows how the entity resolution scorer applies Jaro-Winkler similarity, and illustrates the survivorship rules that produce the Golden Record. This is the best document to read when trying to understand how all the moving parts connect.

**`NEXUS-Iter2-SystemOrchestration-v0.1.md`**
Specifies the three Spark applications that run the pipeline (`spark-stream-transformer`, `spark-batch-jobs`, `spark-maintenance`) and the nine Airflow DAGs that coordinate batch work. It contains the materialization classification heuristic (weighted scoring that decides hot/warm/cold), the Golden Record state machine (CREATE / UPDATE / MERGE / SPLIT / TOMBSTONE transitions), and the per-store routing matrix that controls which entity types go to which stores.

**`NEXUS-Iter2-MaterializationPolicy-v0.1.md`**
Replaces simple per-tenant flags with a policy engine that assigns materialization levels dynamically. It defines five rule types (base, decay, boost, learned, manual), a constrained predicate grammar for writing rules, and a deterministic resolution algorithm for when rules conflict. The policy is time-and-context-aware — a record can be promoted or demoted between hot, warm, and cold tiers as its usage patterns change.

**`NEXUS-Iter2-MaterializationFeatureLearning-v0.1.md`**
Extends the policy engine with a machine-learning loop. Instead of proposing discrete edits, it learns over rule-element features using LightGBM gradient-boosted trees with counterfactual reward estimation. It synthesises new rules from dominant decision paths in historical data and surfaces explanations to admins via change cards. This document describes the RLHF training pipeline, feature definitions, and how the learned models feed back into MaterializationPolicy.

---

## Pipeline Implementation Workstreams

**`NEXUS-Iter2-PipelineRegisters-v0.1.md`**
The coordination document for the five pipeline developer streams. It contains the dependency DAG between streams, the pairwise handover contracts (what each stream delivers to the next), the detailed specifications for all six shared system registers (`entity_resolution_index`, `golden_record_provenance`, `golden_records_index`, `golden_record_redirects`, `materialization_policy`, `store_presence`), and the end-to-end CRUD handling matrix. Developers should read this before their individual stream spec.

**`NEXUS-Iter2-CDCStreaming-v0.1.md`**
Covers the streaming ingestion layer: the Debezium CDC connector for PostgreSQL and the Airbyte bridge for SaaS sources, feeding into `spark-stream-transformer`. It defines how each source operation type (INSERT, UPDATE, DELETE, SNAPSHOT_READ, TRUNCATE) is handled, how schema drift is detected and surfaced, and how per-tenant Kafka partitioning provides fairness guarantees. The connector handover protocol — the contract between this stream and the ER-CRUD stream — is defined here.

**`NEXUS-Iter2-Backfill-v0.1.md`**
Specifies the `nexus_spark_lib` shared library used by both the streaming and batch layers, and the seven Airflow DAGs that handle initial data loads and ongoing reconciliation jobs (m3-reconciliation, CDM version migration, materialization promotion/demotion backfills, er-reindex). It includes the checkpointing strategy that makes all batch jobs restartable and the pre-flight cost estimation that prevents runaway jobs from overloading the cluster.

**`NEXUS-Iter2-ER-CRUD-v0.1.md`**
The most complex individual stream spec. It covers three-signal entity resolution (Jaro-Winkler, Levenshtein, Soundex+Metaphone) with LSH blocking to keep comparisons tractable, and the Neo4j graph lift that persists resolution decisions as edges. It then specifies Stage 3 Golden Record synthesis — how survivorship rules (recency, completeness, source priority) deterministically pick field values when multiple source records contribute — and the source-DELETE propagation algorithm that handles record deletion without corrupting surviving Golden Records.

**`NEXUS-Iter2-MaterializationCoordinator-v0.1.md`**
Implements Stage 0 of the pipeline: the policy evaluation step in Spark and the `m1-worker` that decides whether a given entity is hot, warm, or cold before it reaches the AI stores. It specifies the five tier-movement Airflow DAGs, the RLHF training loop that feeds signals back to `MaterializationFeatureLearning`, and the movement guardrails (rate limits, cool-down periods, manual-override respect). The M4 admin API for policy CRUD is also defined here.

**`NEXUS-Iter2-M3-Writers-v0.1.md`**
The consolidated implementation spec for all three AI store writers, covering the `store_presence` register that tracks which stores hold a given entity. For Elasticsearch it specifies the kNN upsert pattern (`_update` with `doc_as_upsert`), index naming (`nexus_{tenant_slug}_{entity_type}`), and the deletion tombstone. For Neo4j it covers the MERGE pattern with provenance edge attributes. For TimescaleDB it specifies append-only writes with `is_deletion = TRUE` for soft deletes. Cross-store reconciliation and drift detection are also defined here.

---

## AI Stores

**`NEXUS-Iter2-M3-AIStores-v0.4.md`**
The original high-level specification for the three AI stores. It defines the write contracts, idempotency rules, and the Virtual CDM principle that governs what data may and may not be stored. Note: the Pinecone sections in this document have been superseded — Elasticsearch is the vector store for Iteration 2. Read `M3-Elasticsearch-Writer-v0.1.md` for the authoritative Elasticsearch spec.

**`NEXUS-Iter2-M3-Elasticsearch-Writer-v0.1.md`**
The authoritative per-store spec for the Elasticsearch kNN writer. It defines the index schema (`dense_vector` with 1536 dimensions, cosine similarity), the upsert and deletion patterns, the two-phase query flow (Elasticsearch kNN → `cdm_entity_ids` → live values from connector workers), and the monitoring metrics. This is the document Elasticsearch integration work should be built against.

**`NEXUS-Iter2-M3-Neo4j-Writer-v0.1.md`**
Specifies how Golden Record relationships are persisted in Neo4j as a property graph. It covers node MERGE (using `cdm_entity_id` as the unique key), edge MERGE with `(start, end, type, source_fk)` attributes, provenance tracking, and the Cypher patterns for the entity resolution graph lift. Tenant isolation and index strategy are also defined here.

**`NEXUS-Iter2-M3-TimescaleDB-Writer-v0.1.md`**
Defines the append-only write pattern for the time-series AI store. Each Golden Record field change produces a new row; deletions are recorded as rows with `is_deletion = TRUE`. The spec covers hypertable partitioning, compression policies, and the retention schedule that moves cold data out of the hot store.

---

## CDM Governance

**`NEXUS-Iter2-CDM-Mapper-v0.3.md`**
Specifies the Iteration 2 improvements to the CDM mapper: idempotent classification using natural-key UPSERTs (so re-running the mapper on the same record is always safe), a ground-truth validation harness for testing mapping quality, and an RLHF placeholder that will collect human-correction signals in a future iteration. Tier 1/2/3 in this document refers exclusively to mapping confidence — it is distinct from the hot/warm/cold materialization levels used elsewhere.

**`NEXUS-Iter2-CDM-Validation-Workflow-v0.1.md`**
Introduces four new endpoints on `nexus-m4-api` that allow data stewards to validate CDM mapping proposals before approving them: a simulate dry-run, an LLM-powered recommendation engine, a structured decision endpoint, and a CDM version diff view. Human approval is always required — auto-approval is explicitly prohibited in Iteration 2. All LLM calls go through `agent_core.LLMClient` and `PIIChecker` for auditability and safety.

**`NEXUS-Iter2-RHMA-v0.1.md`**
Specifies the Reflexive Hierarchical Multi-Agent system that powers `nexus-m2-executor`. It introduces the `agent_core.orchestration` module with a Supervisor, five domain ExpertAgents (Finance, HR, CRM, Time-Series, Graph), and a CriticAgent that reviews expert outputs before they are returned. In Iteration 2 the agent dispatch is scaffolded — interfaces and tables are in place but no agent logic executes yet. Full dispatch begins after Iteration 3.

---

## Query & Presentation

**`NEXUS-Iter2-QueryEngine-v0.3.md`**
Specifies `nexus-query-api` (the sole user-facing query surface) and `nexus-query-executor` (the internal execution engine). It covers the HTTP and WebSocket entry points, the query planner that decomposes natural-language questions into sub-queries, OPA-based authorisation, the parallel executor that fans out to multiple data sources, and the result merger. The query engine reads the `store_presence` register to know which stores hold data for a given entity before dispatching.

**`NEXUS-Iter2-VisualOutputs-v0.2.md`**
Defines the `RenderedOutput` schema and the rendering pipeline that turns raw query results into charts, tables, dashboards, and exportable reports. It covers ChartSpec (the declarative chart definition), persona overrides (different visualisation defaults per user role), the export service (PDF, CSV, XLSX), and the ReportBuilder that assembles multi-section documents. This spec is consumed by the frontend and the query executor.

**`NEXUS-Iter2-M6-FrontendDelta-v0.2.md`**
A delta spec covering only what changes in the React frontend for Iteration 2. It defines the new TypeScript types, the `useQueryStream` hook that drives the streaming query panel, the list of new and modified components (query input, result renderer, dashboard grid, export controls), and the dashboard grid layout system. The M2 RHMA chat panel is deprecated here — `nexus-query-api` is the sole user-facing chat interface going forward.

---

## Shared Libraries

**`NEXUS-Iter2-LIB-NexusCore-v0.3.md`**
Specifies the Iteration 2 additions to `nexus_core`, the shared Python library used by all NEXUS services. New in this version: the updated Kafka topic definitions, `CdmEntity` model changes, the TimescaleDB helper, the FXService for currency conversion, the identity resolution utilities, and per-tenant schema support. Any service that needs to publish or consume Kafka events, interact with `CdmEntity` objects, or work with the TimescaleDB store imports from this library.

**`NEXUS-Iter2-LIB-AgentCore-v0.1.md`**
Specifies `agent_core`, the shared Python library for all AI and LLM work. It provides `LLMClient` (wraps the language model with automatic audit logging), `EmbeddingClient` (produces text embeddings for Elasticsearch), `PIIChecker` (redacts PII from prompts before submission), `CDMCatalogueBuilder` (constructs the CDM context payload used in expert prompts), `CrossTenantSafetyScanner` (prevents cross-tenant data leakage in generated responses), and `PromptRegistry` (manages versioned prompt templates). A planned v1.1 will add the `agent_core.orchestration` module required by RHMA.

---

*NEXUS Iteration 2 · Mentis Consulting · April 2026 · Confidential*
