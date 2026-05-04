# NEXUS — Iteration 2 · Specification Index

All Iteration 2 specifications live in this folder, organised into seven subfolders by concern. The sprint plan lives at this root level.

---

## Sprint Plan

| File | Description |
|---|---|
| `NEXUS-Iter2-SprintPlan-v0.3.md` | Master task list — all workstreams, phase gates, estimates, dependencies, and open questions |

---

## `architecture/`

| File | Description |
|---|---|
| `architecture/NEXUS-Iter2-SPEC-ServiceTopology-v0.4.md` | Three-layer service model, inviolable rules (0–7), Kafka topic ownership, consumer group registry, network policies. Spark transformation stage and Debezium / Airbyte ingestion tier. |
| `architecture/NEXUS-Iter2-SPEC-DataModel-v0.5.md` | All new and modified DB tables, TimescaleDB DDL, identity_mapping, migration scripts V2.0.1–V2.0.19. Canonical source of truth for migration sequencing. |
| `architecture/NEXUS-Iter2-ArchReview-v0.2.md` | Cross-spec consistency review — findings, corrections applied, open questions |

---

## `pipeline/`

The end-to-end seam between "CDM published" and "data queryable from Elasticsearch, Neo4j, TimescaleDB". Five documents stack in increasing operational depth.

| File | Layer | Description |
|---|---|---|
| `pipeline/NEXUS-Iter2-CDM-AIStores-Pipeline-v0.1.md` | Architecture | Parent spec — five-stage pipeline, CRUD handling, edge-level provenance, Virtual CDM principle |
| `pipeline/NEXUS-Iter2-RecordLifecycle-Structured-v0.1.md` | Walkthrough | Concrete trace of one Salesforce record through all 10 pipeline phases (~31s end-to-end) |
| `pipeline/NEXUS-Iter2-SystemOrchestration-v0.1.md` | Operations | Three Spark applications, nine-job batch DAG catalogue, Golden Record state machine, per-store routing matrix |
| `pipeline/NEXUS-Iter2-MaterializationPolicy-v0.1.md` | Policy engine | Policy-driven hot/warm/cold rules: five rule types, predicate grammar, deterministic resolution |
| `pipeline/NEXUS-Iter2-MaterializationFeatureLearning-v0.1.md` | RLHF | LightGBM feature learning loop; rule synthesis from decision paths; change-card explanations |

---

## `developer-workstreams/`

Six pipeline workstream specs plus eight service specs (new services and Iteration 2 delta specs). Total pipeline effort: 24 person-weeks across 5 developers.

### Service specs

| File | Service | Type |
|---|---|---|
| `developer-workstreams/NEXUS-Iter2-SVC-nexus-spark-transformer-v0.1.md` | `nexus-spark-transformer` | NEW — full spec |
| `developer-workstreams/NEXUS-Iter2-SVC-nexus-airbyte-stream-bridge-v0.1.md` | `nexus-airbyte-stream-bridge` | NEW — full spec |
| `developer-workstreams/NEXUS-Iter2-SVC-nexus-m1-api-v0.1.md` | `nexus-m1-api` | Iter 2 delta |
| `developer-workstreams/NEXUS-Iter2-SVC-nexus-schema-profiler-v0.1.md` | `nexus-schema-profiler` | Iter 2 delta |
| `developer-workstreams/NEXUS-Iter2-SVC-nexus-m2-api-v0.1.md` | `nexus-m2-api` | Iter 2 delta — breaking role change |
| `developer-workstreams/NEXUS-Iter2-SVC-nexus-m1-worker-CDCStreaming-v0.1.md` | `nexus-m1-worker` (extended) | Pipeline workstream + service delta |
| `developer-workstreams/NEXUS-Iter2-SVC-nexus-m3-writer-v0.1.md` | `nexus-m3-writer` | NEW — pipeline workstream + full spec |

### Pipeline workstream specs

| File | Stream | Effort | Primary deliverable |
|---|---|---|---|
| `developer-workstreams/NEXUS-Iter2-SPEC-PipelineRegisters-v0.1.md` | Coordination | — | Dependency DAG, pairwise contracts, the six system registers, CRUD handling end-to-end |
| `developer-workstreams/NEXUS-Iter2-SVC-nexus-m1-worker-CDCStreaming-v0.1.md` | Dev 1 — streaming | 4pw | `nexus-m1-worker` extension, Debezium, `nexus-airbyte-stream-bridge`, `nexus-spark-transformer` deployment |
| `developer-workstreams/NEXUS-Iter2-SPEC-Backfill-v0.1.md` | Dev 2 — batch | 4pw | `nexus_spark_lib`; seven Airflow DAGs; checkpointing; pre-flight cost estimation |
| `developer-workstreams/NEXUS-Iter2-SPEC-ER-CRUD-v0.1.md` | Dev 3 — ER and CRUD | 6pw | Three-signal entity resolution with LSH blocking; Golden Record synthesis; source-DELETE propagation |
| `developer-workstreams/NEXUS-Iter2-SPEC-MaterializationCoordinator-v0.1.md` | Dev 4 — tier coordinator | 5pw | Stage 0 policy evaluation; tier-movement DAGs; RLHF training loop; M4 admin API |
| `developer-workstreams/NEXUS-Iter2-SVC-nexus-m3-writer-v0.1.md` | Dev 5 — store writers | 5pw | Elasticsearch / Neo4j / TimescaleDB writers; `entity_store_presence` register; cross-store reconciliation |

---

## `ai-stores/`

| File | Description |
|---|---|
| `ai-stores/NEXUS-Iter2-SPEC-M3-AIStores-v0.5.md` | `nexus-m3-writer` · Architectural reference — Virtual CDM principle, write contracts, idempotency rules, per-store invariants, FRs/NFRs |
| `ai-stores/NEXUS-Iter2-SVC-nexus-m3-writer-stores-v0.1.md` | `nexus-m3-writer` · **Master service spec** — scope, data model, Kafka contracts, CRUD matrix, hot/warm/cold movement, `presence_lookup` API, acceptance criteria |
| `ai-stores/NEXUS-Iter2-SVC-nexus-m3-writer-elasticsearch-v0.1.md` | `nexus-m3-writer` · **Developer A** — Elasticsearch handler: index schema, upsert algorithm, kNN query contract, circuit breaker |
| `ai-stores/NEXUS-Iter2-SVC-nexus-m3-writer-neo4j-v0.1.md` | `nexus-m3-writer` · **Developer B** — Neo4j handler: DDL, node/edge MERGE patterns, DETACH DELETE, OrgChartBuilder |
| `ai-stores/NEXUS-Iter2-SVC-nexus-m3-writer-timescaledb-v0.1.md` | `nexus-m3-writer` · **Developer C** — TimescaleDB handler: hypertable DDL, continuous aggregates, write paths, FX normalisation |

---

## `cdm-governance/`

| File | Description |
|---|---|
| `cdm-governance/NEXUS-Iter2-SVC-nexus-cdm-mapper-v0.3.md` | `nexus-cdm-mapper` · Idempotent CDM classification, ground-truth harness, RLHF placeholder. Tier 1/2/3 = mapping confidence only. |
| `cdm-governance/NEXUS-Iter2-SVC-nexus-m4-api-nexus-m4-worker-CDMValidation-v0.1.md` | `nexus-m4-api` + `nexus-m4-worker` · Simulate / recommend / decide endpoints; LLM-assisted rationale; human approval always required |
| `cdm-governance/NEXUS-Iter2-SVC-nexus-m2-executor-RHMA-v0.1.md` | `nexus-m2-executor` · `agent_core.orchestration` — Supervisor / ExpertAgent / CriticAgent; 5 expert roles; scaffolded in Iter 2 |

---

## `query-frontend/`

| File | Description |
|---|---|
| `query-frontend/NEXUS-Iter2-SVC-nexus-query-api-nexus-query-executor-v0.3.md` | `nexus-query-api` + `nexus-query-executor` · Query planner, OPA auth, parallel executor, WebSocket streaming |
| `query-frontend/NEXUS-Iter2-SPEC-VisualOutputs-v0.2.md` | RenderedOutput schema, ChartSpec, persona overrides, export service, ReportBuilder |
| `query-frontend/NEXUS-Iter2-SPEC-M6-FrontendDelta-v0.2.md` | React delta — TypeScript types, `useQueryStream` hook, component list, dashboard grid |

---

## `libraries/`

| File | Description |
|---|---|
| `libraries/NEXUS-Iter2-LIB-NexusCore-v0.3.md` | `nexus_core` v2 — Kafka topics, `CdmEntity` updates, TimescaleDB helper, FXService, identity resolution |
| `libraries/NEXUS-Iter2-LIB-AgentCore-v0.1.md` | `agent_core` v1 — `LLMClient`, `EmbeddingClient`, `PIIChecker`, `CDMCatalogueBuilder`, `CrossTenantSafetyScanner`, `PromptRegistry` |

---

## Read order for someone joining mid-stream

1. `architecture/NEXUS-Iter2-SPEC-ServiceTopology-v0.4.md` — the system layout and rules.
2. `architecture/NEXUS-Iter2-SPEC-DataModel-v0.5.md` — the data baseline.
3. `pipeline/NEXUS-Iter2-CDM-AIStores-Pipeline-v0.1.md` — the pipeline architecture this iteration builds.
4. `pipeline/NEXUS-Iter2-RecordLifecycle-Structured-v0.1.md` — to ground the pipeline in a concrete trace.
5. `developer-workstreams/NEXUS-Iter2-PipelineRegisters-v0.1.md` — the dev-facing decomposition + register catalogue.
6. The five developer workstream specs in order.
7. `pipeline/NEXUS-Iter2-MaterializationPolicy-v0.1.md` and `pipeline/NEXUS-Iter2-MaterializationFeatureLearning-v0.1.md` for the dynamic tier model and RLHF loop.

---

## Naming conventions

- **Tier 1 / Tier 2 / Tier 3** = CDM mapping confidence (per `cdm-governance/NEXUS-Iter2-SVC-nexus-cdm-mapper-v0.3.md`), never anything else.
- **Materialization level** = hot / warm / cold (per the policy spec). Stored in columns named `materialization_level`, never `tier`.
- **Internal pipeline topics** = `m1.int.*` (no tenant prefix; `tenant_id` carried in payload).
- **Cross-module topics** = `{tid}.m1.*` or `nexus.*` (tenant-scoped or platform-scoped).
- **Golden Record IDs** = `gr:` + sha256 of `tenant_id || cdm_entity_type || canonical_blocking_key`, truncated to 128 bits.

---

## Dependency Chain

```
libraries/ (nexus_core v2 + agent_core v1)   ← Week 1 hard gate
        │
        ├── SVC-nexus-m1-worker-CDCStreaming + SPEC-Backfill   (data into Delta Lake)
        │   [nexus-m1-worker, nexus-spark-transformer, nexus-airbyte-stream-bridge]
        │       │
        │       └── SPEC-ER-CRUD                            (Golden Records + registers)
        │               │
        │               └── SPEC-MaterializationCoordinator
        │                       │
        │                       └── SVC-nexus-m3-writer
        │                               │
        │                               ├── SVC-nexus-m3-writer-elasticsearch  }
        │                               ├── SVC-nexus-m3-writer-neo4j          } store specs
        │                               └── SVC-nexus-m3-writer-timescaledb    }
        │
        └── SVC-nexus-query-api-nexus-query-executor
                └── SPEC-M6-FrontendDelta
```

---

*NEXUS Iteration 2 · Mentis Consulting · April 2026*
