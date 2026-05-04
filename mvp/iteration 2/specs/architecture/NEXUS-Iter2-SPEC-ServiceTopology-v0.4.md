# NEXUS — Iteration 2 · Updated Service Topology & Kafka Topic Map
**Version 0.4 · Mentis Consulting · April 2026 · Confidential**

> **See also (added 2026-04-27):** The CDM-to-AIStores pipeline series elaborates the M1→M3 path with five stages, a materialization-policy engine, the dynamic hot/warm/cold model, source-CRUD propagation, and the `entity_store_presence` register. New service introduced: `nexus-doc-processor` (document-track owner). New control topics: `m1.int.cdm_entities_ready` (internal), `nexus.materialization.changed`, `nexus.materialization_policy.changed`, `nexus.m3.write_completed/failed`, `nexus.materialization.recommendation_created`, `nexus.gr.state_changed`, `nexus.er.review_queued`, `nexus.backfill.{handover_changed,batch_started,batch_completed}`, `nexus.connector.refresh_required`. See `NEXUS-Iter2-CDM-AIStores-Pipeline-v0.1.md`, `NEXUS-Iter2-SystemOrchestration-v0.1.md`, and `NEXUS-Iter2-PipelineRegisters-v0.1.md`.
>
> **Revision v0.4 — Spark transformation stage introduced**
> `nexus-spark-transformer` added as a new M1 service between Kafka raw ingestion and the CDM Mapper. Spark consumes `m1.int.raw_records` and publishes `m1.int.transformed_records` — a clean, typed, entity-resolved record stream. CDM Mapper now consumes `m1.int.transformed_records` instead of `m1.int.raw_records`. New topic `m1.int.transformed_records` added to M1-owned topics. Delta Lake introduced as optional staging for batch/large-scale jobs only (not a queryable M3 store). Entity resolution (Golden Record ID assignment) moves to the Spark stage. Three new open questions added (OQ-SP-01, OQ-SP-02, OQ-SP-03). Service count updated.
> Ingestion tier section added documenting Debezium (CDC) and Airbyte (batch) as the two upstream producers for `m1.int.raw_records`. Debezium snapshot `READ` op handling clarified. `{tid}.m1.entity_removed` consumer group `m3-writer-entities` confirmed (deletion path shares the same consumer group as entity_routed, dispatched by a separate handler). Kafka topic table updated to explicitly list `{tid}.m1.entity_removed` as M1-owned with routing decision by nexus-m1-worker Op Router.
>
> **Revision v0.2 — Architecture review corrections applied**
> System dependency order added; Rule 0 added; Rule 6 elevated; `{tid}.m1.entity_routed` added; `{tid}.m1.mapping.approved` removed and consolidated under `{tid}.m4.mapping_approved`.

---

## Guiding Principle (unchanged from Iteration 1)

Module boundaries define **ownership**. Service boundaries define **runtime isolation**. The guiding principle from the Service Topology v1.0 document applies unchanged.

**Rule 0 (infrastructure invariant — predates all other rules):** Kong API Gateway is the sole JWT decoder in the system. Every JWT from Okta is validated at Kong, the `tenant_id` claim is extracted, and `X-Tenant-ID` / `X-User-ID` / `X-User-Role` are injected as HTTP headers before a request reaches any NEXUS service. **No application service ever decodes a JWT, calls Okta's JWKS endpoint, or re-validates a token.** Services trust the Kong-injected headers implicitly; they are protected by network policy that permits header ingress only from the Kong namespace.

The three inviolable architectural rules from Iteration 1 remain in force. Three new rules are added for Iteration 2:

- **Rule 4:** `nexus-query-executor` is the only service that contacts live source systems for query execution. It does so via the connector-worker Kafka request-reply pattern.
- **Rule 5:** OPA runs synchronously before any live source system receives a query from the Query Engine. A query denied by OPA is rejected before any data leaves NEXUS.
- **Rule 6:** User identity (`user_id`, `user_role`) is forwarded to source systems unchanged. The query executor forwards the caller's Okta `user_id` to connector-worker so that source-system RBAC applies to every live query. NEXUS never elevates a user's permissions at the source.

---

## System Dependency Order — Three-Layer Model

NEXUS services are not six independent modules — they form three concentric layers, each feeding the next. This dependency order governs implementation sequencing and sprint gates.

| Layer | Components | Constraint |
|---|---|---|
| **Layer 1 — Foundation** | M5 (Platform infra: Kubernetes, Kafka, Kong, ArgoCD, Grafana) + `nexus_core` shared library | Must exist before any other service can write a correct line of code. `nexus_core` is a hard Phase 1 gate: it enforces `NexusMessage` envelope structure, `TenantContext` with `contextvars`, tenant-scoped PostgreSQL connections via RLS, `CrossModuleTopicNamer` (no hand-crafted topic strings), and `OIDC_ISSUER_URL`-based identity. |
| **Layer 2 — Data Pipeline** | M1 (connectors + CDM Mapper + AI Store Router) → nexus-m3-writer, with M2 Structural Agent running alongside | M3 stores must be populated before Layer 3 can serve meaningful queries. The Iteration 2 milestone gate is M1→M3 integration passing end-to-end (Week 8–9). |
| **Layer 3 — Human Interface** | M4 (governance hub) + M6 (React UI) + nexus-query-api / nexus-query-executor | Depends on Layer 2 data flowing. M4 governance and M6 query UI can develop in parallel against mock data, but acceptance tests require live Layer 2 data. |

**`nexus_core` hard gate:** If `nexus_core` is not merged and published before sprint 1 ends, every other team is blocked. The Tech Lead's Week 1 deliverable is the `nexus_core` library with all enforced contracts. This is a non-negotiable dependency.

---

## Ingestion Tier — Upstream of Kafka

All data entering NEXUS flows through one of two ingestion mechanisms before reaching `m1.int.raw_records` on Kafka. These are not NEXUS microservices — they are managed infrastructure components operated by the Platform team (Dev 1).

| Mechanism | Technology | Mode | Produces to | When used |
|---|---|---|---|---|
| **CDC streaming** | Debezium (Kafka Connect) | Continuous, exactly-once | `m1.int.raw_records` | Source DB supports WAL/binlog (PostgreSQL, MySQL, Oracle, SQL Server) |
| **Batch ingestion** | Airbyte | Scheduled / triggered | `m1.int.raw_records` | Historical backfill; systems without CDC support; REST/GraphQL APIs |

Both mechanisms write to `m1.int.raw_records`. **Neither produces a record ready for CDM classification directly** — raw source records carry heterogeneous types, non-normalised currencies, potentially duplicate keys, and unresolved cross-source identities. A dedicated Spark transformation stage sits between `m1.int.raw_records` and the CDM Mapper.

### Spark Transformation Stage (`nexus-spark-transformer`)

A new M1 service, `nexus-spark-transformer`, consumes `m1.int.raw_records` and publishes to `m1.int.transformed_records`. It handles all mechanical transformation before the CDM Mapper touches a record. The CDM Mapper's responsibility is semantic classification only — it never sees a raw source row.

**What Spark does per batch:**

| Responsibility | Description |
|---|---|
| Type coercion | Normalise date formats (YYYY-MM-DD bare), numeric precision, boolean variants |
| Currency normalisation | Convert monetary fields to tenant base currency via `FXService`; store `original_currency` and `fx_rate` in field metadata |
| Data quality | Compute null rates, range violations, format consistency per field; attach quality flags to the record |
| Deduplication | Within a connector snapshot, deduplicate on natural keys before publishing |
| Entity resolution | Look up or assign a `cdm_entity_id` (Golden Record ID) by matching source identifiers against `nexus_system.entity_resolution_index`; enables cross-source joins downstream |
| Schema profiling | Update `nexus_system.schema_snapshots` cardinality and type statistics inline |

**Operating modes:**

| Mode | Spark mode | Delta Lake | Latency |
|---|---|---|---|
| Real-time CDC | Structured Streaming (micro-batch, 500ms trigger) | Bypassed | P95 ≤ 2s end-to-end |
| Batch history | Batch job (Spark on Kubernetes via `spark-submit`) | Optional checkpoint — used when record count > configurable threshold (default 500k) | Minutes to hours depending on volume |

**Delta Lake as optional staging (batch only):** When a batch job processes more than the checkpoint threshold, Spark writes transformed records to a Delta Lake table (`nexus_delta.transformed_{tenant_id}_{connector_id}`) before publishing to `m1.int.transformed_records`. This checkpoint survives job failures — a restart reads from Delta Lake rather than re-pulling from the source system. For real-time CDC and small batch jobs, Delta Lake is bypassed entirely.

> **OQ-SP-01 — Spark infrastructure:** Does `nexus-spark-transformer` run as a long-lived Kubernetes `Deployment` (always-on Structured Streaming for CDC) with Spark batch jobs submitted on demand, or as separate services for CDC and batch? Recommendation: long-lived deployment for CDC streaming, ephemeral `spark-submit` jobs via Airflow for batch. Owner: Tech Lead + Platform team.

> **OQ-SP-02 — Entity resolution index:** `nexus_system.entity_resolution_index` (mapping source identifiers to `cdm_entity_id`) must be seeded. Where does the initial seed come from — Iteration 1 approved mappings, or a first-run Spark job that generates it? Owner: Data Intelligence team.

> **OQ-SP-03 — Delta Lake threshold:** The 500k record threshold for enabling Delta Lake checkpointing is a default. Should it be configurable per connector in `connector_batch_state`? Recommendation: yes — add `delta_checkpoint_threshold INT DEFAULT 500000` to `connector_batch_state`.

### Debezium CDC

Debezium connectors attach to the source database transaction log and emit a change event for every committed row modification. Two operating modes:

| Mode | When | Operation emitted |
|---|---|---|
| **Snapshot** | Once, on connector registration (full table read) | `READ` — treated as `CREATE` by the Op Router (see Rule 7 below) |
| **Streaming** | Ongoing after snapshot completes | `INSERT` → op `c`, `UPDATE` → op `u`, `DELETE` → op `d` |

Log position (LSN / binlog offset) is committed to a Kafka Connect offset topic after each event batch. Connector restart resumes from the last committed offset — downstream idempotency (MERGE, upsert, `ON CONFLICT DO NOTHING`) makes reprocessing safe.

**Latency target:** P95 ≤ 5 seconds source commit → M3 write committed under steady-state load.

### Airbyte Batch

Airbyte connector jobs are driven by the `nexus_batch_history_ingest` Airflow DAG (D1-06). Cursor state (high-water mark of `last_cursor_value`) is persisted in `nexus_system.connector_batch_state` after each committed batch, so partial runs resume without reprocessing history.

Configuration per connector: `years_back` (how far back to pull history), `batch_size` (records per Kafka batch), `cursor_field` (typically `updated_at` or a monotonic sequence ID).

**Rule 7 — Debezium snapshot `READ` op is treated as `CREATE`.** The Op Router inside `nexus-m1-worker` maps Debezium's `READ` operation code (emitted during snapshot mode) to the upsert path (`entity_routed`), identical to `INSERT`. This ensures snapshot and streaming modes produce identical downstream behaviour. No code path should treat `READ` as a no-op.

---



| Service | Iter 1 | Iter 2 | Change |
|---|---|---|---|
| nexus-m1-api | ✅ | ✅ | Unchanged |
| nexus-m1-worker | ✅ | ✅ | Unchanged |
| nexus-cdm-mapper | ✅ | ✅ | Input topic changed: now consumes `m1.int.transformed_records` |
| nexus-schema-profiler | ✅ | ✅ | Unchanged |
| **nexus-spark-transformer** | — | ➕ | NEW — Spark transformation stage between raw ingestion and CDM Mapper |
| nexus-m2-api | ✅ | ✅ | Internal/programmatic only — user-facing chat routes to `nexus-query-api` as of Iteration 2 (OQ-M6-01 resolved) |
| nexus-m2-executor | ✅ | ✅ | Unchanged |
| nexus-m4-api | ✅ | ✅ | Unchanged |
| nexus-m4-worker | ✅ | ✅ | Unchanged |
| nexus-query-api | ➕ | NEW |
| nexus-query-executor | ➕ | NEW |
| nexus-m3-writer | ➕ | NEW |

**Total: 8 services in Iteration 1 → 11 services in Iteration 2**

---

## Iteration 2 Service Topology Diagram

```
                    ┌─────────────────────────────────────────────────────────────┐
                    │                     Kong API Gateway                         │
                    │         JWT validation · X-Tenant-ID injection              │
                    └──────┬───────────┬────────────────┬──────────────┬──────────┘
                           │           │                │              │
                    nexus-m1-api  nexus-m2-api*  nexus-m4-api  nexus-query-api
                    (* internal/programmatic only — no user-facing routes post-Iter-2)
                           │           │                │              │
                           └───────────┴────────────────┴──────────────┘
                                                │
                                   ┌────────────▼──────────────────┐
                                   │          Apache Kafka           │
                                   │  (all inter-service async bus)  │
                                   └────┬──────┬────┬───────────────┘
                                        │      │    │     │
                             nexus-m1-worker   │  nexus-m2-executor
                               (24/7)          │
                                               │  nexus-m4-worker
                             nexus-cdm-mapper──┤
                               (scale-to-zero) │
                                               │  nexus-query-executor  ← NEW
                             nexus-m3-writer───┘
                               (KEDA, event-driven) ← NEW

                             nexus-schema-profiler
                               (K8s Job — on connector registration + weekly)

                    ┌──────────────────────────────────────────────────────────┐
                    │                    AI Stores (M3)                         │
                    │                                                           │
                    │  Elasticsearch      Neo4j Aura         TimescaleDB        │
                    │  (kNN search)       (graph traversal)  (time-series)      │
                    │  ← Written by nexus-m3-writer (NEW)                       │
                    │  ← Read by nexus-m2-executor + nexus-query-executor       │
                    └──────────────────────────────────────────────────────────┘
```

---

## Per-Service Runtime Profile Summary

| Service | Type | Scales on | Min | Max | Team |
|---|---|---|---|---|---|
| nexus-m1-api | Request API | CPU | 1 | 3 | Data Intelligence |
| nexus-m1-worker | Event worker | Kafka lag (KEDA) | 2 | 10 | Data Intelligence |
| nexus-cdm-mapper | Event worker (scale-to-zero) | Kafka lag (KEDA) | 0 | 5 | Data Intelligence |
| nexus-schema-profiler | K8s Job | Schedule / connector reg | — | — | Data Intelligence |
| nexus-m2-api | Request API + WS | CPU / WS connections | 2 | 5 | AI & Knowledge |
| nexus-m2-executor | Event worker | Kafka lag (KEDA) | 2 | 12 | AI & Knowledge |
| nexus-m4-api | Request API | CPU | 1 | 3 | Product |
| nexus-m4-worker | Event worker | Kafka lag (KEDA) | 1 | 3 | Product |
| **nexus-query-api** | Request API + WS | CPU / WS connections | 2 | 6 | AI & Knowledge |
| **nexus-query-executor** | Event worker | Kafka lag (KEDA) | 2 | 12 | AI & Knowledge |
| **nexus-m3-writer** | Event worker | Kafka lag (KEDA) | 1 | 6 | Data Intelligence |

---

## Kafka Topic Ownership Map — Iteration 2 (Complete)

All Iteration 1 topics remain unchanged. New Iteration 2 topics are marked **NEW**.

### M1-Owned Topics

| Topic | Owner | Producer | Consumer(s) |
|---|---|---|---|
| `m1.int.sync_requested` | M1 | nexus-m1-api, nexus-cdm-mapper (backfill), Airflow (`nexus_batch_history_ingest` DAG) | nexus-m1-worker |
| `m1.int.raw_records` | M1 | nexus-m1-worker (from Debezium CDC or Airbyte batch) | **nexus-spark-transformer** |
| **`m1.int.transformed_records`** | M1 | **nexus-spark-transformer** — typed, normalised, entity-resolved records | **nexus-cdm-mapper** |
| `m1.int.cdm_entities_ready` | M1 | nexus-cdm-mapper | nexus-m1-worker (AI router) |
| `m1.int.mapping_failed` | M1 | nexus-cdm-mapper | nexus-m4-worker |
| `m1.int.dead_letter` | M1 | nexus-m1-worker, nexus-cdm-mapper, nexus-spark-transformer | ops tooling |
| `{tid}.m1.sync_completed` | M1 | nexus-m1-worker | M6 (pipeline health) |
| `{tid}.m1.semantic_interpretation_requested` | M1 | nexus-cdm-mapper, nexus-schema-profiler | nexus-m2-executor |
| **`{tid}.m1.entity_routed`** | M1 | nexus-m1-worker (`m1-ai-router`) Op Router — op: create, update, READ (snapshot) | **nexus-m3-writer** (`m3-writer-entities` consumer group) |
| **`{tid}.m1.entity_removed`** | M1 | nexus-m1-worker (`m1-ai-router`) Op Router — op: delete | **nexus-m3-writer** (`m3-writer-entities` consumer group, separate handler) |

*`entity_routed` and `entity_removed` are both published by the Op Router inside `nexus-m1-worker`. `entity_routed` carries all upsert operations (CREATE, UPDATE, and Debezium snapshot READ — see Rule 7). `entity_removed` carries DELETE operations only. Both topics are consumed by the same `m3-writer-entities` consumer group, which dispatches to `write()` or `delete()` on each store writer based on the topic.*

### M2-Owned Topics

| Topic | Owner | Producer | Consumer(s) |
|---|---|---|---|
| `{tid}.m2.knowledge_query` | M2 | nexus-m2-api | nexus-m2-executor |
| `{tid}.m2.agent_response_ready` | M2 | nexus-m2-executor | nexus-m2-api (WS relay) |
| `{tid}.m2.workflow_trigger` | M2 | nexus-m2-executor | nexus-m4-worker |
| `{tid}.m2.semantic_interpretation_complete` | M2 | nexus-m2-executor | nexus-m4-worker (CDM governance) |

### M3-Owned Topics (NEW)

| Topic | Owner | Producer | Consumer(s) |
|---|---|---|---|
| **`nexus.m3.write_completed`** | M3 | nexus-m3-writer | Grafana / observability tooling |
| **`nexus.m3.write_failed`** | M3 | nexus-m3-writer | Alerting / on-call |

### M4-Owned Topics

| Topic | Owner | Producer | Consumer(s) |
|---|---|---|---|
| `nexus.cdm.extension_proposed` | M4 | nexus-m2-executor | nexus-m4-worker |
| `nexus.cdm.version_published` | M4 | nexus-m4-api | nexus-cdm-mapper, nexus-m2-executor, **nexus-m3-writer** (NEW), **nexus-query-executor** (cache invalidation, NEW) |
| `nexus.cdm.extension_rejected` | M4 | nexus-m4-api | ops tooling |
| `{tid}.m4.mapping_approved` | M4 | nexus-m4-api | nexus-cdm-mapper (cache invalidation), **nexus-m3-writer** (catch-up writes for Tier 2→Tier 1 promotions, NEW) |
| `nexus.m4.governance_escalation` | M4 | Airflow SLA DAG | M6 (pipeline health) |

### Query Engine Topics (NEW)

| Topic | Owner | Producer | Consumer(s) |
|---|---|---|---|
| **`{tid}.query.submitted`** | Query Engine | nexus-query-api | nexus-query-executor |
| **`{tid}.query.event`** | Query Engine | nexus-query-executor | nexus-query-api (WS relay) |
| **`{tid}.connector.query_requested`** | Query Engine | nexus-query-executor | nexus-m1-worker (connector execution) |
| **`{tid}.connector.query_result`** | M1 | nexus-m1-worker | nexus-query-executor (request-reply) |

---

## Consumer Group Registry — Iteration 2 (Complete)

| Consumer group | Service | Topics consumed |
|---|---|---|
| `m1-connector-workers` | nexus-m1-worker | `m1.int.sync_requested` |
| `m1-ai-router` | nexus-m1-worker | `m1.int.cdm_entities_ready` |
| `m1-connector-query-handler` | nexus-m1-worker | `{tid}.connector.query_requested` **NEW** |
| **`m1-spark-transformer`** | nexus-spark-transformer | `m1.int.raw_records` **NEW** — transformation stage |
| `m1-cdm-mapper` | nexus-cdm-mapper | **`m1.int.transformed_records`** (changed from `m1.int.raw_records`) |
| `m1-cache-invalidator` | nexus-cdm-mapper | `{tid}.m4.mapping_approved` |
| `m1-cdm-version-listener` | nexus-cdm-mapper | `nexus.cdm.version_published` |
| `m2-structural-agents` | nexus-m2-executor | `{tid}.m1.semantic_interpretation_requested` |
| `m2-query-executors` | nexus-m2-executor | `{tid}.m2.knowledge_query` |
| `m2-api-websocket-relay` | nexus-m2-api | `{tid}.m2.agent_response_ready` |
| `m4-cdm-governance` | nexus-m4-worker | `nexus.cdm.extension_proposed` |
| `m4-mapping-exceptions` | nexus-m4-worker | `m1.int.mapping_failed` |
| `m4-cdm-version-listener` | nexus-m4-worker | `nexus.cdm.version_published` |
| `m4-workflow-triggers` | nexus-m4-worker | `{tid}.m2.workflow_trigger` |
| **`m3-writer-entities`** | nexus-m3-writer | `{tid}.m1.entity_routed` (upsert path) + `{tid}.m1.entity_removed` (delete path) — both topics dispatched by the same consumer group, separate handlers |
| **`m3-writer-mapping-approved`** | nexus-m3-writer | `{tid}.m4.mapping_approved` — catch-up writes on Tier 2→Tier 1 promotions |
| **`m3-writer-cdm-version`** | nexus-m3-writer | `nexus.cdm.version_published` — Elasticsearch batch re-embedding |
| **`query-executor`** | nexus-query-executor | `{tid}.query.submitted` **NEW** |
| **`query-executor-cache-invalidator`** | nexus-query-executor | `nexus.cdm.version_published` **NEW** |
| **`query-api-ws-relay`** | nexus-query-api | `{tid}.query.event` **NEW** |

---

## Environment Variables — Iteration 2 Additions

All Iteration 1 variables remain unchanged. New variables added in Iteration 2:

| Variable | New Services | Source |
|---|---|---|
| `ELASTICSEARCH_URL` | nexus-m3-writer, nexus-query-executor | ConfigMap |
| `ELASTICSEARCH_API_KEY` | nexus-m3-writer, nexus-query-executor | Secrets Manager |
| `NEO4J_URI` | nexus-m3-writer, nexus-query-executor | ConfigMap |
| `NEO4J_USERNAME` | nexus-m3-writer, nexus-query-executor | Secrets Manager |
| `NEO4J_PASSWORD` | nexus-m3-writer, nexus-query-executor | Secrets Manager |
| `TIMESCALEDB_DSN` | nexus-m3-writer, nexus-query-executor | Secrets Manager |
| `POSTGRES_READONLY_DSN` | nexus-m3-writer | Secrets Manager |
| `ANTHROPIC_API_KEY` | nexus-query-executor | Secrets Manager |
| `OPA_URL` | nexus-query-executor | ConfigMap |
| `QUERY_PLAN_LLM_TIMEOUT_SECONDS` | nexus-query-executor | ConfigMap |
| `SOURCE_QUERY_TIMEOUT_SECONDS` | nexus-query-executor | ConfigMap |
| `MINIO_ENDPOINT` | nexus-query-executor | ConfigMap |
| `MINIO_ACCESS_KEY` | nexus-query-executor | Secrets Manager |
| `MINIO_SECRET_KEY` | nexus-query-executor | Secrets Manager |
| `QUERY_TIMEOUT_SECONDS` | nexus-query-api | ConfigMap |
| `EXPORT_MAX_ROWS` | nexus-query-api | ConfigMap |
| `EMBEDDING_MODEL` | nexus-m3-writer | ConfigMap |
| `ELASTICSEARCH_UPSERT_BATCH_SIZE` | nexus-m3-writer | ConfigMap |
| `M3_WRITE_TIMEOUT_SECONDS` | nexus-m3-writer | ConfigMap |

---

## Network Policy Summary — New Services

### nexus-query-api

| Direction | Allows |
|---|---|
| Ingress | Kong (nexus-infra namespace) on port 8005; nexus-monitoring on port 8005 |
| Egress | Kafka (9092), PostgreSQL (5432), Redis (6379) in nexus-data namespace |
| Egress | nexus-opa (8181) in nexus-infra namespace |

### nexus-query-executor

| Direction | Allows |
|---|---|
| Ingress | None (Kafka consumer only) |
| Egress | Kafka (9092), PostgreSQL (5432), Redis (6379) in nexus-data namespace |
| Egress | Elasticsearch (9200 cluster or 443 managed), Neo4j Aura (7687, external), TimescaleDB (5432) |
| Egress | Anthropic API (443, external), OpenAI API (443, external — for embeddings) |
| Egress | nexus-opa (8181) in nexus-infra namespace |
| Egress | MinIO (9000) in nexus-data namespace |

### nexus-m3-writer

| Direction | Allows |
|---|---|
| Ingress | None (Kafka consumer only) |
| Egress | Kafka (9092), PostgreSQL (5432) in nexus-data namespace |
| Egress | Elasticsearch (9200 cluster or 443 managed), Neo4j Aura (7687, external), TimescaleDB (5432) |
| Egress | OpenAI API (443, external — for embeddings) |
| No access | Secrets Manager tenant credentials (`nexus/tenants/*/credentials`) |
| No access | Source system IPs |

---

## Service Account Summary

| ServiceAccount | Services | Key permissions |
|---|---|---|
| nexus-m1-api-sa | nexus-m1-api | nexus_system.connectors (r/w), nexus_system.sync_jobs (w) |
| nexus-m1-worker-sa | nexus-m1-worker | Source credentials path in Secrets Manager, MinIO write |
| nexus-cdm-mapper-sa | nexus-cdm-mapper | nexus_system.cdm_mappings (r), MinIO read |
| nexus-m2-api-sa | nexus-m2-api | nexus_system.query_sessions (r/w), Redis |
| nexus-m2-executor-sa | nexus-m2-executor | Neo4j, TimescaleDB, LLM APIs, Secrets Manager (no direct Elasticsearch — reads via nexus-query-executor) |
| nexus-m4-api-sa | nexus-m4-api | nexus_system.governance_queue (r/w), cdm_versions (r/w), Airflow |
| nexus-m4-worker-sa | nexus-m4-worker | nexus_system.governance_queue (w), mapping_review_queue (w), Temporal |
| **nexus-query-api-sa** | nexus-query-api | nexus_system.query_sessions (r/w), Redis, dashboard_components (r/w) |
| **nexus-query-executor-sa** | nexus-query-executor | Elasticsearch (read), Neo4j (read), TimescaleDB (read), LLM APIs, MinIO (write), OPA |
| **nexus-m3-writer-sa** | nexus-m3-writer | Elasticsearch (write), Neo4j (write), TimescaleDB (write), PostgreSQL (read-only) |

---

## Iteration 2 Split Decisions (Carried Forward from Iter 1)

The split trigger identified in Iteration 1 for `nexus-m2-executor` (P99 > 15s or LLM cost dominates) has **not yet been reached**. `nexus-query-executor` is introduced as a new service rather than a split — the two executors serve different pipelines and have independent operational profiles.

No additional splits are planned for Iteration 2. Re-evaluate at Iteration 3 boundary using:
- `nexus-query-executor`: consider splitting into `nexus-query-planner` (LLM-heavy) + `nexus-query-runner` (I/O-heavy) if P99 > 15s
- `nexus-m1-worker`: consider separating connector-worker from the new `connector.query_requested` handler if query response lag affects sync job throughput

---

*NEXUS Service Topology · Iteration 2 Update · v0.2 · Mentis Consulting · April 2026 · Confidential*
