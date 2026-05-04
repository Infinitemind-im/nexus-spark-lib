# NEXUS — Service Topology & Per-Service Specifications
**Version 1.0 · Mentis Consulting · March 2026 · Confidential**

---

## Guiding Principle

Module boundaries define **ownership** — which team writes the code, which Kafka topics it produces, which domain models it owns. Service boundaries define **runtime isolation** — independent scaling, failure, and deployment. The two do not have to align one-to-one.

NEXUS components fall into three runtime categories that determine the correct deployment type:

| Category | Deployment type | Scales on | Example |
|---|---|---|---|
| **Event-driven worker** | Kubernetes `Deployment` | Kafka consumer lag (HPA via KEDA) | nexus-m1-worker |
| **Request-driven API** | Kubernetes `Deployment` | HTTP request rate / latency | nexus-m1-api |
| **Scheduled / triggered job** | Kubernetes `Job` or Airflow DAG | Schedule / event trigger | nexus-schema-profiler |

All services share infrastructure (Kafka, PostgreSQL, Redis, MinIO) but own separate container images, resource quotas, health checks, and RBAC ServiceAccounts.

---

## Iteration 1 Service Topology

Eight services for Iteration 1. `nexus-cdm-mapper` is split from `nexus-m1-worker` immediately based on known operational profiles: the connector worker runs 24/7, the CDM mapper runs monthly on CDM version publish. Merging them would waste resources around the clock for a workload that is active ~2 hours per month.

```
                    ┌─────────────────────────────────────────────────┐
                    │              Kong API Gateway                    │
                    │   JWT validation · X-Tenant-ID injection         │
                    └──────────┬───────────────┬────────────────┬─────┘
                               │               │                │
                       nexus-m1-api    nexus-m2-api    nexus-m4-api
                               │               │                │
                               └───────────────┴────────────────┘
                                               │
                              ┌────────────────▼──────────────────┐
                              │          Apache Kafka               │
                              │   (all inter-service communication) │
                              └────┬──────┬────┬───────────────────┘
                                   │      │    │
                        nexus-m1-worker   │  nexus-m2-executor
                          (24/7)          │
                                          │  nexus-m4-worker
                        nexus-cdm-mapper──┘
                          (scale-to-zero,
                           monthly runs)

                        nexus-schema-profiler
                          (K8s Job — on connector
                           registration + weekly)
```

---

## Service Specifications

---

### `nexus-m1-api`
**Module:** M1 — Data Intelligence & Mediation
**Type:** Request-driven API
**Team:** Data Intelligence

#### Responsibility
Thin HTTP surface for connector lifecycle management. Receives connector registration requests, validates input, persists connector metadata to PostgreSQL, and publishes `m1.int.sync_requested` to Kafka to hand off actual execution. Returns immediately — it never queries source systems.

#### Endpoints

| Method | Path | Description |
|---|---|---|
| `POST` | `/api/v1/connectors` | Register a new connector for a tenant |
| `GET` | `/api/v1/connectors` | List connectors for authenticated tenant |
| `GET` | `/api/v1/connectors/{id}` | Get connector status and metadata |
| `DELETE` | `/api/v1/connectors/{id}` | Deactivate a connector |
| `POST` | `/api/v1/connectors/{id}/sync` | Trigger manual sync |
| `GET` | `/api/v1/connectors/{id}/sync-jobs` | List sync job history |
| `GET` | `/health` | Liveness + readiness probe |

#### Kafka Topics Produced

| Topic | When |
|---|---|
| `m1.int.sync_requested` | On manual sync trigger or scheduled sync |

#### Kafka Topics Consumed
None. This service has no consumers.

#### Storage Dependencies

| Store | Usage | Access pattern |
|---|---|---|
| PostgreSQL `nexus_system.connectors` | Read/write connector metadata | RLS-scoped via `nexus_app` role |
| PostgreSQL `nexus_system.sync_jobs` | Write job records on trigger | RLS-scoped |

#### Scaling

| Parameter | Value |
|---|---|
| Deployment type | `Deployment` |
| Min replicas | 1 |
| Max replicas | 3 |
| HPA trigger | CPU > 60% |
| Startup time | < 5s |

Low-traffic service. Connector registration is infrequent; manual syncs are occasional. CPU autoscaling is sufficient — no KEDA needed.

#### Resource Profile

```yaml
resources:
  requests:
    cpu: 100m
    memory: 128Mi
  limits:
    cpu: 500m
    memory: 256Mi
```

#### Security Surface
- **Inbound:** HTTP only, via Kong (JWT-validated)
- **Outbound:** PostgreSQL only
- **No access** to Secrets Manager (source system credentials). It registers connectors; it never connects to them.
- ServiceAccount: `nexus-m1-api-sa` (read/write `nexus_system.connectors`, `nexus_system.sync_jobs`)

#### Iteration 2 Split Trigger
No split planned. This service is already as thin as it should be.

---

### `nexus-m1-worker`
**Module:** M1 — Data Intelligence & Mediation
**Type:** Event-driven worker · 24/7 Deployment
**Team:** Data Intelligence

#### Responsibility
Executes the data ingestion stages of the M1 pipeline. Runs continuously. Contains two logical components:

1. **Connector Worker** — consumes `m1.int.sync_requested`, queries source systems via Airbyte / Debezium, writes raw records to Delta Lake (MinIO), publishes `m1.int.raw_records`.
2. **AI Store Router** — consumes `m1.int.cdm_entities_ready` (produced by `nexus-cdm-mapper`), routes canonicalised entities to M3 stores, publishes routing confirmation events.

CDM field classification (Tier 1 / 2 / 3) is handled by the separate `nexus-cdm-mapper` service, which scales to zero between monthly CDM publish events.

#### Kafka Topics Consumed

| Topic | Consumer group | Component |
|---|---|---|
| `m1.int.sync_requested` | `m1-connector-workers` | Connector Worker |
| `m1.int.cdm_entities_ready` | `m1-ai-router` | AI Store Router |

#### Kafka Topics Produced

| Topic | Producer component |
|---|---|
| `m1.int.raw_records` | Connector Worker |
| `m1.int.ai_routing_decided` | AI Store Router |
| `m1.int.ai_write_completed` | AI Store Router |
| `m1.int.dead_letter` | All (on unrecoverable error) |
| `{tid}.m1.sync_completed` | Connector Worker |

#### Storage Dependencies

| Store | Usage | Component |
|---|---|---|
| PostgreSQL `nexus_system.sync_jobs` | Update job status | Connector Worker |
| MinIO `nexus-raw/{tenant_id}/...` | Write raw records (WRITE only) | Connector Worker |
| Secrets Manager | Source system credentials (Salesforce tokens, DB passwords) | Connector Worker |

#### Scaling

| Parameter | Value |
|---|---|
| Deployment type | `Deployment` |
| Min replicas | 2 |
| Max replicas | 10 |
| HPA trigger | KEDA — Kafka consumer lag on `m1.int.sync_requested` > 50 messages |
| Startup time | 15–30s (waits for source system connection pools) |

#### Resource Profile

```yaml
resources:
  requests:
    cpu: 500m
    memory: 1Gi
  limits:
    cpu: 2000m
    memory: 4Gi
```

#### Security Surface
- **Inbound:** None. Kafka consumption only.
- **Outbound:** Kafka, PostgreSQL, MinIO (write), Secrets Manager, source system endpoints.
- This is the **only service** with outbound access to source system IPs.
- ServiceAccount: `nexus-m1-worker-sa` — includes Secrets Manager access to `nexus/tenants/*/credentials`.

#### Iteration 2 Split Trigger
No CDM mapper split needed (already separate). Consider splitting if connector worker lag and AI router lag diverge significantly — indicating the router is blocking the connector's partition assignments.

---

### `nexus-cdm-mapper`
**Module:** M1 — Data Intelligence & Mediation
**Original name:** `nexus-cdm-mapper` (from initial decomposition — unchanged)
**Type:** Event-driven worker · KEDA **scale-to-zero** Deployment
**Team:** Data Intelligence

#### Responsibility
Classifies raw record batches from Delta Lake against the tenant's CDM, produces canonicalised entities, and handles CDM lifecycle events (mapping approvals, version promotions). Runs on demand — **idles at 0 replicas** between monthly CDM publish events.

Three consumer loops:
1. **CDM Mapper** — consumes `m1.int.raw_records`, classifies fields (Tier 1/2/3), publishes `m1.int.cdm_entities_ready`
2. **Cache Invalidator** — consumes `{tid}.m4.mapping_approved`, flushes CDM registry cache
3. **Version Listener** — consumes `nexus.cdm.version_published`, refreshes version pointer and triggers Tier 3 backfill

#### Kafka Topics Consumed

| Topic | Consumer group | Component |
|---|---|---|
| `m1.int.raw_records` | `m1-cdm-mapper` | CDM Mapper |
| `{tid}.m4.mapping_approved` | `m1-cache-invalidator` | Cache Invalidator |
| `nexus.cdm.version_published` | `m1-cdm-version-listener` | Version Listener |

#### Kafka Topics Produced

| Topic | Producer component |
|---|---|
| `m1.int.cdm_entities_ready` | CDM Mapper |
| `m1.int.mapping_failed` | CDM Mapper (Tier 2 governance flag) |
| `{tid}.m1.semantic_interpretation_requested` | CDM Mapper (Tier 2 → M2) |
| `m1.int.sync_requested` | Version Listener (Tier 3 backfill trigger) |
| `m1.int.dead_letter` | All (on unrecoverable error) |

#### Storage Dependencies

| Store | Usage | Access type |
|---|---|---|
| PostgreSQL `nexus_system.cdm_mappings` | Read mapping rules (cached in-process) | READ only |
| PostgreSQL `nexus_system.cdm_versions` | Read/set active CDM version per tenant | READ only |
| PostgreSQL `nexus_system.sync_jobs` | Find connectors with Tier 3 records (backfill) | READ only |
| MinIO `nexus-raw/{tenant_id}/...` | Read raw record batches | READ only |

No Secrets Manager access. No write access to MinIO. No outbound to source systems.

#### Scaling

| Parameter | Value |
|---|---|
| Deployment type | `Deployment` |
| Min replicas | **0** — scale-to-zero when idle |
| Max replicas | 5 |
| KEDA trigger | Kafka lag on `m1.int.raw_records` > 1 |
| Cooldown | 300s (stay up 5 min after processing completes) |
| Startup time | < 30s (no source system pools to initialise) |

#### Resource Profile

```yaml
resources:
  requests:
    cpu: 250m
    memory: 512Mi
  limits:
    cpu: 1000m
    memory: 2Gi
```

Smaller than `nexus-m1-worker` — no source system connection pools, lighter workload.

#### Security Surface
- **Inbound:** None. Kafka consumption only.
- **Outbound:** Kafka, PostgreSQL, MinIO (read only). **No outbound to source systems or Secrets Manager (tenant credentials).**
- ServiceAccount: `nexus-cdm-mapper-sa` — explicitly does NOT include `nexus/tenants/*/credentials` in Secrets Manager IAM policy.

#### Iteration 2 Split Trigger
No split planned. If Tier 3 backfill volume grows large enough that it creates lag for incremental mapping batches, consider separating the Version Listener + backfill trigger into a dedicated lightweight consumer.

---

### `nexus-schema-profiler`
**Module:** M1 — Data Intelligence & Mediation
**Type:** Scheduled / triggered job
**Team:** Data Intelligence

#### Responsibility
Runs once when a connector is first registered (triggered by Airflow via M4's DAG trigger API), then on a weekly schedule for schema drift detection. Extracts source schema metadata, compares against stored snapshots, and — if new unknown fields are detected — publishes `m1.int.structural_cycle_triggered` to start the M2 structural interpretation cycle.

This is a **Kubernetes Job / Airflow DAG task**, not a long-running Deployment. It starts, executes its scan, writes results to PostgreSQL, and exits. There is no process running between invocations.

#### Kafka Topics Produced

| Topic | When |
|---|---|
| `m1.int.source_schema_extracted` | After schema extraction completes |
| `m1.int.structural_cycle_triggered` | When schema drift or new fields detected |

#### Storage Dependencies

| Store | Usage |
|---|---|
| PostgreSQL `nexus_system.schema_snapshots` | Read previous snapshot, write new snapshot |
| PostgreSQL `nexus_system.connectors` | Read connector metadata and credentials path |
| Secrets Manager | Source system credentials for schema query |

#### Scaling

| Parameter | Value |
|---|---|
| Deployment type | Airflow DAG + Kubernetes Job |
| Concurrency | One job per connector per run (parallelised by Airflow) |
| Timeout | 30 minutes per connector |
| Retry | 2 retries with 5-minute delay |

#### Resource Profile

```yaml
resources:
  requests:
    cpu: 200m
    memory: 256Mi
  limits:
    cpu: 1000m
    memory: 512Mi
```

Short-lived. Resources are released on completion. Does not need a persistent process.

#### Security Surface
- **Inbound:** None.
- **Outbound:** Source system (schema introspection query only — read-only), PostgreSQL, Secrets Manager.
- The job is instantiated per-connector with the specific credentials for that connector only. It cannot access credentials for other connectors.

#### Iteration 2 Split Trigger
None — this is already the correct granularity. In Iteration 2, the job definition may be extended to support incremental drift detection (column-level diffing rather than full schema snapshots).

---

### `nexus-m2-api`
**Module:** M2 — AI Intelligence Hub
**Type:** Request-driven API
**Team:** AI & Knowledge

#### Responsibility
Single HTTP entry point for user queries. Receives natural language queries from M6, validates them, publishes to Kafka, and returns a `session_id` to the caller immediately. The actual query result arrives asynchronously via WebSocket subscription to `{tid}.m2.agent_response_ready`.

Also exposes a WebSocket endpoint (`/ws/chat/{session_id}`) that M6 maintains to receive streaming results.

#### Endpoints

| Method | Path | Description |
|---|---|---|
| `POST` | `/api/v1/query` | Submit NL query, returns `{session_id}` immediately |
| `GET` | `/ws/chat/{session_id}` | WebSocket — streams agent response when ready |
| `GET` | `/api/v1/sessions/{id}` | Query session status |
| `GET` | `/health` | Liveness + readiness |

#### Kafka Topics Produced

| Topic | When |
|---|---|
| `{tid}.m2.knowledge_query` | On every accepted query submission |

#### Kafka Topics Consumed

| Topic | Consumer group | Purpose |
|---|---|---|
| `{tid}.m2.agent_response_ready` | `m2-api-websocket-relay` | Forward completed agent responses to open WebSocket connections |

#### Storage Dependencies

| Store | Usage |
|---|---|
| PostgreSQL `nexus_system.query_sessions` | Write session record on query submission, read status |
| Redis | Active WebSocket session registry (which pod holds which `session_id` connection) |

Redis is required because WebSocket relay must route responses to the correct pod that holds the client connection. Without it, a response arriving at pod B cannot be forwarded if the client is connected to pod A.

#### Scaling

| Parameter | Value |
|---|---|
| Deployment type | `Deployment` |
| Min replicas | 2 |
| Max replicas | 5 |
| HPA trigger | CPU > 60% or active WebSocket connections > 500 per replica |
| Startup time | < 5s |

#### Resource Profile

```yaml
resources:
  requests:
    cpu: 200m
    memory: 256Mi
  limits:
    cpu: 1000m
    memory: 512Mi
```

Lightweight — all heavy work is in `nexus-m2-executor`. The API pod holds WebSocket connections open, which is mostly I/O wait rather than CPU.

#### Security Surface
- **Inbound:** HTTP + WebSocket via Kong.
- **Outbound:** Kafka (publish queries), PostgreSQL (session store), Redis (WebSocket registry).
- **No LLM calls.** LLM reasoning is exclusively in `nexus-m2-executor`.

#### Iteration 2 Split Trigger
No split planned. May add a dedicated WebSocket relay service if WebSocket connection density creates memory pressure at scale (> 10,000 concurrent sessions).

---

### `nexus-m2-executor`
**Module:** M2 — AI Intelligence Hub
**Type:** Event-driven worker
**Team:** AI & Knowledge

#### Responsibility
The cognitive core of NEXUS. Executes the full RHMA (Reasoning, Hierarchical, Multi-Agent) pipeline:

1. **Query Planner** — interprets the NL query using an LLM (Claude / GPT-4), produces a structured execution plan.
2. **Query Decomposer** — breaks the plan into parallel sub-queries targeting specific M3 stores.
3. **Parallel Executor** — fans out to vector (Pinecone), graph (Neo4j), and time-series (TimescaleDB) stores concurrently; aggregates results.
4. **Result Synthesizer** — uses an LLM to compose a coherent, sourced natural language response from the aggregated results.
5. **Safety Layer + OPA** — validates the response for cross-tenant data leakage before publishing.

For Iteration 1, all five stages run sequentially in a single consumer loop. The internal architecture is designed for later parallelisation but the container boundary is one service.

#### Kafka Topics Consumed

| Topic | Consumer group |
|---|---|
| `{tid}.m2.knowledge_query` | `m2-query-executors` |

#### Kafka Topics Produced

| Topic | When |
|---|---|
| `{tid}.m2.agent_response_ready` | On successful completion |
| `{tid}.m2.semantic_interpretation_complete` | On structural sub-cycle completion (M1 schema interpretation) |
| `{tid}.m2.workflow_trigger` | When query intent maps to a business workflow (e.g. "start onboarding for Alice") |
| `m1.int.dead_letter` | On unrecoverable LLM or store error |

#### Storage Dependencies

| Store | Usage | Tenant isolation |
|---|---|---|
| Pinecone | Vector similarity search (semantic queries) | Separate index per `{tenant_id}-{entity_type}` |
| Neo4j | Graph traversal (relationship queries) | `tenant_id` property + mandatory WHERE filter |
| TimescaleDB | Time-series queries (trend and metric data) | RLS policy + `tenant_id` column |
| PostgreSQL `nexus_system.query_sessions` | Update session status, write reasoning trace | RLS-scoped |

#### Scaling

| Parameter | Value |
|---|---|
| Deployment type | `Deployment` |
| Min replicas | 2 |
| Max replicas | 12 |
| HPA trigger | KEDA — Kafka consumer lag on `{tid}.m2.knowledge_query` > 10 messages |
| Startup time | 20–40s (loads LLM client, validates M3 store connections) |

LLM calls are the dominant latency source (2–8 seconds per call). Scaling is driven by queue depth, not CPU. KEDA's Kafka lag metric is the correct trigger.

#### Resource Profile

```yaml
resources:
  requests:
    cpu: 1000m
    memory: 2Gi
  limits:
    cpu: 4000m
    memory: 8Gi
```

This is the most resource-intensive service. Each replica may hold multiple concurrent query contexts in flight, and each LLM call buffers a multi-kilobyte context window. High memory limit is appropriate.

#### Security Surface
- **Inbound:** None. Kafka consumption only.
- **Outbound:** Kafka, Pinecone (HTTPS), Neo4j (bolt+s), TimescaleDB (PostgreSQL protocol), LLM APIs (OpenAI/Anthropic — HTTPS, rate-limited).
- **Sensitive:** This service makes LLM API calls. It must never include raw tenant data (records, PII) in LLM prompts — only CDM field names and structural metadata. The Safety Layer (OPA) scans responses before publication.

#### Iteration 2 Split Trigger
Split into `nexus-query-planner` (LLM-heavy, few concurrent, high-latency) and `nexus-query-executor` (I/O-heavy, high concurrency, lower latency) when:
- P99 query latency exceeds 15 seconds consistently — indicates the planning and execution stages have different performance profiles that benefit from independent resource allocation.
- LLM API cost is dominating the executor's resource footprint — the planner can be scaled on queue depth while the executor scales on I/O concurrency independently.

---

### `nexus-m4-api`
**Module:** M4 — Workflow & Integration
**Type:** Request-driven API
**Team:** Product

#### Responsibility
HTTP surface for all human-in-the-loop governance actions and workflow management. Serves three concerns:

1. **CDM Governance** — approve/reject CDM extension proposals (from P6-M4-01).
2. **Mapping Exception Review** — approve/reject Tier 2 field mapping exceptions (from P6-M4-02).
3. **Airflow Orchestration Bridge** — trigger permitted Airflow DAGs, list DAG run history (from P6-M4-04).

Also hosts a thin proxy to Temporal's gRPC API for M6's Workflow Manager surface.

#### Endpoints

| Method | Path | Description |
|---|---|---|
| `GET / POST` | `/api/v1/governance/proposals/*` | CDM proposal review |
| `GET / POST` | `/api/v1/mappings/review/*` | Mapping exception review |
| `POST` | `/api/v1/workflows/dag-trigger` | Trigger Airflow DAG |
| `GET` | `/api/v1/workflows/dag-runs` | List DAG run history |
| `GET` | `/api/v1/workflows/temporal/*` | Proxy to Temporal workflow state |
| `GET` | `/health` | Liveness + readiness |

#### Kafka Topics Produced

| Topic | When |
|---|---|
| `nexus.cdm.version_published` | After CDM proposal approval |
| `nexus.cdm.extension_rejected` | After CDM proposal rejection |
| `{tid}.m4.mapping_approved` | After mapping exception approval |

#### Kafka Topics Consumed
None directly. Governance events arrive via consumers in `nexus-m4-worker`.

#### Storage Dependencies

| Store | Usage |
|---|---|
| PostgreSQL `nexus_system.governance_queue` | Read/write CDM proposals |
| PostgreSQL `nexus_system.mapping_review_queue` | Read/write mapping exceptions |
| PostgreSQL `nexus_system.dag_run_log` | Write DAG run records |
| PostgreSQL `nexus_system.cdm_versions` | Version management on approval |
| PostgreSQL `nexus_system.cdm_mappings` | Insert approved mappings |

#### Scaling

| Parameter | Value |
|---|---|
| Deployment type | `Deployment` |
| Min replicas | 1 |
| Max replicas | 3 |
| HPA trigger | CPU > 60% |
| Startup time | < 5s |

Very low traffic service. The governance console handles at most tens of requests per day per tenant. A single replica is sufficient for MVP; 3-replica ceiling for availability.

#### Resource Profile

```yaml
resources:
  requests:
    cpu: 100m
    memory: 128Mi
  limits:
    cpu: 500m
    memory: 256Mi
```

#### Security Surface
- **Inbound:** HTTP via Kong (JWT-validated, `X-Tenant-ID` injected).
- **Outbound:** PostgreSQL, Kafka (publish governance events), Airflow REST API (internal cluster only), Temporal gRPC (internal cluster only).
- **No outbound to source systems** and **no LLM calls**.

#### Iteration 2 Split Trigger
No split planned. If Temporal proxy adds latency, extract it into a dedicated `nexus-workflow-api` service — but only if it creates measurable impact.

---

### `nexus-m4-worker`
**Module:** M4 — Workflow & Integration
**Type:** Event-driven worker
**Team:** Product

#### Responsibility
Three Kafka consumer loops running in one process for Iteration 1:

1. **CDM Governance Consumer** — subscribes to `nexus.cdm.extension_proposed`, stores proposals in `nexus_system.governance_queue`.
2. **Mapping Exception Consumer** — subscribes to `m1.int.mapping_failed`, deduplicates, stores in `nexus_system.mapping_review_queue`.
3. **CDM Version Published Consumer** — subscribes to `nexus.cdm.version_published`, determines if Tier 3 backfill is warranted, calls the M4 Airflow Bridge API to trigger `m4_cdm_reprocess_trigger`.
4. **Workflow Trigger Consumer** — subscribes to `{tid}.m2.workflow_trigger`, starts the appropriate Temporal workflow (OnboardingWorkflow for Iteration 1).

#### Kafka Topics Consumed

| Topic | Consumer group | Component |
|---|---|---|
| `nexus.cdm.extension_proposed` | `m4-cdm-governance` | CDM Governance Consumer |
| `m1.int.mapping_failed` | `m4-mapping-exceptions` | Mapping Exception Consumer |
| `nexus.cdm.version_published` | `m4-cdm-version-listener` | CDM Version Published Consumer |
| `{tid}.m2.workflow_trigger` | `m4-workflow-triggers` | Workflow Trigger Consumer |

#### Kafka Topics Produced

| Topic | Producer component |
|---|---|
| `nexus.m4.governance_escalation` | Airflow SLA Monitor DAG (not this service directly) |

#### Storage Dependencies

| Store | Usage |
|---|---|
| PostgreSQL `nexus_system.governance_queue` | Write CDM proposals |
| PostgreSQL `nexus_system.mapping_review_queue` | Write mapping exceptions |
| PostgreSQL `nexus_system.dag_run_log` | Write DAG run records (via M4 API internal call) |
| Temporal | Start workflow executions |

#### Scaling

| Parameter | Value |
|---|---|
| Deployment type | `Deployment` |
| Min replicas | 1 |
| Max replicas | 3 |
| HPA trigger | KEDA — Kafka consumer lag on `nexus.cdm.extension_proposed` > 20 messages |
| Startup time | 10s (Temporal client connection) |

Low-volume service. Governance events are sparse compared to pipeline events. A single replica handles steady state; KEDA allows burst scaling during high-volume CDM onboarding phases.

#### Resource Profile

```yaml
resources:
  requests:
    cpu: 200m
    memory: 256Mi
  limits:
    cpu: 1000m
    memory: 512Mi
```

#### Security Surface
- **Inbound:** None. Kafka consumption only.
- **Outbound:** Kafka, PostgreSQL, Temporal (gRPC, internal), M4 API (HTTP, internal — for DAG trigger).

#### Iteration 2 Split Trigger
No split planned unless Temporal workflow execution volume grows enough that the Workflow Trigger Consumer introduces latency for the governance consumers sharing the same process.

---

## Shared Infrastructure (not deployable services)

These components are deployed by the Platform team and consumed by all services. They are not owned by any application team.

| Component | Namespace | Managed by |
|---|---|---|
| Kafka cluster (Strimzi) | `nexus-data` | Platform / ArgoCD |
| PostgreSQL (nexus_system schema + RLS) | `nexus-data` | Platform / ArgoCD |
| Redis | `nexus-data` | Platform |
| MinIO | `nexus-data` | Platform |
| Kong API Gateway | `nexus-gateway` | Platform / ArgoCD |
| Temporal | `nexus-data` | Platform |
| Apache Airflow | `nexus-data` | Platform |
| Pinecone | External (SaaS) | Platform (account provisioning) |
| Neo4j Aura | External (SaaS) | Platform (account provisioning) |
| TimescaleDB | `nexus-data` | Platform |
| Prometheus + Grafana | `nexus-monitoring` | Platform |

---

## Kafka Topic Ownership Map

Each module owns the topics it produces. No service may publish to a topic owned by another module without explicit architectural approval from the Tech Lead.

| Topic pattern | Owner module | Producer service | Consumer services |
|---|---|---|---|
| `m1.int.sync_requested` | M1 | nexus-m1-api, nexus-cdm-mapper (backfill), Airflow | nexus-m1-worker |
| `m1.int.raw_records` | M1 | nexus-m1-worker | nexus-cdm-mapper |
| `m1.int.cdm_entities_ready` | M1 | nexus-cdm-mapper | nexus-m1-worker (AI router) |
| `m1.int.mapping_failed` | M1 | nexus-cdm-mapper | nexus-m4-worker |
| `m1.int.dead_letter` | M1 | nexus-m1-worker, nexus-cdm-mapper | ops tooling / manual review |
| `nexus.cdm.*` | M4 (governance) | nexus-m4-api | nexus-cdm-mapper (cache + version), nexus-m2-executor |
| `{tid}.m1.*` | M1 | nexus-m1-worker, nexus-cdm-mapper | M6 (pipeline health) |
| `{tid}.m2.*` | M2 | nexus-m2-executor | nexus-m2-api (WebSocket relay), nexus-m4-worker (workflow trigger), nexus-m2-executor Pipeline B (`{tid}.m2.schema_narrative_ready` → m2-structural-agents) |
| `{tid}.m4.*` | M4 | nexus-m4-api | nexus-cdm-mapper (cache invalidation) |
| `nexus.m4.governance_escalation` | M4 (Airflow SLA DAG) | Airflow | M6 (pipeline health dashboard) |

---

## Iteration 2 Split Summary

| Service to split | Trigger condition | Result |
|---|---|---|
| `nexus-m2-executor` | P99 query latency > 15s or LLM cost dominates resource cost | `nexus-query-planner` + `nexus-query-executor` |

`nexus-m1-worker` and `nexus-cdm-mapper` are already split in Iteration 1 based on known operational profiles (24/7 connector vs. monthly mapper). No further M1 splits planned unless AI router lag diverges from connector lag.

All other services remain as-is through Iteration 2. Validate with operational metrics before splitting — do not split speculatively.

---

## Environment Variable Summary

All secrets are injected from AWS Secrets Manager via the External Secrets Operator. No secrets in ConfigMaps. No secrets in environment variables defined in Helm values files.

| Variable | Services | Source |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | All workers, all APIs | ConfigMap (non-secret) |
| `POSTGRES_DSN` | All services | Secrets Manager |
| `REDIS_URL` | nexus-m2-api | Secrets Manager |
| `AIRFLOW_BASE_URL` | nexus-m4-api | ConfigMap (internal URL) |
| `AIRFLOW_USERNAME / PASSWORD` | nexus-m4-api | Secrets Manager |
| `TEMPORAL_HOST` | nexus-m4-api, nexus-m4-worker | ConfigMap (internal URL) |
| `OPENAI_API_KEY` | nexus-m2-executor | Secrets Manager |
| `ANTHROPIC_API_KEY` | nexus-m2-executor | Secrets Manager |
| `PINECONE_API_KEY` | nexus-m2-executor | Secrets Manager |
| `NEO4J_URI / NEO4J_PASSWORD` | nexus-m2-executor | Secrets Manager |
| `TIMESCALEDB_DSN` | nexus-m2-executor | Secrets Manager |
| `CDM_CACHE_MAX_SIZE` | nexus-cdm-mapper | ConfigMap |
| `CDM_CACHE_TTL_SECONDS` | nexus-cdm-mapper | ConfigMap |
| Source system credentials | nexus-m1-worker only | Secrets Manager (`nexus/tenants/{tenant_id}/{connector_id}/credentials`) |

---

*NEXUS Service Topology · Mentis Consulting · Iteration 1 · March 2026 · Confidential*
