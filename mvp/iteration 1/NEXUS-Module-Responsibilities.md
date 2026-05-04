# NEXUS Platform — Module & Service Responsibilities
**Version:** 1.0 · March 2026 · Confidential
**Scope:** MVP Iteration 1

---

## Three Inviolable Architectural Rules

Before describing any module, three rules govern the entire platform and take precedence over any service-level decision.

**Rule 1 — LLM calls are M2's exclusive territory.** `nexus-m2-executor` is the only service in the platform permitted to make LLM API calls. Any LLM invocation found in another service is an architectural defect and will fail code review.

**Rule 2 — Kafka is the inter-module async bus.** Modules communicate asynchronously through Kafka events. No module calls another module's internal service directly for pipeline work. The one exception is M4-worker calling M4-api's DAG trigger endpoint as an internal service-to-service call within the same module.

**Rule 3 — Tenant identity flows from Kong, never from request bodies.** Every authenticated HTTP endpoint receives `tenant_id` exclusively from the `X-Tenant-ID` header injected by the Kong API gateway after JWT validation. Reading `tenant_id` from URL paths, query strings, or request bodies is a standards violation.

---

## Module 1 — Data Intelligence & Mediation

M1 is responsible for everything that touches source systems: discovering their schema, extracting and normalising their records, and classifying the resulting records against the Common Data Model (CDM). M1 never makes LLM calls. When it needs semantic reasoning — specifically, when it encounters a schema it has never seen before — it delegates to M2 by publishing a natural-language query to a Kafka topic and moving on.

### nexus-m1-api

The thin HTTP surface of M1. It exposes a REST API that allows tenants to register external system connectors (Salesforce, Odoo, ServiceNow, PostgreSQL, MySQL, SQL Server), configure their sync schedules, trigger manual syncs, and query sync job history. When a sync is triggered — either manually through the API or automatically by Airflow — the API publishes a `m1.int.sync_requested` event to Kafka. It then steps aside. All the actual extraction work is delegated to `nexus-m1-executor`.

The API validates that the tenant is `active` before accepting any write operation, rejecting connectors for suspended or provisioning tenants with HTTP 409. Access to PostgreSQL goes exclusively through RLS-scoped connections so that a query accidentally missing a WHERE clause still returns only the requesting tenant's data.

### nexus-m1-executor

The extraction and schema-profiling engine of M1. It runs two independent Kafka consumer loops and owns the full pipeline from raw source data to CDM-ready records.

**Loop 1 — Sync Consumer** (`m1-executor-sync` group, topic `m1.int.sync_requested`): On receiving a sync request, the executor connects to the source system using credentials retrieved from Vault, extracts records incrementally (using high-watermark cursors), normalises timestamps and type coercions, and stores results in the staging area. It then publishes `{tid}.m1.schema_profiling_requested` — carrying the extracted schema payload and a `job_id` — to Kafka. The executor commits its Kafka offset and moves on — it does not block waiting for the profiler result.

**Loop 2 — Schema Result Consumer** (`m1-executor-schema-results` group, topic `{tid}.m1.schema_profiled`): On receiving a profiler result, the executor uses the `job_id` in the event envelope to correlate the result with the in-flight sync context. It then routes based on the `cache_hit` flag in the payload:

- **Cache hit**: the event carries a pre-computed CDM mapping. The executor emits `{tid}.m1.records_normalised` and the CDM mapper picks it up for classification.
- **Cache miss**: the event carries a structured natural-language schema description. The executor publishes `{tid}.m1.semantic_interpretation_requested` and commits its offset. It does not wait for a reply. M2 will handle the interpretation asynchronously. This fire-and-forget delegation is the mechanism by which M1 offloads all LLM reasoning without violating the boundary.

The executor writes sync job progress and error logs to PostgreSQL and publishes `{tid}.m1.sync_completed` when a job finishes.

### nexus-m1-schema-profiler

A stateless, KEDA-scaled Kubernetes Job. It consumes `{tid}.m1.schema_profiling_requested` events (consumer group `m1-schema-profiler`) — published by `nexus-m1-executor` Loop 1 — and owns the fingerprinting pipeline. Each event carries the extracted schema payload and a `job_id`. The profiler computes structural fingerprints of source schemas: column names, data types, null rates, cardinality, value distributions, and sample values. It checks the computed fingerprint against a fingerprint cache backed by PostgreSQL.

On a **cache hit**, it publishes `{tid}.m1.schema_profiled` with the cached CDM mapping and `cache_hit: true`. On a **cache miss**, it publishes `{tid}.m1.schema_profiled` with a structured natural-language schema description and `cache_hit: false`. In both cases the event envelope includes the `job_id` from the inbound event, which `nexus-m1-executor`'s Loop 2 uses to correlate the result with the in-flight sync context.

The profiler has no HTTP surface. It is kept as a separate deployable unit for independent resource sizing — schema profiling over large source schemas is CPU and memory intensive and must not compete with M1-executor's extraction loop. KEDA scales it to zero between sync cycles.

### nexus-m1-cdm-mapper

A KEDA-scaled, scale-to-zero classification engine. It consumes `{tid}.m1.semantic_interpretation_requested` events under its own consumer group (`m1-cdm-mapper`) — the same topic that `nexus-m2-executor` also consumes under a different group. The two consumers are entirely independent and serve different purposes: the mapper applies deterministic rules to classify records, while M2's executor interprets schema semantics using LLMs.

The mapper applies the three-tier CDM classification logic. For each field in a normalised record it computes a confidence score against the current CDM schema:

- **Tier 1** (confidence ≥ 0.95): auto-mapped. The field is written directly into the CDM table. Throughput: high, zero human involvement.
- **Tier 2** (0.70 ≤ confidence < 0.95): flagged. The field is passed to a human data steward via the `m1.int.mapping_failed` event, which M4-worker stores in `nexus_system.mapping_review_queue` for review.
- **Tier 3** (confidence < 0.70): stored as-is in a `source_extras` JSONB column without CDM classification. It will be revisited in a Tier 3 backfill DAG after the CDM is extended.

The mapper uses no LLMs. Its classification is purely deterministic: it compares field fingerprints (name, type, distribution) against the CDM schema index loaded at startup and refreshed when a new CDM version is published. The reason `nexus-m1-cdm-mapper` is a separate service from `nexus-m1-executor` is operational: the mapper can scale to zero between sync cycles, since it only runs when records are available. The executor, by contrast, must remain responsive to incoming sync requests at all times.

---

## Module 2 — AI Intelligence Hub

M2 is the platform's sole AI brain. It owns all LLM calls, all agent orchestration, and all natural-language reasoning. It serves two distinct pipelines.

### nexus-m2-api

The HTTP surface of M2. It accepts user-facing queries from the frontend (`POST /api/v1/query`) and publishes them to `{tid}.m2.query_requested` for asynchronous processing by `nexus-m2-executor`. It also exposes a streaming endpoint so the UI can receive partial responses in real time as the executor produces them.

The API performs no AI work itself. It validates the request, enriches it with contextual metadata (user ID, tenant ID, session ID), and hands it off to Kafka. Results — whether a structured answer, a clarification request, or a business workflow trigger — stream back through `{tid}.m2.query_responded`.

### nexus-m2-executor

The engine of M2 and the only service in the platform that calls an LLM API. It runs two independent processing pipelines.

**Pipeline A — Executive RHMA (Reasoning-Hierarchical-Multi-Agent).** This pipeline handles user-facing queries arriving from `{tid}.m2.query_requested`. It runs a four-layer LangGraph graph:

1. **Intent Classification Layer**: classifies the query as analytical, operational (triggering a business action), clarification-needed, or out-of-scope.
2. **Task Decomposition Layer**: breaks compound queries into parallel sub-tasks.
3. **Domain Agent Routing Layer**: dispatches sub-tasks to specialised agents — the Data Retrieval Agent queries Pinecone and TimescaleDB, the Graph Intelligence Agent queries Neo4j, and the Integration Agent prepares Temporal workflow payloads.
4. **Safety Layer (OPA)**: validates every response against Open Policy Agent policies before publication. The system is fail-closed — if OPA is unreachable, the response is blocked.

When a query implies a business action (e.g., "start onboarding for Alice Martin"), the executor publishes a `{tid}.m2.workflow_trigger` event rather than a data response.

**Pipeline B — Structural Agent.** This pipeline handles schema interpretation requests arriving from `{tid}.m1.semantic_interpretation_requested` (under consumer group `m2-structural-agents`). The Structural Agent reads the natural-language schema description published by `nexus-m1-executor`, reasons about which CDM entities and fields the source schema maps to, computes confidence scores per field, and produces a CDM extension proposal. This proposal is published to `nexus.cdm.extension_proposed` for human review through M4's governance queue. The Structural Agent never auto-approves a CDM change — that decision belongs to a human data steward.

---

## Module 3 — Intelligent Storage Layer

M3 is not a service — it is a set of managed stores. In Iteration 1, no custom application code runs in M3. The stores are provisioned infrastructure consumed by M2's executor and M4's governance layer.

**Pinecone** stores vector embeddings of CDM records and source documents, enabling semantic similarity search used by M2's Data Retrieval Agent.

**Neo4j** stores the entity relationship graph — how CDM entities relate to each other and to source system records. The Graph Intelligence Agent in M2 queries this to answer relationship and lineage questions.

**TimescaleDB** stores time-series metrics and historical snapshots of CDM records, used for trend analysis queries.

All writes to M3 stores are performed by `nexus-m2-executor` as a side effect of processing queries and proposals. No other service writes to M3.

---

## Module 4 — Workflow & Governance

M4 closes the loop between automated processing and human decisions. It manages the governance of CDM schema changes, the review queue for unmapped fields, the triggering of reprocessing DAGs, and the execution of long-running business workflows.

### nexus-m4-api

The HTTP surface of M4. It exposes endpoints for data stewards and administrators to review and approve (or reject) CDM extension proposals stored in `nexus_system.governance_queue`. When a proposal is approved, the API writes the new CDM version to `nexus_system.cdm_versions`, publishes `nexus.cdm.version_published` to Kafka, and returns. It also exposes an internal DAG trigger endpoint used by `nexus-m4-worker` for service-to-service calls (no Kong, no user JWT — a service identity header is used instead).

The M4 API also provides the mapping review interface, where data stewards can resolve Tier 2 field exceptions accumulated in `nexus_system.mapping_review_queue`.

### nexus-m4-worker

A single Kubernetes deployment running four independent Kafka consumer loops.

**CDM Governance Consumer** (`m4-cdm-governance` group, topic `nexus.cdm.extension_proposed`): stores every incoming CDM extension proposal in `nexus_system.governance_queue` with status `pending`. Never auto-approves. Never modifies the payload.

**Mapping Exception Consumer** (`m4-mapping-exceptions` group, topic `m1.int.mapping_failed`): receives Tier 2 field events from `nexus-m1-cdm-mapper` (M1) and deduplicates them by `(tenant_id, source_system, source_table, source_field)`. Rather than creating one row per occurrence, it increments an `occurrence_count` counter. Data stewards see "this unknown field appeared 847 times across sync cycles" rather than 847 separate rows. Deduplication is strictly per-tenant — the same field appearing in two different tenants' data creates separate records for each.

**CDM Version Published Consumer** (`m4-cdm-version-listener` group, topic `nexus.cdm.version_published`): after a CDM approval is published by M4-api, this consumer determines whether a Tier 3 backfill DAG should be triggered. In Iteration 1, backfill is triggered on every CDM version change. It calls M4-api's internal DAG trigger endpoint and uses the Kafka `correlation_id` as an idempotency key logged in `nexus_system.dag_run_log`, preventing duplicate DAG runs even if the consumer crashes and reprocesses a message.

**Workflow Trigger Consumer** (`m4-workflow-triggers` group, topic `{tid}.m2.workflow_trigger`): receives workflow trigger events published by `nexus-m2-executor` when a user query implies a business action. It looks up the workflow type in `WORKFLOW_MAP`, starts the corresponding Temporal workflow with the `tenant_id` embedded in the workflow ID (the primary tenant isolation mechanism for Temporal), and publishes `{tid}.m4.workflow_completed` when the workflow finishes. Iteration 1 supports one workflow type: `employee_onboarding`.

---

## Module 5 — Platform Infrastructure

M5 is the operational backbone. These are not application services but managed platform components.

**Okta** handles identity. Users authenticate through Okta's OIDC flow. Okta issues JWTs that carry `tenant_id` and `user_id` claims.

**Kong** is the API gateway. It sits in front of all M1, M2, and M4 HTTP surfaces. It validates Okta JWTs, extracts `tenant_id` and `user_id` from the token claims, and injects them as `X-Tenant-ID` and `X-User-ID` headers. Application services trust these headers absolutely and never re-validate the JWT.

**Kafka** (Strimzi on EKS) is the async event bus for all inter-module communication. Topics use a naming convention that encodes the producing module and the event type. Tenant-scoped topics are prefixed with `{tid}` (the tenant ID). Global topics (e.g., `nexus.cdm.version_published`, `nexus.cdm.extension_proposed`) carry `tenant_id` inside the message envelope.

**Apache Airflow** (MWAA) manages scheduled and event-triggered DAGs. M1's sync schedules are modelled as Airflow DAGs. The Tier 3 CDM backfill DAG is triggered by M4-worker via M4-api's internal endpoint.

**Temporal** runs stateful, long-running business workflows. Iteration 1 supports the `employee_onboarding` workflow, which chains four activities: `create_it_account`, `create_hr_record`, `assign_equipment`, `send_welcome_email`. Temporal is not natively multi-tenant; isolation is enforced by embedding `tenant_id` in every workflow ID.

**EKS / ArgoCD / Prometheus / Grafana** form the deployment and observability layer. Services are deployed via GitOps (ArgoCD). Scaling is handled by KEDA for event-driven workers and HPA for HTTP services. All services expose Prometheus metrics including `tenant_id` labels. Grafana dashboards provide per-tenant and per-module visibility.

---

## Module 6 — User Interfaces

M6 exposes four surfaces to end users and administrators.

**Nexus Chat** is the primary user-facing interface. Users submit natural-language queries, which are routed through `nexus-m2-api` and answered by the RHMA pipeline. Responses stream in real time.

**CDM Governance Console** allows data stewards to review and approve or reject CDM extension proposals stored in M4's governance queue. Approvals publish `nexus.cdm.version_published` and cascade into Tier 3 backfills.

**Mapping Review Dashboard** surfaces Tier 2 field exceptions accumulated by M4-worker, showing occurrence counts, confidence scores, and M2's suggested CDM mappings. Stewards can accept, reject, or reclassify each exception.

**Admin Panel** allows platform administrators to manage tenants, connectors, user permissions, and observe system health.

---

## Kafka Topic Registry

This section is the authoritative reference for all Kafka topics in Iteration 1. Every topic, its producer, its consumer(s), and its consumer group are listed here. Any topic not in this table does not exist in the platform.

**Naming conventions:**
- `{tid}` prefix — tenant-scoped topic. The tenant ID is part of the topic name.
- No prefix — global/platform-scoped topic. The tenant ID is carried inside the message envelope.
- `.int.` infix — internal to a module. Not consumed across module boundaries.

### M1 Topics

| Topic | Scope | Producer | Consumer | Consumer Group |
|---|---|---|---|---|
| `m1.int.sync_requested` | Global | `nexus-m1-api` | `nexus-m1-executor` Loop 1 | `m1-executor-sync` |
| `{tid}.m1.schema_profiling_requested` | Per-tenant | `nexus-m1-executor` Loop 1 | `nexus-m1-schema-profiler` | `m1-schema-profiler` |
| `{tid}.m1.schema_profiled` | Per-tenant | `nexus-m1-schema-profiler` | `nexus-m1-executor` Loop 2 | `m1-executor-schema-results` |
| `{tid}.m1.records_normalised` | Per-tenant | `nexus-m1-executor` Loop 2 *(cache hit)* | `nexus-m1-cdm-mapper` | `m1-cdm-mapper` |
| `{tid}.m1.semantic_interpretation_requested` | Per-tenant | `nexus-m1-executor` Loop 2 *(cache miss)* | `nexus-m2-executor` | `m2-structural-agents` |
| `{tid}.m1.semantic_interpretation_requested` | Per-tenant | `nexus-m1-executor` Loop 2 *(cache miss)* | `nexus-m1-cdm-mapper` | `m1-cdm-mapper` |
| `m1.int.mapping_failed` | Global | `nexus-m1-cdm-mapper` | `nexus-m4-worker` | `m4-mapping-exceptions` |
| `{tid}.m1.sync_completed` | Per-tenant | `nexus-m1-executor` | *[CLARIFY: consumer not yet defined]* | — |

### M2 Topics

| Topic | Scope | Producer | Consumer | Consumer Group |
|---|---|---|---|---|
| `{tid}.m2.query_requested` | Per-tenant | `nexus-m2-api` | `nexus-m2-executor` | `m2-executor` |
| `{tid}.m2.query_responded` | Per-tenant | `nexus-m2-executor` | `nexus-m2-api` *(for SSE streaming)* | *[CLARIFY: group not yet defined]* |
| `{tid}.m2.workflow_trigger` | Per-tenant | `nexus-m2-executor` | `nexus-m4-worker` | `m4-workflow-triggers` |

### CDM Governance Topics (cross-module)

| Topic | Scope | Producer | Consumer | Consumer Group |
|---|---|---|---|---|
| `nexus.cdm.extension_proposed` | Global | `nexus-m2-executor` | `nexus-m4-worker` | `m4-cdm-governance` |
| `nexus.cdm.version_published` | Global | `nexus-m4-api` | `nexus-m4-worker` | `m4-cdm-version-listener` |
| `nexus.cdm.version_published` | Global | `nexus-m4-api` | `nexus-m1-cdm-mapper` *(index refresh)* | *[CLARIFY: group not yet defined]* |

### M4 Topics

| Topic | Scope | Producer | Consumer | Consumer Group |
|---|---|---|---|---|
| `{tid}.m4.workflow_completed` | Per-tenant | `nexus-m4-worker` | *[CLARIFY: consumer not yet defined]* | — |

> **Open items:** Four topics have unresolved consumers or consumer groups: `{tid}.m1.sync_completed`, `{tid}.m2.query_responded`, `nexus.cdm.version_published` (M1-cdm-mapper consumer group), and `{tid}.m4.workflow_completed`. These must be resolved before the service topology is finalised.

---

## Typical End-to-End Flow: New Salesforce Connector with Unknown Schema

This scenario illustrates how all modules interact when a tenant registers a new connector whose schema has never been seen before.

1. **Connector registration (M1-api)**: A tenant administrator registers a Salesforce connector via `POST /api/v1/connectors`. M1-api validates the tenant is active, stores the connector record, and returns a `connector_id`.

2. **Sync trigger (M1-api → Kafka)**: Airflow's scheduled DAG (or a manual trigger) fires. M1-api publishes `m1.int.sync_requested` to Kafka with the connector details.

3. **Extraction and profiling request (M1-executor Loop 1 → Kafka)**: `nexus-m1-executor` picks up the sync event under `m1-executor-sync`, connects to Salesforce via Vault credentials, extracts records incrementally, and publishes `{tid}.m1.schema_profiling_requested` carrying the extracted schema payload and a `job_id`. The executor commits its Kafka offset and moves on without blocking.

4. **Schema fingerprinting (M1-schema-profiler → Kafka)**: KEDA scales up a `nexus-m1-schema-profiler` instance on the incoming event. The profiler computes a structural fingerprint of the Salesforce schema, finds no cache match, and publishes `{tid}.m1.schema_profiled` with `cache_hit: false`, a natural-language schema description, and the `job_id`.

5. **LLM delegation (M1-executor Loop 2 → Kafka)**: `nexus-m1-executor`'s Loop 2 consumes `{tid}.m1.schema_profiled` under `m1-executor-schema-results`, correlates the result via `job_id`, reads `cache_hit: false`, and publishes `{tid}.m1.semantic_interpretation_requested`. It commits its offset and moves on.

6. **Structural Agent reasoning (M2-executor)**: `nexus-m2-executor` consumes the event under `m2-structural-agents`. The Structural Agent reasons about the schema, maps fields to CDM entities with confidence scores, and publishes `nexus.cdm.extension_proposed`.

7. **CDM Mapper classification (M1-cdm-mapper in parallel)**: `nexus-m1-cdm-mapper` also consumes `{tid}.m1.semantic_interpretation_requested` under `m1-cdm-mapper`. For fields it can classify deterministically (against the existing CDM), it writes Tier 1 records immediately and emits `m1.int.mapping_failed` for Tier 2 fields.

8. **Governance queue (M4-worker)**: The CDM Governance Consumer stores the M2 proposal in `nexus_system.governance_queue`. The Mapping Exception Consumer deduplicates the Tier 2 field events into `nexus_system.mapping_review_queue`.

9. **Human review (M4-api + M6 console)**: A data steward reviews the proposal in the CDM Governance Console and the Mapping Review Dashboard. They approve the CDM extension.

10. **CDM version published (M4-api → Kafka)**: M4-api writes the new CDM version and publishes `nexus.cdm.version_published`.

11. **Tier 3 backfill (M4-worker → Airflow)**: The CDM Version Published Consumer detects the new version and calls M4-api's DAG trigger endpoint. Airflow schedules the Tier 3 backfill DAG, which reclassifies the `source_extras` records now that the CDM schema has been extended.

12. **User query (M6 chat → M2-api → M2-executor → M3 stores)**: Once records are in the CDM, a business user asks "Show me all open opportunities from our Salesforce connector." M2-api publishes to `{tid}.m2.query_requested`. The RHMA pipeline routes to the Data Retrieval Agent, which queries Pinecone and TimescaleDB. The response streams back through `{tid}.m2.query_responded` and appears in the Nexus Chat interface.

---

*NEXUS Platform · Module Responsibilities · Mentis Consulting · March 2026*
