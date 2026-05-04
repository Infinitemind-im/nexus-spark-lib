# NEXUS — Iteration 2 · `nexus-query-api` + `nexus-query-executor` · Query Engine
**Services:** `nexus-query-api` · `nexus-query-executor`
Mentis Consulting · Version 0.3 · April 2026 · Confidential

> **Revision v0.3 — Architecture review corrections applied**
> This version corrects three issues identified in the April 2026 Architecture Review.
> Critical changes: QE-FR-15 added — post-synthesis cross-tenant safety scan (missing in v0.2);
> `CrossTenantSafetyScanner` class added between Result Merger and result publication;
> `identity_mapping` table enforcement documented under Rule 6 / Query Decomposer;
> Architectural rules section updated to reference Rule 6 globally (previously only implied).

---

## 📦 Codebase & Deployment

> **One repository, many deployments.** All NEXUS services live in the `nexus-platform` monorepo. `nexus-query-api` and `nexus-query-executor` are two separate deployed processes but live in the same repo, import the same shared libraries, and are developed together. Do not treat them as independent projects.

| | |
|---|---|
| **Deployed as** | `nexus-query-api` (HTTP + WebSocket entry point) · `nexus-query-executor` (internal execution engine) |
| **Monorepo paths** | `services/nexus-query-api/` · `services/nexus-query-executor/` |
| **Language / runtime** | Python 3.11 · FastAPI · asyncio |
| **Shared imports** | `from nexus_core.cdm import CdmEntity` · `from agent_core.llm import LLMClient` · `from agent_core.embedding import EmbeddingClient` · `from agent_core.safety import CrossTenantSafetyScanner` |
| **Iteration 2 scope** | Both services are **new** — built from scratch in this iteration. `nexus-m2-api` (Iteration 1) is deprecated as the user-facing query surface and demoted to internal use only. |

---

## Overview

The Query Engine is the user-facing intelligence layer introduced in Iteration 2. It accepts natural language questions, decomposes them into native sub-queries targeting live source systems and M3 stores, executes them in parallel, and renders structured answers as text, tables, charts, or documents.

The Query Engine is architecturally distinct from the M2 RHMA pipeline introduced in Iteration 1:

| Dimension | M2 RHMA (Iteration 1) | Query Engine (Iteration 2) |
|---|---|---|
| Query target | M3 stores (pre-populated knowledge) | Live source systems + M3 stores |
| Output type | Natural language text | Text, table, chart, dashboard, report |
| Latency target | < 30s | < 5s (simple), < 10s (cross-source) |
| Backend routing | Domain agent (finance/hr/ops) | Intent-based backend selector (deterministic) |
| Entrypoint | `nexus-m2-api` | `nexus-query-api` (new) |
| Executor | `nexus-m2-executor` | `nexus-query-executor` (new) |

[CLARIFY: Should `nexus-m2-api` and `nexus-query-api` be unified under a single API gateway service in Iteration 3? Having two separate entry points for AI queries may confuse M6 developers. Recommend keeping them separate in Iteration 2 and evaluating unification at the Iteration 3 boundary.]

---

## Architectural Rules

These rules extend the three inviolable rules from Iteration 1 Module Responsibilities document:

**Rule 4 — nexus-query-executor is the only service that calls live source systems for query execution.** The M2 executor is for knowledge retrieval from M3. The query executor is for live source queries. They do not overlap.

**Rule 5 — OPA runs synchronously before any source system is contacted.** A query that fails OPA is rejected before any data leaves NEXUS or any source system receives a request.

**Rule 6 — User identity is forwarded to source systems unchanged.** The query executor forwards the Okta `user_id` to connector-worker so that source system RBAC applies to the query. NEXUS never elevates a user's permissions at the source. The mapping from Okta `user_id` to source-system identity (where the source uses a different identity model) is resolved via `nexus_system.identity_mapping` at decomposition time. [CLARIFY: `nexus_system.identity_mapping` is described in the architectural specification as "seeded in Iteration 1, enforced in Iteration 2." Its schema must be confirmed against the Iteration 1 data model spec before the Query Decomposer implementation begins. If not yet defined, it must be added to the Iteration 2 Data Model spec.]

**Rule 7 (post-synthesis safety) — nexus-query-executor validates cross-tenant source integrity on every merged result before streaming.** The post-synthesis scanner (see `CrossTenantSafetyScanner` below) runs after the Result Merger and before the `result` event is published. It is a defence-in-depth layer supplementing the pre-query OPA check.

---

## Functional Requirements

| ID | Requirement | Priority |
|---|---|---|
| QE-FR-01 | `POST /query` returns `session_id` in < 200ms | Must |
| QE-FR-02 | Query results stream via WebSocket as events (planning → decomposing → executing → result) | Must |
| QE-FR-03 | Polling fallback (`GET /query/{session_id}`) for non-WebSocket clients | Must |
| QE-FR-04 | Query planner selects correct execution backend (live source, Neo4j, Elasticsearch, TimescaleDB, hybrid) | Must |
| QE-FR-05 | Query decomposer translates CDM plan to native sub-queries (SOQL, SQL, Cypher, ANN) | Must |
| QE-FR-06 | Parallel executor fans out sub-queries concurrently; partial failure returns degraded result | Must |
| QE-FR-07 | OPA blocks queries requesting PII columns for non-authorised roles | Must |
| QE-FR-08 | Results are exported as .xlsx, .csv, or .pdf on demand | Must |
| QE-FR-09 | Charts can be saved as persistent dashboard components | Must |
| QE-FR-10 | Reports are generated as .docx with persona-specific structure | Must |
| QE-FR-11 | CDM catalogue is cached per tenant+CDM version; cache is invalidated on `cdm.version_published` | Must |
| QE-FR-12 | User context (`user_role`, `current_view`) influences output type selection | Should |
| QE-FR-13 | Query session records are retained for 30 days for audit | Should |
| QE-FR-14 | Source query results are never stored in NEXUS — only the rendered output is persisted | Must |
| QE-FR-15 | nexus-query-executor performs a cross-tenant source validation on every merged result set before streaming. Any result that references a connector not owned by the session's `tenant_id` is rejected as a hard failure (not a partial result). | Must |

## Non-Functional Requirements

| ID | Requirement | Target |
|---|---|---|
| QE-NFR-01 | Single-source query end-to-end latency (P95) | < 3s |
| QE-NFR-02 | Cross-source query end-to-end latency (P95, 2 sources) | < 5s |
| QE-NFR-03 | `POST /query` response time | < 200ms |
| QE-NFR-04 | Neo4j relationship query latency (P95) | < 2s |
| QE-NFR-05 | Concurrent query sessions supported per tenant | ≥ 50 |
| QE-NFR-06 | CDM catalogue cache hit rate | ≥ 95% |
| QE-NFR-07 | Query plan LLM call timeout | 8s (fail query if exceeded) |

---

## Service 1: nexus-query-api

### Identity

| Attribute | Value |
|---|---|
| Service name | `nexus-query-api` |
| Module | M2 — AI Intelligence Hub (owned by AI & Knowledge team) |
| Type | Request-driven API + WebSocket relay · Kubernetes `Deployment` |
| Team | AI & Knowledge |
| Port | 8005 |
| Language | Python 3.12 · FastAPI |

### Responsibility

Stateless HTTP service. Acts as the thin entry point for the Query Engine. Accepts NL queries from M6, returns a `session_id` immediately, streams result events via WebSocket, and serves export downloads. Does not execute queries — all execution is delegated to `nexus-query-executor` via Kafka.

Architecturally identical to `nexus-m2-api` but serves a different query pipeline and a different result format (structured visual output vs. natural language text).

### Endpoints

#### POST /query

Accepts a user's NL query. Returns `session_id` in < 200ms.

```
POST /query
Kong: X-Tenant-ID, X-User-ID, X-User-Role, X-User-Email
Authorization: Bearer <JWT>

Request body:
{
  "query":             string,          // Natural language question (required, max 2000 chars)
  "output_preference": string,          // "auto" | "text" | "table" | "chart" | "report" (default: "auto")
  "context": {
    "user_role":       string,          // "cfo" | "ceo" | "data_steward" | "business_user"
    "current_view":    string,          // "financial_dashboard" | "hr_view" | ...  (optional hint)
    "time_zone":       string           // IANA tz (e.g. "Europe/Brussels") for relative date resolution
  }
}

Response 202:
{
  "session_id":     "qsess_a3f9c1d2-...",
  "status":         "planning",
  "websocket_url":  "wss://api.nexus.internal/query/qsess_a3f9c1d2-...",
  "poll_url":       "https://api.nexus.internal/query/qsess_a3f9c1d2-..."
}
```

**Validation:**
- `query` must be non-empty, ≤ 2000 characters
- `output_preference` must be one of the allowed enum values
- 400 returned for invalid body; 409 if tenant status is not `active`

**What this endpoint does:**
1. Validates request body
2. Creates session record in `nexus_system.query_sessions` with status `planning`
3. Publishes `NexusMessage` to `{tid}.query.submitted` Kafka topic
4. Returns `session_id` and WebSocket URL immediately

```python
@router.post("/query")
async def submit_query(
    body:          QueryRequest,
    x_tenant_id:   str = Header(...),
    x_user_id:     str = Header(...),
    x_user_role:   str = Header(...),
    x_user_email:  str = Header(...),
) -> QuerySubmitResponse:

    session_id = f"qsess_{uuid.uuid4()}"

    async with get_tenant_scoped_connection(pool, x_tenant_id) as conn:
        await conn.execute("""
            INSERT INTO nexus_system.query_sessions
                (session_id, tenant_id, user_id, user_role, query_text,
                 output_preference, context, status)
            VALUES ($1, $2, $3, $4, $5, $6, $7, 'planning')
        """, session_id, x_tenant_id, x_user_id, x_user_role,
             body.query, body.output_preference, json.dumps(body.context or {}))

    await producer.publish(NexusMessage(
        topic=CrossModuleTopicNamer.query(x_tenant_id, "submitted"),
        tenant_id=x_tenant_id,
        payload={
            "session_id":        session_id,
            "user_id":           x_user_id,
            "user_role":         x_user_role,
            "user_email":        x_user_email,
            "query_text":        body.query,
            "output_preference": body.output_preference,
            "context":           body.context or {},
        }
    ))

    return QuerySubmitResponse(
        session_id=    session_id,
        status=        "planning",
        websocket_url= f"wss://api.nexus.internal/query/{session_id}",
        poll_url=      f"https://api.nexus.internal/query/{session_id}",
    )
```

---

#### WebSocket /query/{session_id}

Persistent connection. The executor publishes progress events to a Kafka topic; this service relays them to the connected client.

**Connection flow:**
1. Client opens WebSocket to `/query/{session_id}`
2. Kong validates JWT, injects headers
3. API verifies `session_id` belongs to `x_tenant_id` (RLS query)
4. API registers `{tenant_id}:{session_id} → pod_id` in Redis (TTL 300s)
5. Client receives streaming events as they arrive from executor
6. Connection is closed after `result` or `error` event is sent

**WebSocket event stream (server → client):**

```json
// Event 1 — Planning phase (sent immediately by executor)
{ "event": "planning",    "session_id": "qsess_...", "message": "Analysing your question..." }

// Event 2 — Decomposition
{ "event": "decomposing", "session_id": "qsess_...", "message": "Identifying data sources...",
  "details": { "backend": "LIVE_SOURCE", "sources_identified": ["salesforce", "adventureworks"] } }

// Event 3 — Execution
{ "event": "executing",   "session_id": "qsess_...",
  "details": { "sources": ["salesforce", "postgresql/adventureworks"] } }

// Event 4 — Result (terminal)
{ "event": "result",      "session_id": "qsess_...",
  "payload": { /* RenderedOutput — see Visual Outputs spec */ } }

// Event 4 (alternative) — Error (terminal)
{ "event": "error",       "session_id": "qsess_...",
  "error_code": "opa_denied",
  "error_message": "You do not have permission to query nationalidnumber." }
```

**Close codes:**
- `4001` — JWT expired or invalid
- `4003` — session not found or does not belong to authenticated tenant
- `4004` — session timed out (> 30s without result event)
- `1000` — normal closure after `result` or `error` event delivered

---

#### GET /query/{session_id}

Polling fallback. Returns the current session state including the full result payload when complete.

```
GET /query/{session_id}
Authorization: Bearer <JWT>

Response 200:
{
  "session_id":       "qsess_...",
  "status":           "planning" | "decomposing" | "executing" | "completed" | "failed" | "timeout",
  "query_text":       "How many deals were closed last year?",
  "output_preference":"auto",
  "created_at":       "2026-03-15T09:12:00Z",
  "completed_at":     "2026-03-15T09:12:04Z",    // null if still processing
  "result":           { /* RenderedOutput — null if not yet completed */ },
  "error_code":       null,
  "error_message":    null
}

Response 404: session not found or does not belong to authenticated tenant
```

---

#### GET /query/{session_id}/export

Triggers export of the session result in the requested format.

```
GET /query/{session_id}/export?format=xlsx|csv|pdf
Authorization: Bearer <JWT>

Response 200:
Content-Type: application/vnd.openxmlformats-officedocument.spreadsheetml.sheet
Content-Disposition: attachment; filename="nexus-export-{session_id}.xlsx"
[binary file content]

Response 400: session result is not a table type (cannot export chart as xlsx)
Response 404: session not found
Response 409: session not yet completed
```

**Export format rules:**

| Output type | xlsx | csv | pdf |
|---|---|---|---|
| `TABLE` | ✅ | ✅ | ✅ |
| `BAR_CHART` | ✅ (data only) | ✅ (data only) | ❌ (use M6 screenshot) |
| `LINE_CHART` | ✅ (data only) | ✅ (data only) | ❌ |
| `PIE_CHART` | ✅ (data only) | ✅ (data only) | ❌ |
| `TEXT` | ❌ | ❌ | ✅ |
| `REPORT` | ❌ | ❌ | ✅ (.docx already generated) |

---

#### POST /query/{session_id}/save-dashboard

Persists a chart or table result as a dashboard component.

```
POST /query/{session_id}/save-dashboard
Authorization: Bearer <JWT>
Content-Type: application/json

{
  "title":            "Deals closed in 2025 by region",    // optional override
  "refresh_schedule": "daily" | "hourly" | null
}

Response 201:
{
  "component_id":   "dc_7f3a21...",
  "dashboard_url":  "https://nexus.internal/dashboard?component=dc_7f3a21..."
}

Response 400: session result is of type TEXT or REPORT (not dashboardable)
Response 404: session not found
Response 409: component already saved for this session
```

---

### nexus-query-api Kafka Topics

#### Produced

| Topic | When |
|---|---|
| `{tid}.query.submitted` | On every accepted query — triggers nexus-query-executor |

#### Consumed

| Topic | Consumer group | Purpose |
|---|---|---|
| `{tid}.query.event` | `query-api-ws-relay` | Forward progress events to open WebSocket connections |

### nexus-query-api Storage Dependencies

| Store | Table | Usage |
|---|---|---|
| PostgreSQL | `nexus_system.query_sessions` | Write session on submit; read for GET /query/{session_id} |
| Redis | — | WebSocket registry: `{tenant_id}:{session_id} → pod_id` (TTL 300s) |

### nexus-query-api Scaling

| Parameter | Value |
|---|---|
| Min replicas | 2 |
| Max replicas | 6 |
| HPA trigger | CPU > 60% or active WebSocket connections > 200 per replica |
| Startup time | < 5s |

### nexus-query-api Resource Profile

```yaml
resources:
  requests:
    cpu: 200m
    memory: 256Mi
  limits:
    cpu: 1000m
    memory: 512Mi
```

### nexus-query-api Environment Variables

| Variable | Source | Description |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | ConfigMap | Broker addresses |
| `POSTGRES_DSN` | Secrets Manager | PostgreSQL DSN |
| `REDIS_URL` | Secrets Manager | Redis URL for WebSocket registry |
| `POD_NAME` | Downward API | Self-identification in Redis registry |
| `QUERY_TIMEOUT_SECONDS` | ConfigMap | Session timeout before error event sent (default: `30`) |
| `EXPORT_MAX_ROWS` | ConfigMap | Maximum rows returnable in an export (default: `10000`) |

---

## Service 2: nexus-query-executor

### Identity

| Attribute | Value |
|---|---|
| Service name | `nexus-query-executor` |
| Module | M2 — AI Intelligence Hub |
| Type | Event-driven worker · Kubernetes `Deployment` |
| Team | AI & Knowledge |
| Language | Python 3.12 |

### Responsibility

The intelligence layer. Receives query submissions from Kafka, runs four sequential components (Planner → Decomposer → Parallel Executor → Merger/Renderer), and publishes progress events and the final rendered result back to Kafka for the API to relay.

This service contains the only LLM call in the Query Engine (the Query Planner). All other components are deterministic.

### Kafka Topics

#### Consumed

| Topic | Consumer group |
|---|---|
| `{tid}.query.submitted` | `query-executor` |
| `nexus.cdm.version_published` | `query-executor-cache-invalidator` |

#### Produced

| Topic | When |
|---|---|
| `{tid}.query.event` | After each pipeline stage (planning, decomposing, executing, result, error) |

### Query Session State Machine

```
PLANNING → DECOMPOSING → EXECUTING → RENDERING → COMPLETED
                                                ↘
                                              FAILED
     ↑ (at any stage, on unrecoverable error)

TIMEOUT (if total elapsed > QUERY_TIMEOUT_SECONDS)
```

Each state transition publishes a `{tid}.query.event` Kafka message with the event type and optional details payload.

---

### Component 1: Query Planner

**Responsibility:** Interprets the NL query using an LLM and produces a `CDMQueryPlan` — a structured, validated description of what the user is asking and how to answer it.

**Input:** `session_id`, `query_text`, `user_role`, `context`, CDM schema catalogue (from cache)

**LLM used:** Claude Sonnet 4 (Anthropic) — single call with a structured JSON output constraint

**CDM Schema Catalogue (system prompt context):**

The catalogue is a compact JSON document generated from `nexus_system.cdm_mappings` for the tenant's current CDM version. It is cached in Redis (key: `cdm_catalogue:{tenant_id}:{cdm_version}`, TTL 24 hours).

**Issue 4 correction — Catalogue size cap:** Injecting the full catalogue into the LLM prompt is unsafe for tenants with large CDM schemas — it can easily exceed the model's context window and inflate token costs. Instead, the `CDMCatalogueBuilder` first builds and caches the full catalogue (for all entity types), then filters it to the top-k most relevant entity types using an Elasticsearch kNN search on the raw query text. Only the top-k entries are injected into the system prompt.

```python
CATALOGUE_MAX_ENTITY_TYPES = 15   # Maximum entity types injected per LLM call
CATALOGUE_ANN_TOP_K        = 30   # Elasticsearch kNN top-k to retrieve before deduplicating types

class CDMCatalogueBuilder:
    """
    Builds the compact catalogue injected into the Query Planner's system prompt.
    Full catalogue build is called on cache miss and stored in Redis.
    For LLM injection, only the top-k relevant entity types are used (Issue 4).
    """

    async def build(self, tenant_id: str, cdm_version: str) -> dict:
        """Build and cache the FULL catalogue for all CDM entity types."""
        mappings = await self.pg.fetch_cdm_mappings(tenant_id, cdm_version)

        catalogue = {}
        for mapping in mappings:
            entity_key = f"{mapping.entity_type}.{mapping.subtype or 'default'}"
            if entity_key not in catalogue:
                catalogue[entity_key] = {"sources": [], "fields": {}}

            catalogue[entity_key]["sources"].append(
                f"{mapping.source_system}:{mapping.source_table}"
                + (f":{mapping.filter_hint}" if mapping.filter_hint else "")
            )

            for field in mapping.cdm_fields:
                catalogue[entity_key]["fields"][field.cdm_name] = field.description

        return catalogue  # Full catalogue — cached in Redis by the cache layer

    async def get_relevant_subset(
        self,
        query_text: str,
        tenant_id:  str,
        cdm_version: str,
        full_catalogue: dict,
    ) -> dict:
        """
        Returns a filtered subset of the full catalogue containing only the
        CATALOGUE_MAX_ENTITY_TYPES entity types most relevant to the query.

        Steps:
          1. Embed the raw query text using the same embedding model as nexus-m3-writer.
          2. Query Elasticsearch (kNN search on `nexus_{tenant_slug}_{entity_type}` index) for top CATALOGUE_ANN_TOP_K vectors.
          3. Collect the distinct entity_type values from result metadata.
          4. Return the catalogue entries for those entity types only.
          5. If Elasticsearch returns fewer than CATALOGUE_MAX_ENTITY_TYPES distinct types,
             pad with remaining catalogue entries in alphabetical order.
        """
        query_vector = await self.embedder.embed(query_text)

        ann_results = await self.elasticsearch.knn_search(
            index_name = f"{tenant_id}-entities",
            vector     = query_vector,
            top_k      = CATALOGUE_ANN_TOP_K,
            filter     = {"tenant_id": tenant_id},
        )

        # Extract ordered distinct entity types from Elasticsearch kNN result metadata
        seen: set[str] = set()
        relevant_types: list[str] = []
        for match in ann_results.matches:
            etype = match.metadata.get("entity_type", "")
            if etype and etype not in seen:
                seen.add(etype)
                relevant_types.append(etype)

        # Build the filtered catalogue — top-k relevant types first
        subset: dict = {}
        for entity_key in relevant_types:
            if entity_key in full_catalogue:
                subset[entity_key] = full_catalogue[entity_key]
            if len(subset) >= CATALOGUE_MAX_ENTITY_TYPES:
                break

        # Pad with remaining types if needed (alphabetical)
        if len(subset) < CATALOGUE_MAX_ENTITY_TYPES:
            for entity_key in sorted(full_catalogue.keys()):
                if entity_key not in subset:
                    subset[entity_key] = full_catalogue[entity_key]
                if len(subset) >= CATALOGUE_MAX_ENTITY_TYPES:
                    break

        return subset
```

The `get_relevant_subset()` call is made once per query, immediately before the LLM call. The result is not cached — it is query-specific. The full catalogue (used by `build()`) remains cached in Redis as before.

**Query Planner System Prompt:**

```python
QUERY_PLANNER_SYSTEM_PROMPT = """
You are NEXUS Query Planner. Your job is to analyse a user's natural language question
and produce a structured query plan in JSON.

AVAILABLE CDM ENTITIES:
{cdm_catalogue}

USER ROLE: {user_role}
CURRENT DATE: {current_date}
TENANT TIMEZONE: {user_timezone}

OUTPUT RULES:
1. Respond ONLY with a valid JSON object matching the CDMQueryPlan schema below.
2. Do not include any text outside the JSON object.
3. If the query is ambiguous, choose the most likely interpretation and set confidence accordingly.
4. If the query is impossible to answer from available entities, set intent="out_of_scope".
5. Resolve relative dates ("last year", "last quarter", "yesterday") to absolute date ranges
   using CURRENT DATE and TENANT TIMEZONE.

CDMQueryPlan JSON schema:
{
  "original_query":    string,
  "resolved_query":    string,    // Query with relative dates resolved to absolute
  "intent":            "aggregation" | "lookup" | "trend" | "relationship" | "semantic" | "report" | "out_of_scope",
  "entity":            string,    // e.g. "event.deal_closed" | "party.customer" | "employee"
  "filters":           object,    // e.g. {"year": 2025, "region": "BE"}
  "date_range":        { "from": ISO8601, "to": ISO8601 } | null,
  "aggregations":      array,     // e.g. [{"func": "COUNT"}, {"func": "SUM", "field": "amount"}]
  "group_by":          array,     // e.g. ["source_system", "region"]
  "output_type":       "TEXT" | "TABLE" | "BAR_CHART" | "LINE_CHART" | "PIE_CHART" | "DASHBOARD" | "REPORT",
  "execution_backend": "LIVE_SOURCE" | "NEO4J" | "PINECONE" | "TIMESCALEDB" | "HYBRID",
  "pii_columns_requested": array, // Fields the user is asking about that are PII-flagged
  "confidence":        number     // 0.0–1.0
}
"""
```

**CDMQueryPlan dataclass:**

```python
@dataclass
class CDMQueryPlan:
    original_query:       str
    resolved_query:       str           # Dates resolved to absolutes
    intent:               str           # aggregation | lookup | trend | relationship | semantic | report | out_of_scope
    entity:               str           # cdm.event | cdm.party | employee | ...
    filters:              dict          # { "event_type": "deal_closed", "year": 2025 }
    date_range:           DateRange | None
    aggregations:         list[dict]    # [{"func": "COUNT"}, {"func": "SUM", "field": "amount"}]
    group_by:             list[str]
    output_type:          OutputType    # TEXT | TABLE | BAR_CHART | LINE_CHART | PIE_CHART | DASHBOARD | REPORT
    execution_backend:    Backend       # LIVE_SOURCE | NEO4J | PINECONE | TIMESCALEDB | HYBRID
    pii_columns_requested:list[str]     # Empty list if no PII fields in scope
    confidence:           float         # 0.0–1.0

    # Set by Decomposer after planning:
    session_id:           str = ""
    tenant_id:            str = ""
    user_id:              str = ""
    user_role:            str = ""
```

**Output validation:**

```python
def validate_plan(plan: CDMQueryPlan) -> None:
    if plan.intent == "out_of_scope":
        raise OutOfScopeError(f"Cannot answer: {plan.original_query}")
    if plan.confidence < 0.40:
        raise LowConfidenceError(
            f"Query planner confidence {plan.confidence:.2f} below threshold. "
            "Consider rephrasing your question."
        )
    if plan.entity not in self.catalogue:
        raise UnknownEntityError(f"Entity '{plan.entity}' not in CDM catalogue for this tenant.")
```

**Backend selection override (deterministic, post-LLM):**

The LLM suggests a backend, but the following rules override it deterministically to ensure correct routing:

```python
def override_backend(plan: CDMQueryPlan) -> Backend:
    """
    Deterministic backend override applied after LLM planning.
    Prevents LLM from routing incorrectly.
    """
    if plan.intent == "relationship":
        return Backend.NEO4J       # Always — graph joins are the point of Neo4j
    if plan.intent == "semantic":
        return Backend.PINECONE    # Always — semantic similarity has no SQL equivalent
    if plan.intent == "trend" and plan.date_range is not None:
        return Backend.TIMESCALEDB # Time-bucket aggregations are native to TimescaleDB
    if plan.intent in ("aggregation", "lookup") and plan.date_range is not None:
        return Backend.LIVE_SOURCE # Authoritative count/sum always from live source
    if plan.intent == "aggregation" and "trend" in plan.original_query.lower():
        return Backend.HYBRID      # "Total deals by region over time" → live + timeseries
    return plan.execution_backend  # Accept LLM suggestion for other cases
```

**LLM call timeout:** 8 seconds. If exceeded, publish `error` event with `error_code: "planner_timeout"` and end session.

---

### Component 2: Query Decomposer

**Responsibility:** Translates the `CDMQueryPlan` into one or more `SourceQuery` objects — one per source system / backend that needs to be queried.

**Input:** `CDMQueryPlan`, CDM mappings from `nexus_system.cdm_mappings` (in-memory cache per tenant)

**Outputs:** `list[SourceQuery]`

```python
@dataclass
class SourceQuery:
    connector_id:    str             # UUID of the connector in nexus_system.connectors
    source_system:   str             # "salesforce" | "postgresql/adventureworks"
    native_query:    str             # SOQL | SQL | Cypher | ANN query string
    query_dialect:   str             # "SOQL" | "SQL" | "Cypher" | "ANN"
    credential_ref:  str             # Secret path for connector worker to resolve
    parameters:      dict            # Bind parameters (never interpolated into query string)
    timeout_seconds: int = 15        # Per-source timeout
    user_identity:   UserIdentity    # Forwarded for source-system RBAC
```

**Dialect translators:**

```python
class SOQLTranslator:
    """
    Salesforce Object Query Language.

    Issue 3 correction — SOQL date literal format:
      SOQL Date fields (e.g. CloseDate, CreatedDate for date-only fields) require
      YYYY-MM-DD bare format — NOT an ISO 8601 datetime.
      Invalid:   CloseDate >= 2025-01-01T00:00:00Z   ← Salesforce rejects this
      Valid:     CloseDate >= 2025-01-01

      DateTime fields (fields typed DateTime in Salesforce, e.g. CreatedDate when
      mapped to a DateTime CDM field) DO accept the ISO 8601 format with Z suffix:
      Valid:     CreatedDate >= 2025-01-01T00:00:00Z

      The SOQLTranslator inspects the CDM field type to choose the correct format.
    """

    def translate(self, plan: CDMQueryPlan, mapping: CDMMapping) -> tuple[str, dict]:
        select_fields = self.map_cdm_fields_to_soql(plan.aggregations, mapping)
        where_clauses = self.build_soql_where(plan.filters, plan.date_range, mapping)
        query = f"SELECT {select_fields} FROM {mapping.source_table}"
        if where_clauses:
            query += f" WHERE {' AND '.join(where_clauses)}"
        return query, {}  # SOQL does not support bind params — sanitised by field mapping only

    def _format_soql_date(self, iso_datetime: str, cdm_field: CDMField) -> str:
        """
        Formats a date/datetime value for inclusion in a SOQL WHERE clause.
        - CDM field type "date"     → YYYY-MM-DD  (bare, no quotes in SOQL)
        - CDM field type "datetime" → YYYY-MM-DDTHH:MM:SSZ
        """
        if cdm_field.cdm_type == "date":
            # Strip time component — Salesforce Date fields require bare date literal
            return iso_datetime[:10]           # "2025-01-01T00:00:00Z" → "2025-01-01"
        else:
            # DateTime field — keep full ISO 8601 with Z suffix
            if not iso_datetime.endswith("Z"):
                iso_datetime = iso_datetime.rstrip("Z+:00") + "Z"
            return iso_datetime

    def build_soql_where(
        self,
        filters:    dict,
        date_range: DateRange | None,
        mapping:    CDMMapping,
    ) -> list[str]:
        clauses: list[str] = []
        for cdm_field_name, value in filters.items():
            soql_field = mapping.field_map.get(cdm_field_name)
            if soql_field:
                # String values are single-quoted in SOQL; numeric/boolean are bare
                if isinstance(value, str):
                    clauses.append(f"{soql_field} = '{value}'")
                else:
                    clauses.append(f"{soql_field} = {value}")
        if date_range:
            date_cdm_field = mapping.get_date_field()   # Returns the CDMField for the primary date
            if date_cdm_field and date_cdm_field.soql_name:
                from_str = self._format_soql_date(date_range.from_, date_cdm_field)
                to_str   = self._format_soql_date(date_range.to,    date_cdm_field)
                clauses.append(f"{date_cdm_field.soql_name} >= {from_str}")
                clauses.append(f"{date_cdm_field.soql_name} <= {to_str}")
        return clauses

class SQLTranslator:
    """Standard SQL (PostgreSQL, SQL Server, MySQL)"""
    def translate(self, plan: CDMQueryPlan, mapping: CDMMapping) -> tuple[str, dict]:
        select_clause = self.build_select(plan.aggregations, plan.group_by, mapping)
        where_clause, params = self.build_where(plan.filters, plan.date_range, mapping)
        group_clause = self.build_group_by(plan.group_by, mapping)

        query = f"""
            SELECT {select_clause}
            FROM   {mapping.source_schema}.{mapping.source_table}
            WHERE  {where_clause}
            {group_clause}
        """.strip()
        return query, params   # Always parameterised — no string interpolation

class CypherTranslator:
    """Neo4j Cypher"""
    def translate(self, plan: CDMQueryPlan, mapping: CDMMapping) -> tuple[str, dict]:
        # Relationship intent always produces a traversal pattern
        match_clause  = self.build_match(plan.entity, plan.filters)
        where_clause  = self.build_tenant_where(plan.tenant_id)  # Always added
        return_clause = self.build_return(plan.aggregations, plan.group_by)

        query = f"""
            {match_clause}
            WHERE {where_clause}
            RETURN {return_clause}
            LIMIT $limit
        """
        return query, {"limit": 500, "tenant_id": plan.tenant_id}

class ANNTranslator:
    """Elasticsearch kNN Approximate Nearest Neighbour"""
    def translate(self, plan: CDMQueryPlan, mapping: CDMMapping) -> tuple[str, dict]:
        # No query string — returns parameters for Elasticsearch kNN query
        return "", {
            "query_text":  plan.resolved_query,
            "index_name":  f"{plan.tenant_id}-entities",
            "top_k":       20,
            "filter":      {"tenant_id": plan.tenant_id, "entity_type": plan.entity}
        }
```

**SQL injection prevention:** All SQL translators produce parameterised queries. CDM field names are whitelisted against the CDM mapping schema before inclusion in query strings. No user-supplied string is ever interpolated into a query.

**Credential forwarding:** The `credential_ref` in each `SourceQuery` points to the Secrets Manager path for that connector's credentials. The connector worker resolves the secret at execution time. The decomposer never reads credentials directly.

---

### Component 3: Parallel Executor

**Responsibility:** Fans out `SourceQuery` objects concurrently to their respective backends. Returns partial results on failure. Enforces per-source timeouts.

```python
class ParallelExecutor:

    async def execute(
        self,
        queries:       list[SourceQuery],
        user_identity: UserIdentity
    ) -> ExecutionResult:

        # Publish "executing" event before starting
        await self.publisher.emit_event("executing", {
            "sources": [q.source_system for q in queries]
        })

        tasks = [
            asyncio.wait_for(
                self.execute_one(q, user_identity),
                timeout=q.timeout_seconds
            )
            for q in queries
        ]

        raw_results = await asyncio.gather(*tasks, return_exceptions=True)

        successful: list[SourceResult] = []
        failed:     list[SourceFailure] = []

        for query, result in zip(queries, raw_results):
            if isinstance(result, Exception):
                logger.warning(
                    f"Source query failed: source={query.source_system} "
                    f"error={type(result).__name__}: {result}"
                )
                failed.append(SourceFailure(
                    source_system= query.source_system,
                    error_type=    type(result).__name__,
                    error_message= str(result),
                ))
                metrics.query_source_failures.labels(
                    source=query.source_system,
                    error_type=type(result).__name__
                ).inc()
            else:
                successful.append(result)

        if not successful:
            # All sources failed — terminal error
            raise AllSourcesFailedError(
                f"All {len(queries)} source(s) failed. Cannot produce a result.",
                failures=failed
            )

        return ExecutionResult(
            successful=   successful,
            failed=       failed,
            partial=      len(failed) > 0,
        )
```

**execute_one — backend dispatch with two-phase query pattern:**

**Issue C correction — Two-phase query pattern:** Under virtual CDM, Elasticsearch and Neo4j AI stores hold only reference tuples (IDs + structural metadata), not business field values. A single-phase pattern that returns AI store results directly would expose ID lists to the renderer, not usable data. The executor must implement explicit phase separation:

- **Phase 1:** Query the AI store (Cypher/ANN). Collect ranked IDs from result metadata.
- **Phase 2:** For each ID batch, dispatch a live-source fetch via connector-worker using the user's identity (RBAC forwarded). Business field data is retrieved from the source system, not from the AI store.

SOQL/SQL backends are single-phase by definition (they go directly to the live source). TimescaleDB is single-phase (pre-computed aggregates already at field-value level).

```python
async def execute_one(
    self,
    query:         SourceQuery,
    user_identity: UserIdentity
) -> SourceResult:

    match query.query_dialect:

        case "SOQL" | "SQL":
            # Single phase — live source query via connector-worker (Kafka request-reply)
            return await self.connector_worker_client.execute(
                connector_id=  query.connector_id,
                native_query=  query.native_query,
                parameters=    query.parameters,
                user_identity= user_identity,  # Forwarded for source RBAC
                timeout=       query.timeout_seconds,
            )

        case "Cypher":
            # ── Phase 1: retrieve entity IDs from Neo4j ──────────────────────────
            # Neo4j holds IDs and relationship structure only (virtual CDM).
            # The Cypher query returns rows like: {source_record_id, connector_id, entity_type}
            id_rows = await self.neo4j.run(
                query.native_query,
                {**query.parameters, "tenant_id": user_identity.tenant_id}
            )
            if not id_rows:
                return SourceResult(source_system="neo4j", rows=[], phase="completed")

            # ── Phase 2: fetch business field data from live source ───────────────
            # Group IDs by (connector_id, entity_type) to minimise round-trips
            id_batches: dict[tuple, list[str]] = {}
            for row in id_rows:
                key = (row["connector_id"], row["entity_type"])
                id_batches.setdefault(key, []).append(row["source_record_id"])

            live_rows: list[dict] = []
            for (connector_id, entity_type), source_ids in id_batches.items():
                batch_result = await self.connector_worker_client.fetch_by_ids(
                    connector_id=  connector_id,
                    entity_type=   entity_type,
                    source_ids=    source_ids,
                    user_identity= user_identity,   # RBAC forwarded — user may not see all IDs
                    timeout=       query.timeout_seconds,
                )
                live_rows.extend(batch_result.rows)

            return SourceResult(
                source_system= "neo4j+live",
                rows=          live_rows,
                phase=         "completed",
                # Preserve graph structure for relationship rendering
                graph_context= id_rows,
            )

        case "ANN":
            # ── Phase 1: retrieve ranked entity IDs from Elasticsearch (kNN) ──────────
            # Elasticsearch document metadata holds only the reference tuple (see M3 spec §1.4).
            # Matches contain: {tenant_id, entity_type, connector_id, source_system,
            #                   source_record_id, cdm_entity_id, cdm_version}
            ann_results = await self.elasticsearch.knn_search(
                index_name= query.parameters["index_name"],
                vector=     await self.embed(query.parameters["query_text"]),
                top_k=      query.parameters["top_k"],
                filter=     query.parameters["filter"],
            )
            if not ann_results.matches:
                return SourceResult(source_system="elasticsearch", rows=[], phase="completed")

            # ── Phase 2: fetch business field data from live source ───────────────
            id_batches: dict[tuple, list[str]] = {}
            for match in ann_results.matches:
                meta = match.metadata
                key  = (meta["connector_id"], meta["entity_type"])
                id_batches.setdefault(key, []).append(meta["source_record_id"])

            live_rows: list[dict] = []
            for (connector_id, entity_type), source_ids in id_batches.items():
                batch_result = await self.connector_worker_client.fetch_by_ids(
                    connector_id=  connector_id,
                    entity_type=   entity_type,
                    source_ids=    source_ids,
                    user_identity= user_identity,   # RBAC forwarded
                    timeout=       query.timeout_seconds,
                )
                live_rows.extend(batch_result.rows)

            return SourceResult(
                source_system= "elasticsearch+live",
                rows=          live_rows,
                phase=         "completed",
                # Preserve ANN scores for relevance ranking in the Result Merger
                ann_scores=    {m.metadata["source_record_id"]: m.score
                                for m in ann_results.matches},
            )

        case "TIMESCALEDB_SQL":
            # Single phase — pre-computed aggregates are at field-value level already
            rows = await self.timescaledb.fetch(
                query.native_query,
                {**query.parameters, "tenant_id": user_identity.tenant_id}
            )
            return SourceResult(source_system="timescaledb", rows=rows, phase="completed")
```

**`fetch_by_ids` contract (connector-worker extension):** The two-phase pattern requires a new connector-worker endpoint `fetch_by_ids` that retrieves a batch of records by their source IDs, forwarding user identity for RBAC. This endpoint is published to the `{tid}.connector.query_requested` Kafka topic with `query_type: "fetch_by_ids"` and awaits the response on `{tid}.connector.query_result`.

**Connector Worker Request-Reply:** For live-source queries (SOQL/SQL, and phase-2 fetches in Cypher/ANN), the executor does not call source systems directly — it publishes a request to `{tid}.connector.query_requested` and waits for a response on `{tid}.connector.query_result` (correlated by `request_id`). This preserves the architectural rule that `nexus-m1-worker` is the only service with source system credentials.

[CLARIFY: The Kafka request-reply pattern for connector queries introduces latency overhead (publish + poll). For Iteration 2, evaluate whether a direct HTTP call from query-executor to connector-worker (service-to-service, internal cluster) is acceptable for low-latency queries, given that Kafka round-trips add 50–200ms. The architectural purity of Kafka-only inter-module communication must be weighed against the < 3s latency target.]

---

### Component 4: Result Merger + Synthesizer

**Responsibility:** Combines results from multiple sources into a single `MergedResult`, then passes it to the `ResultRenderer` (defined in Visual Outputs spec) to produce the final `RenderedOutput`.

```python
@dataclass
class MergedResult:
    intent:          str
    scalars:         dict                  # {"deal_count": 287, "total_value": 4820000}
    breakdown:       list[dict]            # Per-source breakdown
    rows:            list[dict]            # For lookup / table results
    graph_nodes:     list[dict]            # For Neo4j relationship results
    time_series:     list[dict]            # For TimescaleDB trend results
    sources_queried: list[str]
    sources_failed:  list[SourceFailure]
    partial:         bool                  # True if at least one source failed

class ResultMerger:

    def merge(
        self,
        results: list[SourceResult],
        plan:    CDMQueryPlan,
        failed:  list[SourceFailure]
    ) -> MergedResult:

        match plan.intent:

            case "aggregation":
                return MergedResult(
                    intent="aggregation",
                    scalars=self._sum_scalars(results, plan.aggregations),
                    breakdown=[
                        {
                            "source":  r.source_system,
                            **self._extract_scalars(r, plan.aggregations)
                        }
                        for r in results
                    ],
                    rows=[],
                    graph_nodes=[],
                    time_series=[],
                    sources_queried=[r.source_system for r in results],
                    sources_failed=failed,
                    partial=len(failed) > 0,
                )

            case "lookup":
                all_rows = []
                for r in results:
                    for row in r.rows:
                        row["_source"] = r.source_system
                        all_rows.append(row)
                # Deduplicate on CDM entity ID if present
                deduped = self._deduplicate(all_rows, key="_cdm_entity_id")
                return MergedResult(
                    intent="lookup", rows=deduped,
                    scalars={}, breakdown=[], graph_nodes=[], time_series=[],
                    sources_queried=[r.source_system for r in results],
                    sources_failed=failed, partial=len(failed) > 0,
                )

            case "relationship":
                # Neo4j returns graph nodes/edges — no merging needed
                assert len(results) == 1, "Neo4j queries always return one result"
                return MergedResult(
                    intent="relationship",
                    graph_nodes=results[0].rows,
                    scalars={}, breakdown=[], rows=[], time_series=[],
                    sources_queried=["neo4j"],
                    sources_failed=[], partial=False,
                )

            case "trend":
                # TimescaleDB returns time-bucketed rows
                return MergedResult(
                    intent="trend",
                    time_series=results[0].rows if results else [],
                    scalars={}, breakdown=[], rows=[], graph_nodes=[],
                    sources_queried=["timescaledb"],
                    sources_failed=failed, partial=len(failed) > 0,
                )

    def _sum_scalars(self, results: list[SourceResult], aggregations: list[dict]) -> dict:
        totals = {}
        for agg in aggregations:
            field = agg.get("field") or agg["func"].lower()
            totals[field] = sum(
                r.scalar(field) or 0
                for r in results
            )
        return totals
```

---

### Post-Synthesis Cross-Tenant Safety Scanner

**Responsibility:** After the Result Merger produces a `MergedResult`, and before the `result` event is published to `{tid}.query.event`, the safety scanner verifies that every source referenced in the result belongs to the session's `tenant_id`. This is a defence-in-depth check supplementing the pre-query OPA authorisation (Rule 5). It catches data leakage that could theoretically occur through multi-tenant bugs (e.g., a misconfigured connector or a caching collision).

**When it runs:** Between Component 4 (Result Merger) and final publication of the `result` event to Kafka.

**What it checks:** For each `connector_id` referenced in the merged result (extracted from `sources_queried` and from source record metadata), it performs a tenant-scoped lookup against `nexus_system.connectors` via RLS. If any `connector_id` does not resolve to a connector owned by the session `tenant_id`, the scan fails.

**On failure:** The result is **discarded** — this is not a partial result. The session is moved to `FAILED` and a `security_violation` error event is published. A high-severity alert is raised (Grafana alert rule on `security_violation` counter > 0). The incident is logged with the full list of offending connector IDs for forensic investigation.

```python
class CrossTenantSafetyScanner:
    """
    Post-synthesis cross-tenant validation (Rule 7 / QE-FR-15).

    Verifies that every connector referenced in the merged result belongs to
    the session's tenant_id. Uses PostgreSQL RLS to perform the check —
    the RLS policy on nexus_system.connectors ensures that a connector not
    owned by the current tenant returns zero rows.

    On violation: raises CrossTenantViolationError, which the executor
    catches and converts to a security_violation error event (not a partial).
    """

    async def scan(
        self,
        merged:    MergedResult,
        tenant_id: str,
        session_id: str,
    ) -> None:
        # Collect all connector IDs referenced in the result
        referenced_connector_ids: set[str] = set()
        for source in merged.sources_queried:
            # connector_id is embedded in source metadata for phase-2 fetches
            if hasattr(source, "connector_id") and source.connector_id:
                referenced_connector_ids.add(source.connector_id)

        if not referenced_connector_ids:
            return  # Nothing to check (e.g., pure TimescaleDB result)

        # RLS-scoped query: connectors NOT belonging to this tenant return 0 rows
        async with get_tenant_scoped_connection(self.pool, tenant_id) as conn:
            rows = await conn.fetch(
                """
                SELECT connector_id
                FROM   nexus_system.connectors
                WHERE  connector_id = ANY($1)
                """,
                list(referenced_connector_ids),
            )

        confirmed_ids = {r["connector_id"] for r in rows}
        violating_ids = referenced_connector_ids - confirmed_ids

        if violating_ids:
            metrics.cross_tenant_violations.labels(tenant_id=tenant_id).inc()
            logger.critical(
                f"CROSS-TENANT VIOLATION DETECTED: session={session_id} "
                f"tenant={tenant_id} offending_connectors={violating_ids}"
            )
            raise CrossTenantViolationError(
                session_id=      session_id,
                tenant_id=       tenant_id,
                violating_ids=   violating_ids,
            )
```

**Executor integration — where the scanner is called:**

```python
# Inside nexus-query-executor main pipeline, after Result Merger and before publication:
merged_result = result_merger.merge(execution_result.successful, plan, execution_result.failed)

try:
    await safety_scanner.scan(merged_result, tenant_id=plan.tenant_id, session_id=plan.session_id)
except CrossTenantViolationError as e:
    await self.publisher.emit_event("error", {
        "error_code":    "security_violation",
        "error_message": "Result discarded: cross-tenant source detected. This incident has been logged.",
    })
    await self.update_session_status(plan.session_id, "FAILED")
    return  # Do not render or publish the result

# Safety scan passed — proceed to render and publish
rendered = result_renderer.render(merged_result, plan)
await self.publisher.emit_event("result", rendered)
```

**Observability:**

| Metric | Type | Description |
|---|---|---|
| `query_cross_tenant_violations_total` | Counter | Incremented on every violation. Alert fires at > 0. |
| `query_safety_scan_duration_seconds` | Histogram | Latency of the scanner's PostgreSQL lookup |

---

### OPA Security Check

OPA is called synchronously between the Query Planner and the Query Decomposer. If OPA denies the query, the session is immediately moved to `FAILED` and a `{tid}.query.event` is published with `event: "error"`.

```python
async def check_query_permission(
    plan:    CDMQueryPlan,
    user:    UserIdentity,
    opa_url: str = "http://nexus-opa.nexus-infra:8181"
) -> OPADecision:

    payload = {
        "input": {
            "tenant_id":          user.tenant_id,
            "user_id":            user.user_id,
            "user_role":          user.role,
            "query_intent":       plan.intent,
            "entities":           [plan.entity],
            "pii_columns":        plan.pii_columns_requested,
            "execution_backend":  plan.execution_backend,
        }
    }

    try:
        async with httpx.AsyncClient(timeout=3.0) as client:
            resp = await client.post(
                f"{opa_url}/v1/data/nexus/query/allow",
                json=payload
            )
            result = resp.json()
            return OPADecision(
                allowed=    result.get("result", False),
                deny_reason=result.get("reason", ""),
            )
    except Exception as e:
        logger.error(f"OPA unreachable: {e} — denying query (fail-closed)")
        return OPADecision(allowed=False, deny_reason="opa_unreachable")
```

**Issue 7 correction — `is_pii()` lookup path:** The `pii_columns_requested` field in `CDMQueryPlan` is populated by the Query Planner before the OPA call. The Query Planner checks whether any field in the query plan's aggregations, filters, or group_by is PII-flagged. The source of truth for PII flags is `nexus_system.schema_snapshots.column_profiles`:

```python
class PIIChecker:
    """
    Checks whether a CDM field name is PII-flagged for a given tenant + connector.

    Source of truth: nexus_system.schema_snapshots.column_profiles
    Column profiles are written by nexus-schema-profiler at connector registration
    and updated on subsequent weekly profiling runs.

    The checker caches results in Redis (key: pii:{tenant_id}:{connector_id},
    TTL 1 hour) to avoid repeated PostgreSQL lookups on every query.
    """

    async def is_pii(self, tenant_id: str, connector_id: str, cdm_field_name: str) -> bool:
        cached = await self._get_cached_pii_set(tenant_id, connector_id)
        return cdm_field_name.lower() in cached

    async def get_pii_fields(self, tenant_id: str, connector_id: str) -> set[str]:
        return await self._get_cached_pii_set(tenant_id, connector_id)

    async def _get_cached_pii_set(self, tenant_id: str, connector_id: str) -> set[str]:
        cache_key = f"pii:{tenant_id}:{connector_id}"
        raw = await self.redis.get(cache_key)
        if raw:
            return set(json.loads(raw))

        # Cache miss — query PostgreSQL
        rows = await self.pg.fetch(
            """
            SELECT LOWER(cdm_field_name) AS cdm_field
            FROM   nexus_system.schema_snapshots
            WHERE  tenant_id    = $1
              AND  connector_id = $2
              AND  column_profiles->>'pii' = 'true'
            """,
            tenant_id, connector_id
        )
        pii_set = {r["cdm_field"] for r in rows}
        await self.redis.setex(cache_key, 3600, json.dumps(list(pii_set)))
        return pii_set
```

The Query Planner calls `PIIChecker.get_pii_fields()` for each connector in the decomposed query plan, then populates `CDMQueryPlan.pii_columns_requested` with the intersection of the user's requested fields and the PII-flagged fields. This list is forwarded to OPA in the `pii_columns` input field.

**OPA Policy (nexus/query/allow.rego):**

```rego
package nexus.query

import future.keywords.in

default allow = false

# Allow if no deny rules fire
allow {
    not denied
}

denied {
    deny[_]
}

# PII columns require elevated role
# Note: pii_columns_requested is populated from nexus_system.schema_snapshots.column_profiles
# by PIIChecker before the OPA call. OPA receives the pre-resolved PII column names —
# it does not query the database directly.
deny[msg] {
    col := input.pii_columns[_]
    col in {"nationalidnumber", "ssn", "date_of_birth", "marital_status"}
    not input.user_role in {"hr-admin", "executive", "platform-admin"}
    msg := sprintf("PII column '%v' requires hr-admin, executive, or platform-admin role", [col])
}

# Cross-tenant backend check (should never reach here — belt-and-suspenders)
deny[msg] {
    input.execution_backend == "LIVE_SOURCE"
    not input.tenant_id
    msg := "LIVE_SOURCE query requires a tenant_id"
}

# Report generation requires at least data_steward role
deny[msg] {
    input.query_intent == "report"
    not input.user_role in {"data_steward", "cfo", "ceo", "executive", "platform-admin"}
    msg := "Report generation requires data_steward or executive role"
}

# Out-of-scope queries are always denied
deny[msg] {
    input.query_intent == "out_of_scope"
    msg := "Query is out of scope for available data entities"
}
```

---

### CDM Catalogue Cache

```python
class CDMCatalogueCache:
    """
    Redis-backed cache for CDM entity catalogues.
    Invalidated on nexus.cdm.version_published events.
    """

    KEY_PATTERN = "cdm_catalogue:{tenant_id}:{cdm_version}"
    TTL = 86400  # 24 hours

    async def get(self, tenant_id: str, cdm_version: str) -> dict | None:
        key = self.KEY_PATTERN.format(tenant_id=tenant_id, cdm_version=cdm_version)
        raw = await self.redis.get(key)
        return json.loads(raw) if raw else None

    async def set(self, tenant_id: str, cdm_version: str, catalogue: dict):
        key = self.KEY_PATTERN.format(tenant_id=tenant_id, cdm_version=cdm_version)
        await self.redis.setex(key, self.TTL, json.dumps(catalogue))

    async def invalidate_tenant(self, tenant_id: str):
        """Called on cdm.version_published — deletes all versions for this tenant."""
        pattern = f"cdm_catalogue:{tenant_id}:*"
        keys = await self.redis.keys(pattern)
        if keys:
            await self.redis.delete(*keys)
        logger.info(f"CDM catalogue cache invalidated: tenant={tenant_id} keys_deleted={len(keys)}")
```

**Issue 6 correction — CDM cache invalidation handler:** The `nexus-query-executor` consumer group `query-executor-cache-invalidator` subscribes to `nexus.cdm.version_published`. The explicit handler that processes this event and calls `invalidate_tenant()` is:

```python
class CDMVersionPublishedHandler:
    """
    Consumes nexus.cdm.version_published events and invalidates the CDM catalogue
    cache for the affected tenant so the next Query Planner call rebuilds it.

    Consumer group: query-executor-cache-invalidator
    Topic:          nexus.cdm.version_published (global, not tenant-scoped)
    """

    def __init__(self, cache: CDMCatalogueCache):
        self.cache = cache

    async def handle(self, event: KafkaMessage) -> None:
        """
        Event payload (nexus.cdm.version_published):
          {
            "tenant_id":     "acme-corp",
            "new_version":   "2.1.0",
            "prior_version": "2.0.3",
            "changed_entity_types": ["event.deal_closed", "party.customer"]
          }
        """
        tenant_id   = event.payload["tenant_id"]
        new_version = event.payload["new_version"]

        await self.cache.invalidate_tenant(tenant_id)

        # Eagerly warm the cache for the new version to avoid a cold-start on the
        # first query after a CDM publish. Runs as a background task.
        asyncio.create_task(
            self._warm_cache(tenant_id, new_version)
        )
        logger.info(
            f"CDM catalogue invalidated and warm-up scheduled: "
            f"tenant={tenant_id} new_version={new_version}"
        )

    async def _warm_cache(self, tenant_id: str, cdm_version: str) -> None:
        try:
            catalogue = await self.builder.build(tenant_id, cdm_version)
            await self.cache.set(tenant_id, cdm_version, catalogue)
            logger.info(f"CDM catalogue warm-up complete: tenant={tenant_id} version={cdm_version}")
        except Exception as e:
            # Warm-up failure is not fatal — the next query will rebuild on demand
            logger.warning(f"CDM catalogue warm-up failed: tenant={tenant_id} error={e}")
```

---

### nexus-query-executor Scaling

| Parameter | Value |
|---|---|
| Min replicas | 2 |
| Max replicas | 12 |
| KEDA trigger | Kafka lag on `{tid}.query.submitted` > 5 messages |
| Cooldown | 180s |
| Startup time | 20–40s (LLM client init, M3 store connections, cache warm) |

### nexus-query-executor Resource Profile

```yaml
resources:
  requests:
    cpu: 1000m
    memory: 2Gi
  limits:
    cpu: 4000m
    memory: 8Gi
```

LLM calls buffer multi-kilobyte contexts. Parallel execution holds multiple in-flight source results. High memory limit is appropriate.

### nexus-query-executor Environment Variables

| Variable | Source | Description |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | ConfigMap | Broker addresses |
| `POSTGRES_DSN` | Secrets Manager | PostgreSQL DSN |
| `REDIS_URL` | Secrets Manager | Redis for CDM catalogue cache |
| `ANTHROPIC_API_KEY` | Secrets Manager | LLM for Query Planner |
| `OPENAI_API_KEY` | Secrets Manager | Embedding model for ANN queries |
| `ELASTICSEARCH_API_KEY` | Secrets Manager | Elasticsearch read access (kNN queries) |
| `NEO4J_URI` | ConfigMap | Neo4j Aura endpoint |
| `NEO4J_USERNAME` | Secrets Manager | |
| `NEO4J_PASSWORD` | Secrets Manager | |
| `TIMESCALEDB_DSN` | Secrets Manager | Direct TimescaleDB access (read only) |
| `OPA_URL` | ConfigMap | `http://nexus-opa.nexus-infra:8181` |
| `QUERY_PLAN_LLM_TIMEOUT_SECONDS` | ConfigMap | Default: `8` |
| `SOURCE_QUERY_TIMEOUT_SECONDS` | ConfigMap | Per-source timeout, default: `15` |
| `CDM_CATALOGUE_CACHE_TTL_SECONDS` | ConfigMap | Default: `86400` |
| `MINIO_ENDPOINT` | ConfigMap | For report/export storage |
| `MINIO_ACCESS_KEY` | Secrets Manager | |
| `MINIO_SECRET_KEY` | Secrets Manager | |

---

## Edge Cases

| Case | Handling |
|---|---|
| LLM returns invalid JSON for query plan | Retry once with an explicit JSON correction prompt. If second attempt fails, return `error_code: "planner_parse_error"` |
| LLM plan references an entity not in CDM catalogue | Raise `UnknownEntityError` → `error_code: "entity_not_found"` with suggestion to run data ingestion first |
| All sources fail in parallel executor | Return `error_code: "all_sources_failed"` with `failures` detail |
| One of two sources fails (partial) | Return `partial: true` in result; note missing source in `sources_failed` field |
| OPA unreachable | Fail-closed: deny query with `error_code: "opa_unreachable"` |
| Neo4j graph traversal returns 0 nodes | Return `TEXT` result: "No matching relationships found for your query" |
| Query exceeds total timeout (30s) | Publish `error_code: "query_timeout"` event; mark session `TIMEOUT` |
| CDM catalogue cache miss (Redis cold or invalidated) | Build catalogue from PostgreSQL, cache it, proceed. Log cache miss metric |
| User submits the same query twice in parallel sessions | Both sessions proceed independently — no deduplication |
| `output_preference` = `auto` but user_role = `cfo` and intent = `aggregation` | Planner selects `BAR_CHART`; CFO override in persona map (see Visual Outputs spec) |
| Source returns > 10,000 rows for a lookup query | Truncate at 10,000 rows; add `truncated: true` flag in result metadata; log warning |
| Cross-tenant safety scan detects a violating connector ID | Discard result entirely; publish `security_violation` error event; mark session FAILED; emit high-severity alert. Do NOT return as partial result. |
| Cross-tenant safety scanner PostgreSQL lookup times out | Fail-closed: treat as a violation (discard result, return security_violation error). Never skip the scan. |

---

## Open Questions

| # | Question | Impact |
|---|---|---|
| OQ-QE-01 | Should `nexus-query-api` and `nexus-m2-api` share the same Redis cluster for WebSocket registries? Currently they use separate key namespaces — sharing is safe but adds coupling. | Operational simplicity vs. service isolation |
| OQ-QE-02 | For LIVE_SOURCE queries, should query-executor call connector-worker via Kafka request-reply or direct HTTP? Kafka adds ~100ms overhead but preserves module boundaries. | Latency vs. architectural purity |
| OQ-QE-03 | Should query sessions (and their results) be stored for 30 days, 7 days, or indefinitely? Table will grow large over time. | Storage cost vs. audit completeness |
| OQ-QE-04 | Should the CDM catalogue cache live in the same Redis instance as the M2 WebSocket registry? They have very different access patterns (catalogue: few large keys; WS: many small keys). | Redis memory segmentation |
| OQ-QE-05 | The Query Planner currently uses Claude Sonnet 4 for planning and Claude Sonnet 4 for result synthesis (in Report Builder). Should Haiku be used for the planning step to reduce cost? Haiku is 10× cheaper but less reliable at structured JSON output. | LLM cost vs. plan quality |
| OQ-QE-06 | `nexus_system.identity_mapping` — what is the schema of this table and was it defined in an Iteration 1 spec? The architectural description states it is "seeded in Iteration 1, enforced in Iteration 2." If not yet spec'd, it must be added to the Iteration 2 Data Model doc before the Query Decomposer is implemented. [See Data Model spec for CLARIFY.] | Blocks Rule 6 enforcement in Decomposer |

---

*NEXUS Iteration 2 · Query Engine Spec · v0.3 · Mentis Consulting · April 2026 · Confidential*
