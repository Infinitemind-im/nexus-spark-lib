# Service Spec: `nexus-m2-executor`
**Module:** M2 — AI Intelligence Hub (+ M3 store clients)
**Type:** Event-driven worker · Kubernetes `Deployment`
**Team:** AI & Knowledge
**Version:** 1.1 · March 2026 · Confidential

---

## Purpose

The cognitive core of NEXUS. Consumes queries from Kafka, executes the full RHMA (Reasoning, Hierarchical, Multi-Agent) pipeline, and publishes the completed response. Also handles the M2 Structural Agent cycle — interpreting schema artifacts from M1 and proposing CDM extensions.

This is the most resource-intensive service in the platform. It makes LLM API calls, fans out to three separate knowledge stores, and manages concurrent multi-step reasoning. It has **no HTTP surface** — it exists entirely inside the Kafka pipeline.

**Critical rule: This is the only service permitted to make LLM API calls.** Any LLM call found in another service is a bug.

**M1 delegation pattern:** M1 never calls an LLM directly. When `nexus-m1-executor` finishes schema extraction, it publishes the `StructuralArtifact` to `{tid}.m1.semantic_interpretation_requested` as a delegated natural language query. This service consumes that topic under consumer group `m2-structural-agents` and is solely responsible for making the LLM call (Claude) to interpret the schema and produce CDM extension proposals. M1 fires and forgets — it commits its Kafka offset immediately and has no awareness of M2's processing.

---

## `agent_core` Library

### Purpose

All three pipeline variants (A, B, C) in this service share the same underlying concerns: LLM client management, pipeline state, store access, OPA safety, and Kafka lifecycle. Rather than duplicating these across pipeline implementations, they are encapsulated in a shared internal library: `agent_core`.

`agent_core` is a **versioned Python package** published to NEXUS's internal package registry. It is the **only permitted way** to make LLM calls or query knowledge stores within this service. Pipelines that bypass `agent_core` and call LLM APIs or store clients directly require explicit Tech Lead review before merge.

The library is also **exposed for future module development** — any future M-module pipeline that needs LLM reasoning, store access, or safety validation imports `agent_core` rather than reimplementing those primitives.

### Package Structure

```
agent_core/
├── pipeline/
│   ├── base.py            # AgentPipeline — abstract base class for all pipelines
│   ├── context.py         # PipelineContext — tenant-scoped state per run
│   └── stage.py           # @pipeline_stage decorator — timing, logging, error wrapping
├── llm/
│   ├── client.py          # LLMClient — primary/fallback routing, retry, token tracking
│   ├── prompt_guard.py    # PromptGuard — validates no PII/raw values in prompts before send
│   └── models.py          # LLMRequest / LLMResponse dataclasses
├── stores/
│   ├── vector.py          # VectorStoreClient — Pinecone, tenant-scoped index lookup
│   ├── graph.py           # GraphStoreClient — Neo4j, TenantScopedSession
│   ├── timeseries.py      # TimeSeriesClient — TimescaleDB, get_tenant_scoped_connection()
│   └── query_set.py       # StoreQuerySet — unified fan-out interface across all three stores
├── safety/
│   └── opa.py             # OPASafetyLayer — response validation via OPA HTTP API
└── kafka/
    └── pipeline_consumer.py  # PipelineConsumer — wraps NexusConsumer with run_loop + DLQ
```

### Key Classes

#### `AgentPipeline` (abstract base)

```python
# agent_core/pipeline/base.py
class AgentPipeline(ABC):
    """
    Base class for all M2 executor pipelines.
    Subclasses implement run() and declare their Kafka topic + consumer group.
    The base class handles: set_tenant / clear_tenant, commit-on-success, DLQ routing.
    """
    topic: str                    # Kafka topic to consume — must be set in subclass
    consumer_group: str           # Kafka consumer group — must be set in subclass

    @abstractmethod
    async def run(self, context: PipelineContext) -> None:
        """Execute the full pipeline for one message. Called by PipelineConsumer."""
        ...

    async def on_error(self, context: PipelineContext, exc: Exception) -> None:
        """Default: send to DLQ. Override for custom error handling."""
        await self.dlq.send(context.message, exc)
```

#### `PipelineContext`

```python
# agent_core/pipeline/context.py
@dataclass
class PipelineContext:
    """
    Tenant-scoped state object for one pipeline run.
    Created fresh per message — never shared between tenants or concurrent runs.
    """
    tenant_id:      str
    session_id:     str
    message:        NexusMessage
    started_at:     datetime = field(default_factory=datetime.utcnow)
    stages_passed:  list[str] = field(default_factory=list)
    artifacts:      dict = field(default_factory=dict)   # stage outputs keyed by stage name
    llm_tokens_in:  int = 0
    llm_tokens_out: int = 0
```

#### `LLMClient`

```python
# agent_core/llm/client.py
class LLMClient:
    """
    Single entry point for all LLM calls in the executor.
    Handles: primary/fallback model routing, exponential retry, token counting,
    prompt safety guard (PromptGuard), response logging.
    """
    async def complete(
        self,
        context: PipelineContext,
        prompt: str,
        system_prompt: str,
        stage: str,            # e.g. "planner", "synthesizer", "schema_narrative"
        max_tokens: int = 2048,
    ) -> LLMResponse:
        PromptGuard.validate(prompt)   # raises PromptViolation if raw PII found
        try:
            return await self._call_primary(prompt, system_prompt, max_tokens)
        except LLMUnavailable:
            return await self._call_fallback(prompt, system_prompt, max_tokens)
```

#### `StoreQuerySet`

```python
# agent_core/stores/query_set.py
class StoreQuerySet:
    """
    Unified fan-out interface for querying all three knowledge stores.
    Each query method takes tenant_id — never queries without it.
    """
    async def query_all(
        self,
        tenant_id: str,
        entity_types: list[str],
        vector_query: str | None = None,
        graph_cypher: str | None = None,
        ts_sql: str | None = None,
    ) -> AggregatedResults:
        results = await asyncio.gather(
            self.vector.query(tenant_id, entity_types, vector_query) if vector_query else empty(),
            self.graph.query(tenant_id, graph_cypher) if graph_cypher else empty(),
            self.timeseries.query(tenant_id, ts_sql) if ts_sql else empty(),
        )
        return AggregatedResults.merge(results)
```

### How Pipelines Use `agent_core`

```python
# Example — Pipeline A inheriting AgentPipeline
from agent_core import AgentPipeline, PipelineContext, LLMClient, StoreQuerySet, OPASafetyLayer

class RHMAPipeline(AgentPipeline):
    topic = "{tid}.m2.knowledge_query"
    consumer_group = "m2-query-executors"

    async def run(self, context: PipelineContext) -> None:
        plan = await self.llm.complete(context, build_planning_prompt(context), stage="planner")
        results = await self.stores.query_all(context.tenant_id, ...)
        response = await self.llm.complete(context, build_synthesis_prompt(results), stage="synthesizer")
        if not await self.safety.validate(context.tenant_id, response):
            raise OPAViolation("Response blocked by safety layer")
        await self.producer.publish(f"{context.tenant_id}.m2.agent_response_ready", response)
```

### Versioning and Exposure

`agent_core` follows semantic versioning. Breaking changes (new required methods on `AgentPipeline`, changes to `PipelineContext` fields) increment the major version. The package is published to the NEXUS internal PyPI registry at `nexus-pypi.internal/agent-core`. Future services that need agent capabilities import it as a dependency rather than building LLM/store/safety plumbing from scratch.

---

## Multi-Tenancy Model

### Tenant Identity Source — Kafka Envelope
`tenant_id` always comes from `NexusMessage.tenant_id`. The executor calls `set_tenant()` at the top of each message processing cycle and `clear_tenant()` in the `finally` block.

### Three-Layer Knowledge Store Isolation

Each knowledge store enforces tenant isolation at a different layer. The executor must correctly implement all three:

#### Layer 1 — Pinecone (Vector Store)
Separate Pinecone index per `{tenant_id}-{entity_type}`. The executor constructs the index name from the message's `tenant_id` — it physically cannot query another tenant's vectors without constructing the wrong index name.

```python
def _get_index(self, tenant_id: str, entity_type: str) -> pinecone.Index:
    index_name = f"{tenant_id}-{entity_type}"   # e.g. "acme-corp-party"
    return pinecone.Index(index_name)

async def query_vector_store(self, tenant_id: str, entity_type: str, query_embedding: list[float]) -> list[dict]:
    index = self._get_index(tenant_id, entity_type)
    results = index.query(
        vector=query_embedding,
        top_k=20,
        filter={"tenant_id": tenant_id},   # Belt-and-suspenders metadata filter
        include_metadata=True,
    )
    return results.matches
```

#### Layer 2 — Neo4j (Graph Store)
All nodes carry `tenant_id` as a mandatory property. The `TenantScopedSession` wrapper injects the tenant filter into every Cypher query. Any Cypher query that bypasses this wrapper requires explicit Tech Lead review.

```python
class TenantScopedSession:
    """Wraps a Neo4j session. Makes it impossible to query without a tenant filter."""

    def __init__(self, session: neo4j.AsyncSession, tenant_id: str):
        self._session = session
        self._tenant_id = tenant_id

    async def run(self, cypher: str, **params) -> neo4j.AsyncResult:
        # Automatically inject tenant_id into every query
        if "tenant_id" not in params:
            params["tenant_id"] = self._tenant_id
        if "$tenant_id" not in cypher and "WHERE" in cypher.upper():
            raise TenantFilterMissing(
                f"Cypher query missing $tenant_id parameter: {cypher[:100]}"
            )
        return await self._session.run(cypher, **params)

# Usage (safe):
async with TenantScopedSession(session, tenant_id) as ts:
    result = await ts.run("""
        MATCH (p:Party {tenant_id: $tenant_id})-[:INVOLVED_IN]->(i:Incident)
        WHERE p.name CONTAINS $name_fragment
        RETURN i
    """, name_fragment="Acme")
```

#### Layer 3 — TimescaleDB (Time-Series Store)
Row-Level Security on TimescaleDB enforces `tenant_id` at the database engine level. The executor uses `get_tenant_scoped_connection()` — even if a query accidentally omits `WHERE tenant_id = $1`, the RLS policy returns zero rows for the wrong tenant.

```python
async with get_tenant_scoped_connection(ts_pool, tenant_id) as conn:
    rows = await conn.fetch("""
        SELECT time_bucket('1 hour', ts) AS bucket,
               AVG(metric_value) AS avg_value
        FROM nexus_timeseries.metrics
        WHERE entity_type = $1
          AND ts > NOW() - INTERVAL '7 days'
        ORDER BY bucket DESC
    """, entity_type)
    # RLS ensures only this tenant's metrics are returned
```

### OPA Safety Layer — Response Scanning
Before publishing any LLM-generated response, it passes through the Open Policy Agent (OPA) safety layer. OPA validates:
1. The response does not contain entity data from another tenant's `cdm_id` namespace
2. The `sources` list contains only entities belonging to the authenticated tenant
3. No sensitive patterns (PII formats, credential patterns) are present in the response text

```python
async def validate_response(self, tenant_id: str, response: dict) -> bool:
    opa_input = {
        "tenant_id": tenant_id,
        "response_text": response["response_text"],
        "sources": response.get("sources", []),
    }
    result = await httpx_client.post(
        f"{OPA_URL}/v1/data/nexus/m2/safety/allow",
        json={"input": opa_input},
    )
    decision = result.json()
    if not decision.get("result", False):
        logger.error(
            "OPA safety check failed — response blocked",
            tenant_id=tenant_id,
            violation=decision.get("violations"),
        )
        return False
    return True
```

### LLM Prompt Isolation
LLM prompts must **never** contain raw tenant data (record values, PII, business metrics). Prompts contain only:
- CDM field names and entity types (structural metadata — no tenant-specific values)
- The user's original query text
- Aggregated statistics (counts, ratios — not individual records)

```python
# CORRECT — structural metadata only in prompt
prompt = f"""
You are analyzing enterprise data for a user query.
Available entity types: {', '.join(entity_types)}
User query: {query_text}
Query plan: Fan out to {', '.join(stores_to_query)}
"""

# WRONG — never include raw record values in LLM prompt
# prompt = f"Here are the records: {json.dumps(records)}"  # DO NOT DO THIS
```

The LLM is used only for: (a) interpreting query intent and producing a structured plan, and (b) composing a natural language response from structured query results. Record values are passed directly to the response — not through the LLM.

### Session Management — Tenant-Scoped State
LangGraph maintains a session state object per query. The state object is keyed by `(tenant_id, session_id)` and stored in the executor's in-process memory for the duration of a single query. It is never shared between tenants or persisted to a shared store.

```python
@dataclass
class QueryExecutionState:
    tenant_id:       str
    session_id:      str
    cdm_version:     str
    query_text:      str
    query_plan:      dict | None = None
    sub_results:     list = field(default_factory=list)
    final_response:  str | None = None
    sources:         list = field(default_factory=list)
    reasoning_trace: list = field(default_factory=list)
```

---

## Pipeline Stages (Iteration 1)

This service runs two independent consumer loops concurrently.

### Pipeline A — Executive RHMA (user queries)

```
1.  Consume {tid}.m2.knowledge_query from Kafka
2.  set_tenant(TenantContext)
3.  Query Planner: LLM call → structured query plan
4.  Query Decomposer: break plan into sub-queries per store
5.  Parallel Executor: fan out to Pinecone + Neo4j + TimescaleDB
6.  Result Aggregator: merge sub-results into unified result set
7.  Result Synthesizer: LLM call → natural language response
8.  OPA Safety Layer: validate response for cross-tenant safety
9.  Publish {tid}.m2.agent_response_ready
10. clear_tenant()
11. Commit Kafka offset
```

### Pipeline B — Structural Agent (CDM field classification)

**Consumer group:** `m2-structural-agents`

**Trigger:** `{tid}.m2.schema_narrative_ready` — Pipeline B runs **after** Pipeline C. It consumes a single Kafka message that carries both the `SchemaNarrative` produced by Pipeline C and the original `StructuralArtifact` forwarded by Pipeline C from its own input message. No database read is required — Pipeline C holds the `StructuralArtifact` in memory when it publishes and includes it in the outgoing payload. Combining both gives the LLM structural signals (column names, types, cardinality) enriched with semantic context (domain tags, table summaries, field descriptions) — improving CDM mapping confidence, particularly for ambiguously-named fields that would otherwise fall to Tier 3.

```
1.  Consume {tid}.m2.schema_narrative_ready from Kafka
2.  set_tenant(TenantContext)
3.  Deserialise message payload:
    - SchemaNarrative  (generated by Pipeline C)
    - StructuralArtifact (forwarded by Pipeline C from its own input message)
4.  Build schema interpretation prompt with:
    - CDM system prompt (canonical entity definitions + confidence rules + no-auto-apply constraint)
    - StructuralArtifact (tables, columns, types, cardinality, FK patterns)
    - SchemaNarrative (domain_tags, table_summaries, field-level descriptions, quality_flags)
5.  LLM call via agent_core.LLMClient → ProposedInterpretation JSON
6.  Filter field proposals below 0.50 confidence threshold
7.  Publish nexus.cdm.extension_proposed → M4 governance queue
8.  Publish {tid}.m2.semantic_interpretation_complete → confirmation to M1
9.  clear_tenant()
10. Commit Kafka offset
```

**Implementation class:** `StructuralAgentPipeline(AgentPipeline)`
**File:** `m2/executor/pipelines/structural.py`

---

### Pipeline C — Schema Narrative Agent (db_schema profiling + human-readable narrative)

**Consumer group:** `m2-schema-narrative-agents`

**Runs first in the sequential chain.** Pipeline C consumes `{tid}.m1.semantic_interpretation_requested` directly and produces the `SchemaNarrative` that Pipeline B depends on. It is designed to complete quickly — purely generative, no CDM catalogue matching — so it does not become a bottleneck for the downstream classification step.

This pipeline's purpose is entirely distinct from Pipeline B: rather than proposing CDM field mappings for governance, it produces a **human-readable narrative** about what a source database schema contains. The output serves two consumers: M6 renders it in the "What is in this connector?" admin view, and Pipeline B uses it as enriched LLM context for CDM classification.

Pipeline B asks: *"How should these fields map to the CDM?"* Pipeline C asks: *"What does this database actually contain, in plain language?"*

```
1.  Consume {tid}.m1.semantic_interpretation_requested from Kafka
    (consumer group: m2-schema-narrative-agents — independent of Pipeline B offsets)
2.  set_tenant(TenantContext)
3.  Deserialise StructuralArtifact (tables, columns, types, cardinality, sample values)
4.  Build narrative prompt:
      - Table inventory (names + row counts)
      - Column profiles (name, type, nullability, cardinality estimate)
      - Detected relationships (FK-like patterns, shared key columns)
      - Data quality signals (high null rates, constant columns, outlier distributions)
5.  PromptGuard.validate() — ensure no raw record values or PII in prompt
    (cardinality stats and type metadata are safe; actual cell values are not)
6.  LLM call via agent_core.LLMClient (stage="schema_narrative") → SchemaNarrative JSON
      SchemaNarrative {
          connector_id:    str
          tenant_id:       str
          generated_at:    datetime
          domain_tags:     list[str]        # e.g. ["HR", "Payroll", "Finance"]
          table_summaries: list[TableSummary]
          relationships:   list[Relationship]
          quality_flags:   list[QualityFlag]
          overall_summary: str              # 2–4 sentence human-readable overview
      }
7.  OPA Safety Layer: validate narrative contains no cross-tenant entity references
8.  Publish {tid}.m2.schema_narrative_ready with combined payload:
    - SchemaNarrative (generated above)
    - StructuralArtifact (forwarded unchanged from this pipeline's input message)
    Consumers: M6 (connector detail view — uses SchemaNarrative only)
               Pipeline B m2-structural-agents (uses both)
9.  Update nexus_system.schema_narratives table (upsert by connector_id + cdm_version)
10. clear_tenant()
11. Commit Kafka offset
```

**Implementation class:** `SchemaNarrativePipeline(AgentPipeline)`
**File:** `m2/executor/pipelines/schema_narrative.py`

#### Narrative prompt design

The LLM receives structural metadata only — no actual record values. The prompt is scoped to produce a business-language description, not a technical schema dump:

```python
# m2/executor/pipelines/schema_narrative.py
NARRATIVE_SYSTEM_PROMPT = """
You are a data analyst helping a business user understand what is in a new data source.
You receive a database schema with table names, column names, data types, and cardinality statistics.
Your task is to:
1. Identify the business domain (HR, Finance, Sales, Operations, etc.)
2. Write a 2–4 sentence overview of what this database represents
3. For each table, write 1–2 sentences explaining its business purpose
4. Identify any obvious relationships between tables
5. Flag any data quality concerns (high null rates, suspiciously low cardinality, etc.)

Rules:
- Use business language, not technical database language
- Do not mention specific cell values or records — only structure and statistics
- Do not make assumptions about data you cannot observe from the schema
"""

def build_narrative_prompt(artifact: StructuralArtifact) -> str:
    return f"""
Schema: {artifact.connector_id} ({artifact.source_type})
Tables ({len(artifact.tables)} total):
{format_table_profiles(artifact.tables)}

Column statistics:
{format_column_stats(artifact.columns)}

Detected key relationships:
{format_relationships(artifact.inferred_relationships)}

Please produce a SchemaNarrative JSON object following the schema above.
"""
```

#### New storage dependency

| Store | Usage | Tenant isolation |
|---|---|---|
| PostgreSQL `nexus_system.schema_narratives` | Upsert narrative per connector + CDM version | RLS |

Schema:
```sql
CREATE TABLE nexus_system.schema_narratives (
    narrative_id    UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id       TEXT NOT NULL,
    connector_id    TEXT NOT NULL,
    cdm_version     TEXT NOT NULL,
    domain_tags     TEXT[],
    overall_summary TEXT,
    narrative_json  JSONB NOT NULL,     -- full SchemaNarrative payload
    generated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, connector_id, cdm_version)
);
ALTER TABLE nexus_system.schema_narratives ENABLE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON nexus_system.schema_narratives
    USING (tenant_id = current_setting('app.current_tenant'));
```

---

## Kafka Topics

### Consumed

| Topic | Consumer group | Pipeline |
|---|---|---|
| `{tid}.m2.knowledge_query` | `m2-query-executors` | Pipeline A — RHMA query execution |
| `{tid}.m1.semantic_interpretation_requested` | `m2-schema-narrative-agents` | Pipeline C — schema narrative generation (runs first) |
| `{tid}.m2.schema_narrative_ready` | `m2-structural-agents` | Pipeline B — CDM field classification (runs second) |

Note: Pipelines B and C are **sequential**, not parallel. Pipeline C consumes the raw `StructuralArtifact` from `{tid}.m1.semantic_interpretation_requested` and produces the `SchemaNarrative`. Pipeline B then consumes `{tid}.m2.schema_narrative_ready`, loads the `StructuralArtifact` from `nexus_system.schema_snapshots` via `snapshot_id`, and combines both as LLM context for CDM classification. This sequencing is intentional: the narrative's semantic enrichment (domain tags, field descriptions) measurably improves classification confidence for ambiguously-named fields.

### Produced

| Topic | When | Pipeline |
|---|---|---|
| `{tid}.m2.agent_response_ready` | On successful query completion | A |
| `{tid}.m2.workflow_trigger` | When query intent maps to a business workflow | A |
| `nexus.cdm.extension_proposed` | When CDM field proposals identified | B |
| `{tid}.m2.semantic_interpretation_complete` | After CDM proposals published | B |
| `{tid}.m2.schema_narrative_ready` | After schema narrative generated | C |
| `m1.int.dead_letter` | On unrecoverable error across any pipeline | A / B / C |

---

## Storage Dependencies

| Store | Usage | Pipeline | Tenant isolation |
|---|---|---|---|
| Pinecone | Vector similarity search | A | Separate index per `{tenant_id}-{entity_type}` + metadata filter |
| Neo4j | Graph traversal | A | `TenantScopedSession` wrapper + `tenant_id` property on all nodes |
| TimescaleDB | Time-series queries | A | PostgreSQL RLS policy + `get_tenant_scoped_connection()` |
| PostgreSQL `nexus_system.query_sessions` | Session status + results | A | RLS-scoped |
| PostgreSQL `nexus_system.schema_narratives` | Upsert generated narrative | C | RLS-scoped |
| OpenAI / Anthropic API | LLM calls (all pipelines via `agent_core.LLMClient`) | A / B / C | External — `PromptGuard` blocks raw tenant data before every call |
| OPA | Response safety validation | A / C | Stateless — `tenant_id` included in every OPA input |

---

## Kubernetes Manifests

### Deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: nexus-m2-executor
  namespace: nexus-app
  labels:
    app: nexus-m2-executor
    module: m2
    type: worker
spec:
  replicas: 2
  selector:
    matchLabels:
      app: nexus-m2-executor
  template:
    metadata:
      labels:
        app: nexus-m2-executor
        module: m2
      annotations:
        prometheus.io/scrape: "true"
        prometheus.io/port: "9091"
        prometheus.io/path: "/metrics"
    spec:
      serviceAccountName: nexus-m2-executor-sa
      automountServiceAccountToken: false
      terminationGracePeriodSeconds: 120    # Long — LLM calls can take 30–60s
      containers:
        - name: m2-executor
          image: nexus/m2-executor:latest
          command: ["python", "-m", "m2.executor.entrypoint"]
          ports:
            - containerPort: 9091
              name: metrics
          env:
            - name: POSTGRES_DSN
              valueFrom:
                secretKeyRef:
                  name: nexus-postgres-credentials
                  key: dsn
            - name: TIMESCALE_DSN
              valueFrom:
                secretKeyRef:
                  name: nexus-timescale-credentials
                  key: dsn
            - name: KAFKA_BOOTSTRAP_SERVERS
              valueFrom:
                configMapKeyRef:
                  name: nexus-platform-config
                  key: kafka_bootstrap_servers
            - name: ANTHROPIC_API_KEY
              valueFrom:
                secretKeyRef:
                  name: nexus-llm-credentials
                  key: anthropic_api_key
            - name: OPENAI_API_KEY
              valueFrom:
                secretKeyRef:
                  name: nexus-llm-credentials
                  key: openai_api_key
            - name: PINECONE_API_KEY
              valueFrom:
                secretKeyRef:
                  name: nexus-pinecone-credentials
                  key: api_key
            - name: NEO4J_URI
              valueFrom:
                secretKeyRef:
                  name: nexus-neo4j-credentials
                  key: uri
            - name: NEO4J_PASSWORD
              valueFrom:
                secretKeyRef:
                  name: nexus-neo4j-credentials
                  key: password
            - name: OPA_URL
              valueFrom:
                configMapKeyRef:
                  name: nexus-platform-config
                  key: opa_url
            - name: PRIMARY_LLM
              value: "claude-3-5-sonnet-20241022"
            - name: FALLBACK_LLM
              value: "gpt-4o"
            - name: QUERY_TIMEOUT_SECONDS
              value: "25"     # Must be < m2-api QUERY_TIMEOUT_SECONDS to fail gracefully
          resources:
            requests:
              cpu: 1000m
              memory: 2Gi
            limits:
              cpu: 4000m
              memory: 8Gi
```

### KEDA ScaledObject

```yaml
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: nexus-m2-executor-scaler
  namespace: nexus-app
spec:
  scaleTargetRef:
    name: nexus-m2-executor
  minReplicaCount: 2
  maxReplicaCount: 12
  cooldownPeriod: 180      # LLM calls are slow — don't scale down too fast
  triggers:
    - type: kafka
      metadata:
        bootstrapServers: nexus-kafka-kafka-bootstrap.nexus-data.svc.cluster.local:9092
        consumerGroup: m2-query-executors
        topic: nexus.m2.knowledge_query   # Platform-wide topic (all tenants)
        lagThreshold: "10"                # Each replica handles ~5 concurrent queries
        offsetResetPolicy: latest
```

### NetworkPolicy

```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: nexus-m2-executor-netpol
  namespace: nexus-app
spec:
  podSelector:
    matchLabels:
      app: nexus-m2-executor
  policyTypes:
    - Ingress
    - Egress
  ingress:
    - from:
        - namespaceSelector:
            matchLabels:
              name: nexus-monitoring
      ports:
        - port: 9091
  egress:
    - to:
        - namespaceSelector:
            matchLabels:
              name: nexus-data
      ports:
        - port: 5432    # PostgreSQL + TimescaleDB
        - port: 9092    # Kafka
    - to:
        - namespaceSelector:
            matchLabels:
              name: nexus-infra
      ports:
        - port: 8181    # OPA
    # LLM APIs + Pinecone + Neo4j (external SaaS)
    - to:
        - ipBlock:
            cidr: 0.0.0.0/0
            except:
              - 10.0.0.0/8
      ports:
        - port: 443
    # Block outbound to source systems — M1 exclusively handles that
```

---

## Environment Variables

| Variable | Source | Description |
|---|---|---|
| `POSTGRES_DSN` | Secrets Manager | PostgreSQL (RLS) |
| `TIMESCALE_DSN` | Secrets Manager | TimescaleDB (RLS) |
| `KAFKA_BOOTSTRAP_SERVERS` | ConfigMap | Broker addresses |
| `ANTHROPIC_API_KEY` | Secrets Manager | Primary LLM |
| `OPENAI_API_KEY` | Secrets Manager | Fallback LLM |
| `PINECONE_API_KEY` | Secrets Manager | Vector store |
| `NEO4J_URI` | Secrets Manager | Graph store URI |
| `NEO4J_PASSWORD` | Secrets Manager | Graph store password |
| `OPA_URL` | ConfigMap | `http://nexus-opa.nexus-infra.svc.cluster.local:8181` |
| `PRIMARY_LLM` | ConfigMap | Model string for primary LLM |
| `FALLBACK_LLM` | ConfigMap | Model string for fallback |
| `QUERY_TIMEOUT_SECONDS` | ConfigMap | Max processing time before dead-lettering |

---

## Observability

### Prometheus Metrics

All metrics include a `pipeline` label (`A`, `B`, or `C`) to distinguish load and latency per pipeline type. All LLM calls route through `agent_core.LLMClient` which emits these metrics automatically.

| Metric | Type | Labels |
|---|---|---|
| `m2_executor_pipeline_runs_total` | Counter | `tenant_id`, `pipeline`, `status` |
| `m2_executor_pipeline_duration_seconds` | Histogram | `tenant_id`, `pipeline` |
| `m2_executor_llm_call_duration_seconds` | Histogram | `tenant_id`, `model`, `stage`, `pipeline` |
| `m2_executor_llm_call_tokens_total` | Counter | `tenant_id`, `model`, `direction` (input/output), `stage`, `pipeline` |
| `m2_executor_store_query_duration_seconds` | Histogram | `tenant_id`, `store` (pinecone/neo4j/timescale) |
| `m2_executor_opa_violations_total` | Counter | `tenant_id`, `pipeline`, `violation_type` |
| `m2_executor_prompt_guard_blocks_total` | Counter | `tenant_id`, `pipeline`, `reason` |
| `m2_executor_schema_narratives_generated_total` | Counter | `tenant_id` |
| `m2_executor_kafka_lag` | Gauge | `consumer_group`, `topic` |

### Grafana Alerts
- LLM API error rate > 5% → PagerDuty
- OPA violation rate > 0 → **immediate PagerDuty** (potential data leak)
- P99 query latency > 20s → Slack warning
- Consumer lag on `m2.knowledge_query` > 50 for 5 minutes → PagerDuty

---

## Acceptance Tests

```bash
# ── Pipeline A ─────────────────────────────────────────────────────────────────

# Test A1 — Full query pipeline: query → Kafka → executor → WebSocket response
python tests/e2e_query_test.py \
  --tenant test-tenant-alpha \
  --query "Show me the top 5 open incidents" \
  --expect-entity-type incident \
  --expect-sources-min 1

# Test A2 — Cross-tenant isolation: alpha's query cannot return beta's entities
python tests/e2e_query_test.py --tenant test-tenant-alpha --query "List all parties"
# Inspect sources in response — none should have tenant_id = test-tenant-beta

# Test A3 — OPA blocks cross-tenant response
python tests/opa_violation_test.py --injected-tenant test-tenant-beta
# Expected: OPA returns allow=false, message sent to dead letter

# Test A4 — LLM fallback
# Temporarily disable Anthropic API key
# Expected: query completes via OpenAI fallback
# Check logs: "Primary LLM failed, switching to fallback"

# ── Pipeline B ─────────────────────────────────────────────────────────────────

# Test B1 — Schema interpretation: StructuralArtifact → CDM proposals
python tests/publish_structural_artifact.py \
  --tenant test-tenant-alpha \
  --connector odoo_partner \
  --tables "res.partner,res.company"
# Check nexus.cdm.extension_proposed topic — expect proposals with confidence scores
# Verify all proposals have tenant_id = test-tenant-alpha

# Test B2 — Pipeline B waits for Pipeline C (sequential dependency)
# Publish one StructuralArtifact to {tid}.m1.semantic_interpretation_requested
# Verify m2-schema-narrative-agents (Pipeline C) advances FIRST and publishes {tid}.m2.schema_narrative_ready
# Verify m2-structural-agents (Pipeline B) advances ONLY AFTER schema_narrative_ready is published
# Verify nexus.cdm.extension_proposed contains domain_tags from the narrative in its LLM context trace
# Verify Pipeline B message includes snapshot_id matching the original StructuralArtifact

# ── Pipeline C ─────────────────────────────────────────────────────────────────

# Test C1 — Schema narrative generated for new connector
python tests/publish_structural_artifact.py \
  --tenant test-tenant-alpha \
  --connector odoo_partner \
  --include-cardinality --include-types
# Check {tid}.m2.schema_narrative_ready topic — expect SchemaNarrative JSON
# Verify narrative has: domain_tags, overall_summary, table_summaries, quality_flags
# Verify narrative persisted to nexus_system.schema_narratives for tenant

# Test C2 — PromptGuard blocks raw values
python tests/inject_raw_values_in_artifact.py --tenant test-tenant-alpha
# Expected: PromptGuard raises PromptViolation before LLM call
# Expected: m2_executor_prompt_guard_blocks_total counter incremented
# Expected: message sent to dead letter with reason=prompt_guard_violation

# Test C3 — Narrative isolation across tenants
python tests/publish_structural_artifact.py \
  --tenant test-tenant-alpha --connector odoo_partner &
python tests/publish_structural_artifact.py \
  --tenant test-tenant-beta --connector sf_account &
wait
# Verify nexus_system.schema_narratives has separate rows for alpha and beta
# Verify {alpha}.m2.schema_narrative_ready has no beta entity references
# Verify {beta}.m2.schema_narrative_ready has no alpha entity references

# Test C4 — Upsert on re-run (same connector, new CDM version)
# Publish artifact for test-tenant-alpha with cdm_version=v2
# Expected: existing row in schema_narratives UPDATED, not duplicated
# Verify unique constraint (tenant_id, connector_id, cdm_version) holds

# ── Shared ─────────────────────────────────────────────────────────────────────

# Test S1 — Neo4j TenantScopedSession enforcement (Pipeline A)
# Call graph query without $tenant_id in Cypher
# Expected: TenantFilterMissing raised before query executes

# Test S2 — Kafka offset not committed on LLM failure
# Kill executor mid-processing; restart
# Expected: message redelivered and processed successfully (at-least-once semantics)

# Test S3 — agent_core PromptGuard unit test
python -m pytest tests/unit/test_prompt_guard.py -v
# Verify raw values, email addresses, IBAN patterns are detected and blocked
# Verify CDM field names, entity types, counts pass through cleanly
```

---

*nexus-m2-executor · Service Specification · Mentis Consulting · March 2026*
