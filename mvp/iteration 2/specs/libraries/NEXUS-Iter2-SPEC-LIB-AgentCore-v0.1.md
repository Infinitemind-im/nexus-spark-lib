# NEXUS — Iteration 2 · agent_core Library
**New Shared AI Agent Library — Initial Version**
Mentis Consulting · Version 0.1 · April 2026 · Confidential

**Service owner:** AI & Query workstream
**Consumed by:** nexus-m3-writer (EmbeddingClient), nexus-query-executor (LLMClient, PIIChecker), nexus-m2-executor (PromptRegistry, orchestration)
**Hard gate:** `LLMClient`, `EmbeddingClient`, and `PIIChecker` must be published before nexus-query-executor and nexus-m3-writer begin Phase 2 implementation.
**Target version:** 1.0.0

> **Revision v0.2 — S2 corrections**
> `PIIChecker` extended with `redact_prompt()` and `filter_values()` (§4) — three services blocked on these. `LLMClient.complete()` now documents `llm_audit_log` write-through and returns `llm_call_id` (§1). `EmbeddingClient` and `CDMCatalogueBuilder` de-coupled from Pinecone; `CDMCatalogueBuilder` now uses Elasticsearch kNN for catalogue ANN (§3, §5). `PromptRegistry` backend enum corrected (`PINECONE` → `ELASTICSEARCH`) and RHMA/M4 prompt slots added (§2). `orchestration` module added (§7) — ABCs/Protocols only, all methods raise `NotImplementedError` until post-Iter 3. `pinecone` removed from dependencies; `elasticsearch-async` added.

---

## Rationale

Both `nexus-m2-executor` (Iteration 1) and `nexus-query-executor` (Iteration 2) call LLMs. Both `nexus-m3-writer` and `nexus-query-executor` call the OpenAI embedding API. `PIIChecker` is needed by both the Elasticsearch writer (to exclude PII from embedding text) and the query executor (to enforce OPA PII policy). Without a shared library, this logic will be duplicated and diverge.

`agent_core` is the canonical AI agent infrastructure library. It is published to internal PyPI alongside `nexus_core`. It has no business logic — it is a thin, well-tested wrapper over external AI APIs and shared AI-specific data access patterns.

---

## Module Structure

```
agent_core/
├── llm/
│   ├── client.py          ← LLMClient               [NEW]
│   └── prompts.py         ← PromptRegistry           [NEW]
├── embeddings/
│   └── client.py          ← EmbeddingClient          [NEW]
├── catalogue/
│   └── builder.py         ← CDMCatalogueBuilder      [NEW]
├── security/
│   ├── pii.py             ← PIIChecker               [NEW]
│   └── cross_tenant.py    ← CrossTenantSafetyScanner [NEW]
├── orchestration/
│   ├── base.py            ← Supervisor, ExpertAgent, CriticAgent ABCs [NEW — stubs only]
│   └── experts.py         ← expert agent registry (5 slots, empty templates) [NEW — stubs only]
└── __init__.py
```

> **Iteration 2 & 3 scope for `orchestration/`:** All classes in this module deliver interface definitions only. Every method body raises `NotImplementedError`. No agent logic executes until post-Iteration 3.

---

## 1. LLMClient

**File:** `agent_core/llm/client.py`
**Used by:** Materialization Coordinator (Query Planner), `nexus-m2-executor` (existing)

Wraps OpenAI chat completions. Enforces the 8-second timeout from QE-NFR-07. Retries on transient errors. Tracks token usage for cost monitoring.

```python
@dataclass
class LLMResponse:
    content:      str
    tokens_in:    int
    tokens_out:   int
    model:        str
    latency_ms:   int
    llm_call_id:  str   # UUID — FK into llm_audit_log; callers may store this for traceability


@dataclass
class LLMClient:
    model:        str          = "gpt-4o"
    timeout_s:    float        = 8.0        # QE-NFR-07: fail query if LLM call exceeds 8s
    max_retries:  int          = 2
    redis_client: Redis        = None       # Optional: for prompt caching
    pg_pool:      asyncpg.Pool = None       # Required for llm_audit_log write-through
    tenant_id:    str          = ""         # Set per-call context; used in audit row

    async def complete(
        self,
        system_prompt:   str,
        user_prompt:     str,
        temperature:     float        = 0.0,
        response_format: dict | None  = None,
        call_context:    str          = "",  # short label logged to llm_audit_log.call_context
    ) -> LLMResponse:
        """
        Calls OpenAI chat completions with timeout enforcement.

        Raises LLMTimeoutError if the call exceeds timeout_s.
        Raises LLMRateLimitError (retryable) on 429.
        Raises LLMError (non-retryable) on 4xx other than 429.

        AUDIT WRITE-THROUGH (automatic — callers do not need to write this themselves):
        After every successful or failed call this method inserts one row into
        nexus_system.llm_audit_log:

            INSERT INTO nexus_system.llm_audit_log (
                llm_call_id, tenant_id, model, call_context,
                prompt_hash,     -- sha256 of (system_prompt + user_prompt) — not the raw text
                tokens_in, tokens_out, latency_ms, status,
                error_type,      -- null on success
                created_at
            ) VALUES (...)

        prompt_hash is stored instead of the raw prompt to avoid persisting PII.
        Callers that need to correlate a result back to its audit row use
        LLMResponse.llm_call_id.

        If pg_pool is None the audit write is skipped (test / offline mode).
        """

    async def complete_json(
        self,
        system_prompt: str,
        user_prompt:   str,
        call_context:  str = "",
    ) -> tuple[dict, LLMResponse]:
        """
        Convenience wrapper: enforces JSON mode and parses the response.
        Returns (parsed_dict, LLMResponse) — callers that need llm_call_id
        for audit cross-referencing should destructure the tuple.
        Used by the Query Planner to get structured CDMQueryPlan output.
        """
```

**Error types (importable):**

```python
class LLMTimeoutError(Exception):
    """Raised when the LLM call exceeds timeout_s. Query executor publishes 'timeout' event."""

class LLMRateLimitError(Exception):
    """Raised on 429. LLMClient retries up to max_retries before raising."""

class LLMError(Exception):
    """Non-retryable LLM error (4xx other than 429, malformed response)."""
```

---

## 2. PromptRegistry

**File:** `agent_core/llm/prompts.py`
**Used by:** Materialization Coordinator (Query Planner)

Manages prompt templates. Templates are stored as versioned files (`agent_core/prompts/*.jinja2`). Keeps prompts out of service code, making them independently reviewable and testable.

```python
class PromptRegistry:
    def get(self, name: str, version: str = "latest", **kwargs) -> str:
        """
        Renders a named prompt template with keyword arguments.
        Example: PromptRegistry().get("query_planner", cdm_subset=..., query=...)
        """

# Built-in prompts for Iteration 2:
QUERY_PLANNER_SYSTEM = "query_planner_system"   # System prompt for the Query Planner
QUERY_PLANNER_USER   = "query_planner_user"     # User prompt template (injects CDM subset + query)
```

**Query Planner prompt contract** (summarised — full template in `agent_core/prompts/query_planner_system.jinja2`):

The system prompt instructs the model to return a `CDMQueryPlan` JSON object with:
- `intent`: `"aggregation" | "trend" | "lookup" | "relationship" | "report"`
- `entity_types`: list of CDM entity types required
- `backends`: list of `BackendTarget` objects (`{ "backend": "LIVE_SOURCE|NEO4J|ELASTICSEARCH|TIMESCALEDB|HYBRID", "source_system": "..." }`)
- `output_type_suggestion`: `"text" | "table" | "bar_chart" | "line_chart" | "pie_chart" | "report"`
- `time_range`: optional `{ "from": "ISO8601", "to": "ISO8601" }` for trend queries

**Built-in prompt slots for Iteration 2:**

```python
# Query Engine
QUERY_PLANNER_SYSTEM  = "query_planner_system"   # Query Planner system prompt
QUERY_PLANNER_USER    = "query_planner_user"      # Injects CDM subset + user query

# CDM Validation (M4)
M4_RECOMMEND_SYSTEM   = "m4_recommend_system"     # System prompt for /recommend endpoint
M4_RECOMMEND_USER     = "m4_recommend_user"       # Injects proposal + source field context

# RHMA expert agents — Iteration 2: slots registered with empty templates.
# Templates are populated post-Iteration 3.
RHMA_EXPERT_FINANCE      = "rhma_expert_finance"      # Finance / P&L domain expert
RHMA_EXPERT_HR           = "rhma_expert_hr"           # HR / People domain expert
RHMA_EXPERT_CUSTOMER     = "rhma_expert_customer"     # Customer / CRM domain expert
RHMA_EXPERT_TIMESERIES   = "rhma_expert_timeseries"   # Time-series / metrics expert
RHMA_EXPERT_GRAPH        = "rhma_expert_graph"        # Graph / relationship expert
RHMA_CRITIC              = "rhma_critic"              # CriticAgent evaluation rubric
```

RHMA prompt files exist as empty `.jinja2` stubs in `agent_core/prompts/`. Calling `PromptRegistry().get("rhma_expert_finance", ...)` returns an empty string in Iteration 2 — callers must guard against this.

---

## 3. EmbeddingClient

**File:** `agent_core/embeddings/client.py`
**Used by:** M3 Elasticsearch writer (transient embedding text before upsert), `CDMCatalogueBuilder` (ANN over entity type catalogue)

Wraps OpenAI `text-embedding-3-small`. Handles batching and rate limiting. Embedding text is always treated as transient — callers must not persist it. The resulting vector is written to Elasticsearch by the caller; `EmbeddingClient` itself has no storage dependency.

```python
@dataclass
class EmbeddingClient:
    model:           str   = "text-embedding-3-small"
    dimensions:      int   = 1536   # Must match Elasticsearch index dense_vector dims — see ES-OQ-02
    max_batch_size:  int   = 100    # Matches Elasticsearch BulkIndexer flush size
    max_concurrency: int   = 5      # Max concurrent OpenAI API calls

    async def embed(self, text: str) -> list[float]:
        """Single text embedding. Returns vector of `dimensions` floats."""

    async def embed_batch(self, texts: list[str]) -> list[list[float]]:
        """
        Batch embedding. Automatically splits into max_batch_size chunks.
        Uses asyncio.Semaphore(max_concurrency) to avoid rate limit errors.
        Returns list of vectors in the same order as input texts.
        """
```

**Important:** The `EmbeddingClient` does not know whether the text is PII-safe. The Elasticsearch writer is responsible for passing pre-filtered text with PII fields excluded via `PIIChecker.filter_values()` before calling `embed()` or `embed_batch()`.

---

## 4. PIIChecker

**File:** `agent_core/security/pii.py`
**Used by:** M3 Elasticsearch writer (exclude PII from embedding text), Query Executor (OPA supplement), CDM Mapper v2 (prompt filtering), CDM Validation v2 (prompt filtering), RHMA (prompt filtering)

Determines whether a field is marked PII in the CDM schema snapshot. Reads from `nexus_system.schema_snapshots.column_profiles`. Redis-cached with 1-hour TTL per `(tenant_id, source_system)`.

```python
@dataclass
class PIIChecker:
    pg_pool:      asyncpg.Pool
    redis_client: Redis
    cache_ttl_s:  int = 3600    # 1 hour

    async def is_pii(
        self,
        tenant_id:    str,
        source_system: str,
        field_name:   str,
    ) -> bool:
        """
        Returns True if field_name is flagged pii=true in schema_snapshots.
        Cache key: "pii:{tenant_id}:{source_system}" → set of pii field names.
        """

    async def get_pii_fields(
        self,
        tenant_id:    str,
        source_system: str,
    ) -> set[str]:
        """
        Returns the full set of PII field names for a given tenant + source.
        Cached as a Redis SET. Used by the Elasticsearch writer to bulk-exclude
        PII fields before generating embeddings.
        """

    async def redact_prompt(
        self,
        tenant_id:    str,
        source_system: str,
        text:         str,
    ) -> str:
        """
        Scans `text` for PII field values belonging to (tenant_id, source_system)
        and replaces them with [REDACTED].

        Algorithm:
          1. Fetch PII field names via get_pii_fields()
          2. For each PII field name found as a substring in text, replace with [REDACTED]
          3. Returns the redacted string — never raises; returns original text on error

        Used by: LLM prompt builders in CDM Validation (/recommend), CDM Mapper
        (LLM-assisted rationale), and RHMA (expert agent prompts post-Iter 3).

        Note: This is a best-effort field-name scanner, not a deep NER pass.
        It removes field names embedded in the prompt (e.g. "email: user@x.com" →
        "email: [REDACTED]"). For stronger guarantees, callers should also call
        filter_values() on structured data before building the prompt.
        """

    async def filter_values(
        self,
        tenant_id:       str,
        source_system:   str,
        values:          dict[str, any],
    ) -> dict[str, any]:
        """
        Given a dict of {field_name: value}, returns a copy with PII field values
        replaced by None. Non-PII fields are passed through unchanged.

        Used by: Elasticsearch writer (before embed()), CDM Mapper (before
        injecting field values into classification context).

        Example:
          input:  {"email": "user@example.com", "company": "Acme", "ssn": "123-45-6789"}
          output: {"email": None, "company": "Acme", "ssn": None}
        """
```

---

## 5. CDMCatalogueBuilder

**File:** `agent_core/catalogue/builder.py`
**Used by:** Query Planner (builds LLM context before sending to `LLMClient`)

Builds the CDM entity type catalogue subset used as LLM context in the Query Planner. The full CDM catalogue can contain hundreds of entity types; injecting all of them would exceed the LLM context window. This class uses Elasticsearch kNN to select the top-k most relevant entity types for the given query, capped at 15.

Entity type embeddings are stored in a dedicated Elasticsearch index (`nexus_{tenant_slug}_cdm_catalogue`) separate from the entity data indexes. This index is populated by a nightly Airflow DAG (`cdm-version-migration`) and invalidated on `nexus.cdm.version_published`.

```python
@dataclass
class CDMCatalogueBuilder:
    pg_pool:          asyncpg.Pool
    redis_client:     Redis
    es_client:        AsyncElasticsearch   # from nexus_core.db.elasticsearch.get_es_client()
    embedding_client: EmbeddingClient
    cache_ttl_s:      int = 0   # No TTL — invalidated on nexus.cdm.version_published

    async def get_relevant_subset(
        self,
        tenant_id:   str,
        cdm_version: str,
        query_text:  str,
        top_k:       int = 15,
    ) -> list[EntityTypeDefinition]:
        """
        Returns up to top_k CDM entity type definitions most relevant to query_text.

        Algorithm:
          1. Check Redis cache: key = "cdm_catalogue:{tenant_id}:{cdm_version}"
             If hit: deserialise EntityTypeDefinition list from cache
          2. If miss: fetch all entity types from nexus_system.cdm_catalogue
             for tenant + cdm_version; cache the full list; continue
          3. Embed query_text using EmbeddingClient.embed()
          4. POST {catalogue_index}/_search with kNN block:
               {"knn": {"field": "embedding", "query_vector": [...], "k": top_k,
                        "num_candidates": top_k * 10,
                        "filter": {"term": {"cdm_version": cdm_version}}}}
          5. Return EntityTypeDefinition objects for the top_k hits, ordered by score

        Catalogue index name: nexus_{tenant_slug}_cdm_catalogue
        (follows same naming convention as entity indexes — see nexus_core §8)

        Cache invalidation: call invalidate(tenant_id, cdm_version) on
        nexus.cdm.version_published event.
        """

    async def invalidate(self, tenant_id: str, cdm_version: str) -> None:
        """Deletes the Redis cache entry. Called on CDM version publish."""
```

---

## 6. CrossTenantSafetyScanner

**File:** `agent_core/security/cross_tenant.py`
**Used by:** M3 Writers (Query Executor — Rule 7 post-synthesis check)

Post-synthesis defence-in-depth layer. Validates that every connector referenced in a merged result belongs to the session's tenant. Runs after the Result Merger, before the `result` event is published.

```python
@dataclass
class CrossTenantSafetyScanner:
    pg_pool:    asyncpg.Pool
    timeout_s:  float = 0.5     # 500ms — fail closed on timeout (Rule 7)

    async def scan(
        self,
        tenant_id:     str,
        merged_result: MergedResult,
    ) -> ScanResult:
        """
        For every connector_id referenced in merged_result.sources_queried:
          SELECT COUNT(*) FROM nexus_system.connectors
          WHERE connector_id = $1 AND tenant_id = $2

        If any connector is NOT owned by tenant_id:
          → return ScanResult(passed=False, violation_connectors=[...])
          → caller must reject as hard failure (NOT partial result)

        On timeout: return ScanResult(passed=False, timed_out=True)
        Fail closed — timeout is treated as a hard security failure.
        """

@dataclass
class ScanResult:
    passed:               bool
    violation_connectors: list[str] = field(default_factory=list)
    timed_out:            bool = False
```

---

## 7. Orchestration Module — Iteration 2 & 3 scaffold

**Files:** `agent_core/orchestration/base.py`, `agent_core/orchestration/experts.py`

> **Scope:** Iteration 2 and Iteration 3 deliver interface definitions and stub implementations only. Every method body raises `NotImplementedError`. No agent logic executes. The actual RLHF loop, Supervisor dispatch, and CriticAgent evaluation start after Iteration 3.

### `agent_core/orchestration/base.py`

```python
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any

@dataclass
class AgentContext:
    """Shared context passed to every agent in a run."""
    run_id:      str          # sha256 idempotency key — see M2 Executor Delta spec
    query_id:    str
    tenant_id:   str
    step_seq:    int = 0
    budget:      dict = field(default_factory=lambda: {"max_steps": 5, "max_tokens": 0})
    # max_tokens = 0 in Iter 2/3 — no LLM calls are made; enforced post-Iter 3


@dataclass
class AgentResult:
    """Output returned by an agent after executing its step."""
    agent_role:   str
    step_seq:     int
    status:       str         # 'stub' | 'ok' | 'error' | 'budget_exhausted'
    output:       Any = None  # None in Iter 2/3
    issues:       list[str] = field(default_factory=list)
    retry_hint:   str | None = None


class Supervisor(ABC):
    """
    Orchestrates the plan → dispatch → execute → critique → refine loop.

    Iteration 2 & 3: all methods raise NotImplementedError.
    Post-Iteration 3: dispatches ExpertAgents, collects results, calls CriticAgent,
    and manages retries up to context.budget['max_steps'].
    """

    @abstractmethod
    async def run(self, context: AgentContext, query: str) -> list[AgentResult]:
        """Entry point. Returns one AgentResult per step executed."""
        raise NotImplementedError("RHMA Supervisor not active until post-Iteration 3")

    @abstractmethod
    async def dispatch(self, context: AgentContext, role: str, input_data: Any) -> AgentResult:
        """Dispatch a single expert agent by role key."""
        raise NotImplementedError


class ExpertAgent(ABC):
    """
    Domain-specialist agent. Receives a sub-query and returns a structured answer.

    Iteration 2 & 3: execute() raises NotImplementedError.
    Each expert is identified by a role key (e.g. 'expert_finance') that maps to a
    PromptRegistry template. In Iter 2/3 the template is empty — no LLM call is made.
    """
    role: str = ""

    @abstractmethod
    async def execute(self, context: AgentContext, sub_query: str) -> AgentResult:
        raise NotImplementedError(f"Expert agent '{self.role}' not active until post-Iteration 3")


class CriticAgent(ABC):
    """
    Evaluates an ExpertAgent's output against a rubric and returns a score + issues.

    Iteration 2 & 3: evaluate() raises NotImplementedError.
    Post-Iteration 3: Supervisor calls this after each expert step; if score < threshold,
    Supervisor retries the expert with retry_hint injected into the prompt.
    """

    @abstractmethod
    async def evaluate(
        self,
        context:   AgentContext,
        result:    AgentResult,
        rubric:    str,         # PromptRegistry key for the evaluation rubric
    ) -> AgentResult:
        """Returns an AgentResult with role='critic', issues[], and retry_hint."""
        raise NotImplementedError("CriticAgent not active until post-Iteration 3")
```

### `agent_core/orchestration/experts.py`

```python
from agent_core.orchestration.base import ExpertAgent, AgentContext, AgentResult
from agent_core.llm.prompts import (
    RHMA_EXPERT_FINANCE, RHMA_EXPERT_HR, RHMA_EXPERT_CUSTOMER,
    RHMA_EXPERT_TIMESERIES, RHMA_EXPERT_GRAPH,
)

# Five expert agent stubs. Each class is registered in nexus-m2-executor's
# agent registry at startup. In Iter 2/3 execute() always raises NotImplementedError.
# Post-Iter 3: fill in the execute() body with real LLM dispatch logic.

class FinanceExpert(ExpertAgent):
    role = "expert_finance"
    # Domain: P&L, invoices, budgets, FX-normalised metrics
    # Tool allowlist (post-Iter 3): TimescaleDB, Elasticsearch
    async def execute(self, context: AgentContext, sub_query: str) -> AgentResult:
        raise NotImplementedError("FinanceExpert not active until post-Iteration 3")

class HRExpert(ExpertAgent):
    role = "expert_hr"
    # Domain: headcount, org structure, people analytics
    # Tool allowlist (post-Iter 3): Elasticsearch, Neo4j
    async def execute(self, context: AgentContext, sub_query: str) -> AgentResult:
        raise NotImplementedError("HRExpert not active until post-Iteration 3")

class CustomerExpert(ExpertAgent):
    role = "expert_customer"
    # Domain: contacts, deals, CRM pipeline, churn signals
    # Tool allowlist (post-Iter 3): Elasticsearch, Neo4j
    async def execute(self, context: AgentContext, sub_query: str) -> AgentResult:
        raise NotImplementedError("CustomerExpert not active until post-Iteration 3")

class TimeSeriesExpert(ExpertAgent):
    role = "expert_timeseries"
    # Domain: business metrics, trends, anomalies, forecasting
    # Tool allowlist (post-Iter 3): TimescaleDB
    async def execute(self, context: AgentContext, sub_query: str) -> AgentResult:
        raise NotImplementedError("TimeSeriesExpert not active until post-Iteration 3")

class GraphExpert(ExpertAgent):
    role = "expert_graph"
    # Domain: org hierarchy, entity relationships, network analysis
    # Tool allowlist (post-Iter 3): Neo4j
    async def execute(self, context: AgentContext, sub_query: str) -> AgentResult:
        raise NotImplementedError("GraphExpert not active until post-Iteration 3")


# Registry used by nexus-m2-executor to instantiate agents at startup
EXPERT_REGISTRY: dict[str, type[ExpertAgent]] = {
    "expert_finance":    FinanceExpert,
    "expert_hr":         HRExpert,
    "expert_customer":   CustomerExpert,
    "expert_timeseries": TimeSeriesExpert,
    "expert_graph":      GraphExpert,
}
```

---

## Library Dependencies

```toml
# agent_core/pyproject.toml
[tool.poetry.dependencies]
python        = "^3.12"
openai        = "^1.30"         # LLMClient + EmbeddingClient
redis         = "^5.0"          # Caching (PIIChecker, CDMCatalogueBuilder, FXService)
asyncpg       = "^0.29"         # PIIChecker, CrossTenantSafetyScanner
elasticsearch-async = "^8.x"    # CDMCatalogueBuilder kNN queries
jinja2        = "^3.1"          # PromptRegistry templates
nexus_core    = "^2.0"          # NexusMessage, CDMEntity, TenantContext
```

---

## Services That Import agent_core

| Service | Components used |
|---|---|
| `nexus-m3-writer` (Elasticsearch Writer) | `EmbeddingClient`, `PIIChecker` |
| `nexus-query-executor` (Materialization Coordinator) | `LLMClient`, `PromptRegistry`, `CDMCatalogueBuilder`, `PIIChecker` |
| `nexus-query-executor` (M3 Writers) | `CrossTenantSafetyScanner` |
| `nexus-m2-executor` (existing) | `LLMClient` (replace hand-rolled OpenAI calls) |

---

## Acceptance Criteria

- `from agent_core.llm.client import LLMClient` — 8s timeout enforced; `LLMTimeoutError` raised on timeout; retry on 429
- `from agent_core.embeddings.client import EmbeddingClient` — batch of 100 texts embedded in one OpenAI call; rate-limited to 5 concurrent calls
- `from agent_core.security.pii import PIIChecker` — correct PII field detection; Redis cache populated on first call; TTL 1 hour
- `from agent_core.catalogue.builder import CDMCatalogueBuilder` — returns ≤ 15 entity types; cache invalidated on version publish event
- `from agent_core.security.cross_tenant import CrossTenantSafetyScanner` — cross-tenant connector detected as violation; 500ms timeout triggers fail-closed
- Unit tests cover all error paths (timeout, rate limit, cache miss, cross-tenant violation)
- Published to internal PyPI at version 1.0.0
