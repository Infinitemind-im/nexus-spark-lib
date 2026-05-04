# NEXUS — Iteration 2 · `nexus-m2-executor` · RHMA v2 (Reflexive Hierarchical Multi-Agent)
**Service:** `nexus-m2-executor`
**Multi-Agent Idempotency · Expert Richness · Reflexive Critic Layer**
Mentis Consulting · Version 0.1 · April 2026 · Draft

**Owner:** AI & Knowledge team (M2)
**Depends on:** `nexus-m2-executor` (Iteration 1, unchanged surface), `agent_core` v1.0 (`LLMClient`, `PromptRegistry`, `PIIChecker`, `CDMCatalogueBuilder`), `nexus_core` v2 (NexusMessage envelope), `cdm_feedback` (CDM Mapper v2 spec)
**Related docs:** `NEXUS-Iter2-LIB-AgentCore-v0.1.md`, `NEXUS-Iter2-REF-ArchReview-v0.2.md` (Finding 6 — RHMA framing), `NEXUS-Iter2-M6-FrontendDelta-v0.2.md` (line 770 — RHMA deprecation as user-facing entrypoint), `iter2-gap-analysis-v0.1.md`

---

## Overview

RHMA — Reflexive Hierarchical Multi-Agent — is M2's agent architecture for the **internal knowledge retrieval** pipeline (semantic interpretation, CDM extension proposals, workflow triggers). After Iteration 2 it stops being a user-facing chat (the new `nexus-query-api` takes that surface area) and focuses on being the *reflexive knowledge engine* that supports CDM governance and cross-module reasoning.

Iteration 2 upgrades RHMA on three axes:

1. **Idempotency** — re-dispatching a `knowledge_query` or `semantic_interpretation_requested` event does not re-run the full agent graph; deterministic `run_id` dedup at every agent step.
2. **Expert richness** — the single generic "structural agent" is decomposed into five domain experts (Finance, HR/People, Customer/CRM, Time-Series, Graph/Relationships) with dedicated prompts, tool allowlists, and rubrics.
3. **Reflexive critic** — a `CriticAgent` evaluates each expert answer against a rubric and can trigger one retry with a correction hint before the `Supervisor` returns the final answer.

This is a delta on the Iteration 1 M2 executor: Iteration 2 adds an `agent_core.orchestration` module and new agent-telemetry tables but keeps the external Kafka topics (`{tid}.m2.knowledge_query`, `{tid}.m2.agent_response_ready`, etc.) unchanged.

---

## Functional Requirements

### Must

| ID | Requirement |
|---|---|
| RHMA2-FR-01 | `agent_core.orchestration` module ships with `Supervisor`, `ExpertAgent`, `CriticAgent` abstractions. The `Supervisor` runs a bounded loop: `plan → dispatch → execute → critique → (retry or finalise)`. Hard cap: 6 agent steps per query. |
| RHMA2-FR-02 | Every agent invocation has a deterministic `run_id = sha256(query_id \| role \| step_id \| input_digest)`. The `agent_runs` table is keyed by `run_id` — re-running a step with identical inputs is a no-op (returns the cached output). |
| RHMA2-FR-03 | Five expert agents ship in Iteration 2: `finance_expert`, `people_expert`, `customer_expert`, `timeseries_expert`, `graph_expert`. Each has a prompt template in `PromptRegistry` and a JSON-declared tool allowlist. |
| RHMA2-FR-04 | Each expert answer is passed to `CriticAgent.evaluate(answer, rubric) → {score, issues[], retry_hint?}`. If `score < 0.7`, the Supervisor retries the expert once with the `retry_hint` injected into the prompt. |
| RHMA2-FR-05 | `nexus_system.agent_runs` persists every step (role, input digest, output digest, tokens, latency, status). Writes are at-least-once with natural-key dedup on `run_id`. |
| RHMA2-FR-06 | Per-query budget enforcement — `Supervisor` tracks cumulative `tokens_in + tokens_out` across all agents and aborts with `status='budget_exhausted'` when the tenant's configured ceiling is exceeded. Ceiling lives in `tenant_configs.rhma_max_tokens_per_query` (default 20 000). |
| RHMA2-FR-07 | `GET /api/v1/m2/query/{id}/trace` returns the full agent run graph for an executed query — step list with role, input/output hashes, verdict from critic, retry count. Used by M6 and by debugging tools. |
| RHMA2-FR-08 | `POST /api/v1/m2/query/replay` re-executes a completed query without re-charging LLM calls where `run_id` dedup matches — used by QA and by regression testing. |
| RHMA2-FR-09 | Every LLM call inside RHMA goes through `agent_core.LLMClient` so audit + cost telemetry flow into `llm_audit_log` (shared with CDM Validation and Query Engine). |
| RHMA2-FR-10 | Cross-tenant safety — `CrossTenantSafetyScanner` runs on the Supervisor's final answer before it is published to `{tid}.m2.agent_response_ready` (same policy as the Query Engine — `NEXUS-Iter2-LIB-AgentCore-v0.1.md` §6). |

### Should

| ID | Requirement |
|---|---|
| RHMA2-FR-11 | The Supervisor reads `cdm_feedback` (from CDM Mapper v2 spec) as additional grounding for semantic interpretation — approved mappings become preferred interpretations when tied. |
| RHMA2-FR-12 | Expert allowlists are declared in YAML (`agent_core/experts/*.yaml`) so Ops can promote/demote tool access without code changes. |
| RHMA2-FR-13 | A `consensus` mode — when two experts are relevant, dispatch both in parallel and combine via a dedicated `SynthesiserAgent`. Applies only to cross-domain queries (e.g. "revenue per employee"). |

### Could

| ID | Requirement |
|---|---|
| RHMA2-FR-14 | M6 "Explain this answer" trace viewer (reuses the `GET /trace` endpoint). |
| RHMA2-FR-15 | Cost-per-query Grafana panel broken down by role. |
| RHMA2-FR-16 | Self-consistency sampling — ask the expert twice at temperature 0.3 and pick majority if answers diverge. |

### Won't (Iteration 2)

| ID | Requirement |
|---|---|
| RHMA2-FR-17 | Dynamic agent creation at runtime (no prompt synthesis, no agent spawning from LLM output). |
| RHMA2-FR-18 | Prompt fine-tuning from feedback — we capture `cdm_feedback`, but no training loop ships this iteration. |
| RHMA2-FR-19 | Reopening RHMA as a user-facing chat (M6 routes users to `nexus-query-api`; M4 workbench uses the CDM Validation LLM). |

---

## Non-Functional Requirements

| ID | NFR | Target |
|---|---|---|
| RHMA2-NFR-01 | End-to-end query latency | P95 ≤ 12 s (6 steps × 2 s each max; supervisor overhead negligible) |
| RHMA2-NFR-02 | Replay token savings | ≥ 85 % of re-run tokens avoided when re-running an identical query within 24 h |
| RHMA2-NFR-03 | Critic agreement with human | ≥ 80 % agreement on a seed rubric set (measured via post-hoc human review in the ground-truth DAG) |
| RHMA2-NFR-04 | Per-query token budget | Enforced in `Supervisor`; hard abort at 2× configured ceiling |
| RHMA2-NFR-05 | Idempotency | Replaying a 24 h Kafka slice produces zero new `agent_runs` rows (all deduped) |
| RHMA2-NFR-06 | PII in prompts | 0 — `PIIChecker` filters inputs before any expert prompt |
| RHMA2-NFR-07 | Supervisor decisions logged | 100 % — every plan/critique/retry/finalise decision is persisted |

---

## Data Model

### Migration V2.0.16 — `agent_runs`

```sql
CREATE TABLE nexus_system.agent_runs (
    run_id           CHAR(64) PRIMARY KEY,               -- sha256 digest, natural idempotency key
    query_id         UUID NOT NULL,
    tenant_id        VARCHAR(100) NOT NULL,
    parent_run_id    CHAR(64) REFERENCES nexus_system.agent_runs(run_id),
    step_id          INT NOT NULL,
    role             VARCHAR(40) NOT NULL,               -- 'supervisor' | 'finance_expert' | ...
    input_digest     CHAR(64) NOT NULL,
    input_tokens     INT NOT NULL DEFAULT 0,
    output_digest    CHAR(64),
    output_tokens    INT NOT NULL DEFAULT 0,
    critic_score     NUMERIC(3,2),                       -- only populated on expert rows, NULL for supervisor/critic
    status           VARCHAR(20) NOT NULL,               -- 'pending' | 'success' | 'retried' | 'budget_exhausted' | 'error'
    error_message    TEXT,
    latency_ms       INT,
    started_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at      TIMESTAMPTZ
);
ALTER TABLE nexus_system.agent_runs ENABLE ROW LEVEL SECURITY;
CREATE POLICY agent_runs_tenant_isolation ON nexus_system.agent_runs
    USING (tenant_id = current_setting('nexus.current_tenant_id'));
CREATE INDEX agent_runs_query_idx ON nexus_system.agent_runs (query_id, step_id);
CREATE INDEX agent_runs_tenant_time_idx ON nexus_system.agent_runs (tenant_id, started_at DESC);
```

### Migration V2.0.17 — `rhma_max_tokens_per_query` in `tenant_configs`

```sql
ALTER TABLE nexus_system.tenant_configs
    ADD COLUMN rhma_max_tokens_per_query INT NOT NULL DEFAULT 20000;
```

### New Kafka topic — `{tid}.m2.agent_step_completed` (NEW, internal)

Fires after every agent step. Consumed by `rhma-telemetry` consumer group that writes `platform_metrics` rows for Grafana.

| Field | Type | Notes |
|---|---|---|
| `run_id` | string | natural key |
| `query_id` | uuid | |
| `role` | string | |
| `step_id` | int | |
| `status` | enum | |
| `tokens_in`, `tokens_out` | int | |
| `latency_ms` | int | |
| `critic_score` | float? | |

---

## orchestration module (agent_core addition)

```
agent_core/
└── orchestration/
    ├── supervisor.py         ← Supervisor
    ├── expert.py             ← ExpertAgent base + registry loader
    ├── critic.py             ← CriticAgent
    ├── rubrics.py            ← Rubric definitions per role
    └── experts/              ← YAML tool allowlists + prompt refs
        ├── finance_expert.yaml
        ├── people_expert.yaml
        ├── customer_expert.yaml
        ├── timeseries_expert.yaml
        └── graph_expert.yaml
```

### `Supervisor.run()`

```python
@dataclass
class Supervisor:
    llm:          LLMClient
    prompts:      PromptRegistry
    experts:      dict[str, ExpertAgent]
    critic:       CriticAgent
    pg_pool:      asyncpg.Pool
    max_steps:    int = 6
    max_tokens:   int = 20_000
    pii_checker:  PIIChecker

    async def run(self, query: KnowledgeQuery) -> AgentResponse:
        """
        1. Derive query_id and a deterministic plan_digest.
        2. Call plan() — produces a list of (role, input) pairs.
        3. For each pair:
             - run_id = sha256(query_id | role | step_id | input_digest)
             - if agent_runs(run_id) exists and status='success' → return cached output
             - else dispatch to expert, record run, then CriticAgent.evaluate
             - if score < 0.7 and retries < 1: retry with hint
        4. Synthesise final answer via experts[*].contribute(...)
        5. CrossTenantSafetyScanner.scan(final_answer)
        6. Return AgentResponse with step-trace references.
        Any step failing mid-flight leaves a pending row; the next replay finds it and resumes.
        """
```

### `ExpertAgent.handle()`

```python
@dataclass
class ExpertAgent:
    role:          str                            # 'finance_expert' etc.
    system_prompt_key: str                        # key in PromptRegistry
    tool_allowlist: list[str]
    llm:           LLMClient

    async def handle(self, task: ExpertTask) -> ExpertAnswer:
        """
        Runs the expert's system prompt against task.question + task.context.
        Tools are invoked only if they appear in tool_allowlist.
        Returns ExpertAnswer { text, citations[], tool_calls[], tokens_used }.
        """
```

### `CriticAgent.evaluate()`

```python
@dataclass
class CriticAgent:
    llm:        LLMClient
    rubrics:    dict[str, Rubric]                 # one per expert role

    async def evaluate(self, role: str, answer: ExpertAnswer) -> CriticVerdict:
        """
        Uses the rubric for `role` as a JSON-mode prompt. Returns:
          CriticVerdict {
            score:       float (0..1),
            issues:      list[str],
            retry_hint:  str | None,
          }
        Score ≥ 0.7 → accept. Score < 0.7 and retry_hint present → Supervisor retries once.
        """
```

---

## Expert roster (Iteration 2)

| Expert | Domain | Primary CDM entity types | Tools (allowlist) | Rubric focus |
|---|---|---|---|---|
| `finance_expert` | Revenue, deals, invoices, forecasts | `deal`, `transaction`, `invoice`, `account` | `cdm_catalogue_lookup`, `ts_metric_lookup`, `fx_convert` | Numeric accuracy, unit/currency sanity, time-alignment |
| `people_expert` | Employees, org chart, HR | `employee`, `department`, `role_assignment` | `cdm_catalogue_lookup`, `graph_traverse` | Hierarchy consistency, tenure math |
| `customer_expert` | CRM, segments, accounts | `contact`, `account`, `lead`, `opportunity` | `cdm_catalogue_lookup`, `vector_search` | Entity resolution, segment reasoning |
| `timeseries_expert` | KPIs, trends, seasonality | `business_metric` | `ts_metric_lookup`, `ts_compare_periods` | Time-bucket correctness, trend interpretation |
| `graph_expert` | Relationships, reporting chains | any | `graph_traverse`, `graph_shortest_path` | Path validity, cycle detection |

Each expert is tested against a small canned rubric set shipped with the spec; acceptance depends on ≥ 80 % passing.

---

## API Contracts

### `GET /api/v1/m2/query/{id}/trace`

**Response (200 OK)**
```json
{
  "query_id": "…",
  "total_tokens_in": 3120,
  "total_tokens_out": 842,
  "total_latency_ms": 7420,
  "steps": [
    { "run_id": "a0…", "step_id": 0, "role": "supervisor", "status": "success", "input_digest": "…", "output_digest": "…" },
    { "run_id": "b1…", "step_id": 1, "role": "finance_expert", "status": "retried", "critic_score": 0.62, "latency_ms": 1820 },
    { "run_id": "b2…", "step_id": 2, "role": "finance_expert", "status": "success", "critic_score": 0.88, "latency_ms": 1610 },
    { "run_id": "c3…", "step_id": 3, "role": "critic", "status": "success", "latency_ms": 420 }
  ]
}
```

### `POST /api/v1/m2/query/replay`

**Request**
```json
{ "query_id": "<existing>", "force_rerun_roles": [] }
```

**Response (200 OK)** — same shape as `/trace` after re-execution. By default, steps with matching `run_id` are served from `agent_runs` cache; `force_rerun_roles` opt-outs specific roles from the cache.

---

## Edge Cases

- **Supervisor crashes mid-plan** — on replay, each step's `run_id` is deterministic; steps that completed are served from cache; pending/failed steps re-run.
- **Expert timeout** — propagates `LLMTimeoutError` from `LLMClient`; Supervisor marks step `error`, decides whether to retry based on the critic's would-be hint (not applicable here — no answer to critique). Marks query `partial` if other experts succeeded.
- **Critic score exactly 0.70** — accepts (threshold is strict `<`).
- **Retry produces identical answer (same `output_digest`)** — Supervisor accepts but flags `stuck=true` in the final answer for debuggability.
- **Budget exhausted after expert but before critic** — Supervisor short-circuits: returns expert answer with `budget_exhausted=true`; no critic call charged.
- **Tenant config ceiling changed mid-query** — Supervisor uses the ceiling fetched at query start; changes apply to new queries only (same policy as CDM mapper thresholds — see OQ-TENANT-01).
- **Cross-tenant scanner finds a foreign connector reference in the final answer** — Rule 7 fail-closed: query rejected as hard failure, answer dropped; Grafana alert fires.

---

## Acceptance Criteria

- Running the same `knowledge_query` twice within 24 h issues zero new OpenAI calls on the second run (observed via `llm_audit_log` deltas). `agent_runs` row count unchanged.
- Five experts measurably improve over single-agent baseline on a 20-query acceptance set (≥ +10 points on the canned rubric average).
- CriticAgent triggers retry when given a deliberately weak expert answer in a test fixture; final answer score > 0.7.
- `/trace` returns a complete step DAG for a real query; M6 can render it without additional backend calls.
- Budget test: synthetic query with 30 k token ceiling aborts at step 4 with `status='budget_exhausted'`; partial trace still returned.
- Replay test: `POST /replay` on a completed query returns identical final answer (character-for-character) unless experts include non-determinism (which the rubric suppresses).
- Cross-tenant test: seed a synthetic answer with a foreign connector reference; Supervisor drops the answer, publishes `security_violation` event, no leakage.

---

## Open Questions

- [CLARIFY: consensus mode — which cross-domain queries trigger it? Hand-curated rule set, or a router LLM?]
- [CLARIFY: should the experts share a conversational memory or be stateless per step? Stateless is simpler and matches the idempotency goal; shared memory is richer but harder to dedup.]
- [CLARIFY: rubric maintenance — who owns the rubrics long-term? Recommend AI & Knowledge team with Data Lead sign-off quarterly.]
- [CLARIFY: does the Query Engine's `nexus-query-executor` get access to the same experts, or are they M2-only for Iteration 2? Expert richness is more valuable to the user-facing query path — but adding the dependency now expands scope.]
- [CLARIFY: RHMA token budget default (20 000) — data-driven or guessed? Recommend: capture real usage for two weeks post-launch and revisit.]
- [CLARIFY: `force_rerun_roles` on `/replay` — is this an operator-only endpoint (OPA-gated) or available to every tenant user? Recommend operator-only.]

---

## Dependencies & Sprint Positioning

- Lands in **Gate 3 window (Week 7)**, concurrent with Query Engine and CDM Validation workbench — all three consume a stable `agent_core.LLMClient` + `orchestration`.
- V2.0.16–V2.0.17 migrations fold into the platform bootstrap task (D1-02) when that task is extended for the Iteration 2 delta.
- Five new experts are the biggest team cost; see gap analysis open question about potentially phasing them (land Supervisor/Critic/1–2 experts in Iteration 2, the remaining experts in Iteration 3).
- `agent_core.orchestration` is a net-new module — add to `NEXUS-Iter2-LIB-AgentCore-v0.1.md` as §7, or ship as a v1.1 minor.
- Depends on `cdm_feedback` table (from `NEXUS-Iter2-SPEC-CDM-Mapper-v0.3.md`) for RHMA2-FR-11 (should-have); not blocking for Must requirements.

*RHMA v2 spec v0.1 · Mentis Consulting · April 2026*
