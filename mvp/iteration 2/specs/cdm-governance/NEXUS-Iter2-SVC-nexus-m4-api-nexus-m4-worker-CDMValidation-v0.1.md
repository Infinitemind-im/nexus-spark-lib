# NEXUS — Iteration 2 · `nexus-m4-api` + `nexus-m4-worker` · CDM Validation Workflow v2
**Services:** `nexus-m4-api` · `nexus-m4-worker`
**Validate · Simulate · LLM-Assisted Recommendations**
Mentis Consulting · Version 0.1 · April 2026 · Draft

**Owner:** M4 team (primary) + Data Intelligence team (LLM integration)
**Depends on:** `nexus-m4-api` + `nexus-m4-worker` (Iteration 1), `agent_core` v1.0 (`LLMClient`, `PIIChecker`), `cdm_feedback` (CDM Mapper v2 spec)
**Related docs:** `NEXUS-Iter2-LIB-AgentCore-v0.1.md`, `NEXUS-Iter2-Frontend-v0.1.md`, `iter2-gap-analysis-v0.1.md`, `NEXUS-Iter2-SPEC-CDM-Mapper-v0.3.md`

---

## Overview

Iteration 1 shipped the M4 governance and mapping-exception queues as back-end plumbing: proposals arrive, validators approve/reject from a minimal list view. Iteration 2 turns M4 into a **validation workbench**: validators can simulate the downstream impact of a decision, request an LLM's rationale and alternatives, and compare versions — all before committing.

Three new user-facing capabilities:

1. **Simulate** — dry-run the effect of approving/rejecting a proposal on entity counts, M3 stores, and PII surface area, with zero side effects.
2. **Recommend** — ask `agent_core.LLMClient` for a plain-English rationale plus ranked alternative CDM fields, grounded in the schema snapshot and the current CDM catalogue.
3. **Decide** — approve / reject / modify, with a typed event (`{tid}.m4.validation_decision`) that the CDM Mapper and future RLHF consumers pick up (see `NEXUS-Iter2-SPEC-CDM-Mapper-v0.3.md`).

The Iteration 1 M4 API surface is preserved; this spec adds new endpoints and a new panel in the M6 React app.

---

## Functional Requirements

### Must

| ID | Requirement |
|---|---|
| VAL2-FR-01 | `POST /api/v1/m4/proposals/{id}/simulate` returns an **impact report** without persisting anything. Computes: entity count affected, fields newly surfaced in M3 embeddings, fields newly flagged as PII, downstream query templates that would start/stop matching. |
| VAL2-FR-02 | `POST /api/v1/m4/proposals/{id}/recommend` invokes `agent_core.LLMClient` with a system prompt (`PromptRegistry.get("cdm_validation_recommender")`) and the proposal + schema snapshot + top-10 CDM catalogue neighbours. Returns a `ValidationRecommendation` with rationale + ranked alternatives. |
| VAL2-FR-03 | Recommendation caching — the same `(proposal_id, classifier_version, cdm_version, llm_model)` tuple returns a cached recommendation for 24 h; cache stored in `cdm_validation_recommendations`. |
| VAL2-FR-04 | `POST /api/v1/m4/proposals/{id}/decision` accepts `{verdict, chosen_cdm_field?, verdict_reason?, llm_recommendation_id?}`. Persists the verdict, writes to `cdm_feedback` (table owned by CDM Mapper spec), and publishes `{tid}.m4.validation_decision`. |
| VAL2-FR-05 | The decision endpoint is **idempotent** on `(proposal_id, operator_id, verdict_version)` — re-posting the same body yields HTTP 200 with the existing decision payload, no duplicate events. |
| VAL2-FR-06 | The M6 frontend shows a three-panel workbench: (A) proposal + schema snapshot, (B) simulation impact card (lazy-loaded), (C) LLM recommendation with alternatives (explicit "Ask LLM" button — no automatic calls). |
| VAL2-FR-07 | PII guard — before invoking the LLM, `PIIChecker` filters any sample values from the prompt context. Prompt tokens for PII fields are replaced with `"<pii>"`. |
| VAL2-FR-08 | Audit log — every LLM call is logged to `nexus_system.llm_audit_log` with tenant, operator, model, tokens, latency, prompt hash (not prompt body). |

### Should

| ID | Requirement |
|---|---|
| VAL2-FR-09 | `GET /api/v1/m4/cdm-versions/diff?from=X&to=Y` returns a structural diff between two CDM versions (added/removed/modified entity types and fields). |
| VAL2-FR-10 | Bulk simulate — `POST /api/v1/m4/proposals/simulate-batch` for up to 100 proposals. |
| VAL2-FR-11 | Keyboard-driven workbench (J/K navigation, A to approve, R to reject, M to modify) — signals to validators that throughput matters. |

### Could

| ID | Requirement |
|---|---|
| VAL2-FR-12 | Per-validator calibration report — approval-rate deltas vs team median, flagged via Grafana if a validator drifts > 15 %. |
| VAL2-FR-13 | "Explain this rejection" — an LLM-generated summary of why a rejection might be incorrect, shown after reject but before commit. |

### Won't (Iteration 2)

| ID | Requirement |
|---|---|
| VAL2-FR-14 | Auto-approval based on LLM confidence. Every decision remains a human action in Iteration 2 (strict policy — see `NEXUS-Iteration2-Specification.md` governance principles). |
| VAL2-FR-15 | Direct editing of the CDM catalogue from the workbench. CDM version publishing remains a separate governance flow. |

---

## Non-Functional Requirements

| ID | NFR | Target |
|---|---|---|
| VAL2-NFR-01 | Simulate P95 latency | ≤ 1.5 s for a single proposal (reads M3 store counts only) |
| VAL2-NFR-02 | Recommend P95 latency | ≤ 8 s (bounded by `LLMClient` timeout — QE-NFR-07) |
| VAL2-NFR-03 | Decision endpoint P95 | ≤ 200 ms |
| VAL2-NFR-04 | Recommendation cache hit rate | ≥ 60 % at steady state |
| VAL2-NFR-05 | PII leakage | 0 — enforced by `PIIChecker` pre-filter |
| VAL2-NFR-06 | Cross-tenant isolation | Validator can only see proposals for tenants their Okta group is authorised for; enforced via OPA policy (same pattern as Query Engine) |
| VAL2-NFR-07 | LLM cost ceiling | ≤ 500 tokens input + 500 tokens output per recommendation; hard-capped in `LLMClient.complete()` call |

---

## Data Model

### Migration V2.0.13 — `cdm_validation_simulations`

```sql
CREATE TABLE nexus_system.cdm_validation_simulations (
    simulation_id        UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    proposal_id          UUID NOT NULL REFERENCES nexus_system.cdm_proposals(proposal_id),
    tenant_id            VARCHAR(100) NOT NULL,
    operator_id          VARCHAR(200) NOT NULL,
    impact_report        JSONB NOT NULL,
    simulated_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    simulator_version    VARCHAR(40) NOT NULL
);
ALTER TABLE nexus_system.cdm_validation_simulations ENABLE ROW LEVEL SECURITY;
CREATE POLICY cdm_vs_tenant_isolation ON nexus_system.cdm_validation_simulations
    USING (tenant_id = current_setting('nexus.current_tenant_id'));
CREATE INDEX cdm_vs_proposal_idx ON nexus_system.cdm_validation_simulations (proposal_id);
```

`impact_report` JSONB shape:

```json
{
  "entities_affected": 1234,
  "m3_elasticsearch_documents_new": 1234,
  "m3_elasticsearch_documents_updated": 0,
  "m3_neo4j_nodes_new": 0,
  "m3_timescaledb_rows_new": 0,
  "pii_fields_added": ["email"],
  "pii_fields_removed": [],
  "query_templates_matched_before": 3,
  "query_templates_matched_after": 5,
  "estimated_storage_delta_mb": 0.8
}
```

### Migration V2.0.14 — `cdm_validation_recommendations`

```sql
CREATE TABLE nexus_system.cdm_validation_recommendations (
    recommendation_id    UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    proposal_id          UUID NOT NULL REFERENCES nexus_system.cdm_proposals(proposal_id),
    tenant_id            VARCHAR(100) NOT NULL,
    classifier_version   VARCHAR(40) NOT NULL,
    cdm_version          VARCHAR(20) NOT NULL,
    llm_model            VARCHAR(80) NOT NULL,
    prompt_hash          CHAR(64) NOT NULL,
    rationale            TEXT NOT NULL,
    alternatives         JSONB NOT NULL,               -- ranked array of {cdm_field, confidence, why}
    tokens_in            INT NOT NULL,
    tokens_out           INT NOT NULL,
    generated_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ttl_hours            INT NOT NULL DEFAULT 24,
    UNIQUE (proposal_id, classifier_version, cdm_version, llm_model)
);
ALTER TABLE nexus_system.cdm_validation_recommendations ENABLE ROW LEVEL SECURITY;
CREATE POLICY cdm_vr_tenant_isolation ON nexus_system.cdm_validation_recommendations
    USING (tenant_id = current_setting('nexus.current_tenant_id'));
```

### Migration V2.0.15 — `llm_audit_log`

```sql
CREATE TABLE nexus_system.llm_audit_log (
    audit_id       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id      VARCHAR(100) NOT NULL,
    operator_id    VARCHAR(200),
    call_site      VARCHAR(100) NOT NULL,              -- 'cdm_validation_recommender', 'rhma_supervisor', ...
    llm_model      VARCHAR(80) NOT NULL,
    prompt_hash    CHAR(64) NOT NULL,
    tokens_in      INT NOT NULL,
    tokens_out     INT NOT NULL,
    latency_ms     INT NOT NULL,
    status         VARCHAR(20) NOT NULL,               -- 'success' | 'timeout' | 'rate_limit' | 'error'
    called_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX llm_audit_tenant_time_idx ON nexus_system.llm_audit_log (tenant_id, called_at DESC);
```

Shared by CDM Validation, RHMA, and Query Engine — owned by `agent_core.LLMClient` (adds a write-through hook).

### New Kafka topic — `{tid}.m4.validation_decision` (NEW)

| Field | Type | Notes |
|---|---|---|
| `event_id` | string | `sha256(proposal_id \| verdict_version \| operator_id)` |
| `tenant_id` | string | |
| `proposal_id` | uuid | |
| `verdict` | enum | `approve \| reject \| modify` |
| `verdict_version` | int | monotonic per proposal |
| `chosen_cdm_field` | string? | for `modify` |
| `verdict_reason` | string? | free text, capped 2000 chars |
| `operator_id` | string | Okta sub |
| `llm_assisted` | bool | |
| `llm_recommendation_id` | uuid? | |
| `decided_at` | iso8601 | |

Consumer groups: `cdm-mapper-feedback-consumer` (writes `cdm_feedback`), `m4-audit-logger`, future `rlhf-trainer` placeholder.

---

## API Contracts

### `POST /api/v1/m4/proposals/{id}/simulate`

**Request (empty body)** — the proposal id is the whole input.

**Response (200 OK)**
```json
{
  "simulation_id": "…",
  "impact_report": { "…": "see schema above" },
  "simulator_version": "v1.0.0",
  "simulated_at": "2026-04-20T12:34:56Z"
}
```

**Side effects** — one row in `cdm_validation_simulations` for audit; no writes to `cdm_proposals`, `cdm_feedback`, or any M3 store.

### `POST /api/v1/m4/proposals/{id}/recommend`

**Request (empty body)** — proposal id + caller identity drive the prompt.

**Response (200 OK)**
```json
{
  "recommendation_id": "…",
  "rationale": "The source field `Opportunity.Amount` in Salesforce represents the monetary value of a deal. The CDM `deal.amount` field has identical semantics and is a Tier 1 match based on field name and inferred datatype. …",
  "alternatives": [
    { "cdm_field": "deal.amount", "confidence": 0.95, "why": "name match + numeric type" },
    { "cdm_field": "transaction.amount", "confidence": 0.42, "why": "numeric type; weaker semantic fit — use only if Opportunity is treated as a transactional event" }
  ],
  "llm_model": "gpt-4o",
  "cached": false,
  "generated_at": "2026-04-20T12:35:12Z"
}
```

**Errors** — `503 LLM_TIMEOUT` maps to `agent_core.LLMTimeoutError`; returned as a normal response with `{ "degraded": true }` and no recommendation body. UI falls back to showing the proposal without LLM help.

### `POST /api/v1/m4/proposals/{id}/decision`

**Request**
```json
{
  "verdict": "approve",
  "chosen_cdm_field": null,
  "verdict_reason": "Confirmed by Data Lead — Amount is the deal value.",
  "llm_recommendation_id": "…"
}
```

**Response (200 OK)**
```json
{
  "verdict_version": 1,
  "event_id": "…",
  "decided_at": "2026-04-20T12:36:00Z",
  "deduped": false
}
```

**Idempotency** — same `(proposal_id, operator_id, verdict)` within 60 s returns `deduped=true`.

### `GET /api/v1/m4/cdm-versions/diff?from=2.2.0&to=2.3.0`

**Response (200 OK)**
```json
{
  "from": "2.2.0",
  "to": "2.3.0",
  "entity_types_added":    ["workflow_run"],
  "entity_types_removed":  [],
  "fields_added":          { "deal": ["closed_reason"] },
  "fields_removed":        { "contact": ["legacy_region_code"] },
  "fields_renamed":        [ { "entity": "deal", "from": "amt", "to": "amount" } ]
}
```

---

## Prompt contract — `cdm_validation_recommender`

Stored in `agent_core/prompts/cdm_validation_recommender_system.jinja2`. Must produce strict JSON matching `ValidationRecommendation` schema. Contract summary:

- System prompt states the role: "You help a human data steward decide whether to approve, reject, or modify a proposed CDM field mapping."
- User prompt injects: proposal (without sample values), schema snapshot profile (types, cardinality, null-rate — **no values**), top-10 CDM catalogue neighbours via `CDMCatalogueBuilder.get_relevant_subset()`.
- Response format: JSON object with `rationale` (string, ≤ 600 chars) and `alternatives` (array, 1–5 items).
- Temperature: 0.1 (low but not 0 — we want nuanced language, not brittle deterministic rephrasing).
- Token budget: 500 input / 500 output enforced by `LLMClient`.

---

## Frontend (M6 delta)

- New route: `/m4/proposals/:id/workbench`.
- Uses existing `nexus-query-api` WebSocket pattern for future streaming, but simulate + recommend are plain HTTP in Iteration 2.
- Components:
  - `<ProposalCard />` — shows current proposal, classifier version, confidence, PII flag.
  - `<SimulationPanel />` — lazy-loaded; calls `/simulate` on open; shows impact report as icon tiles.
  - `<RecommendationPanel />` — explicit "Ask LLM" button; shows cached recommendations with a freshness chip.
  - `<DecisionBar />` — sticky footer with Approve / Reject / Modify; optimistic UI with rollback on 5xx.

See `NEXUS-Iter2-Frontend-v0.1.md` for component patterns and the persona-override mechanism that the workbench reuses (steward persona).

---

## Edge Cases

- **LLM recommendation arrives after the validator has decided** — the client discards the response; backend still writes the audit row for cost attribution.
- **Validator rejects a proposal, then the mapper re-runs with a new classifier version** — a new `proposal_id` is generated (natural key includes `classifier_version`); the old rejection stays in `cdm_feedback` tied to the prior proposal.
- **Simulation referenced in a decision is stale (> 24 h)** — decision endpoint warns but does not block; validator can re-simulate in one click.
- **Recommendation cache invalidation** — any `nexus.cdm.version_published` event evicts recommendations for that tenant (they become semantically wrong).
- **Validator with partial group membership** — OPA denies the `simulate` call before the impact report runs; no billable work is performed.

---

## Acceptance Criteria

- Simulate returns a complete impact report in < 1.5 s for a proposal with 1 000 affected entities.
- Recommend returns a well-formed `ValidationRecommendation` in < 8 s; cold call and cached call both tested in CI.
- `{tid}.m4.validation_decision` event observable in Kafka with the documented schema; CDM Mapper consumer writes a single `cdm_feedback` row.
- Replaying the decision topic from an offset one day old produces zero duplicate `cdm_feedback` rows (natural key `(proposal_id, verdict_version)`).
- PII red-team: seed a sample schema with a field labelled PII; `/recommend` prompt body (captured via `llm_audit_log.prompt_hash` + local test double) contains no PII tokens.
- UI keyboard flow: approve → next proposal in ≤ 150 ms (no LLM call on navigation).

---

## Open Questions

- [CLARIFY: does "modify" mean the validator picks one of the LLM alternatives, or can they type a free-form CDM field (which would bypass the catalogue)? Recommendation: restrict to catalogue members to avoid drift.]
- [CLARIFY: should `llm_audit_log` retain full prompt text (for debuggability) or only a hash (for privacy)? Currently spec'd as hash-only.]
- [CLARIFY: OQ-M6-01 interaction — if the M6 team collapses the M2 chat panel, should the validation workbench live in that freed real estate or remain a separate M4 page? (See `NEXUS-Iter2-REF-ArchReview-v0.2.md` §Finding 6.)]
- [CLARIFY: bulk simulate — hard cap at 100 proposals? Or rate-limit by token budget?]
- [CLARIFY: can a validator override the LLM's "do not approve" warning? Hard block or soft warning? Policy decision.]

---

## Dependencies & Sprint Positioning

- Lands in **Gate 3 window (Week 7)** — concurrent with Query Engine; both consume `agent_core.LLMClient` so the shared library must stabilise first.
- Requires `cdm_feedback` table from `NEXUS-Iter2-SPEC-CDM-Mapper-v0.3.md` — migrations V2.0.9–V2.0.12 are joint dependency.
- Frontend work is an M6 delta; schedule as a **Phase 3** add-on if the team cannot absorb with existing Frontend-v0.1 tasks — otherwise fold into D6 scope.

*CDM Validation Workflow v2 spec v0.1 · Mentis Consulting · April 2026*
