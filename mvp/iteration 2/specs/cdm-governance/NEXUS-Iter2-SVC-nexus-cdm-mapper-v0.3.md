# NEXUS — Iteration 2 · `nexus-cdm-mapper` · CDM Mapper v2
**Service:** `nexus-cdm-mapper`
**Idempotency · Ground-Truth Validation · RLHF Placeholder**
Mentis Consulting · Version 0.3 · April 2026 · Draft

> **Routing extension — see `NEXUS-Iter2-SPEC-CDMFieldRouting-v0.1.md` (added 2026-04-29):** `classify_field()` in this spec produces `semantic_class`, `pii`, and `table_role` on every `cdm_proposals` row, but does **not** derive where the field routes in the AI stores. The companion spec `NEXUS-Iter2-SPEC-CDMFieldRouting-v0.1.md` (in `architecture/`) extends this service: four new columns — `db_target_suggestion`, `es_role_suggestion`, `ts_role_suggestion`, `neo4j_role_suggestion` — are added to `cdm_proposals` (migration V2.0.20), and `classify_field()` is extended with a new `nexus_cdm_mapper/routing.py` module that auto-derives these values from the same classification inputs already computed here.
>
> **Naming clarification (added 2026-04-27):** "**Tier 1 / Tier 2 / Tier 3**" in this spec refers exclusively to **CDM mapping confidence** (Tier 1 ≥ 0.95 auto-applied, Tier 2 0.70–0.95 review-queued, Tier 3 < 0.70 stored as `source_extras`). It is **not** the same concept as "**materialization level**" (hot / warm / cold) introduced by the CDM-to-AIStores pipeline series. Materialization governs *which stores* hold a record's projection; mapping tier governs *whether a field has a CDM mapping*. The two are orthogonal — a Tier 1 field on a warm-level entity is correctly mapped but not materialised in M3 stores; a Tier 2 field on a hot-level entity is materialised under Virtual CDM (only the reference is written, not the value, so the review status is irrelevant for the projection itself). See `NEXUS-Iter2-MaterializationPolicy-v0.1.md` and the README naming-conventions section.
>
> **Revision v0.3 — Spark transformation stage upstream**
> CDM Mapper now consumes `m1.int.transformed_records` instead of `m1.int.raw_records`. The upstream `nexus-spark-transformer` service handles type coercion, FX normalisation, data quality checks, deduplication, entity resolution (Golden Record ID assignment), and schema profiling before the mapper receives the record. The mapper's responsibility is **semantic classification only** — it classifies field-to-CDM mappings on already-typed, already-entity-resolved data. The Debezium snapshot `op=READ` note is unchanged: the mapper still treats READ identically to INSERT, regardless of the source of the record.
>
> **Revision v0.2 — data flow spec alignment**
> OQ-CDC-01 resolved: Debezium snapshot `op=READ` treated as `CREATE`.

**Owner:** Data Intelligence team (primary) + Data Lead (ground-truth curation)
**Depends on:** `nexus_core` v2 (D1-01), `tenant_configs` (D1-02), `cdm_proposals` (V2.0.4)
**Related docs:** `NEXUS-Iter2-SprintPlan-v0.3.md` (D1-09), `NEXUS-Iter2-LIB-AgentCore-v0.1.md`, `NEXUS-Iter2-DataFlow-v0.1.md`

---

## 📦 Codebase & Deployment

> **One repository, many deployments.** All NEXUS services live in the `nexus-platform` monorepo. You are contributing improvements to an **existing** service — not building a new one. The Iteration 2 work on this service is a retrofit (idempotency, ground-truth harness, RLHF placeholder); the core mapping logic was built in Iteration 1.

| | |
|---|---|
| **Deployed as** | `nexus-cdm-mapper` (existing service, Iteration 2 retrofit) |
| **Monorepo path** | `services/nexus-cdm-mapper/` |
| **Language / runtime** | Python 3.11 · asyncio |
| **Shared imports** | `from nexus_core.cdm import CdmEntity` · `from agent_core.llm import LLMClient` · `from agent_core.embedding import EmbeddingClient` |
| **Iteration 2 scope** | Idempotency enforcement · ground-truth validation harness · RLHF feedback placeholder. **No breaking changes** to the mapping API or `cdm_proposals` schema. |
| **Pipeline position** | Iteration 1 precondition — CDM Mapper runs schema-level classification **before** any Iteration 2 pipeline records are processed. It does not participate in the hot/warm/cold materialization path. |

---

## Overview

Iteration 2 retrofits three capabilities into the existing `nexus-cdm-mapper` service without rewriting its classification core:

1. **Idempotency** — deterministic re-runs of classification produce the same `cdm_proposals` row (upsert, not insert) and emit an idempotent `classification_produced` event.
2. **Ground-truth validation** — a frozen labelled dataset (per tenant + per CDM version) drives CI regression tests and a `/diff` endpoint, so threshold changes (e.g. D1-09) cannot silently regress quality.
3. **RLHF placeholder** — every M4 validator verdict is persisted in `cdm_feedback` and emitted as a typed Kafka event so a future training loop (Iteration 3 or later) has clean, structured preference data to consume.

This spec is a delta on Iteration 1's `NEXUS-SVC-cdm-mapper.md`. The Iteration 1 service continues to own `classify_field()`, `{tid}.m1.semantic_interpretation_requested`, and the Redis classification cache — nothing in that surface area changes.

**Input record (as of v0.3):** The mapper now consumes `m1.int.transformed_records`, not `m1.int.raw_records`. Records arriving on this topic have already been through `nexus-spark-transformer` and carry:
- Normalised field types (dates as ISO 8601, numerics with consistent precision)
- Currency-normalised monetary fields with `original_currency` and `fx_rate` in metadata
- A `cdm_entity_id` (Golden Record ID) pre-assigned by the Spark entity resolution stage
- Quality flags (`null_rate`, `format_valid`) per field
- Updated schema profile statistics

The mapper's job is therefore purely semantic: given a clean, typed record with a resolved entity ID, classify each field against the CDM catalogue. It does not perform type inference, FX conversion, or entity resolution.

**Debezium snapshot handling (OQ-CDC-01 resolved):** The mapper treats `op=READ` identically to `op=INSERT` — `classify_field()` runs on all fields regardless of operation code. This is unchanged from v0.2; the Spark stage upstream does not alter `op` semantics.

---

## Functional Requirements

### Must

| ID | Requirement |
|---|---|
| CDMM2-FR-01 | `classify_field()` is idempotent per `(tenant_id, source_system, source_table, source_field, cdm_version, classifier_version)`. Re-running with identical inputs MUST produce an identical `cdm_proposals` row — same `proposal_id`, same content — via UPSERT on the natural key. |
| CDMM2-FR-02 | The mapper publishes `{tid}.m1.classification_produced` after every classification. The event `event_id` MUST be a deterministic `sha256(tenant_id \| source_system \| source_table \| source_field \| cdm_version \| classifier_version \| produced_at_day)`. Re-publishing is safe. |
| CDMM2-FR-03 | `nexus_system.cdm_ground_truth` stores reference labels. A nightly Airflow DAG (`cdm_ground_truth_regression`) runs the mapper against all ground-truth rows and writes a run report to `cdm_ground_truth_runs`. |
| CDMM2-FR-04 | `GET /api/v1/cdm/mapper/ground-truth/diff?classifier_version=X&baseline_version=Y` returns precision, recall, and per-tier confusion matrix for the two classifier versions against the current frozen ground truth. |
| CDMM2-FR-05 | `nexus_system.cdm_feedback` records every M4 validator verdict (approve/reject/modify). The mapper consumes `{tid}.m4.validation_decision` (published by M4 — see CDM Validation spec) and upserts feedback rows keyed by `proposal_id + verdict_version`. |
| CDMM2-FR-06 | Feedback is *observable only* in Iteration 2 — the mapper does not retrain, does not rerank, does not bias classification. It only persists and exposes the data. |

### Should

| ID | Requirement |
|---|---|
| CDMM2-FR-07 | CLI tool `nexus-cdmm-replay --tenant <id> --since <date>` re-runs classification against a historic schema snapshot and diffs against the prior run. |
| CDMM2-FR-08 | A nightly metric `cdm_mapper.precision.tier1` is written to TimescaleDB (`platform_metrics`) so Grafana can alert on regressions > 2 percentage points. |

### Could

| ID | Requirement |
|---|---|
| CDMM2-FR-09 | `POST /api/v1/cdm/mapper/classify` synchronous endpoint for ad-hoc classification from the validation workbench. Returns the same proposal object, deterministic with the same input. |

### Won't (Iteration 2)

| ID | Requirement |
|---|---|
| CDMM2-FR-10 | Online model fine-tuning, reward model training, any RL training loop. Explicitly deferred. |

---

## Non-Functional Requirements

| ID | NFR | Target |
|---|---|---|
| CDMM2-NFR-01 | Classification latency unchanged | P95 ≤ Iteration 1 baseline + 50 ms (upsert overhead only) |
| CDMM2-NFR-02 | Idempotency verified | Two identical classify calls within 60 s produce zero duplicate rows and zero duplicate events |
| CDMM2-NFR-03 | Ground-truth DAG runtime | ≤ 15 min for 5 k ground-truth rows across all tenants |
| CDMM2-NFR-04 | Feedback write | ≤ 100 ms P95 per verdict, from topic consume to row commit |
| CDMM2-NFR-05 | Re-run safety (RPO) | Replaying any 24 h Kafka slice MUST leave `cdm_proposals` and `cdm_feedback` bit-identical |
| CDMM2-NFR-06 | PII in prompts | 0 — enforced via `agent_core.PIIChecker` before any LLM call |

---

## Data Model

### Migration V2.0.9 — add natural key + classifier version to `cdm_proposals`

```sql
-- V2.0.9
ALTER TABLE nexus_system.cdm_proposals
    ADD COLUMN classifier_version VARCHAR(40) NOT NULL DEFAULT 'v1.0.0',
    ADD COLUMN input_digest       CHAR(64)    NOT NULL,                          -- sha256 of the inputs
    ADD COLUMN produced_at         TIMESTAMPTZ NOT NULL DEFAULT NOW();

ALTER TABLE nexus_system.cdm_proposals
    ADD CONSTRAINT cdm_proposals_natural_key
    UNIQUE (tenant_id, source_system, source_table, source_field, cdm_version, classifier_version);

CREATE INDEX cdm_proposals_input_digest_idx
    ON nexus_system.cdm_proposals (input_digest);
```

On UPSERT: if the natural key matches and `input_digest` is unchanged → no-op (idempotent). If `input_digest` differs → update in place and bump `produced_at`.

### Migration V2.0.10 — `cdm_ground_truth`

```sql
CREATE TABLE nexus_system.cdm_ground_truth (
    gt_id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id           VARCHAR(100) NOT NULL,
    source_system       VARCHAR(100) NOT NULL,
    source_table        VARCHAR(200) NOT NULL,
    source_field        VARCHAR(200) NOT NULL,
    expected_cdm_field  VARCHAR(200),                 -- NULL = expected Tier 3 (unmapped)
    expected_tier       SMALLINT NOT NULL CHECK (expected_tier IN (1,2,3)),
    cdm_version         VARCHAR(20) NOT NULL,
    curator_id          VARCHAR(200) NOT NULL,
    curator_notes       TEXT,
    frozen_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, source_system, source_table, source_field, cdm_version)
);
ALTER TABLE nexus_system.cdm_ground_truth ENABLE ROW LEVEL SECURITY;
CREATE POLICY cdm_gt_tenant_isolation ON nexus_system.cdm_ground_truth
    USING (tenant_id = current_setting('nexus.current_tenant_id'));
```

### Migration V2.0.11 — `cdm_ground_truth_runs`

```sql
CREATE TABLE nexus_system.cdm_ground_truth_runs (
    run_id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    classifier_version   VARCHAR(40) NOT NULL,
    cdm_version          VARCHAR(20) NOT NULL,
    total_rows           INT NOT NULL,
    correct_rows         INT NOT NULL,
    tier1_precision      NUMERIC(5,4),
    tier1_recall         NUMERIC(5,4),
    tier2_precision      NUMERIC(5,4),
    tier2_recall         NUMERIC(5,4),
    per_tenant_report    JSONB NOT NULL,
    started_at           TIMESTAMPTZ NOT NULL,
    finished_at          TIMESTAMPTZ NOT NULL
);
```

### Migration V2.0.12 — `cdm_feedback`

```sql
CREATE TABLE nexus_system.cdm_feedback (
    feedback_id        UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    proposal_id        UUID NOT NULL REFERENCES nexus_system.cdm_proposals(proposal_id),
    tenant_id          VARCHAR(100) NOT NULL,
    verdict            VARCHAR(20) NOT NULL CHECK (verdict IN ('approve','reject','modify')),
    verdict_version    INT NOT NULL,                         -- monotonic per proposal
    chosen_cdm_field   VARCHAR(200),                         -- for 'modify'
    verdict_reason     TEXT,                                 -- free-text from validator or LLM-summarised
    operator_id        VARCHAR(200) NOT NULL,
    llm_assisted       BOOLEAN NOT NULL DEFAULT FALSE,       -- TRUE if validator accepted an LLM recommendation
    llm_recommendation_id UUID,                              -- FK to cdm_validation_recommendations (nullable)
    created_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (proposal_id, verdict_version)
);
ALTER TABLE nexus_system.cdm_feedback ENABLE ROW LEVEL SECURITY;
CREATE POLICY cdm_feedback_tenant_isolation ON nexus_system.cdm_feedback
    USING (tenant_id = current_setting('nexus.current_tenant_id'));
CREATE INDEX cdm_feedback_proposal_idx ON nexus_system.cdm_feedback (proposal_id);
```

---

## API Contracts

### `POST /api/v1/cdm/mapper/classify`

Idempotent ad-hoc classification, usable by the validation workbench.

**Request**
```json
{
  "tenant_id": "acme-corp",
  "source_system": "salesforce",
  "source_table": "Opportunity",
  "source_field": "Amount",
  "cdm_version": "2.3.0"
}
```

**Response (200 OK)**
```json
{
  "proposal_id": "6c1e0b8e-…",
  "classifier_version": "v1.2.0",
  "tier": 1,
  "cdm_field": "deal.amount",
  "confidence": 0.972,
  "input_digest": "9f…",
  "produced_at": "2026-04-20T12:34:56Z",
  "deduped": true
}
```

`deduped=true` indicates an existing row was returned unchanged (natural-key match, same `input_digest`).

### `GET /api/v1/cdm/mapper/ground-truth/diff`

Query params: `classifier_version`, `baseline_version`, optional `tenant_id`.

**Response (200 OK)**
```json
{
  "classifier_version": "v1.2.0",
  "baseline_version":   "v1.1.0",
  "total_rows": 5120,
  "tier1_precision_delta": +0.003,
  "tier1_recall_delta":    -0.001,
  "tier2_precision_delta": +0.011,
  "regressions": [
    { "tenant_id": "acme-corp", "source_field": "CloseDate", "expected_tier": 1, "got_tier": 2 }
  ]
}
```

### Kafka topic — `{tid}.m1.classification_produced` (NEW)

| Field | Type | Notes |
|---|---|---|
| `event_id` | string | sha256 natural-key digest (idempotent) |
| `tenant_id` | string | |
| `proposal_id` | uuid | |
| `classifier_version` | string | |
| `tier` | int (1-3) | |
| `confidence` | float | |
| `produced_at` | iso8601 | |

Consumer groups: `m4-mapping-exceptions` (existing, consumes Tier 2 events for queue dedup), `cdm-mapper-telemetry` (new — writes precision metric to TimescaleDB).

---

## Edge Cases

- **Concurrent classify calls for the same field** — UPSERT on natural key; Postgres row lock serialises; both callers receive the same `proposal_id`.
- **Ground-truth row exists for a field no longer present in the schema snapshot** — DAG skips and logs a warning; does not count as miss.
- **Classifier version rollback** — because `classifier_version` is part of the natural key, a rollback re-runs classification against the older version without touching the newer rows. Query planners join on `MAX(classifier_version)` per `(tenant, source, field, cdm_version)`.
- **Validator approves then reverts** — `cdm_feedback` stores both verdicts with monotonically increasing `verdict_version`. Latest verdict wins.
- **Feedback arrives before the proposal** — extremely unlikely given M4 pulls proposals first, but guarded by FK; consumer retries with exponential backoff for 5 min then moves to DLQ.
- **Ground-truth DAG runs while a classifier version is mid-deploy** — DAG reads `classifier_version` from `platform_metrics` singleton row; locks out for the run.

---

## Acceptance Criteria

- Re-classifying the same field twice yields `deduped=true` on the second call, zero new `cdm_proposals` rows, zero new Kafka events observable after consumer group commit.
- A malformed threshold change (e.g. Tier 1 min confidence lowered to 0.50) is caught by the ground-truth DAG: `tier1_precision_delta` crosses the −0.02 alert threshold and Grafana pages the Data Lead.
- Approving a proposal in M4 produces exactly one `cdm_feedback` row and one `{tid}.m4.validation_decision` event; replaying the Kafka partition does not duplicate either.
- `POST /api/v1/cdm/mapper/classify` returns the same `proposal_id` for 10 sequential identical requests.
- `nexus-cdmm-replay` can rebuild any tenant's `cdm_proposals` snapshot for a prior day from only (a) the frozen schema snapshot and (b) the classifier version pin.

---

## Open Questions

- [CLARIFY: classifier_version — is it stamped at build time via git SHA, or manually bumped in `pyproject.toml`? The former is robust; the latter is human-readable. Recommend both (build stamp + human tag).]
- [CLARIFY: ground-truth ownership — is the initial seed corpus curated by Data Lead, or mined from existing Iteration 1 approved mappings? Mining is cheaper but risks locking in past biases.]
- [CLARIFY: should `cdm_feedback.verdict_reason` be required (forces validator narrative for future RLHF quality) or optional (faster validator throughput)? Product call.]
- [CLARIFY: OQ-TENANT-01 interaction — if a tenant lowers their Tier 1 threshold after feedback is recorded, do we re-run classification and invalidate prior feedback?]
- [CLARIFY: retention on `cdm_ground_truth_runs` — 90 days or indefinite? Indefinite gives long-range regression trends but grows unbounded.]

---

## Dependencies & Sprint Positioning

- Lands in **Phase 1 extension** — V2.0.9 through V2.0.12 migrations added to the platform bootstrap task list (D1-02).
- D1-09 (load thresholds from `tenant_configs`) is retrofitted to emit `classifier_version` into every proposal — small scope expansion.
- Ground-truth DAG piggybacks on the third Airflow DAG added in D1-06 (sub-result cleanup) — extended to a fourth DAG `cdm_ground_truth_regression`.
- RHMA spec (`NEXUS-Iter2-RHMA-v0.1.md`) and CDM Validation spec both consume the new `cdm_feedback` row shape — shared table.

*CDM Mapper v2 spec v0.1 · Mentis Consulting · April 2026*
