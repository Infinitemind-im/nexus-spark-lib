# Iteration 2 — Materialization Tier Coordinator

**Version:** v0.1 (draft)
**Date:** 2026-04-27
**Companion to:** `iter2-dev-overview-and-registers-v0.1.md`

---

## 📦 Codebase & Deployment

> **One repository, many deployments.** All NEXUS services live in the `nexus-platform` monorepo. You are contributing a policy evaluation module to a shared codebase — your decision logic runs as a stage inside an existing service (`nexus-m1-worker`), not in its own process.

| | |
|---|---|
| **Deployed inside** | `nexus-m1-worker` (policy evaluation stage) + Airflow DAGs (tier-movement orchestration) |
| **Monorepo paths** | `services/nexus-m1-worker/nexus_m1_worker/materialization/` · `dags/materialization-dags/` |
| **Language / runtime** | Python 3.11 · Apache Airflow · LightGBM (RLHF training job) |
| **Iteration 2 owner** | Dev 4 (5 person-weeks) |
| **How your code ships** | Policy evaluation logic lives in `services/nexus-m1-worker/` and is called by the worker during record processing. The five tier-movement DAGs live in `dags/`. The RLHF training job may run as a separate ephemeral Spark job but is not a long-running service. |

---

## 1. Scope

The Materialization Coordinator owns the dynamic tier system — the part of the platform that decides where data lives and orchestrates its movement between tiers. Concretely:

- The Stage 0 policy evaluator: a small library that takes a record and a policy snapshot and returns `materialization_level`. Called by CDC Streaming's streaming Spark and by Batch Backfill's batch jobs via `nexus_spark_lib.transform.materialization_decide`.
- The materialization registers: `cdm_entity_materialization`, `materialization_policy`, `materialization_cohorts`, `materialization_decision_log`, `materialization_signal`, `materialization_movement_log`.
- The five tier-movement DAGs that *trigger* movement: `materialization-recommend` (daily), `materialization-policy-reevaluate` (daily), `materialization-promotion-backfill` (event-driven), `materialization-demotion-cleanup` (event-driven), `materialization-rlhf-update` (daily). The actual data work inside the DAGs is owned by Batch Backfill (replay) and M3 Writers (cleanup writes); The Materialization Coordinator owns the orchestration and the decisions.
- The RLHF feedback loop: signal collection from the query executor, the GBDT training job, rule synthesis, and recommendations.
- The `nexus.materialization_policy.changed` and `nexus.materialization.changed` event topics that drive cache refreshes across the platform.

This component does **not** own: the actual ingestion (CDC Streaming), the actual backfill execution (Batch Backfill — Materialization Coordinator triggers, Batch Backfill runs), the resolution / synthesis (Entity Resolution), or the AI store writes (M3 Writers). Materialization Coordinator is the **decision and orchestration layer** for tiers; everyone else does the data work.

---

## 2. Dependencies

| Depends on | What for | When needed |
|---|---|---|
| Batch Backfill | Backfill DAG framework that Materialization Coordinator's DAGs trigger; `nexus_spark_lib` for embedded policy evaluation | Week 0–1 |
| Entity Resolution | `golden_records_index` and `entity_resolution_index` schemas (Materialization Coordinator reads them for cohort scope queries) | Week 1 |
| M3 Writers | `entity_store_presence` register (Materialization Coordinator reads to check movement completion); `nexus.m3.write_completed` events for telemetry | Week 2 |
| Platform | LightGBM + scikit-learn available in the Airflow + Spark Python environments | Week 3 |
| Platform | S3 bucket for serialised model artefacts | Week 3 |

---

## 3. Functional Requirements (MoSCoW)

### 3.1 Must

- **FR-Dev 4-M-01.** Implement `nexus_spark_lib.transform.materialization_decide(record, policy_snapshot) -> level`. Pure function: given a normalised record and a policy snapshot, return `hot | warm | cold`. The function evaluates rules per the resolution algorithm in `iter2-materialization-policy-engine-v0.1.md` §2.4: filter by scope and validity window, evaluate predicates, sort by `(priority DESC, rule_type ordering, valid_until ASC)`, return the top match's `target_level`. Append a row to `materialization_decision_log` recording the chosen rule.
- **FR-Dev 4-M-02.** Implement the predicate compiler. Input: SQL-like predicate string per the grammar in the policy engine spec §2.2. Output: a Catalyst `Expression` (Spark) and an interpreter bytecode (m1-worker). Compilation includes constant folding for time-dependent terms, common-subexpression deduplication, and Bloom-filter compilation for large `IN` lists. Recompiled on a 1-minute timer for time-dependent terms.
- **FR-Dev 4-M-03.** Maintain the policy broadcast cache in Spark: read `materialization_policy` rows where `superseded_at IS NULL`, package as a `Broadcast[MaterializationPolicy]`, refresh every 5 minutes or on `nexus.materialization_policy.changed` event, whichever comes first. Same for m1-worker's local cache.
- **FR-Dev 4-M-04.** Implement Stage 0 routing in `nexus-m1-worker` (in addition to Stage 0 in Spark). When the worker consumes `m1.cdm_entities_ready`, evaluate policy and publish:
  - `materialization_level=hot` → publish `{tid}.m1.entity_routed` with the level in the headers.
  - `materialization_level=warm` → publish `{tid}.m1.warm_recorded` (governance only; M3 Writers ignores).
  - `materialization_level=cold` → publish `{tid}.m1.cold_skipped` (governance only).
- **FR-Dev 4-M-05.** Implement and operate the daily `materialization-recommend` Airflow DAG. Steps:
  1. Read the past 24 hours of `materialization_signal` rows.
  2. Roll up into per-cohort 7d/30d/90d windows.
  3. Compute reward under each existing rule and under candidate alternatives.
  4. Stage proposals in `materialization_recommendations` with `requires_approval` flag based on impact (records affected × cost delta).
  5. Auto-apply low-impact proposals (rows < 1000 affected); queue the rest for Tenant Admin.
- **FR-Dev 4-M-06.** Implement and operate the daily `materialization-policy-reevaluate` Airflow DAG. For each `(tenant, entity_type)` whose decay or boost rules could have changed effect since the last run, scan `golden_records_index` rows and re-evaluate. Records whose level changed are listed in a Delta Lake staging table; Dev 4 emits `RELEVEL` (for upgrades) or `REMOVE` (for downgrades) events on `entity_routed`. Bounded to 1M records per tenant per night by default.
- **FR-Dev 4-M-07.** Implement and operate `materialization-promotion-backfill` (triggered by `nexus.materialization.changed` warm→hot or cold→hot). Sets `cdm_entity_materialization.transition_status='promoting'`, calls D2's backfill executor with the affected scope, monitors completion, then sets the level to `hot` and clears the transition flag.
- **FR-Dev 4-M-08.** Implement and operate `materialization-demotion-cleanup` (triggered by `nexus.materialization.changed` hot→warm or hot→cold). Sets `transition_status='demoting'`, identifies records in `entity_store_presence` for the cohort where any flag is TRUE, emits `entity_routed` events with `operation='REMOVE'` for each, monitors `nexus.m3.write_completed` events to confirm cleanup, then sets the level and clears the transition flag.
- **FR-Dev 4-M-09.** Implement and operate `materialization-rlhf-update` (daily). Steps per `iter2-materialization-feature-learning-v0.1.md` §3.4: aggregate signals → compute counterfactual reward targets → materialise feature vectors → train three GBDT models per `(tenant, entity_type)` (one per level) → persist to `reward_models` with versioning → score current corpus → propose rule changes via `materialization_recommendations`.
- **FR-Dev 4-M-10.** Implement signal emission integration with the query executor. Define a thin client library `nexus_materialization_signals` that the query executor calls after each query: `emit_signal(query_session_id, cdm_entity_id, signal_kind, latency_ms, applied_rule_id)`. The library writes to `materialization_signal` (partitioned weekly). The Materialization Coordinator owns the table; the query executor owns the call sites.
- **FR-Dev 4-M-11.** Implement the three movement guardrails per the policy engine spec §4.5: bounded change rate (default 3 changes per tenant per evaluation window), cool-down on reversal (no re-promotion within 14 days of a demotion), and override respect (manual rules never proposed for change). Implemented in the recommendation pipeline; suppression reasons recorded in `materialization_recommendations.suppression_reason`.
- **FR-Dev 4-M-12.** Maintain the `materialization_movement_log` table — every level change for an entity type, cohort, or individual record. Used for audit and as input feature `oscillation_count_30d` for the RLHF model.
- **FR-Dev 4-M-13.** Provide an admin API surface (consumed by M4 frontend) for: (a) viewing current rules, (b) authoring/editing rules with predicate validation, (c) reviewing pending recommendations with change cards, (d) approving/rejecting/ignoring recommendations, (e) viewing the movement log filtered by entity type or cohort.

### 3.2 Should

- **FR-Dev 4-S-01.** Predicate validation API: given a candidate predicate string, return a syntax check + an estimate of records it would match. Used in the rule-authoring UI before the Admin commits.
- **FR-Dev 4-S-02.** Recommendation explainability: every recommendation carries the `change card` payload (4 sections per the feature-learning spec §5) directly in `materialization_recommendations.explanation_payload`.
- **FR-Dev 4-S-03.** Cohort manager — let admins define and reuse named cohorts via `materialization_cohorts`, then reference them by name in rules.

### 3.3 Could

- **FR-Dev 4-C-01.** A "shadow mode" for new learned rules: apply them to scoring only (decision-log records what they *would* do), don't actually take effect. Run for 7 days, compare predicted vs actual, then activate. Reduces RLHF risk at the cost of slower rollout.
- **FR-Dev 4-C-02.** Cross-tenant aggregation of feature importance vectors with differential-privacy noise. Tracked in OQ-MPE-04; out of scope for v0.1 details.

### 3.4 Won't

- **FR-Dev 4-W-01.** Materialization Coordinator will not directly write to Elasticsearch, Neo4j, or TimescaleDB. All store changes are mediated by D5.
- **FR-Dev 4-W-02.** Materialization Coordinator will not call source systems. Cold-tier promotions that require fresh extraction are coordinated through D1 via `nexus.connector.refresh_required`.

---

## 4. Non-Functional Requirements

| ID | Requirement | Target |
|---|---|---|
| NFR-D4-01 | Stage 0 evaluation latency per record (in Spark) | p95 ≤ 1 ms |
| NFR-D4-02 | Stage 0 evaluation latency per record (in m1-worker) | p95 ≤ 5 ms |
| NFR-D4-03 | Policy broadcast cache refresh latency after `policy.changed` event | ≤ 5 minutes for Spark, ≤ 30 seconds for m1-worker |
| NFR-D4-04 | `materialization-rlhf-update` runtime | ≤ 90 minutes per tenant on Iter 2 sizing |
| NFR-D4-05 | `materialization-policy-reevaluate` runtime | ≤ 60 minutes for a tenant with 10M records, 100 active rules |
| NFR-D4-06 | Recommendation acceptance rate | ≥ 60% of high-impact recommendations are approved by Tenant Admins after 90 days of operation (calibration metric, not a hard SLO) |

---

## 5. Data Model Ownership

The Materialization Coordinator owns the materialization registers. Consolidated DDL, with cross-references where DDL is in the parent specs:

- `materialization_policy` — DDL in `iter2-materialization-policy-engine-v0.1.md` §8.
- `materialization_cohorts` — DDL in §3.3 of the same.
- `materialization_decision_log` — DDL in §8.
- `materialization_signal` — DDL in §8.
- `cost_model` — DDL in §8.
- `feature_definitions`, `reward_models`, `feature_importance_history` — DDL in `iter2-materialization-feature-learning-v0.1.md` §8.
- `materialization_movement_log` — DDL in `iter2-dev-overview-and-registers-v0.1.md` §7.5.

Two new tables specific to Materialization Coordinator's recommendation pipeline:

```sql
CREATE TABLE nexus_system.materialization_recommendations (
  recommendation_id    UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id            UUID NOT NULL,
  scope                VARCHAR(128) NOT NULL,
  proposed_action      VARCHAR(32) NOT NULL,             -- 'create_rule' | 'modify_rule' | 'retire_rule' | 'change_threshold' | 'change_level'
  proposed_payload     JSONB NOT NULL,                   -- the actual rule / threshold to apply
  current_level        VARCHAR(8),
  proposed_level       VARCHAR(8),
  reward_delta         NUMERIC(10,4),
  records_affected     INTEGER,
  cost_delta_usd       NUMERIC(10,2),
  evidence_model_id    UUID REFERENCES nexus_system.reward_models(model_id),
  explanation_payload  JSONB NOT NULL,
  requires_approval    BOOLEAN NOT NULL,
  status               VARCHAR(16) NOT NULL DEFAULT 'pending'
                       CHECK (status IN ('pending','auto_applied','approved','rejected','expired','suppressed')),
  suppression_reason   VARCHAR(64),
  reviewed_by          VARCHAR(64),
  reviewed_at          TIMESTAMPTZ,
  created_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  expires_at           TIMESTAMPTZ
);
CREATE INDEX idx_mr_tenant_status ON nexus_system.materialization_recommendations(tenant_id, status, created_at DESC);

CREATE TABLE nexus_system.cdm_entity_materialization (
  tenant_id              UUID NOT NULL,
  cdm_entity_type        VARCHAR(128) NOT NULL,
  current_level          VARCHAR(8) NOT NULL CHECK (current_level IN ('hot','warm','cold')),
  transition_status      VARCHAR(16),                     -- NULL | 'promoting' | 'demoting'
  transition_started_at  TIMESTAMPTZ,
  transition_movement_id BIGINT REFERENCES nexus_system.materialization_movement_log(movement_id),
  query_count_30d        BIGINT NOT NULL DEFAULT 0,
  fallback_count_30d     BIGINT NOT NULL DEFAULT 0,
  last_evaluated_at      TIMESTAMPTZ,
  PRIMARY KEY (tenant_id, cdm_entity_type)
);
```

`cdm_entity_materialization` is a *summary* table. It records the *prevailing* level for an entity type. Per-record level may differ when policy rules apply at finer granularity; the per-record level lives in `materialization_decision_log` (latest per record) and in the `materialization_level` field of each record's downstream events.

---

## 6. API / Kafka Contracts

### 6.1 Inbound

- `m1.int.cdm_entities_ready` (from Entity Resolution via cdm-mapper) — the trigger for Stage 0 in m1-worker.
- `nexus.m3.write_completed` (from M3 Writers) — for confirming movement completion.
- `nexus.query.signal_emitted` (synthetic; see FR-Materialization Coordinator-M-10) — RLHF signal stream.
- `nexus.cdm.version_published` — re-evaluate policies that reference renamed attributes.

### 6.2 Outbound

- `{tid}.m1.entity_routed` — the existing topic; Materialization Coordinator republishes events from `cdm_entities_ready` with the level annotation.
- `{tid}.m1.warm_recorded`, `{tid}.m1.cold_skipped` — governance audit only.
- `nexus.materialization_policy.changed` — fired when any rule is created, modified, or retired. Payload identifies affected scopes.
- `nexus.materialization.changed` — fired when an entity type's `current_level` changes. Payload includes `(tenant_id, scope, old_level, new_level, triggered_by)`.
- `nexus.materialization.reevaluated` — fired when the daily reevaluate DAG produces per-record relevels. Carries a count, not the records themselves.
- `nexus.materialization.recommendation_created` — fired when the RLHF DAG stages a new recommendation. M4 uses this to surface notification badges.

### 6.3 HTTP API (consumed by M4 frontend)

```
GET  /api/v1/materialization/policy?tenant=...&scope=...
POST /api/v1/materialization/policy            # create rule
PUT  /api/v1/materialization/policy/{id}       # supersede with new version
POST /api/v1/materialization/policy/{id}/retire

GET  /api/v1/materialization/recommendations?tenant=...&status=pending
POST /api/v1/materialization/recommendations/{id}/approve
POST /api/v1/materialization/recommendations/{id}/reject

GET  /api/v1/materialization/movement-log?tenant=...&scope=...&from=...&to=...
GET  /api/v1/materialization/cohorts?tenant=...
POST /api/v1/materialization/cohorts

POST /api/v1/materialization/predicate/validate    # syntax + match-count estimate
```

All endpoints enforce `X-Tenant-ID` and `X-User-Role` per the existing Kong middleware.

---

## 7. CRUD Handling — Materialization Coordinator's Slice

Materialization Coordinator doesn't directly handle source CRUD, but it interprets it through the lens of policy:

- **INSERT / SNAPSHOT_READ** → record arrives at Stage 0 → policy evaluated → level decided → routed.
- **UPDATE** → if the update changes attributes used in policy predicates, the record's level may change. Materialization Coordinator detects this in Stage 0 and emits with the new level. If the new level differs from the GR's previous level for this record, the path becomes RELEVEL for M3 Writers.
- **DELETE** → the record exits the platform; Materialization Coordinator has nothing to do unless the deletion empties an entire cohort, in which case the daily DAG eventually retires associated rules with no impact (no records to apply to).
- **RELEVEL** → emitted by Materialization Coordinator's own DAGs. Treated by Entity Resolution as a synthesis-only re-run; treated by M3 Writers as a re-projection or tombstone depending on the new level.

The interesting CRUD case for Materialization Coordinator is **policy CRUD** — admins or the RLHF loop creating, modifying, or retiring rules. Rules are append-only with supersession (`materialization_policy.superseded_at`), so a "modify" is logically a new row referencing the old via `superseded_by`. This preserves audit trail and makes rollback trivial.

---

## 8. Hot/Warm/Cold Handling — Materialization Coordinator's Slice (the orchestration)

Materialization Coordinator's whole job. Three classes of movement:

### 8.1 Entity-type-level movements (rare, high-impact)

A whole entity type moves from one level to another. Driven by:
- Manual override by Tenant Admin.
- Auto-promotion or auto-demotion triggered by `materialization-recommend`.

The DAG sequence:

```
1. Materialization Coordinator sets cdm_entity_materialization.transition_status = 'promoting' (or 'demoting').
2. Materialization Coordinator emits nexus.materialization.changed.
3. For promotion: Materialization Coordinator calls Batch Backfill's materialization-promotion-backfill executor.
   For demotion:   Materialization Coordinator calls Batch Backfill's materialization-demotion-cleanup executor (data side)
                  + M3 Writers's cleanup handlers process the resulting REMOVE events.
4. Materialization Coordinator monitors completion via nexus.m3.write_completed events keyed to the
   movement_id.
5. When all expected writes are accounted for: Materialization Coordinator sets current_level to the new
   level, clears transition_status, records movement_log row as 'completed'.
6. Materialization Coordinator emits a follow-up nexus.materialization.changed with status='completed'.
```

### 8.2 Cohort-level movements (driven by boost rules)

A boost rule (e.g. fiscal close) starts/expires. Affects records whose attributes match the boost's predicate. Materialization Coordinator's daily reevaluate DAG identifies them and emits per-record relevels. Same machinery as entity-type movements but scoped to the cohort.

### 8.3 Per-record movements (driven by decay rules)

A record's `age_days` crosses a threshold. The daily reevaluate DAG catches it and emits a per-record relevel. Most common in steady state — thousands per day per tenant.

### 8.4 The transition window

While `transition_status='promoting'` or `'demoting'`, the entity type is in a known-inconsistent state. M3 Writers's `entity_store_presence` flags are partially flipped — some records confirmed, others pending. The query engine reads `cdm_entity_materialization.transition_status` and, if non-null, falls back to source for that entity type until the transition completes. This is the intentional design: brief slow path during movement, fast path resumes after.

### 8.5 Throughput throttling

Materialization Coordinator enforces `tenant_configs.tier_movement_throughput_max` (default 1000 records/second per tenant). Above this rate, movement events queue in `materialization_movement_log` with `status='queued'` and are released by a cadence scheduler. Priority order: manual overrides → learned-rule changes → time-decay → RLHF promotions. Admins see queue depth in the M4 UI.

### 8.6 Oscillation accounting

Every movement is logged. The RLHF model treats `oscillation_count_30d` as a feature; cohorts that oscillate frequently get a small reward penalty (oscillation costs Elasticsearch re-embedding and Spark hours). The system gradually settles to stable assignments — exactly what the user said: "RLHF will decide later, the response time being the reward signal."

---

## 9. Acceptance Criteria

- **AC-D4-01.** Stage 0 throughput test: process 100K records in Spark with a policy of 50 active rules. Assert p95 evaluation latency ≤ 1 ms (NFR-D4-01).
- **AC-D4-02.** Cache refresh test: change a rule via the M4 admin API; assert `nexus.materialization_policy.changed` fires; assert the next streaming Spark micro-batch (within 5 minutes) reflects the new policy in its evaluations.
- **AC-D4-03.** Predicate compilation test: create a rule with `predicate = "AGE(created_at) <= '90 days' AND industry = 'Healthcare'"`; assert the compiled form is correct against a fixture record set; assert `NOW()` substitution is refreshed every minute.
- **AC-D4-04.** Decay test: create records at various ages; let the reevaluate DAG run; assert records older than 90 days are demoted from hot to warm; assert `materialization_movement_log` records each transition.
- **AC-D4-05.** Boost test: create a boost rule for a future window; let time pass to the start of the window; assert affected records are promoted with `triggered_by='boost'`; let time pass to the end; assert they are demoted back to base level.
- **AC-D4-06.** Promotion DAG test: manually promote `Party` from warm to hot for tenant Acme. Assert `transition_status='promoting'`; assert D2's backfill runs; assert all `Party` records in `entity_store_presence` have applicable store flags set to TRUE; assert `current_level='hot'` and `transition_status=NULL` after completion.
- **AC-D4-07.** Demotion DAG test: demote same `Party` back to warm. Assert all `entity_store_presence` flags for the cohort are set to FALSE; assert TimescaleDB rows persist (warm doesn't clean TimescaleDB); assert `current_level='warm'` after completion.
- **AC-D4-08.** RLHF training test: feed a synthetic 90-day signal log with deliberate cohort patterns (Healthcare parties heavily queried, archived orders never queried); run `materialization-rlhf-update`; assert at least one promote-Healthcare and one demote-archived recommendation appears.
- **AC-D4-09.** Guardrail test: trigger 4 RLHF-driven changes for the same tenant in one window; assert the 4th is suppressed with `suppression_reason='change_rate_limit'`.
- **AC-D4-10.** Cool-down test: demote a cohort, then within 14 days the RLHF model would propose re-promotion; assert the proposal is suppressed with `suppression_reason='cool_down'`.
- **AC-D4-11.** Manual override test: pin a manual rule; let RLHF run with strong signals against it; assert no recommendation is generated for that rule (FR-Dev 4-M-11 override respect).
- **AC-D4-12.** Movement throughput test: trigger 5000 simultaneous per-record relevels for a tenant with throttle 1000/s; assert they process at 1000/s with the rest queued; assert priority ordering holds (manual first if any).

---

## 10. Open Questions

- **OQ-D4-01.** Stage 0 evaluation in m1-worker vs in Spark — keep both, or consolidate? Two evaluation points means two cache refresh paths. Recommend keep both: Spark for streaming throughput, m1-worker for the routing decision after CDM mapping (where the Spark micro-batch boundary has already been crossed).
- **OQ-D4-02.** RLHF model retraining cadence — daily is the default; some tenants will benefit from more frequent updates (e.g. high-volume tenants with rapid query pattern shifts). Recommend per-tenant configurable, default daily.
- **OQ-D4-03.** Recommendation expiry — pending recommendations sit indefinitely if the Admin doesn't act. Should they auto-expire? Recommend yes, 30 days, and re-evaluated on the next daily run.
- **OQ-D4-04.** Cross-tenant feature aggregation for the reward model — opt-in only with differential privacy. Detailed design in v0.2 after a security review.
- **OQ-D4-05.** Oscillation feedback into the reward function — what's the right penalty weight? Too high suppresses legitimate movements; too low allows wasteful flapping. Recommend learned per-tenant via the same RLHF mechanism.
- **OQ-D4-06.** When the `materialization-policy-reevaluate` DAG identifies more than the daily 1M-record bound, what's the policy? Recommend deterministic priority: manual-rule scope first, then high-volume cohorts, then the rest; documented to admins as "reevaluation has a daily budget; oversize tenants may see staggered rollouts."

---

## 11. References

- `iter2-dev-overview-and-registers-v0.1.md` — cross-cutting contracts.
- `iter2-materialization-policy-engine-v0.1.md` — the policy model and rule resolution.
- `iter2-materialization-feature-learning-v0.1.md` — the RLHF subsystem.
- `iter2-system-pipeline-orchestration-v0.1.md` — §3 materialization classification, §6 per-store routing matrix.
- `iter2-dev-D2-backfill-pipeline-v0.1.md` — Materialization Coordinator triggers Batch Backfill's promotion-backfill / demotion-cleanup DAGs.
- `iter2-dev-D5-m3-writers-and-presence-v0.1.md` — Materialization Coordinator monitors M3 Writers's `write_completed` to detect movement completion.
