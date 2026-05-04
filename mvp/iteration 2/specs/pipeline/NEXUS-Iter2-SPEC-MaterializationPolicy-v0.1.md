# Iteration 2 — Materialization Policy Engine and RLHF Loop

**Version:** v0.1 (draft)
**Date:** 2026-04-27
**Companion to / refinement of:** `iter2-system-pipeline-orchestration-v0.1.md` §3, `iter2-cdm-to-aistores-pipeline-v0.1.md` §3.0
**Scope:** Replaces the per-`(tenant, entity_type)` materialization model with a policy-driven, time-and-context-aware engine, and specifies the feedback loop that learns from observed usage.

---

## 1. What Changes

The materialization model in v0.1 of the parent specs assigned one level — hot, warm, or cold — per `(tenant_id, cdm_entity_type)`. Every `Transaction.SalesOrder` in tenant Acme shared the same level; every `Party` shared the same level. This is too coarse for three reasons that show up in every real tenant.

**Records age out.** A sales order closed five years ago is queried at perhaps 1% of the rate of one closed last quarter. Holding both at the same level either over-spends on the old one or under-serves the new one. The level needs to follow the record's age, not just its type.

**Context concentrates attention.** During a fiscal close, the tenant's finance team queries this quarter's invoices a hundred times in two weeks, then never again. The level should briefly elevate for that cohort and revert when the close ends.

**Tenants have asymmetric attention.** A B2B SaaS tenant cares deeply about its enterprise accounts and barely tracks SMB. The same `Party` entity type contains both — they should not share a level. Patterns like this are not knowable in advance; they emerge from how the tenant actually uses the platform.

The engine described below replaces the single level per entity type with a **list of policy rules** evaluated against each record's attributes. Rules are typed (base, decay, boost, learned, manual), prioritised, time-bounded, and combined deterministically. A daily learning job watches actual query behaviour and proposes adjustments to the learned-rule set, closing the feedback loop.

The level a record gets is now the *output* of evaluating policy against the record. It still settles to one of the same three values — hot, warm, cold — and still drives the same downstream behaviour described in the parent specs (Spark ER depth, M3 projection, query routing). Only the assignment mechanism changes.

---

## 2. The Policy Model

### 2.1 Rules

A policy rule is a row in `nexus_system.materialization_policy` (NEW). Conceptually:

```
rule := (id, tenant_id, scope, predicate, target_level, priority, rule_type,
         valid_from, valid_until, source, learned_metadata)
```

| Field | Meaning |
|---|---|
| `scope` | The set of CDM entity types the rule applies to. `*` means all types (rare). |
| `predicate` | A boolean expression over canonical attributes of the record (see §2.2). |
| `target_level` | `hot`, `warm`, or `cold` if the rule fires. |
| `priority` | Integer; higher wins on conflict. |
| `rule_type` | `base` / `decay` / `boost` / `learned` / `manual`. Determines who can edit it and how it's surfaced. |
| `valid_from` / `valid_until` | Time window during which the rule is active. `null` means always-on. |
| `source` | `'system'` for defaults, `'admin'` for tenant-set, `'rlhf'` for learned, `'fiscal_calendar'` for calendar-driven. |
| `learned_metadata` | JSONB — only populated for `rule_type = 'learned'`; carries observed evidence (cohort hit rate, fallback rate, sample queries). |

### 2.2 Predicate language

Predicates are a small, deliberately constrained boolean expression language. The grammar:

```
predicate    := comparison
              | predicate AND predicate
              | predicate OR predicate
              | NOT predicate
              | "(" predicate ")"
comparison   := attribute operator literal
              | attribute "IN" literal_list
              | attribute "BETWEEN" literal "AND" literal
              | attribute "MATCHES" regex_literal
              | "AGE" "(" attribute ")" operator interval_literal
              | "COHORT" "(" cohort_id ")"
operator     := "=" | "!=" | "<" | "<=" | ">" | ">="
attribute    := canonical attribute name (e.g. "industry", "fiscal_year", "created_at")
                or virtual attribute (e.g. "_query_count_30d", "_last_queried_at")
```

Predicates are restricted to side-effect-free expressions over the record's canonical attributes plus a small set of system-virtual attributes (prefixed `_`). They are not Turing-complete, are evaluated by a deterministic interpreter in Spark and in `nexus-m1-worker`, and can be statically validated and cost-estimated when admitted.

A predicate is evaluated against the record's *post-Stage-1* canonical form, after normalisation but using the record's current values (not historical). For document records, the predicate has access to extracted attributes after Stage 4d.

The `AGE(attribute)` shorthand expands to `NOW() - attribute` and supports interval literals (`'30 days'`, `'2 years'`). Attributes that are not present on a record evaluate to `null`; comparisons with `null` are `null`-propagating in SQL fashion (a rule with a `null` predicate result does not fire).

### 2.3 The five rule types

**Base.** The fallback level for an entity type when no other rule applies. One per `(tenant, scope)` is required. Created automatically when a CDM entity type is published, using the heuristic-driven default from §3.2 of the parent system spec. Editable by Tenant Admin.

```
priority = 0
predicate = TRUE
rule_type = 'base'
```

**Decay.** Migrates aging records down the levels. Created automatically per entity type whose canonical attributes include a temporal field (`created_at`, `transaction_date`, `event_time`). Tunable by Tenant Admin. Common pattern:

```
-- Hot for 90 days, then warm, then cold after 2 years
priority = 100
predicate = AGE(created_at) <= '90 days'
target_level = hot

priority = 50
predicate = AGE(created_at) > '90 days' AND AGE(created_at) <= '2 years'
target_level = warm

priority = 25
predicate = AGE(created_at) > '2 years'
target_level = cold
```

**Boost.** Time-bounded elevation for a specific cohort. Created by Tenant Admin or by integration with a fiscal calendar service (proposed; see OQ-MPE-03). Lives until `valid_until`, then expires automatically. Common pattern:

```
-- During year-end close, all current-fiscal-year invoices are hot
priority = 500
predicate = entity_type = 'Transaction.Invoice' AND fiscal_year = 2026
target_level = hot
valid_from = 2026-11-01
valid_until = 2027-02-15
rule_type = 'boost'
source = 'fiscal_calendar'
```

When a boost rule expires, records that were hot only because of it revert to whatever the lower-priority rules prescribe — typically warm via a decay rule. The expiry is handled by the daily `materialization-recommend` job.

**Learned.** Output of the RLHF loop (§4). Created by the system when observed query behaviour suggests a cohort is materially under- or over-served. Carries `learned_metadata` documenting the evidence. Can be edited or pinned by a Tenant Admin (which converts it to `manual`).

```
-- The RLHF loop noticed Healthcare-industry parties are queried 4x more than average
priority = 200
predicate = entity_type = 'Party' AND industry = 'Healthcare'
target_level = hot
rule_type = 'learned'
source = 'rlhf'
learned_metadata = {
  "cohort_size": 1284,
  "query_count_90d": 5421,
  "fallback_count_90d": 412,
  "expected_uplift_score": 0.73,
  "evidence_window": "2026-01-27/2026-04-27"
}
```

**Manual.** Tenant Admin pinned. Highest priority. Cannot be overridden by other rule types. Used for "always hot, regardless" cases (a strategic account, a regulated dataset, an SLA-bound entity type).

```
priority = 1000
predicate = entity_type = 'Party' AND tax_id IN ('US-87-4421938', 'US-12-3456789')
target_level = hot
rule_type = 'manual'
```

### 2.4 Resolution algorithm

For a given record, the engine computes its level as follows:

```
1. Filter rules: scope matches AND valid_from <= NOW() < valid_until (null = always)
2. Evaluate predicates against the record. Keep matching rules.
3. Sort matches by (priority DESC, rule_type ordering, valid_until ASC).
   rule_type ordering: manual > boost > learned > decay > base
4. Take the top rule's target_level. That's the answer.
5. Log (record_id, applied_rule_id, target_level, evaluated_at) to
   nexus_system.materialization_decision_log.
```

The decision log is append-only and partitioned by date. It supports the RLHF loop's evidence base and provides audit ("why was this record warm last Tuesday?").

A rule with no matching record across an entire window is a candidate for retirement. A rule that consistently produces the same level as a lower-priority rule is redundant. Both are surfaced by the maintenance job for cleanup.

---

## 3. How Spark and the Routing Layer Use Policy

### 3.1 Where evaluation happens

Policy evaluation runs in two places, both consuming the same `materialization_policy` table via a broadcast cache:

- **`spark-stream-transformer` Stage 0** evaluates per record during ingestion to choose ER depth (§3.2 of the parent pipeline spec).
- **`nexus-m1-worker` Op Router** evaluates per record before publishing `entity_routed` to choose M3 routing.

Both places read the same broadcast snapshot, so a record's ER depth and projection routing always agree on its level. The broadcast snapshot refreshes every 5 minutes or on `nexus.materialization_policy.changed` (NEW Kafka topic), whichever comes first.

### 3.2 Predicate evaluation cost

Predicates are evaluated per record. A naive implementation would not scale at high tenant volume. Two optimisations apply.

**Predicate compilation.** When a rule is admitted, its predicate is compiled to a Spark Catalyst `Expression` (in Spark) and to a small interpreter bytecode (in m1-worker). Compilation includes:
- Constant folding for time-dependent terms (`AGE(created_at) <= '90 days'` becomes a literal timestamp comparison after `NOW()` substitution; recompiled every minute)
- Common-subexpression deduplication across rules

**Bloom-filtered IN lists.** Manual rules with large `IN` lists (a watchlist of strategic accounts) compile their literal lists into Bloom filters, evaluated before any other comparison.

These optimisations are sufficient to evaluate a typical tenant's full rule set (single-digit hundreds of rules in steady state) per record at single-digit microsecond cost.

### 3.3 Cohorts as named predicates

Frequently-used predicates can be promoted to **named cohorts**, stored in `nexus_system.materialization_cohorts`. A cohort is a label + predicate. Rules can reference a cohort by name (`COHORT('healthcare_parties')`), which is more readable, cacheable, and updatable in one place. Cohorts also serve as the unit the RLHF loop reasons about — the loop emits new rules against existing cohorts before it tries to invent new predicates.

```sql
CREATE TABLE nexus_system.materialization_cohorts (
  cohort_id     VARCHAR(64) PRIMARY KEY,
  tenant_id     UUID NOT NULL,
  scope         VARCHAR(128) NOT NULL,
  predicate     TEXT NOT NULL,
  display_name  VARCHAR(128) NOT NULL,
  created_by    VARCHAR(64) NOT NULL,             -- 'admin' | 'rlhf' | 'system'
  created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

---

## 4. The RLHF Loop

### 4.1 Why "RLHF" is the right framing — with one caveat

The loop is framed as RLHF because it follows the canonical pattern: a model (the policy) is updated based on signals derived from human behaviour, with explicit feedback admitted alongside revealed preference. The caveat is that "human feedback" here is mostly **revealed** — what users queried, what they accepted, what they re-queried — rather than explicit thumbs-up / thumbs-down ratings. The architecture supports explicit feedback (M4 surfaces a "this answer was complete / this answer felt slow / this answer was missing data" widget on chatbot responses, which writes to `nexus_system.feedback_events`), but the loop is designed to converge from revealed preference alone if explicit signal is sparse, which it usually is in early operations.

### 4.2 The signals that feed the loop

Five signals are continuously emitted by the rest of the platform and consumed by the learning job. Each is keyed to enough metadata that the loop can attribute it to a cohort.

| Signal | Emitted by | What it tells the loop |
|---|---|---|
| `query_hot_hit` | `nexus-query-executor` | A query landed on a hot record and returned successfully without source fallback. |
| `query_warm_fallback` | `nexus-query-executor` | A query needed a record at warm or cold level and had to fetch from source. The latency cost is recorded. |
| `query_cold_fallback` | `nexus-query-executor` | Same as above for cold. |
| `query_hot_miss` | `nexus-query-executor` | A query hit a hot store but the relevant records weren't there (often because they hadn't been promoted yet despite being needed). |
| `feedback_explicit` | M4 / Chatbot | Explicit user signal: latency complaint, completeness complaint, or positive acknowledgement. |

Every signal carries `(tenant_id, cdm_entity_id, cdm_entity_type, query_session_id, applied_rule_id, observed_latency_ms, signal_strength)`. The decision log entry from §2.4 lets the loop join signals back to the rule that produced the level being judged.

### 4.3 The reward function

For each cohort and each candidate rule change, the learning job computes a reward:

```
reward = w₁ · (latency_saved_p95)
       - w₂ · (incremental_storage_cost)
       - w₃ · (incremental_embedding_cost)
       + w₄ · (explicit_positive_feedback)
       - w₅ · (explicit_negative_feedback)
```

`latency_saved_p95` is the difference between observed p95 query latency under the current policy and the projected p95 latency under the candidate change, estimated by counterfactual replay against the decision log. Storage and embedding cost deltas are computed from the cohort size and the per-record cost coefficients in `nexus_system.cost_model` (managed by Platform). Weights `w₁..w₅` are tenant-tunable; defaults are seeded from a calibration run on a pilot tenant and refined as the platform learns across tenants (the cross-tenant aggregate is opt-in, see OQ-MPE-04).

Cohorts with positive reward become candidate promotions. Cohorts with reward sufficiently negative become candidate demotions. Reward magnitude determines whether the change auto-applies or queues for Tenant Admin approval.

### 4.4 The learning job

A daily Airflow DAG, `materialization-rlhf-update`, owned by the Data Intelligence team:

```
Step 1. Aggregate the past 24 hours of signals into per-cohort counters.
Step 2. Roll counters into rolling 7d / 30d / 90d windows.
Step 3. For each candidate cohort (existing learned rules + emerging cohorts
        identified by the cohort discovery sub-step):
        a. Compute reward under current rule.
        b. Compute reward under each plausible alternative (promote, demote,
           refine predicate).
        c. Pick the highest-reward alternative; record the delta.
Step 4. Filter changes by stability: a change is admitted only if the reward
        delta has been positive across at least 2 of the last 3 weekly
        evaluation windows. This prevents reactive churn.
Step 5. Auto-apply low-impact changes; queue high-impact ones for review in
        materialization_recommendations.
Step 6. For accepted changes, write or update rows in materialization_policy
        with rule_type='learned' and learned_metadata populated.
Step 7. Emit nexus.materialization_policy.changed for affected tenants,
        triggering broadcast cache refresh in Spark and m1-worker.
```

Cohort discovery (Step 3, sub-step) uses a simple, explainable approach: enumerate single-attribute groupings (by `industry`, by `region`, by `account_segment`, etc.), measure variance in query rate within each grouping, and surface groupings whose variance is large. More sophisticated methods (decision-tree mining, embedding-based cluster discovery) are out of scope for v0.1 and tracked as OQ-MPE-05.

### 4.5 Stability and safety

Three guardrails prevent the loop from misbehaving.

**Bounded change rate.** No more than N policy changes per tenant per evaluation window. Default N = 3. A tenant whose policy has just been substantially edited will not see new RLHF-driven changes for one full evaluation cycle.

**Cool-down on reversal.** If the loop demotes a cohort and within 14 days the same cohort is recommended for promotion, the recommendation is suppressed. This prevents oscillation when noise dominates signal.

**Override respect.** A rule of `rule_type = 'manual'` is never proposed for change. Tenant intent is final. Learned rules that a Tenant Admin upgrades to manual are likewise frozen.

These guardrails are tracked in `materialization_recommendations.suppression_reason` with explicit codes so Tenant Admins can see what the loop wanted to do but didn't.

---

## 5. Worked Examples

### 5.1 Time decay on Sales Orders

Tenant Acme has connected Salesforce and SAP. The `Transaction.SalesOrder` entity type is published. The policy seeded at publish time:

```
R1  base    priority 0    predicate=TRUE                                    → warm
R2  decay   priority 100  predicate=AGE(created_at) <= '90 days'            → hot
R3  decay   priority 50   predicate=AGE(created_at) > '2 years'             → cold
```

A new order created today resolves to hot via R2. An order from six months ago: R2 doesn't match, R3 doesn't match, R1 wins → warm. An order from three years ago: R3 matches → cold. The same record's level changes over time without any explicit migration — the next time the policy is evaluated against it (during scheduled re-evaluation, see §6), the new level is computed and any necessary cleanup (vector tombstone, Neo4j detach) runs.

### 5.2 Fiscal close boost

The Tenant Admin schedules a fiscal close boost via the M4 admin UI (or the platform reads it from a fiscal calendar service):

```
R4  boost   priority 500
            predicate=entity_type='Transaction.Invoice' AND fiscal_year=2026
            valid_from=2026-11-01 valid_until=2027-02-15
            → hot
```

On October 31, current-fiscal-year invoices that had aged past 90 days were warm via R1. On November 1 at 00:00 UTC, the policy snapshot refreshes; R4 now matches them and outranks R1's priority 0 result. Spark's `materialization-promotion-backfill` job runs against the cohort, projecting them into Elasticsearch, Neo4j, TimescaleDB. On February 15, R4 expires; another scheduled job demotes them back. The decision log records the pair of transitions for audit. The Tenant Admin can adjust `valid_until` if the close runs long.

### 5.3 RLHF promoting a cohort

After three months of operation, the learning job notices the following pattern in tenant Acme:

```
Cohort: Party WHERE industry='Healthcare'
  cohort_size:               1284 records
  query_count_90d:           5421
  fallback_count_90d:        412 (records were warm; queries fell back to source)
  estimated_latency_saved_p95_if_hot:  +1100 ms per query
  incremental_elasticsearch_cost: +$3.20 / month for the cohort
  reward:                    strongly positive
```

It writes:

```
R5  learned priority 200
            predicate=entity_type='Party' AND industry='Healthcare'
            → hot
            learned_metadata={evidence above}
```

Acme's healthcare parties move to hot. The next month's signals include `query_warm_fallback` rates dropping for healthcare parties and `query_hot_hit` rates climbing — the loop's prediction is validated. R5 stays. If the loop had been wrong (fallback rate stayed flat, costs climbed), the next monthly evaluation would propose demotion under the same mechanism.

### 5.4 Manual override

The CFO at Acme says "I want our top-20 strategic accounts hot at all times, regardless of how often anyone queries them." The Tenant Admin enters them as a cohort and a manual rule:

```
COHORT 'strategic_accounts' = entity_type='Party' AND tax_id IN ('US-87-4421938', ...)

R6  manual  priority 1000  predicate=COHORT('strategic_accounts')   → hot
```

R6 outranks every learned rule. The RLHF loop will never propose changing it. If the strategic accounts list rotates, the cohort is updated and the rule follows automatically.

---

## 6. Re-evaluation: Keeping Existing Records Aligned with Policy

A record's level is decided at ingestion via Stage 0. But policy and time both evolve. A record ingested last quarter as hot may have aged past 90 days and now belongs in warm. A new boost rule may apply to records ingested before it existed. The system handles this with a single nightly DAG, `materialization-policy-reevaluate`:

```
For each tenant:
  For each cohort that intersects a rule whose evaluation depends on time
    (decay rules, expiring boosts, new learned rules, expiring boosts):
    1. Identify the records affected (by querying the decision log + Delta Lake).
    2. Re-evaluate the policy against each.
    3. For records whose level changed, emit entity_routed with operation='relevel'.
    4. m3-writer applies the change: project newly-hot records to stores;
       tombstone newly-warm or newly-cold records' projections.
```

The DAG is bounded — it only revisits records whose cohorts could plausibly have changed level (it doesn't blindly reevaluate every record every night). For typical tenant volumes, this is cheap.

A record that has been re-leveled accumulates a row in `materialization_decision_log` with the new rule and timestamp; the decision log thus serves as both audit trail and signal source for the next RLHF cycle.

---

## 7. Migration from v0.1 of the Parent Specs

The v0.1 model (single level per `(tenant, entity_type)` in `cdm_entity_materialization`) is a **subset** of the policy model: every existing assignment becomes a single base rule. The migration is mechanical:

```
For each existing row in cdm_entity_materialization:
  INSERT INTO materialization_policy
    (id, tenant_id, scope, predicate, target_level, priority, rule_type, source)
  VALUES
    (gen_uuid(), tenant_id, cdm_entity_type, 'TRUE', materialization_level,
     0, 'base', 'system')
```

After migration, the existing table is read-only and deprecated. The Op Router and Spark Stage 0 read from `materialization_policy` exclusively. Decay rules are added per entity type whose canonical attributes include a temporal field; sensible defaults seeded from C.1.2's "active customer / quiet customer / archived" pattern. Learned rules are empty at migration time and accumulate as the RLHF loop runs.

---

## 8. Data Model Additions

```sql
CREATE TABLE nexus_system.materialization_policy (
  id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id           UUID NOT NULL,
  scope               VARCHAR(128) NOT NULL,           -- entity type or '*'
  predicate           TEXT NOT NULL,
  target_level        VARCHAR(8) NOT NULL CHECK (target_level IN ('hot','warm','cold')),
  priority            INTEGER NOT NULL DEFAULT 0,
  rule_type           VARCHAR(16) NOT NULL CHECK (rule_type IN ('base','decay','boost','learned','manual')),
  valid_from          TIMESTAMPTZ,
  valid_until         TIMESTAMPTZ,
  source              VARCHAR(32) NOT NULL,            -- 'system' | 'admin' | 'rlhf' | 'fiscal_calendar'
  learned_metadata    JSONB,
  created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_by          VARCHAR(64) NOT NULL,
  superseded_at       TIMESTAMPTZ,                     -- soft-delete; rules are append-only with supersession
  superseded_by       UUID REFERENCES nexus_system.materialization_policy(id)
);
CREATE INDEX idx_mp_tenant_scope_active ON nexus_system.materialization_policy(tenant_id, scope) WHERE superseded_at IS NULL;
CREATE INDEX idx_mp_validity ON nexus_system.materialization_policy(valid_from, valid_until) WHERE superseded_at IS NULL;

CREATE TABLE nexus_system.materialization_cohorts (
  cohort_id     VARCHAR(64) PRIMARY KEY,
  tenant_id     UUID NOT NULL,
  scope         VARCHAR(128) NOT NULL,
  predicate     TEXT NOT NULL,
  display_name  VARCHAR(128) NOT NULL,
  created_by    VARCHAR(64) NOT NULL,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE nexus_system.materialization_decision_log (
  log_id            BIGSERIAL PRIMARY KEY,
  tenant_id         UUID NOT NULL,
  cdm_entity_id     VARCHAR(48) NOT NULL,
  cdm_entity_type   VARCHAR(128) NOT NULL,
  applied_rule_id   UUID NOT NULL,
  target_level      VARCHAR(8) NOT NULL,
  evaluated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  trigger           VARCHAR(32) NOT NULL          -- 'ingest' | 'reevaluate' | 'manual'
) PARTITION BY RANGE (evaluated_at);

CREATE TABLE nexus_system.materialization_signal (
  signal_id          BIGSERIAL PRIMARY KEY,
  tenant_id          UUID NOT NULL,
  cdm_entity_id      VARCHAR(48) NOT NULL,
  cdm_entity_type    VARCHAR(128) NOT NULL,
  signal_kind        VARCHAR(32) NOT NULL,        -- query_hot_hit | query_warm_fallback | query_cold_fallback | query_hot_miss | feedback_explicit
  query_session_id   UUID,
  applied_rule_id    UUID,
  observed_latency_ms INTEGER,
  signal_strength    NUMERIC(4,3),
  observed_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
) PARTITION BY RANGE (observed_at);

CREATE TABLE nexus_system.cost_model (
  tenant_id                  UUID PRIMARY KEY,
  elasticsearch_per_doc_month  NUMERIC(8,5) NOT NULL DEFAULT 0.000025,
  embedding_per_call         NUMERIC(8,5) NOT NULL DEFAULT 0.000020,
  neo4j_per_node_month       NUMERIC(8,5) NOT NULL DEFAULT 0.000010,
  timescale_per_row_month    NUMERIC(8,5) NOT NULL DEFAULT 0.000005,
  reward_weights             JSONB NOT NULL DEFAULT '{"w1":1.0,"w2":1.0,"w3":1.0,"w4":0.5,"w5":0.5}',
  updated_at                 TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

The `materialization_signal` table is high-volume and is partitioned by week with rolling 90-day retention by default. Older data is summarised into the rolling counters maintained by the learning job and discarded.

---

## 9. Open Questions

- **OQ-MPE-01.** Predicate language ergonomics — should rules be authored in the SQL-like grammar above, in JSON (machine-friendly), or in a tiny DSL (human-friendly)? Recommend SQL-like for v0.2; consider a DSL once usage patterns emerge.
- **OQ-MPE-02.** Predicate evaluation against not-yet-ingested records (cold tier) — how does the engine answer "if I query this cold record, what level should it have been?" without ingesting it? Currently a cold record skips Stage 0 entirely. Counterfactual scoring during the RLHF loop would benefit from cataloged-but-not-ingested attributes; out of scope for v0.1.
- **OQ-MPE-03.** Fiscal calendar integration — does NEXUS provide a built-in fiscal calendar service, or does it consume one from the tenant (e.g. via SAP / NetSuite connector metadata)? Recommend the latter; the boost-rule mechanism is general enough to accept calendar-driven rules from any source.
- **OQ-MPE-04.** Cross-tenant signal aggregation for the reward weights — strictly opt-in, with differential-privacy-friendly aggregation (only releasing weights, never per-tenant signals). Defer detail to a security review in v0.2.
- **OQ-MPE-05.** Cohort discovery sophistication — single-attribute groupings cover the obvious cases. Decision-tree mining over query patterns or embedding-cluster discovery would surface non-obvious cohorts. Plan for v0.3 once baseline RLHF is operating cleanly.
- **OQ-MPE-06.** Explicit feedback UX — what does the chatbot actually present, and how is "this answer felt slow" attributed to a specific cohort vs. an LLM-side latency issue? Coordinate with `design:ux-copy` and the M2 executor team.
- **OQ-MPE-07.** Re-evaluation cost at scale — for a tenant with 100M records and 50 active rules, even a bounded re-evaluation pass is expensive. Need a benchmark on the Spark side and a partitioning strategy for `materialization-policy-reevaluate`.
- **OQ-MPE-08.** Reward function transparency to the Tenant Admin — they should see *why* the loop is recommending a change, not just the recommendation. Format proposed: a "change card" showing the cohort, the signals, the reward components, and the projected delta.

---

## 10. References

- `iter2-cdm-to-aistores-pipeline-v0.1.md` §3.0 — Stage 0 materialization gate, refined here.
- `iter2-system-pipeline-orchestration-v0.1.md` §3 — entity-type-level classification model, generalised here.
- `iter2-record-lifecycle-structured-walkthrough-v0.1.md` §8 — single-level lookup, becomes a per-record evaluation here.
- C.1.2.md §1c (Storage Optimization Model) — origin of the materialization concept; this spec extends it with policy and learning.
- `NEXUS-Iter2-RHMA-v0.1.md` — RLHF placeholder for CDM mapping; the loop shape here is analogous.
