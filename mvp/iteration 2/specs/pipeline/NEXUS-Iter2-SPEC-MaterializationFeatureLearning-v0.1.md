# Iteration 2 — Materialization Feature Learning (RLHF over Rule Elements)

**Version:** v0.1 (draft)
**Date:** 2026-04-27
**Refines:** `iter2-materialization-policy-engine-v0.1.md` §4 (the RLHF loop)
**Scope:** Lifts the RLHF loop from "propose discrete rule edits" to "learn a reward model over rule elements as features." Rules become a human-readable distillation of what the model has learned, not the primitive unit of learning.

---

## 1. The Reframe

The v0.1 policy engine treated rules as the unit of optimisation: the loop proposed a candidate rule (promote Healthcare parties, demote two-year-old orders), computed reward under that rule via counterfactual replay, accepted or rejected. That works for obvious cohorts but it has three limits.

It can only propose what it can enumerate. Single-attribute cohort discovery surfaces obvious patterns (industry, region, segment) but misses interactions — "Healthcare parties *with active opportunities*" might be the actual hot cohort, while Healthcare parties without are merely warm.

It treats rule parameters as fixed knobs. The 90-day cutoff in a decay rule was picked once and only revisited if a Tenant Admin edits it. Whether 90 is the right number for *this* tenant's *this* entity type is a quantitative question the loop should answer.

It can't tell you why. A "promote this cohort" recommendation has no explanation richer than "reward went up." Tenant Admins who are asked to approve high-impact changes deserve to see which observed signals drove the recommendation, in terms of features they recognise.

The reframe is to treat **the elements that appear in rules** — the attributes used in predicates, the thresholds chosen for comparisons, the validity windows attached to boosts, the target levels themselves — as features in a learnable model that predicts reward. Rules become artefacts of the model, generated and retired by it, with the model's feature importances providing the explanation a Tenant Admin needs.

This is a single-step lift, not a redesign. The signal table from v0.1 (`materialization_signal`), the decision log (`materialization_decision_log`), the reward function, the guardrails, and the integration points with Spark and m1-worker are unchanged. What changes is what runs inside the daily learning DAG.

---

## 2. Features

A **feature** is a function from a record (plus its history of decisions and signals) to a value the model can consume. Every attribute that ever appears in a rule's predicate is a feature; so are several derived quantities the loop computes from observation.

### 2.1 Feature taxonomy

| Family | Examples | Source | Type |
|---|---|---|---|
| **Canonical attributes** | `industry`, `region`, `account_segment`, `currency`, `legal_form` | CDM canonical attributes already mapped per record | Categorical (one-hot or target-encoded) |
| **Temporal features** | `age_days`, `days_since_last_query`, `days_to_fiscal_period_end`, `weekday_created`, `month_created` | Derived from canonical timestamp attributes plus tenant calendar | Numeric (continuous or bucketed) |
| **Volumetric features** | `record_size_bytes`, `attribute_completeness`, `connector_count_for_entity` | Computed by Spark Stage 1 and stored on the record | Numeric |
| **Relational features** | `degree_in_graph`, `is_central_party`, `connected_to_strategic_account` | Computed from Neo4j as a periodic snapshot per cohort | Numeric / Boolean |
| **Behavioural features** | `query_count_30d`, `query_count_90d`, `unique_users_querying_30d`, `last_querying_role` | Derived from `materialization_signal` rolled per record/cohort | Numeric |
| **Economic features** | `materialization_cost_per_record`, `embedding_cost_per_record` | Computed from `cost_model` and current store pricing | Numeric |
| **Calendar features** | `is_in_fiscal_close_window`, `is_in_quarterly_review_window` | Derived from tenant fiscal calendar | Boolean |
| **Cross features** | `industry × account_segment`, `age_bucket × region` | Auto-generated up to a depth bound | Categorical |

Features are **registered** in `nexus_system.feature_definitions` (NEW). Each has a stable name, a generator function reference, a type, a tenant scope (most features are tenant-agnostic; some are tenant-specific), and an `enabled` flag. Disabling a feature retires it from training without dropping its history.

### 2.2 Feature derivation, not feature copying

Features are derived on demand by feature generators that read from canonical sources (CDM attributes, signal table, Neo4j snapshots, cost model). The platform does not maintain a separate per-record feature store with fully-materialised vectors; it materialises features lazily during training, then again during scoring at policy-evaluation time, against the canonical truth at that moment. This keeps feature freshness tied to the canonical layer and avoids a second long-lived data copy.

The exception is **rolled behavioural features** like `query_count_90d`, which are too expensive to recompute on the fly. Those are maintained as continuous aggregates against `materialization_signal` (TimescaleDB-style rollups in PostgreSQL), refreshed every 15 minutes.

### 2.3 Auto-discovered features

When a Tenant Admin authors a manual rule referencing an attribute the platform did not previously treat as a feature, the system promotes that attribute to a feature definition automatically. The Admin's act of writing the rule is itself evidence that the attribute matters. The feature is added with `source = 'admin_implicit'` and is included in the next training run.

Symmetrically, when the model finds an attribute with no predictive power across a long window, the feature is flagged for retirement. Tenant Admins can review and confirm.

---

## 3. The Reward Model

### 3.1 Choice of model class

The model is a **gradient-boosted decision tree** (LightGBM by default; XGBoost as a fallback). The choice is deliberate.

Trees handle mixed feature types (categorical, numeric, boolean) without preprocessing pipelines. They produce per-feature importance scores natively, which is the explanation surface Tenant Admins consume. They train fast on the per-tenant data volumes the loop sees (typically tens of thousands to low millions of decision-log rows). They cope well with the missing-feature problem (records that do not carry an attribute are handled, not crashed). They are deeply understood operationally; they fail in legible ways.

Deep learning is out of scope for v0.1. The data volume is small relative to what makes deep learning shine, the interpretability deficit is meaningful given the explanation requirement, and the operational footprint of a tree library is a tiny fraction of the footprint of a serving stack for a neural model. The architecture leaves room for a more capable model class if the tree model saturates.

### 3.2 What the model predicts

Per-tenant, per-entity-type, the model predicts the **expected reward** of materialising a record at a given level, conditional on its features. Concretely, the training target is the reward signal as defined in v0.1 §4.3, computed retrospectively from the decision log over a configurable window (default 90 days):

```
target = w₁ · latency_saved_p95
       - w₂ · storage_cost
       - w₃ · embedding_cost
       + w₄ · explicit_positive_feedback
       - w₅ · explicit_negative_feedback
```

The model is conditional on level. In practice, three models per entity type are trained — one for each candidate level — and the level whose predicted reward is highest for a record's feature vector is the recommended level. Training the three models separately rather than as a single multi-output model keeps the per-class hyperparameters tunable and the explanation simpler.

### 3.3 Counterfactual estimation

The training data is observational, not experimental: every signal in the log was emitted under whatever policy was active at the time. A naive fit would learn the policy that was running, not the policy that ought to be running.

The loop addresses this with **counterfactual reward estimation** at training time. For each decision-log row, the loop estimates what the reward would have been at each of the other two levels using a simple structural model: latency under hot is calibrated from observed `query_hot_hit` latencies; latency under warm and cold is calibrated from observed fallback latencies; storage cost is read from the cost model. The three reward values per row form the per-class training targets.

This is unbiased only under assumptions that are imperfect in the field (latency under hot for a record we never made hot is genuinely uncertain; the calibration borrows from peers in the same cohort). The loop tracks the residual prediction error against actual outcomes after a change is applied; persistent miscalibration triggers a flag in the recommendations UI ("the model has been over-predicting reward for promoted Healthcare parties — consider reverting").

A more rigorous approach is an A/B framework that randomises level assignment for a small slice of new records to generate experimental data. This is tracked as OQ-MFL-04 for v0.2; it requires UX work on the Tenant Admin side because the variance is visible.

### 3.4 Training cadence

Training runs daily as part of the existing `materialization-rlhf-update` DAG, replacing the v0.1 step 3 (compute reward under candidate alternatives). The new step 3 is:

```
3a. Pull decision log + signals for the trailing 90 days.
3b. Compute counterfactual reward targets per row.
3c. Materialise feature vectors for the rows.
3d. Train three GBDT models per (tenant, entity_type), one per level.
    - 80/20 train/validation split, time-based.
    - Early stopping on validation reward.
    - Hyperparameters from a small, tenant-agnostic search space.
3e. Persist model artefacts to nexus_system.reward_models, versioned.
3f. Score the current record corpus under each model;
    propose rules that capture high-importance feature combinations
    (see §4).
```

Training is fast: per (tenant, entity_type) typical training time is single-digit minutes on commodity hardware. The DAG fans out per (tenant, entity_type) for parallelism.

---

## 4. From Learned Models to Readable Rules

The model produces predictions over feature vectors. Tenant Admins read rules. The bridge is a rule-extraction step that summarises the model's behaviour as a small set of high-priority rules.

### 4.1 Rule synthesis from feature importance

After training, the loop computes:

- **Global feature importance** — which features the model uses most (gain or split count).
- **Local feature importance per cohort** — for groups of records with similar predictions, which features distinguish them.
- **Decision paths** — the most common root-to-leaf paths in the trained trees, weighted by how many records they cover.

A small number of decision paths typically dominate. Each dominant path translates directly into a rule predicate: a path like `industry='Healthcare' AND active_opportunity_count >= 1` becomes the predicate of a candidate `learned` rule. The rule's `target_level` is the level whose model gave the highest predicted reward along that path. The rule's `priority` is set in the learned-rule band (default 200).

The rule synthesizer enforces three constraints to keep the output legible:

- **Predicate complexity bound.** No more than four conjuncts and no nested ORs in any synthesized predicate. More complex paths are simplified by keeping the top conjuncts by importance and dropping the rest, even at some cost in fit.
- **Coverage floor.** A rule must apply to at least 1% of the entity type's records (default; tenant-tunable). Below that, it's noise.
- **Distinctness from existing rules.** A synthesized rule that would behave identically to an existing rule on its support set is suppressed.

### 4.2 Threshold selection

When a rule has a numeric predicate (`age_days <= X`), the choice of `X` is itself a learning target. The model gives a continuous risk surface; the rule extraction picks the threshold that maximises reward integrated over the cohort, subject to a stability constraint (the threshold must be within ±15% of the previous run's choice, to prevent jitter). Threshold proposals carry their support — "X=92 days, n=4123 records, expected reward delta +$0.18 per query" — as evidence for the Tenant Admin.

### 4.3 Rule retirement

Existing learned rules whose predicates no longer correspond to high-importance decision paths are candidates for retirement. The loop proposes their retirement explicitly rather than silently superseding them, so the audit trail is clean. Manual rules are never proposed for retirement.

---

## 5. The Explanation Surface

Every recommendation the loop produces — promote a cohort, demote a cohort, change a threshold, retire a rule — comes with a **change card** that is what the Tenant Admin actually sees. The change card combines the model's evidence with the human-readable rule.

The card has four parts:

```
[Recommended change]
   Promote cohort to hot:
     industry = 'Healthcare' AND active_opportunity_count >= 1
   Affected records: 1,284 (4.7% of Party records)

[Why]
   Top features driving this recommendation:
     industry = Healthcare        (importance 0.41)
     active_opportunity_count >= 1 (importance 0.27)
     days_since_last_query <= 14   (importance 0.18)
     other features                (combined 0.14)

   Observed evidence (last 90 days):
     5,421 queries against this cohort
     412 fell back to source (latency penalty p95: +1100 ms)
     0 explicit negative feedback
     17 explicit positive ('this answer was complete')

[Projected impact]
   Latency saved (p95):  +1100 ms × ~60 queries/day = ~66 sec/day
   Storage cost added:   $3.20 / month
   Embedding cost added: $0.45 / month
   Net reward delta:     strongly positive

[Stability]
   This recommendation has been positive across 3 of the last 3 weekly windows.
   Cooldown: not currently in cooldown.
   Manual rule conflict: none.
```

The card is rendered by the M4 admin UI from the JSON payload of `materialization_recommendations`. The same payload is consumable programmatically for tenants who want to wire approvals into their own change-management process.

---

## 6. Worked Example: Discovering an Interaction

Tenant Acme has a base rule keeping `Party` warm and a learned rule promoting Healthcare parties to hot (from v0.1's example). After three months of operation under the feature-learning loop, the GBDT model finds something new.

**The model's view.** In the trained `hot` model for `Party`, the top-three features by gain are:

```
1. industry == 'Healthcare'                     gain 0.31
2. active_opportunity_count                     gain 0.24
3. days_since_last_executive_briefing           gain 0.19
```

Feature 3 is unexpected. It's a derived feature the platform created from observing that an executive-tier user role had been querying Parties in the days following an entry in a `Sales.ExecutiveBriefing` event type. The feature was admitted automatically when an Admin authored a rule mentioning `last_executive_briefing` last quarter; the model picked it up and found it predicted query volume better than several attributes the Admin actually wrote rules around.

**The rule the synthesizer extracts.** The dominant decision path for high predicted reward is:

```
days_since_last_executive_briefing <= 21 AND
(industry == 'Healthcare' OR active_opportunity_count >= 1)
→ hot
```

The path is too complex for the four-conjunct bound, so it's simplified to:

```
days_since_last_executive_briefing <= 21 AND active_opportunity_count >= 1
→ hot
```

with the Healthcare branch left to the existing rule.

**The change card** shows the threshold (`<= 21`) along with the support count and the expected reward delta. The Admin sees that the model has noticed something they had not — that recent executive attention is itself a leading indicator of forthcoming queries — and approves the rule.

The healthcare rule still applies; the new rule applies on top, hitting Healthcare parties with both signals doubly. This is fine: the level resolution algorithm picks the highest-priority match and both rules push the level to hot. There is no over-counting because the level is binary-discrete, not additive.

---

## 7. Implications for the Existing Loop

The v0.1 loop's structure (signals → counters → reward → guardrails → recommendations → broadcast refresh) is preserved. What runs differently:

- **Step 3 of the daily DAG** is replaced as described in §3.4. The existing reward function and weights stay; they are now applied per row in the decision log to compute training targets.
- **Cohort discovery** is no longer a hand-coded enumeration of single-attribute groupings. It is an output of the model's decision-path analysis. The v0.1 enumeration becomes a fallback for tenants whose data is too sparse to train a useful model in the first 30 days of operation.
- **Recommendations payload** carries feature importance and decision-path metadata, surfaced in the change card.
- **Feedback loop on feedback** — a Tenant Admin who rejects a recommendation provides labelled training signal for the *next* model: the model is asked to learn that whatever pattern the rule represented is *not* what the tenant wants, even if the reward calculation suggested it was. This is implemented as a small adversarial regulariser that down-weights features dominant in rejected recommendations during training. (See OQ-MFL-05.)

The guardrails are unchanged, with one addition: a **feature-stability guardrail** that suppresses recommendations whose top features have low stability across consecutive training runs (high churn in feature importance suggests the model is fitting noise). The guardrail uses a simple Spearman rank correlation between consecutive runs' importance vectors.

---

## 8. Data Model Additions

```sql
CREATE TABLE nexus_system.feature_definitions (
  feature_id        VARCHAR(64) PRIMARY KEY,
  tenant_id         UUID,                                -- NULL = platform-global
  family            VARCHAR(32) NOT NULL,                -- canonical | temporal | volumetric | relational | behavioural | economic | calendar | cross
  scope             VARCHAR(128),                        -- entity type or NULL for all
  generator_ref     VARCHAR(255) NOT NULL,               -- importable Python path
  data_type         VARCHAR(16) NOT NULL CHECK (data_type IN ('numeric','categorical','boolean')),
  source            VARCHAR(32) NOT NULL,                -- 'system' | 'admin_implicit' | 'auto_discovered'
  enabled           BOOLEAN NOT NULL DEFAULT TRUE,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  retired_at        TIMESTAMPTZ
);

CREATE TABLE nexus_system.reward_models (
  model_id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id         UUID NOT NULL,
  cdm_entity_type   VARCHAR(128) NOT NULL,
  target_level      VARCHAR(8) NOT NULL CHECK (target_level IN ('hot','warm','cold')),
  trained_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  training_window   TSTZRANGE NOT NULL,
  n_samples         INTEGER NOT NULL,
  validation_score  NUMERIC(8,4) NOT NULL,
  artefact_uri      VARCHAR(255) NOT NULL,               -- s3:// path to the LightGBM model file
  feature_importance JSONB NOT NULL,                     -- top-k feature → importance score
  superseded_by     UUID REFERENCES nexus_system.reward_models(model_id)
);

CREATE TABLE nexus_system.feature_importance_history (
  tenant_id         UUID NOT NULL,
  cdm_entity_type   VARCHAR(128) NOT NULL,
  target_level      VARCHAR(8) NOT NULL,
  evaluated_at      TIMESTAMPTZ NOT NULL,
  feature_id        VARCHAR(64) NOT NULL,
  importance        NUMERIC(8,5) NOT NULL,
  rank              INTEGER NOT NULL,
  PRIMARY KEY (tenant_id, cdm_entity_type, target_level, evaluated_at, feature_id)
);

ALTER TABLE nexus_system.materialization_recommendations
  ADD COLUMN evidence_model_id UUID REFERENCES nexus_system.reward_models(model_id),
  ADD COLUMN explanation_payload JSONB;       -- the change card content
```

The `artefact_uri` points to the serialised LightGBM model in object storage (S3, MinIO). Models are tenant-scoped and are not commingled across tenants; cross-tenant aggregation, if introduced (OQ-MPE-04 from the parent spec), happens only at the level of importance vectors with appropriate aggregation, never at model artefact level.

---

## 9. Open Questions

- **OQ-MFL-01.** Feature engineering library — write a small in-house `nexus_features` library wrapping LightGBM and the generator registry, or adopt Feast / Tecton? Recommend in-house for v0.1 — the surface is small, the dependencies of mature feature stores are heavy, and the requirements (no separate persistent feature store, lazy materialisation) cut against off-the-shelf assumptions. Re-evaluate at scale.
- **OQ-MFL-02.** Categorical encoding — target encoding (mean target value per category) is more efficient than one-hot for high-cardinality categoricals like `connector_id`. Target encoding with a hierarchical Bayesian smoother is recommended; verify against actual cardinalities once observed.
- **OQ-MFL-03.** Cross-feature depth — how deep should auto-generated cross-features go? Default depth 2 (pairs) is proposed. Depth 3 explodes the feature space; only enable per-tenant after benchmarking.
- **OQ-MFL-04.** A/B framework — should the loop randomise level assignment for a small slice of new records to generate experimental (rather than purely observational) data? Recommend yes for v0.2; design the UX so the variance is explainable to Tenant Admins ("for one in twenty new orders, we deliberately picked a level the model didn't recommend, to keep its judgement honest").
- **OQ-MFL-05.** Adversarial regulariser on rejected recommendations — the precise formulation needs benchmarking. Risks underweighting features that are right but unpopular. Recommend coupling with an "I disagree but apply anyway" Admin action that does not feed the regulariser.
- **OQ-MFL-06.** Multi-objective reward — currently reward is a single scalar from a weighted sum. Some tenants will care about latency dominantly, others about cost. The reward model could be vector-valued with a tenant-specified Pareto preference at decision time. Plan for v0.3.
- **OQ-MFL-07.** Privacy-preserving cross-tenant feature importance — if cross-tenant aggregation is enabled, only release importance vectors with differential-privacy noise, never raw feature values. Coordinate with security review.
- **OQ-MFL-08.** Drift detection — when the model's residual prediction error on actual outcomes climbs persistently, what triggers retraining (or a switch to fallback rule-based scoring)? Recommend a simple PSI threshold per feature with operator-tunable sensitivity.

---

## 10. References

- `iter2-materialization-policy-engine-v0.1.md` — parent spec; this document refines its §4.
- `iter2-system-pipeline-orchestration-v0.1.md` — daily DAG hosting the training step.
- `iter2-cdm-to-aistores-pipeline-v0.1.md` — Stage 0 evaluation point that consumes the resulting policy.
- `NEXUS-Iter2-RHMA-v0.1.md` — RLHF placeholder for CDM mapping classification; this document treats materialization analogously and the two loops should converge on shared infrastructure in v0.3.
- C.1.1.md — the project's framing of "human feedback is captured throughout the platform and used to continuously refine its behavior" — this spec is one concrete instance.
