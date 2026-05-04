# NEXUS — Iteration 2 · Architecture Review (Supplementary)
**Follow-up pass after v0.1 — covers late-arriving specs and residual gaps**
Mentis Consulting · Version 0.2 · April 2026 · Confidential

---

## Purpose

This document is a supplement to `NEXUS-Iter2-ArchReview-v0.1.md`. v0.1 reviewed the five Iteration 2 specifications that existed as of March 2026. Since then, three additional specs landed (CDM Mapper v2, CDM Validation Workflow v2, RHMA v2), and Service Topology / DataModel / M3 AIStores were revised to v0.2 / v0.3 / v0.3 respectively. This pass verifies v0.1 findings were absorbed correctly and surfaces new issues introduced by the late-arriving specs.

Findings use the same classification as v0.1:
- 🔴 **Critical** — will produce incorrect or non-functional code if not resolved
- 🟡 **Important** — integration pain but not a system failure
- 🟢 **Confirmed** — alignment verified

Two mechanical issues found during this pass have already been patched in place — see the "Applied in this pass" section at the end.

---

## Status of v0.1 Findings

| v0.1 Finding | Status | Notes |
|---|---|---|
| Critical 1 — Missing continuous write path to M3 | ✅ Resolved | Service Topology v0.2 adds `{tid}.m1.entity_routed`; M3 AIStores v0.3 consumes it as primary trigger |
| Critical 2 — Two "mapping approved" topic names | ⚠ Partially resolved | Topology and writer-specific specs use `{tid}.m4.mapping_approved` consistently, but M3 AIStores v0.3 still had three stale `{tid}.m1.mapping.approved` references. **Patched in v0.4 — see end of document.** |
| Critical 3 — Post-synthesis cross-tenant safety scan | ✅ Resolved | `CrossTenantSafetyScanner` in `agent_core` §6; Query Engine and RHMA v2 both invoke it |
| Important 4 — `identity_mapping` enforcement | ⚠ Partially resolved | Schema now defined in DataModel; but OQ-AR-02 / OQ-DM-06 still open (Iteration 1 seeding uncertainty); Query Planner spec carries `[CLARIFY]` blocking Rule 6 implementation |
| Important 5 — Kong JWT invariant as architectural rule | ✅ Resolved | Rule 0 added to Service Topology v0.2 |
| Important 6 — OQ-M6-01 escalation | ✅ Resolved | Option A confirmed (April 2026 Architecture Review). `nexus-query-api` is the sole user-facing chat surface. M2 RHMA chat panel deprecated. See Finding B actions completed in: ServiceTopology v0.4, DataModel v0.5, M6 Frontend v0.2, Frontend v0.1, SprintPlan v0.3. |
| Important 7 — Tier confidence system not referenced in M3 spec | ✅ Resolved | M3-FR-01/02/03 in v0.3 explicitly state Tier 2/3 fields do not block writes |

---

## 🔴 Critical Finding A — Migration Numbering Conflict Between SprintPlan and DataModel

**Affected docs:** `NEXUS-Iter2-SprintPlan-v0.2.md`, `NEXUS-Iter2-DataModel-v0.3.md` (now v0.4)

### The problem

The two specs disagree on which table is assigned to which migration number for V2.0.3 through V2.0.8. Both orderings are internally consistent, but they are mutually incompatible:

| # | SprintPlan v0.2 | DataModel v0.3 |
|---|---|---|
| V2.0.4 | `cdm_proposals` (documents existing Iter-1 table) | `dashboard_components` |
| V2.0.5 | `query_sessions` ALTER | `cdm_catalogue_cache_log` |
| V2.0.6 | `dashboard_components` | `identity_mapping` |
| V2.0.7 | `cdm_catalogue_cache_log` | `neo4j_indexes.cypher` |
| V2.0.8 | `identity_mapping` | *(absent — missing from ledger)* |

### Impact

Dev 1 cannot author Flyway migration files from either document alone — SprintPlan's D1-02 acceptance criteria reference its numbering, while DataModel is supposed to be the "single source of truth for migration sequencing" (per README). Whichever one Dev 1 picks, the other team's cross-references break.

### Required changes

Tech Lead picks one canonical numbering. **Recommendation:** adopt SprintPlan v0.2's numbering, on the grounds that (a) it was written more recently, (b) it treats `cdm_proposals` as V2.0.4 (documenting an existing Iter-1 table) which is semantically cleaner than threading it through V2.0.9's ALTER, and (c) D1-02's acceptance criteria already reference it. Then reflow the DataModel.

The V2.0.9–V2.0.17 range introduced by CDM Mapper v2 / CDM Validation v2 / RHMA v2 is unambiguous and not affected by this decision.

Tracking: **OQ-DM-07** (added to DataModel v0.4).

---

## 🟢 Finding B — RESOLVED — RHMA v2's User-Facing Chat Deprecation Propagated

**Affected docs:** `NEXUS-Iter2-RHMA-v0.1.md` (FR-19), `NEXUS-Iter2-SPEC-ServiceTopology-v0.4.md` ✅, `NEXUS-Iter2-SPEC-DataModel-v0.5.md` ✅, `NEXUS-Iter2-M6-FrontendDelta-v0.2.md` ✅, `NEXUS-Iter2-Frontend-v0.1.md` ✅, `NEXUS-Iter2-SprintPlan-v0.3.md` ✅

### What the late-arriving spec says

RHMA v2 FR-19 states: *"Reopening RHMA as a user-facing chat (M6 routes users to `nexus-query-api`; M4 workbench uses the CDM Validation LLM)."* This makes `nexus-query-api` the sole user-facing query surface and relegates `nexus-m2-api` to internal knowledge retrieval (semantic interpretation, CDM governance, workflow triggers).

### What the other specs say

- **Service Topology v0.2** lists `nexus-m2-api` as a Kong-fronted entry point with no annotation that its user-facing role has been deprecated.
- **DataModel** retains the `pipeline` discriminator (`'m2'` vs `'query'`) in `query_sessions`. v0.1 of the ArchReview called this a "hedge" — but if RHMA v2 FR-19 is now authoritative, `'m2'` should be marked legacy/internal-only.
- **M6 Frontend v0.2** escalated OQ-M6-01 to "Decision Required" with a recommended merger, but the recommendation has not been formally accepted. The presence of RHMA v2 FR-19 effectively answers OQ-M6-01 — but only if RHMA v2 itself is accepted as authoritative.

### Resolution (April 2026)

RHMA v2 FR-19 is confirmed as authoritative. OQ-M6-01 is settled — `nexus-query-api` is the sole user-facing chat surface.

**All required changes have been applied:**
- **ServiceTopology v0.4:** `nexus-m2-api` annotated as "internal/programmatic only — no user-facing routes post-Iter-2" ✅
- **DataModel v0.5:** `query_sessions.pipeline = 'm2'` marked DEPRECATED; new rows default to `'query'` ✅
- **M6 Frontend v0.2:** OQ-M6-01 resolved as Option A confirmed; `<M2ChatPanel>` deprecated ✅
- **Frontend v0.1:** Decision confirmed section updated; D6-08 task updated ✅
- **SprintPlan v0.3:** D6-08 task and OQ tracker updated ✅

---

## 🟡 Important Finding C — `agent_core` v1.0 Import Table Is Stale for `nexus-m2-executor`

**Affected docs:** `NEXUS-Iter2-LIB-AgentCore-v0.1.md` (Services That Import agent_core), `NEXUS-Iter2-RHMA-v0.1.md`

### The problem

`agent_core` v1.0 lists `nexus-m2-executor` as importing only `LLMClient`. But RHMA v2 (which is the authoritative spec for what M2-executor now does) requires:

- `LLMClient` ✓ (listed)
- `PromptRegistry` (RHMA FR-03 — expert prompt templates)
- `PIIChecker` (RHMA NFR-06 — "0 PII in prompts")
- `CDMCatalogueBuilder` (implicit in expert `cdm_catalogue_lookup` tool)
- `CrossTenantSafetyScanner` (RHMA FR-10)
- `agent_core.orchestration` — the new `Supervisor` / `ExpertAgent` / `CriticAgent` module, which **does not exist in `agent_core` v1.0 at all**. RHMA's "Dependencies & Sprint Positioning" note acknowledges this: *"`agent_core.orchestration` is a net-new module — add to `NEXUS-Iter2-LIB-AgentCore-v0.1.md` as §7, or ship as a v1.1 minor."*

### Impact

Two concrete consequences:

1. `agent_core` v1.0's scope as currently spec'd is too small for RHMA v2 to land in Gate 3 without a concurrent `agent_core` update. If the `agent_core` team ships v1.0 per spec, RHMA v2 will block on the missing orchestration module.
2. The import table understates which services depend on which `agent_core` components, which distorts impact analysis for future `agent_core` changes.

### Required changes

Pick one of:
- **(a)** Extend `agent_core` v1.0 spec to include the orchestration module as §7, update the import table, keep v1.0 as the single delivery.
- **(b)** Ship `agent_core` v1.0 as spec'd, plan a v1.1 minor for the orchestration module gated to Gate 3. Update the import table to flag that `nexus-m2-executor` needs v1.1.

Either is fine; (a) is simpler, (b) is cleaner if the AI & Knowledge team wants a stable v1.0 to build experts against before the orchestration module freezes.

---

## 🟡 Important Finding D — Residual Blocking OQs From v0.1 Still Unresolved

**Affected docs:** v0.1 open questions section, Query Planner spec, M3 Writer specs

### Summary

v0.1 tagged four open questions as blocking and four as non-blocking. Status now:

| OQ | Status | Blocking whom |
|---|---|---|
| OQ-AR-01 (entity_routed topic publisher) | ✅ Resolved — Option A adopted in Service Topology v0.2 | — |
| OQ-AR-02 (identity_mapping schema) | ⚠ Partially resolved — schema defined in DataModel, but Iteration 1 seeding status still `[CLARIFY]`. Query Planner spec carries `[CLARIFY: OQ-QE-06 — confirm identity_mapping is seeded]` | Blocks Dev 5 (Query Planner) Gate 3 |
| OQ-AR-03 (safety scan sidecar vs same-process) | ✅ Resolved — same-process (default accepted) | — |
| OQ-AR-04 (write Tier 2 immediately vs hold) | ✅ Resolved — write immediately (default accepted) | — |
| OQ-M3-DEL-01 (entity deletion contract) | ❌ Open | Blocks all three M3 writer specs (6 cross-references) — flagged for Dev 2A/2B/2C Gate 2 |
| OQ-M3-BF-01 (backfill scope Tier 1 vs 1+2) | ❌ Open | Blocks D2A-02 / D2B-02 / D2C-02 backfill jobs |
| OQ-TENANT-01 (threshold change → re-classify?) | ❌ Open, referenced by CDM Mapper v2 | Non-blocking (policy decision, not code) |

### Impact and recommendation

**OQ-M3-DEL-01** is the highest-impact unresolved item. Neo4j Writer, Pinecone Writer, and TimescaleDB Writer all block their deletion-handling tasks on it, and it's referenced as a dependency on six task items across those three specs. If it is not resolved before Gate 2 (Week 5), at least three developer-weeks of deletion-related work will stall.

**OQ-M3-BF-01** (backfill scope) is smaller but blocks the same three writers at Gate 2.

**Recommendation:** schedule a short Tech Lead + Data Intelligence team sync before Week 4 to close OQ-M3-DEL-01, OQ-M3-BF-01, OQ-AR-02 (identity_mapping seeding), and the v0.2 Finding A (migration numbering). None of these need more than an hour of discussion — they're all "pick an option and document it."

### Secondary issue — duplicate OQ IDs

OQ-AR-01 as defined in v0.1 (entity_routed publisher) and OQ-AR-01 as used in `NEXUS-Iter2-M3-Neo4j-Writer-v0.1.md` (Neo4j tenant isolation) share an identifier but refer to different questions. The Neo4j Writer spec should use OQ-M3-02 (which is the correct identifier per M3 AIStores v0.3).

---

## 🟢 Confirmed Alignments (new to this pass)

| Area | Spec | Verdict |
|---|---|---|
| `{tid}.m4.mapping_approved` consistently cited across Topology, M3 AIStores v0.4, and writer-specific specs | ServiceTopology v0.2, M3 v0.4, writer specs | ✅ Correct (after v0.4 patch) |
| `{tid}.m1.entity_routed` as primary continuous write path | M3 v0.4, writer specs, Topology | ✅ Correct |
| Three-layer dependency model with `nexus_core` as Phase 1 gate | ServiceTopology v0.2 | ✅ Correct |
| Rule 0 (Kong sole JWT decoder) + Rules 4/5/6 (executor, OPA, identity) | ServiceTopology v0.2 | ✅ Correct |
| CDM Mapper v2 migrations are sequential and non-overlapping with Validation v2 and RHMA v2 (V2.0.9–V2.0.17) | All three late specs | ✅ Correct |
| CrossTenantSafetyScanner is invoked both by Query Engine and RHMA v2 before publishing results | agent_core v1.0 §6, RHMA FR-10, Query Engine QE-FR-15 | ✅ Correct |
| `cdm_feedback` shared table ownership (defined by CDM Mapper v2, consumed by CDM Validation v2 and future RLHF) | CDM Mapper v2 V2.0.12, Validation v2 VAL2-FR-04 | ✅ Correct |

---

## Summary of Required Actions

### Tech Lead (unblocking decisions — before Week 4)

1. Resolve OQ-DM-07 (Finding A) — pick canonical migration numbering
2. Resolve OQ-M3-DEL-01 — pick entity deletion contract
3. Resolve OQ-M3-BF-01 — pick backfill scope
4. Resolve OQ-AR-02 / OQ-DM-06 — confirm `identity_mapping` Iteration 1 seeding status
5. Decide RHMA v2 FR-19 (Finding B) — is user-facing chat fully migrated to nexus-query-api?

### Spec updates after decisions

6. ServiceTopology v0.2 → v0.3: annotate `nexus-m2-api` user-facing scope per Finding B outcome
7. M6 Frontend v0.2 → v0.3: close OQ-M6-01 with accepted resolution
8. agent_core v1.0 spec: add §7 orchestration module or plan v1.1 minor (Finding C)
9. M3 Writer specs: update OQ-AR-01 duplicate reference in Neo4j Writer

### Applied in this pass (no further action needed)

- ✅ **M3 AIStores v0.3 → v0.4** — three stale `{tid}.m1.mapping.approved` references at lines 96, 160, 316 replaced with the correct topic names (`{tid}.m1.entity_routed` for the primary write path, `{tid}.m4.mapping_approved` for catch-up triggers). No schema or functional changes.
- ✅ **DataModel v0.3 → v0.4** — migration ledger extended through V2.0.17, with schema ownership delegated to the CDM Mapper v2 / CDM Validation v2 / RHMA v2 specs (no DDL duplication). OQ-DM-07 added to surface the V2.0.3–V2.0.8 numbering conflict with SprintPlan v0.2.

---

*NEXUS Iteration 2 · Architecture Review Supplement · v0.2 · Mentis Consulting · April 2026 · Confidential*
