# CDM Governance

Specifications for the workstreams responsible for classifying, validating, and reasoning over Canonical Data Model mappings. These three specs cover the full governance lifecycle: automated classification (CDM Mapper), human-in-the-loop validation (CDM Validation Workflow), and AI-assisted expert reasoning (RHMA).

**Important:** "Tier 1 / 2 / 3" in these specs refers exclusively to CDM mapping confidence levels. This is distinct from the hot/warm/cold materialization levels defined in the pipeline specs.

---

## Files

**`NEXUS-Iter2-CDM-Mapper-v0.3.md`**
Specifies the Iteration 2 improvements to the CDM mapper service. The main additions are idempotent classification using natural-key UPSERTs (rerunning the mapper on the same record is always safe and produces the same result), a ground-truth validation harness for testing mapping quality against a labelled dataset, and an RLHF placeholder that captures human corrections for use in a future training loop. Tier 1 mappings are high-confidence auto-approvals; Tier 2 and 3 require human review but do not block writes to AI stores.

**`NEXUS-Iter2-CDM-Validation-Workflow-v0.1.md`**
Introduces four new endpoints on `nexus-m4-api` that give data stewards tooling to evaluate mapping proposals before committing to them: a simulate dry-run (shows downstream impact without writing anything), an LLM-powered recommendation engine (returns a rationale and top-3 alternative CDM fields), a structured decision endpoint (approve/reject with a required reason), and a CDM version diff view (field-level comparison between two CDM versions). Human approval is always required — auto-approval is explicitly prohibited in Iteration 2. All LLM calls are routed through `agent_core.LLMClient` and `PIIChecker` for auditability.

**`NEXUS-Iter2-RHMA-v0.1.md`**
Specifies the Reflexive Hierarchical Multi-Agent system that runs inside `nexus-m2-executor`. Introduces the `agent_core.orchestration` module with three agent types: a Supervisor that decomposes incoming queries into sub-tasks, five domain ExpertAgents (Finance, HR/People, Customer/CRM, Time-Series, Graph) that answer sub-tasks using specialist prompts and CDM catalogue context, and a CriticAgent that reviews expert outputs for accuracy and cross-tenant safety before they are returned. In Iteration 2 the dispatch infrastructure is scaffolded — interfaces, tables, and Kafka events are in place but no agent logic executes yet. Full dispatch begins after Iteration 3.

---

*NEXUS Iteration 2 · Mentis Consulting · April 2026*
