# Shared Libraries

Specifications for the two shared Python libraries that underpin all NEXUS services. These libraries must be delivered and stable before any dependent service can begin integration work — they are the Week 1 hard gate for the entire iteration.

---

## Files

**`NEXUS-Iter2-LIB-NexusCore-v0.3.md`**
Specifies the Iteration 2 additions to `nexus_core`, the shared library imported by every NEXUS service for common infrastructure concerns. New in v2: updated Kafka topic definitions and publisher/consumer helpers for all Iteration 2 topics, `CdmEntity` model updates (new fields for materialization level and provenance), the TimescaleDB connection helper, the FXService for currency conversion in financial queries, identity resolution utilities used by the ER-CRUD stream, and per-tenant schema support. Any service that publishes or consumes Kafka events, works with `CdmEntity` objects, or connects to TimescaleDB must import from this library.

**`NEXUS-Iter2-LIB-AgentCore-v0.1.md`**
Specifies `agent_core` v1, the shared library for all AI and LLM work. Provides six components: `LLMClient` (wraps the language model with automatic `llm_audit_log` writes on every call), `EmbeddingClient` (produces 1536-dimensional text embeddings for Elasticsearch), `PIIChecker` (redacts PII from prompts before they reach the LLM), `CDMCatalogueBuilder` (assembles the CDM schema context payload injected into expert prompts), `CrossTenantSafetyScanner` (scans generated responses for cross-tenant data leakage before they are returned to users), and `PromptRegistry` (manages versioned prompt templates for all expert agent roles). A planned v1.1 will add the `agent_core.orchestration` module (Supervisor / ExpertAgent / CriticAgent) required by the RHMA spec.

---

*NEXUS Iteration 2 · Mentis Consulting · April 2026*
