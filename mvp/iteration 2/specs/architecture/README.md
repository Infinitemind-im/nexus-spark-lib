# Architecture & Design

Cross-cutting specifications that govern the entire system. Every other spec in this iteration must be consistent with these three documents. When two specs disagree on a service boundary, topic name, table definition, or migration number, the files here are the tiebreaker.

---

## Files

**`NEXUS-Iter2-SPEC-ServiceTopology-v0.4.md`**
Defines the three-layer service model (ingestion → processing → query) and the seven inviolable architectural rules. Rule 0 establishes Kong as the sole JWT decoder; the rules collectively enforce tenant isolation, OPA authorisation, and the principle that `nexus-query-api` is the only user-facing query surface. It also owns the Kafka topic registry, consumer group assignments, service account permissions, and network policies. Any new service, topic, or network route must be registered here before implementation begins.

**`NEXUS-Iter2-SPEC-DataModel-v0.5.md`**
The canonical source of truth for every database table added or modified in Iteration 2. Contains the full DDL for all PostgreSQL and TimescaleDB tables, the Flyway migration ledger (V2.0.1–V2.0.19), and the deprecation notice for `pipeline = 'm2'`. The CDM-to-AIStores pipeline series extends the ledger from V2.0.20 onwards. When any other spec defines a table, it must match what is written here.

**`NEXUS-Iter2-ArchReview-v0.2.md`**
A two-pass cross-spec consistency review written once all Iteration 2 specifications existed. It records every conflict found between specs, classifies each as critical or important, and tracks its resolution status. Key decisions recorded here: Elasticsearch replaces Pinecone throughout, `nexus-query-api` is the sole user-facing chat surface (OQ-M6-01 resolved), and the canonical migration numbering adopted from the SprintPlan.

---

*NEXUS Iteration 2 · Mentis Consulting · April 2026*
