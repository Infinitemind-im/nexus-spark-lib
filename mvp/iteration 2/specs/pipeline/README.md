# CDM-to-AIStores Pipeline

Specifications for the end-to-end data pipeline that moves a published CDM entity into the three AI stores (Elasticsearch, Neo4j, TimescaleDB). The five documents in this folder stack in increasing operational depth — start with the architecture doc, then work down.

The pipeline enforces the **Virtual CDM principle**: no raw business field values are written to AI stores, only embeddings and reference tuples. Live values are always fetched from source systems at query time.

---

## Files

**`NEXUS-Iter2-CDM-AIStores-Pipeline-v0.1.md`**
The parent architecture document. It defines the five processing stages (Stage 0 materialization gate + Stages 1–4 for structured records + a parallel document track), how each source operation type (INSERT, UPDATE, DELETE, SNAPSHOT_READ) is handled end-to-end, edge-level provenance for Golden Record fields, and the gating logic that decides whether a record is materialised at all. Read this first.

**`NEXUS-Iter2-RecordLifecycle-Structured-v0.1.md`**
A step-by-step walkthrough of a single Salesforce contact record travelling through the entire pipeline — from a source UPDATE event to completed writes in all three AI stores. Traces all 10 phases with timing (~31 seconds end-to-end), showing how entity resolution scores are computed (Jaro-Winkler 0.945 auto-apply threshold), and how survivorship rules produce the Golden Record. The best document to read when trying to understand how all moving parts connect in practice.

**`NEXUS-Iter2-SystemOrchestration-v0.1.md`**
Specifies the three Spark applications that execute the pipeline (`spark-stream-transformer`, `spark-batch-jobs`, `spark-maintenance`) and the nine Airflow DAGs that coordinate batch work. Contains the materialization classification heuristic (weighted scoring that assigns hot/warm/cold), the Golden Record state machine (CREATE / UPDATE / MERGE / SPLIT / TOMBSTONE transitions), and the per-store routing matrix that controls which entity types go to which stores.

**`NEXUS-Iter2-MaterializationPolicy-v0.1.md`**
Defines the policy engine that assigns materialization levels dynamically rather than using simple per-tenant flags. Introduces five rule types (base, decay, boost, learned, manual), a constrained predicate grammar for writing rules, and a deterministic resolution algorithm for conflicting rules. Policies are time-and-context-aware — a record can be promoted or demoted between tiers as its usage patterns change.

**`NEXUS-Iter2-MaterializationFeatureLearning-v0.1.md`**
Extends the policy engine with a machine-learning loop. Uses LightGBM gradient-boosted trees with counterfactual reward estimation to learn over rule-element features from human feedback signals. Synthesises new rules from dominant decision paths in historical data and surfaces explanations to admins via change cards. Describes the full RLHF training pipeline, feature definitions, and how learned models feed back into `MaterializationPolicy`.

---

*NEXUS Iteration 2 · Mentis Consulting · April 2026*
