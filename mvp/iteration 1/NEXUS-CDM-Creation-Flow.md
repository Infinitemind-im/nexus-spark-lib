# NEXUS — CDM Creation Flow (Iteration 1)
**Module:** M1 + M2 — Data Intelligence & AI Hub
**Version:** 1.0 · March 2026 · Confidential

---

## Purpose

This document describes the end-to-end call path from connector registration through to the creation of a tenant's Canonical Data Model (CDM). It covers every service interaction, Kafka topic, and Airflow task involved in Iteration 1.

Iteration 1 scope: **schema profiling + CDM bootstrap only**. No full-row replication. No incremental sync. The goal is to understand the shape of a source system well enough to produce an initial CDM.

---

## Interaction Graph

```
┌─────────────────────────────────────────────────────────────────────────┐
│ USER / UI                                                               │
│  POST /api/v1/connectors        POST /api/v1/connectors/{id}/sync      │
└───────────────┬─────────────────────────────┬───────────────────────────┘
                │                             │
                ▼                             ▼
┌──────────────────────────────────────────────────────┐
│ nexus-m1-api  (FastAPI)                              │
│  • validates tenant via X-Tenant-ID header           │
│  • stores connector + credentials_secret_path in DB  │
│  • publishes sync request to Kafka                   │
└──────────────────────────┬───────────────────────────┘
                           │ Kafka
                           │ → m1.int.sync_requested
                           ▼
┌──────────────────────────────────────────────────────┐
│ nexus-m1-executor  (Kafka consumer)                  │
│  • consumes m1.int.sync_requested                    │
│  • sole job: bridge Kafka → Airflow REST API         │
│  • commits offset immediately after trigger          │
└──────────────────────────┬───────────────────────────┘
                           │ HTTP POST
                           │ /api/v1/dags/m1_sync_orchestrator/dagRuns
                           ▼
┌──────────────────────────────────────────────────────┐
│ Airflow — m1_sync_orchestrator DAG                   │
│ (backed by nexus-schema-profiler container)          │
│                                                      │
│  1. resolve_credentials                              │
│       GET nexus-ui /credentials/{path}               │
│       → returns { host, port, db, user, pass }       │
│                                                      │
│  2. register_airflow_conn                            │
│       creates temporary Airflow Connection           │
│       scoped to this DAG run                         │
│                                                      │
│  3. extract_and_publish   (PostgresHook)             │
│       discovers all user tables via                  │
│       information_schema — no table list needed      │
│       at registration time                           │
│                                                      │
│       per table publishes to Kafka:                  │
│       ├── m1.int.raw_records                         │
│       │     sample rows (≤ 50) + column profiles     │
│       │     { min, max, avg, null_count,             │
│       │       distinct_values (≤ 50 for low-card) }  │
│       │                                              │
│       └── {tenant}.m1.semantic_interpretation_       │
│               requested                              │
│             schema metadata + sample payload         │
│             structured as a delegated LLM prompt     │
│                                                      │
│  4. save_schema_snapshot                             │
│       POST nexus-m1-api /connectors/{id}/schema      │
│       persists full snapshot for UI + audit          │
│                                                      │
│  5. cleanup_airflow_conn                             │
│       removes temp Connection — always runs          │
│       even if upstream tasks fail                    │
└──────────────────────────┬───────────────────────────┘
                           │
           ┌───────────────┴──────────────────────┐
           │ Kafka                                │ Kafka
           ▼                                      ▼
  m1.int.raw_records               {tenant}.m1.semantic_interpretation_requested
  ┌───────────────────┐            ┌──────────────────────────────────┐
  │ Per table:        │            │ Per table:                       │
  │ • sample_rows     │            │ • table name + column metadata   │
  │ • column_profiles │            │ • data types + cardinality hints │
  │ • row_count       │            │ • sample values                  │
  │                   │            │ • structured as natural language  │
  │ Consumers:        │            │   prompt for the LLM             │
  │ (iteration 2+)    │            └────────────────┬─────────────────┘
  │ analytics,        │                             │
  │ warehouse         │                             ▼
  └───────────────────┘            ┌──────────────────────────────────┐
                                   │ nexus-m2-executor  (LLM agent)   │
                                   │ consumer group:                  │
                                   │   m2-structural-agents           │
                                   │                                  │
                                   │ • only service permitted to      │
                                   │   make LLM API calls             │
                                   │ • calls LLM via agent_core    │
                                   │ • interprets schema semantics    │
                                   │ • proposes CDM field mappings    │
                                   │   (Tier 1 / 2 / 3)              │
                                   │ • M1 has no awareness of this    │
                                   │   step — fire and forget         │
                                   └────────────────┬─────────────────┘
                                                    │ Kafka
                                                    │ → {tenant}.m1.cdm_proposal_created
                                                    ▼
                                   ┌──────────────────────────────────┐
                                   │ nexus-cdm-mapper                 │
                                   │ (KEDA scale-to-zero)             │
                                   │                                  │
                                   │ • classifies fields:             │
                                   │   Tier 1 — exact CDM match       │
                                   │   Tier 2 — fuzzy / mapped        │
                                   │   Tier 3 — unmapped / new        │
                                   │ • deterministic rule-based       │
                                   │   confidence scoring — no LLM    │
                                   │ • publishes CDM proposal         │
                                   └────────────────┬─────────────────┘
                                                    │ Kafka
                                                    │ → {tenant}.m1.cdm_mapping_ready
                                                    ▼
                                   ┌──────────────────────────────────┐
                                   │ nexus-m1-api                     │
                                   │  • persists CDM mapping to DB    │
                                   │  • CDM is now queryable via      │
                                   │    GET /api/v1/cdm               │
                                   └──────────────────────────────────┘
```

---

## Topic Reference

| Topic | Producer | Consumer(s) | Payload |
|---|---|---|---|
| `m1.int.sync_requested` | nexus-m1-api | nexus-m1-executor | connector_id, tenant_id, system_type |
| `m1.int.raw_records` | nexus-schema-profiler (DAG) | *(iteration 2)* | sample rows + column profiles per table |
| `{tid}.m1.semantic_interpretation_requested` | nexus-schema-profiler (DAG) | nexus-m2-executor | schema metadata + sample payload as LLM prompt |
| `{tid}.m1.cdm_proposal_created` | nexus-m2-executor | nexus-cdm-mapper | proposed CDM field mappings with confidence scores |
| `{tid}.m1.cdm_mapping_ready` | nexus-cdm-mapper | nexus-m1-api | finalised CDM mapping ready for persistence |

---

## Profiling Payload — `m1.int.raw_records` (Iteration 1)

In Iteration 1 `raw_records` carries a **profiling sample**, not a full row dump. Its purpose is to give downstream services enough information to build the CDM — not to replicate data.

```json
{
  "connector_id": "2c8a66ee-...",
  "tenant_id":    "acme",
  "table":        "orders",
  "row_count":    84312,
  "sample_rows": [
    { "id": 1, "status": "shipped", "amount": 99.99 }
  ],
  "column_profiles": {
    "status":  { "distinct_values": ["pending","shipped","cancelled"], "nulls": 0 },
    "amount":  { "min": 0.99, "max": 4999.00, "avg": 142.30, "nulls": 12 },
    "country": { "distinct_values": ["US","UK","DE","FR"], "nulls": 0 }
  }
}
```

**Sampling rules:**

| Column cardinality | What is captured |
|---|---|
| Low (< 50 distinct values) | All distinct values |
| High (IDs, free text) | 5–10 representative samples |
| Numeric | min / max / avg / null count |
| Rows per table | ≤ 50 sample rows |

Full replication (all rows, incremental watermarks, CDC) is Iteration 2.

---

## Ownership & Responsibility Boundaries

| Rule | Rationale |
|---|---|
| nexus-m1-executor never calls an LLM | LLM calls belong exclusively to nexus-m2-executor |
| nexus-m2-executor never connects to source DBs | Source access belongs to nexus-schema-profiler |
| nexus-cdm-mapper makes no LLM calls | Classification is deterministic; LLM calls are a cost and latency risk |
| nexus-m1-executor commits Kafka offset before DAG completes | M1 is fire-and-forget; it does not wait for M2 to process the schema |
| Credentials never appear in Kafka messages | Only `credentials_secret_path` travels over Kafka; the DAG resolves the actual secret at runtime |
