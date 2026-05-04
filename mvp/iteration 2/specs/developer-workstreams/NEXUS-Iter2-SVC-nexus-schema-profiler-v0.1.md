# NEXUS — Iteration 2 · `nexus-schema-profiler` · Iteration 2 Delta
**Service:** `nexus-schema-profiler`
**Status: No breaking changes. Schema drift detection extended; inline stats updates from Spark.**
Mentis Consulting · Version 0.1 · April 2026 · Draft

**Owner:** Data Intelligence team
**Baseline spec:** `iteration 1/services/NEXUS-SVC-schema-profiler.md`
**Related docs:**
- `architecture/NEXUS-Iter2-SPEC-ServiceTopology-v0.4.md` — runtime profile (unchanged: K8s Job, scheduled + triggered)
- `architecture/NEXUS-Iter2-SPEC-DataModel-v0.5.md` — `schema_snapshots` table updates
- `developer-workstreams/NEXUS-Iter2-SVC-nexus-m1-worker-CDCStreaming-v0.1.md` §FR-Dev1-M-10 — schema drift detection overlap

---

## What Changed in Iteration 2

`nexus-schema-profiler` is **largely unchanged** in structure and execution model. The Iteration 1 spec (`NEXUS-SVC-schema-profiler.md`) remains the authoritative reference. This document records the Iteration 2 delta only.

| Area | Change | Detail |
|---|---|---|
| Trigger | New trigger added | `nexus.cdm.schema_drift_detected` event (published by `nexus-m1-worker` when Debezium surfaces a column change) triggers an on-demand profiling run for the affected connector, in addition to the existing scheduled and connector-registration triggers |
| Schema snapshots | Write pattern narrowed | `nexus-spark-transformer` now updates `schema_snapshots` inline (cardinality, type stats per micro-batch). `nexus-schema-profiler` retains full-profile authority and overwrites on a scheduled or triggered run, but no longer has exclusive write access to `schema_snapshots` |
| Output event | Unchanged | `{tid}.m1.semantic_interpretation_requested` published on completion — consumed by `nexus-m2-executor` |
| Runtime profile | Unchanged | Kubernetes `Job`; one instance per connector; exits on completion |
| Authentication | Unchanged | `tenant_id` and `connector_id` injected as Job environment variables from Airflow |

---

## Schema Drift Trigger

In Iteration 1, `nexus-schema-profiler` ran on connector registration and on a weekly schedule. In Iteration 2, a third trigger is added:

When `nexus-m1-worker` detects a schema change via the Debezium Connect status API (a column was added, dropped, or had its type changed), it publishes `nexus.cdm.schema_drift_detected` with the `connector_id` and the affected fields. The Airflow DAG `nexus_schema_profile_on_drift` (new in Iteration 2) listens for this event and triggers a `nexus-schema-profiler` Job for the affected connector.

The profiler then re-reads the source schema, updates `schema_snapshots`, and publishes `{tid}.m1.semantic_interpretation_requested` to prompt M2 to propose CDM extensions for the new or changed fields.

---

## Coexistence with `nexus-spark-transformer`

`nexus-spark-transformer` writes incremental cardinality and type statistics to `nexus_system.schema_snapshots` after each micro-batch (best-effort, non-blocking). These inline updates keep the snapshot reasonably fresh between full profiler runs.

`nexus-schema-profiler` performs a full profile on its trigger schedule and overwrites the inline stats with a complete, authoritative snapshot. There is no conflict: the profiler's full run always takes precedence. The Spark inline updates fill the gap between scheduled runs; they do not replace the profiler.

---

## No-Change Confirmation

The following Iteration 1 contracts are confirmed unchanged in Iteration 2:

- One K8s Job instance per `(tenant_id, connector_id)` — no shared state between jobs.
- Credentials fetched from Vault at `nexus/tenants/{tenant_id}/{connector_id}/credentials`.
- `schema_snapshots` write uses `get_tenant_scoped_connection()` with RLS.
- The profiler never writes to CDM tables or Kafka topics other than `{tid}.m1.semantic_interpretation_requested`.

---

*NEXUS Iteration 2 · nexus-schema-profiler · delta v0.1 · Mentis Consulting · April 2026 · Confidential*
