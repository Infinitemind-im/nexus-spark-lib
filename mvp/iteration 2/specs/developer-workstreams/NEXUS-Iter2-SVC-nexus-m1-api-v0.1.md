# NEXUS — Iteration 2 · `nexus-m1-api` · Iteration 2 Delta
**Service:** `nexus-m1-api`
**Status: No breaking changes. One new endpoint (connector refresh).**
Mentis Consulting · Version 0.1 · April 2026 · Draft

**Owner:** Data Intelligence team
**Baseline spec:** `iteration 1/services/NEXUS-SVC-m1-api.md`
**Related docs:**
- `architecture/NEXUS-Iter2-SPEC-ServiceTopology-v0.4.md` — runtime profile (unchanged: 1–3 replicas, CPU scaling)
- `architecture/NEXUS-Iter2-SPEC-DataModel-v0.5.md` — new tables readable by this service

---

## What Changed in Iteration 2

`nexus-m1-api` is **unchanged** in its core contract. The Iteration 1 spec (`NEXUS-SVC-m1-api.md`) remains the authoritative implementation reference. This document records the Iteration 2 delta only.

| Area | Change | Detail |
|---|---|---|
| Endpoints | One new endpoint added | `POST /api/v1/connectors/{connector_id}/refresh` — triggers a connector credential refresh cycle (publishes `nexus.connector.refresh_required` event); required to reset `consecutive_failures` in `nexus-airbyte-stream-bridge` |
| Endpoints | Unchanged | `POST /api/v1/connectors`, `GET /api/v1/connectors`, `POST /api/v1/connectors/{id}/sync`, `GET /api/v1/sync-jobs/{id}` |
| Kafka topics produced | Unchanged | `m1.int.sync_requested` |
| DB tables | `nexus_system.connector_batch_state` — now readable (new in DataModel v0.5) | Used in `GET /api/v1/connectors/{id}` response to surface `last_cursor_value` and `consecutive_failures` |
| Authentication | Unchanged | Kong injects `X-Tenant-ID`; no JWT decoding in-service |
| Runtime profile | Unchanged | 1–3 replicas, CPU scaling |

---

## New Endpoint: Connector Refresh

```
POST /api/v1/connectors/{connector_id}/refresh
X-Tenant-ID: {tenant_id}   (Kong-injected)
X-User-Role: admin          (required)
```

**Purpose:** Resets the `consecutive_failures` counter in `nexus_system.connector_poll_state` and publishes `nexus.connector.refresh_required` to resume a paused `nexus-airbyte-stream-bridge` polling loop. Also used to trigger a Vault credential rotation for Debezium connectors.

**Response:** `202 Accepted` with `{"connector_id": "...", "status": "refresh_queued"}`.

**Authorization:** Restricted to `admin` role (`X-User-Role: admin`). Non-admin requests return `403 Forbidden`.

---

## No-Change Confirmation

The following Iteration 1 contracts are confirmed unchanged in Iteration 2:

- Tenant identity always from `X-Tenant-ID` header (Rule 0 — Kong-injected).
- All PostgreSQL access via `get_tenant_scoped_connection()` with RLS.
- `nexus-m1-api` never calls source systems directly.
- `nexus-m1-api` never decodes JWTs.
- Schema profiler job is still triggered via `nexus-schema-profiler` K8s Job on connector registration; `nexus-m1-api` triggers the job dispatch but does not run profiling itself.

---

*NEXUS Iteration 2 · nexus-m1-api · delta v0.1 · Mentis Consulting · April 2026 · Confidential*
