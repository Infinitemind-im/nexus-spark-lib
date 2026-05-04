# NEXUS — Iteration 2 · `nexus-m2-api` · Iteration 2 Delta
**Service:** `nexus-m2-api`
**Status: Breaking role change. No longer the user-facing query entry point.**
Mentis Consulting · Version 0.1 · April 2026 · Draft

**Owner:** AI & Knowledge team
**Baseline spec:** `iteration 1/services/NEXUS-SVC-m2-api.md`
**Related docs:**
- `architecture/NEXUS-Iter2-SPEC-ServiceTopology-v0.4.md` — role change documented (§service table, OQ-M6-01 resolved)
- `query-frontend/NEXUS-Iter2-SVC-nexus-query-api-nexus-query-executor-v0.3.md` — the new user-facing query surface
- `query-frontend/NEXUS-Iter2-SPEC-M6-FrontendDelta-v0.2.md` — M6 routing change away from `nexus-m2-api`

---

## What Changed in Iteration 2

This is a **significant role change**. `nexus-m2-api` is no longer the user-facing entry point for queries. The Iteration 1 spec (`NEXUS-SVC-m2-api.md`) must be read with this change applied.

### Role change summary

| Dimension | Iteration 1 | Iteration 2 |
|---|---|---|
| User-facing query entry point | `nexus-m2-api` `POST /api/v1/query` | `nexus-query-api` `POST /query` |
| WebSocket for user query results | `nexus-m2-api` | `nexus-query-api` |
| M6 "Ask NEXUS" routes to | `nexus-m2-api` | `nexus-query-api` |
| `nexus-m2-api` purpose from Iter 2 | — | **Internal / programmatic only** — semantic interpretation requests from CDM Mapper and Schema Profiler |

### Why

OQ-M6-01 (open in Iteration 1) has been resolved: the Query Engine (`nexus-query-api` + `nexus-query-executor`) is a distinct pipeline from RHMA. It targets live source systems and structured visual output, while RHMA targets M3 stores and returns natural language. Mixing them under a single entry point would have created an undifferentiated surface with incompatible response formats. The two APIs remain separate; unification is deferred to Iteration 3 boundary evaluation (see `NEXUS-Iter2-SVC-nexus-query-api-nexus-query-executor-v0.3.md` §CLARIFY).

---

## Retained Responsibilities (Internal Only)

`nexus-m2-api` still exists and handles these internal call patterns:

| Caller | Route | Purpose |
|---|---|---|
| `nexus-cdm-mapper` | via Kafka `{tid}.m1.semantic_interpretation_requested` | Semantic interpretation of newly classified fields — triggers `nexus-m2-executor` RHMA pipeline |
| `nexus-schema-profiler` | via Kafka `{tid}.m1.semantic_interpretation_requested` | Semantic interpretation after schema drift profiling |
| Internal programmatic API calls | `POST /api/v1/query` (internal network only) | Still accessible for programmatic use within the cluster; **not exposed via Kong to end users** |

The WebSocket relay for `{tid}.m2.agent_response_ready` is retained for internal consumers (e.g. M4 governance workflows that need RHMA output).

---

## Network Policy Change

In Iteration 2, `nexus-m2-api` is removed from the Kong routing table for user-facing paths. The Kong ingress rule for `/api/v1/query` is deleted. The service remains reachable within the cluster on its internal ClusterIP (port 8002) for programmatic calls.

| Direction | Iteration 1 | Iteration 2 |
|---|---|---|
| Ingress from Kong | Yes — user queries | **No** — removed from Kong routing |
| Ingress from cluster-internal | Yes | Yes (unchanged) |

---

## No-Change Confirmation

The following Iteration 1 contracts are confirmed unchanged in Iteration 2:

- `nexus-m2-api` still publishes to `{tid}.m2.knowledge_query` for RHMA pipeline execution.
- `nexus-m2-api` still relays `{tid}.m2.agent_response_ready` via WebSocket for internal consumers.
- `tenant_id` is still sourced from `X-Tenant-ID` (Kong-injected); the header is trusted even on internal calls.
- `nexus_system.query_sessions` RLS pattern is unchanged for sessions created by this service.
- Runtime profile unchanged: 2–5 replicas, CPU + WebSocket connection scaling.

---

*NEXUS Iteration 2 · nexus-m2-api · delta v0.1 · Mentis Consulting · April 2026 · Confidential*
