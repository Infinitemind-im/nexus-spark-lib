# NEXUS — Iteration 2 · `nexus-airbyte-stream-bridge` · SaaS Polling Bridge
**Service:** `nexus-airbyte-stream-bridge`
**SaaS CDC bridge — poll → delta-compute → Debezium-envelope emit**
Mentis Consulting · Version 0.1 · April 2026 · Draft

> **Topology note:** This service is defined in `NEXUS-Iter2-SVC-nexus-m1-worker-CDCStreaming-v0.1.md` §FR-Dev1-M-04 but was omitted from the 11-service count table in `NEXUS-Iter2-SPEC-ServiceTopology-v0.4.md`. It is a distinct NEXUS microservice. The service topology table should be updated to list it (12 services total in Iteration 2).

**Owner:** Data Intelligence team (Dev 1)
**Depends on:** `nexus_core` v2, Platform M5 (EKS, Kafka, Vault credentials), `nexus-m1-worker` (reads `connector_poll_state` table schema), `m1.int.raw_records` topic provisioned
**Related docs:**
- `developer-workstreams/NEXUS-Iter2-SVC-nexus-m1-worker-CDCStreaming-v0.1.md` §FR-Dev1-M-04 — original requirement
- `architecture/NEXUS-Iter2-SPEC-ServiceTopology-v0.4.md` §Ingestion Tier — ingestion context

---

## 📦 Codebase & Deployment

> **One repository, many deployments.** All NEXUS services live in the `nexus-platform` monorepo. You are contributing a new service module to a shared codebase — not a standalone repository. Shared libraries (`nexus_core` v2, `agent_core` v1) live in `libs/` and are imported across all services. Never duplicate logic that already exists there.

| | |
|---|---|
| **Deployed as** | `nexus-airbyte-stream-bridge` (**NEW** service — new process, new Docker image) |
| **Monorepo path** | `services/nexus-airbyte-stream-bridge/` |
| **Language / runtime** | Python 3.11 · asyncio |
| **Iteration 2 owner** | Dev 1 (scoped within the 4pw CDC Streaming workstream) |
| **Relationship** | Sibling service to `nexus-m1-worker` in the same monorepo. Both live under `services/`. Both import `nexus_core`. The bridge emits events that `nexus-m1-worker` consumes via Kafka — they are decoupled at runtime but co-developed in the same repo. |

---

## Overview

`nexus-airbyte-stream-bridge` is a lightweight polling service that bridges SaaS source systems lacking CDC (no WAL, no binlog) into the NEXUS ingestion pipeline. It polls configured SaaS APIs at a per-connector interval, computes INSERT / UPDATE / DELETE deltas against a stored snapshot, and emits Debezium-envelope-shaped events to the `cdc.*` topic family — exactly as if Debezium had produced them.

From `nexus-spark-transformer` and all downstream services, records from SaaS sources are indistinguishable from Debezium CDC records. The bridge is the only place where the polling vs. streaming distinction is visible.

**When is this service used?**

| Source type | Ingestion mechanism | Why |
|---|---|---|
| PostgreSQL, MySQL, Oracle, SQL Server | Debezium (Kafka Connect) | WAL / binlog available |
| Salesforce, SAP, ServiceNow, REST/GraphQL APIs | **nexus-airbyte-stream-bridge** | No transaction log; API-only access |

---

## Functional Requirements (MoSCoW)

### Must

- **FR-ASB-M-01.** For each registered SaaS connector, poll the configured source API at `poll_interval_seconds` (default: 300s / 5 min). The interval is stored per connector in `nexus_system.connector_poll_state.poll_interval_seconds` and is configurable at runtime without redeployment.
- **FR-ASB-M-02.** After each successful poll, compute the delta against the previous snapshot:
  - Records present in the current poll but absent from the snapshot → emit `op = "c"` (INSERT).
  - Records present in both but with a changed payload (by `updated_at` or payload hash) → emit `op = "u"` (UPDATE).
  - Records present in the snapshot but absent from the current poll → emit `op = "d"` (DELETE).
  - No change → emit nothing (silent).
- **FR-ASB-M-03.** Emit delta events as Debezium-envelope-shaped Kafka messages to `cdc.{source_system}.{tenant_id}.{table}`:
  ```json
  {
    "before": { ... } | null,
    "after":  { ... } | null,
    "source": {
      "connector_id": "...",
      "source_system": "salesforce",
      "table": "Account",
      "ts_ms": 1714220400000
    },
    "op": "c" | "u" | "d",
    "ts_ms": 1714220400000
  }
  ```
  `before` is `null` for INSERT; `after` is `null` for DELETE.
- **FR-ASB-M-04.** Persist poll cursor state after each committed batch in `nexus_system.connector_poll_state`:
  - `last_polled_at` — timestamp of the last successful poll.
  - `last_cursor_value` — high-water mark (`updated_at` or monotonic sequence ID) for incremental polling where the API supports it.
  - `last_snapshot_hash` — hash of the previous full-response snapshot for full-scan connectors.
  - `consecutive_failures` — incremented on error; reset to 0 on success.
  On service restart, resume from the stored cursor. No records are re-fetched unnecessarily.
- **FR-ASB-M-05.** After `consecutive_failures` exceeds `failure_threshold` (default: 3), publish a `nexus.connector.refresh_required` event and pause polling for that connector until an operator resets the state. Do not silently continue polling a failing connector indefinitely.
- **FR-ASB-M-06.** Idempotency: if the same poll is replayed (e.g. due to a transient downstream failure), the delta computed against the stored snapshot must produce identical events. The Kafka producer key is `{tenant_id}:{connector_id}:{source_record_id}` to ensure per-record ordering.
- **FR-ASB-M-07.** Credentials for SaaS sources are fetched from Vault at startup and cached with a TTL of 1 hour. On credential refresh, the service re-fetches without restart.

### Should

- **FR-ASB-S-01.** Support incremental polling (cursor-based) for SaaS APIs that expose `updated_at` or a sequence cursor. Only fetch records modified since `last_cursor_value`. Fall back to full-scan for APIs without cursor support.
- **FR-ASB-S-02.** Expose a `/metrics` endpoint (Prometheus) with: polls per connector per minute, delta sizes (inserts/updates/deletes) per connector, polling latency P95, failure counts per connector.
- **FR-ASB-S-03.** Emit a `nexus.connector.poll_completed` event after each successful poll cycle with connector ID, delta counts, and poll duration — consumed by M6 pipeline health view.

### Could

- **FR-ASB-C-01.** Support webhook registration for SaaS APIs that offer push notifications (e.g. Salesforce Streaming API). When a webhook is active, the poll cycle is suppressed for that connector; the webhook handler emits events directly.

---

## Non-Functional Requirements

| ID | Requirement |
|---|---|
| NFR-ASB-01 | Polling must not run more frequently than `poll_interval_seconds` per connector, regardless of how many bridge instances are running (distributed lock via Redis or PostgreSQL advisory lock) |
| NFR-ASB-02 | A poll failure must not affect other connectors — each connector's polling loop is independent |
| NFR-ASB-03 | Credential values (API keys, OAuth tokens) must never appear in structured logs or Kafka payloads |
| NFR-ASB-04 | The service must handle SaaS API rate limits gracefully: on HTTP 429, back off exponentially and retry before incrementing `consecutive_failures` |

---

## Data Model Ownership

| Table | Access | Notes |
|---|---|---|
| `nexus_system.connector_poll_state` | Read + Write | Primary owner. Stores cursor, snapshot hash, failure count per connector. |
| `nexus_system.connectors` | Read | Reads connector config (poll interval, source system, credential ref). |

---

## Kafka Contracts

| Topic | Role | Notes |
|---|---|---|
| `cdc.{source_system}.{tenant_id}.{table}` | **Publishes** | Debezium-envelope events; consumed by `nexus-m1-worker` raw-capture step |
| `nexus.connector.refresh_required` | **Publishes** | On `consecutive_failures > threshold` |
| `nexus.connector.poll_completed` | **Publishes** | Should — after each successful poll cycle |

---

## Deployment

| Aspect | Value |
|---|---|
| Type | Long-lived Kubernetes `Deployment` |
| Scales on | Number of active connectors (horizontal); each replica handles a subset of connectors via distributed lock |
| Min replicas | 1 |
| Team | Data Intelligence (Dev 1) |
| Service account | `nexus-m1-api-sa` (shared with `nexus-m1-worker` in Iteration 2; reassess for a dedicated SA if secrets isolation is required) |

---

## Acceptance Criteria

- [ ] A Salesforce Account record created via the Salesforce API appears on `cdc.salesforce.{tenant_id}.account` as `op = "c"` within one poll cycle (≤ 5 minutes).
- [ ] A Salesforce Account record updated between two polls appears as `op = "u"` with correct `before` / `after` payloads.
- [ ] A deleted record (absent in current poll, present in previous snapshot) emits `op = "d"` with `before` set and `after = null`.
- [ ] Three consecutive API failures pause polling and emit `nexus.connector.refresh_required`. Other connectors continue polling unaffected.
- [ ] After a service restart, no records are re-emitted for records already committed in `connector_poll_state`.
- [ ] `consecutive_failures` is reset to 0 after a successful poll following a failure sequence.
- [ ] Credentials are never logged in plaintext under any error condition.

---

*NEXUS Iteration 2 · nexus-airbyte-stream-bridge · v0.1 · Mentis Consulting · April 2026 · Confidential*
