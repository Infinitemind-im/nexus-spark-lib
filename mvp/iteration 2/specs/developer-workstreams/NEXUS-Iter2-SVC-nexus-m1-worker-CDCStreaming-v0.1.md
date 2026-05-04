# NEXUS — Iteration 2 · `nexus-m1-worker` · Real-Time CDC Streaming Ingestion

**Service:** `nexus-m1-worker` (extended) · `nexus-spark-transformer` (deployment owned here) · `nexus-airbyte-stream-bridge` (new)
**Version:** v0.1 (draft)
**Date:** 2026-04-27
**Companion to:** `iter2-dev-overview-and-registers-v0.1.md` (cross-cutting contracts)

---

## 📦 Codebase & Deployment

> **One repository, many deployments.** All NEXUS services live in the `nexus-platform` monorepo. You are contributing modules within a shared codebase — not isolated repositories. Shared libraries (`nexus_core` v2, `agent_core` v1, `nexus_spark_lib`) live in `libs/` and are imported across all services. Never duplicate logic that already exists there.

| | |
|---|---|
| **Deployed as** | `nexus-m1-worker` · `nexus-airbyte-stream-bridge` (two separate service processes) |
| **Monorepo paths** | `services/nexus-m1-worker/` · `services/nexus-airbyte-stream-bridge/` |
| **Language / runtime** | Python 3.11 · asyncio · PySpark 3.5 (Spark Structured Streaming application) |
| **Iteration 2 owner** | Dev 1 (4 person-weeks) |
| **Relationship** | `nexus-m1-worker` is an existing service being extended. `nexus-airbyte-stream-bridge` is a **new** service in this iteration — its own process, its own Docker image, but same repo. |

> ⚠️ **Note:** The ER algorithm stages (Stages 2 + 3) run inside the `nexus-spark-transformer` Spark application but their **logic** is owned by Dev 3 (Entity Resolution) and lives in `libs/nexus_spark_lib/`. Dev 1 owns the deployment and operation of the Spark application; Dev 3 owns what runs inside it.

---

## 1. Scope

The CDC Streaming component owns the streaming side of the ingestion pipeline. Concretely:

- The Debezium connect cluster and its source-connector configurations (Salesforce, SAP, ServiceNow, PostgreSQL, MySQL, SQL Server). Operating model only; the connectors themselves are off-the-shelf.
- The `nexus-airbyte-stream-bridge` service (NEW, small) for SaaS sources without Debezium support, polling APIs at configured intervals and emitting CDC-shaped events.
- The `spark-stream-transformer` Spark Structured Streaming application — Stages 0 + 1 of the pipeline (materialization gate + normalisation) on the streaming side. Stages 2 and 3 are owned by Entity Resolution but live in the same Spark application (shared library), so The CDC Streaming component owns the deployment and operation, Entity Resolution owns the algorithmic content.
- All CDC source-op semantics: how `INSERT`, `UPDATE`, `DELETE`, and `SNAPSHOT_READ` flow through the streaming pipeline.

This component does **not** own: the ER algorithm itself (Entity Resolution), the policy evaluation logic (Materialization Coordinator — only the cache invocation), the M3 writes (M3 Writers), or any backfill jobs (Batch Backfill, even though both share the `nexus_spark_lib`).

---

## 2. Dependencies

| Depends on | What for | When needed |
|---|---|---|
| Platform team (M5) | EKS cluster, Strimzi Kafka, Debezium Connect cluster pre-provisioned | Week 0 |
| Batch Backfill | `nexus_spark_lib` shared library frozen | Week 0–1 (Batch Backfill ships this first) |
| Entity Resolution | Stages 2 + 3 implementation as library calls | Week 2 |
| Materialization Coordinator | `cdm_entity_materialization` + `materialization_policy` schema frozen | Week 0–1 |
| M3 Writers | None inbound; M3 Writers consumes CDC Streaming's output via Entity Resolution/Materialization Coordinator | n/a |
| Platform team | Vault-stored connector credentials accessible from Spark executors | Week 1 |

---

## 3. Functional Requirements (MoSCoW)

### 3.1 Must

- **FR-Dev 1-M-01.** Operate a Debezium Connect cluster with one connector instance per `(tenant, source_system, source_table)` triple. Connector configurations are stored as YAML in `infrastructure/debezium/{tenant_id}/{connector_id}.yaml` and applied via Argo CD.
- **FR-Dev 1-M-02.** Each Debezium source connector emits to topic `cdc.{source_system}.{tenant_id}.{table}` with the standard Debezium envelope (`before`, `after`, `source`, `op`, `ts_ms`).
- **FR-Dev 1-M-03.** `nexus-m1-worker` (existing service, extended by Dev 1) consumes `cdc.*` topics with a per-tenant consumer group, performs the raw-capture step (Phase 3 of the lifecycle walkthrough), writes to Delta Lake, and publishes `m1.int.raw_records` with payload defined in §6.
- **FR-Dev 1-M-04.** Operate the `nexus-airbyte-stream-bridge` service for SaaS sources without Debezium support. It polls the source at a configurable per-connector interval (default 5 minutes), computes `INSERT`/`UPDATE`/`DELETE` deltas against the previous poll's snapshot stored in `nexus_system.connector_poll_state`, and emits Debezium-envelope-shaped events to the same `cdc.*` topic family.
- **FR-Dev 1-M-05.** Operate the `spark-stream-transformer` Spark Structured Streaming application. It reads `m1.int.raw_records` for all tenants, runs Stages 0 + 1 + 2 + 3 (calling D3's library for 2 + 3 and D4's library for 0), and writes to `m1.int.transformed_records`. Micro-batch interval default 30 seconds, configurable per tenant in `tenant_configs.spark_stream_trigger_seconds`.
- **FR-Dev 1-M-06.** Faithfully translate source ops to D3-consumable operations:

| Debezium `op` | CDC Streaming emits as `source_op` |
|---|---|
| `c` (create) | `INSERT` |
| `u` (update) | `UPDATE` |
| `d` (delete) | `DELETE` |
| `r` (snapshot read) | `SNAPSHOT_READ` |
| `t` (truncate) | `DELETE` per row in tombstone batch (FR-CDC Streaming-M-09) |

- **FR-Dev 1-M-07.** Preserve Debezium tombstones (the `null`-payload event sent after a delete) by mapping them to `source_op = DELETE` with the `before` image as the payload. Without this, downstream cannot identify what was deleted.
- **FR-Dev 1-M-08.** Idempotent raw-capture: the Delta Lake write key is `(tenant_id, source_system, source_table, source_record_id, source_op, source_ts)`. Re-delivery of the same Debezium event is a no-op write.
- **FR-Dev 1-M-09.** Truncates from relational sources (`op=t`) are expanded by Dev 1 to a synthetic `DELETE` event per affected source record, sourced from the Delta Lake snapshot. The expansion is bounded — for tables larger than `truncate_expansion_limit` (default 100K rows), Dev 1 emits a single `nexus.m1.truncate_alert` event and pauses the connector pending operator action.
- **FR-Dev 1-M-10.** Schema drift in source tables (added column, dropped column, type change) is detected by Debezium and surfaced via the Connect status API. Dev 1's `nexus-m1-worker` polls this status every 60 seconds and publishes `nexus.cdm.schema_drift_detected` to alert Discovery / CDM Mapper. Ingestion continues for unaffected fields; affected fields are quarantined to `source_extras` until the CDM is updated.
- **FR-Dev 1-M-11.** Per-tenant fairness: the streaming Spark application uses Kafka topic-partition assignment such that no tenant's burst can starve another. Implemented via a `tenant_priority` configuration on the consumer.

### 3.2 Should

- **FR-Dev 1-S-01.** Per-connector backpressure: if `m1.raw_records` lag exceeds 10K messages on a partition, the corresponding Debezium connector is paused and resumed automatically when lag drops to 1K. Implemented via Kafka Connect's `ConnectorStatus` REST API.
- **FR-Dev 1-S-02.** Connector health dashboard endpoints (Prometheus metrics) exposing per-connector lag, error rate, last successful event timestamp, and source-system reachability.
- **FR-Dev 1-S-03.** Configurable per-table inclusion / exclusion lists so a connector can scope what it captures without a separate connector instance.

### 3.3 Could

- **FR-Dev 1-C-01.** Schema-registry integration with Apicurio for Avro-encoded payloads on the `cdc.*` topics, reducing payload size for high-volume sources. JSON envelope remains the default for v0.1.
- **FR-Dev 1-C-02.** Per-event encryption at the Kafka layer using KMS-managed keys, on top of the cluster-wide TLS already in place. Tracked for compliance-tier tenants.

### 3.4 Won't

- **FR-Dev 1-W-01.** CDC Streaming will not perform any business-logic transformation, deduplication, or filtering beyond what is required to produce a normalised event. All semantic processing is downstream.
- **FR-Dev 1-W-02.** CDC Streaming will not emit to a per-record store directly. Every record reaches Elasticsearch / Neo4j / TimescaleDB only via D5 via the standard topic chain.

---

## 4. Non-Functional Requirements

| ID | Requirement | Target |
|---|---|---|
| NFR-D1-01 | End-to-end latency, source mutation → `m1.transformed_records` published, p95 | ≤ 35 seconds |
| NFR-D1-02 | Throughput per Spark application | ≥ 5,000 records/second sustained per cluster on Iter 2 sizing |
| NFR-D1-03 | Restart recovery time after Spark application crash | ≤ 60 seconds; offset commits guarantee no replay beyond the last committed micro-batch |
| NFR-D1-04 | Connector recovery after source-system outage | Automatic resume from last LSN/SCN within 30 seconds of source recovery |
| NFR-D1-05 | Memory footprint per connector instance | ≤ 512 MB (Debezium standard tuning) |
| NFR-D1-06 | Multi-tenant isolation under load | A single tenant's burst (10× normal volume) must not increase p95 latency for other tenants by more than 20% |

---

## 5. Data Model Ownership

The CDC Streaming component owns the following tables in `nexus_system`. Other developers read from these but do not write.

```sql
-- Connector poll state for the Airbyte bridge
CREATE TABLE nexus_system.connector_poll_state (
  connector_id      VARCHAR(64) PRIMARY KEY,
  tenant_id         UUID NOT NULL,
  source_system     VARCHAR(64) NOT NULL,
  source_table      VARCHAR(255) NOT NULL,
  last_polled_at    TIMESTAMPTZ NOT NULL,
  last_cursor       VARCHAR(255),                -- API cursor / etag / max-id, source-specific
  last_snapshot_uri VARCHAR(255),                -- Delta Lake URI of the previous snapshot for diff
  status            VARCHAR(32) NOT NULL DEFAULT 'active'
);

-- connector_backfill_handover: co-owned with Batch Backfill.
-- Batch Backfill inserts/updates it; CDC Streaming reads it to determine the correct LSN offset to start CDC.
-- Authoritative DDL: NEXUS-Iter2-SPEC-Backfill-v0.1.md §5.

-- CDC Streaming streaming Spark checkpoint metadata (small audit; the actual checkpoints live in S3)
CREATE TABLE nexus_system.spark_stream_checkpoint_audit (
  application_id    VARCHAR(64) PRIMARY KEY,
  micro_batch_id    BIGINT NOT NULL,
  committed_at      TIMESTAMPTZ NOT NULL,
  records_processed INTEGER NOT NULL,
  watermark         TIMESTAMPTZ,
  errors_count      INTEGER NOT NULL DEFAULT 0
);
```

---

## 6. API / Kafka Contracts

### 6.1 Outbound: `m1.int.raw_records`

Schema (JSON):

```json
{
  "tenant_id":         "uuid",
  "connector_id":      "string",
  "source_system":     "string",
  "source_table":      "string",
  "source_record_id":  "string",
  "source_op":         "INSERT|UPDATE|DELETE|SNAPSHOT_READ",
  "source_ts":         "iso8601",
  "ingested_at":       "iso8601",
  "delta_pointer":     "s3://...",
  "ingest_offset":     "long",
  "before_payload":    "object|null",
  "after_payload":     "object|null",
  "headers": {
    "schema_version": "string",
    "backfill_batch_id": "string|null"
  }
}
```

Headers carry `tenant_id` and `connector_id` for fast filtering by consumers.

For `DELETE` events, `after_payload` is `null` and `before_payload` carries the row's last-known image. For `INSERT` and `SNAPSHOT_READ`, `before_payload` is `null` and `after_payload` carries the new image. For `UPDATE`, both are populated so Entity Resolution can compute the attribute-level diff if needed.

### 6.2 Outbound: `m1.int.transformed_records`

After Stages 0, 1, 2, 3 of the streaming pipeline complete. Schema is owned by Entity Resolution (see Entity Resolution spec); CDC Streaming calls Entity Resolution's library to populate it.

### 6.3 Outbound: control topics

- `nexus.m1.truncate_alert` — operator notification for large truncates.
- `nexus.cdm.schema_drift_detected` — schema change detected by Debezium.
- `nexus.connector.health` — per-connector heartbeat with status + lag.

### 6.4 Inbound: control topics consumed

- `nexus.materialization_policy.changed` — refresh broadcast cache used by Stage 0.
- `nexus.cdm.version_published` — refresh broadcast cache for canonical mappings.
- `nexus.connector.config_updated` — apply connector config changes (pause / resume / reconfigure).

---

## 7. CRUD Handling — CDC Streaming's Slice

CDC Streaming is the surface where source CRUD enters the platform. The translation contract is mechanical and well-defined; the value CDC Streaming adds is making sure no signal is lost.

**INSERT.** The simplest. `op=c` → `source_op=INSERT`, `after_payload` populated, `before_payload=null`. Delta Lake write is straightforward; the row is appended.

**UPDATE.** `op=u` → `source_op=UPDATE`. Both payloads populated. Delta Lake stores the new state but CDC Streaming also persists the `before_payload` so Entity Resolution can compute the attribute-level diff and decide whether ER needs to re-run. (Many updates change attributes that don't affect ER signals — e.g. a `last_modified_at` field — and Entity Resolution short-circuits in those cases.)

**DELETE.** `op=d` → `source_op=DELETE`. The Debezium event sequence is two messages: first the change event with `before_payload` populated and `after_payload=null`, then a tombstone with `null` value. CDC Streaming consumes both but emits exactly one `m1.raw_records` event per logical delete, carrying the `before_payload` so Entity Resolution can identify what was deleted. The tombstone is consumed for offset commit only.

**SNAPSHOT_READ.** Emitted by Debezium during the connector's initial snapshot, or by Batch Backfill's backfill job. `op=r` → `source_op=SNAPSHOT_READ`. CDC Streaming treats it identically to `INSERT` for raw capture, but the downstream `transformed_records` event carries `source_op=SNAPSHOT_READ` so Entity Resolution can apply batch-mode ER if Batch Backfill's `backfill_batch_id` is present.

**TRUNCATE.** `op=t` is rare and dangerous. CDC Streaming expands it as described in FR-CDC Streaming-M-09 and pauses the connector if the table is large. Operators must confirm before resumption.

---

## 8. Hot/Warm/Cold Handling — CDC Streaming's Slice

CDC Streaming's role in tier movement is small but load-bearing.

**Stage 0 evaluation.** CDC Streaming's Spark application calls Materialization Coordinator's policy library with each record's normalised attributes to determine the level. The library returns `hot`, `warm`, or `cold`. CDC Streaming then:

- For `hot`: continues with full Stages 1 + 2 + 3.
- For `warm`: continues Stage 1 (normalisation), but the resolved record carries a `materialization_level=warm` flag forward; Entity Resolution runs deterministic ER only and Synthesis is skipped.
- For `cold`: short-circuits — the record is in Delta Lake (raw capture done), but no `transformed_records` event is published. The Delta Lake row is the only artefact.

**Backfill on level promotion.** When Materialization Coordinator emits `nexus.materialization.changed` (warm → hot), CDC Streaming does not directly act. Batch Backfill's `materialization-promotion-backfill` reads Delta Lake records for the cohort and re-emits them on `m1.raw_records` with `source_op = RELEVEL`. CDC Streaming's streaming application is not involved beyond consuming the resulting events through the same path it uses for live data.

**Per-record reevaluation.** Materialization Coordinator's daily DAG can issue per-record relevels. They arrive on `m1.raw_records` with `source_op = RELEVEL` and `before_payload = null`, `after_payload` reconstructed from Delta Lake. CDC Streaming treats them like a `SNAPSHOT_READ` for the streaming pipeline.

**Cold record promoted to warm or hot.** Cold means "not in Delta Lake." A cold→warm or cold→hot promotion requires re-extracting from source. CDC Streaming receives a `nexus.connector.refresh_required` event per record (or per cohort), and the connector is asked to re-issue the corresponding records via Debezium snapshot or Airbyte poll. From there the path is the same as `SNAPSHOT_READ`.

---

## 9. Acceptance Criteria

A test passes if it produces the expected outcome under a clean environment with a single tenant and a controlled Debezium source.

- **AC-D1-01.** Insert a row in a Salesforce sandbox; assert `m1.raw_records` carries the corresponding `INSERT` event within 5 seconds; assert Delta Lake row exists; assert `m1.transformed_records` emitted within 35 seconds (NFR-D1-01).
- **AC-D1-02.** Update the same row; assert `UPDATE` event with both payloads populated.
- **AC-D1-03.** Delete the row; assert exactly one `DELETE` event with `before_payload` populated and `after_payload=null`. Assert Debezium tombstone is consumed but does not produce a second event.
- **AC-D1-04.** Bulk-insert 10K rows into a PostgreSQL source; assert the Spark Structured Streaming application processes them within one micro-batch without lag exceeding the threshold.
- **AC-D1-05.** Drop a column on a source table; assert `nexus.cdm.schema_drift_detected` is emitted within 60 seconds; assert ingestion continues for unaffected fields.
- **AC-D1-06.** Stop and restart the streaming Spark application; assert no record is processed twice (offset commits prove this) and no record is lost.
- **AC-D1-07.** Run the truncate test on a 200-row table; assert all 200 are emitted as DELETE events. Run on a 200K-row table (above the limit); assert the connector pauses and `nexus.m1.truncate_alert` is emitted.
- **AC-D1-08.** Configure two tenants; have one burst at 10× normal load; assert the other tenant's p95 latency is unaffected by more than 20% (NFR-D1-06).
- **AC-D1-09.** Trigger a `nexus.materialization_policy.changed` event; assert the streaming Spark application's broadcast cache refreshes within 5 minutes; the next record's Stage 0 result reflects the new policy.

---

## 10. Open Questions

- **OQ-D1-01.** Do we run one Spark Structured Streaming application across all tenants, or one per tenant? Cross-stream parent spec proposed single-application; this dev spec assumes single. Tenant isolation depends on Kafka partition strategy.
- **OQ-D1-02.** Airbyte source connectors that support neither CDC nor sensible polling (some IoT / file-based sources) — out of scope for v0.1, but CDC Streaming should flag them at registration time so connector setup doesn't appear to succeed silently.
- **OQ-D1-03.** Schema drift on a column referenced in an active materialization rule — should CDC Streaming pause Stage 0 for that entity type until the CDM is updated? Recommend yes; otherwise rules silently misfire.
- **OQ-D1-04.** Truncate expansion limit — 100K is a guess. Need to benchmark Spark's ability to produce 100K DELETE events from a Delta Lake snapshot in one micro-batch without saturating the application.
- **OQ-D1-05.** Per-connector poll interval for the Airbyte bridge — 5 minutes default. For high-importance sources, do we support sub-minute polling? Tradeoff is API rate limits vs latency.

---

## 11. References

- `iter2-dev-overview-and-registers-v0.1.md` — cross-cutting contracts.
- `iter2-cdm-to-aistores-pipeline-v0.1.md` — parent spec, §3.1.
- `iter2-record-lifecycle-structured-walkthrough-v0.1.md` — phases 1–3 trace a record through CDC Streaming.
- `NEXUS-Iter2-SPEC-ServiceTopology-v0.4.md` — `nexus-m1-worker` baseline (extended by CDC Streaming).
- `iter2-system-pipeline-orchestration-v0.1.md` — `spark-stream-transformer` operational definition.
