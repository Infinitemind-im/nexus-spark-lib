# NEXUS — Iteration 2 · `nexus-spark-transformer` · Spark Transformation Stage
**Service:** `nexus-spark-transformer`
**Type coercion · Entity resolution · Golden Record assignment · Delta Lake checkpointing**
Mentis Consulting · Version 0.1 · April 2026 · Draft

**Owner:** Data Intelligence team (Dev 1 — deployment + streaming; Dev 3 — ER algorithm library)
**Depends on:** `nexus_core` v2, `nexus_spark_lib` (from Batch Backfill — Week 0–1 gate), Platform M5 (Spark on EKS), `entity_resolution_index` seeded (OQ-SP-02)
**Related docs:**
- `architecture/NEXUS-Iter2-SPEC-ServiceTopology-v0.4.md` §Spark Transformation Stage — original description
- `developer-workstreams/NEXUS-Iter2-SVC-nexus-m1-worker-CDCStreaming-v0.1.md` §FR-Dev1-M-05 — deployment ownership
- `pipeline/NEXUS-Iter2-REF-SystemOrchestration-v0.1.md` §2.1 — three Spark applications, operational definitions
- `libraries/NEXUS-Iter2-SPEC-LIB-NexusCore-v0.3.md` §10 — `SparkTransformResult` output envelope

---

## 📦 Codebase & Deployment

> **One repository, many deployments.** All NEXUS services live in the `nexus-platform` monorepo. You are contributing a new service to a shared codebase — not a standalone repository. Shared libraries (`nexus_core` v2, `agent_core` v1, `nexus_spark_lib`) live in `libs/` and are imported by all services. Never duplicate logic that already exists there.

| | |
|---|---|
| **Deployed as** | `nexus-spark-transformer` (**NEW** service — Spark Structured Streaming application) |
| **Monorepo path** | `services/nexus-spark-transformer/` |
| **Language / runtime** | Python 3.11 · PySpark 3.5 · Spark Structured Streaming |
| **Iteration 2 owner** | Dev 1 (deployment + streaming operations) · Dev 3 (ER algorithm, contributed via `libs/nexus_spark_lib/`) |
| **Split ownership** | Dev 1 owns the Spark application shell, job lifecycle, and operational config. Dev 3 owns the entity resolution and Golden Record synthesis logic, which lives in `libs/nexus_spark_lib/` and is called from within this service. **Both developers commit to the same repo.** |

---

## Overview

`nexus-spark-transformer` is the M1 transformation stage that sits between raw Kafka ingestion and the CDM Mapper. It consumes `m1.int.raw_records` — heterogeneous, source-native rows — and publishes `m1.int.transformed_records` — typed, normalised, entity-resolved records ready for semantic classification.

The CDM Mapper's responsibility is **semantic classification only**. It must never receive a raw source row. `nexus-spark-transformer` owns all mechanical preparation: type coercion, FX normalisation, data quality scoring, deduplication, entity resolution, and schema profiling.

### Position in the pipeline

```
Debezium / Airbyte
        │
        ▼
 m1.int.raw_records
        │
        ▼
nexus-spark-transformer  ◄── this service
  Stage 0: materialization gate (calls nexus_spark_lib.transform.materialization_decide)
  Stage 1: normalise (type coerce, FX, dedup, DQ flags)
  Stage 2: entity resolution  (D3 library — LSH + signal scoring)
  Stage 3: Golden Record synthesis  (D3 library — survivorship)
        │
        ▼
 m1.int.transformed_records
        │
        ▼
  nexus-cdm-mapper
```

---

## Open Questions — Resolved

> **OQ-SP-01 — Spark infrastructure (RESOLVED)**
> `nexus-spark-transformer` runs as a **long-lived Kubernetes `Deployment`** for CDC streaming (always-on Structured Streaming). Batch history jobs are submitted as ephemeral `spark-submit` pods via Airflow, sharing the same `nexus_spark_lib` code but executing independently. There is no separate "batch transformer" service — batch jobs are Airflow DAGs, not a Deployment.

> **OQ-SP-02 — Entity resolution index seeding (RESOLVED)**
> `nexus_system.entity_resolution_index` is seeded by a first-run Spark batch job (`initial-load` Airflow DAG) executed on connector registration. The job processes all historical records from Delta Lake, resolves entity identities from scratch, and populates `entity_resolution_index` before streaming begins. If Iteration 1 approved mappings exist, they are imported as the seed set; otherwise the index is built from the full snapshot. Owner: Data Intelligence team.

> **OQ-SP-03 — Delta Lake checkpoint threshold (RESOLVED)**
> `delta_checkpoint_threshold` is configurable per connector. The column `delta_checkpoint_threshold INT DEFAULT 500000` has been added to `nexus_system.connector_batch_state` (migration V2.0.19 — see `architecture/NEXUS-Iter2-SPEC-DataModel-v0.5.md`). The 500k default applies when no per-connector override is set.

---

## Operating Modes

| Mode | Spark mode | Deployment | Delta Lake | Latency target |
|---|---|---|---|---|
| **Real-time CDC** | Structured Streaming (micro-batch) | Long-lived `Deployment` | Bypassed | P95 ≤ 2s (raw record → `transformed_records`) |
| **Batch history** | Batch job (`spark-submit`) | Ephemeral pod via Airflow | Used when record count > `delta_checkpoint_threshold` | Minutes–hours depending on volume |

The micro-batch interval defaults to 30 seconds and is configurable per tenant in `nexus_system.tenant_configs.spark_stream_trigger_seconds`.

---

## Functional Requirements (MoSCoW)

### Must

- **FR-ST-M-01.** Consume `m1.int.raw_records` (consumer group `m1-spark-transformer`) and publish `m1.int.transformed_records` with the `SparkTransformResult` envelope defined in `nexus_core.models`.
- **FR-ST-M-02. Type coercion.** Normalise all source field types before any downstream processing:
  - Dates → ISO 8601 bare (`YYYY-MM-DD`) or datetime (`YYYY-MM-DDTHH:MM:SSZ`). Reject unparseable dates to `source_extras`; do not silently null them.
  - Numerics → `Decimal` with consistent precision (source precision preserved; no rounding unless currency normalisation applies).
  - Booleans → canonical `True` / `False` (handles `"yes"/"no"`, `1/0`, `"true"/"false"`).
  - Strings → strip leading/trailing whitespace; normalise null-like strings (`"null"`, `"NULL"`, `"N/A"`, `""`) to Python `None`.
- **FR-ST-M-03. Currency normalisation.** For fields identified as monetary in `nexus_system.schema_snapshots.column_profiles`:
  - Convert to tenant base currency using `FXService.convert()`.
  - Store `original_currency` and `fx_rate` in the `TransformedField` metadata.
  - The FX rate used must be the rate at the record's `source_ts`, not the processing time. If the historical rate is unavailable, use the most recent available rate and set a `fx_rate_approximate: true` flag.
- **FR-ST-M-04. Data quality scoring.** For each field, compute and attach a `FieldQuality` object:
  - `null_rate`: fraction of nulls within the current micro-batch window for this field (streaming) or the full dataset (batch).
  - `format_valid`: whether the raw value matched the expected type pattern before coercion.
  - `cardinality`: distinct values seen in the current batch (set to `None` if not computed for cost reasons on high-cardinality fields).
- **FR-ST-M-05. Deduplication.** Within a connector snapshot or micro-batch window, deduplicate on natural key `(tenant_id, connector_id, source_table, source_record_id)`. On duplicate: keep the record with the most recent `source_ts`; discard the older one silently (no error event). Cross-batch deduplication is handled by downstream idempotency (upsert / MERGE), not here.
- **FR-ST-M-06. Entity resolution — lookup.** For each record, look up `nexus_system.entity_resolution_index` by `(tenant_id, connector_id, source_table, source_record_id)`. If a `cdm_entity_id` exists, attach it to the `SparkTransformResult` and skip the resolution computation. This is the fast path for known records.
- **FR-ST-M-07. Entity resolution — assignment.** If no `cdm_entity_id` exists in the index, execute entity resolution (Stages 2 + 3, implemented as library calls to the ER-CRUD library provided by Dev 3):
  - Signal A: Jaro-Winkler on normalised name fields.
  - Signal B: Levenshtein on composite blocking key (name + postcode / tax ID).
  - Signal C: Soundex + Metaphone on phonetic representation.
  - LSH blocking limits comparison fan-out to records sharing the same blocking bucket.
  - If a match is found above threshold: assign the existing `cdm_entity_id`; write the `(source_identifier → cdm_entity_id)` mapping to `entity_resolution_index`.
  - If no match: generate a new `cdm_entity_id` (`gr:` + sha256 of `tenant_id || cdm_entity_type || canonical_blocking_key`, truncated to 128 bits); write to `entity_resolution_index`.
- **FR-ST-M-08. Schema profiling.** After processing each micro-batch, update `nexus_system.schema_snapshots` with cardinality and type statistics for fields seen in the batch. This is an upsert (do not overwrite full history — update running aggregates). Update is best-effort; a failure must not abort record processing.
- **FR-ST-M-09. Dead letter.** Records that fail transformation irrecoverably (unparseable payload, missing `tenant_id`, fatal type conflict) must be published to `m1.int.dead_letter` with the original payload and a structured error reason. The transformation stage must never silently drop a record.
- **FR-ST-M-10. Offset commit discipline.** Kafka offsets are committed only after the micro-batch has been fully processed and published to `m1.int.transformed_records`. A crash before commit causes the batch to be reprocessed from the last committed offset. Downstream idempotency (`SparkTransformResult.message_id` deduplication in CDM Mapper) handles re-delivery safely.
- **FR-ST-M-11. Materialization gate (Stage 0).** Before transformation, call `nexus_spark_lib.transform.materialization_decide(tenant_id, cdm_entity_type, record)` to determine the `materialization_level` (hot / warm / cold). Records classified as `cold` are transformed and published normally — the tier decision is carried in the `SparkTransformResult` payload for downstream routing; the transformer does not filter or skip cold records.

### Should

- **FR-ST-S-01.** Emit a `nexus.er.review_queued` event for records where entity resolution produced a match confidence between the low threshold (0.70) and the high threshold (0.95), flagging the pair for human review in M4.
- **FR-ST-S-02.** Expose a `/metrics` endpoint (Prometheus) with: records processed per tenant per second, entity resolution cache hit rate, average `transformation_ms`, dead-letter rate.
- **FR-ST-S-03.** Per-tenant fairness: Spark consumer uses weighted partition assignment so that no tenant's burst starves another. Implemented via `tenant_priority` config in `nexus_system.tenant_configs`.

### Could

- **FR-ST-C-01.** When batch volume exceeds `delta_checkpoint_threshold`, write transformed records to `nexus_delta.transformed_{tenant_id}_{connector_id}` before publishing to Kafka. A restart reads from Delta Lake rather than re-pulling from the source. For real-time CDC and small batch jobs, Delta Lake is bypassed entirely.
- **FR-ST-C-02.** Support a dry-run mode (triggered by the M4 CDM Validation simulate endpoint) where transformation runs against a sample payload and returns `SparkTransformResult` without publishing to Kafka or writing to the index.

---

## Non-Functional Requirements

| ID | Requirement |
|---|---|
| NFR-ST-01 | P95 latency raw_record → transformed_records ≤ 2s under CDC streaming mode at steady-state load |
| NFR-ST-02 | The streaming deployment must be restart-safe: Kafka offset commits + Delta Lake idempotent writes guarantee no record loss on pod failure |
| NFR-ST-03 | No business field values are stored in `entity_resolution_index` or `schema_snapshots` beyond what is needed for blocking/matching |
| NFR-ST-04 | PII-flagged fields (per `schema_snapshots.column_profiles`) are never logged in plaintext; `TransformedField.pii_flag = True` suppresses the field value from all structured logs |
| NFR-ST-05 | The service account `nexus-m1-worker-sa` (shared with `nexus-m1-worker`) covers this service's Kubernetes identity; no separate SA is required in Iteration 2 |

---

## Kafka Contracts

| Topic | Role | Notes |
|---|---|---|
| `m1.int.raw_records` | **Consumes** | Consumer group `m1-spark-transformer` |
| `m1.int.transformed_records` | **Publishes** | Payload: `SparkTransformResult` (see `nexus_core.models`) |
| `m1.int.dead_letter` | **Publishes** | Fatal transformation failures |
| `nexus.er.review_queued` | **Publishes** | Should — ER confidence in [0.70, 0.95) range |

---

## Data Model Ownership

| Table | Access | Notes |
|---|---|---|
| `nexus_system.entity_resolution_index` | Read + Write | Primary owner. Keyed on `(tenant_id, connector_id, source_table, source_record_id)`. See DataModel v0.5 §V2.0.18. |
| `nexus_system.schema_snapshots` | Write (upsert) | Shared with `nexus-schema-profiler`; updates cardinality/type stats inline. |
| `nexus_system.connector_batch_state` | Read | Reads `delta_checkpoint_threshold` per connector. Written by `nexus-m1-worker`. |
| `nexus_system.tenant_configs` | Read | Reads `spark_stream_trigger_seconds`, `tenant_priority`, `base_currency`. |
| `nexus_system.cdm_entity_materialization` | Read (broadcast join) | Stage 0 materialization gate. |
| `nexus_delta.transformed_{tenant_id}_{connector_id}` | Write (conditional) | Delta Lake tables; only created when `delta_checkpoint_threshold` is exceeded in batch mode. |

---

## Output Envelope

`SparkTransformResult` is defined in `nexus_core.models` (see `libraries/NEXUS-Iter2-SPEC-LIB-NexusCore-v0.3.md` §10). Key fields:

| Field | Type | Description |
|---|---|---|
| `message_id` | `str` (UUID) | Idempotency key for CDM Mapper dedup |
| `tenant_id` | `str` | Tenant identifier |
| `cdm_entity_id` | `str` | Golden Record ID assigned or looked up by Stage 2/3 |
| `op` | `Literal["c","u","d","r"]` | Source operation code, unchanged from raw record |
| `fields` | `list[TransformedField]` | Typed, normalised fields with quality and PII flags |
| `spark_job_id` | `str` | Lineage tracing |
| `delta_checkpoint_path` | `str | None` | Set if batch-mode Delta Lake checkpoint was used |
| `transformation_ms` | `int` | Processing latency for this record |

---

## Deployment

| Aspect | Value |
|---|---|
| Runtime | Spark 3.x on EKS via Strimzi + `spark-on-k8s-operator` |
| Streaming deployment | Long-lived `Deployment` (`spark-stream-transformer`), always-on |
| Batch jobs | Ephemeral `spark-submit` pods triggered by Airflow DAGs; not a Deployment |
| Scales on | Kafka partition count for `m1.int.raw_records`; Spark executor autoscaling within the pod |
| Min replicas | 1 driver pod (streaming); executors scale dynamically |
| Team | Data Intelligence (Dev 1 owns deployment; Dev 3 owns ER algorithm library calls) |

---

## Acceptance Criteria

- [ ] A Debezium CDC event emitted for a Salesforce Account `UPDATE` arrives on `m1.int.transformed_records` within 2 seconds (P95), fully typed and with a `cdm_entity_id` attached.
- [ ] Re-delivering the same raw record (same `message_id`) produces exactly one `SparkTransformResult` on `m1.int.transformed_records` (CDM Mapper dedup gate on `message_id`).
- [ ] A monetary field in EUR on a tenant with base currency USD is converted correctly, with `original_currency = "EUR"` and `fx_rate` set to the rate at `source_ts`.
- [ ] A record with an unparseable date field publishes to `m1.int.dead_letter` rather than silently nulling the field.
- [ ] A batch job processing 600k records checkpoints to Delta Lake; a simulated job failure followed by restart resumes from the Delta Lake checkpoint rather than re-pulling from the source.
- [ ] `entity_resolution_index` contains the correct `cdm_entity_id` mapping after first-run seeding via the `initial-load` DAG.
- [ ] ER match confidence in [0.70, 0.95) emits a `nexus.er.review_queued` event visible in the M4 review queue.
- [ ] `nexus_system.schema_snapshots` cardinality stats are updated after each micro-batch without blocking record publishing.

---

*NEXUS Iteration 2 · nexus-spark-transformer · v0.1 · Mentis Consulting · April 2026 · Confidential*
