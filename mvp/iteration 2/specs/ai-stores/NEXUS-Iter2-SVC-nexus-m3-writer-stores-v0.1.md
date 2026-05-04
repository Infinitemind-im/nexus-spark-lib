# NEXUS — Iteration 2 · `nexus-m3-writer` · Master Service Spec
**Service:** `nexus-m3-writer`
**Dev 5 workstream · 5 person-weeks**
Mentis Consulting · Version 0.1 · April 2026 · Confidential

> **Neo4j handler superseded by v0.2 (added 2026-04-29):** `NEXUS-Iter2-SVC-nexus-m3-writer-neo4j-v0.2.md` replaces v0.1 for the Neo4j handler (Developer B task). Key changes: (1) no hot/warm/cold tiering — every `graph_persistent` entity is written unconditionally; (2) entity set aligned to the CDM ground truth — 14 node labels replacing the old Employee/Customer/Department/Event set; (3) edge-only entity routing for `party_email`, `product_bom`, `product_photo`, `job_candidate`, `product_vendor`; (4) Golden Record merge and CDM version publish triggers added. **The CRUD matrix and Hot/Warm/Cold Movement Matrix below are updated accordingly** — RELEVEL rows for Neo4j are superseded; see inline notes.
>
> **Routing extension — see `NEXUS-Iter2-SPEC-CDMFieldRouting-v0.1.md` (added 2026-04-29):** This spec describes two artefacts that are manually maintained by Dev 5 and have no programmatic link to the CDM catalogue:
> 1. **`cdm_entity_storage_config`** — hand-seeded at tenant provisioning by `nexus_core.provisioning.onboard_tenant()`. Any new entity type or store assignment requires a manual Dev 5 update.
> 2. **`FieldManifest` in `nexus_core`** — per-field ES roles hard-coded in `field_manifest_registry.py`. A new CDM field approved by M4 is silently skipped by the m3-writer until Dev 5 updates this file.
>
> The companion spec `NEXUS-Iter2-SPEC-CDMFieldRouting-v0.1.md` (in `architecture/`) replaces both with a catalogue-driven approach within this iteration: a new canonical table `nexus_system.cdm_field_routing` (migration V2.0.21) aggregates routing decisions approved through M4; a new Airflow task `refresh_routing_tables` derives `cdm_entity_storage_config` from it automatically on every CDM version publish; `FieldManifest.load_from_catalogue()` replaces the hard-coded registry. Three new columns are also added to `cdm_entity_storage_config` (migration V2.0.22): `derived_from_cdm_version`, `auto_derived`, `derived_at`. The `router.py` consumption of `cdm_entity_storage_config` is **unchanged** — only how the table is populated changes.

**Related docs:**
- `NEXUS-Iter2-SPEC-M3-AIStores-v0.5.md` — architectural invariants, Virtual CDM principle, FRs/NFRs
- `NEXUS-Iter2-SVC-nexus-m3-writer-elasticsearch-v0.1.md` — **Developer A task** — Elasticsearch handler
- `NEXUS-Iter2-SVC-nexus-m3-writer-neo4j-v0.3.md` — **Developer B task** — Neo4j handler (**v0.3** — use this; v0.1 and v0.2 superseded)
- `NEXUS-Iter2-SVC-nexus-m3-writer-timescaledb-v0.1.md` — **Developer C task** — TimescaleDB handler
- `developer-workstreams/NEXUS-Iter2-SVC-nexus-m3-writer-v0.1.md` — sprint plan, milestones, person-week breakdown

---

## 📦 Codebase & Deployment

> **One repository, many deployments.** All NEXUS services live in the `nexus-platform` monorepo. The three store handlers described in this spec are **modules within a single deployed service** (`nexus-m3-writer`) — not three separate services or repositories. You are writing one Python file inside one service directory.

| | |
|---|---|
| **Deployed as** | `nexus-m3-writer` (one process, three internal handler modules) |
| **Monorepo path** | `services/nexus-m3-writer/` |
| **Language / runtime** | Python 3.11 · asyncio |
| **Integration lead** | Dev 5 — owns service shell, Kafka loop, `entity_store_presence`, cross-store reconciliation |
| **Handler ownership** | Developer A → `stores/elasticsearch_writer.py` · Developer B → `stores/neo4j_writer.py` · Developer C → `stores/timescale_writer.py` |

---

## 🗂️ Project Organisation

All four contributors (Dev 5 + Developer A/B/C) commit to the same directory: `services/nexus-m3-writer/` inside the `nexus-platform` monorepo. The structure below is the agreed layout — do not create files outside it without coordinating with Dev 5.

```
nexus-platform/
└── services/
    └── nexus-m3-writer/
        │
        ├── Dockerfile                              # Dev 5
        ├── pyproject.toml                          # Dev 5 — add your deps here, do not use pip install ad-hoc
        ├── .env.example                            # Dev 5
        │
        ├── nexus_m3_writer/                        # main Python package
        │   ├── __init__.py
        │   │
        │   ├── main.py                             # Dev 5 — service entry point, wires everything together
        │   ├── consumer.py                         # Dev 5 — Kafka consumer loop, dispatches to router
        │   ├── router.py                           # Dev 5 — routes entity_routed events to the right handler(s)
        │   ├── presence.py                         # Dev 5 — entity_store_presence read/write (shared by all handlers)
        │   ├── config.py                           # Dev 5 — Pydantic settings model
        │   ├── health.py                           # Dev 5 — /health and /ready endpoints
        │   │
        │   └── stores/
        │       ├── __init__.py
        │       ├── base.py                         # Dev 5 — StoreHandler abstract base class + StoreHealth type
        │       │
        │       ├── elasticsearch_writer.py         # ◀ Developer A — ElasticsearchWriter(StoreHandler)
        │       ├── neo4j_writer.py                 # ◀ Developer B — Neo4jWriter(StoreHandler)
        │       └── timescale_writer.py             # ◀ Developer C — TimescaleWriter(StoreHandler)
        │
        └── tests/
            ├── conftest.py                         # Dev 5 — shared fixtures (mock Kafka, mock presence DB)
            ├── test_consumer.py                    # Dev 5
            ├── test_router.py                      # Dev 5
            ├── test_presence.py                    # Dev 5
            │
            └── stores/
                ├── test_elasticsearch_writer.py    # ◀ Developer A — unit + integration tests for ES handler
                ├── test_neo4j_writer.py            # ◀ Developer B — unit + integration tests for Neo4j handler
                └── test_timescale_writer.py        # ◀ Developer C — unit + integration tests for TS handler
```

### File ownership at a glance

| File | Owner | Notes |
|---|---|---|
| `main.py` | Dev 5 | Do not modify — it imports your handler via `router.py` |
| `consumer.py` | Dev 5 | Do not modify |
| `router.py` | Dev 5 | Do not modify — if a new entity type needs routing, raise it with Dev 5 |
| `presence.py` | Dev 5 | Call `upsert_entity_store_presence(...)` from your handler; never write to the table directly |
| `stores/base.py` | Dev 5 | Your handler must subclass `StoreHandler` and implement all abstract methods |
| `stores/elasticsearch_writer.py` | **Developer A** | Your primary deliverable |
| `stores/neo4j_writer.py` | **Developer B** | Your primary deliverable |
| `stores/timescale_writer.py` | **Developer C** | Your primary deliverable |
| `tests/stores/test_elasticsearch_writer.py` | **Developer A** | Required — must reach ≥ 80% branch coverage on your file |
| `tests/stores/test_neo4j_writer.py` | **Developer B** | Required — must reach ≥ 80% branch coverage on your file |
| `tests/stores/test_timescale_writer.py` | **Developer C** | Required — must reach ≥ 80% branch coverage on your file |
| `pyproject.toml` | Dev 5 (merge) | Add your store-specific dependency (e.g. `elasticsearch`, `neo4j`, `asyncpg`) via PR — do not edit arbitrarily |

### Handler interface contract

Every handler file must implement the abstract base defined in `stores/base.py`. Dev 5 will provide this stub in Week 1; all three handlers must conform to it exactly so `router.py` can call them polymorphically:

```python
# stores/base.py  —  DO NOT MODIFY (Dev 5 owns this)
from abc import ABC, abstractmethod
from nexus_core.cdm import CdmEntity
from nexus_m3_writer.presence import StoreHealth

class StoreHandler(ABC):

    @abstractmethod
    async def write(self, entity: CdmEntity) -> None:
        """Upsert / append — called on UPSERT and RELEVEL-promotion."""

    @abstractmethod
    async def delete(self, entity: CdmEntity) -> None:
        """Tombstone / soft-delete — called on REMOVE and RELEVEL-demotion."""

    @abstractmethod
    async def health_check(self) -> StoreHealth:
        """Returns store connectivity status for /health endpoint."""
```

### Collaboration rules

1. **Do not edit files you do not own.** If you need a change in `base.py`, `router.py`, or `presence.py`, open a PR and tag Dev 5 for review.
2. **All dependencies go through `pyproject.toml`.** Never install a package ad-hoc in the container.
3. **Your handler must be stateless across calls.** Connection pools are initialised once in `__init__`; do not store entity-level state on `self`.
4. **Use `upsert_entity_store_presence()` from `presence.py` after every successful write.** Do not write to `entity_store_presence` directly via SQL.
5. **Week 1 gate:** Dev 5 ships `base.py` and `conftest.py` with working Kafka mocks. Developer A/B/C cannot begin integration tests before this gate.

---

## 1. Scope

`nexus-m3-writer` owns the projection layer: it translates Golden Record state into writes to Elasticsearch, Neo4j, and TimescaleDB, and maintains the `entity_store_presence` register that the query engine consults to know where data actually lives.

This service does **not** own ingestion (CDC Streaming / Batch Backfill), entity resolution (ER-CRUD), or materialization decisions (Materialization Coordinator). It receives `entity_routed` events with a labelled `operation` and `materialization_level`, and faithfully projects to each applicable store.

**Three store handlers — one developer each:**

| Handler | Developer task | Module | Store |
|---|---|---|---|
| Elasticsearch | Developer A | `elasticsearch_writer.py` | kNN vector search |
| Neo4j | Developer B | `neo4j_writer.py` | Property graph / relationships |
| TimescaleDB | Developer C | `timescale_writer.py` | Time-series metrics |

**Service-level responsibilities (owned by the service lead, coordinated across handlers):**
- `entity_store_presence` register — written by all three handlers, read by `nexus-query-executor`
- `cdm_entity_storage_config` register — which stores apply per entity type per tenant
- `presence_lookup` API — hot-path HTTP/gRPC endpoint for the query engine
- Cross-store failure isolation — partial failure does not block the other two stores
- Nightly cross-store reconciliation — `m3-reconciliation` Airflow DAG detects drift and replays
- RLHF signal emission to Materialization Coordinator

---

## 2. Dependencies

| Depends on | What for | When needed |
|---|---|---|
| Platform (M5) | Elasticsearch index provisioned per tenant; Neo4j Aura with constraints; TimescaleDB extension; `nexus_system` schema | Week 0 |
| Entity Resolution | `entity_routed` payload schema frozen, including all `operation` values | Week 1 |
| Materialization Coordinator | `entity_routed` schema frozen with `materialization_level` header | Week 1 |
| Batch Backfill | `m3-reconciliation` DAG framework that this service plugs into | Week 4 |
| Platform | OpenAI embeddings API key; embedding model version pinned | Week 2 |
| `nexus_core` v2.0 | Kafka consumer pattern, `FXService`, `get_timescale_connection()` | Week 1 |

---

## 3. Virtual CDM Rule (applies to all three handlers)

No business field values are ever written to any AI store. Each store holds only:

- **Elasticsearch**: vector embedding + reference tuple (`cdm_entity_id`, `cdm_entity_type`, `tenant_id`, `materialization_level`, `provenance_hash`, model version). Field values are used transiently to generate the embedding and discarded immediately.
- **Neo4j**: entity IDs + `tenant_id` on nodes; structural metadata (`source_fk`, `since`, `connector_id`) on edges only.
- **TimescaleDB**: pre-computed numeric metric aggregates (metric name, normalised value, dimensions). No raw field values.

A consequence: if a miscoded query bypasses OPA tenant isolation, the attacker receives only opaque ID pairs and metric hashes — no usable business data.

---

## 4. Data Model Ownership

This service owns three tables in `nexus_system`:

```sql
-- Per-record presence flags: confirmed written state per store
CREATE TABLE nexus_system.entity_store_presence (
  tenant_id      UUID          NOT NULL,
  cdm_entity_id  VARCHAR(48)   NOT NULL,
  es_present     BOOLEAN       NOT NULL DEFAULT FALSE,
  neo4j_present  BOOLEAN       NOT NULL DEFAULT FALSE,
  ts_present     BOOLEAN       NOT NULL DEFAULT FALSE,
  updated_at     TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
  PRIMARY KEY (tenant_id, cdm_entity_id)
);
-- Partial indexes on FALSE for efficient reconciliation job scans
CREATE INDEX idx_esp_missing_es    ON nexus_system.entity_store_presence(tenant_id) WHERE es_present    = FALSE;
CREATE INDEX idx_esp_missing_neo4j ON nexus_system.entity_store_presence(tenant_id) WHERE neo4j_present = FALSE;
CREATE INDEX idx_esp_missing_ts    ON nexus_system.entity_store_presence(tenant_id) WHERE ts_present    = FALSE;

-- Configuration register: which stores apply per entity type
CREATE TABLE nexus_system.cdm_entity_storage_config (
  tenant_id               UUID          NOT NULL,
  cdm_entity_type         VARCHAR(128)  NOT NULL,
  embeddable              BOOLEAN       NOT NULL DEFAULT FALSE,
  graph_persistent        BOOLEAN       NOT NULL DEFAULT TRUE,
  metricable              BOOLEAN       NOT NULL DEFAULT FALSE,
  metric_value_attr       VARCHAR(128),
  metric_time_attr        VARCHAR(128),
  metric_name_template    VARCHAR(255),
  embed_attrs             VARCHAR(128)[] NOT NULL DEFAULT '{}',
  pii_excluded_attrs      VARCHAR(128)[] NOT NULL DEFAULT '{}',
  updated_at              TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
  PRIMARY KEY (tenant_id, cdm_entity_type)
);

-- Drift audit log
```

`entity_store_presence` has one row per record — 10M hot records = 10M rows, compared to 30M under the old per-store-per-record design. The partial indexes make the nightly reconciliation scan efficient: it only reads rows where a flag is FALSE and the routing config says that store should be populated.

---

## 5. Kafka Contracts

### 5.1 Consumed

| Topic | Consumer group | Handler |
|---|---|---|
| `{tid}.m1.entity_routed` | `m3-writer-entities` | Primary write path — all three handlers |
| `{tid}.m1.entity_removed` | `m3-writer-entities` | Tombstone path — all three handlers |
| `nexus.materialization.changed` | `m3-writer-matl` | Demotion cleanup for a whole entity type |
| `nexus.cdm.version_published` | `m3-writer-cdm-version` | Elasticsearch batch re-embedding + Neo4j org chart rebuild |

### 5.2 Produced

| Topic | When | Payload key fields |
|---|---|---|
| `nexus.m3.write_completed` | All three handlers succeeded (or were deliberately skipped) | `stores_written`, `skipped_stores`, `operation`, `provenance_hash` |
| `nexus.m3.write_failed` | At least one handler failed non-transiently | `failed_stores`, per-store error codes |
| `nexus.m3.short_circuit_skipped` | Provenance-hash match — embedding call skipped | Observability only |
| `nexus.m3.store_circuit_open` / `close` | Per-store circuit breaker triggers | Alerting |
| `nexus.m3.embedding_call_made` | OpenAI embed called | RLHF cost signal to Materialization Coordinator |

### 5.3 `write_completed` Payload

```json
{
  "tenant_id":               "uuid",
  "cdm_entity_id":           "gr:...",
  "cdm_entity_type":         "Party",
  "operation":               "UPSERT",
  "stores_written":          ["elasticsearch", "neo4j"],
  "skipped_stores":          ["timescaledb"],
  "embedding_model_version": "openai/text-embedding-3-small@2025-01-15",
  "provenance_hash":         "sha256:...",
  "completed_at":            "2026-04-27T10:00:00Z",
  "trace_id":                "string"
}
```

---

## 6. Write Orchestration

For each `entity_routed` event the service follows this flow:

```python
async def handle_entity_routed(event: EntityRoutedEvent):
    config = await storage_config_lookup(event.tenant_id, event.cdm_entity_type)

    results = await asyncio.gather(
        elasticsearch_writer.write(event) if config.embeddable        else skip("not_embeddable"),
        neo4j_writer.write(event)         if config.graph_persistent  else skip("not_graph"),
        timescale_writer.write(event)     if config.metricable        else skip("not_metric"),
        return_exceptions=True
    )

    failed = [(store, err) for store, err in zip(STORES, results) if isinstance(err, Exception)]
    written = [store for store, res in zip(STORES, results) if res == "ok"]
    skipped = [store for store, res in zip(STORES, results) if res == "skipped"]

    if failed:
        await producer.publish("nexus.m3.write_failed", {
            "failed_stores": [s for s, _ in failed],
            "errors":        {s: str(e) for s, e in failed},
        })
    else:
        await producer.publish("nexus.m3.write_completed", {
            "stores_written": written, "skipped_stores": skipped,
            "operation": event.operation, ...
        })

    await update_entity_store_presence(event, written, failed)
    # Kafka offset committed regardless of per-store outcome
```

Partial store failure does not cause Kafka offset rollback. Recovery is by topic replay via the `m3-reconciliation` Airflow DAG.

---

## 7. CRUD Handling — Operation Matrix

| `operation` | Elasticsearch | Neo4j | TimescaleDB |
|---|---|---|---|
| `UPSERT` | upsert document | MERGE node + relationships | INSERT row |
| `RELEVEL` (promotion to hot) | upsert document | ~~MERGE node~~ **no-op** ¹ | INSERT row |
| `RELEVEL` (demotion to warm) | set `deleted:true` | ~~DETACH DELETE~~ **no-op** ¹ | **preserve rows** |
| `RELEVEL` (demotion to cold) | set `deleted:true` | ~~DETACH DELETE~~ **no-op** ¹ | append `is_deletion=TRUE` |
| `MERGE` (Golden Record survivor) | upsert with new `provenance_hash` | `merge_golden_record()` — redirect edges to canonical node | re-INSERT under survivor ID |
| `SUPERSEDE` (Golden Record loser) | set `deleted:true` | DETACH DELETE loser (handled inside `merge_golden_record()`) | append `is_deletion=TRUE` |
| `REMOVE` | set `deleted:true` | DETACH DELETE | append `is_deletion=TRUE` |

¹ **Neo4j has no materialization tiers.** Every entity with `graph_persistent=TRUE` is written on first `UPSERT` and stays in the graph until `REMOVE`. RELEVEL events targeting ES or TimescaleDB are ignored by the Neo4j handler. See `NEXUS-Iter2-SVC-nexus-m3-writer-neo4j-v0.2.md` §1.1.

**MERGE / SUPERSEDE ordering:** SUPERSEDE is published first (small delay before MERGE). If MERGE arrives first, the query engine resolves through `golden_record_redirects` — slightly wasteful but correct.

**TimescaleDB asymmetry on hot→warm:** historical metric rows are preserved on warm demotion. Only hot→cold triggers a tombstone append. This is intentional — fiscal analytics remain queryable even for warm-tier entities.

---

## 8. Hot / Warm / Cold Movement Matrix

> **Neo4j column note:** Neo4j has no materialization tiers. All movements are no-ops for the graph store. The `neo4j_present` flag in `entity_store_presence` is set to `TRUE` on first `UPSERT` and only cleared on `REMOVE`. It is unaffected by RELEVEL events.

| Movement | Elasticsearch | Neo4j | TimescaleDB | `entity_store_presence` |
|---|---|---|---|---|
| cold → warm | no action | **no-op** | no action | (no rows) |
| warm → hot | upsert | **no-op** | INSERT | `absent` → `present` |
| hot → warm | `deleted:true` | **no-op** | (preserved) | `present` → `tombstoned` (ES only) |
| hot → cold | `deleted:true` | **no-op** | `is_deletion=TRUE` | `present` → `tombstoned` (ES + TS) |
| warm → cold | no action | **no-op** | no action | (no rows) |

Oscillation cost: dominated by re-embedding. If `provenance_hash` is unchanged (underlying sources did not change), the embedding short-circuit fires and cost is near-zero.

---

## 9. Presence Lookup

`nexus-query-executor` queries `entity_store_presence` directly via a shared read-only PostgreSQL connection pool. No dedicated API, no Redis cache layer.

```sql
-- Single record lookup
SELECT es_present, neo4j_present, ts_present
FROM nexus_system.entity_store_presence
WHERE tenant_id = $1 AND cdm_entity_id = $2;

-- Batch lookup (query engine passes a list of IDs)
SELECT cdm_entity_id, es_present, neo4j_present, ts_present
FROM nexus_system.entity_store_presence
WHERE tenant_id = $1 AND cdm_entity_id = ANY($2);
```

A missing row means the record has never been written to any store — treat all flags as FALSE. NFR: p95 ≤ 5ms on primary key lookup.

---

## 10. Idempotency Summary

| Store | Idempotency mechanism |
|---|---|
| Elasticsearch | `_id = cdm_entity_id`; `_update` with `doc_as_upsert:true` |
| Neo4j | `MERGE` on `(id, tenant_id)` composite |
| TimescaleDB | `INSERT … ON CONFLICT (time, tenant_id, metric_name, cdm_entity_id) DO NOTHING` |
| `entity_store_presence` | `INSERT … ON CONFLICT (tenant_id, cdm_entity_id) DO UPDATE SET es_present/neo4j_present/ts_present` |

---

## 11. Non-Functional Requirements

| ID | Requirement | Target |
|---|---|---|
| NFR-D5-01 | Three-store parallel write p95 latency | ≤ 200ms (ES dominates via embedding; ~60ms on hash short-circuit) |
| NFR-D5-02 | Throughput | ≥ 200 records/sec per replica; KEDA scales 1–6 replicas |
| NFR-D5-03 | `entity_store_presence` direct SQL lookup p95 | ≤ 10ms (connection pool hit) |
| NFR-D5-04 | Idempotency | Re-delivery produces no incremental store change beyond observability counters |
| NFR-D5-05 | Cross-store consistency | ≥ 99.99% of records have `entity_store_presence` matching actual store state post-reconciliation |
| NFR-D5-06 | Per-store failure isolation | 30-min ES outage does not increase Neo4j or TimescaleDB write latencies by more than 10% |

---

## 12. Observability

| Metric | Type | Labels |
|---|---|---|
| `m3_writer_writes_total` | Counter | `store`, `outcome` (ok/skipped/error) |
| `m3_writer_latency_seconds` | Histogram | `store` |
| `m3_writer_elasticsearch_embeddings_total` | Counter | `tenant_id` |
| `m3_writer_elasticsearch_short_circuits_total` | Counter | `tenant_id` |
| `entity_store_presence_drift_detected_total` | Counter | `store` |
| `m3_store_health` | Gauge | `store` (1=ok, 0=degraded) |

---

## 13. Acceptance Criteria

- **AC-D5-01.** UPSERT for `Party` (embeddable + graph_persistent, not metricable): document in Elasticsearch, node in Neo4j, no TimescaleDB row; `entity_store_presence` shows `es_present=TRUE, neo4j_present=TRUE, ts_present=FALSE`; `write_completed` emitted with correct `stores_written`.
- **AC-D5-02.** Same UPSERT 100 times: final state identical to single processing; no embedding call after first (provenance hash unchanged).
- **AC-D5-03.** UPSERT for `Transaction.SalesOrder` (graph_persistent + metricable, not embeddable): no ES write, Neo4j MERGE, TimescaleDB INSERT.
- **AC-D5-04.** REMOVE for previously-written `Party`: ES tombstoned, Neo4j DETACH DELETE, no TimescaleDB action; `entity_store_presence` row shows `es_present=FALSE, neo4j_present=FALSE, ts_present=FALSE`.
- **AC-D5-05.** ES outage simulation: Neo4j and TimescaleDB continue; `write_failed` emitted; `m3-reconciliation` replays after recovery; `entity_store_presence` catches up.
- **AC-D5-06.** Warm→hot promotion: all cohort records appear in ES and Neo4j; `entity_store_presence` shows `es_present=TRUE, neo4j_present=TRUE` for all applicable records.
- **AC-D5-07.** Hot→warm demotion: ES tombstoned, Neo4j DETACH DELETE; TimescaleDB untouched.
- **AC-D5-08.** Hot→cold demotion: all stores tombstoned including TimescaleDB `is_deletion=TRUE`.
- **AC-D5-09.** MERGE+SUPERSEDE pair: survivor vector updated, loser tombstoned; `golden_record_redirects` resolves on subsequent query.
- **AC-D5-10.** Short-circuit: 50 UPSERTs same entity, no provenance change → 49 skips in `m3_writer_elasticsearch_short_circuits_total`.
- **AC-D5-11.** `presence_lookup` batch: 1,000 IDs, p95 ≤ 50ms; all states match underlying table.
- **AC-D5-12.** Drift detection: manually delete ES document; `m3-reconciliation` detects, logs, replays, updates `entity_store_presence`.

---

## 14. Open Questions

| # | Status | Question |
|---|---|---|
| OQ-D5-01 | ❌ Open | TimescaleDB hot→cold: soft tombstone (`is_deletion=TRUE`) vs. hard partition drop? Recommend soft for v0.1. |
| OQ-D5-02 | ❌ Open | ES embedding text format: single string with separators vs. structured input? Recommend single deterministic string for v0.1. |
| OQ-D5-03 | ❌ Open | `presence_lookup` Redis TTL of 30s: too long? A just-demoted record may still appear `present`. Instrument and revisit. |
| OQ-D5-04 | ❌ Open | Neo4j edge MERGE on `(start, end, type, source_fk)` allows duplicate logical edges per source. Confirm query engine handles per-`source_fk` filtering correctly. |
| OQ-D5-05 | ❌ Open | Embedding model migration: lazy (on next write, via `embedding_model_version` mismatch) vs. bulk rebuild? Recommend lazy. |
| OQ-D5-06 | ❌ Open | Single shared ES cluster vs. one cluster per tenant? Recommend shared, one index per tenant per entity type. |

---

*NEXUS Iteration 2 · nexus-m3-writer · Master Service Spec · v0.1 · Mentis Consulting · April 2026 · Confidential*
