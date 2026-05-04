# NEXUS — Iteration 2 · `nexus-m3-writer` · Elasticsearch Store Handler
**Service:** `nexus-m3-writer` · **Module:** `nexus_m3_writer/stores/elasticsearch_writer.py`
**Developer A task**
Mentis Consulting · Version 0.1 · April 2026 · Confidential

**Related docs:**
- `NEXUS-Iter2-SVC-nexus-m3-writer-stores-v0.1.md` — master service spec (scope, data model, CRUD matrix, Kafka)
- `NEXUS-Iter2-SPEC-M3-AIStores-v0.5.md` — architectural invariants, Virtual CDM principle

---

## 📦 Codebase & Deployment

> **One repository, one service, three handler files.** `nexus-m3-writer` is a single deployed process. You (Developer A) are contributing **one file** to it. The service shell, Kafka consumer loop, and `entity_store_presence` writes are owned by Dev 5 — do not reimplement them. All code lives in the `nexus-platform` monorepo.

| | |
|---|---|
| **Deployed as** | `nexus-m3-writer` (shared with Developer B — Neo4j, and Developer C — TimescaleDB) |
| **Your file** | `services/nexus-m3-writer/nexus_m3_writer/stores/elasticsearch_writer.py` |
| **Your tests** | `services/nexus-m3-writer/tests/stores/test_elasticsearch_writer.py` |
| **Integration lead** | Dev 5 — owns the service shell; you own your handler file only |
| **Shared imports** | `from nexus_core.cdm import CdmEntity` · `from nexus_core.kafka import EventEnvelope` · `from nexus_m3_writer.presence import upsert_entity_store_presence` |

---

## Functional Requirements

| ID | Requirement | Priority |
|---|---|---|
| ES-FR-01 | Consume `{tid}.m1.entity_routed` and upsert the document into the correct index when `cdm_entity_storage_config.embeddable = true` | Must |
| ES-FR-02 | Consume `{tid}.m1.entity_removed` — soft-delete by setting `is_deleted = true` on the existing document | Must |
| ES-FR-03 | Consume `nexus.materialization.changed` — update `materialization_level` (and add/remove embedding if level changes) | Must |
| ES-FR-04 | Skip upsert if incoming `provenance_hash` matches stored document's hash and `embedding_model_version` is unchanged (no-op short-circuit) | Must |
| ES-FR-05 | Write `entity_store_presence` register entry after every successful upsert | Must |
| ES-FR-06 | Emit `nexus.m3.write_completed` with `store='elasticsearch'` and `status='ok'|'skipped'|'error'` after each operation | Must |
| ES-FR-07 | Idempotency: re-processing the same event produces the same document state | Must |
| ES-FR-08 | Create index with correct HNSW mapping if it does not exist (`PUT /{index}`) | Must |
| ES-FR-09 | Bulk indexing: batch up to 100 documents per `BulkIndexer` flush, configurable via `cdm_entity_storage_config.es_bulk_size` | Should |
| ES-FR-10 | On `entity_removed`, if `entity_store_presence` shows entity was never written, emit `write_completed` with `status='skipped'` | Should |
| ES-FR-11 | Materialization routing: `hot` entities → dense embedding; `warm` → BM25-only (no dense vector); `cold` → not written | Should |

## Non-Functional Requirements

| ID | Requirement | Target |
|---|---|---|
| ES-NFR-01 | P95 write latency (single upsert, excluding embedding generation) | ≤ 150ms |
| ES-NFR-02 | Throughput | ≥ 500 upserts/sec per Kafka partition |
| ES-NFR-03 | Index mapping: `type: dense_vector`, `dims: 1536`, `index: true`, `similarity: cosine` | — |
| ES-NFR-04 | All operations use `elasticsearch-py` v8.x; no Pinecone SDK dependency | — |
| ES-NFR-05 | Circuit breaker: if cluster health is RED, pause consumption and emit `status='circuit_open'` | Must |

---

## Index Schema

### Naming convention

```
nexus_{tenant_slug}_{cdm_entity_type_lower}
```

Examples: `nexus_acme_corp_contact`, `nexus_globex_invoice`. Tenant slug = slugified `tenant_id` (`acme-corp` → `acme_corp`). Provisioned by `nexus_core.provisioning.onboard_tenant()` — never created by the writer at write time.

### Index mapping (applied at creation time)

```json
{
  "settings": {
    "number_of_shards": 2,
    "number_of_replicas": 1,
    "index.knn": true
  },
  "mappings": {
    "properties": {
      "cdm_entity_id":         { "type": "keyword" },
      "tenant_id":             { "type": "keyword" },
      "cdm_entity_type":       { "type": "keyword" },
      "golden_record_id":      { "type": "keyword" },
      "embedding":             { "type": "dense_vector", "dims": 1536, "index": true, "similarity": "cosine" },
      "provenance_hash":       { "type": "keyword" },
      "materialization_level": { "type": "keyword" },
      "is_deleted":            { "type": "boolean" },
      "source_systems":        { "type": "keyword" },
      "cdm_version":           { "type": "keyword" },
      "embedding_model_version": { "type": "keyword" },
      "created_at":            { "type": "date" },
      "updated_at":            { "type": "date" }
    }
  }
}
```

Document `_id` = `cdm_entity_id` — makes upsert idempotent via `_update` with `doc_as_upsert: true`.

---

## Kafka Contracts

### Consumed

#### `{tid}.m1.entity_routed`
```json
{
  "event_id":               "sha256-...",
  "tenant_id":              "acme-corp",
  "cdm_entity_id":          "uuid",
  "cdm_entity_type":        "Contact",
  "golden_record_id":       "gr:abc123",
  "embedding":              [0.12, -0.34, "..."],
  "provenance_hash":        "sha256-...",
  "materialization_level":  "hot",
  "source_systems":         ["salesforce", "hubspot"],
  "cdm_version":            "v3.2",
  "embedding_model_version":"openai/text-embedding-3-small@2025-01-15",
  "routed_stores":          ["elasticsearch", "neo4j"]
}
```
Action: upsert if `routed_stores` contains `"elasticsearch"`.

#### `{tid}.m1.entity_removed`
```json
{
  "event_id":       "sha256-...",
  "tenant_id":      "acme-corp",
  "cdm_entity_id":  "uuid",
  "cdm_entity_type":"Contact"
}
```
Action: set `is_deleted = true`; update `entity_store_presence` row setting `es_present = FALSE`.

#### `nexus.materialization.changed`
```json
{
  "tenant_id":             "acme-corp",
  "cdm_entity_id":         "uuid",
  "old_level":             "warm",
  "new_level":             "hot",
  "embedding":             [0.12, "..."],
  "embedding_model_version":"openai/text-embedding-3-small@2025-01-15"
}
```
Action: update `materialization_level`; add `embedding` field if promoting to `hot`.

### Produced

#### `nexus.m3.write_completed`
```json
{
  "store":          "elasticsearch",
  "tenant_id":      "acme-corp",
  "cdm_entity_id":  "uuid",
  "status":         "ok | skipped | error | circuit_open",
  "skipped_reason": "provenance_hash_match | not_routed | entity_absent",
  "latency_ms":     42,
  "timestamp":      "2026-04-27T10:00:00Z"
}
```

---

## Upsert Algorithm

```python
async def handle_entity_routed(event: EntityRoutedEvent):
    if "elasticsearch" not in event.routed_stores:
        return

    index = index_name(event.tenant_id, event.cdm_entity_type)
    await ensure_index_exists(index)  # idempotent PUT with mapping

    existing = await es.get(index=index, id=event.cdm_entity_id, ignore=404)
    if existing and (
        existing["_source"]["provenance_hash"] == event.provenance_hash and
        existing["_source"]["embedding_model_version"] == event.embedding_model_version
    ):
        await emit_write_completed(status="skipped", reason="provenance_hash_match")
        return

    doc = {
        "cdm_entity_id":          event.cdm_entity_id,
        "tenant_id":              event.tenant_id,
        "cdm_entity_type":        event.cdm_entity_type,
        "golden_record_id":       event.golden_record_id,
        "embedding":              event.embedding,   # None if materialization_level != 'hot'
        "provenance_hash":        event.provenance_hash,
        "materialization_level":  event.materialization_level,
        "embedding_model_version":event.embedding_model_version,
        "is_deleted":             False,
        "source_systems":         event.source_systems,
        "cdm_version":            event.cdm_version,
        "updated_at":             utcnow(),
    }
    await es.update(index=index, id=event.cdm_entity_id,
                    body={"doc": doc, "doc_as_upsert": True},
                    retry_on_conflict=3)

    await upsert_entity_store_presence(event.tenant_id, event.cdm_entity_id,
                                es_present=True)
    await emit_write_completed(status="ok")
```

---

## kNN Query Contract (for Query Engine reference)

```json
POST /nexus_acme_corp_contact/_search
{
  "knn": {
    "field": "embedding",
    "query_vector": ["..."],
    "k": 10,
    "num_candidates": 100,
    "filter": { "term": { "is_deleted": false } }
  },
  "_source": ["cdm_entity_id", "golden_record_id", "cdm_entity_type", "materialization_level"]
}
```

Do not return `embedding` in `_source` — large and not needed by the query layer.

---

## Edge Cases

| Scenario | Handling |
|---|---|
| Index doesn't exist on first write | `ensure_index_exists()` uses `PUT /{index}` — safe under concurrent first writes via `if_primary_term` guard |
| Kafka redelivery of same `event_id` | `provenance_hash` match → `status='skipped'`, idempotent |
| ES cluster health RED | Circuit breaker opens; consumer pauses; alert emitted; auto-resume after 60s health check |
| `entity_removed` for entity not in ES | Check `entity_store_presence` first; if `es_present = FALSE` (or row absent), emit `skipped`, no ES call |
| Warm entity promoted to hot | `nexus.materialization.changed` carries `embedding`; writer adds it on update |
| Embedding dimension mismatch | Reject event; emit `error`; alert — do not write partial document |
| All entity fields are PII-flagged | Embed empty-fields representation — entity shape still useful for type-level similarity |

---

## Implementation Phases

### Phase 1 — Setup (Weeks 1–2)

**D2A-01 · Client, index lifecycle, schema migration** (2 days · Must)
- `elasticsearch-py` v8.x client initialisation; connection pool; health check
- `ensure_index_exists()` with mapping from §Index Schema
- `index_name()` helper; slug normalisation
- Write/read one test document; confirm kNN query returns it

**D2A-02 · Short-circuit and upsert** (2 days · Must · Depends on D2A-01)
- Full upsert algorithm from §Upsert Algorithm
- `provenance_hash` + `embedding_model_version` comparison
- `entity_store_presence` update after write

### Phase 2 — Implementation (Weeks 3–6)

**D2A-03 · Tombstone and deletion maintenance** (2 days · Must · Depends on D2A-02)
- `entity_removed` handler: set `is_deleted=true`
- Nightly Delete By Query for documents with `is_deleted=true` older than 24 hours

**D2A-04 · Materialization level changes** (1.5 days · Must · Depends on D2A-02)
- `nexus.materialization.changed` handler
- Adding embedding on warm→hot promotion; removing on hot→warm demotion (set `is_deleted:true`)

**D2A-05 · Bulk indexer** (1.5 days · Should · Depends on D2A-02)
- Accumulate up to `es_bulk_size` documents per `_bulk` call
- Flush on batch size or 500ms timeout

### Phase 3 — Integration (Weeks 7–9)

**D2A-06 · Circuit breaker and throughput validation** (1.5 days · Must)
- Circuit breaker: RED cluster health → pause consumer → resume after 60s
- Throughput target: ≥ 500 upserts/sec per partition documented

---

## Open Questions

| OQ | Status | Question |
|---|---|---|
| ES-OQ-01 | ❌ Open | Per-tenant ES cluster (hard isolation) vs. shared cluster with index-level ACL? Affects `nexus_core.get_es_client()` signature. |
| ES-OQ-02 | ❌ Open | Embedding model confirmed as `text-embedding-3-small` (1536 dims). Model change requires full reindex — cannot change `dims` after index creation. Document upgrade path. |
| ES-OQ-03 | ❌ Open | Warm entities: BM25-only (no dense vector) vs. excluded entirely? ES-FR-11 proposes BM25-only, but requires conditional field strategy. |

---

## Acceptance Criteria

- [ ] 1,000 `entity_routed` events processed and upserted into correct indices within 5 seconds
- [ ] Re-consuming the same 1,000 events produces zero mutations (all `provenance_hash_match` skips)
- [ ] `entity_removed` correctly sets `is_deleted=true`; subsequent kNN query does not return the document
- [ ] `entity_store_presence` register correctly reflects `es_present=TRUE` after upsert and `es_present=FALSE` after tombstone for all entities
- [ ] `write_completed` events emitted for every input event (`ok` / `skipped` / `error`)
- [ ] Circuit breaker: consumer pauses with ES stopped and resumes correctly after cluster restart

---

*NEXUS Iteration 2 · nexus-m3-writer · Elasticsearch Handler · v0.1 · Mentis Consulting · April 2026 · Confidential*
