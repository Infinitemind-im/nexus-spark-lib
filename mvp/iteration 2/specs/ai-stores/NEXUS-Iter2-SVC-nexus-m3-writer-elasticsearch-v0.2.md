# NEXUS — Iteration 2 · `nexus-m3-writer` · Elasticsearch Store Handler
**Service:** `nexus-m3-writer` · **Module:** `nexus_m3_writer/stores/elasticsearch_writer.py`
**Developer A task**
Mentis Consulting · Version 0.2 · April 2026 · Confidential

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
| **Additional imports** | `from nexus_m3_writer.indexing.field_manifest import FieldManifest` · `from nexus_m3_writer.indexing.narrative import NarrativeBuilder` · `from nexus_m3_writer.indexing.signals import BehavioralSignalReader` |

---

## Indexing Architecture — Two Tracks

Every entity written to Elasticsearch passes through two independent indexing tracks. Both tracks are derived from the same `CdmEntity` payload, but they serve different query modes and store different data.

```
CdmEntity arrives
      │
      ├─── Track 1: Surface Indexing ──────────────────────────────────────────┐
      │    Read FieldManifest → extract SURFACE fields → store as              │
      │    keyword / text / boolean / float fields in ES document               │
      │    Enables: exact match, range filter, fuzzy name search, facets        │
      │                                                                          │
      └─── Track 2: Semantic Embedding ────────────────────────────────────────┤
           Step A — pull behavioral signals (TimescaleDB + Neo4j read)          │
           Step B — NarrativeBuilder: NARRATIVE_STRUCTURED fields → text block  │
           Step C — append NARRATIVE_FREETEXT fields (notes, comments, docs)    │
           Step D — concatenate → hash (SHA-256) → compare stored hash          │
                     ├── hash match → skip embedding API call (no-op)           │
                     └── hash mismatch → call embedding model → store vector    │
                                         discard source text; store hash only   │
                                                                                 │
                                                       ES document written  ◄───┘
```

The source text of the narrative and free-text fields **is never persisted** — only the 1536-dim dense vector and the SHA-256 hash are stored. The hash is the re-embedding gate: the embedding API is only called when the content has actually changed.

---

## CDM Field Manifest — How the Indexer Knows the Fields

The ES writer does not hard-code which CDM fields to index. Instead, it reads a **CDM Field Manifest** registered in `nexus_core`. The manifest is a per-entity-type list of `CdmFieldSpec` entries that assign each CDM field to a role.

### Field Roles

| Role | What it means | Where it ends up |
|---|---|---|
| `SURFACE` | Structured metadata useful for filtering or faceting | Stored in ES document as keyword / text / boolean / float |
| `NARRATIVE_STRUCTURED` | Structured field whose value is woven into the narrative text | Fed to `NarrativeBuilder`; source value **not** stored in ES |
| `NARRATIVE_FREETEXT` | Pre-existing human-authored text (notes, comments, document bodies) | Appended after the narrative; source text **not** stored in ES |
| `EXCLUDED` | PII, financial data, or fields with no indexing value | Never read by the writer |

> A field can carry **more than one role**. `party_name` is both `SURFACE` (stored as a searchable keyword in the document) and `NARRATIVE_STRUCTURED` (woven into the narrative sentence). `notes_text` is both `NARRATIVE_FREETEXT` (embedded) and optionally `SURFACE` (stored as short excerpt). Roles are additive.

### Field Spec Dataclass

Defined in `libs/nexus_core/nexus_core/cdm/field_manifest.py`. **Do not redefine in your handler — import it.**

```python
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Literal

FieldRole = Literal["SURFACE", "NARRATIVE_STRUCTURED", "NARRATIVE_FREETEXT", "EXCLUDED"]
EsType    = Literal["keyword", "text", "boolean", "float", "integer", "date"]

@dataclass
class CdmFieldSpec:
    field_name:   str
    roles:        list[FieldRole]
    es_type:      EsType | None     = None   # required if SURFACE
    es_analyzer:  str | None        = None   # e.g. "edge_ngram_analyzer" for name fields
    pii:          bool              = False  # safety flag; EXCLUDED implied if True
    excerpt_chars: int | None       = None   # if SURFACE on a FREETEXT field: max chars to store
```

### Field Manifest Registry

Defined in `libs/nexus_core/nexus_core/cdm/field_manifest.py`. Each CDM entity type has its own list of specs. The ES writer calls `FieldManifest.for_entity_type(entity_type)` to get the applicable specs at write time.

```python
class FieldManifest:
    _registry: dict[str, list[CdmFieldSpec]] = {}

    @classmethod
    def register(cls, entity_type: str, specs: list[CdmFieldSpec]) -> None:
        cls._registry[entity_type] = specs

    @classmethod
    def for_entity_type(cls, entity_type: str) -> list[CdmFieldSpec]:
        return cls._registry.get(entity_type, [])

    @classmethod
    def surface_fields(cls, entity_type: str) -> list[CdmFieldSpec]:
        return [s for s in cls.for_entity_type(entity_type) if "SURFACE" in s.roles]

    @classmethod
    def narrative_structured(cls, entity_type: str) -> list[CdmFieldSpec]:
        return [s for s in cls.for_entity_type(entity_type) if "NARRATIVE_STRUCTURED" in s.roles]

    @classmethod
    def narrative_freetext(cls, entity_type: str) -> list[CdmFieldSpec]:
        return [s for s in cls.for_entity_type(entity_type) if "NARRATIVE_FREETEXT" in s.roles]
```

### Example Field Manifests

Field manifests for all entity types are registered at service startup in `nexus_m3_writer/indexing/field_manifest_registry.py`. Examples below. Dev 5 owns this registry file; Developer A must not edit it — raise a PR to Dev 5 to add or change field classifications.

#### Entity type: `party`

| Field | Roles | ES type | Notes |
|---|---|---|---|
| `party_name` | SURFACE, NARRATIVE_STRUCTURED | `text`, `edge_ngram_analyzer` | Stored + embedded |
| `party_subtype` | SURFACE, NARRATIVE_STRUCTURED | `keyword` | e.g. vendor, customer, partner |
| `city` | SURFACE, NARRATIVE_STRUCTURED | `keyword` | |
| `country_code` | SURFACE, NARRATIVE_STRUCTURED | `keyword` | ISO-2 |
| `is_active` | SURFACE, NARRATIVE_STRUCTURED | `boolean` | |
| `credit_rating` | NARRATIVE_STRUCTURED | — | Embedded only; not a filter field |
| `notes_text` | NARRATIVE_FREETEXT | — | Human-authored; never stored |
| `tax_id` | EXCLUDED | — | PII |
| `address_line_1` | EXCLUDED | — | PII |
| `address_line_2` | EXCLUDED | — | PII |

#### Entity type: `contact`

| Field | Roles | ES type | Notes |
|---|---|---|---|
| `full_name` | SURFACE, NARRATIVE_STRUCTURED | `text`, `edge_ngram_analyzer` | |
| `job_title` | SURFACE, NARRATIVE_STRUCTURED | `keyword` | |
| `department` | SURFACE, NARRATIVE_STRUCTURED | `keyword` | |
| `is_active` | SURFACE | `boolean` | |
| `contact_notes` | NARRATIVE_FREETEXT | — | Never stored |
| `bio_text` | NARRATIVE_FREETEXT | — | Never stored |
| `email` | EXCLUDED | — | PII |
| `phone` | EXCLUDED | — | PII |

#### Entity type: `invoice` / `transaction`

| Field | Roles | ES type | Notes |
|---|---|---|---|
| `transaction_type` | SURFACE, NARRATIVE_STRUCTURED | `keyword` | |
| `currency_code` | SURFACE | `keyword` | |
| `status` | SURFACE, NARRATIVE_STRUCTURED | `keyword` | |
| `transaction_comment_text` | NARRATIVE_FREETEXT | — | Never stored |
| `description` | NARRATIVE_FREETEXT | — | Never stored |
| `amount` | EXCLUDED | — | Financial PII |
| `account_number` | EXCLUDED | — | PII |

---

## Track 1 — Surface Indexing

Surface fields are extracted from the `CdmEntity` using the manifest and stored directly in the ES document body. They enable pre-filtering before kNN (cheap Boolean evaluation runs before the expensive vector scan).

```python
def extract_surface_fields(
    entity: CdmEntity,
    specs: list[CdmFieldSpec]
) -> dict[str, Any]:
    surface = {}
    for spec in specs:
        if "SURFACE" not in spec.roles:
            continue
        value = entity.fields.get(spec.field_name)
        if value is None:
            continue
        if spec.excerpt_chars and isinstance(value, str):
            value = value[: spec.excerpt_chars]
        surface[spec.field_name] = value
    return surface
```

Surface fields are indexed under their field names verbatim. The ES index mapping (§ Index Schema) declares the type for each field. Unknown surface fields not in the mapping are rejected at write time — do not use `dynamic: true`.

---

## Track 2 — Semantic Embedding

### Step A — Pull Behavioral Signals

Before building the narrative, the ES writer reads behavioral signals from the stores that have already been written in the same pipeline run (TimescaleDB and Neo4j are written before ES — see ordering constraint in §Upsert Algorithm).

```python
@dataclass
class BehavioralSignals:
    purchase_count:              int       = 0
    dominant_products:           list[str] = field(default_factory=list)
    open_issue_count:            int       = 0
    dominant_issue_category:     str | None = None
    tenure_months:               int       = 0
    avg_tx_frequency_label:      str       = "unknown"   # low / medium / high
    linked_entity_count:         int       = 0           # from Neo4j

class BehavioralSignalReader:
    async def read(
        self,
        tenant_id: str,
        cdm_entity_id: str,
        entity_type: str
    ) -> BehavioralSignals:
        # reads from TimescaleDB metrics tables and Neo4j relationship count
        # returns zero-value BehavioralSignals if entity not yet in those stores
        ...
```

Zero values are valid — first-write entities have no behavioral history yet. The narrative template gracefully omits zero-valued behavioral clauses.

### Step B — NarrativeBuilder

The narrative builder is defined in `nexus_m3_writer/indexing/narrative.py`. It reads `NARRATIVE_STRUCTURED` fields from the CDM entity (via the manifest), combines them with behavioral signals, and produces a natural-language passage. The passage is **not** a rigid template — it branches based on entity type and omits clauses where data is absent to avoid template-collapse in the vector space.

```python
@dataclass
class NarrativeResult:
    text: str    # feed to embedding model; discard after
    hash: str    # SHA-256 of text; store in ES

class NarrativeBuilder:
    def build(
        self,
        entity:    CdmEntity,
        specs:     list[CdmFieldSpec],
        signals:   BehavioralSignals,
    ) -> NarrativeResult:
        ...
```

Example output for a `party` entity with behavioral signals:

> *Contoso GmbH is an active vendor based in Berlin, DE. Credit rating: 2.*
> *Transaction history: 247 purchases over 38 months, primarily Industrial Components and Logistics Services (high frequency).*
> *Open issues: 3 (billing).*
> *Linked counterparties: 14.*

Example output on first write (no signals yet):

> *Contoso GmbH is an active vendor based in Berlin, DE. Credit rating: 2.*

Both are valid embeddings. The second will be overwritten automatically when signals accumulate and the hash changes.

### Step C — Append Free-Text Fields

After the narrative, any `NARRATIVE_FREETEXT` fields present on the entity are appended verbatim:

```python
def build_embed_input(
    narrative: str,
    entity: CdmEntity,
    specs: list[CdmFieldSpec],
) -> str:
    freetext_parts = [
        entity.fields[s.field_name]
        for s in specs
        if "NARRATIVE_FREETEXT" in s.roles and entity.fields.get(s.field_name)
    ]
    return "\n\n".join([narrative] + freetext_parts)
```

The combined string is the sole input to the embedding model. The source string is **never stored**.

### Step D — Hash Gate

```python
import hashlib

def compute_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()

async def embedding_with_gate(
    embed_input: str,
    stored_hash: str | None,
    stored_model_version: str | None,
    current_model_version: str,
    embed_fn: Callable[[str], list[float]],
) -> tuple[list[float] | None, str, bool]:
    """
    Returns (vector, new_hash, did_embed).
    If hash matches AND model version unchanged → returns (None, stored_hash, False).
    """
    new_hash = compute_hash(embed_input)
    if new_hash == stored_hash and current_model_version == stored_model_version:
        return None, stored_hash, False   # skip embedding API call
    vector = await embed_fn(embed_input)  # only call if content changed
    return vector, new_hash, True
```

The embedding API is only called when the content hash or model version changes. This makes re-processing the same CDM event cheap — the surface fields are updated, the narrative is rebuilt and hashed, and if the hash matches, the vector is left untouched.

---

## Functional Requirements

| ID | Requirement | Priority |
|---|---|---|
| ES-FR-01 | Consume `{tid}.m1.entity_routed` and upsert the document into the correct index when `cdm_entity_storage_config.embeddable = true` | Must |
| ES-FR-02 | Consume `{tid}.m1.entity_removed` — soft-delete by setting `is_deleted = true` | Must |
| ES-FR-03 | Consume `nexus.materialization.changed` — update `materialization_level`; add/remove embedding on tier change | Must |
| ES-FR-04 | Extract Track 1 surface fields via `FieldManifest.surface_fields(entity_type)` — do not hard-code field names | Must |
| ES-FR-05 | Build Track 2 embedding input via `NarrativeBuilder` + `NARRATIVE_FREETEXT` fields as per §Track 2 | Must |
| ES-FR-06 | Apply hash gate before calling embedding API — skip if `narrative_hash` and `embedding_model_version` unchanged | Must |
| ES-FR-07 | Read behavioral signals via `BehavioralSignalReader` before building narrative | Must |
| ES-FR-08 | Never persist narrative text or free-text source content in the ES document | Must |
| ES-FR-09 | Write `entity_store_presence` register entry after every successful upsert | Must |
| ES-FR-10 | Emit `nexus.m3.write_completed` with `store='elasticsearch'` and `status='ok'|'skipped'|'error'` after each operation | Must |
| ES-FR-11 | Idempotency: re-processing the same event produces the same document state | Must |
| ES-FR-12 | Create index with correct mapping (§ Index Schema) if it does not exist | Must |
| ES-FR-13 | Bulk indexing: batch up to 100 documents per `BulkIndexer` flush, configurable via `cdm_entity_storage_config.es_bulk_size` | Should |
| ES-FR-14 | Materialization routing: `hot` entities → surface fields + dense embedding; `warm` → surface fields only (no vector); `cold` → not written | Should |
| ES-FR-15 | PII check: if a field is marked `pii: True` in the manifest, exclude it regardless of role assignment | Must |

---

## Non-Functional Requirements

| ID | Requirement | Target |
|---|---|---|
| ES-NFR-01 | P95 write latency (single upsert, excluding embedding API call) | ≤ 150ms |
| ES-NFR-02 | Throughput | ≥ 500 upserts/sec per Kafka partition |
| ES-NFR-03 | Index mapping: `type: dense_vector`, `dims: 1536`, `index: true`, `similarity: cosine` | — |
| ES-NFR-04 | All operations use `elasticsearch-py` v8.x | — |
| ES-NFR-05 | Circuit breaker: if cluster health is RED, pause consumption and emit `status='circuit_open'` | Must |
| ES-NFR-06 | Embedding API calls must be batched when processing bulk events (one batch call per flush, not one call per document) | Should |

---

## Index Schema

### Naming convention

```
nexus_{tenant_slug}_{cdm_entity_type_lower}
```

Examples: `nexus_acme_corp_party`, `nexus_globex_contact`. Tenant slug = slugified `tenant_id`. Provisioned by `nexus_core.provisioning.onboard_tenant()` — never created by the writer at write time.

### Index mapping (applied at creation time)

The mapping has three zones: system fields (fixed), surface fields (per-entity-type, declared explicitly), and the embedding vector.

```json
{
  "settings": {
    "number_of_shards": 2,
    "number_of_replicas": 1,
    "index.knn": true,
    "analysis": {
      "analyzer": {
        "edge_ngram_analyzer": {
          "tokenizer": "edge_ngram_tokenizer",
          "filter": ["lowercase"]
        }
      },
      "tokenizer": {
        "edge_ngram_tokenizer": {
          "type": "edge_ngram",
          "min_gram": 2,
          "max_gram": 20,
          "token_chars": ["letter", "digit"]
        }
      }
    }
  },
  "mappings": {
    "dynamic": "strict",
    "properties": {

      "cdm_entity_id":           { "type": "keyword" },
      "tenant_id":               { "type": "keyword" },
      "cdm_entity_type":         { "type": "keyword" },
      "golden_record_id":        { "type": "keyword" },
      "materialization_level":   { "type": "keyword" },
      "is_deleted":              { "type": "boolean" },
      "source_systems":          { "type": "keyword" },
      "cdm_version":             { "type": "keyword" },
      "created_at":              { "type": "date" },
      "updated_at":              { "type": "date" },

      "party_name":              { "type": "text", "analyzer": "edge_ngram_analyzer", "search_analyzer": "standard" },
      "party_subtype":           { "type": "keyword" },
      "city":                    { "type": "keyword" },
      "country_code":            { "type": "keyword" },
      "is_active":               { "type": "boolean" },

      "full_name":               { "type": "text", "analyzer": "edge_ngram_analyzer", "search_analyzer": "standard" },
      "job_title":               { "type": "keyword" },
      "department":              { "type": "keyword" },

      "transaction_type":        { "type": "keyword" },
      "currency_code":           { "type": "keyword" },
      "status":                  { "type": "keyword" },

      "entity_embedding":        { "type": "dense_vector", "dims": 1536, "index": true, "similarity": "cosine" },
      "narrative_hash":          { "type": "keyword" },
      "embedding_model_version": { "type": "keyword" },
      "embedding_updated_at":    { "type": "date" }
    }
  }
}
```

> **`dynamic: strict`** — any field not declared above will be rejected at write time. To add a new surface field: update the mapping here (owned by Dev 5) and add the `CdmFieldSpec` to the field manifest registry.

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
  "cdm_entity_type":        "party",
  "golden_record_id":       "gr:abc123",
  "cdm_fields":             { "party_name": "Contoso GmbH", "city": "Berlin", "country_code": "DE", "party_subtype": "vendor", "is_active": true, "credit_rating": "2", "notes_text": "Long-standing supplier..." },
  "provenance_hash":        "sha256-...",
  "materialization_level":  "hot",
  "source_systems":         ["sap", "salesforce"],
  "cdm_version":            "v3.2",
  "routed_stores":          ["elasticsearch", "neo4j", "timescaledb"]
}
```

Note: `cdm_fields` carries the full CDM payload. The writer extracts surface fields and narrative inputs from this map using the FieldManifest. No pre-computed embedding is passed — the ES writer generates its own.

Action: upsert if `routed_stores` contains `"elasticsearch"`.

#### `{tid}.m1.entity_removed`
```json
{
  "event_id":       "sha256-...",
  "tenant_id":      "acme-corp",
  "cdm_entity_id":  "uuid",
  "cdm_entity_type":"party"
}
```
Action: set `is_deleted = true`; update `entity_store_presence` row setting `es_present = FALSE`.

#### `nexus.materialization.changed`
```json
{
  "tenant_id":            "acme-corp",
  "cdm_entity_id":        "uuid",
  "cdm_entity_type":      "party",
  "old_level":            "warm",
  "new_level":            "hot",
  "cdm_fields":           { "party_name": "Contoso GmbH", "..." : "..." }
}
```
Action: update `materialization_level`. If promoting to `hot`, run full Track 2 pipeline to generate embedding. If demoting to `warm`, set `entity_embedding = null` and clear `narrative_hash`.

### Produced

#### `nexus.m3.write_completed`
```json
{
  "store":          "elasticsearch",
  "tenant_id":      "acme-corp",
  "cdm_entity_id":  "uuid",
  "status":         "ok | skipped | error | circuit_open",
  "skipped_reason": "narrative_hash_match | not_routed | entity_absent",
  "did_embed":      true,
  "latency_ms":     42,
  "timestamp":      "2026-04-29T10:00:00Z"
}
```

`did_embed: false` when the hash gate short-circuited the embedding API call (surface fields may still have been updated).

---

## Upsert Algorithm

```python
async def handle_entity_routed(event: EntityRoutedEvent):
    if "elasticsearch" not in event.routed_stores:
        return

    index = index_name(event.tenant_id, event.cdm_entity_type)
    await ensure_index_exists(index)

    specs = FieldManifest.for_entity_type(event.cdm_entity_type)

    # --- Track 1: surface fields ---
    surface = extract_surface_fields(event.cdm_fields, specs)

    # --- Track 2: semantic embedding (hot entities only) ---
    vector, narrative_hash, did_embed = None, None, False
    if event.materialization_level == "hot":
        signals = await BehavioralSignalReader().read(
            event.tenant_id, event.cdm_entity_id, event.cdm_entity_type
        )
        narrative_result = NarrativeBuilder().build(event.cdm_fields, specs, signals)
        embed_input      = build_embed_input(narrative_result.text, event.cdm_fields, specs)

        existing = await es.get(index=index, id=event.cdm_entity_id, ignore=404)
        stored_hash    = existing["_source"].get("narrative_hash")   if existing else None
        stored_model_v = existing["_source"].get("embedding_model_version") if existing else None

        vector, narrative_hash, did_embed = await embedding_with_gate(
            embed_input, stored_hash, stored_model_v,
            current_model_version=EMBEDDING_MODEL_VERSION,
            embed_fn=embed_texts,
        )
        if not did_embed:
            # surface fields still need to be updated; embedding left unchanged
            vector = existing["_source"].get("entity_embedding")
            narrative_hash = stored_hash

    doc = {
        **surface,
        "cdm_entity_id":          event.cdm_entity_id,
        "tenant_id":              event.tenant_id,
        "cdm_entity_type":        event.cdm_entity_type,
        "golden_record_id":       event.golden_record_id,
        "materialization_level":  event.materialization_level,
        "is_deleted":             False,
        "source_systems":         event.source_systems,
        "cdm_version":            event.cdm_version,
        "updated_at":             utcnow(),
        # Track 2 fields (None for warm entities)
        "entity_embedding":       vector,
        "narrative_hash":         narrative_hash,
        "embedding_model_version": EMBEDDING_MODEL_VERSION if did_embed else stored_model_v,
        "embedding_updated_at":   utcnow() if did_embed else None,
    }

    await es.update(
        index=index,
        id=event.cdm_entity_id,
        body={"doc": doc, "doc_as_upsert": True},
        retry_on_conflict=3,
    )

    await upsert_entity_store_presence(event.tenant_id, event.cdm_entity_id, es_present=True)
    await emit_write_completed(status="ok", did_embed=did_embed)
```

### Store write ordering within one pipeline run

TimescaleDB and Neo4j writers complete before the ES writer within the same pipeline run. This ensures `BehavioralSignalReader` sees the current run's signals, not the prior run's. The M3 writer orchestrator (Dev 5) enforces this ordering — do not add ordering logic to your handler.

---

## Query Contract (for Query Engine reference)

### Hybrid query — pre-filter + kNN

The standard query pattern is a pre-filter on surface fields (fast, Boolean) followed by kNN on the filtered candidate set (slow, vector). Do not use post-filter kNN — it wastes candidates and degrades recall.

```json
POST /nexus_acme_corp_party/_search
{
  "knn": {
    "field":          "entity_embedding",
    "query_vector":   [0.12, -0.34, "..."],
    "k":              10,
    "num_candidates": 100,
    "filter": {
      "bool": {
        "must": [
          { "term":  { "is_deleted":    false        } },
          { "term":  { "party_subtype": "vendor"     } },
          { "term":  { "country_code":  "DE"         } },
          { "term":  { "is_active":     true         } }
        ]
      }
    }
  },
  "_source": ["cdm_entity_id", "golden_record_id", "cdm_entity_type",
              "party_name", "city", "materialization_level"]
}
```

### Name fuzzy search (no vector required)

For cases where the user supplies a name string and expects near-exact matches:

```json
POST /nexus_acme_corp_party/_search
{
  "query": {
    "bool": {
      "must":   { "match": { "party_name": { "query": "Contoso", "fuzziness": "AUTO" } } },
      "filter": { "term": { "is_deleted": false } }
    }
  }
}
```

### What NOT to return in `_source`

Never return `entity_embedding` to the query layer — it is large and unused by callers. Always use `_source` inclusion list.

---

## Edge Cases

| Scenario | Handling |
|---|---|
| Entity has no `NARRATIVE_FREETEXT` fields present | Narrative text only; still a valid embedding |
| All CDM fields are `EXCLUDED` or `NARRATIVE_STRUCTURED` only (no surface fields) | Surface dict is empty; entity still written with embedding and system fields |
| `BehavioralSignalReader` times out or returns error | Use zero-value `BehavioralSignals`; log warning; do not block write |
| Hash unchanged but `embedding_model_version` changed | Force re-embed regardless of text hash — model upgrade invalidates all existing vectors |
| ES cluster health RED | Circuit breaker opens; consumer pauses; alert emitted; auto-resume after 60s health check |
| `entity_removed` for entity not in ES | Check `entity_store_presence` first; if `es_present = FALSE` (or row absent), emit `skipped` |
| Warm entity promoted to hot | `nexus.materialization.changed` triggers full Track 2 pipeline |
| Hot entity demoted to warm | Set `entity_embedding = null`; clear `narrative_hash` and `embedding_model_version` |
| Embedding dimension mismatch | Reject event; emit `error`; alert — do not write partial document |
| New entity type with no manifest registered | Raise `MissingManifestError`; emit `error`; do not write; alert Dev 5 |

---

## Implementation Phases

### Phase 1 — Setup (Weeks 1–2)

**D2A-01 · Client, index lifecycle, schema** (2 days · Must)
- `elasticsearch-py` v8.x client; connection pool; health check
- `ensure_index_exists()` with full mapping from §Index Schema
- `index_name()` helper; slug normalisation
- Write and query one test document; confirm kNN and fuzzy name search both work

**D2A-02 · Track 1 — Surface indexing** (1.5 days · Must · Depends on D2A-01)
- `extract_surface_fields()` wired to `FieldManifest`
- Upsert with surface-only document (no embedding yet)
- `entity_store_presence` update after write

**D2A-03 · Track 2 — Embedding pipeline** (2.5 days · Must · Depends on D2A-02)
- `NarrativeBuilder` integration; `BehavioralSignalReader` integration
- `build_embed_input()` combining narrative + free text
- Hash gate; embedding API call; vector storage
- Verify: same event processed twice → `did_embed: false` on second pass

### Phase 2 — Implementation (Weeks 3–6)

**D2A-04 · Tombstone and deletion** (1.5 days · Must · Depends on D2A-02)
- `entity_removed` handler: set `is_deleted=true`; clear `entity_embedding`
- Nightly Delete By Query for `is_deleted=true` older than 24h

**D2A-05 · Materialization tier changes** (1.5 days · Must · Depends on D2A-03)
- `nexus.materialization.changed` handler
- Warm→hot: run full Track 2; Hot→warm: null the vector

**D2A-06 · Bulk indexer** (1.5 days · Should · Depends on D2A-02)
- Accumulate up to `es_bulk_size` docs per `_bulk` call; batch embedding API calls
- Flush on batch size or 500ms timeout

### Phase 3 — Integration (Weeks 7–9)

**D2A-07 · Circuit breaker and throughput validation** (1.5 days · Must)
- Circuit breaker: RED cluster → pause consumer → resume after 60s
- Throughput target: ≥ 500 upserts/sec per partition documented

---

## Open Questions

| OQ | Status | Question |
|---|---|---|
| ES-OQ-01 | ❌ Open | Per-tenant ES cluster (hard isolation) vs. shared cluster with index-level ACL? Affects `nexus_core.get_es_client()` signature. |
| ES-OQ-02 | ❌ Open | Embedding model confirmed as `text-embedding-3-small` (1536 dims). Model change requires full reindex — cannot change `dims` after index creation. Document upgrade path before production. |
| ES-OQ-03 | ❌ Open | `BehavioralSignalReader` read targets: direct TimescaleDB SQL query or a pre-aggregated metrics view? Affects latency budget for hot-path writes. |
| ES-OQ-04 | ❌ Open | Narrative template ownership: who writes `NarrativeBuilder` for each entity type? Currently assumed Dev 5 or Dev A. Needs explicit ownership assignment. |

---

## Acceptance Criteria

- [ ] 1,000 `entity_routed` events processed; correct surface fields and embedding stored per entity type
- [ ] Re-consuming the same 1,000 events produces zero embedding API calls (all hash-gate skips)
- [ ] Surface field updates on re-consume still write correctly even when embedding is skipped
- [ ] `entity_removed` sets `is_deleted=true`; subsequent kNN query does not return the document
- [ ] Hybrid kNN query (pre-filter + vector) returns correct candidates in < 200ms on 10k-document index
- [ ] `entity_store_presence` correctly reflects `es_present=TRUE` after upsert and `es_present=FALSE` after tombstone
- [ ] `write_completed` events emitted for every input event with correct `did_embed` flag
- [ ] Circuit breaker: consumer pauses with ES stopped; resumes after cluster restart

---

*NEXUS Iteration 2 · nexus-m3-writer · Elasticsearch Handler · v0.2 · Mentis Consulting · April 2026 · Confidential*
