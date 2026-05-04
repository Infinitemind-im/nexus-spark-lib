# Iteration 2 — Life of a Record: Structured Path End-to-End Walkthrough

**Version:** v0.1 (draft)
**Date:** 2026-04-27
**Companion to:** `iter2-cdm-to-aistores-pipeline-v0.1.md`
**Scope:** Structured path only. Document path will be covered in a separate walkthrough.

This document traces one concrete record from the moment it appears in a source system to the moment `nexus.m3.write_completed` is emitted. Every phase names the responsible service, the inputs read, the outputs written, the Kafka topics involved, and the database tables touched. The example is illustrative; volumes, IDs, and values are made up.

---

## 0. Setup — What Already Exists

The walkthrough assumes the following state is already in place. Each item is a precondition; if it weren't true, the lifecycle below would diverge in identifiable ways called out in §13 (Edge Cases).

**Tenant.** `tenant_id = a1b2c3d4-...-acme` (Acme Corp).

**Connectors registered** in `nexus_system.connectors`:

| connector_id | source_system | type | mode |
|---|---|---|---|
| `c-sf-001` | salesforce | CRM | Debezium CDC |
| `c-sap-001` | sap_erp | ERP | Airbyte batch |
| `c-zd-001` | zendesk | ticketing | Airbyte batch |

**CDM published.** The `Party` entity has been approved by M4 governance with the canonical attributes `legal_name`, `tax_id`, `industry`, `domain`, `primary_phone`, `address`. Field-level mappings exist in `nexus_system.cdm_mappings` for all three structured sources.

**Materialization level.** `(a1b2c3d4-...-acme, 'Party') = 'hot'` in `nexus_system.cdm_entity_materialization`.

**Existing Golden Record.** A previous batch from SAP already created `gr:9f8e7d6c5b4a39281706` for "Globex Corporation" with provenance:

| attribute | source_system | source_record_id | rule_kind |
|---|---|---|---|
| `legal_name` | sap_erp | `0000012847` | source_priority |
| `tax_id` | sap_erp | `0000012847` | most_recent |
| `address` | sap_erp | `0000012847` | most_complete |

No Salesforce or Zendesk record has been resolved to this Golden Record yet.

**Now an event happens in Salesforce:** a sales rep creates a new Account record. We follow that record from this moment.

---

## 1. T₀ — Source Mutation

```
13:42:08.103 UTC  Salesforce → Account.insert
                  Id:        001Hs00003xYZABC
                  Name:      Globex Corp.
                  Industry:  Manufacturing
                  Phone:     +1 (555) 019-9
                  Website:   globex.com
                  CreatedDate: 2026-04-27T13:42:08.103Z
```

The Salesforce database commits the row. Salesforce's CDC stream emits a row-level change event on its internal change log.

**Owner:** External system. NEXUS not yet involved.

---

## 2. T₁ — Connector Extraction (Debezium CDC)

**Service:** `debezium-connect` cluster (in M5 infrastructure, not a NEXUS-owned service per `NEXUS-Iter2-SPEC-ServiceTopology-v0.4.md`).

Debezium's Salesforce source connector reads the change event and publishes it to its own raw topic, with the connector's tenant tag in the topic name:

```
Topic:    cdc.salesforce.acme.Account
Key:      001Hs00003xYZABC
Payload:  {
  "before": null,
  "after": {
    "Id": "001Hs00003xYZABC",
    "Name": "Globex Corp.",
    "Industry": "Manufacturing",
    "Phone": "+1 (555) 019-9",
    "Website": "globex.com",
    "CreatedDate": "2026-04-27T13:42:08.103Z"
  },
  "source": { "ts_ms": 1745763728103, "connector": "salesforce", "name": "c-sf-001", ... },
  "op": "c",
  "ts_ms": 1745763728145
}
```

**Latency budget so far:** ~50 ms (Salesforce → Debezium publish).

---

## 3. T₂ — Raw Capture into Delta Lake

**Service:** `nexus-m1-worker` (Iter 1, existing).
**Consumer group:** `m1-worker-cdc-salesforce`.
**Inputs:** `cdc.salesforce.acme.Account`.
**Outputs:** Delta Lake write; Kafka publish.

`nexus-m1-worker` consumes the Debezium event, attaches NEXUS metadata, and writes the raw payload to Delta Lake.

**Delta Lake target:** `s3://nexus-deltalake/raw/{tenant_id}/{source_system}/{source_table}/year=2026/month=04/day=27/`

**Row written (parquet):**

```
{
  "tenant_id":          "a1b2c3d4-...-acme",
  "connector_id":       "c-sf-001",
  "source_system":      "salesforce",
  "source_table":       "Account",
  "source_record_id":   "001Hs00003xYZABC",
  "source_op":          "INSERT",
  "source_ts":          "2026-04-27T13:42:08.103Z",
  "ingested_at":        "2026-04-27T13:42:08.520Z",
  "raw_payload":        { ...the Salesforce after-image as JSON... },
  "ingest_offset":      189471
}
```

After the Delta Lake commit returns, `nexus-m1-worker` publishes:

```
Topic:    m1.int.raw_records          (where {tid} = a1b2c3d4-...-acme)
Key:      "salesforce::Account::001Hs00003xYZABC"
Headers:  tenant_id, connector_id, source_op
Payload:  {
  "delta_pointer": "s3://nexus-deltalake/raw/.../part-00007.parquet#row=42",
  "tenant_id":     "a1b2c3d4-...-acme",
  "connector_id":  "c-sf-001",
  "source_system": "salesforce",
  "source_table":  "Account",
  "source_record_id": "001Hs00003xYZABC",
  "source_op":     "INSERT",
  "source_ts":     "2026-04-27T13:42:08.103Z"
}
```

The Kafka offset is committed only after the Delta Lake write returns and the publish acknowledges. If `nexus-m1-worker` crashes between the Delta write and the publish, the next replica re-reads the same Debezium event, the Delta Lake write is idempotent on `(tenant_id, source_system, source_table, source_record_id, source_op, source_ts)`, and the publish goes through.

**Latency budget so far:** ~500 ms.

---

## 4. T₃ — Spark Transformation: Normalisation (Stage 1)

**Service:** `nexus-spark-transformer` (existing, extended per spec §3.1).
**Consumer group:** `spark-transformer`.
**Inputs:** `m1.int.raw_records`; reads `nexus_system.tenants.base_currency`; reads `nexus_system.entity_blocking_rules` (NEW table).
**Outputs:** Normalised record in Spark working memory; not yet published.

The Spark job (running as a long-lived structured-streaming Deployment per OQ-SP-01 in `iter2-gap-analysis-v0.1.md`) micro-batches every 30 seconds. Our record arrives in batch number 4,128.

**Normalisation operations:**

| Operation | Result |
|---|---|
| Type coercion | `Phone` parsed via `phonenumbers` → E.164 `+15550199` |
| Timestamp canonicalisation | `CreatedDate` → UTC `2026-04-27T13:42:08.103Z` (already UTC, no change) |
| FX normalisation | No monetary fields on this record; no-op |
| Whitespace stripping | `Name` "Globex Corp." → "Globex Corp." (no change) |
| Domain canonicalisation | `Website` → `domain = "globex.com"` (lowercase, scheme stripped) |
| Blocking key | Per `entity_blocking_rules` for `Party`: `lower(left(legal_name, 4)) || ":" || left(domain, 6)` → `"glob:globex"` |

**Tier 1/2/3 not yet evaluated** — that happens in Phase 6 (CDM Mapper). Don't conflate with materialization level (already known to be `hot` for the entity type).

**Spark working representation:**

```python
NormalisedRecord(
  tenant_id =          "a1b2c3d4-...-acme",
  source_system =      "salesforce",
  source_record_id =   "001Hs00003xYZABC",
  cdm_entity_type =    "Party",                # inferred from connector + table mapping
  canonical_attrs = {
    "legal_name":      "Globex Corp.",
    "industry":        "Manufacturing",
    "primary_phone":   "+15550199",
    "domain":          "globex.com"
  },
  blocking_key =       "glob:globex",
  source_ts =          "2026-04-27T13:42:08.103Z"
)
```

No Kafka publish yet — the same Spark job continues into Stage 2.

---

## 5. T₄ — Spark Transformation: Entity Resolution (Stage 2)

Within the same micro-batch, Spark runs the three-signal resolver per spec §3.2.

### 5.1 Materialization-aware depth check

Spark looks up `cdm_entity_materialization` for `(tenant_id, 'Party')` → `hot`. **Full three-signal resolution runs.** (If it had been `warm`, only Signal A would run; `cold` records never reach Spark.)

### 5.2 Signal A — Deterministic match

The deterministic identifier columns for `Party` (per `nexus_system.deterministic_id_columns`) are `tax_id`, `domain`, `duns_number`. Our record has only `domain = "globex.com"`.

Spark queries `nexus_system.entity_resolution_index` filtered by `tenant_id` and `cdm_entity_type = 'Party'`, projecting attributes through `golden_record_provenance`. Looking for any existing Golden Record whose `domain` attribute equals `globex.com`.

**Result:** No deterministic match. The existing `gr:9f8e7d6c5b4a39281706` was created from SAP, which carries `tax_id` but not `domain`. Signal A returns no match.

### 5.3 Signal B — Probabilistic match

LSH blocking on `blocking_key = "glob:globex"` returns one candidate Golden Record from the bucket: `gr:9f8e7d6c5b4a39281706` (SAP's "Globex Corporation").

Per-attribute similarity computed against the candidate's current provenance values:

| attribute | new value | candidate value | function | score |
|---|---|---|---|---|
| `legal_name` | "Globex Corp." | "Globex Corporation" | Jaro-Winkler | 0.945 |
| `tax_id` | (null) | "US-87-4421938" | exact | (skipped — null) |
| `domain` | "globex.com" | (null) | exact | (skipped — null) |
| `primary_phone` | "+15550199" | (null) | exact | (skipped — null) |

Per `nexus_system.er_thresholds` for `(acme, 'Party')`:

```
{
  "weights": { "legal_name": 0.55, "tax_id": 0.30, "domain": 0.10, "primary_phone": 0.05 },
  "auto_apply_threshold": 0.92,
  "review_lower_bound": 0.75
}
```

Combined score (only `legal_name` contributes; weights re-normalised):
`0.945 × 1.0 = 0.945`.

**Score sits at 0.945 — above the 0.92 auto-apply threshold.** Match accepted on Signal B alone. Signal C is **not** consulted (short-circuit per spec §3.2).

### 5.4 Resolution outcome

Spark assigns `cdm_entity_id = "gr:9f8e7d6c5b4a39281706"` to the new Salesforce record (the existing Golden Record absorbs it).

**Writes to PostgreSQL `nexus_system.entity_resolution_index`:**

```sql
INSERT INTO nexus_system.entity_resolution_index (
  cdm_entity_id, tenant_id, connector_id, source_system,
  source_table, source_record_id, confidence, resolution_method,
  resolved_at
) VALUES (
  'gr:9f8e7d6c5b4a39281706', 'a1b2c3d4-...-acme', 'c-sf-001', 'salesforce',
  'Account', '001Hs00003xYZABC', 0.945, 'spark_probabilistic',
  '2026-04-27T13:42:38.812Z'
)
ON CONFLICT (tenant_id, connector_id, source_table, source_record_id)
DO UPDATE SET cdm_entity_id = EXCLUDED.cdm_entity_id,
              confidence = EXCLUDED.confidence,
              resolution_method = EXCLUDED.resolution_method,
              resolved_at = EXCLUDED.resolved_at;
```

The `ON CONFLICT` clause makes re-resolution on event redelivery a no-op.

**No `er_review_queue` row** — auto-applied. **No `nexus.er.review_queued` event** emitted.

---

## 6. T₅ — Spark Transformation: Golden Record Synthesis (Stage 3)

Spark now reconciles the new contributing record against the existing Golden Record under the survivorship rules in `nexus_system.survivorship_rules` for `(acme, 'Party', *)`.

Survivorship table for Party at this tenant:

| attribute | rule_kind | config |
|---|---|---|
| `legal_name` | source_priority | `["sap_erp","salesforce","zendesk"]` |
| `tax_id` | source_priority | `["sap_erp","salesforce","zendesk"]` |
| `industry` | source_priority | `["salesforce","sap_erp","zendesk"]` |
| `primary_phone` | most_recent | `{}` |
| `domain` | most_recent | `{}` |
| `address` | most_complete | `{}` |

Per-attribute survivorship decisions:

| attribute | existing winner | new contender | decision |
|---|---|---|---|
| `legal_name` | sap_erp ("Globex Corporation") | salesforce ("Globex Corp.") | **sap_erp keeps** — higher priority |
| `tax_id` | sap_erp ("US-87-4421938") | (null) | **sap_erp keeps** |
| `industry` | (none) | salesforce ("Manufacturing") | **salesforce wins** — first observed and matches priority order |
| `primary_phone` | (none) | salesforce ("+15550199") | **salesforce wins** — only contender |
| `domain` | (none) | salesforce ("globex.com") | **salesforce wins** — only contender |
| `address` | sap_erp ("...") | (null) | **sap_erp keeps** |

**Writes to PostgreSQL `nexus_system.golden_record_provenance`:**

Three new rows are inserted (one per attribute the Salesforce record won or contributes to):

```sql
INSERT INTO nexus_system.golden_record_provenance (
  cdm_entity_id, tenant_id, attribute_name, source_system,
  source_record_id, source_attr_value_hash, observed_at, rule_kind,
  attribute_source_kind, visibility_policy
) VALUES
  ('gr:9f8e7d6c5b4a39281706','a1b2c3d4-...-acme','industry',
   'salesforce','001Hs00003xYZABC',
   sha256('Manufacturing'),'2026-04-27T13:42:08.103Z','source_priority',
   'structured','permissive'),
  ('gr:9f8e7d6c5b4a39281706','a1b2c3d4-...-acme','primary_phone',
   'salesforce','001Hs00003xYZABC',
   sha256('+15550199'),'2026-04-27T13:42:08.103Z','most_recent',
   'structured','permissive'),
  ('gr:9f8e7d6c5b4a39281706','a1b2c3d4-...-acme','domain',
   'salesforce','001Hs00003xYZABC',
   sha256('globex.com'),'2026-04-27T13:42:08.103Z','most_recent',
   'structured','permissive')
ON CONFLICT (cdm_entity_id, attribute_name, source_system, source_record_id)
DO UPDATE SET source_attr_value_hash = EXCLUDED.source_attr_value_hash,
              observed_at = EXCLUDED.observed_at,
              rule_kind = EXCLUDED.rule_kind;
```

Existing provenance rows for `legal_name`, `tax_id`, `address` are untouched — SAP keeps those slots.

**Crucially, no business values land in any AI store.** The values `"Manufacturing"`, `"+15550199"`, `"globex.com"` exist only in (a) Salesforce, (b) Delta Lake's raw layer for audit, and (c) the SHA-256 hashes in provenance for change detection. The query layer fetches them from Salesforce on demand.

### 6.1 Computing the provenance summary

Spark builds a small aggregate that summarises the post-synthesis state of the Golden Record. This is what flows downstream — not the values themselves:

```json
{
  "cdm_entity_id": "gr:9f8e7d6c5b4a39281706",
  "contributing_sources": ["sap_erp", "salesforce"],
  "attribute_provenance": {
    "legal_name":    "sap_erp:0000012847",
    "tax_id":        "sap_erp:0000012847",
    "industry":      "salesforce:001Hs00003xYZABC",
    "primary_phone": "salesforce:001Hs00003xYZABC",
    "domain":        "salesforce:001Hs00003xYZABC",
    "address":       "sap_erp:0000012847"
  },
  "provenance_hash": "sha256:1a2b3c..."
}
```

`provenance_hash` is the SHA-256 of the canonicalised provenance summary. Pinecone uses it to detect staleness (spec §3.4).

### 6.2 Spark publishes downstream

```
Topic:    m1.int.transformed_records
Key:      "Party::gr:9f8e7d6c5b4a39281706"
Payload:  {
  "tenant_id":         "a1b2c3d4-...-acme",
  "cdm_entity_id":     "gr:9f8e7d6c5b4a39281706",
  "cdm_entity_type":   "Party",
  "operation":         "UPSERT",
  "contributing_record": {
    "source_system":   "salesforce",
    "source_record_id":"001Hs00003xYZABC",
    "source_table":    "Account",
    "source_op":       "INSERT",
    "source_ts":       "2026-04-27T13:42:08.103Z"
  },
  "provenance_summary": { ... see 6.1 ... },
  "blocking_key":      "glob:globex",
  "delta_pointer":     "s3://nexus-deltalake/raw/.../part-00007.parquet#row=42"
}
```

**Latency budget so far:** ~30 seconds (Spark micro-batch interval dominates).

---

## 7. T₆ — CDM Field Classification

**Service:** `nexus-cdm-mapper` (existing, behaviour unchanged).
**Consumer group:** `cdm-mapper`.
**Inputs:** `m1.int.transformed_records`.
**Outputs:** `m1.int.cdm_entities_ready`.

The CDM Mapper applies the deterministic Tier 1/2/3 classification rules from `NEXUS-Iter2-CDM-Mapper-v0.3.md`. For each canonical attribute the contributing record provided:

| attribute | classifier confidence | tier | action |
|---|---|---|---|
| `industry` | 0.97 | Tier 1 | auto-applied |
| `primary_phone` | 0.99 | Tier 1 | auto-applied |
| `domain` | 0.98 | Tier 1 | auto-applied |

All three are Tier 1 — no Tier 2 review needed for this record. (If `industry` had returned 0.83, it would have triggered an `m1.int.mapping_failed` event for the M4 review queue, in parallel with the rest of the pipeline continuing for the other attributes.)

The mapper publishes:

```
Topic:    m1.int.cdm_entities_ready
Key:      "Party::gr:9f8e7d6c5b4a39281706"
Payload:  {
  "tenant_id":       "a1b2c3d4-...-acme",
  "cdm_entity_id":   "gr:9f8e7d6c5b4a39281706",
  "cdm_entity_type": "Party",
  "operation":       "UPSERT",
  "tier_summary": {
    "tier_1_attrs":  ["industry","primary_phone","domain"],
    "tier_2_attrs":  [],
    "tier_3_attrs":  []
  },
  "provenance_summary": { ...same as before, passed through... },
  "contributing_record": { ...passed through... },
  "delta_pointer":   "..."
}
```

**Latency budget so far:** ~30.4 seconds.

---

## 8. T₇ — Stage 0: Materialization Level Resolution

**Service:** `nexus-m1-worker` (Iter 1, extended per spec §3.0).
**Consumer group:** `m1-worker-op-router`.
**Inputs:** `m1.int.cdm_entities_ready`; reads `nexus_system.cdm_entity_materialization`.
**Outputs:** `{tid}.m1.entity_routed` OR `{tid}.m1.warm_recorded` OR `{tid}.m1.cold_skipped`.

**Lookup:** `cdm_entity_materialization` for `(a1b2c3d4-...-acme, 'Party')` → `materialization_level = 'hot'`.

**Routing decision:** Hot. The Op Router enriches the payload with the materialization level and dry-run flag, then publishes:

```
Topic:    {tid}.m1.entity_routed
Key:      "Party::gr:9f8e7d6c5b4a39281706"
Headers:  tenant_id=a1b2c3d4-...-acme,
          materialization_level=hot,
          dry_run=false
Payload:  {
  "tenant_id":       "a1b2c3d4-...-acme",
  "cdm_entity_id":   "gr:9f8e7d6c5b4a39281706",
  "cdm_entity_type": "Party",
  "operation":       "UPSERT",
  "materialization_level": "hot",
  "dry_run":         false,
  "tier_summary":    { "tier_1_attrs":["industry","primary_phone","domain"], "tier_2_attrs":[], "tier_3_attrs":[] },
  "provenance_summary": { ... },
  "contributing_record": { ... },
  "delta_pointer":   "..."
}
```

If the level had been `warm`, the Op Router would have published `{tid}.m1.warm_recorded` (governance-only consumers) and **not** published `entity_routed`. M3-writer would never have seen the record. The Salesforce row would still sit in Delta Lake, queryable on demand by `nexus-query-executor` phase 2 with a deterministic-only resolution check.

**Latency budget so far:** ~30.5 seconds.

---

## 9. T₈ — Stage 4: M3 Projection

**Service:** `nexus-m3-writer` (existing, extended per spec §3.4).
**Consumer group:** `m3-writer-entities`.
**Inputs:** `{tid}.m1.entity_routed`; reads `nexus_system.golden_record_provenance` (for the affected `cdm_entity_id`); reads `nexus_system.schema_snapshots` (for PII flags); reads Salesforce live (for the values to embed — see 9.1).
**Outputs:** Pinecone, Neo4j, TimescaleDB writes; `nexus.m3.write_completed`.

The M3 writer parallelises the three store writes. They are independent of each other; failure in one does not block the others. Below they are described sequentially for clarity.

### 9.1 Pinecone write

The writer needs **text** to feed the embedding model. Per the Virtual CDM principle, the canonical values are not in any nexus store. The writer fetches them live:

1. Reads `golden_record_provenance` for `gr:9f8e7d6c5b4a39281706` — this gives the source pointers per attribute.
2. For each pointer, calls the appropriate connector worker via the existing connector-RPC mechanism.

For our entity, the writer assembles:

```
legal_name (sap_erp:0000012847)     → "Globex Corporation"
industry (salesforce:001Hs00003xYZABC) → "Manufacturing"
domain (salesforce:001Hs00003xYZABC)   → "globex.com"
address (sap_erp:0000012847)        → "100 Industrial Way, Wilmington DE"
```

PII fields per `schema_snapshots` are dropped. `tax_id` is flagged PII at this tenant — excluded from the embedding text. `primary_phone` is also PII-flagged — excluded.

Embedding text (transient, discarded after `embed()` returns):
```
"Globex Corporation. Manufacturing. globex.com. 100 Industrial Way, Wilmington DE."
```

The writer calls `EmbeddingClient.embed(text)` (OpenAI `text-embedding-3-small`, 1536 dims) and upserts into Pinecone:

```
Index:        a1b2c3d4-...-acme-entities
Vector ID:    a1b2c3d4-...-acme::Party::gr:9f8e7d6c5b4a39281706
Vector:       [0.0123, -0.0456, ...]   (1536 floats)
Metadata: {
  "tenant_id":             "a1b2c3d4-...-acme",
  "cdm_entity_id":         "gr:9f8e7d6c5b4a39281706",
  "cdm_entity_type":       "Party",
  "contributing_sources":  ["sap_erp","salesforce"],
  "materialization_level": "hot",
  "embedding_model_version":"openai/text-embedding-3-small@2025-01-15",
  "provenance_hash":       "sha256:1a2b3c...",
  "deleted":               false
}
```

Idempotency: re-upsert of the same vector ID overwrites in place — safe on event replay.

If `provenance_hash` matches the previous upsert and nothing else changed, the writer **skips** the embedding call — it would produce the same vector. This is the OQ-PROV-01 short-circuit; default behaviour in this spec.

### 9.2 Neo4j write

The writer issues a single Cypher transaction. Node MERGE (idempotent on `(id, tenant_id)` composite unique constraint):

```cypher
MERGE (p:Party { id: 'gr:9f8e7d6c5b4a39281706', tenant_id: 'a1b2c3d4-...-acme' })
ON CREATE SET p.created_at = $now,
              p.materialization_level = 'hot'
ON MATCH SET  p.updated_at = $now,
              p.materialization_level = 'hot'
;
```

Since this Golden Record already exists (created from the SAP batch), `ON MATCH` fires — only `updated_at` is bumped.

For the Salesforce contributing record, the writer also reflects any **new edges**. The Salesforce Account has child Contacts and Opportunities (separately processed records that resolve to other Golden Records over time). Those edges are written as their own `entity_routed` events arrive. For *this* record, no immediate child edges exist yet.

Suppose, hypothetically, a Contact record `003Hs00003xZYZ` was processed earlier and resolved to `gr:abc...contact-sarah-chen`. The Salesforce join `Contact.AccountId = 001Hs00003xYZABC` would have produced this edge:

```cypher
MERGE (p:Party { id: 'gr:9f8e7d6c5b4a39281706', tenant_id: 'a1b2c3d4-...-acme' })
MERGE (c:Party { id: 'gr:abc...contact-sarah-chen', tenant_id: 'a1b2c3d4-...-acme' })
MERGE (p)-[r:HAS_CONTACT { source_fk: 'salesforce:Contact.AccountId' }]->(c)
ON CREATE SET r.since = $source_ts,
              r.connector_id = 'c-sf-001',
              r.materialization_level = 'hot',
              r.created_at = $now
;
```

Note the relationship MERGE key includes `source_fk` — per spec FR-M-07 a logical edge from a different source (e.g. SAP's `VBAK.KUNNR`) coexists as a *separate* edge with its own provenance, rather than being deduplicated.

### 9.3 TimescaleDB write

Party creation is itself a business event. The writer emits one row to `nexus_ts.business_metrics_raw`:

```sql
INSERT INTO nexus_ts.business_metrics_raw (
  time, tenant_id, metric_name, normalised_value, base_currency,
  dimensions, source_system, cdm_entity_id, cdm_version,
  materialization_level, is_correction, is_deletion
) VALUES (
  '2026-04-27T13:42:08.103Z',
  'a1b2c3d4-...-acme',
  'party.created',
  1,
  'EUR',
  '{"industry":"Manufacturing","contributing_source":"salesforce"}'::jsonb,
  'salesforce',
  'gr:9f8e7d6c5b4a39281706',
  '2026-04-15-v3',
  'hot',
  FALSE,
  FALSE
)
ON CONFLICT (time, tenant_id, metric_name, cdm_entity_id) DO NOTHING;
```

Continuous aggregates `metrics_weekly`, `metrics_monthly`, and `metrics_yearly` will pick this up via their refresh policies (hourly / daily / weekly respectively). The writer does not refresh them inline.

### 9.4 Cross-store summary

Three writes complete. Latencies inside m3-writer:

| Store | p50 |
|---|---|
| Pinecone | ~120 ms (dominated by `embed()` API call; ~60 ms if hash short-circuit) |
| Neo4j | ~25 ms |
| TimescaleDB | ~10 ms |

All three succeed. The writer commits its Kafka offset.

---

## 10. T₉ — `nexus.m3.write_completed`

**Service:** `nexus-m3-writer`.
**Outputs:** `nexus.m3.write_completed`.

```
Topic:    nexus.m3.write_completed
Key:      "Party::gr:9f8e7d6c5b4a39281706"
Payload:  {
  "tenant_id":       "a1b2c3d4-...-acme",
  "cdm_entity_id":   "gr:9f8e7d6c5b4a39281706",
  "cdm_entity_type": "Party",
  "operation":       "UPSERT",
  "stores_written":  ["pinecone","neo4j","timescaledb"],
  "skipped_stores":  [],
  "embedding_model_version": "openai/text-embedding-3-small@2025-01-15",
  "provenance_hash": "sha256:1a2b3c...",
  "completed_at":    "2026-04-27T13:42:39.205Z",
  "trace_id":        "01HW8X7K...4ZQ"
}
```

Consumers:
- Governance dashboards (Grafana counter `nexus_m3_writes_completed_total{tenant,entity_type}`)
- The CDM Validation workflow (it consumes write_completed to know when an entity it just approved is queryable)
- Any downstream alerting

**Total latency, source mutation to write_completed:** ~31.1 seconds.
**Dominant cost:** Spark micro-batch interval (~30 seconds out of 31). Everything after Spark publishes is sub-second.

---

## 11. What Did NOT Happen

It's worth being explicit about what the lifecycle does **not** include, because the Virtual CDM principle and the materialization model deliberately constrain it.

The string `"Manufacturing"` is **not** stored in any NEXUS service except (a) the Salesforce live connection at query time, (b) Delta Lake's raw audit record (immutable, used only for replay), and (c) as a SHA-256 hash in `golden_record_provenance` for change detection. There is no row in PostgreSQL containing the literal value, no node property in Neo4j, and no copy in Pinecone metadata.

The `tax_id` and `primary_phone` PII values were **fetched live for embedding context** but not embedded into the vector text — the PII filter dropped them before `embed()`. They never left m3-writer's process memory.

The Golden Record `gr:9f8e7d6c5b4a39281706` was **not duplicated**. The Salesforce record absorbed into the existing SAP-sourced Golden Record. Both source records are now linked to the same `cdm_entity_id` via separate rows in `entity_resolution_index`.

The Salesforce Account itself was **not re-resolved** — the entity_resolution_index assignment from this run is the authoritative one going forward. Future updates from Salesforce on `001Hs00003xYZABC` will skip Signals A, B, C and use the cached `cdm_entity_id`.

No Tier 2 mapping review queue entry was created — all three contributed attributes were classified Tier 1.

No `er_review_queue` entry was created — the probabilistic match auto-applied above the 0.92 threshold.

---

## 12. Counterfactual: What Changes if `materialization_level = 'warm'`

If the entity type `Party` had been `warm` instead of `hot`, the lifecycle diverges starting at Phase 5 (Spark Stage 2):

- **Phase 5 (ER):** Only Signal A runs. Domain = `globex.com` doesn't match any existing Golden Record's `domain` (SAP record has none). Result: **no merge**. Spark assigns a fresh `cdm_entity_id = "gr:NEW_..."` and writes it to `entity_resolution_index` with `resolution_method = 'spark_deterministic_only'`.
- **Phase 6 (Synthesis):** Skipped. No `golden_record_provenance` rows written. The new `cdm_entity_id` exists but has no merged attribute history.
- **Phase 7 (CDM Mapper):** Runs as normal.
- **Phase 8 (Op Router):** Looks up materialization level → `warm`. Publishes `{tid}.m1.warm_recorded` instead of `entity_routed`. The payload includes the same fields but the topic name signals to consumers that this is a "Delta-Lake-only" record.
- **Phase 9 (M3 Writer):** Does not consume `warm_recorded`. **Nothing is written to Pinecone, Neo4j, or TimescaleDB.**
- **Phase 10:** No `nexus.m3.write_completed`.

The Salesforce record is queryable from `nexus-query-executor` phase 2 by routing the query directly to Salesforce. When the entity is later promoted to `hot` (manually or by usage signal), a backfill DAG (`m3-promotion-backfill`) replays the Delta Lake records for that entity type through the Spark transformer and onward, producing the missed Pinecone / Neo4j / TimescaleDB writes.

---

## 13. Edge Cases Touching This Lifecycle

The following are not exhaustive but cover the failure modes the test suite must address.

**EC-13.1 — Spark crashes between provenance write and Kafka publish.** Provenance rows are committed to PostgreSQL but `transformed_records` was never published. On restart, Spark re-reads the same Delta Lake offset; the `ON CONFLICT DO UPDATE` clauses on `entity_resolution_index` and `golden_record_provenance` make the re-write idempotent; the publish goes through.

**EC-13.2 — m3-writer succeeds on Pinecone, fails on Neo4j.** Per spec §7, the writer logs the partial success, emits `nexus.m3.write_failed` with `failed_stores: ["neo4j"]`, and commits the Kafka offset. The Pinecone write is not rolled back. A Grafana alert fires within 5 minutes. On Neo4j recovery, `m3-writer` is restarted with `--replay-from=<outage_start_offset>` against `{tid}.m1.entity_routed`. Pinecone re-upserts are no-ops; Neo4j MERGE catches up.

**EC-13.3 — Salesforce record updated to "Globex International Inc." (rebrand).** Debezium emits an `op: "u"` event. The lifecycle replays from Phase 2. ER Signal A still fails (no deterministic IDs change). Signal B re-runs against the existing Golden Record. New Jaro-Winkler on `legal_name` drops to 0.71 — *below* both thresholds. The match is rejected; Spark assigns a *new* `cdm_entity_id`. The old Golden Record's Salesforce contribution is removed. **This is an unwanted split.** Mitigation: a tenant administrator detects this through usage and uses the M4 split/merge tool to manually re-link. This is exactly the EC-02 case in the parent spec.

**EC-13.4 — Salesforce record deleted.** Debezium emits `op: "d"`. `nexus-m1-worker` publishes `{tid}.m1.entity_removed`. The m3-writer removes the Salesforce-sourced provenance rows (`industry`, `primary_phone`, `domain`). If any remain (`legal_name`, `tax_id`, `address` from SAP do remain), the Golden Record persists. Pinecone is re-upserted with new embedding text excluding the now-orphaned attributes. Neo4j edges with `source_fk` starting `salesforce:` are detached. TimescaleDB gets an `is_deletion=TRUE` row.

**EC-13.5 — Materialization level demoted from hot to warm mid-flight.** Salesforce record is in Spark Stage 2 when Tenant Admin demotes. Spark continues with full ER (it already started). When m3-writer receives the `entity_routed` event, it observes the demotion via `nexus.materialization.changed` and writes anyway (the event preceded the demotion). A nightly reconciliation DAG detects "warm entity with hot data" and removes the Pinecone vector and Neo4j relationships; the TimescaleDB row stays until natural retention.

**EC-13.6 — ER review band match.** Combined score is 0.83 — between thresholds. Spark assigns a *provisional* `cdm_entity_id` (the merge candidate's ID with a flag), publishes `nexus.er.review_queued`, and continues the pipeline. M3 writes proceed against the provisional ID — the Pinecone vector and Neo4j node carry `provisional=true` in metadata. M4 surfaces the review item to a data steward. On approval: vector and node are committed (flag cleared). On rejection: a fresh `cdm_entity_id` is minted, the provisional vector is tombstoned, the provisional Neo4j node is `DETACH DELETE`d, and the record is re-projected with the new ID.

---

## 14. Test Strategy Implications

This walkthrough drives the integration test plan for the seam:

- **Happy path test:** the exact scenario in Phases 1–10. Asserts every table write and every Kafka publish.
- **ER variants:** deterministic match (Signal A short-circuits), graph lift (Signal C salvages a review-band match), reject (no merge — fresh `cdm_entity_id`), review band (provisional flag flow).
- **Materialization variants:** hot, warm, cold, plus mid-flight promotion and demotion (EC-13.5).
- **Failure injection:** kill spark-transformer between Phase 5 and Phase 6 publish; kill m3-writer between Pinecone and Neo4j writes (EC-13.2); transient Pinecone 503; transient Neo4j connection reset.
- **Replay test:** delete `{tid}.m1.entity_routed` consumer group offset, restart m3-writer, assert no duplicate writes.
- **Cross-tenant safety test:** inject a record with `tenant_id` = tenant A in the payload but published to tenant B's topic — assert m3-writer rejects it before any store write.

---

## 15. References

- `iter2-cdm-to-aistores-pipeline-v0.1.md` — the parent spec defining stages, tables, topics, FRs.
- `NEXUS-Iter2-SPEC-ServiceTopology-v0.4.md` — service ownership and Kafka topic conventions.
- `NEXUS-Iter2-CDM-Mapper-v0.3.md` — Tier 1/2/3 classification semantics referenced in Phase 7.
- `NEXUS-Iter2-M3-AIStores-v0.4.md` — Virtual CDM principle, store-level idempotency contracts.
- `NEXUS-Iter2-SPEC-DataModel-v0.5.md` — table schemas this walkthrough cites.
- `NEXUS-Module-Responsibilities.md` — Rule 1, 2, 3 governing the structural choices visible in the trace.
