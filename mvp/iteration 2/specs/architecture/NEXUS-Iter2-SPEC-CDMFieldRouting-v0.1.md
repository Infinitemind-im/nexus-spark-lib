# NEXUS — Iteration 2 · CDM Field Routing
**Spec:** `NEXUS-Iter2-SPEC-CDMFieldRouting-v0.1`
**Part of Iteration 2 · closes routing gap identified in:** `NEXUS-Iter2-SPEC-M3-AIStores-v0.5.md` + `NEXUS-Iter2-SVC-nexus-m3-writer-stores-v0.1.md`
Mentis Consulting · Version 0.1 · April 2026 · Confidential

---

## Problem Statement

As currently specified across the Iteration 2 spec set, three separate artefacts hold overlapping routing knowledge with no formal derivation link between them:

| Artefact | What it stores | Owner | How it is kept current |
|---|---|---|---|
| `nexus_cdm_ground_truth_v3.json` | Source→CDM mapping labels (quality benchmark) | Data Lead | Manual curation |
| `nexus_system.cdm_entity_storage_config` | Entity-level `embeddable / graph_persistent / metricable` flags | Dev 5 | Hand-seeded at tenant provisioning |
| `FieldManifest` in `nexus_core` | Per-field ES roles (`SURFACE`, `NARRATIVE_STRUCTURED`, etc.) | Dev 5 | Hard-coded in `field_manifest_registry.py` |

This creates three failure modes:

- **Drift.** A new CDM field is approved by M4 and enters `cdm_proposals`, but neither `cdm_entity_storage_config` nor `FieldManifest` is updated. The field is silently skipped by the m3-writer.
- **Inconsistency.** A PII flag is toggled on a field. The ground truth is updated; `FieldManifest` is not. The field continues to be embedded.
- **Bootstrapping cost.** Every new tenant requires Dev 5 to manually seed `cdm_entity_storage_config` with correct flags for each entity type. There is no derivation path.

The root cause is that the CDM mapper output (approved `cdm_proposals`) and the ground truth file share the same format, but neither carries **where a field routes** as a first-class property. Routing knowledge is externalised into a config table that nobody keeps in sync with the CDM catalogue.

### Resolution

Add `db_target`, `es_role`, `ts_role`, and `neo4j_role` as first-class properties of the CDM field definition. Introduce a new table `nexus_system.cdm_field_routing` as the canonical store for these properties. Make `cdm_entity_storage_config` and `FieldManifest` **derived artefacts** refreshed automatically on every CDM version publish.

---

## Scope of Changes

| Module | Change type | Summary |
|---|---|---|
| `nexus_system.cdm_proposals` | DDL addition | Four new advisory columns: `db_target_suggestion`, `es_role_suggestion`, `ts_role_suggestion`, `neo4j_role_suggestion` |
| `nexus_system.cdm_field_routing` | New table | Canonical per-field routing at CDM version level; one row per `(cdm_version, entity, cdm_field)` |
| `nexus_system.cdm_entity_storage_config` | DDL addition + data flow | Add `derived_from_cdm_version`; rows are now auto-derived from `cdm_field_routing` on CDM publish, not hand-seeded |
| `nexus-cdm-mapper` | Service change | `classify_field()` auto-derives routing suggestion; writes it to `cdm_proposals` |
| `nexus-m4-api` | Service change (minor) | Validation UI exposes routing suggestion; steward can override before approving |
| `nexus_core` — `FieldManifest` | Library refactor | `load_from_catalogue()` replaces hard-coded `register()` |
| `nexus-m3-writer` | Config change | Routing table refresh replaces Dev 5 hand-seed |
| Airflow | New DAG task | `refresh_routing_tables` added to `nexus_cdm_version_published` DAG |

---

## 1. Data Model

### 1.1 Migration V2.0.20 — add routing suggestion columns to `cdm_proposals`

```sql
-- V2.0.20
ALTER TABLE nexus_system.cdm_proposals
    ADD COLUMN IF NOT EXISTS db_target_suggestion   TEXT[]      NOT NULL DEFAULT '{}',
    -- e.g. ARRAY['es','neo4j'] — auto-derived by classify_field(); reviewable in M4

    ADD COLUMN IF NOT EXISTS es_role_suggestion     VARCHAR(30),
    -- 'surface' | 'narrative_structured' | 'narrative_freetext' | NULL

    ADD COLUMN IF NOT EXISTS ts_role_suggestion     VARCHAR(30),
    -- 'entity_id' | 'time_dimension' | 'metric_value' | 'dimension' | 'event_dimension' | NULL

    ADD COLUMN IF NOT EXISTS neo4j_role_suggestion  VARCHAR(30),
    -- 'edge_key' | 'edge_property' | 'node_key' | NULL

    ADD COLUMN IF NOT EXISTS routing_overridden     BOOLEAN     NOT NULL DEFAULT FALSE,
    -- TRUE when a data steward has manually changed the auto-derived suggestion in M4

    ADD COLUMN IF NOT EXISTS routing_override_by    VARCHAR(200),
    -- user_id of the steward who overrode (NULL if auto-derived, unchanged)

    ADD COLUMN IF NOT EXISTS routing_override_at    TIMESTAMPTZ;
    -- timestamp of override
```

**Backward compatibility.** All new columns have defaults. Existing `cdm_proposals` rows get `db_target_suggestion = '{}'` (empty array), which the routing refresh treats as "not yet classified" — those rows are excluded from `cdm_field_routing` until the mapper re-runs them.

---

### 1.2 Migration V2.0.21 — new `cdm_field_routing` table

This is the **canonical source of truth** for per-field routing. It operates at the CDM field level (not the source field level). Multiple source fields may map to the same CDM field; the routing is a CDM-level decision.

```sql
-- V2.0.21
CREATE TABLE IF NOT EXISTS nexus_system.cdm_field_routing (
    cdm_version         VARCHAR(20)     NOT NULL,
    entity              VARCHAR(128)    NOT NULL,
    cdm_field           VARCHAR(200)    NOT NULL,

    -- Routing destination(s)
    db_target           TEXT[]          NOT NULL,
    -- One or more of: 'es' | 'neo4j' | 'ts' | 'ref' | 'excluded'
    -- 'ref'      = stays in operational DB; fetched live by query engine
    -- 'excluded' = PII / sensitive / binary; never written to any AI store

    -- Per-store roles (NULL when the store is not in db_target)
    es_role             VARCHAR(30),
    -- 'surface' | 'narrative_structured' | 'narrative_freetext'

    ts_role             VARCHAR(30),
    -- 'entity_id' | 'time_dimension' | 'metric_value' | 'dimension' | 'event_dimension'

    neo4j_role          VARCHAR(30),
    -- 'edge_key' | 'edge_property' | 'node_key'

    -- Provenance
    pii                 BOOLEAN         NOT NULL DEFAULT FALSE,
    semantic_class      VARCHAR(50),
    derived_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    overridden_by       VARCHAR(200),
    -- NULL = auto-derived from classify_field(); user_id = data steward override via M4

    target_note         TEXT,
    -- Human-readable rationale (mirrors the ground truth file's target_note field)

    PRIMARY KEY (cdm_version, entity, cdm_field)
);

-- Index: query engine and FieldManifest both look up by entity type + version
CREATE INDEX cfr_entity_version_idx
    ON nexus_system.cdm_field_routing (cdm_version, entity);

-- Index: find all fields for a given entity that route to a specific store
CREATE INDEX cfr_db_target_gin_idx
    ON nexus_system.cdm_field_routing USING GIN (db_target);

-- No RLS — this is a platform-level catalogue table; access controlled at API layer
-- Retention: all versions retained (routing history is audit-critical)
```

**Uniqueness guarantee.** The primary key `(cdm_version, entity, cdm_field)` enforces exactly one routing decision per field per CDM version. When the CDM version increments, all rows are re-derived (or copied forward and re-evaluated for changed fields only).

---

### 1.3 Migration V2.0.22 — extend `cdm_entity_storage_config`

```sql
-- V2.0.22
ALTER TABLE nexus_system.cdm_entity_storage_config
    ADD COLUMN IF NOT EXISTS derived_from_cdm_version   VARCHAR(20),
    -- The CDM version from which this row was last derived.
    -- NULL = hand-seeded (pre-routing rows — before this spec was applied). Non-null = auto-derived.

    ADD COLUMN IF NOT EXISTS auto_derived               BOOLEAN NOT NULL DEFAULT FALSE,
    -- FALSE for pre-routing rows; TRUE for rows generated by the refresh DAG.

    ADD COLUMN IF NOT EXISTS derived_at                 TIMESTAMPTZ;
    -- Timestamp of last derivation run.
```

**Backward compatibility.** All three columns are nullable / have defaults. Existing pre-routing rows remain valid until the first CDM publish event triggers a refresh and overwrites them (with `auto_derived = TRUE`).

---

## 2. CDM Mapper — `nexus-cdm-mapper`

### 2.1 What changes

`classify_field()` already infers `semantic_class`, `pii`, and `table_role` for each proposal. It now also auto-derives routing suggestions using the same deterministic rules applied to the ground truth file (§2.2 below), and writes those suggestions into the four new `cdm_proposals` columns.

This is a **non-breaking addition**. The classification core is unchanged. Routing suggestion is an extra output alongside the existing confidence score and CDM path.

### 2.2 Routing derivation rules (embedded in `classify_field()`)

The rules below are the same logic used to classify the ground truth file, now formalised as in-service code. They live in a new module `services/nexus-cdm-mapper/nexus_cdm_mapper/routing.py`.

```python
# nexus_cdm_mapper/routing.py

from dataclasses import dataclass
from typing import Literal

DbTarget  = Literal['es', 'neo4j', 'ts', 'ref', 'excluded']
EsRole    = Literal['surface', 'narrative_structured', 'narrative_freetext']
TsRole    = Literal['entity_id', 'time_dimension', 'metric_value', 'dimension', 'event_dimension']
Neo4jRole = Literal['edge_key', 'edge_property', 'node_key']

# Entity-level store assignment (constant per CDM version; updated when new entities added)
ENTITY_STORES: dict[str, list[DbTarget]] = {
    'party':               ['es', 'neo4j'],
    'party_email':         ['neo4j'],
    'employee':            ['es', 'neo4j', 'ts'],
    'org_unit':            ['es', 'neo4j'],
    'address':             ['es', 'neo4j'],
    'country_region':      ['ref'],
    'currency':            ['ref'],
    'currency_rate':       ['ts'],
    'document':            ['es', 'neo4j'],
    'engagement':          ['es', 'neo4j'],
    'interaction':         ['es', 'neo4j', 'ts'],
    'job_candidate':       ['neo4j'],
    'location':            ['es', 'neo4j'],
    'product':             ['es', 'neo4j', 'ts'],
    'product_bom':         ['neo4j'],
    'product_photo':       ['neo4j'],
    'product_vendor':      ['neo4j', 'ts'],
    'promotion':           ['es', 'neo4j'],
    'sales_reason':        ['ref'],
    'scrap_reason':        ['ref'],
    'shift':               ['ref'],
    'ship_method':         ['ref'],
    'state_province':      ['ref'],
    'transaction':         ['es', 'neo4j', 'ts'],
    'unit_measure':        ['ref'],
    'work_order':          ['neo4j', 'ts'],
    'payment_method':      ['neo4j'],
    'position_assignment': ['es', 'neo4j'],
}

@dataclass
class RoutingSuggestion:
    db_target:   list[DbTarget]
    es_role:     EsRole | None
    ts_role:     TsRole | None
    neo4j_role:  Neo4jRole | None
    target_note: str

def derive_routing(
    entity:       str,
    cdm_field:    str,
    semantic_class: str,
    table_role:   str,
    pii:          bool,
    primary_key:  bool,
) -> RoutingSuggestion:
    """
    Deterministic routing derivation.  Priority order matches the classification
    rules applied to nexus_cdm_ground_truth_v3.json.
    Callers: classify_field() in nexus-cdm-mapper.
    """
    stores   = ENTITY_STORES.get(entity, ['ref'])
    in_es    = 'es'    in stores
    in_neo4j = 'neo4j' in stores
    in_ts    = 'ts'    in stores
    ts_only  = stores  == ['ts']
    ref_only = stores  == ['ref']

    def out(target, note, *, es_role=None, ts_role=None, neo4j_role=None):
        return RoutingSuggestion(target, es_role, ts_role, neo4j_role, note)

    # R0 — PII / sensitive
    if pii or semantic_class in ('sensitive', 'payment_attribute'):
        return out(['excluded'], 'PII/sensitive — fetched live only')

    # R1 — reference-only entity
    if ref_only:
        return out(['ref'], 'Lookup entity — stays in PostgreSQL')

    # R2 — TS-only entity (e.g. currency_rate): route by semantic class
    if ts_only:
        role = ('entity_id'      if (semantic_class == 'primary_identity' or primary_key) else
                'time_dimension' if semantic_class in ('temporal', 'time') else
                'metric_value'   if semantic_class == 'metric' else
                'dimension')
        return out(['ts'], f'TS-only entity — {role}', ts_role=role)

    # R3 — explicit time_series table_role
    if table_role == 'time_series' and in_ts:
        role = ('metric_value'   if semantic_class == 'metric' else
                'time_dimension' if semantic_class in ('time', 'temporal') else
                'entity_id'      if semantic_class == 'primary_identity' else
                'dimension')
        return out(['ts'], f'Time-series {role}', ts_role=role)

    # R4 — transaction table_role
    if table_role == 'transaction' and in_ts:
        role = ('metric_value'   if semantic_class == 'metric' else
                'time_dimension' if semantic_class in ('time', 'temporal') else
                'entity_id'      if semantic_class == 'primary_identity' else
                'event_dimension')
        return out(['ts'], f'Transaction event {role}', ts_role=role)

    # R5 — primary identity → all entity stores
    if semantic_class == 'primary_identity' or primary_key:
        target = ([s for s in ['es', 'neo4j', 'ts'] if s in stores]
                  or ['ref'])
        return out(target, 'Entity identifier — present in all stores this entity lives in',
                   es_role='surface' if 'es' in target else None,
                   ts_role='entity_id' if 'ts' in target else None,
                   neo4j_role='node_key' if 'neo4j' in target else None)

    # R6 — free text
    if semantic_class in ('text', 'text_blob'):
        if in_es:
            return out(['es'], 'Human-authored text — narrative embedding', es_role='narrative_freetext')
        return out(['ref'], 'Free text — no embedding for this entity')

    # R7 — foreign reference → Neo4j edge
    if semantic_class == 'foreign_reference':
        if in_neo4j:
            return out(['neo4j'], 'FK — Neo4j edge key', neo4j_role='edge_key')
        return out(['ref'], 'FK — stays in operational DB')

    # R8 — descriptor / attribute → ES
    if semantic_class in ('descriptor', 'attribute') and in_es:
        role = 'narrative_structured' if cdm_field in _NARRATIVE_FIELDS else 'surface'
        return out(['es'], f'Descriptive attribute — ES {role}', es_role=role)

    # R9 — classifier / category → ES surface
    if semantic_class in ('classifier', 'category') and in_es:
        role = 'narrative_structured' if cdm_field in _NARRATIVE_FIELDS else 'surface'
        return out(['es'], 'Classifier — ES surface pre-filter keyword', es_role=role)

    # R10 — metric (point-in-time) → ref
    if semantic_class == 'metric':
        if in_ts and entity in ('product_vendor', 'work_order'):
            return out(['ts'], 'Operational metric on TS entity', ts_role='metric_value')
        return out(['ref'], 'Point-in-time metric — fetched live')

    # R11 — temporal fields
    if semantic_class in ('temporal', 'time'):
        if in_ts:
            return out(['ts'], 'Temporal anchor — TimescaleDB time dimension', ts_role='time_dimension')
        if in_neo4j:
            return out(['neo4j'], 'Temporal edge property (since/until)', neo4j_role='edge_property')
        if in_es:
            return out(['es'], 'Date field — ES surface range filter', es_role='surface')
        return out(['ref'], 'Temporal — stays in operational DB')

    # R12 — alternate identity → ES surface
    if semantic_class == 'alternate_identity' and in_es:
        return out(['es'], 'Alternate identifier — ES surface exact-match', es_role='surface')

    # R13 — reference FK
    if semantic_class == 'reference' and in_neo4j:
        return out(['neo4j'], 'Reference FK — Neo4j edge', neo4j_role='edge_key')

    # Default
    if in_es:
        return out(['es'], f'Default ES surface (sc={semantic_class})', es_role='surface')
    if in_neo4j:
        return out(['neo4j'], f'Default Neo4j (sc={semantic_class})')
    return out(['ref'], f'Unclassified — stays in operational DB')


# Fields that receive narrative_structured role (woven into the NL embedding sentence)
_NARRATIVE_FIELDS = frozenset({
    'party_name','party_subtype','party_status','credit_rating','is_active',
    'full_name','first_name','last_name','job_title','is_salaried','org_hierarchy_path',
    'product_name','product_sku','product_class','product_line','product_style',
    'colour','is_discontinued','is_finished_good','is_manufactured',
    'product_category','product_subcategory_name','product_model_name',
    'document_title','filename','file_extension','document_status','is_folder',
    'orgunit_name','orgunit_group','sales_group',
    'city','country_code','postal_code','region',
    'transaction_status','transaction_subtype','is_online_order',
    'pipeline_status','engagement_source','interaction_type',
    'location_name','promotion_type','promotion_category','description',
    'role_type','assignment_region',
})
```

### 2.3 Changes to `classify_field()` call site

```python
# services/nexus-cdm-mapper/nexus_cdm_mapper/classifier.py  (existing file, delta only)

from nexus_cdm_mapper.routing import derive_routing

async def classify_field(
    proposal: CdmProposal,
    ...
) -> CdmProposal:
    # ... existing classification logic unchanged ...

    # ── NEW: derive routing suggestion ──────────────────────────────────────
    routing = derive_routing(
        entity        = proposal.target_entity,
        cdm_field     = proposal.target_attribute,
        semantic_class= proposal.inferred_semantic_class,  # existing field
        table_role    = proposal.inferred_table_role,       # existing field
        pii           = proposal.pii,                       # existing field
        primary_key   = proposal.is_primary_key,            # existing field
    )

    proposal.db_target_suggestion  = routing.db_target
    proposal.es_role_suggestion    = routing.es_role
    proposal.ts_role_suggestion    = routing.ts_role
    proposal.neo4j_role_suggestion = routing.neo4j_role
    # routing_overridden stays FALSE — this is auto-derived
    # ────────────────────────────────────────────────────────────────────────

    return proposal
```

**No change to the natural-key upsert logic.** Routing suggestion columns update in-place on re-classification.

---

## 3. M4 Validation — `nexus-m4-api`

### 3.1 What changes

The validation workbench surface gains a read-only routing panel alongside the existing confidence score and CDM path review. Data stewards can accept the auto-derived routing or override individual fields before approving.

### 3.2 New API endpoint

```
GET /api/v1/cdm/proposals/{proposal_id}/routing
```

Returns:
```json
{
  "proposal_id": "uuid",
  "cdm_path":    "party.party_name",
  "db_target_suggestion":  ["es"],
  "es_role_suggestion":    "narrative_structured",
  "ts_role_suggestion":    null,
  "neo4j_role_suggestion": null,
  "routing_overridden":    false,
  "target_note": "Descriptive attribute — ES narrative_structured"
}
```

```
PATCH /api/v1/cdm/proposals/{proposal_id}/routing
```

Request body:
```json
{
  "db_target_suggestion":  ["es", "neo4j"],
  "es_role_suggestion":    "surface",
  "ts_role_suggestion":    null,
  "neo4j_role_suggestion": null
}
```

Sets `routing_overridden = TRUE`, records `routing_override_by` and `routing_override_at`. The override persists through subsequent re-classifications — `classify_field()` checks `routing_overridden` and skips the derivation step if the flag is set.

### 3.3 Approval flow unchanged

Approving a proposal via the existing `POST /api/v1/cdm/proposals/{id}/approve` endpoint is unchanged. The routing columns are bundled with the approved mapping and are present in `cdm_proposals` for the routing refresh step to consume.

---

## 4. Routing Refresh — Airflow DAG

### 4.1 New task in `nexus_cdm_version_published` DAG

The existing `nexus_cdm_version_published` DAG already triggers Elasticsearch re-embedding and Neo4j org-chart rebuild on CDM version publish. A new task `refresh_routing_tables` is added as the **first** task in the DAG — it must complete before any downstream store writes begin.

```python
# dags/nexus_cdm_version_published.py  (existing DAG, delta only)

@dag(schedule=None, ...)  # triggered by nexus.cdm.version_published Kafka event
def nexus_cdm_version_published():

    @task
    def refresh_routing_tables(cdm_version: str, tenant_id: str):
        """
        1. Consolidate approved cdm_proposals routing columns into cdm_field_routing.
        2. Derive cdm_entity_storage_config rows from cdm_field_routing.
        Both operations are idempotent (upsert on primary key).
        """
        with get_system_connection() as conn:

            # ── Step 1: upsert cdm_field_routing from approved proposals ───
            conn.execute("""
                INSERT INTO nexus_system.cdm_field_routing
                    (cdm_version, entity, cdm_field,
                     db_target, es_role, ts_role, neo4j_role,
                     pii, semantic_class, overridden_by, target_note, derived_at)
                SELECT
                    :cdm_version,
                    target_entity,
                    target_attribute,
                    -- If overridden by steward use their value; else use suggestion
                    CASE WHEN routing_overridden THEN db_target_suggestion
                         ELSE db_target_suggestion END,
                    CASE WHEN routing_overridden THEN es_role_suggestion
                         ELSE es_role_suggestion END,
                    CASE WHEN routing_overridden THEN ts_role_suggestion
                         ELSE ts_role_suggestion END,
                    CASE WHEN routing_overridden THEN neo4j_role_suggestion
                         ELSE neo4j_role_suggestion END,
                    pii,
                    inferred_semantic_class,
                    CASE WHEN routing_overridden THEN routing_override_by ELSE NULL END,
                    NULL,   -- target_note: not stored in proposals, omit for now
                    NOW()
                FROM nexus_system.cdm_proposals
                WHERE tenant_id  = :tenant_id
                  AND cdm_version = :cdm_version
                  AND status      = 'confirmed'
                  AND array_length(db_target_suggestion, 1) > 0
                ON CONFLICT (cdm_version, entity, cdm_field)
                DO UPDATE SET
                    db_target    = EXCLUDED.db_target,
                    es_role      = EXCLUDED.es_role,
                    ts_role      = EXCLUDED.ts_role,
                    neo4j_role   = EXCLUDED.neo4j_role,
                    overridden_by= EXCLUDED.overridden_by,
                    derived_at   = NOW()
            """, cdm_version=cdm_version, tenant_id=tenant_id)

            # ── Step 2: derive cdm_entity_storage_config from cdm_field_routing ─
            conn.execute("""
                INSERT INTO nexus_system.cdm_entity_storage_config
                    (tenant_id, cdm_entity_type,
                     embeddable, graph_persistent, metricable,
                     metric_value_attr, metric_time_attr,
                     embed_attrs, pii_excluded_attrs,
                     derived_from_cdm_version, auto_derived, derived_at, updated_at)
                SELECT
                    :tenant_id,
                    entity,
                    bool_or('es'    = ANY(db_target))   AS embeddable,
                    bool_or('neo4j' = ANY(db_target))   AS graph_persistent,
                    bool_or('ts'    = ANY(db_target))   AS metricable,

                    -- First metric_value field for each entity (for TimescaleDB writer)
                    min(cdm_field) FILTER (
                        WHERE ts_role = 'metric_value')             AS metric_value_attr,

                    -- First time_dimension field for each entity
                    min(cdm_field) FILTER (
                        WHERE ts_role = 'time_dimension')           AS metric_time_attr,

                    -- All fields that reach Elasticsearch (surface or narrative)
                    array_agg(DISTINCT cdm_field) FILTER (
                        WHERE es_role IN (
                            'surface','narrative_structured','narrative_freetext')
                        )                                           AS embed_attrs,

                    -- All PII-excluded fields
                    array_agg(DISTINCT cdm_field) FILTER (
                        WHERE 'excluded' = ANY(db_target))          AS pii_excluded_attrs,

                    :cdm_version,
                    TRUE,
                    NOW(),
                    NOW()
                FROM nexus_system.cdm_field_routing
                WHERE cdm_version = :cdm_version
                GROUP BY entity

                ON CONFLICT (tenant_id, cdm_entity_type)
                DO UPDATE SET
                    embeddable               = EXCLUDED.embeddable,
                    graph_persistent         = EXCLUDED.graph_persistent,
                    metricable               = EXCLUDED.metricable,
                    metric_value_attr        = EXCLUDED.metric_value_attr,
                    metric_time_attr         = EXCLUDED.metric_time_attr,
                    embed_attrs              = EXCLUDED.embed_attrs,
                    pii_excluded_attrs       = EXCLUDED.pii_excluded_attrs,
                    derived_from_cdm_version = EXCLUDED.derived_from_cdm_version,
                    auto_derived             = TRUE,
                    derived_at               = NOW(),
                    updated_at               = NOW()
            """, tenant_id=tenant_id, cdm_version=cdm_version)

            logger.info(
                f"refresh_routing_tables: tenant={tenant_id} cdm_version={cdm_version} — "
                f"cdm_field_routing and cdm_entity_storage_config refreshed"
            )

    # Task ordering: routing refresh must complete before any store re-index
    routing = refresh_routing_tables(cdm_version=..., tenant_id=...)
    es_reindex >> routing          # routing refresh before re-embed
    neo4j_rebuild >> routing       # routing refresh before org-chart rebuild
    routing >> store_writes_begin  # no store writes until routing is current
```

**Idempotency.** Both upserts use `ON CONFLICT DO UPDATE`. Re-running for the same `(tenant_id, cdm_version)` produces no net change if nothing was modified.

---

## 5. `nexus_core` — `FieldManifest` Refactor

### 5.1 What changes

`FieldManifest` currently hard-codes field→role mappings in `nexus_m3_writer/indexing/field_manifest_registry.py`. This is replaced by a catalogue read from `cdm_field_routing`.

The hard-coded registry is retained as a **fallback** for the first boot before the routing refresh DAG has run. After the first CDM publish event, the catalogue-driven path takes over permanently.

### 5.2 New `nexus_core` API

```python
# libs/nexus_core/nexus_core/cdm/field_manifest.py  (existing file, extend)

class FieldManifest:

    @classmethod
    async def load_from_catalogue(
        cls,
        cdm_version: str,
        tenant_id: str | None = None,
        conn: AsyncConnection | None = None,
    ) -> None:
        """
        Loads the field manifest for a given CDM version from
        nexus_system.cdm_field_routing into the in-memory registry.

        Called once at service startup and on receipt of nexus.cdm.version_published.
        Falls back to the hard-coded registry if the table is empty for that version.
        """
        rows = await conn.fetch("""
            SELECT entity, cdm_field, db_target, es_role, ts_role, neo4j_role, pii
            FROM nexus_system.cdm_field_routing
            WHERE cdm_version = $1
        """, cdm_version)

        if not rows:
            logger.warning(
                f"FieldManifest.load_from_catalogue: no rows for cdm_version={cdm_version}. "
                "Falling back to hard-coded registry."
            )
            return  # hard-coded registry remains active

        new_registry: dict[str, list[CdmFieldSpec]] = {}
        for row in rows:
            entity = row['entity']
            spec = CdmFieldSpec(
                field_name  = row['cdm_field'],
                roles       = _es_role_to_field_roles(row['es_role'], row['db_target']),
                es_type     = None,     # type inference unchanged — not stored in routing
                pii         = row['pii'],
            )
            new_registry.setdefault(entity, []).append(spec)

        cls._registry = new_registry
        logger.info(
            f"FieldManifest loaded from catalogue: "
            f"cdm_version={cdm_version}, {len(rows)} field specs across "
            f"{len(new_registry)} entity types"
        )


def _es_role_to_field_roles(es_role: str | None, db_target: list[str]) -> list[FieldRole]:
    """Converts the cdm_field_routing es_role back to the FieldRole vocabulary."""
    if 'excluded' in db_target:
        return ['EXCLUDED']
    if es_role == 'narrative_freetext':
        return ['NARRATIVE_FREETEXT']
    if es_role == 'narrative_structured':
        return ['NARRATIVE_STRUCTURED', 'SURFACE']   # both: woven into narrative AND stored
    if es_role == 'surface':
        return ['SURFACE']
    return ['EXCLUDED']   # not in ES — exclude from manifest
```

### 5.3 Call site in `nexus-m3-writer`

```python
# services/nexus-m3-writer/nexus_m3_writer/main.py  (existing file, delta only)

@app.on_event("startup")
async def startup():
    cdm_version = await get_current_cdm_version()
    await FieldManifest.load_from_catalogue(cdm_version, conn=db_pool)
    # Falls back to hard-coded registry if table is empty — safe on first boot

# On CDM version publish event:
async def handle_cdm_version_published(event: CdmVersionPublishedEvent):
    await FieldManifest.load_from_catalogue(event.cdm_version, conn=db_pool)
    logger.info(f"FieldManifest reloaded for cdm_version={event.cdm_version}")
```

Hard-coded `field_manifest_registry.py` is retained unchanged as the cold-start fallback. It can be removed in Iteration 4 once the first full routing refresh has run in production.

---

## 6. `nexus-m3-writer` — Routing Table Consumption

### 6.1 What does NOT change

The `handle_entity_routed()` function and the three store handlers (ES, Neo4j, TimescaleDB) are unchanged. They continue to read `cdm_entity_storage_config` to decide which stores to invoke:

```python
config = await storage_config_lookup(event.tenant_id, event.cdm_entity_type)
elasticsearch_writer.write(event) if config.embeddable       else skip()
neo4j_writer.write(event)         if config.graph_persistent else skip()
timescale_writer.write(event)     if config.metricable       else skip()
```

### 6.2 What changes

`cdm_entity_storage_config` rows are no longer hand-seeded by Dev 5. They are populated by the Airflow refresh task (§4). The only action required from Dev 5 is to **remove the manual seed step** from `nexus_core.provisioning.onboard_tenant()` and replace it with a call to trigger the routing refresh DAG for the tenant's current CDM version.

```python
# libs/nexus_core/nexus_core/provisioning.py  (existing file, delta only)

async def onboard_tenant(tenant_id: str, cdm_version: str):
    # ... existing provisioning steps unchanged ...

    # REMOVE: manual cdm_entity_storage_config seed (replaced by routing refresh DAG)
    # ADD: trigger routing refresh DAG for this tenant + version
    await airflow_client.trigger_dag(
        dag_id    = 'nexus_cdm_version_published',
        conf      = {'tenant_id': tenant_id, 'cdm_version': cdm_version},
    )
    logger.info(
        f"onboard_tenant: routing refresh DAG triggered for "
        f"tenant={tenant_id} cdm_version={cdm_version}"
    )
```

The ES index provisioning step in `onboard_tenant()` (creating `nexus_{tenant_slug}_{entity_type}` indices) is unchanged — it fires before the routing refresh, which is correct: the index must exist before the refresh tries to derive `embeddable=TRUE` for any entity type.

---

## 7. Bootstrap Migration from Ground Truth v3

The classified ground truth file (`nexus_cdm_ground_truth_v3_classified.json`) contains the `db_target`, `es_role`, `ts_role`, and `neo4j_role` values for all 467 field pairs at CDM version `v3`. Rather than wait for the mapper to re-classify every field, a one-time bootstrap script seeds `cdm_field_routing` and `cdm_entity_storage_config` directly from that file.

```python
# scripts/bootstrap_routing_from_ground_truth.py  (one-time migration script)

import json
import asyncpg

async def bootstrap(tenant_id: str, cdm_version: str = 'v3'):
    with open('nexus_cdm_ground_truth_v3_classified.json') as f:
        data = json.load(f)

    conn = await asyncpg.connect(...)

    # Seed cdm_field_routing
    for pair in data['pairs']:
        if not pair.get('db_target'):
            continue
        await conn.execute("""
            INSERT INTO nexus_system.cdm_field_routing
                (cdm_version, entity, cdm_field,
                 db_target, es_role, ts_role, neo4j_role,
                 pii, semantic_class, overridden_by, target_note, derived_at)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,NOW())
            ON CONFLICT (cdm_version, entity, cdm_field) DO UPDATE SET
                db_target   = EXCLUDED.db_target,
                es_role     = EXCLUDED.es_role,
                ts_role     = EXCLUDED.ts_role,
                neo4j_role  = EXCLUDED.neo4j_role,
                derived_at  = NOW()
        """,
            cdm_version,
            pair['entity'],
            pair['cdm_field'],
            pair['db_target'],
            pair.get('es_role'),
            pair.get('ts_role'),
            pair.get('neo4j_role'),
            pair.get('pii', False),
            pair.get('semantic_class'),
            None,           # overridden_by — NULL, this is from the ground truth review
            pair.get('target_note'),
        )

    # Then derive cdm_entity_storage_config from the seeded rows
    # (same SQL as the Airflow task — see §4)
    await _refresh_entity_storage_config(conn, tenant_id, cdm_version)
    await conn.close()
    print(f"Bootstrap complete: {len(data['pairs'])} field routing rows seeded")
```

This script runs once as part of the Iteration 2 migration (`V2.0.23__bootstrap_routing.sql` / Python equivalent). After it runs, the Airflow routing refresh DAG takes over for all subsequent CDM version publishes.

---

## 8. Migration Ledger

```
V2.0.20__add_routing_columns_to_cdm_proposals.sql
V2.0.21__create_cdm_field_routing.sql
V2.0.22__extend_cdm_entity_storage_config.sql
V2.0.23__bootstrap_routing_from_ground_truth_v3.py   (Python migration script)
```

All four must be applied in order before the Iteration 2 CDM mapper is deployed.

---

## 9. Functional Requirements

| ID | Requirement | Priority |
|---|---|---|
| FR-CFR-01 | Every `cdm_proposals` row produced by `nexus-cdm-mapper` after V2.0.20 must carry non-empty `db_target_suggestion`. | Must |
| FR-CFR-02 | `routing_overridden = FALSE` rows must be re-derived on every re-classification; `routing_overridden = TRUE` rows must NOT be overwritten by `classify_field()`. | Must |
| FR-CFR-03 | `refresh_routing_tables` Airflow task must complete before any `nexus-m3-writer` store write begins after a CDM version publish. | Must |
| FR-CFR-04 | `cdm_entity_storage_config` rows produced by `refresh_routing_tables` must match the `db_target` aggregation of `cdm_field_routing` for the same `(tenant_id, cdm_version)`. | Must |
| FR-CFR-05 | `FieldManifest.load_from_catalogue()` must fall back gracefully to the hard-coded registry if `cdm_field_routing` is empty for the requested version. No startup failure. | Must |
| FR-CFR-06 | Bootstrap script must be idempotent: re-running it for the same `(tenant_id, cdm_version)` produces no net change to `cdm_field_routing` or `cdm_entity_storage_config`. | Must |
| FR-CFR-07 | A data steward override (`routing_overridden = TRUE`) must survive: re-classification, CDM patch version increments, and bootstrap re-runs. It must only be cleared by an explicit steward action. | Must |
| FR-CFR-08 | `nexus_core.provisioning.onboard_tenant()` must no longer hand-seed `cdm_entity_storage_config`. It must trigger the routing refresh DAG instead. | Must |

---

## 10. Non-Functional Requirements

| ID | Requirement | Target |
|---|---|---|
| NFR-CFR-01 | `refresh_routing_tables` task runtime | ≤ 30 s for 5,000 approved `cdm_proposals` rows across all entity types |
| NFR-CFR-02 | `FieldManifest.load_from_catalogue()` call latency | ≤ 200 ms (single DB round-trip on primary key range scan) |
| NFR-CFR-03 | `cdm_field_routing` query for a single entity type | ≤ 5 ms p95 (covered by `cfr_entity_version_idx`) |
| NFR-CFR-04 | Bootstrap script runtime for 467 pairs | ≤ 10 s |

---

## 11. Open Questions

| # | Question | Impact |
|---|---|---|
| OQ-CFR-01 | Should `cdm_field_routing` be tenant-scoped (one row per `tenant_id + cdm_version + entity + cdm_field`) or global (one row per `cdm_version + entity + cdm_field`, shared across tenants)? Global is cheaper; per-tenant allows tenant-specific overrides. Recommend global for Iteration 2, per-tenant in a future iteration. | Schema PK change if per-tenant is chosen |
| OQ-CFR-02 | The `metric_value_attr` column on `cdm_entity_storage_config` is currently `min(cdm_field)` — i.e. the lexicographically first metric field. For entities with multiple metrics (e.g. `transaction` has `subtotal_amount`, `total_amount`, `tax_amount`), this is arbitrary. Should `cdm_field_routing` carry a `metric_priority` integer to express which field is the primary metric value? | TimescaleDB writer metric selection |
| OQ-CFR-03 | `derive_routing()` references `ENTITY_STORES` — a hard-coded dict in `routing.py`. When a new entity type is added to the CDM, this dict must be updated manually. Should `ENTITY_STORES` itself be derived from `cdm_field_routing` (bootstrap round-trip) or stay explicit? Recommend explicit for now: entity-level store assignments are architectural decisions, not classifier outputs. | CDM extensibility |
| OQ-CFR-04 | `FieldManifest` currently does not store ES type (`keyword`, `text`, `date`) — that is determined by the hard-coded registry. The catalogue-driven path omits it. Should `cdm_field_routing` carry an `es_type` column, or should the ES handler infer it from `semantic_class`? | ES index mapping accuracy |

---

*NEXUS Iteration 2 · CDM Field Routing Gap · v0.1 · Mentis Consulting · April 2026 · Confidential*
