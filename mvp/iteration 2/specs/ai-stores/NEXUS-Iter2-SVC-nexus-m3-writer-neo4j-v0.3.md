# NEXUS — Iteration 2 · `nexus-m3-writer` · Neo4j Graph Store Handler
**Service:** `nexus-m3-writer` · **Module:** `nexus_m3_writer/stores/neo4j_writer.py`
**Developer B task · Version 0.3**
Mentis Consulting · April 2026 · Confidential

> **Revision v0.3 — Ground-truth relationship catalog + incremental connector model (2026-04-29)**
> Supersedes v0.2. Three categories of additions:
> 1. **Complete relationship catalog** — every edge type, its direction, and its edge properties, derived directly from `nexus_cdm_ground_truth_v3_classified.json`. The abstract §3 from v0.2 is replaced.
> 2. **Pending edge protocol** — formalises what happens when an edge's target node does not yet exist (connector not yet onboarded). This is the default operating mode, not an edge case.
> 3. **Three lifecycle events** — connector added one by one (§11), CDM version updated (§12), field mapping corrected (§13). Each defines the precise write path and reconciliation steps.

**Related docs:**
- `NEXUS-Iter2-SVC-nexus-m3-writer-stores-v0.1.md` — master service spec (scope, data model, Kafka)
- `NEXUS-Iter2-SPEC-M3-AIStores-v0.5.md` — architectural invariants, Virtual CDM principle
- `NEXUS-Iter2-SPEC-CDMFieldRouting-v0.1.md` — canonical entity→store routing
- `nexus_cdm_ground_truth_v3_classified.json` — authoritative source for relationship definitions in §3

---

## 1. Design Decisions

### 1.1 No materialization tiers
Unchanged from v0.2. Every `graph_persistent` entity is written unconditionally. No `materialization_level` on any node or edge. RELEVEL events are no-ops.

### 1.2 Incremental connector model
Connectors are onboarded one at a time. At any point in time, the graph is **partially populated** — nodes from connector A exist; nodes from connector B do not yet. This is the normal operating state, not an error condition.

Consequence: **most FKs on a freshly ingested entity will point to nodes that do not yet exist.** The writer must handle this gracefully every time, not just during backfill. The pending edge protocol (§7) is a permanent first-class path, not a workaround.

### 1.3 FK direction vs edge direction
Several entities hold a FK that points "upward" to an entity that logically owns them. The edge in the graph may point in the opposite direction to the FK.

**Rule:** the edge is always written by the UPSERT of the entity that **holds the FK field** — regardless of which direction the edge points in Cypher.

Example: `address` holds `party_id_ref`. The edge is `(:Party)-[:HAS_ADDRESS]->(:Address)`, pointing from Party to Address. But it is the **Address UPSERT** that writes this edge, because `party_id_ref` is on the address CDM record.

This means: for every edge in §3, the "written by" column shows which entity event triggers the write. Developers must not assume that the edge source in Cypher matches the Kafka event source.

### 1.4 Virtual CDM rule — unchanged
Nodes store `{id, tenant_id, connector_id, source_ref, golden_record, created_at, updated_at}` only. No business field values. Edge properties in §3 are the only exception — they carry structural metadata (dates, quantities, prices) needed to make the graph traversal meaningful without fetching from the live source.

---

## 2. Entity Set

Unchanged from v0.2. 14 node labels + 5 edge-only entities. See v0.2 §2 for the full table.

---

## 3. Complete Relationship Catalog

Derived directly from `nexus_cdm_ground_truth_v3_classified.json` (fields with `neo4j_role = edge_key` or `edge_property`). This is the authoritative definition for Developer B. Do not invent relationship types not listed here.

### 3.1 Notation

Each entry shows:
- **Cypher pattern** — the exact relationship type and direction
- **Written by** — which entity UPSERT event triggers this edge
- **FK field** — the CDM field on the triggering entity that provides the target node ID
- **Edge properties** — CDM fields written onto the relationship (not the node)
- **Target entity** — the node type being pointed to; `[ref]` means the target is a ref-only entity (no Neo4j node exists) — skip the edge

---

### 3.2 Organisational structure

| Cypher pattern | Written by | FK field | Edge properties |
|---|---|---|---|
| `(:Employee)-[:REPORTS_TO]->(:Employee)` | `employee` UPSERT | `reports_to_employee_id` | — |
| `(:Employee)-[:BELONGS_TO]->(:OrgUnit)` | `employee` UPSERT | `territory_orgunit_id` | — |
| `(:Employee)-[:HAS_ASSIGNMENT]->(:PositionAssignment)` | `position_assignment` UPSERT | `employee_id` | `assignment_start`, `assignment_end`, `created_at` |
| `(:PositionAssignment)-[:IN_ORG_UNIT]->(:OrgUnit)` | `position_assignment` UPSERT | `orgunit_id` | — |

> `REPORTS_TO` forms an arbitrary-depth tree. Cypher variable-length paths (`[:REPORTS_TO*]`) traverse the full hierarchy. Cycles are possible if source data is corrupt — the writer logs a warning if the employee's own ID appears in `reports_to_employee_id`.

---

### 3.3 Party and address

| Cypher pattern | Written by | FK field | Edge properties |
|---|---|---|---|
| `(:Party)-[:MANAGED_BY]->(:Employee)` | `party` UPSERT | `managed_by_employee_id` | `created_at` |
| `(:Party)-[:IN_TERRITORY]->(:OrgUnit)` | `party` UPSERT | `territory_orgunit_id_ref` | — |
| `(:Party)-[:HAS_ADDRESS]->(:Address)` | `address` UPSERT | `party_id_ref` | — |
| `(:Party)-[:HAS_EMAIL {email_address}]->(:Party)` | `party_email` UPSERT | `party_id` | `email_address` (inline property on edge) |
| `(:Party)-[:HAS_PAYMENT_METHOD]->(:PaymentMethod)` | `payment_method` UPSERT | `party_id_ref` | — |
| `(:Party)-[:WAS_CANDIDATE]->(:JobCandidate)` | `job_candidate` UPSERT | `candidate_party_id` | — |

> `HAS_EMAIL` is a self-loop on `:Party` carrying the email address as an edge property. No separate Email node exists.
> `address.state_province_id_ref` points to a `[ref]` entity — **skip this edge**, no StateProvince node exists in Neo4j.

---

### 3.4 Product graph

| Cypher pattern | Written by | FK field | Edge properties |
|---|---|---|---|
| `(:Product)-[:SUPPLIED_BY]->(:Party)` | `product` UPSERT | `supplied_by_party_id` | — |
| `(:Product)-[:SUPPLIED_BY {price, lead_time}]->(:Party)` | `product_vendor` UPSERT | `vendor_party_id_ref` | `standard_price`, `lead_time_days`, `min_order_qty` (also written to TimescaleDB) |
| `(:Product)-[:HAS_COMPONENT {qty, dates}]->(:Product)` | `product_bom` UPSERT | `parent_product_id_ref` → `component_product_id_ref` | `effective_from`, `effective_to`, `unit_code_ref` |
| `(:Product)-[:HAS_PHOTO {url}]->(:Product)` | `product_photo` UPSERT | `product_id_ref` | `photo_url` (inline property; self-loop on `:Product`) |
| `(:Promotion)-[:APPLIES_TO]->(:Product)` | `promotion` UPSERT | `eligible_product_id_ref` | `start_date`, `end_date` |
| `(:WorkOrder)-[:FOR_PRODUCT]->(:Product)` | `work_order` UPSERT | `product_id_ref` | — |
| `(:WorkOrder)-[:AT_LOCATION]->(:Location)` | `work_order` UPSERT | `location_id_ref` | — |

> `product_bom` produces a directed edge from parent to component. `parent_product_id_ref` is the start node; `component_product_id_ref` is the end node. Both must exist before the edge is created — if either is absent the edge is pending.
> `product.raw_category_id` and `product.model_id_ref` point to product taxonomy entities not yet in the CDM as standalone graph nodes — **skip these edges** for now. [CLARIFY: OQ-NEO4J-05 — should ProductCategory and ProductModel become graph nodes in a future CDM version?]
> `product.supplied_by_party_id` and `product_vendor.vendor_party_id_ref` both produce `SUPPLIED_BY` edges. They may coexist for the same product if both the product record and a separate vendor record exist. Edge identity `(product_id, party_id, :SUPPLIED_BY, source_fk)` prevents duplicates.

---

### 3.5 Transactions

| Cypher pattern | Written by | FK field | Edge properties |
|---|---|---|---|
| `(:Transaction)-[:PLACED_BY]->(:Party)` | `transaction` UPSERT | `placed_by_party_id` | — |
| `(:Transaction)-[:PLACED_WITH]->(:Party)` | `transaction` UPSERT | `placed_with_party_id` | — |
| `(:Transaction)-[:HANDLED_BY]->(:Employee)` | `transaction` UPSERT | `handled_by_employee_id` | — |
| `(:Transaction)-[:RAISED_BY_EMP]->(:Employee)` | `transaction` UPSERT | `raised_by_employee_id` | — |
| `(:Transaction)-[:BILLED_TO]->(:Address)` | `transaction` UPSERT | `bill_to_address_id` | — |
| `(:Transaction)-[:SHIPPED_TO]->(:Address)` | `transaction` UPSERT | `ship_to_address_id` | — |
| `(:Transaction)-[:IN_TERRITORY]->(:OrgUnit)` | `transaction` UPSERT | `territory_orgunit_id` | — |
| `(:Transaction)-[:PART_OF]->(:Transaction)` | `transaction` UPSERT | `parent_transaction_id` | — |
| `(:Transaction)-[:REFERENCES_ORDER]->(:Transaction)` | `transaction` UPSERT | `reference_order_id` | — |

> `transaction.sales_reason_id_ref` points to a `[ref]` entity — **skip this edge**.
> `PART_OF` creates the sales order header / line item hierarchy. Line items point to their header. A transaction without `parent_transaction_id` is a header.
> `reference_order_line_id` is a sub-key within a referenced order — it is stored as an edge property on `REFERENCES_ORDER` rather than a separate edge.

---

### 3.6 Engagement, interaction, document

| Cypher pattern | Written by | FK field | Edge properties |
|---|---|---|---|
| `(:Engagement)-[:RAISED_BY]->(:Party)` | `engagement` UPSERT | `raised_by_party_id` | `created_at` |
| `(:Engagement)-[:ASSIGNED_TO]->(:Employee)` | `engagement` UPSERT | `assigned_to_employee_id` | — |
| `(:Interaction)-[:RAISED_BY]->(:Party)` | `interaction` UPSERT | `raised_by_party_id` | — |
| `(:Interaction)-[:HANDLED_BY]->(:Employee)` | `interaction` UPSERT | `handled_by_employee_id` | — |
| `(:Interaction)-[:ABOUT_PRODUCT]->(:Product)` | `interaction` UPSERT | `product_id_ref` | — |
| `(:Document)-[:OWNED_BY]->(:Employee)` | `document` UPSERT | `owner_employee_id` | — |
| `(:Document)-[:REFERENCES_PRODUCT]->(:Product)` | `document` UPSERT | `product_id_ref` | — |

---

## 4. Schema DDL

`v2.1.0_ground_truth_alignment.cypher` — supersedes v2.0.0. Full DDL from v0.2 §4 is unchanged. Additional relationship indexes covering the new edge types:

```cypher
CREATE INDEX rel_managed_by_src      IF NOT EXISTS FOR ()-[r:MANAGED_BY]-()        ON (r.source_fk);
CREATE INDEX rel_in_territory_src    IF NOT EXISTS FOR ()-[r:IN_TERRITORY]-()       ON (r.source_fk);
CREATE INDEX rel_has_address_src     IF NOT EXISTS FOR ()-[r:HAS_ADDRESS]-()        ON (r.source_fk);
CREATE INDEX rel_has_assignment_src  IF NOT EXISTS FOR ()-[r:HAS_ASSIGNMENT]-()     ON (r.source_fk);
CREATE INDEX rel_placed_by_src       IF NOT EXISTS FOR ()-[r:PLACED_BY]-()          ON (r.source_fk);
CREATE INDEX rel_part_of_src         IF NOT EXISTS FOR ()-[r:PART_OF]-()            ON (r.source_fk);
CREATE INDEX rel_about_product_src   IF NOT EXISTS FOR ()-[r:ABOUT_PRODUCT]-()      ON (r.source_fk);
CREATE INDEX rel_applies_to_src      IF NOT EXISTS FOR ()-[r:APPLIES_TO]-()         ON (r.source_fk);
CREATE INDEX rel_has_component_src   IF NOT EXISTS FOR ()-[r:HAS_COMPONENT]-()      ON (r.source_fk);
CREATE INDEX rel_raised_by_src       IF NOT EXISTS FOR ()-[r:RAISED_BY]-()          ON (r.source_fk);
```

---

## 5. Python Interface

```python
class Neo4jWriter:
    async def write(self, entity: CdmEntity) -> None:
        """UPSERT — MERGE node + reconcile outbound edges. Pending edges recorded if target absent."""

    async def delete(self, entity: CdmEntity) -> None:
        """REMOVE — DETACH DELETE node. Any pending edges for this node are also purged."""

    async def merge_golden_record(
        self, canonical_id: str, superseded_id: str, tenant_id: str
    ) -> None:
        """Redirect edges from superseded node to canonical; delete superseded."""

    async def migrate_schema(self, cdm_version: str, tenant_id: str) -> None:
        """CDM publish — create constraints/indexes for new entity types."""

    async def replay_pending_edges(
        self, node_id: str, tenant_id: str
    ) -> int:
        """
        Called after a node is successfully MERGEd.
        Looks up all pending edges whose target_id matches this node_id,
        attempts to write each one, removes successfully written edges from the
        pending set.  Returns the count of edges replayed.
        """

    async def bulk_reconcile(
        self, entity_type: str, tenant_id: str, connector_id: str | None = None
    ) -> BulkReconcileResult:
        """
        Full edge reconciliation for an entity type — used after a connector
        is onboarded or after a CDM field correction.  Replays all UPSERT events
        for the given entity type (driven by Airflow, not real-time).
        """

    async def health_check(self) -> StoreHealth: ...
    async def backfill(self, tenant_id: str) -> BackfillResult: ...
```

---

## 6. Write Operations

### 6.1 UPSERT — node + edges

```python
async def write(self, entity: CdmEntity) -> None:
    label = ENTITY_LABEL[entity.entity_type]

    if label.startswith("__edge__"):
        return await self._write_edge_only(entity)

    # Step 1 — MERGE the node
    await self.session.run(f"""
        MERGE (n:{label} {{id: $id, tenant_id: $tid}})
        ON CREATE SET n.connector_id = $cid,
                      n.source_ref   = $ref,
                      n.golden_record = true,
                      n.created_at   = datetime()
        ON MATCH SET  n.connector_id = $cid,
                      n.updated_at   = datetime()
    """, id=entity.cdm_entity_id, tid=entity.tenant_id,
         cid=entity.connector_id,  ref=entity.source_record_id)

    # Step 2 — replay any pending edges that were waiting for this node
    replayed = await self.replay_pending_edges(entity.cdm_entity_id, entity.tenant_id)
    if replayed:
        logger.info("Replayed %d pending edges for %s/%s",
                    replayed, entity.entity_type, entity.cdm_entity_id)

    # Step 3 — reconcile outbound edges from this entity
    for rel in entity.get_relationships():
        if rel.target_id is None:
            continue   # FK field was null in source — skip
        await self._write_edge(entity, rel)
```

### 6.2 Edge write with pending-edge fallback

```python
async def _write_edge(self, entity: CdmEntity, rel: RelSpec) -> None:
    # Check if target node exists
    result = await self.session.run("""
        MATCH (b {id: $target_id, tenant_id: $tid}) RETURN b.id LIMIT 1
    """, target_id=rel.target_id, tid=entity.tenant_id)
    target_exists = await result.single() is not None

    if not target_exists:
        # Target connector not yet onboarded — park the edge
        await self._record_pending_edge(entity, rel)
        return

    # Remove stale edge: same source_fk, wrong target
    await self.session.run(f"""
        MATCH (a {{id: $src_id, tenant_id: $tid}})
              -[r:{rel.rel_type} {{source_fk: $sfk}}]->()
        WHERE NOT endNode(r).id = $target_id
        DELETE r
    """, src_id=rel.start_id, tid=entity.tenant_id,
         sfk=rel.source_fk, target_id=rel.target_id)

    # MERGE correct edge
    await self.session.run(f"""
        MATCH (a {{id: $start_id, tenant_id: $tid}})
        MATCH (b {{id: $end_id,   tenant_id: $tid}})
        MERGE (a)-[r:{rel.rel_type} {{source_fk: $sfk}}]->(b)
        ON CREATE SET r.connector_id = $cid, r.created_at = datetime(),
                      {rel.on_create_props}
        ON MATCH SET  r.updated_at   = datetime(), {rel.on_match_props}
    """, start_id=rel.start_id, end_id=rel.end_id,
         tid=entity.tenant_id, sfk=rel.source_fk,
         cid=entity.connector_id, **rel.prop_values)
```

---

## 7. Pending Edge Protocol

### 7.1 Why it exists

When a transaction UPSERT arrives and the Party node it references (`placed_by_party_id`) does not yet exist because the CRM connector has not been onboarded, the edge cannot be written. This is not an error — it is the default state when connectors are added incrementally.

The pending edge protocol ensures no edges are silently lost during incremental onboarding.

### 7.2 Storage

Pending edges are stored in a PostgreSQL table (not Redis — they must survive service restarts and are needed across DAG runs):

```sql
-- Migration V2.0.24 (new, part of this spec)
CREATE TABLE IF NOT EXISTS nexus_system.neo4j_pending_edges (
    id              BIGSERIAL       PRIMARY KEY,
    tenant_id       UUID            NOT NULL,
    -- The entity event that triggered this pending edge
    source_entity_type  VARCHAR(128)    NOT NULL,
    source_entity_id    VARCHAR(200)    NOT NULL,
    connector_id        VARCHAR(200)    NOT NULL,
    -- The edge to be written
    start_node_id   VARCHAR(200)    NOT NULL,
    end_node_id     VARCHAR(200)    NOT NULL,   -- the missing target
    rel_type        VARCHAR(100)    NOT NULL,
    source_fk       VARCHAR(200)    NOT NULL,
    edge_props      JSONB           NOT NULL DEFAULT '{}',
    -- Status
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    last_attempted  TIMESTAMPTZ,
    attempt_count   INT             NOT NULL DEFAULT 0,
    resolved        BOOLEAN         NOT NULL DEFAULT FALSE,
    resolved_at     TIMESTAMPTZ,

    -- Index: look up pending edges for a given target node (used by replay_pending_edges)
    CONSTRAINT neo4j_pending_edges_unique
        UNIQUE (tenant_id, start_node_id, end_node_id, rel_type, source_fk)
);

CREATE INDEX neo4j_pending_edges_target_idx
    ON nexus_system.neo4j_pending_edges (tenant_id, end_node_id)
    WHERE resolved = FALSE;

CREATE INDEX neo4j_pending_edges_source_entity_idx
    ON nexus_system.neo4j_pending_edges (tenant_id, source_entity_type, source_entity_id)
    WHERE resolved = FALSE;
```

### 7.3 Replay trigger

`replay_pending_edges(node_id, tenant_id)` is called automatically by `write()` immediately after a node is MERGEd (Step 2 in §6.1). It looks up all unresolved pending edges where `end_node_id = node_id`, attempts to write each one, and marks resolved ones.

```python
async def replay_pending_edges(self, node_id: str, tenant_id: str) -> int:
    rows = await postgres.fetch("""
        SELECT * FROM nexus_system.neo4j_pending_edges
        WHERE end_node_id = $1 AND tenant_id = $2 AND resolved = FALSE
    """, node_id, tenant_id)

    replayed = 0
    for row in rows:
        try:
            await self.session.run(f"""
                MATCH (a {{id: $start_id, tenant_id: $tid}})
                MATCH (b {{id: $end_id,   tenant_id: $tid}})
                MERGE (a)-[r:{row['rel_type']} {{source_fk: $sfk}}]->(b)
                ON CREATE SET r.connector_id=$cid, r.created_at=datetime(),
                              {_props_clause(row['edge_props'], 'ON CREATE')}
                ON MATCH SET  r.updated_at=datetime(),
                              {_props_clause(row['edge_props'], 'ON MATCH')}
            """, start_id=row['start_node_id'], end_id=row['end_node_id'],
                 tid=tenant_id, sfk=row['source_fk'],
                 cid=row['connector_id'], **row['edge_props'])

            await postgres.execute("""
                UPDATE nexus_system.neo4j_pending_edges
                SET resolved=TRUE, resolved_at=NOW()
                WHERE id = $1
            """, row['id'])
            replayed += 1
        except Exception as e:
            await postgres.execute("""
                UPDATE nexus_system.neo4j_pending_edges
                SET attempt_count = attempt_count + 1, last_attempted = NOW()
                WHERE id = $1
            """, row['id'])
            logger.warning("Pending edge replay failed for id=%d: %s", row['id'], e)

    return replayed
```

### 7.4 Stale pending edge cleanup

A pending edge becomes stale if its source entity is later superseded by a Golden Record merge or deleted. The `neo4j_pending_edges_source_entity_idx` index lets the writer efficiently purge pending edges for a deleted or merged entity:

```python
async def _purge_pending_edges_for(self, entity_id: str, tenant_id: str) -> None:
    await postgres.execute("""
        UPDATE nexus_system.neo4j_pending_edges
        SET resolved=TRUE, resolved_at=NOW()
        WHERE (start_node_id = $1 OR end_node_id = $1)
          AND tenant_id = $2 AND resolved = FALSE
    """, entity_id, tenant_id)
```

This is called from `delete()` and from `merge_golden_record()` on the superseded node.

---

## 8. Connector Onboarding Sequence

Each time a new connector is added, the graph transitions from a partial state to a more complete one. The following describes the exact sequence and what the writer does at each step.

### 8.1 First connector (e.g. AdventureWorks ERP)

1. `onboard_tenant()` triggers the routing refresh DAG — `cdm_entity_storage_config` is populated.
2. Backfill DAG runs for AdventureWorks:
   - **Phase 1 — nodes:** All entity records from AdventureWorks are replayed as UPSERT events. `write()` creates nodes for Employee, OrgUnit, Product, Transaction, WorkOrder, Address.
   - **Phase 2 — edges:** All FK fields are processed. Since all referenced nodes are from the same connector and exist after Phase 1, most edges write successfully. Exception: self-referential FKs like `reports_to_employee_id` — if employees arrive in the wrong order, some `REPORTS_TO` edges land in `neo4j_pending_edges` and are replayed once the manager node arrives.
   - **Phase 3 — edge-only entities:** `product_bom`, `product_vendor`, `product_photo` records are processed. Both `parent_product_id_ref` and `component_product_id_ref` should exist after Phase 1.
3. End state: graph is complete for AdventureWorks entities. `neo4j_pending_edges` should be empty (all intra-connector edges resolved).

### 8.2 Second connector added (e.g. Salesforce CRM)

1. Salesforce backfill Phase 1 creates Party, Engagement, Interaction, Document nodes.
2. During Phase 1, `write()` immediately triggers `replay_pending_edges()` for each new Party node. Any pending `MANAGED_BY`, `PLACED_BY`, or `RAISED_BY` edges that AdventureWorks transactions had been waiting on are written immediately as the Salesforce Party nodes arrive.
3. Phase 2 writes Salesforce-side edges:
   - `(:Party)-[:MANAGED_BY]->(:Employee)` — Employee exists (AdventureWorks Phase 1). Written immediately.
   - `(:Engagement)-[:ASSIGNED_TO]->(:Employee)` — Employee exists. Written immediately.
   - `(:Interaction)-[:ABOUT_PRODUCT]->(:Product)` — Product exists. Written immediately.
4. After Phase 2, `neo4j_pending_edges` count should drop significantly. Any remaining rows are edges whose target entity type is not yet connected (e.g. a `party_id_ref` pointing to a Party that exists in a third system not yet onboarded).

### 8.3 Steady-state (both connectors live)

Real-time CDC events arrive interleaved from both connectors. A new Transaction from AdventureWorks pointing to a Salesforce Party that was just created: the Party node was created seconds ago; `replay_pending_edges` may have already written the edge. The UPSERT's stale-edge check prevents duplicates.

Cross-connector edges are the primary value of the graph — they exist only because two separate sources have been linked through the CDM and Golden Record merge.

### 8.4 Pending edge monitoring

The Airflow `m3-reconciliation` DAG (nightly) should include a step that:
1. Counts unresolved `neo4j_pending_edges` rows per `tenant_id` and `source_entity_type`.
2. Logs a warning if any row has `attempt_count > 3` — this means the target node has not appeared after multiple replay attempts and may indicate a connector configuration issue.
3. For rows older than 30 days with `attempt_count > 5`, raises an alert to the ops team.

---

## 9. CDM Version Update Protocol

### 9.1 What triggers a CDM version update

- A new source table is profiled and mapped → new entity type added to the CDM.
- A new FK column is discovered in an existing source table → new edge type added.
- An existing FK column is reclassified (different `semantic_class` or `neo4j_role`) → edge type or direction changes.

### 9.2 New entity type added

1. `migrate_schema()` runs (triggered by `nexus.cdm.version_published`). It reads `cdm_field_routing` for the new CDM version and creates constraints + indexes for any new entity types with `neo4j` in `db_target`.
2. Backfill DAG runs for the affected connector to emit UPSERT events for the new entity type. Writer creates nodes and edges normally.
3. If the new entity type has FKs to existing node types, those edges write immediately (target nodes exist). If the new entity type itself is a FK target for an existing entity type, `replay_pending_edges` fires for each new node and resolves any pending edges immediately.

```python
async def migrate_schema(self, cdm_version: str, tenant_id: str) -> None:
    routing_rows = await postgres.fetch("""
        SELECT DISTINCT entity
        FROM nexus_system.cdm_field_routing
        WHERE cdm_version = $1 AND 'neo4j' = ANY(db_target)
    """, cdm_version)

    for row in routing_rows:
        label = ENTITY_LABEL.get(row['entity'])
        if not label or label.startswith("__edge__"):
            continue
        await self.session.run(f"""
            CREATE CONSTRAINT IF NOT EXISTS FOR (n:{label})
            REQUIRE (n.id, n.tenant_id) IS UNIQUE
        """)
        await self.session.run(f"""
            CREATE INDEX IF NOT EXISTS FOR (n:{label}) ON (n.tenant_id)
        """)
    logger.info("migrate_schema complete: cdm_version=%s", cdm_version)
```

### 9.3 New FK field (new edge type) added

1. `cdm_field_routing` for the new CDM version contains a new field with `neo4j_role = edge_key`.
2. `ENTITY_LABEL` and `RelSpec` derivation in the mapper must be updated to include the new relationship type and its Cypher direction.
3. The connector's records are re-processed through the CDM mapper (triggered by CDM version publish). Each record produces a new UPSERT event carrying the new FK in its payload.
4. `write()` handles it naturally — the new FK is present in `entity.get_relationships()`, the edge is written or pending-queued.
5. A targeted `bulk_reconcile()` run for the affected entity type ensures historical records (pre-CDM-update) are back-processed.

```python
async def bulk_reconcile(
    self, entity_type: str, tenant_id: str, connector_id: str | None = None
) -> BulkReconcileResult:
    """
    Re-issues UPSERT for every CDM record of this entity type.
    Called by Airflow after a CDM version update that adds a new edge type.
    Uses batched Cypher writes to avoid overwhelming Neo4j.
    """
    # Pull approved CDM records from cdm_proposals for this entity type
    query = """
        SELECT cdm_entity_id, cdm_payload
        FROM nexus_system.cdm_proposals
        WHERE tenant_id = $1 AND target_entity = $2 AND status = 'confirmed'
    """
    params = [tenant_id, entity_type]
    if connector_id:
        query += " AND connector_id = $3"
        params.append(connector_id)

    rows = await postgres.fetch(query, *params)
    written = 0
    for batch in _batches(rows, size=500):
        for row in batch:
            entity = CdmEntity.from_payload(row)
            await self.write(entity)
            written += 1

    return BulkReconcileResult(entity_type=entity_type, records_processed=written)
```

### 9.4 Relationship type changed

If a CDM update renames or redirects an existing relationship (e.g. `MANAGED_BY` becomes `ACCOUNT_OWNER`), the old edges must be removed and new ones written:

```python
# Run once as a Cypher migration script before deploying the new CDM version
# neo4j/migrations/v2.2.0_rename_managed_by.cypher

MATCH ()-[r:MANAGED_BY]->()
WITH r, startNode(r) AS a, endNode(r) AS b,
     r.source_fk AS sfk, r.connector_id AS cid,
     r.created_at AS cat
DELETE r
WITH a, b, sfk, cid, cat
MERGE (a)-[r2:ACCOUNT_OWNER {source_fk: sfk}]->(b)
ON CREATE SET r2.connector_id=cid, r2.created_at=cat, r2.updated_at=datetime()
```

This runs **before** the new CDM mapper version is deployed, so there is no window where old edges coexist with new UPSERT events.

### 9.5 Entity removed from CDM

If an entity type is removed from `cdm_field_routing` (i.e. it is no longer `graph_persistent`):

1. Add a REMOVE event batch for all records of that entity type. The writer calls `delete()` for each — `DETACH DELETE` removes nodes and all their edges.
2. Drop the constraint and index in a Cypher migration.
3. Any pending edges referencing this entity type's nodes are purged via `_purge_pending_edges_for()`.

---

## 10. Correction Protocol

Corrections occur when a CDM field mapping was wrong and is now fixed. Three sub-cases.

### 10.1 FK field value corrected (wrong ID in source data)

The source system had a bad FK value (e.g. `managed_by_employee_id = 999` which doesn't exist). A source data correction updates the value to `managed_by_employee_id = 42`.

What happens:
1. CDC captures the UPDATE event on the source record.
2. CDM mapper re-processes the record → new UPSERT event with corrected FK.
3. `write()` executes Step 3 (edge reconciliation):
   - Stale edge with `source_fk` pointing to employee 999 is deleted (or was never written — was pending).
   - New edge to employee 42 is MERGEd.
4. If the old edge was in `neo4j_pending_edges` (employee 999 doesn't exist), it is deleted and replaced with a new pending edge for employee 42.

No special handling needed — the standard edge reconciliation loop covers this case.

### 10.2 FK semantic class corrected (wrong relationship type)

A field was classified as `edge_key` for the wrong relationship. Example: `owner_id` on a document record was incorrectly mapped to `PLACED_BY` (transaction relationship type) and should have been `OWNED_BY`. A CDM correction reclassifies the field.

What happens:
1. New CDM version is published with the corrected `cdm_field_routing` entry.
2. The old relationship type is no longer in the `RelSpec` list for `document`.
3. `bulk_reconcile("document", tenant_id)` runs:
   - For each document record, `write()` executes the full edge reconciliation.
   - The stale `PLACED_BY` edges (which have `source_fk` values identifying them as coming from document records) are deleted.
   - The new `OWNED_BY` edges are written.

```python
# The stale edge deletion in _write_edge() handles this:
# It deletes ANY edge of the CURRENT rel_type from this node with this source_fk
# where the target is wrong. But it does NOT clean up edges of a DIFFERENT rel_type
# that used to exist for the same source_fk.
# For a relationship-type correction, an explicit cleanup is needed first:

async def cleanup_stale_rel_type(
    self,
    entity_id: str,
    tenant_id: str,
    old_rel_type: str,
    source_fk_pattern: str,    # used to identify edges from this entity
) -> int:
    result = await self.session.run(f"""
        MATCH (n {{id: $id, tenant_id: $tid}})-[r:{old_rel_type}]->()
        WHERE r.source_fk STARTS WITH $sfk_prefix
        DELETE r
        RETURN count(r) AS deleted
    """, id=entity_id, tid=tenant_id, sfk_prefix=source_fk_pattern)
    record = await result.single()
    return record["deleted"] if record else 0
```

This is called by the Airflow correction DAG before `bulk_reconcile()` runs.

### 10.3 PII field reclassified (was embedded, now excluded)

If a field changes from a non-PII classification to `excluded` in `cdm_field_routing`, the primary impact is on Elasticsearch (embeddings must be regenerated without the field). For Neo4j the impact is narrower: if the field was used as an edge property, those properties must be removed.

```cypher
-- Run as a targeted migration: remove the property from all edges of this type
MATCH ()-[r:HAS_EMAIL]->()
REMOVE r.email_address
```

The Neo4j writer never re-adds PII fields to edge properties because `write()` derives edge properties only from the current CDM field routing — once a field is `excluded`, it is absent from `RelSpec.prop_values`.

---

## 11. Entity Label Map

```python
ENTITY_LABEL: dict[str, str] = {
    # Node entities
    "party":               "Party",
    "employee":            "Employee",
    "org_unit":            "OrgUnit",
    "address":             "Address",
    "product":             "Product",
    "document":            "Document",
    "engagement":          "Engagement",
    "interaction":         "Interaction",
    "location":            "Location",
    "promotion":           "Promotion",
    "position_assignment": "PositionAssignment",
    "transaction":         "Transaction",
    "work_order":          "WorkOrder",
    "payment_method":      "PaymentMethod",
    "job_candidate":       "JobCandidate",

    # Edge-only entities — no node created; payload written as relationship properties
    "party_email":    "__edge__:HAS_EMAIL:party",
    "product_bom":    "__edge__:HAS_COMPONENT:product",
    "product_photo":  "__edge__:HAS_PHOTO:product",
    "product_vendor": "__edge__:SUPPLIED_BY:product",
}
```

---

## 12. Implementation Phases

### Phase 1 — Setup (Weeks 1–2)

**D2B-01 · DDL migration + entity label map** (1.5 days · Must)
- Apply `v2.1.0_ground_truth_alignment.cypher` (14 node constraints, 10 tenant indexes, 10 relationship indexes)
- Apply migration V2.0.24 (`neo4j_pending_edges` table)
- Implement `ENTITY_LABEL` map and `__edge__` dispatch
- Cross-tenant isolation test

### Phase 2 — Core write path (Weeks 3–6)

**D2B-02 · Relationship catalog — all 23 edge types** (3 days · Must · Depends on D2B-01)
- Implement `RelSpec` derivation for all 23 relationship types in §3
- Unit test: each entity type produces the correct `RelSpec` list with correct Cypher direction
- Integration test: UPSERT event → correct edges in Neo4j

**D2B-03 · Pending edge protocol** (2 days · Must · Depends on D2B-02)
- Implement `_record_pending_edge()` writing to `neo4j_pending_edges`
- Implement `replay_pending_edges()` triggered from `write()` after node MERGE
- Implement `_purge_pending_edges_for()` called from `delete()` and `merge_golden_record()`
- Test: two-connector scenario — Transaction UPSERT arrives before Party UPSERT; edge is pending; Party UPSERT resolves it

**D2B-04 · Real-time delete + edge cleanup** (1 day · Must)

**D2B-05 · Golden Record merge** (2 days · Should)

**D2B-06 · CDM version published — schema migration** (1 day · Should)

### Phase 3 — Lifecycle events (Weeks 7–9)

**D2B-07 · Connector onboarding sequence** (2 days · Must · Depends on D2B-03)
- Test: two-connector backfill in sequence; verify pending edge count drops to zero after both connectors complete
- Test: `m3-reconciliation` nightly DAG flags stale pending edges correctly

**D2B-08 · CDM update — new edge type + bulk_reconcile** (2 days · Should · Depends on D2B-06)
- Test: add a new FK field to CDM; run `bulk_reconcile()`; confirm new edges written for all historical records

**D2B-09 · Correction protocol** (1.5 days · Could · Depends on D2B-08)
- Test: FK value corrected in source → stale edge deleted, new edge written
- Test: FK semantic class corrected → `cleanup_stale_rel_type()` + `bulk_reconcile()` produces clean edge set

**D2B-10 · Tenant isolation and performance validation** (1.5 days · Must)

---

## 13. Functional Requirements

| ID | Requirement | Priority |
|---|---|---|
| FR-NEO-01 | Every `UPSERT` must MERGE the node, call `replay_pending_edges()`, then reconcile outbound edges — in that order. | Must |
| FR-NEO-02 | `materialization_level` must never be written to any node or edge property. | Must |
| FR-NEO-03 | A missing target node must write a row to `neo4j_pending_edges` and return without error. | Must |
| FR-NEO-04 | `replay_pending_edges()` must be called on every successful node MERGE, not only during backfill. | Must |
| FR-NEO-05 | `neo4j_pending_edges` rows with `attempt_count > 3` must generate a monitoring warning. | Must |
| FR-NEO-06 | Edge reconciliation must delete stale edges (same `source_fk`, wrong target) before writing the new edge. | Must |
| FR-NEO-07 | `merge_golden_record()` must call `_purge_pending_edges_for(superseded_id)` before DETACH DELETE. | Must |
| FR-NEO-08 | `bulk_reconcile()` must be idempotent — running it twice for the same `(entity_type, tenant_id)` must produce no net change if the underlying data has not changed. | Must |
| FR-NEO-09 | `migrate_schema()` must be idempotent — `IF NOT EXISTS` guards on all DDL. | Must |
| FR-NEO-10 | Every Cypher write must include `tenant_id` in the MATCH/MERGE pattern. | Must |
| FR-NEO-11 | Edge-only entities (`party_email`, `product_bom`, `product_photo`, `product_vendor`) must write edge properties only — no standalone node. | Must |
| FR-NEO-12 | `cleanup_stale_rel_type()` must be run before `bulk_reconcile()` when a relationship type is renamed or redirected. | Must |

---

## 14. Open Questions

| # | Status | Question | Impact |
|---|---|---|---|
| OQ-NEO4J-01 | ❌ Open | APOC required for `merge_golden_record()` edge redirect. Confirm availability on deployment target. If absent, implement manual edge-recreate fallback. | D2B-05 |
| OQ-NEO4J-02 | ❌ Open | Superseded Golden Record nodes: hard-delete (current spec) vs soft-delete (`golden_record=false`, node retained for history). Soft-delete preserves traversal audit trail. | Storage + query engine |
| OQ-NEO4J-03 | ❌ Open | Any connector delivering hierarchical paths (like AdventureWorks `OrganizationNode`) needs special-case path parsing for `REPORTS_TO`. Confirm with connector owners before D2B-02. | Backfill correctness |
| OQ-NEO4J-04 | ❌ Open | Neo4j tenant isolation: per-tenant Aura DB vs `WHERE tenant_id` property filter. Re-evaluate at 20+ tenants. | Scalability |
| OQ-NEO4J-05 | ❌ Open | `product.raw_category_id` and `product.model_id_ref` are skipped (no ProductCategory/ProductModel nodes). Should these become first-class graph nodes in a future CDM version? Would enable "all products in category X" traversals. | CDM roadmap |
| OQ-NEO4J-06 | ❌ Open | `neo4j_pending_edges` retention: rows older than 30 days with `attempt_count > 5` — should they be auto-expired or kept indefinitely for ops investigation? | Storage + alerting |
| OQ-NEO4J-07 | ❌ Open | `bulk_reconcile()` reads from `cdm_proposals` directly. If approved proposals have been archived or partitioned, this may fail for historical data. Confirm data retention policy on `cdm_proposals` before implementing D2B-08. | Correction protocol |

---

*NEXUS Iteration 2 · nexus-m3-writer · Neo4j Handler · v0.3 · Mentis Consulting · April 2026 · Confidential*
