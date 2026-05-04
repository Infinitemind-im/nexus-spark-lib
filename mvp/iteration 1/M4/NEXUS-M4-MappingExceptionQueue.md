# NEXUS — Task Plan: Mapping Exception Queue
**Task ID: P6-M4-02 · Owner: Product Team (Full-Stack) · Days 5–6**
**Mentis Consulting · February 2026 · Confidential**

---

## What This Task Is and Why It Exists

The Mapping Exception Queue handles a specific and unavoidable situation in the data pipeline: M1's CDM Mapper encounters a field it has seen before (a mapping exists) but that mapping was proposed by M2 with medium confidence (≥ 0.80 but not yet confirmed by a human). The mapping is applied — the data flows through — but the field is flagged for human review.

This is the **Tier 2 mechanism**. It exists because the platform must never block data flow waiting for human decisions, but it also must not silently apply unconfirmed mappings indefinitely without a human eventually confirming them.

### The Three-Tier Mapping System (Context)

Understanding why this queue exists requires understanding the full tier model:

| Tier | Source | Confidence | Behaviour | Human action needed |
|---|---|---|---|---|
| **Tier 1** | Human-approved | Confirmed | Applied silently, permanently | None |
| **Tier 2** | M2 Structural Agent | ≥ 0.80 | Applied, flagged for review | Yes — this queue |
| **Tier 3** | No mapping found | — | Field stored in `source_extras`, never discarded | Optional (create new mapping) |

The Mapping Exception Queue is exclusively for **Tier 2 fields**. When a data steward approves a Tier 2 mapping through this queue, it is promoted to Tier 1 permanently — the same field on future records will be mapped silently with no further review.

### Relationship to P6-M4-01 (CDM Governance Queue)

These two queues handle different events from different producers:

| | CDM Governance Queue (M4-01) | Mapping Exception Queue (M4-02) |
|---|---|---|
| **Triggered by** | M2 Structural Agent — new schema proposal | M1 CDM Mapper — Tier 2 field on a live record |
| **Kafka source topic** | `nexus.cdm.extension_proposed` | `m1.int.mapping_failed` |
| **What's being reviewed** | Entire CDM extension (entity type + multiple fields) | Individual field mapping instance |
| **On approval** | New CDM version published, batch of mappings activated | Single field promoted Tier 2 → Tier 1 |
| **Frequency** | Low — triggered by new source schemas | High — triggered on every Tier 2 field in every record |
| **Urgency** | Low — pipeline continues without it | Medium — repeated Tier 2 flags accumulate rapidly |

---

## Scope

**In scope for Days 5–6:**
- PostgreSQL table: `nexus_system.mapping_review_queue`
- Kafka consumer: listens to `m1.int.mapping_failed`, deduplicates and stores exceptions
- FastAPI endpoints: list, get single, approve with field correction, reject
- Kafka producer: publishes `{tid}.m4.mapping_approved` on approval (M1 CDM Mapper subscribes to this to invalidate cache)
- Deduplication logic — the same Tier 2 field on the same source system will arrive repeatedly; the queue must not accumulate thousands of duplicates
- Kubernetes deployment extension (same pod as M4-01 governance API)
- Full acceptance test sequence

**Not in scope:**
- The UI (M6 — Day 8–10)
- Tier 3 (unmapped) field handling — those go to `source_extras`, not to this queue
- Bulk approval of multiple exceptions in one API call (Iteration 2)
- Auto-promotion after N approvals (Iteration 2)

---

## Dependencies

| Dependency | Owner | Must be done before |
|---|---|---|
| `nexus_core` library installed | Tech Lead | Week 1 |
| `nexus_system.cdm_mappings` table exists | Tech Lead | Week 1 DDL |
| `nexus_system.cdm_versions` table exists | Tech Lead | Week 1 DDL |
| `m1.int.mapping_failed` Kafka topic exists | Platform Team | Phase 0 |
| `{tid}.m4.mapping_approved` per-tenant topics exist | Platform Team / provisioning.py | Phase 0 |
| P6-M4-01 (CDM Governance Queue) complete | Product Team | Day 4 — shares same FastAPI app and DB pool |
| M1 CDM Mapper producing `m1.int.mapping_failed` events | Data Intelligence | Week 5 — but can stub in tests |

---

## Data Flow

```
M1 CDM Mapper — encounters Tier 2 field on a live record
    ↓  applies the mapping (data flows through)
    ↓  simultaneously publishes
m1.int.mapping_failed  (Kafka)
    ↓  consumed by
MappingExceptionConsumer (this task)
    ↓  deduplicates and stores in
nexus_system.mapping_review_queue  (PostgreSQL)
    ↓  read by
GET /api/v1/mappings/exceptions  (FastAPI — consumed by M6)
    ↓  data steward reviews, optionally corrects the CDM field, calls:
POST /api/v1/mappings/exceptions/{id}/approve
    ↓  triggers:
1. Mapping promoted Tier 2 → Tier 1 in nexus_system.cdm_mappings
2. {tid}.m4.mapping_approved published on Kafka
    ↓  consumed by:
M1 CDM Mapper — invalidates CDM Registry cache for this tenant
    ↓  effect:
Next record with this field uses Tier 1 mapping silently — no more exceptions for this field
```

On rejection:
```
POST /api/v1/mappings/exceptions/{id}/reject
    ↓
1. Exception marked rejected in mapping_review_queue
2. Tier 2 mapping removed from cdm_mappings (field goes back to Tier 3 / source_extras)
3. {tid}.m4.mapping_approved NOT published — M1 cache not affected
```

---

## Database Schema

### mapping_review_queue table

```sql
CREATE TABLE nexus_system.mapping_review_queue (
    review_id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id           TEXT NOT NULL,
    source_system       TEXT NOT NULL,    -- 'salesforce' | 'servicenow' | 'odoo' | ...
    source_table        TEXT NOT NULL,    -- e.g. 'res.partner'
    source_field        TEXT NOT NULL,    -- e.g. 'vat'
    suggested_cdm_entity TEXT,           -- M2's suggestion e.g. 'crm.party'
    suggested_cdm_field  TEXT,           -- M2's suggestion e.g. 'tax_id'
    approved_cdm_entity  TEXT,           -- Set by human on approval (may differ from suggestion)
    approved_cdm_field   TEXT,           -- Set by human on approval (may differ from suggestion)
    confidence          FLOAT,           -- M2's confidence score (0.80–0.94 range for Tier 2)
    status              TEXT NOT NULL DEFAULT 'pending',
                                         -- pending | approved | rejected
    occurrence_count    INT NOT NULL DEFAULT 1,  -- How many times this field triggered Tier 2
    first_seen_at       TIMESTAMPTZ DEFAULT NOW(),
    last_seen_at        TIMESTAMPTZ DEFAULT NOW(),
    reviewed_at         TIMESTAMPTZ,
    reviewed_by         TEXT,            -- nexus_user_id from X-User-ID header
    review_notes        TEXT,

    -- Deduplication: one pending review per (tenant, source system, table, field)
    CONSTRAINT uq_pending_review UNIQUE (tenant_id, source_system, source_table, source_field),

    CONSTRAINT valid_status CHECK (status IN ('pending', 'approved', 'rejected')),
    CONSTRAINT valid_confidence CHECK (confidence >= 0 AND confidence <= 1)
);

-- RLS: tenant can only see their own exceptions
ALTER TABLE nexus_system.mapping_review_queue ENABLE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON nexus_system.mapping_review_queue
    USING (tenant_id = current_setting('app.current_tenant'));

-- Index for the most common query: pending exceptions per tenant, oldest first
CREATE INDEX idx_mapping_review_tenant_status
    ON nexus_system.mapping_review_queue (tenant_id, status, first_seen_at ASC);

-- Index for deduplication upsert performance
CREATE INDEX idx_mapping_review_dedup
    ON nexus_system.mapping_review_queue (tenant_id, source_system, source_table, source_field, status);
```

### Why occurrence_count matters

The same Tier 2 field fires on every record that contains it. A Salesforce sync of 50,000 Accounts where the `vat` field has a Tier 2 mapping produces 50,000 `m1.int.mapping_failed` events. Without deduplication, the review queue has 50,000 rows for the same field. The data steward sees noise, not signal.

`occurrence_count` + the `UNIQUE` constraint on `(tenant_id, source_system, source_table, source_field)` means the queue has exactly **one row per unique field** regardless of how many records triggered it. The data steward sees "res.partner / vat — seen 50,000 times — confidence 0.88" and makes one decision that affects all past and future occurrences.

---

## Implementation

### Step 1 — Project Structure Extension

This task extends the existing M4 project created in P6-M4-01. No new service is needed — the consumer and endpoints are added to the same application.

```
m4/
├── consumers/
│   ├── cdm_governance_consumer.py    ← already exists (M4-01)
│   └── mapping_exception_consumer.py ← NEW this task
├── api/
│   ├── main.py                       ← extend to include new router
│   ├── governance.py                 ← already exists (M4-01)
│   └── mapping_exceptions.py         ← NEW this task
├── models/
│   ├── governance.py                 ← already exists (M4-01)
│   └── mapping_exceptions.py         ← NEW this task
└── tests/
    ├── test_governance.py            ← already exists (M4-01)
    └── test_mapping_exceptions.py    ← NEW this task
```

### Step 2 — Kafka Consumer with Deduplication

```python
# m4/consumers/mapping_exception_consumer.py

import logging
from nexus_core.messaging import NexusConsumer, NexusMessage
from nexus_core.topics import CrossModuleTopicNamer
from nexus_core.db import get_tenant_scoped_connection

logger = logging.getLogger(__name__)

class MappingExceptionConsumer:
    """
    Consumes m1.int.mapping_failed.
    Stores Tier 2 field mapping exceptions for human review.

    Deduplication strategy:
    - UNIQUE constraint on (tenant_id, source_system, source_table, source_field)
      with status='pending' enforced at DB level
    - On conflict: increment occurrence_count, update last_seen_at
    - Already-reviewed (approved/rejected) fields restart as new pending items
      when they appear again (source system may have changed the field's context)
    """

    def __init__(self, db_pool, kafka_bootstrap: str):
        self.consumer = NexusConsumer(
            bootstrap_servers=kafka_bootstrap,
            group_id="m4-mapping-exceptions",
            topics=[CrossModuleTopicNamer.M1Internal.MAPPING_FAILED],
        )
        self.db = db_pool

    async def run(self):
        logger.info("Mapping Exception Consumer started")
        while True:
            message = self.consumer.poll(timeout=1.0)
            if not message:
                continue
            try:
                await self._upsert_exception(message)
                self.consumer.commit(message)
            except Exception as e:
                logger.error(
                    f"Failed to store mapping exception: {e}",
                    extra={"tenant_id": message.tenant_id},
                    exc_info=True,
                )
                # Do NOT commit — redelivers on restart

    async def _upsert_exception(self, message: NexusMessage):
        payload = message.payload
        async with get_tenant_scoped_connection(self.db, message.tenant_id) as conn:
            await conn.execute("""
                INSERT INTO nexus_system.mapping_review_queue
                    (tenant_id, source_system, source_table, source_field,
                     suggested_cdm_entity, suggested_cdm_field, confidence)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                ON CONFLICT (tenant_id, source_system, source_table, source_field)
                DO UPDATE SET
                    occurrence_count = mapping_review_queue.occurrence_count + 1,
                    last_seen_at = NOW(),
                    -- Update suggestion if confidence improved
                    suggested_cdm_entity = CASE
                        WHEN EXCLUDED.confidence > mapping_review_queue.confidence
                        THEN EXCLUDED.suggested_cdm_entity
                        ELSE mapping_review_queue.suggested_cdm_entity
                    END,
                    suggested_cdm_field = CASE
                        WHEN EXCLUDED.confidence > mapping_review_queue.confidence
                        THEN EXCLUDED.suggested_cdm_field
                        ELSE mapping_review_queue.suggested_cdm_field
                    END,
                    confidence = GREATEST(mapping_review_queue.confidence, EXCLUDED.confidence)
                WHERE mapping_review_queue.status = 'pending'
            """,
                message.tenant_id,
                payload["source_system"],
                payload.get("source_table", ""),
                payload["source_field"],
                payload.get("suggested_cdm_entity"),
                payload.get("suggested_cdm_field"),
                payload.get("confidence"),
            )
        logger.info(
            "Mapping exception upserted",
            extra={
                "tenant_id": message.tenant_id,
                "source_field": payload.get("source_field"),
                "confidence": payload.get("confidence"),
            }
        )
```

### Step 3 — Pydantic Models

```python
# m4/models/mapping_exceptions.py

from pydantic import BaseModel
from typing import Optional
from datetime import datetime
import uuid

class MappingExceptionResponse(BaseModel):
    review_id:             uuid.UUID
    tenant_id:             str
    source_system:         str
    source_table:          str
    source_field:          str
    suggested_cdm_entity:  Optional[str] = None
    suggested_cdm_field:   Optional[str] = None
    approved_cdm_entity:   Optional[str] = None
    approved_cdm_field:    Optional[str] = None
    confidence:            Optional[float] = None
    status:                str
    occurrence_count:      int
    first_seen_at:         datetime
    last_seen_at:          datetime
    reviewed_at:           Optional[datetime] = None
    reviewed_by:           Optional[str] = None
    review_notes:          Optional[str] = None

class MappingExceptionListResponse(BaseModel):
    exceptions: list[MappingExceptionResponse]
    total: int

class ApproveMappingBody(BaseModel):
    """
    The data steward may accept M2's suggestion as-is, or override it.
    If cdm_entity and cdm_field are not provided, the suggestion is used.
    """
    cdm_entity:   Optional[str] = None   # Override M2's suggestion if needed
    cdm_field:    Optional[str] = None   # Override M2's suggestion if needed
    review_notes: Optional[str] = ""

class RejectMappingBody(BaseModel):
    reason: str   # Required — must explain why mapping is wrong
```

### Step 4 — FastAPI Endpoints

```python
# m4/api/mapping_exceptions.py

from fastapi import APIRouter, HTTPException, Header
from nexus_core.messaging import NexusProducer, NexusMessage
from nexus_core.topics import CrossModuleTopicNamer
from nexus_core.db import get_tenant_scoped_connection
from m4.models.mapping_exceptions import (
    MappingExceptionListResponse, MappingExceptionResponse,
    ApproveMappingBody, RejectMappingBody
)
import logging

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/v1/mappings", tags=["Mapping Exceptions"])


@router.get("/exceptions", response_model=MappingExceptionListResponse)
async def list_exceptions(
    x_tenant_id: str = Header(...),
    x_user_id:   str = Header(...),
    status:      str = "pending",
    source_system: str = None,   # Optional filter by source system
):
    """
    List mapping exceptions for this tenant.
    Default: pending only, oldest first (highest priority to resolve).
    Supports optional filtering by source_system.
    """
    async with get_tenant_scoped_connection(db_pool, x_tenant_id) as conn:
        query = """
            SELECT * FROM nexus_system.mapping_review_queue
            WHERE tenant_id = $1 AND status = $2
        """
        params = [x_tenant_id, status]

        if source_system:
            query += " AND source_system = $3"
            params.append(source_system)

        query += " ORDER BY first_seen_at ASC LIMIT 200"
        rows = await conn.fetch(query, *params)

    exceptions = [MappingExceptionResponse(**dict(r)) for r in rows]
    return MappingExceptionListResponse(exceptions=exceptions, total=len(exceptions))


@router.get("/exceptions/{review_id}", response_model=MappingExceptionResponse)
async def get_exception(
    review_id:   str,
    x_tenant_id: str = Header(...),
    x_user_id:   str = Header(...),
):
    """Get a single mapping exception by ID."""
    async with get_tenant_scoped_connection(db_pool, x_tenant_id) as conn:
        row = await conn.fetchrow("""
            SELECT * FROM nexus_system.mapping_review_queue
            WHERE review_id = $1 AND tenant_id = $2
        """, review_id, x_tenant_id)
    if not row:
        raise HTTPException(404, "Exception not found")
    return MappingExceptionResponse(**dict(row))


@router.post("/exceptions/{review_id}/approve")
async def approve_exception(
    review_id:   str,
    body:        ApproveMappingBody,
    x_tenant_id: str = Header(...),
    x_user_id:   str = Header(...),
):
    """
    Approve a mapping exception — promotes Tier 2 → Tier 1.

    The data steward can:
    - Accept M2's suggestion as-is (omit cdm_entity and cdm_field)
    - Override with a corrected CDM field (provide cdm_entity and cdm_field)

    On approval:
    1. Resolves final CDM field (override if provided, else suggestion)
    2. Upserts into cdm_mappings as Tier 1 (permanent, no further review)
    3. Marks exception as approved with reviewer identity and final fields
    4. Publishes {tid}.m4.mapping_approved — M1 CDM Mapper invalidates cache
    """
    async with get_tenant_scoped_connection(db_pool, x_tenant_id) as conn:

        # Load exception — must be pending and belong to this tenant
        row = await conn.fetchrow("""
            SELECT * FROM nexus_system.mapping_review_queue
            WHERE review_id = $1 AND tenant_id = $2 AND status = 'pending'
        """, review_id, x_tenant_id)

        if not row:
            raise HTTPException(404, "Exception not found or already reviewed")

        # Resolve final CDM mapping — human override takes priority over M2 suggestion
        final_cdm_entity = body.cdm_entity or row["suggested_cdm_entity"]
        final_cdm_field  = body.cdm_field  or row["suggested_cdm_field"]

        if not final_cdm_entity or not final_cdm_field:
            raise HTTPException(
                422,
                "Cannot approve: no CDM mapping available. "
                "Provide cdm_entity and cdm_field in the request body."
            )

        # Get active CDM version for this tenant
        active_version = await conn.fetchval("""
            SELECT version FROM nexus_system.cdm_versions
            WHERE tenant_id = $1 AND status = 'active'
            ORDER BY published_at DESC LIMIT 1
        """, x_tenant_id)

        if not active_version:
            raise HTTPException(
                422,
                "No active CDM version found for this tenant. "
                "At least one CDM proposal must be approved before mapping exceptions can be resolved."
            )

        # Promote to Tier 1 in cdm_mappings
        # ON CONFLICT: update tier and approver (field may already exist as Tier 2)
        await conn.execute("""
            INSERT INTO nexus_system.cdm_mappings
                (tenant_id, cdm_version, source_system, source_table, source_field,
                 cdm_entity, cdm_field, confidence, tier, approved_by, approved_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 1, $9, NOW())
            ON CONFLICT (tenant_id, cdm_version, source_system, source_table, source_field)
            DO UPDATE SET
                cdm_entity   = EXCLUDED.cdm_entity,
                cdm_field    = EXCLUDED.cdm_field,
                confidence   = EXCLUDED.confidence,
                tier         = 1,
                approved_by  = EXCLUDED.approved_by,
                approved_at  = NOW()
        """,
            x_tenant_id, active_version,
            row["source_system"], row["source_table"], row["source_field"],
            final_cdm_entity, final_cdm_field,
            row["confidence"],
            x_user_id,
        )

        # Record the human's decision on the exception
        await conn.execute("""
            UPDATE nexus_system.mapping_review_queue
            SET status            = 'approved',
                reviewed_at       = NOW(),
                reviewed_by       = $1,
                review_notes      = $2,
                approved_cdm_entity = $3,
                approved_cdm_field  = $4
            WHERE review_id = $5
        """, x_user_id, body.review_notes, final_cdm_entity, final_cdm_field, review_id)

    # Publish mapping_approved AFTER DB transaction committed
    # M1 CDM Mapper subscribes to this per-tenant topic and invalidates its registry cache
    was_overridden = bool(body.cdm_entity or body.cdm_field)
    producer.publish(NexusMessage(
        topic=CrossModuleTopicNamer.m4(x_tenant_id, "mapping_approved"),
        tenant_id=x_tenant_id,
        source_system="nexus_m4",
        source_record_id=review_id,
        permission_scope={},
        entity_type="cdm.mapping",
        payload={
            "review_id":         review_id,
            "source_system":     row["source_system"],
            "source_table":      row["source_table"],
            "source_field":      row["source_field"],
            "cdm_entity":        final_cdm_entity,
            "cdm_field":         final_cdm_field,
            "promoted_to_tier_1": True,
            "human_override":    was_overridden,   # True if human corrected M2's suggestion
            "approved_by":       x_user_id,
        },
    ))

    logger.info(
        "Mapping exception approved",
        extra={
            "tenant_id":      x_tenant_id,
            "review_id":      review_id,
            "source_field":   row["source_field"],
            "final_cdm_field": final_cdm_field,
            "human_override": was_overridden,
            "approved_by":    x_user_id,
        }
    )
    return {
        "status":            "approved",
        "promoted_to_tier_1": True,
        "final_cdm_entity":  final_cdm_entity,
        "final_cdm_field":   final_cdm_field,
        "human_override":    was_overridden,
    }


@router.post("/exceptions/{review_id}/reject")
async def reject_exception(
    review_id:   str,
    body:        RejectMappingBody,
    x_tenant_id: str = Header(...),
    x_user_id:   str = Header(...),
):
    """
    Reject a mapping exception.

    The Tier 2 mapping is removed from cdm_mappings.
    The field will fall back to Tier 3 (stored in source_extras) on future records.
    No mapping_approved event is published — M1 cache is not affected.
    """
    async with get_tenant_scoped_connection(db_pool, x_tenant_id) as conn:

        row = await conn.fetchrow("""
            SELECT * FROM nexus_system.mapping_review_queue
            WHERE review_id = $1 AND tenant_id = $2 AND status = 'pending'
        """, review_id, x_tenant_id)

        if not row:
            raise HTTPException(404, "Exception not found or already reviewed")

        # Get active CDM version
        active_version = await conn.fetchval("""
            SELECT version FROM nexus_system.cdm_versions
            WHERE tenant_id = $1 AND status = 'active'
            ORDER BY published_at DESC LIMIT 1
        """, x_tenant_id)

        # Remove the Tier 2 mapping from cdm_mappings
        # Field will fall back to Tier 3 (source_extras) on future records
        if active_version:
            await conn.execute("""
                DELETE FROM nexus_system.cdm_mappings
                WHERE tenant_id     = $1
                  AND cdm_version   = $2
                  AND source_system = $3
                  AND source_table  = $4
                  AND source_field  = $5
                  AND tier = 2
            """,
                x_tenant_id, active_version,
                row["source_system"], row["source_table"], row["source_field"],
            )

        # Mark exception as rejected
        await conn.execute("""
            UPDATE nexus_system.mapping_review_queue
            SET status       = 'rejected',
                reviewed_at  = NOW(),
                reviewed_by  = $1,
                review_notes = $2
            WHERE review_id = $3
        """, x_user_id, body.reason, review_id)

    logger.info(
        "Mapping exception rejected — field falls back to Tier 3 (source_extras)",
        extra={
            "tenant_id":    x_tenant_id,
            "review_id":    review_id,
            "source_field": row["source_field"],
            "reason":       body.reason,
            "rejected_by":  x_user_id,
        }
    )
    return {"status": "rejected", "tier_fallback": "source_extras"}
```

### Step 5 — Register Router in App Factory

```python
# m4/api/main.py  (extend from M4-01)

from fastapi import FastAPI
from m4.api.governance import router as governance_router
from m4.api.mapping_exceptions import router as mapping_router   # NEW

def create_app() -> FastAPI:
    app = FastAPI(
        title="NEXUS M4 Governance API",
        version="1.0.0",
    )
    app.include_router(governance_router)
    app.include_router(mapping_router)   # NEW

    @app.get("/health")
    async def health():
        return {"status": "ok", "service": "m4-governance-api"}

    return app
```

### Step 6 — Register Consumer Startup

```python
# m4/entrypoint.py  (start both consumers alongside the API)

import asyncio
from m4.consumers.cdm_governance_consumer import CDMGovernanceConsumer
from m4.consumers.mapping_exception_consumer import MappingExceptionConsumer

async def main():
    governance_consumer  = CDMGovernanceConsumer(db_pool, KAFKA_BOOTSTRAP)
    exception_consumer   = MappingExceptionConsumer(db_pool, KAFKA_BOOTSTRAP)

    # Run both consumers concurrently alongside the FastAPI server
    await asyncio.gather(
        governance_consumer.run(),
        exception_consumer.run(),
    )
```

### Step 7 — Kong Route Addition

No new service needed — extend the existing Kong route to include the new path:

```yaml
# kong/routes/m4-governance.yaml  (extend from M4-01)

services:
  - name: m4-governance
    url: http://m4-governance-api.nexus-app.svc.cluster.local:8002
    routes:
      - name: governance-proposals
        paths: [/api/v1/governance]
        methods: [GET, POST]
      - name: mapping-exceptions        # NEW
        paths: [/api/v1/mappings]
        methods: [GET, POST]
    plugins:
      - name: jwt
      - name: prometheus
```

---

## API Contract Summary

| Method | Path | Description | Publishes to Kafka |
|---|---|---|---|
| `GET` | `/api/v1/mappings/exceptions` | List exceptions (filter by status, source_system) | — |
| `GET` | `/api/v1/mappings/exceptions/{id}` | Get single exception with full context | — |
| `POST` | `/api/v1/mappings/exceptions/{id}/approve` | Approve — promote Tier 2 → Tier 1 | `{tid}.m4.mapping_approved` |
| `POST` | `/api/v1/mappings/exceptions/{id}/reject` | Reject — field falls back to Tier 3 | — |

All endpoints require `X-Tenant-ID` and `X-User-ID` headers injected by Kong.

---

## Acceptance Test Sequence

Run this in order on Day 6 after all components are deployed.

```bash
# ── 1. Simulate M1 CDM Mapper publishing Tier 2 mapping exceptions ────────────

python3 -c "
from nexus_core.messaging import NexusProducer, NexusMessage
from nexus_core.topics import CrossModuleTopicNamer

p = NexusProducer('nexus-kafka-kafka-bootstrap.nexus-data.svc.cluster.local:9092')

# Publish 3 exceptions for the same field (simulates 3 records hitting Tier 2)
for i in range(3):
    p.publish(NexusMessage(
        topic=CrossModuleTopicNamer.M1Internal.MAPPING_FAILED,
        tenant_id='test-tenant',
        source_system='odoo',
        source_record_id=f'rec-{i}',
        permission_scope={},
        entity_type='mapping.exception',
        payload={
            'source_system':        'odoo',
            'source_table':         'res.partner',
            'source_field':         'vat',
            'suggested_cdm_entity': 'crm.party',
            'suggested_cdm_field':  'tax_id',
            'confidence':           0.88,
        }
    ))

# Publish 1 exception for a different field
p.publish(NexusMessage(
    topic=CrossModuleTopicNamer.M1Internal.MAPPING_FAILED,
    tenant_id='test-tenant',
    source_system='odoo',
    source_record_id='rec-100',
    permission_scope={},
    entity_type='mapping.exception',
    payload={
        'source_system':        'odoo',
        'source_table':         'res.partner',
        'source_field':         'website',
        'suggested_cdm_entity': 'crm.party',
        'suggested_cdm_field':  'web_url',
        'confidence':           0.81,
    }
))
print('Exceptions published')
"

# ── 2. Verify deduplication — must have 2 rows, not 4 ─────────────────────────

psql $POSTGRES_DSN -c "
    SELECT source_field, occurrence_count, status, confidence
    FROM nexus_system.mapping_review_queue
    WHERE tenant_id = 'test-tenant';
"
# Expected: 2 rows
# - res.partner/vat: occurrence_count=3, status=pending
# - res.partner/website: occurrence_count=1, status=pending

# ── 3. List exceptions via API ────────────────────────────────────────────────

curl -s https://api.nexus.internal/api/v1/mappings/exceptions \
  -H "Authorization: Bearer $TEST_JWT" | jq .
# Expected: {"exceptions": [...], "total": 2}

# ── 4. Filter by source_system ────────────────────────────────────────────────

curl -s "https://api.nexus.internal/api/v1/mappings/exceptions?source_system=odoo" \
  -H "Authorization: Bearer $TEST_JWT" | jq '.total'
# Expected: 2

# ── 5. Approve with M2's suggestion ──────────────────────────────────────────

VAT_REVIEW_ID=$(psql $POSTGRES_DSN -t -c \
  "SELECT review_id FROM nexus_system.mapping_review_queue
   WHERE source_field='vat' AND tenant_id='test-tenant';")

curl -s -X POST \
  "https://api.nexus.internal/api/v1/mappings/exceptions/$VAT_REVIEW_ID/approve" \
  -H "Authorization: Bearer $TEST_JWT" \
  -H "Content-Type: application/json" \
  -d '{"review_notes": "Confirmed: VAT number is tax_id"}' | jq .
# Expected:
# {
#   "status": "approved",
#   "promoted_to_tier_1": true,
#   "final_cdm_entity": "crm.party",
#   "final_cdm_field": "tax_id",
#   "human_override": false
# }

# ── 6. Verify Tier 1 mapping created in cdm_mappings ─────────────────────────

psql $POSTGRES_DSN -c "
    SELECT source_field, cdm_field, tier, approved_by, approved_at
    FROM nexus_system.cdm_mappings
    WHERE tenant_id='test-tenant' AND source_field='vat';
"
# Expected: tier=1, cdm_field='tax_id', approved_by='<okta_user_id>'

# ── 7. Verify {tid}.m4.mapping_approved published on Kafka ───────────────────

# In Kafka UI: topic test-tenant.m4.mapping_approved
# Expected: 1 message, payload.source_field='vat', payload.promoted_to_tier_1=true,
#           payload.human_override=false

# ── 8. Approve with human override (correcting M2's suggestion) ───────────────

WEBSITE_REVIEW_ID=$(psql $POSTGRES_DSN -t -c \
  "SELECT review_id FROM nexus_system.mapping_review_queue
   WHERE source_field='website' AND tenant_id='test-tenant';")

curl -s -X POST \
  "https://api.nexus.internal/api/v1/mappings/exceptions/$WEBSITE_REVIEW_ID/approve" \
  -H "Authorization: Bearer $TEST_JWT" \
  -H "Content-Type: application/json" \
  -d '{
    "cdm_entity": "crm.party",
    "cdm_field": "website_url",
    "review_notes": "M2 suggested web_url but our CDM uses website_url"
  }' | jq .
# Expected: human_override=true, final_cdm_field='website_url'

# ── 9. Test rejection flow ────────────────────────────────────────────────────

# Publish another exception
python3 -c "..." # Publish a new exception for a different field

curl -s -X POST \
  "https://api.nexus.internal/api/v1/mappings/exceptions/$NEW_REVIEW_ID/reject" \
  -H "Authorization: Bearer $TEST_JWT" \
  -H "Content-Type: application/json" \
  -d '{"reason": "This field contains internal IDs — should not be mapped to CDM"}' | jq .
# Expected: {"status": "rejected", "tier_fallback": "source_extras"}

# Verify Tier 2 mapping removed from cdm_mappings for this field
# Verify no mapping_approved event published on Kafka

# ── 10. Multi-tenant isolation test ──────────────────────────────────────────

curl -s "https://api.nexus.internal/api/v1/mappings/exceptions" \
  -H "Authorization: Bearer $TEST_BETA_JWT" | jq '.total'
# Expected: 0 — test-beta cannot see test-tenant exceptions

# ── 11. Verify M1 CDM Mapper cache invalidated ────────────────────────────────

# Check M1 CDM Mapper logs after step 5:
# Expected log line: "CDM cache invalidated for tenant=test-tenant: N entries removed"
# This confirms M1 received the mapping_approved event and invalidated its registry cache
```

---

## Acceptance Criteria

| # | Test | Expected Result |
|---|---|---|
| 1 | 3 exceptions for same field published | One row in `mapping_review_queue`, `occurrence_count=3` |
| 2 | GET /exceptions | Returns pending exceptions oldest-first |
| 3 | GET /exceptions?source_system=odoo | Filters correctly |
| 4 | Approve with suggestion | Tier 2 → Tier 1 in `cdm_mappings`, `human_override=false` |
| 5 | Approve with override | Final CDM field uses human's value, `human_override=true` |
| 6 | Approve without any CDM mapping available | Returns HTTP 422 |
| 7 | Reject exception | Tier 2 mapping deleted from `cdm_mappings`, field → `source_extras` |
| 8 | `mapping_approved` on Kafka | Published on approval, NOT on rejection |
| 9 | M1 cache invalidation | M1 logs show cache invalidated after `mapping_approved` event |
| 10 | Cross-tenant isolation | test-beta sees zero exceptions from test-tenant |
| 11 | Consumer crash mid-batch | Offset not committed, exception reprocessed, no duplicate in DB |
| 12 | `reviewed_by` field | Contains Okta user ID from `X-User-ID` header |

---

## What Downstream Tasks Depend On

Once P6-M4-02 is complete and passing:

- **P7-M6-02 (CDM Governance Console)** — the Next.js UI adds a second tab for Mapping Exceptions alongside CDM Proposals, reading from `GET /api/v1/mappings/exceptions`
- **M1 CDM Mapper** — subscribes to `{tid}.m4.mapping_approved` to invalidate its CDM Registry cache; without this the newly promoted Tier 1 mapping takes 5 minutes to activate
- **Iteration 2** — bulk approval endpoint, auto-promotion after N human approvals, exception statistics dashboard

---

## Key Design Decisions

**Why deduplication at the DB level and not in the consumer?** The consumer could maintain an in-memory set of seen fields, but this resets on restart. The `UNIQUE` constraint is durable — it survives consumer crashes and pod restarts without leaking duplicates.

**Why update the suggestion if confidence improves on re-arrival?** M2 may refine its understanding of a field as it processes more records. If a second arrival has higher confidence and a different CDM suggestion, the queue should show the best current suggestion to the data steward, not the first one seen.

**Why does rejection delete the Tier 2 mapping instead of just marking it rejected?** If the Tier 2 mapping stays in `cdm_mappings` after rejection, M1 CDM Mapper will continue applying it on future records and generating more Tier 2 exceptions indefinitely. Deleting it returns the field to Tier 3 — future records store the field in `source_extras` cleanly.

**Why is there no `mapping_approved` published on rejection?** M1's CDM Mapper cache only needs to be invalidated when a mapping changes. On rejection, the mapping is deleted — M1 will simply find no mapping and use Tier 3. No cache invalidation needed.

---

*NEXUS M4 Mapping Exception Queue Task Plan · Mentis Consulting · February 2026 · Confidential*
