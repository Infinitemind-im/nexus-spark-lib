# NEXUS — Task Plan: CDM Governance Queue + FastAPI
**Task ID: P6-M4-01 · Owner: Product Team (Full-Stack) · Days 3–4**
**Mentis Consulting · February 2026 · Confidential**

---

## What This Task Is and Why It Exists

The CDM Governance Queue is the only point in the entire platform where a human intervenes in the automated pipeline. Every other step — ingestion, mapping, routing, writing to M3 — is fully automated. This task is the exception handler: when M2's Structural Agent proposes a change to the Canonical Data Model, a human data steward must review and either approve or reject it before the platform acts on it.

Without this task, one of two bad things happens: CDM proposals are auto-approved (dangerous — bad mappings propagate silently into M3 and corrupt the knowledge stores) or CDM proposals are dropped (also dangerous — new source schemas never get mapped and data stops flowing).

This task is built before M4-02 (Mapping Exception Queue) and before M6 (the UI that will display it), because both depend on the database schema and API contracts defined here.

---

## Scope

**In scope for Days 3–4:**
- PostgreSQL table: `nexus_system.governance_queue`
- Kafka consumer: listens to `nexus.cdm.extension_proposed`, stores proposals
- FastAPI service: 4 endpoints (list, approve, reject, get single)
- Kafka producer: publishes `nexus.cdm.version_published` on approval, `nexus.cdm.extension_rejected` on rejection
- CDM version bump logic in `nexus_system.cdm_versions`
- Kubernetes deployment + Kong route registration
- Full acceptance test sequence

**Not in scope:**
- The UI (M6 Governance Console — built in Days 8–10)
- Mapping exception queue (P6-M4-02 — Days 5–6)
- Temporal workflow engine (P6-M4-03 — Days 5–6)
- RBAC enforcement on who can approve (Iteration 2)

---

## Dependencies

| Dependency | Owner | Must be done before |
|---|---|---|
| `nexus_core` library installed | Tech Lead | Day 1 of Week 1 |
| `nexus_system.cdm_versions` table exists | Tech Lead | Week 1 DDL |
| `nexus_system.cdm_mappings` table exists | Tech Lead | Week 1 DDL |
| Kafka topic `nexus.cdm.extension_proposed` exists | Platform Team | Phase 0 |
| Kafka topic `nexus.cdm.version_published` exists | Platform Team | Phase 0 |
| Kafka topic `nexus.cdm.extension_rejected` exists | Platform Team | Phase 0 |
| Kong JWT plugin active | Platform Team | Phase 0 |
| PostgreSQL RLS active on `nexus_system` | Tech Lead | Week 1 |

---

## Data Flow

```
M2 Structural Agent
    ↓  publishes
nexus.cdm.extension_proposed  (Kafka)
    ↓  consumed by
CDMGovernanceConsumer (this task)
    ↓  stores in
nexus_system.governance_queue  (PostgreSQL)
    ↓  read by
GET /api/v1/governance/proposals  (FastAPI — consumed by M6)
    ↓  human reviews in M6 UI, calls:
POST /api/v1/governance/proposals/{id}/approve
    ↓  triggers:
1. New CDM version inserted into nexus_system.cdm_versions
2. Approved mappings inserted into nexus_system.cdm_mappings
3. nexus.cdm.version_published published on Kafka
    ↓  consumed by:
M1 CDM Mapper (invalidates cache, activates new mappings)
M3 Writers (aware new CDM version is active)
```

On rejection:
```
POST /api/v1/governance/proposals/{id}/reject
    ↓
nexus.cdm.extension_rejected published on Kafka
M2 Structural Agent logs rejection, does not re-propose same schema
```

---

## Database Schema

### governance_queue table

```sql
CREATE TABLE nexus_system.governance_queue (
    proposal_id     UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    proposal_type   TEXT NOT NULL,         -- 'cdm_extension' | 'mapping_exception'
    tenant_id       TEXT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'pending',
                                           -- pending | approved | rejected
    payload         JSONB NOT NULL,        -- full proposal from M2
    submitted_at    TIMESTAMPTZ DEFAULT NOW(),
    reviewed_at     TIMESTAMPTZ,
    reviewed_by     TEXT,                  -- nexus_user_id from X-User-ID header
    review_notes    TEXT,

    CONSTRAINT valid_status CHECK (status IN ('pending', 'approved', 'rejected')),
    CONSTRAINT valid_proposal_type CHECK (proposal_type IN ('cdm_extension', 'mapping_exception'))
);

-- RLS: tenant can only see their own proposals
ALTER TABLE nexus_system.governance_queue ENABLE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON nexus_system.governance_queue
    USING (tenant_id = current_setting('app.current_tenant'));

-- Index for the most common query pattern (pending proposals per tenant)
CREATE INDEX idx_governance_queue_tenant_status
    ON nexus_system.governance_queue (tenant_id, status, submitted_at DESC);
```

### CDM proposal payload structure (JSONB)

The `payload` field stores the full proposal from M2. The governance consumer does not parse or validate this — it stores it as-is and returns it as-is. The approval endpoint reads specific fields from it.

```json
{
  "cdm_version_from": "1.0",
  "source_system": "odoo",
  "source_schema_snapshot": {
    "table": "res.partner",
    "columns": ["id", "name", "email", "phone", "vat"]
  },
  "field_proposals": [
    {
      "source_table": "res.partner",
      "source_field": "name",
      "cdm_entity": "crm.party",
      "cdm_field": "display_name",
      "confidence": 0.97,
      "reasoning": "Standard name field maps directly to CDM party display name"
    },
    {
      "source_table": "res.partner",
      "source_field": "vat",
      "cdm_entity": "crm.party",
      "cdm_field": "tax_id",
      "confidence": 0.88,
      "reasoning": "VAT number is a tax identifier"
    }
  ]
}
```

---

## Implementation

### Step 1 — Project Structure

```
m4/
├── __init__.py
├── consumers/
│   └── cdm_governance_consumer.py
├── api/
│   ├── __init__.py
│   ├── main.py            # FastAPI app factory
│   ├── governance.py      # CDM proposal endpoints
│   └── dependencies.py    # Shared: db pool, producer, identity extraction
├── models/
│   └── governance.py      # Pydantic request/response models
└── tests/
    └── test_governance.py
```

### Step 2 — Kafka Consumer

```python
# m4/consumers/cdm_governance_consumer.py

import asyncio
import logging
from nexus_core.messaging import NexusConsumer, NexusMessage
from nexus_core.topics import CrossModuleTopicNamer
from nexus_core.db import get_tenant_scoped_connection

logger = logging.getLogger(__name__)

class CDMGovernanceConsumer:
    """
    Consumes nexus.cdm.extension_proposed.
    Stores every proposal in nexus_system.governance_queue with status='pending'.
    Does NOT auto-approve anything — every proposal waits for human review.
    Commits offset only after successful DB write.
    """

    def __init__(self, db_pool, kafka_bootstrap: str):
        self.consumer = NexusConsumer(
            bootstrap_servers=kafka_bootstrap,
            group_id="m4-cdm-governance",
            topics=[CrossModuleTopicNamer.CDM.EXTENSION_PROPOSED],
        )
        self.db = db_pool

    async def run(self):
        logger.info("CDM Governance Consumer started")
        while True:
            message = self.consumer.poll(timeout=1.0)
            if not message:
                continue
            try:
                await self._store_proposal(message)
                self.consumer.commit(message)   # Commit only after successful write
            except Exception as e:
                logger.error(
                    f"Failed to store CDM proposal: {e}",
                    extra={"tenant_id": message.tenant_id},
                    exc_info=True,
                )
                # Do NOT commit — message will redeliver on restart

    async def _store_proposal(self, message: NexusMessage):
        async with get_tenant_scoped_connection(self.db, message.tenant_id) as conn:
            await conn.execute("""
                INSERT INTO nexus_system.governance_queue
                    (proposal_type, tenant_id, status, payload)
                VALUES ($1, $2, 'pending', $3)
            """,
                "cdm_extension",
                message.tenant_id,
                message.payload,    # asyncpg serialises dict → JSONB
            )
        logger.info(
            "CDM proposal stored",
            extra={"tenant_id": message.tenant_id, "source_system": message.source_system}
        )
```

### Step 3 — Pydantic Models

```python
# m4/models/governance.py

from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime
import uuid

class ProposalResponse(BaseModel):
    proposal_id: uuid.UUID
    proposal_type: str
    status: str
    payload: dict
    submitted_at: datetime
    reviewed_at: Optional[datetime] = None
    reviewed_by: Optional[str] = None
    review_notes: Optional[str] = None

class ProposalListResponse(BaseModel):
    proposals: List[ProposalResponse]
    total: int

class ApproveProposalBody(BaseModel):
    reviewed_by: str        # nexus_user_id — populated from X-User-ID header in M6
    notes: Optional[str] = ""

class RejectProposalBody(BaseModel):
    reviewed_by: str
    reason: str             # Required on rejection — must explain why
```

### Step 4 — FastAPI Endpoints

```python
# m4/api/governance.py

from fastapi import APIRouter, HTTPException, Header, Depends
from nexus_core.messaging import NexusProducer, NexusMessage
from nexus_core.topics import CrossModuleTopicNamer
from nexus_core.db import get_tenant_scoped_connection
from m4.models.governance import (
    ProposalListResponse, ProposalResponse,
    ApproveProposalBody, RejectProposalBody
)

router = APIRouter(prefix="/api/v1/governance", tags=["CDM Governance"])


@router.get("/proposals", response_model=ProposalListResponse)
async def list_proposals(
    x_tenant_id: str = Header(...),    # Kong injects this — never decoded by service
    x_user_id: str = Header(...),      # Kong injects this from Okta sub claim
    status: str = "pending",
):
    """
    List CDM extension proposals for this tenant.
    Supports filtering by status: pending | approved | rejected
    """
    async with get_tenant_scoped_connection(db_pool, x_tenant_id) as conn:
        rows = await conn.fetch("""
            SELECT proposal_id, proposal_type, status, payload,
                   submitted_at, reviewed_at, reviewed_by, review_notes
            FROM nexus_system.governance_queue
            WHERE tenant_id = $1 AND status = $2
            ORDER BY submitted_at DESC
            LIMIT 100
        """, x_tenant_id, status)
    proposals = [ProposalResponse(**dict(r)) for r in rows]
    return ProposalListResponse(proposals=proposals, total=len(proposals))


@router.get("/proposals/{proposal_id}", response_model=ProposalResponse)
async def get_proposal(
    proposal_id: str,
    x_tenant_id: str = Header(...),
    x_user_id: str = Header(...),
):
    """Get a single CDM proposal by ID."""
    async with get_tenant_scoped_connection(db_pool, x_tenant_id) as conn:
        row = await conn.fetchrow("""
            SELECT * FROM nexus_system.governance_queue
            WHERE proposal_id = $1 AND tenant_id = $2
        """, proposal_id, x_tenant_id)
    if not row:
        raise HTTPException(404, "Proposal not found")
    return ProposalResponse(**dict(row))


@router.post("/proposals/{proposal_id}/approve")
async def approve_proposal(
    proposal_id: str,
    body: ApproveProposalBody,
    x_tenant_id: str = Header(...),
    x_user_id: str = Header(...),
):
    """
    Approve a CDM extension proposal.

    On approval:
    1. Bumps CDM version (minor increment: 1.0 → 1.1)
    2. Marks old version as deprecated
    3. Inserts approved field mappings into cdm_mappings (confidence >= 0.80 only)
    4. Marks proposal as approved with reviewer identity
    5. Publishes nexus.cdm.version_published — M1 CDM Mapper invalidates cache
    """
    async with get_tenant_scoped_connection(db_pool, x_tenant_id) as conn:

        # Load proposal — must be pending and belong to this tenant
        row = await conn.fetchrow("""
            SELECT * FROM nexus_system.governance_queue
            WHERE proposal_id = $1 AND tenant_id = $2 AND status = 'pending'
        """, proposal_id, x_tenant_id)

        if not row:
            raise HTTPException(404, "Proposal not found or already reviewed")

        # Determine new CDM version
        current_version = await conn.fetchval("""
            SELECT version FROM nexus_system.cdm_versions
            WHERE tenant_id = $1 AND status = 'active'
            ORDER BY published_at DESC LIMIT 1
        """, x_tenant_id)

        new_version = _bump_minor_version(current_version or "1.0")

        # Deprecate current active version
        await conn.execute("""
            UPDATE nexus_system.cdm_versions
            SET status = 'deprecated', deprecated_at = NOW()
            WHERE tenant_id = $1 AND status = 'active'
        """, x_tenant_id)

        # Insert new active version
        await conn.execute("""
            INSERT INTO nexus_system.cdm_versions
                (version, status, tenant_id, changes_summary, published_by, published_at)
            VALUES ($1, 'active', $2, $3, $4, NOW())
        """, new_version, x_tenant_id, body.notes, x_user_id)

        # Insert approved field mappings (confidence >= 0.80 only)
        payload = row["payload"]
        approved_mappings = 0
        for fp in payload.get("field_proposals", []):
            if fp.get("confidence", 0) >= 0.80:
                await conn.execute("""
                    INSERT INTO nexus_system.cdm_mappings
                        (tenant_id, cdm_version, source_system, source_table, source_field,
                         cdm_entity, cdm_field, confidence, tier, approved_by, approved_at)
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,1,$9,NOW())
                    ON CONFLICT (tenant_id, cdm_version, source_system, source_table, source_field)
                    DO NOTHING
                """,
                    x_tenant_id, new_version,
                    payload.get("source_system", ""),
                    fp["source_table"], fp["source_field"],
                    fp["cdm_entity"], fp["cdm_field"],
                    fp["confidence"], x_user_id,
                )
                approved_mappings += 1

        # Mark proposal approved — record reviewer identity for audit
        await conn.execute("""
            UPDATE nexus_system.governance_queue
            SET status = 'approved',
                reviewed_at = NOW(),
                reviewed_by = $1,
                review_notes = $2
            WHERE proposal_id = $3
        """, x_user_id, body.notes, proposal_id)

    # Publish version_published AFTER the DB transaction is committed
    # M1 CDM Mapper subscribes to this and will invalidate its cache
    producer.publish(NexusMessage(
        topic=CrossModuleTopicNamer.CDM.VERSION_PUBLISHED,
        tenant_id=x_tenant_id,
        source_system="nexus_m4",
        source_record_id=proposal_id,
        permission_scope={},
        entity_type="cdm.version",
        payload={
            "cdm_version_new": new_version,
            "cdm_version_previous": current_version,
            "approved_mappings_count": approved_mappings,
            "change_summary": body.notes,
            "approved_by": x_user_id,
        },
    ))

    logger.info(
        "CDM proposal approved",
        extra={
            "tenant_id": x_tenant_id,
            "proposal_id": proposal_id,
            "new_version": new_version,
            "approved_by": x_user_id,
            "mappings_activated": approved_mappings,
        }
    )
    return {
        "new_cdm_version": new_version,
        "status": "approved",
        "mappings_activated": approved_mappings,
    }


@router.post("/proposals/{proposal_id}/reject")
async def reject_proposal(
    proposal_id: str,
    body: RejectProposalBody,
    x_tenant_id: str = Header(...),
    x_user_id: str = Header(...),
):
    """
    Reject a CDM extension proposal.
    Publishes nexus.cdm.extension_rejected so M2 can log and not re-propose
    the same schema immediately.
    """
    async with get_tenant_scoped_connection(db_pool, x_tenant_id) as conn:
        result = await conn.execute("""
            UPDATE nexus_system.governance_queue
            SET status = 'rejected',
                reviewed_at = NOW(),
                reviewed_by = $1,
                review_notes = $2
            WHERE proposal_id = $3 AND tenant_id = $4 AND status = 'pending'
            RETURNING proposal_id
        """, x_user_id, body.reason, proposal_id, x_tenant_id)

        if not result:
            raise HTTPException(404, "Proposal not found or already reviewed")

    producer.publish(NexusMessage(
        topic=CrossModuleTopicNamer.CDM.EXTENSION_REJECTED,
        tenant_id=x_tenant_id,
        source_system="nexus_m4",
        source_record_id=proposal_id,
        permission_scope={},
        entity_type="cdm.rejection",
        payload={
            "proposal_id": proposal_id,
            "rejection_reason": body.reason,
            "rejected_by": x_user_id,
        },
    ))

    logger.info(
        "CDM proposal rejected",
        extra={
            "tenant_id": x_tenant_id,
            "proposal_id": proposal_id,
            "rejected_by": x_user_id,
        }
    )
    return {"status": "rejected"}


def _bump_minor_version(version: str) -> str:
    """1.0 → 1.1 · 1.9 → 1.10 · Never touches major version."""
    major, minor = version.split(".")
    return f"{major}.{int(minor) + 1}"
```

### Step 5 — FastAPI App Factory

```python
# m4/api/main.py

from fastapi import FastAPI
from m4.api.governance import router as governance_router

def create_app() -> FastAPI:
    app = FastAPI(
        title="NEXUS M4 Governance API",
        version="1.0.0",
        description="CDM proposal and mapping exception governance for NEXUS platform",
    )
    app.include_router(governance_router)

    @app.get("/health")
    async def health():
        return {"status": "ok", "service": "m4-governance-api"}

    return app

app = create_app()
```

### Step 6 — Kubernetes Deployment

```yaml
# k8s/m4-governance-api.yaml

apiVersion: apps/v1
kind: Deployment
metadata:
  name: m4-governance-api
  namespace: nexus-app
spec:
  replicas: 1
  selector:
    matchLabels:
      app: m4-governance-api
  template:
    metadata:
      labels:
        app: m4-governance-api
    spec:
      containers:
        - name: governance-api
          image: nexus/m4-governance-api:latest
          command: ["uvicorn", "m4.api.main:app", "--host", "0.0.0.0", "--port", "8002"]
          ports:
            - containerPort: 8002
          env:
            - name: KAFKA_BOOTSTRAP
              valueFrom:
                secretKeyRef:
                  name: nexus-kafka-credentials
                  key: bootstrap_servers
            - name: POSTGRES_DSN
              valueFrom:
                secretKeyRef:
                  name: nexus-postgres-credentials
                  key: dsn
          livenessProbe:
            httpGet:
              path: /health
              port: 8002
            initialDelaySeconds: 10
            periodSeconds: 30
---
apiVersion: v1
kind: Service
metadata:
  name: m4-governance-api
  namespace: nexus-app
spec:
  selector:
    app: m4-governance-api
  ports:
    - port: 8002
      targetPort: 8002
```

### Step 7 — Kong Route Registration

```yaml
# kong/routes/m4-governance.yaml

services:
  - name: m4-governance
    url: http://m4-governance-api.nexus-app.svc.cluster.local:8002
    routes:
      - name: governance-proposals
        paths: [/api/v1/governance]
        methods: [GET, POST]
    plugins:
      - name: jwt           # Kong validates Okta JWT, injects X-Tenant-ID + X-User-ID
      - name: prometheus     # Emit request metrics with tenant_id label
```

---

## API Contract Summary

| Method | Path | Description | Publishes to Kafka |
|---|---|---|---|
| `GET` | `/api/v1/governance/proposals` | List proposals (filter by status) | — |
| `GET` | `/api/v1/governance/proposals/{id}` | Get single proposal | — |
| `POST` | `/api/v1/governance/proposals/{id}/approve` | Approve + activate CDM version | `nexus.cdm.version_published` |
| `POST` | `/api/v1/governance/proposals/{id}/reject` | Reject proposal | `nexus.cdm.extension_rejected` |

All endpoints require:
- `X-Tenant-ID` header (injected by Kong from Okta JWT `tenant_id` claim)
- `X-User-ID` header (injected by Kong from Okta JWT `sub` claim)
- Services never decode JWTs directly — Kong owns that

---

## Acceptance Test Sequence

Run this sequence in order on Day 4 to verify the complete flow.

```bash
# ── 1. Publish a test CDM proposal manually (simulates M2 Structural Agent) ──

python3 -c "
from nexus_core.messaging import NexusProducer, NexusMessage
from nexus_core.topics import CrossModuleTopicNamer

p = NexusProducer('nexus-kafka-kafka-bootstrap.nexus-data.svc.cluster.local:9092')
p.publish(NexusMessage(
    topic=CrossModuleTopicNamer.CDM.EXTENSION_PROPOSED,
    tenant_id='test-tenant',
    source_system='odoo',
    source_record_id='schema-001',
    permission_scope={},
    entity_type='cdm.proposal',
    payload={
        'cdm_version_from': '1.0',
        'source_system': 'odoo',
        'field_proposals': [
            {
                'source_table': 'res.partner',
                'source_field': 'name',
                'cdm_entity': 'crm.party',
                'cdm_field': 'display_name',
                'confidence': 0.97,
            },
            {
                'source_table': 'res.partner',
                'source_field': 'vat',
                'cdm_entity': 'crm.party',
                'cdm_field': 'tax_id',
                'confidence': 0.62,    # Below 0.80 — should NOT be auto-activated on approval
            }
        ],
    }
))
print('Proposal published')
"

# Expected: Consumer logs show "CDM proposal stored"

# ── 2. Verify proposal is in governance_queue ──────────────────────────────────

psql $POSTGRES_DSN -c "
    SELECT proposal_id, status, submitted_at
    FROM nexus_system.governance_queue
    WHERE tenant_id = 'test-tenant';
"
# Expected: 1 row, status = 'pending'

# ── 3. List proposals via API ──────────────────────────────────────────────────

curl -s https://api.nexus.internal/api/v1/governance/proposals \
  -H "Authorization: Bearer $TEST_JWT" | jq .
# Expected: {"proposals": [...], "total": 1}

# ── 4. Approve the proposal ────────────────────────────────────────────────────

PROPOSAL_ID=$(psql $POSTGRES_DSN -t -c \
  "SELECT proposal_id FROM nexus_system.governance_queue WHERE tenant_id='test-tenant' LIMIT 1;")

curl -s -X POST \
  https://api.nexus.internal/api/v1/governance/proposals/$PROPOSAL_ID/approve \
  -H "Authorization: Bearer $TEST_JWT" \
  -H "Content-Type: application/json" \
  -d '{"reviewed_by": "data-steward-1", "notes": "Confirmed correct mapping"}' | jq .
# Expected: {"new_cdm_version": "1.1", "status": "approved", "mappings_activated": 1}
# Note: only 1 mapping activated — the 0.62 confidence field is excluded

# ── 5. Verify CDM version bumped ───────────────────────────────────────────────

psql $POSTGRES_DSN -c "
    SELECT version, status, published_by, published_at
    FROM nexus_system.cdm_versions
    WHERE tenant_id = 'test-tenant'
    ORDER BY published_at DESC;
"
# Expected: version='1.1' status='active', version='1.0' status='deprecated'

# ── 6. Verify only high-confidence mapping was activated ──────────────────────

psql $POSTGRES_DSN -c "
    SELECT source_field, cdm_field, confidence, tier
    FROM nexus_system.cdm_mappings
    WHERE tenant_id = 'test-tenant';
"
# Expected: 1 row — res.partner/name → crm.party/display_name (confidence 0.97)
# NOT present: res.partner/vat (confidence 0.62 — below threshold)

# ── 7. Verify nexus.cdm.version_published on Kafka ────────────────────────────

# Check Kafka UI at kafka-ui.nexus.internal
# Topic: nexus.cdm.version_published
# Expected: 1 message, payload contains cdm_version_new='1.1'

# ── 8. Test rejection flow ─────────────────────────────────────────────────────

# Publish another proposal
python3 -c "..." # Same as step 1

PROPOSAL_ID_2=$(psql $POSTGRES_DSN -t -c \
  "SELECT proposal_id FROM nexus_system.governance_queue WHERE status='pending' LIMIT 1;")

curl -s -X POST \
  https://api.nexus.internal/api/v1/governance/proposals/$PROPOSAL_ID_2/reject \
  -H "Authorization: Bearer $TEST_JWT" \
  -H "Content-Type: application/json" \
  -d '{"reviewed_by": "data-steward-1", "reason": "Incorrect mapping — vat is not tax_id in our CDM"}' | jq .
# Expected: {"status": "rejected"}

# Verify nexus.cdm.extension_rejected published on Kafka
# Verify CDM version is still 1.1 (no bump on rejection)
# Verify governance_queue row has status='rejected'

# ── 9. Multi-tenant isolation test ────────────────────────────────────────────

# Login as a test-beta user
curl -s https://api.nexus.internal/api/v1/governance/proposals \
  -H "Authorization: Bearer $TEST_BETA_JWT" | jq .
# Expected: {"proposals": [], "total": 0}
# test-beta must see zero proposals from test-tenant — RLS enforces this
```

---

## Acceptance Criteria

| # | Test | Expected Result |
|---|---|---|
| 1 | CDM proposal published to Kafka | Appears in `governance_queue` within 5 seconds |
| 2 | GET /proposals | Returns pending proposals for correct tenant only |
| 3 | Approve proposal | CDM version bumps 1.0 → 1.1, `nexus.cdm.version_published` published |
| 4 | Approve with mixed confidence | Only fields with confidence ≥ 0.80 inserted into `cdm_mappings` |
| 5 | Approve already-approved proposal | Returns HTTP 404 |
| 6 | Reject proposal | Status → rejected, `nexus.cdm.extension_rejected` published, no version bump |
| 7 | `reviewed_by` field | Contains `x_user_id` from header (not hardcoded) |
| 8 | Cross-tenant query | test-beta JWT cannot see test-tenant proposals |
| 9 | `/health` endpoint | Returns `{"status": "ok"}` without authentication |
| 10 | Consumer crash mid-batch | Offset not committed, proposal reprocessed on restart, no duplicate in DB |

---

## What Downstream Tasks Depend On

Once P6-M4-01 is complete and passing:

- **P6-M4-02 (Mapping Exception Queue)** — reuses the same `governance_queue` table with `proposal_type='mapping_exception'`, the same FastAPI app, and the same deployment
- **P7-M6-02 (CDM Governance Console)** — the Next.js UI reads from `GET /api/v1/governance/proposals` and calls approve/reject endpoints
- **M1 CDM Mapper** — subscribes to `nexus.cdm.version_published` to invalidate its mapping cache

---

*NEXUS M4 CDM Governance Queue Task Plan · Mentis Consulting · February 2026 · Confidential*