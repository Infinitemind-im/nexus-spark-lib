# NEXUS — Task Plan: Temporal Workflow State Machine
**Task ID: P6-M4-03 · Owner: Product Team (Full-Stack) · Days 5–6**
**Mentis Consulting · February 2026 · Confidential**

---

## What This Task Is and Why It Exists

The Temporal Workflow State Machine is the execution engine of M4. The two previous tasks (CDM Governance Queue, Mapping Exception Queue) handle human-in-the-loop decisions about data schema. This task handles human-in-the-loop decisions about business processes — and the automated steps that surround them.

In Iteration 1, the scope is deliberately narrow: deploy Temporal, implement the canonical state machine, build **one real workflow** (OnboardingWorkflow) to prove the full execution path, and build the Kafka consumer that triggers workflows from M2 signals. The full suite of 10 pre-defined workflows (Finance + HR) is Iteration 2.

### Why Temporal, Not a Custom State Machine

A workflow like employee onboarding involves 25+ steps across multiple systems that can take hours or days. Custom state machines written in application code fail in predictable ways:

- A pod restart mid-workflow loses all in-memory state
- A step that fails needs its own retry logic in every activity
- Waiting for a human approval requires polling loops or complex callback mechanisms
- Audit trails have to be hand-coded alongside every state transition

Temporal solves all of these at the infrastructure level. Workflows are durable — a Temporal workflow survives pod restarts, database reconnections, and infrastructure failures transparently. Activities are independently retryable with configurable backoff. Waiting for human signals is a first-class primitive (`workflow.wait_condition`). The complete execution history is stored automatically.

### Iteration 1 Scope vs Iteration 2

| Concern | Iteration 1 | Iteration 2 |
|---|---|---|
| Temporal deployment | ✅ Real | — |
| State machine definition | ✅ Real | — |
| OnboardingWorkflow | ✅ Real (stub activities) | Activities call real systems |
| Finance workflows (5) | ❌ Deferred | ✅ |
| HR workflows (4 remaining) | ❌ Deferred | ✅ |
| WorkflowTriggerConsumer | ✅ Real | Extended for new workflow types |
| Workflow REST API for M6 | ✅ Real | Extended |
| OPA policy engine | ❌ Deferred | ✅ |
| Notification service (SendGrid) | Stub only | ✅ Real |

---

## The State Machine

Every workflow in NEXUS follows the same canonical state machine. Temporal tracks this internally, but NEXUS also mirrors it in PostgreSQL for M6 to query without hitting Temporal directly.

```
                        ┌─────────────┐
                        │   PENDING   │ ← Workflow created, not yet started
                        └──────┬──────┘
                               │ Worker picks up task
                               ▼
                        ┌─────────────┐
              ┌────────▶│ IN_PROGRESS │ ← Activities executing
              │         └──────┬──────┘
              │                │
              │         ┌──────┴──────────────────┐
              │         │                          │
              │         ▼                          ▼
              │  ┌─────────────────┐        ┌──────────┐
              │  │AWAITING_APPROVAL│        │  FAILED  │ ← Activity exhausted retries
              │  └────────┬────────┘        └──────────┘
              │           │
              │    ┌──────┴──────┐
              │    │             │
              │    ▼             ▼
              │  ┌──────────┐  ┌──────────┐
              │  │ APPROVED │  │ REJECTED │
              │  └────┬─────┘  └──────────┘
              │       │ Resume execution
              └───────┘
                       │ All activities complete
                       ▼
                  ┌───────────┐
                  │ COMPLETED │
                  └───────────┘

              ┌───────────┐
              │ CANCELLED │ ← Can be triggered from PENDING or IN_PROGRESS
              └───────────┘
```

### State Transition Rules

| From | To | Trigger | Who |
|---|---|---|---|
| `PENDING` | `IN_PROGRESS` | Temporal worker picks up task | Temporal (automatic) |
| `IN_PROGRESS` | `AWAITING_APPROVAL` | Workflow reaches a human approval step | Workflow code |
| `IN_PROGRESS` | `FAILED` | Activity exhausts all retry attempts | Temporal (automatic) |
| `IN_PROGRESS` | `CANCELLED` | External cancellation signal | API caller |
| `PENDING` | `CANCELLED` | External cancellation before pickup | API caller |
| `AWAITING_APPROVAL` | `APPROVED` | Human approves via M6 UI | API caller |
| `AWAITING_APPROVAL` | `REJECTED` | Human rejects via M6 UI | API caller |
| `APPROVED` | `IN_PROGRESS` | Workflow resumes after approval | Temporal (signal) |
| `IN_PROGRESS` | `COMPLETED` | All activities complete successfully | Temporal (automatic) |

**Each transition is logged** with: timestamp, actor (Temporal system or nexus_user_id), and reason. This is the audit trail required for compliance.

---

## Scope

**In scope for Days 5–6:**
- Temporal deployment via Helm into `nexus-data` namespace
- `nexus_system.workflow_runs` tracking table + audit log
- `WorkflowTriggerConsumer` — Kafka consumer on `{tid}.m2.workflow_trigger`
- `OnboardingWorkflow` — one real workflow with stub activities proving the full state machine
- `NexusTemporalWorker` — worker process running the workflow + activities
- FastAPI endpoint: `GET /api/v1/workflows` — M6 Workflow Manager reads this
- FastAPI endpoint: `POST /api/v1/workflows/{id}/signal` — send approval/rejection signals
- Kong route registration for workflow endpoints

**Not in scope:**
- The remaining 9 workflows (Iteration 2)
- OPA policy enforcement on workflow steps (Iteration 2)
- SendGrid real email sending (stub in Iteration 1)
- Real IT/HR/facilities system integration in activities (stubs in Iteration 1)

---

## Dependencies

| Dependency | Owner | Must be done before |
|---|---|---|
| `nexus_core` library installed | Tech Lead | Week 1 |
| PostgreSQL `nexus_system` schema | Tech Lead | Week 1 DDL |
| `{tid}.m2.workflow_trigger` Kafka topic exists | Platform Team | Phase 0 |
| `{tid}.m4.workflow_completed` Kafka topic exists | Platform Team | Phase 0 |
| Kong JWT plugin active | Platform Team | Phase 0 |
| P6-M4-01 (CDM Governance Queue) complete | Product Team | Day 4 — shares FastAPI app |
| P6-M4-02 (Mapping Exception Queue) complete | Product Team | Day 6 — same deployment |

---

## Data Flow

```
M2 Executive RHMA Agent — detects business event needing a workflow
    ↓  publishes
{tid}.m2.workflow_trigger  (Kafka)
    ↓  consumed by
WorkflowTriggerConsumer  (this task)
    ↓  creates row in
nexus_system.workflow_runs  (status=PENDING)
    ↓  starts Temporal workflow via client
Temporal Server  (stores durable execution state)
    ↓  dispatches to
NexusTemporalWorker  (executes activities)
    ↓  on human approval step:
workflow_runs status → AWAITING_APPROVAL
    ↓  M6 shows pending approval in Workflow Manager
Data steward approves via:
POST /api/v1/workflows/{id}/signal
    ↓  NEXUS sends signal to Temporal
Temporal resumes workflow  (status → IN_PROGRESS → COMPLETED)
    ↓
workflow_runs status → COMPLETED
    ↓  WorkflowTriggerConsumer publishes
{tid}.m4.workflow_completed  (Kafka)
```

---

## Database Schema

### workflow_runs table

Temporal stores its own internal execution state. NEXUS mirrors the workflow lifecycle in PostgreSQL so M6 can query workflow status without needing a Temporal SDK client.

```sql
CREATE TABLE nexus_system.workflow_runs (
    run_id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    temporal_workflow_id TEXT NOT NULL UNIQUE,   -- Temporal's own workflow ID
    temporal_run_id     TEXT,                    -- Temporal's run ID (changes on retry)
    workflow_type       TEXT NOT NULL,           -- 'employee_onboarding' | 'invoice_processing' | ...
    tenant_id           TEXT NOT NULL,
    status              TEXT NOT NULL DEFAULT 'pending',
    triggered_by        TEXT NOT NULL,           -- nexus_user_id or 'system'
    context             JSONB NOT NULL,          -- Input payload (employee data, invoice ref, etc.)
    result              JSONB,                   -- Final output when COMPLETED
    error_message       TEXT,                    -- Populated on FAILED
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    started_at          TIMESTAMPTZ,             -- When Temporal picked it up
    awaiting_since      TIMESTAMPTZ,             -- When it entered AWAITING_APPROVAL
    completed_at        TIMESTAMPTZ,

    CONSTRAINT valid_status CHECK (
        status IN ('pending', 'in_progress', 'awaiting_approval',
                   'approved', 'rejected', 'completed', 'failed', 'cancelled')
    )
);

-- RLS: tenant sees only their own workflows
ALTER TABLE nexus_system.workflow_runs ENABLE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON nexus_system.workflow_runs
    USING (tenant_id = current_setting('app.current_tenant'));

-- Index for M6 Workflow Manager (most common query)
CREATE INDEX idx_workflow_runs_tenant_status
    ON nexus_system.workflow_runs (tenant_id, status, created_at DESC);
```

### workflow_audit_log table

Every state transition is written here. This is the compliance trail.

```sql
CREATE TABLE nexus_system.workflow_audit_log (
    log_id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id          UUID NOT NULL REFERENCES nexus_system.workflow_runs(run_id),
    tenant_id       TEXT NOT NULL,
    from_status     TEXT,           -- NULL for initial PENDING entry
    to_status       TEXT NOT NULL,
    actor           TEXT NOT NULL,  -- nexus_user_id or 'temporal_system'
    reason          TEXT,           -- Optional explanation
    logged_at       TIMESTAMPTZ DEFAULT NOW()
);

-- No RLS needed — audit log is immutable and append-only
-- Access controlled at API layer (only admins in Iteration 2)
CREATE INDEX idx_audit_log_run
    ON nexus_system.workflow_audit_log (run_id, logged_at ASC);
```

---

## Implementation

### Step 1 — Deploy Temporal

```bash
# Add Temporal Helm repo
helm repo add temporalio https://go.temporal.io/helm-charts
helm repo update

# Deploy Temporal using NEXUS PostgreSQL as backend
helm install nexus-temporal temporalio/temporal \
  --namespace nexus-data \
  --set server.config.persistence.defaultStore=postgresql \
  --set server.config.persistence.additionalStores.visibility.driver=sql \
  --set postgresql.enabled=false \
  --set externalPostgresql.host=nexus-postgres.nexus-data.svc.cluster.local \
  --set externalPostgresql.port=5432 \
  --set externalPostgresql.database=nexus \
  --set externalPostgresql.user=nexus_app \
  --set externalPostgresql.existingSecret=nexus-postgres-credentials \
  --set web.enabled=true \
  --set web.service.type=ClusterIP

# Expose Temporal UI via ingress at temporal.nexus.internal
# Verify deployment
kubectl get pods -n nexus-data | grep temporal
# Expected: temporal-frontend, temporal-history, temporal-matching, temporal-worker all Running
```

**Verify Temporal is reachable from nexus-app namespace:**

```bash
kubectl run -it --rm --restart=Never temporal-test \
  --image=temporalio/admin-tools \
  --namespace=nexus-app \
  -- tctl --address nexus-temporal-frontend.nexus-data.svc.cluster.local:7233 namespace list
# Expected: lists default namespace without error
```

**Create NEXUS namespace in Temporal:**

```bash
kubectl exec -it deployment/nexus-temporal-admintools -n nexus-data -- \
  tctl --namespace nexus-workflows namespace register --retention 30
# All NEXUS workflows run in the 'nexus-workflows' namespace
# 30-day retention for workflow history
```

### Step 2 — Project Structure Extension

```
m4/
├── consumers/
│   ├── cdm_governance_consumer.py      ← already exists (M4-01)
│   ├── mapping_exception_consumer.py   ← already exists (M4-02)
│   └── workflow_trigger_consumer.py    ← NEW this task
├── workflows/
│   ├── __init__.py
│   ├── base.py                         ← State transition helpers, audit logger
│   ├── onboarding.py                   ← OnboardingWorkflow + activities
│   └── registry.py                     ← Maps workflow_type string → Workflow class
├── worker/
│   └── nexus_worker.py                 ← Temporal worker process
├── api/
│   ├── governance.py                   ← already exists (M4-01)
│   ├── mapping_exceptions.py           ← already exists (M4-02)
│   └── workflows.py                    ← NEW this task
```

### Step 3 — State Transition Helper

```python
# m4/workflows/base.py

import logging
from nexus_core.db import get_tenant_scoped_connection

logger = logging.getLogger(__name__)

VALID_TRANSITIONS = {
    "pending":            {"in_progress", "cancelled"},
    "in_progress":        {"awaiting_approval", "completed", "failed", "cancelled"},
    "awaiting_approval":  {"approved", "rejected"},
    "approved":           {"in_progress"},
    "in_progress":        {"completed"},   # Terminal
}

async def transition_workflow(
    db_pool,
    run_id: str,
    tenant_id: str,
    to_status: str,
    actor: str,
    reason: str = None,
    extra_fields: dict = None,
):
    """
    Transitions a workflow to a new status.
    Validates the transition is legal, updates workflow_runs,
    and appends to workflow_audit_log.
    Raises ValueError if transition is not permitted.
    """
    async with get_tenant_scoped_connection(db_pool, tenant_id) as conn:
        row = await conn.fetchrow(
            "SELECT status FROM nexus_system.workflow_runs WHERE run_id = $1",
            run_id
        )
        if not row:
            raise ValueError(f"Workflow run {run_id} not found")

        from_status = row["status"]

        # Build UPDATE fields
        update_fields = {"status": to_status}
        if to_status == "in_progress" and from_status == "pending":
            update_fields["started_at"] = "NOW()"
        if to_status == "awaiting_approval":
            update_fields["awaiting_since"] = "NOW()"
        if to_status in ("completed", "failed", "cancelled", "rejected"):
            update_fields["completed_at"] = "NOW()"
        if extra_fields:
            update_fields.update(extra_fields)

        await conn.execute("""
            UPDATE nexus_system.workflow_runs
            SET status = $1, started_at = CASE WHEN $2 THEN NOW() ELSE started_at END,
                awaiting_since = CASE WHEN $3 THEN NOW() ELSE awaiting_since END,
                completed_at = CASE WHEN $4 THEN NOW() ELSE completed_at END
            WHERE run_id = $5 AND tenant_id = $6
        """,
            to_status,
            to_status == "in_progress" and from_status == "pending",
            to_status == "awaiting_approval",
            to_status in ("completed", "failed", "cancelled", "rejected"),
            run_id, tenant_id,
        )

        # Append audit log entry
        await conn.execute("""
            INSERT INTO nexus_system.workflow_audit_log
                (run_id, tenant_id, from_status, to_status, actor, reason)
            VALUES ($1, $2, $3, $4, $5, $6)
        """, run_id, tenant_id, from_status, to_status, actor, reason)

    logger.info(
        "Workflow state transition",
        extra={
            "run_id": run_id,
            "tenant_id": tenant_id,
            "from_status": from_status,
            "to_status": to_status,
            "actor": actor,
        }
    )
```

### Step 4 — OnboardingWorkflow

The first workflow. Activities are stubs in Iteration 1 — they succeed after a short delay. The workflow logic, state machine, retry policies, and signal handling are real.

```python
# m4/workflows/onboarding.py

import asyncio
from datetime import timedelta
from temporalio import workflow, activity
from temporalio.common import RetryPolicy

# ── Activities (stubs in Iteration 1) ────────────────────────────────────────

@activity.defn
async def create_it_account(employee: dict) -> dict:
    """
    Iteration 1: Stub — simulates IT account creation delay.
    Iteration 2: Calls IT provisioning API (Active Directory / Okta provisioning).
    """
    await asyncio.sleep(2)
    return {
        "status": "created",
        "username": f"{employee['first_name'].lower()}.{employee['last_name'].lower()}",
        "email": employee.get("email"),
        "note": "STUB — replace with real IT provisioning in Iteration 2",
    }

@activity.defn
async def create_hr_record(employee: dict) -> dict:
    """
    Iteration 1: Stub — simulates HR system write.
    Iteration 2: Calls Odoo hr.employee API via M4 write connector.
    """
    await asyncio.sleep(1)
    return {
        "status": "created",
        "hr_id": f"HR-{employee.get('employee_id', 'UNKNOWN')}",
        "note": "STUB — replace with real Odoo write in Iteration 2",
    }

@activity.defn
async def assign_equipment(employee: dict) -> dict:
    """
    Iteration 1: Stub — simulates facilities assignment.
    Iteration 2: Calls facilities management system API.
    """
    await asyncio.sleep(1)
    return {
        "status": "assigned",
        "equipment": ["laptop", "access_badge", "desk"],
        "note": "STUB",
    }

@activity.defn
async def send_welcome_email(employee: dict) -> dict:
    """
    Iteration 1: Stub — logs email instead of sending.
    Iteration 2: Calls SendGrid API with onboarding email template.
    """
    await asyncio.sleep(0.5)
    import logging
    logging.getLogger(__name__).info(
        f"STUB: Would send welcome email to {employee.get('email')}"
    )
    return {"status": "sent_stub", "recipient": employee.get("email")}

# ── Workflow ───────────────────────────────────────────────────────────────────

@workflow.defn
class OnboardingWorkflow:
    """
    Employee onboarding workflow.

    State machine:
    PENDING → IN_PROGRESS (activities execute)
            → AWAITING_APPROVAL (manager must confirm before equipment assigned)
            → IN_PROGRESS (resumes after approval)
            → COMPLETED

    Signals:
    - 'manager_approved': resumes workflow after AWAITING_APPROVAL
    - 'manager_rejected': terminates workflow with REJECTED status
    """

    def __init__(self):
        self._approval_received = False
        self._approval_actor = None
        self._rejected = False

    @workflow.signal
    async def manager_approved(self, approved_by: str):
        """Signal sent by M4 API when manager approves in M6 UI."""
        self._approval_received = True
        self._approval_actor = approved_by

    @workflow.signal
    async def manager_rejected(self, rejected_by: str, reason: str):
        """Signal sent by M4 API when manager rejects in M6 UI."""
        self._rejected = True
        self._approval_actor = rejected_by

    @workflow.run
    async def run(self, context: dict) -> dict:
        employee = context.get("employee", {})
        results = {}

        # ── Phase 1: System provisioning (no approval needed) ─────────────────
        results["it_account"] = await workflow.execute_activity(
            create_it_account,
            employee,
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=RetryPolicy(
                maximum_attempts=3,
                initial_interval=timedelta(seconds=5),
                backoff_coefficient=2.0,
            ),
        )

        results["hr_record"] = await workflow.execute_activity(
            create_hr_record,
            employee,
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=RetryPolicy(maximum_attempts=3),
        )

        # ── Phase 2: Await manager approval before equipment assignment ────────
        # In a real deployment, M4 API would notify M6 here via WebSocket
        # The workflow pauses durably — survives pod restarts
        await workflow.wait_condition(
            lambda: self._approval_received or self._rejected,
            timeout=timedelta(days=3),   # Auto-fail if no approval in 3 days
        )

        if self._rejected:
            return {
                "status": "rejected",
                "rejected_by": self._approval_actor,
                "completed_phases": results,
            }

        # ── Phase 3: Post-approval steps ──────────────────────────────────────
        results["equipment"] = await workflow.execute_activity(
            assign_equipment,
            employee,
            start_to_close_timeout=timedelta(minutes=10),
            retry_policy=RetryPolicy(maximum_attempts=3),
        )

        results["welcome_email"] = await workflow.execute_activity(
            send_welcome_email,
            employee,
            start_to_close_timeout=timedelta(minutes=2),
            retry_policy=RetryPolicy(maximum_attempts=5),
        )

        return {
            "status": "completed",
            "approved_by": self._approval_actor,
            "activities": results,
        }
```

### Step 5 — Workflow Registry

```python
# m4/workflows/registry.py

from m4.workflows.onboarding import OnboardingWorkflow, (
    create_it_account, create_hr_record, assign_equipment, send_welcome_email
)

# Maps workflow_type string (from Kafka message) → Temporal Workflow class
WORKFLOW_REGISTRY = {
    "employee_onboarding": OnboardingWorkflow,
    # Iteration 2 additions:
    # "invoice_processing":    InvoiceProcessingWorkflow,
    # "expense_reconciliation": ExpenseReconciliationWorkflow,
    # "month_end_close":       MonthEndCloseWorkflow,
    # "performance_review":    PerformanceReviewWorkflow,
    # "leave_management":      LeaveManagementWorkflow,
}

# All activities that must be registered with the worker
ALL_ACTIVITIES = [
    create_it_account,
    create_hr_record,
    assign_equipment,
    send_welcome_email,
]
```

### Step 6 — Temporal Worker Process

```python
# m4/worker/nexus_worker.py

import asyncio
import logging
from temporalio.client import Client
from temporalio.worker import Worker
from m4.workflows.registry import WORKFLOW_REGISTRY, ALL_ACTIVITIES

logger = logging.getLogger(__name__)

async def run_worker():
    client = await Client.connect(
        "nexus-temporal-frontend.nexus-data.svc.cluster.local:7233",
        namespace="nexus-workflows",
    )

    worker = Worker(
        client,
        task_queue="nexus-workflows",
        workflows=list(WORKFLOW_REGISTRY.values()),
        activities=ALL_ACTIVITIES,
    )

    logger.info("Temporal worker started on task queue: nexus-workflows")
    await worker.run()

if __name__ == "__main__":
    asyncio.run(run_worker())
```

**Worker Kubernetes Deployment** (separate pod from the FastAPI service):

```yaml
# k8s/m4-temporal-worker.yaml

apiVersion: apps/v1
kind: Deployment
metadata:
  name: m4-temporal-worker
  namespace: nexus-app
spec:
  replicas: 2    # 2 replicas for resilience — Temporal distributes work automatically
  selector:
    matchLabels:
      app: m4-temporal-worker
  template:
    metadata:
      labels:
        app: m4-temporal-worker
    spec:
      containers:
        - name: temporal-worker
          image: nexus/m4-governance-api:latest
          command: ["python", "-m", "m4.worker.nexus_worker"]
          env:
            - name: TEMPORAL_HOST
              value: "nexus-temporal-frontend.nexus-data.svc.cluster.local:7233"
            - name: POSTGRES_DSN
              valueFrom:
                secretKeyRef:
                  name: nexus-postgres-credentials
                  key: dsn
```

### Step 7 — Workflow Trigger Consumer

```python
# m4/consumers/workflow_trigger_consumer.py

import asyncio
import uuid
import logging
from temporalio.client import Client
from nexus_core.messaging import NexusConsumer, NexusProducer, NexusMessage
from nexus_core.topics import CrossModuleTopicNamer
from nexus_core.db import get_tenant_scoped_connection
from m4.workflows.base import transition_workflow
from m4.workflows.registry import WORKFLOW_REGISTRY

logger = logging.getLogger(__name__)

class WorkflowTriggerConsumer:
    """
    Consumes {tid}.m2.workflow_trigger.
    For each message: creates a workflow_run row, starts a Temporal workflow,
    monitors completion, and publishes {tid}.m4.workflow_completed.

    IMPORTANT: Uses fire-and-forget pattern for long-running workflows.
    The consumer starts the workflow and commits the Kafka offset immediately.
    A separate monitor coroutine polls Temporal for completion.
    This prevents the consumer from blocking on workflows that take hours/days.
    """

    def __init__(self, db_pool, kafka_bootstrap: str, temporal_host: str):
        self.consumer = NexusConsumer(
            bootstrap_servers=kafka_bootstrap,
            group_id="m4-workflow-triggers",
            topics=[],   # Populated dynamically per active tenant
        )
        self.producer = NexusProducer(kafka_bootstrap)
        self.db = db_pool
        self.temporal_host = temporal_host
        self._active_workflows: dict = {}  # run_id → temporal handle

    async def run(self):
        logger.info("Workflow Trigger Consumer started")
        temporal_client = await Client.connect(
            self.temporal_host, namespace="nexus-workflows"
        )

        # Subscribe to workflow_trigger topics for all active tenants
        await self._subscribe_to_active_tenants()

        # Run consumer loop and workflow monitor concurrently
        await asyncio.gather(
            self._consume_loop(temporal_client),
            self._monitor_completions(),
        )

    async def _consume_loop(self, client: Client):
        while True:
            message = self.consumer.poll(timeout=1.0)
            if not message:
                continue
            try:
                await self._start_workflow(client, message)
                self.consumer.commit(message)  # Commit immediately — fire and forget
            except Exception as e:
                logger.error(
                    f"Failed to start workflow: {e}",
                    extra={"tenant_id": message.tenant_id},
                    exc_info=True,
                )

    async def _start_workflow(self, client: Client, message: NexusMessage):
        payload = message.payload
        workflow_type = payload.get("workflow_type")
        workflow_class = WORKFLOW_REGISTRY.get(workflow_type)

        if not workflow_class:
            logger.warning(
                f"Unknown workflow_type: '{workflow_type}' — no workflow registered",
                extra={"tenant_id": message.tenant_id}
            )
            return

        temporal_workflow_id = f"{workflow_type}-{uuid.uuid4()}"

        # Create tracking row before starting Temporal workflow
        async with get_tenant_scoped_connection(self.db, message.tenant_id) as conn:
            run_id = await conn.fetchval("""
                INSERT INTO nexus_system.workflow_runs
                    (temporal_workflow_id, workflow_type, tenant_id,
                     status, triggered_by, context)
                VALUES ($1, $2, $3, 'pending', $4, $5)
                RETURNING run_id
            """,
                temporal_workflow_id,
                workflow_type,
                message.tenant_id,
                payload.get("triggered_by", "system"),
                payload.get("context", {}),
            )

            # Initial audit log entry
            await conn.execute("""
                INSERT INTO nexus_system.workflow_audit_log
                    (run_id, tenant_id, from_status, to_status, actor, reason)
                VALUES ($1, $2, NULL, 'pending', 'system', 'Workflow triggered by M2 agent')
            """, run_id, message.tenant_id)

        # Start Temporal workflow
        handle = await client.start_workflow(
            workflow_class.run,
            payload.get("context", {}),
            id=temporal_workflow_id,
            task_queue="nexus-workflows",
        )

        # Track for completion monitoring
        self._active_workflows[str(run_id)] = {
            "handle": handle,
            "tenant_id": message.tenant_id,
            "workflow_type": workflow_type,
        }

        logger.info(
            "Temporal workflow started",
            extra={
                "run_id": str(run_id),
                "temporal_workflow_id": temporal_workflow_id,
                "workflow_type": workflow_type,
                "tenant_id": message.tenant_id,
            }
        )

    async def _monitor_completions(self):
        """Polls active Temporal workflows for completion. Runs concurrently."""
        while True:
            await asyncio.sleep(10)   # Check every 10 seconds
            completed = []

            for run_id, info in self._active_workflows.items():
                try:
                    result = await asyncio.wait_for(
                        info["handle"].result(), timeout=0.5
                    )
                    await self._on_workflow_completed(run_id, info, result)
                    completed.append(run_id)
                except asyncio.TimeoutError:
                    pass  # Still running — check next cycle
                except Exception as e:
                    await self._on_workflow_failed(run_id, info, str(e))
                    completed.append(run_id)

            for run_id in completed:
                del self._active_workflows[run_id]

    async def _on_workflow_completed(self, run_id: str, info: dict, result: dict):
        status = "completed" if result.get("status") != "rejected" else "rejected"

        async with get_tenant_scoped_connection(self.db, info["tenant_id"]) as conn:
            await conn.execute("""
                UPDATE nexus_system.workflow_runs
                SET status = $1, result = $2, completed_at = NOW()
                WHERE run_id = $3
            """, status, result, run_id)

            await conn.execute("""
                INSERT INTO nexus_system.workflow_audit_log
                    (run_id, tenant_id, from_status, to_status, actor, reason)
                VALUES ($1, $2, 'in_progress', $3, 'temporal_system', NULL)
            """, run_id, info["tenant_id"], status)

        self.producer.publish(NexusMessage(
            topic=CrossModuleTopicNamer.m4(info["tenant_id"], "workflow_completed"),
            tenant_id=info["tenant_id"],
            source_system="nexus_m4",
            source_record_id=run_id,
            permission_scope={},
            entity_type="workflow.run",
            payload={
                "run_id": run_id,
                "workflow_type": info["workflow_type"],
                "status": status,
                "result": result,
            },
        ))

    async def _on_workflow_failed(self, run_id: str, info: dict, error: str):
        async with get_tenant_scoped_connection(self.db, info["tenant_id"]) as conn:
            await conn.execute("""
                UPDATE nexus_system.workflow_runs
                SET status = 'failed', error_message = $1, completed_at = NOW()
                WHERE run_id = $2
            """, error, run_id)

    async def _subscribe_to_active_tenants(self):
        """Subscribe to {tid}.m2.workflow_trigger for all active tenants."""
        async with get_tenant_scoped_connection(self.db, "__system__") as conn:
            rows = await conn.fetch(
                "SELECT tenant_id FROM nexus_system.tenants WHERE status = 'active'"
            )
        topics = [
            CrossModuleTopicNamer.m2(r["tenant_id"], "workflow_trigger")
            for r in rows
        ]
        self.consumer._consumer.subscribe(topics)
        logger.info(f"Subscribed to workflow_trigger for {len(topics)} tenants")
```

### Step 8 — FastAPI Workflow Endpoints

```python
# m4/api/workflows.py

from fastapi import APIRouter, HTTPException, Header
from pydantic import BaseModel
from typing import Optional
from temporalio.client import Client
from nexus_core.db import get_tenant_scoped_connection
from m4.workflows.base import transition_workflow
import logging

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/v1/workflows", tags=["Workflows"])


class WorkflowSignalBody(BaseModel):
    signal:    str           # 'manager_approved' | 'manager_rejected'
    reason:    Optional[str] = None


@router.get("")
async def list_workflows(
    x_tenant_id: str = Header(...),
    x_user_id:   str = Header(...),
    status:      str = None,     # Optional filter
    limit:       int = 50,
):
    """
    List workflow runs for this tenant.
    Used by M6 Workflow Manager page.
    Queries workflow_runs table directly — no Temporal SDK needed.
    """
    async with get_tenant_scoped_connection(db_pool, x_tenant_id) as conn:
        query = """
            SELECT run_id, temporal_workflow_id, workflow_type, status,
                   triggered_by, created_at, started_at,
                   awaiting_since, completed_at, error_message
            FROM nexus_system.workflow_runs
            WHERE tenant_id = $1
        """
        params = [x_tenant_id]
        if status:
            query += " AND status = $2"
            params.append(status)
        query += f" ORDER BY created_at DESC LIMIT {min(limit, 200)}"

        rows = await conn.fetch(query, *params)

    return {"workflows": [dict(r) for r in rows], "total": len(rows)}


@router.get("/{run_id}")
async def get_workflow(
    run_id:      str,
    x_tenant_id: str = Header(...),
    x_user_id:   str = Header(...),
):
    """Get a single workflow run with full audit trail."""
    async with get_tenant_scoped_connection(db_pool, x_tenant_id) as conn:
        row = await conn.fetchrow("""
            SELECT * FROM nexus_system.workflow_runs
            WHERE run_id = $1 AND tenant_id = $2
        """, run_id, x_tenant_id)

        if not row:
            raise HTTPException(404, "Workflow run not found")

        audit = await conn.fetch("""
            SELECT from_status, to_status, actor, reason, logged_at
            FROM nexus_system.workflow_audit_log
            WHERE run_id = $1
            ORDER BY logged_at ASC
        """, run_id)

    return {
        **dict(row),
        "audit_trail": [dict(a) for a in audit],
    }


@router.post("/{run_id}/signal")
async def signal_workflow(
    run_id:      str,
    body:        WorkflowSignalBody,
    x_tenant_id: str = Header(...),
    x_user_id:   str = Header(...),
):
    """
    Send a signal to a running Temporal workflow.
    Used by M6 when a human approves or rejects a workflow step.

    Supported signals:
    - manager_approved: resumes workflow after AWAITING_APPROVAL
    - manager_rejected: terminates workflow with REJECTED status
    """
    async with get_tenant_scoped_connection(db_pool, x_tenant_id) as conn:
        row = await conn.fetchrow("""
            SELECT temporal_workflow_id, status, workflow_type
            FROM nexus_system.workflow_runs
            WHERE run_id = $1 AND tenant_id = $2
        """, run_id, x_tenant_id)

    if not row:
        raise HTTPException(404, "Workflow run not found")

    if row["status"] != "awaiting_approval":
        raise HTTPException(
            409,
            f"Cannot signal workflow in status '{row['status']}'. "
            "Only workflows in 'awaiting_approval' status accept signals."
        )

    if body.signal not in ("manager_approved", "manager_rejected"):
        raise HTTPException(422, f"Unknown signal: '{body.signal}'")

    # Send signal to Temporal
    temporal_client = await Client.connect(
        "nexus-temporal-frontend.nexus-data.svc.cluster.local:7233",
        namespace="nexus-workflows",
    )
    handle = temporal_client.get_workflow_handle(row["temporal_workflow_id"])

    if body.signal == "manager_approved":
        await handle.signal("manager_approved", x_user_id)
        new_status = "approved"
    else:
        await handle.signal("manager_rejected", x_user_id, body.reason or "")
        new_status = "rejected"

    # Update tracking table
    await transition_workflow(
        db_pool, run_id, x_tenant_id,
        to_status=new_status,
        actor=x_user_id,
        reason=body.reason,
    )

    logger.info(
        "Workflow signal sent",
        extra={
            "run_id": run_id,
            "signal": body.signal,
            "actor": x_user_id,
            "tenant_id": x_tenant_id,
        }
    )
    return {"status": new_status, "signal_sent": body.signal}
```

### Step 9 — Register Router + Consumer in Entrypoint

```python
# m4/api/main.py  (extend from M4-01 and M4-02)

from fastapi import FastAPI
from m4.api.governance import router as governance_router
from m4.api.mapping_exceptions import router as mapping_router
from m4.api.workflows import router as workflow_router   # NEW

def create_app() -> FastAPI:
    app = FastAPI(title="NEXUS M4 Governance API", version="1.0.0")
    app.include_router(governance_router)
    app.include_router(mapping_router)
    app.include_router(workflow_router)   # NEW

    @app.get("/health")
    async def health():
        return {"status": "ok", "service": "m4-governance-api"}

    return app
```

```python
# m4/entrypoint.py  (extend from M4-02)

import asyncio
from m4.consumers.cdm_governance_consumer import CDMGovernanceConsumer
from m4.consumers.mapping_exception_consumer import MappingExceptionConsumer
from m4.consumers.workflow_trigger_consumer import WorkflowTriggerConsumer   # NEW

async def main():
    await asyncio.gather(
        CDMGovernanceConsumer(db_pool, KAFKA_BOOTSTRAP).run(),
        MappingExceptionConsumer(db_pool, KAFKA_BOOTSTRAP).run(),
        WorkflowTriggerConsumer(             # NEW
            db_pool, KAFKA_BOOTSTRAP, TEMPORAL_HOST
        ).run(),
    )
```

### Step 10 — Kong Route Addition

```yaml
# kong/routes/m4-governance.yaml  (extend from M4-01 and M4-02)

services:
  - name: m4-governance
    url: http://m4-governance-api.nexus-app.svc.cluster.local:8002
    routes:
      - name: governance-proposals
        paths: [/api/v1/governance]
        methods: [GET, POST]
      - name: mapping-exceptions
        paths: [/api/v1/mappings]
        methods: [GET, POST]
      - name: workflows                    # NEW
        paths: [/api/v1/workflows]
        methods: [GET, POST]
    plugins:
      - name: jwt
      - name: prometheus
```

---

## API Contract Summary

| Method | Path | Description | Temporal interaction |
|---|---|---|---|
| `GET` | `/api/v1/workflows` | List workflow runs (filter by status) | None — queries PostgreSQL |
| `GET` | `/api/v1/workflows/{id}` | Get run + full audit trail | None — queries PostgreSQL |
| `POST` | `/api/v1/workflows/{id}/signal` | Send approval/rejection signal | `handle.signal()` → Temporal |

---

## Acceptance Test Sequence

```bash
# ── 1. Verify Temporal is running ─────────────────────────────────────────────

curl -s http://temporal.nexus.internal  # Temporal UI should load
kubectl get pods -n nexus-data | grep temporal
# Expected: all temporal pods Running

# ── 2. Verify worker is running ───────────────────────────────────────────────

kubectl get pods -n nexus-app | grep m4-temporal-worker
# Expected: 2 pods Running

kubectl logs deployment/m4-temporal-worker -n nexus-app | grep "worker started"
# Expected: "Temporal worker started on task queue: nexus-workflows"

# ── 3. Trigger an OnboardingWorkflow via Kafka ────────────────────────────────

python3 -c "
from nexus_core.messaging import NexusProducer, NexusMessage
from nexus_core.topics import CrossModuleTopicNamer

p = NexusProducer('nexus-kafka-kafka-bootstrap.nexus-data.svc.cluster.local:9092')
p.publish(NexusMessage(
    topic=CrossModuleTopicNamer.m2('test-tenant', 'workflow_trigger'),
    tenant_id='test-tenant',
    source_system='nexus_m2',
    source_record_id='trigger-001',
    permission_scope={},
    entity_type='workflow.trigger',
    payload={
        'workflow_type': 'employee_onboarding',
        'triggered_by': 'user-alice',
        'context': {
            'employee': {
                'first_name': 'Alice',
                'last_name':  'Martin',
                'email':      'alice@acme.be',
                'employee_id': 'EMP-001',
            }
        }
    }
))
print('Workflow trigger published')
"

# ── 4. Verify workflow_runs row created ───────────────────────────────────────

psql $POSTGRES_DSN -c "
    SELECT run_id, workflow_type, status, created_at
    FROM nexus_system.workflow_runs
    WHERE tenant_id = 'test-tenant';
"
# Expected: 1 row, status='pending' or 'in_progress'

# ── 5. Verify in Temporal UI ──────────────────────────────────────────────────

# Open temporal.nexus.internal
# Namespace: nexus-workflows
# Expected: OnboardingWorkflow visible, status Running or TimerFired (waiting for approval)

# ── 6. Verify status transitions via API ──────────────────────────────────────

RUN_ID=$(psql $POSTGRES_DSN -t -c \
  "SELECT run_id FROM nexus_system.workflow_runs WHERE tenant_id='test-tenant' LIMIT 1;")

curl -s "https://api.nexus.internal/api/v1/workflows/$RUN_ID" \
  -H "Authorization: Bearer $TEST_JWT" | jq '{status, audit_trail}'
# Expected: status='awaiting_approval', audit_trail shows
# pending → in_progress → awaiting_approval transitions

# ── 7. Send manager approval signal ──────────────────────────────────────────

curl -s -X POST \
  "https://api.nexus.internal/api/v1/workflows/$RUN_ID/signal" \
  -H "Authorization: Bearer $TEST_JWT" \
  -H "Content-Type: application/json" \
  -d '{"signal": "manager_approved"}' | jq .
# Expected: {"status": "approved", "signal_sent": "manager_approved"}

# ── 8. Wait for completion and verify ────────────────────────────────────────

sleep 10  # Allow worker to complete remaining activities

psql $POSTGRES_DSN -c "
    SELECT status, completed_at, result->>'status' as result_status
    FROM nexus_system.workflow_runs
    WHERE run_id = '$RUN_ID'::uuid;
"
# Expected: status='completed', result_status='completed'

# ── 9. Verify full audit trail ────────────────────────────────────────────────

psql $POSTGRES_DSN -c "
    SELECT from_status, to_status, actor, logged_at
    FROM nexus_system.workflow_audit_log
    WHERE run_id = '$RUN_ID'::uuid
    ORDER BY logged_at;
"
# Expected: NULL→pending, pending→in_progress, in_progress→awaiting_approval,
#           awaiting_approval→approved, approved→in_progress (implied), in_progress→completed

# ── 10. Verify {tid}.m4.workflow_completed on Kafka ──────────────────────────

# Kafka UI: topic test-tenant.m4.workflow_completed
# Expected: 1 message, payload.status='completed', payload.workflow_type='employee_onboarding'

# ── 11. Test rejection path ───────────────────────────────────────────────────

# Trigger another workflow (step 3 again)
# Wait for awaiting_approval status
# Send rejection signal:

curl -s -X POST \
  "https://api.nexus.internal/api/v1/workflows/$RUN_ID_2/signal" \
  -H "Authorization: Bearer $TEST_JWT" \
  -H "Content-Type: application/json" \
  -d '{"signal": "manager_rejected", "reason": "Missing documentation"}' | jq .
# Expected: {"status": "rejected", "signal_sent": "manager_rejected"}

# Verify workflow_runs status = 'rejected'
# Verify Temporal UI shows workflow Completed (rejection is a clean exit)

# ── 12. Signal wrong-status workflow ─────────────────────────────────────────

# Try to signal a completed workflow
curl -s -X POST \
  "https://api.nexus.internal/api/v1/workflows/$COMPLETED_RUN_ID/signal" \
  -H "Authorization: Bearer $TEST_JWT" \
  -H "Content-Type: application/json" \
  -d '{"signal": "manager_approved"}' | jq .
# Expected: HTTP 409 — "Cannot signal workflow in status 'completed'"

# ── 13. Multi-tenant isolation ────────────────────────────────────────────────

curl -s "https://api.nexus.internal/api/v1/workflows" \
  -H "Authorization: Bearer $TEST_BETA_JWT" | jq '.total'
# Expected: 0 — test-beta cannot see test-tenant workflows
```

---

## Acceptance Criteria

| # | Test | Expected Result |
|---|---|---|
| 1 | Temporal pods running | All temporal pods in Running state |
| 2 | Worker registered | Temporal UI shows `nexus-workflows` task queue with 2 workers |
| 3 | Workflow trigger published | `workflow_runs` row created within 5 seconds |
| 4 | OnboardingWorkflow state transitions | `pending → in_progress → awaiting_approval` in audit log |
| 5 | Manager approval signal | Workflow resumes, completes all remaining activities |
| 6 | Manager rejection signal | Workflow terminates cleanly, status = `rejected` |
| 7 | Full audit trail | All transitions logged with actor and timestamp |
| 8 | `workflow_completed` on Kafka | Published after COMPLETED or REJECTED terminal state |
| 9 | Signal on wrong-status workflow | HTTP 409 returned |
| 10 | Unknown workflow_type | Consumer logs warning, commits offset, does not crash |
| 11 | Worker pod restart mid-workflow | Temporal resumes from last checkpoint, no duplicate activities |
| 12 | Multi-tenant isolation | test-beta sees zero workflows from test-tenant |
| 13 | `GET /api/v1/workflows` | Returns workflow list without hitting Temporal SDK |

---

## Key Design Decisions

**Why fire-and-forget in WorkflowTriggerConsumer?** Employee onboarding can take 3 days waiting for manager approval. If the consumer awaited `handle.result()` before committing the Kafka offset, the consumer would hold the message for 3 days, blocking the topic partition. Fire-and-forget with a separate monitor coroutine separates the concerns cleanly.

**Why mirror Temporal state into PostgreSQL?** Temporal's own API requires the Temporal SDK client to query workflow state. The M6 Workflow Manager page would need to run Python code or call Temporal's gRPC API directly. Mirroring into `workflow_runs` lets M6 use the same REST API pattern as everything else in M4, keeping the frontend simple.

**Why is the audit log a separate table rather than JSONB on `workflow_runs`?** The audit log is append-only and queried separately from the workflow record itself. A separate table with a foreign key is faster to query, easier to index, and cleaner to reason about than a growing JSONB array that gets updated on every transition.

**Why 2 worker replicas?** Temporal distributes activities across all workers on the same task queue. Two replicas means one pod restart never interrupts a running workflow — Temporal simply routes to the healthy pod.

---

## What Downstream Tasks Depend On

- **P7-M6-04 (Workflow Manager UI)** — reads `GET /api/v1/workflows`, calls `POST /api/v1/workflows/{id}/signal` for human approval actions
- **Iteration 2** — adds 9 more workflow types to `WORKFLOW_REGISTRY`, replaces stub activities with real system calls, activates OPA policy enforcement per workflow step

---

*NEXUS M4 Temporal Workflow State Machine Task Plan · Mentis Consulting · February 2026 · Confidential*
