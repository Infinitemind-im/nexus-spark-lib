# NEXUS — Task Plan: Airflow Orchestration Bridge
**Task ID: P6-M4-04 · Owner: Product Team (Full-Stack) · Weeks 5–7**
**Mentis Consulting · February 2026 · Confidential**

---

## What This Task Is and Why It Exists

The three preceding M4 tasks (CDM Governance Queue, Mapping Exception Queue, Temporal Workflow Engine) handle the human decision layer and business process execution inside NEXUS. This task builds the **orchestration bridge between M4 and Apache Airflow**, the DAG scheduler that controls the M1 data pipeline.

There are two distinct problems this task solves:

### Problem 1 — CDM Approval Does Not Re-Process Historical Data

When a data steward approves a CDM proposal via P6-M4-01, a `nexus.cdm.version_published` event is published. M1's CDM Mapper invalidates its in-memory cache. But the **historical records that were previously stored as Tier 3 (source_extras) remain un-canonicalised**.

Example: A tenant's Odoo `res.partner.vat` field was unknown when 50,000 records were first ingested. Those records have `source_extras: {"vat": "BE0542736408"}`. After a data steward approves the mapping (`vat → crm.party.tax_id`), the cache is updated — but those 50,000 historical records still sit in Delta Lake with `tax_id` absent and `source_extras` populated.

Without a re-processing trigger, the tenant's knowledge stores are permanently incomplete for any field that was ever Tier 3 at ingestion time. The only fix is to re-run the M1 Structural Sub-Cycle on the affected tenant's data. M4 must trigger this by calling Airflow's REST API.

### Problem 2 — No Governance SLA Enforcement

The governance queue in P6-M4-01 can accumulate stale items that no data steward has reviewed. Without monitoring, a CDM proposal can sit unreviewed for weeks, blocking downstream structural enrichment. M4 needs a scheduled Airflow DAG that:
- Scans the governance queue and mapping exception queue for items pending beyond a configurable SLA (default: 48 hours)
- Publishes escalation alerts to a Kafka topic that M6 can surface in the Pipeline Health Dashboard
- Optionally auto-escalates by updating the item's priority, enabling M6 to sort by urgency

### Why Airflow and Not Kafka

M4 could publish a Kafka event and rely on M1 to react. The problem is timing and idempotency: M1's Kafka consumers are always-on workers that process records as they arrive. A re-processing job is a bounded batch operation that has a start, an end, and a result. It must be retried on failure, tracked in a state store, and given a time budget. Airflow's DAG execution model provides all of this. Publishing a Kafka event to trigger a re-scan does not.

The Airflow REST API (`/api/v1/dags/{dag_id}/dagRuns`) is the correct integration point. M4 triggers the DAG; Airflow executes it; the DAG publishes a completion event back to Kafka; M4 records the result.

---

## Scope

**In scope for Weeks 5–7:**

1. **Airflow DAG: `m4_cdm_reprocess_trigger`** — triggered by M4 via Airflow REST API after CDM version publication. Re-submits uncanonicalised (Tier 3) Delta Lake records for a given tenant through the M1 CDM Mapper pipeline.
2. **Airflow DAG: `m4_governance_sla_monitor`** — scheduled daily (or configurable). Scans both governance queues, publishes escalation alerts for stale items, updates queue priority flags in PostgreSQL.
3. **M4 FastAPI endpoint: `POST /api/v1/workflows/dag-trigger`** — authenticated endpoint that M4 internal services call to trigger an Airflow DAG run programmatically.
4. **Kafka consumer in M4: `CDMVersionPublishedConsumer`** — subscribes to `nexus.cdm.version_published`, determines if a Tier 3 backfill is warranted, calls the M4 Airflow Trigger API.
5. **PostgreSQL table: `nexus_system.dag_run_log`** — records every Airflow DAG run triggered by M4 (tenant, DAG id, run id, status, timestamps).
6. **Kafka topic: `nexus.m4.governance_escalation`** — published by the SLA monitor DAG to notify M6.

**Not in scope for Iteration 1:**
- Triggering DAGs from M6 UI (Iteration 2 — Workflow Manager surface)
- Dynamic DAG parameter UI in M6 (Iteration 2)
- Multi-DAG chaining / conditional DAG graph definitions
- Custom Airflow operators beyond the standard `PythonOperator` and `HttpOperator`

---

## Dependencies

| Dependency | Owner | Must be done before |
|---|---|---|
| `nexus_core` library installed | Tech Lead | Week 1 |
| Airflow deployed in `nexus-data` namespace | Platform Team | Phase 0, Day 2 |
| Airflow REST API enabled (`api.auth_backend = basic_auth` or token) | Platform Team | Phase 0 |
| `nexus_system` PostgreSQL schema with RLS applied | Tech Lead | Week 1 DDL |
| P6-M4-01 (CDM Governance Queue) complete | Product Team | Week 4 |
| P6-M4-02 (Mapping Exception Queue) complete | Product Team | Week 5 |
| M1 `m1_sync_orchestrator` Airflow DAG deployed | Data Intelligence | Week 3 |
| M1 `m1_spark_processor` Airflow DAG deployed | Data Intelligence | Week 5 |
| `nexus.cdm.version_published` Kafka topic exists | Platform Team | Phase 0 |
| Airflow service account credentials in Kubernetes Secret | Platform Team | Phase 0 |

---

## Architecture Overview

```
                        ┌─────────────────────────────────────┐
                        │         M4 Service Layer             │
                        │                                      │
  nexus.cdm.            │  CDMVersionPublished                │
  version_published  ──►│  Consumer                           │
  (Kafka)               │      │                              │
                        │      ▼                              │
                        │  POST /api/v1/workflows/dag-trigger │
                        │      │                              │
                        └──────┼──────────────────────────────┘
                               │  Airflow REST API
                               ▼
                        ┌─────────────────┐
                        │ Apache Airflow  │
                        │                │
                        │  m4_cdm_       │    ┌──────────────────┐
                        │  reprocess_    │───►│ M1 Kafka Topics  │
                        │  trigger DAG   │    │ m1.int.sync_req  │
                        │                │    └──────────────────┘
                        │  m4_governance_│
                        │  sla_monitor   │───►  nexus.m4.
                        │  DAG (daily)   │      governance_
                        │                │      escalation (Kafka)
                        └────────────────┘
                               │
                               │  DAG completion
                               ▼
                        nexus_system.dag_run_log (PostgreSQL)
```

---

## Data Flow

### Flow A — CDM Approval Triggers Tier 3 Backfill

```
1. Data steward approves CDM proposal via M4 API
       ↓
2. P6-M4-01 publishes nexus.cdm.version_published (Kafka)
       ↓
3. CDMVersionPublishedConsumer (this task) receives event
       ↓
4. Consumer queries nexus_system.dag_run_log:
   "Has a reprocess DAG already been triggered for this tenant+cdm_version?"
   If yes → skip (idempotency)
   If no → continue
       ↓
5. Consumer calls POST /api/v1/workflows/dag-trigger:
   {
     "dag_id": "m4_cdm_reprocess_trigger",
     "tenant_id": "<tid>",
     "params": {
       "cdm_version": "1.1",
       "source_systems": ["odoo"],   ← from the proposal payload
       "full_backfill": false        ← only re-processes records with source_extras
     }
   }
       ↓
6. M4 Trigger API calls Airflow REST: POST /api/v1/dags/m4_cdm_reprocess_trigger/dagRuns
   Records dag_run in nexus_system.dag_run_log with status='triggered'
       ↓
7. Airflow executes m4_cdm_reprocess_trigger DAG:
   - Reads unprocessed Tier 3 records from Delta Lake for this tenant
   - Publishes them to m1.int.sync_requested (scoped to tenant)
   - M1 CDM Mapper re-classifies them using the new Tier 1 mappings
   - Publishes m1.int.cdm_entities_ready
       ↓
8. DAG completion callback → M4 updates dag_run_log: status='completed'
```

### Flow B — Daily SLA Monitoring

```
Airflow scheduler: daily 08:00 UTC
       ↓
m4_governance_sla_monitor DAG starts
       ↓
For each tenant in nexus_system.tenants (status='active'):
  Query nexus_system.governance_queue WHERE status='pending'
    AND submitted_at < NOW() - INTERVAL '<sla_hours> hours'
  Query nexus_system.mapping_review_queue WHERE status='pending'
    AND first_seen_at < NOW() - INTERVAL '<sla_hours> hours'
       ↓
  For each stale item found:
    - UPDATE priority='high' in respective queue table
    - Publish to nexus.m4.governance_escalation (Kafka):
      {
        "tenant_id": "<tid>",
        "item_type": "cdm_proposal" | "mapping_exception",
        "item_id": "<uuid>",
        "hours_pending": 72,
        "priority": "high"
      }
       ↓
M6 Pipeline Health Dashboard subscribes to nexus.m4.governance_escalation
and surfaces a "Governance SLA breach" banner to the data steward
```

---

## Database Schema

### dag_run_log table

```sql
CREATE TABLE nexus_system.dag_run_log (
    run_log_id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id           TEXT NOT NULL,
    dag_id              TEXT NOT NULL,
    airflow_run_id      TEXT,                   -- Airflow's own run identifier
    triggered_by        TEXT NOT NULL,           -- 'cdm_version_published' | 'manual' | 'sla_monitor'
    trigger_event_id    TEXT,                   -- correlation_id of the Kafka message that triggered this
    dag_params          JSONB,                  -- parameters passed to the DAG run
    status              TEXT NOT NULL DEFAULT 'triggered',
    -- status: triggered | running | completed | failed | skipped
    triggered_at        TIMESTAMPTZ DEFAULT NOW(),
    started_at          TIMESTAMPTZ,
    completed_at        TIMESTAMPTZ,
    records_reprocessed INT,                    -- populated by DAG callback on completion
    error_message       TEXT,

    CONSTRAINT valid_status CHECK (
        status IN ('triggered', 'running', 'completed', 'failed', 'skipped')
    )
);

-- RLS: tenant can only see their own DAG runs
ALTER TABLE nexus_system.dag_run_log ENABLE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON nexus_system.dag_run_log
    FOR ALL TO nexus_app
    USING (tenant_id = current_setting('nexus.current_tenant_id', true));

-- Idempotency index: prevent duplicate DAG runs for same (tenant, dag, trigger event)
CREATE UNIQUE INDEX idx_dag_run_idempotency
    ON nexus_system.dag_run_log (tenant_id, dag_id, trigger_event_id)
    WHERE trigger_event_id IS NOT NULL;

-- Lookup index for status polling
CREATE INDEX idx_dag_run_status
    ON nexus_system.dag_run_log (tenant_id, dag_id, status, triggered_at DESC);
```

### Additions to governance_queue and mapping_review_queue

```sql
-- Add priority column to existing M4-01 and M4-02 queue tables
ALTER TABLE nexus_system.governance_queue
    ADD COLUMN IF NOT EXISTS priority TEXT NOT NULL DEFAULT 'normal'
    CONSTRAINT valid_priority CHECK (priority IN ('normal', 'high', 'critical'));

ALTER TABLE nexus_system.mapping_review_queue
    ADD COLUMN IF NOT EXISTS priority TEXT NOT NULL DEFAULT 'normal'
    CONSTRAINT valid_priority CHECK (priority IN ('normal', 'high', 'critical'));

-- Index for SLA monitoring queries (scan all tenants efficiently)
CREATE INDEX idx_governance_queue_pending_age
    ON nexus_system.governance_queue (submitted_at ASC)
    WHERE status = 'pending';

CREATE INDEX idx_mapping_review_pending_age
    ON nexus_system.mapping_review_queue (first_seen_at ASC)
    WHERE status = 'pending';
```

---

## Implementation

### Part 1 — Airflow DAGs

#### DAG 1: `m4_cdm_reprocess_trigger`

```python
# airflow/dags/m4_cdm_reprocess_trigger.py
"""
Triggered by: M4 Airflow Bridge API (POST /api/v1/workflows/dag-trigger)
Purpose: Re-processes Tier 3 (source_extras) records for a tenant after a CDM mapping
         is approved. Publishes records back through the M1 CDM Mapper pipeline so
         they acquire their newly available canonical fields.

Params:
  tenant_id:       Required. The tenant whose records should be re-processed.
  cdm_version:     Required. The new CDM version (e.g. '1.1') to map against.
  source_systems:  Optional list. If provided, only re-processes records from these
                   source systems (e.g. ['odoo']). If absent, all source systems.
  full_backfill:   Optional bool (default False). If True, re-processes ALL records
                   for the tenant, not just those with source_extras populated.
                   Use only for initial CDM seeding — not routine approvals.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
import json
import logging

logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = "nexus-kafka-kafka-bootstrap.nexus-data.svc.cluster.local:9092"
DELTA_BASE_PATH = "s3a://nexus-raw"


def _validate_params(**context):
    """Validate required DAG params before executing any steps."""
    params = context["params"]
    tenant_id = params.get("tenant_id")
    cdm_version = params.get("cdm_version")

    if not tenant_id:
        raise ValueError("dag param 'tenant_id' is required")
    if not cdm_version:
        raise ValueError("dag param 'cdm_version' is required")
    if "." in tenant_id:
        raise ValueError(f"tenant_id '{tenant_id}' contains a dot — invalid tenant identifier")

    logger.info(f"Reprocess DAG validated: tenant={tenant_id}, cdm_version={cdm_version}")
    return {"tenant_id": tenant_id, "cdm_version": cdm_version}


def _find_tier3_records(**context):
    """
    Queries the Delta Lake for records that have non-empty source_extras for this tenant.
    Returns a list of (source_system, entity_type, delta_path, record_count) tuples.

    In practice this calls the M1 Delta Lake reader via an internal API or
    directly queries the Delta table metadata using delta-rs.
    For MVP: queries PostgreSQL sync_jobs to find completed syncs with known Tier 3 fields.
    """
    import asyncpg
    import asyncio
    from nexus_core.db import get_tenant_scoped_connection

    params = context["params"]
    tenant_id = params["tenant_id"]
    source_systems = params.get("source_systems")

    async def _query():
        pool = await asyncpg.create_pool(dsn=POSTGRES_DSN)
        async with get_tenant_scoped_connection(pool, tenant_id) as conn:
            query = """
                SELECT DISTINCT source_system, entity_type,
                       COUNT(*) AS approx_tier3_count
                FROM nexus_system.schema_snapshots
                WHERE tenant_id = $1
                  AND tier3_field_count > 0
            """
            params_list = [tenant_id]
            if source_systems:
                query += " AND source_system = ANY($2)"
                params_list.append(source_systems)
            query += " GROUP BY source_system, entity_type"
            rows = await conn.fetch(query, *params_list)
        return [dict(r) for r in rows]

    sources = asyncio.run(_query())
    logger.info(f"Found {len(sources)} source/entity combos with Tier 3 data for tenant={tenant_id}")

    # Push to XCom for downstream tasks
    context["ti"].xcom_push(key="tier3_sources", value=sources)
    return sources


def _submit_reprocess_jobs(**context):
    """
    For each source system with Tier 3 records, publishes a sync request
    to the M1 pipeline targeting only the affected Delta Lake partitions.

    Uses a 'reprocess' mode flag in the sync_requested payload that tells
    the M1 Connector Worker to skip source extraction and instead re-read
    from the existing Delta Lake snapshot.
    """
    from nexus_core.messaging import NexusProducer, NexusMessage
    from nexus_core.topics import CrossModuleTopicNamer

    params = context["params"]
    tenant_id = params["tenant_id"]
    cdm_version = params["cdm_version"]

    tier3_sources = context["ti"].xcom_pull(key="tier3_sources")
    if not tier3_sources:
        logger.info(f"No Tier 3 records found for tenant={tenant_id} — nothing to reprocess")
        context["ti"].xcom_push(key="jobs_submitted", value=0)
        return 0

    producer = NexusProducer(KAFKA_BOOTSTRAP)
    jobs_submitted = 0

    for source in tier3_sources:
        message = NexusMessage(
            topic=CrossModuleTopicNamer.M1Internal.SYNC_REQUESTED,
            tenant_id=tenant_id,
            payload={
                "source_system":  source["source_system"],
                "entity_type":    source["entity_type"],
                "mode":           "reprocess_tier3",     # M1 Connector Worker checks this flag
                "cdm_version":    cdm_version,           # Version to map against
                "triggered_by":   "m4_cdm_reprocess_trigger",
                "airflow_run_id": context["run_id"],
            },
        )
        producer.publish(message)
        jobs_submitted += 1
        logger.info(
            f"Submitted reprocess job: tenant={tenant_id}, "
            f"source={source['source_system']}/{source['entity_type']}, "
            f"approx_records={source['approx_tier3_count']}"
        )

    context["ti"].xcom_push(key="jobs_submitted", value=jobs_submitted)
    return jobs_submitted


def _notify_completion(**context):
    """
    Updates nexus_system.dag_run_log with completion status.
    Called by the M4 callback endpoint (see Part 2 — Airflow Callback Endpoint).
    This task just writes completion to the DB directly for MVP simplicity.
    """
    import asyncpg
    import asyncio
    from nexus_core.db import get_tenant_scoped_connection

    params = context["params"]
    tenant_id = params["tenant_id"]
    jobs_submitted = context["ti"].xcom_pull(key="jobs_submitted") or 0

    async def _update():
        pool = await asyncpg.create_pool(dsn=POSTGRES_DSN)
        async with get_tenant_scoped_connection(pool, tenant_id) as conn:
            await conn.execute("""
                UPDATE nexus_system.dag_run_log
                SET status              = 'completed',
                    completed_at        = NOW(),
                    records_reprocessed = $1
                WHERE airflow_run_id    = $2
                  AND tenant_id         = $3
            """, jobs_submitted, context["run_id"], tenant_id)

    asyncio.run(_update())
    logger.info(
        f"Reprocess DAG completed: tenant={tenant_id}, "
        f"jobs_submitted={jobs_submitted}, run_id={context['run_id']}"
    )


# ── DAG Definition ─────────────────────────────────────────────────────────────

default_args = {
    "owner":            "nexus-m4",
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}

with DAG(
    dag_id="m4_cdm_reprocess_trigger",
    default_args=default_args,
    description="Re-processes Tier 3 records after CDM mapping approval",
    schedule_interval=None,         # Triggered externally only — never on a schedule
    start_date=datetime(2026, 3, 1),
    catchup=False,
    max_active_runs=5,              # Allow up to 5 concurrent runs (one per tenant at a time)
    tags=["nexus", "m4", "reprocess"],
    params={
        "tenant_id":     "",
        "cdm_version":   "",
        "source_systems": None,
        "full_backfill":  False,
    },
) as dag:

    validate = PythonOperator(
        task_id="validate_params",
        python_callable=_validate_params,
    )

    find_records = PythonOperator(
        task_id="find_tier3_records",
        python_callable=_find_tier3_records,
    )

    submit_jobs = PythonOperator(
        task_id="submit_reprocess_jobs",
        python_callable=_submit_reprocess_jobs,
    )

    notify = PythonOperator(
        task_id="notify_completion",
        python_callable=_notify_completion,
        trigger_rule="all_done",    # Always run — even if submit_jobs found nothing
    )

    validate >> find_records >> submit_jobs >> notify
```

#### DAG 2: `m4_governance_sla_monitor`

```python
# airflow/dags/m4_governance_sla_monitor.py
"""
Schedule: Daily at 08:00 UTC.
Purpose:  Scans both M4 governance queues for items exceeding the configured SLA.
          Marks stale items as high priority in PostgreSQL.
          Publishes escalation alerts to nexus.m4.governance_escalation (Kafka).
          M6 Pipeline Health Dashboard subscribes and surfaces alerts to data stewards.

Configuration (Airflow Variables — set per environment):
  nexus.governance_sla_hours      default: 48
  nexus.governance_critical_hours default: 120  (5 days → escalates to 'critical')
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import logging

logger = logging.getLogger(__name__)

GOVERNANCE_SLA_TOPIC = "nexus.m4.governance_escalation"


def _scan_and_escalate(**context):
    """
    Main SLA scan logic. Runs once per DAG execution.
    Queries both queue tables across ALL active tenants.
    Publishes escalation alerts and updates priority flags.
    """
    import asyncpg
    import asyncio
    from nexus_core.messaging import NexusProducer, NexusMessage

    sla_hours      = int(Variable.get("nexus.governance_sla_hours",      default_var=48))
    critical_hours = int(Variable.get("nexus.governance_critical_hours", default_var=120))

    POSTGRES_DSN = Variable.get("nexus.postgres_dsn")
    KAFKA_BOOTSTRAP = Variable.get("nexus.kafka_bootstrap")

    async def _run():
        pool = await asyncpg.create_pool(dsn=POSTGRES_DSN)
        producer = NexusProducer(KAFKA_BOOTSTRAP)
        total_escalated = 0

        # Use superuser connection for cross-tenant monitoring query
        # This is one of the few legitimate cross-tenant queries in the platform
        admin_conn = await asyncpg.connect(dsn=POSTGRES_DSN)

        # ── Scan CDM Governance Queue ──────────────────────────────────────────
        stale_proposals = await admin_conn.fetch("""
            SELECT proposal_id, tenant_id, proposal_type, submitted_at,
                   EXTRACT(EPOCH FROM (NOW() - submitted_at)) / 3600 AS hours_pending
            FROM nexus_system.governance_queue
            WHERE status = 'pending'
              AND submitted_at < NOW() - ($1 || ' hours')::INTERVAL
            ORDER BY submitted_at ASC
        """, str(sla_hours))

        for row in stale_proposals:
            hours_pending = row["hours_pending"]
            new_priority = "critical" if hours_pending >= critical_hours else "high"

            # Update priority in DB
            await admin_conn.execute("""
                UPDATE nexus_system.governance_queue
                SET priority = $1
                WHERE proposal_id = $2 AND status = 'pending'
            """, new_priority, row["proposal_id"])

            # Publish escalation alert
            producer.publish(NexusMessage(
                topic=GOVERNANCE_SLA_TOPIC,
                tenant_id=row["tenant_id"],
                payload={
                    "item_type":     "cdm_proposal",
                    "item_id":       str(row["proposal_id"]),
                    "tenant_id":     row["tenant_id"],
                    "proposal_type": row["proposal_type"],
                    "hours_pending": round(hours_pending, 1),
                    "priority":      new_priority,
                    "sla_hours":     sla_hours,
                    "alert_message": (
                        f"CDM proposal pending for {round(hours_pending, 0):.0f} hours "
                        f"(SLA: {sla_hours}h). Priority escalated to {new_priority}."
                    ),
                },
            ))
            total_escalated += 1
            logger.warning(
                f"SLA breach — CDM proposal: tenant={row['tenant_id']}, "
                f"id={row['proposal_id']}, hours={hours_pending:.1f}, priority={new_priority}"
            )

        # ── Scan Mapping Exception Queue ──────────────────────────────────────
        stale_exceptions = await admin_conn.fetch("""
            SELECT review_id, tenant_id, source_system, source_field,
                   occurrence_count,
                   EXTRACT(EPOCH FROM (NOW() - first_seen_at)) / 3600 AS hours_pending
            FROM nexus_system.mapping_review_queue
            WHERE status = 'pending'
              AND first_seen_at < NOW() - ($1 || ' hours')::INTERVAL
            ORDER BY occurrence_count DESC, first_seen_at ASC
        """, str(sla_hours))

        for row in stale_exceptions:
            hours_pending = row["hours_pending"]
            new_priority = "critical" if hours_pending >= critical_hours else "high"

            await admin_conn.execute("""
                UPDATE nexus_system.mapping_review_queue
                SET priority = $1
                WHERE review_id = $2 AND status = 'pending'
            """, new_priority, row["review_id"])

            producer.publish(NexusMessage(
                topic=GOVERNANCE_SLA_TOPIC,
                tenant_id=row["tenant_id"],
                payload={
                    "item_type":       "mapping_exception",
                    "item_id":         str(row["review_id"]),
                    "tenant_id":       row["tenant_id"],
                    "source_system":   row["source_system"],
                    "source_field":    row["source_field"],
                    "occurrence_count": row["occurrence_count"],
                    "hours_pending":   round(hours_pending, 1),
                    "priority":        new_priority,
                    "sla_hours":       sla_hours,
                    "alert_message": (
                        f"Mapping exception '{row['source_field']}' "
                        f"({row['occurrence_count']} occurrences) pending "
                        f"{round(hours_pending, 0):.0f}h. SLA: {sla_hours}h."
                    ),
                },
            ))
            total_escalated += 1

        await admin_conn.close()
        logger.info(
            f"SLA monitor complete: {total_escalated} items escalated "
            f"({len(stale_proposals)} proposals, {len(stale_exceptions)} exceptions)"
        )
        return total_escalated

    return asyncio.run(_run())


default_args = {
    "owner":   "nexus-m4",
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id="m4_governance_sla_monitor",
    default_args=default_args,
    description="Daily SLA scan for governance queue backlog",
    schedule_interval="0 8 * * *",     # 08:00 UTC daily
    start_date=datetime(2026, 3, 1),
    catchup=False,
    max_active_runs=1,                  # Never two runs overlapping
    tags=["nexus", "m4", "monitoring"],
) as dag:

    PythonOperator(
        task_id="scan_and_escalate",
        python_callable=_scan_and_escalate,
    )
```

---

### Part 2 — M4 Airflow Bridge API and Consumer

#### FastAPI Endpoint: `/api/v1/workflows/dag-trigger`

```python
# m4/api/airflow_bridge.py

import os
import httpx
import logging
from fastapi import APIRouter, HTTPException, Header
from pydantic import BaseModel
from typing import Optional
import asyncpg
import uuid

from nexus_core.db import get_tenant_scoped_connection
from nexus_core.tenant_validator import is_active_tenant

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/v1/workflows", tags=["Airflow Bridge"])

AIRFLOW_BASE_URL = os.environ["AIRFLOW_BASE_URL"]   # e.g. http://nexus-airflow.nexus-data.svc.cluster.local:8080
AIRFLOW_USERNAME  = os.environ["AIRFLOW_USERNAME"]
AIRFLOW_PASSWORD  = os.environ["AIRFLOW_PASSWORD"]

# Whitelist of DAGs that M4 is permitted to trigger
# Prevents M4 from triggering arbitrary DAGs if the endpoint is called with unexpected dag_ids
PERMITTED_DAG_IDS = {
    "m4_cdm_reprocess_trigger",
    "m1_sync_orchestrator",     # M4 may trigger a full re-sync for a tenant on request
}


class DagTriggerRequest(BaseModel):
    dag_id:           str
    tenant_id:        str
    params:           dict = {}
    trigger_event_id: Optional[str] = None   # Kafka correlation_id for idempotency


class DagTriggerResponse(BaseModel):
    run_log_id:     str
    airflow_run_id: str
    dag_id:         str
    tenant_id:      str
    status:         str


@router.post("/dag-trigger", response_model=DagTriggerResponse)
async def trigger_dag(
    body:        DagTriggerRequest,
    x_tenant_id: str = Header(...),   # Must match body.tenant_id
    x_user_id:   str = Header(...),
):
    """
    Triggers an Airflow DAG run for a specific tenant.
    Records the run in nexus_system.dag_run_log with idempotency.

    Security rules:
    - x_tenant_id (from Kong JWT) must match body.tenant_id
    - dag_id must be in PERMITTED_DAG_IDS
    - If trigger_event_id is provided and a run already exists for it, returns existing run (idempotent)
    """

    # ── Security checks ────────────────────────────────────────────────────────
    if body.tenant_id != x_tenant_id:
        raise HTTPException(
            403,
            f"tenant_id in body ({body.tenant_id}) does not match authenticated tenant ({x_tenant_id}). "
            "You can only trigger DAGs for your own tenant."
        )

    if body.dag_id not in PERMITTED_DAG_IDS:
        raise HTTPException(
            403,
            f"DAG '{body.dag_id}' is not in the permitted DAG list for M4. "
            f"Permitted: {sorted(PERMITTED_DAG_IDS)}"
        )

    if not is_active_tenant(body.tenant_id):
        raise HTTPException(404, f"Tenant '{body.tenant_id}' is not active")

    # ── Idempotency check ──────────────────────────────────────────────────────
    if body.trigger_event_id:
        async with get_tenant_scoped_connection(db_pool, x_tenant_id) as conn:
            existing = await conn.fetchrow("""
                SELECT run_log_id, airflow_run_id, status
                FROM nexus_system.dag_run_log
                WHERE tenant_id        = $1
                  AND dag_id           = $2
                  AND trigger_event_id = $3
            """, x_tenant_id, body.dag_id, body.trigger_event_id)

            if existing:
                logger.info(
                    f"Idempotent skip: DAG {body.dag_id} already triggered for "
                    f"event {body.trigger_event_id}, run_log_id={existing['run_log_id']}"
                )
                return DagTriggerResponse(
                    run_log_id=str(existing["run_log_id"]),
                    airflow_run_id=existing["airflow_run_id"] or "",
                    dag_id=body.dag_id,
                    tenant_id=x_tenant_id,
                    status=existing["status"],
                )

    # ── Call Airflow REST API ──────────────────────────────────────────────────
    airflow_run_id = f"nexus-{body.tenant_id}-{uuid.uuid4().hex[:8]}"
    airflow_payload = {
        "dag_run_id": airflow_run_id,
        "conf": {
            **body.params,
            "tenant_id": body.tenant_id,
            "triggered_by_nexus": True,
        },
    }

    async with httpx.AsyncClient(timeout=10.0) as client:
        response = await client.post(
            f"{AIRFLOW_BASE_URL}/api/v1/dags/{body.dag_id}/dagRuns",
            json=airflow_payload,
            auth=(AIRFLOW_USERNAME, AIRFLOW_PASSWORD),
        )

    if response.status_code not in (200, 201):
        logger.error(
            f"Airflow API error: {response.status_code} {response.text}",
            extra={"tenant_id": x_tenant_id, "dag_id": body.dag_id}
        )
        raise HTTPException(
            502,
            f"Airflow API returned {response.status_code}: {response.text[:200]}"
        )

    # ── Record in dag_run_log ──────────────────────────────────────────────────
    async with get_tenant_scoped_connection(db_pool, x_tenant_id) as conn:
        row = await conn.fetchrow("""
            INSERT INTO nexus_system.dag_run_log
                (tenant_id, dag_id, airflow_run_id, triggered_by,
                 trigger_event_id, dag_params, status)
            VALUES ($1, $2, $3, $4, $5, $6, 'triggered')
            RETURNING run_log_id
        """,
            x_tenant_id, body.dag_id, airflow_run_id,
            body.params.get("triggered_by", "api"),
            body.trigger_event_id,
            body.params,
        )

    logger.info(
        f"DAG triggered: {body.dag_id}, tenant={x_tenant_id}, "
        f"airflow_run_id={airflow_run_id}, run_log_id={row['run_log_id']}"
    )

    return DagTriggerResponse(
        run_log_id=str(row["run_log_id"]),
        airflow_run_id=airflow_run_id,
        dag_id=body.dag_id,
        tenant_id=x_tenant_id,
        status="triggered",
    )


@router.get("/dag-runs")
async def list_dag_runs(
    x_tenant_id: str = Header(...),
    x_user_id:   str = Header(...),
    dag_id:      Optional[str] = None,
    status:      Optional[str] = None,
    limit:       int = 50,
):
    """List DAG runs triggered for this tenant. Used by M6 Workflow Manager."""
    async with get_tenant_scoped_connection(db_pool, x_tenant_id) as conn:
        query = """
            SELECT run_log_id, dag_id, airflow_run_id, triggered_by, status,
                   triggered_at, completed_at, records_reprocessed, error_message
            FROM nexus_system.dag_run_log
            WHERE tenant_id = $1
        """
        params = [x_tenant_id]
        if dag_id:
            query += f" AND dag_id = ${len(params)+1}"
            params.append(dag_id)
        if status:
            query += f" AND status = ${len(params)+1}"
            params.append(status)
        query += f" ORDER BY triggered_at DESC LIMIT {limit}"
        rows = await conn.fetch(query, *params)

    return {"dag_runs": [dict(r) for r in rows], "total": len(rows)}
```

#### Kafka Consumer: `CDMVersionPublishedConsumer`

```python
# m4/consumers/cdm_version_published_consumer.py

import logging
import asyncpg
import httpx
from nexus_core.messaging import NexusConsumer, NexusMessage
from nexus_core.topics import CrossModuleTopicNamer
from nexus_core.db import get_tenant_scoped_connection

logger = logging.getLogger(__name__)

M4_API_BASE = "http://localhost:8002"  # Internal — no Kong, no JWT required for internal calls


class CDMVersionPublishedConsumer:
    """
    Subscribes to nexus.cdm.version_published.
    When a new CDM version is published for a tenant, determines whether
    a Tier 3 backfill is warranted and calls the M4 DAG Trigger API if so.

    Decision logic:
    - If the version change includes new CDM entities or new CDM fields that
      previously had no Tier 1 or Tier 2 mapping, a backfill is warranted.
    - If the version change only modifies confidence scores or notes, skip.
    - Uses correlation_id as trigger_event_id for idempotency.
    """

    def __init__(self, db_pool: asyncpg.Pool, kafka_bootstrap: str):
        self.consumer = NexusConsumer(
            bootstrap_servers=kafka_bootstrap,
            group_id="m4-cdm-version-listener",
            topics=[CrossModuleTopicNamer.CDM.VERSION_PUBLISHED],
        )
        self.db = db_pool

    async def run(self):
        logger.info("CDMVersionPublishedConsumer started")
        while True:
            message = self.consumer.poll(timeout=1.0)
            if not message:
                continue
            try:
                await self._handle(message)
                self.consumer.commit(message)
            except Exception as e:
                logger.error(
                    f"Failed to handle cdm.version_published: {e}",
                    extra={"tenant_id": message.tenant_id},
                    exc_info=True,
                )

    async def _handle(self, message: NexusMessage):
        payload   = message.payload
        tenant_id = message.tenant_id
        new_version = payload.get("cdm_version_new")
        change_summary = payload.get("change_summary", "")

        logger.info(
            f"CDM version published: tenant={tenant_id}, version={new_version}",
            extra={"tenant_id": tenant_id}
        )

        # Determine if a Tier 3 backfill is warranted
        # For MVP: trigger backfill on every CDM version change
        # For Iteration 2: inspect the diff (new fields only, not score changes)
        should_backfill = True

        if not should_backfill:
            logger.info(f"No backfill warranted for version {new_version} — skipping DAG trigger")
            return

        # Determine which source systems are affected (from the proposal payload)
        source_systems = payload.get("affected_source_systems")

        # Call the M4 DAG Trigger endpoint internally
        trigger_payload = {
            "dag_id":           "m4_cdm_reprocess_trigger",
            "tenant_id":        tenant_id,
            "trigger_event_id": message.correlation_id or message.message_id,
            "params": {
                "tenant_id":      tenant_id,
                "cdm_version":    new_version,
                "source_systems": source_systems,
                "full_backfill":  False,
                "triggered_by":   "cdm_version_published",
            },
        }

        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.post(
                f"{M4_API_BASE}/api/v1/workflows/dag-trigger",
                json=trigger_payload,
                headers={
                    # Internal call — use a service account header rather than a user JWT
                    "X-Tenant-ID": tenant_id,
                    "X-User-ID":   "nexus-system",
                    "X-Service":   "m4-cdm-consumer",
                },
            )

        if response.status_code in (200, 201):
            result = response.json()
            logger.info(
                f"Backfill DAG triggered: run_log_id={result['run_log_id']}, "
                f"airflow_run_id={result['airflow_run_id']}",
                extra={"tenant_id": tenant_id}
            )
        else:
            logger.error(
                f"Failed to trigger backfill DAG: {response.status_code} {response.text}",
                extra={"tenant_id": tenant_id}
            )
            raise RuntimeError(f"DAG trigger failed: {response.status_code}")
```

#### Register in `m4/api/main.py`

```python
# m4/api/main.py  (extend from M4-01 and M4-02)

from fastapi import FastAPI
from m4.api.governance import router as governance_router
from m4.api.mapping_exceptions import router as mapping_router
from m4.api.airflow_bridge import router as airflow_router    # NEW

def create_app() -> FastAPI:
    app = FastAPI(
        title="NEXUS M4 Governance & Orchestration API",
        version="1.2.0",
    )
    app.include_router(governance_router)
    app.include_router(mapping_router)
    app.include_router(airflow_router)    # NEW

    @app.get("/health")
    async def health():
        return {"status": "ok", "service": "m4-governance-api"}

    return app
```

#### Register Consumer in Startup

```python
# m4/entrypoint.py  (extend from M4-02)

import asyncio
from m4.consumers.cdm_governance_consumer     import CDMGovernanceConsumer
from m4.consumers.mapping_exception_consumer  import MappingExceptionConsumer
from m4.consumers.cdm_version_published_consumer import CDMVersionPublishedConsumer  # NEW

async def main():
    governance_consumer    = CDMGovernanceConsumer(db_pool, KAFKA_BOOTSTRAP)
    exception_consumer     = MappingExceptionConsumer(db_pool, KAFKA_BOOTSTRAP)
    cdm_version_consumer   = CDMVersionPublishedConsumer(db_pool, KAFKA_BOOTSTRAP)  # NEW

    await asyncio.gather(
        governance_consumer.run(),
        exception_consumer.run(),
        cdm_version_consumer.run(),    # NEW
    )
```

---

### Part 3 — Kong Route and Kubernetes

#### Kong Route Addition

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
      - name: workflow-dag-trigger     # NEW
        paths: [/api/v1/workflows]
        methods: [GET, POST]
    plugins:
      - name: jwt
      - name: prometheus
```

#### Airflow credentials secret

```yaml
# k8s/secrets/airflow-credentials.yaml
apiVersion: v1
kind: Secret
metadata:
  name: nexus-airflow-credentials
  namespace: nexus-app
type: Opaque
stringData:
  AIRFLOW_BASE_URL: "http://nexus-airflow-webserver.nexus-data.svc.cluster.local:8080"
  AIRFLOW_USERNAME: "nexus-service"     # Airflow user created with 'Op' role
  AIRFLOW_PASSWORD: "<stored-in-vault>"
```

#### M4 Deployment update (add env vars)

```yaml
# k8s/m4-governance-api.yaml  (add to existing deployment)
env:
  - name: AIRFLOW_BASE_URL
    valueFrom:
      secretKeyRef:
        name: nexus-airflow-credentials
        key: AIRFLOW_BASE_URL
  - name: AIRFLOW_USERNAME
    valueFrom:
      secretKeyRef:
        name: nexus-airflow-credentials
        key: AIRFLOW_USERNAME
  - name: AIRFLOW_PASSWORD
    valueFrom:
      secretKeyRef:
        name: nexus-airflow-credentials
        key: AIRFLOW_PASSWORD
```

#### Airflow Variables (set via Airflow UI or CLI)

```bash
# Run once after Airflow is deployed
airflow variables set nexus.postgres_dsn    "postgresql://nexus_app:password@nexus-postgres.nexus-data.svc.cluster.local:5432/nexus"
airflow variables set nexus.kafka_bootstrap "nexus-kafka-kafka-bootstrap.nexus-data.svc.cluster.local:9092"
airflow variables set nexus.governance_sla_hours      "48"
airflow variables set nexus.governance_critical_hours "120"
```

---

## API Contract Summary

| Method | Path | Description | Triggers Airflow |
|---|---|---|---|
| `POST` | `/api/v1/workflows/dag-trigger` | Trigger a permitted Airflow DAG for a tenant | Yes |
| `GET` | `/api/v1/workflows/dag-runs` | List DAG runs for this tenant | — |

New Kafka topics consumed/produced by this task:

| Direction | Topic | Consumer/Producer | Purpose |
|---|---|---|---|
| Consumed | `nexus.cdm.version_published` | `CDMVersionPublishedConsumer` | Triggers Tier 3 backfill DAG |
| Produced | `nexus.m4.governance_escalation` | Airflow SLA monitor DAG | Notifies M6 of SLA breaches |

---

## Acceptance Test Sequence

Run this on Week 7 after all components are deployed.

```bash
# ── PRE-REQUISITES ──────────────────────────────────────────────────────────────

# 1. Verify Airflow DAGs are deployed and visible
curl -s http://nexus-airflow.nexus-data.svc.cluster.local:8080/api/v1/dags \
  -u nexus-service:<password> | jq '.dags[].dag_id' | grep m4
# Expected: "m4_cdm_reprocess_trigger" and "m4_governance_sla_monitor"

# ── TEST 1: Manual DAG Trigger via M4 API ──────────────────────────────────────

curl -s -X POST https://api.nexus.internal/api/v1/workflows/dag-trigger \
  -H "Authorization: Bearer $TEST_JWT" \
  -H "Content-Type: application/json" \
  -d '{
    "dag_id": "m4_cdm_reprocess_trigger",
    "tenant_id": "test-tenant",
    "params": {
      "cdm_version": "1.1",
      "source_systems": ["odoo"],
      "full_backfill": false
    }
  }' | jq .
# Expected:
# {
#   "run_log_id":     "<uuid>",
#   "airflow_run_id": "nexus-test-tenant-<hex>",
#   "dag_id":         "m4_cdm_reprocess_trigger",
#   "tenant_id":      "test-tenant",
#   "status":         "triggered"
# }

# Verify in Airflow UI: DAG run visible with status 'running' then 'success'

# ── TEST 2: Idempotency ─────────────────────────────────────────────────────────

# Re-trigger with the same trigger_event_id
curl -s -X POST https://api.nexus.internal/api/v1/workflows/dag-trigger \
  -H "Authorization: Bearer $TEST_JWT" \
  -H "Content-Type: application/json" \
  -d '{
    "dag_id": "m4_cdm_reprocess_trigger",
    "tenant_id": "test-tenant",
    "trigger_event_id": "event-abc-123",
    "params": { "cdm_version": "1.1" }
  }' | jq .

# Second call with same trigger_event_id:
curl -s -X POST https://api.nexus.internal/api/v1/workflows/dag-trigger \
  -H "Authorization: Bearer $TEST_JWT" \
  -H "Content-Type: application/json" \
  -d '{
    "dag_id": "m4_cdm_reprocess_trigger",
    "tenant_id": "test-tenant",
    "trigger_event_id": "event-abc-123",
    "params": { "cdm_version": "1.1" }
  }' | jq .
# Expected: same run_log_id returned, status = 'triggered' (not a new run)

# ── TEST 3: Security — Cross-tenant trigger rejected ───────────────────────────

# test-beta tries to trigger a DAG for test-tenant
curl -s -X POST https://api.nexus.internal/api/v1/workflows/dag-trigger \
  -H "Authorization: Bearer $TEST_BETA_JWT" \
  -H "Content-Type: application/json" \
  -d '{
    "dag_id": "m4_cdm_reprocess_trigger",
    "tenant_id": "test-tenant",    ← mismatch with beta JWT
    "params": {}
  }' | jq .
# Expected: HTTP 403 — "tenant_id in body does not match authenticated tenant"

# ── TEST 4: Security — Unpermitted DAG rejected ────────────────────────────────

curl -s -X POST https://api.nexus.internal/api/v1/workflows/dag-trigger \
  -H "Authorization: Bearer $TEST_JWT" \
  -H "Content-Type: application/json" \
  -d '{
    "dag_id": "m1_spark_processor",
    "tenant_id": "test-tenant",
    "params": {}
  }' | jq .
# Expected: HTTP 403 — "DAG 'm1_spark_processor' is not in the permitted DAG list"

# ── TEST 5: End-to-End — CDM Approval triggers backfill ───────────────────────

# Step 1: Approve a CDM proposal (from M4-01 test sequence)
# Step 2: Observe nexus.cdm.version_published in Kafka UI
# Step 3: Check M4 logs — CDMVersionPublishedConsumer should log DAG trigger
kubectl logs -n nexus-app deployment/m4-governance-api | grep "Backfill DAG triggered"
# Expected: log line with airflow_run_id

# Step 4: Check dag_run_log table
psql $POSTGRES_DSN -c "
  SELECT dag_id, airflow_run_id, triggered_by, status, triggered_at
  FROM nexus_system.dag_run_log
  WHERE tenant_id = 'test-tenant'
  ORDER BY triggered_at DESC LIMIT 5;"
# Expected: one row with dag_id='m4_cdm_reprocess_trigger', triggered_by='cdm_version_published'

# Step 5: Verify Airflow UI shows DAG run completed successfully

# ── TEST 6: SLA Monitor DAG ────────────────────────────────────────────────────

# Create a stale governance item by back-dating submitted_at
psql $POSTGRES_DSN -c "
  UPDATE nexus_system.governance_queue
  SET submitted_at = NOW() - INTERVAL '72 hours'
  WHERE tenant_id = 'test-tenant' AND status = 'pending'
  LIMIT 1;"

# Trigger the SLA monitor DAG manually
curl -s -X POST \
  http://nexus-airflow.nexus-data.svc.cluster.local:8080/api/v1/dags/m4_governance_sla_monitor/dagRuns \
  -u nexus-service:<password> \
  -H "Content-Type: application/json" \
  -d '{"dag_run_id": "test-manual-run"}'

# Wait for DAG to complete, then check:
# 1. governance_queue row: priority = 'high'
psql $POSTGRES_DSN -c "SELECT proposal_id, priority FROM nexus_system.governance_queue WHERE tenant_id='test-tenant';"

# 2. Kafka: nexus.m4.governance_escalation topic has a message
# In Kafka UI: verify message payload contains item_type='cdm_proposal', priority='high'

# ── TEST 7: DAG Run List ────────────────────────────────────────────────────────

curl -s "https://api.nexus.internal/api/v1/workflows/dag-runs" \
  -H "Authorization: Bearer $TEST_JWT" | jq .
# Expected: list of dag runs for test-tenant (from tests above)

curl -s "https://api.nexus.internal/api/v1/workflows/dag-runs?dag_id=m4_cdm_reprocess_trigger" \
  -H "Authorization: Bearer $TEST_JWT" | jq '.total'
# Expected: >= 1
```

---

## Acceptance Criteria

| # | Test | Expected Result |
|---|---|---|
| 1 | Manual DAG trigger via API | Airflow run created, `dag_run_log` row with `status='triggered'` |
| 2 | Idempotent re-trigger | Same `run_log_id` returned — no duplicate Airflow run |
| 3 | Cross-tenant trigger attempt | HTTP 403 |
| 4 | Unpermitted `dag_id` | HTTP 403 |
| 5 | CDM proposal approved → backfill DAG | `CDMVersionPublishedConsumer` logs trigger, Airflow run visible |
| 6 | Stale governance item after SLA breach | `priority = 'high'` in DB, escalation message on `nexus.m4.governance_escalation` |
| 7 | Critical SLA breach (> 120h) | `priority = 'critical'` |
| 8 | SLA monitor runs at 08:00 UTC | Airflow schedule shows correct next_dagrun |
| 9 | `dag_run_log` RLS isolation | test-beta cannot see test-tenant's DAG runs |
| 10 | Airflow failure (503) | M4 API returns HTTP 502, does NOT write to `dag_run_log` |
| 11 | Airflow variable `governance_sla_hours` | Changing the variable changes escalation threshold on next DAG run |
| 12 | `GET /api/v1/workflows/dag-runs` | Returns tenant's DAG runs, filterable by dag_id and status |

---

## Key Design Decisions

**Why does M4 call the Airflow REST API instead of publishing a Kafka event?**
M1 already subscribes to `nexus.cdm.version_published` for cache invalidation. If M4 published a second event and M1 reacted to it by starting a re-scan, the re-scan would fire on every CDM change even when no Tier 3 records exist. The DAG trigger model is explicit: M4 queries the DB first, decides a backfill is needed, then triggers the DAG with the exact parameters for this tenant's situation. This avoids unnecessary re-scans and gives a concrete run ID for audit purposes.

**Why is the permitted DAG whitelist in Python code rather than a DB config table?**
Changing which DAGs M4 can trigger is an architectural decision that requires code review, not a runtime config change. If it were a DB config, any data steward with DB write access could add `all_dags` to the whitelist. The whitelist is intentionally in application code so it goes through the PR review process.

**Why does the SLA monitor DAG use a direct admin DB connection instead of the M4 API?**
The SLA monitor needs to scan all tenants at once. If it called the M4 API per tenant, it would need to generate a JWT for each tenant, which requires the Airflow DAG to have access to the Okta signing key. This is a worse security posture than a read-only admin DB query scoped to the SLA scan. The admin connection is justified here by the cross-tenant monitoring use case — it is the correct exception to the single-tenant query rule.

**Why does the backfill DAG publish to `m1.int.sync_requested` with `mode: 'reprocess_tier3'` instead of calling M1 directly?**
All pipeline communication between modules goes through Kafka (Rule 1 from the NEXUS Developer Operational Plan). The `mode` field on the message payload is how M4 communicates intent to M1 without coupling to M1's internal architecture. M1's Connector Worker checks this flag and routes to its internal re-processing branch instead of querying the source system.

---

## What Downstream Tasks Depend On

Once P6-M4-04 is complete and passing:

- **P7-M6-04 (Pipeline Health Dashboard)** — add a "Governance Escalations" panel reading from `nexus.m4.governance_escalation` via WebSocket subscription
- **P7-M6-04 (Workflow Manager)** — add a "DAG Run History" table reading from `GET /api/v1/workflows/dag-runs`
- **Iteration 2** — configurable SLA thresholds per tenant (stored in `nexus_system.tenants`), bulk re-trigger UI, DAG dependency graph visualisation in M6

---

*NEXUS M4 Airflow Orchestration Bridge Task Plan · Mentis Consulting · February 2026 · Confidential*
