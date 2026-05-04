# Service Spec: `nexus-m4-worker`
**Module:** M4 — Workflow & Integration
**Type:** Event-driven worker · Kubernetes `Deployment`
**Team:** Product
**Version:** 1.0 · March 2026 · Confidential

---

## Purpose

Processes all M4 asynchronous events. Four Kafka consumer loops run in a single process for Iteration 1: storing CDM proposals, storing mapping exceptions, triggering Tier 3 backfill DAGs after CDM approvals, and starting Temporal business workflows. Has no HTTP surface — it only consumes from Kafka and writes to PostgreSQL or calls internal APIs.

---

## Multi-Tenancy Model

### Tenant Identity Source — Kafka Envelope
All four consumers derive `tenant_id` exclusively from `NexusMessage.tenant_id`. The `set_tenant()` / `clear_tenant()` pattern is mandatory around every message processing cycle.

### Tenant Validation at Consumer Level
All consumers pass `tenant_validator=is_active_tenant` to `NexusConsumer`. Messages for suspended or unknown tenants are committed without processing. This prevents a decommissioned tenant's pipeline from blocking consumer group progress.

### Database Writes — RLS-Scoped
Every PostgreSQL write goes through `get_tenant_scoped_connection(pool, tenant_id)`. The governance and mapping queue tables have RLS policies — a write that accidentally omits `tenant_id` in the INSERT statement will still be scoped to the connection's active tenant.

```python
async with get_tenant_scoped_connection(pool, message.tenant_id) as conn:
    await conn.execute("""
        INSERT INTO nexus_system.governance_queue
            (proposal_type, tenant_id, status, payload, priority)
        VALUES ($1, $2, 'pending', $3, 'normal')
    """, "cdm_extension", message.tenant_id, message.payload)
```

### Mapping Exception Deduplication — Per-Tenant
The Mapping Exception Consumer deduplicates exceptions by `(tenant_id, source_system, source_table, source_field)`. Deduplication is strictly per-tenant — the same unknown field appearing in two different tenants' data creates **separate** exception records for each tenant, not a merged one.

```python
await conn.execute("""
    INSERT INTO nexus_system.mapping_review_queue
        (tenant_id, source_system, source_table, source_field, cdm_entity_suggestion,
         cdm_field_suggestion, confidence, occurrence_count, first_seen_at)
    VALUES ($1, $2, $3, $4, $5, $6, $7, 1, NOW())
    ON CONFLICT (tenant_id, source_system, source_table, source_field)   -- tenant_id is part of key
    DO UPDATE SET
        occurrence_count = nexus_system.mapping_review_queue.occurrence_count + 1,
        last_seen_at     = NOW(),
        confidence       = EXCLUDED.confidence   -- Update to latest confidence score
""", message.tenant_id, ...)
```

### CDM Version Published Consumer — Idempotency
The DAG trigger consumer uses the Kafka `correlation_id` (or `message_id`) as a `trigger_event_id` to prevent duplicate DAG runs. The uniqueness constraint on `nexus_system.dag_run_log(tenant_id, dag_id, trigger_event_id)` ensures that even if the consumer processes the same message twice (e.g. after a crash before committing the offset), no duplicate Airflow DAG is triggered.

```python
async def _handle_version_published(self, message: NexusMessage):
    trigger_event_id = message.correlation_id or message.message_id

    # Check idempotency before calling Airflow
    async with get_tenant_scoped_connection(pool, message.tenant_id) as conn:
        existing = await conn.fetchval("""
            SELECT run_log_id FROM nexus_system.dag_run_log
            WHERE tenant_id = $1 AND dag_id = 'm4_cdm_reprocess_trigger'
              AND trigger_event_id = $2
        """, message.tenant_id, trigger_event_id)

    if existing:
        logger.info(f"Idempotent skip: DAG already triggered for event {trigger_event_id}")
        self.consumer.commit(message)
        return

    # Trigger DAG via M4 API (internal call — no Kong, no user JWT)
    await self._call_dag_trigger_api(message, trigger_event_id)
    self.consumer.commit(message)
```

### Temporal Workflow Scoping
The Workflow Trigger Consumer starts Temporal workflows scoped to the tenant. The workflow ID includes the `tenant_id` prefix to prevent collisions between tenants running the same workflow type:

```python
handle = await temporal_client.start_workflow(
    OnboardingWorkflow.run,
    payload.get("context", {}),
    id=f"{message.tenant_id}-onboarding-{uuid.uuid4()}",   # tenant_id in workflow ID
    task_queue="nexus-workflows",
)
```

Temporal does not natively enforce multi-tenancy, so the workflow ID naming convention is the primary isolation mechanism. Workflow results are also stored in `nexus_system.dag_run_log` with RLS enforcement.

---

## Component Details

### Component 1 — CDM Governance Consumer

**Consumer group:** `m4-cdm-governance`
**Input topic:** `nexus.cdm.extension_proposed`

Stores every incoming CDM extension proposal in `nexus_system.governance_queue`. Never auto-approves. Never modifies the payload.

### Component 2 — Mapping Exception Consumer

**Consumer group:** `m4-mapping-exceptions`
**Input topic:** `m1.int.mapping_failed`

Receives Tier 2 field detection events from M1's CDM Mapper. Deduplicates per `(tenant_id, source_system, source_table, source_field)` using `ON CONFLICT DO UPDATE occurrence_count`. Groups related exceptions so data stewards see "this field appeared 847 times" rather than 847 separate rows to review.

### Component 3 — CDM Version Published Consumer

**Consumer group:** `m4-cdm-version-listener`
**Input topic:** `nexus.cdm.version_published`

After a CDM approval, determines whether Tier 3 backfill is warranted and calls the M4 API's DAG trigger endpoint internally (no Kong, no user JWT — uses a service-to-service auth header). For Iteration 1, backfill is triggered on every CDM version change. Idempotency prevents double-triggering.

### Component 4 — Workflow Trigger Consumer

**Consumer group:** `m4-workflow-triggers`
**Input topic:** `{tid}.m2.workflow_trigger`

Receives workflow trigger events from M2 (when a user query implies a business action, e.g. "start onboarding for Alice Martin"). Looks up the workflow type in `WORKFLOW_MAP`, starts the corresponding Temporal workflow, and publishes `{tid}.m4.workflow_completed` on completion.

**Iteration 1 workflow map:**
```python
WORKFLOW_MAP = {
    "employee_onboarding": OnboardingWorkflow,
}
```

---

## Kafka Topics

### Consumed

| Topic | Consumer group | Component |
|---|---|---|
| `nexus.cdm.extension_proposed` | `m4-cdm-governance` | CDM Governance Consumer |
| `m1.int.mapping_failed` | `m4-mapping-exceptions` | Mapping Exception Consumer |
| `nexus.cdm.version_published` | `m4-cdm-version-listener` | CDM Version Published Consumer |
| `{tid}.m2.workflow_trigger` | `m4-workflow-triggers` | Workflow Trigger Consumer |

### Produced

| Topic | Component | When |
|---|---|---|
| `{tid}.m4.workflow_completed` | Workflow Trigger Consumer | When Temporal workflow finishes |

Note: `nexus.m4.governance_escalation` is produced by the Airflow SLA Monitor DAG, not this service.

---

## Storage Dependencies

| Store | Table | Operations | Isolation |
|---|---|---|---|
| PostgreSQL | `nexus_system.governance_queue` | INSERT | RLS |
| PostgreSQL | `nexus_system.mapping_review_queue` | INSERT / ON CONFLICT UPDATE | RLS + unique key includes `tenant_id` |
| PostgreSQL | `nexus_system.dag_run_log` | R (idempotency check) | RLS |
| Temporal | Workflow engine | Start workflows, poll status | Workflow ID prefixed with `tenant_id` |
| M4 API (internal HTTP) | `/api/v1/workflows/dag-trigger` | POST | Internal call with `X-Tenant-ID` header |

---

## Kubernetes Manifests

### Deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: nexus-m4-worker
  namespace: nexus-app
  labels:
    app: nexus-m4-worker
    module: m4
    type: worker
spec:
  replicas: 1
  selector:
    matchLabels:
      app: nexus-m4-worker
  template:
    metadata:
      labels:
        app: nexus-m4-worker
        module: m4
      annotations:
        prometheus.io/scrape: "true"
        prometheus.io/port: "9092"
        prometheus.io/path: "/metrics"
    spec:
      serviceAccountName: nexus-m4-worker-sa
      automountServiceAccountToken: false
      terminationGracePeriodSeconds: 60
      containers:
        - name: m4-worker
          image: nexus/m4-worker:latest
          command: ["python", "-m", "m4.worker.entrypoint"]
          ports:
            - containerPort: 9092
              name: metrics
          env:
            - name: POSTGRES_DSN
              valueFrom:
                secretKeyRef:
                  name: nexus-postgres-credentials
                  key: dsn
            - name: KAFKA_BOOTSTRAP_SERVERS
              valueFrom:
                configMapKeyRef:
                  name: nexus-platform-config
                  key: kafka_bootstrap_servers
            - name: TEMPORAL_HOST
              value: "nexus-temporal.nexus-data.svc.cluster.local:7233"
            - name: M4_API_BASE_URL
              value: "http://nexus-m4-api.nexus-app.svc.cluster.local:8002"
          resources:
            requests:
              cpu: 200m
              memory: 256Mi
            limits:
              cpu: 1000m
              memory: 512Mi
          livenessProbe:
            exec:
              command: ["python", "-c", "import m4.worker.health; m4.worker.health.check()"]
            initialDelaySeconds: 20
            periodSeconds: 30
            failureThreshold: 3
```

### KEDA ScaledObject

```yaml
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: nexus-m4-worker-scaler
  namespace: nexus-app
spec:
  scaleTargetRef:
    name: nexus-m4-worker
  minReplicaCount: 1
  maxReplicaCount: 3
  cooldownPeriod: 300
  triggers:
    - type: kafka
      metadata:
        bootstrapServers: nexus-kafka-kafka-bootstrap.nexus-data.svc.cluster.local:9092
        consumerGroup: m4-cdm-governance
        topic: nexus.cdm.extension_proposed
        lagThreshold: "20"
    - type: kafka
      metadata:
        bootstrapServers: nexus-kafka-kafka-bootstrap.nexus-data.svc.cluster.local:9092
        consumerGroup: m4-mapping-exceptions
        topic: m1.int.mapping_failed
        lagThreshold: "100"
```

### NetworkPolicy

```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: nexus-m4-worker-netpol
  namespace: nexus-app
spec:
  podSelector:
    matchLabels:
      app: nexus-m4-worker
  policyTypes:
    - Ingress
    - Egress
  ingress:
    - from:
        - namespaceSelector:
            matchLabels:
              name: nexus-monitoring
      ports:
        - port: 9092
  egress:
    - to:
        - namespaceSelector:
            matchLabels:
              name: nexus-data
      ports:
        - port: 5432    # PostgreSQL
        - port: 9092    # Kafka (note: same port as metrics, different context)
        - port: 7233    # Temporal gRPC
    - to:
        - podSelector:
            matchLabels:
              app: nexus-m4-api    # Internal DAG trigger call
      ports:
        - port: 8002
```

---

## Environment Variables

| Variable | Source | Description |
|---|---|---|
| `POSTGRES_DSN` | Secrets Manager | PostgreSQL DSN |
| `KAFKA_BOOTSTRAP_SERVERS` | ConfigMap | Broker addresses |
| `TEMPORAL_HOST` | ConfigMap | `nexus-temporal.nexus-data.svc.cluster.local:7233` |
| `M4_API_BASE_URL` | ConfigMap | Internal M4 API URL for DAG trigger calls |
| `CDM_BACKFILL_ON_EVERY_VERSION` | ConfigMap | `true` for Iteration 1 (backfill on every CDM change) |

---

## Observability

### Prometheus Metrics

| Metric | Type | Labels |
|---|---|---|
| `m4_worker_governance_proposals_stored_total` | Counter | `tenant_id` |
| `m4_worker_mapping_exceptions_stored_total` | Counter | `tenant_id`, `source_system` |
| `m4_worker_mapping_exceptions_deduplicated_total` | Counter | `tenant_id` |
| `m4_worker_dag_triggers_total` | Counter | `tenant_id`, `dag_id`, `status` |
| `m4_worker_workflows_started_total` | Counter | `tenant_id`, `workflow_type` |
| `m4_worker_kafka_consumer_lag` | Gauge | `consumer_group`, `topic` |

---

## Acceptance Tests

```bash
# Test 1 — CDM proposal stored on kafka event
python3 -c "
from nexus_core.messaging import NexusProducer, NexusMessage
from nexus_core.topics import CrossModuleTopicNamer
p = NexusProducer('$KAFKA_BOOTSTRAP')
p.publish(NexusMessage(
    topic=CrossModuleTopicNamer.CDM.EXTENSION_PROPOSED,
    tenant_id='test-tenant-alpha',
    payload={
        'cdm_version_from': '1.0',
        'source_system': 'odoo',
        'field_proposals': [
            {'source_table': 'res.partner', 'source_field': 'vat',
             'cdm_entity': 'party', 'cdm_field': 'tax_id', 'confidence': 0.97}
        ],
    }
))"

# After ~5s:
psql $POSTGRES_DSN -c "
  SELECT tenant_id, status, submitted_at
  FROM nexus_system.governance_queue
  WHERE tenant_id = 'test-tenant-alpha';"
# Expected: one row, status='pending'

# Test 2 — Mapping exception deduplication across occurrences
# Publish the same mapping_failed event 5 times for same (tenant, source_field)
for i in {1..5}; do python3 tests/publish_mapping_failed.py --tenant test-tenant-alpha; done

psql $POSTGRES_DSN -c "
  SELECT source_field, occurrence_count
  FROM nexus_system.mapping_review_queue
  WHERE tenant_id = 'test-tenant-alpha';"
# Expected: 1 row with occurrence_count = 5 (not 5 separate rows)

# Test 3 — Cross-tenant deduplication does NOT merge
# Publish same mapping_failed for alpha AND beta
python3 tests/publish_mapping_failed.py --tenant test-tenant-alpha
python3 tests/publish_mapping_failed.py --tenant test-tenant-beta

psql $POSTGRES_DSN -c "
  SELECT tenant_id, source_field, occurrence_count
  FROM nexus_system.mapping_review_queue;"
# Expected: 2 rows — one per tenant — with occurrence_count = 1 each

# Test 4 — CDM version published triggers backfill DAG (idempotent)
# Approve a proposal via M4 API → triggers nexus.cdm.version_published
# Worker should call DAG trigger API and log "Backfill DAG triggered"
kubectl logs -n nexus-app deployment/nexus-m4-worker | grep "Backfill DAG triggered"

# Re-publish same nexus.cdm.version_published with same correlation_id
# Worker should log "Idempotent skip: DAG already triggered"
kubectl logs -n nexus-app deployment/nexus-m4-worker | grep "Idempotent skip"

# Test 5 — Workflow trigger starts Temporal workflow
python3 tests/publish_workflow_trigger.py \
  --tenant test-tenant-alpha \
  --type employee_onboarding \
  --context '{"full_name": "Alice Martin", "email": "alice@acme.be"}'

# Check Temporal UI (temporal.nexus.internal):
# - Workflow ID starts with "test-tenant-alpha-onboarding-"
# - Status: Running → Completed
# - 4 activities visible: create_it_account, create_hr_record, assign_equipment, send_welcome_email
```

---

*nexus-m4-worker · Service Specification · Mentis Consulting · March 2026*
