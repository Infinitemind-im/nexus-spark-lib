# Service Spec: `nexus-m4-api`
**Module:** M4 — Workflow & Integration
**Type:** Request-driven API · Kubernetes `Deployment`
**Team:** Product
**Version:** 1.0 · March 2026 · Confidential

---

## Purpose

HTTP surface for all human-in-the-loop governance actions and workflow management. Serves data stewards (CDM governance decisions) and process managers (DAG run history, Temporal workflow state). All sensitive operations are tenant-scoped via Kong-injected JWT headers. No LLM calls. No direct source system access.

---

## Multi-Tenancy Model

### Tenant Identity Source
`tenant_id` always comes from the `X-Tenant-ID` header injected by Kong. It is never read from the request body, URL path, or any application-layer decoding of the JWT.

```python
# All governance endpoints follow this pattern
@router.post("/api/v1/governance/proposals/{proposal_id}/approve")
async def approve_proposal(
    proposal_id: str,
    body: ApproveProposalBody,
    x_tenant_id: str = Header(...),   # Kong-injected — authoritative
    x_user_id:   str = Header(...),   # For audit trail
    x_user_email: str = Header(...),  # For notification
):
    tenant_id = x_tenant_id
    # body never contains tenant_id — it would be ignored if present
```

### Database Isolation
Every query that reads or writes tenant-owned data goes through `get_tenant_scoped_connection(pool, tenant_id)`. RLS is the backstop: even if a developer accidentally omits a `WHERE tenant_id = $1` clause, the policy prevents cross-tenant data access.

The four tables managed by this service all have RLS policies:
- `nexus_system.governance_queue`
- `nexus_system.mapping_review_queue`
- `nexus_system.cdm_versions`
- `nexus_system.cdm_mappings`
- `nexus_system.dag_run_log`

### Airflow DAG Trigger — Cross-Tenant Protection
The `/api/v1/workflows/dag-trigger` endpoint enforces a hard rule: `body.tenant_id` must match `x_tenant_id` (the Kong-authenticated tenant). A tenant cannot trigger a DAG for another tenant's data, even if they know the other tenant's ID.

```python
if body.tenant_id != x_tenant_id:
    raise HTTPException(
        403,
        f"You ({x_tenant_id}) cannot trigger a DAG for tenant {body.tenant_id}."
    )
```

Additionally, only a whitelist of DAG IDs can be triggered. The whitelist is in application code (not a DB table) so that changing it requires a PR review rather than a runtime config change.

```python
PERMITTED_DAG_IDS = {
    "m4_cdm_reprocess_trigger",
    "m1_sync_orchestrator",
}
```

### CDM Version Management — Atomicity
When a CDM proposal is approved, the service must atomically:
1. Deprecate the current active CDM version
2. Insert the new version
3. Insert approved field mappings
4. Mark the proposal as approved
5. Publish `nexus.cdm.version_published` to Kafka

All five steps happen inside a single PostgreSQL transaction. If Kafka publication fails after the DB commit, the worker retries publication on restart (the event is idempotent — `ON CONFLICT DO NOTHING` on CDM mappings). If the DB transaction fails, the proposal remains `pending` and no Kafka event is published.

```python
async with get_tenant_scoped_connection(pool, x_tenant_id) as conn:
    async with conn.transaction():
        await _deprecate_current_version(conn, x_tenant_id)
        new_version = await _insert_new_version(conn, x_tenant_id, body)
        await _insert_approved_mappings(conn, x_tenant_id, new_version, proposal_payload)
        await _mark_proposal_approved(conn, proposal_id, body)
    # Transaction committed — now publish to Kafka
    await _publish_version_published(x_tenant_id, new_version)
```

### Audit Trail
Every governance action writes an audit entry with: `tenant_id`, `user_id`, `user_email`, `action`, `proposal_id` or `review_id`, `timestamp`. These records are append-only and protected by a separate RLS policy that allows INSERT but prevents UPDATE or DELETE from the `nexus_app` role.

---

## Endpoints

### CDM Governance

| Method | Path | Description |
|---|---|---|
| `GET` | `/api/v1/governance/proposals` | List proposals. `?status=pending\|approved\|rejected` |
| `GET` | `/api/v1/governance/proposals/{id}` | Proposal detail with full field proposal list |
| `POST` | `/api/v1/governance/proposals/{id}/approve` | Approve — bumps CDM version, inserts Tier 1 mappings |
| `POST` | `/api/v1/governance/proposals/{id}/reject` | Reject — no CDM change, logs rejection reason |

### Mapping Exception Review

| Method | Path | Description |
|---|---|---|
| `GET` | `/api/v1/mappings/review` | List mapping exceptions. `?status=pending\|approved\|rejected&priority=high` |
| `GET` | `/api/v1/mappings/review/{id}` | Exception detail with occurrence count and sample values |
| `POST` | `/api/v1/mappings/review/{id}/approve` | Approve — promotes to Tier 1 in `cdm_mappings`, publishes `mapping_approved` |
| `POST` | `/api/v1/mappings/review/{id}/reject` | Reject — marks as rejected, CDM field stays unmapped |
| `POST` | `/api/v1/mappings/review/{id}/override` | Override CDM field assignment (reviewer manually specifies target field) |

### Airflow Orchestration Bridge (from P6-M4-04)

| Method | Path | Description |
|---|---|---|
| `POST` | `/api/v1/workflows/dag-trigger` | Trigger a permitted Airflow DAG for this tenant |
| `GET` | `/api/v1/workflows/dag-runs` | List DAG run history. `?dag_id=...&status=...` |

### Temporal Workflow Proxy

| Method | Path | Description |
|---|---|---|
| `GET` | `/api/v1/workflows/temporal` | List active and recent Temporal workflows for this tenant |
| `GET` | `/api/v1/workflows/temporal/{workflow_id}` | Temporal workflow status and activity history |

### Meta

| Method | Path | Description |
|---|---|---|
| `GET` | `/health` | Liveness + readiness |
| `GET` | `/metrics` | Prometheus metrics |

---

## Kafka Topics

### Produced

| Topic | When |
|---|---|
| `nexus.cdm.version_published` | On CDM proposal approval |
| `nexus.cdm.extension_rejected` | On CDM proposal rejection |
| `{tid}.m4.mapping_approved` | On mapping exception approval |

### Consumed
None directly. All governance events are consumed by `nexus-m4-worker`. This service only produces.

---

## Storage Dependencies

| Store | Table | Operations | Isolation |
|---|---|---|---|
| PostgreSQL | `nexus_system.governance_queue` | R/W | RLS |
| PostgreSQL | `nexus_system.mapping_review_queue` | R/W | RLS |
| PostgreSQL | `nexus_system.cdm_versions` | R/W | RLS |
| PostgreSQL | `nexus_system.cdm_mappings` | R/W | RLS |
| PostgreSQL | `nexus_system.dag_run_log` | R/W | RLS |
| PostgreSQL | `nexus_system.audit_log` | W (append-only) | RLS + no UPDATE/DELETE policy |
| Airflow REST API | `/api/v1/dags/{id}/dagRuns` | W | Internal cluster only |
| Temporal gRPC | Workflow state queries | R | Internal cluster only |

---

## Kubernetes Manifests

### Deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: nexus-m4-api
  namespace: nexus-app
  labels:
    app: nexus-m4-api
    module: m4
    type: api
spec:
  replicas: 1
  selector:
    matchLabels:
      app: nexus-m4-api
  template:
    metadata:
      labels:
        app: nexus-m4-api
        module: m4
      annotations:
        prometheus.io/scrape: "true"
        prometheus.io/port: "8002"
        prometheus.io/path: "/metrics"
    spec:
      serviceAccountName: nexus-m4-api-sa
      automountServiceAccountToken: false
      containers:
        - name: m4-api
          image: nexus/m4-api:latest
          command: ["uvicorn", "m4.api.main:app", "--host", "0.0.0.0", "--port", "8002", "--workers", "2"]
          ports:
            - containerPort: 8002
              name: http
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
            - name: AIRFLOW_BASE_URL
              value: "http://nexus-airflow-webserver.nexus-data.svc.cluster.local:8080"
            - name: AIRFLOW_USERNAME
              valueFrom:
                secretKeyRef:
                  name: nexus-airflow-credentials
                  key: username
            - name: AIRFLOW_PASSWORD
              valueFrom:
                secretKeyRef:
                  name: nexus-airflow-credentials
                  key: password
            - name: TEMPORAL_HOST
              value: "nexus-temporal.nexus-data.svc.cluster.local:7233"
          resources:
            requests:
              cpu: 100m
              memory: 128Mi
            limits:
              cpu: 500m
              memory: 256Mi
          livenessProbe:
            httpGet:
              path: /health
              port: 8002
            initialDelaySeconds: 10
            periodSeconds: 15
          readinessProbe:
            httpGet:
              path: /health
              port: 8002
            initialDelaySeconds: 5
            periodSeconds: 10
---
apiVersion: v1
kind: Service
metadata:
  name: nexus-m4-api
  namespace: nexus-app
spec:
  selector:
    app: nexus-m4-api
  ports:
    - port: 8002
      targetPort: 8002
      name: http
```

### HPA

```yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: nexus-m4-api-hpa
  namespace: nexus-app
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: nexus-m4-api
  minReplicas: 1
  maxReplicas: 3
  metrics:
    - type: Resource
      resource:
        name: cpu
        target:
          type: Utilization
          averageUtilization: 60
```

### NetworkPolicy

```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: nexus-m4-api-netpol
  namespace: nexus-app
spec:
  podSelector:
    matchLabels:
      app: nexus-m4-api
  policyTypes:
    - Ingress
    - Egress
  ingress:
    - from:
        - namespaceSelector:
            matchLabels:
              name: nexus-infra    # Kong
      ports:
        - port: 8002
    - from:
        - namespaceSelector:
            matchLabels:
              name: nexus-monitoring
      ports:
        - port: 8002
    - from:
        - podSelector:
            matchLabels:
              app: nexus-m4-worker   # Internal call from CDM version consumer
      ports:
        - port: 8002
  egress:
    - to:
        - namespaceSelector:
            matchLabels:
              name: nexus-data
      ports:
        - port: 5432    # PostgreSQL
        - port: 9092    # Kafka
        - port: 8080    # Airflow webserver
        - port: 7233    # Temporal gRPC
```

---

## Environment Variables

| Variable | Source | Description |
|---|---|---|
| `POSTGRES_DSN` | Secrets Manager | PostgreSQL DSN |
| `KAFKA_BOOTSTRAP_SERVERS` | ConfigMap | Broker addresses |
| `AIRFLOW_BASE_URL` | ConfigMap | Internal Airflow URL |
| `AIRFLOW_USERNAME` | Secrets Manager | Airflow service account |
| `AIRFLOW_PASSWORD` | Secrets Manager | Airflow service account |
| `TEMPORAL_HOST` | ConfigMap | Internal Temporal gRPC address |

---

## Observability

### Prometheus Metrics

| Metric | Type | Labels |
|---|---|---|
| `m4_api_proposals_reviewed_total` | Counter | `tenant_id`, `action` (approved/rejected) |
| `m4_api_mappings_reviewed_total` | Counter | `tenant_id`, `action` |
| `m4_api_dag_triggers_total` | Counter | `tenant_id`, `dag_id`, `status` |
| `m4_api_cdm_version_bumps_total` | Counter | `tenant_id` |
| `m4_api_http_request_duration_seconds` | Histogram | `method`, `path`, `status_code` |
| `m4_api_governance_queue_depth` | Gauge | `tenant_id`, `queue` (proposals/mappings) |

---

## Acceptance Tests

```bash
# Test 1 — CDM proposal approval creates new CDM version
curl -X POST https://api.nexus.internal/api/v1/governance/proposals/$PROPOSAL_ID/approve \
  -H "Authorization: Bearer $ALPHA_JWT" \
  -H "Content-Type: application/json" \
  -d '{"reviewed_by": "data.steward@acme.be", "notes": "Confirmed mapping"}'
# Expected: {"new_cdm_version": "1.1", "status": "approved"}

psql $POSTGRES_DSN -c "
  SELECT version, status FROM nexus_system.cdm_versions
  WHERE tenant_id = 'test-tenant-alpha' ORDER BY published_at DESC LIMIT 2;"
# Expected: 1.1 active, 1.0 deprecated

# Test 2 — Cross-tenant governance access denied
# Beta tries to approve alpha's proposal
curl -X POST https://api.nexus.internal/api/v1/governance/proposals/$ALPHA_PROPOSAL_ID/approve \
  -H "Authorization: Bearer $BETA_JWT" \
  -H "Content-Type: application/json" \
  -d '{"reviewed_by": "attacker@beta.be"}'
# Expected: 404 (RLS makes proposal invisible to beta — not 403, which would confirm existence)

# Test 3 — DAG trigger cross-tenant blocked
curl -X POST https://api.nexus.internal/api/v1/workflows/dag-trigger \
  -H "Authorization: Bearer $ALPHA_JWT" \
  -H "Content-Type: application/json" \
  -d '{"dag_id": "m4_cdm_reprocess_trigger", "tenant_id": "test-tenant-beta", "params": {}}'
# Expected: 403 — tenant_id mismatch

# Test 4 — Unpermitted DAG blocked
curl -X POST https://api.nexus.internal/api/v1/workflows/dag-trigger \
  -H "Authorization: Bearer $ALPHA_JWT" \
  -H "Content-Type: application/json" \
  -d '{"dag_id": "arbitrary_dag", "tenant_id": "test-tenant-alpha", "params": {}}'
# Expected: 403 — dag_id not in PERMITTED_DAG_IDS

# Test 5 — Audit trail written on every approval
psql $POSTGRES_DSN -c "
  SELECT action, user_id, tenant_id, created_at
  FROM nexus_system.audit_log
  WHERE tenant_id = 'test-tenant-alpha'
  ORDER BY created_at DESC LIMIT 5;"
# Expected: rows with action='cdm_proposal_approved'
```

---

*nexus-m4-api · Service Specification · Mentis Consulting · March 2026*
