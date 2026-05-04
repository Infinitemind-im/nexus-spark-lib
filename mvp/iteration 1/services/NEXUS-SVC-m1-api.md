# Service Spec: `nexus-m1-api`
**Module:** M1 — Data Intelligence & Mediation
**Type:** Request-driven API · Kubernetes `Deployment`
**Team:** Data Intelligence
**Version:** 1.0 · March 2026 · Confidential

---

## Purpose

Thin HTTP surface for connector lifecycle management. Registers connectors, triggers syncs, and exposes sync job history. Does **not** query source systems, process records, or make LLM calls. All heavy work is delegated to `nexus-m1-executor` via Kafka.

---

## Multi-Tenancy Model

### Tenant Identity Source
`tenant_id` is **always** sourced from the `X-Tenant-ID` HTTP header, which Kong injects after validating the inbound JWT. It is never read from the request body, URL path, or query string.

```python
# Every endpoint signature looks like this — never derive tenant_id from elsewhere
@router.post("/api/v1/connectors")
async def register_connector(
    body: ConnectorRegistrationRequest,
    x_tenant_id: str = Header(...),   # Kong-injected — authoritative
):
    # body.tenant_id, if present, is ignored
    tenant_id = x_tenant_id
```

### Database Isolation
All PostgreSQL access goes through `get_tenant_scoped_connection(pool, tenant_id)` from `nexus_core.db`. This sets `nexus.current_tenant_id` on the connection, activating RLS. Unscoped `pool.acquire()` calls are a standards violation that will fail code review.

```python
async with get_tenant_scoped_connection(pool, tenant_id) as conn:
    # RLS ensures only this tenant's connectors are returned
    # even if the WHERE clause is accidentally omitted
    rows = await conn.fetch("SELECT * FROM nexus_system.connectors")
```

### Tenant Validation on Write
Before inserting a connector record, the service verifies the tenant is `active` via `is_active_tenant(tenant_id)`. A connector for a `provisioning` or `suspended` tenant must be rejected with HTTP 409.

### Two-Tenant Test Rule
Every integration test must use `test-tenant-alpha` and `test-tenant-beta`. A test that creates a connector for alpha must assert that a query scoped to beta returns zero connectors.

---

## Endpoints

| Method | Path | Auth | Description |
|---|---|---|---|
| `POST` | `/api/v1/connectors` | JWT | Register a new connector |
| `GET` | `/api/v1/connectors` | JWT | List connectors for authenticated tenant |
| `GET` | `/api/v1/connectors/{connector_id}` | JWT | Get connector status and metadata |
| `PATCH` | `/api/v1/connectors/{connector_id}` | JWT | Update connector config (schedule, enabled) |
| `DELETE` | `/api/v1/connectors/{connector_id}` | JWT | Soft-delete (sets `status = 'disabled'`) |
| `POST` | `/api/v1/connectors/{connector_id}/sync` | JWT | Trigger manual sync — publishes `m1.int.sync_requested` |
| `GET` | `/api/v1/connectors/{connector_id}/sync-jobs` | JWT | List sync job history (paginated) |
| `GET` | `/api/v1/connectors/{connector_id}/sync-jobs/{job_id}` | JWT | Sync job detail + error log |
| `GET` | `/health` | None | Liveness + readiness |
| `GET` | `/metrics` | Internal | Prometheus metrics (Kubernetes scrape only) |

### Request / Response Contract — `POST /api/v1/connectors`

```json
// Request body
{
  "connector_name": "Acme Salesforce CRM",
  "system_type": "salesforce",            // salesforce | odoo | servicenow | postgresql | mysql | sqlserver
  "credentials_secret_path": "nexus/tenants/acme-corp/salesforce/credentials",
  "sync_schedule": "0 2 * * *",           // cron — UTC
  "enabled": true,
  "config": {
    "objects": ["Lead", "Contact", "Account", "Opportunity"],
    "include_deleted": false
  }
}

// Response 201
{
  "connector_id": "a3f9c1d2-...",
  "tenant_id": "acme-corp",
  "status": "registered",
  "created_at": "2026-03-09T10:00:00Z"
}
```

### Error Codes

| HTTP | Condition |
|---|---|
| 400 | Invalid `system_type`, missing required field, invalid cron expression |
| 403 | `X-Tenant-ID` mismatch or tenant not authorised for this operation |
| 404 | Connector not found for this tenant |
| 409 | Tenant not active (status = `provisioning` or `suspended`) |
| 409 | Duplicate connector (same `system_type` + `credentials_secret_path`) |
| 502 | Failed to publish to Kafka on sync trigger |

---

## Kafka Topics

### Produced

| Topic | When | Key |
|---|---|---|
| `m1.int.sync_requested` | On `POST /connectors/{id}/sync` or Airflow schedule trigger | `connector_id` |

Payload structure:
```json
{
  "connector_id": "a3f9c1d2-...",
  "tenant_id": "acme-corp",
  "system_type": "salesforce",
  "credentials_secret_path": "nexus/tenants/acme-corp/salesforce/credentials",
  "sync_mode": "incremental",
  "triggered_by": "manual | schedule | airflow",
  "requested_at": "2026-03-09T10:00:00Z"
}
```

### Consumed
None. This is a pure HTTP service.

---

## Storage Dependencies

| Store | Table / Path | Operations | Isolation |
|---|---|---|---|
| PostgreSQL | `nexus_system.connectors` | R/W | RLS — `tenant_id = current_setting(...)` |
| PostgreSQL | `nexus_system.sync_jobs` | R | RLS |
| PostgreSQL | `nexus_system.tenants` | R (status check) | Direct query — superuser read for validation only |

### Schema Reference

```sql
-- nexus_system.connectors (Tech Lead DDL)
connector_id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
tenant_id                TEXT NOT NULL,
connector_name           TEXT NOT NULL,
system_type              TEXT NOT NULL,
credentials_secret_path  TEXT NOT NULL,
sync_schedule            TEXT,          -- cron expression
enabled                  BOOLEAN DEFAULT true,
status                   TEXT DEFAULT 'registered',  -- registered | active | error | disabled
config                   JSONB,
created_at               TIMESTAMPTZ DEFAULT NOW(),
updated_at               TIMESTAMPTZ DEFAULT NOW()
```

---

## Kubernetes Manifests

### Deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: nexus-m1-api
  namespace: nexus-app
  labels:
    app: nexus-m1-api
    module: m1
    type: api
spec:
  replicas: 1
  selector:
    matchLabels:
      app: nexus-m1-api
  template:
    metadata:
      labels:
        app: nexus-m1-api
        module: m1
      annotations:
        prometheus.io/scrape: "true"
        prometheus.io/port: "8001"
        prometheus.io/path: "/metrics"
    spec:
      serviceAccountName: nexus-m1-api-sa
      automountServiceAccountToken: false
      containers:
        - name: m1-api
          image: nexus/m1-api:latest
          command: ["uvicorn", "m1.api.main:app", "--host", "0.0.0.0", "--port", "8001", "--workers", "2"]
          ports:
            - containerPort: 8001
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
            - name: LOG_LEVEL
              value: "INFO"
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
              port: 8001
            initialDelaySeconds: 10
            periodSeconds: 15
            failureThreshold: 3
          readinessProbe:
            httpGet:
              path: /health
              port: 8001
            initialDelaySeconds: 5
            periodSeconds: 10
            failureThreshold: 2
---
apiVersion: v1
kind: Service
metadata:
  name: nexus-m1-api
  namespace: nexus-app
spec:
  selector:
    app: nexus-m1-api
  ports:
    - port: 8001
      targetPort: 8001
      name: http
```

### HorizontalPodAutoscaler

```yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: nexus-m1-api-hpa
  namespace: nexus-app
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: nexus-m1-api
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
  name: nexus-m1-api-netpol
  namespace: nexus-app
spec:
  podSelector:
    matchLabels:
      app: nexus-m1-api
  policyTypes:
    - Ingress
    - Egress
  ingress:
    - from:
        - namespaceSelector:
            matchLabels:
              name: nexus-infra   # Kong only
      ports:
        - port: 8001
  egress:
    - to:
        - namespaceSelector:
            matchLabels:
              name: nexus-data    # PostgreSQL + Kafka
      ports:
        - port: 5432    # PostgreSQL
        - port: 9092    # Kafka
    # Block all outbound to source systems — nexus-m1-worker handles that
```

### ServiceAccount & RBAC

```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: nexus-m1-api-sa
  namespace: nexus-app
  annotations:
    eks.amazonaws.com/role-arn: arn:aws:iam::ACCOUNT:role/nexus-m1-api-role
---
# IAM role trust policy grants access to:
# - nexus/platform/postgres/* (read DSN)
# - nexus/platform/kafka/*    (read bootstrap config)
# NOT granted:
# - nexus/tenants/* (source system credentials — only nexus-m1-worker-sa gets these)
```

---

## Environment Variables

| Variable | Source | Description |
|---|---|---|
| `POSTGRES_DSN` | Secrets Manager → K8s Secret | PostgreSQL connection string (`nexus_app` role) |
| `KAFKA_BOOTSTRAP_SERVERS` | ConfigMap | Kafka broker addresses |
| `LOG_LEVEL` | ConfigMap | `INFO` in production, `DEBUG` in dev |
| `TENANT_CACHE_TTL_SECONDS` | ConfigMap | How long to cache tenant status checks (default: 60) |

---

## Observability

### Prometheus Metrics (exposed on `/metrics`)

| Metric | Type | Labels |
|---|---|---|
| `m1_api_connector_registrations_total` | Counter | `tenant_id`, `system_type`, `status` |
| `m1_api_sync_triggers_total` | Counter | `tenant_id`, `trigger_source` |
| `m1_api_http_request_duration_seconds` | Histogram | `method`, `path`, `status_code` |
| `m1_api_db_query_duration_seconds` | Histogram | `operation` |

### Structured Log Fields (structlog)
Every log entry must include: `tenant_id`, `connector_id` (when applicable), `request_id`, `method`, `path`.

```python
logger.info(
    "connector_registered",
    tenant_id=tenant_id,
    connector_id=str(connector_id),
    system_type=body.system_type,
)
```

---

## Acceptance Tests

```bash
# Test 1 — Register connector for tenant alpha
curl -X POST https://api.nexus.internal/api/v1/connectors \
  -H "Authorization: Bearer $ALPHA_JWT" \
  -H "Content-Type: application/json" \
  -d '{"connector_name":"Alpha Salesforce","system_type":"salesforce","credentials_secret_path":"nexus/tenants/alpha/sf/creds"}'
# Expected: 201, connector_id returned

# Test 2 — Alpha cannot see beta's connectors
curl https://api.nexus.internal/api/v1/connectors \
  -H "Authorization: Bearer $ALPHA_JWT" | jq '.connectors | length'
# Expected: 1 (alpha's only)

curl https://api.nexus.internal/api/v1/connectors \
  -H "Authorization: Bearer $BETA_JWT" | jq '.connectors | length'
# Expected: 0 (beta has no connectors yet)

# Test 3 — Manual sync trigger publishes to Kafka
curl -X POST https://api.nexus.internal/api/v1/connectors/$CONNECTOR_ID/sync \
  -H "Authorization: Bearer $ALPHA_JWT"
# Expected: 202, Kafka UI shows m1.int.sync_requested with tenant_id=test-tenant-alpha

# Test 4 — Inactive tenant rejected
# Suspend alpha in DB, attempt registration → Expected: 409

# Test 5 — Health check
curl https://api.nexus.internal/health
# Expected: {"status":"ok","service":"nexus-m1-api"}
```

---

*nexus-m1-api · Service Specification · Mentis Consulting · March 2026*
