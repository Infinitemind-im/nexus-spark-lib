# Service Spec: `nexus-schema-profiler`
**Module:** M1 — Data Intelligence & Mediation
**Type:** Scheduled / triggered job · Kubernetes `Job` + Airflow DAG
**Team:** Data Intelligence
**Version:** 1.0 · March 2026 · Confidential

---

## Purpose

Extracts schema metadata from source systems (column names, types, cardinality, null rates) and detects schema drift between connector registrations. When new or changed fields are detected, it triggers the M2 Structural Agent via Kafka to propose CDM extensions.

This is **not a long-running Deployment**. It starts, runs for a bounded time per connector, writes results, and exits. Running it as a persistent process would waste resources, complicate health checks, and create a false sense of availability for a component that does nothing between runs.

---

## Multi-Tenancy Model

### One Job Instance Per Connector
Each Airflow DAG task instantiates one Kubernetes Job targeting **exactly one connector** for **exactly one tenant**. The `tenant_id` and `connector_id` are passed as Job environment variables from the Airflow task context — never derived from introspecting source system data.

```yaml
# Job instantiated per connector — Job name includes connector_id for uniqueness
metadata:
  name: nexus-schema-profiler-{{ connector_id }}
  namespace: nexus-app
```

### Credentials Scoping
The Job fetches credentials from AWS Secrets Manager at path `nexus/tenants/{tenant_id}/{connector_id}/credentials`. A schema profiler job for tenant A cannot access tenant B's credentials — the path is constructed from the `tenant_id` and `connector_id` received from Airflow, and the Job's IAM role has a condition restricting access to paths matching its injected `tenant_id`.

```
# IAM condition on nexus-schema-profiler-role:
# "StringLike": {
#   "secretsmanager:SecretId": "nexus/tenants/${aws:PrincipalTag/tenant_id}/*"
# }
```

### Schema Snapshot Isolation
Schema snapshots are stored in `nexus_system.schema_snapshots` with RLS enforced. When the Job writes a snapshot, it uses `get_tenant_scoped_connection(pool, tenant_id)` with the `tenant_id` from the Job's environment. Even if a code defect omitted the WHERE clause, the RLS policy would prevent cross-tenant snapshot reads.

### Delta Detection — Cross-Tenant Safety
When comparing a new schema snapshot to the previous one, the query explicitly filters by both `tenant_id` and `connector_id`. There is no global schema comparison that could accidentally mix tenants.

```python
prev_snapshot = await conn.fetchrow("""
    SELECT schema_json, snapshot_taken_at
    FROM nexus_system.schema_snapshots
    WHERE tenant_id    = $1    -- from Job env, not inferred
      AND connector_id = $2
    ORDER BY snapshot_taken_at DESC
    LIMIT 1
""", tenant_id, connector_id)
```

---

## Execution Flow

```
1. Airflow DAG: m1_sync_orchestrator starts schema profiler task for connector X
       ↓
2. Airflow creates Kubernetes Job: nexus-schema-profiler-{connector_id}
   (Job env: tenant_id, connector_id, system_type)
       ↓
3. Job fetches connector metadata from PostgreSQL (RLS-scoped)
4. Job fetches source system credentials from Secrets Manager
       ↓
5. Job connects to source system
   - Salesforce/ServiceNow/Odoo: introspects object metadata via API
   - PostgreSQL/MySQL/SQL Server: queries information_schema
       ↓
6. Job computes schema fingerprint (sorted column list + types + nullable)
       ↓
7. Job fetches previous snapshot from nexus_system.schema_snapshots
       ↓
8. If snapshot differs (new columns, type changes, dropped columns) OR no previous snapshot exists:
   - Writes new snapshot to nexus_system.schema_snapshots
   - Publishes m1.int.source_schema_extracted to Kafka
   - Publishes m1.int.structural_cycle_triggered if new/changed fields detected
   - Builds StructuralArtifact from the full schema snapshot (tables, columns, types,
     cardinality stats, detected FK patterns — never raw record values)
   - Publishes {tid}.m1.semantic_interpretation_requested with the StructuralArtifact
     → M2 Pipeline B (m2-structural-agents) picks this up and proposes CDM extensions
     → M2 Pipeline C (m2-schema-narrative-agents) picks this up independently and
       generates a human-readable narrative (table summaries, domain tags, overall summary)
       Both pipelines consume the same topic under separate consumer groups — publishing
       once triggers both without coupling their processing.

9. If no drift detected (and previous snapshot exists):
   - Updates snapshot `last_checked_at` only (no Kafka publish, no LLM delegation)
       ↓
10. Job exits with code 0 (success) or 1 (failure)
    Airflow records outcome and schedules retry if failure
```

---

## LLM Delegation via M2

The schema profiler **never calls an LLM directly.** All natural language interpretation of schemas is delegated to `nexus-m2-executor`, which is the only permitted LLM caller in the platform.

When a new or changed schema is detected, the profiler publishes a `StructuralArtifact` to `{tid}.m1.semantic_interpretation_requested`. This triggers two M2 pipelines that run **sequentially**, not in parallel:

| Order | M2 Pipeline | Trigger topic | What it produces |
|---|---|---|---|
| 1st | Pipeline C — Schema Narrative Agent | `{tid}.m1.semantic_interpretation_requested` | Human-readable schema narrative → `{tid}.m2.schema_narrative_ready` + `nexus_system.schema_narratives` |
| 2nd | Pipeline B — CDM Classifier | `{tid}.m2.schema_narrative_ready` | CDM field mapping proposals → `nexus.cdm.extension_proposed` |

**Why sequential:** Pipeline B's CDM classification quality depends on the semantic context that Pipeline C produces. A field named `ref_ext_1 varchar(50)` is structurally ambiguous; Pipeline C's narrative ("external CRM reference identifier used in customer master data") gives Pipeline B's LLM the context needed to propose a correct CDM mapping rather than falling back to Tier 3. Pipeline C runs first because it is purely generative — no CDM catalogue matching — and completes faster. Pipeline B then combines the `StructuralArtifact` (loaded from `nexus_system.schema_snapshots` via the `snapshot_id` carried in the `schema_narrative_ready` message) with the `SchemaNarrative` to produce enriched CDM proposals.

The profiler publishes to `{tid}.m1.semantic_interpretation_requested` and immediately moves on — it has no awareness of either M2 pipeline's processing outcomes.

### StructuralArtifact Payload

```python
# m1/schema_profiler/models.py
@dataclass
class StructuralArtifact:
    tenant_id:      str
    connector_id:   str
    source_type:    str                  # "salesforce" / "odoo" / "postgresql" / etc.
    snapshot_id:    str                  # UUID of the snapshot in schema_snapshots
    profiled_at:    str                  # ISO 8601 timestamp
    tables: list[TableProfile]

@dataclass
class TableProfile:
    name:           str                  # Table or object name
    row_count_est:  int | None           # Approximate row count (NULL if unavailable)
    columns: list[ColumnProfile]

@dataclass
class ColumnProfile:
    name:           str
    data_type:      str                  # Canonical type string (e.g. "varchar(255)", "int4")
    nullable:       bool
    cardinality_est: int | None          # Approximate distinct value count
    null_rate:      float | None         # Fraction of NULLs in sampled rows (0.0–1.0)
    is_pk:          bool
    fk_target:      str | None           # "table.column" if FK pattern detected, else None
    # NOTE: actual cell values are NEVER included — PromptGuard in M2 enforces this
```

The artifact is serialised as JSON and placed in the Kafka message value. It contains only structural metadata — no record values, no PII, no business content. `PromptGuard` in M2 validates this before any LLM call.

---

## Kafka Topics

### Produced

| Topic | When | Partitions | Consumers |
|---|---|---|---|
| `m1.int.source_schema_extracted` | Every time profiling completes (drift or not) | 4 | Internal M1 audit / monitoring |
| `m1.int.structural_cycle_triggered` | Only when schema drift or new fields detected | 4 | Internal M1 — Connector Worker |
| `{tid}.m1.semantic_interpretation_requested` | On first profile OR when drift detected | 4 | M2 Pipelines B + C (two independent consumer groups) |

`m1.int.structural_cycle_triggered` carries a **delta-only** payload (new/changed/dropped fields) for internal M1 bookkeeping. `{tid}.m1.semantic_interpretation_requested` carries the **full `StructuralArtifact`** — structural metadata about the entire schema — which M2 uses for both CDM classification and narrative generation.

Payload for `structural_cycle_triggered`:
```json
{
  "tenant_id": "acme-corp",
  "connector_id": "a3f9c1d2-...",
  "system_type": "odoo",
  "new_fields": [
    {"table": "res.partner", "column": "vat_country_code", "type": "varchar(2)"}
  ],
  "changed_fields": [],
  "dropped_fields": [],
  "snapshot_id": "snap_2026_03_09_001",
  "profiled_at": "2026-03-09T08:01:32Z"
}
```

Payload for `{tid}.m1.semantic_interpretation_requested` — the full `StructuralArtifact`:
```json
{
  "tenant_id": "acme-corp",
  "connector_id": "a3f9c1d2-...",
  "source_type": "odoo",
  "snapshot_id": "snap_2026_03_09_001",
  "profiled_at": "2026-03-09T08:01:32Z",
  "tables": [
    {
      "name": "res.partner",
      "row_count_est": 12400,
      "columns": [
        {"name": "id",               "data_type": "int4",        "nullable": false, "cardinality_est": 12400, "null_rate": 0.0,  "is_pk": true,  "fk_target": null},
        {"name": "name",             "data_type": "varchar(128)","nullable": false, "cardinality_est": 11900, "null_rate": 0.0,  "is_pk": false, "fk_target": null},
        {"name": "vat_country_code", "data_type": "varchar(2)",  "nullable": true,  "cardinality_est": 32,    "null_rate": 0.44, "is_pk": false, "fk_target": null}
      ]
    }
  ]
}
```

Note: actual cell values are never included. PromptGuard in M2 validates this before any LLM call.

### Consumed
None. The job is triggered by Airflow, not by Kafka messages.

---

## Storage Dependencies

| Store | Table | Operations | Isolation |
|---|---|---|---|
| PostgreSQL | `nexus_system.schema_snapshots` | R/W | RLS-scoped |
| PostgreSQL | `nexus_system.connectors` | R (connector config) | RLS-scoped |
| AWS Secrets Manager | `nexus/tenants/{tenant_id}/{connector_id}/credentials` | R | Path-scoped by tenant_id |

### schema_snapshots table

```sql
CREATE TABLE nexus_system.schema_snapshots (
    snapshot_id       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id         TEXT NOT NULL,
    connector_id      UUID NOT NULL,
    system_type       TEXT NOT NULL,
    schema_json       JSONB NOT NULL,       -- full schema as JSON
    schema_fingerprint TEXT NOT NULL,       -- hash for quick drift detection
    snapshot_taken_at TIMESTAMPTZ DEFAULT NOW(),
    last_checked_at   TIMESTAMPTZ DEFAULT NOW(),
    tier3_field_count INT DEFAULT 0         -- count of fields with no CDM mapping
);
ALTER TABLE nexus_system.schema_snapshots ENABLE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON nexus_system.schema_snapshots
    FOR ALL TO nexus_app
    USING (tenant_id = current_setting('nexus.current_tenant_id', true));
CREATE INDEX idx_schema_snapshots_connector
    ON nexus_system.schema_snapshots (tenant_id, connector_id, snapshot_taken_at DESC);
```

---

## Kubernetes Manifests

### Job Template (instantiated by Airflow)

```yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: nexus-schema-profiler-{{ connector_id }}
  namespace: nexus-app
  labels:
    app: nexus-schema-profiler
    module: m1
    type: job
    tenant-id: "{{ tenant_id }}"      # Used by NetworkPolicy
spec:
  ttlSecondsAfterFinished: 3600       # Clean up completed jobs after 1 hour
  activeDeadlineSeconds: 1800         # Hard 30-minute timeout
  backoffLimit: 2                     # 2 retries on failure
  template:
    metadata:
      labels:
        app: nexus-schema-profiler
        module: m1
    spec:
      serviceAccountName: nexus-schema-profiler-sa
      restartPolicy: OnFailure
      automountServiceAccountToken: false
      containers:
        - name: schema-profiler
          image: nexus/schema-profiler:latest
          command: ["python", "-m", "m1.schema_profiler.main"]
          env:
            - name: TENANT_ID
              value: "{{ tenant_id }}"
            - name: CONNECTOR_ID
              value: "{{ connector_id }}"
            - name: SYSTEM_TYPE
              value: "{{ system_type }}"
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
            - name: AWS_REGION
              value: "eu-west-1"
          resources:
            requests:
              cpu: 200m
              memory: 256Mi
            limits:
              cpu: 1000m
              memory: 512Mi
```

### NetworkPolicy

```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: nexus-schema-profiler-netpol
  namespace: nexus-app
spec:
  podSelector:
    matchLabels:
      app: nexus-schema-profiler
  policyTypes:
    - Ingress
    - Egress
  ingress: []   # No inbound traffic — job only
  egress:
    - to:
        - namespaceSelector:
            matchLabels:
              name: nexus-data
      ports:
        - port: 5432    # PostgreSQL
        - port: 9092    # Kafka
    # Source system outbound
    - to:
        - ipBlock:
            cidr: 0.0.0.0/0
            except:
              - 10.0.0.0/8
      ports:
        - port: 443     # SaaS APIs
        - port: 5432    # External PostgreSQL
        - port: 3306    # External MySQL
        - port: 1433    # External SQL Server
```

### ServiceAccount

```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: nexus-schema-profiler-sa
  namespace: nexus-app
  annotations:
    eks.amazonaws.com/role-arn: arn:aws:iam::ACCOUNT:role/nexus-schema-profiler-role
# IAM conditions on the role restrict Secrets Manager access
# to paths matching the tenant_id tag injected into the Job.
```

---

## Airflow Integration

This job is triggered by two Airflow contexts:

**Context 1 — Connector Registration** (immediate, once)
When `nexus-m1-api` registers a new connector, it publishes `m1.int.sync_requested`. The Connector Worker picks this up and, if it's the first sync, calls the M4 Airflow Bridge to trigger `m1_schema_profiler_initial`:

```python
# m1/worker/connector_worker.py
if is_first_sync:
    await httpx_client.post(
        f"{M4_API_BASE}/api/v1/workflows/dag-trigger",
        json={
            "dag_id": "m1_schema_profiler_initial",
            "tenant_id": message.tenant_id,
            "params": {
                "connector_id": str(connector_id),
                "system_type": system_type,
                "tenant_id": message.tenant_id,
            }
        },
        headers={"X-Tenant-ID": message.tenant_id, "X-User-ID": "nexus-system"},
    )
```

**Context 2 — Scheduled Drift Detection** (weekly)
The `m1_schema_drift_detector` DAG runs weekly at 02:00 UTC. For each active connector across all tenants, it fans out one Kubernetes Job:

```python
# airflow/dags/m1_schema_drift_detector.py
# schedule_interval="0 2 * * 1"  # Every Monday 02:00 UTC

@task
def get_active_connectors():
    """Returns list of {tenant_id, connector_id, system_type} for all active connectors."""
    ...

@task
def trigger_profiler_job(connector: dict):
    """Creates a Kubernetes Job for one connector."""
    ...
```

---

## Environment Variables

| Variable | Source | Description |
|---|---|---|
| `TENANT_ID` | Airflow task params | Tenant this job profiles |
| `CONNECTOR_ID` | Airflow task params | Specific connector to profile |
| `SYSTEM_TYPE` | Airflow task params | `salesforce` / `odoo` / etc. |
| `POSTGRES_DSN` | Secrets Manager | PostgreSQL connection string |
| `KAFKA_BOOTSTRAP_SERVERS` | ConfigMap | Broker addresses |
| `AWS_REGION` | ConfigMap | `eu-west-1` |

---

## Acceptance Tests

```bash
# Test 1 — Schema profiler runs successfully for alpha
# Create connector for test-tenant-alpha, trigger initial profiler job
python tests/trigger_profiler_job.py --tenant test-tenant-alpha --connector $CONNECTOR_ID

# After job completes, verify:
psql $POSTGRES_DSN -c "
  SELECT schema_fingerprint, snapshot_taken_at
  FROM nexus_system.schema_snapshots
  WHERE tenant_id = 'test-tenant-alpha' AND connector_id = '$CONNECTOR_ID';"
# Expected: one row

# Test 2 — Drift detection publishes to Kafka
# Artificially alter schema snapshot to simulate schema change
psql $POSTGRES_DSN -c "
  UPDATE nexus_system.schema_snapshots
  SET schema_json = schema_json || '{\"new_column\": {\"type\": \"varchar\"}}'::jsonb
  WHERE tenant_id = 'test-tenant-alpha';"

# Run profiler again — it should detect the diff
# Expected: m1.int.structural_cycle_triggered appears in Kafka UI with new_fields populated

# Test 3 — Tenant isolation
# Run profilers for both alpha and beta simultaneously
# Verify alpha's snapshots are only visible in alpha scope:
psql $POSTGRES_DSN -c "SET nexus.current_tenant_id = 'test-tenant-beta';
  SELECT COUNT(*) FROM nexus_system.schema_snapshots;"
# Expected: 0 (beta sees only its own snapshots)

# Test 4 — Job timeout
# Point connector at a non-responding source system
# Expected: Job exits with code 1 after 1800s, Airflow marks task as failed, schedules retry

# Test 5 — LLM delegation: semantic_interpretation_requested published on first profile
python tests/trigger_profiler_job.py --tenant test-tenant-alpha --connector $CONNECTOR_ID
# Expected: {tid}.m1.semantic_interpretation_requested appears in Kafka UI
# Verify message contains full StructuralArtifact (tables, columns, types, cardinality)
# Verify no raw record values are present in the payload

# Test 6 — LLM delegation: semantic_interpretation_requested published on drift
# First profile already completed for test-tenant-alpha
# Alter source schema to add a column, re-run profiler
# Expected: {tid}.m1.semantic_interpretation_requested published again with updated StructuralArtifact
# Verify new column appears in the published artifact

# Test 7 — No LLM delegation when no drift
# Run profiler twice on unchanged schema
# Expected: second run does NOT publish {tid}.m1.semantic_interpretation_requested
# (only last_checked_at is updated in schema_snapshots)
```

---

*nexus-schema-profiler · Service Specification · Mentis Consulting · March 2026*
