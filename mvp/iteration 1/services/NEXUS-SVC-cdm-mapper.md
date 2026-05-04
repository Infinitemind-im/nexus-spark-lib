# Service Spec: `nexus-cdm-mapper`
**Module:** M1 — Data Intelligence & Mediation
**Original name:** `nexus-cdm-mapper` (from initial service decomposition — not renamed)
**Type:** Event-driven worker · KEDA scale-to-zero `Deployment`
**Team:** Data Intelligence
**Version:** 1.1 · March 2026 · Confidential

---

## Operational Context

This service has a fundamentally different operational profile from `nexus-m1-executor`:

| | `nexus-m1-executor` | `nexus-cdm-mapper` |
|---|---|---|
| Role | Schema extraction (Docker) + Kafka delegation to M2 for interpretation | CDM field classification + cache management |
| Run frequency | On-demand — triggered by profiling requests | On-demand — triggered by schema snapshots; monthly on CDM version publish |
| Trigger source | Kafka lag on `m1.profiling_requested` | `{tid}.m1.semantic_interpretation_requested` |
| Replicas (idle) | 1 minimum | **0** — scaled to zero when idle |
| Replicas (active) | 1–5 via KEDA | 0–5 via KEDA |
| LLM calls | None — schema artifacts are delegated to nexus-m2-executor via Kafka | None — deterministic confidence scoring against CDM registry only |
| Bottleneck | Docker container I/O | CDM registry lookups |

The CDM mapper is kept separate because it can scale to zero replicas between events. It is only active when new schema snapshots arrive or a CDM version is published — roughly 2–4 hours per month at steady state. Running it as part of the always-on executor would waste compute resources on a process that is idle the vast majority of the time.

---

## Purpose

Classifies schema snapshots against the tenant's Canonical Data Model (CDM) and produces field mapping results for ingestion and governance review. This is a **pure classification service** — it reads schema metadata published by `nexus-m1-executor`, applies deterministic rule-based confidence scoring against the CDM registry, and publishes results. It never connects to source systems directly and makes no LLM calls.

Three consumer loops handle the complete CDM lifecycle:
1. **CDM Mapper** — classifies schema fields into Tier 1 / 2 / 3 and publishes CDM proposals
2. **Cache Invalidator** — flushes in-process CDM registry cache when a mapping is approved
3. **Version Listener** — refreshes the active CDM version pointer on version publish, then triggers reprocessing

---

## Multi-Tenancy Model

### Tenant Identity Source — Kafka Envelope

`tenant_id` is **always** taken from `NexusMessage.tenant_id`. It is never inferred from schema snapshot content, artifact metadata, or CDM registry data.

```python
async def run_loop(topic: str, group: str, handler: Callable) -> None:
    consumer = NexusConsumer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=group,
        topics=[topic],
        tenant_validator=is_active_tenant,
    )
    async for message in consumer:
        set_tenant(TenantContext(tenant_id=message.tenant_id))
        try:
            await handler(message)
            await consumer.commit()
        except Exception as e:
            await send_to_dlq(message, e, topic="m1.int.dead_letter")
            await consumer.commit()
        finally:
            clear_tenant()
```

### CDM Registry Cache — Per-Tenant Isolation

The `CDMRegistryService` in-process cache is keyed by `(tenant_id, source_system, source_table, source_field, cdm_version)`. Two tenants using the same source system (e.g. both on Salesforce) have independent caches and mappings. The `cdm_version` component ensures old entries become stale automatically when a tenant's CDM is promoted.

```python
# Cache key structure — tenant_id is always first
CacheKey = Tuple[str, str, str, str, str]  # (tenant_id, source_system, source_table, field, cdm_version)

def invalidate_tenant(self, tenant_id: str) -> int:
    """Called on {tid}.m4.mapping_approved. Flushes ALL cache entries for this tenant."""
    with self._lock:
        before = len(self._cache)
        self._cache = {k: v for k, v in self._cache.items() if k[0] != tenant_id}
        return before - len(self._cache)
```

### Database Access

All PostgreSQL queries use `get_tenant_scoped_connection(pool, tenant_id)`. RLS on `nexus_system.cdm_mappings` and `nexus_system.cdm_versions` ensures a bug in CDM registry loading cannot serve another tenant's mapping rules.

### Two-Tenant Test Rule

Every integration test must use `test-tenant-alpha` and `test-tenant-beta`. Tests publish interleaved schema snapshots for both tenants and assert that CDM proposals for alpha are never published with `tenant_id = test-tenant-beta` and vice versa.

---

## Event Processing — Inputs, Outputs & Handler Functions

### Full Data Flow

```
EXTERNAL TRIGGER                   KAFKA TOPIC                            HANDLER                           OUTPUT TOPIC(S)
──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────
nexus-m1-executor          ──►  {tid}.m1.semantic_interpretation_  ──►  handle_schema_snapshot()       ──►  {tid}.m2.semantic_interp_complete (Tier 1/2)
  (schema extracted,              requested                                                             ──►  m1.int.mapping_failed             (Tier 2 flag)
   StructuralArtifact built)      (per-tenant, 7-day retention)                                            [Tier 3 → stored in source_extras, no publish]

m4-api (CDM approved)      ──►  {tid}.m4.mapping_approved          ──►  handle_mapping_approved()      ──►  [no publish — cache invalidation only]
                                 (per-tenant, 7-day retention)

m4-worker (CDM promoted)   ──►  nexus.cdm.version_published        ──►  handle_cdm_version_             ──►  {tid}.m1.semantic_interpretation_requested
                                 (platform topic, 30-day)                published()                         (reprocess trigger — Tier 3 backfill)
```

**Note:** `nexus-m1-executor` also publishes `m1.int.schema_extracted` (14-day retention, configurable via `SCHEMA_EXTRACTED_RETENTION_DAYS`). This topic is consumed by `nexus-cdm-platform`'s `M1SchemaListener`, which bridges it to `{tid}.m1.semantic_interpretation_requested` for the CDM mapper to consume.

### Service Entrypoint

`m1/mapper/entrypoint.py` — called by `python -m m1.mapper.entrypoint`. Scale-to-zero means cold start time matters: the pod must be ready to consume within 30 seconds of KEDA requesting scale-up.

```python
# m1/mapper/entrypoint.py
async def main():
    """
    Boots three consumer loops concurrently.
    Pod starts at 0 replicas. KEDA scales up when {tid}.m1.semantic_interpretation_requested
    lag > 0 (triggered after nexus-m1-executor publishes a new schema snapshot).
    """
    await asyncio.gather(
        run_loop(
            topic="{tid}.m1.semantic_interpretation_requested",  # resolved per active tenant
            group="m1-cdm-mapper",
            handler=handle_schema_snapshot,
        ),
        run_loop(
            topic="{tid}.m4.mapping_approved",   # resolved at runtime per active tenant
            group="m1-cache-invalidator",
            handler=handle_mapping_approved,
        ),
        run_loop(
            topic="nexus.cdm.version_published",
            group="m1-cdm-version-listener",
            handler=handle_cdm_version_published,
        ),
    )
```

---

### Handler Functions

#### `handle_schema_snapshot(message: NexusMessage) → None`
**File:** `m1/mapper/handlers.py`
**Triggered by:** `{tid}.m1.semantic_interpretation_requested`
**Consumer group:** `m1-cdm-mapper`

Classifies schema fields from a StructuralArtifact against the tenant's active CDM. The primary processing function — all other handlers exist to keep this one running correctly.

| Step | Action | Helper called |
|---|---|---|
| 1 | Load active CDM version | `cdm_registry.get_active_cdm_version(tenant_id)` |
| 2 | Extract schema tables + fields from artifact payload | `parse_structural_artifact(message.payload)` |
| 3 | For each field, score confidence | `cdm_registry.classify_field(tenant_id, source_system, source_table, field_name)` |
| 4 | Build CDM proposal with mapped fields | `build_cdm_proposal(artifact, mapping_result)` |
| 5 | If Tier 2 fields exist → flag for governance review | `publish_mapping_flag(proposal, tier2_fields)` → `m1.int.mapping_failed` |
| 6 | Publish proposal | `producer.publish("{tid}.m2.semantic_interp_complete", cdm_proposal)` |

**Tier classification rules:**

| Tier | Condition | Action |
|---|---|---|
| 1 | confidence ≥ 0.95 | Map silently — auto-approved in governance UI |
| 2 | 0.70 ≤ confidence < 0.95 | Propose tentatively + publish governance flag — requires human approval |
| 3 | confidence < 0.70 or field unknown | Store in `source_extras` — not mapped, not published |

**CDM proposal examples:**

```python
# Tier 1 proposal — fully classified, auto-approved
CDMFieldProposal(
    artifact_id="abc123",
    tenant_id="acme-corp",
    source_table="SalesOrderHeader",
    source_field="OrderDate",
    cdm_entity="Transaction",
    cdm_field="transaction_date",
    confidence=0.97,
    tier=1,
    source_extras={},
)

# Mixed Tier 1 + Tier 3 proposal — partially classified
CDMFieldProposal(
    artifact_id="abc123",
    tenant_id="acme-corp",
    source_table="SalesOrderHeader",
    source_field="ShipDate",
    cdm_entity="Transaction",
    cdm_field=None,                  # Tier 3 — confidence 0.55
    confidence=0.55,
    tier=3,
    source_extras={
        "SalesOrderHeader.ShipDate": "date",
        "SalesOrderHeader.DueDate": "date",
    }
)
```

---

#### `handle_mapping_approved(message: NexusMessage) → None`
**File:** `m1/mapper/handlers.py`
**Triggered by:** `{tid}.m4.mapping_approved`
**Consumer group:** `m1-cache-invalidator`

Purges stale CDM mapping cache entries for the tenant whose mapping was just approved. Ensures the next schema snapshot processed uses the newly approved field mappings without requiring a pod restart.

```python
async def handle_mapping_approved(message: NexusMessage) -> None:
    n_evicted = cdm_registry.invalidate_tenant(message.tenant_id)
    logger.info(
        f"CDM cache invalidated for tenant={message.tenant_id}: {n_evicted} entries removed"
    )
    # No Kafka publish — side-effect only
```

---

#### `handle_cdm_version_published(message: NexusMessage) → None`
**File:** `m1/mapper/handlers.py`
**Triggered by:** `nexus.cdm.version_published`
**Consumer group:** `m1-cdm-version-listener`

Handles a CDM version promotion event. Two actions:
1. Refreshes the active CDM version pointer so subsequent `handle_schema_snapshot` calls use the new version
2. Triggers reprocessing of Tier 3 fields by re-publishing `{tid}.m1.semantic_interpretation_requested` for each affected connector

```python
async def handle_cdm_version_published(message: NexusMessage) -> None:
    new_version = message.payload["new_cdm_version"]
    old_version = message.payload["old_cdm_version"]

    # 1. Refresh version pointer
    cdm_registry.set_active_version(message.tenant_id, new_version)
    logger.info(f"CDM version updated tenant={message.tenant_id}: v{old_version} → v{new_version}")

    # 2. Trigger Tier 3 backfill — find connectors with Tier 3 fields for this tenant
    affected_connectors = await find_connectors_with_tier3_fields(
        tenant_id=message.tenant_id,
        entity_types=message.payload.get("affected_entity_types", []),
    )

    for connector_id in affected_connectors:
        await producer.publish(
            topic=f"{message.tenant_id}.m1.semantic_interpretation_requested",
            payload=NexusMessage(
                tenant_id=message.tenant_id,
                payload={
                    "connector_id": connector_id,
                    "reprocess_mode": "tier3_backfill",
                    "cdm_version": new_version,
                    "triggered_by": "cdm_version_published",
                },
            )
        )
        logger.info(
            f"Tier 3 backfill triggered: tenant={message.tenant_id} "
            f"connector={connector_id} new_version={new_version}"
        )
```

---

### Shared Helper Functions

These live in `m1/mapper/shared/`:

| Function | File | Purpose |
|---|---|---|
| `parse_structural_artifact(payload)` | `artifact.py` | Extracts tables + fields from StructuralArtifact payload |
| `build_cdm_proposal(artifact, mapping)` | `mapper.py` | Constructs a `CDMFieldProposal` from artifact + mapping result |
| `publish_mapping_flag(proposal, tier2_fields)` | `mapper.py` | Builds and publishes `m1.int.mapping_failed` message for governance queue |
| `find_connectors_with_tier3_fields(tenant_id, ...)` | `db.py` | Queries `nexus_system.cdm_proposals` for connectors with Tier 3 fields |
| `is_active_tenant(tenant_id)` | `validators.py` | Checks `nexus_system.tenants` — used as `NexusConsumer.tenant_validator` |
| `send_to_dlq(message, error, topic)` | `dlq.py` | Publishes poisoned message + error details to `m1.int.dead_letter` |

---

## Kafka Topics

### Consumed

| Topic | Consumer group | Handler |
|---|---|---|
| `{tid}.m1.semantic_interpretation_requested` | `m1-cdm-mapper` | `handle_schema_snapshot()` |
| `{tid}.m4.mapping_approved` | `m1-cache-invalidator` | `handle_mapping_approved()` |
| `nexus.cdm.version_published` | `m1-cdm-version-listener` | `handle_cdm_version_published()` |

### Produced

| Topic | Partitions | Retention | Config key | When |
|---|---|---|---|---|
| `{tid}.m2.semantic_interp_complete` | per-tenant | 7 days | — | Per classified schema snapshot |
| `m1.int.mapping_failed` | 4 | 14 days | — | Per Tier 2 field requiring governance review |
| `{tid}.m1.semantic_interpretation_requested` | per-tenant | 7 days | — | On CDM version publish — Tier 3 backfill |
| `m1.int.dead_letter` | 4 | 30 days | — | On unrecoverable processing error |

**Upstream topic:** `m1.int.schema_extracted` (published by `nexus-m1-executor`) has a retention of `SCHEMA_EXTRACTED_RETENTION_DAYS` days (default: **14**), set in the `nexus-platform-config` ConfigMap. This gives the CDM mapper sufficient time to process schema snapshots even if it has been idle.

---

## Storage Dependencies

| Store | Usage | Access type | Tenant isolation mechanism |
|---|---|---|---|
| PostgreSQL `nexus_system.cdm_mappings` | Read mapping rules per tenant + source field | READ only | RLS + cache keyed by `(tenant_id, ...)` |
| PostgreSQL `nexus_system.cdm_versions` | Read/set active CDM version per tenant | READ only | RLS |
| PostgreSQL `nexus_system.cdm_proposals` | Find connectors with Tier 3 fields | READ only | RLS |

**This service has no MinIO access, no Secrets Manager access, and no outbound network access to source systems.** Its IAM role is intentionally narrower than `nexus-m1-executor`.

---

## Kubernetes Manifests

### Deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: nexus-cdm-mapper
  namespace: nexus-app
  labels:
    app: nexus-cdm-mapper
    module: m1
    type: worker
spec:
  replicas: 0          # KEDA controls this — starts at 0 (idle)
  selector:
    matchLabels:
      app: nexus-cdm-mapper
  template:
    metadata:
      labels:
        app: nexus-cdm-mapper
        module: m1
      annotations:
        prometheus.io/scrape: "true"
        prometheus.io/port: "9090"
        prometheus.io/path: "/metrics"
    spec:
      serviceAccountName: nexus-cdm-mapper-sa
      automountServiceAccountToken: false
      terminationGracePeriodSeconds: 120   # Allow in-flight Kafka batch processing to complete cleanly
      containers:
        - name: cdm-mapper
          image: nexus/cdm-mapper:latest
          command: ["python", "-m", "m1.mapper.entrypoint"]
          ports:
            - containerPort: 9090
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
            - name: SCHEMA_EXTRACTED_RETENTION_DAYS
              valueFrom:
                configMapKeyRef:
                  name: nexus-platform-config
                  key: schema_extracted_retention_days   # default: 14 — must match nexus-m1-executor
            - name: CDM_CACHE_MAX_SIZE
              valueFrom:
                configMapKeyRef:
                  name: nexus-platform-config
                  key: cdm_cache_max_size      # default: 10000
            - name: CDM_CACHE_TTL_SECONDS
              valueFrom:
                configMapKeyRef:
                  name: nexus-platform-config
                  key: cdm_cache_ttl_seconds   # default: 3600
          resources:
            requests:
              cpu: 250m
              memory: 512Mi
            limits:
              cpu: 1000m
              memory: 2Gi
          livenessProbe:
            exec:
              command: ["python", "-c", "import m1.mapper.health; m1.mapper.health.check()"]
            initialDelaySeconds: 20
            periodSeconds: 30
            failureThreshold: 3
```

### KEDA ScaledObject (scale-to-zero)

```yaml
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: nexus-cdm-mapper-scaler
  namespace: nexus-app
spec:
  scaleTargetRef:
    name: nexus-cdm-mapper
  minReplicaCount: 0       # ← Scale to ZERO when idle
  maxReplicaCount: 5
  cooldownPeriod: 300      # Stay up 5 minutes after lag clears
  triggers:
    - type: kafka
      metadata:
        bootstrapServers: nexus-kafka-kafka-bootstrap.nexus-data.svc.cluster.local:9092
        consumerGroup: m1-cdm-mapper
        topic: m1.int.semantic_interpretation_requested   # platform-wide lag indicator
        lagThreshold: "1"
        offsetResetPolicy: earliest
```

`lagThreshold: "1"` is intentional: any new schema snapshot means a profiling run has completed and we want classification to start immediately.

### NetworkPolicy

```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: nexus-cdm-mapper-netpol
  namespace: nexus-app
spec:
  podSelector:
    matchLabels:
      app: nexus-cdm-mapper
  policyTypes:
    - Ingress
    - Egress
  ingress:
    - from:
        - namespaceSelector:
            matchLabels:
              name: nexus-monitoring
      ports:
        - port: 9090
  egress:
    - to:
        - namespaceSelector:
            matchLabels:
              name: nexus-data
      ports:
        - port: 5432    # PostgreSQL
        - port: 9092    # Kafka
    # No outbound rule to 0.0.0.0/0 — this service cannot reach source systems
```

### ServiceAccount

```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: nexus-cdm-mapper-sa
  namespace: nexus-app
  annotations:
    eks.amazonaws.com/role-arn: arn:aws:iam::ACCOUNT:role/nexus-cdm-mapper-role
# IAM role grants:
# - secretsmanager:GetSecretValue on nexus/platform/postgres/*  (DB credentials only)
# Explicitly NOT granted:
# - secretsmanager:GetSecretValue on nexus/tenants/*/credentials (source system creds)
# - s3:PutObject / s3:GetObject                                 (no MinIO access)
# - LLM API key                                                 (LLM calls are exclusively in nexus-m2-executor)
```

---

## Environment Variables

| Variable | Source | Description |
|---|---|---|
| `POSTGRES_DSN` | Secrets Manager | PostgreSQL (`nexus_app` role with RLS) |
| `KAFKA_BOOTSTRAP_SERVERS` | ConfigMap | Broker addresses |
| `SCHEMA_EXTRACTED_RETENTION_DAYS` | ConfigMap | Retention for upstream `m1.int.schema_extracted` (default: **`14`**) — must match `nexus-m1-executor` |
| `CDM_CACHE_MAX_SIZE` | ConfigMap | Max CDM mapping cache entries (default: `10000`) |
| `CDM_CACHE_TTL_SECONDS` | ConfigMap | Cache TTL in seconds (default: `3600`) |

No LLM credentials are accessed by this service. LLM calls for schema interpretation happen exclusively inside `nexus-m2-executor`, which consumes `{tid}.m1.semantic_interpretation_requested` under a separate consumer group. This service applies **deterministic rule-based confidence scoring** against the CDM registry only.

---

## Observability

### Prometheus Metrics

| Metric | Type | Labels |
|---|---|---|
| `cdm_mapper_snapshots_processed_total` | Counter | `tenant_id`, `connector_id` |
| `cdm_mapper_fields_classified_total` | Counter | `tenant_id`, `tier` (1/2/3) |
| `cdm_mapper_classification_duration_seconds` | Histogram | `tenant_id`, `connector_id` |
| `cdm_mapper_cache_hit_ratio` | Gauge | `tenant_id` |
| `cdm_mapper_kafka_lag` | Gauge | `consumer_group`, `topic` |
| `cdm_mapper_dead_letter_total` | Counter | `tenant_id`, `reason` |
| `cdm_mapper_tier3_backfill_triggered_total` | Counter | `tenant_id` |
| `cdm_mapper_replicas_active` | Gauge | — (0 or N — useful for billing and cost tracking) |

### Grafana Alerts
- `cdm_mapper_dead_letter_total` rate > 5/min during a run → PagerDuty
- Classification run exceeds 4 hours → Slack warning
- `cdm_mapper_replicas_active` > 0 outside expected window → Slack notification (unexpected activation)

---

## Acceptance Tests

```bash
# Test 1 — Scale-to-zero: no pods running at rest
kubectl get pods -n nexus-app -l app=nexus-cdm-mapper
# Expected: No resources found

# Test 2 — Scale-up on schema snapshot arrival
# Publish a synthetic semantic_interpretation_requested for test-tenant-alpha
python tests/publish_schema_snapshot.py --tenant test-tenant-alpha --connector aw_person
# Watch KEDA scale the deployment:
kubectl get pods -n nexus-app -l app=nexus-cdm-mapper -w
# Expected: pod appears within 30s, processes message, scales back to 0 within 5 minutes

# Test 3 — Tier isolation across two tenants
python tests/publish_schema_batch.py \
  --tenant test-tenant-alpha --connector odoo_partner --count 5 \
  --tenant test-tenant-beta  --connector sf_account  --count 5
# After processing, check {tid}.m2.semantic_interp_complete:
# All alpha proposals must have tenant_id = test-tenant-alpha
# All beta proposals must have tenant_id = test-tenant-beta

# Test 4 — Cache invalidation on mapping approval
# 1. Process a snapshot to populate cache for test-tenant-alpha
# 2. Publish {tid}.m4.mapping_approved for test-tenant-alpha
# Expected log: "CDM cache invalidated for tenant=test-tenant-alpha: N entries removed"
# 3. Process another snapshot — must fetch fresh rules from DB (cache miss on first call)

# Test 5 — Tier 3 backfill trigger on CDM version publish
# Publish nexus.cdm.version_published for test-tenant-alpha with new_version=v2
# Expected: handler publishes {tid}.m1.semantic_interpretation_requested with reprocess_mode=tier3_backfill

# Test 6 — Retention config consistency
# Verify SCHEMA_EXTRACTED_RETENTION_DAYS matches between nexus-m1-executor and nexus-cdm-mapper:
kubectl get configmap nexus-platform-config -o jsonpath='{.data.schema_extracted_retention_days}'
# Expected: 14 (same value read by both services)
```

---

*nexus-cdm-mapper · Service Specification · Mentis Consulting · March 2026*
