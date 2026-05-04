# Service Spec: `nexus-m1-api` + `nexus-m1-executor`
**Module:** M1 — Schema Profiling
**Split from:** `nexus-m1-worker` (Iteration 1 — API and execution separated at service boundary)
**Type:**
- `nexus-m1-api`: HTTP service · Kubernetes `Deployment` · always-on
- `nexus-m1-executor`: Event-driven worker · Kubernetes `Deployment` · KEDA-scaled
**Team:** Data Intelligence
**Version:** 1.2 · March 2026 · Confidential

---

## Operational Context

`nexus-m1-worker` has been split into two focused services from Iteration 1:

| | `nexus-m1-api` | `nexus-m1-executor` |
|---|---|---|
| Role | HTTP trigger API | Schema profiling + Kafka delegation to M2 for interpretation |
| Type | FastAPI · always-on | Kafka consumer · KEDA-scaled |
| Replicas (idle) | 2 minimum | 1 minimum |
| Replicas (active) | 2 (fixed) | 1–5 via KEDA |
| Trigger | REST POST from `nexus-cdm-platform` | Kafka lag on `m1.profiling_requested` |
| Outbound network | Kafka only | Kafka + Docker daemon + source system credentials |
| LLM calls | None | None — schema artifacts are published to Kafka; all LLM processing is delegated to M2 |

Keeping them merged would mean the always-on HTTP process holds Docker-daemon access and source credentials continuously — an unnecessary attack surface. The split confines both to only what they need.

---

## Purpose

Together, these two services handle the **schema profiling stage** of the M1 pipeline:

1. **`nexus-m1-api`** accepts a profiling trigger from `nexus-cdm-platform`, creates a session record, and dispatches a `m1.profiling_requested` Kafka message.
2. **`nexus-m1-executor`** consumes that message, launches `nexus-schema-profiler` Docker containers per connector, collects schema metadata, and publishes two Kafka events:
   - `m1.int.schema_extracted` — schema snapshot consumed by `nexus-cdm-platform`
   - `{tid}.m1.semantic_interpretation_requested` — delegated query to M2; `nexus-m2-executor` picks this up, runs the LLM-based schema interpretation, and publishes CDM proposals to `nexus.cdm.extension_proposed`

CDM field classification (Tier 1 / 2 / 3 mapping) is handled by the separate `nexus-cdm-mapper` service which subscribes to `{tid}.m1.semantic_interpretation_requested`.

---

## Multi-Tenancy Model

### Tenant Identity Source — Kafka Envelope

`tenant_id` is **always** taken from the `m1.profiling_requested` payload, which is set by `nexus-cdm-platform` using the authenticated session tenant. It is never inferred from connector metadata or Docker container output.

```python
# nexus-m1-executor: every message handler must follow this pattern
async def process_message(self, message: dict):
    tenant_id = message["tenant_id"]   # from Kafka payload — never inferred
    set_tenant(TenantContext(tenant_id=tenant_id))
    try:
        await self._do_work(message)
    finally:
        clear_tenant()
```

### Source System Credentials — Per-Tenant Isolation

Credentials are embedded in the `m1.profiling_requested` payload (encrypted, set by `nexus-cdm-platform`). The executor never receives a credential bundle covering multiple tenants and never reads Secrets Manager paths belonging to another tenant.

### Schema Snapshot Isolation

Schema artifacts are tagged with `tenant_id` at creation and validated by `nexus-cdm-mapper` before any CDM proposal is generated. A bug in the executor cannot produce schema snapshots attributed to the wrong tenant.

### Two-Tenant Test Rule

Every integration test must use `test-tenant-alpha` and `test-tenant-beta`. Tests trigger interleaved profiling runs for both tenants and assert that `m1.int.schema_extracted` events for alpha are never attributed to beta and vice versa.

---

## Event Processing — Inputs, Outputs & Handler Functions

### Full Data Flow

```
EXTERNAL TRIGGER              KAFKA TOPIC                    HANDLER                          OUTPUT TOPIC(S)
─────────────────────────────────────────────────────────────────────────────────────────────────────────────
nexus-cdm-platform     ──►  (HTTP POST                ──►  nexus-m1-api                ──►  m1.profiling_requested
  POST /api/pipeline/         /api/v1/profiling/              trigger_profiling()              (14 days — configurable)
  trigger)                    trigger)

nexus-m1-api           ──►  m1.profiling_requested    ──►  nexus-m1-executor            ──►  m1.int.schema_extracted
  (Kafka dispatch)            (8 partitions,                 process_run()                    (14 days — configurable)
                              14-day retention)                                           ──►  {tid}.m1.semantic_interpretation_requested
                                                                                               (delegated query to M2 — 7 days)
                                                                                          ──►  m1.int.dead_letter (on error)
```

### `nexus-m1-api` Entrypoint

`m1/api/main.py` — served by uvicorn. Starts a Kafka producer on startup; returns `503` on trigger requests if Kafka is unreachable (pod stays healthy for monitoring).

```python
# m1/api/routers/profiling.py
@router.post("/api/v1/profiling/trigger", response_model=ProfilingTriggerResponse)
async def trigger_profiling(req: ProfilingTriggerRequest) -> ProfilingTriggerResponse:
    """
    Accepts connector list from nexus-cdm-platform.
    Creates a ProfilingSession, publishes m1.profiling_requested, returns immediately.
    """
    run_id = req.run_id or str(uuid.uuid4())[:8]
    store.create(ProfilingSession(run_id=run_id, tenant_id=req.tenant_id, ...))
    await publish_profiling_requested(
        topic=settings.profiling_requested_topic,
        run_id=run_id,
        tenant_id=req.tenant_id,
        connectors=[c.model_dump() for c in req.connectors],
    )
    return ProfilingTriggerResponse(run_id=run_id, status="started", ...)
```

### `nexus-m1-executor` Entrypoint

`nexus_m1_executor/entrypoint.py` — boots one task: `KafkaM1Consumer`. All schema interpretation is delegated to M2 via Kafka.

```python
# nexus_m1_executor/entrypoint.py
async def run(config: ExecutorConfig) -> None:
    bus = build_message_bus(bootstrap_servers=config.kafka_bootstrap_servers)
    await bus.start()

    # ProfilerProcessor — handles Docker containers + schema extraction + Kafka publish
    # After extraction, process_run() publishes {tid}.m1.semantic_interpretation_requested
    # as a delegated query to M2. No LLM calls happen in this process.
    processor = ProfilerProcessor(bus=bus, profiler_image=config.profiler_image, ...)

    # Kafka consumer — m1.profiling_requested → processor.process_run()
    consumer = KafkaM1Consumer(bootstrap_servers=config.kafka_bootstrap_servers, processor=processor)
    await consumer.start()
    await consumer.run()   # blocks until SIGTERM
```

---

### Handler Functions

#### `trigger_profiling(req)` — nexus-m1-api
**File:** `m1/api/routers/profiling.py`
**Triggered by:** `POST /api/v1/profiling/trigger`

| Step | Action |
|---|---|
| 1 | Validate request (tenant_id, connector list) |
| 2 | Generate or accept `run_id` |
| 3 | Persist `ProfilingSession` in session store |
| 4 | Publish `m1.profiling_requested` with connector payloads |
| 5 | Return `202 ProfilingTriggerResponse` immediately |

---

#### `process_run(message)` — nexus-m1-executor
**File:** `nexus_m1_executor/processor.py`
**Triggered by:** `m1.profiling_requested`
**Consumer group:** `m1-profiling-executors`

| Step | Action | Helper called |
|---|---|---|
| 1 | Parse `run_id`, `tenant_id`, `connectors` from payload | — |
| 2 | Set tenant context | `set_tenant(TenantContext(tenant_id=...))` |
| 3 | For each connector, launch Docker schema profiler | `DockerRunner.run_profiler(image, connector)` |
| 4 | Wait for container exit; collect schema metadata | `SchemaBridge.collect(run_id, connector_id)` |
| 5 | Build StructuralArtifact from schema metadata | `SchemaBridge._build_artifact(...)` |
| 6 | Publish schema snapshot to Kafka | `publish_schema_extracted(bus, artifact_id, connector_id, ...)` → `m1.int.schema_extracted` |
| 7 | Publish semantic interpretation request | `bus.publish("{tid}.m1.semantic_interpretation_requested", artifact)` |

**Errors:** Container failure → log + skip connector; persistent error → `m1.int.dead_letter`. Offset is committed only after `process_run()` succeeds.

---

#### `publish_semantic_interpretation_request()` — nexus-m1-executor
**File:** `nexus_m1_executor/producer.py`
**Called by:** `process_run()` after schema extraction completes
**Publishes to:** `{tid}.m1.semantic_interpretation_requested`

After the schema profiler containers finish and the `StructuralArtifact` is built, the executor publishes it to `{tid}.m1.semantic_interpretation_requested`. This is a delegated query to M2: the artifact payload acts as a natural language handoff containing the full schema metadata (tables, columns, types, cardinality). The executor then commits the Kafka offset and has no further role in CDM creation.

**M2 handles everything from here.** `nexus-m2-executor` subscribes to `{tid}.m1.semantic_interpretation_requested` under consumer group `m2-structural-agents`, makes the LLM call (Claude) to interpret the schema, and publishes CDM extension proposals to `nexus.cdm.extension_proposed`. M1 does not call any LLM, does not await a response, and has no dependency on M2's processing completing.

`nexus-cdm-mapper` independently consumes the same topic under consumer group `m1-cdm-mapper` to run deterministic Tier 1 / 2 / 3 confidence scoring against the *existing* CDM mappings — no LLM involved.

---

### Shared Helper Functions

| Function | File | Purpose |
|---|---|---|
| `publish_profiling_requested(topic, run_id, ...)` | `m1/services/kafka_producer.py` | Serialises and sends trigger message from nexus-m1-api |
| `process_run(message)` | `nexus_m1_executor/processor.py` | Orchestrates Docker + schema extraction + Kafka publish |
| `DockerRunner.run_profiler(image, connector)` | `nexus_m1_executor/docker_runner.py` | Runs `nexus-schema-profiler` container per connector |
| `SchemaBridge.collect(run_id, connector_id)` | `nexus_m1_executor/bridge.py` | Collects schema output from completed container |
| `publish_schema_extracted(bus, ...)` | `nexus_m1_executor/producer.py` | Wraps payload in NexusMessage and publishes to `m1.int.schema_extracted` |
| `is_active_tenant(tenant_id)` | `nexus_m1_executor/consumer.py` | Guards against unknown/suspended tenants |

---

## Kafka Topics

### Consumed

| Topic | Consumer group | Service | Handler |
|---|---|---|---|
| `m1.profiling_requested` | `m1-profiling-executors` | nexus-m1-executor | `process_run()` |

### Produced

| Topic | Partitions | Retention | Config key | Publisher |
|---|---|---|---|---|
| `m1.profiling_requested` | 8 | `M1_PROFILING_REQUESTED_RETENTION_DAYS` days (default: **14**) | `M1_PROFILING_REQUESTED_RETENTION_DAYS` | nexus-m1-api |
| `m1.int.schema_extracted` | 8 | `SCHEMA_EXTRACTED_RETENTION_DAYS` days (default: **14**) | `SCHEMA_EXTRACTED_RETENTION_DAYS` | nexus-m1-executor |
| `{tid}.m1.semantic_interpretation_requested` | per-tenant | 7 days | — | nexus-m1-executor — delegated query to M2; consumed by both `nexus-m2-executor` (LLM reasoning) and `nexus-cdm-mapper` (deterministic classification) |
| `m1.int.dead_letter` | 4 | 30 days | — | nexus-m1-executor |

**Retention rationale:** `m1.int.schema_extracted` must remain available long enough for `nexus-cdm-platform`'s `M1SchemaListener` and `nexus-cdm-mapper` to process it, even if those services are briefly down. 14 days is the default; set `SCHEMA_EXTRACTED_RETENTION_DAYS` to override.

---

## Storage Dependencies

| Store | Service | Usage | Tenant isolation mechanism |
|---|---|---|---|
| In-memory session store | nexus-m1-api | Track `ProfilingSession` per `run_id` | Session keyed by `run_id`; `tenant_id` stored inside |
| Docker daemon | nexus-m1-executor | Launch `nexus-schema-profiler` containers | One container per connector; no cross-tenant container sharing |
| Kafka | Both | Message passing | `tenant_id` in every message envelope |

Note: neither service has direct PostgreSQL or MinIO access. Database reads (connector configs, CDM mappings) are owned by `nexus-cdm-platform` and `nexus-cdm-mapper`.

---

## Kubernetes Manifests

### Deployment — nexus-m1-api

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
  replicas: 2
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
        prometheus.io/port: "9090"
        prometheus.io/path: "/metrics"
    spec:
      serviceAccountName: nexus-m1-api-sa
      automountServiceAccountToken: false
      containers:
        - name: m1-api
          image: nexus/m1-api:latest
          command: ["python", "entrypoint.py"]
          ports:
            - containerPort: 8000
              name: http
            - containerPort: 9090
              name: metrics
          env:
            - name: KAFKA_BOOTSTRAP_SERVERS
              valueFrom:
                configMapKeyRef:
                  name: nexus-platform-config
                  key: kafka_bootstrap_servers
            - name: M1_PROFILING_REQUESTED_TOPIC
              value: "m1.profiling_requested"
            - name: M1_PROFILING_REQUESTED_RETENTION_DAYS
              valueFrom:
                configMapKeyRef:
                  name: nexus-platform-config
                  key: m1_profiling_requested_retention_days   # default: 14
          resources:
            requests:
              cpu: 200m
              memory: 256Mi
            limits:
              cpu: 500m
              memory: 512Mi
          livenessProbe:
            httpGet:
              path: /api/v1/health
              port: 8000
            initialDelaySeconds: 10
            periodSeconds: 15
```

### Deployment — nexus-m1-executor

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: nexus-m1-executor
  namespace: nexus-app
  labels:
    app: nexus-m1-executor
    module: m1
    type: worker
spec:
  replicas: 1            # KEDA scales from here
  selector:
    matchLabels:
      app: nexus-m1-executor
  template:
    metadata:
      labels:
        app: nexus-m1-executor
        module: m1
      annotations:
        prometheus.io/scrape: "true"
        prometheus.io/port: "9090"
        prometheus.io/path: "/metrics"
    spec:
      serviceAccountName: nexus-m1-executor-sa
      automountServiceAccountToken: false
      terminationGracePeriodSeconds: 300   # Docker containers can take time to finish
      volumes:
        - name: docker-sock
          hostPath:
            path: /var/run/docker.sock
      containers:
        - name: m1-executor
          image: nexus/m1-executor:latest
          command: ["python", "entrypoint.py"]
          ports:
            - containerPort: 9090
              name: metrics
          volumeMounts:
            - name: docker-sock
              mountPath: /var/run/docker.sock
          env:
            - name: KAFKA_BOOTSTRAP_SERVERS
              valueFrom:
                configMapKeyRef:
                  name: nexus-platform-config
                  key: kafka_bootstrap_servers
            - name: SCHEMA_EXTRACTED_RETENTION_DAYS
              valueFrom:
                configMapKeyRef:
                  name: nexus-platform-config
                  key: schema_extracted_retention_days   # default: 14
            - name: PROFILER_IMAGE
              valueFrom:
                configMapKeyRef:
                  name: nexus-platform-config
                  key: profiler_image
          resources:
            requests:
              cpu: 500m
              memory: 1Gi
            limits:
              cpu: 2000m
              memory: 4Gi
          livenessProbe:
            exec:
              command: ["python", "-c", "import nexus_m1_executor.health; nexus_m1_executor.health.check()"]
            initialDelaySeconds: 30
            periodSeconds: 30
            failureThreshold: 3
          lifecycle:
            preStop:
              exec:
                command: ["sh", "-c", "sleep 10"]   # Allow in-flight containers to be noticed
```

### KEDA ScaledObject — nexus-m1-executor

```yaml
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: nexus-m1-executor-scaler
  namespace: nexus-app
spec:
  scaleTargetRef:
    name: nexus-m1-executor
  minReplicaCount: 1
  maxReplicaCount: 5
  cooldownPeriod: 180
  triggers:
    - type: kafka
      metadata:
        bootstrapServers: nexus-kafka-kafka-bootstrap.nexus-data.svc.cluster.local:9092
        consumerGroup: m1-profiling-executors
        topic: m1.profiling_requested
        lagThreshold: "1"            # One profiling run per executor replica
        offsetResetPolicy: earliest
```

### NetworkPolicy

```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: nexus-m1-executor-netpol
  namespace: nexus-app
spec:
  podSelector:
    matchLabels:
      app: nexus-m1-executor
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
        - port: 9092    # Kafka
    # Docker daemon on host — required to launch schema-profiler containers
    - to:
        - ipBlock:
            cidr: 127.0.0.1/32
      ports:
        - port: 2375    # Docker daemon (TLS-secured in production: 2376)
    # Source system outbound — schema profiler containers reach source systems
    - to:
        - ipBlock:
            cidr: 0.0.0.0/0
            except:
              - 10.0.0.0/8
      ports:
        - port: 443
        - port: 5432
        - port: 3306
        - port: 1433
```

### ServiceAccounts

```yaml
# nexus-m1-api — minimal: Kafka only
apiVersion: v1
kind: ServiceAccount
metadata:
  name: nexus-m1-api-sa
  namespace: nexus-app
  annotations:
    eks.amazonaws.com/role-arn: arn:aws:iam::ACCOUNT:role/nexus-m1-api-role
# IAM grants:
# - (none) — Kafka access is network-level only; no AWS resource access needed

---
# nexus-m1-executor — no LLM credentials; LLM work is delegated to M2 via Kafka
apiVersion: v1
kind: ServiceAccount
metadata:
  name: nexus-m1-executor-sa
  namespace: nexus-app
  annotations:
    eks.amazonaws.com/role-arn: arn:aws:iam::ACCOUNT:role/nexus-m1-executor-role
# IAM grants:
# - (none beyond Kafka network access) — executor publishes schema artifacts to Kafka only
# Explicitly NOT granted:
# - secretsmanager:GetSecretValue on nexus/platform/llm/*  (LLM keys belong to nexus-m2-executor only)
# - s3:PutObject / s3:GetObject   (no MinIO access — executor never writes raw data)
# - secretsmanager:GetSecretValue on nexus/tenants/*/credentials  (source creds in Kafka payload)
```

---

## Environment Variables

### nexus-m1-api

| Variable | Source | Description |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | ConfigMap | Broker addresses |
| `M1_PROFILING_REQUESTED_TOPIC` | ConfigMap | Topic for profiling trigger (default: `m1.profiling_requested`) |
| `M1_PROFILING_REQUESTED_RETENTION_DAYS` | ConfigMap | Kafka retention for trigger topic (default: `14`) |
| `API_HOST` | ConfigMap | Bind address (default: `0.0.0.0`) |
| `API_PORT` | ConfigMap | HTTP port (default: `8000`) |

### nexus-m1-executor

| Variable | Source | Description |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | ConfigMap | Broker addresses |
| `SCHEMA_EXTRACTED_RETENTION_DAYS` | ConfigMap | Kafka retention for `m1.int.schema_extracted` (default: **`14`**) |
| `PROFILER_IMAGE` | ConfigMap | Docker image for `nexus-schema-profiler` containers |
| `PROFILER_DOCKER_NETWORK` | ConfigMap | Docker network for profiler containers (default: `nexus-net`) |

---

## Observability

### Prometheus Metrics

| Metric | Type | Labels | Service |
|---|---|---|---|
| `m1_api_profiling_requests_total` | Counter | `tenant_id`, `status` | nexus-m1-api |
| `m1_api_kafka_publish_errors_total` | Counter | `tenant_id` | nexus-m1-api |
| `m1_executor_runs_total` | Counter | `tenant_id`, `status` | nexus-m1-executor |
| `m1_executor_run_duration_seconds` | Histogram | `tenant_id` | nexus-m1-executor |
| `m1_executor_containers_launched_total` | Counter | `tenant_id`, `connector_id` | nexus-m1-executor |
| `m1_executor_container_errors_total` | Counter | `tenant_id`, `connector_id` | nexus-m1-executor |
| `m1_executor_schema_extracted_total` | Counter | `tenant_id` | nexus-m1-executor |
| `m1_executor_dead_letter_total` | Counter | `tenant_id`, `reason` | nexus-m1-executor |
| `m1_executor_kafka_lag` | Gauge | `consumer_group`, `topic` | nexus-m1-executor |

### Grafana Alerts
- `m1_executor_dead_letter_total` rate > 5/min → PagerDuty
- `m1_executor_run_duration_seconds` p95 > 300s → Slack warning
- Kafka lag on `m1.profiling_requested` > 5 for > 10 minutes → Slack warning (executor may be stuck)

---

## Acceptance Tests

```bash
# Test 1 — End-to-end: HTTP trigger → schema extracted event
curl -X POST http://nexus-m1-api/api/v1/profiling/trigger \
  -H "Content-Type: application/json" \
  -d '{"tenant_id": "test-tenant-alpha", "connectors": [{"connector_id": "aw_person", "system_type": "mssql", ...}]}'
# Expected: 200 {"run_id": "abc123", "status": "started"}

# After ~60s, check m1.int.schema_extracted topic:
python tests/consume_once.py --topic m1.int.schema_extracted --tenant test-tenant-alpha
# Expected: message with artifact_id, connector_id=aw_person, column_types populated

# Test 2 — Tenant isolation
python tests/trigger_interleaved.py --tenants test-tenant-alpha test-tenant-beta
# All schema_extracted events for alpha must have tenant_id = test-tenant-alpha
# All events for beta must have tenant_id = test-tenant-beta

# Test 3 — KEDA scale-out
# Publish 5 profiling_requested messages for various tenants
python tests/publish_profiling_batch.py --count 5
# Watch executor replicas increase:
kubectl get pods -n nexus-app -l app=nexus-m1-executor -w
# Expected: replica count increases from 1 toward 5

# Test 4 — Kafka unavailable → 503
# Scale down Kafka briefly, attempt trigger:
# Expected: POST /api/v1/profiling/trigger returns 503
# Expected: /api/v1/health still returns 200 (pod stays alive)

# Test 5 — Retention config
# Verify topic retention is set from SCHEMA_EXTRACTED_RETENTION_DAYS:
kubectl get configmap nexus-platform-config -o jsonpath='{.data.schema_extracted_retention_days}'
# Expected: 14
# kafka-configs.sh --describe --topic m1.int.schema_extracted
# Expected: retention.ms = 1209600000  (14 × 24 × 3600 × 1000)
```

---

*nexus-m1-api + nexus-m1-executor · Service Specification · Mentis Consulting · March 2026*
