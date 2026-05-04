# NEXUS — Platform Team Operational Plan
### Team 1 · 1 Senior DevOps Engineer · days 1–16
**Mentis Consulting · February 2026 · Confidential**

---

## Your Role in One Paragraph

You build and operate the ground everything else runs on. Every other team is blocked until you deliver Phase 0 infrastructure. Your first two days are sequential: each component depends on the one before it. Do not attempt to parallelise within Phase 0. After day 2, your role shifts to platform reliability: monitoring Kafka consumer lag, tuning Spark resources as Team 2 adds connectors, and ensuring the observability stack gives every team visibility into their pipelines.

Multi-tenancy is baked into your infrastructure from day one: dynamic Kafka topic creation per tenant, PostgreSQL row-level security enforcement, per-tenant object storage paths, and per-tenant AI store provisioning. You do not implement multi-tenancy in application code — but you are responsible for the infrastructure primitives that make it enforeable.

---

## Multi-Tenancy: What Infrastructure Enables

From a platform perspective, multi-tenancy means:

**Kafka:** You create 18 static platform topics at cluster startup. Per-tenant dynamic topics (`{tid}.m1.*`, `{tid}.m2.*`, `{tid}.m4.*`) are created by the Tech Lead's `onboard_tenant.py` script, which calls the Kafka Admin API. Your job is to ensure `auto.create.topics.enable = false` on the cluster so no topic is ever created silently by a misconfigured producer. You also need to ensure the Kafka ACL model allows workers to subscribe to topics in their own tenant namespace.

**PostgreSQL:** RLS is defined in SQL and applied to every `nexus_system` table. You apply the DDL. The enforcement is automatic and requires no application-layer involvement.

**MinIO:** Object paths include `{tenant_id}` as the first segment after the bucket name. You configure bucket policies to prevent cross-tenant path writes. In the MVP, this is enforced by convention; in production it is enforced by IAM-style bucket policies per service account.

**Kong:** The JWT plugin extracts `tenant_id` from every JWT and injects it as the `X-Tenant-ID` header. This is the gateway-layer enforcement that prevents application services from ever needing to decode JWTs.

**Observability:** Grafana dashboards must support tenant-level filtering. Every metric emitted by application services must include a `tenant_id` label so you can isolate one tenant's pipeline health in Grafana.

---

## Timeline

| days | Focus |
|---|---|
| 1 | Kubernetes, Kafka (18 static topics), PostgreSQL (schema + RLS), Redis, MinIO |
| 2 | Kong, Airflow, Spark, Secrets Manager, Observability stack |
| 3–6 | Support Team 2 onboarding. Tune Kafka partitions as load grows. |
| 7–10 | Provision Pinecone, Neo4j, TimescaleDB for Team 3. Pipeline health monitoring. |
| 11–14 | Load testing. Security hardening. Pre-production readiness. |
| 15–16 | E2E test support. Production deployment prep. |

---

## Phase 0 — Infrastructure Foundation

---

### P0-INFRA-01 — Kubernetes Cluster and Namespace Isolation

**What to build and why:**

AWS EKS cluster with four namespaces. Namespaces provide the first layer of network isolation between infrastructure components and application services. Network policies enforce that application pods (which run tenant workloads) cannot reach Kong's admin port, cannot connect to each other across modules (M1 workers cannot directly reach M3 writers), and can only talk to infrastructure on the specific ports required.

This matters for multi-tenancy because all tenants' workloads share the same pods in the MVP. If a bug in one tenant's connector worker can reach another tenant's Redis keys or MinIO objects, isolation fails. The network policies limit blast radius.

**Sub-task 01-A — Cluster creation (AWS EKS):**

```bash
eksctl create cluster \
  --name nexus-dev \
  --region eu-west-1 \
  --nodegroup-name nexus-workers \
  --node-type m5.2xlarge \
  --nodes 9 \
  --nodes-min 6 \
  --nodes-max 15 \
  --with-oidc
```

**Sub-task 01-B — Namespace creation:**

```bash
kubectl create namespace nexus-infra    # Kong, observability stack
kubectl create namespace nexus-data     # Kafka, PostgreSQL, Redis, Airflow, Spark
kubectl create namespace nexus-storage  # MinIO
kubectl create namespace nexus-app      # All application pods (M1, M2, M3, M4, M6)

kubectl label namespace nexus-infra    project=nexus tier=infra
kubectl label namespace nexus-data     project=nexus tier=data
kubectl label namespace nexus-storage  project=nexus tier=storage
kubectl label namespace nexus-app      project=nexus tier=app
```

**Sub-task 01-C — Network policies (critical for multi-tenant blast radius):**

```yaml
# deny-infra-from-app.yaml
# Prevents application pods from reaching Kong admin port
# (prevents a compromised app pod from adding Kong routes)
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: deny-kong-admin-from-app
  namespace: nexus-infra
spec:
  podSelector:
    matchLabels:
      app: nexus-kong
  ingress:
    - from:
        - namespaceSelector:
            matchLabels:
              tier: infra
      ports:
        - port: 8001   # Kong admin — infra namespace only
    - from:
        - namespaceSelector:
            matchLabels:
              tier: app
      ports:
        - port: 8000   # Kong proxy — app namespace allowed
```

**Sub-task 01-D — Service accounts with per-team RBAC:**

```yaml
# One service account per team — limits what each team's pods can access
---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: team2-data-intelligence
  namespace: nexus-app
  annotations:
    eks.amazonaws.com/role-arn: arn:aws:iam::ACCOUNT:role/nexus-team2
---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: team3-ai-knowledge
  namespace: nexus-app
  annotations:
    eks.amazonaws.com/role-arn: arn:aws:iam::ACCOUNT:role/nexus-team3
---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: team4-product
  namespace: nexus-app
  annotations:
    eks.amazonaws.com/role-arn: arn:aws:iam::ACCOUNT:role/nexus-team4
```

The AWS IAM roles restrict Secrets Manager access: `nexus-team2` role can only `GetSecretValue` on secrets matching `nexus/*/team2/*` and `nexus/*/connectors/*`. This prevents a Team 2 connector from accidentally reading Team 3's Pinecone credentials.

**Acceptance verification:**

```bash
# Namespaces exist
kubectl get namespaces -l project=nexus

# Verify nexus-app cannot reach Kong admin
kubectl run test-probe --image=curlimages/curl --rm -it --restart=Never -n nexus-app \
  -- curl --max-time 3 http://nexus-kong.nexus-infra.svc.cluster.local:8001
# Expected: connection refused or timeout (network policy blocks it)

# Verify nexus-app CAN reach Kong proxy
kubectl run test-probe --image=curlimages/curl --rm -it --restart=Never -n nexus-app \
  -- curl --max-time 3 http://nexus-kong.nexus-infra.svc.cluster.local:8000
# Expected: HTTP 404 or 401 (Kong responding, not blocked)
```

---

### P0-INFRA-02 — Kafka Cluster (18 Static Topics + Dynamic Topic Readiness)

**What to build and why:**

Kafka is the nervous system of NEXUS. Every inter-service communication that is part of the pipeline travels over Kafka. The 18 static topics handle internal M1 pipeline events and platform-wide CDM lifecycle events. Per-tenant dynamic topics are created by `onboard_tenant.py` — you do not create them here, but you must configure Kafka so that they can be created programmatically and so that no topic is created accidentally.

The partition counts below are deliberate: `m1.int.raw_records` gets 16 partitions because it is the highest-volume topic (raw extracted records from all source systems for all tenants). Other topics get 4–8 partitions.

**Sub-task 02-A — Strimzi operator and Kafka cluster:**

```bash
kubectl create namespace nexus-data
kubectl apply -f https://strimzi.io/install/latest?namespace=nexus-data
# Wait for operator to be ready
kubectl wait --for=condition=Ready pod -l strimzi.io/kind=cluster-operator -n nexus-data --timeout=120s
```

```yaml
# kafka-cluster.yaml
apiVersion: kafka.strimzi.io/v1beta2
kind: Kafka
metadata:
  name: nexus-kafka
  namespace: nexus-data
spec:
  kafka:
    version: 3.6.0
    replicas: 3
    config:
      auto.create.topics.enable: "false"     # CRITICAL — no silent topic creation
      default.replication.factor: "2"
      min.insync.replicas: "1"
      log.retention.hours: "168"             # 7 days default
      num.partitions: "4"
    storage:
      type: persistent-claim
      size: 500Gi
      class: gp3
  zookeeper:
    replicas: 3
    storage:
      type: persistent-claim
      size: 100Gi
      class: gp3
```

**Sub-task 02-B — 18 static platform topics via KafkaTopic CRD:**

Use KafkaTopic CRDs (not `kafka-topics.sh`) so topic configuration is version-controlled in Git. Any operator can reproduce the exact cluster state by applying the manifests.

```yaml
# Topics with specific partition counts — apply all 18
---
apiVersion: kafka.strimzi.io/v1beta2
kind: KafkaTopic
metadata:
  name: m1-int-raw-records
  namespace: nexus-data
  labels: {strimzi.io/cluster: nexus-kafka}
spec:
  partitions: 16
  replicas: 2
  config: {retention.ms: "259200000"}   # 3 days — high volume, short retention
---
apiVersion: kafka.strimzi.io/v1beta2
kind: KafkaTopic
metadata:
  name: m1-int-cdm-entities-ready
  namespace: nexus-data
  labels: {strimzi.io/cluster: nexus-kafka}
spec:
  partitions: 8
  replicas: 2
  config: {retention.ms: "604800000"}   # 7 days
---
apiVersion: kafka.strimzi.io/v1beta2
kind: KafkaTopic
metadata:
  name: m1-int-dead-letter
  namespace: nexus-data
  labels: {strimzi.io/cluster: nexus-kafka}
spec:
  partitions: 4
  replicas: 2
  config: {retention.ms: "7776000000"}  # 90 days — keep dead letters long
```

Full partition map:

| Topic | Partitions | Retention | Reason |
|---|---|---|---|
| `m1.int.raw_records` | 16 | 3 days | Highest volume — one partition per expected connector |
| `m1.int.classified_records` | 8 | 3 days | Post-Spark — volume reduced |
| `m1.int.cdm_entities_ready` | 8 | 7 days | Consumed by M3 — needs durable delivery |
| `m1.int.ai_write_completed` | 4 | 14 days | Low volume confirmation messages |
| `m1.int.sync_requested` | 4 | 7 days | One message per sync trigger |
| `m1.int.delta_batch_ready` | 4 | 7 days | One message per Delta flush |
| `m1.int.ai_routing_decided` | 4 | 7 days | M1→M3 routing decisions |
| `m1.int.sync_failed` | 4 | 30 days | Failure events need longer retention for debugging |
| `m1.int.delta_write_failed` | 4 | 30 days | Same |
| `m1.int.spark_job_failed` | 4 | 30 days | Same |
| `m1.int.mapping_failed` | 4 | 30 days | Review backlog |
| `m1.int.dead_letter` | 4 | 90 days | Never lose dead letters |
| `m1.int.structural_cycle_triggered` | 4 | 7 days | Low volume |
| `m1.int.source_schema_extracted` | 4 | 14 days | Schema snapshots are valuable |
| `nexus.cdm.extension_proposed` | 4 | 30 days | Governance events |
| `nexus.cdm.version_published` | 4 | 90 days | CDM history |
| `nexus.cdm.extension_rejected` | 4 | 30 days | Governance audit |
| `nexus.cdm.rollback_requested` | 4 | 30 days | Recovery events |

**Sub-task 02-C — Verify auto.create.topics is disabled:**

```bash
kubectl exec -it nexus-kafka-kafka-0 -n nexus-data -- \
  bin/kafka-configs.sh --bootstrap-server localhost:9092 \
  --entity-type brokers --entity-default --describe | grep auto.create

# Expected output: auto.create.topics.enable=false
# If this shows 'true', stop — do not proceed until fixed.

# Attempt to publish to a non-existent topic — should fail
kubectl exec -it nexus-kafka-kafka-0 -n nexus-data -- \
  bin/kafka-console-producer.sh --bootstrap-server localhost:9092 \
  --topic this.topic.does.not.exist
# Expected: LEADER_NOT_AVAILABLE error, NOT silent topic creation
```

**Sub-task 02-D — Deploy Redpanda Console (Kafka UI):**

```yaml
# redpanda-console.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: redpanda-console
  namespace: nexus-data
spec:
  template:
    spec:
      containers:
        - name: console
          image: docker.redpanda.com/redpandadata/console:latest
          env:
            - name: KAFKA_BROKERS
              value: nexus-kafka-kafka-bootstrap.nexus-data.svc.cluster.local:9092
```

Register a Kong route to expose at `kafka-ui.nexus.internal` (infra team access only — protect with IP allowlist in Kong).

---

### P0-INFRA-03 — PostgreSQL with RLS Applied

**What to build and why:**

PostgreSQL 15 via CloudNativePG. The schema is applied from the migration file that the Tech Lead writes. Your job is deploying the database and applying the schema — the RLS policies in the schema are the most important part for multi-tenancy.

**Sub-task 03-A — Database deployment:**

```bash
kubectl apply -f https://raw.githubusercontent.com/cloudnative-pg/cloudnative-pg/release-1.22/releases/cnpg-1.22.0.yaml
kubectl apply -f postgres-cluster.yaml -n nexus-data
```

```yaml
# postgres-cluster.yaml
apiVersion: postgresql.cnpg.io/v1
kind: Cluster
metadata:
  name: nexus-postgres
  namespace: nexus-data
spec:
  instances: 1
  storage: {size: 100Gi, storageClass: gp3}
  postgresql:
    parameters:
      shared_buffers: "2GB"
      max_connections: "200"
```

**Sub-task 03-B — Schema + RLS application:**

```bash
# Copy schema migration to pod
kubectl cp nexus-schema-with-rls.sql nexus-data/nexus-postgres-1:/tmp/

# Apply — the Tech Lead provides this file with all tables + RLS policies
kubectl exec -it nexus-postgres-1 -n nexus-data -- psql -U postgres -f /tmp/nexus-schema-with-rls.sql
```

**Sub-task 03-C — Create application role with restricted privileges:**

```sql
-- Run as postgres superuser
CREATE ROLE nexus_app WITH LOGIN PASSWORD '<from-secrets-manager>';
-- Grant access to schema and tables — NOT superuser, NOT a role that bypasses RLS
GRANT USAGE ON SCHEMA nexus_system TO nexus_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA nexus_system TO nexus_app;

-- CRITICAL: nexus_app must NOT be a superuser
SELECT rolsuper FROM pg_roles WHERE rolname = 'nexus_app';
-- Must return: f
```

**Sub-task 03-D — RLS verification (coordinate with Tech Lead):**

After schema application, run the Tech Lead's `tests/security/test_rls_isolation.py` against this database. All assertions must pass before any team starts building on top of this database. If RLS is not working, every other team's data access code is built on a broken foundation.

**Sub-task 03-E — Redis deployment:**

```bash
helm install nexus-redis bitnami/redis \
  --namespace nexus-data \
  --set auth.enabled=false \
  --set master.persistence.size=10Gi
```

Redis is used by M1 workers for backpressure signalling and by M4 for session state. It does not require per-tenant isolation in the MVP because it only stores ephemeral operational state (not tenant business data). Workers namespace their Redis keys with `{tenant_id}:` prefixes by convention.

**Acceptance verification:**

```bash
# All 7 tables exist
kubectl exec -it nexus-postgres-1 -n nexus-data -- psql -U postgres \
  -c "\dt nexus_system.*"

# nexus_app is not a superuser
kubectl exec -it nexus-postgres-1 -n nexus-data -- psql -U postgres \
  -c "SELECT rolsuper FROM pg_roles WHERE rolname = 'nexus_app';"
# Must return: f

# RLS enabled on all tables
kubectl exec -it nexus-postgres-1 -n nexus-data -- psql -U postgres \
  -c "SELECT tablename, rowsecurity FROM pg_tables WHERE schemaname='nexus_system';"
# rowsecurity column must be 't' for all rows

# Tech Lead's RLS verification test passes
pytest tests/security/test_rls_isolation.py -v
```

---

### P0-INFRA-04 — MinIO with Tenant Path Convention

**What to build and why:**

MinIO stores Delta Lake data at three stages: raw extracted records, Spark-classified records, and CDM-mapped entities. Multi-tenancy is enforced via the path convention: `s3a://nexus-raw/{tenant_id}/{system_type}/{entity}/`. A Spark job for tenant A always receives `tenant_id=acme-corp` as a parameter and writes to `nexus-raw/acme-corp/...`. There is no technical control in the MVP preventing a misconfigured job from writing to another tenant's path — this is enforced by the application code convention. In production, IAM-style bucket policies would enforce this at the storage layer.

**Sub-task 04-A — MinIO deployment:**

```bash
helm install nexus-minio bitnami/minio \
  --namespace nexus-storage \
  --set auth.rootUser=nexus-admin \
  --set auth.rootPassword='<from-secrets-manager>' \
  --set persistence.size=500Gi \
  --set defaultBuckets="nexus-raw,nexus-classified,nexus-cdm,nexus-jobs"
```

**Sub-task 04-B — Verify tenant path convention works:**

```bash
kubectl run minio-test --image=minio/mc --rm -it --restart=Never -n nexus-app -- sh -c "
mc alias set nexus http://nexus-minio.nexus-storage.svc.cluster.local:9000 nexus-admin '<password>'
# Write to tenant A's path
mc cp /etc/hostname nexus/nexus-raw/test-tenant-alpha/salesforce/account/test.txt
# Write to tenant B's path
mc cp /etc/hostname nexus/nexus-raw/test-tenant-beta/odoo/res.partner/test.txt
# Read — each tenant sees only their path (by convention in application code)
mc ls nexus/nexus-raw/test-tenant-alpha/
mc ls nexus/nexus-raw/test-tenant-beta/
# Cleanup
mc rm nexus/nexus-raw/test-tenant-alpha/salesforce/account/test.txt
mc rm nexus/nexus-raw/test-tenant-beta/odoo/res.partner/test.txt
"
```

---

### P0-INFRA-05 — Kong API Gateway with JWT Tenant Extraction

**What to build and why:**

Kong is the only entry point for all HTTP requests to NEXUS services. Its JWT plugin validates the incoming JWT and, crucially, extracts the `tenant_id` claim and injects it as the `X-Tenant-ID` header. This header injection is the gateway-layer multi-tenancy enforcement: every service behind Kong receives a pre-validated, trusted `X-Tenant-ID` header and never needs to decode a JWT.

This matters because if every service decoded its own JWTs, any service that made a mistake in JWT validation would be a security hole. Kong centralises this concern.

**Sub-task 05-A — Kong deployment:**

```bash
helm install nexus-kong kong/kong \
  --namespace nexus-infra \
  --set proxy.enabled=true \
  --set admin.enabled=true \
  --set admin.http.enabled=true \
  --set postgresql.enabled=true
```

**Sub-task 05-B — Global plugins via declarative config (`deck sync`):**

```yaml
# kong.yaml
_format_version: "3.0"

plugins:
  - name: jwt
    config:
      claims_to_verify: [exp]
      key_claim_name: kid
      secret_is_base64: false

  - name: rate-limiting
    config:
      minute: 100
      policy: header
      header_name: X-Tenant-ID
      limit_by: header
      error_message: "Rate limit exceeded. Tenant quota is 100 requests/min."

  - name: correlation-id
    config:
      header_name: X-Correlation-ID
      generator: uuid#counter
      echo_downstream: true

  - name: request-transformer
    config:
      # Extract tenant_id from JWT payload and inject as header
      # Kong's JWT plugin populates X-Consumer-Username with the JWT subject
      # We use the jwt-keycloak or a custom plugin to inject X-Tenant-ID
      # from the 'tenant_id' claim in the JWT payload
      add:
        headers:
          - "X-Tenant-ID: $(jwt_claims.tenant_id)"
```

**Sub-task 05-C — Tenant-aware rate limiting verification:**

Rate limiting is applied per tenant (via `X-Tenant-ID` header) not per IP address. This is important: a shared enterprise IP should not cause one tenant's usage to throttle another tenant.

```bash
# Test rate limiting per tenant (requires valid JWTs for two tenants)
# Send 101 requests from tenant-alpha — 101st should receive 429
# Send 50 requests from tenant-beta — should all succeed (different quota)
# Verify: tenant-alpha's rate limiting does not affect tenant-beta
for i in $(seq 1 101); do
  curl -s -o /dev/null -w "%{http_code}\n" \
    -H "Authorization: Bearer $TENANT_ALPHA_JWT" \
    http://nexus-kong-proxy.nexus-infra.svc.cluster.local/api/v1/health
done | tail -5
# Expected: last few requests return 429
```

**Sub-task 05-D — Register service routes for M1 and M4:**

```yaml
# kong-services.yaml
services:
  - name: m1-connector-api
    url: http://m1-connector-api.nexus-app.svc.cluster.local:8001
    routes:
      - name: m1-connectors
        paths: [/api/v1/connectors]
        methods: [GET, POST]
      - name: m1-sync
        paths: [/api/v1/connectors/*/sync]
        methods: [POST]

  - name: m4-governance-api
    url: http://m4-governance-api.nexus-app.svc.cluster.local:8002
    routes:
      - name: m4-governance
        paths: [/api/v1/governance]
        methods: [GET, POST]
      - name: m4-mappings
        paths: [/api/v1/mappings]
        methods: [GET, POST]

  - name: m2-query-api
    url: http://m2-query-api.nexus-app.svc.cluster.local:8003
    routes:
      - name: m2-query
        paths: [/api/v1/query]
        methods: [POST]
```

---

### P0-INFRA-06 — Airflow

**What to build and why:**

Airflow orchestrates the M1 Operational and Structural cycles. It provides retry logic, scheduling, and the `KafkaSensor` that allows DAGs to wait for pipeline events without busy-polling.

**Sub-task 06-A — Airflow deployment with custom providers:**

```dockerfile
# airflow/Dockerfile
FROM apache/airflow:2.8.0
RUN pip install \
    apache-airflow-providers-apache-kafka==1.3.0 \
    apache-airflow-providers-apache-spark==4.7.0 \
    apache-airflow-providers-amazon==8.10.0 \
    confluent-kafka==2.3.0 \
    "git+https://github.com/mentis-consulting/nexus-core.git@main#egg=nexus-core"
```

```bash
helm install nexus-airflow apache-airflow/airflow \
  --namespace nexus-data \
  --set executor=KubernetesExecutor \
  --set config.core.load_examples=false \
  --set dags.persistence.enabled=true
```

**Sub-task 06-B — Connections:**

```bash
airflow connections add nexus_kafka \
  --conn-type kafka \
  --conn-extra '{"bootstrap.servers": "nexus-kafka-kafka-bootstrap.nexus-data.svc.cluster.local:9092"}'

airflow connections add nexus_spark \
  --conn-type spark \
  --conn-host nexus-spark-master.nexus-data.svc.cluster.local \
  --conn-port 7077
```

**Sub-task 06-C — Multi-tenant Airflow DAG pattern (document for Team 2):**

Airflow DAGs in NEXUS are tenant-parameterised: every DAG run is triggered with a `conf` dictionary that includes `tenant_id`. DAG tasks that interact with Kafka or PostgreSQL must thread this `tenant_id` through all downstream calls. Provide Team 2 with this pattern:

```python
# Pattern: how tenant_id flows through a DAG
# In every task: conf["tenant_id"] is the authoritative tenant
def my_dag_task(**context):
    tenant_id = context["dag_run"].conf["tenant_id"]
    # All Kafka publishes use this tenant_id in NexusMessage
    # All database calls use get_tenant_scoped_connection(pool, tenant_id)
    # Never infer tenant_id from other data sources
```

---

### P0-INFRA-07 — Spark Cluster

**What to build and why:**

Spark runs the M1 classification job. It reads Delta Lake data from MinIO, classifies entity types, and writes classified records back to MinIO. Spark jobs always receive `tenant_id` as a command-line argument — this is how tenant isolation is enforced in Spark: the job only reads from and writes to paths that include its tenant_id.

**Sub-task 07-A — Deployment:**

```bash
helm install nexus-spark bitnami/spark \
  --namespace nexus-data \
  --set worker.replicaCount=3 \
  --set worker.resources.requests.cpu="4" \
  --set worker.resources.requests.memory="8Gi"
```

**Sub-task 07-B — Spark defaults with tenant-scoped S3A configuration:**

```bash
kubectl create configmap spark-defaults -n nexus-data \
  --from-literal=spark.jars.packages="io.delta:delta-core_2.12:2.4.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0" \
  --from-literal=spark.hadoop.fs.s3a.endpoint="http://nexus-minio.nexus-storage.svc.cluster.local:9000" \
  --from-literal=spark.hadoop.fs.s3a.path.style.access="true" \
  --from-literal=spark.hadoop.fs.s3a.impl="org.apache.hadoop.fs.s3a.S3AFileSystem"
```

**Sub-task 07-C — Multi-tenant Spark acceptance test:**

Run two Spark jobs simultaneously for different tenants. Verify the jobs write to different MinIO paths and their outputs do not overlap.

```bash
# Submit jobs for two tenants in parallel
spark-submit m1_classify_and_prepare.py batch-001 s3a://nexus-raw/alpha-corp/salesforce/account/ alpha-corp account &
spark-submit m1_classify_and_prepare.py batch-002 s3a://nexus-raw/beta-corp/odoo/res.partner/ beta-corp res.partner &
wait

# Verify no cross-contamination in MinIO
mc ls nexus/nexus-classified/alpha-corp/ | grep beta-corp
# Expected: no output

mc ls nexus/nexus-classified/beta-corp/ | grep alpha-corp
# Expected: no output
```

---

### P0-INFRA-08 — AWS Secrets Manager with Per-Tenant Paths

**What to build and why:**

Secrets are stored per-tenant and per-connector. The path convention is `nexus/{tenant_id}/{connector_id}/credentials`. This means:
- A connector worker for tenant A retrieves `nexus/tenant-a/connector-xyz/credentials`
- It cannot retrieve `nexus/tenant-b/.../credentials` because the IAM role for tenant A's service account only grants `GetSecretValue` on `nexus/tenant-a/*`

This is the Secrets Manager layer of tenant isolation: even if an application bug constructed the wrong `tenant_id`, the IAM policy would deny the secret retrieval.

**Sub-task 08-A — External Secrets Operator:**

```bash
helm install external-secrets external-secrets/external-secrets --namespace nexus-infra
```

**Sub-task 08-B — Per-team IAM policies:**

```json
// IAM policy for team2 service account (nexus-team2 role)
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["secretsmanager:GetSecretValue", "secretsmanager:DescribeSecret"],
      "Resource": "arn:aws:secretsmanager:eu-west-1:ACCOUNT:secret:nexus/*/connectors/*"
    }
  ]
}
// Note: * matches any tenant_id — the tenant_id in the path is a convention,
// not an IAM restriction in the MVP. In production, add per-tenant IAM conditions.
```

**Sub-task 08-C — Create test secrets for both test tenants:**

```bash
# Create test credentials for test-tenant-alpha
aws secretsmanager create-secret \
  --name nexus/test-tenant-alpha/connector-001/credentials \
  --secret-string '{"host":"pg-alpha.internal","database":"alpha_crm","username":"reader","password":"test-pw-1"}'

# Create test credentials for test-tenant-beta
aws secretsmanager create-secret \
  --name nexus/test-tenant-beta/connector-002/credentials \
  --secret-string '{"url":"https://beta.odoo.cloud","database":"beta_erp","username":"api_user","api_key":"test-key-2"}'
```

**Acceptance verification:**

```bash
# Verify team2 service account can retrieve connector credentials
kubectl run secrets-test --image=amazon/aws-cli --rm -it --restart=Never \
  -n nexus-app --serviceaccount=team2-data-intelligence -- \
  aws secretsmanager get-secret-value \
  --secret-id nexus/test-tenant-alpha/connector-001/credentials

# Verify team2 service account CANNOT retrieve non-connector secrets
kubectl run secrets-deny-test --image=amazon/aws-cli --rm -it --restart=Never \
  -n nexus-app --serviceaccount=team2-data-intelligence -- \
  aws secretsmanager get-secret-value \
  --secret-id nexus/platform/pinecone/credentials
# Expected: AccessDeniedException
```

---

### P0-INFRA-09 — Observability Stack with Tenant-Level Metrics

**What to build and why:**

Every Prometheus metric emitted by application services must include a `tenant_id` label. Without this label, you cannot tell which tenant is experiencing high consumer lag or slow sync times. You enforce this by documenting the metric naming convention (see STANDARDS.md contribution) and by configuring Grafana dashboards with tenant_id as a template variable.

**Sub-task 09-A — Prometheus + Grafana + Loki + Jaeger:**

```bash
helm install nexus-monitoring prometheus-community/kube-prometheus-stack \
  --namespace nexus-infra --set grafana.adminPassword='<from-secrets>'

helm install nexus-loki grafana/loki-stack --namespace nexus-infra

helm install nexus-jaeger jaegertracing/jaeger --namespace nexus-infra
```

**Sub-task 09-B — NEXUS Pipeline Health Dashboard (tenant-aware):**

The dashboard must have a `tenant_id` template variable at the top. When a data steward selects their tenant, all panels filter to show only their tenant's metrics.

```json
{
  "title": "NEXUS Pipeline Health",
  "templating": {
    "list": [
      {
        "name": "tenant_id",
        "type": "query",
        "query": "label_values(kafka_consumer_group_lag, tenant_id)",
        "label": "Tenant",
        "multi": false,
        "includeAll": true
      }
    ]
  },
  "panels": [
    {
      "title": "Connector Workers Lag — $tenant_id",
      "type": "timeseries",
      "targets": [{
        "expr": "kafka_consumer_group_lag{group='m1-connector-workers', tenant_id=~'$tenant_id'}"
      }]
    },
    {
      "title": "Delta Writers Lag — $tenant_id",
      "type": "timeseries",
      "targets": [{
        "expr": "kafka_consumer_group_lag{group='m1-delta-writers', tenant_id=~'$tenant_id'}"
      }]
    },
    {
      "title": "CDM Entities Written/min — $tenant_id",
      "type": "stat",
      "targets": [{
        "expr": "sum(rate(nexus_ai_writes_total{tenant_id=~'$tenant_id'}[1m])) * 60"
      }]
    },
    {
      "title": "Dead Letter Queue Rate — $tenant_id",
      "type": "timeseries",
      "targets": [{
        "expr": "rate(kafka_topic_messages_in_total{topic='m1.int.dead_letter'}[5m])"
      }]
    }
  ]
}
```

**Sub-task 09-C — Prometheus alert rules:**

```yaml
groups:
  - name: nexus-pipeline-health
    rules:
      - alert: M1DeltaWriterLagCritical
        expr: kafka_consumer_group_lag{group="m1-delta-writers"} > 50000
        for: 5m
        labels: {severity: critical}
        annotations:
          summary: "Delta writer lag > 50k for tenant {{ $labels.tenant_id }}"

      - alert: DeadLetterSpike
        expr: rate(kafka_topic_messages_in_total{topic="m1.int.dead_letter"}[5m]) > 10
        for: 2m
        labels: {severity: warning}
        annotations:
          summary: "Dead letter queue receiving {{ $value }} msgs/sec"

      - alert: TenantSyncStalled
        expr: time() - nexus_last_successful_sync_timestamp > 7200
        for: 10m
        labels: {severity: warning}
        annotations:
          summary: "Tenant {{ $labels.tenant_id }} has not completed a sync in 2+ hours"
```

---

### P0-INFRA-10 — HashiCorp Vault — Gestion Centralisée des Secrets

**What to build and why:**

HashiCorp Vault remplace tous les secrets en clair (YAML, Helm values, variables d'environnement, GitHub Secrets) par un accès sécurisé, auditable et multi-tenant. Vault est le seul endroit où résident les credentials réels. Les DAGs Airflow, les pods K8s, et les workflows CI/CD lisent dans Vault à l'exécution — aucun credential n'est committé dans Git.

**Structure des secrets (KV v2):**

```
secret/nexus/
├── platform/          # Secrets infra (équipe NEXUS uniquement)
│   ├── postgres       # host, port, database, username, password
│   ├── adventureworks # host, port, database, username, password
│   ├── minio          # endpoint, access_key, secret_key
│   ├── redis          # url
│   ├── neo4j          # uri, username, password
│   ├── elasticsearch  # url, password
│   ├── airflow        # fernet_key, admin_password
│   ├── grafana        # admin_password
│   ├── kong-oidc      # client_id, client_secret, session_secret
│   ├── llm            # openai_api_key, gemini_api_key, ...
│   ├── dockerhub      # username, token
│   └── github         # token (PAT)
└── tenants/           # Secrets clients (multi-tenant)
    └── {tenant_id}/sources/
        ├── salesforce  # client_id, client_secret, refresh_token, instance_url
        ├── servicenow  # instance_url, username, password
        ├── odoo        # host, port, database, username, password
        ├── postgresql  # host, port, database, username, password
        └── rest_api    # base_url, auth_type, api_key
```

**Sub-task 10-A — Déploiement local (Docker Compose, dev):**

Vault démarre en mode `dev` avec un token fixe. Le seed initial des secrets est exécuté au démarrage du compose.

```bash
# Lancer Vault avec les autres services
docker compose up -d

# Vérifier
docker exec nexus-vault vault status
docker exec nexus-vault vault kv list secret/nexus/platform

# Accès UI : http://localhost:8200  |  Token : nexus-dev-token

# Injecter un secret plateforme (dev)
docker exec nexus-vault vault kv put secret/nexus/platform/llm \
  openai_api_key="sk-proj-..." gemini_api_key="AIza..."

# Injecter des credentials tenant
docker exec nexus-vault vault kv put secret/nexus/tenants/acme-corp/sources/salesforce \
  client_id="3MVG9..." client_secret="B5E9BC..." \
  refresh_token="5Aep8..." instance_url="https://acme.my.salesforce.com"
```

Variables d'environnement requises dans `docker-compose.yaml` pour les DAGs Airflow :

| Variable | Valeur (dev) |
|----------|-------------|
| `VAULT_ADDR` | `http://vault:8200` |
| `VAULT_TOKEN` | `nexus-dev-token` |

**Sub-task 10-B — Déploiement Kubernetes (Helm, production):**

```bash
# Ajouter le repo officiel
helm repo add hashicorp https://helm.releases.hashicorp.com && helm repo update

# Installer Vault standalone
helm install vault hashicorp/vault \
  --namespace nexus-infra --create-namespace \
  --set server.standalone.enabled=true \
  --set server.dataStorage.size=10Gi \
  --set ui.enabled=true \
  --set injector.enabled=true

kubectl -n nexus-infra wait --for=condition=ready pod/vault-0 --timeout=120s

# Initialiser (prod uniquement — sauvegarder les unseal keys hors cluster)
kubectl -n nexus-infra exec vault-0 -- vault operator init \
  -key-shares=5 -key-threshold=3

# Unseal avec 3 des 5 clés
kubectl -n nexus-infra exec vault-0 -- vault operator unseal <KEY_1>
kubectl -n nexus-infra exec vault-0 -- vault operator unseal <KEY_2>
kubectl -n nexus-infra exec vault-0 -- vault operator unseal <KEY_3>

# Activer le moteur KV v2
kubectl -n nexus-infra exec vault-0 -- vault secrets enable -path=secret kv-v2
```

**Sub-task 10-C — Kubernetes Auth Method + Policies + Rôles:**

```bash
# Activer l'auth Kubernetes
kubectl -n nexus-infra exec vault-0 -- vault auth enable kubernetes
kubectl -n nexus-infra exec vault-0 -- vault write auth/kubernetes/config \
  kubernetes_host="https://kubernetes.default.svc:443"

# Policy pipeline (Airflow — lecture plateforme + tenants)
kubectl -n nexus-infra exec vault-0 -- vault policy write nexus-pipeline - <<'EOF'
path "secret/data/nexus/platform/*"            { capabilities = ["read", "list"] }
path "secret/data/nexus/tenants/*/sources/*"   { capabilities = ["read", "list"] }
path "secret/metadata/nexus/tenants/*/sources/*" { capabilities = ["list"] }
EOF

# Policy module (M1/M2/M4 APIs — lecture plateforme uniquement)
kubectl -n nexus-infra exec vault-0 -- vault policy write nexus-module - <<'EOF'
path "secret/data/nexus/platform/*" { capabilities = ["read"] }
EOF

# Policy tenant-admin (onboarding client — écriture tenant)
kubectl -n nexus-infra exec vault-0 -- vault policy write nexus-tenant-admin - <<'EOF'
path "secret/data/nexus/tenants/+/sources/*"     { capabilities = ["create", "read", "update", "delete", "list"] }
path "secret/metadata/nexus/tenants/+/sources/*" { capabilities = ["list", "delete"] }
EOF

# Rôle Airflow (namespaces data pipeline)
kubectl -n nexus-infra exec vault-0 -- vault write auth/kubernetes/role/nexus-pipeline \
  bound_service_account_names="*" \
  bound_service_account_namespaces="nexus,nexus-data,mindy-data" \
  policies="nexus-pipeline" ttl=1h

# Rôle APIs modules (namespace applicatif)
kubectl -n nexus-infra exec vault-0 -- vault write auth/kubernetes/role/nexus-module \
  bound_service_account_names="*" \
  bound_service_account_namespaces="nexus-app,nexus-dev" \
  policies="nexus-module" ttl=1h
```

**Sub-task 10-D — External Secrets Operator + Vault:**

ESO est déjà configuré dans le chart NEXUS (`k8s/templates/vault.yaml`). Il crée les Kubernetes Secrets à partir des chemins Vault selon l'interval de synchronisation (5 min).

```bash
# Vérifier l'installation ESO
kubectl get pods -n nexus-infra | grep external-secrets

# Si manquant
helm repo add external-secrets https://charts.external-secrets.io
helm install external-secrets external-secrets/external-secrets \
  --namespace nexus-infra --create-namespace

# Vérifier les ressources ESO
kubectl get externalsecrets -A
kubectl get clustersecretstores
```

**Sub-task 10-E — GitHub Actions AppRole (CI/CD):**

```bash
# Activer AppRole
vault auth enable approle

# Policy CI/CD (dockerhub + github PAT uniquement)
vault policy write nexus-ci-cd - <<'EOF'
path "secret/data/nexus/platform/dockerhub" { capabilities = ["read"] }
path "secret/data/nexus/platform/github"    { capabilities = ["read"] }
EOF

# Créer le rôle CI/CD
vault write auth/approle/role/nexus-ci-cd \
  token_policies="nexus-ci-cd" token_ttl=30m secret_id_ttl=0

# Récupérer les identifiants (stocker dans GitHub Secrets)
vault read auth/approle/role/nexus-ci-cd/role-id
vault write -f auth/approle/role/nexus-ci-cd/secret-id
```

Workflow GitHub Actions :

```yaml
# .github/workflows/build-and-push.yml
steps:
  - uses: hashicorp/vault-action@v3
    with:
      url: https://vault.nexus.internal:8200
      method: approle
      roleId: ${{ secrets.VAULT_ROLE_ID }}
      secretId: ${{ secrets.VAULT_SECRET_ID }}
      secrets: |
        secret/data/nexus/platform/dockerhub username | DOCKERHUB_USERNAME ;
        secret/data/nexus/platform/dockerhub token    | DOCKERHUB_TOKEN ;
        secret/data/nexus/platform/github token       | GITHUB_TOKEN
```

> Seuls `VAULT_ROLE_ID` et `VAULT_SECRET_ID` restent dans GitHub Secrets. Tous les autres credentials sont dans Vault.

**Sub-task 10-F — Client Python (DAGs Airflow):**

```python
import hvac, os

def get_vault_client() -> hvac.Client:
    client = hvac.Client(url=os.environ.get("VAULT_ADDR", "http://vault:8200"))
    token = os.environ.get("VAULT_TOKEN")
    if token:
        client.token = token
        return client
    # K8s auth method (production)
    jwt_path = "/var/run/secrets/kubernetes.io/serviceaccount/token"
    with open(jwt_path) as f:
        client.auth.kubernetes.login(
            role=os.environ.get("VAULT_ROLE", "nexus-pipeline"), jwt=f.read()
        )
    return client

def get_platform_secret(path: str) -> dict:
    """Lecture d'un secret plateforme. Ex: path='postgres', 'minio', 'llm'"""
    return get_vault_client().secrets.kv.v2.read_secret_version(
        path=f"nexus/platform/{path}", mount_point="secret"
    )["data"]["data"]

def get_tenant_source_credentials(tenant_id: str, source_type: str) -> dict:
    """Lecture des credentials d'une source client. Ex: tenant_id='acme-corp', source_type='salesforce'"""
    return get_vault_client().secrets.kv.v2.read_secret_version(
        path=f"nexus/tenants/{tenant_id}/sources/{source_type}", mount_point="secret"
    )["data"]["data"]

def list_tenant_sources(tenant_id: str) -> list:
    return get_vault_client().secrets.kv.v2.list_secrets(
        path=f"nexus/tenants/{tenant_id}/sources", mount_point="secret"
    )["data"]["keys"]
```

**Sub-task 10-G — Onboarding multi-tenant (flux M1 API → Vault):**

```
Client s'inscrit → Okta crée le user → M1 API crée le tenant_id
    → Vault: crée secret/nexus/tenants/{tenant_id}/
    → Client configure ses sources via UI admin ou API
    → Credentials écrits dans Vault (jamais dans un fichier YAML)
    → Pipeline Airflow lit les credentials depuis Vault à l'exécution
```

```bash
# Créer un tenant (M1 API)
curl -X POST http://kong:30800/api/v1/m1/tenants \
  -H "Authorization: Bearer $OKTA_TOKEN" \
  -d '{"name": "ACME Corp", "plan": "enterprise"}'

# Ajouter une source (credentials → Vault, pas dans Git)
curl -X POST http://kong:30800/api/v1/m1/tenants/acme-corp/sources \
  -H "Authorization: Bearer $OKTA_TOKEN" \
  -d '{"type": "salesforce", "connection": {"client_id": "3MVG9...", ...}}'
```

**Acceptance criteria:**

- [ ] `vault status` retourne `Sealed: false` et `HA Enabled: false` (standalone) ou `true` (HA prod)
- [ ] `vault kv list secret/nexus/platform` liste tous les secrets plateforme seedés
- [ ] Un pod Airflow dans `nexus-data` peut lire `secret/nexus/platform/postgres` via K8s auth
- [ ] Un pod Airflow ne peut PAS lire `secret/nexus/tenants/autre-tenant/sources/*` (policy enforcement)
- [ ] ESO synchronise les Kubernetes Secrets dans les 5 minutes suivant une modification Vault
- [ ] Le workflow GitHub Actions récupère dockerhub credentials depuis Vault sans GitHub Secret supplémentaire
- [ ] Aucun credential en clair dans Git (`sources.yaml`, `values.yaml`, `pyproject.toml`, `.env`)
- [ ] Audit log Vault activé et consultable (`vault audit list`)

**Migration checklist:**

| Phase | Action | Priorité |
|-------|--------|----------|
| 1 — Quick wins | Révoquer le token GitHub hardcodé dans `nexus-m2-api/pyproject.toml` | Immédiat |
| 1 | Activer GitHub Secret Scanning (Settings → Code security) | Immédiat |
| 1 | Ajouter `*.env`, `secrets/` au `.gitignore` | Immédiat |
| 2 — Vault local | Lancer Vault via Docker Compose ; seed les secrets réels | Semaine 1 |
| 2 | Tester un DAG Airflow avec lecture Vault | Semaine 1 |
| 3 — Vault K8s | Installer via Helm chart officiel ; init + unseal ; K8s auth | Semaines 1–2 |
| 3 | Installer ESO ; créer policies et rôles ; migrer tous les secrets plateforme | Semaines 1–2 |
| 4 — CI/CD | Configurer AppRole ; mettre à jour workflows ; supprimer anciens GitHub Secrets | Semaines 2–3 |
| 5 — Multi-tenant | Implémenter API onboarding dans M1 ; intégrer création Vault ; tests E2E | Mois 1 |

---

## days 3–16 — Platform Operations

**dayly tasks:**
- Review Grafana dashboard: check for consumer lag and dead letter queue spikes
- Review Kubernetes resource usage: resize Spark workers if OOM events appear
- Check Vault audit log for `permission denied` events (possible misconfiguration ou credential leak)
- Rotate credentials in Vault on the first Monday of each month (`vault kv put` + ESO re-sync)

**Team support schedule:**

| day | Action for other teams |
|---|---|
| 3 | Verify Team 2's connector pods can read from Vault (`nexus-pipeline` role) for both test tenants |
| 5 | Debug Spark S3A connectivity issues (common on first Spark job) |
| 7 | Provision Pinecone indexes, Neo4j AuraDB, TimescaleDB on RDS for Team 3 |
| 9 | Help Team 3 debug Neo4j connectivity from nexus-app namespace |
| 11 | Run concurrent load test: two tenants syncing simultaneously — check for interference |
| 13 | Enable PostgreSQL audit logging. Enable Kafka TLS. Enable MinIO server-side encryption. |

---

## What You Produce — Complete Output List

| Output | Format | When |
|---|---|---|
| EKS cluster + 4 namespaces + RBAC + network policies | K8s manifests in Git | day 1 |
| 18 KafkaTopic CRDs with correct partitions/retention | YAML in Git | day 1 |
| PostgreSQL cluster + nexus_system schema + RLS policies | Helm values + SQL migration in Git | day 1 |
| Redis deployment | Helm values in Git | day 1 |
| 4 MinIO buckets with path convention test | Helm values + test script | day 1 |
| Kong with 3 global plugins + all service routes | kong.yaml declarative config in Git | day 2 |
| Airflow with custom Docker image + Kafka + Spark providers | Dockerfile + Helm values | day 2 |
| Spark cluster with Delta + Kafka packages + spark-defaults | Helm values + ConfigMap | day 2 |
| External Secrets Operator + per-tenant secret paths | ClusterSecretStore + IAM policies | day 2 |
| HashiCorp Vault (standalone) + K8s auth + policies + roles | Helm values + vault policy files in Git | day 2 |
| Vault Python client (`vault_client.py`) pour DAGs Airflow | Module Python dans `nexus-m5-data-pipeline/` | day 2 |
| Tenant-aware Grafana dashboard | Dashboard JSON in Git | day 2 |
| Prometheus alert rules (3 alerts) | prometheus-rules.yaml in Git | day 2 |
| Pinecone indexes + Neo4j + TimescaleDB provisioned | Terraform or console + credentials in Secrets Manager | day 7 |
| Multi-tenant parallel Spark test results | Test output document | day 11 |

---

*NEXUS Developer Operational Plan — Platform Team Edition*
*Mentis Consulting · InfiniteMind · Brussels · Confidential*
