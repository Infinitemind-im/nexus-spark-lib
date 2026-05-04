# NEXUS — Developer Operational Plan
### Build Specification v1.0 · Mentis Consulting · February 2026

---

## How to Read This Document

This is the authoritative task list for the NEXUS MVP build. Every task has an ID, an owner role, a list of tasks it depends on, a precise description of what to build, and a set of acceptance criteria that define done. Nothing is done until its acceptance criteria pass.

**Task IDs follow the pattern:** `{Phase}-{Module}-{Sequence}` — e.g. `P0-INFRA-03` is Phase 0, Infrastructure, task 3.

**Owner roles:**
- **DevOps** — Infrastructure & platform engineer
- **Backend-M1** — M1 Data Intelligence Layer developer
- **Backend-M2** — M2 AI Intelligence Hub developer
- **Backend-M4** — M4 Workflow developer
- **Full-stack** — M6 UI developer
- **ML** — ML/AI engineer (Spark jobs, classification, embeddings)
- **Lead** — Tech lead / architect

**Three hard rules that apply to every task:**

> 1. No module calls another module directly via REST for pipeline events. All pipeline communication goes through Kafka. REST is only for synchronous management operations (CRUD, status queries, UI requests).
> 2. No LLM reasoning in the Operational pipeline (M1-B). LLM calls belong only in M2 Structural mode.
> 3. No task is started before its dependencies are marked done and their acceptance criteria verified.

---

## Table of Contents

- [Phase 0 — Infrastructure Foundation](#phase-0--infrastructure-foundation)
- [Phase 1 — Kafka Bus & CDM Registry](#phase-1--kafka-bus--cdm-registry)
- [Phase 2 — M1 Connectors & Operational Pipeline](#phase-2--m1-connectors--operational-pipeline)
- [Phase 3 — M1 Structural Sub-Cycle](#phase-3--m1-structural-sub-cycle)
- [Phase 4 — M2 Intelligence Hub](#phase-4--m2-intelligence-hub)
- [Phase 5 — M3 Knowledge Specialization](#phase-5--m3-knowledge-specialization)
- [Phase 6 — M4 Workflow & Governance](#phase-6--m4-workflow--governance)
- [Phase 7 — M6 User Interface](#phase-7--m6-user-interface)
- [Phase 8 — Integration & End-to-End Testing](#phase-8--integration--end-to-end-testing)
- [Inter-Team Contracts](#inter-team-contracts)
- [Definition of Done Checklist](#definition-of-done-checklist)

---

## Phase 0 — Infrastructure Foundation
**Duration:** Week 1–2 · **Owner:** DevOps + Lead · **Prerequisite for everything**

Nothing else starts until Phase 0 is complete. All developers block on this phase.

---

### P0-INFRA-01 — Kubernetes Namespaces & RBAC

**Owner:** DevOps  
**Depends on:** Nothing  

**What to build:**  
Create four Kubernetes namespaces with RBAC policies. No pod in `nexus-app` may directly address a pod in `nexus-data` except through the defined service names. No pod outside `nexus-infra` may modify Kong configuration.

```
nexus-infra    — Kong, cert-manager, External Secrets Operator
nexus-data     — Kafka, PostgreSQL, Redis, Spark, Airflow
nexus-storage  — MinIO (Delta Lake)
nexus-app      — All M1–M6 application pods
```

**Acceptance criteria:**
- All four namespaces exist and are labelled `project=nexus`
- A pod in `nexus-app` cannot `kubectl exec` into `nexus-data`
- Network policies in place: `nexus-app` → `nexus-data` allowed only on defined service ports
- `kubectl get namespaces` shows all four with status `Active`

---

### P0-INFRA-02 — Kafka Cluster

**Owner:** DevOps  
**Depends on:** P0-INFRA-01  

**What to build:**  
Deploy a 3-broker Kafka cluster in `nexus-data` namespace using the Strimzi operator. Deploy Kafka UI (Redpanda Console or Kafdrop) for developer visibility at `kafka-ui.nexus.internal`.

```yaml
# Minimum broker config
num.partitions: 4
default.replication.factor: 2
min.insync.replicas: 1
log.retention.hours: 168  # 7 days default
auto.create.topics.enable: false  # Topics must be created explicitly
```

Create **all internal M1 topics** listed in the table below. Topics must exist before any worker tries to produce or consume.

| Topic | Partitions | Replication | Retention |
|---|---|---|---|
| `m1.int.structural_cycle_triggered` | 4 | 2 | 7d |
| `m1.int.source_schema_extracted` | 4 | 2 | 14d |
| `m1.int.sync_requested` | 4 | 2 | 7d |
| `m1.int.raw_records` | 16 | 2 | 3d |
| `m1.int.delta_batch_ready` | 4 | 2 | 7d |
| `m1.int.classified_records` | 8 | 2 | 3d |
| `m1.int.cdm_entities_ready` | 8 | 2 | 7d |
| `m1.int.ai_routing_decided` | 4 | 2 | 7d |
| `m1.int.ai_write_completed` | 4 | 2 | 14d |
| `m1.int.sync_failed` | 4 | 2 | 30d |
| `m1.int.delta_write_failed` | 4 | 2 | 30d |
| `m1.int.spark_job_failed` | 4 | 2 | 30d |
| `m1.int.mapping_failed` | 4 | 2 | 30d |
| `m1.int.dead_letter` | 4 | 3 | 90d |
| `nexus.cdm.extension_proposed` | 4 | 2 | 30d |
| `nexus.cdm.version_published` | 4 | 3 | 90d |
| `nexus.cdm.extension_rejected` | 4 | 2 | 30d |
| `nexus.cdm.rollback_requested` | 4 | 2 | 30d |

Cross-module tenant-scoped topics (`{tid}.m1.*`, `{tid}.m2.*`, etc.) are created **dynamically by application code** when a new tenant is onboarded. Do not create them manually.

**Acceptance criteria:**
- `kafka-topics.sh --list` returns all 18 topics listed above
- `m1.int.raw_records` confirms 16 partitions, RF 2
- `m1.int.dead_letter` confirms RF 3
- Kafka UI accessible at `kafka-ui.nexus.internal` showing all topics
- Produce and consume a test message on `m1.int.sync_requested` successfully

---

### P0-INFRA-03 — PostgreSQL & Redis

**Owner:** DevOps  
**Depends on:** P0-INFRA-01  

**What to build:**  
Deploy PostgreSQL 15 and Redis 7 in `nexus-data` namespace.

**PostgreSQL — create these schemas and tables immediately:**

```sql
-- Schema: nexus_system (platform config, CDM registry)
CREATE SCHEMA nexus_system;

-- CDM version registry
CREATE TABLE nexus_system.cdm_versions (
    version          VARCHAR(20) PRIMARY KEY,
    status           VARCHAR(20) NOT NULL CHECK (status IN ('draft','active','deprecated')),
    tenant_id        VARCHAR(100) NOT NULL,
    changes_summary  TEXT,
    published_at     TIMESTAMPTZ,
    published_by     VARCHAR(200),
    created_at       TIMESTAMPTZ DEFAULT NOW()
);

-- Connector registry
CREATE TABLE nexus_system.connectors (
    connector_id     UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id        VARCHAR(100) NOT NULL,
    system_type      VARCHAR(50) NOT NULL,
    connector_name   VARCHAR(200) NOT NULL,
    status           VARCHAR(20) DEFAULT 'inactive',
    config           JSONB,
    last_sync_at     TIMESTAMPTZ,
    created_at       TIMESTAMPTZ DEFAULT NOW()
);

-- CDM field mapping registry
CREATE TABLE nexus_system.cdm_mappings (
    mapping_id       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id        VARCHAR(100) NOT NULL,
    cdm_version      VARCHAR(20) NOT NULL,
    source_system    VARCHAR(100) NOT NULL,
    source_table     VARCHAR(200) NOT NULL,
    source_field     VARCHAR(200) NOT NULL,
    cdm_entity       VARCHAR(100) NOT NULL,
    cdm_field        VARCHAR(100) NOT NULL,
    confidence       DECIMAL(5,2),
    tier             SMALLINT CHECK (tier IN (1,2,3)),
    approved_by      VARCHAR(200),
    approved_at      TIMESTAMPTZ,
    created_at       TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (tenant_id, source_system, source_table, source_field, cdm_version)
);

-- Sync job tracking
CREATE TABLE nexus_system.sync_jobs (
    job_id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    connector_id     UUID NOT NULL REFERENCES nexus_system.connectors(connector_id),
    tenant_id        VARCHAR(100) NOT NULL,
    status           VARCHAR(20) DEFAULT 'pending',
    sync_mode        VARCHAR(20),
    started_at       TIMESTAMPTZ,
    completed_at     TIMESTAMPTZ,
    records_extracted INT DEFAULT 0,
    records_failed    INT DEFAULT 0,
    error_message    TEXT
);
```

**Redis** — used by M1 CDM Mapper for mapping registry cache (5-minute TTL), and by M2 for working memory across agents.

**Acceptance criteria:**
- `psql -c "\dn"` shows `nexus_system` schema
- All four tables exist with correct columns — verify with `\d table_name`
- Redis PING returns PONG from inside `nexus-app` namespace
- PostgreSQL accessible at `nexus-postgres.nexus-data.svc.cluster.local:5432`
- Redis accessible at `nexus-redis.nexus-data.svc.cluster.local:6379`

---

### P0-INFRA-04 — MinIO (Delta Lake Storage)

**Owner:** DevOps  
**Depends on:** P0-INFRA-01  

**What to build:**  
Deploy MinIO in `nexus-storage` namespace. Create three buckets for the three Delta Lake zones.

```
nexus-raw         — M1 Delta Writer raw zone writes here
nexus-classified  — M1 Spark Classify job writes here
nexus-cdm         — M1 Spark CDM rebuild job writes here
```

Path convention inside each bucket: `/{tenant_id}/{system_type}/{entity}/`

Expose an S3-compatible endpoint at `minio.nexus-storage.svc.cluster.local:9000`. All application code uses AWS S3 SDK pointing to this endpoint.

**Acceptance criteria:**
- MinIO console accessible at `minio-console.nexus.internal`
- Three buckets exist
- Write a test file to `nexus-raw/test-tenant/test-system/test-entity/test.parquet` from a pod in `nexus-app`
- Read it back successfully
- Delete test file

---

### P0-INFRA-05 — Kong API Gateway

**Owner:** DevOps  
**Depends on:** P0-INFRA-01  

**What to build:**  
Deploy Kong in `nexus-infra` namespace with three global plugins active on all routes from day one. No route may be registered without these plugins.

```
Plugin 1: jwt           — Validates JWT on every request. Extracts tenant_id from
                          claims and injects as X-Tenant-ID header. Services never
                          decode JWT themselves — they read X-Tenant-ID only.

Plugin 2: rate-limiting — Per-tenant rate limits: 100 req/min (MVP).
                          Key: X-Tenant-ID header.

Plugin 3: correlation-id — Injects X-Correlation-ID on every request.
                           Propagate this ID in all Kafka message headers.
```

**Acceptance criteria:**
- Request without JWT returns 401
- Request with valid JWT but mismatched tenant_id returns 403
- `X-Tenant-ID` header present on all requests reaching backend services
- `X-Correlation-ID` header present and unique per request
- Rate limiting triggers 429 after 100 requests/min from same tenant

---

### P0-INFRA-06 — Airflow

**Owner:** DevOps  
**Depends on:** P0-INFRA-02, P0-INFRA-03  

**What to build:**  
Deploy Apache Airflow 2.8+ in `nexus-data` namespace with `airflow-providers-apache-kafka` installed. Airflow must be able to submit Spark jobs (SparkSubmitOperator) and subscribe to Kafka topics (KafkaSensor).

Create a dedicated Airflow service account with permissions to read Kafka consumer group offsets and submit Spark jobs to the Spark cluster.

**Acceptance criteria:**
- Airflow UI accessible at `airflow.nexus.internal`
- `from airflow.providers.apache.kafka.sensors.kafka import KafkaSensor` imports without error in a test DAG
- SparkSubmitOperator can reach Spark master at `spark://spark-master.nexus-data.svc.cluster.local:7077`
- Airflow PostgreSQL metadata DB is separate from `nexus-postgres` (use a dedicated Airflow DB)

---

### P0-INFRA-07 — Spark Cluster

**Owner:** DevOps  
**Depends on:** P0-INFRA-01, P0-INFRA-04  

**What to build:**  
Deploy Spark 3.4+ standalone cluster in `nexus-data` namespace. Install the Delta Lake and Kafka Spark connectors as default packages so every submitted job has access without specifying `--packages`.

```
Required packages (pre-installed on cluster):
- io.delta:delta-core_2.12:2.4.0
- org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0
```

Configure Spark to read/write MinIO via S3A:
```
spark.hadoop.fs.s3a.endpoint       = minio.nexus-storage.svc.cluster.local:9000
spark.hadoop.fs.s3a.path.style.access = true
spark.hadoop.fs.s3a.access.key     = <from secrets>
spark.hadoop.fs.s3a.secret.key     = <from secrets>
```

**Acceptance criteria:**
- Submit a test Spark job that reads a parquet file from `nexus-raw` bucket and prints row count — must succeed
- Submit a test Spark job that reads from `m1.int.sync_requested` Kafka topic — must connect and consume
- Submit a test Spark job that writes a Delta table to `nexus-classified` bucket — must produce `_delta_log/` directory

---

### P0-INFRA-08 — Secrets Manager & External Secrets Operator

**Owner:** DevOps  
**Depends on:** P0-INFRA-01  

**What to build:**  
Deploy External Secrets Operator. Configure it to sync from AWS Secrets Manager into Kubernetes secrets in the `nexus-app` namespace. Every connector credential must be stored as a separate secret with the path convention:

```
nexus/{tenant_id}/{connector_id}/credentials
```

Secret contains fields appropriate to the connector type:
- Salesforce: `username`, `password`, `security_token`, `domain`
- Odoo: `url`, `database`, `username`, `api_key`
- ServiceNow: `instance`, `username`, `password`
- Database connectors: `host`, `port`, `database`, `username`, `password`

Application code **never reads credentials from environment variables or config files**. All credential access goes through AWS Secrets Manager at job start. Credentials are never logged.

**Acceptance criteria:**
- Create a test secret in AWS Secrets Manager at `nexus/test-tenant/test-connector/credentials`
- ExternalSecret resource syncs it into a Kubernetes Secret within 60 seconds
- A pod in `nexus-app` can read the secret value
- Verify secret value does NOT appear in pod logs or env (`kubectl exec` and inspect)

---

### P0-INFRA-09 — Observability Stack

**Owner:** DevOps  
**Depends on:** P0-INFRA-01  

**What to build:**  
Deploy Prometheus, Grafana, Loki, and Jaeger. Configure Kafka JMX exporter so consumer group lag is visible in Prometheus immediately.

Create one Grafana dashboard from day one: **NEXUS Pipeline Health**. It must show:
- Consumer lag per consumer group (4 groups: `m1-connector-workers`, `m1-delta-writers`, `m1-cdm-mappers`, `m1-ai-store-writers`)
- Records produced to `m1.int.raw_records` per minute
- Records produced to `m1.int.ai_write_completed` per minute

Configure Prometheus alert rules:

```yaml
# Alert: m1-delta-writers lag critical
- alert: M1DeltaWriterLagCritical
  expr: kafka_consumer_group_lag{group="m1-delta-writers"} > 50000
  for: 5m
  labels:
    severity: critical

# Alert: Any worker dead letter spike
- alert: DeadLetterSpike
  expr: rate(kafka_topic_messages_in_total{topic="m1.int.dead_letter"}[5m]) > 10
  for: 2m
  labels:
    severity: warning
```

**Acceptance criteria:**
- Grafana NEXUS Pipeline Health dashboard loads and shows all four consumer group lag panels (all showing 0 — no consumers yet)
- Prometheus scrapes Kafka JMX metrics — verify with `kafka_consumer_group_lag` metric query
- Jaeger UI accessible at `jaeger.nexus.internal`
- Loki receives logs from at least one test pod

---

## Phase 1 — Kafka Bus & CDM Registry
**Duration:** Week 2–3 · **Owner:** Backend-M1 + Lead · **Prerequisite for Phase 2 and 3**

---

### P1-CORE-01 — Base Message Envelope

**Owner:** Backend-M1  
**Depends on:** P0-INFRA-02  

**What to build:**  
Create the Python package `nexus_core` (shared library imported by all workers). Define the standard Kafka message envelope. Every message published on any Kafka topic must use this envelope — no exceptions.

```python
# nexus_core/messaging.py

from dataclasses import dataclass, field
from typing import Any, Optional
import uuid
from datetime import datetime, timezone

@dataclass
class NexusMessage:
    topic: str
    tenant_id: str
    payload: dict[str, Any]
    message_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    correlation_id: str = ""          # Propagated from X-Correlation-ID header
    trace_id: str = ""                # OpenTelemetry trace ID
    schema_version: str = "1.0"
    published_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )

    def to_kafka_value(self) -> bytes:
        """Serialise to JSON bytes for Kafka producer."""
        ...

    @classmethod
    def from_kafka_value(cls, raw: bytes) -> "NexusMessage":
        """Deserialise from Kafka consumer record value."""
        ...
```

Also implement:
- `NexusProducer` — wraps `confluent_kafka.Producer`. Enforces topic name format. Adds `correlation_id` and `trace_id` to every message. Logs every publish at DEBUG level (never logs payload values, only topic + message_id + tenant_id).
- `NexusConsumer` — wraps `confluent_kafka.Consumer`. Offset is **never committed automatically**. The worker must call `consumer.commit()` explicitly after successful processing. On exception, offset is not committed.

**Acceptance criteria:**
- Publish 10 messages to `m1.int.sync_requested` using `NexusProducer`
- Consume them with `NexusConsumer` — all 10 received with correct envelope fields
- Simulate a consumer crash mid-batch (raise exception after message 5). Restart consumer. Confirm messages 5–10 are redelivered (not skipped)
- Confirm credential fields (`password`, `api_key`, `token`, `secret`) are not loggable — the `NexusProducer` must strip these from log output

---

### P1-CORE-02 — Tenant Context

**Owner:** Backend-M1  
**Depends on:** P1-CORE-01  

**What to build:**  
`TenantContext` is a Python `contextvars.ContextVar` that holds the current `tenant_id` throughout a request or message processing chain. Every worker sets this at the start of processing a Kafka message. All downstream code reads from it.

```python
# nexus_core/tenant.py

from contextvars import ContextVar
from dataclasses import dataclass

_tenant_ctx: ContextVar["TenantContext"] = ContextVar("tenant_ctx")

@dataclass(frozen=True)
class TenantContext:
    tenant_id: str
    plan: str              # "starter" | "professional" | "enterprise"
    cdm_version: str       # e.g. "1.2"

def get_tenant() -> TenantContext:
    """Raises LookupError if called outside a tenant context."""
    return _tenant_ctx.get()

def set_tenant(ctx: TenantContext) -> None:
    _tenant_ctx.set(ctx)
```

Also implement `CrossModuleTopicNamer` — the single source of truth for tenant-scoped topic names:

```python
# nexus_core/topics.py

class CrossModuleTopicNamer:
    @staticmethod
    def m1_outbound(tenant_id: str, event: str) -> str:
        # e.g. "acme-corp.m1.sync_completed"
        return f"{tenant_id}.m1.{event}"

    @staticmethod
    def m2_outbound(tenant_id: str, event: str) -> str:
        return f"{tenant_id}.m2.{event}"

    @staticmethod
    def m4_outbound(tenant_id: str, event: str) -> str:
        return f"{tenant_id}.m4.{event}"

    @staticmethod
    def cdm(event: str) -> str:
        # Platform-wide, no tenant prefix
        return f"nexus.cdm.{event}"
```

**Rule:** Any code that constructs a topic name by string concatenation outside this class is wrong and must be refactored.

**Acceptance criteria:**
- `CrossModuleTopicNamer.m1_outbound("acme-corp", "sync_completed")` returns `"acme-corp.m1.sync_completed"`
- `get_tenant()` raises `LookupError` when called outside a context — verified by unit test
- `set_tenant()` + `get_tenant()` returns correct context — verified by unit test
- Async context propagation: context set in coroutine A is readable in coroutine B spawned from A — verified by unit test

---

### P1-CORE-03 — CDM Registry Service

**Owner:** Backend-M1  
**Depends on:** P0-INFRA-03, P1-CORE-02  

**What to build:**  
A Python service class `CDMRegistryService` that provides the mapping registry to all workers. It wraps the `nexus_system.cdm_mappings` PostgreSQL table and maintains an in-memory cache per tenant.

```python
# nexus_core/cdm_registry.py

class CDMRegistryService:

    def get_mapping(
        self,
        tenant_id: str,
        source_system: str,
        source_table: str,
        source_field: str,
        cdm_version: str,
    ) -> MappingResult | None:
        """
        Returns the CDM mapping for a field.
        Cache TTL: 5 minutes per tenant.
        Returns None if no mapping exists (caller handles Tier 3).
        """

    def invalidate_cache(self, tenant_id: str) -> None:
        """
        Called when {tid}.m4.mapping_approved is received.
        Drops the in-memory cache for this tenant.
        Next call to get_mapping() hits PostgreSQL.
        """

    def get_active_cdm_version(self, tenant_id: str) -> str:
        """
        Returns the version string of the currently active CDM for this tenant.
        e.g. "1.2"
        """

@dataclass
class MappingResult:
    cdm_entity: str
    cdm_field: str
    confidence: float
    tier: int   # 1, 2, or 3
```

**Acceptance criteria:**
- Insert a test mapping row into `nexus_system.cdm_mappings`
- `get_mapping()` returns it on first call (cache miss → PostgreSQL hit)
- Call `get_mapping()` again — verify it returns from cache (no PostgreSQL query on second call)
- Call `invalidate_cache()` — verify next call hits PostgreSQL again
- `get_mapping()` for a non-existent field returns `None` — not an exception

---

## Phase 2 — M1 Connectors & Operational Pipeline
**Duration:** Week 3–8 · **Blocks:** Phase 5 (M3 writers), Phase 8 (E2E)

The operational pipeline is built left to right, one stage at a time. Do not build stage N+1 until stage N passes its acceptance criteria and publishes its Kafka message.

---

### P2-M1-01 — Base Connector Interface

**Owner:** Backend-M1  
**Depends on:** P1-CORE-01, P1-CORE-02  

**What to build:**  
The abstract `BaseConnector` class that all seven source connectors implement. No connector may access Kafka, PostgreSQL, or any storage directly. Connectors do one thing: connect to a source system and yield records.

```python
# m1/connectors/base.py

from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import AsyncIterator

class SyncMode(str, Enum):
    INCREMENTAL = "incremental"
    FULL_REFRESH = "full_refresh"

class SystemType(str, Enum):
    SALESFORCE   = "salesforce"
    ODOO         = "odoo"
    SERVICENOW   = "servicenow"
    WORKDAY      = "workday"
    POSTGRESQL   = "postgresql"
    MYSQL        = "mysql"
    SQLSERVER    = "sqlserver"

@dataclass
class ConnectorConfig:
    connector_id: str
    tenant_id: str
    system_type: SystemType
    sync_mode: SyncMode
    batch_size: int = 1000
    timeout_sec: int = 300
    # credentials: loaded from Secrets Manager at connect() time, never stored here

@dataclass
class ConnectionResult:
    success: bool
    system_version: str = ""   # e.g. "Salesforce API v58", "Odoo 16.0"
    error: str = ""

@dataclass
class RawRecord:
    source_id: str             # The primary key value from the source system
    source_table: str          # Table or object name in the source system
    raw_data: dict             # Exact field names and values from source — no transformation
    extracted_at: str          # ISO timestamp

class BaseConnector(ABC):

    @abstractmethod
    async def connect(self) -> ConnectionResult:
        """Load credentials from Secrets Manager. Establish connection. Return result."""

    @abstractmethod
    async def disconnect(self) -> None:
        """Close connection. Release resources."""

    @abstractmethod
    async def get_schema(self) -> dict:
        """
        Return full schema metadata for this source system.
        Format: { "table_name": { "fields": [...], "row_count": int, ... } }
        Used by M1-A Structural sub-cycle only.
        """

    @abstractmethod
    def extract(self, last_sync_at: str | None) -> AsyncIterator[RawRecord]:
        """
        Stream records from source.
        If last_sync_at is provided: incremental (only records updated after that timestamp).
        If None: full refresh.
        Yields RawRecord objects. Never buffers the entire result in memory.
        """
```

**Acceptance criteria:**
- `BaseConnector` is abstract — `ConnectorFactory.create(config)` raises `TypeError` if instantiated directly
- `ConnectorFactory.create(config)` returns the correct subclass for each `SystemType`
- `RawRecord.raw_data` field values are never transformed — if source returns `"KUNNR": "12345"`, raw_data must contain exactly that

---

### P2-M1-02 — PostgreSQL Connector

**Owner:** Backend-M1  
**Depends on:** P2-M1-01  

**What to build:**  
The first connector implementation. PostgreSQL is built first because it connects to the NEXUS internal database — no external credentials needed, easiest to validate.

**Schema extraction (`get_schema`):**
- Query `information_schema.columns` for all tables in the target schema
- Query `information_schema.table_constraints` + `constraint_column_usage` to identify primary keys
- For each table, count rows (approximate: `pg_class.reltuples` — do not run `COUNT(*)` on large tables)
- Collect 3 sample rows per table (non-PII fields only — skip fields named `email`, `phone`, `password`, `ssn`, `tax_id`)

**Extraction (`extract`):**
- `INCREMENTAL`: `SELECT * FROM {table} WHERE {updated_at_col} > $last_sync_at ORDER BY {updated_at_col} ASC LIMIT {batch_size}`
- `FULL_REFRESH`: same query without the WHERE clause
- Auto-detect the incremental key by looking for columns named `updated_at`, `modified_at`, `last_modified`, `update_date` in that priority order. If none found, fall back to full refresh and log a warning.
- Always add `LIMIT {batch_size}` — never fetch unbounded result sets
- Use `asyncpg` connection pool (min 2, max 10 connections)

**Acceptance criteria:**
- Connect to `nexus-postgres.nexus-data.svc.cluster.local` using the test connector credentials
- `get_schema()` returns correct field names and types for `nexus_system.connectors` table
- `extract()` with `last_sync_at=None` yields all rows from a test table with 100 rows
- `extract()` with `last_sync_at=<timestamp>` yields only rows modified after that time — verify with a row inserted before and after the timestamp
- No row appears twice in a single extraction run

---

### P2-M1-03 — Connector Worker (Kafka Consumer)

**Owner:** Backend-M1  
**Depends on:** P2-M1-01, P2-M1-02, P1-CORE-01, P1-CORE-02, P1-CORE-03  

**What to build:**  
The long-running Kubernetes Deployment that subscribes to `m1.int.sync_requested` and orchestrates connector execution. This is the bridge between Airflow's trigger and the connector code.

**Processing loop:**

```
1. Consume message from m1.int.sync_requested
2. Set TenantContext from message tenant_id
3. Load ConnectorConfig from nexus_system.connectors by connector_id
4. Load credentials from Secrets Manager at nexus/{tenant_id}/{connector_id}/credentials
5. connector = ConnectorFactory.create(config)
6. result = await connector.connect()
   - On failure: publish to m1.int.sync_failed, DO NOT commit offset, return
7. Check backpressure: query Kafka consumer lag for m1.int.raw_records
   - If lag > 50,000: sleep 30s and re-check. Do not extract until lag < 10,000.
8. batch_seq = 0
   async for record in connector.extract(last_sync_at):
       batch.append(record)
       if len(batch) == config.batch_size:
           publish batch to m1.int.raw_records with batch_seq
           batch_seq += 1
           batch = []
   if batch:  # publish remaining partial batch
       publish batch to m1.int.raw_records with batch_seq
9. Update nexus_system.sync_jobs: status=completed, records_extracted=total
10. commit Kafka offset for m1.int.sync_requested
11. await connector.disconnect()
```

**Kafka message published to `m1.int.raw_records`:**
```json
{
  "connector_id": "uuid",
  "tenant_id": "acme-corp",
  "entity": "res.partner",
  "batch_seq": 0,
  "job_id": "uuid",
  "records": [
    {
      "source_id": "42",
      "source_table": "res.partner",
      "raw_data": { "id": 42, "name": "Acme SA", "email": "..." },
      "extracted_at": "2026-02-25T10:00:00Z"
    }
  ]
}
```

**Deployment spec:**
- One Deployment per `SystemType`. Label: `nexus.io/connector-type: salesforce`
- Consumer group: `m1-connector-workers`
- `replicas: 1` for MVP. Scale by adding replicas — Kafka partition assignment handles distribution.

**Acceptance criteria:**
- Start the worker. Publish a `sync_requested` message manually (using `NexusProducer`)
- Worker connects to PostgreSQL test connector, extracts 100 test rows, publishes them to `m1.int.raw_records` in batches of the configured batch_size
- Crash the worker mid-extraction (SIGKILL). Restart it. Confirm it re-processes from the beginning (offset not committed) — no data loss
- Simulate lag > 50,000 on `m1.int.raw_records` (pause the Delta Writer). Confirm connector worker enters backpressure wait and resumes when lag drops
- Credentials never appear in pod logs — verify with `kubectl logs`

---

### P2-M1-04 — Airflow DAG: `m1_sync_orchestrator`

**Owner:** Backend-M1  
**Depends on:** P0-INFRA-06, P1-CORE-01, P2-M1-03  

**What to build:**  
The Airflow DAG that triggers connector syncs. This DAG does not call connector code directly — it publishes a Kafka message and waits for the result.

```python
# dags/m1_sync_orchestrator.py

with DAG(
    dag_id="m1_sync_orchestrator",
    schedule="@hourly",      # Overridable per connector via Airflow Variables
    catchup=False,
) as dag:

    validate_config = PythonOperator(
        task_id="validate_connector_config",
        python_callable=validate_connector_credentials,  # Checks Secrets Manager only — no connection
    )

    publish_sync_request = PythonOperator(
        task_id="publish_sync_requested",
        python_callable=publish_to_kafka,
        op_kwargs={
            "topic": "m1.int.sync_requested",
            "payload_fn": build_sync_request_payload,   # Reads connector_id from dag_run.conf
        },
    )

    wait_for_delta_ready = KafkaSensor(
        task_id="wait_for_delta_batch_ready",
        topic="m1.int.delta_batch_ready",
        kafka_config_id="nexus_kafka",
        apply_function=filter_by_job_id,               # Only react to this job's message
        timeout=1800,                                  # 30 minutes
        mode="poke",
        poke_interval=30,
    )

    publish_status_to_m4 = PythonOperator(
        task_id="publish_sync_status",
        python_callable=publish_sync_status,           # Publishes to {tid}.m1.sync_completed
    )

    validate_config >> publish_sync_request >> wait_for_delta_ready >> publish_status_to_m4
```

**Acceptance criteria:**
- Trigger DAG manually with `dag_run.conf = {"connector_id": "<uuid>", "tenant_id": "test-tenant"}`
- `validate_connector_config` task succeeds when secret exists in Secrets Manager, fails when it does not
- `publish_sync_requested` publishes exactly one message to `m1.int.sync_requested` — verify in Kafka UI
- `wait_for_delta_batch_ready` times out correctly if no matching message arrives within 30 minutes
- `publish_sync_status` publishes to `test-tenant.m1.sync_completed` after successful completion

---

### P2-M1-05 — Delta Writer Worker

**Owner:** Backend-M1  
**Depends on:** P2-M1-03, P0-INFRA-04, P0-INFRA-07  

**What to build:**  
Long-running worker that subscribes to `m1.int.raw_records` and writes batches to the Delta Lake Raw Zone.

**Processing logic:**
```
1. Consume messages from m1.int.raw_records (consumer group: m1-delta-writers)
2. Accumulate records in memory buffer:
   - Flush condition A: buffer has 5,000 records
   - Flush condition B: 30 seconds have elapsed since last flush
   - Whichever comes first triggers a flush
3. On flush:
   a. Convert records list to PyArrow Table
   b. Add mandatory columns: tenant_id, system_type, extracted_at, batch_id, nexus_ingested_at
   c. Write to Delta Lake at s3a://nexus-raw/{tenant_id}/{system_type}/{entity}/
      - Write mode: MERGE on source_id (upsert — same source_id updates the row)
   d. On successful write: publish to m1.int.delta_batch_ready
   e. Commit Kafka offset for all messages in this batch
4. On write failure:
   - DO NOT commit Kafka offset — messages will be redelivered
   - After 3 consecutive failures: publish to m1.int.delta_write_failed
```

**Delta batch ready message:**
```json
{
  "connector_id": "uuid",
  "tenant_id": "acme-corp",
  "entity": "res.partner",
  "system_type": "odoo",
  "delta_path": "s3a://nexus-raw/acme-corp/odoo/res.partner/",
  "record_count": 1000,
  "batch_id": "uuid",
  "written_at": "2026-02-25T10:05:00Z",
  "job_id": "uuid"
}
```

**Acceptance criteria:**
- Publish 3,000 test `raw_records` messages manually. Verify worker flushes after 5,000 (not before) OR after 30 seconds — whichever comes first
- After flush: `delta_batch_ready` message appears in Kafka — verify in Kafka UI
- Delta table at `s3a://nexus-raw/test-tenant/postgresql/test_table/` exists with `_delta_log/` directory
- Publish the same `source_id` twice. Verify only one row exists in the Delta table (MERGE upsert, not duplicate insert)
- Simulate write failure (make MinIO unavailable). Confirm offset is NOT committed. Restart worker. Confirm messages are reprocessed

---

### P2-M1-06 — Airflow DAG: `m1_spark_processor`

**Owner:** Backend-M1  
**Depends on:** P0-INFRA-06, P0-INFRA-07, P2-M1-05  

**What to build:**  
The reactive Airflow DAG that watches `m1.int.delta_batch_ready` and submits the Spark classification job. This is the key decoupling point between storage and processing.

```python
# dags/m1_spark_processor.py

with DAG(
    dag_id="m1_spark_processor",
    schedule=None,    # Event-driven only — never scheduled
    catchup=False,
) as dag:

    sense_delta_ready = KafkaSensor(
        task_id="sense_delta_batch_ready",
        topic="m1.int.delta_batch_ready",
        kafka_config_id="nexus_kafka",
        mode="poke",
        poke_interval=30,
        do_xcom_push=True,   # Push message payload to XCom for next task
    )

    submit_spark_job = SparkSubmitOperator(
        task_id="submit_classify_job",
        application="s3a://nexus-jobs/m1_classify_and_prepare.py",
        # Arguments come from XCom (message payload)
        application_args=["{{ ti.xcom_pull('sense_delta_batch_ready')['batch_id'] }}",
                          "{{ ti.xcom_pull('sense_delta_batch_ready')['delta_path'] }}",
                          "{{ ti.xcom_pull('sense_delta_batch_ready')['tenant_id'] }}",
                          "{{ ti.xcom_pull('sense_delta_batch_ready')['entity'] }}"],
        conf={"spark.executor.memory": "4g", "spark.executor.cores": "2"},
        execution_timeout=timedelta(minutes=45),
    )

    publish_job_submitted = PythonOperator(
        task_id="publish_spark_job_submitted",
        python_callable=publish_spark_submitted,    # Publishes to m1.int.spark_job_submitted
    )

    sense_delta_ready >> submit_spark_job >> publish_job_submitted
```

**Acceptance criteria:**
- Publish a test `delta_batch_ready` message manually. DAG triggers within 60 seconds.
- Spark job is submitted to the cluster — verify in Spark UI
- If Spark job takes > 45 minutes, Airflow kills it and publishes `spark_job_failed`
- `spark_job_submitted` message published with correct job_id and batch_id

---

### P2-M1-07 — Spark Job: `m1_classify_and_prepare`

**Owner:** ML  
**Depends on:** P0-INFRA-07, P2-M1-05  

**What to build:**  
The Spark job that reads a Delta batch, classifies entity types, scores CDM field mappings, and publishes classified records back to Kafka.

**Job arguments:** `batch_id`, `delta_path`, `tenant_id`, `entity`

**Step 1 — Load:**
```python
df = spark.read.format("delta").load(delta_path).filter(col("batch_id") == batch_id)
```

**Step 2 — Clean:**
- Null handling: for each column, if null rate > 80% → drop column. Otherwise fill with type-appropriate default (empty string for text, 0 for numeric).
- Deduplicate on `source_id + extracted_at` — keep latest.

**Step 3 — Classify entity_type:**
Apply these rules in order. First match wins.

```python
ENTITY_CLASSIFICATION_RULES = {
    "party":       ["partner", "account", "customer", "user", "contact", "client", "kunnr"],
    "transaction": ["order", "invoice", "transaction", "sale", "purchase", "facture", "vente"],
    "product":     ["product", "item", "sku", "article", "produit"],
    "employee":    ["employee", "worker", "staff", "personnel", "employe"],
    "incident":    ["incident", "ticket", "case", "issue", "problem", "request"],
}
# Match against: table name, and field names in the record
# If no match: entity_type = "unknown"
```

**Step 4 — Score fields against CDM registry:**  
For each column in the DataFrame, query the CDM mapping registry (load as a broadcast variable at job start — do not query per row). Assign:
- `tier_1_fields`: list of columns with confidence > 95%
- `tier_2_fields`: list of columns with confidence 80–95%
- `tier_3_fields`: list of columns with confidence < 80% or no mapping found

**Step 5 — Enrich:**  
Add columns: `entity_type`, `mapping_tier` (overall tier = worst tier of any field), `classification_confidence`, `record_hash` (MD5 of `source_id + extracted_at`)

**Step 6 — Write:**  
```python
enriched_df.write.format("delta").mode("append").save(
    f"s3a://nexus-classified/{tenant_id}/{system_type}/{entity}_classified/"
)
```

**Step 7 — Publish to Kafka:**  
Collect enriched records and publish in batches of 500 to `m1.int.classified_records`. Use `spark-sql-kafka` connector:
```python
# Each Kafka message contains a batch of records — not one message per record
batch_df.write \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
    .option("topic", "m1.int.classified_records") \
    .save()
```

**Acceptance criteria:**
- Run job against a test Delta batch containing 1,000 rows from `res.partner` table
- All rows classified as `entity_type = "party"` (table name `res.partner` matches `"partner"`)
- Classified Delta table created at `nexus-classified` bucket
- `classified_records` messages appear in Kafka UI
- Run against a table with no matching classification rules — rows get `entity_type = "unknown"`
- Job fails gracefully (non-zero exit code) if Delta path is not found — does not hang

---

### P2-M1-08 — CDM Mapper Worker

**Owner:** Backend-M1  
**Depends on:** P2-M1-07, P1-CORE-03  

**What to build:**  
Worker subscribing to `m1.int.classified_records`. Applies CDM mappings and constructs CDMEntity objects.

**Processing logic:**
```
1. Consume message from m1.int.classified_records
2. For each record in the batch:
   a. Skip if entity_type == "unknown": publish to m1.int.mapping_failed with reason="unclassified_entity"
   b. For each field:
      - mapping = registry.get_mapping(tenant_id, source_system, source_table, field)
      - Tier 1 (confidence > 0.95): apply mapping directly
      - Tier 2 (0.80–0.95): apply mapping, add to needs_review list
      - None / Tier 3: skip field, log it
   c. Construct CDMEntity from mapped fields
   d. Add unmapped source fields as `source_extras` dict (do not discard them)
3. Publish batch of CDMEntity objects to m1.int.cdm_entities_ready
4. For each Tier 2 mapping in the batch: publish one m1.int.mapping_failed with reason="tier_2_review_needed"
   (This triggers M4 governance queue — human reviews and promotes to Tier 1)
5. Commit offset
```

**CDMEntity structure (MVP — Party example):**
```python
@dataclass
class CDMParty:
    cdm_id: str              # "party_{uuid}" — generated here if new, looked up if existing source_id
    tenant_id: str
    source_system: str
    source_id: str
    party_type: str          # customer | vendor | contact | employee | prospect
    name: str
    email: str | None
    phone: str | None
    country: str | None
    city: str | None
    source_created_at: str | None
    source_updated_at: str | None
    cdm_created_at: str
    cdm_updated_at: str
    cdm_version: str
    source_extras: dict      # All unmapped fields preserved here
    mapping_tier: int        # 1, 2, or 3 (worst tier of any applied mapping)
```

**Cache invalidation:** The worker subscribes to `{tid}.m4.mapping_approved` in a background thread. On receipt: call `registry.invalidate_cache(tenant_id)`.

**Acceptance criteria:**
- Process a batch of 100 classified Party records from Salesforce
- All Tier 1 fields mapped correctly (e.g. `Account.BillingCountry` → `party.country`)
- Tier 2 records published to `mapping_failed` with correct reason
- `cdm_entities_ready` message published with correct CDMEntity structure
- Source extras preserved: verify a non-CDM field from source appears in `source_extras`
- Run against an "unknown" entity_type batch — all records go to `mapping_failed`, none to `cdm_entities_ready`

---

### P2-M1-09 — AI Store Router

**Owner:** Backend-M1  
**Depends on:** P2-M1-08, P1-CORE-03  

**What to build:**  
Worker subscribing to `m1.int.cdm_entities_ready`. Applies CDM semantic classification rules to determine which knowledge paradigm(s) each entity type should be written to. Publishes routing decisions and fans out to writer workers.

**Routing rules table (read from CDM registry — not hardcoded):**

| entity_type | Vector | Graph | Time-Series |
|---|---|---|---|
| party | ✓ | ✓ | — |
| transaction | — | ✓ | ✓ |
| product | ✓ | — | — |
| employee | ✓ | ✓ | — |
| incident | ✓ | — | ✓ |

**Processing:**
```
1. Consume from m1.int.cdm_entities_ready
2. For each entity in batch:
   - Look up routing rules for entity_type
   - Check for override: has M4 published a routing_override for this entity_id?
     (Store overrides in Redis with TTL 24h)
   - Publish routing decision to m1.int.ai_routing_decided
3. Commit offset
```

The AI Store Router does NOT write to any AI store itself. It only publishes decisions. The three AI Store Writer pods consume `m1.int.ai_routing_decided` filtered to their own store type.

**Acceptance criteria:**
- Party entity routed to both Vector and Graph — verify two messages on `m1.int.ai_routing_decided`, one per store
- Transaction entity routed to Graph and Time-Series — same verification
- Unknown entity_type: publish to `m1.int.mapping_failed`, do not publish to `ai_routing_decided`
- Apply an override via Redis manually. Confirm router uses override instead of default rule.

---

### P2-M1-10 — Salesforce, Odoo & ServiceNow Connectors

**Owner:** Backend-M1  
**Depends on:** P2-M1-01  

**What to build:**  
Three additional connector implementations following the same `BaseConnector` interface established in P2-M1-01.

**Salesforce:**
- Library: `simple_salesforce`
- Auth: OAuth2 username-password flow. Credentials from Secrets Manager: `username`, `password`, `security_token`, `domain`
- Incremental key: `LastModifiedDate`
- Entities (MVP): `Account`, `Contact`, `Opportunity`, `Case`
- Rate limiting: parse `X-Salesforce-Limit-Info` response header. If `Api-Usage > 80%` of daily limit: stop extraction and publish `sync_failed` with `error_code: RATE_LIMIT_WARNING`

**Odoo:**
- Protocol: `xmlrpc.client` wrapped in `asyncio.get_event_loop().run_in_executor()` (XML-RPC is synchronous)
- Auth: database + username + api_key. Call `/web/dataset/call_kw` for data, `/xmlrpc/2/common` for auth
- Incremental key: `write_date`
- Entities (MVP): `res.partner`, `sale.order`, `account.move`, `product.product`
- Multi-company: if Odoo instance has multiple companies, filter by `company_id` from config

**ServiceNow:**
- Protocol: aiohttp REST against Table API (`/api/now/table/{table_name}`)
- Auth: Basic Auth or OAuth2 (support both via config flag)
- Incremental key: `sys_updated_on`
- Entities (MVP): `incident`, `sys_user`
- Pagination: use `sysparm_offset` + `sysparm_limit`. Always set `sysparm_display_value=false` to get raw IDs not display values

**Acceptance criteria for each connector:**
- `connect()` returns `ConnectionResult(success=True)` with correct `system_version`
- `get_schema()` returns at least the MVP entity tables with field names and types
- `extract()` yields at least 10 records from a test/sandbox account with correct field names
- Incremental extraction: extract all records, note timestamp. Insert/update one record. Extract again with `last_sync_at=<timestamp>`. Only the new/updated record is returned.

---

## Phase 3 — M1 Structural Sub-Cycle
**Duration:** Week 6–9 · **Depends on:** Phase 2 (connectors), Phase 4 (M2 Structural Agent)

The structural cycle depends on M2 being partially built. Build tasks P3-M1-01 through P3-M1-03 in parallel with Phase 4. P3-M1-04 requires Phase 4 to be complete.

---

### P3-M1-01 — Schema Profiler

**Owner:** Backend-M1 + ML  
**Depends on:** P2-M1-10  

**What to build:**  
A service that uses `BaseConnector.get_schema()` to produce a full Source Knowledge Artifact. This runs in the Structural sub-cycle only — never in the Operational pipeline.

```python
@dataclass
class FieldProfile:
    name: str
    type: str
    nullable: bool
    is_pk: bool
    null_pct: float           # Percentage of null values in sample
    cardinality: int          # Approximate distinct value count
    sample_values: list       # Up to 5 non-null, non-PII values

@dataclass
class TableProfile:
    table_name: str
    row_count: int            # Approximate — from pg_class or API count
    field_count: int
    primary_key: str | None
    fields: list[FieldProfile]
    sample_rows: list[dict]   # Up to 3 rows — PII fields blanked

@dataclass
class SourceKnowledgeArtifact:
    connector_id: str
    tenant_id: str
    system_type: str
    profiled_at: str
    cdm_version_current: str  # The CDM version active at time of profiling
    tables: list[TableProfile]
    total_tables: int
    total_rows: int           # Sum of all table row counts
```

**PII fields to blank in samples:** any field whose name contains `email`, `phone`, `password`, `ssn`, `tax`, `national_id`, `birth`, `salary`, `wage`, `account_number`.

**Acceptance criteria:**
- Profile `nexus_system` schema (5 tables) — `SourceKnowledgeArtifact` returned in < 10 seconds
- `null_pct` is correct: create a test table with 100 rows where 40% have null in one column. Verify `null_pct = 0.40` for that field.
- Sample rows never contain values for PII-named fields
- `profiled_at` reflects actual execution time, not a hardcoded timestamp

---

### P3-M1-02 — Airflow DAG: `m1_structural_cycle`

**Owner:** Backend-M1  
**Depends on:** P3-M1-01, P0-INFRA-06  

**What to build:**  
The Airflow DAG for the Structural sub-cycle. Triggered weekly or on `m1.int.schema_drift_detected`.

```python
with DAG(
    dag_id="m1_structural_cycle",
    schedule="@weekly",
    catchup=False,
) as dag:

    trigger_check = KafkaSensor(
        task_id="check_for_drift_signal",
        topic="m1.int.structural_cycle_triggered",
        kafka_config_id="nexus_kafka",
        mode="reschedule",
        poke_interval=300,   # Check every 5 minutes
        timeout=None,        # Never time out — wait for weekly schedule or event
    )

    profile_schema = PythonOperator(
        task_id="profile_source_schema",
        python_callable=run_schema_profiler,   # Uses SchemaProfiler to produce SourceKnowledgeArtifact
    )

    publish_for_interpretation = PythonOperator(
        task_id="publish_to_m2",
        python_callable=publish_interpretation_request,
        # Publishes to {tid}.m1.semantic_interpretation_requested
    )

    wait_for_m2_response = KafkaSensor(
        task_id="wait_for_m2_interpretation",
        topic="{tid}.m2.semantic_interpretation_complete",  # Resolved dynamically
        kafka_config_id="nexus_kafka",
        timeout=86400,       # 24 hours — M2 may queue if busy
        mode="reschedule",
    )

    package_cdm_proposal = PythonOperator(
        task_id="package_cdm_proposal",
        python_callable=package_and_publish_cdm_proposal,
        # Publishes to nexus.cdm.extension_proposed
    )

    trigger_check >> profile_schema >> publish_for_interpretation >> wait_for_m2_response >> package_cdm_proposal
```

**Acceptance criteria:**
- Publish a test `structural_cycle_triggered` message. DAG fires within 5 minutes.
- `profile_source_schema` produces a `SourceKnowledgeArtifact` and stores it in PostgreSQL (`nexus_system.schema_snapshots` — create this table)
- `publish_to_m2` publishes correct payload to `{tid}.m1.semantic_interpretation_requested`
- `wait_for_m2_interpretation` waits and unblocks when M2 publishes its response (simulate M2 response with manual Kafka publish for now)
- `package_cdm_proposal` publishes to `nexus.cdm.extension_proposed`

---

### P3-M1-03 — Schema Drift Detector

**Owner:** Backend-M1  
**Depends on:** P3-M1-01  

**What to build:**  
A lightweight background process that compares the current schema profile against the previous stored snapshot. Runs after every successful Operational pipeline sync completes.

**Detection logic:**
- Load previous `SourceKnowledgeArtifact` from `nexus_system.schema_snapshots` for this `connector_id`
- Load current schema via `connector.get_schema()`
- Compare: new tables, removed tables, new fields, changed field types, significantly changed null rates (> 20% change)
- If any change detected: publish `m1.int.schema_drift_detected` with diff summary
- Store new snapshot as the current version

**Acceptance criteria:**
- Add a new column to a test table. Run drift detector. `schema_drift_detected` message published within one detection cycle.
- Remove a column from a test table. Same.
- No changes made. Run detector. No message published.
- `schema_drift_detected` message payload contains the exact list of changed fields

---

## Phase 4 — M2 Intelligence Hub
**Duration:** Week 5–10 · **Depends on:** Phase 1, P3-M1-01

---

### P4-M2-01 — Structural Agent

**Owner:** Backend-M2  
**Depends on:** P1-CORE-01, P1-CORE-02, P3-M1-01 (SourceKnowledgeArtifact schema)  

**What to build:**  
The LLM-powered agent that consumes `{tid}.m1.semantic_interpretation_requested` and produces CDM extension proposals.

The agent receives a `SourceKnowledgeArtifact` and the current CDM state. It performs:

1. **Entity classification** — For each table in the artifact, assign the most likely CDM entity type. Provide a confidence score.
2. **Field alignment** — For each field in each table, suggest the most likely CDM field mapping. Provide confidence score.
3. **Cross-system alignment** — If this is not the first source system for this tenant, look for overlapping entity types and suggest identity alignment keys.
4. **Relationship discovery** — From foreign key relationships in the schema, suggest CDM relationship definitions.

**LLM prompt structure (system prompt excerpt):**
```
You are a semantic data architect. Your job is to analyse enterprise data schemas
and propose mappings to a Canonical Data Model.

STRICT RULES:
- You only propose mappings. You never auto-apply them.
- Every proposal includes a confidence score between 0.0 and 1.0.
- Confidence > 0.95: the mapping is nearly certain based on field name and type.
- Confidence 0.80–0.95: plausible but requires human review.
- Confidence < 0.80: uncertain — flag for manual classification.
- Output ONLY valid JSON matching the ProposedInterpretation schema. No prose.
```

**Output structure:**
```python
@dataclass
class FieldMappingProposal:
    source_table: str
    source_field: str
    cdm_entity: str
    cdm_field: str
    confidence: float
    reasoning: str      # One sentence explaining the mapping

@dataclass
class ProposedInterpretation:
    connector_id: str
    tenant_id: str
    cdm_version_base: str
    entity_proposals: list[dict]   # table → cdm_entity mappings
    field_proposals: list[FieldMappingProposal]
    relationship_proposals: list[dict]
    identity_alignment_proposals: list[dict]
```

Publish result to `{tid}.m2.semantic_interpretation_complete`.

**Acceptance criteria:**
- Send a `SourceKnowledgeArtifact` for `res.partner` table (Odoo). Agent proposes `res.partner` → `party` entity mapping.
- Agent proposes `name` → `party.name`, `email` → `party.email` with confidence > 0.95
- Agent proposes `write_date` is NOT a CDM field (it is a system field) — confidence for CDM mapping < 0.50
- Output is always valid JSON matching `ProposedInterpretation` schema — test with 10 different schemas
- Agent never auto-applies mappings — output is a proposal only, no side effects

---

### P4-M2-02 — Executive Agent (RHMA Core)

**Owner:** Backend-M2  
**Depends on:** P1-CORE-01, P1-CORE-02, Phase 5 (M3 must be partially complete)  

**What to build:**  
The user-facing reasoning engine. Implement with LangGraph for agent orchestration. Three agent layers: Reasoning (Intent Classifier + Task Decomposer), Hierarchical (Supervisor), Multi-Agent (Finance, HR, Operations workers).

**Key constraints:**
- Agents query M3 via `{tid}.m2.knowledge_query` events — never via direct REST to M3 internals
- Agents never access raw source data — only canonicalized data in M3
- Every response includes `sources[]` (which CDM entities were consulted) and `reasoning_trace` (what steps the agent took)
- All agent outputs pass through the Safety layer (Policy Agent + OPA check) before being published to M6

**Acceptance criteria:**
- Query: "Show me all customers from France" → Agent queries M3 Vector store with correct filter, returns Party entities with `country = France`
- Query: "What are the open invoices for Acme SA?" → Agent queries M3 Graph store, traverses Party → Transaction chain, returns filtered Transaction entities
- Every response includes `sources[]` list — verify at least one CDM entity reference
- Safety layer blocks a query that would return entities outside the authenticated user's tenant — verify with a cross-tenant test query

---

## Phase 5 — M3 Knowledge Specialization
**Duration:** Week 7–10 · **Depends on:** P2-M1-09  

---

### P5-M3-01 — Vector Store Writer (Pinecone)

**Owner:** Backend-M2 or ML  
**Depends on:** P2-M1-09  

**What to build:**  
Kubernetes pod subscribing to `m1.int.ai_routing_decided` filtered to `routing_decision.stores` containing `"vector"`.

**Processing:**
```
1. Receive CDMEntity from routing decision
2. Build text representation for embedding:
   - Party: "{name} {party_type} {country} {city}"
   - Product: "{name} {category} {description}"
   - Employee: "{full_name} {job_title} {department}"
   - Incident: "{title} {description} {priority}"
3. Generate embedding using sentence-transformers (all-MiniLM-L6-v2, 384 dimensions)
   DO NOT call OpenAI API for embeddings in MVP — use local model to avoid per-token cost
4. Upsert to Pinecone index named "{tenant_id}-{entity_type}" (auto-create if missing)
   - vector_id: cdm_id
   - values: embedding (384 dims)
   - metadata: { tenant_id, entity_type, source_system, name, cdm_version }
5. Publish m1.int.ai_write_completed with store="vector"
```

**Acceptance criteria:**
- Upsert 100 Party entities from Salesforce. Query Pinecone for "French customer manufacturing". Top result should be a Party with `country = France` and relevant name.
- Upsert same `cdm_id` twice with different name. Verify only one vector exists (upsert, not insert)
- Index `test-tenant-party` auto-created if it does not exist
- `ai_write_completed` published after successful upsert batch

---

### P5-M3-02 — Graph Store Writer (Neo4j)

**Owner:** Backend-M2  
**Depends on:** P2-M1-09  

**What to build:**  
Pod subscribing to `m1.int.ai_routing_decided` filtered to `"graph"`. Writes nodes and relationships to Neo4j.

**Node write contract:**
```cypher
// All node writes use MERGE — never CREATE
MERGE (p:Party {cdm_id: $cdm_id, tenant_id: $tenant_id})
ON CREATE SET p += $properties, p.created_at = datetime()
ON MATCH  SET p += $properties, p.updated_at = datetime()
```

**Relationship write contract:**
```cypher
// Only create relationship if BOTH endpoints already exist
MATCH (p:Party {cdm_id: $party_id, tenant_id: $tenant_id})
MATCH (t:Transaction {cdm_id: $transaction_id, tenant_id: $tenant_id})
MERGE (p)-[r:HAS_TRANSACTION]->(t)
ON CREATE SET r.since = datetime()
```

If either endpoint does not exist yet: publish `m1.int.mapping_failed` with `reason="relationship_endpoint_missing"`. The relationship will be created when the missing entity arrives.

**Acceptance criteria:**
- Write 50 Party nodes. Query Neo4j: `MATCH (p:Party {tenant_id: "test"}) RETURN count(p)` → 50
- Write same Party node twice (same `cdm_id`). Query: should still return 50, not 51.
- Write a Transaction linked to a Party that doesn't exist yet → `mapping_failed` published
- Write the missing Party, then the Transaction again → relationship created correctly

---

### P5-M3-03 — Time-Series Store Writer (TimescaleDB)

**Owner:** Backend-M2  
**Depends on:** P2-M1-09  

**What to build:**  
Pod subscribing to `m1.int.ai_routing_decided` filtered to `"timeseries"`. Writes time-series records to TimescaleDB.

**Schema (create on first write, not on startup):**
```sql
CREATE TABLE IF NOT EXISTS nexus_timeseries (
    time           TIMESTAMPTZ NOT NULL,
    tenant_id      VARCHAR(100) NOT NULL,
    entity_type    VARCHAR(50) NOT NULL,
    entity_id      VARCHAR(200) NOT NULL,  -- cdm_id
    metric_name    VARCHAR(200) NOT NULL,
    metric_value   DOUBLE PRECISION,
    metadata       JSONB,
    cdm_version    VARCHAR(20)
);

SELECT create_hypertable('nexus_timeseries', 'time', if_not_exists => TRUE);
```

**Insert contract:**
```sql
INSERT INTO nexus_timeseries (time, tenant_id, entity_type, entity_id, metric_name, metric_value, metadata)
VALUES ($time, $tenant_id, $entity_type, $cdm_id, $metric_name, $metric_value, $metadata)
ON CONFLICT DO NOTHING;  -- Idempotent
```

For Transaction entities: write `metric_name = "amount"`, `metric_value = transaction.amount`, `time = transaction.date`
For Incident entities: write `metric_name = "incident_open"`, `metric_value = 1`, `time = incident.created_at`

**Acceptance criteria:**
- Write 1,000 Transaction records. Query: `SELECT count(*) FROM nexus_timeseries WHERE entity_type = 'transaction'` → 1000
- Insert same record twice (same entity_id + time + metric_name). Count still 1000 (ON CONFLICT DO NOTHING works)
- Hypertable confirmed: `SELECT * FROM timescaledb_information.hypertables WHERE hypertable_name = 'nexus_timeseries'` returns one row

---

## Phase 6 — M4 Workflow & Governance
**Duration:** Week 8–12 · **Depends on:** Phase 1, Phase 2 (partially)  

---

### P6-M4-01 — CDM Governance Queue

**Owner:** Backend-M4  
**Depends on:** P1-CORE-01, P0-INFRA-03  

**What to build:**  
A service that consumes `nexus.cdm.extension_proposed` and stores proposals in a PostgreSQL governance queue table. Proposals wait here until a data steward approves or rejects them via M6.

```sql
CREATE TABLE nexus_system.governance_queue (
    proposal_id      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    proposal_type    VARCHAR(50) NOT NULL,  -- 'cdm_extension' | 'field_mapping' | 'entity_classification'
    tenant_id        VARCHAR(100) NOT NULL,
    status           VARCHAR(20) DEFAULT 'pending',  -- pending | approved | rejected
    payload          JSONB NOT NULL,   -- The full ProposedInterpretation from M2
    submitted_at     TIMESTAMPTZ DEFAULT NOW(),
    reviewed_at      TIMESTAMPTZ,
    reviewed_by      VARCHAR(200),
    review_notes     TEXT
);
```

On approval:
- Insert new row into `nexus_system.cdm_versions` with status `active`, set previous version to `deprecated`
- Insert all approved field mappings into `nexus_system.cdm_mappings` with `tier = 1`
- Publish `nexus.cdm.version_published`

On rejection:
- Update `governance_queue.status = 'rejected'`
- Publish `nexus.cdm.extension_rejected`

**Acceptance criteria:**
- Publish a test `nexus.cdm.extension_proposed` message. Row appears in `governance_queue` table within 10 seconds.
- Approve via direct PostgreSQL UPDATE (M6 not built yet). Verify `nexus.cdm.version_published` published.
- Reject. Verify `nexus.cdm.extension_rejected` published.
- Approve a proposal with 5 field mappings. Verify all 5 rows appear in `cdm_mappings` table with `tier = 1`.

---

### P6-M4-02 — Mapping Review Queue

**Owner:** Backend-M4  
**Depends on:** P6-M4-01  

**What to build:**  
Consumer for `{tid}.m1.mapping_review_needed`. Stores Tier 2 mapping proposals in a separate review queue. On human approval, promotes mapping to Tier 1 and publishes `{tid}.m4.mapping_approved`.

```sql
CREATE TABLE nexus_system.mapping_review_queue (
    review_id        UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id        VARCHAR(100) NOT NULL,
    source_system    VARCHAR(100) NOT NULL,
    source_table     VARCHAR(200) NOT NULL,
    source_field     VARCHAR(200) NOT NULL,
    cdm_entity       VARCHAR(100),
    cdm_field        VARCHAR(100),
    confidence       DECIMAL(5,2),
    status           VARCHAR(20) DEFAULT 'pending',
    submitted_at     TIMESTAMPTZ DEFAULT NOW(),
    reviewed_by      VARCHAR(200),
    reviewed_at      TIMESTAMPTZ
);
```

On approval: UPDATE `cdm_mappings` SET `tier = 1`, `approved_by`, `approved_at`. Publish `{tid}.m4.mapping_approved`.

**Acceptance criteria:**
- Publish 5 `mapping_review_needed` messages. Five rows appear in `mapping_review_queue`.
- Approve one. Verify `cdm_mappings` row updated to `tier = 1`. Verify `mapping_approved` published.
- CDM Mapper Worker cache invalidated: send a new `classified_records` message. Worker re-queries PostgreSQL instead of using cached mapping.

---

### P6-M4-03 — Temporal Workflow Engine

**Owner:** Backend-M4  
**Depends on:** P1-CORE-01  

**What to build:**  
Deploy Temporal server. Implement the first Temporal workflow: `OnboardingWorkflow` (triggered by `{tid}.m2.workflow_trigger` where `workflow_type = "employee_onboarding"`).

The workflow must:
1. Create IT account (stub activity — returns success after 2s delay)
2. Create HR record (stub activity)
3. Assign equipment (stub activity)
4. Send welcome email (stub activity using SendGrid)
5. On all steps complete: publish `{tid}.m4.workflow_completed`

Each activity must be individually retryable. If step 2 fails, steps 1 and 3–4 are not re-run.

**Acceptance criteria:**
- Publish a `workflow_trigger` message. Workflow starts within 10 seconds.
- Simulate activity 2 failing. Temporal retries activity 2 independently. Activities 1, 3, 4 run exactly once.
- `workflow_completed` published with `result = "success"` after all steps complete
- Workflow state visible in Temporal UI at `temporal.nexus.internal`

---

## Phase 7 — M6 User Interface
**Duration:** Week 10–14 · **Depends on:** Phase 4, Phase 6  

---

### P7-M6-01 — AI Chat Interface

**Owner:** Full-stack  
**Depends on:** P4-M2-02  

**What to build:**  
Next.js 14 chat interface. User types a query. Query sent to M2 via REST (`POST /api/v1/query`). M2 RHMA processes it asynchronously. Response delivered via WebSocket subscribed to `{tid}.m2.agent_response_ready`.

The UI must display:
- Response text
- Sources consulted (collapsible — list of CDMEntity types and IDs)
- Reasoning trace (collapsible — step-by-step what the agent did)

**Acceptance criteria:**
- Type "Show me open incidents from last week" and submit
- Response appears within 15 seconds
- Sources panel shows at least one CDMIncident entity reference
- Reasoning trace shows at least two steps (query M3, filter by date)

---

### P7-M6-02 — CDM Governance Console

**Owner:** Full-stack  
**Depends on:** P6-M4-01, P6-M4-02  

**What to build:**  
Data steward interface for reviewing CDM proposals and field mapping approvals.

Two pages:
1. **CDM Proposals** — Lists pending rows from `governance_queue`. Each row shows: proposed entity, proposed fields, M2 confidence scores. Buttons: Approve / Reject / Request revision.
2. **Mapping Review** — Lists pending rows from `mapping_review_queue`. Each row shows: source field, suggested CDM field, confidence score. Buttons: Approve / Reject.

Approval and rejection actions call M4 REST endpoints — not PostgreSQL directly.

**Acceptance criteria:**
- Load governance console with 3 pending proposals
- Approve one — row disappears from list, `nexus.cdm.version_published` appears in Kafka UI
- Reject one — row disappears from list, `nexus.cdm.extension_rejected` appears in Kafka UI
- Approve a mapping — row disappears, `mapping_approved` appears in Kafka UI

---

## Phase 8 — Integration & End-to-End Testing
**Duration:** Week 13–16 · **Depends on:** All previous phases  

---

### P8-E2E-01 — Full Operational Pipeline Test (Salesforce)

**Owner:** Lead + All  
**Depends on:** All Phase 2, Phase 4, Phase 5, Phase 6 tasks  

**What to build:** A complete end-to-end test run — not a unit test, a real pipeline execution with a live Salesforce sandbox.

**Test scenario:**
1. Register a Salesforce connector for tenant `test-acme-corp`
2. Store credentials in Secrets Manager
3. Trigger `m1_sync_orchestrator` DAG with this connector
4. Wait for pipeline to complete (max 20 minutes)
5. Verify results at every stage

**Verification checklist:**

```
☐ Kafka: m1.int.sync_requested — message present
☐ Kafka: m1.int.raw_records — messages present, batch_seq sequential
☐ MinIO nexus-raw: files written at /test-acme-corp/salesforce/Account/
☐ Kafka: m1.int.delta_batch_ready — message present
☐ Spark job: completed successfully in Spark UI
☐ MinIO nexus-classified: files at /test-acme-corp/salesforce/Account_classified/
☐ Kafka: m1.int.classified_records — entity_type = "party" for all Account records
☐ Kafka: m1.int.cdm_entities_ready — CDMParty objects with correct fields
☐ Kafka: m1.int.ai_routing_decided — Party routed to vector + graph
☐ Pinecone: index test-acme-corp-party exists with records
☐ Neo4j: MATCH (p:Party {tenant_id: "test-acme-corp"}) RETURN count(p) > 0
☐ Kafka: m1.int.ai_write_completed — two messages (vector, graph)
☐ Kafka: test-acme-corp.m1.sync_completed — message present
☐ Jaeger: single trace spans full pipeline from sync_requested to ai_write_completed
☐ Grafana: NEXUS Pipeline Health dashboard shows non-zero records processed
```

**Acceptance criteria:** All 14 checkboxes pass in a single pipeline run.

---

### P8-E2E-02 — Multi-Tenant Isolation Test

**Owner:** Lead + DevOps  
**Depends on:** P8-E2E-01  

**Test scenario:**
- Register two tenants: `tenant-alpha` and `tenant-beta`
- Run simultaneous syncs for both tenants
- Verify complete isolation at every layer

**Verification checklist:**
```
☐ Kafka: tenant-alpha messages contain only tenant_id = "tenant-alpha"
☐ Kafka: tenant-beta messages contain only tenant_id = "tenant-beta"
☐ MinIO: /tenant-alpha/* and /tenant-beta/* paths — no cross-contamination
☐ Pinecone: tenant-alpha-party and tenant-beta-party are separate indexes
☐ Neo4j: Party nodes for each tenant have correct tenant_id — no cross-tenant edges
☐ M2 query: querying as tenant-alpha never returns entities with tenant_id = "tenant-beta"
☐ Kong: JWT with tenant-alpha claim cannot access tenant-beta.m1.* endpoints
```

**Acceptance criteria:** All 7 checkboxes pass.

---

### P8-E2E-03 — Structural Cycle Test

**Owner:** Lead + Backend-M1 + Backend-M2  
**Depends on:** Phase 3, Phase 4  

**Test scenario:**
1. Connect a new Odoo instance that has not been seen before
2. Trigger `m1_structural_cycle` DAG
3. Verify M2 produces interpretation
4. Approve CDM extension via governance console
5. Run an Operational sync of the same Odoo instance
6. Verify Operational pipeline uses the new CDM mappings

**Verification checklist:**
```
☐ Kafka: m1.int.source_schema_extracted — SourceKnowledgeArtifact for Odoo
☐ Kafka: test-tenant.m1.semantic_interpretation_requested — sent to M2
☐ Kafka: test-tenant.m2.semantic_interpretation_complete — ProposedInterpretation received
☐ Kafka: nexus.cdm.extension_proposed — proposal stored in governance_queue
☐ Governance console: proposal visible with correct entity and field suggestions
☐ After approval: nexus_system.cdm_mappings has rows for res.partner fields
☐ Kafka: nexus.cdm.version_published — all modules receive new CDM version
☐ Operational sync: CDM Mapper Worker uses new mappings (check logs for "cache invalidated")
☐ Pinecone: Odoo partner records indexed correctly using new field mappings
```

**Acceptance criteria:** All 9 checkboxes pass.

---

## Inter-Team Contracts

These contracts define the boundaries between teams. If you are consuming something produced by another team, treat this section as immutable — do not request changes without a formal architectural review with the Lead.

### M1 → M2 Contract

M1 publishes a `SourceKnowledgeArtifact` to `{tid}.m1.semantic_interpretation_requested`. M2 must be able to consume this and return a `ProposedInterpretation` to `{tid}.m2.semantic_interpretation_complete` within 24 hours. The schema for both objects is defined in `nexus_core/schemas.py` and is the single source of truth — neither team may change it without updating the shared library and notifying all consumers.

### M1 → M3 Contract

M1 publishes `CDMEntity` objects to `m1.int.cdm_entities_ready`. M3 writers consume `m1.int.ai_routing_decided` (produced by M1's AI Store Router). M3 never reads directly from the Delta Lake — only from Kafka messages. The `CDMEntity` dataclasses defined in `nexus_core/entities.py` are the contract — M3 writes exactly what M1 sends, no field transformations.

### M2 → M4 Contract

M2 publishes `{tid}.m2.workflow_trigger` with a `workflow_type` string and a `context` dict. M4 must implement a Temporal workflow for every `workflow_type` value currently defined. New `workflow_type` values require M4 to implement the corresponding workflow before M2 may publish them.

### M4 → M1 Contract

M4 publishes `{tid}.m4.mapping_approved` and `{tid}.m4.routing_override`. M1 subscribes to both. M4 must publish `mapping_approved` within 5 seconds of a human approval action in M6 — this is latency-sensitive because M1's CDM Mapper Worker holds the mapping cache invalidation on this event.

### CDM Version Contract

When `nexus.cdm.version_published` is received by any module, that module must complete its current batch using the old CDM version and switch to the new version on the next batch. No module may use two CDM versions simultaneously within a single batch. The `cdm_version` field in every Kafka message payload identifies which version governed its production.

---

## Definition of Done Checklist

Before any task is marked done, verify all of the following:

```
Code quality
☐ Unit tests written and passing (minimum 80% coverage on business logic)
☐ No print() statements — structured logging only (use Python logging module)
☐ No hardcoded credentials, connection strings, or tenant IDs
☐ All topic names constructed via CrossModuleTopicNamer — no string concatenation

Kafka behaviour
☐ Offset committed only after successful processing — verified by crash-and-restart test
☐ Message envelope uses NexusMessage — no raw dict publishes
☐ correlation_id propagated from incoming message to all outgoing messages

Security
☐ Credentials never logged — grep pod logs for "password", "api_key", "token"
☐ TenantContext set at message receipt and readable by all downstream calls
☐ No cross-tenant data in any query result

Observability
☐ OpenTelemetry span created for the task's main operation
☐ trace_id present in all Kafka messages produced
☐ Prometheus counter or histogram incremented on success and on failure

Acceptance criteria
☐ Every acceptance criterion in the task spec passes
☐ Acceptance criteria run in the shared development environment (not local only)
☐ Lead has reviewed and signed off
```

---

*NEXUS Developer Operational Plan v1.0 — Mentis Consulting | InfiniteMind | Brussels*  
*This document supersedes all previous task lists. Raise conflicts with the Lead immediately.*
