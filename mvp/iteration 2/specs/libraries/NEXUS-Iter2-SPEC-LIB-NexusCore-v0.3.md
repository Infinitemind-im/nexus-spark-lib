# NEXUS — Iteration 2 · nexus_core Library Update
**Shared Infrastructure Library — Iteration 2 Additions**
Mentis Consulting · Version 0.3 · April 2026 · Confidential

**Service owner:** Platform / Infra workstream
**Hard gate:** Must be published to internal PyPI before end of Week 1. All other developers are blocked until this is done.
**Current version:** 1.x (Iteration 1)
**Target version:** 2.0.0

> **Revision v0.4 — Elasticsearch correction + missing topics**
> `onboard_tenant()` §7 rewritten: Pinecone index creation replaced with Elasticsearch index-template provisioning. `db/elasticsearch.py` module added (§8). `CrossModuleTopicNamer` extended with five missing topics. Consumer group table corrected (stale `nexus.query_*` names fixed). `CDMEntity.source_record_id` comment corrected (Pinecone → Elasticsearch).
>
> **Revision v0.3 — Spark transformation stage**
> `m1_transformed_records()` topic method added to `CrossModuleTopicNamer`. `SparkTransformResult` dataclass added (§9) — the envelope published to `m1.int.transformed_records` by `nexus-spark-transformer` and consumed by `nexus-cdm-mapper`. Overview updated to reference the new transformation layer.
>
> **Revision v0.2 — data flow spec alignment**
> `event_action` field retained for audit but no longer used for M3 write routing. `ConnectorBatchConfig` dataclass added (§8).

---

## Overview

`nexus_core` is the shared Python library imported by every NEXUS service. It enforces system-wide contracts: message envelope structure, tenant context propagation, Kafka topic naming, and database connection patterns. No service hand-crafts topic strings or decodes JWTs — all of this is enforced through `nexus_core`.

Iteration 2 adds three new Kafka topics, a TimescaleDB-aware connection helper, source identity resolution for Rule 6 enforcement, FX rate conversion, and updated CDM entity and message models.

---

## Module Structure

```
nexus_core/
├── topics.py          ← CrossModuleTopicNamer       [UPDATE]
├── models.py          ← NexusMessage, CDMEntity, ConnectorBatchConfig [UPDATE]
├── context.py         ← TenantContext                [unchanged]
├── db/
│   ├── postgres.py    ← RLS-scoped connections, get_tenant_scoped_connection() [UPDATE]
│   ├── timescale.py   ← TimescaleDB helper           [NEW]
│   ├── elasticsearch.py ← ES client + index helpers  [NEW]
│   └── identity.py    ← resolve_source_identity      [NEW]
├── fx.py              ← FXService                    [NEW]
└── provisioning.py    ← onboard_tenant()             [UPDATE]
```

---

## 1. CrossModuleTopicNamer — New Topics

**File:** `nexus_core/topics.py`

Add the following topic entries. No service may hand-craft topic strings — all must go through `CrossModuleTopicNamer`.

```python
class CrossModuleTopicNamer:
    # --- Existing (Iteration 1, unchanged) ---
    # m1.entity_approved, m4.mapping_approved, nexus.cdm.version_published ...

    # --- NEW in Iteration 2 ---

    @staticmethod
    def m1_transformed_records() -> str:
        """
        Published by: nexus-spark-transformer (Spark transformation stage)
        Consumed by:  nexus-cdm-mapper
        Trigger:      Every record after Spark type coercion, FX normalisation,
                      entity resolution, and quality checks. This is the input
                      to the CDM classification stage — not a raw source record.
        Note:         Global topic (no tenant prefix) — Spark processes all
                      tenants' raw_records and emits transformed records with
                      tenant_id in the payload.
        """
        return "m1.int.transformed_records"

    @staticmethod
    def m1_entity_routed(tenant_id: str) -> str:
        """
        Published by: nexus-m1-worker (AI Store Router)
        Consumed by:  nexus-m3-writer (primary continuous write path)
        Trigger:      Every CDM entity that passes the AI Store Router (Tier 1 + Tier 2).
        """
        return f"{tenant_id}.m1.entity_routed"

    @staticmethod
    def m1_entity_removed(tenant_id: str) -> str:
        """
        Published by: nexus-m1-worker
        Consumed by:  nexus-m3-writer (deletion path — all three stores)
        Trigger:      Entity permanently deleted in the source system.
        OQ-M3-DEL-01 resolved: dedicated topic (not event_action flag).
        """
        return f"{tenant_id}.m1.entity_removed"

    @staticmethod
    def query_submitted(tenant_id: str) -> str:
        """
        Published by: nexus-query-api
        Consumed by:  nexus-query-executor
        Trigger:      User submits a natural language query via POST /query.
        """
        return f"{tenant_id}.query.submitted"

    @staticmethod
    def query_event(tenant_id: str) -> str:
        """
        Published by: nexus-query-executor (status events + final RenderedOutput)
        Consumed by:  nexus-query-api (relayed to WebSocket / polling store)
        Events:       planning | decomposing | executing | result | error | timeout
        """
        return f"{tenant_id}.query.event"

    @staticmethod
    def m1_classification_produced(tenant_id: str) -> str:
        """
        Published by: nexus-cdm-mapper
        Consumed by:  nexus-m4-worker (deduplicates into governance queue)
        Trigger:      Every classification result (Tier 1, 2, or 3). Deterministic
                      event_id = sha256(tenant_id|source_system|table|field|cdm_version|classifier_version).
        """
        return f"{tenant_id}.m1.classification_produced"

    @staticmethod
    def m4_validation_decision(tenant_id: str) -> str:
        """
        Published by: nexus-m4-worker (on approve / reject / defer via /decision endpoint)
        Consumed by:  nexus-cdm-mapper (updates cdm_feedback), observability pipeline
        Trigger:      Human operator submits a decision on a CDM proposal.
        """
        return f"{tenant_id}.m4.validation_decision"

    @staticmethod
    def m2_agent_step_completed(tenant_id: str) -> str:
        """
        Published by: nexus-m2-executor
        Consumed by:  observability pipeline, llm_audit_log writer
        Trigger:      Every agent step completion (stub payloads in Iter 2/3;
                      real payloads post-Iter 3 when RHMA loop is active).
        """
        return f"{tenant_id}.m2.agent_step_completed"

    @staticmethod
    def m3_write_completed() -> str:
        """
        Published by: nexus-m3-writer (all three store writers)
        Consumed by:  observability pipeline
        Payload:      store, tenant_id, cdm_entity_id, status (ok|skipped|error|circuit_open)
        Note:         Global topic — no tenant prefix; tenant_id is in the payload.
        """
        return "nexus.m3.write_completed"

    @staticmethod
    def materialization_changed() -> str:
        """
        Published by: nexus-m4-worker (materialization policy engine)
        Consumed by:  nexus-m3-writer (triggers re-routing on tier change)
        Payload:      tenant_id, cdm_entity_id, old_level, new_level, embedding (if promoting to hot)
        Note:         Global topic — no tenant prefix.
        """
        return "nexus.materialization.changed"
```

**Consumer group names** (registered in `NEXUS-Iter2-SPEC-ServiceTopology-v0.4.md`):

| Topic | Consumer | Group name |
|---|---|---|
| `m1.int.transformed_records` | nexus-cdm-mapper | `m1-cdm-mapper` |
| `{tid}.m1.entity_routed` | nexus-m3-writer (primary) | `m3-writer-entities` |
| `{tid}.m1.entity_removed` | nexus-m3-writer (deletion) | `m3-writer-entities` (separate handler) |
| `{tid}.query.submitted` | nexus-query-executor | `query-executor` |
| `{tid}.query.event` | nexus-query-api | `query-api-relay` |
| `{tid}.m1.classification_produced` | nexus-m4-worker | `m4-cdm-governance` |
| `{tid}.m4.validation_decision` | nexus-cdm-mapper | `m1-cdm-mapper-feedback` |
| `{tid}.m2.agent_step_completed` | observability pipeline | `obs-agent-steps` |
| `nexus.m3.write_completed` | observability pipeline | `obs-m3-writes` |
| `nexus.materialization.changed` | nexus-m3-writer | `m3-writer-materialization` |

---

## 2. NexusMessage Envelope — event_action Field

**File:** `nexus_core/models.py`

`event_action` is retained on the `NexusMessage` envelope for backward compatibility and audit purposes, but is **no longer used to route M3 write operations**. Delete operations are now signalled by the dedicated `{tid}.m1.entity_removed` topic — the Op Router publishes to `entity_removed` for delete ops and to `entity_routed` for all upsert ops. Consumers of `entity_routed` must treat every message as an upsert regardless of `event_action` value.

```python
@dataclass
class NexusMessage:
    topic:        str
    tenant_id:    str
    payload:      dict
    message_id:   str = field(default_factory=lambda: str(uuid.uuid4()))
    schema_version: str = "2.0"        # bumped for Iteration 2
    published_at: datetime = field(default_factory=datetime.utcnow)

    # Retained for audit / observability — NOT used for M3 write routing
    event_action: Literal["created", "updated", "deleted", "read"] | None = None
    # "created"  — entity is new in the source system
    # "updated"  — entity was modified in the source system
    # "deleted"  — entity was removed (will arrive on entity_removed topic, not entity_routed)
    # "read"     — Debezium snapshot op (treated as "created" by upsert paths — Rule 7)
    # None       — action unknown (backward-compatible; treat as "created")
```

**Backward compatibility:** `event_action = None` is the default. Iteration 1 messages that omit this field are treated as `"created"` by Iteration 2 consumers. M3 writer consumers must not branch on `event_action` for routing — topic identity (`entity_routed` vs `entity_removed`) is the authoritative signal.

---

## 3. CDMEntity Model — Iteration 2 Additions

**File:** `nexus_core/models.py`

Extend `CDMEntity` with fields required by the M3 writer and query executor.

```python
@dataclass
class CDMEntity:
    # --- Existing fields (Iteration 1, unchanged) ---
    tenant_id:      str
    entity_type:    str          # e.g. "party.customer", "employee"
    cdm_entity_id:  str          # Stable CDM identifier
    connector_id:   str
    source_system:  str          # "salesforce" | "adventureworks"
    fields:         dict         # Source field values (in-memory only for Iteration 2)
    cdm_version:    str
    approved_at:    datetime

    # --- NEW in Iteration 2 ---
    source_record_id: str | None = None
    # PK of this entity in the live source system (e.g. Salesforce Id, AdventureWorks SalesOrderID).
    # Used by nexus-query-executor in phase-2 fetch_by_ids() call.
    # Also stored in the Elasticsearch document and Neo4j node as source_ref.

    event_action: Literal["created", "updated", "deleted"] | None = None
    # Forwarded from NexusMessage.event_action. Consumers use this to choose
    # between upsert, re-embed, and delete paths.

    event_type: str | None = None
    # CDM event classification (e.g. "salesforce.opportunity.won").
    # Used by TimescaleDB writer to look up EVENT_TO_METRIC_MAP.
```

---

## 4. TimescaleDB Connection Helper

**File:** `nexus_core/db/timescale.py` ← NEW

All TimescaleDB operations require the RLS policy `nexus.current_tenant_id` to be set on the connection before any query. This helper enforces that — developers must not use raw asyncpg connections for TimescaleDB.

```python
from contextlib import asynccontextmanager
import asyncpg

@asynccontextmanager
async def get_timescale_connection(pool: asyncpg.Pool, tenant_id: str):
    """
    Yields a connection with RLS tenant context set.
    Sets nexus.current_tenant_id before yielding; releases after.

    Usage:
        async with get_timescale_connection(pool, tenant_id) as conn:
            await conn.execute("INSERT INTO nexus_ts.business_metrics_raw ...")
    """
    async with pool.acquire() as conn:
        await conn.execute(
            "SET LOCAL nexus.current_tenant_id = $1", tenant_id
        )
        yield conn
        # Connection returns to pool automatically; SET LOCAL is scoped to the transaction
```

**Pool initialisation** (called once at service startup):

```python
async def create_timescale_pool(dsn: str, min_size: int = 5, max_size: int = 20) -> asyncpg.Pool:
    return await asyncpg.create_pool(dsn, min_size=min_size, max_size=max_size)
```

---

## 5. Source Identity Resolution

**File:** `nexus_core/db/identity.py` ← NEW

Implements Rule 6: user identity forwarded to source systems unchanged. The query executor resolves Okta `user_id` to the source-system's native identity before dispatching sub-queries.

```python
async def resolve_source_identity(
    pg_conn:      asyncpg.Connection,
    tenant_id:    str,
    user_id:      str,        # Okta user_id from X-User-ID header
    connector_id: str,        # Which connector the sub-query targets
) -> str:
    """
    Looks up the source-system identity for this user + connector pair.
    Returns the source_user_id if a mapping exists, or the original user_id
    if no mapping is found (pass-through — source uses Okta identity directly).

    Reads from: nexus_system.identity_mapping
    [CLARIFY: OQ-QE-06 / OQ-DM-06 — confirm identity_mapping is seeded for all
     connectors before this function is called in production.]
    """
    row = await pg_conn.fetchrow("""
        SELECT source_user_id
        FROM   nexus_system.identity_mapping
        WHERE  tenant_id    = $1
        AND    okta_user_id = $2
        AND    connector_id = $3
    """, tenant_id, user_id, connector_id)

    return row["source_user_id"] if row else user_id
```

---

## 6. FX Service

**File:** `nexus_core/fx.py` ← NEW

Shared by `nexus-m3-writer` (TimescaleDB writer) and potentially by `nexus-query-executor` (for displaying currency-normalised results). Fetches ECB reference exchange rates; Redis-cached per `(from_currency, to_currency, date)` with 24-hour TTL.

```python
@dataclass
class FXService:
    redis_client: Redis
    ecb_base_url: str = "https://data-api.ecb.europa.eu/service/data/EXR"

    async def convert(
        self,
        amount:        Decimal,
        from_currency: str,
        to_currency:   str,
        as_of:         datetime,
    ) -> tuple[Decimal, Decimal]:  # (normalised_value, fx_rate)
        """
        Returns (converted_amount, fx_rate).
        Uses historical ECB rate for the given date.
        Caches rate in Redis: key = "fx:{from}:{to}:{YYYY-MM-DD}", TTL = 86400s
        """
        if from_currency == to_currency:
            return amount, Decimal("1.0")

        cache_key = f"fx:{from_currency}:{to_currency}:{as_of.date().isoformat()}"
        cached = await self.redis_client.get(cache_key)

        if cached:
            rate = Decimal(cached)
        else:
            rate = await self._fetch_ecb_rate(from_currency, to_currency, as_of.date())
            await self.redis_client.setex(cache_key, 86400, str(rate))

        return amount * rate, rate
```

---

## 7. onboard_tenant() — Elasticsearch Index Template Provisioning

**File:** `nexus_core/provisioning.py` ← UPDATE

Add Elasticsearch index template provisioning to the tenant onboarding flow. `nexus-m3-writer` never creates per-entity-type indexes itself — it calls `ensure_index_exists()` (§8) lazily on first write, which is safe and idempotent. However, the per-tenant index template (which enforces the dense_vector mapping and HNSW settings) is provisioned here at onboarding time so that any index auto-created later inherits the correct mapping.

```python
from nexus_core.db.elasticsearch import get_es_client, build_entity_index_template_name

async def onboard_tenant(tenant_id: str, pg_pool, es_client=None, ...) -> None:
    # ... existing onboarding steps (create DB schema, seed tenant_configs, etc.) ...

    # NEW Step 1: Create per-tenant schema and query tables in PostgreSQL
    async with pg_pool.acquire() as conn:
        tenant_schema = f"tenant_{tenant_id.replace('-', '_')}"
        await conn.execute(f"""
            CREATE SCHEMA IF NOT EXISTS {tenant_schema};
            REVOKE ALL ON SCHEMA {tenant_schema} FROM PUBLIC;
            GRANT USAGE ON SCHEMA {tenant_schema} TO nexus_app;
        """)
        await conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {tenant_schema}.query_sessions (
                -- DDL defined in NEXUS-Iter2-SPEC-DataModel-v0.5.md V2.0.5
            );
            CREATE TABLE IF NOT EXISTS {tenant_schema}.query_sub_results (
                -- DDL defined in NEXUS-Iter2-SPEC-DataModel-v0.5.md V2.0.5
            );
        """)

    # NEW Step 2: Register Elasticsearch index template for this tenant
    # Template matches all indexes named nexus_{tenant_slug}_*
    # Ensures every per-entity-type index inherits the correct dense_vector mapping.
    if es_client is None:
        es_client = get_es_client()

    tenant_slug = tenant_id.replace("-", "_")
    template_name = build_entity_index_template_name(tenant_slug)

    await es_client.indices.put_index_template(
        name=template_name,
        body={
            "index_patterns": [f"nexus_{tenant_slug}_*"],
            "template": {
                "settings": {
                    "number_of_shards": 2,
                    "number_of_replicas": 1,
                    "index.knn": True,
                },
                "mappings": {
                    "properties": {
                        "cdm_entity_id":         {"type": "keyword"},
                        "tenant_id":             {"type": "keyword"},
                        "cdm_entity_type":       {"type": "keyword"},
                        "golden_record_id":      {"type": "keyword"},
                        "embedding":             {
                            "type": "dense_vector",
                            "dims": 1536,          # [CLARIFY: ES-OQ-02 — confirm model + dims]
                            "index": True,
                            "similarity": "cosine",
                        },
                        "provenance_hash":       {"type": "keyword"},
                        "materialization_level": {"type": "keyword"},
                        "is_deleted":            {"type": "boolean"},
                        "source_systems":        {"type": "keyword"},
                        "cdm_version":           {"type": "keyword"},
                        "created_at":            {"type": "date"},
                        "updated_at":            {"type": "date"},
                    }
                },
            },
            "priority": 100,
        },
    )
```

**Important:** The `dims: 1536` value must match the embedding model configured in `agent_core.EmbeddingClient`. Once an index is created it cannot have its `dims` changed without a full reindex. Decision ES-OQ-02 (embedding model selection) must be resolved before the first `onboard_tenant()` call in any environment.

---

## 8. Elasticsearch Helper

**File:** `nexus_core/db/elasticsearch.py` ← NEW

All services that read from or write to Elasticsearch must obtain their client through this module. No service may instantiate `Elasticsearch(...)` directly.

```python
from elasticsearch import AsyncElasticsearch
from functools import lru_cache
import os

@lru_cache(maxsize=1)
def get_es_client() -> AsyncElasticsearch:
    """
    Returns a shared AsyncElasticsearch client.
    Configured from environment variables:
      NEXUS_ES_HOSTS        — comma-separated hosts, e.g. "https://es-node1:9200,https://es-node2:9200"
      NEXUS_ES_API_KEY      — Elasticsearch API key (preferred) OR
      NEXUS_ES_USERNAME /
      NEXUS_ES_PASSWORD     — basic auth fallback
      NEXUS_ES_CA_CERTS     — path to CA certificate bundle (for self-hosted TLS)
    """
    hosts = os.environ["NEXUS_ES_HOSTS"].split(",")
    api_key = os.getenv("NEXUS_ES_API_KEY")

    if api_key:
        return AsyncElasticsearch(hosts=hosts, api_key=api_key)

    return AsyncElasticsearch(
        hosts=hosts,
        basic_auth=(os.environ["NEXUS_ES_USERNAME"], os.environ["NEXUS_ES_PASSWORD"]),
        ca_certs=os.getenv("NEXUS_ES_CA_CERTS"),
    )


def get_entity_index_name(tenant_id: str, cdm_entity_type: str) -> str:
    """
    Returns the Elasticsearch index name for a given tenant + entity type.

    Convention: nexus_{tenant_slug}_{entity_type_lower}
    Examples:
      ("acme-corp", "Contact")    → "nexus_acme_corp_contact"
      ("globex", "Invoice")       → "nexus_globex_invoice"
    """
    tenant_slug = tenant_id.replace("-", "_").lower()
    entity_slug = cdm_entity_type.replace(".", "_").replace("-", "_").lower()
    return f"nexus_{tenant_slug}_{entity_slug}"


def build_entity_index_template_name(tenant_slug: str) -> str:
    """Returns the index template name for a tenant (used in onboard_tenant)."""
    return f"nexus_{tenant_slug}_entity_template"


async def ensure_index_exists(
    es_client: AsyncElasticsearch,
    index_name: str,
) -> None:
    """
    Creates the index if it does not exist. Safe to call on every write — uses
    a conditional PUT so concurrent first-writes are harmless.

    The index inherits its mapping from the per-tenant index template registered
    in onboard_tenant(). This function only creates the index shell; mapping comes
    from the template.

    Raises: elasticsearch.BadRequestError if the index exists with an incompatible
    mapping (should not happen if onboard_tenant() was called correctly).
    """
    if not await es_client.indices.exists(index=index_name):
        try:
            await es_client.indices.create(index=index_name)
        except Exception as e:
            # Concurrent creation — another writer beat us. Safe to ignore.
            if "resource_already_exists_exception" not in str(e).lower():
                raise
```

**Usage pattern in `nexus-m3-writer`:**

```python
from nexus_core.db.elasticsearch import get_es_client, get_entity_index_name, ensure_index_exists

es = get_es_client()
index = get_entity_index_name(event.tenant_id, event.cdm_entity_type)
await ensure_index_exists(es, index)
await es.update(index=index, id=event.cdm_entity_id, body={"doc": doc, "doc_as_upsert": True})
```

---

## 9. ConnectorBatchConfig — Batch History Ingestion Config

**File:** `nexus_core/models.py`

Dataclass representing the batch history configuration for a connector. Read by the `nexus_batch_history_ingest` Airflow DAG and by `nexus-m1-worker` when processing batch-sourced records. Persisted in `nexus_system.connector_batch_state` (DataModel v0.4).

```python
@dataclass
class ConnectorBatchConfig:
    connector_id:       str
    tenant_id:          str
    years_back:         int   = 5
    # How many years of history to process on initial backfill.
    batch_size:         int   = 500
    # Records per Kafka message batch published to m1.int.raw_records.
    cursor_field:       str   = "updated_at"
    # Source field used as incremental cursor (e.g. updated_at, seq_id, created_date).
    last_cursor_value:  str | None = None
    # High-water mark of last committed batch page. None = not yet started.
    # Updated after each committed batch; survives Airflow DAG restarts.

    @classmethod
    def from_db_row(cls, row: dict) -> "ConnectorBatchConfig":
        """Hydrate from a nexus_system.connector_batch_state row."""
        return cls(
            connector_id=row["connector_id"],
            tenant_id=row["tenant_id"],
            years_back=row["years_back"],
            batch_size=row["batch_size"],
            cursor_field=row["cursor_field"],
            last_cursor_value=row["last_cursor_value"],
        )

    def cursor_start_value(self) -> str:
        """Returns last_cursor_value if set, else computes the historical start date."""
        if self.last_cursor_value:
            return self.last_cursor_value
        return (datetime.utcnow() - timedelta(days=self.years_back * 365)).isoformat()
```

**Who uses this:**
- `nexus_batch_history_ingest` Airflow DAG (D1-06) — reads config, drives Airbyte connector job, updates `last_cursor_value` after each page
- `nexus-spark-transformer` — reads batch config to know cursor position; feeds into Spark job parameters

---

## 10. SparkTransformResult — Transformed Record Envelope

**File:** `nexus_core/models.py`

The envelope published to `m1.int.transformed_records` by `nexus-spark-transformer` and consumed by `nexus-cdm-mapper`. This replaces the raw `NexusMessage` payload that the CDM Mapper previously consumed directly from `m1.int.raw_records`. It carries the same routing metadata as a `NexusMessage` plus Spark-produced transformation outputs.

```python
@dataclass
class FieldQuality:
    null_rate:     float          # 0.0–1.0
    format_valid:  bool
    cardinality:   int | None     # distinct values seen in this batch (None if not computed)

@dataclass
class TransformedField:
    source_field:   str
    raw_value:      Any            # original value from source (retained for audit)
    typed_value:    Any            # after Spark type coercion
    coerced_type:   str            # "date", "decimal", "string", "boolean", "integer"
    original_currency: str | None  # e.g. "USD" — only set for monetary fields
    fx_rate:        float | None   # rate used to convert to base currency
    normalised_value: Any | None   # FX-converted value (monetary fields only)
    pii_flag:       bool           # pre-computed from schema_snapshots.column_profiles
    quality:        FieldQuality

@dataclass
class SparkTransformResult:
    # Routing metadata (same as NexusMessage)
    message_id:      str = field(default_factory=lambda: str(uuid.uuid4()))
    tenant_id:       str
    connector_id:    str
    source_system:   str
    source_table:    str
    op:              Literal["c", "u", "d", "r"]  # r = Debezium READ (snapshot)
    schema_version:  str = "2.0"
    published_at:    datetime = field(default_factory=datetime.utcnow)

    # Entity identity — assigned by Spark entity resolution stage
    cdm_entity_id:   str          # Golden Record ID; empty string if no match found
    entity_type:     str | None   # e.g. "party.customer" — pre-classified if obvious
    source_record_id: str         # source system's primary key

    # Transformed fields — CDM Mapper classifies against these
    fields:          list[TransformedField]

    # Transformation provenance
    spark_job_id:    str          # for replay and lineage tracing
    delta_checkpoint_path: str | None  # set if record was checkpointed via Delta Lake
    transformation_ms: int        # Spark processing latency for this record
```

**What the CDM Mapper does with this:**
- Iterates `fields` — calls `classify_field()` for each `TransformedField`
- Uses `typed_value` (not `raw_value`) for classification context
- Reads `pii_flag` directly — no longer calls `PIIChecker` per-field (Spark pre-computed it from `schema_snapshots`)
- `cdm_entity_id` is trusted if non-empty — mapper does not re-run entity resolution
- Persists the result as a `cdm_proposals` row with `proposal_id` keyed on natural key

**Virtual CDM note:** `typed_value` and `normalised_value` are used transiently during classification and embedding generation. They are not persisted in any M3 store. The Virtual CDM principle is unchanged.

---

## Acceptance Criteria

- `from nexus_core.topics import CrossModuleTopicNamer` — all 10 topic methods resolve correctly, including `m1_classification_produced()`, `m4_validation_decision()`, `m2_agent_step_completed()`, `m3_write_completed()`, `materialization_changed()`
- `from nexus_core.topics import CrossModuleTopicNamer` — `query_submitted(tid)` returns `"{tid}.query.submitted"` (not `"{tid}.nexus.query_submitted"`)
- `from nexus_core.db.elasticsearch import get_es_client, get_entity_index_name, ensure_index_exists` — client instantiates from env vars; `get_entity_index_name("acme-corp", "Contact")` returns `"nexus_acme_corp_contact"`
- `onboard_tenant()` calls `put_index_template` with `index_patterns: ["nexus_{tenant_slug}_*"]` and correct `dense_vector` mapping; calling it twice for the same tenant does not raise an error
- `ensure_index_exists()` is idempotent — calling it 10 times for the same index produces exactly one index
- `from nexus_core.models import SparkTransformResult, TransformedField, FieldQuality` — dataclasses instantiate correctly; `cdm_entity_id` defaults to empty string; `delta_checkpoint_path` defaults to `None`
- `from nexus_core.models import ConnectorBatchConfig` — `cursor_start_value()` returns correct historical start when `last_cursor_value` is `None`; returns `last_cursor_value` when set
- `from nexus_core.models import NexusMessage` — `event_action` field present with `"read"` as a valid value
- `from nexus_core.db.timescale import get_timescale_connection` — RLS policy set and verified in integration test
- `from nexus_core.db.identity import resolve_source_identity` — returns source identity or Okta passthrough
- `from nexus_core.fx import FXService` — ECB rate fetched and Redis-cached; historical date lookup works
- No `import pinecone` anywhere in the library — verified by `grep -r "pinecone" nexus_core/`
- All existing Iteration 1 unit tests still pass (no breaking changes)
- New minor version published to internal PyPI before end of Week 1

---

## Breaking Changes

None. All changes are additive. `event_action = None` default ensures backward compatibility with all Iteration 1 message producers.
