# NEXUS — nexus_core Library: Full Task Breakdown
**Owner: Tech Lead · Duration: Week 1–2 · Hard gate for all teams**

---

## Why nexus_core Exists

NEXUS is built by four teams working in parallel across six modules. Each team writes Python services that produce and consume Kafka messages, query PostgreSQL, manage tenant context, and authenticate users. Without a shared foundation, each team independently solves the same problems — and solves them differently.

This is not a theoretical risk. It is what always happens in distributed teams without a shared library:

- Team 2 defines `NexusMessage` with a `tenant_id` field. Team 3 defines theirs with `tenantId`. The messages are incompatible and integration fails in Week 7.
- Team 2 calls `pool.acquire()` directly. Team 4 uses `get_tenant_scoped_connection()`. One of them leaks cross-tenant data silently — PostgreSQL RLS only enforces what you give it.
- Team 3 constructs Kafka topic names as f-strings. A tenant ID containing a dot (`acme.corp`) splits into two topic segments. Messages go to the wrong consumer group for three days before anyone notices.
- Team 4 logs the full request payload including `api_key` fields. A production log aggregation pipeline indexes credentials. A security audit fails.

`nexus_core` exists to make all of these mistakes impossible by construction. It is not a convenience library — it is the enforcement mechanism for every cross-cutting architectural constraint in the platform.

### What nexus_core Enforces

**Tenant isolation.** `TenantContext` via `contextvars` ensures every function in a processing chain operates on the correct tenant without passing `tenant_id` through every call signature. `get_tenant_scoped_connection()` ensures PostgreSQL RLS is always activated before a query runs. `NexusConsumer` rejects messages from unknown or suspended tenants before they reach application code.

**Message contract.** Every Kafka message in the platform is a `NexusMessage`. The schema is defined once. The `permission_scope` field is validated as present on every message — even when empty — so Iteration 2 RBAC enforcement has a consistent field to read from day one.

**Topic naming.** `CrossModuleTopicNamer` validates tenant IDs and event names at call time. No service can accidentally produce to a malformed topic or an undefined event type.

**User identity.** `NexusUserIdentity` and `get_user_identity()` provide a single, consistent way to extract the requesting user's identity from Kong-injected headers across all HTTP-facing services. No service reads JWTs directly — Kong owns that concern.

**Tenant provisioning.** `provisioning.py` is the only way a tenant is created. It is importable by any service and callable as a CLI command. There is no loose script that can drift out of sync with the library.

**Security hygiene.** `NexusMessage.safe_log_repr()` strips sensitive fields before any logging call. Sensitive field scrubbing is enforced at the library level, not left to each developer to remember.

### What nexus_core Does Not Do

`nexus_core` is a shared foundation, not a framework. It does not own business logic, ML inference, workflow execution, or UI rendering. Each module team owns their domain completely — `nexus_core` only owns the contracts between them.

If a team needs to do something not covered by `nexus_core`, they implement it in their own module. If the same need arises in a second module, it gets promoted into `nexus_core`. The library grows by pull, not by anticipation.

---

## Why This Is a Hard Gate

Every team imports from `nexus_core`. Without it, each team writes their own version of `NexusMessage`, their own topic strings, their own tenant handling — and by Week 3 you have four incompatible implementations. `nexus_core` must be delivered and verified before any application code begins.

---

## Package Structure

```
nexus_core/
├── __init__.py
├── messaging.py        # NexusMessage, NexusProducer, NexusConsumer
├── tenant.py           # TenantContext, get_tenant(), set_tenant(), clear_tenant()
├── tenant_validator.py # is_active_tenant() — passed to NexusConsumer
├── provisioning.py     # onboard_tenant() function + CLI entry point
├── identity.py         # NexusUserIdentity, get_user_identity()  ← NEW Iter 1
├── topics.py           # CrossModuleTopicNamer (with validation)
├── entities.py         # CDM Entity dataclasses
├── schemas.py          # SourceKnowledgeArtifact, ProposedInterpretation
├── errors.py           # NexusException hierarchy
├── logging.py          # structlog setup
├── db.py               # get_tenant_scoped_connection()
└── cdm_registry.py     # CDMRegistryService (per-tenant cache)
```

---

## LEAD-00 — Tenant Provisioning

**Depends on:** Kafka cluster up (P0-INFRA-02), PostgreSQL up (P0-INFRA-03)

### Sub-task 00-A — Tenants Table DDL

Create the `nexus_system.tenants` table. This is the authoritative registry of all active tenants. Every NexusConsumer validates against it before processing a message.

```sql
CREATE TABLE nexus_system.tenants (
    tenant_id       TEXT PRIMARY KEY,
    plan            TEXT NOT NULL DEFAULT 'professional',
    cdm_version     TEXT NOT NULL DEFAULT '1.0.0',
    status          TEXT NOT NULL DEFAULT 'active',   -- active | suspended | offboarding
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);
```

### Sub-task 00-B — provisioning.py Module + CLI Entry Point

Tenant provisioning lives inside the `nexus_core` package as `provisioning.py`. This means:
- The logic is **importable** by any service that needs to trigger provisioning programmatically
- It is **versioned** with the library — no drift between script and package
- It is **testable** as a proper module with pytest

```python
# nexus_core/provisioning.py

async def onboard_tenant(tenant_id: str, plan: str = "professional") -> None:
    """
    Creates DB row, Kafka topics, verifies RLS isolation. Idempotent — safe to re-run.
    
    Steps:
    1. Insert into nexus_system.tenants (ON CONFLICT DO NOTHING)
    2. Create all per-tenant Kafka topics via Admin API
    3. Verify topics exist before returning
    4. Log completion with topic count
    """
    ...

def cli():
    """Entry point for the onboard-tenant CLI command."""
    import argparse
    parser = argparse.ArgumentParser(description="Provision a new NEXUS tenant")
    parser.add_argument("--tenant", required=True, help="Tenant ID (no dots)")
    parser.add_argument("--plan", default="professional", choices=["professional", "enterprise"])
    args = parser.parse_args()
    asyncio.run(onboard_tenant(args.tenant, args.plan))
```

Registered as a CLI entry point in `pyproject.toml` so it's available as a command after install:

```toml
# pyproject.toml
[project.scripts]
onboard-tenant = "nexus_core.provisioning:cli"
```

Usage after package install:

```bash
# As CLI command (after pip install)
onboard-tenant --tenant acme-corp --plan professional

# Or programmatically from any service
from nexus_core.provisioning import onboard_tenant
await onboard_tenant("acme-corp", plan="enterprise")
```

What provisioning does:
- Inserts row into `nexus_system.tenants` (`ON CONFLICT DO NOTHING` — idempotent)
- Creates all per-tenant Kafka topics: `{tid}.m1.*`, `{tid}.m2.*`, `{tid}.m3.*`, `{tid}.m4.*`
- Verifies all topics exist before returning
- Idempotent — safe to re-run if interrupted

**Acceptance criteria:**
- Running twice for the same tenant does not error and does not duplicate topics
- A tenant not in the `tenants` table cannot have messages processed (NexusConsumer rejects them)
- `onboard-tenant --tenant test-alpha` completes in under 10 seconds
- `from nexus_core.provisioning import onboard_tenant` works from any service

### Sub-task 00-C — Tenant Validator for NexusConsumer

```python
# nexus_core/tenant_validator.py

def is_active_tenant(tenant_id: str) -> bool:
    """
    Checks nexus_system.tenants for active status.
    Called by NexusConsumer before passing message to application code.
    Result is cached for 60 seconds — balance between freshness and DB load.
    """
```

**Why this matters:** If a tenant is suspended or offboarded, their messages must stop being processed immediately. Without this check, a suspended tenant's data continues flowing through the pipeline.

---

## LEAD-01 — PostgreSQL Row-Level Security

**Depends on:** Sub-task 00-A (tenants table)

### Sub-task 01-A — Enable RLS on All nexus_system Tables

```sql
-- Apply to every table in nexus_system schema
ALTER TABLE nexus_system.cdm_versions    ENABLE ROW LEVEL SECURITY;
ALTER TABLE nexus_system.cdm_mappings    ENABLE ROW LEVEL SECURITY;
ALTER TABLE nexus_system.connectors      ENABLE ROW LEVEL SECURITY;
ALTER TABLE nexus_system.sync_jobs       ENABLE ROW LEVEL SECURITY;
ALTER TABLE nexus_system.governance_proposals ENABLE ROW LEVEL SECURITY;
ALTER TABLE nexus_system.identity_mapping     ENABLE ROW LEVEL SECURITY;

-- Policy: rows only visible when app.current_tenant matches
CREATE POLICY tenant_isolation ON nexus_system.cdm_mappings
    USING (tenant_id = current_setting('app.current_tenant'));
-- (repeat for each table)
```

### Sub-task 01-B — get_tenant_scoped_connection() in db.py

Every service that queries PostgreSQL must use this function. Calling `pool.acquire()` directly is a standards violation.

```python
# nexus_core/db.py

async def get_tenant_scoped_connection(pool, tenant_id: str):
    """
    Returns a connection with app.current_tenant set.
    RLS policies enforce isolation automatically from this point.
    Never call pool.acquire() directly in application code.
    """
    conn = await pool.acquire()
    await conn.execute(f"SET app.current_tenant = '{tenant_id}'")
    return conn
```

### Sub-task 01-C — RLS Verification Test

```python
# Must pass before any team writes application DB queries

async def test_rls_isolation():
    conn_alpha = await get_tenant_scoped_connection(pool, "test-alpha")
    conn_beta  = await get_tenant_scoped_connection(pool, "test-beta")

    # Insert record for alpha
    await conn_alpha.execute(
        "INSERT INTO nexus_system.sync_jobs (tenant_id, ...) VALUES ('test-alpha', ...)"
    )
    # Query from beta connection — must return zero rows
    rows = await conn_beta.fetch("SELECT * FROM nexus_system.sync_jobs")
    assert len(rows) == 0, "RLS FAILURE: beta can see alpha data"
```

**Acceptance criteria:**
- Test above passes
- Direct `pool.acquire()` anywhere in codebase fails code review (linting rule added to standards doc)

---

## LEAD-02 — nexus_core Python Package

### Sub-task 02-A — TenantContext and contextvars (tenant.py)

`TenantContext` carries per-request tenant state through the Python process via `contextvars` — thread-safe, coroutine-safe, request-scoped. Every function that needs the current tenant calls `get_tenant()` instead of receiving `tenant_id` as a parameter.

```python
# nexus_core/tenant.py

@dataclass
class TenantContext:
    tenant_id:           str
    plan:                str    # professional | enterprise
    cdm_version:         str    # Active CDM version at processing time
    max_connectors:      int = 10
    max_records_per_day: int = 1_000_000

_tenant_context: ContextVar[TenantContext | None] = ContextVar("tenant_context", default=None)

def set_tenant(ctx: TenantContext) -> None:
    """Call at the start of each Kafka message processing iteration."""
    _tenant_context.set(ctx)

def get_tenant() -> TenantContext:
    """
    Raises TenantContextMissing if called outside a tenant-scoped block.
    Intentional — code accessing tenant data without context is a bug.
    """
    ctx = _tenant_context.get()
    if ctx is None:
        raise TenantContextMissing(
            "get_tenant() called outside a tenant context. "
            "Every Kafka worker must call set_tenant() at the top of its processing loop."
        )
    return ctx

def clear_tenant() -> None:
    """Call in a finally block. Prevents stale context leaking into pooled coroutines."""
    _tenant_context.set(None)
```

**Required worker pattern — every Kafka worker must look like this:**

```python
async def process_message(self, message: NexusMessage):
    cdm_version = self.registry.get_active_cdm_version(message.tenant_id)
    set_tenant(TenantContext(
        tenant_id=message.tenant_id,
        plan="professional",
        cdm_version=cdm_version,
    ))
    try:
        await self._do_actual_work(message)
    finally:
        clear_tenant()  # Always — prevents context leaking into next message
```

**Acceptance criteria:**
- `set_tenant(alpha)` then `get_tenant()` returns alpha context
- `clear_tenant()` then `get_tenant()` raises `TenantContextMissing`

---

### Sub-task 02-B — NexusMessage + NexusProducer + NexusConsumer (messaging.py)

The complete Kafka message envelope. The offset commit rule is non-negotiable: **never commit before the message is fully processed.**

```python
# nexus_core/messaging.py

_SENSITIVE_FIELDS = {
    "password", "api_key", "token", "secret", "access_token",
    "refresh_token", "security_token", "private_key", "credentials"
}

@dataclass
class NexusMessage:
    topic:            str
    tenant_id:        str       # Always authoritative — comes from Kafka message key
    source_system:    str       # 'salesforce' | 'servicenow' | 'odoo' | 'postgresql' | 'mysql' | 'sqlserver'
    source_record_id: str       # Native PK from source system
    permission_scope: dict      # ALWAYS {} in Iteration 1 — never None, never omitted
    entity_type:      str       # CDM entity type e.g. 'crm.opportunity'
    payload:          dict      # The actual data
    message_id:       str = field(default_factory=lambda: str(uuid.uuid4()))
    correlation_id:   str = ""
    trace_id:         str = ""
    schema_version:   str = "1.0"
    published_at:     str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def safe_log_repr(self) -> dict:
        """Always use this for logging. Never log the raw payload."""
        return {
            "message_id":      self.message_id,
            "topic":           self.topic,
            "tenant_id":       self.tenant_id,
            "source_system":   self.source_system,
            "source_record_id":self.source_record_id,
            "permission_scope":self.permission_scope,  # safe — always {}
            "published_at":    self.published_at,
            "payload_keys": [k for k in self.payload if k.lower() not in _SENSITIVE_FIELDS],
        }
```

**NexusConsumer — the two non-negotiable rules:**
1. `enable.auto.commit: False` — never change this
2. Only call `commit()` after full successful processing — on exception, do not commit, message redelivers on restart

```python
class NexusConsumer:
    def __init__(self, bootstrap_servers, group_id, topics, tenant_validator=None):
        self._consumer = Consumer({
            "bootstrap.servers":  bootstrap_servers,
            "group.id":           group_id,
            "auto.offset.reset":  "earliest",
            "enable.auto.commit": False,    # NEVER change this
        })
        self._consumer.subscribe(topics)
        self._tenant_validator = tenant_validator

    def poll(self, timeout: float = 1.0) -> NexusMessage | None:
        msg = self._consumer.poll(timeout=timeout)
        if msg is None or msg.error():
            return None
        message = NexusMessage.from_kafka_value(msg.value())
        # Reject unknown/suspended tenants before reaching application code
        if self._tenant_validator and not self._tenant_validator(message.tenant_id):
            self._consumer.commit(asynchronous=False)  # Commit to avoid poison-pill loop
            return None
        return message

    def commit(self, message: NexusMessage) -> None:
        """Call ONLY after full successful processing."""
        self._consumer.commit(asynchronous=False)
```

**Acceptance criteria:**
- Crash-restart test: worker crashes mid-batch → offset not committed → message redelivered on restart
- Sensitive field test: publish `NexusMessage` with `payload={"api_key": "mysecret"}` → grep logs for "mysecret" → must not appear
- `permission_scope` is present and `{}` on every message — schema validation rejects `None`

---

### Sub-task 02-C — NexusUserIdentity (identity.py) ← NEW IN ITERATION 1

Every HTTP-facing service must extract the requesting user's identity from headers injected by Kong. Never read from request body — headers are the only authoritative source.

```python
# nexus_core/identity.py

@dataclass
class NexusUserIdentity:
    user_id:   str   # Okta sub claim — from X-User-ID header
    tenant_id: str   # Okta custom claim — from X-Tenant-ID header
    email:     str   # Okta email claim — from X-User-Email header

def get_user_identity(request_headers: dict) -> NexusUserIdentity:
    """
    Extracts user identity from Kong-injected headers.
    Raises IdentityMissingError if any required header is absent.
    
    Kong validates the JWT and injects these headers before the request
    reaches any service. If headers are absent, Kong already returned 401.
    This function is a safety net for internal calls that bypass Kong.
    """
    user_id   = request_headers.get("X-User-ID")
    tenant_id = request_headers.get("X-Tenant-ID")
    email     = request_headers.get("X-User-Email")

    if not all([user_id, tenant_id, email]):
        raise IdentityMissingError(
            "Missing identity headers. All HTTP requests must pass through Kong. "
            f"Present headers: user_id={bool(user_id)}, tenant_id={bool(tenant_id)}, email={bool(email)}"
        )
    return NexusUserIdentity(user_id=user_id, tenant_id=tenant_id, email=email)
```

**OIDC Configuration — single environment variable:**

```python
# nexus_core/oidc.py

OIDC_ISSUER_URL = os.getenv("OIDC_ISSUER_URL")
# Examples:
# Dev/Demo (Okta developer org):  https://dev-xxxxx.okta.com
# Client A (their Okta):          https://acme.okta.com
# Client B (Azure AD):            https://login.microsoftonline.com/{tenant}/v2.0

# Kong reads this at startup to configure JWT plugin.
# Swapping IdP = one environment variable change, no code change.
```

**Acceptance criteria:**
- `get_user_identity()` raises `IdentityMissingError` when any header is absent
- All M4 governance actions log `nexus_user_id` from identity
- `OIDC_ISSUER_URL` is the only config change needed to swap IdP

---

### Sub-task 02-D — CrossModuleTopicNamer with Validation (topics.py)

Validates tenant_id format (no dots — dots are Kafka topic separators) and event name against a whitelist. Programming errors caught at call time, not at runtime.

```python
# nexus_core/topics.py

class CrossModuleTopicNamer:

    _VALID_M1_EVENTS = {"semantic_interpretation_requested", "sync_completed"}
    _VALID_M2_EVENTS = {
        "semantic_interpretation_complete", "agent_response_ready",
        "workflow_trigger", "knowledge_query", "knowledge_query_result",
    }
    _VALID_M4_EVENTS = {"mapping_approved", "workflow_completed"}

    # Static platform topics — always exist, always tenant-scoped by payload field
    class M1Internal:
        SYNC_REQUESTED              = "m1.int.sync_requested"
        RAW_RECORDS                 = "m1.int.raw_records"
        DELTA_BATCH_READY           = "m1.int.delta_batch_ready"
        CDM_ENTITIES_READY          = "m1.int.cdm_entities_ready"
        AI_ROUTING_DECIDED          = "m1.int.ai_routing_decided"
        MAPPING_FAILED              = "m1.int.mapping_failed"
        STRUCTURAL_CYCLE_TRIGGERED  = "m1.int.structural_cycle_triggered"
        DEAD_LETTER                 = "m1.int.dead_letter"

    class CDM:
        EXTENSION_PROPOSED = "nexus.cdm.extension_proposed"
        VERSION_PUBLISHED  = "nexus.cdm.version_published"
        EXTENSION_REJECTED = "nexus.cdm.extension_rejected"

    @staticmethod
    def _validate_tenant_id(tenant_id: str):
        if not tenant_id:
            raise ValueError("tenant_id must not be empty")
        if "." in tenant_id:
            raise ValueError(
                f"tenant_id '{tenant_id}' contains a dot. "
                "Dots are reserved as Kafka topic segment separators."
            )

    @staticmethod
    def m1(tenant_id: str, event: str) -> str:
        CrossModuleTopicNamer._validate_tenant_id(tenant_id)
        if event not in CrossModuleTopicNamer._VALID_M1_EVENTS:
            raise ValueError(f"Unknown M1 cross-module event: '{event}'")
        return f"{tenant_id}.m1.{event}"
    
    # m2(), m4() follow same pattern
```

**Acceptance criteria:**
- `CrossModuleTopicNamer.m1("acme.corp", "sync_completed")` → raises `ValueError` (dot in tenant_id)
- `CrossModuleTopicNamer.m1("acme-corp", "unknown_event")` → raises `ValueError` (unknown event)
- `CrossModuleTopicNamer.m1("acme-corp", "sync_completed")` → returns `"acme-corp.m1.sync_completed"`

---

### Sub-task 02-E — CDMRegistryService with Per-tenant Cache (cdm_registry.py)

Cache key is `(tenant_id, source_system, source_table, source_field, cdm_version)`. Two tenants on the same source system never share a cache entry. When CDM version is promoted, old cache entries go cold automatically.

```python
# nexus_core/cdm_registry.py

class CDMRegistryService:
    CACHE_TTL_SECONDS = 300

    def get_mapping(self, tenant_id, source_system, source_table, source_field, cdm_version):
        """Returns CDM mapping or None. Cache key includes tenant_id — no cross-tenant sharing."""
        cache_key = (tenant_id, source_system, source_table, source_field, cdm_version)
        ...

    def invalidate_tenant(self, tenant_id: str) -> int:
        """
        Purges ALL cache entries for this tenant across all CDM versions.
        Called by every worker subscribing to {tid}.m4.mapping_approved.
        Returns count of entries removed.
        """
        ...

    def get_active_cdm_version(self, tenant_id: str) -> str:
        """
        Returns active CDM version for this tenant.
        Always queries DB — never cached.
        Called at start of each Kafka message processing.
        """
        ...
```

---

### Sub-task 02-F — CDM Entity Dataclasses (entities.py)

Five canonical entity types. All include `tenant_id` (mandatory), `source_extras` (never discard unmapped fields), and `permission_scope` (always `{}` in Iteration 1).

| Entity | Maps from |
|---|---|
| `CDMParty` | Salesforce Account/Contact, Odoo res.partner |
| `CDMTransaction` | Odoo sale.order/invoice, SQL Server financial tables |
| `CDMProduct` | Odoo product.product, ERP product tables |
| `CDMEmployee` | BambooHR, Workday, HR database tables |
| `CDMIncident` | ServiceNow incident/problem/change |

All entities carry:
- `tenant_id: str` — mandatory, never None
- `source_system: str` — mandatory
- `source_record_id: str` — mandatory
- `permission_scope: dict` — always `{}` in Iteration 1
- `source_extras: dict` — unmapped source fields preserved here
- `mapping_tier: int` — 1=confirmed, 2=needs review

---

### Sub-task 02-G — Identity Mapping Table (db.py)

```sql
-- Created by Tech Lead. Populated from Okta group sync.
-- Enforcement NOT activated in Iteration 1.
-- Foundation for Iteration 2 RBAC.

CREATE TABLE nexus_system.identity_mapping (
    nexus_user_id    TEXT NOT NULL,   -- Okta sub claim
    tenant_id        TEXT NOT NULL,
    source_system    TEXT NOT NULL,   -- 'salesforce' | 'servicenow' | 'odoo' | ...
    source_user_id   TEXT NOT NULL,   -- Native user ID in that system
    source_username  TEXT,            -- Human-readable, optional
    created_at       TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (nexus_user_id, tenant_id, source_system)
);
```

**Iteration 1:** Table exists, seeded manually from Okta group export. Not queried by any enforcement logic yet.
**Iteration 2:** Permission Resolver reads this table to map `X-User-ID` → source system user → source system permissions.

---

### Sub-task 02-H — Package Publication and Team Onboarding

```bash
# All teams add to requirements.txt:
git+https://github.com/mentis-consulting/nexus-core.git@main#egg=nexus-core

# Each team verifies before starting Week 3:
pip install git+https://github.com/mentis-consulting/nexus-core.git@main
python -c "from nexus_core.tenant import get_tenant; print('nexus_core OK')"
python -c "from nexus_core.identity import get_user_identity; print('identity OK')"
python -c "from nexus_core.messaging import NexusMessage; print('messaging OK')"
python -c "from nexus_core.provisioning import onboard_tenant; print('provisioning OK')"

# CLI entry point available after install:
onboard-tenant --tenant test-alpha --plan professional
```

**Acceptance criteria:**
- All four import checks pass on a clean environment for all four team members
- `onboard-tenant` CLI command available after install
- `pytest nexus_core/tests/ -v --cov=nexus_core --cov-report=term-missing` shows 100% coverage
- No team begins application code before their verification passes

---

## Full Acceptance Criteria Summary

| Sub-task | Test | Expected |
|---|---|---|
| 00-B provisioning.py | Run twice for same tenant | Idempotent, no error, no duplicate topics |
| 00-B provisioning.py | `from nexus_core.provisioning import onboard_tenant` | Imports cleanly from any service |
| 00-C tenant_validator | Message for suspended tenant | Rejected by NexusConsumer, offset committed |
| 01-C RLS | Beta connection queries alpha data | Zero rows returned |
| 02-A TenantContext | `clear_tenant()` then `get_tenant()` | Raises `TenantContextMissing` |
| 02-B NexusMessage | Log message with `api_key` in payload | "mysecret" never appears in logs |
| 02-B NexusConsumer | Crash mid-batch | Message redelivered on restart |
| 02-B permission_scope | Produce message with `permission_scope=None` | Schema validation rejects it |
| 02-C NexusUserIdentity | Call with missing headers | Raises `IdentityMissingError` |
| 02-D TopicNamer | Tenant ID with dot | Raises `ValueError` |
| 02-D TopicNamer | Unknown event name | Raises `ValueError` |
| 02-H package install | Fresh environment import | All three checks print OK |

---

## Delivery Checklist

- [ ] `nexus_system.tenants` table created with RLS
- [ ] `nexus_system.identity_mapping` table created
- [ ] `provisioning.py` module in package with `onboard-tenant` CLI entry point registered
- [ ] `onboard-tenant --tenant test-alpha` and `test-beta` run successfully
- [ ] RLS cross-tenant isolation test passes
- [ ] `nexus_core` package installs cleanly from Git
- [ ] 100% test coverage on all modules
- [ ] `OIDC_ISSUER_URL` environment variable documented and wired into Kong config
- [ ] All four teams have verified installation and passed import checks
- [ ] Development standards document published (LEAD-03)

---

*NEXUS nexus_core Task Breakdown · Mentis Consulting · February 2026 · Confidential*
