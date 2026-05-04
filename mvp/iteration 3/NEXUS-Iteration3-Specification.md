# NEXUS — Iteration 3 Specification
**Application Discovery · Process Intelligence · MVP Hardening**
Mentis Consulting · Version 1.0 · March 2026

---

## Context

### What Iteration 2 delivered

| Component | Status |
|---|---|
| M3 AI stores (Pinecone, Neo4j, TimescaleDB) | ✅ Done |
| nexus-query-api + nexus-query-executor | ✅ Done |
| Result Renderer (charts, tables, reports) | ✅ Done |
| Dashboard components with auto-refresh | ✅ Done |
| M6 Ask NEXUS connected to live query engine | ✅ Done |

### What Iteration 3 must deliver

Iteration 3 completes the MVP. Three objectives:

1. **Application Discovery** — NEXUS reads the processes that already exist inside the client's applications (Salesforce, ServiceNow, SAP, Odoo) and maps the gaps between them.
2. **Process Intelligence** — Based on the discovery, NEXUS proposes and executes coordination patterns — filling gaps between systems without replacing any existing process.
3. **Hardening** — OPA/PII enforcement, multi-tenant validation, performance baselines, and audit completeness required for the first real client deployment.

### Core design principle

NEXUS never redefines the client's processes. It reads what each application already knows and coordinates the gaps. An invoice approval process that exists in SAP stays in SAP. NEXUS only acts on what SAP cannot do alone — notify the right person, trigger the corresponding ServiceNow task, escalate if no action is taken.

---

## Workstream 1 — Application Discovery Engine

### 1.1 Application Registry

Two entry modes for registering applications, identical to connector registration in M1.

**Mode A — Manual registration (always available)**

```http
POST /applications
Authorization: Bearer <JWT>
X-Tenant-ID: acme-corp

{
  "app_type":       "salesforce",
  "display_name":   "Salesforce CRM (Acme)",
  "base_url":       "https://acme.my.salesforce.com",
  "credential_ref": "nexus/acme-corp/salesforce/credentials",
  "discovery_scope": ["objects", "flows", "approval_processes", "reports"]
}
```

**Mode B — Network auto-scan (optional, opt-in)**

```http
POST /applications/scan
{
  "network_range": "10.0.1.0/24",
  "timeout_seconds": 30
}
```

The scanner probes known application signatures:

```python
KNOWN_APP_SIGNATURES = {
    "salesforce": {
        "probe_url":   "/services/data/",
        "indicator":   "sobjects",
        "auth_type":   "oauth2"
    },
    "servicenow": {
        "probe_url":   "/api/now/table/sys_db_object",
        "indicator":   "result",
        "auth_type":   "basic_or_oauth"
    },
    "sap": {
        "probe_url":   "/sap/opu/odata/sap/API_BUSINESS_PARTNER/",
        "indicator":   "d",
        "auth_type":   "basic"
    },
    "odoo": {
        "probe_url":   "/web/dataset/call_kw",
        "indicator":   "jsonrpc",
        "auth_type":   "session"
    }
}
```

**Database table**

```sql
CREATE TABLE nexus_system.applications (
    app_id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id         VARCHAR(100) NOT NULL,
    app_type          VARCHAR(50)  NOT NULL,
    display_name      VARCHAR(200) NOT NULL,
    base_url          VARCHAR(500) NOT NULL,
    credential_ref    VARCHAR(300) NOT NULL,
    discovery_scope   JSONB,
    status            VARCHAR(20)  DEFAULT 'registered',
    -- registered → probing → probed → active
    discovered_at     TIMESTAMPTZ,
    last_probed_at    TIMESTAMPTZ
);
```

---

### 1.2 Application Probes

One probe per application type. Each probe reads the application's automation metadata — not data, not records, only configuration: objects, workflows, approval rules, reports.

Probes use Airflow provider hooks exactly as in M1. `nexus-app-discoverer` is a Kafka consumer that triggers the appropriate probe on `application.registered`.

---

**Salesforce Probe**

Uses Salesforce Tooling API and Metadata API.

```python
class SalesforceProbe:
    async def probe(self, app: Application, creds: dict) -> AppCapabilityMap:
        sf = SalesforceHook(
            username=     creds["username"],
            password=     creds["password"],
            security_token: creds["security_token"],
            instance_url: app.base_url
        )

        return AppCapabilityMap(
            app_id=    app.app_id,
            objects=   await self._discover_objects(sf),
            processes= await self._discover_flows(sf)
                     + await self._discover_approval_processes(sf),
            hooks=     await self._discover_outbound_messages(sf),
            reports=   await self._discover_reports(sf)
        )

    async def _discover_flows(self, sf) -> list[AppProcess]:
        # Tooling API — reads Flow definitions
        records = sf.query(
            "SELECT Id, ApiName, Label, TriggerType, Status FROM Flow"
        )["records"]

        result = []
        for flow in records:
            if flow["Status"] != "Active":
                continue
            detail = self._get_flow_detail(sf, flow["Id"])
            result.append(AppProcess(
                id=              flow["Id"],
                name=            flow["Label"],
                trigger_type=    flow["TriggerType"],
                trigger_object=  detail.trigger_object,
                trigger_condition: detail.trigger_condition,
                steps=           detail.steps,
                process_type=    self._classify(flow, detail)
                # → "approval" | "notification" | "data_sync" | "escalation" | "scheduled"
            ))
        return result

    async def _discover_approval_processes(self, sf) -> list[AppProcess]:
        # Metadata API — reads ApprovalProcess definitions
        # Returns: name, entry criteria, approval steps, approver list
        pass

    async def _discover_reports(self, sf) -> list[AppReport]:
        # Returns existing reports → importable as NEXUS dashboard components
        pass
```

What Salesforce exposes:

| Discovery target | API | Example output |
|---|---|---|
| Business objects | REST describe | Opportunity, Account, Invoice__c, Case |
| Active Flows | Tooling API | "Large Deal Approval Flow" |
| Approval Processes | Metadata API | "Opportunity Approval > €50K — 2-level approval" |
| Outbound Messages | Metadata API | What Salesforce already sends to external systems |
| Reports | REST API | "Q1 Pipeline by Region", "Monthly Closed Deals" |

---

**ServiceNow Probe**

```python
class ServiceNowProbe:
    async def probe(self, app: Application, creds: dict) -> AppCapabilityMap:
        snow = ServiceNowHook(
            instance= app.base_url,
            username= creds["username"],
            password= creds["password"]
        )

        return AppCapabilityMap(
            app_id=   app.app_id,
            objects=  await self._discover_tables(snow),
            processes=await self._discover_workflows(snow)
                    + await self._discover_business_rules(snow),
            slas=     await self._discover_slas(snow)
        )

    async def _discover_workflows(self, snow) -> list[AppProcess]:
        # Table API: wf_workflow
        records = snow.get_records("wf_workflow", {"active": "true"})
        return [
            AppProcess(
                id=           r["sys_id"],
                name=         r["name"],
                trigger_type= "record_insert_or_update",
                trigger_object: r["table"],
                process_type= self._classify_workflow(r)
            )
            for r in records
        ]

    async def _discover_business_rules(self, snow) -> list[AppProcess]:
        # Table API: sys_script
        # Returns active business rules with their trigger conditions
        pass

    async def _discover_slas(self, snow) -> list[AppSLA]:
        # Table API: contract_sla
        # Returns SLA definitions — used for gap detection (escalation timing)
        pass
```

What ServiceNow exposes:

| Discovery target | Table | Example output |
|---|---|---|
| Tables | sys_db_object | Incident, Request, Approval, Change |
| Active Workflows | wf_workflow | "IT Request Approval", "Change Management" |
| Business Rules | sys_script | "Auto-assign P1", "Notify manager on approval" |
| SLA Definitions | contract_sla | "P1 Response: 1h", "Approval: 5 business days" |

---

**SAP Probe**

```python
class SAPProbe:
    async def probe(self, app: Application, creds: dict) -> AppCapabilityMap:
        # SAP API Business Hub — OData catalog discovery
        objects =   await self._discover_business_objects(creds)
        workflows = await self._discover_workflows(creds)
        bapis =     await self._discover_bapis(creds)

        return AppCapabilityMap(
            app_id=   app.app_id,
            objects=  objects,
            processes=workflows,
            actions=  bapis      # what NEXUS can call in SAP
        )

    async def _discover_bapis(self, creds) -> list[AppAction]:
        # BAPIs are callable actions NEXUS can invoke on behalf of a user
        # Examples: BAPI_INVOICE_CREATE, BAPI_PO_CHANGE, BAPI_EMPLOYEE_GETDATA
        # Discovery via RFC_FUNCTION_SEARCH or SAP API Hub catalog
        pass
```

**Odoo Probe**

```python
class OdooProbe:
    async def probe(self, app: Application, creds: dict) -> AppCapabilityMap:
        # Odoo JSON-RPC — ir.actions.server, mail.activity.type, approval.approval
        objects =        await self._discover_models(creds)
        automated_actions = await self._discover_automated_actions(creds)
        approvals =      await self._discover_approval_flows(creds)

        return AppCapabilityMap(
            app_id=   app.app_id,
            objects=  objects,
            processes=automated_actions + approvals
        )
```

---

### 1.3 Cross-System Gap Detection

After all probes complete for a tenant, the `CrossSystemGapDetector` runs. It compares the `AppCapabilityMap` of each application and identifies where the same process type appears in multiple systems without coordination.

```python
class CrossSystemGapDetector:
    def detect(self, capability_maps: list[AppCapabilityMap]) -> list[ProcessGap]:
        gaps = []

        # Group processes by type across all applications
        by_type = defaultdict(list)
        for app_map in capability_maps:
            for process in app_map.processes:
                by_type[process.process_type].append((app_map.app_id, process))

        for process_type, process_list in by_type.items():

            # Same process type in multiple apps → potential fragmentation
            if len(set(app_id for app_id, _ in process_list)) > 1:
                gaps.append(ProcessGap(
                    process_type=     process_type,
                    systems=          [app_id for app_id, _ in process_list],
                    gap_type=         "fragmented_process",
                    description=      self._describe_gap(process_list),
                    suggested_patterns=self._suggest_patterns(process_list),
                    confidence=       self._score_gap(process_list)
                ))

            # Intra-app gap: actor column nullable → step can be skipped
            for app_id, process in process_list:
                for actor in process.nullable_actors:
                    gaps.append(ProcessGap(
                        process_type=  process_type,
                        systems=       [app_id],
                        gap_type=      "skippable_approval",
                        description=   f"Actor '{actor.name}' is null in "
                                       f"{actor.null_rate:.0%} of records — "
                                       f"approval step may be bypassed",
                        suggested_patterns=["enforced_approval"],
                        confidence=    0.9
                    ))

        return gaps
```

**Example output for client with Salesforce + ServiceNow + SAP:**

```
Gap 1 — fragmented_process (confidence: 0.91)
  Invoice approval exists in SAP (steps 1-3: draft → pending → approved)
  and ServiceNow (steps: task created → assigned → resolved)
  with no automatic handoff between them.
  Suggested: [sap_to_snow_handoff] [snow_to_sap_post]

Gap 2 — skippable_approval (confidence: 0.88)
  SAP.SalesInvoice.approved_by is NULL in 22% of records.
  Approval step appears to be bypassable.
  Suggested: [enforced_approval]

Gap 3 — fragmented_process (confidence: 0.84)
  Deal approval in Salesforce (Opportunity Approval Flow)
  is not linked to PO creation in SAP.
  Suggested: [sf_deal_approved → sap_create_po]
```

---

### 1.4 Process Catalogue in Neo4j

Discovery results are stored as a graph in M3, extending the existing Neo4j schema.

```cypher
// Application node
CREATE (app:Application {
  id:       "app-sf-acme",
  type:     "salesforce",
  name:     "Salesforce CRM (Acme)",
  tenant_id:"acme-corp",
  status:   "probed"
})

// Process discovered in the application
CREATE (p:AppProcess {
  id:            "sf-flow-large-deal-approval",
  name:          "Large Deal Approval",
  app_type:      "salesforce",
  process_type:  "approval",
  trigger:       "Opportunity.Amount > 50000",
  steps:         ["submit", "manager_approval", "director_approval", "notify"],
  status_in_source: "active"
})
CREATE (app)-[:HAS_PROCESS]->(p)

// Link to CDM entity
MATCH (e:CDMEntity { event_type: "deal_closed", tenant_id: "acme-corp" })
CREATE (p)-[:INVOLVES]->(e)

// Gap between Salesforce and SAP
CREATE (g:ProcessGap {
  id:          "gap-sf-sap-deal-to-po",
  type:        "fragmented_process",
  description: "Deal approved in Salesforce does not trigger PO in SAP",
  confidence:  0.91
})
CREATE (p)-[:HAS_GAP]->(g)

// Suggested pattern to fill the gap
CREATE (pat:ProcessPattern {
  id:       "sf_deal_approved_to_sap_po",
  name:     "Create SAP PO on Salesforce deal approval",
  action:   "create_record",
  source_event:  "salesforce.opportunity.stage_changed",
  source_condition: "StageName = 'Closed Won' AND Amount > 50000",
  target_app:    "sap",
  target_action: "BAPI_PO_CREATE"
})
CREATE (g)-[:SUGGESTS_PATTERN]->(pat)
```

---

### 1.5 Governance UI — Application Discovery Screen

A new tab in the existing governance UI. Same approval mechanic as CDM mappings and process discovery — data steward reviews and activates.

```
Admin UI M6 — Governance → Application Discovery

APPLICATIONS DETECTED

Application              Type          Processes   Objects   Gaps   Status
────────────────────────────────────────────────────────────────────────────
Salesforce CRM (Acme)    salesforce    8 flows     47 objs   3      [Review]
ServiceNow ITSM          servicenow    12 wflows   89 tables 2      [Review]
SAP ERP                  sap           6 wflows    120 BOs   4      [Review]
AdventureWorks HR        postgresql    (inferred)  39 tables 1      [Review]

[Scan network]   [Add manually]


REVIEW: Salesforce CRM (Acme)

ACTIVE PROCESSES IN SALESFORCE

Name                      Type         Trigger                    Source status
────────────────────────────────────────────────────────────────────────────────
Large Deal Approval       approval     Opportunity.Amount > 50K   ✓ Active
New Lead Assignment       assignment   Lead.Created               ✓ Active
Invoice Reminder          notification Invoice.DueDate - 7 days   ✓ Active
Renewal Opportunity       scheduled    Contract.EndDate - 90d     ✓ Active


GAPS DETECTED

Gap 1 ⚠ fragmented_process (confidence: 91%)
  Large Deal Approval (Salesforce) and Invoice Approval (SAP) are not connected.
  When Salesforce approves a deal > €50K, SAP creates an invoice
  without cross-validation.

  Suggested patterns:
  [✓] Sync deal approval to SAP before invoice creation
  [ ] Notify CFO when both approvals complete
  [ ] Block SAP invoice if Salesforce approval is pending

Gap 2 ⚠ skippable_approval (confidence: 88%)
  SAP Invoice.approved_by is null in 22% of records.

  Suggested patterns:
  [✓] Enforce mandatory approval (block if approved_by is null)
  [ ] Escalate to CFO if no approval after 5 days

[Activate selected patterns]


EXISTING REPORTS IN SALESFORCE (importable)

"Q1 Pipeline by Region"       [Import to dashboard]
"Monthly Closed Deals"        [Import to dashboard]
"Sales by Representative"     [Import to dashboard]
```

---

## Workstream 2 — Process Executor

### 2.1 Design principle

The process executor has zero hardcoded business logic. It executes patterns that are stored in `nexus_system.process_patterns` and were activated by the data steward. Adding a new integration behaviour requires no code deployment — only a new row in the patterns table.

### 2.2 nexus-process-executor (new service)

```python
class ProcessExecutor:
    """
    Consumes all CDC events for the tenant from M1.
    For each event, evaluates active patterns.
    Executes actions for matching patterns.
    No business logic hardcoded here.
    """

    async def handle(self, event: ConnectorEvent):

        # Load active patterns for this tenant + source + event type
        # (in-memory cache, invalidated when pattern is activated/deactivated)
        patterns = await self.pattern_cache.get(
            tenant_id=   event.tenant_id,
            source=      event.source_system,
            event_type=  event.event_type    # "salesforce.opportunity.stage_changed"
        )

        for pattern in patterns:
            if await self.evaluate_condition(pattern.condition, event):
                await self.execute_action(pattern.action, event)
                await self.log_execution(pattern, event)

    async def execute_action(self, action: PatternAction, event: ConnectorEvent):

        match action.type:

            case "create_record":
                # Create a record in the target application
                # via nexus-connector-worker (reuses M1 write capability)
                await self.connector_worker.write(
                    connector_id= action.target_connector,
                    operation=    "create",
                    entity=       action.target_entity,
                    data=         self.map_fields(event.data, action.field_mapping)
                )

            case "update_record":
                # Update a field in the target application
                await self.connector_worker.write(
                    connector_id= action.target_connector,
                    operation=    "update",
                    filter=       action.correlation_key,
                    data=         self.map_fields(event.data, action.field_mapping)
                )

            case "notify":
                # Find the correct person via Neo4j org chart
                # (role-based, not hardcoded to a specific user)
                actor = await self.neo4j.resolve_actor(
                    role=      action.actor_role,  # "cfo" | "direct_manager" | "department_head"
                    context=   event.data,
                    tenant_id= event.tenant_id
                )
                await self.notifier.send(
                    recipient= actor,
                    template=  action.notification_template,
                    data=      event.data
                )

            case "enforce_field":
                # If a required field is null, raise an exception
                # which surfaces in the governance UI
                if not event.data.get(action.required_field):
                    await self.raise_process_exception(
                        event=       event,
                        pattern=     action,
                        description= f"Required field '{action.required_field}' is null"
                    )

            case "sync_status":
                # Propagate a status value from one system to another
                await self.connector_worker.write(
                    connector_id= action.target_connector,
                    operation=    "update",
                    filter=       action.correlation_key,
                    data=         { action.target_field: event.data[action.source_field] }
                )

            case "escalate":
                # Escalate to a higher-level actor if condition is met
                # (e.g. no action taken within SLA window)
                if await self.sla_breached(event, action.sla_hours):
                    actor = await self.neo4j.resolve_actor(
                        role=      action.escalation_role,
                        context=   event.data,
                        tenant_id= event.tenant_id
                    )
                    await self.notifier.send(actor, action.escalation_template, event.data)
```

---

### 2.3 Process patterns table

```sql
CREATE TABLE nexus_system.process_patterns (
    pattern_id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id           VARCHAR(100) NOT NULL,
    app_process_id      VARCHAR(200),     -- link to AppProcess in Neo4j
    gap_id              VARCHAR(200),     -- link to ProcessGap in Neo4j
    name                VARCHAR(300) NOT NULL,

    -- Trigger
    source_app_type     VARCHAR(50)  NOT NULL,
    source_connector_id VARCHAR(100) NOT NULL,
    source_event_type   VARCHAR(200) NOT NULL,
    condition           JSONB,
    -- e.g. {"field": "StageName", "op": "eq", "value": "Closed Won",
    --        "and": {"field": "Amount", "op": "gt", "value": 50000}}

    -- Action
    action_type         VARCHAR(50)  NOT NULL,
    -- create_record | update_record | notify | enforce_field | sync_status | escalate
    action_config       JSONB NOT NULL,
    -- { target_connector, target_entity, field_mapping, actor_role, template, ... }

    -- Lifecycle
    status              VARCHAR(20)  DEFAULT 'active',
    activated_by        VARCHAR(200),
    activated_at        TIMESTAMPTZ,
    deactivated_at      TIMESTAMPTZ,

    -- Observability
    execution_count     INT DEFAULT 0,
    last_executed_at    TIMESTAMPTZ,
    exception_count     INT DEFAULT 0
);
```

**Example row — deal approved in Salesforce → create PO in SAP**

```json
{
  "pattern_id":        "pat-sf-sap-deal-to-po",
  "tenant_id":         "acme-corp",
  "name":              "Create SAP PO on Salesforce deal closure",
  "source_app_type":   "salesforce",
  "source_event_type": "salesforce.opportunity.stage_changed",
  "condition": {
    "field": "StageName", "op": "eq", "value": "Closed Won",
    "and": { "field": "Amount", "op": "gt", "value": 50000 }
  },
  "action_type":   "create_record",
  "action_config": {
    "target_connector": "connector-sap-acme",
    "target_entity":    "PurchaseOrder",
    "target_action":    "BAPI_PO_CREATE",
    "field_mapping": {
      "AccountId":  "CustomerID",
      "Amount":     "NetAmount",
      "CloseDate":  "DeliveryDate"
    }
  }
}
```

---

### 2.4 Process exception handling

When a pattern raises an exception (enforce_field violation, SLA breach, failed write), it surfaces in the governance UI — same mechanic as CDM mapping exceptions.

```
Admin UI M6 — Governance → Process Exceptions

Exception                      Pattern                    Raised       Action
───────────────────────────────────────────────────────────────────────────────
Invoice.approved_by is null    Enforce mandatory approval  2 min ago   [Review]
SAP PO creation failed         Deal → PO sync             1 hour ago   [Retry]
Approval overdue > 5 days      Escalate to CFO            Yesterday    [Escalate]
```

---

## Workstream 3 — Security Hardening

### 3.1 OPA policy expansion

Iteration 2 introduced basic PII checks. Iteration 3 adds process-level policies.

**New policies**

```rego
package nexus.process

# Only platform-admin can activate or deactivate patterns
deny[msg] {
    input.action == "activate_pattern"
    not input.user_role in {"platform-admin", "data-steward"}
    msg := "Pattern activation requires platform-admin or data-steward role"
}

# Process executor cannot write to source systems without explicit approval
deny[msg] {
    input.action == "pattern_write"
    input.pattern.action_type in {"create_record", "update_record"}
    input.pattern.status != "active"
    msg := "Write actions require the pattern to be in active status"
}

# Escalation targets must be resolvable in the org chart
deny[msg] {
    input.action == "escalate"
    not neo4j_actor_exists(input.actor_role, input.context)
    msg := sprintf("Cannot escalate: no actor found for role '%v'", [input.actor_role])
}
```

---

### 3.2 Multi-tenant isolation validation

Full validation suite across all enforcement layers. Must pass before any client deployment.

```python
class MultiTenantIsolationTest:
    """
    Provisions two tenants: acme-corp and beta-corp.
    Executes identical operations for both.
    Verifies at every layer that data never crosses tenants.
    """

    async def test_query_isolation(self):
        # Query from acme-corp context must never return beta-corp data
        result = await self.query_executor.execute(
            query="show all customers",
            tenant_id="acme-corp"
        )
        assert all(r["tenant_id"] == "acme-corp" for r in result.rows)

    async def test_kafka_isolation(self):
        # acme-corp consumer must not receive beta-corp messages
        # Topic naming: {tid}.m1.* ensures isolation at broker level
        acme_consumer = NexusConsumer(tenant_id="acme-corp", ...)
        # Publish to beta-corp topic
        await self.publish("beta-corp.m1.sync_completed", {...})
        # acme consumer must receive nothing
        messages = await acme_consumer.poll(timeout_ms=1000)
        assert len(messages) == 0

    async def test_postgres_rls_isolation(self):
        async with get_tenant_scoped_connection(pool, "acme-corp") as conn:
            rows = await conn.fetch("SELECT * FROM nexus_system.connectors")
            # Must only return acme-corp connectors — RLS enforces this
            assert all(r["tenant_id"] == "acme-corp" for r in rows)

    async def test_neo4j_isolation(self):
        result = self.neo4j.run(
            "MATCH (n) WHERE n.tenant_id = $tid RETURN n",
            tid="acme-corp"
        )
        assert all(r["n"]["tenant_id"] == "acme-corp" for r in result)

    async def test_pinecone_isolation(self):
        # Index is per tenant — no cross-tenant query possible by construction
        # Verify index names enforce tenant scope
        indexes = pinecone.list_indexes()
        assert "acme-corp-entities" in indexes
        assert "beta-corp-entities" in indexes
        # Querying acme-corp index cannot return beta-corp vectors

    async def test_process_pattern_isolation(self):
        # acme-corp pattern must not trigger on beta-corp events
        await self.publish("beta-corp.m1.sync_completed", {
            "source_system": "salesforce",
            "event_type": "opportunity.stage_changed",
            "tenant_id": "beta-corp"
        })
        # acme-corp pattern executor must not fire
        await asyncio.sleep(2)
        assert self.pattern_execution_log.count("acme-corp") == 0
```

---

### 3.3 Audit trail completeness

Every action in the platform is auditable. Iteration 3 ensures the audit trail is complete and queryable.

**Audit events logged**

| Event | Actor | Where logged |
|---|---|---|
| Connector registered | Platform admin | nexus_system.audit_log |
| CDM mapping approved | Data steward | nexus_system.audit_log |
| Application probe executed | System | nexus_system.audit_log |
| Process pattern activated | Data steward | nexus_system.audit_log |
| Pattern executed | System | nexus_system.pattern_executions |
| Query executed | End user | nexus_system.query_log |
| PII access blocked | OPA | nexus_system.security_log |
| Tenant provisioned | Platform admin | nexus_system.audit_log |

**Query log schema**

```sql
CREATE TABLE nexus_system.query_log (
    query_id        UUID PRIMARY KEY,
    tenant_id       VARCHAR(100) NOT NULL,
    user_id         VARCHAR(200) NOT NULL,
    user_role       VARCHAR(100),
    query_nl        TEXT NOT NULL,
    query_plan      JSONB,
    sources_queried JSONB,      -- ["salesforce", "adventureworks"]
    cdm_version     VARCHAR(20),
    output_type     VARCHAR(50),
    execution_ms    INT,
    status          VARCHAR(20), -- success | partial | failed | blocked
    blocked_reason  TEXT,        -- OPA denial message if blocked
    executed_at     TIMESTAMPTZ DEFAULT NOW()
);
```

---

## Workstream 4 — Performance Baselines

### 4.1 Target metrics

All targets must be met on a single-tenant deployment before MVP sign-off.

| Operation | Target | Measurement |
|---|---|---|
| Single-source NL query | < 2s end-to-end | P95, 100 concurrent queries |
| Cross-source NL query (2 sources) | < 4s end-to-end | P95, 50 concurrent queries |
| Neo4j 3-hop traversal | < 100ms | P95 |
| Pinecone ANN top-5 | < 50ms | P95 |
| TimescaleDB 30-day aggregation | < 200ms | P95 |
| Application probe (Salesforce full) | < 60s | Single run |
| Process pattern evaluation | < 50ms per event | P99, Kafka consumer |
| Dashboard component refresh | < 30s per component | Airflow DAG task |

### 4.2 Load test scenario

```python
# Simulates a real client deployment day:
# - 50 concurrent users across CFO, CEO, business roles
# - 10 queries per minute per user
# - 2 active connectors (Salesforce + PostgreSQL)
# - 5 active process patterns
# - Dashboard refresh running in background

async def load_test_scenario():
    users = [
        UserSession(role="cfo",            query_rate=10),
        UserSession(role="ceo",            query_rate=5),
        UserSession(role="business_user",  query_rate=15, count=45),
    ]
    await asyncio.gather(*[u.run(duration_minutes=30) for u in users])
    assert p95_latency < 4000ms
    assert error_rate < 0.01
```

---

## Service Topology — End of Iteration 3

```
Kong (API Gateway)
        │
        ├── nexus-query-api            Iter 2
        ├── nexus-connector-api        Iter 1
        ├── nexus-governance-api       Iter 1 + extended (app discovery screen)
        ├── nexus-application-api      NEW — application register/scan HTTP
        │
Kafka topics
        │
        ├── nexus-query-executor       Iter 2
        ├── nexus-m3-writer            Iter 2
        ├── nexus-app-discoverer       NEW — probes applications, writes to Neo4j
        ├── nexus-process-executor     NEW — pattern evaluation + action execution
        ├── nexus-connector-worker     Iter 1 (extended for write operations)
        ├── nexus-cdm-mapper           Iter 1
        ├── nexus-schema-profiler      Iter 1
        ├── nexus-governance-processor Iter 1
        │
Airflow DAGs
        ├── connector_onboarding       Iter 1
        ├── schema_drift_detection     Iter 1
        ├── dashboard_refresh          Iter 2
        ├── application_probe_schedule NEW — weekly re-probe of all applications
        └── process_sla_monitor        NEW — detect SLA breaches, trigger escalations
```

---

## Acceptance Criteria

### Application Discovery

- [ ] Salesforce probe returns all active Flows and Approval Processes
- [ ] ServiceNow probe returns all active Workflows and Business Rules
- [ ] Gap detection identifies at least one cross-system gap when both connectors are active
- [ ] Neo4j contains Application, AppProcess, and ProcessGap nodes after probe completes
- [ ] Governance UI displays discovered applications with gap details
- [ ] Data steward can activate a pattern from the governance UI without writing code
- [ ] Existing Salesforce reports are importable as NEXUS dashboard components

### Process Executor

- [ ] Pattern `create_record` executes successfully against a live target connector
- [ ] Pattern `notify` resolves actor via Neo4j org chart and sends notification
- [ ] Pattern `enforce_field` raises a process exception surfaced in governance UI
- [ ] Pattern execution is logged in `nexus_system.pattern_executions`
- [ ] Deactivated pattern stops executing within one cache refresh cycle (< 60s)
- [ ] Pattern from tenant A never executes on event from tenant B

### Security

- [ ] OPA blocks PII query for non-authorised role with explicit error message
- [ ] Multi-tenant isolation test suite passes all 6 test cases
- [ ] Audit log contains entries for every query, pattern activation, and probe execution
- [ ] Pattern write actions require `status = 'active'` — OPA blocks inactive patterns

### Performance

- [ ] Single-source query P95 < 2s under 100 concurrent users
- [ ] Cross-source query P95 < 4s under 50 concurrent users
- [ ] Neo4j traversal P95 < 100ms
- [ ] Load test scenario (30 min, 50 users) completes with error rate < 1%

---

## Timeline — 6 Weeks

| Weeks | Focus | Owner |
|---|---|---|
| 1–2 | Application Registry API + Salesforce/ServiceNow probes | Data Intelligence |
| 2–3 | SAP/Odoo probes + CrossSystemGapDetector + Neo4j Process Catalogue | Data Intelligence |
| 3–4 | nexus-process-executor + pattern table + 5 action types | AI & Knowledge |
| 4–5 | Governance UI — Application Discovery screen + pattern activation | Product / M6 |
| 5–6 | Security hardening + multi-tenant validation + load tests | All teams |

---

## MVP Sign-off Checklist

The following must all be true before the MVP is considered ready for first client deployment.

**Functional**
- [ ] Admin registers a connector → full M1 pipeline completes in < 5 min
- [ ] Data steward approves CDM mappings → connector becomes queryable
- [ ] User asks NL question → structured answer (text + chart) returned in < 4s
- [ ] Chart saved to dashboard → visible on CFO/CEO dashboard, refreshes daily
- [ ] Application probe discovers at least one process in Salesforce or ServiceNow
- [ ] At least one process pattern is active and has executed at least once

**Security**
- [ ] Multi-tenant isolation validated at all 6 enforcement layers
- [ ] PII columns blocked for unauthorised roles
- [ ] All queries, pattern activations, and probe executions in audit log

**Operational**
- [ ] All 11 services healthy in Kubernetes with readiness probes
- [ ] Grafana dashboard shows per-tenant query latency, Kafka lag, pattern execution rate
- [ ] `onboard_tenant.py` tested with a net-new tenant — full stack operational in < 10 min
- [ ] Load test scenario passes performance targets
