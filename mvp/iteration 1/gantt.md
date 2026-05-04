# NEXUS — Iteration 1 Gantt Chart
**Mentis Consulting · February 2026 · Confidential**

---

```mermaid
%%{init: {'gantt': {'useWidth': 1400}}}%%
gantt
    title NEXUS Iteration 1 — 14 Week Delivery Plan
    dateFormat  YYYY-MM-DD
    axisFormat  W%W

    section M5 · Platform
    Okta + Kong JWT integration          :crit, p0a, 2026-03-02, 3d
    Per-tenant Kafka topic provisioning  :crit, p0b, after p0a, 2d
    ArgoCD application definitions       :p0c, after p0a, 3d
    Grafana tenant dashboards            :p0d, after p0b, 3d

    section Core · Tech Lead
    nexus_core library + NexusMessage    :crit, c1, 2026-03-02, 5d
    Okta OIDC config + OIDC_ISSUER_URL   :crit, c2, after c1, 3d
    Identity mapping table (seed)        :c3, after c2, 2d
    provisioning.py script             :c4, after c2, 2d
    Inter-team contract reviews          :c5, 2026-03-16, 42d

    section M1 · Data Intelligence
    PostgreSQL Debezium connector        :m1a, 2026-03-16, 5d
    MySQL Debezium connector             :m1b, after m1a, 4d
    SQL Server Debezium connector        :m1c, after m1a, 4d
    Salesforce Airbyte connector         :m1d, 2026-03-16, 6d
    ServiceNow Airbyte connector         :m1e, after m1d, 4d
    Odoo Airbyte connector               :m1f, after m1d, 4d
    CDM Mapper                           :m1g, 2026-04-06, 7d
    AI Store Router                      :m1h, after m1g, 5d
    Structural Sub-Cycle Trigger         :m1i, after m1g, 4d

    section M3 · AI & Knowledge
    Vector Store Writer — unit tests     :m3a, 2026-03-23, 10d
    Graph Store Writer — unit tests      :m3b, 2026-03-23, 10d
    TimeSeries Writer — unit tests       :m3c, 2026-03-23, 10d
    M3 integration (real M1 data)        :crit, m3d, 2026-04-20, 7d

    section M2 · AI & Knowledge
    Structural Agent (LLM schema interp) :m2a, 2026-04-06, 10d
    Executive RHMA Agent                 :m2b, 2026-04-13, 14d

    section M4 · Product
    CDM Governance Queue + FastAPI       :m4a, 2026-03-04, 5d
    Mapping Exception Queue              :m4b, after m4a, 4d
    Temporal workflow state machine      :m4c, after m4b, 3d

    section M6 · Product
    Okta login flow (Next.js)            :crit, m6a, 2026-03-11, 4d
    CDM Governance Console               :m6b, after m6a, 6d
    AI Chat Interface                    :m6c, 2026-04-27, 6d
    Pipeline Health Dashboard            :m6d, after m6c, 4d

    section Integration & Testing
    Happy path end-to-end test           :crit, t1, 2026-05-04, 5d
    Structural cycle test                :t2, after t1, 3d
    Multi-tenant isolation test          :crit, t3, after t1, 4d
    Okta identity test                   :t4, after t1, 3d
    Performance baseline                 :t5, after t3, 4d
    Iteration 1 sign-off                 :milestone, crit, 2026-05-25, 0d
```

---

## Phase Summary

| Phase | Scope | Owner | Weeks | Gate |
|---|---|---|---|---|
| **Phase 0** | M5 completion — Okta+Kong, Kafka topics, ArgoCD, Grafana | Platform + Tech Lead | W1–2 | Hard gate — nothing starts until done |
| **Phase 1** | nexus_core, OIDC config, identity mapping table | Tech Lead | W1–2 | Hard gate — all teams depend on library |
| **Phase 2** | M1 connectors — all 6 real sources | Data Intelligence | W3–6 | Real data flowing on Kafka |
| **Phase 3** | M1 CDM Mapper, AI Store Router, Structural Trigger | Data Intelligence | W5–8 | CDM entities flowing to M3 |
| **Phase 4** | M3 writers — Vector, Graph, TimeSeries | AI & Knowledge | W4–8 | Knowledge stores populated |
| **Phase 5** | M2 Structural Agent + Executive RHMA Agent | AI & Knowledge | W6–11 | AI queries working on real data |
| **Phase 6** | M4 Governance Queue + Exception Queue | Product | W1–2 | Human review flow active |
| **Phase 7** | M6 — Okta login, Governance Console, Chat, Health | Product | W2–9 | UI live with real Okta auth |
| **Phase 8** | End-to-end integration + multi-tenant isolation tests | All teams | W12–14 | ✅ Iteration 1 complete |

---

## Critical Path

The following tasks are on the critical path — any delay cascades to the final sign-off date:

1. **Okta + Kong JWT integration** (Platform) — nothing authenticates without this
2. **nexus_core + OIDC_ISSUER_URL** (Tech Lead) — all application code depends on this
3. **M1 connectors** (Data Intelligence) — M3 and M2 cannot integrate without real data
4. **M3 integration with real M1 data** (AI & Knowledge) — M2 Executive Agent needs populated stores
5. **Okta login flow in M6** (Product) — all UI features require authenticated sessions
6. **Happy path + multi-tenant isolation tests** (All) — sign-off blocked until both pass

---

## Key Dependencies

```
Okta dev org registered (Tech Lead, Day 1)
    ↓
nexus_core OIDC config  ──────────────────────────→  M6 Okta login
    ↓
Kong JWT plugin (Platform)
    ↓
M1 Connectors (all 6)
    ↓
M1 CDM Mapper → M1 AI Store Router
    ↓
M3 Writers integration ───────────────────────────→  M2 Executive Agent
    ↓
End-to-end happy path test
    ↓
✅ Iteration 1 sign-off  (Week 14)
```

---

## OIDC Configuration Note

NEXUS uses a single environment variable to support any OIDC-compliant IdP.
Swapping from the dev Okta org to a client's corporate IdP is a one-line config change — no code change required.

```
# Development / Demo (Okta developer org)
OIDC_ISSUER_URL=https://dev-xxxxx.okta.com

# Enterprise Client A (their Okta)
OIDC_ISSUER_URL=https://acme.okta.com

# Enterprise Client B (Azure AD)
OIDC_ISSUER_URL=https://login.microsoftonline.com/{tenant}/v2.0
```

---

*NEXUS Iteration 1 Gantt · Mentis Consulting · February 2026 · Confidential*