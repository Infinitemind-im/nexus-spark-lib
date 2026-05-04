# NEXUS — Iteration 2 · Visual Outputs
**Workstream 3 · Detailed Specification: Result Rendering, Dashboards, Reports**
Mentis Consulting · Version 0.2 · March 2026 · Confidential

> **Review v0.2 — Architecture corrections applied**
> This version corrects issues identified in the Architecture Review (March 2026).
> Critical changes: Airflow `get_components_due_refresh` predicate fixed — hourly components
> now require `last_refreshed_at < NOW() - INTERVAL '1 hour'` (Issue 5);
> ReportBuilder section writes parallelised with `asyncio.gather` (~2s vs ~10s, Issue 8);
> Chart example values aligned with M6 UX mockup — €11.8M total across both sources (Issue 12).

---

## Overview

Visual Outputs is the rendering layer of the Query Engine. After the Result Merger produces a `MergedResult`, the `ResultRenderer` converts it into a `RenderedOutput` — a typed, self-describing JSON structure that M6 consumes directly without further transformation.

Rendering is a pure function: same `MergedResult` + same `CDMQueryPlan` always produces the same `RenderedOutput`. No LLM calls are made during rendering for charts and tables. LLM calls are made only for `REPORT` type output (section writing + structure planning).

---

## Functional Requirements

| ID | Requirement | Priority |
|---|---|---|
| VO-FR-01 | Output type selected by planner; user can override via `output_preference` | Must |
| VO-FR-02 | Bar, line, and pie charts rendered as Recharts-compatible JSON consumed directly by M6 | Must |
| VO-FR-03 | Table output includes column type metadata for M6 to apply formatting (currency, badge, date) | Must |
| VO-FR-04 | All outputs include `sources_queried`, `cdm_version`, and `partial` flags | Must |
| VO-FR-05 | Tables exportable as .xlsx, .csv, and .pdf via `/export` endpoint | Must |
| VO-FR-06 | Charts exportable as .xlsx and .csv (data only, not image) | Must |
| VO-FR-07 | Charts saveable as persistent dashboard components via `/save-dashboard` | Must |
| VO-FR-08 | Dashboard components auto-refresh on Airflow DAG schedule (hourly or daily) | Must |
| VO-FR-09 | Reports generated as .docx with persona-specific section structure | Must |
| VO-FR-10 | Report preview (first 500 chars) included in the `RenderedOutput` | Must |
| VO-FR-11 | Reports stored in MinIO and served via a signed download URL (TTL 15 min) | Must |
| VO-FR-12 | Text answers formatted in Markdown for M6 rendering | Should |
| VO-FR-13 | Output summary always present — one-sentence NL description of the result | Must |

---

## 1. RenderedOutput Schema

This is the canonical output format delivered in the `result` WebSocket event and stored in `nexus_system.query_sessions.result`.

```python
@dataclass
class RenderedOutput:
    output_type:      OutputType         # TEXT | TABLE | BAR_CHART | LINE_CHART | PIE_CHART | REPORT
    title:            str                # Auto-generated from query + result
    summary:          str                # One-sentence NL summary of the result
    sources_queried:  list[str]          # ["salesforce", "postgresql/adventureworks"]
    sources_failed:   list[SourceFailure]# Empty if all sources succeeded
    cdm_version:      str                # "1.3"
    partial:          bool               # True if some sources failed
    can_save_dashboard: bool             # True for chart types
    can_export:       list[str]          # ["xlsx", "csv"] | ["xlsx", "csv", "pdf"] | []

    # Mutually exclusive fields — only one is populated based on output_type:
    text_content:     str | None         # Markdown text (for TEXT type)
    table:            TableOutput | None # (for TABLE type)
    chart_spec:       ChartSpec | None   # (for BAR_CHART | LINE_CHART | PIE_CHART)
    report:           ReportOutput | None# (for REPORT type)

@dataclass
class OutputType(str, Enum):
    TEXT       = "text"
    TABLE      = "table"
    BAR_CHART  = "bar_chart"
    LINE_CHART = "line_chart"
    PIE_CHART  = "pie_chart"
    REPORT     = "report"
```

---

## 2. Output Type Selection Logic

The planner suggests an `output_type`. The renderer applies persona overrides before rendering:

```python
PERSONA_OUTPUT_OVERRIDE: dict[str, dict[str, OutputType]] = {
    # role → intent → preferred output_type
    "cfo": {
        "aggregation": OutputType.BAR_CHART,
        "trend":       OutputType.LINE_CHART,
        "lookup":      OutputType.TABLE,
    },
    "ceo": {
        "aggregation": OutputType.BAR_CHART,
        "trend":       OutputType.LINE_CHART,
        "lookup":      OutputType.TABLE,
    },
    "data_steward": {
        "aggregation": OutputType.TABLE,
        "trend":       OutputType.TABLE,
        "lookup":      OutputType.TABLE,
    },
    "business_user": {
        "aggregation": OutputType.TEXT,
        "trend":       OutputType.LINE_CHART,
        "lookup":      OutputType.TABLE,
    },
}

def resolve_output_type(
    plan:              CDMQueryPlan,
    output_preference: str,         # "auto" | explicit type
    user_role:         str,
) -> OutputType:

    # Explicit preference always wins
    if output_preference != "auto":
        return OutputType(output_preference)

    # Persona override next
    persona_map = PERSONA_OUTPUT_OVERRIDE.get(user_role, {})
    if plan.intent in persona_map:
        return persona_map[plan.intent]

    # Planner suggestion as fallback
    return plan.output_type
```

---

## 3. Text Output

For simple factual questions and out-of-context summaries.

```python
@dataclass
class TextOutput:
    """No separate class — text_content is a Markdown string in RenderedOutput"""
    pass

# Example rendered output (Issue 12 — values aligned with M6 UX mockup):
{
  "output_type":       "text",
  "title":             "Deals closed in 2025",
  "summary":           "287 deals were closed in 2025, totalling €11.8M.",
  "text_content":      "## Deals Closed in 2025\n\n**287 deals** were closed ...",
  "sources_queried":   ["salesforce", "postgresql/adventureworks"],
  "sources_failed":    [],
  "cdm_version":       "1.3",
  "partial":           false,
  "can_save_dashboard": false,
  "can_export":        []
}
```

Text answers are synthesised by a lightweight LLM call (Claude Haiku) from the `MergedResult` scalars. This is the only case where a non-report, non-planner LLM call is made.

```python
async def render_text(merged: MergedResult, plan: CDMQueryPlan) -> str:
    prompt = f"""
    User asked: "{plan.original_query}"
    Result data: {json.dumps(merged.scalars)}
    Sources: {', '.join(merged.sources_queried)}
    Partial result: {merged.partial}

    Write a concise Markdown answer (2-4 sentences) that directly answers the question.
    If result is partial, note which sources were unavailable.
    Use bold for key numbers. Do not make up data not present in the result.
    """
    return await self.llm.complete(prompt, model="claude-haiku-4-5", max_tokens=300)
```

---

## 4. Table Output

For lookup, list, and inventory queries.

```python
@dataclass
class ColumnSpec:
    key:    str              # Internal data key
    label:  str              # Display label
    type:   ColumnType       # string | integer | decimal | currency | date | datetime | badge | boolean

@dataclass
class TableOutput:
    columns:    list[ColumnSpec]
    rows:       list[list]          # Row-major, aligned to columns order
    row_count:  int
    truncated:  bool                # True if > 10,000 rows returned from source
```

**Column type inference:**

```python
def infer_column_type(cdm_field: CDMField) -> ColumnType:
    match cdm_field.cdm_type:
        case "DECIMAL" if cdm_field.is_currency:    return ColumnType.CURRENCY
        case "DECIMAL":                             return ColumnType.DECIMAL
        case "INTEGER":                             return ColumnType.INTEGER
        case "TEXT" if cdm_field.is_status:         return ColumnType.BADGE
        case "TEXT":                                return ColumnType.STRING
        case "DATE":                                return ColumnType.DATE
        case "DATETIME":                            return ColumnType.DATETIME
        case "BOOLEAN":                             return ColumnType.BOOLEAN
        case _:                                     return ColumnType.STRING
```

**Example table output:**

```json
{
  "output_type": "table",
  "title":       "Overdue invoices — March 2026",
  "summary":     "2 invoices are overdue by more than 18 days, totalling €81,000.",
  "table": {
    "columns": [
      { "key": "customer",     "label": "Customer",     "type": "string"   },
      { "key": "invoice_ref",  "label": "Invoice",      "type": "string"   },
      { "key": "amount",       "label": "Amount",       "type": "currency" },
      { "key": "days_overdue", "label": "Days overdue", "type": "integer"  },
      { "key": "status",       "label": "Status",       "type": "badge"    }
    ],
    "rows": [
      ["Northwind Traders", "INV-2026-0798", 62800.00, 18, "Overdue 30+"],
      ["Trey Research DE",  "INV-2026-0744", 18200.00, 33, "Overdue 30+"]
    ],
    "row_count": 2,
    "truncated":  false
  },
  "sources_queried": ["postgresql/adventureworks"],
  "sources_failed":  [],
  "cdm_version":     "1.3",
  "partial":         false,
  "can_save_dashboard": false,
  "can_export":      ["xlsx", "csv", "pdf"]
}
```

---

## 5. Chart Outputs

### 5.1 ChartSpec Format (Recharts-compatible)

M6 consumes `chart_spec` directly as Recharts component props — no transformation needed in the frontend.

```python
@dataclass
class BarDef:
    dataKey: str
    name:    str
    fill:    str   # Hex colour — always from NEXUS palette

@dataclass
class LineDef:
    dataKey: str
    name:    str
    stroke:  str
    dot:     bool = True

@dataclass
class ChartSpec:
    type:     str            # "BarChart" | "LineChart" | "PieChart"
    data:     list[dict]     # Row data
    xAxis:    str | None     # Field name for x-axis (BarChart, LineChart)
    bars:     list[BarDef]   # (BarChart only)
    lines:    list[LineDef]  # (LineChart only)
    pie:      PieDef | None  # (PieChart only)
```

**NEXUS colour palette (for consistent branding across charts):**

```python
NEXUS_PALETTE = [
    "#7c3aed",  # Primary purple
    "#2dd4bf",  # Teal
    "#f59e0b",  # Amber
    "#ef4444",  # Red
    "#10b981",  # Green
    "#3b82f6",  # Blue
    "#8b5cf6",  # Violet
    "#f97316",  # Orange
]
```

### 5.2 Bar Chart

Triggered by: aggregation intent with `group_by`, keywords "by", "per", "breakdown".

```python
def render_bar_chart(merged: MergedResult, plan: CDMQueryPlan) -> ChartSpec:
    # Build data rows from breakdown
    data = [
        {
            plan.group_by[0] if plan.group_by else "source": row.get("source", row.get(plan.group_by[0])),
            **{
                agg["field"] if "field" in agg else agg["func"].lower(): row.get(agg["field"] or agg["func"].lower(), 0)
                for agg in plan.aggregations
            }
        }
        for row in (merged.breakdown if merged.breakdown else [merged.scalars])
    ]

    bars = [
        BarDef(
            dataKey= agg.get("field") or agg["func"].lower(),
            name=    self.friendly_label(agg),
            fill=    NEXUS_PALETTE[i % len(NEXUS_PALETTE)]
        )
        for i, agg in enumerate(plan.aggregations)
    ]

    return ChartSpec(
        type=  "BarChart",
        data=  data,
        xAxis= plan.group_by[0] if plan.group_by else "source",
        bars=  bars,
        lines= [],
        pie=   None,
    )
```

**Example bar chart output (Issue 12 — values aligned with M6 UX mockup, total €11.8M):**

```json
{
  "output_type": "bar_chart",
  "title":       "Deals closed in 2025 by source",
  "summary":     "287 deals totalling €11.8M across 2 sources.",
  "chart_spec": {
    "type":  "BarChart",
    "data":  [
      { "name": "Salesforce",     "count": 194, "total_value": 7800000 },
      { "name": "AdventureWorks", "count": 93,  "total_value": 4000000 }
    ],
    "xAxis": "name",
    "bars": [
      { "dataKey": "count",       "name": "Number of deals", "fill": "#7c3aed" },
      { "dataKey": "total_value", "name": "Total value (€)", "fill": "#2dd4bf" }
    ]
  },
  "sources_queried":    ["salesforce", "postgresql/adventureworks"],
  "cdm_version":        "1.3",
  "partial":            false,
  "can_save_dashboard": true,
  "can_export":         ["xlsx", "csv"]
}
```

### 5.3 Line Chart

Triggered by: trend intent, keywords "over time", "trend", "evolution", "per month/quarter/year".

```python
def render_line_chart(merged: MergedResult, plan: CDMQueryPlan) -> ChartSpec:
    # time_series rows from TimescaleDB: [{"bucket": "2025-01", "value": 420000}, ...]
    data = [
        {
            "bucket": row["bucket"].strftime("%b %Y") if isinstance(row["bucket"], datetime) else row["bucket"],
            "value":  float(row.get("metric_value", 0)),
        }
        for row in merged.time_series
    ]

    return ChartSpec(
        type=  "LineChart",
        data=  data,
        xAxis= "bucket",
        bars=  [],
        lines= [LineDef(dataKey="value", name=self.friendly_label_trend(plan), stroke="#7c3aed")],
        pie=   None,
    )
```

### 5.4 Pie Chart

Triggered by: aggregation intent with `group_by`, keywords "distribution", "share", "proportion", "breakdown by".

```python
@dataclass
class PieDef:
    dataKey:    str     # Field name for pie slice value
    nameKey:    str     # Field name for slice label
    innerRadius: int    # 0 for pie, 60 for donut
    outerRadius: int
    cells:      list[dict]  # [{"name": "Belgium", "fill": "#7c3aed"}, ...]
```

---

## 6. Export Service

### 6.1 Export Architecture

Export requests hit `GET /query/{session_id}/export?format=xlsx|csv|pdf`. The `nexus-query-api` service handles the export inline (synchronous, no Kafka) using the result already stored in `nexus_system.query_sessions.result`.

```python
@router.get("/query/{session_id}/export")
async def export_result(
    session_id:  str,
    format:      str,               # "xlsx" | "csv" | "pdf"
    x_tenant_id: str = Header(...),
    x_user_id:   str = Header(...),
) -> StreamingResponse:

    session = await self.pg.get_session(session_id, x_tenant_id)
    if not session:
        raise HTTPException(404, "Session not found")
    if session.status != "completed":
        raise HTTPException(409, "Session not yet completed")

    result = RenderedOutput(**session.result)

    if format == "xlsx":
        content = await self.export_xlsx(result)
        media_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        filename   = f"nexus-export-{session_id}.xlsx"

    elif format == "csv":
        content = self.export_csv(result)
        media_type = "text/csv"
        filename   = f"nexus-export-{session_id}.csv"

    elif format == "pdf":
        content = await self.export_pdf(result)
        media_type = "application/pdf"
        filename   = f"nexus-export-{session_id}.pdf"

    else:
        raise HTTPException(400, f"Unsupported format: {format}")

    return StreamingResponse(
        io.BytesIO(content),
        media_type=  media_type,
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"',
            "X-Row-Count":         str(result.table.row_count if result.table else 0),
        }
    )
```

### 6.2 XLSX Export

Uses the existing `xlsx` skill infrastructure (openpyxl-based). Column widths are auto-fitted. Currency columns receive accounting format. Header row is bold with NEXUS purple background.

```python
async def export_xlsx(self, result: RenderedOutput) -> bytes:
    wb = openpyxl.Workbook()
    ws = wb.active

    if result.output_type == OutputType.TABLE and result.table:
        # Header row
        header_fill = PatternFill(fgColor="7c3aed", fill_type="solid")
        for col_idx, col_spec in enumerate(result.table.columns, 1):
            cell = ws.cell(row=1, column=col_idx, value=col_spec.label)
            cell.font  = Font(bold=True, color="FFFFFF")
            cell.fill  = header_fill

        # Data rows
        for row_idx, row in enumerate(result.table.rows, 2):
            for col_idx, (value, col_spec) in enumerate(zip(row, result.table.columns), 1):
                cell = ws.cell(row=row_idx, column=col_idx, value=value)
                if col_spec.type == ColumnType.CURRENCY:
                    cell.number_format = '#,##0.00 €'
                elif col_spec.type == ColumnType.DATE:
                    cell.number_format = 'YYYY-MM-DD'

        # Auto-fit columns
        for col in ws.columns:
            max_len = max(len(str(cell.value or "")) for cell in col)
            ws.column_dimensions[col[0].column_letter].width = min(max_len + 2, 50)

    elif result.output_type in (OutputType.BAR_CHART, OutputType.LINE_CHART, OutputType.PIE_CHART):
        # Export chart data (not image)
        ws.append(list(result.chart_spec.data[0].keys()))
        for row in result.chart_spec.data:
            ws.append(list(row.values()))

    buffer = io.BytesIO()
    wb.save(buffer)
    return buffer.getvalue()
```

### 6.3 CSV Export

```python
def export_csv(self, result: RenderedOutput) -> bytes:
    output = io.StringIO()
    writer = csv.writer(output)

    if result.output_type == OutputType.TABLE and result.table:
        writer.writerow([col.label for col in result.table.columns])
        writer.writerows(result.table.rows)
    elif result.chart_spec:
        writer.writerow(list(result.chart_spec.data[0].keys()))
        for row in result.chart_spec.data:
            writer.writerow(list(row.values()))

    return output.getvalue().encode("utf-8-sig")  # UTF-8 BOM for Excel compatibility
```

### 6.4 PDF Export (Tables and Text)

For tables, a simple formatted PDF is generated using `reportlab`. For text results, the Markdown is rendered as plain text in a styled PDF.

[CLARIFY: Should PDF export use `reportlab` directly or invoke the `pdf` skill? The `pdf` skill is designed for document creation, not tabular data. Recommend `reportlab` for table exports and the `pdf` skill for text/report exports.]

---

## 7. Dashboard Components

### 7.1 Data Model

```sql
CREATE TABLE IF NOT EXISTS nexus_system.dashboard_components (
    component_id      UUID            PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id         VARCHAR(100)    NOT NULL,
    created_by        VARCHAR(200)    NOT NULL,
    title             VARCHAR(300)    NOT NULL,
    chart_spec        JSONB           NOT NULL,
    output_type       VARCHAR(20)     NOT NULL,
    query_nl          TEXT            NOT NULL,     -- Original NL question
    query_plan        JSONB           NOT NULL,     -- Full CDMQueryPlan for refresh
    refresh_schedule  VARCHAR(20),                  -- "hourly" | "daily" | null
    last_refreshed_at TIMESTAMPTZ,
    cdm_version       VARCHAR(20)     NOT NULL,
    source_session_id TEXT            NOT NULL,     -- Session that created the component
    created_at        TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    updated_at        TIMESTAMPTZ
);

-- Row-level security
ALTER TABLE nexus_system.dashboard_components ENABLE ROW LEVEL SECURITY;
CREATE POLICY dc_tenant_isolation ON nexus_system.dashboard_components
    FOR ALL TO nexus_app
    USING (tenant_id = current_setting('nexus.current_tenant_id', true));

-- Indexes
CREATE INDEX dc_tenant_created_idx
    ON nexus_system.dashboard_components (tenant_id, created_at DESC);
CREATE INDEX dc_refresh_schedule_idx
    ON nexus_system.dashboard_components (refresh_schedule, last_refreshed_at)
    WHERE refresh_schedule IS NOT NULL;
```

### 7.2 Save Flow (POST /query/{session_id}/save-dashboard)

```python
async def save_dashboard_component(
    session_id: str,
    body:       SaveDashboardRequest,
    tenant_id:  str,
    user_id:    str,
) -> SaveDashboardResponse:

    session = await self.pg.get_session(session_id, tenant_id)
    result  = RenderedOutput(**session.result)
    plan    = CDMQueryPlan(**session.query_plan)

    if result.output_type not in (OutputType.BAR_CHART, OutputType.LINE_CHART, OutputType.PIE_CHART, OutputType.TABLE):
        raise HTTPException(400, "Only chart and table outputs can be saved as dashboard components")

    # Prevent duplicate saves
    existing = await self.pg.find_component_by_session(session_id, tenant_id)
    if existing:
        raise HTTPException(409, "Component already saved from this session")

    component_id = await self.pg.insert_dashboard_component(
        tenant_id=        tenant_id,
        created_by=       user_id,
        title=            body.title or result.title,
        chart_spec=       json.dumps(dataclasses.asdict(result.chart_spec or {})),
        output_type=      result.output_type.value,
        query_nl=         plan.original_query,
        query_plan=       json.dumps(dataclasses.asdict(plan)),
        refresh_schedule= body.refresh_schedule,
        cdm_version=      result.cdm_version,
        source_session_id=session_id,
    )

    return SaveDashboardResponse(
        component_id= str(component_id),
        dashboard_url=f"https://nexus.internal/dashboard?component={component_id}",
    )
```

### 7.3 Dashboard Refresh — Airflow DAG

The `dashboard_refresh` DAG runs hourly and re-executes stored queries for components with a `refresh_schedule`.

```python
# dags/dashboard_refresh.py

from airflow.decorators import dag, task
from datetime import datetime
import httpx, json

NEXUS_QUERY_API_URL = "https://api.nexus.internal"
NEXUS_SERVICE_TOKEN = Variable.get("nexus_service_token")  # Internal service JWT

@dag(
    dag_id=          "dashboard_refresh",
    schedule=        "@hourly",
    start_date=      datetime(2026, 3, 1),
    catchup=         False,
    max_active_runs= 1,
    tags=            ["nexus", "iteration2", "dashboard"],
)
def dashboard_refresh():

    @task
    def get_components_due_refresh() -> list[dict]:
        """
        Returns components due for refresh based on their schedule and last refresh time.

        Issue 5 correction — Logically inverted predicate fix:
          Original code used `last_refreshed_at > threshold` which returned components
          that were RECENTLY refreshed (wrong). The correct predicate is
          `last_refreshed_at < threshold` which returns components that have NOT been
          refreshed within the schedule interval (or have never been refreshed).

        Refresh criteria:
          - refresh_schedule = "hourly" AND (never refreshed OR refreshed > 1 hour ago)
          - refresh_schedule = "daily"  AND (never refreshed OR refreshed > 23 hours ago)

        Both cases use `last_refreshed_at < NOW() - INTERVAL` (strict less-than),
        not `>`. This prevents refreshing a component that was just refreshed in the
        previous DAG run (e.g. on retry or overlapping run).
        """
        from nexus_core.db import get_system_connection
        with get_system_connection() as conn:
            return conn.execute("""
                SELECT
                    component_id::text,
                    tenant_id,
                    query_nl,
                    query_plan,
                    refresh_schedule,
                    created_by
                FROM nexus_system.dashboard_components
                WHERE refresh_schedule IS NOT NULL
                  AND (
                    (
                        refresh_schedule = 'hourly'
                        AND (last_refreshed_at IS NULL OR last_refreshed_at < NOW() - INTERVAL '1 hour')
                    )
                    OR (
                        refresh_schedule = 'daily'
                        AND (last_refreshed_at IS NULL OR last_refreshed_at < NOW() - INTERVAL '23 hours')
                    )
                  )
            """).fetchall()

    @task
    def refresh_component(component: dict) -> dict:
        """
        Re-executes the stored NL query via nexus-query-api and updates the component.
        Uses a service JWT (not a user JWT) — OPA must allow service role for refresh.
        """
        plan = json.loads(component["query_plan"])

        # Submit query
        submit_resp = httpx.post(
            f"{NEXUS_QUERY_API_URL}/query",
            headers={
                "Authorization":  f"Bearer {NEXUS_SERVICE_TOKEN}",
                "X-Tenant-ID":    component["tenant_id"],
                "X-User-ID":      component["created_by"],
                "X-User-Role":    "dashboard-refresh",
            },
            json={
                "query":             component["query_nl"],
                "output_preference": "auto",
                "context":           {"refresh": True},
            },
            timeout=5.0,
        )
        session_id = submit_resp.json()["session_id"]

        # Poll for result (max 60 attempts × 2s = 2 minutes)
        for attempt in range(60):
            time.sleep(2)
            status_resp = httpx.get(
                f"{NEXUS_QUERY_API_URL}/query/{session_id}",
                headers={
                    "Authorization": f"Bearer {NEXUS_SERVICE_TOKEN}",
                    "X-Tenant-ID":   component["tenant_id"],
                },
                timeout=5.0,
            )
            status_data = status_resp.json()

            if status_data["status"] == "completed":
                new_chart_spec = status_data["result"].get("chart_spec")
                break
            elif status_data["status"] in ("failed", "timeout"):
                raise Exception(f"Dashboard refresh query failed: {status_data}")
        else:
            raise Exception(f"Dashboard refresh timed out for component {component['component_id']}")

        # Update component with fresh chart_spec
        from nexus_core.db import get_system_connection
        with get_system_connection() as conn:
            conn.execute("""
                UPDATE nexus_system.dashboard_components
                SET chart_spec        = $1,
                    last_refreshed_at = NOW(),
                    updated_at        = NOW()
                WHERE component_id = $2
            """, json.dumps(new_chart_spec), component["component_id"])

        return {"component_id": component["component_id"], "status": "refreshed"}

    components = get_components_due_refresh()
    refresh_component.expand(component=components)

dashboard_refresh_dag = dashboard_refresh()
```

**Service JWT for DAG:** The Airflow DAG uses an internal service token rather than a user JWT. The `X-User-Role` is set to `"dashboard-refresh"`. OPA must include a rule allowing the `dashboard-refresh` role to execute queries:

```rego
# OPA addition — allow dashboard-refresh service role
allow {
    input.user_role == "dashboard-refresh"
    not input.pii_columns[_]   # No PII access from refresh service
}
```

---

## 8. Report Builder

### 8.1 Pipeline Overview

Report generation is a multi-step LLM pipeline invoked when `output_type == REPORT`.

```
MergedResult
    │
    ├── Step 1: plan_report_structure (LLM)
    │       → ReportStructure: title + ordered sections list
    │
    ├── Step 2: write_section (LLM × N sections)
    │       → section.text (Markdown)
    │       + section.chart_spec (if data available for section)
    │
    ├── Step 3: build_docx (docx skill / python-docx)
    │       → .docx bytes
    │
    └── Step 4: store_report (MinIO)
            → signed download URL (TTL 15min)
```

### 8.2 Persona-Aware Report Structure

```python
REPORT_SECTION_TEMPLATES: dict[str, list[ReportSectionTemplate]] = {
    "cfo": [
        ReportSectionTemplate("Executive Financial Summary",    data_fields=["total_value", "deal_count"],  chart_type="BAR_CHART"),
        ReportSectionTemplate("Revenue Breakdown by Source",    data_fields=["breakdown"],                  chart_type="PIE_CHART"),
        ReportSectionTemplate("Trend Analysis",                 data_fields=["time_series"],                chart_type="LINE_CHART"),
        ReportSectionTemplate("Variance vs. Prior Period",      data_fields=["scalars"],                    chart_type=None),
        ReportSectionTemplate("Cash Flow Impact",               data_fields=["scalars"],                    chart_type=None),
        ReportSectionTemplate("Risk Flags & Open Actions",      data_fields=[],                             chart_type=None),
    ],
    "ceo": [
        ReportSectionTemplate("Strategic Overview",             data_fields=["scalars"],                    chart_type="BAR_CHART"),
        ReportSectionTemplate("Key Performance Indicators",     data_fields=["scalars"],                    chart_type=None),
        ReportSectionTemplate("Risks & Opportunities",          data_fields=[],                             chart_type=None),
        ReportSectionTemplate("Recommended Actions",            data_fields=[],                             chart_type=None),
    ],
    "business_user": [
        ReportSectionTemplate("Summary",                        data_fields=["scalars"],                    chart_type="BAR_CHART"),
        ReportSectionTemplate("Details",                        data_fields=["rows"],                       chart_type=None),
        ReportSectionTemplate("What This Means for You",        data_fields=[],                             chart_type=None),
    ],
    "data_steward": [
        ReportSectionTemplate("Data Coverage & Sources",        data_fields=["sources_queried"],            chart_type=None),
        ReportSectionTemplate("CDM Mapping Summary",            data_fields=[],                             chart_type=None),
        ReportSectionTemplate("Data Quality Notes",             data_fields=["sources_failed"],             chart_type=None),
        ReportSectionTemplate("Raw Metrics",                    data_fields=["scalars", "breakdown"],       chart_type="TABLE"),
    ],
}
```

### 8.3 Step 1: Report Structure Planning (LLM)

```python
REPORT_PLANNER_SYSTEM_PROMPT = """
You are NEXUS Report Planner. Given a user query and data summary, determine the
optimal report structure for the specified user persona.

USER PERSONA: {user_role}
USER QUERY: {original_query}
DATA SUMMARY: {data_summary}
AVAILABLE SECTIONS: {available_sections}

Rules:
1. Select 3-6 sections from the AVAILABLE SECTIONS list.
2. Order sections from most important to least important for this persona.
3. Respond ONLY with a JSON object:
   { "title": string, "sections": [{ "name": string, "data_fields": [string], "chart_type": string|null }] }
4. Title should be specific and include the time period if relevant.
5. Do not invent sections not in the AVAILABLE SECTIONS list.
"""
```

### 8.4 Step 2: Section Writing (LLM)

```python
async def write_section(
    section:  ReportSection,
    merged:   MergedResult,
    plan:     CDMQueryPlan,
) -> str:
    data_for_section = {
        field: getattr(merged, field, None)
        for field in section.data_fields
        if hasattr(merged, field)
    }

    prompt = f"""
    Write the "{section.name}" section of a {plan.user_role.upper()} report.

    User query: "{plan.original_query}"
    Section data: {json.dumps(data_for_section, default=str)}
    Sources queried: {', '.join(merged.sources_queried)}
    Partial result: {merged.partial}

    Rules:
    - Write 2-4 paragraphs of professional prose.
    - Use Markdown for structure (## headings, **bold** for key numbers).
    - If partial is True, clearly state which sources were unavailable.
    - Do not use jargon inappropriate for the {plan.user_role} persona.
    - Do not invent numbers not present in the data.
    """

    return await self.llm.complete(prompt, model="claude-sonnet-4-6", max_tokens=600)
```

**Issue 8 correction — Parallelise section writes with `asyncio.gather`:** The original sequential `for section in sections` loop called `write_section()` one at a time, producing ~2 seconds per section × 5 sections ≈ 10 seconds of total LLM wait time for a CFO report. Each section write is independent — they share the same `merged` and `plan` inputs but do not depend on each other's output. Using `asyncio.gather` runs all section LLM calls concurrently, reducing total report generation time to roughly the duration of the slowest single section (~2 seconds).

```python
class ReportBuilder:
    """
    Orchestrates the full report generation pipeline:
      Step 1 → plan_report_structure (LLM — sequential, must complete before step 2)
      Step 2 → write_section × N      (LLM — PARALLELISED with asyncio.gather)
      Step 3 → build_docx             (CPU — sequential, depends on step 2 output)
      Step 4 → store_report           (I/O — sequential, depends on step 3 output)
    """

    async def build(self, merged: MergedResult, plan: CDMQueryPlan) -> ReportOutput:

        # Step 1: Plan report structure (sequential — decomposition must happen first)
        persona       = plan.user_role if plan.user_role in REPORT_SECTION_TEMPLATES else "business_user"
        templates     = REPORT_SECTION_TEMPLATES[persona]
        report_struct = await self.plan_report_structure(merged, plan, templates)

        # Step 2: Write all sections in PARALLEL (Issue 8 fix)
        # Each section write is an independent LLM call — no cross-section dependency.
        # asyncio.gather fires all calls concurrently; total wall-clock time ≈ slowest section.
        section_tasks = [
            write_section(section, merged, plan)
            for section in report_struct.sections
        ]
        section_texts: list[str] = await asyncio.gather(*section_tasks)
        # On individual section timeout: write_section returns the placeholder string
        # "Section unavailable — LLM timeout" (see Edge Cases). gather does not fail fast.

        # Step 3: Assemble DOCX (sequential — needs all sections complete)
        docx_bytes = await self.build_docx(report_struct, section_texts, merged, plan)

        # Step 4: Store in MinIO and generate presigned URL
        download_url = await self.store_report(docx_bytes, plan)

        preview_section = section_texts[0] if section_texts else ""
        return ReportOutput(
            download_url=   download_url,
            filename=       self.generate_filename(plan),
            preview=        preview_section[:500],
            section_count=  len(section_texts),
            page_count_est= int(len(section_texts) * 1.5),
        )
```

### 8.5 Step 3: DOCX Generation

Report generation uses `python-docx`. Each section is rendered with:
- H2 heading (Heading 2 style)
- 3-4 paragraphs of text
- An embedded chart image (if `chart_type` is set for the section — chart is rendered as PNG client-side by M6 for dashboard, but as a static Recharts SVG server-side for reports)

[CLARIFY: Server-side chart rendering for DOCX — Recharts is a React library and cannot run server-side without a headless browser. Options: (1) Use `matplotlib` for server-side chart images in reports; (2) Use a headless Playwright instance; (3) Omit chart images from DOCX and include data tables instead. Recommend option 3 for Iteration 2 (data tables) and revisit server-side chart rendering in Iteration 3.]

```python
@dataclass
class ReportOutput:
    download_url:    str          # MinIO signed URL (TTL 15 min)
    filename:        str          # "Q1-2026-Financial-Report.docx"
    preview:         str          # First 500 characters of Executive Summary section
    section_count:   int
    page_count_est:  int          # Estimated pages (section_count × 1.5)
```

### 8.6 Step 4: Report Storage (MinIO)

```python
REPORT_MINIO_PATH = "nexus-reports/{tenant_id}/{session_id}/{filename}"
REPORT_URL_TTL = 900  # 15 minutes

async def store_report(docx_bytes: bytes, plan: CDMQueryPlan) -> str:
    filename  = self.generate_filename(plan)
    minio_path = REPORT_MINIO_PATH.format(
        tenant_id=  plan.tenant_id,
        session_id= plan.session_id,
        filename=   filename,
    )

    await self.minio.put_object(
        bucket_name= "nexus-reports",
        object_name= minio_path,
        data=        io.BytesIO(docx_bytes),
        length=      len(docx_bytes),
        content_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    )

    # Generate presigned URL
    url = self.minio.presigned_get_object(
        bucket_name= "nexus-reports",
        object_name= minio_path,
        expires=     timedelta(seconds=REPORT_URL_TTL),
    )

    return url

def generate_filename(self, plan: CDMQueryPlan) -> str:
    """
    Generates a readable filename from the query and date range.
    "How many deals in Q1 2026" → "Q1-2026-Deals-Report.docx"
    """
    date_tag = ""
    if plan.date_range:
        from_dt = datetime.fromisoformat(plan.date_range["from"])
        quarter = (from_dt.month - 1) // 3 + 1
        date_tag = f"Q{quarter}-{from_dt.year}-"

    entity_tag = plan.entity.split(".")[-1].title()
    return f"{date_tag}{entity_tag}-Report.docx"
```

---

## 9. ResultRenderer (Full Class)

```python
class ResultRenderer:
    """
    Orchestrates rendering from MergedResult → RenderedOutput.
    Called by nexus-query-executor after ResultMerger.
    """

    async def render(
        self,
        merged:      MergedResult,
        plan:        CDMQueryPlan,
        user_role:   str,
        output_pref: str,
    ) -> RenderedOutput:

        output_type = resolve_output_type(plan, output_pref, user_role)

        match output_type:
            case OutputType.TEXT:
                text_content = await self.render_text(merged, plan)
                return RenderedOutput(
                    output_type=      OutputType.TEXT,
                    title=            self.generate_title(plan),
                    summary=          self.generate_summary(merged, plan),
                    text_content=     text_content,
                    table=            None,
                    chart_spec=       None,
                    report=           None,
                    sources_queried=  merged.sources_queried,
                    sources_failed=   merged.sources_failed,
                    cdm_version=      plan.cdm_version,
                    partial=          merged.partial,
                    can_save_dashboard=False,
                    can_export=       [],
                )

            case OutputType.TABLE:
                table = self.render_table(merged, plan)
                return RenderedOutput(
                    output_type=       OutputType.TABLE,
                    title=             self.generate_title(plan),
                    summary=           self.generate_summary(merged, plan),
                    table=             table,
                    chart_spec=        None,
                    text_content=      None,
                    report=            None,
                    sources_queried=   merged.sources_queried,
                    sources_failed=    merged.sources_failed,
                    cdm_version=       plan.cdm_version,
                    partial=           merged.partial,
                    can_save_dashboard=False,
                    can_export=        ["xlsx", "csv", "pdf"],
                )

            case OutputType.BAR_CHART | OutputType.LINE_CHART | OutputType.PIE_CHART:
                chart_spec = self.render_chart(output_type, merged, plan)
                return RenderedOutput(
                    output_type=       output_type,
                    title=             self.generate_title(plan),
                    summary=           self.generate_summary(merged, plan),
                    chart_spec=        chart_spec,
                    table=             None,
                    text_content=      None,
                    report=            None,
                    sources_queried=   merged.sources_queried,
                    sources_failed=    merged.sources_failed,
                    cdm_version=       plan.cdm_version,
                    partial=           merged.partial,
                    can_save_dashboard=True,
                    can_export=        ["xlsx", "csv"],
                )

            case OutputType.REPORT:
                report_output = await self.report_builder.build(merged, plan)
                return RenderedOutput(
                    output_type=       OutputType.REPORT,
                    title=             report_output.filename.replace(".docx", "").replace("-", " "),
                    summary=           report_output.preview[:200],
                    report=            report_output,
                    chart_spec=        None,
                    table=             None,
                    text_content=      None,
                    sources_queried=   merged.sources_queried,
                    sources_failed=    merged.sources_failed,
                    cdm_version=       plan.cdm_version,
                    partial=           merged.partial,
                    can_save_dashboard=False,
                    can_export=        ["pdf"],
                )
```

---

## 10. Title and Summary Generation

Both are deterministic (no LLM) for chart and table outputs:

```python
def generate_title(self, plan: CDMQueryPlan) -> str:
    """Generates a human-readable title from the query plan."""
    entity_label = ENTITY_LABELS.get(plan.entity, plan.entity)
    action_label = INTENT_LABELS.get(plan.intent, "Analysis")

    date_tag = ""
    if plan.date_range:
        from_dt = datetime.fromisoformat(plan.date_range["from"])
        to_dt   = datetime.fromisoformat(plan.date_range["to"])
        if (to_dt - from_dt).days > 300:
            date_tag = f" — {from_dt.year}"
        else:
            date_tag = f" — {from_dt.strftime('%b')}–{to_dt.strftime('%b %Y')}"

    group_tag = f" by {plan.group_by[0].replace('_', ' ').title()}" if plan.group_by else ""

    return f"{entity_label} {action_label}{group_tag}{date_tag}"
    # → "Deal Closed Count by Source — 2025"
    # → "Revenue Trend — Jan–Mar 2026"

def generate_summary(self, merged: MergedResult, plan: CDMQueryPlan) -> str:
    """One-sentence summary. Never uses LLM."""
    if plan.intent == "aggregation" and "count" in [a.get("func","").lower() for a in plan.aggregations]:
        total = merged.scalars.get("count", 0)
        return f"{total:,} {ENTITY_LABELS.get(plan.entity, 'records')} found across {len(merged.sources_queried)} source(s)."
    if plan.intent == "aggregation" and "sum" in [a.get("func","").lower() for a in plan.aggregations]:
        total = merged.scalars.get("total_value", merged.scalars.get("sum", 0))
        return f"Total: {self.format_currency(total)} across {len(merged.sources_queried)} source(s)."
    if merged.partial:
        return f"Partial result — {len(merged.sources_failed)} source(s) unavailable."
    return f"Result from {', '.join(merged.sources_queried)}."
```

---

## 11. Edge Cases

| Case | Handling |
|---|---|
| Aggregation result has no `group_by` | Bar chart falls back to a single-bar chart; consider forcing `TEXT` output instead |
| Time-series data has fewer than 3 data points | Force `TABLE` output; a line chart with 1-2 points is misleading |
| Chart data contains `null` values | Replace with `0` for numeric aggregations; log a data quality warning |
| Report section LLM call times out | Skip section, include a placeholder: *"Section unavailable — LLM timeout"* |
| MinIO unavailable during report storage | Return error with `error_code: "report_storage_failed"`; do not return partial report |
| Dashboard refresh query returns different output_type than original | Keep the original `output_type`; update only `chart_spec.data` — do not change chart type |
| Export requested for session with `output_type = TEXT` | Return 400: "TEXT results cannot be exported as XLSX or CSV" |
| Report for a query with `partial: true` | Include data quality note in every section: *"Note: {source} data unavailable"* |
| User requests `output_preference = "chart"` for a relationship query | Relationship results are graph structures — cannot be charted. Fallback to `TABLE` and log override |

---

## 12. Open Questions

| # | Question | Impact |
|---|---|---|
| OQ-VO-01 | Should chart images be embedded in DOCX reports (requires Playwright/headless browser) or replaced with data tables? | Report visual quality vs. implementation complexity |
| OQ-VO-02 | MinIO presigned URL TTL is 15 minutes. Should reports be re-downloadable after TTL expires? Requires re-generating the signed URL or making reports publicly accessible for longer. | User experience for slow email recipients |
| OQ-VO-03 | Should the dashboard component show a "last refreshed" timestamp in M6? This requires the UI to display `last_refreshed_at` from the component record. | M6 frontend scope |
| OQ-VO-04 | Should users be able to delete dashboard components? Not in scope for Iteration 2 — confirm with Product. | M6 feature scope |
| OQ-VO-05 | Text output uses Claude Haiku for synthesis to reduce cost. Is the quality acceptable for CFO/CEO personas, or should Sonnet be used for all user_roles? | Output quality vs. cost |

---

*NEXUS Iteration 2 · Visual Outputs Spec · v0.1 · Mentis Consulting · March 2026 · Confidential*
