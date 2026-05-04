# NEXUS — Iteration 2 · M6 Frontend Delta
**Workstream 4 · React UI Changes for Query Engine Integration**
Mentis Consulting · Version 0.2 · April 2026 · Confidential

> **Revision v0.2 — Architecture review corrections applied**
> OQ-M6-01 (M2 RHMA chat panel vs. Query Engine panel) escalated from Open Question to Decision Required. The architectural description presents a single unified "AI Chat Interface" for all query types, which implies these should be merged. A recommended resolution is provided below.

---

## 📦 Codebase & Deployment

> **One repository, many deployments.** The NEXUS React frontend lives in the `nexus-platform` monorepo alongside all backend services. You are not in a separate frontend repo — shared TypeScript types and API contracts are co-located with the backend specs. Do not invent types; import them from the shared `@nexus/types` package.

| | |
|---|---|
| **Deployed as** | `nexus-m6-frontend` (React SPA, served via CDN / nginx) |
| **Monorepo path** | `frontend/nexus-m6/` |
| **Language / runtime** | TypeScript · React 18 · Vite · TailwindCSS · Zustand · React Query |
| **Shared types package** | `packages/nexus-types/` — `RenderedOutput`, `ChartSpec`, `QueryStreamEvent` are defined here and consumed by both this frontend and `nexus-query-api` |
| **Iteration 2 scope** | Delta spec — only new and changed components are described here. All Iteration 1 M6 components are unchanged unless explicitly noted. |

---

## Overview

Module 6 (M6) is the NEXUS React frontend. In Iteration 1, M6 covered connector management, CDM governance review, and the M2 knowledge-query chat panel. In Iteration 2, M6 gains the **Query Engine UI**: a natural-language query input that streams results, renders charts and tables, lets users save components to a personal dashboard, and generates downloadable reports.

This document specifies only the **delta** — new components, hooks, and flows introduced for Iteration 2. All existing Iteration 1 M6 components are unchanged unless explicitly noted.

**Tech stack (unchanged from Iteration 1):** React 18, TypeScript, Recharts, TailwindCSS, Zustand (state), React Query (server state), Vite.

---

## Functional Requirements

| ID | Requirement | Priority |
|---|---|---|
| M6-FR-01 | User can submit a natural language query from a dedicated query input panel | Must |
| M6-FR-02 | Results stream progressively via WebSocket — user sees status events before final result | Must |
| M6-FR-03 | Bar, Line, and Pie charts render using Recharts with correct data keys from `ChartSpec` | Must |
| M6-FR-04 | User can switch between available output types (chart / table / text) for any result | Must |
| M6-FR-05 | User can save a chart or table as a named dashboard component | Must |
| M6-FR-06 | "My Dashboard" grid renders saved components with refresh indicators | Must |
| M6-FR-07 | Stale components (refresh schedule exceeded) display a visible staleness badge | Should |
| M6-FR-08 | User can download query results as XLSX, CSV, or PDF | Must |
| M6-FR-09 | Report download triggers `.docx` generation and presents a download link | Must |
| M6-FR-10 | Partial results (one source failed) are displayed with a warning banner | Must |
| M6-FR-11 | WebSocket connection drops gracefully — client falls back to polling | Should |
| M6-FR-12 | Output type switcher is disabled for output types not supported by the current result (e.g. chart disabled for relationship intents) | Should |

## Non-Functional Requirements

| ID | Requirement | Target |
|---|---|---|
| M6-NFR-01 | Time to first streaming event in UI | < 300ms after submit |
| M6-NFR-02 | Chart render time (Recharts, 200 data points) | < 100ms |
| M6-NFR-03 | Dashboard grid loads 20 components | < 1s |
| M6-NFR-04 | No layout shift during result streaming | Zero CLS during stream |

---

## 1. API Integration

### 1.1 nexus-query-api Endpoints Used by M6

| Endpoint | Method | Used when |
|---|---|---|
| `/query` | POST | Submit query, get `session_id` |
| `/query/{session_id}/ws` | WebSocket | Stream query events |
| `/query/{session_id}` | GET | Poll for result (WebSocket fallback) |
| `/query/{session_id}/save-dashboard` | POST | Save chart/table as component |
| `/query/{session_id}/export` | POST | Export as xlsx / csv / pdf |
| `/dashboard/components` | GET | List saved components |
| `/dashboard/components/{component_id}` | DELETE | Remove component |

### 1.2 Query Submission

```typescript
interface QuerySubmitRequest {
  query:              string;        // Natural language question
  output_preference:  "auto" | "chart" | "table" | "text" | "report";
  context?: {
    current_view?:    string;        // e.g. "salesforce-deals"
    user_timezone?:   string;        // IANA timezone string
  };
}

interface QuerySubmitResponse {
  session_id: string;               // Used to open WebSocket or poll
}
```

POST to `/query` — expects HTTP 202 Accepted with `session_id` in < 200ms.

---

## 2. WebSocket Client

### 2.1 Connection Protocol

```typescript
const WS_URL = `wss://${API_HOST}/query/${sessionId}/ws`;

// Event envelope (matches nexus-query-api stream protocol)
interface QueryStreamEvent {
  event:      "planning" | "decomposing" | "executing" | "result" | "error" | "timeout";
  session_id: string;
  payload:    QueryEventPayload;
}
```

### 2.2 `useQueryStream` Hook

```typescript
/**
 * Manages WebSocket connection for a query session.
 * Automatically falls back to polling if WebSocket is unavailable.
 */
function useQueryStream(sessionId: string | null) {
  const [status,  setStatus]  = useState<QueryStatus>("idle");
  const [events,  setEvents]  = useState<QueryStreamEvent[]>([]);
  const [result,  setResult]  = useState<RenderedOutput | null>(null);
  const [error,   setError]   = useState<string | null>(null);
  const wsRef = useRef<WebSocket | null>(null);

  useEffect(() => {
    if (!sessionId) return;

    setStatus("connecting");
    const ws = new WebSocket(`${WS_BASE_URL}/query/${sessionId}/ws`);
    wsRef.current = ws;

    ws.onopen  = ()  => setStatus("streaming");
    ws.onclose = ()  => {
      if (status !== "done" && status !== "error") {
        // WebSocket dropped mid-stream — fall back to polling
        startPollingFallback(sessionId);
      }
    };
    ws.onerror = ()  => startPollingFallback(sessionId);

    ws.onmessage = (msg: MessageEvent) => {
      const event: QueryStreamEvent = JSON.parse(msg.data);
      setEvents(prev => [...prev, event]);

      if (event.event === "result") {
        setResult(event.payload as RenderedOutput);
        setStatus("done");
        ws.close();
      }
      if (event.event === "error" || event.event === "timeout") {
        setError(event.payload.message ?? "Query failed");
        setStatus("error");
        ws.close();
      }
    };

    return () => { ws.close(); wsRef.current = null; };
  }, [sessionId]);

  function startPollingFallback(sid: string) {
    setStatus("polling");
    // Poll GET /query/{session_id} every 2s until done or error
    const interval = setInterval(async () => {
      const resp = await nexusApi.getQueryResult(sid);
      if (resp.status === "COMPLETED") {
        setResult(resp.rendered_output);
        setStatus("done");
        clearInterval(interval);
      }
      if (resp.status === "FAILED" || resp.status === "TIMEOUT") {
        setError(resp.error_message ?? "Query failed");
        setStatus("error");
        clearInterval(interval);
      }
    }, 2000);
  }

  return { status, events, result, error };
}
```

### 2.3 Streaming Status Indicator

Each event type maps to a user-visible status label:

| Event | Label shown | Progress bar |
|---|---|---|
| `planning` | "Understanding your question…" | 15% |
| `decomposing` | "Preparing source queries…" | 35% |
| `executing` | "Querying {source_count} source(s)…" | 55% |
| `result` | — (result renders) | 100% |
| `error` | Error banner with `error_message` | — |
| `timeout` | "Query timed out. Try a simpler question." | — |

---

## 3. Recharts Integration

### 3.1 `ChartSpec` → Recharts Mapping

The `ChartSpec` produced by `nexus-query-executor` is Recharts-compatible JSON. M6 maps it to the corresponding Recharts component:

```typescript
interface ChartSpec {
  type:     "BarChart" | "LineChart" | "PieChart";
  data:     Record<string, unknown>[];
  xAxis?:   string;               // Key from data[] used as X-axis label
  bars?:    BarDef[];             // For BarChart
  lines?:   LineDef[];            // For LineChart
  cells?:   CellDef[];            // For PieChart (nameKey + valueKey)
  yAxisFormatter?: "currency" | "number" | "percent";
}

interface BarDef   { dataKey: string; name: string; fill: string; }
interface LineDef  { dataKey: string; name: string; stroke: string; }
interface CellDef  { nameKey: string; valueKey: string; }
```

### 3.2 `<NexusChart>` Component

```tsx
/**
 * Renders a RenderedOutput chart using the appropriate Recharts component.
 * Uses the ChartSpec from nexus-query-executor directly — no transformation needed.
 *
 * Issue 12 reference: example values in chart spec use €11.8M total
 * (aligned with M6 UX mockup — Salesforce €7.8M + AdventureWorks €4.0M).
 */
function NexusChart({ spec }: { spec: ChartSpec }) {
  const formatter = spec.yAxisFormatter === "currency"
    ? (v: number) => `€${(v / 1_000_000).toFixed(1)}M`
    : (v: number) => v.toLocaleString();

  switch (spec.type) {

    case "BarChart":
      return (
        <ResponsiveContainer width="100%" height={320}>
          <BarChart data={spec.data}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey={spec.xAxis} />
            <YAxis tickFormatter={formatter} />
            <Tooltip formatter={(v: number) => formatter(v)} />
            <Legend />
            {spec.bars?.map(bar => (
              <Bar key={bar.dataKey} dataKey={bar.dataKey}
                   name={bar.name} fill={bar.fill} />
            ))}
          </BarChart>
        </ResponsiveContainer>
      );

    case "LineChart":
      return (
        <ResponsiveContainer width="100%" height={320}>
          <LineChart data={spec.data}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey={spec.xAxis} />
            <YAxis tickFormatter={formatter} />
            <Tooltip formatter={(v: number) => formatter(v)} />
            <Legend />
            {spec.lines?.map(line => (
              <Line key={line.dataKey} type="monotone"
                    dataKey={line.dataKey} name={line.name}
                    stroke={line.stroke} dot={false} />
            ))}
          </LineChart>
        </ResponsiveContainer>
      );

    case "PieChart":
      const COLORS = ["#7c3aed", "#2dd4bf", "#f59e0b", "#ef4444", "#3b82f6"];
      return (
        <ResponsiveContainer width="100%" height={320}>
          <PieChart>
            <Pie
              data={spec.data}
              nameKey={spec.cells?.[0]?.nameKey ?? "name"}
              dataKey={spec.cells?.[0]?.valueKey ?? "value"}
              cx="50%" cy="50%" outerRadius={120}
              label={({ name, percent }) => `${name}: ${(percent * 100).toFixed(1)}%`}
            >
              {spec.data.map((_, idx) => (
                <Cell key={idx} fill={COLORS[idx % COLORS.length]} />
              ))}
            </Pie>
            <Tooltip formatter={(v: number) => formatter(v)} />
          </PieChart>
        </ResponsiveContainer>
      );
  }
}
```

---

## 4. Output Type Switcher

### 4.1 Available Output Types Per Intent

Not all output types are valid for all query intents. The switcher disables unavailable options (M6-FR-12):

| Intent | Available output types |
|---|---|
| `aggregation` | `TEXT`, `TABLE`, `BAR_CHART`, `PIE_CHART` |
| `trend` | `TEXT`, `TABLE`, `LINE_CHART` |
| `lookup` | `TEXT`, `TABLE` |
| `relationship` | `TEXT`, `TABLE` |
| `semantic` | `TEXT`, `TABLE` |
| `report` | `REPORT` only |

### 4.2 `<OutputTypeSwitcher>` Component

```tsx
const OUTPUT_TYPE_LABELS: Record<string, { label: string; icon: string }> = {
  TEXT:       { label: "Text",    icon: "📝" },
  TABLE:      { label: "Table",   icon: "📊" },
  BAR_CHART:  { label: "Bar",     icon: "📶" },
  LINE_CHART: { label: "Line",    icon: "📈" },
  PIE_CHART:  { label: "Pie",     icon: "🥧" },
  REPORT:     { label: "Report",  icon: "📄" },
};

function OutputTypeSwitcher({
  intent,
  current,
  onChange,
}: {
  intent:   string;
  current:  string;
  onChange: (type: string) => void;
}) {
  const available = INTENT_OUTPUT_TYPES[intent] ?? ["TEXT", "TABLE"];

  return (
    <div className="flex gap-2 p-2 bg-gray-50 rounded-lg">
      {Object.entries(OUTPUT_TYPE_LABELS).map(([type, { label, icon }]) => {
        const isEnabled = available.includes(type);
        return (
          <button
            key={type}
            onClick={() => isEnabled && onChange(type)}
            disabled={!isEnabled}
            className={`
              px-3 py-1.5 text-sm font-medium rounded-md transition-colors
              ${current === type
                ? "bg-violet-600 text-white"
                : isEnabled
                  ? "bg-white text-gray-700 hover:bg-gray-100 border border-gray-200"
                  : "bg-gray-100 text-gray-400 cursor-not-allowed opacity-50"
              }
            `}
            title={!isEnabled ? `Not available for ${intent} queries` : undefined}
          >
            {icon} {label}
          </button>
        );
      })}
    </div>
  );
}
```

---

## 5. Save to Dashboard Flow

### 5.1 Save Flow

When a user clicks "Save to Dashboard" on a chart or table result:

```
User clicks "Save to Dashboard"
    │
    ├── Show <SaveDashboardModal>
    │       Inputs: component_name (required), refresh_schedule ("none" | "hourly" | "daily")
    │
    └── POST /query/{session_id}/save-dashboard
            {
              "component_name":   "Q1 Revenue by Source",
              "refresh_schedule": "daily"
            }
            ↓
        Response: { component_id, component_name, last_refreshed_at }
            ↓
        Show success toast: "Saved to My Dashboard"
        Update local dashboard component cache via React Query invalidation
```

### 5.2 `<SaveDashboardModal>` Component

```tsx
function SaveDashboardModal({
  sessionId,
  onSaved,
  onClose,
}: {
  sessionId: string;
  onSaved:   (componentId: string) => void;
  onClose:   () => void;
}) {
  const [name,            setName]            = useState("");
  const [refreshSchedule, setRefreshSchedule] = useState<"none" | "hourly" | "daily">("daily");
  const [saving,          setSaving]          = useState(false);

  const handleSave = async () => {
    if (!name.trim()) return;
    setSaving(true);
    try {
      const resp = await nexusApi.saveDashboardComponent(sessionId, {
        component_name:   name.trim(),
        refresh_schedule: refreshSchedule === "none" ? null : refreshSchedule,
      });
      onSaved(resp.component_id);
    } finally {
      setSaving(false);
    }
  };

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40">
      <div className="bg-white rounded-xl shadow-xl p-6 w-96">
        <h2 className="text-lg font-semibold mb-4">Save to My Dashboard</h2>
        <label className="block text-sm font-medium text-gray-700 mb-1">
          Component name
        </label>
        <input
          value={name}
          onChange={e => setName(e.target.value)}
          className="w-full border border-gray-300 rounded-md px-3 py-2 text-sm mb-4"
          placeholder="e.g. Q1 Revenue by Source"
          autoFocus
        />
        <label className="block text-sm font-medium text-gray-700 mb-1">
          Auto-refresh
        </label>
        <select
          value={refreshSchedule}
          onChange={e => setRefreshSchedule(e.target.value as "none" | "hourly" | "daily")}
          className="w-full border border-gray-300 rounded-md px-3 py-2 text-sm mb-6"
        >
          <option value="none">No auto-refresh</option>
          <option value="hourly">Every hour</option>
          <option value="daily">Every day</option>
        </select>
        <div className="flex justify-end gap-3">
          <button onClick={onClose}
            className="px-4 py-2 text-sm text-gray-600 hover:text-gray-900">
            Cancel
          </button>
          <button onClick={handleSave} disabled={!name.trim() || saving}
            className="px-4 py-2 text-sm bg-violet-600 text-white rounded-md
                       hover:bg-violet-700 disabled:opacity-50">
            {saving ? "Saving…" : "Save"}
          </button>
        </div>
      </div>
    </div>
  );
}
```

---

## 6. My Dashboard Grid

### 6.1 Dashboard Component Card

The "My Dashboard" page renders a responsive grid of `<DashboardComponentCard>` widgets. Each card:
- Shows the component's saved chart or table (re-rendered from `chart_spec`)
- Displays a staleness badge if the component's `last_refreshed_at` is older than its `refresh_schedule` interval
- Shows a loading spinner while the Airflow DAG is actively refreshing the component (M6-FR-07)

```tsx
function DashboardComponentCard({ component }: { component: DashboardComponent }) {
  const isStale = useMemo(() => {
    if (!component.refresh_schedule || !component.last_refreshed_at) return false;
    const refreshedAt = new Date(component.last_refreshed_at).getTime();
    const threshold = component.refresh_schedule === "hourly"
      ? 60 * 60 * 1000        // 1 hour in ms
      : 23 * 60 * 60 * 1000;  // 23 hours in ms
    return Date.now() - refreshedAt > threshold;
  }, [component.last_refreshed_at, component.refresh_schedule]);

  return (
    <div className="bg-white rounded-xl border border-gray-200 shadow-sm overflow-hidden">
      {/* Card header */}
      <div className="px-4 py-3 flex items-center justify-between border-b border-gray-100">
        <span className="text-sm font-semibold text-gray-900 truncate">
          {component.component_name}
        </span>
        <div className="flex items-center gap-2">
          {isStale && (
            <span className="inline-flex items-center gap-1 text-xs text-amber-600
                             bg-amber-50 border border-amber-200 rounded-full px-2 py-0.5">
              <span className="w-1.5 h-1.5 rounded-full bg-amber-500" />
              Stale
            </span>
          )}
          <span className="text-xs text-gray-400">
            {formatRelative(new Date(component.last_refreshed_at ?? component.created_at))}
          </span>
        </div>
      </div>

      {/* Chart or table */}
      <div className="p-4">
        {component.output_type === "TABLE"
          ? <NexusTable   rows={component.chart_spec.data}
                          columns={component.chart_spec.columns ?? []} />
          : <NexusChart   spec={component.chart_spec as ChartSpec} />
        }
      </div>
    </div>
  );
}
```

### 6.2 Dashboard Page Layout

```tsx
function MyDashboard() {
  const { data: components, isLoading } = useQuery({
    queryKey:  ["dashboard-components"],
    queryFn:   () => nexusApi.getDashboardComponents(),
    staleTime: 60_000,   // Refresh component list every 60s
  });

  if (isLoading) return <DashboardSkeleton />;
  if (!components?.length) return <DashboardEmptyState />;

  return (
    <div className="p-6">
      <h1 className="text-2xl font-bold text-gray-900 mb-6">My Dashboard</h1>
      <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-6">
        {components.map(c => (
          <DashboardComponentCard key={c.component_id} component={c} />
        ))}
      </div>
    </div>
  );
}
```

---

## 7. Export Flow

### 7.1 Export Request

```typescript
interface ExportRequest {
  format:          "xlsx" | "csv" | "pdf";
  include_title?:  boolean;    // Default true
  max_rows?:       number;     // Default: server-side EXPORT_MAX_ROWS limit
}

// POST /query/{session_id}/export
// Response: { download_url: string, filename: string, expires_at: ISO8601 }
```

### 7.2 `<ExportMenu>` Component

```tsx
function ExportMenu({ sessionId, outputType }: { sessionId: string; outputType: string }) {
  const [exporting, setExporting] = useState<string | null>(null);

  const handleExport = async (format: "xlsx" | "csv" | "pdf") => {
    if (outputType === "TEXT") {
      toast.error("Text results cannot be exported as XLSX or CSV");
      return;
    }
    setExporting(format);
    try {
      const resp = await nexusApi.exportResult(sessionId, { format });
      // Trigger browser download
      const a = document.createElement("a");
      a.href     = resp.download_url;
      a.download = resp.filename;
      a.click();
    } catch {
      toast.error("Export failed. Please try again.");
    } finally {
      setExporting(null);
    }
  };

  return (
    <div className="flex gap-2">
      {(["xlsx", "csv", "pdf"] as const).map(fmt => (
        <button
          key={fmt}
          onClick={() => handleExport(fmt)}
          disabled={!!exporting}
          className="px-3 py-1.5 text-xs font-medium text-gray-600 bg-white
                     border border-gray-200 rounded-md hover:bg-gray-50 disabled:opacity-50"
        >
          {exporting === fmt ? "…" : fmt.toUpperCase()}
        </button>
      ))}
    </div>
  );
}
```

---

## 8. Partial Result Banner

When `rendered_output.partial === true`, M6 displays a warning banner above the result:

```tsx
function PartialResultBanner({ sourcesFailed }: { sourcesFailed: SourceFailure[] }) {
  if (!sourcesFailed.length) return null;
  return (
    <div className="flex items-start gap-3 p-4 bg-amber-50 border border-amber-200
                    rounded-lg text-sm text-amber-800 mb-4">
      <span className="text-amber-500 mt-0.5">⚠</span>
      <div>
        <p className="font-semibold">Partial result</p>
        <p className="mt-1">
          The following source(s) were unavailable during this query:
          {" "}
          <strong>{sourcesFailed.map(f => f.source_system).join(", ")}</strong>.
          The result may be incomplete.
        </p>
      </div>
    </div>
  );
}
```

---

## 9. Query Panel — Full Assembly

```tsx
function QueryPanel() {
  const [query,      setQuery]      = useState("");
  const [outputPref, setOutputPref] = useState<string>("auto");
  const [sessionId,  setSessionId]  = useState<string | null>(null);
  const [saving,     setSaving]     = useState(false);
  const [savedId,    setSavedId]    = useState<string | null>(null);
  const [switchedType, setSwitchedType] = useState<string | null>(null);

  const { status, events, result, error } = useQueryStream(sessionId);

  const handleSubmit = async () => {
    if (!query.trim()) return;
    setSavedId(null);
    setSwitchedType(null);
    const resp = await nexusApi.submitQuery({
      query,
      output_preference: outputPref,
    });
    setSessionId(resp.session_id);
  };

  const displayedType = switchedType ?? result?.output_type ?? null;
  const displayedResult = result
    ? { ...result, output_type: displayedType ?? result.output_type }
    : null;

  return (
    <div className="flex flex-col h-full">
      {/* Input area */}
      <div className="p-4 border-b border-gray-200 bg-white">
        <div className="flex gap-3">
          <input
            value={query}
            onChange={e => setQuery(e.target.value)}
            onKeyDown={e => e.key === "Enter" && handleSubmit()}
            placeholder="Ask about your data… e.g. 'How many deals closed in Q1 2026?'"
            className="flex-1 border border-gray-300 rounded-lg px-4 py-2.5 text-sm
                       focus:outline-none focus:ring-2 focus:ring-violet-500"
          />
          <button
            onClick={handleSubmit}
            disabled={!query.trim() || status === "streaming" || status === "polling"}
            className="px-4 py-2.5 bg-violet-600 text-white text-sm font-medium
                       rounded-lg hover:bg-violet-700 disabled:opacity-50"
          >
            {status === "streaming" || status === "polling" ? "Querying…" : "Ask"}
          </button>
        </div>
        {/* Status progress */}
        {(status === "streaming" || status === "polling") && (
          <QueryProgressIndicator events={events} />
        )}
      </div>

      {/* Result area */}
      <div className="flex-1 overflow-auto p-4">
        {error && (
          <div className="p-4 bg-red-50 border border-red-200 rounded-lg text-red-700 text-sm">
            {error}
          </div>
        )}

        {displayedResult && (
          <>
            {displayedResult.partial && (
              <PartialResultBanner sourcesFailed={displayedResult.sources_failed} />
            )}

            {/* Toolbar */}
            <div className="flex items-center justify-between mb-4">
              <h2 className="text-lg font-semibold text-gray-900">
                {displayedResult.title}
              </h2>
              <div className="flex items-center gap-3">
                <OutputTypeSwitcher
                  intent={displayedResult.intent ?? "aggregation"}
                  current={displayedType ?? displayedResult.output_type}
                  onChange={setSwitchedType}
                />
                <ExportMenu sessionId={sessionId!} outputType={displayedResult.output_type} />
                {!savedId && displayedResult.can_save_dashboard && (
                  <button
                    onClick={() => setSaving(true)}
                    className="px-3 py-1.5 text-sm bg-violet-50 text-violet-700
                               border border-violet-200 rounded-md hover:bg-violet-100"
                  >
                    Save to Dashboard
                  </button>
                )}
                {savedId && (
                  <span className="text-sm text-green-600">✓ Saved</span>
                )}
              </div>
            </div>

            {/* Chart / Table / Text renderer */}
            <NexusResultRenderer result={displayedResult} />
          </>
        )}
      </div>

      {/* Save modal */}
      {saving && sessionId && (
        <SaveDashboardModal
          sessionId={sessionId}
          onSaved={id => { setSavedId(id); setSaving(false); }}
          onClose={() => setSaving(false)}
        />
      )}
    </div>
  );
}
```

---

## 10. Edge Cases

| Case | Handling |
|---|---|
| WebSocket connection fails on open | Immediately fall back to polling (2s interval) |
| User navigates away during active query | Close WebSocket on component unmount; session continues server-side |
| `output_type = REPORT` returned | Hide chart/table switcher; show report download button with `.docx` link |
| Switcher selected to `BAR_CHART` but result has no `chart_spec` | Show informational message: "Chart not available for this result" |
| Dashboard component load fails | Show per-card error state with "Retry" button; do not fail entire dashboard |
| Export download URL has expired (15-min TTL) | Request a new export URL; do not reuse stale presigned URL |
| `can_save_dashboard = false` | Hide "Save to Dashboard" button entirely |
| `sources_failed` contains all queried sources | Show full error state, not partial banner |

---

## 11. Architecture Decision — Query Engine Panel vs. M2 RHMA Chat Panel

> **✅ DECISION CONFIRMED — Option A adopted (April 2026 Architecture Review).**
> OQ-M6-01 resolved. User-facing chat is handled entirely by `nexus-query-api`. The M2 RHMA chat panel is deprecated as a user-facing entrypoint in Iteration 2. No further action required from the M6 team before implementation.

### Context

M6 currently has an M2 RHMA chat panel (Iteration 1) that sends natural-language queries to `nexus-m2-api` and receives conversational text responses. The new Query Engine (Iteration 2) sends structured queries to `nexus-query-api` and receives visual outputs (charts, tables, reports).

The NEXUS architectural description presents a single unified "AI Chat Interface" — not two separate panels. The `pipeline` discriminator in `nexus_system.query_sessions` (`'m2'` vs `'query'`) supports both approaches at the data layer, so the implementation can go either way.

### Options

**Option A — Merged single interface (CONFIRMED):**
A single query input. When the user types a question, `nexus-query-api` handles it. The Query Engine's backend selector routes to M3 stores (semantic intent) or live sources (aggregation/lookup) automatically. The M2 RHMA pipeline is deprecated as a user-facing entrypoint — M2 executor continues to exist for the Structural Agent (schema interpretation) and for internal knowledge retrieval, but M6 no longer surfaces a separate M2 chat panel.
- Simpler UX — one input for all query types
- Consistent visual output model (charts, tables, text) for all results
- Requires the Query Engine's backend selector to handle the semantic/knowledge query cases that M2 RHMA previously handled exclusively

**Option B — Co-existing panels:**
Two separate tabs: "Ask NEXUS" (Query Engine, visual results) and "Knowledge Chat" (M2 RHMA, conversational text). Users can choose which interface to use.
- Preserves M2 RHMA behaviour for conversational/exploratory queries
- Doubles the UI surface area — two WebSocket clients, two result rendering flows, two tabs to maintain
- Users must understand the distinction between the two modes (a cognitive burden)

### Recommendation

**Adopt Option A for Iteration 2.** The architectural description's vision is a single AI Chat Interface. The `pipeline` discriminator in the DB allows both queries to coexist at the data layer, so Option A does not require schema changes. The M2 RHMA WebSocket endpoint (`nexus-m2-api`) remains available for automated/programmatic callers; M6 simply stops routing user queries there.

**M6 team action:** Option A is confirmed. The existing M2 chat panel component is deprecated in this iteration — remove it from the user-facing interface. The `nexus-m2-api` service continues running for the Structural Agent pipeline (programmatic/internal callers only); M6 simply stops routing user queries there.

---

## 12. Open Questions

| # | Question | Impact |
|---|---|---|
| OQ-M6-02 | Should `<NexusChart>` support click-through drilling (click a bar to drill into that source's data)? Requires a follow-up query with an additional filter. | Feature scope vs. Iteration 2 timeline |
| OQ-M6-03 | Dashboard component grid — should components from different tenants ever appear on the same dashboard for platform admins? | Multi-tenant UX |
| OQ-M6-04 | What is the maximum number of dashboard components per user? At 20+ components the grid becomes unwieldy. Recommend pagination or category grouping. | UX / performance |

---

*NEXUS Iteration 2 · M6 Frontend Delta Spec · v0.2 · Mentis Consulting · April 2026 · Confidential*
