# Query & Frontend

Specifications for the user-facing query layer and the React frontend that presents results. These three specs describe how a natural-language question travels from the browser, through the query engine, and back as a rendered chart, table, or report.

`nexus-query-api` is the **sole user-facing query surface** in Iteration 2. The M2 RHMA chat panel is deprecated; all user queries go through this service.

---

## Files

**`NEXUS-Iter2-QueryEngine-v0.3.md`**
Specifies `nexus-query-api` (HTTP and WebSocket entry point) and `nexus-query-executor` (the internal execution engine). The query planner decomposes a natural-language question into typed sub-queries; OPA enforces tenant-scoped authorisation at the field level; the parallel executor fans out to the AI stores and connector workers simultaneously; the result merger assembles the responses into a coherent answer. The query engine reads the `entity_store_presence` register (owned by Dev 5) to know which stores hold data for a given entity before dispatching. Export and dashboard endpoints are also defined here.

**`NEXUS-Iter2-VisualOutputs-v0.2.md`**
Defines the rendering layer that turns raw query results into visualisations. Introduces the `RenderedOutput` schema (the common envelope wrapping any result type), `ChartSpec` (a declarative chart definition covering bar, line, scatter, and heatmap types), persona overrides (different visualisation defaults per user role), the export service (produces PDF, CSV, and XLSX from any `RenderedOutput`), and the `ReportBuilder` that assembles multi-section documents from multiple query results. This spec is consumed by both the frontend and the query executor.

**`NEXUS-Iter2-M6-FrontendDelta-v0.2.md`**
A delta spec covering only what changes in the React frontend for Iteration 2 — it does not re-describe existing components. Defines the new TypeScript types aligned with `RenderedOutput` and `ChartSpec`, the `useQueryStream` hook that manages the WebSocket connection to `nexus-query-api`, the list of new and modified components (query input panel, streaming result renderer, chart container, dashboard grid, export controls), and the dashboard grid layout system. The M2 RHMA chat panel is formally deprecated here in favour of the query panel.

---

*NEXUS Iteration 2 · Mentis Consulting · April 2026*
