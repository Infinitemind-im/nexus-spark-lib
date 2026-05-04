# Service Spec: `nexus-m2-api`
**Module:** M2 — AI Intelligence Hub
**Type:** Request-driven API + WebSocket relay · Kubernetes `Deployment`
**Team:** AI & Knowledge
**Version:** 1.0 · March 2026 · Confidential

---

## Purpose

Single HTTP entry point for user queries. Receives natural language queries from M6, returns a `session_id` immediately, and delivers the asynchronous agent response over a persistent WebSocket connection. All LLM reasoning and knowledge store access happens in `nexus-m2-executor`. This service is stateless except for the WebSocket connection registry in Redis.

---

## Multi-Tenancy Model

### Tenant Identity Source
`tenant_id` is always sourced from the `X-Tenant-ID` header injected by Kong. It is never read from the request body or derived from the session.

### Query Session Isolation
Each query session is scoped to a single tenant. The session record in `nexus_system.query_sessions` is RLS-protected — a query that omits the `WHERE tenant_id = $1` clause will still return zero rows for the wrong tenant.

```python
@router.post("/api/v1/query")
async def submit_query(
    body: QueryRequest,
    x_tenant_id: str = Header(...),
    x_user_id:   str = Header(...),
):
    session_id = str(uuid.uuid4())

    async with get_tenant_scoped_connection(pool, x_tenant_id) as conn:
        await conn.execute("""
            INSERT INTO nexus_system.query_sessions
                (session_id, tenant_id, user_id, query_text, status)
            VALUES ($1, $2, $3, $4, 'pending')
        """, session_id, x_tenant_id, x_user_id, body.query)

    producer.publish(NexusMessage(
        topic=CrossModuleTopicNamer.m2(x_tenant_id, "knowledge_query"),
        tenant_id=x_tenant_id,
        payload={
            "session_id":  session_id,
            "user_id":     x_user_id,
            "query_text":  body.query,
            "context":     body.context or {},
        },
    ))

    return {"session_id": session_id, "status": "pending"}
```

### WebSocket Session Registry — Redis Isolation
When a client opens a WebSocket connection, this service registers the mapping `{tenant_id}:{session_id} → pod_id` in Redis. This allows the Kafka relay consumer to route the agent response to the correct pod — and the correct WebSocket connection — when the response arrives.

The Redis key always includes `tenant_id` as the namespace prefix. A relay consumer handling a response for tenant A will only look up Redis keys prefixed with `A:`, preventing cross-tenant routing.

```python
# On WebSocket connect
await redis.setex(
    f"{tenant_id}:{session_id}",   # Key includes tenant_id
    ttl=300,                        # 5-minute TTL — WebSocket must heartbeat
    value=pod_id,
)

# On agent_response_ready (Kafka relay consumer)
pod_id = await redis.get(f"{message.tenant_id}:{payload['session_id']}")
if pod_id == MY_POD_ID:
    await websocket_manager.send(payload["session_id"], response)
```

### WebSocket Authentication
The WebSocket handshake at `/ws/chat/{session_id}` requires the same JWT as HTTP endpoints. Kong validates the JWT and injects `X-Tenant-ID` before the request reaches the pod. If the `session_id` in the URL path does not belong to the authenticated tenant (RLS query returns nothing), the WebSocket is rejected with close code 4003.

```python
@router.websocket("/ws/chat/{session_id}")
async def websocket_endpoint(
    websocket: WebSocket,
    session_id: str,
    x_tenant_id: str = Header(...),
):
    # Verify session belongs to authenticated tenant
    async with get_tenant_scoped_connection(pool, x_tenant_id) as conn:
        session = await conn.fetchrow("""
            SELECT session_id, status FROM nexus_system.query_sessions
            WHERE session_id = $1   -- RLS ensures only this tenant's sessions visible
        """, session_id)

    if not session:
        await websocket.close(code=4003, reason="session_not_found_or_not_authorized")
        return

    await websocket.accept()
    # Register in Redis, then wait for relay consumer to push the response
    ...
```

---

## Endpoints

| Method | Path | Auth | Description |
|---|---|---|---|
| `POST` | `/api/v1/query` | JWT | Submit NL query, returns `session_id` immediately |
| `GET` | `/api/v1/sessions/{session_id}` | JWT | Poll session status (fallback if WebSocket unavailable) |
| `GET` | `/ws/chat/{session_id}` | JWT (WebSocket upgrade) | Persistent connection for receiving agent response |
| `GET` | `/health` | None | Liveness + readiness |
| `GET` | `/metrics` | Internal | Prometheus metrics |

### Request Contract — `POST /api/v1/query`

```json
// Request
{
  "query": "Show me open incidents from last week by severity",
  "context": {
    "preferred_entities": ["incident"],
    "max_results": 20
  }
}

// Response 202 — query accepted, not yet processed
{
  "session_id": "sess_a3f9c1d2-...",
  "status": "pending",
  "websocket_url": "wss://api.nexus.internal/ws/chat/sess_a3f9c1d2-..."
}
```

### WebSocket Message Contract

The WebSocket delivers a single JSON message when the agent completes:

```json
{
  "type": "agent_response",
  "session_id": "sess_a3f9c1d2-...",
  "status": "completed",
  "response_text": "Last week there were 14 open incidents...",
  "sources": [
    {"entity_type": "incident", "cdm_id": "inc_abc123", "name": "Prod DB outage"}
  ],
  "reasoning_trace": [
    "Queried M3 vector store for 'open incidents'",
    "Applied time filter: last 7 days",
    "Resolved 14 matching incidents from TimescaleDB"
  ],
  "processing_time_ms": 4820,
  "model_used": "claude-3-5-sonnet"
}
```

On error:
```json
{
  "type": "agent_error",
  "session_id": "sess_a3f9c1d2-...",
  "status": "failed",
  "error_code": "query_timeout",
  "error_message": "Query processing exceeded 30 second limit"
}
```

---

## Kafka Topics

### Produced

| Topic | When |
|---|---|
| `{tid}.m2.knowledge_query` | On every accepted query — triggers `nexus-m2-executor` |

### Consumed

| Topic | Consumer group | Purpose |
|---|---|---|
| `{tid}.m2.agent_response_ready` | `m2-api-websocket-relay` | Forward completed agent responses to open WebSocket connections |

The relay consumer processes messages for **all tenants** in a single consumer group. Per-message routing to the correct WebSocket connection is handled by the Redis registry lookup using the `{tenant_id}:{session_id}` key.

---

## Storage Dependencies

| Store | Usage | Tenant isolation |
|---|---|---|
| PostgreSQL `nexus_system.query_sessions` | Write session on query submit; read for status and WebSocket validation | RLS-scoped |
| Redis | WebSocket session registry: `{tenant_id}:{session_id} → pod_id` | Key prefix is `tenant_id` — no cross-tenant lookup possible |

### query_sessions table

```sql
CREATE TABLE nexus_system.query_sessions (
    session_id    TEXT PRIMARY KEY,
    tenant_id     TEXT NOT NULL,
    user_id       TEXT NOT NULL,
    query_text    TEXT NOT NULL,
    status        TEXT NOT NULL DEFAULT 'pending',
    -- status: pending | processing | completed | failed | timeout
    response_text TEXT,
    sources       JSONB,
    reasoning_trace JSONB,
    model_used    TEXT,
    error_code    TEXT,
    created_at    TIMESTAMPTZ DEFAULT NOW(),
    completed_at  TIMESTAMPTZ,
    processing_time_ms INT
);
ALTER TABLE nexus_system.query_sessions ENABLE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON nexus_system.query_sessions
    FOR ALL TO nexus_app
    USING (tenant_id = current_setting('nexus.current_tenant_id', true));
CREATE INDEX idx_query_sessions_tenant_status
    ON nexus_system.query_sessions (tenant_id, status, created_at DESC);
```

---

## Kubernetes Manifests

### Deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: nexus-m2-api
  namespace: nexus-app
  labels:
    app: nexus-m2-api
    module: m2
    type: api
spec:
  replicas: 2
  selector:
    matchLabels:
      app: nexus-m2-api
  template:
    metadata:
      labels:
        app: nexus-m2-api
        module: m2
      annotations:
        prometheus.io/scrape: "true"
        prometheus.io/port: "8003"
        prometheus.io/path: "/metrics"
    spec:
      serviceAccountName: nexus-m2-api-sa
      automountServiceAccountToken: false
      terminationGracePeriodSeconds: 30
      containers:
        - name: m2-api
          image: nexus/m2-api:latest
          command: ["uvicorn", "m2.api.main:app", "--host", "0.0.0.0", "--port", "8003",
                    "--workers", "2", "--ws", "websockets"]
          ports:
            - containerPort: 8003
              name: http
          env:
            - name: POSTGRES_DSN
              valueFrom:
                secretKeyRef:
                  name: nexus-postgres-credentials
                  key: dsn
            - name: REDIS_URL
              valueFrom:
                secretKeyRef:
                  name: nexus-redis-credentials
                  key: url
            - name: KAFKA_BOOTSTRAP_SERVERS
              valueFrom:
                configMapKeyRef:
                  name: nexus-platform-config
                  key: kafka_bootstrap_servers
            - name: POD_NAME
              valueFrom:
                fieldRef:
                  fieldPath: metadata.name    # Used as pod_id in Redis registry
            - name: QUERY_TIMEOUT_SECONDS
              value: "30"
          resources:
            requests:
              cpu: 200m
              memory: 256Mi
            limits:
              cpu: 1000m
              memory: 512Mi
          livenessProbe:
            httpGet:
              path: /health
              port: 8003
            initialDelaySeconds: 10
            periodSeconds: 15
          readinessProbe:
            httpGet:
              path: /health
              port: 8003
            initialDelaySeconds: 5
            periodSeconds: 10
```

### HPA

```yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: nexus-m2-api-hpa
  namespace: nexus-app
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: nexus-m2-api
  minReplicas: 2
  maxReplicas: 5
  metrics:
    - type: Resource
      resource:
        name: cpu
        target:
          type: Utilization
          averageUtilization: 60
```

### NetworkPolicy

```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: nexus-m2-api-netpol
  namespace: nexus-app
spec:
  podSelector:
    matchLabels:
      app: nexus-m2-api
  policyTypes:
    - Ingress
    - Egress
  ingress:
    - from:
        - namespaceSelector:
            matchLabels:
              name: nexus-infra    # Kong
      ports:
        - port: 8003
    - from:
        - namespaceSelector:
            matchLabels:
              name: nexus-monitoring
      ports:
        - port: 8003
  egress:
    - to:
        - namespaceSelector:
            matchLabels:
              name: nexus-data
      ports:
        - port: 5432    # PostgreSQL
        - port: 9092    # Kafka
        - port: 6379    # Redis
    # No outbound to source systems, no LLM APIs — all that is in nexus-m2-executor
```

---

## Environment Variables

| Variable | Source | Description |
|---|---|---|
| `POSTGRES_DSN` | Secrets Manager | PostgreSQL DSN |
| `REDIS_URL` | Secrets Manager | Redis connection URL |
| `KAFKA_BOOTSTRAP_SERVERS` | ConfigMap | Broker addresses |
| `POD_NAME` | Downward API | Self-identification in Redis WebSocket registry |
| `QUERY_TIMEOUT_SECONDS` | ConfigMap | Max wait time before sending timeout error via WebSocket (default: `30`) |

---

## Observability

### Prometheus Metrics

| Metric | Type | Labels |
|---|---|---|
| `m2_api_queries_submitted_total` | Counter | `tenant_id` |
| `m2_api_websocket_connections_active` | Gauge | `tenant_id` |
| `m2_api_websocket_relay_latency_seconds` | Histogram | `tenant_id` |
| `m2_api_query_timeout_total` | Counter | `tenant_id` |
| `m2_api_http_request_duration_seconds` | Histogram | `method`, `path`, `status_code` |

---

## Acceptance Tests

```bash
# Test 1 — Submit query, receive response via WebSocket
wscat -c "wss://api.nexus.internal/ws/chat/$SESSION_ID" \
  -H "Authorization: Bearer $ALPHA_JWT" &

curl -X POST https://api.nexus.internal/api/v1/query \
  -H "Authorization: Bearer $ALPHA_JWT" \
  -H "Content-Type: application/json" \
  -d '{"query": "Show me recent incidents"}'
# Expected: WebSocket receives agent_response within 15s

# Test 2 — Tenant A cannot access Tenant B's session via WebSocket
curl -X POST https://api.nexus.internal/api/v1/query \
  -H "Authorization: Bearer $BETA_JWT" \
  -H "Content-Type: application/json" \
  -d '{"query": "test"}' | jq '.session_id'
# Store BETA_SESSION_ID

# Now try to connect to beta's session using alpha's JWT
wscat -c "wss://api.nexus.internal/ws/chat/$BETA_SESSION_ID" \
  -H "Authorization: Bearer $ALPHA_JWT"
# Expected: WebSocket closes with code 4003 (not authorized)

# Test 3 — Status poll fallback
curl https://api.nexus.internal/api/v1/sessions/$SESSION_ID \
  -H "Authorization: Bearer $ALPHA_JWT" | jq '.status'
# Expected: "completed" after executor finishes

# Test 4 — Redis registry correctness
# After submitting a query, check Redis:
redis-cli -u $REDIS_URL GET "test-tenant-alpha:$SESSION_ID"
# Expected: pod name of one of the m2-api replicas
```

---

*nexus-m2-api · Service Specification · Mentis Consulting · March 2026*
