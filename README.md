# nexus-spark-lib

Shared PySpark transformation library for the NEXUS platform pipeline.

## Requirements

- Python 3.11 or 3.12
- **Java 17** (required by PySpark 3.5 — Java 21+ is not supported due to a Hadoop incompatibility)
- PySpark 3.5.x

> **Java version**: Hadoop 3.3.4 (bundled with PySpark 3.5) calls `Subject.getSubject(AccessControlContext)`, which was removed in Java 21. Install [Temurin 17](https://adoptium.net/temurin/releases/?version=17) and set `JAVA_HOME`.

## Installation

```bash
# Production dependencies only
pip install -e .

# With dev/test dependencies
pip install -e ".[dev]"
```

## Pipeline stages

Records flow through four stages in order:

| Stage | Module | Purpose |
|-------|--------|---------|
| Stage 0 | `transform.stage0_materialization` | Materialization gate — HOT / WARM / COLD tier decision on raw payload, resolves cdm_entity_type |
| Stage 1 | `transform.stage1_normalise` | Type coercion, FX conversion, DQ scoring, blocking key, deduplication |
| Stage 2 | `transform.stage2_resolve` | Entity Resolution — deterministic + fuzzy signals + state machine |
| Stage 3 | `transform.stage3_synthesise` | Survivorship — build Golden Record from provenance |

> Stage 0 **must** run before Stage 1. The materialization gate resolves `cdm_entity_type` and sets the tier on the raw payload; Stage 1 normalise uses the already-resolved `cdm_entity_type` rather than re-deriving it.

## Running tests

```bash
# All tests
make test

# Unit tests only (no Spark required)
make test-unit

# With coverage
make test-cov
```

### Spark integration tests (Java 17 required)

```powershell
# Windows — set JAVA_HOME before running
$env:JAVA_HOME = "C:\Program Files\Eclipse Adoptium\jdk-17.x.x"
$env:PATH = "$env:JAVA_HOME\bin;$env:PATH"
pytest tests/integration/ -v
```

## Development commands

```bash
make install-dev   # install with dev deps
make lint          # ruff linter
make format        # auto-format with ruff
make typecheck     # mypy type check
make build         # build wheel
make clean         # remove build artifacts
```

## Key dependencies

| Package | Purpose |
|---------|---------|
| `pyspark>=3.5,<4.0` | Spark runtime |
| `nexus-core` | `NexusMessage`, `CrossModuleTopicNamer`, `get_tenant_scoped_connection`, `FieldQuality` |
| `asyncpg` | Async PostgreSQL writes from Spark executors |
| `neo4j` | Graph store for Signal C entity resolution |
| `jellyfish` | Jaro-Winkler + Levenshtein similarity (Signal B) |
| `metaphone` | Phonetic matching (Signal B) |
| `datasketch` | LSH blocking for fan-out reduction |
| `prometheus-client` | Metrics |
| `opentelemetry-sdk` | Tracing |

## Package layout

```
nexus_spark_lib/
  transform/
    stage0_materialization.py  # Stage 0 — materialization gate (resolves cdm_entity_type + tier)
    stage1_normalise.py        # Stage 1 — normalisation (type coercion, FX, DQ, dedup)
    stage2_resolve/            # Stage 2 — entity resolution
    stage3_synthesise.py       # Stage 3 — golden record synthesis
  db/
    er_index.py                # entity_resolution_index CRUD
    golden_records.py          # provenance + golden record CRUD
    redirects.py               # GR redirect table
  models/                      # Pydantic models (ErTypes, RawRecord, etc.)
  observability/
    metrics.py                 # Prometheus counters/histograms
    tracing.py                 # OpenTelemetry spans
  kafka/                       # Kafka reader/writer helpers
  broadcast/                   # CDM mapping + FX rate broadcast helpers
  config/                      # Pydantic settings
```
