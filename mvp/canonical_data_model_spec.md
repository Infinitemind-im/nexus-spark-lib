# Functional Specifications: Canonical Data Model Library
### For a Federated SQL Query System

**Version:** 1.0.0-draft
**Date:** 2026-04-16
**Status:** Draft

---

## Table of Contents

1. [Introduction](#1-introduction)
2. [Glossary](#2-glossary)
3. [System Context](#3-system-context)
4. [Library Architecture Overview](#4-library-architecture-overview)
5. [Module 1 — Canonical Data Model (CDM) Core](#5-module-1--canonical-data-model-cdm-core)
6. [Module 2 — Enriched Database Profile Ingestion](#6-module-2--enriched-database-profile-ingestion)
7. [Module 3 — Reference Entity Model (REM)](#7-module-3--reference-entity-model-rem)
8. [Module 4 — Data Mapping Engine](#8-module-4--data-mapping-engine)
9. [Module 5 — Performance & Quality Measurement Library](#9-module-5--performance--quality-measurement-library)
10. [Cross-Module Interfaces](#10-cross-module-interfaces)
11. [Error Handling & Validation](#11-error-handling--validation)
12. [Extensibility & Plugin Model](#12-extensibility--plugin-model)
13. [Non-Functional Requirements](#13-non-functional-requirements)
14. [Appendix A — Type System Reference](#14-appendix-a--type-system-reference)
15. [Appendix B — Example Workflows](#15-appendix-b--example-workflows)

---

## 1. Introduction

### 1.1 Purpose

This document defines the functional specifications for a Python library — hereafter referred to as the **Canonical Data Model (CDM) Library** — that serves as the semantic backbone of a federated SQL query system. The library is responsible for:

- Representing a shared, source-agnostic **Canonical Data Model** against which all federated sources are mapped.
- Ingesting and interpreting **Enriched Database Profiles** (EDPs) that describe the structure, statistics, and semantics of individual SQL data sources.
- Maintaining a **Reference Entity Model** (REM) that defines the authoritative set of business entities and their relationships.
- Generating explicit, validated **data mappings** from each source schema to the canonical model.
- Optionally measuring the **quality and performance** of those mappings when ground truth data is available.

### 1.2 Scope

This specification covers:

- The Python-level API surface of the library.
- Data structures for the CDM, EDP, REM, mappings, and quality metrics.
- Validation and error-handling contracts.
- Extension points for custom type resolvers, mapping strategies, and metric collectors.

This specification **does not** cover:

- The federated query execution engine itself (this library produces artefacts consumed by that engine).
- Storage backends or serialisation wire formats beyond the Python object model and standard JSON/YAML.
- Authentication or connection management for individual SQL sources.

### 1.3 Intended Audience

- **Data engineers** building or extending the federated query system.
- **Platform architects** designing the overall data integration landscape.
- **Library contributors** implementing or reviewing modules.

---

## 2. Glossary

| Term | Definition |
|---|---|
| **Canonical Data Model (CDM)** | The single, source-agnostic schema that all federated sources are normalised to. Acts as the lingua franca of the system. |
| **Enriched Database Profile (EDP)** | A structured representation of a SQL data source including its schema (tables, columns, data types), statistical summaries (cardinality, null rates, value distributions), and optionally semantic tags. |
| **Reference Entity Model (REM)** | An authoritative registry of named business entities (e.g. `Customer`, `Order`, `Product`) with typed attributes and declared relationships. The REM is the basis for CDM entity definitions. |
| **Mapping** | A declarative artefact that describes how a column or expression in a source EDP corresponds to an attribute in the CDM. |
| **Ground Truth Dataset** | A curated, labelled dataset of known-correct mappings used to evaluate mapping quality. |
| **Federated Query** | A query executed across multiple heterogeneous SQL sources via a unified CDM layer. |
| **Confidence Score** | A numeric value in `[0.0, 1.0]` representing the estimated correctness of a generated mapping. |

---

## 3. System Context

```
┌──────────────────────────────────────────────────────────────┐
│                     Federated Query Engine                    │
│  (consumes CDM definitions + resolved mappings at query time) │
└───────────────────────────┬──────────────────────────────────┘
                            │  Mapping Artefacts
                            ▼
┌──────────────────────────────────────────────────────────────┐
│                   CDM Library  (this spec)                    │
│                                                              │
│  ┌─────────────┐  ┌─────────────┐  ┌──────────────────────┐ │
│  │  CDM Core   │  │ EDP Ingest  │  │  Reference Entity    │ │
│  │  (Module 1) │  │ (Module 2)  │  │  Model  (Module 3)   │ │
│  └──────┬──────┘  └──────┬──────┘  └──────────┬───────────┘ │
│         │                │                     │             │
│         └────────────────▼─────────────────────┘             │
│                          │                                   │
│                ┌─────────▼──────────┐                        │
│                │  Mapping Engine    │                        │
│                │   (Module 4)       │                        │
│                └─────────┬──────────┘                        │
│                          │                                   │
│                ┌─────────▼──────────┐                        │
│                │  Perf & Quality    │                        │
│                │  Library (Mod. 5)  │ ◄── Ground Truth       │
│                └────────────────────┘                        │
└──────────────────────────────────────────────────────────────┘
         ▲                           ▲
  Enriched DB Profiles         REM definitions
  (JSON/YAML from profiler)    (YAML/Python DSL)
```

---

## 4. Library Architecture Overview

### 4.1 Package Structure

```
cdm_library/
├── core/
│   ├── types.py          # Canonical type system
│   ├── model.py          # CDMEntity, CDMAttribute, CDMRelationship
│   └── registry.py       # CDMRegistry (singleton + multi-tenant)
├── profile/
│   ├── schema.py         # EDP data classes (Table, Column, Stats)
│   ├── loader.py         # Load EDP from JSON/YAML/dict
│   └── validator.py      # EDP structural validation
├── rem/
│   ├── model.py          # REMEntity, REMAttribute, REMRelationship
│   ├── loader.py         # Load REM from YAML/Python DSL
│   └── validator.py      # REM structural validation
├── mapping/
│   ├── model.py          # MappingRule, MappingSet, MappingStatus
│   ├── engine.py         # MappingEngine (strategy-based)
│   ├── strategies/
│   │   ├── base.py       # AbstractMappingStrategy
│   │   ├── name_match.py # Lexical / fuzzy name matching
│   │   ├── type_match.py # Type-compatibility scoring
│   │   └── stats_match.py# Statistical profile alignment
│   └── resolver.py       # MappingResolver (conflict resolution)
├── quality/
│   ├── metrics.py        # MappingQualityMetrics dataclass
│   ├── evaluator.py      # MappingEvaluator (against ground truth)
│   ├── benchmark.py      # PerformanceBenchmark (latency, throughput)
│   └── report.py         # QualityReport generator
├── io/
│   ├── serialise.py      # JSON/YAML serialisation
│   └── export.py         # Export to engine-specific formats
└── exceptions.py         # All library exceptions
```

### 4.2 Core Design Principles

- **Immutability by default.** CDM entities and mapping rules are frozen dataclasses once committed to the registry. Mutations create new versioned instances.
- **Explicit over implicit.** All mappings carry a `MappingStatus` (CONFIRMED, INFERRED, CONFLICTED, UNMAPPED) and a `confidence_score`. Nothing is silently assumed.
- **Strategy pattern for mapping generation.** Mapping logic is decomposed into pluggable `MappingStrategy` objects so teams can inject domain-specific heuristics.
- **Separation of generation and validation.** Generating a mapping and validating it are distinct operations, both callable independently.
- **Ground truth decoupled.** The quality module is entirely optional; the library functions fully without it.

---

## 5. Module 1 — Canonical Data Model (CDM) Core

### 5.1 Purpose

Define and manage the shared canonical schema to which all SQL sources are mapped. This module provides the core data structures, type system, and registry.

### 5.2 Canonical Type System

The CDM type system is a normalised superset of common SQL types. Each canonical type has a set of **compatible source types** used during mapping validation.

| Canonical Type | Python Representation | Compatible SQL Types |
|---|---|---|
| `CDMString` | `str` | `VARCHAR`, `TEXT`, `CHAR`, `NVARCHAR`, `CLOB` |
| `CDMInteger` | `int` | `INTEGER`, `INT`, `BIGINT`, `SMALLINT`, `TINYINT` |
| `CDMDecimal` | `Decimal` | `DECIMAL`, `NUMERIC`, `FLOAT`, `DOUBLE`, `REAL` |
| `CDMBoolean` | `bool` | `BOOLEAN`, `BIT`, `TINYINT(1)` |
| `CDMDate` | `datetime.date` | `DATE` |
| `CDMDateTime` | `datetime.datetime` | `TIMESTAMP`, `DATETIME`, `TIMESTAMPTZ` |
| `CDMTime` | `datetime.time` | `TIME`, `TIMETZ` |
| `CDMBinary` | `bytes` | `BLOB`, `BYTEA`, `BINARY`, `VARBINARY` |
| `CDMJson` | `dict` | `JSON`, `JSONB` |
| `CDMUUID` | `uuid.UUID` | `UUID`, `VARCHAR(36)` (pattern-validated) |
| `CDMEnum` | `enum.Enum` subclass | `ENUM`, `VARCHAR` with check constraints |
| `CDMArray` | `list` | `ARRAY`, `JSON arrays` |

### 5.3 CDMAttribute

```python
@dataclass(frozen=True)
class CDMAttribute:
    name: str                          # snake_case canonical name
    canonical_type: CDMType            # One of the types above
    nullable: bool = True
    is_primary_key: bool = False
    is_foreign_key: bool = False
    description: str = ""
    tags: frozenset[str] = field(default_factory=frozenset)
    constraints: tuple[CDMConstraint, ...] = ()
    version: str = "1.0.0"
```

### 5.4 CDMEntity

```python
@dataclass(frozen=True)
class CDMEntity:
    name: str                              # PascalCase, e.g. "Customer"
    attributes: tuple[CDMAttribute, ...]
    primary_key: tuple[str, ...]           # attribute names
    description: str = ""
    tags: frozenset[str] = field(default_factory=frozenset)
    version: str = "1.0.0"
    deprecated: bool = False
```

**Functional Requirements:**

- `FR-CDM-01`: Every `CDMEntity` must have at least one attribute designated as part of the primary key.
- `FR-CDM-02`: Attribute names within a single entity must be unique (case-insensitive comparison at validation time).
- `FR-CDM-03`: The `CDMRegistry` must support versioned snapshots of entity definitions.
- `FR-CDM-04`: An entity may be marked `deprecated=True`; the registry will issue a warning on access but continue to serve it.

### 5.5 CDMRelationship

```python
@dataclass(frozen=True)
class CDMRelationship:
    name: str
    from_entity: str                  # CDMEntity name
    from_attributes: tuple[str, ...]  # attribute names acting as FK
    to_entity: str                    # CDMEntity name
    to_attributes: tuple[str, ...]    # attribute names acting as PK target
    cardinality: Cardinality          # ONE_TO_ONE | ONE_TO_MANY | MANY_TO_MANY
    optional: bool = True
```

### 5.6 CDMRegistry

The registry is the central store for CDM entities and relationships.

```python
class CDMRegistry:
    def register_entity(self, entity: CDMEntity) -> None: ...
    def register_relationship(self, rel: CDMRelationship) -> None: ...
    def get_entity(self, name: str, version: str = "latest") -> CDMEntity: ...
    def list_entities(self, tags: set[str] | None = None) -> list[CDMEntity]: ...
    def list_relationships(self, entity: str | None = None) -> list[CDMRelationship]: ...
    def snapshot(self) -> CDMSnapshot: ...               # Immutable point-in-time copy
    def diff(self, other: CDMRegistry) -> CDMDiff: ...  # Structural diff between registries
```

**Functional Requirements:**

- `FR-REG-01`: Registering an entity with the same `name` and `version` as an existing entry must raise `DuplicateEntityError` unless `overwrite=True` is passed.
- `FR-REG-02`: The registry must support serialisation to and deserialisation from JSON and YAML.
- `FR-REG-03`: `CDMRegistry.snapshot()` must be a deep, immutable copy — mutations to the live registry must not affect previously taken snapshots.

---

## 6. Module 2 — Enriched Database Profile Ingestion

### 6.1 Purpose

Ingest, validate, and expose structured representations of SQL database schemas alongside their statistical and semantic enrichments. The EDP is the primary source-side input to the mapping engine.

### 6.2 Enriched Database Profile Structure

```python
@dataclass
class ColumnProfile:
    name: str
    sql_type: str                        # Raw SQL type string, e.g. "VARCHAR(255)"
    nullable: bool
    is_primary_key: bool
    is_foreign_key: bool
    foreign_key_reference: FKReference | None = None

    # Statistical enrichments (all optional)
    row_count: int | None = None
    null_count: int | None = None
    null_rate: float | None = None       # null_count / row_count
    distinct_count: int | None = None
    cardinality_ratio: float | None = None
    min_value: Any | None = None
    max_value: Any | None = None
    mean_value: float | None = None
    sample_values: list[Any] = field(default_factory=list)
    value_frequencies: dict[Any, int] = field(default_factory=dict)

    # Semantic enrichments (optional)
    semantic_tags: list[str] = field(default_factory=list)
    inferred_concept: str | None = None  # e.g. "email_address", "currency_code"
    description: str = ""
```

```python
@dataclass
class TableProfile:
    name: str
    schema_name: str | None
    columns: list[ColumnProfile]
    row_count: int | None = None
    description: str = ""
    tags: list[str] = field(default_factory=list)
```

```python
@dataclass
class EnrichedDatabaseProfile:
    source_id: str                       # Unique identifier for this data source
    database_name: str
    dialect: SQLDialect                  # POSTGRESQL | MYSQL | MSSQL | SQLITE | GENERIC
    tables: list[TableProfile]
    profile_timestamp: datetime
    profiler_version: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)
```

### 6.3 EDP Loader

```python
class EDPLoader:
    @staticmethod
    def from_dict(data: dict) -> EnrichedDatabaseProfile: ...
    @staticmethod
    def from_json(path: str | Path) -> EnrichedDatabaseProfile: ...
    @staticmethod
    def from_yaml(path: str | Path) -> EnrichedDatabaseProfile: ...
    @staticmethod
    def from_sqlalchemy_engine(
        engine: Engine,
        profiler: BaseProfiler | None = None
    ) -> EnrichedDatabaseProfile: ...
```

### 6.4 EDP Validator

```python
class EDPValidator:
    def validate(self, profile: EnrichedDatabaseProfile) -> ValidationResult: ...
```

**Validation Rules:**

- `FR-EDP-01`: `source_id` must be non-empty and unique within a mapping session.
- `FR-EDP-02`: Column names within a table must be unique (case-insensitive).
- `FR-EDP-03`: If `null_rate` is present, it must satisfy `0.0 ≤ null_rate ≤ 1.0`.
- `FR-EDP-04`: If `cardinality_ratio` is present, it must satisfy `0.0 ≤ cardinality_ratio ≤ 1.0`.
- `FR-EDP-05`: Foreign key references must point to tables and columns that exist within the same EDP (intra-source) or are registered in a cross-source FK registry (inter-source, optional).
- `FR-EDP-06`: At least one table must be present in a valid EDP.

---

## 7. Module 3 — Reference Entity Model (REM)

### 7.1 Purpose

The REM is the authoritative, business-defined ontology of entities. It acts as the bridge between the CDM type layer and domain meaning. The mapping engine uses the REM to anchor generated mappings to semantic concepts.

### 7.2 REM Structure

```python
@dataclass(frozen=True)
class REMAttribute:
    name: str
    canonical_type: CDMType
    description: str = ""
    aliases: tuple[str, ...] = ()        # Known alternative names in source systems
    semantic_concept: str | None = None  # e.g. "email_address", "iso_4217_currency"
    nullable: bool = True
    is_identifier: bool = False
    example_values: tuple[Any, ...] = ()
    tags: frozenset[str] = field(default_factory=frozenset)
```

```python
@dataclass(frozen=True)
class REMEntity:
    name: str                               # Matches CDMEntity.name
    attributes: tuple[REMAttribute, ...]
    identifier_attributes: tuple[str, ...]  # attribute names
    description: str = ""
    aliases: tuple[str, ...] = ()           # Known table name aliases in source DBs
    tags: frozenset[str] = field(default_factory=frozenset)
    version: str = "1.0.0"
```

### 7.3 REM Loader

The REM supports two authoring formats:

**YAML format (recommended for non-developers):**

```yaml
entities:
  - name: Customer
    description: A person or organisation that has placed at least one order.
    aliases: [client, account, user]
    identifier_attributes: [customer_id]
    attributes:
      - name: customer_id
        type: CDMInteger
        is_identifier: true
      - name: email_address
        type: CDMString
        semantic_concept: email_address
        aliases: [email, e_mail, contact_email]
      - name: registered_at
        type: CDMDateTime
        aliases: [created_at, signup_date, registration_date]
```

**Python DSL (recommended for programmatic generation):**

```python
from cdm_library.rem import REMBuilder

rem = (
    REMBuilder()
    .entity("Customer", description="...", aliases=["client", "account"])
    .attribute("customer_id", CDMInteger, is_identifier=True)
    .attribute("email_address", CDMString, semantic_concept="email_address",
               aliases=["email", "e_mail"])
    .build()
)
```

### 7.4 REM Validator

**Functional Requirements:**

- `FR-REM-01`: Every REM entity name must exactly match a registered `CDMEntity` name (after normalisation).
- `FR-REM-02`: Attribute types in the REM must be valid `CDMType` values.
- `FR-REM-03`: Identifier attributes must exist in the `attributes` list of the same entity.
- `FR-REM-04`: Entity aliases are case-insensitive; duplicates across entities trigger a `DuplicateAliasWarning` (not an error).

---

## 8. Module 4 — Data Mapping Engine

### 8.1 Purpose

Generate, validate, resolve conflicts in, and export mappings from one or more EDPs to the CDM, guided by the REM. This is the primary operational module of the library.

### 8.2 Mapping Model

```python
class MappingStatus(Enum):
    CONFIRMED  = "confirmed"   # Manually verified or high-confidence automatic
    INFERRED   = "inferred"    # Automatically generated, not yet verified
    CONFLICTED = "conflicted"  # Multiple candidates with similar scores
    UNMAPPED   = "unmapped"    # No satisfactory candidate found
    EXCLUDED   = "excluded"    # Explicitly excluded by user

@dataclass
class MappingRule:
    source_id: str
    source_table: str
    source_column: str
    source_sql_type: str

    target_entity: str         # CDMEntity name
    target_attribute: str      # CDMAttribute name

    status: MappingStatus
    confidence_score: float    # [0.0, 1.0]
    strategy_contributions: dict[str, float]  # strategy_name -> partial score
    transform_expression: str | None = None   # SQL expression, e.g. "CAST(x AS DATE)"
    notes: str = ""
    created_at: datetime = field(default_factory=datetime.utcnow)
    confirmed_by: str | None = None
```

```python
@dataclass
class MappingSet:
    source_id: str
    cdm_version: str
    rules: list[MappingRule]
    created_at: datetime
    coverage_rate: float       # proportion of CDM attributes with at least one CONFIRMED/INFERRED mapping
    metadata: dict[str, Any] = field(default_factory=dict)
```

### 8.3 Mapping Engine

```python
class MappingEngine:
    def __init__(
        self,
        cdm_registry: CDMRegistry,
        rem: REMModel,
        strategies: list[AbstractMappingStrategy] | None = None,
        confidence_threshold: float = 0.5,
        conflict_threshold: float = 0.1,  # if top-2 scores differ by < this, flag CONFLICTED
    ): ...

    def generate(
        self,
        profile: EnrichedDatabaseProfile,
        target_entities: list[str] | None = None,  # None = all CDM entities
    ) -> MappingSet: ...

    def generate_batch(
        self,
        profiles: list[EnrichedDatabaseProfile],
    ) -> dict[str, MappingSet]: ...  # keyed by source_id

    def confirm(
        self,
        mapping_set: MappingSet,
        rule_updates: list[MappingRuleUpdate],
        confirmed_by: str,
    ) -> MappingSet: ...

    def validate(self, mapping_set: MappingSet) -> MappingValidationResult: ...

    def export(
        self,
        mapping_set: MappingSet,
        format: ExportFormat,           # JSON | YAML | DBTMODEL | SQLALCHEMY
        output_path: Path | None = None,
    ) -> str | None: ...
```

### 8.4 Mapping Strategies

Each strategy implements the following interface:

```python
class AbstractMappingStrategy(ABC):
    name: str                    # e.g. "name_match"
    weight: float = 1.0          # relative contribution to final score

    @abstractmethod
    def score(
        self,
        column: ColumnProfile,
        cdm_attribute: CDMAttribute,
        rem_attribute: REMAttribute | None,
    ) -> float:
        """Return a score in [0.0, 1.0] for how well this column maps to this attribute."""
        ...
```

**Built-in Strategies:**

| Strategy | Description |
|---|---|
| `NameMatchStrategy` | Fuzzy lexical matching of column name against CDM attribute name, REM aliases, and semantic concepts. Uses token-based similarity (e.g. Jaro-Winkler). |
| `TypeCompatibilityStrategy` | Scores type alignment between source SQL type and CDM canonical type using the compatibility matrix in §5.2. Penalises lossy conversions. |
| `StatisticalProfileStrategy` | Leverages EDP statistics (null rate, cardinality, value distribution) to confirm or reject type and semantic hypotheses. E.g., low-cardinality strings may match `CDMEnum`. |
| `SemanticTagStrategy` | Direct match between EDP `inferred_concept` tags and REM `semantic_concept` values. Returns 1.0 on exact match, 0.0 otherwise. |
| `ForeignKeyStrategy` | Boosts confidence if FK relationships in the EDP align with CDM relationship declarations. |

**Composite Scoring:**

```
final_score = Σ (strategy.weight × strategy.score) / Σ strategy.weight
```

### 8.5 Mapping Resolver

When multiple source columns are candidates for a single CDM attribute:

```python
class MappingResolver:
    def resolve(
        self,
        candidates: list[MappingRule],
        policy: ResolutionPolicy,
    ) -> MappingRule: ...
```

**Resolution Policies:**

- `HIGHEST_SCORE`: Select the candidate with the highest `confidence_score`. If top-2 differ by less than `conflict_threshold`, mark as `CONFLICTED`.
- `REQUIRE_CONFIRMED`: Only accept rules with status `CONFIRMED`; all others become `UNMAPPED`.
- `ALLOW_MULTIPLE`: Permit multiple source columns to map to a single CDM attribute (produces an array-type CDM value at query time).

**Functional Requirements:**

- `FR-MAP-01`: The mapping engine must produce exactly one `MappingRule` per (source column, CDM attribute) pair considered — no duplicate rules within a `MappingSet`.
- `FR-MAP-02`: Every CDM attribute must appear in the `MappingSet`, even if its status is `UNMAPPED`.
- `FR-MAP-03`: `confidence_score` must always be in `[0.0, 1.0]`; the engine must clamp any strategy output outside this range.
- `FR-MAP-04`: A transform expression, if provided, must be syntactically valid SQL for the source dialect (validated via a lightweight parser or regex heuristic; full execution not required).
- `FR-MAP-05`: Batch generation must be embarrassingly parallel-safe; strategies may not share mutable state across profiles.
- `FR-MAP-06`: The engine must complete generation for a single EDP with up to 500 tables and 10 000 columns in under 30 seconds on a single CPU core (see §13).

---

## 9. Module 5 — Performance & Quality Measurement Library

### 9.1 Purpose

When a **ground truth dataset** is available (a curated set of verified mappings), this module enables quantitative evaluation of automatically generated `MappingSet` quality and library performance benchmarking.

This module is **optional** and has no runtime dependency on Modules 1–4 beyond the shared data models.

### 9.2 Ground Truth Format

```python
@dataclass
class GroundTruthEntry:
    source_id: str
    source_table: str
    source_column: str
    target_entity: str
    target_attribute: str
    is_positive: bool = True    # True = valid mapping; False = known non-mapping (hard negative)
    notes: str = ""
```

```python
@dataclass
class GroundTruthDataset:
    name: str
    version: str
    entries: list[GroundTruthEntry]
    created_at: datetime
    description: str = ""
```

Ground truth datasets are loaded from JSON/YAML:

```python
class GroundTruthLoader:
    @staticmethod
    def from_json(path: str | Path) -> GroundTruthDataset: ...
    @staticmethod
    def from_yaml(path: str | Path) -> GroundTruthDataset: ...
    @staticmethod
    def from_dataframe(df: "pd.DataFrame", column_map: dict[str, str]) -> GroundTruthDataset: ...
```

### 9.3 Mapping Quality Metrics

```python
@dataclass
class MappingQualityMetrics:
    source_id: str
    mapping_set_version: str
    ground_truth_version: str
    evaluated_at: datetime

    # Classification metrics (treating each (source_col, cdm_attr) pair as a binary decision)
    true_positives: int
    false_positives: int
    false_negatives: int
    true_negatives: int

    precision: float          # TP / (TP + FP)
    recall: float             # TP / (TP + FN)
    f1_score: float           # 2 * P * R / (P + R)
    accuracy: float           # (TP + TN) / total

    # Ranking metrics (treating mapping generation as a ranked retrieval task)
    mean_reciprocal_rank: float    # MRR over all CDM attributes
    mean_average_precision: float  # MAP over all CDM attributes
    ndcg_at_k: dict[int, float]   # NDCG@1, @3, @5, @10

    # Coverage metrics
    cdm_attribute_coverage: float  # proportion of CDM attrs with ≥1 CONFIRMED/INFERRED rule
    confirmed_rate: float          # proportion of rules with CONFIRMED status
    conflicted_rate: float         # proportion of rules with CONFLICTED status
    unmapped_rate: float           # proportion of CDM attrs with UNMAPPED status

    # Confidence calibration
    expected_calibration_error: float   # ECE: mean |confidence - accuracy| per bucket
    confidence_histogram: dict[str, int]  # bucket label -> count
```

### 9.4 Mapping Evaluator

```python
class MappingEvaluator:
    def __init__(self, ground_truth: GroundTruthDataset): ...

    def evaluate(
        self,
        mapping_set: MappingSet,
        k_values: list[int] = [1, 3, 5, 10],
    ) -> MappingQualityMetrics: ...

    def compare(
        self,
        mapping_sets: dict[str, MappingSet],   # label -> MappingSet
    ) -> ComparisonReport: ...

    def per_entity_breakdown(
        self,
        mapping_set: MappingSet,
    ) -> dict[str, MappingQualityMetrics]: ...  # CDMEntity name -> metrics

    def per_strategy_attribution(
        self,
        mapping_set: MappingSet,
    ) -> dict[str, StrategyContributionReport]: ...
```

### 9.5 Performance Benchmark

```python
class PerformanceBenchmark:
    def __init__(self, engine: MappingEngine): ...

    def run(
        self,
        profiles: list[EnrichedDatabaseProfile],
        repetitions: int = 5,
        warmup_runs: int = 1,
    ) -> BenchmarkReport: ...
```

```python
@dataclass
class BenchmarkReport:
    run_at: datetime
    engine_config: dict
    results: list[BenchmarkResult]

@dataclass
class BenchmarkResult:
    source_id: str
    table_count: int
    column_count: int
    wall_time_seconds: float
    cpu_time_seconds: float
    peak_memory_mb: float
    rules_generated: int
    rules_per_second: float
```

### 9.6 Quality Report Generator

```python
class QualityReportGenerator:
    def generate(
        self,
        metrics: MappingQualityMetrics | list[MappingQualityMetrics],
        benchmark: BenchmarkReport | None = None,
        output_format: ReportFormat = ReportFormat.JSON,
        output_path: Path | None = None,
    ) -> str: ...
```

Supported output formats: `JSON`, `YAML`, `MARKDOWN`, `HTML`.

**Functional Requirements:**

- `FR-QUA-01`: Evaluation must only consider entries in the ground truth that have a corresponding source EDP loaded in the session; mismatched entries must be reported as warnings, not errors.
- `FR-QUA-02`: Precision, recall, F1, and accuracy must be computed per CDM entity as well as globally.
- `FR-QUA-03`: Confidence calibration (ECE) must use 10 equal-width buckets over `[0.0, 1.0]`.
- `FR-QUA-04`: `ComparisonReport` must include a ranked leaderboard and a per-metric delta table.
- `FR-QUA-05`: The benchmark must measure and report both wall-clock time and CPU time independently.
- `FR-QUA-06`: Benchmark runs must be isolated; each run receives a fresh `MappingEngine` instance to avoid warm-cache effects.

---

## 10. Cross-Module Interfaces

### 10.1 Session Object

A `MappingSession` ties together all modules for a single end-to-end workflow:

```python
class MappingSession:
    def __init__(
        self,
        cdm_registry: CDMRegistry,
        rem: REMModel,
        engine_config: EngineConfig | None = None,
    ): ...

    def add_profile(self, profile: EnrichedDatabaseProfile) -> None: ...
    def remove_profile(self, source_id: str) -> None: ...

    def generate_mappings(
        self,
        target_entities: list[str] | None = None,
    ) -> dict[str, MappingSet]: ...

    def evaluate_mappings(
        self,
        ground_truth: GroundTruthDataset,
    ) -> dict[str, MappingQualityMetrics]: ...

    def export_all(
        self,
        format: ExportFormat,
        output_dir: Path,
    ) -> list[Path]: ...

    def snapshot(self) -> SessionSnapshot: ...
```

### 10.2 Event System

The library emits typed events for observability and extension hooks:

```python
class CDMEvent(Protocol):
    event_type: str
    timestamp: datetime
    metadata: dict[str, Any]

# Examples
EntityRegisteredEvent
EDPLoadedEvent
MappingGeneratedEvent
MappingConflictDetectedEvent
MappingConfirmedEvent
EvaluationCompletedEvent
```

```python
class EventBus:
    def subscribe(self, event_type: type[CDMEvent], handler: Callable) -> None: ...
    def publish(self, event: CDMEvent) -> None: ...
```

---

## 11. Error Handling & Validation

### 11.1 Exception Hierarchy

```
CDMLibraryError (base)
├── ValidationError
│   ├── EDPValidationError
│   ├── REMValidationError
│   └── MappingValidationError
├── RegistryError
│   ├── DuplicateEntityError
│   ├── EntityNotFoundError
│   └── VersionConflictError
├── MappingError
│   ├── NoMappingCandidateError
│   ├── UnresolvableConflictError
│   └── InvalidTransformExpressionError
└── QualityError
    ├── GroundTruthMismatchError
    └── InsufficientDataError
```

### 11.2 Validation Results

Rather than raising exceptions eagerly, all validators return a `ValidationResult`:

```python
@dataclass
class ValidationResult:
    is_valid: bool
    errors: list[ValidationIssue]       # blocking issues
    warnings: list[ValidationIssue]     # non-blocking issues
    info: list[ValidationIssue]         # informational notices

@dataclass
class ValidationIssue:
    code: str                  # e.g. "EDP-003"
    message: str
    path: str                  # dot-separated path to the offending field
    severity: IssueSeverity    # ERROR | WARNING | INFO
```

Callers can choose to raise on errors via `result.raise_if_invalid()`.

---

## 12. Extensibility & Plugin Model

### 12.1 Custom Mapping Strategies

Third-party strategies are registered at engine construction time:

```python
engine = MappingEngine(
    cdm_registry=registry,
    rem=rem,
    strategies=[
        NameMatchStrategy(weight=2.0),
        TypeCompatibilityStrategy(weight=1.5),
        MyCustomEmbeddingStrategy(model_path="...", weight=3.0),
    ]
)
```

A custom strategy only needs to implement `AbstractMappingStrategy.score()`.

### 12.2 Custom Type Resolvers

```python
class AbstractTypeResolver(ABC):
    @abstractmethod
    def resolve(self, sql_type_string: str, dialect: SQLDialect) -> CDMType: ...
```

Register via `CDMRegistry.register_type_resolver(resolver)`. Resolvers are tried in registration order; the first non-`None` return wins.

### 12.3 Export Adapters

```python
class AbstractExportAdapter(ABC):
    format: ExportFormat

    @abstractmethod
    def export(self, mapping_set: MappingSet, output_path: Path | None) -> str: ...
```

Custom adapters (e.g. for dbt model generation, Spark schema, Airbyte config) are registered via `MappingEngine.register_export_adapter(adapter)`.

---

## 13. Non-Functional Requirements

| ID | Requirement | Target |
|---|---|---|
| NFR-01 | Single-EDP mapping generation (500 tables, 10 000 cols) | ≤ 30 s on a 1-core, 4 GB machine |
| NFR-02 | Batch mapping generation (50 EDPs) | Linear scaling; no shared mutable state between profiles |
| NFR-03 | CDMRegistry load from JSON (10 000 entity registry) | ≤ 2 s |
| NFR-04 | Memory footprint for a 10 000-column EDP in RAM | ≤ 200 MB |
| NFR-05 | Python version support | 3.10 + |
| NFR-06 | Zero mandatory third-party runtime dependencies | Core modules use stdlib only; extras declared in `[extras]` |
| NFR-07 | Thread safety | All public read operations are thread-safe; writes to registry require explicit locking |
| NFR-08 | Test coverage | ≥ 90 % line coverage for Modules 1–4 |
| NFR-09 | Type annotations | All public API surfaces must be fully annotated; `mypy --strict` must pass |
| NFR-10 | Serialisation round-trip | Any object serialised to JSON/YAML and deserialised must be equal to the original (all public types) |

---

## 14. Appendix A — Type System Reference

### A.1 SQL-to-CDM Type Compatibility Matrix

| Source SQL Type | CDM Type | Lossless? | Notes |
|---|---|---|---|
| `VARCHAR(n)`, `TEXT`, `CHAR(n)` | `CDMString` | Yes | — |
| `INTEGER`, `INT`, `BIGINT` | `CDMInteger` | Yes | — |
| `SMALLINT`, `TINYINT` | `CDMInteger` | Yes | Range narrower than `CDMInteger` |
| `DECIMAL(p,s)`, `NUMERIC(p,s)` | `CDMDecimal` | Yes | Precision/scale preserved as metadata |
| `FLOAT`, `DOUBLE`, `REAL` | `CDMDecimal` | No | Floating-point precision loss possible |
| `BOOLEAN`, `BIT` | `CDMBoolean` | Yes | — |
| `DATE` | `CDMDate` | Yes | — |
| `TIMESTAMP`, `DATETIME` | `CDMDateTime` | Yes | Timezone normalised to UTC unless specified |
| `TIME` | `CDMTime` | Yes | — |
| `UUID` | `CDMUUID` | Yes | — |
| `VARCHAR(36)` | `CDMUUID` | Conditional | Only if pattern validation passes |
| `JSON`, `JSONB` | `CDMJson` | Yes | — |
| `BLOB`, `BYTEA` | `CDMBinary` | Yes | — |
| `ARRAY` | `CDMArray` | Yes | Element type resolved recursively |
| `ENUM` | `CDMEnum` | Yes | Values captured as allowed set |

---

## 15. Appendix B — Example Workflows

### B.1 End-to-End Mapping Generation

```python
from cdm_library import MappingSession, CDMRegistry, EDPLoader, REMLoader

# 1. Load the CDM registry
registry = CDMRegistry.from_yaml("cdm_registry.yaml")

# 2. Load the Reference Entity Model
rem = REMLoader.from_yaml("reference_entities.yaml")

# 3. Load enriched database profiles
edp_orders = EDPLoader.from_json("profiles/orders_db.json")
edp_crm    = EDPLoader.from_json("profiles/crm_db.json")

# 4. Create a mapping session
session = MappingSession(cdm_registry=registry, rem=rem)
session.add_profile(edp_orders)
session.add_profile(edp_crm)

# 5. Generate mappings
mapping_sets = session.generate_mappings(target_entities=["Customer", "Order"])

# 6. Inspect results
for source_id, ms in mapping_sets.items():
    print(f"{source_id}: coverage={ms.coverage_rate:.1%}, rules={len(ms.rules)}")
    conflicted = [r for r in ms.rules if r.status.name == "CONFLICTED"]
    print(f"  Conflicts requiring review: {len(conflicted)}")

# 7. Export for the query engine
session.export_all(format=ExportFormat.JSON, output_dir=Path("output/mappings/"))
```

### B.2 Evaluating Against Ground Truth

```python
from cdm_library.quality import GroundTruthLoader, MappingEvaluator, QualityReportGenerator

# Load ground truth
gt = GroundTruthLoader.from_yaml("ground_truth/crm_mappings_gt.yaml")

# Evaluate the CRM mapping set
evaluator = MappingEvaluator(ground_truth=gt)
metrics = evaluator.evaluate(mapping_sets["crm_db"])

print(f"Precision: {metrics.precision:.3f}")
print(f"Recall:    {metrics.recall:.3f}")
print(f"F1 Score:  {metrics.f1_score:.3f}")
print(f"MAP:       {metrics.mean_average_precision:.3f}")

# Generate a report
reporter = QualityReportGenerator()
reporter.generate(
    metrics=metrics,
    output_format=ReportFormat.MARKDOWN,
    output_path=Path("output/quality_report.md"),
)
```

### B.3 Adding a Custom Strategy

```python
from cdm_library.mapping.strategies import AbstractMappingStrategy

class ColumnDescriptionEmbeddingStrategy(AbstractMappingStrategy):
    name = "description_embedding"

    def __init__(self, model, weight: float = 1.5):
        self.model = model
        self.weight = weight

    def score(self, column, cdm_attribute, rem_attribute):
        if not column.description or not cdm_attribute.description:
            return 0.0
        col_vec = self.model.encode(column.description)
        attr_vec = self.model.encode(cdm_attribute.description)
        return float(cosine_similarity(col_vec, attr_vec))

engine = MappingEngine(
    cdm_registry=registry,
    rem=rem,
    strategies=[
        NameMatchStrategy(weight=2.0),
        TypeCompatibilityStrategy(weight=1.5),
        ColumnDescriptionEmbeddingStrategy(model=my_model, weight=3.0),
    ]
)
```

---

*End of Functional Specifications v1.0.0-draft*
