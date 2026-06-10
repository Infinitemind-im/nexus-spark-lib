"""Integration benchmark — Entity Resolution end-to-end timing.

Runs Stage 0 → 1 → 2 (resolve) on Spark local mode and reports wall-clock
latency per scenario and per record. No live PostgreSQL/Kafka required.

Usage (PowerShell, Python 3.11 + Java 17):

    cd nexus-spark-lib
    $env:JAVA_HOME = "C:\\Program Files\\Microsoft\\jdk-17.0.19.10-hotspot"
    $env:Path = "$env:JAVA_HOME\\bin;$env:Path"
    $env:NEXUS_DB_DSN = "postgresql://nexus_app:nexusapp@127.0.0.1:5444/nexus_db?sslmode=disable"
    ..\\nexus-spark-transformer\\.venv311\\Scripts\\python.exe scripts/benchmark_entity_resolution.py
"""

from __future__ import annotations

import json
import os
import statistics
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime
from types import SimpleNamespace
from typing import Any, Callable

from pyspark.sql import SparkSession
from pyspark.sql.types import MapType, StringType, StructField, StructType, TimestampType

from nexus_spark_lib._internal.hash_utils import er_source_lookup_key
from nexus_spark_lib.models.fx import FxRates
from nexus_spark_lib.models.materialization import MaterializationLevel, MaterializationPolicy, PolicyRule
from nexus_spark_lib.transform.stage0_materialization import drop_cold, materialization_gate
from nexus_spark_lib.transform.stage1_normalise import normalise
from nexus_spark_lib.transform.stage2_resolve import resolve


@dataclass
class _FakeErIndex:
    snapshot: dict = field(default_factory=dict)
    deterministic_columns: dict = field(default_factory=dict)
    thresholds: dict = field(default_factory=dict)
    lsh_index: object | None = None
    source_records_by_entity: dict = field(default_factory=dict)

    def get_fields(self, cdm_entity_id: str) -> dict:
        _ = cdm_entity_id
        return {}

    def find_entity_by_source_record(
        self, tenant_id: str, cdm_entity_type: str, source_system: str, source_record_id: str
    ):
        return self.source_records_by_entity.get(
            (tenant_id, cdm_entity_type, source_system, source_record_id)
        )


def _mock_cdm_broadcast() -> SimpleNamespace:
    mapping = SimpleNamespace(
        get_cdm_entity_type=lambda *_a, **_k: "contact",
        get_field_map=lambda *_a, **_k: {
            "full_name": "full_name",
            "email": "email",
            "phone": "phone",
            "__meta__full_name": {"type": "string"},
            "__meta__email": {"type": "string"},
            "__meta__phone": {"type": "string"},
        },
    )
    return SimpleNamespace(value=mapping)


def _mock_policy_broadcast() -> SimpleNamespace:
    policy = MaterializationPolicy()
    policy.rules_by_scope[("tenant_acme", "contact")] = [
        PolicyRule(
            rule_id="hot-contact",
            tenant_id="tenant_acme",
            scope="contact",
            predicate="TRUE",
            target_level=MaterializationLevel.HOT,
            priority=100,
            rule_type="manual",
        )
    ]
    return SimpleNamespace(value=policy)


def _mock_fx_broadcast() -> SimpleNamespace:
    return SimpleNamespace(value=FxRates(rates=[]))


def _mock_er_broadcast(**kwargs: Any) -> SimpleNamespace:
    return SimpleNamespace(value=_FakeErIndex(**kwargs))


RAW_SCHEMA = StructType([
    StructField("tenant_id", StringType(), False),
    StructField("connector_id", StringType(), False),
    StructField("source_system", StringType(), False),
    StructField("source_table", StringType(), False),
    StructField("source_record_id", StringType(), False),
    StructField("source_op", StringType(), False),
    StructField("source_ts", TimestampType(), False),
    StructField("after_payload", MapType(StringType(), StringType()), False),
    StructField("before_payload", MapType(StringType(), StringType()), True),
    StructField("message_id", StringType(), False),
    StructField("backfill_batch_id", StringType(), True),
    StructField("trace_id", StringType(), True),
])


def _raw_row(
    *,
    source_op: str = "INSERT",
    source_record_id: str = "003abc",
    after: dict[str, str] | None = None,
    before: dict[str, str] | None = None,
) -> tuple:
    return (
        "tenant_acme",
        "conn_salesforce",
        "salesforce",
        "Contact",
        source_record_id,
        source_op,
        datetime(2024, 3, 1, 12, 0, 0),
        after or {"full_name": "Alice Smith", "email": "alice@acme.com", "phone": "+18005550100"},
        before,
        f"msg-{source_record_id}",
        "batch-001",
        "trace-001",
    )


def _run_resolve_only(
    spark: SparkSession,
    normalised_rows: list[dict[str, Any]],
    er_broadcast: SimpleNamespace,
) -> tuple[list[Any], float]:
    """Stage 2 only — pre-normalised input."""
    schema = StructType([
        StructField("tenant_id", StringType(), False),
        StructField("connector_id", StringType(), False),
        StructField("source_system", StringType(), False),
        StructField("source_table", StringType(), False),
        StructField("source_record_id", StringType(), False),
        StructField("source_op", StringType(), False),
        StructField("normalised_json", StringType(), False),
        StructField("changed_canonical_attributes_json", StringType(), False),
        StructField("materialization_level", StringType(), False),
        StructField("cdm_entity_type", StringType(), False),
        StructField("blocking_key", StringType(), False),
    ])
    tuples = [
        (
            r["tenant_id"],
            r["connector_id"],
            r["source_system"],
            r["source_table"],
            r["source_record_id"],
            r["source_op"],
            r["normalised_json"],
            r.get("changed_canonical_attributes_json", "[]"),
            r["materialization_level"],
            r["cdm_entity_type"],
            r["blocking_key"],
        )
        for r in normalised_rows
    ]
    df = spark.createDataFrame(tuples, schema=schema)
    t0 = time.perf_counter()
    df = resolve(df, er_broadcast)
    result_rows = df.collect()
    return result_rows, time.perf_counter() - t0


def _warmup(spark: SparkSession, er_broadcast: SimpleNamespace) -> None:
    """One dry run so JVM + Python workers are hot before timing."""
    _run_pipeline(spark, [_raw_row(source_record_id="warmup-000")], er_broadcast)


def _run_pipeline(
    spark: SparkSession,
    rows: list[tuple],
    er_broadcast: SimpleNamespace,
) -> tuple[list[Any], float]:
    """Stage 0 → 1 → 2; returns (collected rows, elapsed seconds)."""
    cdm = _mock_cdm_broadcast()
    policy = _mock_policy_broadcast()
    fx = _mock_fx_broadcast()

    df = spark.createDataFrame(rows, schema=RAW_SCHEMA)
    t0 = time.perf_counter()
    df = materialization_gate(df, cdm, policy)
    df = drop_cold(df)
    df = normalise(df, cdm, fx)
    df = resolve(df, er_broadcast)
    result_rows = df.collect()
    elapsed = time.perf_counter() - t0
    return result_rows, elapsed


def _print_header(title: str) -> None:
    print()
    print("=" * 72)
    print(title)
    print("=" * 72)


def main() -> int:
    _print_header("NEXUS Entity Resolution — integration benchmark")

    python_exe = sys.executable
    os.environ["PYSPARK_PYTHON"] = python_exe
    os.environ["PYSPARK_DRIVER_PYTHON"] = python_exe
    print(f"Python worker: {python_exe}")

    t_spark0 = time.perf_counter()
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("er-integration-benchmark")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
        .config("spark.pyspark.python", sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    spark_ms = (time.perf_counter() - t_spark0) * 1000
    print(f"SparkSession startup: {spark_ms:.0f} ms (one-time cost)")

    er_empty = _mock_er_broadcast()
    print("Warm-up run (JVM + Python workers)...")
    _warmup(spark, er_empty)
    print("Warm-up done.")

    scenarios: list[tuple[str, list[tuple], SimpleNamespace, Callable[[list[Any]], None] | None]] = []

    # 1. INSERT — new entity (hot, full Signal A/B path)
    scenarios.append((
        "INSERT new entity (hot)",
        [_raw_row(source_op="INSERT", source_record_id="rec-new-001")],
        _mock_er_broadcast(),
        lambda rows: None
        if rows[0]["er_resolution_method"] in ("new_entity", "warm_new")
        else (_ for _ in ()).throw(AssertionError(rows[0]["er_resolution_method"])),
    ))

    # 2. Fast-path — already indexed, no ER-relevant UPDATE diff
    lookup_key = er_source_lookup_key("tenant_acme", "conn_salesforce", "Contact", "rec-fp-001")
    scenarios.append((
        "Fast-path lookup (indexed INSERT)",
        [_raw_row(source_op="INSERT", source_record_id="rec-fp-001")],
        _mock_er_broadcast(snapshot={lookup_key: "gr:existing-fastpath"}),
        lambda rows: None
        if rows[0]["er_resolution_method"] == "fast_path" and rows[0]["cdm_entity_id"] == "gr:existing-fastpath"
        else (_ for _ in ()).throw(AssertionError(f"{rows[0]['er_resolution_method']} {rows[0]['cdm_entity_id']}")),
    ))

    # 3. UPDATE — ER-relevant attribute changed (email weight 0.30)
    upd_key = er_source_lookup_key("tenant_acme", "conn_salesforce", "Contact", "rec-upd-001")
    scenarios.append((
        "UPDATE re-ER (email changed, weight 0.30)",
        [
            _raw_row(
                source_op="UPDATE",
                source_record_id="rec-upd-001",
                after={"full_name": "Alice Smith", "email": "alice.new@acme.com"},
                before={"full_name": "Alice Smith", "email": "alice@acme.com"},
            )
        ],
        _mock_er_broadcast(
            snapshot={upd_key: "gr:existing-update"},
            thresholds={("tenant_acme", "contact"): {"weights": {"email": 0.30}}},
        ),
        lambda rows: None,
    ))

    # 4. UPDATE — non-ER diff keeps fast-path
    upd2_key = er_source_lookup_key("tenant_acme", "conn_salesforce", "Contact", "rec-upd-002")
    scenarios.append((
        "UPDATE fast-path (full_name only, weight 0.30 on email)",
        [
            _raw_row(
                source_op="UPDATE",
                source_record_id="rec-upd-002",
                after={"full_name": "Alice Newname", "email": "alice@acme.com"},
                before={"full_name": "Alice Smith", "email": "alice@acme.com"},
            )
        ],
        _mock_er_broadcast(
            snapshot={upd2_key: "gr:existing-update2"},
            thresholds={("tenant_acme", "contact"): {"weights": {"email": 0.30}}},
        ),
        lambda rows: None
        if rows[0]["er_resolution_method"] == "fast_path"
        else (_ for _ in ()).throw(AssertionError(rows[0]["er_resolution_method"])),
    ))

    # 5. RELEVEL skip
    rel_key = er_source_lookup_key("tenant_acme", "conn_salesforce", "Contact", "rec-rel-001")
    scenarios.append((
        "RELEVEL skip (no re-ER)",
        [_raw_row(source_op="RELEVEL", source_record_id="rec-rel-001")],
        _mock_er_broadcast(snapshot={rel_key: "gr:existing-relevel"}),
        lambda rows: None
        if rows[0]["er_resolution_method"] == "relevel_skip"
        else (_ for _ in ()).throw(AssertionError(rows[0]["er_resolution_method"])),
    ))

    _print_header("Single-record scenarios (Stage 0 + 1 + 2, after warm-up)")
    print(f"{'Scenario':<45} {'Total ms':>10} {'Method':>18}")
    print("-" * 72)

    single_times_ms: list[float] = []
    for name, rows, er_bc, validator in scenarios:
        result_rows, elapsed = _run_pipeline(spark, rows, er_bc)
        if validator:
            validator(result_rows)
        total_ms = elapsed * 1000
        single_times_ms.append(total_ms)
        method = result_rows[0]["er_resolution_method"]
        print(f"{name:<45} {total_ms:>10.1f} {method:>18}")

    _print_header("Stage 2 resolve() only (Entity Resolution latency)")
    fp_key = er_source_lookup_key("tenant_acme", "conn_salesforce", "Contact", "er-only-fp")
    resolve_cases: list[tuple[str, list[dict[str, Any]], SimpleNamespace]] = [
        (
            "resolve INSERT new entity",
            [{
                "tenant_id": "tenant_acme", "connector_id": "conn_salesforce",
                "source_system": "salesforce", "source_table": "Contact",
                "source_record_id": "er-only-new", "source_op": "INSERT",
                "normalised_json": json.dumps({"email": {"value": "x@acme.com"}}),
                "changed_canonical_attributes_json": "[]",
                "materialization_level": "hot", "cdm_entity_type": "contact",
                "blocking_key": "bk-er-only-new",
            }],
            _mock_er_broadcast(),
        ),
        (
            "resolve fast-path",
            [{
                "tenant_id": "tenant_acme", "connector_id": "conn_salesforce",
                "source_system": "salesforce", "source_table": "Contact",
                "source_record_id": "er-only-fp", "source_op": "INSERT",
                "normalised_json": json.dumps({"email": {"value": "fp@acme.com"}}),
                "changed_canonical_attributes_json": "[]",
                "materialization_level": "hot", "cdm_entity_type": "contact",
                "blocking_key": "bk-er-only-fp",
            }],
            _mock_er_broadcast(snapshot={fp_key: "gr:fastpath-er-only"}),
        ),
        (
            "resolve RELEVEL skip",
            [{
                "tenant_id": "tenant_acme", "connector_id": "conn_salesforce",
                "source_system": "salesforce", "source_table": "Contact",
                "source_record_id": "er-only-rel", "source_op": "RELEVEL",
                "normalised_json": json.dumps({"email": {"value": "rel@acme.com"}}),
                "changed_canonical_attributes_json": "[]",
                "materialization_level": "hot", "cdm_entity_type": "contact",
                "blocking_key": "bk-er-only-rel",
            }],
            _mock_er_broadcast(snapshot={
                er_source_lookup_key("tenant_acme", "conn_salesforce", "Contact", "er-only-rel"): "gr:relevel-er-only"
            }),
        ),
    ]
    print(f"{'Scenario':<35} {'resolve ms':>12} {'Method':>18}")
    print("-" * 72)
    er_only_times_ms: list[float] = []
    for name, norm_rows, er_bc in resolve_cases:
        result_rows, elapsed = _run_resolve_only(spark, norm_rows, er_bc)
        ms = elapsed * 1000
        er_only_times_ms.append(ms)
        print(f"{name:<35} {ms:>12.1f} {result_rows[0]['er_resolution_method']:>18}")

    _print_header("Batch throughput (Stage 0 + 1 + 2)")
    batch_sizes = [10, 100, 500]
    for n in batch_sizes:
        batch_rows = [
            _raw_row(source_op="INSERT", source_record_id=f"batch-{i:05d}")
            for i in range(n)
        ]
        _, elapsed = _run_pipeline(spark, batch_rows, _mock_er_broadcast())
        total_ms = elapsed * 1000
        per_record_ms = total_ms / n
        rps = n / elapsed if elapsed > 0 else 0.0
        print(
            f"  {n:>4} records -> {total_ms:>8.1f} ms total | "
            f"{per_record_ms:>6.2f} ms/record | {rps:>8.1f} records/s"
        )

    _print_header("Summary")
    print(f"  Spark startup (one-time):       {spark_ms:.0f} ms")
    print(f"  Full pipeline p50 (1 record):   {statistics.median(single_times_ms):.1f} ms")
    print(f"  Full pipeline min/max:          {min(single_times_ms):.1f} / {max(single_times_ms):.1f} ms")
    print(f"  resolve() only p50:             {statistics.median(er_only_times_ms):.1f} ms")
    print(f"  resolve() only min/max:         {min(er_only_times_ms):.1f} / {max(er_only_times_ms):.1f} ms")
    print()
    print("  Spec target (streaming):        p95 <= 5000 ms/record (NFR-D3-01)")
    print("  Spec target (throughput):       >= 5000 records/s/executor (NFR-D3-02)")
    print()
    print("  Note: local[2] on Windows includes Spark stage overhead;")
    print("  production cluster numbers will differ.")

    spark.stop()
    _print_header("Benchmark complete - all scenarios OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
