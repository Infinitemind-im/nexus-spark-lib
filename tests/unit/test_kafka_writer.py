from __future__ import annotations

import json

from pyspark.sql.types import StringType, StructField, StructType

from nexus_spark_lib.kafka.writer import prepare_kafka_output


def test_prepare_kafka_output_preserves_source_record_id_in_envelope(spark):
    schema = StructType([
        StructField("tenant_id", StringType(), False),
        StructField("source_system", StringType(), False),
        StructField("source_record_id", StringType(), False),
        StructField("cdm_entity_id", StringType(), False),
        StructField("cdm_entity_type", StringType(), False),
        StructField("transform_result_json", StringType(), False),
        StructField("trace_id", StringType(), False),
        StructField("correlation_id", StringType(), False),
    ])

    transform_result_json = json.dumps({
        "tenant_id": "tenant_acme",
        "cdm_entity_id": "gr:123",
        "cdm_entity_type": "transaction",
        "contributing_record": {
            "source_system": "salesforce",
            "source_record_id": "006fj00000FhrRpAAJ",
        },
    })

    df = spark.createDataFrame(
        [(
            "tenant_acme",
            "salesforce",
            "006fj00000FhrRpAAJ",
            "gr:123",
            "transaction",
            transform_result_json,
            "trace-001",
            "corr-001",
        )],
        schema=schema,
    )

    row = prepare_kafka_output(df).select("kafka_key", "kafka_value").collect()[0]
    envelope = json.loads(row["kafka_value"])

    assert row["kafka_key"] == "tenant_acme"
    assert envelope["source_record_id"] == "006fj00000FhrRpAAJ"
    assert envelope["payload"]["contributing_record"]["source_record_id"] == "006fj00000FhrRpAAJ"
