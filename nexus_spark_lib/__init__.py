"""
nexus_spark_lib — Shared Spark transformation library for the NEXUS platform pipeline.

Public API surface (stable, SemVer):
    from nexus_spark_lib.transform import (
        normalise,
        resolve,
        synthesise,
        materialization_decide,
    )
    from nexus_spark_lib.kafka import write_transformed_records

Every NEXUS service that runs Spark pipeline stages imports from here.
CDC Streaming and Batch Backfill pin a specific version of this library per release.
Breaking changes require a major version bump and a platform-wide coordination window.
"""

__version__ = "0.1.1"

from nexus_spark_lib.transform.stage0_materialization import (
    materialization_gate,
    drop_cold,
    materialization_decide,
)
from nexus_spark_lib.transform.stage1_normalise import normalise

__all__ = [
    "materialization_gate",
    "materialization_decide",
    "drop_cold",
    "normalise",
    "__version__",
]
