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

__version__ = "0.1.4"

from importlib import import_module

# Capability discovery must not import Spark/Core or the transformation stages.
# Keep the public names available, loading their owning modules on first access.
_EXPORTS = {
    "materialization_gate": "nexus_spark_lib.transform.stage0_materialization",
    "drop_cold": "nexus_spark_lib.transform.stage0_materialization",
    "materialization_decide": "nexus_spark_lib.transform.stage0_materialization",
    "normalise": "nexus_spark_lib.transform.stage1_normalise",
}


def __getattr__(name):
    if name not in _EXPORTS:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    value = getattr(import_module(_EXPORTS[name]), name)
    globals()[name] = value
    return value

__all__ = [
    "materialization_gate",
    "materialization_decide",
    "drop_cold",
    "normalise",
    "__version__",
]
