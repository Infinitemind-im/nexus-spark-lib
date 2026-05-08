from nexus_spark_lib.transform.stage0_materialization import (
    drop_cold,
    materialization_gate,
    materialization_decide,  # backward-compatible alias
)
from nexus_spark_lib.transform.stage1_normalise import normalise

# stage2_resolve and stage3_synthesise are not loaded here to avoid
# pulling in heavy dependencies (neo4j, jellyfish, etc.) that are
# not needed by the spark-transformer service.

__all__ = [
    "materialization_gate",
    "materialization_decide",
    "drop_cold",
    "normalise",
]
