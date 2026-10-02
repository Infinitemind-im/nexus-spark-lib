"""The raw Kafka log must retain a batch for at least fourteen days."""
import os


MINIMUM_RETENTION_MS = 14*24*60*60*1000


def require_raw_retention(topic, *, admin=None):
    from confluent_kafka.admin import AdminClient, ConfigResource, ResourceType
    if admin is None:
        from nexus_spark_lib.backfill import _get_var
        bootstrap=(os.getenv("BACKFILL_KAFKA_BOOTSTRAP") or os.getenv("NEXUS_KAFKA_BOOTSTRAP")
                   or os.getenv("KAFKA_BOOTSTRAP_SERVERS") or _get_var("nexus_kafka_bootstrap"))
        if not bootstrap:
            raise RuntimeError("Kafka bootstrap is required to check raw retention")
        admin=AdminClient({"bootstrap.servers":bootstrap})
    resource=ConfigResource(ResourceType.TOPIC,topic)
    config=admin.describe_configs([resource],request_timeout=10)[resource].result(timeout=15)
    retention=int(config["retention.ms"].value)
    # Kafka -1 means unlimited retention.
    if retention != -1 and retention < MINIMUM_RETENTION_MS:
        raise RuntimeError("Raw Kafka topic retention must be at least fourteen days")
    return {"topic":topic,"retention_ms":retention}
