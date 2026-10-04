"""Bounded prospective raw archives, written before Kafka publication.

This preserves Core wire messages from new extractions. It does not establish
historical completeness, a migration replay plan, or a CDC fence.
"""
import hashlib
import io
import json
import re
from urllib.parse import urlsplit

MAX_RECORDS = 1000
MAX_BYTES = 16 * 1024 * 1024


def raw_scope(topic, key, wire):
    """Reject incomplete/flattened records and inconsistent tenant keys."""
    try:
        record = json.loads(wire)
        payload = record["payload"]
        fields = (record["tenant_id"], payload["connector_id"],
                  payload["backfill_batch_id"], payload["source_table"])
        valid = (all(isinstance(value, str) and value for value in fields)
                 and record["topic"] == topic and key == fields[0].encode()
                 and isinstance(record["source_system"], str) and record["source_system"]
                 and isinstance(record["source_record_id"], str) and record["source_record_id"]
                 and isinstance(payload["after_payload"], dict)
                 and payload["source_op"] == "SNAPSHOT_READ")
        if not valid or b"\n" in wire or b"\r" in wire:
            raise ValueError
    except (KeyError, TypeError, ValueError, AttributeError):
        raise ValueError("Raw archive requires a complete tenant-bound Core source message") from None
    return fields + (topic,)


class MinioRawArchive:
    def __init__(self, client, bucket, prefix="backfill/v1"):
        if not re.fullmatch(r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]", bucket):
            raise ValueError("Invalid raw archive bucket")
        if not re.fullmatch(r"[A-Za-z0-9_/-]+", prefix) or any(
                part in ("", ".", "..") for part in prefix.split("/")):
            raise ValueError("Invalid raw archive prefix")
        self.client, self.bucket, self.prefix = client, bucket, prefix

    def persist(self, scope, wires):
        if not wires or len(wires) > MAX_RECORDS:
            raise ValueError("Raw archive chunk exceeds record bounds")
        body = b"".join(wire + b"\n" for wire in wires)
        if len(body) > MAX_BYTES:
            raise ValueError("Raw archive chunk exceeds byte bounds")
        tenant, connector, batch, table, topic = scope
        for wire in wires:
            if raw_scope(topic, tenant.encode(), wire) != scope:
                raise ValueError("Raw archive chunk mixes source scopes")
        digest = hashlib.sha256(body).hexdigest()
        # Hash path components to prevent traversal and avoid exposing source IDs
        # in object names. The exact identities remain in the preserved envelope.
        paths = [hashlib.sha256(value.encode()).hexdigest() for value in scope]
        name = f"{self.prefix}/{'/'.join(paths)}/{digest}.ndjson"
        metadata = {"sha256": digest, "records": str(len(wires)), "format": "nexus-core-ndjson-v1"}
        try:
            self.client.put_object(self.bucket, name, io.BytesIO(body), len(body),
                                   content_type="application/x-ndjson", metadata=metadata)
            stored = self.client.stat_object(self.bucket, name)
            if stored.size != len(body) or stored.metadata.get("x-amz-meta-sha256") != digest:
                raise RuntimeError
        except Exception:
            # Client exceptions may contain source keys, endpoints or credentials.
            raise RuntimeError("Raw archive write was not confirmed; Kafka publication stopped") from None
        return {"bucket": self.bucket, "object_name": name, "sha256": digest,
                "records": len(wires), "bytes": len(body)}


def archive_from_settings(setting):
    bucket = setting("BACKFILL_RAW_ARCHIVE_BUCKET")
    if not bucket:
        return None
    endpoint = setting("MINIO_ENDPOINT")
    access = setting("MINIO_ACCESS_KEY")
    secret = setting("MINIO_SECRET_KEY")
    if not endpoint or not access or not secret:
        raise RuntimeError("Raw archive requires the existing MinIO endpoint and credentials")
    parsed = urlsplit(endpoint if "://" in endpoint else "http://" + endpoint)
    if (parsed.scheme not in ("http", "https") or not parsed.hostname or parsed.username
            or parsed.password or parsed.path not in ("", "/") or parsed.query or parsed.fragment):
        raise ValueError("Invalid MinIO archive endpoint")
    from minio import Minio
    import urllib3
    client = Minio(parsed.netloc, access_key=access, secret_key=secret,
                   secure=parsed.scheme == "https",
                   http_client=urllib3.PoolManager(timeout=urllib3.Timeout(connect=5, read=30),
                                                   retries=2))
    archive = MinioRawArchive(client, bucket, setting("BACKFILL_RAW_ARCHIVE_PREFIX") or "backfill/v1")
    try:
        if not client.bucket_exists(bucket):
            raise RuntimeError
    except Exception:
        raise RuntimeError("Configured raw archive bucket is unavailable") from None
    return archive
