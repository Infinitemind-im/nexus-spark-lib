import json
import os
import re
import time
import xmlrpc.client
from typing import Any
from datetime import datetime, timezone

import requests
from confluent_kafka import Producer  # type: ignore
from contextlib import contextmanager
from threading import RLock
from nexus_spark_lib.extract_registry import ExtractRegistry, registry
try:
    from airflow.models import Variable  # type: ignore
except Exception:  # noqa: BLE001
    Variable = None  # type: ignore


class ConfirmedProducer:
    def __init__(self, config):
        self.client = Producer(config)
        self.pending = 0
        self.errors = []

    def produce(self, *args, **kwargs):
        self.pending += 1

        def delivered(error, _message):
            self.pending -= 1
            if error is not None:
                self.errors.append(error)

        try:
            self.client.produce(*args, on_delivery=delivered, **kwargs)
        except Exception:
            self.pending -= 1
            raise
        self.client.poll(0)

    def flush(self, timeout=60):
        remaining = self.client.flush(timeout)
        if remaining or self.pending or self.errors:
            raise RuntimeError("Backfill Kafka delivery was not confirmed")
        return 0


def _producer():
    configured = None
    if Variable is not None:
        try:
            configured = (
                Variable.get("BACKFILL_KAFKA_BOOTSTRAP", default_var=None)
                or Variable.get("nexus_kafka_bootstrap", default_var=None)
            )
        except Exception:
            configured = None
    bootstrap = (
        configured
        or os.environ.get("BACKFILL_KAFKA_BOOTSTRAP")
        or os.environ.get("NEXUS_KAFKA_BOOTSTRAP")
        or os.environ.get("KAFKA_BOOTSTRAP_SERVERS")
    )
    if not bootstrap:
        raise RuntimeError(
            "Missing Kafka bootstrap: set BACKFILL_KAFKA_BOOTSTRAP, "
            "NEXUS_KAFKA_BOOTSTRAP, KAFKA_BOOTSTRAP_SERVERS, or Airflow Variable nexus_kafka_bootstrap"
        )
    return ConfirmedProducer({"bootstrap.servers": bootstrap, "enable.idempotence": True,
                              "acks": "all", "delivery.timeout.ms": 30000})


def _get_var(name: str, default: str | None = None) -> str | None:
    if Variable is not None:
        try:
            return Variable.get(name, default_var=default)
        except Exception:
            pass
    return default


def _csv(value: str | None) -> list[str]:
    return [x.strip() for x in (value or "").split(",") if x.strip()]


def _connector_var(connector: Any, suffix: str, default: str | None = None) -> str | None:
    value = _get_var(f"bf__{getattr(connector, 'connector_id', '')}__{suffix}")
    if value is not None:
        return value
    from nexus_spark_lib.backfill_config import registered_value
    return registered_value(connector, suffix, default)


def _flush_or_raise(producer: Producer, timeout: float = 60) -> None:
    remaining = producer.flush(timeout)
    if remaining:
        raise RuntimeError(f"Kafka producer still has {remaining} undelivered message(s) after flush")


def record_handover_start(connector_id: str, tenant_id: str) -> dict:
    with _control_connection(tenant_id) as conn:
        conn.execute("""INSERT INTO nexus_system.connector_backfill_handover
            (tenant_id,connector_id,status) VALUES (%s,%s::uuid,'running')
            ON CONFLICT (tenant_id,connector_id) DO UPDATE
            SET status='running',started_at=now(),completed_at=NULL,failure_reason=NULL,updated_at=now()""",
            (tenant_id,connector_id))
    return {"status": "running", "connector_id": connector_id, "tenant_id": tenant_id}


def estimate_cost(connector_id: str, tenant_id: str) -> dict:
    with _control_connection(tenant_id) as conn:
        row = conn.execute("""SELECT source_corpus_estimate FROM nexus_system.connector_backfill_handover
            WHERE tenant_id=%s AND connector_id=%s::uuid""", (tenant_id,connector_id)).fetchone()
    return {"records_estimate": int(row[0]) if row else None, "estimated_cost_usd": None}


@contextmanager
def _control_connection(tenant_id):
    import psycopg
    dsn = os.environ.get("CDM_DB_DSN") or os.environ.get("NEXUS_DB_DSN") or _get_var("CDM_DB_DSN") or _get_var("NEXUS_DB_DSN")
    if not dsn or not tenant_id:
        raise RuntimeError("Backfill control database and tenant are required")
    with psycopg.connect(dsn,connect_timeout=10) as conn:
        conn.execute("SELECT set_config('nexus.current_tenant_id',%s,true)", (tenant_id,))
        yield conn


def complete_handover(connector_id, tenant_id):
    with _control_connection(tenant_id) as conn:
        updated = conn.execute("""UPDATE nexus_system.connector_backfill_handover
            SET status='completed',completed_at=now(),updated_at=now()
            WHERE tenant_id=%s AND connector_id=%s::uuid AND status IN ('running','completed')""",
            (tenant_id,connector_id)).rowcount
        if not updated:
            raise RuntimeError("Initial load has no running handover")


def _iso_now() -> str:
    return datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()


def _base_payload(
    connector: Any,
    source_system: str,
    source_table: str,
    source_record_id: str,
    source_ts: str,
    after_payload: dict,
    window: dict | None = None,
) -> dict:
    payload = {
        "tenant_id": getattr(connector, "tenant_id", os.environ.get("NEXUS_TENANT_ID", "system")),
        "connector_id": getattr(connector, "connector_id", source_system),
        "source_system": source_system,
        "source_table": source_table,
        "source_record_id": source_record_id,
        "source_op": "SNAPSHOT_READ",
        "source_ts": source_ts,
        "after_payload": after_payload,
        "backfill_batch_id": os.environ.get("BACKFILL_BATCH_ID", str(int(time.time()))),
    }
    if window:
        payload["window"] = window
        payload["job_kind"] = "windowed_backfill"
    else:
        payload["job_kind"] = "initial_load"
    return payload


def publish_raw_record(producer, topic, payload):
    """Use the Core wire contract consumed by the real Spark raw reader."""
    from nexus_core.messaging import NexusMessage

    message = NexusMessage(topic=topic, tenant_id=payload["tenant_id"],
        source_system=payload["source_system"], source_record_id=payload["source_record_id"],
        payload={key:value for key,value in payload.items()
                 if key not in ("tenant_id","source_system","source_record_id")},
        permission_scope={}, event_action="read",
        correlation_id=payload["backfill_batch_id"])
    producer.produce(topic, key=message.tenant_id.encode(), value=message.to_json())


def _sf_env() -> tuple[str, str, str, str, str | None, str | None, str | None]:
    login_base = os.getenv("SALESFORCE_LOGIN_URL") or os.getenv("SALESFORCE_INSTANCE_URL")
    api_base = os.getenv("SALESFORCE_INSTANCE_URL") or login_base
    cid = os.getenv("SALESFORCE_CLIENT_ID")
    csec = os.getenv("SALESFORCE_CLIENT_SECRET")
    user = os.getenv("SALESFORCE_USERNAME")
    pwd = os.getenv("SALESFORCE_PASSWORD")
    refresh_token = os.getenv("SALESFORCE_REFRESH_TOKEN")
    sec_tok = os.getenv("SALESFORCE_SECURITY_TOKEN", "")
    if pwd and sec_tok and not pwd.endswith(sec_tok):
        pwd += sec_tok
    if not all([login_base, api_base, cid, csec]):
        raise ValueError("Missing Salesforce credentials")
    return login_base.rstrip("/"), api_base.rstrip("/"), cid, csec, user, pwd, refresh_token


def _sf_error_code(resp: requests.Response) -> str | None:
    try:
        body = resp.json()
    except Exception:
        return None
    if isinstance(body, list) and body:
        first = body[0]
        if isinstance(first, dict):
            return first.get("errorCode")
    if isinstance(body, dict):
        return body.get("errorCode")
    return None


def _sf_request(method: str, url: str, **kwargs) -> requests.Response:
    max_attempts = max(1, int(os.getenv("SF_HTTP_MAX_ATTEMPTS", "8") or 8))
    base_sleep = max(1, int(os.getenv("SF_HTTP_RETRY_BASE_SEC", "15") or 15))
    max_sleep = max(base_sleep, int(os.getenv("SF_HTTP_RETRY_MAX_SEC", "300") or 300))

    for attempt in range(1, max_attempts + 1):
        resp = requests.request(method, url, **kwargs)
        if resp.ok:
            return resp

        error_code = _sf_error_code(resp)
        retryable = resp.status_code in (429, 500, 502, 503, 504) or (
            resp.status_code == 403 and error_code == "REQUEST_LIMIT_EXCEEDED"
        )

        if not retryable or attempt == max_attempts:
            try:
                body = resp.text[:500]
            except Exception:
                body = "<response body unavailable>"
            raise RuntimeError(
                f"Salesforce API error {resp.status_code} ({error_code or 'unknown'}) for {url}: {body}"
            )

        sleep_s = min(max_sleep, base_sleep * (2 ** (attempt - 1)))
        time.sleep(sleep_s)

    raise RuntimeError(f"Salesforce request exhausted retries for {url}")


def _sf_token() -> tuple[str, str]:
    login_base, api_base, cid, csec, user, pwd, refresh_token = _sf_env()
    url = f"{login_base}/services/oauth2/token"
    ctype = (os.getenv("SALESFORCE_CONNECTION_TYPE") or "").lower()
    if ctype in ("client_credentials", "oauth_2.0_client_credentials", "client-credentials"):
        data = {"grant_type": "client_credentials", "client_id": cid, "client_secret": csec}
    elif ctype in ("refresh_token", "oauth_2.0_refresh_token", "oauth_2_0_refresh_token") or refresh_token:
        if not refresh_token:
            raise ValueError("Missing Salesforce refresh token for refresh-token OAuth flow")
        data = {
            "grant_type": "refresh_token",
            "client_id": cid,
            "client_secret": csec,
            "refresh_token": refresh_token,
        }
    else:
        if not all([user, pwd]):
            raise ValueError("Missing Salesforce username/password for password OAuth flow")
        data = {
            "grant_type": "password",
            "client_id": cid,
            "client_secret": csec,
            "username": user,
            "password": pwd,
        }
    r = _sf_request("post", url, data=data, timeout=60)
    payload = r.json()
    return payload["access_token"], (payload.get("instance_url") or api_base).rstrip("/")


def _sf_exclude_object(name: str, exclude_system_tables: bool) -> bool:
    if not exclude_system_tables:
        return False
    regex = os.getenv("SF_OBJECTS_EXCLUDE_REGEX", r"(History$|Share$|Feed$|^EventLogFile$)")
    return re.search(regex, name) is not None


def _sf_format_datetime(value: str) -> str:
    try:
        dt = datetime.fromisoformat(value)
    except ValueError:
        dt = datetime.fromisoformat(f"{value}T00:00:00")
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.isoformat().replace("+00:00", "Z")


def _sf_timestamp_fields(connector: Any) -> list[str]:
    configured = (
        _connector_var(connector, "timestamp_fields")
        or os.getenv("SF_TIMESTAMP_FIELDS")
        or "LastModifiedDate,CreatedDate,SystemModstamp"
    )
    return _csv(configured)


def _sf_window_field(connector: Any, fields: set[str]) -> str | None:
    preferred = (
        _connector_var(connector, "ts_column")
        or os.getenv("SF_WINDOW_FIELD")
        or os.getenv("SF_DATE_FIELD")
        or "LastModifiedDate"
    )
    if preferred in fields:
        return preferred
    for candidate in _sf_timestamp_fields(connector):
        if candidate in fields:
            return candidate
    return None


def _extract_salesforce(
    connector: Any,
    publish_topic: str,
    exclude_system_tables: bool,
    window: dict | None = None,
    full_snapshot: bool = False,
) -> None:
    token, base = _sf_token()
    p = _producer()
    url = f"{base}/services/data/v59.0/sobjects"
    r = _sf_request("get", url, headers={"Authorization": f"Bearer {token}"}, timeout=60)
    objects = [o["name"] for o in r.json().get("sobjects", []) if o.get("queryable")]
    objects = [n for n in objects if not _sf_exclude_object(n, exclude_system_tables)]
    wl = (_connector_var(connector, "objects") or os.getenv("SF_OBJECTS") or os.getenv("SF_OBJECTS_INCLUDE") or "").strip()
    if wl:
        allowed = {x.strip() for x in wl.split(",") if x.strip()}
        objects = [n for n in objects if n in allowed]
    limit = int(os.getenv("SF_OBJECT_LIMIT", "0") or 0)
    if limit and len(objects) > limit:
        objects = objects[:limit]

    window_start = _sf_format_datetime(window["start"]) if window else None
    window_end = _sf_format_datetime(window["end"]) if window else None
    explicit_initial = set(_csv(_connector_var(connector, "initial_objects") or os.getenv("SF_INITIAL_OBJECTS")))

    for obj in objects:
        durl = f"{base}/services/data/v59.0/sobjects/{obj}/describe"
        try:
            dr = _sf_request("get", durl, headers={"Authorization": f"Bearer {token}"}, timeout=60)
        except Exception as exc:
            raise RuntimeError("Source object discovery failed") from exc
        dj = dr.json()
        fields = [f.get("name") for f in dj.get("fields", []) if f.get("name")]
        if not fields:
            continue
        field_set = set(fields)
        window_field = _sf_window_field(connector, field_set)
        has_any_ts = bool(field_set.intersection(_sf_timestamp_fields(connector)))
        if window_start and window_end and not window_field:
            continue
        if not window and not full_snapshot and has_any_ts and obj not in explicit_initial:
            continue

        fields_csv = ", ".join(fields)
        where_clause = ""
        if window_start and window_end and window_field:
            where_clause = f" WHERE {window_field} >= {window_start} AND {window_field} < {window_end}"
        soql = f"SELECT {fields_csv} FROM {obj}{where_clause}"
        qbase = f"{base}/services/data/v59.0/query"
        next_url = None
        while True:
            try:
                if next_url:
                    qr = _sf_request("get", next_url, headers={"Authorization": f"Bearer {token}"}, timeout=120)
                else:
                    qr = _sf_request(
                        "get",
                        qbase,
                        params={"q": soql},
                        headers={"Authorization": f"Bearer {token}"},
                        timeout=120,
                    )
            except Exception as exc:
                raise RuntimeError("Source extraction page failed") from exc
            data = qr.json()
            for rec in data.get("records", []):
                rec.pop("attributes", None)
                payload = _base_payload(
                    connector=connector,
                    source_system="salesforce",
                    source_table=obj,
                    source_record_id=str(rec.get("Id", "")),
                    source_ts=str(rec.get(window_field or "LastModifiedDate", _iso_now())),
                    after_payload=rec,
                    window=window,
                )
                publish_raw_record(p,publish_topic,payload)
            _flush_or_raise(p)
            if data.get("done"):
                break
            next_url = base + data.get("nextRecordsUrl")


def _sn_env() -> tuple[str, str, str]:
    base = os.getenv("SERVICENOW_INSTANCE_URL")
    user = os.getenv("SERVICENOW_USERNAME")
    pwd = os.getenv("SERVICENOW_PASSWORD")
    if not all([base, user, pwd]):
        raise ValueError("Missing ServiceNow credentials")
    if not base.startswith("http"):
        base = f"https://{base}"
    if base.startswith("http://"):
        base = f"https://{base[len('http://'):]}"
    return base.rstrip("/"), user, pwd


def _sn_exclude_table(name: str, exclude_system_tables: bool) -> bool:
    if not exclude_system_tables:
        return False
    regex = os.getenv("SN_TABLES_EXCLUDE_REGEX", r"^sys_|^syslog|^sys_history")
    return re.search(regex, name) is not None


def _sn_format_datetime(value: str) -> str:
    try:
        dt = datetime.fromisoformat(value)
    except ValueError:
        dt = datetime.fromisoformat(f"{value}T00:00:00")
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def _sn_timestamp_fields(connector: Any) -> list[str]:
    configured = (
        _connector_var(connector, "timestamp_fields")
        or os.getenv("SN_TIMESTAMP_FIELDS")
        or "sys_updated_on,sys_created_on"
    )
    return _csv(configured)


def _sn_window_field(connector: Any) -> str:
    return (
        _connector_var(connector, "ts_column")
        or os.getenv("SN_WINDOW_FIELD")
        or os.getenv("SN_DATE_FIELD")
        or "sys_updated_on"
    )


def _sn_table_has_field(base: str, user: str, pwd: str, table: str, field: str) -> bool:
    try:
        probe = requests.get(
            f"{base}/api/now/table/{table}",
            auth=(user, pwd),
            headers={"Accept": "application/json"},
            params={"sysparm_limit": 1, "sysparm_fields": field},
            timeout=30,
        )
        if probe.ok and "application/json" in probe.headers.get("content-type", ""):
            rows = probe.json().get("result", [])
            if rows and field in rows[0]:
                return True
    except Exception:
        pass

    try:
        dictionary = requests.get(
            f"{base}/api/now/table/sys_dictionary",
            auth=(user, pwd),
            headers={"Accept": "application/json"},
            params={
                "sysparm_limit": 1,
                "sysparm_fields": "element",
                "sysparm_query": f"name={table}^element={field}",
            },
            timeout=30,
        )
        if dictionary.ok and "application/json" in dictionary.headers.get("content-type", ""):
            return bool(dictionary.json().get("result", []))
    except Exception:
        pass
    return False


def _sn_tables_with_field(base: str, user: str, pwd: str, field: str) -> set[str]:
    try:
        resp = requests.get(
            f"{base}/api/now/table/sys_dictionary",
            auth=(user, pwd),
            headers={"Accept": "application/json"},
            params={
                "sysparm_limit": 10000,
                "sysparm_fields": "name,element",
                "sysparm_query": f"element={field}",
            },
            timeout=60,
        )
        resp.raise_for_status()
        if "application/json" not in resp.headers.get("content-type", ""):
            return set()
        return {row.get("name") for row in resp.json().get("result", []) if row.get("name")}
    except Exception:
        return set()


def _extract_servicenow(
    connector: Any,
    publish_topic: str,
    exclude_system_tables: bool,
    window: dict | None = None,
    full_snapshot: bool = False,
) -> None:
    base, user, pwd = _sn_env()
    p = _producer()
    url = f"{base}/api/now/table/sys_db_object?sysparm_fields=name&sysparm_limit=10000"
    r = requests.get(url, auth=(user, pwd), headers={"Accept": "application/json"}, timeout=60)
    r.raise_for_status()
    if "application/json" not in r.headers.get("content-type", ""):
        raise RuntimeError(f"ServiceNow returned non-JSON response: {r.status_code} {r.text[:200]}")
    names = [item.get("name") for item in r.json().get("result", []) if item.get("name")]
    tables = [n for n in names if not _sn_exclude_table(n, exclude_system_tables)]
    wl = (
        _connector_var(connector, "tables")
        or os.getenv("SN_TABLES_WHITELIST")
        or os.getenv("SN_TABLES_INCLUDE")
        or ""
    ).strip()
    if wl:
        allowed = {x.strip() for x in wl.split(",") if x.strip()}
        tables = [n for n in tables if n in allowed]
    limit = int(os.getenv("SN_TABLE_LIMIT", "0") or 0)
    if limit and len(tables) > limit:
        tables = tables[:limit]

    window_field = _sn_window_field(connector)
    window_query = ""
    if window:
        start = _sn_format_datetime(window["start"])
        end = _sn_format_datetime(window["end"])
        window_query = f"{window_field}>={start}^{window_field}<{end}"

    explicit_initial = set(_csv(_connector_var(connector, "initial_tables") or os.getenv("SN_INITIAL_TABLES")))
    tables_with_window_field = _sn_tables_with_field(base, user, pwd, window_field)
    tables_with_any_ts = set()
    for field in _sn_timestamp_fields(connector):
        tables_with_any_ts.update(_sn_tables_with_field(base, user, pwd, field))
    if window and tables_with_window_field:
        tables = [table for table in tables if table in tables_with_window_field]
    elif not window and not full_snapshot and explicit_initial:
        tables = [table for table in tables if table in explicit_initial]
    elif not window and not full_snapshot and tables_with_any_ts:
        tables = [table for table in tables if table not in tables_with_any_ts]

    page_size = int(os.getenv("SN_PAGE_SIZE", "200"))
    for table in tables:
        offset = 0
        while True:
            q = f"{base}/api/now/table/{table}?sysparm_limit={page_size}&sysparm_offset={offset}"
            if window_query:
                q += f"&sysparm_query={requests.utils.quote(window_query)}"
            resp = requests.get(q, auth=(user, pwd), headers={"Accept": "application/json"}, timeout=120)
            try:
                resp.raise_for_status()
            except Exception as exc:
                raise RuntimeError("Source extraction page failed") from exc
            if "application/json" not in resp.headers.get("content-type", ""):
                raise RuntimeError(f"ServiceNow returned non-JSON response: {resp.status_code} {resp.text[:200]}")
            rows = resp.json().get("result", [])
            if not rows:
                break
            for rec in rows:
                payload = _base_payload(
                    connector=connector,
                    source_system="servicenow",
                    source_table=table,
                    source_record_id=str(rec.get("sys_id", "")),
                    source_ts=str(rec.get("sys_updated_on", _iso_now())),
                    after_payload=rec,
                    window=window,
                )
                publish_raw_record(p,publish_topic,payload)
            _flush_or_raise(p)
            offset += page_size


def _odoo_env() -> tuple[str, str, str, str]:
    url = os.getenv("ODOO_URL")
    db = os.getenv("ODOO_DB")
    user = os.getenv("ODOO_USERNAME") or os.getenv("ODOO_LOGIN")
    api_key = os.getenv("ODOO_API_KEY") or os.getenv("ODOO_PASSWORD")
    if not all([url, db, user, api_key]):
        raise ValueError("Missing Odoo credentials")
    if not url.startswith("http"):
        url = f"https://{url}"
    if url.startswith("http://"):
        url = f"https://{url[len('http://'):]}"
    return url, db, user, api_key


def _odoo_exclude_model(name: str, exclude_system_tables: bool) -> bool:
    if not exclude_system_tables:
        return False
    regex = os.getenv(
        "ODOO_MODELS_EXCLUDE_REGEX",
        r"^(ir\.|mail\.|base\.|bus\.|fetchmail\.|iap\.|im_|utm\."
        r"|gamification\.|digest\.|resource\.|web\.|http\.|report\.|uom\."
        r"|account\.|res.users$)",
    )
    return re.search(regex, name) is not None


def _odoo_format_datetime(value: str) -> str:
    try:
        dt = datetime.fromisoformat(value)
    except ValueError:
        dt = datetime.fromisoformat(f"{value}T00:00:00")
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def _extract_odoo(
    connector: Any,
    publish_topic: str,
    exclude_system_tables: bool,
    window: dict | None = None,
    full_snapshot: bool = False,
) -> None:
    url, db, user, api_key = _odoo_env()
    common = xmlrpc.client.ServerProxy(f"{url}/xmlrpc/2/common")
    try:
        uid = common.authenticate(db, user, api_key, {})
    except xmlrpc.client.ProtocolError as err:
        # Handle common redirects without exposing credentials
        if err.errcode in (301, 302, 303):
            # Prefer https if http was used
            if url.startswith("http://"):
                url = f"https://{url[len('http://'):]}"
                common = xmlrpc.client.ServerProxy(f"{url}/xmlrpc/2/common")
                uid = common.authenticate(db, user, api_key, {})
            # Some Odoo deployments require '/odoo' prefix for xmlrpc endpoints
            elif not url.rstrip("/").endswith("/odoo"):
                url = f"{url.rstrip('/')}/odoo"
                common = xmlrpc.client.ServerProxy(f"{url}/xmlrpc/2/common")
                uid = common.authenticate(db, user, api_key, {})
            else:
                raise
        else:
            raise
    if not uid:
        raise RuntimeError("Odoo auth failed")

    models = xmlrpc.client.ServerProxy(f"{url}/xmlrpc/2/object")
    ids = models.execute_kw(db, uid, api_key, "ir.model", "search", [[("state", "!=", "manual")]])
    recs = models.execute_kw(db, uid, api_key, "ir.model", "read", [ids], {"fields": ["model"]})
    names = [r["model"] for r in recs if "model" in r]
    names = [n for n in names if not _odoo_exclude_model(n, exclude_system_tables)]

    # Optional whitelist/limit for faster/dev runs
    wl = os.getenv("ODOO_MODELS", "").strip()
    if wl:
        allowed = {x.strip() for x in wl.split(",") if x.strip()}
        names = [n for n in names if n in allowed]
    lim = int(os.getenv("ODOO_MODELS_LIMIT", "0") or 0)
    if lim and len(names) > lim:
        names = names[:lim]

    window_field = os.getenv("ODOO_WINDOW_FIELD", "write_date")
    window_domain = None
    if window:
        start = _odoo_format_datetime(window["start"])
        end = _odoo_format_datetime(window["end"])
        window_domain = [(window_field, ">=", start), (window_field, "<", end)]

    p = _producer()
    limit = int(os.getenv("ODOO_PAGE_SIZE", "500"))
    for model in names:
        offset = 0
        while True:
            domain = [("id", ">", 0)]
            if window_domain:
                domain = domain + window_domain
            try:
                ids = models.execute_kw(
                    db, uid, api_key, model, "search", [domain], {"offset": offset, "limit": limit}
                )
            except Exception as exc:
                raise RuntimeError("Source model extraction failed") from exc
            if not ids:
                break
            rows = models.execute_kw(db, uid, api_key, model, "read", [ids])
            for rec in rows:
                # Trim heavy/binary fields to avoid Kafka MSG_SIZE_TOO_LARGE
                for k in list(rec.keys()):
                    try:
                        if k.startswith("image_") or k.startswith("avatar_"):
                            rec.pop(k, None)
                            continue
                        v = rec[k]
                        if isinstance(v, (bytes, str)) and len(str(v)) > int(os.getenv("ODOO_FIELD_MAXLEN", "50000")):
                            rec.pop(k, None)
                    except Exception:
                        pass
                payload = _base_payload(
                    connector=connector,
                    source_system="odoo",
                    source_table=model,
                    source_record_id=str(rec.get("id", "")),
                    source_ts=_iso_now(),
                    after_payload=rec,
                    window=window,
                )
                publish_raw_record(p,publish_topic,payload)
            p.flush(5)
            offset += limit


class ApiExtractor:
    def __init__(self, extract):
        self.extract = extract

    def snapshot(self, connector, publish_topic, exclude_system_tables=True, *, full_snapshot=False):
        with credential_environment(connector):
            return self.extract(connector, publish_topic, exclude_system_tables,full_snapshot=full_snapshot)

    def window(self, connector, publish_topic, start_date, end_date, **_options):
        with credential_environment(connector):
            return self.extract(connector, publish_topic, True,
                                window={"start": start_date, "end": end_date})


_credential_lock = RLock()


@contextmanager
def credential_environment(connector):
    # Existing vendor functions resolve environment credentials. Bind those
    # to the registered connector rather than sharing them across tenants.
    raw = _connector_var(connector,"credentials")
    if raw:
        values = json.loads(raw)
        prefixes = ("SALESFORCE_","SERVICENOW_","ODOO_","SF_","SN_")
        if not isinstance(values,dict) or not values or any(
                not isinstance(key,str) or not key.startswith(prefixes) or not isinstance(value,str)
                for key,value in values.items()):
            raise ValueError("Invalid connector credential environment")
    elif (os.getenv("BACKFILL_CONNECTOR_ID") == connector.connector_id
          and os.getenv("NEXUS_TENANT_ID") == connector.tenant_id):
        values = {}
    else:
        raise RuntimeError("API credentials must be bound to this connector and tenant")
    with _credential_lock:
        # Clear unspecified credentials too: otherwise a partial connector
        # configuration could inherit another tenant's refresh token/user.
        keys = set(values) | {key for key in os.environ if key.startswith(prefixes)} if raw else set()
        previous = {key:os.environ.get(key) for key in keys}
        try:
            for key in keys:
                os.environ.pop(key,None)
            os.environ.update(values)
            yield
        finally:
            for key,value in previous.items():
                if value is None:
                    os.environ.pop(key,None)
                else:
                    os.environ[key] = value


class DatabaseExtractor:
    def snapshot(self, connector, publish_topic, exclude_system_tables=True, *, full_snapshot=False):
        from nexus_spark_lib.backfill_database import extract_tables
        return extract_tables(connector,publish_topic,full_snapshot=full_snapshot)

    def window(self, connector, publish_topic, start_date, end_date, **options):
        from nexus_spark_lib.backfill_database import extract_tables
        return extract_tables(connector,publish_topic,start_date,end_date,
            table_name=options.get("table_name"),timestamp_column=options.get("timestamp_column"))


def run_snapshot_extract(connector, exclude_system_tables, publish_topic):
    _validate_connector(connector)
    return registry.get(connector.source_type).snapshot(connector,publish_topic,exclude_system_tables)


def run_full_snapshot_extract(connector, publish_topic):
    _validate_connector(connector)
    return registry.get(connector.source_type).snapshot(connector,publish_topic,True,full_snapshot=True)


def run_windowed_backfill(connector,start_date,end_date,tables_selector,exclude_system_tables,
                          publish_topic,table_name=None,timestamp_column=None):
    _validate_connector(connector)
    return registry.get(connector.source_type).window(connector,publish_topic,start_date,end_date,
        table_name=table_name,timestamp_column=timestamp_column)


def _validate_connector(connector):
    if not connector.tenant_id or not connector.connector_id:
        raise ValueError("Backfill requires a registered tenant and connector")
    with _control_connection(connector.tenant_id) as conn:
        row = conn.execute("""SELECT lower(COALESCE(NULLIF(system_type,''),
                NULLIF(connector_type,''),source_system))
            FROM nexus_system.connectors WHERE tenant_id=%s AND connector_id::text=%s
            AND enabled AND active""", (connector.tenant_id,connector.connector_id)).fetchone()
    expected = "postgresql" if connector.source_type.lower() == "postgres" else connector.source_type.lower()
    actual = "postgresql" if row and row[0] == "postgres" else row[0] if row else None
    if actual != expected:
        raise ValueError("Backfill connector is inactive, has changed type, or belongs to another tenant")
