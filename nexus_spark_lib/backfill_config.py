"""Resolve bulk-extract configuration from the registered tenant connector.

Secrets stay in the source registry; never copy them to XCom, logs or Variables.
Explicit per-connector Variables still override registry configuration.
"""
import json

from psycopg.conninfo import make_conninfo


_ENV_FIELDS = {
    "salesforce": {
        "instance_url": "SALESFORCE_INSTANCE_URL", "login_url": "SALESFORCE_LOGIN_URL",
        "client_id": "SALESFORCE_CLIENT_ID", "client_secret": "SALESFORCE_CLIENT_SECRET",
        "refresh_token": "SALESFORCE_REFRESH_TOKEN", "username": "SALESFORCE_USERNAME",
        "password": "SALESFORCE_PASSWORD", "security_token": "SALESFORCE_SECURITY_TOKEN",
    },
    "servicenow": {"instance_url": "SERVICENOW_INSTANCE_URL",
                   "username": "SERVICENOW_USERNAME", "password": "SERVICENOW_PASSWORD"},
    "odoo": {"url": "ODOO_URL", "database": "ODOO_DB", "db": "ODOO_DB",
             "username": "ODOO_USERNAME", "login": "ODOO_LOGIN",
             "api_key": "ODOO_API_KEY", "password": "ODOO_PASSWORD"},
}


def registered_value(connector, suffix, default=None):
    from nexus_spark_lib.backfill import _control_connection

    if not connector.tenant_id or not connector.connector_id:
        raise ValueError("Backfill configuration requires a tenant and connector")
    with _control_connection(connector.tenant_id) as conn:
        row = conn.execute("""SELECT config FROM nexus_system.connectors
            WHERE tenant_id=%s AND connector_id::text=%s AND enabled AND active
            AND lower(COALESCE(NULLIF(system_type,''),NULLIF(connector_type,''),source_system))
                = ANY(%s)""", (connector.tenant_id, connector.connector_id,
                ["postgres", "postgresql"] if connector.source_type in ("postgres", "postgresql")
                else [connector.source_type.lower()])).fetchone()
    if row is None:
        raise ValueError("Backfill configuration belongs to no active tenant connector")
    config = row[0] or {}
    if not isinstance(config, dict):
        raise ValueError("Registered connector configuration must be an object")
    bulk = config.get("backfill") or {}
    if not isinstance(bulk, dict):
        raise ValueError("Registered backfill configuration must be an object")
    if suffix in bulk:
        value = bulk[suffix]
        return json.dumps(value) if isinstance(value, (dict, list)) else str(value)
    credentials = config.get("credentials") or {}
    if not isinstance(credentials, dict):
        raise ValueError("Registered credentials must be an object")
    if suffix == "credentials":
        fields = _ENV_FIELDS.get(connector.source_type.lower(), {})
        values = {env: str(credentials[key]) for key, env in fields.items()
                  if credentials.get(key) is not None}
        return json.dumps(values) if values else default
    if suffix == "dsn" and connector.source_type.lower() in ("postgres", "postgresql"):
        names = {"host": ("host", "hostname"), "port": ("port",),
                 "dbname": ("database", "dbname"), "user": ("username", "user"),
                 "password": ("password",), "sslmode": ("sslmode",)}
        values = {name: next((credentials[k] for k in keys if credentials.get(k) is not None), None)
                  for name, keys in names.items()}
        if not all(values[k] for k in ("host", "dbname", "user", "password")):
            return default
        return make_conninfo(**{k: str(v) for k, v in values.items() if v is not None})
    return default
