"""Plan bounded extraction windows; advance only after confirmed publication."""
from datetime import date, timedelta

from nexus_spark_lib.backfill import _control_connection, _get_var


def _cursor_key(connector_id, table_name=None):
    # Preserve the historical key for existing checkpoints.
    suffix = "__" + table_name.replace(".","__").replace("/","__") if table_name else ""
    return f"bf__{connector_id}__cursor{suffix}"


def _delta(length):
    pieces = length.lower().strip().split()
    if len(pieces) != 2 or int(pieces[0]) < 1:
        raise ValueError("Invalid backfill window length")
    size, unit = int(pieces[0]), pieces[1].rstrip("s")
    if unit == "month":
        from dateutil.relativedelta import relativedelta
        return relativedelta(months=size)
    if unit == "quarter":
        from dateutil.relativedelta import relativedelta
        return relativedelta(months=3*size)
    if unit == "week":
        return timedelta(weeks=size)
    if unit == "day":
        return timedelta(days=size)
    raise ValueError("Unsupported backfill window unit")


def plan_window(connector_id, cfg, *, today=None):
    today = today or date.today()
    key = _cursor_key(connector_id,cfg.get("table_name"))
    persisted = _get_var(key)
    initial = (cfg.get("start_index") or "")[:10]
    backward = cfg.get("fill_direction") in ("backward","newest_to_oldest")
    cursor = date.fromisoformat(persisted or initial) if persisted or initial else (today if backward else today-timedelta(days=365*20))
    boundary = (date.fromisoformat(cfg["stopping_date"][:10]) if cfg.get("stopping_criteria") == "fixed_date"
                else today-timedelta(days=int(cfg.get("overlap_buffer_days",3))))
    delta = _delta(cfg.get("time_window_length") or "1 month")
    if backward:
        if cursor <= boundary:
            return None
        start, end = max(cursor-delta,boundary),cursor
        next_cursor = start
    else:
        if cursor >= boundary:
            return None
        start,end = cursor,min(cursor+delta,boundary)
        next_cursor = end
    return {"start":start.isoformat(),"end":end.isoformat(),"table_name":cfg.get("table_name"),
            "timestamp_column":cfg.get("timestamp_column"),"cursor_key":key,
            "expected_cursor":persisted,"next_cursor":next_cursor.isoformat()}


def plan_windows(connector):
    from nexus_spark_lib.backfill import _validate_connector
    _validate_connector(connector)
    with _control_connection(connector.tenant_id) as conn:
        with conn.cursor() as cursor:
            cursor.execute("""SELECT table_name,timestamp_column,fill_direction,start_index,
                time_window_length,stopping_criteria,stopping_date,overlap_buffer_days
                FROM nexus_system.transaction_backfill_configs
                WHERE connector_id::text=%s ORDER BY table_name""", (connector.connector_id,))
            names = [column.name for column in cursor.description]
            configs = [dict(zip(names,row)) for row in cursor.fetchall()]
    if not configs:
        raise RuntimeError("Transaction backfill requires a persisted per-table policy")
    for cfg in configs:
        for key in ("start_index","stopping_date"):
            if cfg.get(key):
                cfg[key] = cfg[key].isoformat()
    return [planned for cfg in configs if (planned := plan_window(connector.connector_id,cfg))]


def complete_window(window, *, variables=None):
    if variables is None:
        from airflow.models import Variable
        variables = Variable
    current = variables.get(window["cursor_key"],default_var=None)
    if current == window["next_cursor"]:
        return  # Replay after checkpoint commit and before completion event.
    if current != window["expected_cursor"]:
        raise RuntimeError("Backfill checkpoint changed while this window was running")
    variables.set(window["cursor_key"],window["next_cursor"])
