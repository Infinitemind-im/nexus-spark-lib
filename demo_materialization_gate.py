#!/usr/bin/env python3
"""Demo Op 3 — Materialization Gate (ER + entity_store_presence)."""

from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

try:
    from dotenv import load_dotenv

    load_dotenv(ROOT.parent / ".env")
except ImportError:
    pass

from nexus_spark_lib.transform.stage2_resolve.materialization_gate import run_materialization_gate

DEFAULT_DSN = "postgresql://nexus_app:nexusapp@localhost:5444/nexus_db"


def main() -> None:
    dsn = os.getenv("NEXUS_SYSTEM_DSN") or os.getenv("NEXUS_DB_DSN") or DEFAULT_DSN
    tenant = "tenant_abc"
    entity = "gr:demo1"

    print("Materialization Gate demo (Spark ER Op 3)\n")
    print(f"DSN tail: {dsn.split('@')[-1]}\n")

    cases = [
        ("Acme-like (tax_id + hot entity)", {"tax_id": {"value": "BE0123456789"}}, entity),
        ("Unknown tax_id", {"tax_id": {"value": "XX000"}}, None),
    ]

    for label, fields, force_id in cases:
        out = run_materialization_gate(
            tenant_id=tenant,
            cdm_entity_type="Party.Organisation",
            source_connector="salesforce-tenant-abc",
            source_record_id="0010Y00000XxXxXXAA",
            fields=fields,
            system_dsn=dsn,
            cdm_entity_id=force_id,
        )
        print(f"[{label}]")
        print(f"  cdm_entity_id={out.cdm_entity_id!r}")
        print(f"  materialization={out.materialization_level}")
        print(f"  proceed_pipeline={out.proceed_pipeline} write_ai_stores={out.write_ai_stores}")
        print(f"  register_er_index_only={out.register_er_index_only} skip={out.skip_reason!r}\n")

    print("Prep hot entity for live test:")
    print(f"  python scripts/test_entity_store_presence.py set-hot --tenant {tenant} --entity {entity}")


if __name__ == "__main__":
    main()
