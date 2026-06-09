"""Lance demo/signal_a_demo.py depuis la racine du monorepo (parent de ce dossier).

Usage (tu peux etre dans nexus-spark-lib) :
  py run_signal_a_demo.py
"""

from __future__ import annotations

import pathlib
import runpy

_demo = pathlib.Path(__file__).resolve().parent.parent / "demo" / "signal_a_demo.py"
if not _demo.is_file():
    raise SystemExit(f"Demo introuvable: {_demo}")
runpy.run_path(str(_demo), run_name="__main__")
