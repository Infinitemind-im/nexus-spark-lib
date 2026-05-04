"""Unit tests for Signal C — Neo4j graph lift."""

from unittest.mock import MagicMock

from nexus_spark_lib.transform.stage2_resolve.signals.signal_c_graph import (
    _MAX_LIFT,
    _compute_graph_lift,
    run_signal_c,
)


def _make_driver(depth1: int, depth2: int) -> MagicMock:
    record = MagicMock()
    record.__getitem__ = lambda self, key: {"depth1_count": depth1, "depth2_count": depth2}[key]
    result = MagicMock()
    result.single.return_value = record
    session = MagicMock()
    session.run.return_value = result
    session.__enter__ = lambda s: session
    session.__exit__ = MagicMock(return_value=False)
    driver = MagicMock()
    driver.session.return_value = session
    return driver


class TestSignalC:
    def test_no_driver_returns_zero(self):
        assert run_signal_c(None, "gr:abc", "t1") == 0.0

    def test_depth1_lift(self):
        driver = _make_driver(depth1=1, depth2=0)
        lift = _compute_graph_lift(driver, "gr:abc", "t1")
        assert lift == pytest.approx(0.05)

    def test_depth2_lift(self):
        driver = _make_driver(depth1=0, depth2=1)
        lift = _compute_graph_lift(driver, "gr:abc", "t1")
        assert lift == pytest.approx(0.02)

    def test_cap_at_max_lift(self):
        driver = _make_driver(depth1=10, depth2=10)
        lift = _compute_graph_lift(driver, "gr:abc", "t1")
        assert lift == pytest.approx(_MAX_LIFT)

    def test_no_record_returns_zero(self):
        result = MagicMock()
        result.single.return_value = None
        session = MagicMock()
        session.run.return_value = result
        session.__enter__ = lambda s: session
        session.__exit__ = MagicMock(return_value=False)
        driver = MagicMock()
        driver.session.return_value = session
        lift = _compute_graph_lift(driver, "gr:abc", "t1")
        assert lift == 0.0

    def test_exception_returns_zero(self):
        driver = MagicMock()
        driver.session.side_effect = RuntimeError("neo4j down")
        lift = run_signal_c(driver, "gr:abc", "t1")
        assert lift == 0.0


import pytest  # noqa: E402 — needed for approx in class scope
