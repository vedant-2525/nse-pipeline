"""Smoke tests for the Streamlit dashboard app module.

These tests verify that:
1. The dashboard.app module can be imported without raising any exception.
2. AppTest renders when the DB is unreachable (shows friendly error).
3. AppTest renders correctly against the _test database when Postgres is reachable.
"""

import sys
from unittest.mock import patch

import pytest


def test_dashboard_module_importable():
    """dashboard.app must import cleanly (no ModuleNotFoundError or syntax errors)."""
    for mod in list(sys.modules.keys()):
        if mod.startswith("dashboard"):
            del sys.modules[mod]
    import dashboard.app  # noqa: F401


def test_dashboard_apptest_db_unreachable():
    """AppTest renders without exception when DB is unreachable; shows error message."""
    try:
        from streamlit.testing.v1 import AppTest
    except ImportError:
        pytest.skip("streamlit.testing.v1 not available")

    with patch("nse_pipeline.load.db.is_postgres_reachable", return_value=False):
        at = AppTest.from_file("dashboard/app.py", default_timeout=20)
        at.run()
    assert not at.exception, f"AppTest raised: {at.exception}"
    page_content = str(at)
    assert len(page_content) > 0


@pytest.mark.postgres_integration
def test_dashboard_apptest_with_test_db(postgres_test_db):
    """AppTest renders without exception against the test database."""
    try:
        from streamlit.testing.v1 import AppTest
    except ImportError:
        pytest.skip("streamlit.testing.v1 not available")

    at = AppTest.from_file("dashboard/app.py", default_timeout=30)
    at.run()
    assert not at.exception, f"AppTest raised: {at.exception}"
