from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


@pytest.fixture(scope="session")
def spark():
    from common.config import load_config
    from common.spark import create_spark_session, stop_spark

    cfg = load_config()
    session = create_spark_session(
        "pytest-spark", cfg, master="local[2]",
        extra_conf={"spark.driver.memory": "2g", "spark.ui.enabled": "false"},
    )
    yield session
    stop_spark(session)
