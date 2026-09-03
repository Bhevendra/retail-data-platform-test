import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


@pytest.fixture(scope="session")
def spark():
    """Local Spark session for pure-DataFrame tests (no Delta / Unity Catalog needed)."""
    pytest.importorskip("pyspark")
    import tempfile

    from pyspark.sql import SparkSession

    scratch = tempfile.mkdtemp(prefix="rdp-spark-")

    session = (
        SparkSession.builder.master("local[2]")
        .appName("retail-platform-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.driver.extraJavaOptions", f"-Duser.timezone=UTC -Dderby.system.home={scratch}/derby")
        .config("spark.sql.warehouse.dir", f"{scratch}/warehouse")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()


class FakeDbutils:
    """Minimal dbutils stand-in for unit tests."""

    class widgets:
        _values: dict = {}

        @classmethod
        def text(cls, name, default):
            cls._values.setdefault(name, default)

        @classmethod
        def get(cls, name):
            return cls._values[name]

    class secrets:
        @staticmethod
        def get(scope, key):
            return f"{scope}/{key}"


@pytest.fixture
def dbutils():
    FakeDbutils.widgets._values = {}
    return FakeDbutils
