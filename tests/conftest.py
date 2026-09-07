import sys
import tempfile
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


@pytest.fixture(scope="session")
def spark():
    """Local Spark for testing pure DataFrame logic. No Delta, no Unity Catalog."""
    pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession

    scratch = tempfile.mkdtemp(prefix="rdp-spark-")
    session = (
        SparkSession.builder.master("local[2]")
        .appName("retail-platform-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.warehouse.dir", f"{scratch}/warehouse")
        .config("spark.driver.extraJavaOptions", f"-Duser.timezone=UTC -Dderby.system.home={scratch}/derby")
        .enableHiveSupport()
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture(scope="session")
def project_root():
    return ROOT
