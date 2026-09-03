"""Retail data platform library.

Shared, unit-tested building blocks used by the Databricks notebooks:

* ``config``        - typed, validated configuration models loaded from JSON
* ``runtime``       - run context (run id, run date, environment) and structured logging
* ``sources``       - source readers and raw-volume landing (Serverless-safe)
* ``bronze``        - idempotent raw -> Bronze loads with audit columns
* ``quality``       - declarative data-quality rule engine with quarantine support
* ``scd``           - deterministic SCD type 1 / type 2 merges
* ``silver``        - Bronze -> Silver standardisation driven by configuration
* ``gold``          - star-schema builder, semantic (metric) views, data dictionary
* ``governance``    - Unity Catalog comments, tags, constraints, table properties, grants
* ``observability`` - run/entity-level operational metrics table

Everything here is compatible with Databricks Serverless compute: no JVM
bridges (``spark._jvm``), no RDD APIs and no cluster-scoped libraries.
"""

__version__ = "1.0.0"
