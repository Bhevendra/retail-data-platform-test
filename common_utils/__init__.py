"""Reusable data-engineering helpers — generic, not tied to this project or domain.

Everything here should be useful on *any* Spark/Delta project. Nothing in this
package knows what a "customer" or a "sales order" is; project-specific logic
lives in the layer notebooks.

    logger          JSON-line logging
    metadata        audit / lineage columns
    ingestors       read from JDBC, MongoDB/Cosmos, S3, Excel, XML, CSV/JSON/Parquet files
    writers         idempotent and overwrite writes, table helpers
    transforms      rename, cast, trim, null literals, parse JSON, explode arrays
    scd             row hashing, de-duplication, SCD type 1 and type 2 merges
    quality         declarative data-quality rule engine
    governance      comments, tags, constraints, table properties, grants
    observability   per-run / per-entity operational logging
    settings        tiny config + secret helpers (JSON files, secret scopes)
"""

__version__ = "2.0.0"
