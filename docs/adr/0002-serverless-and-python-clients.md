# ADR 0002 – Serverless compute with Python source clients

**Status**: accepted (2026-09)

## Context
The workspace is Databricks Free Edition: Serverless only, no cluster-scoped JARs,
no `SparkContext` / `spark._jvm`. The Mongo Spark connector and `dbutils.fs.cp` from
`/tmp` are unavailable; Azure SQL JDBC needs `trustServerCertificate=true` from Serverless.

## Decision
* Cosmos DB is read with `pymongo`; nested documents are serialised to JSON strings in
  Bronze and parsed with explicit schemas in Silver.
* S3 objects are streamed with `boto3` and written through the Databricks Files API.
* SQL Server uses the bundled JDBC driver with driver options declared in config.
* Dependencies are declared once in the job `environments` block.

## Consequences
* Extraction of very large collections runs on the driver; acceptable for these feeds.
  Move to a Spark connector (classic compute) if a source exceeds a few million rows.
* The same code runs unchanged on classic clusters.
