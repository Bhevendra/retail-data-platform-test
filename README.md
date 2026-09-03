# Retail Data Platform

Config-driven Databricks lakehouse for Cosmos DB, Amazon S3, and SQL Server.

The platform lands immutable source files in the Unity Catalog volume
`retaildataplatform.bronze.raw_data`, creates auditable Bronze Delta tables,
applies configurable SCD processing in Silver, and publishes governed Gold
data products. It is deployed with Databricks Asset Bundles (DAB).

## Design

```
Cosmos DB / S3 / SQL Server
             │
             ▼
 bronze.raw_data volume (original bytes, partitioned by source/entity/load date)
             │
             ▼
 bronze.<entity> (Delta + last_update_ts + file_path + operational metadata)
             │
             ▼
 silver.<entity> (SCD type 1 or type 2, selected in configuration)
             │
             ▼
 gold.<data product> (BI/AI-ready views or tables)
```

## Repository layout

* `src/<layer>/config/` — one JSON configuration per pipeline notebook/task.
* `common_utils/` — reusable config, secret, ingestion, quality, and SCD helpers.
* `src/ingestion/` — three source-specific ingestion notebooks, each landing data in the raw Unity Catalog volume.
* `src/<layer>/notebook/` — Databricks source notebooks: `ds2b`, `b2s`, `s2g`.
* `resources/` — DAB jobs and pipeline permissions.
* `tests/` — fast local unit tests for config and transformation logic.

## Prerequisites and one-time setup

1. Install the Databricks CLI and authenticate to each workspace.
2. Create a secret scope named `retail-platform-<environment>` (or change
   `secret_scope` in the target configuration). Store the required keys shown
   in `src/bronze/config/bronze.json`; never commit values.
3. On AWS, create an external location/storage credential with read access to
   the S3 prefix, or enable the JDBC/S3 fallback secrets defined in the config.
4. Grant the deployment identity `USE CATALOG`, `CREATE SCHEMA`,
   `CREATE VOLUME`, `CREATE TABLE`, and `MODIFY` on `retaildataplatform`.

The platform creates the catalog objects if they do not already exist. In a
production organisation, a platform-admin deployment should own catalog,
schema, external-location, and grants provisioning separately from pipeline
deployment.

## Deploy

```bash
databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle run retail_data_platform -t dev
```

Use `-t prod` only after the secret scope, Unity Catalog grants, job service
principal, and production configuration have been reviewed.

## Data quality and governance

Every Bronze write records a run identifier, source system, load timestamp and
input path. Quality rules are declared per entity in `src/bronze/config/bronze.json` and
write results to `bronze.data_quality_results`. A failed blocking rule stops the
entity before Silver processing. Unity Catalog tags, ownership, comments,
least-privilege grants, lineage through Delta tables, schema evolution controls,
and a PII classification field are all defined in configuration.

## Notes on supplied upstream notebooks

They are treated only as source-system references. Their embedded credentials
must be rotated and stored in a secret manager; this project never contains
those credentials.

## Manual Serverless execution

The Cosmos notebook installs `pymongo` with `%pip` and uses the Python MongoDB
client, so it can run on Serverless compute without uploading a JVM Spark
connector JAR. Run all notebook cells once after pulling the latest changes.

The S3 notebook uses the Databricks Workspace Files API to stream source files
into the volume. This avoids the Serverless restriction on copying local `/tmp`
files into Unity Catalog volumes.
