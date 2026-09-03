# Operations runbook

## Secret contract

Create the scope named by the DAB target, then add these keys. Values belong in
Databricks-backed secret storage or an Azure Key Vault-backed scope only.

| Source | Secret keys |
| --- | --- |
| Cosmos DB | `cosmos-connection-string` |
| SQL Server | `sqlserver-username`, `sqlserver-password` |
| S3 | `aws-access-key-id`, `aws-secret-access-key` |

The current S3 ingestion uses the scoped AWS keys to copy the original source
object into the raw volume. In production, replace these with a Unity Catalog
storage credential/external location and an IAM role with read-only access.

## SQL Server JDBC options

`jdbc_options` on the SQL Server source in `bronze.json` are appended to the
JDBC URL. The dev configuration sets `trustServerCertificate=true` and
`loginTimeout=90` because the Serverless JDBC driver fails certificate
validation/handshake against Azure SQL with the defaults. Encryption stays on.
For production, prefer `trustServerCertificate=false` with
`hostNameInCertificate=*.database.windows.net` once verified.

## Production controls

* Use a dedicated service principal to deploy and run the job; do not use a
  personal token.
* Give consumers `SELECT` on Gold only. Keep Bronze and Silver access for data
  engineering and approved stewards.
* Configure alerts on failed job runs and on failed `error` severity records in
  `retaildataplatform.bronze.data_quality_results`.
* Retain raw-volume data according to the organisation's retention policy;
  changes to the source contract require a reviewed configuration pull request.
* Rotate upstream credentials because reference notebooks exposed them. No
  credentials from those notebooks should be reused until rotated.
