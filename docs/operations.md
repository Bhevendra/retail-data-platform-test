# Operations runbook

## Secret contract

Create the scope named by the DAB target, then add these keys. Values belong in
Databricks-backed secret storage or an Azure Key Vault-backed scope only.

| Source | Secret keys |
| --- | --- |
| Cosmos DB | `cosmos-connection-string` |
| SQL Server | `sqlserver-username`, `sqlserver-password` |
| S3 when not using a Unity Catalog storage credential | `aws-access-key-id`, `aws-secret-access-key` |

The current S3 path is read through the cluster's Unity Catalog storage
credential. Do not add AWS static keys unless that temporary fallback is
implemented and approved by security.

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
