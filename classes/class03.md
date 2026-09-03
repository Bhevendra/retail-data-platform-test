# Class 3 — Amazon S3 → volume with boto3

## Objectives

* Install a Python library in a notebook with `%pip` and explain why Serverless needs it that way.
* Use `boto3` to list and download an object from S3 with access keys.
* Stream the bytes into the raw volume with the Databricks Files API, unchanged.
* Read a Parquet file and compare it with CSV.

## Time plan (95 min)

| Min | Segment |
| --- | --- |
| 0–10 | Recap: what "land raw" meant for SQL Server; today the raw file already exists |
| 10–25 | Mini-lesson: libraries, `%pip`, `import` |
| 25–50 | v1: list and download with boto3 (hard-coded keys) |
| 50–70 | v2: upload into the volume with the Files API |
| 70–85 | Mini-lesson: Parquet vs CSV; read what we landed |
| 85–95 | Homework |

## Mini-lesson: libraries (15 min)

Python has batteries (`datetime`, `json`) and a shop (`pip`). `boto3` is Amazon's
official Python library. On Serverless there is no cluster to pre-install things on,
so a notebook says what it needs at the top:

```python
%pip install -q boto3==1.35.98 databricks-sdk
```

Pin the version (`==`) so the class and the job get the same behaviour. Show that after
`%pip` the cell `import boto3` works and before it does not.

## v1 — list and download (25 min)

```python
import boto3

client = boto3.client(
    "s3",
    aws_access_key_id="PUT_KEY_HERE",            # hard-coded today, secret scope in Class 5
    aws_secret_access_key="PUT_SECRET_HERE",
    region_name="eu-north-1",
)

bucket = "bucket-rivadata"
key = "retail/sales.parquet"

response = client.list_objects_v2(Bucket=bucket, Prefix="retail/")
for obj in response["Contents"]:
    print(obj["Key"], obj["Size"])
```

Explain `response` is a dictionary (a Python type they know) and `["Contents"]` is a
list of dictionaries — walk through it with prints before looping.

Download the bytes:

```python
body = client.get_object(Bucket=bucket, Key=key)["Body"].read()
print(type(body), len(body), "bytes")
```

`bytes` is a new type: raw file content, not text. We do not open it, we move it.

## v2 — land into the volume (20 min)

Why not `dbutils.fs.cp` from `/tmp`? Because Serverless does not allow copying from local
disk into a volume (a real error from the project). The Files API streams instead:

```python
import io
from datetime import date
from databricks.sdk import WorkspaceClient

run_date = date.today().isoformat()
target = f"/Volumes/retaildataplatform/bronze/raw_data/s3_sales/load_date={run_date}"

w = WorkspaceClient()
w.files.create_directory(target)
w.files.upload(f"{target}/sales.parquet", io.BytesIO(body), overwrite=True)

for f in dbutils.fs.ls(target):
    print(f.name, f.size)
```

New syntax to point out: the `f"..."` string with `{run_date}` inside — same as `+` but
easier to read; from now on we use it everywhere.

Checkpoint: the file size in the volume equals `len(body)`. That equality **is** the
"byte-for-byte" promise of the raw layer.

## Mini-lesson: Parquet (15 min)

```python
sales = spark.read.parquet(target)
sales.printSchema()
display(sales.limit(5))
print(sales.count())
```

Compare with the CSV from Class 2: Parquet carries the schema, is compressed and
column-oriented. Show the `product` column — it is a JSON *string* inside a column; we
will parse it in Silver (Class 9). Ask them to spot that `sales` has **no order id
column** — write it on the "data findings" board; it becomes a design problem in Class 8.

## Homework

1. Change `key` to a wrong name and read the error; then change the region and read that error. Write down how the two differ.
2. Land the same file into yesterday's `load_date` folder as well.
3. Count `sales` rows per `product_category` (brand) and per `order_date`.

## Common problems

* `%pip` must be the first thing in the cell and restarts nothing on Serverless — but the import must come *after* the `%pip` cell.
* `NoCredentialsError` → keys not passed; `AccessDenied` → keys wrong or no permission; `NoSuchBucket` → bucket name; region mismatch gives a redirect error.
* `create_directory` on an existing folder is fine; `upload(..., overwrite=True)` is what makes the day re-runnable.
