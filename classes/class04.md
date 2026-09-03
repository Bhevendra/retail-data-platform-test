# Class 4 — Cosmos DB → volume with pymongo

## Objectives

* Explain what a document database is and how it differs from a table.
* Read a collection with `pymongo` and look at documents as Python dictionaries.
* Turn nested fields into JSON strings so every document has the same flat shape.
* Build a Spark DataFrame from a list of Python rows and land it as JSON.

## Time plan (95 min)

| Min | Segment |
| --- | --- |
| 0–10 | Recap: three sources, two landed; what is different about Cosmos |
| 10–25 | Mini-lesson: documents, dictionaries, nesting |
| 25–50 | v1: connect, fetch, inspect documents |
| 50–75 | v2: flatten nested values, create a DataFrame |
| 75–90 | v3: land as JSON; read back |
| 90–95 | Homework |

## Mini-lesson: documents are dictionaries (15 min)

Put one order on the board as JSON and as a Python dict — they are the same thing:

```python
order = {
    "order_number": 317568014,
    "customer_id": 19476252,
    "ordered_products": [
        {"id": "AVpfuJ4pilAPnD_xhDyM", "name": "Rony LBT-GPX555", "price": "993", "qty": "3"},
        {"id": "AVpe6jFBilAPnD_xQxO2", "name": "Aeon Screen", "price": "218", "qty": "3"},
    ],
    "promo_info": [],
}
print(order["order_number"])
print(order["ordered_products"][0]["name"])
print(len(order["ordered_products"]))
```

Point: a table cell holds one value; a document field can hold a *list of dictionaries*.
A table cannot store that directly — so in Bronze we keep the nested part as a JSON
*string* and parse it later.

## v1 — connect and fetch (25 min)

```python
%pip install -q pymongo==4.10.1
```

```python
from pymongo import MongoClient

connection_string = "PUT_CONNECTION_STRING_HERE"   # secret scope in Class 5
client = MongoClient(connection_string, serverSelectionTimeoutMS=30_000)
collection = client["retail"]["sales_orders"]

first = collection.find_one()
print(type(first))
for key, value in first.items():
    print(key, "->", type(value).__name__)
```

Discuss: `_id` is an `ObjectId` (not a Python basic type), `ordered_products` is a
list, `order_datetime` may be a float. Three different problems, one solution next.

```python
documents = list(collection.find({}))
print("documents:", len(documents))
client.close()
```

## v2 — flatten (25 min)

Version A — one document by hand:

```python
from bson import json_util

flat = {}
for field, value in first.items():
    if isinstance(value, (dict, list)):
        flat[field] = json_util.dumps(value)      # nested -> JSON string
    elif value is None or isinstance(value, (str, int, float, bool)):
        flat[field] = value                       # basic types stay
    else:
        flat[field] = str(value)                  # ObjectId, datetime -> text
for k, v in flat.items():
    print(k, "->", type(v).__name__)
```

`isinstance` is new: "is this value one of these types?". Show the three branches with
one example each.

Version B — wrap it as a function and apply to all documents (the sum2no moment):

```python
def flatten_document(document):
    out = {}
    for field, value in document.items():
        if isinstance(value, (dict, list)):
            out[field] = json_util.dumps(value)
        elif value is None or isinstance(value, (str, int, float, bool)):
            out[field] = value
        else:
            out[field] = str(value)
    return out

rows = [flatten_document(d) for d in documents]
print(rows[0])
```

The list comprehension `[f(d) for d in documents]` is new — write the `for` loop version
first, then show the one-liner is the same.

Create the DataFrame:

```python
df = spark.createDataFrame(rows)
df.printSchema()
display(df.limit(5))
```

If the class hits `CANNOT_DETERMINE_TYPE`, that is the real bug from the project: a field
that is `None` in every document. Show the fix idea (decide a type per column by
looking at the non-null values) and tell them the final version lives in
`common_utils/sources.py` as `infer_schema` — they will read it in Class 16.

## v3 — land as JSON (15 min)

```python
from datetime import date
run_date = date.today().isoformat()
target = f"/Volumes/retaildataplatform/bronze/raw_data/cosmos_sales_orders/load_date={run_date}"
df.write.mode("overwrite").format("json").save(target)

back = spark.read.json(target)
print(back.count(), "rows;", "ordered_products type:", back.schema["ordered_products"].dataType.simpleString())
```

Why JSON and not CSV for documents? Because the nested strings contain commas and quotes; JSON handles it natively.

## Homework

1. Count how many orders have an empty `promo_info` (`"[]"`) vs non-empty using only string comparison on the Bronze-style column.
2. Find the order numbers that appear more than once (`groupBy("order_number").count().filter("count > 1")`). Note the number for Class 8.
3. Write `flatten_document` from memory.

## Common problems

* `ServerSelectionTimeoutError` — connection string wrong or firewall; nothing to do with code.
* `bson` comes with `pymongo`; do not `pip install bson` separately (it conflicts).
* `spark.createDataFrame(list_of_dicts)` shows a deprecation warning — fine for now; Class 16 shows the schema-driven version.
