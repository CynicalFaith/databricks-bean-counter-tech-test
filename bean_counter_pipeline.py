# Databricks notebook source
# MAGIC %md
# MAGIC # Bean Counter: medallion pipeline (Databricks Data Engineer tech test)
# MAGIC
# MAGIC **Layout:** `landing` volume → **bronze** (Auto Loader, raw strings + lineage) → **silver** (typed, cleansed, quarantined, MERGE'd) → **gold** (SQL aggregates).
# MAGIC All tables live in `tech_assessment.bean_counter`.
# MAGIC
# MAGIC ### Assumptions
# MAGIC 1. **Late arrival is simulated.** The config cell writes every file at once, so I move `customers_batch2.json` and the 2 and 3 March order files into a `_holding` folder, run the pipeline (**Run 1**), move them into landing (**Run 2**), then run again with no new files (**Run 3**) to prove the pipeline is idempotent.
# MAGIC 2. **Bronze stays raw.** Every source column lands as a string, plus `_source_file` and `_ingested_at` (and Auto Loader's `_rescued_data` for orders). All typing happens in silver, so a malformed value can never fail ingestion.
# MAGIC 3. **The generator writes Python `None` as the literal text `"None"`** (`",".join(map(str, r))`). So for `customer_id`, `NULL`, `""`, whitespace and `"None"` all count as *missing*.
# MAGIC 4. **Exact duplicates** means identical on all 7 business columns. The lineage columns differ per row and are ignored. Once exact duplicates are gone, `order_id` is unique, so it serves as the merge key into silver.
# MAGIC 5. **Dedup runs before quarantine.** A duplicated bad row is quarantined once. The spec's two rejection rules (`quantity <= 0`, missing `customer_id`) are applied. I also quarantine rows whose quantity or timestamp can't be parsed, or whose price is still missing after the product lookup, so silver only holds rows that are safe to aggregate. A row can carry several reasons (`;`-separated).
# MAGIC 6. **`unit_price` is filled from `products.csv` only when it is missing.** A price supplied on the order is trusted, because the order is the record of what was charged.
# MAGIC 7. **Customers:** "current" means the latest `updated_at`. A later file carrying an *older* `updated_at` for an existing customer does **not** overwrite the row (out-of-order safe). `marketing_opt_in` is `NULL` for customers never seen in a file that carries it.
# MAGIC 8. **Serverless runs with ANSI mode on,** so every cast or parse uses `try_cast` / `try_to_timestamp`, which return NULL instead of failing the job.
# MAGIC 9. **Revenue** = `quantity * unit_price` on clean silver rows. **Distinct orders** = `COUNT(DISTINCT order_id)`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Configuration code (provided, unchanged apart from creating the catalog)

# COMMAND ----------

spark.sql("CREATE CATALOG IF NOT EXISTS tech_assessment")

# COMMAND ----------

import random, json, datetime as dt
catalog, schema, volume = "tech_assessment", "bean_counter", "landing"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.{volume}")
base = f"/Volumes/{catalog}/{schema}/{volume}"
random.seed(42)
stores = ["LDN01", "MAN01", "LDS01", "BRS01", "WEB01"]
products = [(f"P{i:03d}", n, c, p) for i, (n, c, p) in enumerate([
    ("Flat White", "Hot Drinks", 3.20), ("Latte", "Hot Drinks", 3.40),
    ("Cappuccino", "Hot Drinks", 3.30), ("Iced Mocha", "Cold Drinks", 4.10),
    ("Croissant", "Food", 2.50), ("Banana Bread", "Food", 2.80),
    ("Beans 1kg", "Retail", 14.00), ("Reusable Cup", "Retail", 9.50)], start=1)]
# Products (clean reference data)
with open(f"{base}/products.csv", "w") as f:
    f.write("product_id,product_name,category,unit_price\n")
    for p in products:
        f.write(",".join(map(str, p)) + "\n")
# Customers (JSON lines, batch 1 and a later batch with a new column)
def customers(start, n, extra=False):
    rows = []
    for i in range(start, start + n):
        r = {"customer_id": f"C{i:05d}", "email": f"user{i}@example.com",
             "city": random.choice(["London", "Manchester", "Leeds", "Bristol", None]),
             "signup_date": str(dt.date(2024, 1, 1) + dt.timedelta(days=random.randint(0, 600))),
             "updated_at": str(dt.datetime(2025, 1, 1) + dt.timedelta(hours=random.randint(0, 2000)))}
        if extra:
            r["marketing_opt_in"] = random.choice([True, False])
        rows.append(r)
    return rows
def write_jsonl(path, rows):
    with open(path, "w") as f:
        for r in rows:
            f.write(json.dumps(r) + "\n")
import os
for d in ["orders", "customers"]:
    os.makedirs(f"{base}/{d}", exist_ok=True)
write_jsonl(f"{base}/customers/customers_batch1.json", customers(1, 500))
# batch 2: 100 updated customers (changes) + 100 new, plus a NEW column
write_jsonl(f"{base}/customers/customers_batch2.json", customers(401, 200, extra=True))
# Orders: 3 daily files, with duplicates, nulls, bad values, mixed date formats
def orders(day, n, start_id):
    rows = []
    for i in range(n):
        oid = start_id + i
        p = random.choice(products)
        qty = random.choice([1, 1, 1, 2, 3, -1, 0])  # bad quantities
        cust = random.choice([f"C{random.randint(1, 550):05d}", "", None])  # missing customers
        ts = dt.datetime.combine(day, dt.time(random.randint(7, 20), random.randint(0, 59)))
        fmt = random.choice(["%Y-%m-%d %H:%M:%S", "%d/%m/%Y %H:%M"])  # mixed formats
        rows.append([f"O{oid:06d}", cust, random.choice(stores), p[0], qty,
                     p[3] if random.random() > 0.03 else "", ts.strftime(fmt)])
        if random.random() < 0.05:  # duplicates
            rows.append(rows[-1])
    return rows
for n, day in enumerate([dt.date(2025, 3, 1), dt.date(2025, 3, 2), dt.date(2025, 3, 3)]):
    with open(f"{base}/orders/orders_{day}.csv", "w") as f:
        f.write("order_id,customer_id,store_id,product_id,quantity,unit_price,order_ts\n")
        for r in orders(day, 2000, n * 10000):
            f.write(",".join(map(str, r)) + "\n")
print("Landing files:", os.listdir(f"{base}/orders"), os.listdir(f"{base}/customers"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Pipeline setup
# MAGIC Checkpoints and Auto Loader schema state get their own volume, kept apart from the landing data.
# MAGIC `reset_pipeline()` is a convenience for a clean rerun of the whole test. It is **not** part of the normal run.

# COMMAND ----------

from pyspark.sql import functions as F, Window
from delta.tables import DeltaTable

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.pipeline_state")

fq = lambda t: f"{catalog}.{schema}.{t}"
state = f"/Volumes/{catalog}/{schema}/pipeline_state"
holding = f"{base}/_holding"

def reset_pipeline():
    """Drop every pipeline table and checkpoint (landing files are untouched)."""
    for t in ["bronze_orders", "bronze_customers", "silver_products", "silver_orders",
              "silver_orders_quarantine", "silver_customers", "gold_daily_sales", "gold_product_ranking"]:
        spark.sql(f"DROP TABLE IF EXISTS {fq(t)}")
    dbutils.fs.rm(state, recurse=True)

reset_pipeline()  # start this demo from a clean slate

# COMMAND ----------

# MAGIC %md
# MAGIC ### Simulate late arrival
# MAGIC Hold back `customers_batch2.json` and the 2 and 3 March order files. They are released before Run 2.

# COMMAND ----------

late_files = ["customers/customers_batch2.json", "orders/orders_2025-03-02.csv", "orders/orders_2025-03-03.csv"]

def hold_back():
    for f in late_files:
        dbutils.fs.mv(f"{base}/{f}", f"{holding}/{f}")

def release():
    for f in late_files:
        dbutils.fs.mv(f"{holding}/{f}", f"{base}/{f}")

hold_back()
print("orders landing:   ", [f.name for f in dbutils.fs.ls(f"{base}/orders")])
print("customers landing:", [f.name for f in dbutils.fs.ls(f"{base}/customers")])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Bronze: incremental ingestion (Auto Loader for orders, COPY INTO for customers)
# MAGIC * **Only new files are processed on rerun.** Auto Loader tracks ingested files in its checkpoint, and `trigger(availableNow=True)` runs it as an incremental batch: it processes whatever is new, then stops. `COPY INTO` keeps the list of loaded files in the target table's metadata and skips them on rerun (it is idempotent).
# MAGIC * **Lineage:** `_source_file` comes from `_metadata.file_path` and `_ingested_at` from `current_timestamp()`.
# MAGIC * **Schema drift (`marketing_opt_in`).** I tried Auto Loader for customers first, but with `addNewColumns` it *deliberately stops the stream* when it sees a new column, and expects a restart. Even with an automatic retry, Databricks marks the notebook command as failed ("Some streams terminated…"), so the pipeline effectively breaks. For the drifting JSON source I therefore use **`COPY INTO` with `mergeSchema`** in both the format and copy options. The new column is added to `bronze_customers` in place, earlier rows get `NULL`, and nothing stops. Orders (a stable CSV contract) stay on Auto Loader, with `_rescued_data` catching anything unexpected.
# MAGIC * Bronze keeps every value as a string (`inferColumnTypes=false` / `primitivesAsString=true`). All typing happens in silver.

# COMMAND ----------

def run_autoloader(source_dir, fmt, target, extra_opts=None):
    """Incrementally load only new files from source_dir into a bronze Delta table (Auto Loader)."""
    name = target.split(".")[-1]
    reader = (spark.readStream.format("cloudFiles")
              .option("cloudFiles.format", fmt)
              .option("cloudFiles.schemaLocation", f"{state}/schemas/{name}")
              .option("cloudFiles.inferColumnTypes", "false"))          # bronze = raw strings
    for k, v in (extra_opts or {}).items():
        reader = reader.option(k, v)
    (reader.load(source_dir)
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_ingested_at", F.current_timestamp())
        .writeStream
        .option("checkpointLocation", f"{state}/checkpoints/{name}")
        .trigger(availableNow=True)
        .toTable(target)
        .awaitTermination())


def run_copy_into(source_dir, fmt, target):
    """Idempotent, schema-evolving load of only new files (COPY INTO)."""
    spark.sql(f"CREATE TABLE IF NOT EXISTS {target}")      # schemaless placeholder; COPY INTO sets the schema
    res = spark.sql(f"""
        COPY INTO {target}
        FROM (SELECT *, _metadata.file_path AS _source_file, current_timestamp() AS _ingested_at
              FROM '{source_dir}')
        FILEFORMAT = {fmt}
        FORMAT_OPTIONS ('mergeSchema' = 'true', 'primitivesAsString' = 'true')
        COPY_OPTIONS ('mergeSchema' = 'true')""").first()
    print(f"[{target.split('.')[-1]}] COPY INTO inserted {res['num_inserted_rows']} rows")


def run_bronze():
    run_autoloader(f"{base}/orders", "csv", fq("bronze_orders"), {"header": "true"})
    run_copy_into(f"{base}/customers", "JSON", fq("bronze_customers"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Silver: cleansing
# MAGIC
# MAGIC Silver reads bronze **incrementally** too (a Delta change stream with its own checkpoint), and `foreachBatch` applies each micro-batch with `MERGE`. Each run therefore only touches new bronze rows, and replaying a batch can't create duplicates.
# MAGIC
# MAGIC Each table is created up front with an explicit schema, so silver's contract never depends on what bronze happened to infer.

# COMMAND ----------

spark.sql(f"""CREATE TABLE IF NOT EXISTS {fq('silver_orders')} (
    order_id STRING NOT NULL, customer_id STRING NOT NULL, store_id STRING, product_id STRING,
    quantity INT, unit_price DECIMAL(10,2), unit_price_filled BOOLEAN, order_ts TIMESTAMP,
    _source_file STRING, _ingested_at TIMESTAMP) CLUSTER BY (order_id)""")

spark.sql(f"""CREATE TABLE IF NOT EXISTS {fq('silver_orders_quarantine')} (
    row_hash STRING, order_id STRING, customer_id STRING, store_id STRING, product_id STRING,
    quantity STRING, unit_price STRING, order_ts STRING, rejection_reason STRING,
    _source_file STRING, _ingested_at TIMESTAMP, _quarantined_at TIMESTAMP)""")

spark.sql(f"""CREATE TABLE IF NOT EXISTS {fq('silver_customers')} (
    customer_id STRING NOT NULL, email STRING, city STRING, signup_date DATE,
    marketing_opt_in BOOLEAN, updated_at TIMESTAMP,
    _source_file STRING, _ingested_at TIMESTAMP, _merged_at TIMESTAMP) CLUSTER BY (customer_id)""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3a. Products reference
# MAGIC The products file is small and clean, so it's reloaded in full each run.

# COMMAND ----------

def load_products():
    (spark.read.option("header", "true")
          .schema("product_id STRING, product_name STRING, category STRING, unit_price DECIMAL(10,2)")
          .csv(f"{base}/products.csv")
          .write.mode("overwrite").saveAsTable(fq("silver_products")))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3b. Orders: type, dedupe, fill price, quarantine
# MAGIC Steps per micro-batch:
# MAGIC 1. **Exact dedupe** on the 7 raw business columns. A SHA-256 `row_hash` of those columns is also the quarantine merge key.
# MAGIC 2. **Type:** `try_cast` quantity to INT and price to DECIMAL. Parse **both** timestamp formats with `coalesce(try_to_timestamp(.., 'yyyy-MM-dd HH:mm:ss'), try_to_timestamp(.., 'dd/MM/yyyy HH:mm'))`.
# MAGIC 3. **Fill price** from `silver_products` when it is missing, and flag it with `unit_price_filled`.
# MAGIC 4. **Validate:** build a `rejection_reason`. Rows that have one are *merged* into quarantine with the original raw values. Clean rows are *merged* into `silver_orders` on `order_id`.

# COMMAND ----------

BIZ_COLS = ["order_id", "customer_id", "store_id", "product_id", "quantity", "unit_price", "order_ts"]

def transform_orders(bronze_df):
    prices = spark.table(fq("silver_products")).select("product_id", F.col("unit_price").alias("ref_price"))
    cust = F.trim(F.col("customer_id"))
    df = (bronze_df
          .withColumn("row_hash", F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c), F.lit("<null>")) for c in BIZ_COLS]), 256))
          .withColumn("_rn", F.row_number().over(Window.partitionBy("row_hash").orderBy("_ingested_at", "_source_file")))
          .filter("_rn = 1").drop("_rn")                                     # exact duplicates removed
          .withColumn("qty_int", F.expr("try_cast(quantity AS INT)"))
          .withColumn("ts", F.expr("coalesce(try_to_timestamp(order_ts, 'yyyy-MM-dd HH:mm:ss'), "
                                   "try_to_timestamp(order_ts, 'dd/MM/yyyy HH:mm'))"))
          .withColumn("price_in", F.expr("try_cast(nullif(trim(unit_price), '') AS DECIMAL(10,2))"))
          .join(F.broadcast(prices), "product_id", "left")
          .withColumn("price", F.coalesce("price_in", "ref_price"))
          .withColumn("cust_missing", cust.isNull() | (cust == "") | (F.lower(cust).isin("none", "null", "nan"))))
    reasons = F.concat_ws("; ",
        F.when(F.col("qty_int").isNull(), F.lit("invalid_quantity")),
        F.when(F.col("qty_int") <= 0, F.lit("quantity_le_0")),
        F.when(F.col("cust_missing"), F.lit("missing_customer_id")),
        F.when(F.col("ts").isNull(), F.lit("unparseable_order_ts")),
        F.when(F.col("price").isNull(), F.lit("missing_unit_price")))
    return df.withColumn("rejection_reason", F.nullif(reasons, F.lit("")))


def upsert_orders(batch_df, batch_id):
    df = transform_orders(batch_df)   # no .cache(): not supported on serverless

    good = df.filter("rejection_reason IS NULL").select(
        "order_id", F.trim("customer_id").alias("customer_id"), "store_id", "product_id",
        F.col("qty_int").alias("quantity"), F.col("price").alias("unit_price"),
        (F.col("price_in").isNull() & F.col("price").isNotNull()).alias("unit_price_filled"),
        F.col("ts").alias("order_ts"), "_source_file", "_ingested_at")
    (DeltaTable.forName(spark, fq("silver_orders")).alias("t")
        .merge(good.alias("s"), "t.order_id = s.order_id")
        .whenNotMatchedInsertAll().execute())

    bad = df.filter("rejection_reason IS NOT NULL").select(
        "row_hash", *BIZ_COLS, "rejection_reason", "_source_file", "_ingested_at",
        F.current_timestamp().alias("_quarantined_at"))
    (DeltaTable.forName(spark, fq("silver_orders_quarantine")).alias("t")
        .merge(bad.alias("s"), "t.row_hash = s.row_hash")
        .whenNotMatchedInsertAll().execute())

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3c. Customers: one current row per `customer_id` via MERGE
# MAGIC * Within a micro-batch, keep the latest `updated_at` per customer (`row_number`).
# MAGIC * `MERGE` updates only when the incoming `updated_at` is **newer** (`>`). Replaying the same data, or receiving an older record, is a no-op, so no duplicates and no regressions.
# MAGIC * `marketing_opt_in` is selected only if bronze has it (schema-tolerant).

# COMMAND ----------

def upsert_customers(batch_df, batch_id):
    opt_in = (F.expr("try_cast(marketing_opt_in AS BOOLEAN)") if "marketing_opt_in" in batch_df.columns
              else F.lit(None).cast("boolean"))
    latest = (batch_df
        .select(F.trim("customer_id").alias("customer_id"), F.lower(F.trim("email")).alias("email"),
                F.nullif(F.trim("city"), F.lit("")).alias("city"),
                F.expr("try_cast(signup_date AS DATE)").alias("signup_date"),
                opt_in.alias("marketing_opt_in"),
                F.expr("try_to_timestamp(updated_at)").alias("updated_at"),
                "_source_file", "_ingested_at")
        .filter("customer_id IS NOT NULL AND customer_id <> ''")
        .withColumn("_rn", F.row_number().over(
            Window.partitionBy("customer_id").orderBy(F.col("updated_at").desc_nulls_last(), F.col("_ingested_at").desc())))
        .filter("_rn = 1").drop("_rn")
        .withColumn("_merged_at", F.current_timestamp()))
    (DeltaTable.forName(spark, fq("silver_customers")).alias("t")
        .merge(latest.alias("s"), "t.customer_id = s.customer_id")
        .whenMatchedUpdateAll(condition="s.updated_at > t.updated_at")
        .whenNotMatchedInsertAll()
        .execute())

# COMMAND ----------

def run_silver_stream(source, name, fn):
    (spark.readStream.option("mergeSchema", "true").table(fq(source))   # tolerate bronze schema growth
        .writeStream.foreachBatch(fn)
        .option("checkpointLocation", f"{state}/checkpoints/{name}")
        .trigger(availableNow=True).start().awaitTermination())

def run_silver():
    load_products()
    run_silver_stream("bronze_orders", "silver_orders", upsert_orders)
    run_silver_stream("bronze_customers", "silver_customers", upsert_customers)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Gold (Spark SQL)
# MAGIC Gold is small, so it's rebuilt from silver each run with `CREATE OR REPLACE`, which is idempotent by construction.

# COMMAND ----------

def run_gold():
    spark.sql(f"""
    CREATE OR REPLACE TABLE {fq('gold_daily_sales')} AS
    SELECT  CAST(order_ts AS DATE)               AS order_date,
            store_id,
            SUM(quantity * unit_price)           AS revenue,
            SUM(quantity)                        AS units,
            COUNT(DISTINCT order_id)             AS distinct_orders
    FROM    {fq('silver_orders')}
    GROUP BY CAST(order_ts AS DATE), store_id
    """)
    spark.sql(f"""
    CREATE OR REPLACE TABLE {fq('gold_product_ranking')} AS
    WITH sales AS (
        SELECT product_id, SUM(quantity * unit_price) AS revenue, SUM(quantity) AS units
        FROM   {fq('silver_orders')}
        GROUP BY product_id)
    SELECT  p.category,
            p.product_id,
            p.product_name,
            COALESCE(s.revenue, 0)                                              AS revenue,
            COALESCE(s.units, 0)                                                AS units,
            RANK() OVER (PARTITION BY p.category ORDER BY COALESCE(s.revenue, 0) DESC) AS rank_in_category
    FROM    {fq('silver_products')} p
    LEFT JOIN sales s USING (product_id)
    """)

# COMMAND ----------

def row_counts():
    tables = ["bronze_orders", "bronze_customers", "silver_orders", "silver_orders_quarantine",
              "silver_customers", "gold_daily_sales", "gold_product_ranking"]
    return spark.createDataFrame([(t, spark.table(fq(t)).count()) for t in tables], "table STRING, rows LONG")

def run_pipeline(label):
    print(f"===== {label} =====")
    run_bronze(); run_silver(); run_gold()
    display(row_counts())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Runs
# MAGIC ### Run 1: initial files only (1 March orders, customers batch 1)

# COMMAND ----------

run_pipeline("Run 1 - initial landing files")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run 2: late files arrive (2 and 3 March orders, customers batch 2 with the new `marketing_opt_in` column)
# MAGIC `COPY INTO` picks up only the new files and adds `marketing_opt_in` to `bronze_customers` without stopping.

# COMMAND ----------

release()
run_pipeline("Run 2 - late files arrived")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run 3: rerun with no new files (idempotency check)
# MAGIC Every count should be identical to Run 2.

# COMMAND ----------

run_pipeline("Run 3 - rerun, nothing new")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Validation

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Bronze: each file ingested exactly once, with lineage
# MAGIC SELECT 'bronze_orders' AS tbl, _source_file, COUNT(*) AS rows, MIN(_ingested_at) AS ingested_at FROM tech_assessment.bean_counter.bronze_orders GROUP BY ALL
# MAGIC UNION ALL
# MAGIC SELECT 'bronze_customers', _source_file, COUNT(*), MIN(_ingested_at) FROM tech_assessment.bean_counter.bronze_customers GROUP BY ALL
# MAGIC ORDER BY 1, 2

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Bronze customers schema now includes marketing_opt_in (NULL for batch 1)
# MAGIC SELECT _source_file, COUNT(*) AS rows, COUNT(marketing_opt_in) AS rows_with_opt_in
# MAGIC FROM tech_assessment.bean_counter.bronze_customers GROUP BY ALL ORDER BY 1

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Orders reconciliation: bronze = exact duplicates + silver + quarantine
# MAGIC WITH b AS (SELECT COUNT(*) AS bronze_rows,
# MAGIC                   COUNT(DISTINCT order_id, customer_id, store_id, product_id, quantity, unit_price, order_ts) AS distinct_rows_approx
# MAGIC            FROM tech_assessment.bean_counter.bronze_orders)
# MAGIC SELECT b.bronze_rows,
# MAGIC        b.bronze_rows - (s.n + q.n)                    AS exact_duplicates_removed,
# MAGIC        s.n                                             AS silver_rows,
# MAGIC        q.n                                             AS quarantined_rows,
# MAGIC        s.filled                                        AS prices_filled_from_products,
# MAGIC        s.n - s.distinct_ids                            AS duplicate_order_ids_in_silver
# MAGIC FROM b,
# MAGIC      (SELECT COUNT(*) n, COUNT(DISTINCT order_id) distinct_ids, COUNT_IF(unit_price_filled) filled FROM tech_assessment.bean_counter.silver_orders) s,
# MAGIC      (SELECT COUNT(*) n FROM tech_assessment.bean_counter.silver_orders_quarantine) q

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Quarantine breakdown by reason
# MAGIC SELECT rejection_reason, COUNT(*) AS rows
# MAGIC FROM tech_assessment.bean_counter.silver_orders_quarantine
# MAGIC GROUP BY ALL ORDER BY rows DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Silver orders: both timestamp formats parsed, no bad rows leaked through
# MAGIC SELECT COUNT(*)                                        AS rows,
# MAGIC        COUNT_IF(order_ts IS NULL)                      AS null_ts,
# MAGIC        COUNT_IF(quantity <= 0)                         AS bad_qty,
# MAGIC        COUNT_IF(unit_price IS NULL)                    AS null_price,
# MAGIC        MIN(order_ts) AS min_ts, MAX(order_ts) AS max_ts
# MAGIC FROM tech_assessment.bean_counter.silver_orders

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Raw formats -> parsed timestamp (one example of each)
# MAGIC SELECT b.order_ts AS raw_order_ts, s.order_ts AS parsed_order_ts
# MAGIC FROM tech_assessment.bean_counter.bronze_orders b
# MAGIC JOIN tech_assessment.bean_counter.silver_orders s USING (order_id)
# MAGIC QUALIFY ROW_NUMBER() OVER (PARTITION BY b.order_ts LIKE '%/%' ORDER BY order_id) = 1

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Customers: one row per customer, batch-2 changes applied only where newer
# MAGIC SELECT COUNT(*) AS customers, COUNT(DISTINCT customer_id) AS distinct_ids,
# MAGIC        COUNT_IF(_source_file LIKE '%batch2%') AS rows_from_batch2,
# MAGIC        COUNT(marketing_opt_in) AS with_opt_in
# MAGIC FROM tech_assessment.bean_counter.silver_customers

# COMMAND ----------

# MAGIC %sql
# MAGIC -- For the 100 overlapping customers (C00401-C00500): batch2 wins only where its updated_at is newer
# MAGIC WITH b1 AS (SELECT customer_id, CAST(updated_at AS TIMESTAMP) u FROM tech_assessment.bean_counter.bronze_customers WHERE _source_file LIKE '%batch1%'),
# MAGIC      b2 AS (SELECT customer_id, CAST(updated_at AS TIMESTAMP) u FROM tech_assessment.bean_counter.bronze_customers WHERE _source_file LIKE '%batch2%')
# MAGIC SELECT CASE WHEN b2.u > b1.u THEN 'batch2 newer' ELSE 'batch2 older/equal' END AS comparison,
# MAGIC        COUNT(*) AS customers,
# MAGIC        COUNT_IF(s._source_file LIKE '%batch2%') AS silver_took_batch2
# MAGIC FROM b1 JOIN b2 USING (customer_id)
# MAGIC JOIN tech_assessment.bean_counter.silver_customers s USING (customer_id)
# MAGIC GROUP BY ALL

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Gold outputs

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM tech_assessment.bean_counter.gold_daily_sales ORDER BY order_date, store_id

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM tech_assessment.bean_counter.gold_product_ranking ORDER BY category, rank_in_category

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Notes and what I'd do next
# MAGIC * **Auto Loader vs COPY INTO.** Auto Loader scales to millions of files (it can switch to file-notification mode) and rescues bad data, so it is my default for high-volume feeds like orders. `COPY INTO` suits low-volume, schema-drifting batches like the customers extracts: it evolves the schema in place, without the stop-and-restart Auto Loader needs. In a scheduled Job with retries, Auto Loader `addNewColumns` would also be fine for customers.
# MAGIC * **Productionising.** I'd split this into bronze, silver and gold tasks in a Lakeflow Job, or port it to Lakeflow Declarative Pipelines, where `expect_or_drop` would replace the hand-rolled quarantine and Auto CDC (`APPLY CHANGES`, SCD1, sequenced by `updated_at`) would replace the customer MERGE. Jobs retry automatically, which covers Auto Loader's schema-evolution restart.
# MAGIC * **Data quality to raise with the business.** About two thirds of orders lack a customer (the generator picks `""` or `None` two times in three), and about 2 in 7 have quantity ≤ 0. I'd want to know whether negative quantities are refunds that belong in revenue, rather than errors.
# MAGIC * **Gold at scale.** I'd compute it incrementally (MERGE only the affected dates and stores) or as materialized views, instead of a full rebuild.
# MAGIC * **Tests.** I'd add unit tests for `transform_orders` with a small fixture DataFrame covering each rejection rule and both timestamp formats.
