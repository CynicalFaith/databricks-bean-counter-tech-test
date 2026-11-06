# Bean Counter: Databricks medallion pipeline

Databricks Data Engineer tech test. Bronze → silver → gold on Delta tables in Unity Catalog (`tech_assessment.bean_counter`), built and run on Databricks Free Edition (serverless).

| File | What it is |
|---|---|
| `bean_counter_pipeline.py` | Notebook source (Databricks format; import with *Workspace → Import → URL*) |
| `bean_counter_pipeline.ipynb` / `.html` | The executed notebook **with outputs** |

Assumptions are listed at the top of the notebook.

## Design

**Bronze** (`bronze_orders`, `bronze_customers`): every value lands as a raw string, plus `_source_file` and `_ingested_at`.
- **Orders:** Auto Loader with `trigger(availableNow=True)`. Its checkpoint ensures only new files are read on rerun.
- **Customers:** `COPY INTO` with `mergeSchema`. It is idempotent per file, and it adds the new `marketing_opt_in` column in place. Auto Loader's `addNewColumns` instead stops the stream on a new column, which marks the notebook command as failed.

**Silver:** incremental Delta change-stream reads with `foreachBatch` + `MERGE`, so replays can't duplicate.
- **`silver_orders`:**
  - exact-duplicate removal (SHA-256 of the 7 business columns)
  - `try_cast` typing (serverless runs ANSI mode)
  - both timestamp formats parsed with `coalesce(try_to_timestamp(...), try_to_timestamp(...))`
  - missing `unit_price` filled from `products.csv` and flagged
- **`silver_orders_quarantine`:** rows with `quantity <= 0` or a missing `customer_id` (`NULL`, `""` or the literal `"None"` the generator writes), kept with their raw values and a `rejection_reason`.
- **`silver_customers`:** one current row per customer. `MERGE` updates only when the incoming `updated_at` is newer.

**Gold (Spark SQL):**
- `gold_daily_sales`: revenue, units and distinct orders per day per store.
- `gold_product_ranking`: revenue per product, with `RANK()` within its category.

**Late arrival** is simulated by holding back `customers_batch2.json` and the 2 and 3 March orders. Run 1 loads the initial files, Run 2 loads the late ones, and Run 3 has nothing new.

## Results

| Table | Run 1 | Run 2 | Run 3 (no new files) |
|---|---:|---:|---:|
| bronze_orders | 2,102 | 6,306 | 6,306 |
| bronze_customers | 500 | 700 | 700 |
| silver_orders | 477 | 1,433 | 1,433 |
| silver_orders_quarantine | 1,523 | 4,567 | 4,567 |
| silver_customers | 500 | 600 | 600 |
| gold_daily_sales | 5 | 15 | 15 |

- **Orders reconcile:** 6,306 bronze rows = 306 exact duplicates + 1,433 clean + 4,567 quarantined. No duplicate `order_id`s, no null timestamps or prices in silver, and 52 prices were filled from the product list.
- **Quarantine reasons:**
  - `missing_customer_id`: 2,865
  - `quantity_le_0; missing_customer_id`: 1,134
  - `quantity_le_0`: 568
- **Customers:** 600 rows and 600 distinct IDs. Of the 100 customers in both batches, the 46 whose batch-2 `updated_at` is newer were updated, and the 54 older ones were correctly ignored.
- **Idempotency:** Run 3 counts are identical to Run 2.
- **Independent check:** gold for 1 March matches a pure-Python replay of the generator and rules to the penny.

The seeded generator isn't bit-for-bit reproducible on Databricks. Something else in the notebook process appears to share Python's global `random` state, so counts can differ by a row or two between fresh runs. The reconciliation above holds on every run.
