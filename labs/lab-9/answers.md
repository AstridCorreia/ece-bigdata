# Lab 9 - Trino: querying the silver layer and building the gold layer

Trino was deployed with Helm (deployment lab), coordinator only, with a `lakehouse` catalog of type Iceberg REST pointing to Polaris (principal `dev`). The Python connection is in `labs/lab-9/trino_conn.py`, run in `uv run python` with `exec(open(...).read())`. The `trino` and `pandas` dependencies were added with `uv add`.

## Environment

### What does the `catalog=` parameter of `connect()` change in the statements which follow?
It sets the default catalog of the session. We could write `silver.orders` instead of `lakehouse.silver.orders`, and Trino would complete it with `lakehouse`. In the lab we still use the full name everywhere, so it doesn't change the results, but it avoids errors if we forget it.

## Catalogs, schemas and tables

`SHOW CATALOGS` gives `lakehouse`, `system`, `tpcds` and `tpch`. In `lakehouse` we find the `silver` schema with `orders` and `users`, the tables registered in Polaris in the previous lab. `users` also has the `country` column added during the Polaris lab.

### Which Trino type holds `ordered_at`? Does it keep a time zone?
`timestamp(6) with time zone`. Yes, it's the equivalent of the Iceberg `timestamptz` type written by Spark: the value is stored in UTC and Trino displays it with `+00:00`, with microsecond precision.

### The Spark lab declared `order_id` as a string because Spark has no `UUID` type. What does Trino report?
`varchar`. Trino has a `uuid` type, but it reads the type of the Iceberg table, which is `string` because Spark wrote it. The type is set by the table, not by the engine reading it.

## Query the silver layer

Results: 2829 orders and 50 users, like in the Spark lab. The query by state gives at most 3 users per state, and only AE (3/3) and AP (2/2) have missing cities. The orphan orders query returns 0.

### Explain the output of the queries just run.
The counts are the same as in Spark because Trino reads exactly the same Iceberg table (same snapshot), through Polaris. The missing cities come from military addresses ("DPO AP 09617"), which have no city, so the regex of the Spark lab set `city` to NULL. 0 orphan orders: every `user_id` in `orders` exists in `users`, so an inner join won't lose any order.

### Which test of the dbt lab does the orphan query replace? What would you write to run it on every refresh?
The `relationships` test on `stg_orders.user_id` (and our singular test `assert_orders_have_users`). To run it on every refresh, we can put it in the validation step of the pipeline with an `assert orphan_orders == 0` before building the gold layer, or keep the test in dbt and run `dbt test` at each run, so the pipeline stops if it fails.

### Trino returned the results without a job to submit or a session to create. What does that tell about when to choose Trino instead of Spark?
Trino is a service that is always running: we send a SQL query and get the answer in a few seconds. It fits interactive queries, exploration and dashboards. Spark needs to start a session (JVM, packages, config), so it's better for big batch processing and transformations in Python.

## Iceberg metadata

`orders$snapshots` and `orders$history` show a single snapshot (`overwrite`), and `orders$files` a single Parquet file with 2829 rows (84,055 bytes).

### How many snapshots does `silver.orders` have? Which engine wrote them?
One `overwrite` snapshot, written by Spark with `writeTo(...).createOrReplace()` when I recreated the table in the `lab` catalog, then registered in Polaris. Trino didn't write anything in this table, it only reads it.

### List the files of the table with `aws s3 ls`, and compare with the `$files` table. Which one does not need access to the bucket, and why?
`aws s3 ls` lists all the objects of the folder: the Parquet file in `data/` but also the files in `metadata/` (JSON, Avro manifests, `version-hint.text`). `$files` only shows the data file that belongs to the current snapshot, with its statistics. `$files` is the one that doesn't need bucket access on the user side: we just query Trino, which finds the metadata thanks to Polaris and reads the manifests with its own credentials. With `aws s3 ls`, we need our own S3 keys.

## Query plan and execution

The distributed plan has 3 fragments:
- Fragment 2 (`SOURCE`): `TableScan` of `silver.users` (50 rows), output as `BROADCAST`.
- Fragment 1 (`SOURCE`): `ScanFilter` of `silver.orders` (2829 rows), `InnerJoin` with `distribution = REPLICATED` (the small `users` table is copied to each task, no shuffle of `orders`), then a `PARTIAL` aggregation by `state`, output partitioned with `HASH [state]`.
- Fragment 0: `FINAL` aggregation by `state` and `SINGLE` output to the client.

The `RemoteSource` nodes are the exchanges between fragments (over the network between tasks). Trino also adds a dynamic filter on `user_id`: the 50 ids from `users` are used to filter the scan of `orders`.

With `EXPLAIN ANALYZE`: execution in about 2.9 s, 2829 rows read for `orders`, 50 for `users`, 37 rows in the output (37 states), peak memory of 227 kB for the final aggregation. The join takes the biggest part of the CPU (about 41%). `system.runtime.queries` shows the history of our queries with their duration.

## Gold layer

The `lakehouse.gold` schema was created, then the 3 tables with `CREATE TABLE AS SELECT`: `orders_by_state_month` (50 rows), `top_products` (6 rows, croissant first with 1523 units) and `user_activity` (50 rows). `orders_by_state_month` only has 50 rows because each user places all their orders within a few days, so in a single month.

### Why does `user_activity` use a `LEFT JOIN` while `orders_by_state_month` uses an inner join? Which one depends on the result of the orphan query?
`user_activity` starts from the users: the `LEFT JOIN` also keeps the ones who never ordered (with 0 orders). `orders_by_state_month` starts from the orders and needs the user's state, so we only keep orders that have a user. This inner join is the one that depends on the orphan query: if there were orders without a user, they would disappear without any error. Here there are 0, so nothing is lost.

### `current_date` makes `age` change each day. What does it imply for the refresh of the table?
The table is frozen on the day it was created: the next day, some ages are already wrong. It has to be refreshed at least once a day, or we shouldn't store the age and compute it at query time (in a view or in the dashboard) from `birthdate`.

### Where do the data files of the gold tables live, and who decided their location?
In the bucket, under `s3://user-a-correia-ece/warehouse/gold/<table>-<id>/`. Polaris chose the location, from the catalog's `default-base-location` (`s3://<bucket>/warehouse/`) and the `gold` namespace. Trino then writes the files there.

## Validation

All the `assert` pass: 8615 units in `silver.orders`, the same in `orders_by_state_month` and in `top_products`, 50 users and 2829 orders in `user_activity`.

## Refresh a gold table

After the `DELETE` (6 rows) and the `INSERT` (6 rows), `top_products` has 3 snapshots. Time travel on the first snapshot (`FOR VERSION AS OF`) gives 6 products, like the current version.

### How many snapshots does the table have after the refresh, and what is the `operation` of each?
3 snapshots: `append` (the `CREATE TABLE AS SELECT`), `delete` (the `DELETE`) and `append` (the `INSERT`).

### Between the `DELETE` and the `INSERT`, a dashboard reads the table. What does it see? What would change with a `MERGE`?
It sees an empty table, because the `DELETE` is a commit on its own: for a few seconds the current snapshot has no rows. With a `MERGE`, the update is done in a single statement and a single atomic commit: the dashboard sees either the old version or the new one, never an empty table.

## Compare with Spark

### What does each engine need to run the `top_products` query, beyond the query itself?
Trino: just the Python client and the cluster address (host, port, user). The cluster is already running, and it holds the Polaris and S3 credentials. Spark: a SparkSession to start, the `iceberg-spark-runtime` package, the Polaris catalog config with the principal's credential, the S3 keys in the S3A config, and running the driver (a local job or one submitted to a cluster).

### Trino or Spark: a dashboard refreshed every minute, a nightly transformation of 10 TB, a data scientist exploring the silver layer?
- Dashboard refreshed every minute on the gold layer: Trino, fast answers and no session to start each time.
- Nightly transformation of 10 TB of bronze files: Spark, made for big batches, with task retries when something fails.
- Data scientist exploring with a query: Trino for quick SQL. Spark if they then need to work in Python or do ML on the data.

### Trino fails the whole query when a worker is lost, unless fault-tolerant execution is enabled. Why is it acceptable here, and not for 10 TB?
The queries of the lab take a few seconds: if they fail, we run them again and lose almost nothing. A 10 TB transformation can take hours, so losing a worker near the end would mean starting everything again. In that case we need an engine that only reruns the lost tasks, like Spark, or we need to enable Trino's fault-tolerant execution.
