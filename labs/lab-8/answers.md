# Lab 8 - Govern Iceberg tables with Polaris

Polaris was deployed with Helm in the namespace (deployment lab). The SparkSession code is in `labs/lab-8/session.py`, run in the PySpark shell with `exec(open(...).read())`. The principal is chosen with the `POLARIS_PRINCIPAL` environment variable (`dev` by default).

Note: the `warehouse/` folder had been deleted at the end of the Iceberg lab (cleanup). So before starting, I recreated `lab.silver.orders` and `lab.silver.users` from the Parquet files of the Spark lab (`silver/spark/*.parquet`) with `writeTo(...).createOrReplace()`.

## SparkSession with the Polaris catalog

`SHOW NAMESPACES IN lab` returns `silver`, and `SHOW NAMESPACES IN polaris` returns nothing at the beginning.

### Which component answered each of the two `SHOW NAMESPACES` commands? How can you tell?
For `lab`, it's Spark itself: the `hadoop` catalog just lists the folders under `s3a://<bucket>/warehouse/`, so it finds `silver`. For `polaris`, it's the Polaris server answering through the REST API (`http://polaris:8181/api/catalog`), from its PostgreSQL database. We can tell because the `silver` folder does exist in the bucket but Polaris returns an empty list: it doesn't look at the bucket, only at what was declared in its catalog.

### Where is the credential used: in the Spark session, in the bucket, or in a request to Polaris?
In a request to Polaris. Spark sends the principal's `clientId:clientSecret` to Polaris (OAuth endpoint `/api/catalog/v1/oauth/tokens`) to get a token, then puts this token in every REST call. The bucket doesn't know this credential: files are read and written with the Onyxia S3 keys set in the S3A config.

## Register the Silver table in Polaris

`version-hint.text` contained `1`, so the table was registered with `v1.metadata.json`. Result: 2829 orders, 1 data file, the same snapshot as in `lab`.

### What did `register_table` write, in Polaris and in the bucket?
In Polaris, an entry `silver.orders` in the catalog, pointing to `v1.metadata.json`. In the bucket, nothing: there were 5 objects under `warehouse/silver/orders/` before and still 5 after. Data and metadata are not copied, Polaris only keeps the pointer.

### Why is it dangerous to keep writing to this table through the `lab` catalog once it is registered in `polaris`?
Each catalog has its own pointer to the current version. A commit through `lab` creates a `v2.metadata.json` and updates `version-hint.text`, but Polaris still points to the old version: readers going through Polaris (like Trino later) don't see the new data. If we write on both sides, we end up with two different histories for the same table. Also, going through `lab` completely bypasses the Polaris permissions.

## Create tables as `dev`

`SHOW TABLES IN polaris.silver` shows `orders`, with a single `overwrite` snapshot (the one from `createOrReplace()`). The table `polaris.silver.users` was created from `lab.silver.users` (50 users).

### Where did Polaris store the files for `silver.users`, and why?
In `s3://<bucket>/warehouse/silver/users/`, because the catalog's `default-base-location` is `s3://<bucket>/warehouse/` and Polaris adds `<namespace>/<table>`. It's exactly the same folder as `lab.silver.users`, so the files of both tables are mixed: we can recognize the `lab` ones (`v1.metadata.json`, `version-hint.text`) and the Polaris ones (`00000-<uuid>.metadata.json`, then `00001-...` after the schema change). There are also 2 Parquet files in `data/`, one per table.

### Both tables are in your bucket. What is the difference between "registered" and "created through Polaris" for the data files?
For `orders` (registered), the files were written before, by Spark through the `hadoop` catalog, and Polaris only references them. For `users` (created), Polaris chose the location and manages the metadata from the start. In both cases, the Parquet files are written by Spark with the S3 keys, not by Polaris.

## Schema change as `dev`

`ALTER TABLE polaris.silver.users ADD COLUMN country STRING` works, and `DESCRIBE` shows the new column.

### Which metadata file records the change? Is it the same file that `register_table` pointed to?
A new file `00001-<uuid>.metadata.json` in `warehouse/silver/users/metadata/`. It's not the same: `register_table` pointed to `orders/metadata/v1.metadata.json`, and anyway Iceberg never modifies an existing metadata file, it writes a new one at each commit and Polaris moves its pointer to it.

### Which privilege does Polaris check for this statement?
The operation checked is `ADD_TABLE_SCHEMA` (it's the name we see in the analyst's error). It needs a write privilege on the table metadata (`TABLE_WRITE_PROPERTIES`, or the finer `TABLE_ADD_SCHEMA` privilege in recent versions). `dev` has it through `CATALOG_MANAGE_CONTENT` (via `catalog_contributor`), which includes `TABLE_FULL_METADATA` and `TABLE_WRITE_DATA`.

## Read as the analyst

As `analyst`, `SHOW TABLES` and the `SELECT product, count(*)` work. The `INSERT` fails with:
`ForbiddenException: Forbidden: Principal 'analyst' with activated PrincipalRoles '[data_analyst]' and activated grants via '[data_analyst, catalog_reader]' is not authorized for op ADD_TABLE_SNAPSHOT`
The `ALTER TABLE ... ADD COLUMN phone` fails the same way, with `op ADD_TABLE_SCHEMA`.

### Which privilege is missing for the `INSERT`? Which privileges does `catalog_reader` provide?
A write privilege on the table is missing: `TABLE_WRITE_DATA` (or the finer `TABLE_ADD_SNAPSHOT` privilege), needed for the `ADD_TABLE_SNAPSHOT` operation. `catalog_reader` only gives `TABLE_READ_DATA`, `TABLE_READ_PROPERTIES` and `CATALOG_READ_PROPERTIES`, so read access only.

### Check the snapshots of `silver.orders` as `dev`. Did the failed `INSERT` create one?
No, `silver.orders` still has a single snapshot and 2829 rows. Spark first writes the Parquet file, then asks Polaris to commit, and Polaris refuses. Spark then aborted the write (`IcebergBatchWrite ... aborted`) and deleted the file: there is still only one file in `warehouse/silver/orders/data/`.

Limit: Polaris only controls the catalog. The analyst's session still has the S3 keys, so it could write directly in the bucket or go through the `lab` catalog. Without credential vending, Polaris can't prevent that.

## Cleanup

I didn't drop `polaris.silver.users`: the Trino lab reads `lakehouse.silver.users` and `lakehouse.silver.orders` through Polaris, so both tables have to stay registered.
