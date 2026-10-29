import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

BUCKET = os.environ["KUBERNETES_NAMESPACE"]

spark = (
    SparkSession.builder
    .appName("lab-iceberg")
    .config("spark.hadoop.fs.s3a.endpoint", f"https://{os.environ['AWS_S3_ENDPOINT']}")
    .config("spark.hadoop.fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"])
    .config("spark.hadoop.fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"])
    .config("spark.hadoop.fs.s3a.session.token", os.environ.get("AWS_SESSION_TOKEN", ""))
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.sql.catalog.lab", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lab.type", "hadoop")
    .config("spark.sql.catalog.lab.warehouse", f"s3a://{BUCKET}/warehouse")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

print("=== Catalog")
spark.sql("SHOW NAMESPACES IN lab").show()

print("=== Query the table (created in the Spark silver lab)")
spark.sql("""
    SELECT count(*) AS orders, sum(quantity) AS quantity
    FROM lab.silver.orders
""").show()

print("=== Metadata: snapshots and files")
spark.sql("SELECT * FROM lab.silver.orders.snapshots").show(truncate=False)
spark.sql("""
    SELECT file_path, file_format, record_count, file_size_in_bytes
    FROM lab.silver.orders.files
""").show(truncate=False)

print("=== Second snapshot: append 3 new orders")
stg_orders = spark.table("lab.silver.orders")
stg_orders.show(5)
new_orders = stg_orders.limit(3).withColumn("order_id", F.expr("uuid()"))
new_orders.writeTo("lab.silver.orders").append()

spark.sql("""
    SELECT snapshot_id, operation, committed_at
    FROM lab.silver.orders.snapshots
    ORDER BY committed_at
""").show(truncate=False)
spark.sql("SELECT count(*) FROM lab.silver.orders").show()
spark.sql("SELECT file_path, record_count FROM lab.silver.orders.files").show(truncate=False)

print("=== Time travel")
snapshots = spark.sql("""
    SELECT snapshot_id
    FROM lab.silver.orders.snapshots
    ORDER BY committed_at
""").collect()
print(snapshots)
first_snapshot = snapshots[0]["snapshot_id"]
spark.sql(f"""
    SELECT count(*)
    FROM lab.silver.orders
    VERSION AS OF {first_snapshot}
""").show()
spark.sql("SELECT count(*) FROM lab.silver.orders").show()

print("=== Schema evolution")
spark.sql("ALTER TABLE lab.silver.orders ADD COLUMN source STRING")
spark.sql("DESCRIBE lab.silver.orders").show()
spark.sql("""
    INSERT INTO lab.silver.orders
    SELECT order_id, user_id, ordered_at, quantity, product, 'spark'
    FROM lab.silver.orders
    LIMIT 1
""")
spark.sql("SELECT source, count(*) AS rows FROM lab.silver.orders GROUP BY source").show()
spark.sql("""
    SELECT snapshot_id, operation, committed_at
    FROM lab.silver.orders.snapshots
    ORDER BY committed_at
""").show(truncate=False)
