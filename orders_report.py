import duckdb
from pathlib import Path

DB_PATH = "analytics.duckdb"
OUTPUT_PATH = Path("analytics/orders_report.csv")

OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

with duckdb.connect(DB_PATH, read_only=True) as con:
    report = con.execute("""
        SELECT
            product,
            COUNT(*) AS number_of_orders,
            SUM(quantity) AS total_quantity
        FROM orders
        GROUP BY product
        ORDER BY total_quantity DESC
    """).df()

report.to_csv(OUTPUT_PATH, index=False)
print(f"Rapport créé : {OUTPUT_PATH}")
print(report.to_string(index=False))
