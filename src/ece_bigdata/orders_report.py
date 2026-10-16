import argparse
import os

import duckdb


def orders_report(product=None):
    bucket = os.environ.get("LAB_BUCKET_NAME")

    if not bucket:
        raise RuntimeError(
            "La variable LAB_BUCKET_NAME n'est pas définie."
        )

    parquet_path = f"s3://{bucket}/analytics/orders_by_product/**/*.parquet"

    query = """
        SELECT
            date_trunc('month', date)::DATE AS month,
            product,
            COUNT(*) AS number_of_orders,
            SUM(quantity) AS total_quantity
        FROM read_parquet(?)
    """

    params = [parquet_path]

    if product:
        query += " WHERE product = ?"
        params.append(product)

    query += """
        GROUP BY month, product
        ORDER BY month, product
    """

    with duckdb.connect() as con:
        report = con.execute(query, params).df()

    print(report.to_string(index=False))
    return report


def main():
    parser = argparse.ArgumentParser(
        description="Rapport mensuel des commandes."
    )
    parser.add_argument(
        "-p",
        "--product",
        help="Filtrer les résultats sur un produit, par exemple cookie."
    )
    args = parser.parse_args()

    orders_report(args.product)


if __name__ == "__main__":
    main()
