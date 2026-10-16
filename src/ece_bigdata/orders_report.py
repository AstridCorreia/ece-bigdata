import argparse
import os

import duckdb


def orders_report(product=None):
    bucket = os.environ.get("LAB_BUCKET_NAME")

    if not bucket:
        raise RuntimeError(
            "La variable LAB_BUCKET_NAME n'est pas définie."
        )

    csv_path = f"s3://{bucket}/bronze/orders.csv"

    query = """
        SELECT
            date_trunc('month', date)::DATE AS month,
            COUNT(*) AS number_of_orders,
            SUM(quantity) AS total_quantity
        FROM read_csv_auto(?, header = true)
    """

    params = [csv_path]

    if product:
        query += " WHERE product = ?"
        params.append(product)

    query += """
        GROUP BY month
        ORDER BY month
    """

    with duckdb.connect() as con:
        report = con.execute(query, params).fetchall()

    print("Rapport mensuel des commandes")
    print("==============================")

    for month, number_of_orders, total_quantity in report:
        print(
            f"{month:%Y-%m} : {number_of_orders} commandes, "
            f"{total_quantity} unités vendues"
        )

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
