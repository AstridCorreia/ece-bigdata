import os
import pandas as pd
import trino

CATALOG = "lakehouse"

conn = trino.dbapi.connect(
    host=os.environ["TRINO_HOST"],
    port=int(os.environ["TRINO_PORT"]),
    user=os.environ["TRINO_USER"],
    http_scheme="http",
    catalog=CATALOG,
)

def sql(statement):
    cur = conn.cursor()
    cur.execute(statement)
    rows = cur.fetchall()
    columns = [c[0] for c in cur.description] if cur.description else []
    return pd.DataFrame(rows, columns=columns)
