# Lab 10 - Trino: gold layer as a star schema

Run in the same Python session as lab 9 (`labs/lab-9/trino_conn.py`), with the same Trino connection.

## Gold layer with star schema

Tables created with `CREATE TABLE AS SELECT` in `lakehouse.gold`:
- `dim_user`: 50 rows, key `user_key` generated with `row_number()`
- `dim_product`: 6 rows, key `product_key` generated with `row_number()`
- `dim_date`: 118 rows, one per day between the first and the last order, key `date_key` in `YYYYMMDD` format
- `fact_orders`: 2829 rows, like `silver.orders`

The query on the star (year, state, product) works, and rebuilding `orders_by_state_month` from `fact_orders`, `dim_user` and `dim_date` gives the same numbers as the gold table of lab 9.

## Validation

The 3 assertions pass: 2829 orders and 8615 units in `fact_orders` like in `silver.orders`, and 0 orders without a dimension row.

### Are these checks sufficient to guarantee that every order is correctly represented in `fact_orders`? Give an example of a data defect that could go undetected, and identify additional checks
No. They check the totals and that every key exists in its dimension, but not that the key points to the right row. For example, if two orders had their `user_key` swapped, or if `date_key` was computed in another time zone (an order at 11:30 pm put on the next day), the row count, the sum of quantities and the joins would all still be fine. We should add:
- a row by row comparison: join `fact_orders` to the dimensions then to `silver.orders` on `order_id`, and check that `user_id`, `product` and `CAST(ordered_at AS date)` are the same as in silver
- uniqueness of `order_id` in `fact_orders`
- uniqueness of `user_id` in `dim_user` and of `product` in `dim_product` (one row per business value)

### What is the grain of `fact_orders`? Of `dim_date`?
`fact_orders`: one row per order, the same grain as `silver.orders`. `dim_date`: one row per calendar day.

### Which assertion fails if `silver.users` contains the same `user_id` twice? Which one fails if an order has no product (`NULL`)?
With a duplicated `user_id`, `dim_user` has two rows for this customer with two different `user_key`. The `LEFT JOIN` then duplicates all their orders, so `silver_orders == fact_orders` fails (and the units one too). With a `NULL` product, the `LEFT JOIN` doesn't find a `product_key`, so the order is kept with a `NULL` key: the totals are fine, but the `unmatched == 0` assertion fails.

### The surrogate keys are generated with `row_number()`. What happens to `user_key` if `dim_user` is rebuilt after a new user is added? What does it imply for `fact_orders`?
The keys are given in the order of `user_id`. If the new user lands in the middle of this order, all the users after them get a different `user_key`. If `fact_orders` isn't rebuilt at the same time, the old orders point to the wrong users. So the fact table always has to be rebuilt with the dimensions, or we need stable keys (for example keep the existing keys and give `max(user_key) + 1` to new users, or a key computed from `user_id`).

### `dim_user` overwrites the attributes of a user. If a user moves to another state, which state do the past orders show? What would a dimension that keeps the history of the changes need?
They show the new state, because there is only one row per user, with its current values: the sales history by state changes afterwards. To keep the history, we need a type 2 slowly changing dimension: a new row (with a new `user_key`) for each change, with `valid_from`, `valid_to` and `is_current` columns. The fact table takes the key of the version that was valid at the order date.

### Compare `orders_by_state_month` and the star query which rebuilds it. Which questions does each one answer without a new table, and what does the star cost at query time?
`orders_by_state_month` only answers questions by state and by month. It's already aggregated (50 rows), so it's very fast to read, but for another question (by product, by weekday, by city...) we have to create a new table. The star answers any combination of the dimensions (year, quarter, weekend, product, city, birthdate...) without a new table. In exchange, each query has to read the whole fact table, do the joins with the dimensions and then the aggregation, which costs more at query time when the fact table gets big.

## Commit

The metadata of the gold tables is copied from the bucket into `lab_trino/gold/` (without the `data/` folders) and committed.
