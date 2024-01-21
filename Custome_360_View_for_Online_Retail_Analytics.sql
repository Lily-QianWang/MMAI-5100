WITH conversions_with_customer_id AS(
SELECT cd.customer_id,
       cd.first_name,
       cd.last_name,
       c.conversion_id,
       c.conversion_date,
       ROW_NUMBER() OVER (PARTITION BY cd.customer_id ORDER BY c.conversion_date) AS recurrence, --- conversion_number
       c.conversion_type,
       c.conversion_channel,
       LEAD(c.conversion_date) OVER (PARTITION BY cd.customer_id ORDER BY c.conversion_date) AS next_conversion_date,
       dd.year_week AS conversion_week,
       LEAD(dd.year_week) OVER (PARTITION BY cd.customer_id ORDER BY dd.year_week) AS next_conversion_week,
       c.order_number,
       cd.sk_customer
FROM fact_tables.conversions AS c
INNER JOIN dimensions.customer_dimension AS cd ON c.fk_customer = cd.sk_customer
INNER JOIN dimensions.date_dimension AS dd ON c.fk_conversion_date = dd.sk_date),

    orders_with_customer_id AS(
SELECT cd.customer_id,
       o.order_number,
       o.order_date,
       dd.year_week AS order_week,
       pd.product_name AS order_product,
       o.unit_price AS order_unit_price,
       o.discount AS order_discount,
       o.price_paid AS order_price_paid,
       o.fk_customer
FROM fact_tables.orders AS o
INNER JOIN dimensions.customer_dimension AS cd ON o.fk_customer = cd.sk_customer
INNER JOIN dimensions.product_dimension AS pd ON o.fk_product = pd.sk_product
INNER JOIN dimensions.date_dimension AS dd ON o.fk_order_date = dd.sk_date),

   conversions_with_first_orders AS(
SELECT cwci.*,
       owci.order_number AS first_order_number,
       owci.order_date AS first_order_date,
       owci.order_week AS first_order_week,
       owci.order_product AS first_order_product,
       owci.order_unit_price AS first_order_unit_price,
       owci.order_discount AS first_order_discount,
       owci.order_price_paid AS first_order_price_paid
FROM conversions_with_customer_id AS cwci
LEFT JOIN orders_with_customer_id AS owci ON cwci.order_number = owci.order_number),

     orders_with_order_week AS(
SELECT cd.customer_id,
       dd.year_week AS order_week,
       CASE WHEN dd.year_week IS NULL THEN 0
           ELSE 1
              END AS had_delivery,
       SUM(o.unit_price) AS grand_total,
       SUM(o.discount) AS total_discount,
       SUM(o.price_paid) AS total_paid
FROM fact_tables.orders AS o
INNER JOIN dimensions.customer_dimension AS cd ON o.fk_customer = cd.sk_customer
INNER JOIN dimensions.date_dimension AS dd ON o.fk_order_date = dd.sk_date
GROUP BY cd.customer_id, dd.year_week),

    year_week_table AS(
SELECT DISTINCT year_week
FROM dimensions.date_dimension AS dd
WHERE dd.date <= CURRENT_DATE
ORDER BY year_week),

    conversions_with_first_orders_year_week AS(
SELECT cwfo.*,
       ywt.year_week AS delivery_week
FROM conversions_with_first_orders AS cwfo
INNER JOIN year_week_table AS ywt ON cwfo.conversion_week <= ywt.year_week
AND ywt.year_week < cwfo.next_conversion_week
OR (cwfo.conversion_week <= ywt.year_week AND cwfo.next_conversion_week IS NULL)),

    orders_conversions_with_first_orders_year_week AS(
SELECT cwfoyw.customer_id,
       cwfoyw.first_name,
       cwfoyw.last_name,
       cwfoyw.conversion_id,
       cwfoyw.recurrence, ---(conversion_number)
       cwfoyw.conversion_type, ---(activation or reactivation)
       cwfoyw.conversion_date,
       cwfoyw.conversion_week,
       cwfoyw.conversion_channel,
       cwfoyw.next_conversion_week,
       cwfoyw.first_order_number,
       cwfoyw.first_order_date,
       cwfoyw.first_order_week,
       cwfoyw.first_order_product,
       cwfoyw.first_order_unit_price,
       cwfoyw.first_order_discount,
       cwfoyw.first_order_price_paid,
       ROW_NUMBER() OVER (PARTITION BY cwfoyw.customer_id, cwfoyw.conversion_date ORDER BY cwfoyw.delivery_week) AS week_counter,
       cwfoyw.delivery_week AS order_week,
       owow.grand_total,
       owow.total_discount,
       CASE WHEN owow.total_paid IS NULL THEN 0
           ELSE owow.total_paid
               END AS total_paid,
       SUM(owow.total_paid) OVER (PARTITION BY cwfoyw.customer_id, cwfoyw.conversion_date ORDER BY cwfoyw.delivery_week) AS cum_revenue,
       SUM(owow.total_paid) OVER (PARTITION BY cwfoyw.customer_id ORDER BY cwfoyw.delivery_week) AS cum_revenue_lifetime,
       SUM(owow.had_delivery) OVER (PARTITION BY cwfoyw.customer_id, cwfoyw.conversion_date ORDER BY cwfoyw.delivery_week) AS loyalty, ---number of orders
       SUM(owow.had_delivery) OVER (PARTITION BY cwfoyw.customer_id ORDER BY cwfoyw.delivery_week) AS loyalty_lifetime
FROM conversions_with_first_orders_year_week AS cwfoyw
LEFT JOIN orders_with_order_week AS owow ON cwfoyw.customer_id = owow.customer_id
AND cwfoyw.delivery_week = owow.order_week
AND((conversion_week <= delivery_week AND delivery_week  < next_conversion_week)
OR (conversion_week <= delivery_week  AND next_conversion_week IS NULL))
ORDER BY cwfoyw.customer_id, cwfoyw.conversion_id, cwfoyw.delivery_week)

SELECT ocwfoyw.*
FROM orders_conversions_with_first_orders_year_week AS ocwfoyw
ORDER BY customer_id, conversion_date, order_week;
