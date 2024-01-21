-- MMAI 5100 S - A1 [Basic SQL]

-- 1. Identify the items with the highest and lowest (non-zero) unit price?
SELECT bs.article,
       MAX(bs.unit_price) AS highest_unit_price,
       MIN(bs.unit_price) AS lowest_unit_price
FROM assignment01.bakery_sales AS bs
WHERE bs.unit_price != 0
GROUP BY bs.article
ORDER BY bs.article;


-- 2. Write a SQL query to report the second most sold item from the bakery table. If there is no second
-- most sold item, the query should report NULL.
WITH sales_rank AS (
    SELECT bs.article,
           SUM(bs.quantity) AS total_volume,
           RANK() OVER (ORDER BY SUM(bs.quantity) DESC) AS sales_volume_rank
    FROM assignment01.bakery_sales AS bs
    GROUP BY bs.article
)
SELECT article,
       CASE
           WHEN sales_volume_rank = 2 THEN total_volume
           ELSE NULL
       END AS total_volume
FROM sales_rank
WHERE sales_volume_rank = 2;


-- 3. Write a SQL query to report the top 3 most sold items for every month in 2022 including their monthly sales.
WITH monthly_rank AS (
    SELECT bs.article,
       DATE_PART('year', bs.sale_datetime) AS sale_year,
       DATE_PART('month', bs.sale_datetime) AS sale_month,
       SUM(bs.quantity) AS monthly_quantity,
       SUM(bs.quantity*bs.unit_price) AS monthly_sales,
       RANK() OVER (PARTITION BY DATE_PART('year', bs.sale_datetime),
                                DATE_PART('month', bs.sale_datetime)
                    ORDER BY SUM(bs.quantity) DESC) AS sales_rank
    FROM assignment01.bakery_sales AS bs
    GROUP BY bs.article, DATE_PART('year', bs.sale_datetime), DATE_PART('month', bs.sale_datetime)
)
SELECT *
FROM monthly_rank
WHERE sales_rank <= 3 AND sale_year = 2022;


-- 4. Write a SQL query to report all the tickets with 5 or more articles in August 2022 including the number of
-- articles in each ticket.
WITH article_rank AS (
    SELECT bs.ticket_number,
           DATE_PART('year', bs.sale_datetime) AS sale_year,
           DATE_PART('month', bs.sale_datetime) AS sale_month,
           COUNT(bs.article) AS number_of_article
    FROM assignment01.bakery_sales AS bs
    WHERE DATE_PART('year', bs.sale_datetime) = 2022 AND
          DATE_PART('month', bs.sale_datetime) = 8
    GROUP BY bs.ticket_number, DATE_PART('month', bs.sale_datetime), DATE_PART('year', bs.sale_datetime)
    ORDER BY number_of_article
)
SELECT *
FROM article_rank
WHERE number_of_article >= 5;


-- 5. Write a SQL query to calculate the average sales per day in August 2022?
SELECT DATE_PART('year', bs.sale_datetime) AS sale_year,
       DATE_PART('month', bs.sale_datetime) AS sale_month,
       DATE_PART('day', bs.sale_datetime) AS sale_day,
       AVG(bs.quantity*bs.unit_price) AS Average_sales_revenue
FROM assignment01.bakery_sales AS bs
WHERE DATE_PART('year', bs.sale_datetime) = 2022 AND
      DATE_PART('month', bs.sale_datetime) = 8
GROUP BY sale_year, sale_month, sale_day;


-- 6. Write a SQL query to identify the day of the week with more sales?
WITH week_rank AS (
    SELECT DATE_PART('year', bs.sale_datetime) AS sale_year,
           DATE_PART('month', bs.sale_datetime) AS sale_month,
           DATE_PART('week', bs.sale_datetime) AS sale_week,
           DATE_PART('dow', bs.sale_datetime) AS sale_day,
           SUM(bs.quantity*bs.unit_price) AS sales_revenue,
           RANK() OVER (PARTITION BY DATE_PART('year', bs.sale_datetime),
                                     DATE_PART('month', bs.sale_datetime),
                                     DATE_PART('week', bs.sale_datetime)
                        ORDER BY SUM(bs.quantity*bs.unit_price) DESC) AS sales_rank
    FROM assignment01.bakery_sales AS bs
    GROUP BY sale_year, sale_month, sale_week, sale_day
)
SELECT sale_year,
       sale_month,
       sale_week,
       sale_day,
       sales_revenue
FROM week_rank
WHERE sales_rank = 1
ORDER BY sale_year, sale_month, sale_week, sale_day;


-- 7. What time of the day is the traditional Baguette more popular?
WITH time_rank AS (
    SELECT bs.article,
           DATE_PART('hour', bs.sale_datetime) AS sale_hour,
           DATE_PART('minute', bs.sale_datetime) AS sale_minute,
           SUM(bs.quantity) AS total_sale,
           RANK() OVER(PARTITION BY bs.article
                       ORDER BY SUM(bs.quantity) DESC) AS sales_rank
    FROM assignment01.bakery_sales AS bs
    WHERE bs.article LIKE 'TRADITIONAL BAGUETTE'
    GROUP BY bs.article, DATE_PART('hour', bs.sale_datetime), DATE_PART('minute', bs.sale_datetime)
)
SELECT *
FROM time_rank
WHERE sales_rank <= 3;


-- 8. Write a SQL query to find the articles with the lowest sales in each month?
WITH monthly_sale AS (
    SELECT bs.article,
           DATE_PART('year', bs.sale_datetime) AS sale_year,
           DATE_PART('month', bs.sale_datetime) AS sale_month,
           SUM(bs.quantity*bs.unit_price) AS sales_revenue,
           RANK() OVER (PARTITION BY DATE_PART('year', bs.sale_datetime),
                                     DATE_PART('month', bs.sale_datetime)
                        ORDER BY SUM(bs.quantity*bs.unit_price) ASC) AS sales_rank
    FROM assignment01.bakery_sales AS bs
    GROUP BY bs.article, DATE_PART('year', bs.sale_datetime), DATE_PART('month', bs.sale_datetime)
)
SELECT article,
       sale_year,
       sale_month,
       sales_revenue
FROM monthly_sale
WHERE sales_rank = 1;


-- 9. Write a query to calculate the percentage of sales for each item between 2022-01-01 and 2022-01-31
SELECT bs.article,
       ROUND((SUM(bs.unit_price*bs.quantity)/(SELECT SUM(bs.unit_price*bs.quantity) AS total_revenue
                                              FROM assignment01.bakery_sales AS bs
                                              WHERE CAST(bs.sale_date AS text) LIKE '2022-01-%'))*100, 2
           ) AS percentage_of_sales_revenue
FROM assignment01.bakery_sales AS bs
WHERE bs.sale_date BETWEEN '2022-01-01' AND '2022-01-31'
GROUP BY bs.article;


-- 10. The order rate is computed by dividing the volume of a specific article divided by the total amount
-- of items ordered in a specific date. Calculate the order rate for the Banette for every month during 2022.
WITH total_sales AS (
    SELECT DATE_PART('year', bs.sale_datetime) AS sale_year,
           DATE_PART('month', bs.sale_datetime) AS sale_month,
           SUM(bs.quantity) AS total_volume
    FROM assignment01.bakery_sales AS bs
    WHERE DATE_PART('year', bs.sale_datetime) = 2022
    GROUP BY sale_year, sale_month
),
banette_sales AS (
    SELECT DATE_PART('year', bs.sale_datetime) AS sale_year,
           DATE_PART('month', bs.sale_datetime) AS sale_month,
           SUM(bs.quantity) AS banette_volume
    FROM assignment01.bakery_sales AS bs
    WHERE DATE_PART('year', bs.sale_datetime) = 2022 AND
          bs.article = 'BANETTE'
    GROUP BY sale_year, sale_month
)
SELECT ts.sale_year,
       ts.sale_month,
       banette_volume,
       total_volume,
       ROUND(bs2.banette_volume * 1.0 / ts.total_volume, 4) AS order_rate
FROM total_sales AS ts
JOIN banette_sales AS bs2 ON ts.sale_year = bs2.sale_year AND
                             ts.sale_month = bs2.sale_month
ORDER BY ts.sale_month;

