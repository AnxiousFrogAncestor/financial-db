-- retrieve for the last 3 hours, rank the companies in terms of highest (avg_open) and lowest open_price_p5
COLUMN stock_symbol  FORMAT A16
COLUMN company_name  FORMAT A30
COLUMN price_hour    FORMAT A16
COLUMN avg_open      FORMAT 999999.99
COLUMN open_price_p5 FORMAT 999999.99
COLUMN rank_highest_avg FORMAT 99
COLUMN rank_lowest_p5   FORMAT 99

WITH latest_available_hour AS (
    SELECT MAX(TO_DATE(price_hour, 'YYYY-MM-DD HH24:MI')) AS max_hour
    FROM mv_price_intraday_stats
),
last_3_hours_of_data AS (
    SELECT
        stock_symbol,
        company_name,
        TO_DATE(price_hour, 'YYYY-MM-DD HH24:MI') AS price_hour,
        avg_open,
        open_price_p5
    FROM
        mv_price_intraday_stats
    WHERE
        TO_DATE(price_hour, 'YYYY-MM-DD HH24:MI') >= (
            (SELECT max_hour FROM latest_available_hour) - INTERVAL '2' HOUR
        )
)
SELECT
    stock_symbol,
    company_name,
    TO_CHAR(price_hour, 'YYYY-MM-DD HH24:MI') AS price_hour,
    avg_open,
    open_price_p5,
    RANK() OVER (ORDER BY avg_open DESC) AS rank_highest_avg,
    RANK() OVER (ORDER BY open_price_p5 ASC) AS rank_lowest_p5
FROM
    last_3_hours_of_data
ORDER BY
    rank_highest_avg, rank_lowest_p5;