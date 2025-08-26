COLUMN stock_symbol FORMAT A10
COLUMN company_name FORMAT A50
COLUMN price_hour FORMAT A16
COLUMN avg_open FORMAT 999999.99
COLUMN open_price_p5 FORMAT 999999.99
COLUMN open_price_p95 FORMAT 999999.99


WITH latest_hour AS (
    SELECT MAX(TO_DATE(price_hour, 'YYYY-MM-DD HH24:MI')) AS max_hour
    FROM mv_price_intraday_stats
),
deltas AS (
    SELECT
        stock_symbol,
        company_name,
        TO_DATE(price_hour, 'YYYY-MM-DD HH24:MI') AS price_hour,
        avg_open,
        open_price_p5,
        open_price_p95,
        -- latest avg_open per stock
        MAX(CASE WHEN TO_DATE(price_hour, 'YYYY-MM-DD HH24:MI') = (SELECT max_hour FROM latest_hour)
                 THEN avg_open END) 
                 OVER (PARTITION BY stock_symbol) AS latest_avg_open
    FROM mv_price_intraday_stats
)
SELECT
    stock_symbol,
    company_name,
    TO_CHAR(price_hour, 'YYYY-MM-DD HH24:MI') AS price_hour,
    avg_open,
    open_price_p5,
    open_price_p95,
    latest_avg_open,
    ROUND(((avg_open - latest_avg_open) / latest_avg_open) * 100, 2) AS hourly_return
FROM deltas
ORDER BY price_hour, hourly_return DESC;
