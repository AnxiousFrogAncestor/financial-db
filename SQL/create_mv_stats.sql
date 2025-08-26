COLUMN stock_symbol FORMAT A10
COLUMN company_name FORMAT A50
COLUMN price_hour FORMAT A16
COLUMN avg_open FORMAT 999999.99
COLUMN open_price_p5 FORMAT 999999.99
COLUMN open_price_p95 FORMAT 999999.99

CREATE MATERIALIZED VIEW mv_price_intraday_stats
BUILD IMMEDIATE
REFRESH COMPLETE ON DEMAND
AS
SELECT
    c.stock_symbol,
    c.company_name,
    TO_CHAR(TRUNC(f.price_time, 'HH24'), 'YYYY-MM-DD HH24:MI') AS price_hour,
    AVG(f.open_price) AS avg_open,
    PERCENTILE_CONT(0.05) WITHIN GROUP (ORDER BY f.open_price) AS open_price_p5,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY f.open_price) AS open_price_p95
FROM fact_price_intraday f
JOIN dim_company c
    ON f.stock_sk = c.stock_sk
GROUP BY
    c.stock_symbol,
    c.company_name,
    TRUNC(f.price_time, 'HH24');