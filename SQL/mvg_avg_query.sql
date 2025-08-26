COLUMN stock_symbol FORMAT A10
COLUMN company_name FORMAT A50
COLUMN price_time FORMAT A19
COLUMN open_price FORMAT 999999.99

SELECT
    c.stock_symbol,
    c.company_name,
    TO_CHAR(f.price_time, 'YYYY-MM-DD HH24:MI:SS') AS price_time,
    f.open_price,
    AVG(f.open_price) OVER (
        PARTITION BY f.stock_sk
        ORDER BY f.price_time
        -- 5 (min) is the frequency of the time-series
        ROWS BETWEEN 12 PRECEDING AND CURRENT ROW
    ) AS mvg_avg_1hr
FROM
    fact_price_intraday f
JOIN
    dim_company c
ON
    f.stock_sk = c.stock_sk
ORDER BY
    c.stock_symbol, f.price_time;