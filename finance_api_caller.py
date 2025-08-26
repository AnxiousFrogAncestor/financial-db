import oracledb
from dotenv import load_dotenv
import os

load_dotenv()

username = os.environ.get("DB_USER")
password = os.environ.get("DB_PASSWORD")
host = os.environ.get("host")
port = os.environ.get("port")
service_name = os.environ.get("service_name")
dsn = f"{host}:{port}/{service_name}"
DB_SCHEMA = os.environ.get("DB_SCHEMA")

symbols = ["AAPL", "MSFT", "AMZN", "TSLA", "NVDA", "JNJ", "JPM", "DIS", "KO", "XOM"]
# https://ranaroussi.github.io/yfinance/reference/yfinance.price_history.html
from rich import print
import yfinance as yf
import pandas as pd
from pandas import IndexSlice as idx


def melt_multiindex_prices(df):
    """
    Convert a multi-indexed price DataFrame (Metric x Ticker) into long format.

    Args:
        df (pd.DataFrame) :
            Multi-indexed DataFrame with columns (Metric, Ticker) and Datetime index.

    Returns:
        pd.DataFrame
            Long-format DataFrame with columns:
            ['Datetime', 'Ticker', 'Close', 'Dividends', 'High', 'Low', 'Open', 'Stock Splits', 'Volume']
    """
    if not isinstance(df.columns, pd.MultiIndex):
        raise ValueError(
            "Expected a multi-indexed DataFrame with (Metric, Ticker) columns."
        )

    # swap levels so Ticker is the outer level
    df_swapped = df.copy()
    df_swapped.columns = df_swapped.columns.swaplevel(0, 1)
    df_swapped = df_swapped.sort_index(axis=1, level=0)

    df_long = df_swapped.stack(level=0).reset_index()
    df_long = df_long.rename(
        columns={"level_1": "Ticker", df.index.name or "index": "Datetime"}
    )

    return df_long


def get_price_data(symbol_ls):
    # plural of Tickers for multiple symbols!
    dat = yf.Tickers(symbol_ls)
    price_history_df = dat.history(period="1d", interval="5m")
    # print(price_history_df.columns.levels)
    # print(price_history_df.loc[:, idx[:, "JPM"]])
    # print(price_history_df)
    price_history_df_long = melt_multiindex_prices(price_history_df)
    # print(price_history_df_long)
    return price_history_df["Open"], price_history_df_long


def get_cash_flow_and_company_data(symbols_ls):
    company_info = []
    cash_flows = []

    for symbol in symbols_ls:
        t = yf.Ticker(symbol)
        info = t.info
        company_info.append(
            {
                "stock_symbol": symbol,
                "company_name": info.get("shortName"),
                "address1": info.get("address1"),
                "city": info.get("city"),
                "state": info.get("state"),
                "zip": info.get("zip"),
                "country": info.get("country"),
                "phone": info.get("phone"),
                "website": info.get("website"),
                "industry": info.get("industry"),
                "sector": info.get("sector"),
            }
        )
        cf = t.quarterly_cash_flow
        if cf is not None and not cf.empty:
            cf_t = cf.T.reset_index().rename(columns={"index": "fiscal_date"})
            cf_t["stock_symbol"] = symbol
            cash_flows.append(cf_t)
    company_info_df = pd.DataFrame(company_info)
    cash_flow_df = (
        pd.concat(cash_flows, ignore_index=True) if cash_flows else pd.DataFrame()
    )

    return company_info_df, cash_flow_df


def generate_company_info_upsert_sql(schema: str) -> str:
    """

    Generates a MERGE statement for the
    dim_company table.

    The business key (stock_symbol), and the primary key (COMPANY_SK)
    is handled automatically by the database.

    Args:
        schema (str): The database schema/owner.

    Returns:
        str: A hardcoded SQL MERGE statement.
    """

    sql = f"""
MERGE INTO {schema}.dim_company d
USING (
    SELECT
        :stock_symbol AS stock_symbol,
        :company_name AS company_name,
        :address1     AS address1,
        :city         AS city,
        :state        AS state,
        :zip          AS zip,
        :country      AS country,
        :phone        AS phone,
        :website      AS website,
        :industry     AS industry,
        :sector       AS sector
    FROM dual
) s
ON (
    d.stock_symbol = s.stock_symbol
)
WHEN MATCHED THEN
    UPDATE SET
        d.company_name = s.company_name,
        d.address1     = s.address1,
        d.city         = s.city,
        d.state        = s.state,
        d.zip          = s.zip,
        d.country      = s.country,
        d.phone        = s.phone,
        d.website      = s.website,
        d.industry     = s.industry,
        d.sector       = s.sector,
        d.inserted_at  = SYSTIMESTAMP
WHEN NOT MATCHED THEN
    INSERT (
        stock_symbol, company_name, address1, city, state, zip,
        country, phone, website, industry, sector
    )
    VALUES (
        s.stock_symbol, s.company_name, s.address1, s.city, s.state, s.zip,
        s.country, s.phone, s.website, s.industry, s.sector
    )
"""
    return sql


def generate_cashflow_upsert_sql(
    schema: str, table_name: str, company_lookup_table: str, columns: list
) -> str:
    """
    Generates a MERGE statement that respects the table's DEFAULT values.

    Args:
        schema (str): The database schema/owner.
        table_name (str): The target fact table (e.g., 'fact_cashflow_quarterly').
        company_lookup_table (str): The table to find stock_sk (e.g., 'dim_company').
        columns (list): Column names from the source DataFrame.

    Returns:
        str: A generated SQL MERGE statement.
    """
    # columns that are handled automatically by the database, they are also immutable
    DB_AUTO_COLUMNS = {"CASH_FLOW_SK", "INSERTED_AT"}

    # columns that are part of the business key, they are also immutable
    KEY_COLUMNS = {"STOCK_SK", "FISCAL_DATE", "STOCK_SYMBOL"}

    # columns that need to be provided from the DataFrame
    data_columns = [c.upper() for c in columns if c.upper() not in DB_AUTO_COLUMNS]

    # columns to UPDATE that are not business keys
    metric_columns = [c for c in data_columns if c not in KEY_COLUMNS]

    update_set_str = ",\n        ".join([f"d.{c} = s.{c}" for c in metric_columns])
    insert_cols_str = ",\n        ".join(data_columns)
    values_cols_str = ",\n        ".join([f"s.{c}" for c in data_columns])
    bind_vars_str = ",\n        ".join(
        [f":{c.lower()} AS {c.upper()}" for c in data_columns if c not in KEY_COLUMNS]
    )

    sql = f"""
MERGE INTO {schema}.{table_name} d
USING (
    SELECT
        (SELECT stock_sk FROM {schema}.{company_lookup_table} WHERE stock_symbol = :stock_symbol) AS stock_sk,
        :fiscal_date AS fiscal_date,
        :stock_symbol AS stock_symbol,
        {bind_vars_str}
    FROM dual
) s
ON (d.stock_sk = s.stock_sk AND d.fiscal_date = s.fiscal_date)
WHEN MATCHED THEN
    UPDATE SET
        {update_set_str}
WHEN NOT MATCHED THEN
    INSERT ({insert_cols_str})
    VALUES ({values_cols_str})
"""
    return sql


def generate_price_intraday_upsert_sql(schema: str) -> str:
    """
    Generates a specialized and robust MERGE statement for fact_price_intraday.
    This version uses a more stable USING clause to avoid optimizer issues.
    """
    sql = f"""
        MERGE INTO {schema}.fact_price_intraday d
        USING (
        SELECT
            c.stock_sk,
            CAST(:datetime AS TIMESTAMP(0)) AS price_time,
            :open   AS open_price,
            :high   AS high_price,
            :low    AS low_price,
            :close  AS close_price,
            :volume AS volume,
            :dividends AS dividends,
            :stock_splits AS stock_splits
        FROM {schema}.dim_company c
        WHERE UPPER(TRIM(c.stock_symbol)) = UPPER(TRIM(:ticker))
        ) s
        ON (
        d.stock_sk = s.stock_sk
        AND CAST(d.price_time AS TIMESTAMP(0)) = s.price_time
        )
        WHEN MATCHED THEN
        UPDATE SET
            d.close_price  = s.close_price,
            d.open_price   = s.open_price,
            d.high_price   = s.high_price,
            d.low_price    = s.low_price,
            d.volume       = s.volume,
            d.dividends    = s.dividends,
            d.stock_splits = s.stock_splits,
            d.inserted_at  = SYSTIMESTAMP
        WHEN NOT MATCHED THEN
        INSERT (
            stock_sk, price_time, close_price, open_price, high_price, low_price,
            volume, dividends, stock_splits, inserted_at
        )
        VALUES (
            s.stock_sk, s.price_time, s.close_price, s.open_price, s.high_price, s.low_price,
            s.volume, s.dividends, s.stock_splits, SYSTIMESTAMP
        )
        """
    return sql


def sanitize_row(row_dict, datetime_cols=("fiscal_date",)):
    """
    Convert NaNs to None, coerce datetime columns,
    and force string values to lowercase.

    Args:
        row_dict (dict): row as dict from pandas
        datetime_cols (list[str], optional): columns to coerce to datetime

    Returns:
        dict: sanitized row
    """
    sanitized = {}
    datetime_cols = set(datetime_cols or [])

    for k, v in row_dict.items():
        if pd.isna(v):
            sanitized[k] = None
        elif k in datetime_cols:
            if isinstance(v, pd.Timestamp):
                sanitized[k] = v.to_pydatetime()
            elif isinstance(v, str):
                sanitized[k] = pd.to_datetime(v).to_pydatetime()
            else:
                sanitized[k] = v
        elif isinstance(v, str):
            sanitized[k] = v.lower()
        else:
            sanitized[k] = v
    return sanitized


def normalize_columns(df, upper_case=True):
    if upper_case:
        df = df.rename(columns=lambda x: x.upper().replace(" ", "_"))
    else:
        df = df.rename(columns=lambda x: x.lower().replace(" ", "_"))
    return df


def upsert_row(row_dict, sql_command, user, password, dsn):
    """Upsert a single row into Oracle using the given SQL and credentials."""
    sanitized_dict = sanitize_row(row_dict)
    with oracledb.connect(user=user, password=password, dsn=dsn) as connection:
        with connection.cursor() as cursor:
            cursor.execute(sql_command, sanitized_dict)
        connection.commit()


def upsert_df_rows(df, sql_command, user, password, dsn):
    """
    Loop through DataFrame rows and upsert each row individually.

    Handles NaNs and datetime objects automatically.
    """
    for i, row in df.iterrows():
        row_dict = row.to_dict()
        print("bind:", row_dict)
        print("BIND KEYS:", list(row_dict.keys()))
        # make sure the naming conventions are consistent
        upsert_row(row_dict, sql_command, user, password, dsn)
        print("upserted", i)


def populate_tables(symbol_ls, user_name, pw, dsn, db_schema):
    price_history_open_df, price_history_df_long = get_price_data(symbol_ls)

    company_info_df, cash_flow_df = get_cash_flow_and_company_data(symbol_ls)

    # ensure no spaces in column names
    cash_flow_df = normalize_columns(cash_flow_df)
    price_history_df_long = normalize_columns(price_history_df_long, upper_case=False)
    # 1. upsert company info (parent)
    # 2. upsert cash_flow
    # 3. upsert price

    UPSERT_COMPANY_SQL = generate_company_info_upsert_sql(db_schema)
    # upsert_df_rows(company_info_df, UPSERT_COMPANY_SQL, user_name, pw, dsn)
    TABLE_NAME = "fact_cashflow_quarterly"
    COMPANY_TABLE = "dim_company"
    columns = list(cash_flow_df.columns)
    UPSERT_CASH_FLOW_SQL = generate_cashflow_upsert_sql(
        db_schema, TABLE_NAME, COMPANY_TABLE, columns
    )
    # upsert_df_rows(cash_flow_df, UPSERT_CASH_FLOW_SQL, user_name, pw, dsn)
    UPSERT_PRICE_SQL = generate_price_intraday_upsert_sql(db_schema)
    # print(UPSERT_PRICE_SQL)
    upsert_df_rows(price_history_df_long, UPSERT_PRICE_SQL, user_name, pw, dsn)
    # print(price_history_df_long.columns)
    # print(price_history_df_long)


# populate_tables(symbols, username, password, dsn, DB_SCHEMA)
