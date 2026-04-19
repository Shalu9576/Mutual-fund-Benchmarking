import sys
import time
from pathlib import Path

import pandas as pd
import requests
import yfinance as yf
from sqlalchemy import bindparam, create_engine, text
from sqlalchemy.exc import OperationalError

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

import config


START_DATE = "2019-01-01"
END_DATE = "2024-01-01"

# Scheme names from MFAPI can change; treat comments as historical hints only.
FUND_SCHEME_CODES = [
    "120503",
    "100033",
    "118989",
    "120716",
    "119598",
]

BENCHMARKS = [
    {"ticker": "^NSEI", "index_name": "Nifty 50"},
    # Yahoo Finance doesn't reliably expose this index as ^NSEMDCP150.
    # The widely available Yahoo symbol is NIFTYMIDCAP150.NS.
    {"ticker": "NIFTYMIDCAP150.NS", "index_name": "Nifty Midcap 150"},
]

try:
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")
except Exception:
    pass


def build_mysql_url(include_db: bool) -> str:
    base = f"mysql+pymysql://{config.DB_USER}:{config.DB_PASSWORD}@{config.DB_HOST}"
    return f"{base}/{config.DB_NAME}" if include_db else f"{base}/"


def get_engine(include_db: bool):
    return create_engine(build_mysql_url(include_db), pool_pre_ping=True, future=True)


def bootstrap_database_and_tables() -> None:
    sql_path = Path("sql") / "create_tables.sql"
    if not sql_path.exists():
        print(f"❌ Missing SQL file: {sql_path.resolve()}")
        sys.exit(1)

    ddl_sql = sql_path.read_text(encoding="utf-8")

    # Step 1: try connecting directly to the target DB.
    # Some MySQL users are granted access only to specific databases (not to 'mysql'),
    # so connecting to the target DB first is the most compatible approach.
    try:
        db_engine = get_engine(include_db=True)
        with db_engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print(f"✅ Connected to database: {config.DB_NAME}")
    except OperationalError as e:
        msg = str(e).lower()
        if "unknown database" in msg or "1049" in msg:
            # DB doesn't exist; attempt to create it (requires higher privileges).
            try:
                server_engine = get_engine(include_db=False)
                with server_engine.begin() as conn:
                    conn.execute(text(f"CREATE DATABASE IF NOT EXISTS {config.DB_NAME}"))
                print(f"✅ Database created/ensured: {config.DB_NAME}")
            except Exception as create_err:
                print(f"❌ Could not create database '{config.DB_NAME}': {create_err}")
                print("❌ Please create the database manually, then rerun Script 1.")
                sys.exit(1)
        else:
            print(f"❌ MySQL connection/bootstrap failed: {e}")
            sys.exit(1)
    except Exception as e:
        print(f"❌ MySQL connection/bootstrap failed: {e}")
        sys.exit(1)

    try:
        statements = [s.strip() for s in ddl_sql.split(";") if s.strip()]
        with db_engine.begin() as conn:
            for stmt in statements:
                conn.execute(text(stmt))
        print("✅ Tables ensured via sql/create_tables.sql")
    except Exception as e:
        print(f"❌ Table creation failed: {e}")
        sys.exit(1)


def fetch_mfapi_scheme(scheme_code: str) -> dict | None:
    url = f"https://api.mfapi.in/mf/{scheme_code}"
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        if not isinstance(data, dict) or "meta" not in data or "data" not in data:
            print(f"❌ MFAPI unexpected response for {scheme_code}")
            return None
        print(f"✅ MFAPI fetched: {scheme_code}")
        return data
    except Exception as e:
        print(f"❌ MFAPI fetch failed for {scheme_code}: {e}")
        return None


def parse_fund_metadata(mfapi_response: dict) -> dict:
    meta = mfapi_response.get("meta", {}) or {}
    return {
        "fund_id": str(meta.get("scheme_code", "")).strip(),
        "fund_name": str(meta.get("scheme_name", "")).strip(),
        "fund_house": str(meta.get("fund_house", "")).strip(),
        "category": str(meta.get("scheme_category", "")).strip(),
        "scheme_type": str(meta.get("scheme_type", "")).strip(),
    }


def parse_nav_history(mfapi_response: dict, fund_id: str) -> pd.DataFrame:
    rows = mfapi_response.get("data", []) or []
    df = pd.DataFrame(rows)
    if df.empty:
        return pd.DataFrame(columns=["fund_id", "date", "nav"])

    df = df.rename(columns={"date": "date_str"})
    df["date"] = pd.to_datetime(df["date_str"], format="%d-%m-%Y", errors="coerce")
    df["nav"] = pd.to_numeric(df.get("nav"), errors="coerce")

    df = df.dropna(subset=["date", "nav"]).copy()
    df["date"] = df["date"].dt.date
    df["fund_id"] = fund_id

    start_dt = pd.to_datetime(START_DATE).date()
    end_dt = pd.to_datetime(END_DATE).date()
    df = df[(df["date"] >= start_dt) & (df["date"] < end_dt)]

    df = df[["fund_id", "date", "nav"]].sort_values(["fund_id", "date"])
    return df


def load_extra_fund_info() -> pd.DataFrame:
    csv_path = Path("data") / "extra_fund_info.csv"
    if not csv_path.exists():
        print(f"❌ Missing CSV: {csv_path.resolve()}")
        sys.exit(1)

    df = pd.read_csv(csv_path, dtype={"fund_id": "string"})
    for col in ["benchmark", "expense_ratio", "aum_cr"]:
        if col not in df.columns:
            print(f"❌ CSV missing column: {col}")
            sys.exit(1)
    df["fund_id"] = df["fund_id"].astype(str)
    return df


def delete_existing_rows(engine) -> None:
    fund_ids = FUND_SCHEME_CODES
    start_dt = START_DATE
    end_dt = END_DATE

    try:
        with engine.begin() as conn:
            delete_fund_nav = text(
                "DELETE FROM fund_nav "
                "WHERE fund_id IN :fund_ids AND date >= :start_dt AND date < :end_dt"
            ).bindparams(bindparam("fund_ids", expanding=True))
            conn.execute(
                delete_fund_nav,
                {"fund_ids": fund_ids, "start_dt": start_dt, "end_dt": end_dt},
            )

            delete_fund_metrics = text("DELETE FROM fund_metrics WHERE fund_id IN :fund_ids").bindparams(
                bindparam("fund_ids", expanding=True)
            )
            conn.execute(
                delete_fund_metrics,
                {"fund_ids": fund_ids},
            )

            delete_fund_metadata = text("DELETE FROM fund_metadata WHERE fund_id IN :fund_ids").bindparams(
                bindparam("fund_ids", expanding=True)
            )
            conn.execute(
                delete_fund_metadata,
                {"fund_ids": fund_ids},
            )
            conn.execute(
                text(
                    "DELETE FROM benchmark_data "
                    "WHERE index_name IN ('Nifty 50','Nifty Midcap 150') "
                    "AND date >= :start_dt AND date < :end_dt"
                ),
                {"start_dt": start_dt, "end_dt": end_dt},
            )
        print("✅ Cleared existing rows for idempotent reload")
    except Exception as e:
        print(f"❌ Failed to delete existing rows: {e}")
        raise


def insert_dataframe(engine, df: pd.DataFrame, table_name: str) -> None:
    if df.empty:
        print(f"❌ No rows to insert into {table_name}")
        return
    try:
        df.to_sql(table_name, engine, if_exists="append", index=False, method="multi", chunksize=2000)
        print(f"✅ Inserted {len(df):,} rows into {table_name}")
    except Exception as e:
        print(f"❌ Insert failed for {table_name}: {e}")
        raise


def fetch_and_prepare_benchmarks() -> pd.DataFrame:
    def extract_close(download_df: pd.DataFrame) -> pd.Series:
        # yfinance may return either flat columns (Close) or MultiIndex columns (Close, TICKER)
        if "Close" in download_df.columns:
            close_obj = download_df["Close"]
            if isinstance(close_obj, pd.DataFrame):
                return close_obj.iloc[:, 0]
            return close_obj
        if isinstance(download_df.columns, pd.MultiIndex):
            close_cols = [c for c in download_df.columns if len(c) >= 1 and c[0] == "Close"]
            if close_cols:
                close_obj = download_df[close_cols[0]]
                if isinstance(close_obj, pd.DataFrame):
                    return close_obj.iloc[:, 0]
                return close_obj
        raise KeyError("Close column not found in yfinance download result")

    all_frames: list[pd.DataFrame] = []

    for b in BENCHMARKS:
        ticker = b["ticker"]
        index_name = b["index_name"]
        try:
            df = yf.download(ticker, start=START_DATE, end=END_DATE, progress=False, auto_adjust=False)
            if df is None or df.empty:
                print(f"❌ yfinance returned no data for {ticker}")
                continue

            close_series = extract_close(df)
            close = close_series.to_frame(name="close_price").copy()
            close = close.reset_index().rename(columns={"Date": "date"})
            close["index_name"] = index_name
            close["date"] = pd.to_datetime(close["date"]).dt.date
            close["close_price"] = pd.to_numeric(close["close_price"], errors="coerce")
            close = close.dropna(subset=["date", "close_price"])
            close = close[["index_name", "date", "close_price"]].sort_values(["index_name", "date"])

            all_frames.append(close)
            print(f"✅ yfinance fetched: {ticker} ({index_name}) rows={len(close):,}")
        except Exception as e:
            print(f"❌ yfinance download failed for {ticker}: {e}")

    if not all_frames:
        return pd.DataFrame(columns=["index_name", "date", "close_price"])
    return pd.concat(all_frames, ignore_index=True)


def print_table_counts(engine) -> None:
    queries = {
        "fund_metadata": "SELECT COUNT(*) AS cnt FROM fund_metadata",
        "fund_nav": "SELECT COUNT(*) AS cnt FROM fund_nav",
        "benchmark_data": "SELECT COUNT(*) AS cnt FROM benchmark_data",
        "fund_metrics": "SELECT COUNT(*) AS cnt FROM fund_metrics",
    }
    try:
        with engine.connect() as conn:
            for table, q in queries.items():
                cnt = conn.execute(text(q)).scalar_one()
                print(f"✅ {table} rows: {cnt:,}")
    except Exception as e:
        print(f"❌ Count verification failed: {e}")


def main() -> None:
    print("=== Script 1: Fetch and Load Data ===")
    print(f"Date range: {START_DATE} to {END_DATE}")

    bootstrap_database_and_tables()

    try:
        engine = get_engine(include_db=True)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("✅ Connected to MySQL successfully!")
    except Exception as e:
        print(f"❌ MySQL connection failed: {e}")
        sys.exit(1)

    try:
        delete_existing_rows(engine)
    except Exception:
        print("❌ Cannot proceed due to cleanup failure.")
        sys.exit(1)

    extra_df = load_extra_fund_info()

    meta_rows: list[dict] = []
    nav_frames: list[pd.DataFrame] = []

    for scheme_code in FUND_SCHEME_CODES:
        mf_data = fetch_mfapi_scheme(scheme_code)
        if mf_data is None:
            time.sleep(1)
            continue

        meta = parse_fund_metadata(mf_data)
        fund_id = meta.get("fund_id") or scheme_code
        meta["fund_id"] = fund_id
        meta_rows.append(meta)

        nav_df = parse_nav_history(mf_data, fund_id=fund_id)
        if not nav_df.empty:
            nav_frames.append(nav_df)

        time.sleep(1)

    meta_df = pd.DataFrame(meta_rows)
    if meta_df.empty:
        print("❌ No fund metadata fetched; exiting.")
        sys.exit(1)

    meta_df["fund_id"] = meta_df["fund_id"].astype(str)
    merged_meta = meta_df.merge(extra_df, on="fund_id", how="left")

    if merged_meta[["benchmark", "expense_ratio", "aum_cr"]].isna().any().any():
        missing = merged_meta[merged_meta["benchmark"].isna()][["fund_id", "fund_name"]]
        if not missing.empty:
            print("❌ Missing extra_fund_info.csv rows for these funds:")
            print(missing.to_string(index=False))
            print("❌ Exiting to avoid hardcoding metadata.")
            sys.exit(1)

    merged_meta = merged_meta[
        [
            "fund_id",
            "fund_name",
            "fund_house",
            "category",
            "scheme_type",
            "benchmark",
            "expense_ratio",
            "aum_cr",
        ]
    ].copy()

    try:
        insert_dataframe(engine, merged_meta, "fund_metadata")
    except Exception:
        print("❌ Exiting due to fund_metadata insert failure.")
        sys.exit(1)

    nav_all = (
        pd.concat(nav_frames, ignore_index=True)
        if nav_frames
        else pd.DataFrame(columns=["fund_id", "date", "nav"])
    )
    try:
        insert_dataframe(engine, nav_all, "fund_nav")
    except Exception:
        print("❌ Exiting due to fund_nav insert failure.")
        sys.exit(1)

    bench_df = fetch_and_prepare_benchmarks()
    try:
        insert_dataframe(engine, bench_df, "benchmark_data")
    except Exception:
        print("❌ Exiting due to benchmark_data insert failure.")
        sys.exit(1)

    print_table_counts(engine)
    print("✅ Script 1 complete.")


if __name__ == "__main__":
    main()

