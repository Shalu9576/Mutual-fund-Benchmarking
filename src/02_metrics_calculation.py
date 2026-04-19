import sys
from datetime import date
from math import sqrt
from pathlib import Path

import numpy as np
import pandas as pd
from sqlalchemy import bindparam, create_engine, text

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

import config


START_DATE = "2019-01-01"
END_DATE = "2024-01-01"

TRADING_DAYS = 252
RISK_FREE_DAILY = 0.065 / TRADING_DAYS  # 6.5% annual risk-free rate (India)

try:
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")
except Exception:
    pass


def build_mysql_url() -> str:
    return f"mysql+pymysql://{config.DB_USER}:{config.DB_PASSWORD}@{config.DB_HOST}/{config.DB_NAME}"


def get_engine():
    return create_engine(build_mysql_url(), pool_pre_ping=True, future=True)


def load_tables(engine) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    try:
        fund_nav = pd.read_sql(
            text(
                "SELECT fund_id, date, nav "
                "FROM fund_nav "
                "WHERE date >= :start_dt AND date < :end_dt"
            ),
            engine,
            params={"start_dt": START_DATE, "end_dt": END_DATE},
            parse_dates=["date"],
        )
        bench = pd.read_sql(
            text(
                "SELECT index_name, date, close_price "
                "FROM benchmark_data "
                "WHERE date >= :start_dt AND date < :end_dt"
            ),
            engine,
            params={"start_dt": START_DATE, "end_dt": END_DATE},
            parse_dates=["date"],
        )
        meta = pd.read_sql(
            text(
                "SELECT fund_id, fund_name, benchmark "
                "FROM fund_metadata"
            ),
            engine,
        )
        print("✅ Loaded tables from MySQL")
        return fund_nav, bench, meta
    except Exception as e:
        print(f"❌ Failed to load tables from MySQL: {e}")
        sys.exit(1)


def annualized_return_from_daily(daily_returns: pd.Series) -> float:
    if daily_returns.empty:
        return float("nan")
    total = (1.0 + daily_returns).prod()
    n_years = len(daily_returns) / TRADING_DAYS
    if n_years <= 0:
        return float("nan")
    return float(total ** (1.0 / n_years) - 1.0)


def compute_beta(fund_returns: pd.Series, bench_returns: pd.Series) -> float:
    if fund_returns.empty or bench_returns.empty:
        return float("nan")
    cov_matrix = np.cov(fund_returns.values, bench_returns.values)
    var_bench = cov_matrix[1][1]
    if var_bench == 0 or np.isnan(var_bench):
        return float("nan")
    return float(cov_matrix[0][1] / var_bench)


def compute_alpha(
    fund_annualized: float,
    bench_annualized: float,
    beta: float,
) -> float:
    rf_annual = RISK_FREE_DAILY * TRADING_DAYS
    return float(fund_annualized - (rf_annual + beta * (bench_annualized - rf_annual)))


def compute_sharpe(daily_returns: pd.Series) -> float:
    excess = daily_returns - RISK_FREE_DAILY
    std = excess.std(ddof=0)
    if std == 0 or np.isnan(std):
        return float("nan")
    return float((excess.mean() / std) * sqrt(TRADING_DAYS))


def compute_sortino(daily_returns: pd.Series) -> float:
    downside = daily_returns[daily_returns < 0]
    downside_std = downside.std(ddof=0)
    if downside_std == 0 or np.isnan(downside_std):
        return float("nan")
    return float((daily_returns.mean() / downside_std) * sqrt(TRADING_DAYS))


def compute_max_drawdown(daily_returns: pd.Series) -> float:
    if daily_returns.empty:
        return float("nan")
    cumulative = (1.0 + daily_returns).cumprod()
    rolling_max = cumulative.cummax()
    drawdown = (cumulative - rolling_max) / rolling_max
    return float(drawdown.min())


def calculate_metrics_for_fund(
    fund_id: str,
    fund_nav: pd.DataFrame,
    bench: pd.DataFrame,
    benchmark_name: str,
) -> dict | None:
    fund_df = fund_nav[fund_nav["fund_id"] == fund_id].copy()
    if fund_df.empty:
        print(f"❌ No NAV rows for fund_id={fund_id}; skipping")
        return None

    bench_df = bench[bench["index_name"] == benchmark_name].copy()
    if bench_df.empty:
        print(f"❌ No benchmark rows for {benchmark_name}; skipping fund_id={fund_id}")
        return None

    fund_df = fund_df.sort_values("date")
    bench_df = bench_df.sort_values("date")

    merged = pd.merge(fund_df, bench_df, on="date", how="inner")
    if merged.empty:
        print(f"❌ No overlapping dates for fund_id={fund_id} vs {benchmark_name}; skipping")
        return None

    merged["fund_return"] = merged["nav"].pct_change()
    merged["bench_return"] = merged["close_price"].pct_change()
    merged = merged.dropna(subset=["fund_return", "bench_return"]).copy()
    if merged.empty:
        print(f"❌ Not enough data after returns calc for fund_id={fund_id}; skipping")
        return None

    fund_returns = merged["fund_return"]
    bench_returns = merged["bench_return"]

    fund_ann = annualized_return_from_daily(fund_returns)
    bench_ann = annualized_return_from_daily(bench_returns)
    beta = compute_beta(fund_returns, bench_returns)
    alpha = compute_alpha(fund_ann, bench_ann, beta) if not np.isnan(beta) else float("nan")
    sharpe = compute_sharpe(fund_returns)
    sortino = compute_sortino(fund_returns)
    max_dd = compute_max_drawdown(fund_returns)

    return {
        "fund_id": fund_id,
        "calculated_on": date.today(),
        "annualized_return": fund_ann,
        "benchmark_return": bench_ann,
        "alpha": alpha,
        "beta": beta,
        "sharpe_ratio": sharpe,
        "sortino_ratio": sortino,
        "max_drawdown": max_dd,
    }


def upsert_metrics(engine, metrics_df: pd.DataFrame) -> None:
    if metrics_df.empty:
        print("❌ No metrics to insert")
        return

    fund_ids = metrics_df["fund_id"].astype(str).tolist()
    try:
        with engine.begin() as conn:
            delete_stmt = text("DELETE FROM fund_metrics WHERE fund_id IN :fund_ids").bindparams(
                bindparam("fund_ids", expanding=True)
            )
            conn.execute(delete_stmt, {"fund_ids": fund_ids})
        print("✅ Cleared existing fund_metrics rows for these funds")
    except Exception as e:
        print(f"❌ Failed to clear existing fund_metrics rows: {e}")
        raise

    try:
        metrics_df.to_sql("fund_metrics", engine, if_exists="append", index=False, method="multi", chunksize=2000)
        print(f"✅ Inserted {len(metrics_df):,} rows into fund_metrics")
    except Exception as e:
        print(f"❌ Insert failed for fund_metrics: {e}")
        raise


def main() -> None:
    print("=== Script 2: Metrics Calculation ===")
    engine = None

    try:
        engine = get_engine()
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("✅ Connected to MySQL successfully!")
    except Exception as e:
        print(f"❌ MySQL connection failed: {e}")
        sys.exit(1)

    fund_nav, bench, meta = load_tables(engine)

    fund_nav["date"] = pd.to_datetime(fund_nav["date"]).dt.date
    bench["date"] = pd.to_datetime(bench["date"]).dt.date
    meta["fund_id"] = meta["fund_id"].astype(str)

    results: list[dict] = []

    for _, row in meta.iterrows():
        fund_id = str(row["fund_id"])
        benchmark_name = str(row["benchmark"])
        metrics = calculate_metrics_for_fund(
            fund_id=fund_id,
            fund_nav=fund_nav,
            bench=bench,
            benchmark_name=benchmark_name,
        )
        if metrics is None:
            continue
        results.append(metrics)
        print(f"✅ Metrics calculated: fund_id={fund_id}")

    metrics_df = pd.DataFrame(results)
    if metrics_df.empty:
        print("❌ No metrics calculated for any fund; exiting.")
        sys.exit(1)

    try:
        upsert_metrics(engine, metrics_df)
    except Exception:
        print("❌ Exiting due to DB write failure.")
        sys.exit(1)

    summary = meta.merge(metrics_df, on="fund_id", how="left")
    ordered_cols = [
        "fund_id",
        "fund_name",
        "benchmark",
        "annualized_return",
        "benchmark_return",
        "alpha",
        "beta",
        "sharpe_ratio",
        "sortino_ratio",
        "max_drawdown",
        "calculated_on",
    ]
    summary = summary[ordered_cols].copy()

    pd.set_option("display.width", 180)
    pd.set_option("display.max_columns", 50)
    print("\n=== Metrics Summary (side-by-side) ===")
    print(summary.to_string(index=False))
    print("\n✅ Script 2 complete.")


if __name__ == "__main__":
    main()

