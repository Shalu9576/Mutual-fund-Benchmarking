import sys
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from sqlalchemy import create_engine, text

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

import config


START_DATE = "2019-01-01"
END_DATE = "2024-01-01"

try:
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")
except Exception:
    pass


def build_mysql_url() -> str:
    return f"mysql+pymysql://{config.DB_USER}:{config.DB_PASSWORD}@{config.DB_HOST}/{config.DB_NAME}"


def get_engine():
    return create_engine(build_mysql_url(), pool_pre_ping=True, future=True)


def ensure_outputs_dir() -> Path:
    out_dir = Path("outputs")
    out_dir.mkdir(parents=True, exist_ok=True)
    return out_dir


def load_data(engine) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    try:
        meta = pd.read_sql(
            text("SELECT fund_id, fund_name, benchmark FROM fund_metadata"),
            engine,
        )
        nav = pd.read_sql(
            text(
                "SELECT fund_id, date, nav FROM fund_nav "
                "WHERE date >= :start_dt AND date < :end_dt"
            ),
            engine,
            params={"start_dt": START_DATE, "end_dt": END_DATE},
            parse_dates=["date"],
        )
        bench = pd.read_sql(
            text(
                "SELECT index_name, date, close_price FROM benchmark_data "
                "WHERE date >= :start_dt AND date < :end_dt"
            ),
            engine,
            params={"start_dt": START_DATE, "end_dt": END_DATE},
            parse_dates=["date"],
        )
        metrics = pd.read_sql(text("SELECT * FROM fund_metrics"), engine, parse_dates=["calculated_on"])
        print("✅ Loaded data for visualization")
        return meta, nav, bench, metrics
    except Exception as e:
        print(f"❌ Failed to load data for visualization: {e}")
        sys.exit(1)


def compute_growth_of_one(series: pd.Series) -> pd.Series:
    returns = series.pct_change()
    growth = (1.0 + returns).cumprod()
    return growth


def build_fund_growth_series(nav: pd.DataFrame, meta: pd.DataFrame) -> pd.DataFrame:
    nav = nav.copy()
    nav["date"] = pd.to_datetime(nav["date"])

    merged = nav.merge(meta[["fund_id", "fund_name"]], on="fund_id", how="left")
    merged = merged.dropna(subset=["fund_name"]).copy()

    frames: list[pd.DataFrame] = []
    for fund_name, g in merged.groupby("fund_name"):
        g = g.sort_values("date")
        growth = compute_growth_of_one(g["nav"])
        out = pd.DataFrame({"date": g["date"].values, "name": fund_name, "growth": growth.values})
        frames.append(out)
    if not frames:
        return pd.DataFrame(columns=["date", "name", "growth"])
    return pd.concat(frames, ignore_index=True).dropna(subset=["growth"])


def build_benchmark_growth_series(bench: pd.DataFrame) -> pd.DataFrame:
    bench = bench.copy()
    bench["date"] = pd.to_datetime(bench["date"])

    frames: list[pd.DataFrame] = []
    for index_name, g in bench.groupby("index_name"):
        g = g.sort_values("date")
        growth = compute_growth_of_one(g["close_price"])
        out = pd.DataFrame({"date": g["date"].values, "name": index_name, "growth": growth.values})
        frames.append(out)
    if not frames:
        return pd.DataFrame(columns=["date", "name", "growth"])
    return pd.concat(frames, ignore_index=True).dropna(subset=["growth"])


def chart_cumulative_returns(out_dir: Path, fund_growth: pd.DataFrame, bench_growth: pd.DataFrame) -> None:
    plt.figure(figsize=(14, 7))

    for name, g in fund_growth.groupby("name"):
        plt.plot(g["date"], g["growth"], linewidth=2, label=name)

    for name, g in bench_growth.groupby("name"):
        plt.plot(g["date"], g["growth"], linewidth=2, linestyle="--", label=f"{name} (Benchmark)")

    plt.title("Cumulative Returns: Funds vs Benchmark (2019-2024)")
    plt.xlabel("Date")
    plt.ylabel("Growth of ₹1 invested")
    plt.grid(True, alpha=0.3)
    plt.legend(loc="best", fontsize=9)
    plt.tight_layout()

    path = out_dir / "performance_chart.png"
    plt.savefig(path, dpi=200)
    plt.close()
    print(f"✅ Saved: {path}")


def chart_risk_vs_return(out_dir: Path, summary: pd.DataFrame) -> None:
    plt.figure(figsize=(12, 7))

    x = summary["annualized_return"]
    y = summary["sharpe_ratio"]
    plt.scatter(x, y, s=80)

    for _, r in summary.iterrows():
        plt.text(
            r["annualized_return"],
            r["sharpe_ratio"],
            str(r["fund_name"]),
            fontsize=9,
            ha="left",
            va="bottom",
        )

    plt.title("Risk-Adjusted Performance Comparison")
    plt.xlabel("Annualized Return")
    plt.ylabel("Sharpe Ratio")
    plt.grid(True, alpha=0.3)
    plt.tight_layout()

    path = out_dir / "risk_return_scatter.png"
    plt.savefig(path, dpi=200)
    plt.close()
    print(f"✅ Saved: {path}")


def chart_metrics_bar(out_dir: Path, summary: pd.DataFrame) -> None:
    plot_df = summary[
        ["fund_name", "alpha", "beta", "sharpe_ratio", "sortino_ratio"]
    ].copy()

    plot_long = plot_df.melt(
        id_vars=["fund_name"],
        value_vars=["alpha", "beta", "sharpe_ratio", "sortino_ratio"],
        var_name="metric",
        value_name="value",
    )

    plt.figure(figsize=(14, 7))
    sns.barplot(data=plot_long, x="fund_name", y="value", hue="metric")
    plt.title("Key Performance Metrics by Fund")
    plt.xlabel("Fund")
    plt.ylabel("Metric value")
    plt.xticks(rotation=20, ha="right")
    plt.grid(True, axis="y", alpha=0.3)
    plt.tight_layout()

    path = out_dir / "metrics_bar_chart.png"
    plt.savefig(path, dpi=200)
    plt.close()
    print(f"✅ Saved: {path}")


def chart_max_drawdown(out_dir: Path, summary: pd.DataFrame) -> None:
    plot_df = summary[["fund_name", "max_drawdown"]].copy()
    plot_df = plot_df.sort_values("max_drawdown")  # most negative first

    plt.figure(figsize=(12, 6))
    plt.barh(plot_df["fund_name"], plot_df["max_drawdown"], color="red")
    plt.title("Maximum Drawdown by Fund")
    plt.xlabel("Max Drawdown")
    plt.ylabel("Fund")
    plt.grid(True, axis="x", alpha=0.3)
    plt.tight_layout()

    path = out_dir / "max_drawdown_chart.png"
    plt.savefig(path, dpi=200)
    plt.close()
    print(f"✅ Saved: {path}")


def main() -> None:
    print("=== Script 3: Visualization ===")
    out_dir = ensure_outputs_dir()

    try:
        engine = get_engine()
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("✅ Connected to MySQL successfully!")
    except Exception as e:
        print(f"❌ MySQL connection failed: {e}")
        sys.exit(1)

    meta, nav, bench, metrics = load_data(engine)

    meta["fund_id"] = meta["fund_id"].astype(str)
    nav["fund_id"] = nav["fund_id"].astype(str)

    summary = meta.merge(
        metrics,
        on="fund_id",
        how="left",
        suffixes=("", "_m"),
    )

    needed_cols = [
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
    missing_cols = [c for c in needed_cols if c not in summary.columns]
    if missing_cols:
        print(f"❌ Missing expected columns for summary: {missing_cols}")
        sys.exit(1)

    summary_out = summary[needed_cols].copy()
    summary_csv_path = out_dir / "metrics_summary.csv"
    summary_out.to_csv(summary_csv_path, index=False)
    print(f"✅ Saved: {summary_csv_path}")

    fund_growth = build_fund_growth_series(nav, meta)
    bench_growth = build_benchmark_growth_series(bench)

    if fund_growth.empty or bench_growth.empty:
        print("❌ Insufficient time series data to plot cumulative returns.")
    else:
        chart_cumulative_returns(out_dir, fund_growth, bench_growth)

    summary_metrics_only = summary_out.dropna(
        subset=["annualized_return", "sharpe_ratio", "alpha", "beta", "sortino_ratio", "max_drawdown"]
    ).copy()

    if summary_metrics_only.empty:
        print("❌ Insufficient metrics data to plot charts.")
        sys.exit(1)

    chart_risk_vs_return(out_dir, summary_metrics_only)
    chart_metrics_bar(out_dir, summary_metrics_only)
    chart_max_drawdown(out_dir, summary_metrics_only)

    print("✅ Script 3 complete.")


if __name__ == "__main__":
    sns.set_theme(style="whitegrid")
    main()

