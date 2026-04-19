# Equity mutual fund performance benchmarking (India)

Pipeline that loads Indian equity mutual fund NAVs from **MFAPI**, benchmark indices from **Yahoo Finance**, stores them in **MySQL**, computes risk/return metrics, and writes **charts plus a CSV summary** under `outputs/`.

## Repository layout (what stays in Git)

```
mf_benchmarking/
├── config.example.py       # Copy to config.py and set your MySQL password
├── requirements.txt
├── README.md
├── .gitignore
├── data/
│   └── extra_fund_info.csv # Per-fund benchmark + expense ratio + AUM
├── sql/
│   └── create_tables.sql   # Schema (drops/recreates tables on script 1)
├── src/
│   ├── 01_fetch_and_load_data.py
│   ├── 02_metrics_calculation.py
│   └── 03_visualization.py
└── outputs/
    └── .gitkeep            # Folder for generated PNGs + metrics_summary.csv
```

Removed from the repo for a clean GitHub project: ad-hoc connection helpers, debug logs, and committed secrets. **`config.py` is gitignored**; only `config.example.py` is tracked.

## Prerequisites

- Python **3.11+**
- **MySQL** (user able to create DB/tables, or DB pre-created)
- Internet access for MFAPI and Yahoo Finance

## First-time setup

1. Clone and open the project root (`mf_benchmarking/`).

2. Create a virtual environment and install dependencies:

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

3. Create local config (not pushed to GitHub):

```bash
copy config.example.py config.py
```

Edit **`config.py`**: set `DB_PASSWORD` and adjust `DB_USER` / `DB_HOST` / `DB_NAME` if needed.

4. Ensure **`data/extra_fund_info.csv`** includes one row per scheme code in `FUND_SCHEME_CODES` inside `01_fetch_and_load_data.py`, with columns `fund_id`, `benchmark`, `expense_ratio`, `aum_cr`. The `benchmark` value must match index names used in code: `Nifty 50` or `Nifty Midcap 150`.

## Run (from project root)

```bash
python src/01_fetch_and_load_data.py
python src/02_metrics_calculation.py
python src/03_visualization.py
```

- **Script 1:** Connects to MySQL, creates DB if missing, runs `sql/create_tables.sql`, deletes prior rows for the configured funds and date window, fetches MF + benchmark data, inserts into `fund_metadata`, `fund_nav`, `benchmark_data`.
- **Script 2:** Reads NAV + benchmarks + metadata, aligns dates, computes metrics, writes `fund_metrics`.
- **Script 3:** Merges metadata + metrics → **`outputs/metrics_summary.csv`**, saves PNG charts (`performance_chart.png`, `risk_return_scatter.png`, `metrics_bar_chart.png`, `max_drawdown_chart.png`).

Analysis window is **`2019-01-01` to before `2024-01-01`** (`START_DATE` / `END_DATE` in each script).

## Metrics (script 2)

| Metric | Meaning |
|--------|--------|
| Annualized return | Geometric annualization from daily returns (252-day year). |
| Benchmark return | Same for the chosen index series. |
| Beta | Covariance(fund, benchmark) / variance(benchmark). |
| Alpha | CAPM-style vs benchmark; **6.5% annual** risk-free rate in code. |
| Sharpe | Excess daily return vs risk-free, annualized with √252. |
| Sortino | Mean daily return / downside stdev of negative days, √252-scaled. |
| Max drawdown | Worst peak-to-trough on cumulative daily return curve. |

**Interpretation:** Alpha and beta are only economically meaningful if **`extra_fund_info.csv` assigns the right benchmark** to each fund (e.g. Nifty index fund → Nifty 50).

## Publishing to GitHub

1. If `config.py` was ever committed with a real password, change that password in MySQL and avoid recommitting it.

2. Initialize git **inside this folder** (if you have not already):

```bash
cd path\to\mf_benchmarking
git init
git add .
git status
```

Confirm **`config.py` does not appear** in `git status` (it should be ignored).

3. If `config.py` was tracked before adding `.gitignore`:

```bash
git rm --cached config.py
```

4. Commit and push:

```bash
git commit -m "Initial commit: MF benchmarking pipeline"
git branch -M main
git remote add origin https://github.com/YOUR_USER/YOUR_REPO.git
git push -u origin main
```

## Notes

- `create_tables.sql` **drops** the four application tables before recreate — fine for this analytics workflow, not for production data you must keep.
- MFAPI scheme metadata (names, categories) reflects the API at fetch time; scheme codes in `FUND_SCHEME_CODES` are the stable identifiers you configure.
