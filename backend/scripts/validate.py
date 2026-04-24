"""
Phase 5 — Alpha Validation Harness (M2.3 + M2.5).

Takes the ``signals_awards`` table produced by Phase 4 and enriches each
resolved row with forward total-return, SPY-excess returns, AND
industry-excess returns (using ITA / XLK / SPY depending on the row's
Yahoo ``industry``) at T+1, T+5, T+20, T+60 trading days. Persists the
joined table to DuckDB and exports a Parquet snapshot for the
downstream analytics and report.

Pipeline:

    1. Load `signals_awards` rows with is_public=True AND market_cap NOT NULL.
    2. Compute the global fetch window:
         start = min(action_date) - 5 calendar days
         end   = max(action_date) + 95 calendar days  (covers T+60 trading days)
    3. Pre-fetch every benchmark (SPY, ITA, XLK) for the global range.
    4. For each distinct ticker, fetch bars if cache doesn't already cover
       the range. Bars are split/dividend adjusted (auto_adjust=True).
    5. Compute trading-day forward returns per row:
         return_Nd  = close_adj(t+N) / close_adj(t) - 1
       where t is the first trading day on-or-after action_date.
    6. Compute SPY-excess returns:
         excess_return_Nd = return_Nd - spy_return_Nd
    7. Compute industry-excess returns using the row's industry benchmark:
         industry_excess_return_Nd = return_Nd - industry_benchmark_return_Nd
    8. Stamp each row with its ``industry_benchmark_ticker`` (SPY / ITA / XLK)
       for traceability, and with a ``fiscal_quarter`` tag (FY24Q1..Q4).
    9. Persist to cleaned.duckdb::signals_with_returns; export Parquet.

Re-running is idempotent and cheap: all Yahoo calls are cached, so the
join + write phase re-runs in seconds once the cache is warm.

Usage:
    uv run --env-file .env backend/scripts/validate.py
    uv run --env-file .env backend/scripts/validate.py --debug
    uv run --env-file .env backend/scripts/validate.py --limit 100  # dev only
"""

from __future__ import annotations

import argparse
import logging
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

import duckdb
import numpy as np
import pandas as pd

from backend.app.services.providers.returns import (
    ALL_BENCHMARK_TICKERS,
    BENCHMARK_TICKER,
    ReturnsProvider,
    YFinanceReturnsProvider,
    industry_benchmark_ticker_for,
)
from backend.src.io import (
    ensure_benchmark_pre_fetched,
    ensure_cache_tables,
    get_cached_returns_df,
    get_cleaned_conn,
    upsert_cached_returns,
)

# Trading-day horizons to measure. Short end captures event-driven
# reactions; long end captures slower drift and mean reversion.
#
#   T+1   -- next-day reaction
#   T+5   -- weekly
#   T+20  -- monthly (common quant-research horizon)
#   T+60  -- quarterly
#   T+90  -- ~1.3 months beyond quarter
#   T+120 -- ~half-year
#   T+180 -- ~9 months (annualized-return scale)
#
# Rows land NaN for any horizon where the cache doesn't yet extend that
# far past action_date; the analytics layer skips them. Later-quarter
# rows have less long-horizon coverage than earlier-quarter rows, so
# T+120 / T+180 are computed on a shrinking sample as we approach the
# end of the cached window.
FORWARD_HORIZONS: List[int] = [1, 5, 20, 60, 90, 120, 180]

# Pad on each side of the action-date window. LOOKFORWARD_DAYS is the
# calendar-day pad used by the provider fetch; 180 trading days ~= 252
# calendar days, but we only need full coverage for the earliest rows
# in the dataset (FY2024 start). Later rows get the longer horizons
# only as market data accrues; see the FORWARD_HORIZONS docstring.
LOOKBACK_DAYS: int = 5
LOOKFORWARD_DAYS: int = 265  # ~ 180 trading days + weekends + holidays


def setup_logging(debug: bool = False) -> logging.Logger:
    log_dir = Path("backend/data/logs")
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / "validate.log"
    level = logging.DEBUG if debug else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.FileHandler(log_file), logging.StreamHandler()],
    )
    if debug:
        logging.getLogger("urllib3").setLevel(logging.INFO)
    return logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------


def compute_forward_returns(
    bars: pd.DataFrame, horizons: List[int] = FORWARD_HORIZONS
) -> pd.DataFrame:
    """
    Add ``return_Nd`` columns to a bars DataFrame for each horizon.

    Assumes ``bars`` is indexed by trading date with a ``close_adj``
    column (the shape emitted by ``YFinanceReturnsProvider``). The
    computed ``return_Nd`` on row ``t`` is ``close_adj[t+N] / close_adj[t] - 1``
    and will be NaN for the last ``N`` rows (no T+N price yet).
    """
    if bars is None or bars.empty:
        out_cols = ["close_adj", "return_1d"] + [f"return_{h}d" for h in horizons]
        return pd.DataFrame(columns=out_cols)

    out = bars.copy()
    close = out["close_adj"]
    for h in horizons:
        # shift(-h) brings the T+h close up to row t, so dividing yields
        # the forward return. pandas guarantees NaN where the shift runs
        # off the end of the frame.
        future = close.shift(-h)
        out[f"return_{h}d"] = future / close - 1.0
    return out


def pick_trading_day_row(
    bars: pd.DataFrame, action_date: pd.Timestamp
) -> Optional[pd.Series]:
    """
    Return the row of ``bars`` whose index is the first trading day
    on-or-after ``action_date``. ``None`` if no such row exists (action
    date is after the last cached bar).

    Semantic note: USASpending action_dates are recorded as calendar
    dates. When one lands on a Saturday, Sunday, or market holiday we
    use the next open trading day as the signal's reference bar. This
    is the standard event-study convention.
    """
    if bars is None or bars.empty:
        return None
    idx = bars.index
    # searchsorted with side='left' gives the first index >= action_date.
    pos = idx.searchsorted(action_date, side="left")
    if pos >= len(bars):
        return None
    row = bars.iloc[pos].copy()
    row["reference_trading_date"] = idx[pos]
    return row


# ---------------------------------------------------------------------------
# Core orchestration steps
# ---------------------------------------------------------------------------


def load_signals_for_validation(
    conn: duckdb.DuckDBPyConnection, limit: Optional[int] = None
) -> pd.DataFrame:
    """
    Load all signals_awards rows eligible for return validation.

    Filters to rows that (a) have a ticker and (b) have a market cap --
    without those, there's no signal to validate.
    """
    limit_clause = f"LIMIT {int(limit)}" if limit else ""
    return conn.execute(
        f"""
        SELECT *
        FROM signals_awards
        WHERE is_public = TRUE
          AND market_cap IS NOT NULL
        ORDER BY ticker, action_date
        {limit_clause}
        """
    ).df()


def fetch_ticker_bars(
    conn: duckdb.DuckDBPyConnection,
    provider: ReturnsProvider,
    ticker: str,
    start: date,
    end: date,
    logger: logging.Logger,
) -> pd.DataFrame:
    """
    Return a per-ticker DataFrame of bars over ``[start, end]``.

    Cache-first: if ``cache_returns`` already has bars that fully cover
    the requested range, no network call is made. Otherwise we fetch
    from the provider and upsert the result.
    """
    # Coverage check: does the cache have rows straddling [start, end]?
    # Query unbounded (no WHERE date BETWEEN ...) so we can compare the
    # TRUE min/max cached date for this ticker, not the min/max within
    # a window we just sliced.
    coverage = conn.execute(
        "SELECT MIN(date) AS min_d, MAX(date) AS max_d, COUNT(*) AS n "
        "FROM cache.cache_returns WHERE ticker = ?",
        [ticker],
    ).fetchone()
    cached_min, cached_max, cached_n = coverage
    if cached_n and cached_min is not None and cached_max is not None:
        if cached_min <= start and cached_max >= end:
            logger.debug(f"cache hit for {ticker} [{start}, {end}]")
            return get_cached_returns_df(conn, ticker, start, end)

    logger.info(f"fetching {ticker} bars [{start}, {end}]")
    try:
        bars = provider.fetch_daily_bars(ticker, start, end)
    except Exception as e:
        logger.warning(f"provider failed for {ticker}: {e}")
        # Return whatever we had cached (may be empty); the row-level
        # join will produce NaN forward returns for this ticker.
        return get_cached_returns_df(conn, ticker, start, end)

    upsert_cached_returns(conn, ticker, bars, source="yfinance", is_benchmark=False)
    # Re-read through the cache so the result covers both the freshly-
    # fetched bars AND any previously cached ones outside this call's
    # requested window (unlikely but harmless).
    return get_cached_returns_df(conn, ticker, start, end)


def _fiscal_quarter_tag(action_date: pd.Timestamp) -> str:
    """
    Map a calendar date to the US federal fiscal-year quarter it falls in.

    US federal FY runs Oct-Sep. FY2024 = Oct 2023 through Sep 2024:
        FY24Q1 = Oct-Dec 2023
        FY24Q2 = Jan-Mar 2024
        FY24Q3 = Apr-Jun 2024
        FY24Q4 = Jul-Sep 2024

    Returns ``"UNKNOWN"`` for NaN dates. The stamping happens in
    ``compute_excess_returns_for_signals`` so the downstream report's
    per-quarter stability filter can group on a single column.
    """
    if pd.isna(action_date):
        return "UNKNOWN"
    ts = pd.Timestamp(action_date)
    m = ts.month
    # Fiscal year: the calendar year in which the fiscal year ENDS.
    # Oct/Nov/Dec -> next calendar year's FY; Jan-Sep -> current year's FY.
    if m >= 10:
        fy = ts.year + 1
        q = 1  # Oct, Nov, Dec all land in Q1
    elif m <= 3:
        fy = ts.year
        q = 2  # Jan, Feb, Mar -> Q2
    elif m <= 6:
        fy = ts.year
        q = 3  # Apr, May, Jun -> Q3
    else:
        fy = ts.year
        q = 4  # Jul, Aug, Sep -> Q4
    # Compact short form: "FY24Q2" rather than "FY2024Q2".
    return f"FY{fy % 100:02d}Q{q}"


def compute_excess_returns_for_signals(
    signals: pd.DataFrame,
    ticker_bars: Dict[str, pd.DataFrame],
    benchmark_bars: Dict[str, pd.DataFrame],
    horizons: List[int] = FORWARD_HORIZONS,
    logger: Optional[logging.Logger] = None,
) -> pd.DataFrame:
    """
    Join forward returns onto every signals row using BOTH the canonical
    broad benchmark (SPY) and the appropriate industry benchmark.

    For each row we compute:

    * ``return_Nd`` -- forward total return on the ticker
    * ``spy_return_Nd`` -- forward total return on SPY (always broad)
    * ``excess_return_Nd`` -- ticker minus SPY  (= SPY-neutral)
    * ``industry_benchmark_return_Nd`` -- forward return on the row's
      industry-appropriate ETF (ITA / XLK / SPY as per
      :func:`industry_benchmark_ticker_for`)
    * ``industry_excess_return_Nd`` -- ticker minus industry benchmark

    Also stamps each row with:

    * ``reference_trading_date`` -- first trading day on-or-after
      ``action_date``
    * ``industry_benchmark_ticker`` -- which ETF was subtracted
    * ``fiscal_quarter`` -- FY24Q1 / FY24Q2 / FY24Q3 / FY24Q4 tag

    ``benchmark_bars`` is a dict keyed by ticker (``SPY``, ``ITA``,
    ``XLK``) containing the full-window bars for each benchmark. It
    MUST at minimum contain ``SPY`` for the SPY-excess columns to be
    computed. Missing industry benchmarks cause the per-row industry-
    excess columns to fall back to NaN (not an error).

    Output preserves all rows in ``signals`` even when returns are
    missing; missing-ticker rows get NaN across all return columns.
    """
    logger = logger or logging.getLogger(__name__)

    # Pre-compute forward returns for each unique ticker and each
    # benchmark once. Avoids O(rows * horizons) shifts in the hot loop.
    bars_with_fwd: Dict[str, pd.DataFrame] = {
        t: compute_forward_returns(b, horizons) for t, b in ticker_bars.items()
    }
    benchmark_with_fwd: Dict[str, pd.DataFrame] = {
        t: compute_forward_returns(b, horizons) for t, b in benchmark_bars.items()
    }
    spy_with_fwd = benchmark_with_fwd.get(BENCHMARK_TICKER)

    # Accumulate output columns in numpy arrays for speed.
    n = len(signals)
    ref_dates: List[Optional[pd.Timestamp]] = [None] * n
    industry_benchmarks: List[Optional[str]] = [None] * n
    fiscal_quarters: List[str] = ["UNKNOWN"] * n
    cols: Dict[str, np.ndarray] = {}
    for h in horizons:
        cols[f"return_{h}d"] = np.full(n, np.nan, dtype=float)
        cols[f"spy_return_{h}d"] = np.full(n, np.nan, dtype=float)
        cols[f"excess_return_{h}d"] = np.full(n, np.nan, dtype=float)
        cols[f"industry_benchmark_return_{h}d"] = np.full(n, np.nan, dtype=float)
        cols[f"industry_excess_return_{h}d"] = np.full(n, np.nan, dtype=float)

    missed_tickers: set[str] = set()
    # Pre-resolve the ``industry`` column (defensively handle absence).
    has_industry = "industry" in signals.columns
    for i, row in enumerate(signals.itertuples(index=False)):
        ticker = row.ticker
        action_dt = pd.Timestamp(row.action_date)
        if pd.isna(action_dt):
            continue
        fiscal_quarters[i] = _fiscal_quarter_tag(action_dt)

        tbars = bars_with_fwd.get(ticker)
        if tbars is None or tbars.empty:
            missed_tickers.add(ticker)
            continue

        t_row = pick_trading_day_row(tbars, action_dt)
        if t_row is None:
            continue
        ref_dates[i] = t_row["reference_trading_date"]

        # Pick the row's industry benchmark (SPY / ITA / XLK).
        ind_value = getattr(row, "industry", None) if has_industry else None
        if isinstance(ind_value, float) and np.isnan(ind_value):
            ind_value = None
        ind_bench_ticker = industry_benchmark_ticker_for(ind_value)
        industry_benchmarks[i] = ind_bench_ticker

        # SPY reference row (for SPY-neutral columns).
        spy_row = None
        if spy_with_fwd is not None:
            spy_row = pick_trading_day_row(
                spy_with_fwd, t_row["reference_trading_date"]
            )

        # Industry benchmark reference row (may be same as SPY if mapping
        # falls back).
        ind_bars = benchmark_with_fwd.get(ind_bench_ticker)
        ind_row = None
        if ind_bars is not None and not ind_bars.empty:
            ind_row = pick_trading_day_row(
                ind_bars, t_row["reference_trading_date"]
            )

        for h in horizons:
            tr = t_row.get(f"return_{h}d", np.nan)
            if pd.notna(tr):
                cols[f"return_{h}d"][i] = float(tr)
            # SPY-neutral columns
            if spy_row is not None:
                spy_r = spy_row.get(f"return_{h}d", np.nan)
                if pd.notna(spy_r):
                    cols[f"spy_return_{h}d"][i] = float(spy_r)
                    if pd.notna(tr):
                        cols[f"excess_return_{h}d"][i] = float(tr - spy_r)
            # Industry-neutral columns
            if ind_row is not None:
                ind_r = ind_row.get(f"return_{h}d", np.nan)
                if pd.notna(ind_r):
                    cols[f"industry_benchmark_return_{h}d"][i] = float(ind_r)
                    if pd.notna(tr):
                        cols[f"industry_excess_return_{h}d"][i] = float(tr - ind_r)

    if missed_tickers:
        logger.warning(
            f"No bars available for {len(missed_tickers)} tickers: "
            f"{sorted(missed_tickers)[:10]}{'...' if len(missed_tickers) > 10 else ''}"
        )

    out = signals.copy()
    out["reference_trading_date"] = ref_dates
    out["industry_benchmark_ticker"] = industry_benchmarks
    out["fiscal_quarter"] = fiscal_quarters
    for col, arr in cols.items():
        out[col] = arr
    return out


# ---------------------------------------------------------------------------
# Main orchestrator
# ---------------------------------------------------------------------------


def run_validation(
    provider: Optional[ReturnsProvider] = None,
    *,
    cleaned_db_path: str = "backend/data/cleaned/cleaned.duckdb",
    cache_db_path: str = "backend/data/cache/cache.duckdb",
    parquet_export_path: Optional[
        str
    ] = "backend/data/analysis/signals_with_returns.parquet",
    limit: Optional[int] = None,
    logger: Optional[logging.Logger] = None,
) -> int:
    """
    Top-level orchestrator. Returns the number of rows written to
    ``signals_with_returns``.

    Split out from ``main()`` so tests (and the integration harness)
    can call it with a mocked provider and in-process DB paths.
    """
    logger = logger or logging.getLogger(__name__)
    provider = provider or YFinanceReturnsProvider()

    conn = get_cleaned_conn(cleaned_db_path)
    conn.execute(f"ATTACH '{cache_db_path}' AS cache;")
    ensure_cache_tables(conn)

    try:
        # --- 1. Load eligible signals rows ---------------------------------
        signals = load_signals_for_validation(conn, limit=limit)
        logger.info(f"Loaded {len(signals)} signals rows for validation.")
        if signals.empty:
            logger.warning("No signals rows to validate; aborting.")
            return 0

        # --- 2. Compute global fetch window --------------------------------
        action_dates = pd.to_datetime(signals["action_date"]).dt.date
        fetch_start = action_dates.min() - timedelta(days=LOOKBACK_DAYS)
        fetch_end = action_dates.max() + timedelta(days=LOOKFORWARD_DAYS)
        logger.info(f"Global fetch window: {fetch_start} through {fetch_end}")

        # --- 3. Pre-fetch EVERY benchmark (SPY, ITA, XLK) -----------------
        # SPY is required (broad-market neutralization). ITA and XLK are
        # used for industry-neutralization; if a fetch fails the pipeline
        # still proceeds with just SPY-neutral columns populated.
        benchmark_bars: Dict[str, pd.DataFrame] = {}
        for bench_ticker in ALL_BENCHMARK_TICKERS:
            n_new = ensure_benchmark_pre_fetched(
                conn,
                provider,
                fetch_start,
                fetch_end,
                benchmark_ticker=bench_ticker,
            )
            logger.info(f"Benchmark ({bench_ticker}): {n_new} new bars cached.")
            bench_df = get_cached_returns_df(
                conn, bench_ticker, fetch_start, fetch_end
            )
            if bench_df.empty:
                logger.warning(
                    f"Benchmark {bench_ticker} returned no bars; "
                    f"industry-neutralized columns will be NaN for rows mapped to it."
                )
            benchmark_bars[bench_ticker] = bench_df
        # SPY specifically is required for the SPY-excess columns.
        if benchmark_bars.get(BENCHMARK_TICKER) is None or benchmark_bars[
            BENCHMARK_TICKER
        ].empty:
            raise RuntimeError(
                f"Benchmark {BENCHMARK_TICKER} returned no bars; cannot compute excess returns."
            )

        # --- 4. Fetch per-ticker bars (cache-first) ----------------------
        tickers = sorted(signals["ticker"].dropna().unique().tolist())
        logger.info(f"Fetching bars for {len(tickers)} unique tickers.")
        ticker_bars: Dict[str, pd.DataFrame] = {}
        for i, ticker in enumerate(tickers, start=1):
            ticker_bars[ticker] = fetch_ticker_bars(
                conn, provider, ticker, fetch_start, fetch_end, logger
            )
            if i % 25 == 0:
                logger.info(f"  ...{i}/{len(tickers)} tickers processed.")

        # --- 5. Compute forward + excess returns (SPY AND industry) -------
        enriched = compute_excess_returns_for_signals(
            signals, ticker_bars, benchmark_bars, FORWARD_HORIZONS, logger
        )
        coverage = (
            enriched[[f"return_{h}d" for h in FORWARD_HORIZONS]].notna().sum().to_dict()
        )
        logger.info(f"Return coverage per horizon: {coverage}")
        # Industry-benchmark-ticker distribution: lets us verify ITA/XLK
        # actually got wired up on enough rows to be meaningful.
        ind_dist = (
            enriched["industry_benchmark_ticker"].value_counts(dropna=False).to_dict()
        )
        logger.info(f"industry_benchmark_ticker distribution: {ind_dist}")

        # --- 6. Persist signals_with_returns ------------------------------
        conn.register("signals_with_returns_tmp", enriched)
        conn.execute(
            "CREATE OR REPLACE TABLE signals_with_returns AS "
            "SELECT * FROM signals_with_returns_tmp"
        )
        row_count = conn.execute(
            "SELECT COUNT(*) FROM signals_with_returns"
        ).fetchone()[0]
        logger.info(f"Wrote signals_with_returns: {row_count} rows.")

        # --- 7. Parquet export (optional) ---------------------------------
        if parquet_export_path:
            Path(parquet_export_path).parent.mkdir(parents=True, exist_ok=True)
            # Write via DuckDB so schema types round-trip cleanly.
            conn.execute(
                f"COPY signals_with_returns TO '{parquet_export_path}' (FORMAT PARQUET)"
            )
            logger.info(f"Exported Parquet: {parquet_export_path}")

        return row_count
    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="M2 alpha-validation harness: join forward returns onto signals_awards."
    )
    parser.add_argument(
        "--debug", action="store_true", help="Enable DEBUG-level logging"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Cap the number of signals rows processed (for dev/smoke runs).",
    )
    parser.add_argument(
        "--no-parquet",
        action="store_true",
        help="Skip the Parquet export (default: write to backend/data/analysis/).",
    )
    args = parser.parse_args()

    logger = setup_logging(debug=args.debug)
    logger.info("=" * 70)
    logger.info("Starting Phase 5: Alpha Validation Harness")
    logger.info("=" * 70)

    run_validation(
        limit=args.limit,
        parquet_export_path=None
        if args.no_parquet
        else "backend/data/analysis/signals_with_returns.parquet",
        logger=logger,
    )

    logger.info("Phase 5 complete.")


if __name__ == "__main__":
    main()
