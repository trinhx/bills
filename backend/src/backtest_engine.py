"""
Pure backtest engine for the Milestone 3 (M_backtest) event-driven study.

Functions here operate on pandas DataFrames already loaded from
``signals_with_returns`` and return plain dicts / scalars / DataFrames.
No DuckDB, no filesystem, no plotting -- those belong in the
``scripts/backtest.py`` layer. This separation keeps the engine fully
unit-testable against synthetic fixtures with known ground-truth Sharpe
ratios and decile-spread monotonicity.

Convention recap (M_backtest):

* Each row of the input DataFrame is a single contract event. We treat
  each event-class qualifying row as **one independent trade**: a
  ~20-business-day holding period that opens at ``action_date`` and
  closes at T+20.
* Equal-weight within each decile bucket. The long sleeve gets weight
  ``+1/n_long``; the short sleeve, if present, gets ``-1/n_short``.
* Sharpe is computed on per-trade T+20 returns with annualisation factor
  ``sqrt(252 / 20) = sqrt(12.6)``. We do **not** model overlapping
  positions or capacity constraints in this milestone -- this is a
  signal-validation engine, not a portfolio simulator.
* Transaction costs are a per-trade round-trip in basis points,
  subtracted from gross returns to produce net returns. Default 15 bps.
* Winsorisation is per-event-class at the (1%, 99%) tails, used only
  for ranking into deciles (the decile assignment is rank-based so
  this is mostly a defensive cosmetic for ``relative_ceiling_change``).
* Fiscal year is parsed from the first 4 chars of ``fiscal_quarter``
  (e.g. ``'FY24Q1'`` -> ``'FY24'``), matching report.py convention.
"""

from __future__ import annotations

import logging
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

#: Default round-trip transaction cost charged on every trade, in basis points.
DEFAULT_TXN_COST_BPS: float = 15.0

#: Default holding period in trading days. Matches the T+20 IC sweet spot.
DEFAULT_HOLD_DAYS: int = 20

#: Default winsorisation tails for per-class clipping.
DEFAULT_WINSOR_LOWER: float = 0.01
DEFAULT_WINSOR_UPPER: float = 0.99

#: Default decile granularity. 10 = standard top/bottom decile sleeves.
DEFAULT_N_BUCKETS: int = 10

#: Trading days per year, used to annualise per-trade Sharpe.
TRADING_DAYS_PER_YEAR: int = 252

#: Sharpe periods-per-year scaling = TRADING_DAYS_PER_YEAR / hold_days.
def _periods_per_year(hold_days: int) -> float:
    return TRADING_DAYS_PER_YEAR / float(hold_days)


# ---------------------------------------------------------------------------
# Pre-processing helpers
# ---------------------------------------------------------------------------


def winsorize_within_class(
    df: pd.DataFrame,
    signal_col: str,
    class_col: str,
    lower: float = DEFAULT_WINSOR_LOWER,
    upper: float = DEFAULT_WINSOR_UPPER,
) -> pd.Series:
    """
    Clip a signal column at the (lower, upper) quantiles **within each class**.

    Returns a new ``pd.Series`` aligned to ``df.index``. Rows with NaN
    signals are passed through unchanged. Classes with fewer than 5
    non-null values are also passed through unchanged (quantile
    estimates would be unstable). Use this on
    ``relative_ceiling_change`` to defang the 295 outlier rows
    documented in the v2 analysis without destroying rank order.
    """
    # Cast to float so we can safely clip without dtype-upcast errors when
    # the input is integer-typed (newer pandas raises on int->float coercion).
    out = df[signal_col].astype(float).copy()
    if class_col not in df.columns:
        logger.warning("class_col %s missing; returning signal unchanged", class_col)
        return out
    for cls, sub in df.groupby(class_col, dropna=False):
        if pd.isna(cls):
            continue
        vals = sub[signal_col].dropna().astype(float)
        if len(vals) < 5:
            continue
        lo = float(vals.quantile(lower))
        hi = float(vals.quantile(upper))
        idx = sub.index
        clipped = vals.clip(lower=lo, upper=hi)
        out.loc[clipped.index] = clipped.values
    return out


def _decile_assign(values: pd.Series, n_buckets: int) -> pd.Series:
    """
    Assign decile bucket integers ``1..n_buckets`` to a Series.

    NaN-preserving. Uses ``pd.qcut`` with ``duplicates='drop'`` so that
    near-constant signals (e.g., a class where 80% of rows have signal
    = 0) gracefully degrade to fewer buckets rather than raising.
    Returns ``Int64`` with ``pd.NA`` for NaN inputs.
    """
    clean = values.dropna()
    if len(clean) < n_buckets:
        # Not enough data for n buckets; everyone in NaN.
        return pd.Series([pd.NA] * len(values), index=values.index, dtype="Int64")
    try:
        cats = pd.qcut(clean, q=n_buckets, labels=False, duplicates="drop")
    except ValueError:
        # All-equal signal column: cannot bucketise.
        return pd.Series([pd.NA] * len(values), index=values.index, dtype="Int64")
    # qcut returns 0-indexed; we want 1-indexed.
    cats = cats + 1
    out = pd.Series([pd.NA] * len(values), index=values.index, dtype="Int64")
    out.loc[clean.index] = cats.astype("Int64")
    return out


def assign_deciles(
    df: pd.DataFrame,
    signal_col: str,
    class_col: Optional[str] = None,
    target_class: Optional[str] = None,
    n_buckets: int = DEFAULT_N_BUCKETS,
) -> pd.Series:
    """
    Assign decile bucket integers to ``df[signal_col]``.

    If ``class_col`` and ``target_class`` are both provided, only rows
    where ``df[class_col] == target_class`` are bucketed; all other
    rows receive ``pd.NA``. If only ``class_col`` is given,
    bucketisation is performed independently within each class. If
    neither is given, the entire column is bucketed in one pass.

    Buckets are 1-indexed (1 = lowest signal, n_buckets = highest)
    so that ``bucket == n_buckets`` is the canonical long sleeve.
    """
    if class_col is not None and target_class is not None:
        mask = df[class_col] == target_class
        out = pd.Series([pd.NA] * len(df), index=df.index, dtype="Int64")
        sub = df.loc[mask, signal_col]
        out.loc[mask] = _decile_assign(sub, n_buckets)
        return out
    if class_col is not None:
        out = pd.Series([pd.NA] * len(df), index=df.index, dtype="Int64")
        for cls, sub in df.groupby(class_col, dropna=False):
            if pd.isna(cls):
                continue
            out.loc[sub.index] = _decile_assign(sub[signal_col], n_buckets)
        return out
    return _decile_assign(df[signal_col], n_buckets)


# ---------------------------------------------------------------------------
# Portfolio construction
# ---------------------------------------------------------------------------


def build_in_class_portfolio(
    df: pd.DataFrame,
    signal_col: str,
    return_col: str,
    class_col: str,
    target_class: str,
    n_buckets: int = DEFAULT_N_BUCKETS,
    txn_cost_bps: float = DEFAULT_TXN_COST_BPS,
) -> pd.DataFrame:
    """
    Build a long-top-decile / short-bottom-decile portfolio within one event class.

    Returns a trade-level DataFrame with columns ``[reference_trading_date,
    fiscal_year, ticker, side, signal_value, gross_return, net_return,
    weight]``. The ``side`` column is ``'long'`` or ``'short'``;
    ``weight`` is signed (+ for long, - for short) and equal-magnitude
    within each sleeve. Transaction cost ``txn_cost_bps`` is subtracted
    from the absolute gross return on each trade (so it always reduces
    profitability regardless of direction).
    """
    sub = df[df[class_col] == target_class].copy()
    sub = sub.dropna(subset=[signal_col, return_col])
    if len(sub) < n_buckets:
        return _empty_trades()
    sub["__bucket"] = _decile_assign(sub[signal_col], n_buckets)
    sub = sub.dropna(subset=["__bucket"])
    actual_n_buckets = int(sub["__bucket"].max())  # may be < n_buckets
    if actual_n_buckets < 2:
        return _empty_trades()
    long_bucket = actual_n_buckets
    short_bucket = 1
    longs = sub[sub["__bucket"] == long_bucket]
    shorts = sub[sub["__bucket"] == short_bucket]
    return _assemble_trades(
        longs, shorts, signal_col, return_col, txn_cost_bps
    )


def build_long_short_portfolio(
    df: pd.DataFrame,
    signal_col: str,
    return_col: str,
    class_col: str,
    long_class: str,
    short_class: str,
    txn_cost_bps: float = DEFAULT_TXN_COST_BPS,
) -> pd.DataFrame:
    """
    Build a cross-class portfolio: long every row in ``long_class``,
    short every row in ``short_class``. Equal-weight within each sleeve.

    No bucketing is applied -- the event-class membership *is* the
    selection criterion. Returns the same trade-level shape as
    ``build_in_class_portfolio``. ``signal_value`` is included for
    downstream attribution but does not affect weights.
    """
    longs = df[df[class_col] == long_class].dropna(subset=[return_col]).copy()
    shorts = df[df[class_col] == short_class].dropna(subset=[return_col]).copy()
    if len(longs) == 0 or len(shorts) == 0:
        return _empty_trades()
    return _assemble_trades(longs, shorts, signal_col, return_col, txn_cost_bps)


# ---------------------------------------------------------------------------
# Trade assembly (private)
# ---------------------------------------------------------------------------


def _empty_trades() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "reference_trading_date",
            "fiscal_year",
            "ticker",
            "side",
            "signal_value",
            "gross_return",
            "net_return",
            "weight",
        ]
    )


def _fiscal_year_from_quarter(s: pd.Series) -> pd.Series:
    """Slice the first 4 chars (e.g., 'FY24') from fiscal_quarter."""
    return s.astype(str).str.slice(0, 4)


def _assemble_trades(
    longs: pd.DataFrame,
    shorts: pd.DataFrame,
    signal_col: str,
    return_col: str,
    txn_cost_bps: float,
) -> pd.DataFrame:
    """Combine long and short legs into a single trade-level frame."""
    cost_decimal = txn_cost_bps / 10_000.0
    n_long = len(longs)
    n_short = len(shorts)
    long_w = (1.0 / n_long) if n_long > 0 else 0.0
    short_w = (-1.0 / n_short) if n_short > 0 else 0.0
    parts: List[pd.DataFrame] = []
    if n_long > 0:
        long_frame = pd.DataFrame(
            {
                "reference_trading_date": longs.get(
                    "reference_trading_date", pd.Series([pd.NaT] * n_long)
                ).values,
                "fiscal_year": _fiscal_year_from_quarter(
                    longs.get("fiscal_quarter", pd.Series([""] * n_long))
                ).values,
                "ticker": longs.get("ticker", pd.Series([""] * n_long)).values,
                "side": "long",
                "signal_value": longs[signal_col].values
                if signal_col in longs.columns
                else np.nan,
                "gross_return": longs[return_col].values,
                "net_return": longs[return_col].values - cost_decimal,
                "weight": long_w,
            }
        )
        parts.append(long_frame)
    if n_short > 0:
        short_frame = pd.DataFrame(
            {
                "reference_trading_date": shorts.get(
                    "reference_trading_date", pd.Series([pd.NaT] * n_short)
                ).values,
                "fiscal_year": _fiscal_year_from_quarter(
                    shorts.get("fiscal_quarter", pd.Series([""] * n_short))
                ).values,
                "ticker": shorts.get("ticker", pd.Series([""] * n_short)).values,
                "side": "short",
                "signal_value": shorts[signal_col].values
                if signal_col in shorts.columns
                else np.nan,
                "gross_return": -shorts[return_col].values,  # short profits when stock falls
                "net_return": -shorts[return_col].values - cost_decimal,
                "weight": short_w,
            }
        )
        parts.append(short_frame)
    if not parts:
        return _empty_trades()
    return pd.concat(parts, ignore_index=True)


# ---------------------------------------------------------------------------
# Performance summarisation
# ---------------------------------------------------------------------------


def summarize_portfolio(
    trades: pd.DataFrame,
    hold_days: int = DEFAULT_HOLD_DAYS,
    *,
    aggregation: str = "per_trade",
) -> Dict[str, Optional[float]]:
    """
    Compute headline portfolio statistics from a trade-level DataFrame.

    Returns ``{n_trades, n_long, n_short, gross_mean, net_mean,
    net_stddev, sharpe, sortino, hit_rate, gross_total, net_total,
    aggregation, n_periods}``. All scalar fields are native ``float`` or
    ``int``; missing / undefined statistics return ``None`` (e.g.,
    Sharpe with stddev=0).

    ``aggregation`` controls how trades are pooled into return periods:

    * ``'per_trade'`` (default): treat each trade as one independent
      ~T+hold_days return realisation. Correct for in-class long/short
      backtests where the long and short sleeves have equal size and
      every position is one period. Annualises by ``sqrt(252 / hold_days)``.
    * ``'monthly'``: bucket trades by calendar month of
      ``reference_trading_date`` and within each month compute a
      sleeve-aware spread ``mean(long net_returns) + mean(short
      net_returns)`` (where short net_returns already carry the
      sign-flip). Necessary for cross-class long/short backtests with
      unequal sleeves; the per-trade pool is biased there. Annualises
      by ``sqrt(12)``.
    """
    empty = {
        "n_trades": 0,
        "n_long": 0,
        "n_short": 0,
        "gross_mean": None,
        "net_mean": None,
        "net_stddev": None,
        "sharpe": None,
        "sortino": None,
        "hit_rate": None,
        "gross_total": None,
        "net_total": None,
        "aggregation": aggregation,
        "n_periods": 0,
    }
    if len(trades) == 0:
        return empty
    if aggregation not in ("per_trade", "monthly"):
        raise ValueError(
            f"aggregation must be 'per_trade' or 'monthly', got {aggregation!r}"
        )
    n_long = int((trades["side"] == "long").sum())
    n_short = int((trades["side"] == "short").sum())
    if aggregation == "per_trade":
        return _summarize_per_trade(trades, hold_days, n_long, n_short)
    return _summarize_monthly(trades, n_long, n_short)


def _summarize_per_trade(
    trades: pd.DataFrame, hold_days: int, n_long: int, n_short: int
) -> Dict[str, Optional[float]]:
    gross = trades["gross_return"].astype(float)
    net = trades["net_return"].astype(float)
    n = len(trades)
    gross_mean = float(gross.mean())
    net_mean = float(net.mean())
    net_stddev = float(net.std(ddof=1)) if n > 1 else 0.0
    ann_scale = float(np.sqrt(_periods_per_year(hold_days)))
    sharpe = float((net_mean / net_stddev) * ann_scale) if net_stddev > 0 else None
    downside = net[net < 0]
    sortino = (
        float((net_mean / float(downside.std(ddof=1))) * ann_scale)
        if len(downside) > 1 and float(downside.std(ddof=1)) > 0
        else None
    )
    return {
        "n_trades": int(n),
        "n_long": n_long,
        "n_short": n_short,
        "gross_mean": gross_mean,
        "net_mean": net_mean,
        "net_stddev": net_stddev,
        "sharpe": sharpe,
        "sortino": sortino,
        "hit_rate": float((net > 0).mean()),
        "gross_total": float(gross.sum()),
        "net_total": float(net.sum()),
        "aggregation": "per_trade",
        "n_periods": int(n),
    }


def _summarize_monthly(
    trades: pd.DataFrame, n_long: int, n_short: int
) -> Dict[str, Optional[float]]:
    """
    Bucket trades by calendar month of reference_trading_date and compute
    a sleeve-aware spread per month. Within each month, the portfolio
    return is ``mean(long net_returns) + mean(short net_returns)``
    where short ``net_returns`` already carry the sign flip applied in
    ``_assemble_trades``. Months with only one sleeve present contribute
    only that sleeve's mean (this is rare with cross-class on a 6-year
    dataset but possible at the edges).
    """
    df = trades.copy()
    df["reference_trading_date"] = pd.to_datetime(df["reference_trading_date"])
    df = df.dropna(subset=["reference_trading_date"])
    if df.empty:
        return _summarize_per_trade(df, DEFAULT_HOLD_DAYS, n_long, n_short)
    df["__month"] = df["reference_trading_date"].dt.to_period("M")
    rows: List[Dict[str, float]] = []
    for month, sub in df.groupby("__month"):
        long_mean_g = float(sub.loc[sub["side"] == "long", "gross_return"].mean()) if (sub["side"] == "long").any() else 0.0
        short_mean_g = float(sub.loc[sub["side"] == "short", "gross_return"].mean()) if (sub["side"] == "short").any() else 0.0
        long_mean_n = float(sub.loc[sub["side"] == "long", "net_return"].mean()) if (sub["side"] == "long").any() else 0.0
        short_mean_n = float(sub.loc[sub["side"] == "short", "net_return"].mean()) if (sub["side"] == "short").any() else 0.0
        # Both sleeve means are added because short net_return already carries
        # the sign flip from _assemble_trades.
        rows.append(
            {
                "month": str(month),
                "gross_period": long_mean_g + short_mean_g,
                "net_period": long_mean_n + short_mean_n,
                "n_long": int((sub["side"] == "long").sum()),
                "n_short": int((sub["side"] == "short").sum()),
            }
        )
    if not rows:
        return _summarize_per_trade(trades, DEFAULT_HOLD_DAYS, n_long, n_short)
    periods = pd.DataFrame(rows)
    n_periods = len(periods)
    net = periods["net_period"]
    gross = periods["gross_period"]
    gross_mean = float(gross.mean())
    net_mean = float(net.mean())
    net_stddev = float(net.std(ddof=1)) if n_periods > 1 else 0.0
    ann_scale = float(np.sqrt(12))
    sharpe = float((net_mean / net_stddev) * ann_scale) if net_stddev > 0 else None
    downside = net[net < 0]
    sortino = (
        float((net_mean / float(downside.std(ddof=1))) * ann_scale)
        if len(downside) > 1 and float(downside.std(ddof=1)) > 0
        else None
    )
    return {
        "n_trades": int(len(trades)),
        "n_long": n_long,
        "n_short": n_short,
        "gross_mean": gross_mean,
        "net_mean": net_mean,
        "net_stddev": net_stddev,
        "sharpe": sharpe,
        "sortino": sortino,
        "hit_rate": float((net > 0).mean()),
        "gross_total": float(gross.sum()),
        "net_total": float(net.sum()),
        "aggregation": "monthly",
        "n_periods": int(n_periods),
    }


def summarize_by_year(
    trades: pd.DataFrame,
    hold_days: int = DEFAULT_HOLD_DAYS,
    *,
    aggregation: str = "per_trade",
) -> pd.DataFrame:
    """
    Per-fiscal-year breakdown of trade statistics.

    Returns a DataFrame with columns ``[fiscal_year, n_trades, gross_mean,
    net_mean, net_stddev, sharpe, hit_rate]`` sorted ascending by fiscal
    year. Empty input yields an empty DataFrame with the right columns.

    When ``aggregation='monthly'``, each fiscal year's stats are
    computed from monthly portfolio returns within that year (sleeve-
    aware), matching the headline ``summarize_portfolio`` aggregation.
    """
    cols = [
        "fiscal_year",
        "n_trades",
        "gross_mean",
        "net_mean",
        "net_stddev",
        "sharpe",
        "hit_rate",
    ]
    if len(trades) == 0 or "fiscal_year" not in trades.columns:
        return pd.DataFrame(columns=cols)
    rows: List[Dict[str, object]] = []
    for fy, sub in trades.groupby("fiscal_year"):
        if not fy or pd.isna(fy):
            continue
        if aggregation == "monthly":
            # Build per-month spread within this FY, then aggregate.
            stats = _summarize_monthly(
                sub,
                int((sub["side"] == "long").sum()),
                int((sub["side"] == "short").sum()),
            )
        else:
            stats = _summarize_per_trade(
                sub,
                hold_days,
                int((sub["side"] == "long").sum()),
                int((sub["side"] == "short").sum()),
            )
        rows.append(
            {
                "fiscal_year": fy,
                "n_trades": stats["n_trades"],
                "gross_mean": stats["gross_mean"],
                "net_mean": stats["net_mean"],
                "net_stddev": stats["net_stddev"],
                "sharpe": stats["sharpe"],
                "hit_rate": stats["hit_rate"],
            }
        )
    out = pd.DataFrame(rows, columns=cols)
    return out.sort_values("fiscal_year").reset_index(drop=True)


# ---------------------------------------------------------------------------
# Decile spread (for monotonicity diagnostic)
# ---------------------------------------------------------------------------


def decile_returns(
    df: pd.DataFrame,
    signal_col: str,
    return_col: str,
    class_col: Optional[str] = None,
    target_class: Optional[str] = None,
    n_buckets: int = DEFAULT_N_BUCKETS,
) -> pd.DataFrame:
    """
    Return mean gross return per decile bucket for monotonicity diagnostics.

    Optional ``class_col`` + ``target_class`` filters rows to one event
    class first, matching ``assign_deciles``. Returns a DataFrame with
    columns ``[bucket, n, mean_return, std_return]`` sorted ascending
    by bucket. A monotone-rising column is the in-sample story we
    expect for a tradable signal.
    """
    if class_col is not None and target_class is not None:
        sub = df[df[class_col] == target_class].copy()
    else:
        sub = df.copy()
    sub = sub.dropna(subset=[signal_col, return_col])
    if len(sub) < n_buckets:
        return pd.DataFrame(columns=["bucket", "n", "mean_return", "std_return"])
    sub["__bucket"] = _decile_assign(sub[signal_col], n_buckets)
    sub = sub.dropna(subset=["__bucket"])
    grouped = sub.groupby("__bucket")[return_col].agg(["count", "mean", "std"])
    grouped = grouped.reset_index()
    grouped.columns = ["bucket", "n", "mean_return", "std_return"]
    grouped["bucket"] = grouped["bucket"].astype(int)
    return grouped.sort_values("bucket").reset_index(drop=True)


# ---------------------------------------------------------------------------
# Top-level orchestrator
# ---------------------------------------------------------------------------


def run_backtest(
    df: pd.DataFrame,
    signal_col: str,
    return_col: str,
    class_col: str,
    *,
    framing: str,
    target_class: Optional[str] = None,
    long_class: Optional[str] = None,
    short_class: Optional[str] = None,
    winsorize: bool = False,
    n_buckets: int = DEFAULT_N_BUCKETS,
    txn_cost_bps: float = DEFAULT_TXN_COST_BPS,
    hold_days: int = DEFAULT_HOLD_DAYS,
) -> Dict[str, object]:
    """
    Run one backtest variant end-to-end and return a structured result dict.

    ``framing`` must be either ``'in_class'`` (top vs bottom decile within
    one ``target_class``) or ``'cross_class'`` (long every row in
    ``long_class``, short every row in ``short_class``).

    If ``winsorize=True``, ``df[signal_col]`` is clipped per-class at
    the (1%, 99%) tails before bucketing. This is the recommended
    setting for ``relative_ceiling_change``.

    Returns a dict with keys ``{config, summary, by_year, deciles,
    trades}`` where ``trades`` is the full trade-level DataFrame.
    """
    if framing not in ("in_class", "cross_class"):
        raise ValueError(f"framing must be 'in_class' or 'cross_class', got {framing!r}")
    work = df.copy()
    if winsorize:
        work[signal_col] = winsorize_within_class(work, signal_col, class_col)
    if framing == "in_class":
        if target_class is None:
            raise ValueError("framing='in_class' requires target_class")
        trades = build_in_class_portfolio(
            work,
            signal_col=signal_col,
            return_col=return_col,
            class_col=class_col,
            target_class=target_class,
            n_buckets=n_buckets,
            txn_cost_bps=txn_cost_bps,
        )
        deciles = decile_returns(
            work,
            signal_col=signal_col,
            return_col=return_col,
            class_col=class_col,
            target_class=target_class,
            n_buckets=n_buckets,
        )
    else:
        if long_class is None or short_class is None:
            raise ValueError(
                "framing='cross_class' requires both long_class and short_class"
            )
        trades = build_long_short_portfolio(
            work,
            signal_col=signal_col,
            return_col=return_col,
            class_col=class_col,
            long_class=long_class,
            short_class=short_class,
            txn_cost_bps=txn_cost_bps,
        )
        deciles = pd.DataFrame(
            columns=["bucket", "n", "mean_return", "std_return"]
        )  # not meaningful for cross-class
    # In-class with equal sleeves uses per-trade pooling. Cross-class with
    # unequal sleeves uses calendar-monthly aggregation; per-trade pooling
    # would silently bias toward the larger sleeve's mean.
    aggregation = "per_trade" if framing == "in_class" else "monthly"
    summary = summarize_portfolio(trades, hold_days=hold_days, aggregation=aggregation)
    by_year = summarize_by_year(trades, hold_days=hold_days, aggregation=aggregation)
    return {
        "config": {
            "signal_col": signal_col,
            "return_col": return_col,
            "class_col": class_col,
            "framing": framing,
            "target_class": target_class,
            "long_class": long_class,
            "short_class": short_class,
            "winsorize": winsorize,
            "n_buckets": n_buckets,
            "txn_cost_bps": txn_cost_bps,
            "hold_days": hold_days,
        },
        "summary": summary,
        "by_year": by_year,
        "deciles": deciles,
        "trades": trades,
    }


# ---------------------------------------------------------------------------
# Cross-regime stability check
# ---------------------------------------------------------------------------


def same_sign_year_count(by_year: pd.DataFrame, metric: str = "net_mean") -> Dict[str, int]:
    """
    Count fiscal years with net positive vs negative ``metric``.

    Returns ``{n_years, n_positive, n_negative, n_zero}``. Used to
    enforce the project-wide pass criterion of "same-signed in >= 5 of
    6 fiscal years". The caller decides which sign was expected and
    whether the count is sufficient.
    """
    if len(by_year) == 0 or metric not in by_year.columns:
        return {"n_years": 0, "n_positive": 0, "n_negative": 0, "n_zero": 0}
    vals = by_year[metric].dropna()
    return {
        "n_years": int(len(vals)),
        "n_positive": int((vals > 0).sum()),
        "n_negative": int((vals < 0).sum()),
        "n_zero": int((vals == 0).sum()),
    }
