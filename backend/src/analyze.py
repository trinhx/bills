"""
Pure analytics for the Milestone 2 alpha-validation harness.

Functions here operate on pandas DataFrames and return plain dicts /
scalars / DataFrames. No DuckDB, no filesystem, no plotting -- those
are the report.py layer's concern. This separation keeps analytics
fully unit-testable against synthetic fixtures with known ground-truth
Information Coefficient and decile spreads.

Conventions:

* ``signal`` = a per-row continuous value, higher typically means more
  bullish (we don't flip signs). Call sites can negate before passing.
* ``forward_return`` = a per-row scalar realised return, e.g.
  ``excess_return_20d``. NaN rows are silently dropped by every helper.
* All IC values are Spearman rank correlations (robust to non-linear
  relationships and outliers, which the raw signal distributions have).
"""

from __future__ import annotations

import logging
from typing import Dict, Iterable, List, Optional

import numpy as np
import pandas as pd
from scipy.stats import spearmanr

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Information Coefficient
# ---------------------------------------------------------------------------


def information_coefficient(
    signal: pd.Series, forward_return: pd.Series
) -> Dict[str, Optional[float]]:
    """
    Spearman rank correlation between a signal and forward returns.

    NaN-safe: pairs where either value is NaN are dropped before the
    correlation is computed. Returns ``{'ic': float, 'p_value': float,
    'n': int}``; ``ic`` and ``p_value`` are ``None`` if fewer than 3
    non-NaN pairs exist (Spearman is undefined) or if every value is
    identical (constant-variance corner case).
    """
    df = pd.DataFrame({"s": signal.values, "r": forward_return.values}).dropna()
    n = len(df)
    if n < 3:
        return {"ic": None, "p_value": None, "n": n}
    # Constant signal or constant returns -> correlation undefined.
    if df["s"].nunique() < 2 or df["r"].nunique() < 2:
        return {"ic": None, "p_value": None, "n": n}

    rho, p = spearmanr(df["s"].values, df["r"].values)
    # scipy can return numpy.float64; normalise to native Python floats.
    return {"ic": float(rho), "p_value": float(p), "n": n}


def ic_per_sector(
    df: pd.DataFrame,
    signal_col: str,
    return_col: str,
    sector_col: str = "sector",
    min_rows_per_sector: int = 30,
) -> pd.DataFrame:
    """
    IC broken down by sector. Sectors with fewer than
    ``min_rows_per_sector`` non-NaN pairs are dropped (too noisy to
    report). Returns a DataFrame sorted by absolute IC, descending.
    """
    rows = []
    for sector, sub in df.groupby(sector_col, dropna=False):
        sector_key = "UNKNOWN" if pd.isna(sector) else sector
        result = information_coefficient(sub[signal_col], sub[return_col])
        if result["n"] is None or result["n"] < min_rows_per_sector:
            continue
        rows.append(
            {
                "sector": sector_key,
                "n": result["n"],
                "ic": result["ic"],
                "p_value": result["p_value"],
            }
        )
    out = pd.DataFrame(rows, columns=["sector", "n", "ic", "p_value"])
    if out.empty:
        return out
    return out.sort_values("ic", key=lambda s: s.abs(), ascending=False).reset_index(
        drop=True
    )


# ---------------------------------------------------------------------------
# Decile / quantile spreads
# ---------------------------------------------------------------------------


def decile_spread(
    df: pd.DataFrame,
    signal_col: str,
    return_col: str,
    n_buckets: int = 10,
) -> pd.DataFrame:
    """
    Rank rows by ``signal_col`` into ``n_buckets`` equal-size buckets,
    then compute mean and std forward return per bucket.

    Returns a DataFrame with columns ``bucket, n, mean_return,
    std_return``, one row per bucket. Buckets are numbered 1..n_buckets
    with 1 = lowest-signal and n_buckets = highest-signal.
    """
    sub = df[[signal_col, return_col]].dropna()
    if len(sub) < n_buckets:
        return pd.DataFrame(columns=["bucket", "n", "mean_return", "std_return"])

    # qcut with duplicates='drop' handles ties gracefully -- if many rows
    # have identical signal values, we collapse the overlapping buckets
    # rather than raising.
    try:
        sub = sub.assign(
            bucket=pd.qcut(sub[signal_col], n_buckets, labels=False, duplicates="drop")
            + 1
        )
    except ValueError:
        return pd.DataFrame(columns=["bucket", "n", "mean_return", "std_return"])

    grouped = sub.groupby("bucket", observed=True)[return_col].agg(
        ["count", "mean", "std"]
    )
    grouped = grouped.reset_index().rename(
        columns={"count": "n", "mean": "mean_return", "std": "std_return"}
    )
    return grouped[["bucket", "n", "mean_return", "std_return"]]


def top_minus_bottom(
    df: pd.DataFrame,
    signal_col: str,
    return_col: str,
    n_buckets: int = 10,
) -> Optional[float]:
    """
    Top-decile minus bottom-decile mean return. Classic "spread"
    measure of whether a signal's ranking has economic value.

    Returns ``None`` when there aren't enough rows to form both tails.
    """
    spread_df = decile_spread(df, signal_col, return_col, n_buckets)
    if spread_df.empty:
        return None
    # After ``duplicates='drop'`` the actual observed bucket range may
    # be smaller than n_buckets; take the min and max of what we got.
    b_min, b_max = spread_df["bucket"].min(), spread_df["bucket"].max()
    if b_min == b_max:
        return None
    top = spread_df[spread_df["bucket"] == b_max]["mean_return"].iloc[0]
    bot = spread_df[spread_df["bucket"] == b_min]["mean_return"].iloc[0]
    return float(top - bot)


# ---------------------------------------------------------------------------
# Cumulative P&L
# ---------------------------------------------------------------------------


def cumulative_pnl(
    df: pd.DataFrame,
    signal_col: str,
    return_col: str,
    strategy: str = "long_top_decile",
    n_buckets: int = 10,
    date_col: str = "reference_trading_date",
) -> pd.Series:
    """
    Build a date-indexed cumulative return curve for a trading
    strategy defined by ``strategy``:

    * ``long_top_decile``  -- equal-weight long positions in the top
      decile on each action_date, no rebalancing within the horizon.
    * ``long_short``       -- long top decile, short bottom decile,
      equal notional on each side.

    Returns a Series indexed by ``reference_trading_date`` containing
    the cumulative P&L (starting at 0 on the first observed date).

    Note: this is a simple event-driven approximation, not a realistic
    backtest. It's purpose here is signal-ranking, not portfolio construction.
    """
    if strategy not in {"long_top_decile", "long_short"}:
        raise ValueError(f"Unknown strategy: {strategy!r}")

    sub = df[[signal_col, return_col, date_col]].dropna()
    if len(sub) < n_buckets:
        return pd.Series(dtype=float, name=strategy)

    try:
        sub = sub.assign(
            bucket=pd.qcut(sub[signal_col], n_buckets, labels=False, duplicates="drop")
            + 1
        )
    except ValueError:
        return pd.Series(dtype=float, name=strategy)

    b_min, b_max = sub["bucket"].min(), sub["bucket"].max()
    if b_min == b_max:
        return pd.Series(dtype=float, name=strategy)

    # Compute per-row portfolio return: +return_col if in top bucket,
    # -return_col if in bottom bucket (for long_short only), else 0.
    if strategy == "long_top_decile":
        pos = sub["bucket"].eq(b_max).astype(int).values  # 0 or 1
        leg_return = sub[return_col].values * pos
    else:  # long_short
        top = sub["bucket"].eq(b_max).astype(int).values
        bot = sub["bucket"].eq(b_min).astype(int).values
        leg_return = sub[return_col].values * (top - bot)

    # Equal-weight within a given date: average across that day's rows.
    pnl = (
        pd.DataFrame({"d": sub[date_col].values, "r": leg_return})
        .groupby("d")["r"]
        .mean()
        .sort_index()
        .cumsum()
    )
    pnl.name = strategy
    return pnl


# ---------------------------------------------------------------------------
# Signal coverage summary
# ---------------------------------------------------------------------------


def signal_coverage(df: pd.DataFrame, signal_cols: Iterable[str]) -> pd.DataFrame:
    """
    One-row-per-signal coverage report: how many non-null values each
    candidate has, plus coverage as a percentage of the full dataset.
    """
    n_total = len(df)
    rows = []
    for col in signal_cols:
        if col not in df.columns:
            rows.append({"signal": col, "n_non_null": 0, "pct": 0.0})
            continue
        n_non_null = int(df[col].notna().sum())
        rows.append(
            {
                "signal": col,
                "n_non_null": n_non_null,
                "pct": round(100.0 * n_non_null / n_total, 2) if n_total else 0.0,
            }
        )
    return pd.DataFrame(rows, columns=["signal", "n_non_null", "pct"])


# ---------------------------------------------------------------------------
# Multi-signal / multi-horizon summary
# ---------------------------------------------------------------------------


def summarize_all_signals(
    df: pd.DataFrame,
    signal_cols: Iterable[str],
    horizons: Iterable[int],
    return_prefix: str = "excess_return_",
) -> pd.DataFrame:
    """
    Build the top-of-report summary table: one row per (signal, horizon)
    containing IC, top-minus-bottom spread, and sample size.
    """
    rows = []
    for signal in signal_cols:
        if signal not in df.columns:
            continue
        for h in horizons:
            return_col = f"{return_prefix}{h}d"
            if return_col not in df.columns:
                continue
            ic = information_coefficient(df[signal], df[return_col])
            tmb = top_minus_bottom(df, signal, return_col)
            rows.append(
                {
                    "signal": signal,
                    "horizon": f"T+{h}d",
                    "n": ic["n"],
                    "ic": ic["ic"],
                    "p_value": ic["p_value"],
                    "top_minus_bottom": tmb,
                }
            )
    return pd.DataFrame(
        rows, columns=["signal", "horizon", "n", "ic", "p_value", "top_minus_bottom"]
    )
