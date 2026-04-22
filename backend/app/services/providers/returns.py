"""
Daily-returns provider abstraction for the alpha-validation harness (M2.1).

The pipeline's Milestone 2 validation step needs split- and dividend-adjusted
daily closing prices for every resolved ticker over a ~5-month window
(min(action_date) - 5d through max(action_date) + 95d) so that forward
returns at T+1 / T+5 / T+20 / T+60 can be computed against each signal.

Design goals:

* **Swappable sources** — yfinance today, but the Protocol lets us drop in
  Polygon, CRSP, or a paid feed later without touching ``validate.py`` or
  ``analyze.py``.
* **Split-adjusted by default** — ``auto_adjust=True`` in yfinance so
  corporate actions are already baked into the close prices we cache.
  Eliminates a whole class of look-ahead-bias bugs.
* **Rate-limited** — shares the existing ``@with_retry`` decorator and a
  conservative token-bucket to coexist with the market-cap provider.
* **Ticker-normalised at the API boundary** — OpenFIGI's ``BRK/A`` still
  lives as the cache key in ``cache_market_cap``; Yahoo sees ``BRK-A``.
  Reuses the M1.5 ``_normalize_ticker_for_yahoo`` helper.
"""

from __future__ import annotations

import json
import logging
from datetime import date, timedelta
from typing import Optional, Protocol, runtime_checkable

import pandas as pd
import requests
import yfinance as yf

from backend.app.services.providers.base import RateLimiter, with_retry
from backend.app.services.providers.yahoo import _normalize_ticker_for_yahoo

logger = logging.getLogger(__name__)

# SPY is the canonical US broad-market benchmark used for excess-return
# calculations in the validation harness. Kept as a constant so downstream
# code and reports don't string-literal it in multiple places.
BENCHMARK_TICKER: str = "SPY"


# ---------------------------------------------------------------------------
# Industry-level benchmark mapping (M2.5)
# ---------------------------------------------------------------------------
#
# Government-prime contractors dominate the public universe (~77% of
# resolved rows). The broad-market SPY benchmark doesn't neutralize the
# sector-specific moves that drive those names -- defense rallies /
# drawdowns, IT-services consolidation themes, etc. To separate "news
# about a specific contract" from "news about the industry", we subtract
# an industry-appropriate ETF instead of (or alongside) SPY.
#
# The mapping is deliberately conservative:
#   * Aerospace & Defense      -> ITA  (iShares U.S. Aerospace & Defense ETF)
#   * Information Technology
#     Services                 -> XLK  (Technology Select Sector SPDR)
#   * Everything else          -> SPY  (falls back to the broad benchmark)
#
# The mapping keys are the Yahoo Finance ``industry`` column (NOT sector).
# Unknown industries or NaN/None fall through to SPY.
INDUSTRY_BENCHMARK_MAP: dict[str, str] = {
    "Aerospace & Defense": "ITA",
    "Information Technology Services": "XLK",
}

# The full set of benchmarks the validator pre-fetches on every run.
# ``BENCHMARK_TICKER`` (SPY) is always included; the values of the
# industry map are added on top. Deduplicated for the fetch loop.
ALL_BENCHMARK_TICKERS: list[str] = sorted(
    {BENCHMARK_TICKER, *INDUSTRY_BENCHMARK_MAP.values()}
)


def industry_benchmark_ticker_for(industry: str | None) -> str:
    """
    Return the benchmark ticker appropriate for a given Yahoo ``industry``.

    Falls back to ``BENCHMARK_TICKER`` (SPY) for unmapped / NULL inputs.
    Callers should use this as the single source of truth so that Phase 5
    and the reporting layer stay consistent.
    """
    if industry is None:
        return BENCHMARK_TICKER
    return INDUSTRY_BENCHMARK_MAP.get(industry, BENCHMARK_TICKER)

# yfinance and CRSP/Polygon can each raise a variety of non-``requests``
# exceptions on transient failures (malformed JSON, missing keys on a
# partial response, etc.). The retry decorator needs a broader tuple than
# just ``RequestException`` so these don't skip the retry and bubble up
# immediately.
_RETURNS_RETRY_EXCEPTIONS = (
    requests.exceptions.RequestException,
    json.JSONDecodeError,
    ValueError,
    KeyError,
)

# Daily-bar fetches are small (~150 rows per ticker per full FY window)
# but Yahoo throttles aggressively. Match the market-cap rate -- 10
# requests per 10 seconds -- so the two providers share roughly one
# request budget.
RETURNS_RATE_LIMITER = RateLimiter(max_requests=10, time_window=10.0)


# ---------------------------------------------------------------------------
# Protocol
# ---------------------------------------------------------------------------


@runtime_checkable
class ReturnsProvider(Protocol):
    """
    Interface every returns source must satisfy.

    ``fetch_daily_bars`` is the workhorse. Returns a DataFrame indexed by
    trading date with (at minimum) a ``close_adj`` column and a
    ``return_1d`` column. Implementations MUST guarantee that prices are
    split- and dividend-adjusted so ``return_1d`` is directly comparable
    across corporate actions.

    ``fetch_benchmark`` is a thin convenience over ``fetch_daily_bars``
    pinned to the canonical benchmark ticker (SPY). Exposed separately so
    the validation orchestrator can pre-fetch it once up front.
    """

    def fetch_daily_bars(self, ticker: str, start: date, end: date) -> pd.DataFrame: ...

    def fetch_benchmark(self, start: date, end: date) -> pd.DataFrame: ...


# ---------------------------------------------------------------------------
# YFinanceReturnsProvider
# ---------------------------------------------------------------------------


def _compute_daily_bars_frame(history_df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert a ``yf.Ticker.history`` DataFrame into the canonical output
    shape: ``date`` index, ``close_adj`` column, ``return_1d`` column.

    Separated from network code so it can be unit-tested against fixture
    DataFrames without mocking yfinance.
    """
    if history_df is None or history_df.empty:
        return pd.DataFrame(columns=["close_adj", "return_1d"])

    # Normalise tz so the returned frame has a naive DatetimeIndex --
    # downstream joins align on DATE (no hour component), so the tz
    # would just confuse things.
    idx = history_df.index
    if getattr(idx, "tz", None) is not None:
        history_df = history_df.copy()
        history_df.index = idx.tz_localize(None)

    out = pd.DataFrame(index=history_df.index)
    # With ``auto_adjust=True`` the ``Close`` column is already adjusted;
    # we rename it to make the adjustment status explicit in our schema.
    out["close_adj"] = history_df["Close"].astype(float)
    # Strict prior-close percent change. Leaves the first row as NaN,
    # which is correct: no prior day exists in the fetch window.
    out["return_1d"] = out["close_adj"].pct_change()
    return out


class YFinanceReturnsProvider:
    """
    yfinance-backed implementation of :class:`ReturnsProvider`.

    Thin wrapper around ``yf.Ticker(...).history(...)``. Responsibilities
    live in small private helpers to keep each unit independently testable:

    * :func:`_normalize_ticker_for_yahoo` (imported from the market-cap
      provider) -- maps ``BRK/A`` -> ``BRK-A``.
    * :func:`_compute_daily_bars_frame` -- shapes the raw history into our
      canonical ``(close_adj, return_1d)`` DataFrame.
    """

    def __init__(self) -> None:
        # No instance state today; provided so future subclasses can
        # override behaviour without changing the call sites.
        pass

    @with_retry(
        max_attempts=3,
        base_delay=2.0,
        retry_exceptions=_RETURNS_RETRY_EXCEPTIONS,
    )
    def fetch_daily_bars(self, ticker: str, start: date, end: date) -> pd.DataFrame:
        """
        Fetch split/dividend-adjusted daily bars for ``ticker`` over
        ``[start, end)``. Returns a DataFrame indexed by trading date
        with ``close_adj`` and ``return_1d`` columns, or an empty frame
        on no data.
        """
        RETURNS_RATE_LIMITER.wait()
        yf_ticker = _normalize_ticker_for_yahoo(ticker)
        # yfinance's ``end`` is exclusive; we push it out by one day so
        # callers can pass the intuitive inclusive range.
        yf_end = end + timedelta(days=1)
        logger.debug(
            f"Returns API Request: GET bars for {yf_ticker} "
            f"[{start.isoformat()}, {yf_end.isoformat()})"
        )
        history = yf.Ticker(yf_ticker).history(
            start=start.strftime("%Y-%m-%d"),
            end=yf_end.strftime("%Y-%m-%d"),
            auto_adjust=True,  # splits AND dividends baked in
        )
        return _compute_daily_bars_frame(history)

    def fetch_benchmark(self, start: date, end: date) -> pd.DataFrame:
        """Convenience wrapper pinned to the canonical ``SPY`` benchmark."""
        return self.fetch_daily_bars(BENCHMARK_TICKER, start, end)
