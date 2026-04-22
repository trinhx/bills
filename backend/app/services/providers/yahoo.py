"""
Yahoo Finance enrichment provider.

Computes a **point-in-time** market capitalization for a given ticker on a
given ``action_date`` by combining:

1. The historical close price on (or closest trading day prior to) ``action_date``.
2. The historical ``sharesOutstanding`` value reported for the nearest date
   prior to ``action_date`` via ``yf.Ticker.get_shares_full(start, end)``.

The previous implementation multiplied a historical close by *today's* share
count, which introduced look-ahead bias (splits, buybacks, and secondary
offerings between the contract date and today would distort the historical
market cap). This module fixes that and records the provenance
(``market_cap_quality``) so downstream analysis can filter on data quality.

``market_cap_quality`` values:

* ``ok``           -- both historical close and historical shares found.
* ``stale_shares`` -- historical close found; fell back to the most recent
                      ``sharesOutstanding`` from ``Ticker.info`` because
                      ``get_shares_full`` returned no data (common for ADRs,
                      foreign listings, or very new IPOs).
* ``no_close``     -- no historical price was available; market cap is NULL.
* ``no_shares``    -- no share count available at all; market cap is NULL.
"""

import json
import logging
from datetime import timedelta
from typing import Any, Dict, Optional

import pandas as pd
import requests
import yfinance as yf

from backend.app.services.providers.base import RateLimiter, with_retry


def _normalize_ticker_for_yahoo(ticker: str) -> str:
    """
    Convert share-class separators to Yahoo Finance's convention.

    OpenFIGI (and many enterprise feeds) report multi-class tickers with
    a forward slash: ``BRK/A``, ``MOG/A``. Yahoo Finance exposes them
    with a hyphen: ``BRK-A``, ``MOG-A``. If we don't translate, every
    ``yf.Ticker('BRK/A')`` call 500s because yfinance treats ``/`` as a
    URL path separator against query2.finance.yahoo.com.

    Cache keys and logging continue to use the original (OpenFIGI-native)
    form so provenance is preserved; normalisation happens strictly at
    the network boundary.
    """
    if not ticker:
        return ticker
    return ticker.replace("/", "-")


logger = logging.getLogger(__name__)

# Yahoo Finance is throttled/unstable, so we implement a conservative rate limit.
YAHOO_RATE_LIMITER = RateLimiter(max_requests=10, time_window=10.0)

# yfinance wraps requests internally and can raise a variety of non-requests
# exceptions (malformed JSON, missing keys on partial responses, etc.). We
# include them explicitly so transient library-internal failures still retry.
_YAHOO_RETRY_EXCEPTIONS = (
    requests.exceptions.RequestException,
    json.JSONDecodeError,
    ValueError,
    KeyError,
)

# Lookback window for history queries. 10 trading days is enough to find a
# prior close even across long weekends / holidays.
_PRICE_LOOKBACK_DAYS = 10

# Lookback window when fetching historical shares_outstanding. Companies
# typically report share counts quarterly, so 180 days is safe.
_SHARES_LOOKBACK_DAYS = 180


def _get_historical_close(
    ticker_obj: yf.Ticker, target_dt: pd.Timestamp
) -> Optional[float]:
    """Return the close price on the closest trading day <= ``target_dt``."""
    start_dt = target_dt - timedelta(days=_PRICE_LOOKBACK_DAYS)
    end_dt = target_dt + timedelta(days=1)
    hist = ticker_obj.history(
        start=start_dt.strftime("%Y-%m-%d"),
        end=end_dt.strftime("%Y-%m-%d"),
        auto_adjust=False,
    )
    if hist.empty:
        return None
    # Normalise index tz so the comparison works regardless of Yahoo's tz quirks.
    hist.index = (
        hist.index.tz_localize(None) if hist.index.tz is not None else hist.index
    )
    valid = hist[hist.index <= target_dt]
    if valid.empty:
        return None
    return float(valid.iloc[-1]["Close"])


def _get_historical_shares(
    ticker_obj: yf.Ticker, target_dt: pd.Timestamp
) -> Optional[float]:
    """
    Return the last reported ``shares_outstanding`` on or before ``target_dt``.

    ``yf.Ticker.get_shares_full`` returns a ``Series`` indexed by report date,
    with share-count values. We pick the most recent value whose index is
    at or before ``target_dt``. Returns ``None`` if no qualifying data exists
    (caller should fall back to today's ``sharesOutstanding`` and flag the
    row as ``stale_shares``).
    """
    start_dt = target_dt - timedelta(days=_SHARES_LOOKBACK_DAYS)
    end_dt = target_dt + timedelta(days=1)
    try:
        series = ticker_obj.get_shares_full(
            start=start_dt.strftime("%Y-%m-%d"),
            end=end_dt.strftime("%Y-%m-%d"),
        )
    except Exception as e:
        logger.debug(f"get_shares_full failed: {e}")
        return None

    if series is None or len(series) == 0:
        return None

    # Normalise tz on the index for safe comparison.
    if isinstance(series, pd.Series):
        idx = series.index
        if hasattr(idx, "tz") and idx.tz is not None:
            series = series.copy()
            series.index = idx.tz_localize(None)
        qualifying = series[series.index <= target_dt]
        if qualifying.empty:
            return None
        value = qualifying.iloc[-1]
    else:
        # Defensive: if it's a DataFrame for some reason, pick its first col.
        df = series
        if hasattr(df.index, "tz") and df.index.tz is not None:
            df = df.copy()
            df.index = df.index.tz_localize(None)
        qualifying = df[df.index <= target_dt]
        if qualifying.empty:
            return None
        value = qualifying.iloc[-1, 0]

    try:
        val = float(value)
    except (TypeError, ValueError):
        return None
    # Share counts should be positive; treat 0 / NaN as missing.
    if val <= 0 or val != val:  # NaN check
        return None
    return val


@with_retry(max_attempts=3, base_delay=2.0, retry_exceptions=_YAHOO_RETRY_EXCEPTIONS)
def fetch_yahoo_data(ticker: str, action_date: str) -> Optional[Dict[str, Any]]:
    """
    Fetch a point-in-time market capitalization snapshot for ``ticker`` as of
    ``action_date``.

    Returns a dict with keys:

    * ``market_cap``         -- ``close_price * shares_outstanding`` or ``None``
    * ``close_price``        -- historical close on or before ``action_date``
    * ``shares_outstanding`` -- historical (or fallback current) share count
    * ``market_cap_quality`` -- ``ok``/``stale_shares``/``no_close``/``no_shares``
    * ``sector``             -- from ``Ticker.info`` (time-invariant)
    * ``industry``           -- from ``Ticker.info``
    """
    YAHOO_RATE_LIMITER.wait()

    # M1.5 P1-4: OpenFIGI reports multi-class tickers with '/' (BRK/A);
    # Yahoo expects '-' (BRK-A). Normalise before calling yfinance so
    # these rows stop 500-ing, but retain the original in the cache key.
    yf_ticker = _normalize_ticker_for_yahoo(ticker)
    t = yf.Ticker(yf_ticker)

    # Sector / industry are effectively time-invariant descriptors; pulling
    # the current value is acceptable. ``info`` is cached by yfinance after
    # the first access so the extra call is cheap.
    try:
        info = t.info or {}
    except Exception as e:
        logger.debug(f"Ticker.info failed for {ticker}: {e}")
        info = {}
    sector = info.get("sector") or "UNKNOWN"
    industry = info.get("industry") or "UNKNOWN"

    target_dt = pd.to_datetime(action_date)
    if target_dt.tzinfo is not None:
        target_dt = target_dt.tz_localize(None)

    close_price = _get_historical_close(t, target_dt)
    if close_price is None:
        logger.warning(f"No historical close for {ticker} near {action_date}")
        return {
            "market_cap": None,
            "close_price": None,
            "shares_outstanding": None,
            "market_cap_quality": "no_close",
            "sector": sector,
            "industry": industry,
        }

    shares = _get_historical_shares(t, target_dt)
    quality = "ok"
    if shares is None:
        # Fall back to the most recent (i.e., "current") share count. This
        # reintroduces the old look-ahead bias for this single row, but
        # flagging lets researchers filter out these rows at analysis time.
        shares = info.get("sharesOutstanding")
        try:
            shares = float(shares) if shares else None
        except (TypeError, ValueError):
            shares = None
        if shares is None or shares <= 0:
            logger.warning(f"No shares_outstanding available for {ticker}")
            return {
                "market_cap": None,
                "close_price": close_price,
                "shares_outstanding": None,
                "market_cap_quality": "no_shares",
                "sector": sector,
                "industry": industry,
            }
        quality = "stale_shares"
        logger.debug(
            f"Using stale (current) shares_outstanding for {ticker} @ {action_date}"
        )

    market_cap = close_price * shares
    result = {
        "market_cap": float(market_cap),
        "close_price": float(close_price),
        "shares_outstanding": float(shares),
        "market_cap_quality": quality,
        "sector": sector,
        "industry": industry,
    }
    logger.debug(f"Yahoo fields for {ticker} @ {action_date}: {result}")
    return result
