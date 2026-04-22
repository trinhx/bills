"""
Unit tests for the point-in-time Yahoo market-cap provider (M1.1).

Tests exercise the four ``market_cap_quality`` branches: ``ok``,
``stale_shares``, ``no_close``, ``no_shares``. yfinance network calls are
mocked so the tests run offline.
"""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from backend.app.services.providers import yahoo as yahoo_mod


@pytest.fixture(autouse=True)
def bypass_rate_limiter():
    """Don't actually sleep during tests."""
    with patch.object(yahoo_mod.YAHOO_RATE_LIMITER, "wait", lambda: None):
        yield


def _make_ticker(
    *,
    history_df: pd.DataFrame | None = None,
    shares_series: pd.Series | None = None,
    info: dict | None = None,
) -> MagicMock:
    """Build a ``yf.Ticker``-like mock."""
    mock = MagicMock()
    mock.history.return_value = history_df if history_df is not None else pd.DataFrame()
    mock.get_shares_full.return_value = shares_series
    mock.info = info or {}
    return mock


def test_market_cap_ok_path():
    """Historical close AND historical shares both present -> quality='ok'."""
    hist = pd.DataFrame(
        {"Close": [100.0, 101.0, 102.0]},
        index=pd.DatetimeIndex(["2024-01-02", "2024-01-03", "2024-01-04"]),
    )
    shares = pd.Series(
        [1_000_000, 1_100_000],
        index=pd.DatetimeIndex(["2023-10-01", "2024-01-01"]),
    )
    mock = _make_ticker(
        history_df=hist,
        shares_series=shares,
        info={"sector": "Tech", "industry": "Software"},
    )

    with patch.object(yahoo_mod.yf, "Ticker", return_value=mock):
        res = yahoo_mod.fetch_yahoo_data("AAPL", "2024-01-05")

    assert res["market_cap_quality"] == "ok"
    # closest prior close is 2024-01-04 -> 102.0; closest prior shares
    # is 2024-01-01 -> 1,100,000. Market cap = 102 * 1.1M.
    assert res["close_price"] == 102.0
    assert res["shares_outstanding"] == 1_100_000
    assert res["market_cap"] == 102.0 * 1_100_000
    assert res["sector"] == "Tech"
    assert res["industry"] == "Software"


def test_market_cap_stale_shares_fallback():
    """No historical shares -> fall back to info['sharesOutstanding']."""
    hist = pd.DataFrame(
        {"Close": [50.0]},
        index=pd.DatetimeIndex(["2024-01-04"]),
    )
    mock = _make_ticker(
        history_df=hist,
        shares_series=pd.Series(dtype=float),  # empty
        info={
            "sharesOutstanding": 2_000_000,
            "sector": "Energy",
            "industry": "Oil",
        },
    )

    with patch.object(yahoo_mod.yf, "Ticker", return_value=mock):
        res = yahoo_mod.fetch_yahoo_data("XOM", "2024-01-05")

    assert res["market_cap_quality"] == "stale_shares"
    assert res["close_price"] == 50.0
    assert res["shares_outstanding"] == 2_000_000
    assert res["market_cap"] == 50.0 * 2_000_000


def test_market_cap_no_close():
    """No history data -> market_cap NULL, quality='no_close'."""
    mock = _make_ticker(
        history_df=pd.DataFrame(),  # empty
        shares_series=pd.Series([1_000_000], index=pd.DatetimeIndex(["2024-01-01"])),
        info={"sector": "Utilities"},
    )

    with patch.object(yahoo_mod.yf, "Ticker", return_value=mock):
        res = yahoo_mod.fetch_yahoo_data("DELISTED", "2024-01-05")

    assert res["market_cap_quality"] == "no_close"
    assert res["market_cap"] is None
    assert res["close_price"] is None
    assert res["shares_outstanding"] is None
    assert res["sector"] == "Utilities"


def test_market_cap_no_shares_anywhere():
    """Close present but neither historical nor current shares -> 'no_shares'."""
    hist = pd.DataFrame(
        {"Close": [75.0]},
        index=pd.DatetimeIndex(["2024-01-04"]),
    )
    mock = _make_ticker(
        history_df=hist,
        shares_series=pd.Series(dtype=float),
        info={},  # no sharesOutstanding fallback
    )

    with patch.object(yahoo_mod.yf, "Ticker", return_value=mock):
        res = yahoo_mod.fetch_yahoo_data("NOCAP", "2024-01-05")

    assert res["market_cap_quality"] == "no_shares"
    assert res["market_cap"] is None
    # close_price should still be preserved for provenance.
    assert res["close_price"] == 75.0


def test_shares_future_dates_are_ignored():
    """Shares reported AFTER action_date must not be used (prevents look-ahead)."""
    hist = pd.DataFrame(
        {"Close": [100.0]},
        index=pd.DatetimeIndex(["2024-01-04"]),
    )
    shares = pd.Series(
        [1_000_000, 5_000_000],  # big jump AFTER action_date
        index=pd.DatetimeIndex(["2023-06-01", "2024-06-01"]),
    )
    mock = _make_ticker(
        history_df=hist,
        shares_series=shares,
        info={"sharesOutstanding": 5_000_000},
    )

    with patch.object(yahoo_mod.yf, "Ticker", return_value=mock):
        res = yahoo_mod.fetch_yahoo_data("AAPL", "2024-01-05")

    # Must pick the 2023-06-01 value (prior to action_date), NOT the
    # 2024-06-01 value (future).
    assert res["market_cap_quality"] == "ok"
    assert res["shares_outstanding"] == 1_000_000
    assert res["market_cap"] == 100.0 * 1_000_000


def test_ticker_format_normalized_for_yahoo():
    """
    M1.5 P1-4 regression guard.

    OpenFIGI returns multi-class tickers as ``BRK/A``; yfinance expects
    ``BRK-A`` (slashes 500 because they're interpreted as URL paths).
    ``fetch_yahoo_data`` must translate the separator before constructing
    the ``yf.Ticker`` object.
    """
    from backend.app.services.providers.yahoo import _normalize_ticker_for_yahoo

    # Pure-function unit check
    assert _normalize_ticker_for_yahoo("BRK/A") == "BRK-A"
    assert _normalize_ticker_for_yahoo("MOG/A") == "MOG-A"
    assert _normalize_ticker_for_yahoo("AAPL") == "AAPL"
    assert _normalize_ticker_for_yahoo("") == ""
    assert _normalize_ticker_for_yahoo(None) is None

    # Integration: yf.Ticker must be called with the normalised form.
    hist = pd.DataFrame(
        {"Close": [500.0]},
        index=pd.DatetimeIndex(["2024-01-04"]),
    )
    mock = _make_ticker(
        history_df=hist,
        shares_series=pd.Series([1_000_000], index=pd.DatetimeIndex(["2023-10-01"])),
        info={"sector": "Financial", "industry": "Insurance"},
    )
    with patch.object(yahoo_mod.yf, "Ticker", return_value=mock) as patched:
        res = yahoo_mod.fetch_yahoo_data("BRK/A", "2024-01-05")

    # The call site must have used the hyphen form.
    patched.assert_called_once_with("BRK-A")
    assert res["market_cap_quality"] == "ok"


def test_tz_aware_history_handled():
    """Yahoo sometimes returns tz-aware DatetimeIndex; provider must cope."""
    hist = pd.DataFrame(
        {"Close": [200.0]},
        index=pd.DatetimeIndex(["2024-01-04"], tz="America/New_York"),
    )
    shares = pd.Series([1_000_000], index=pd.DatetimeIndex(["2023-10-01"]))
    mock = _make_ticker(
        history_df=hist,
        shares_series=shares,
        info={},
    )

    with patch.object(yahoo_mod.yf, "Ticker", return_value=mock):
        res = yahoo_mod.fetch_yahoo_data("AAPL", "2024-01-05")

    assert res["market_cap_quality"] == "ok"
    assert res["close_price"] == 200.0
