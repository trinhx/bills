"""
Unit tests for ``backend.app.services.providers.returns`` (M2.1).

Covers:

* Happy path: bars from yfinance are re-shaped into the canonical
  ``close_adj`` / ``return_1d`` schema.
* Empty response (delisted ticker / no history): returns an empty
  DataFrame with the canonical columns.
* tz-aware DatetimeIndex handled without raising.
* Split-adjustment preserved: ``auto_adjust=True`` is passed to yfinance.
* ``_normalize_ticker_for_yahoo`` is called at the network boundary
  (``BRK/A`` routed to ``BRK-A``).
* ``fetch_benchmark`` delegates to ``fetch_daily_bars('SPY', ...)``.

No real network calls; yfinance is mocked.
"""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from backend.app.services.providers import returns as returns_mod
from backend.app.services.providers.returns import (
    BENCHMARK_TICKER,
    YFinanceReturnsProvider,
    _compute_daily_bars_frame,
)


@pytest.fixture(autouse=True)
def bypass_rate_limiter():
    """Don't actually sleep on the token bucket during unit tests."""
    with patch.object(returns_mod.RETURNS_RATE_LIMITER, "wait", lambda: None):
        yield


# ---------------------------------------------------------------------------
# _compute_daily_bars_frame (pure-function unit tests)
# ---------------------------------------------------------------------------


def test_compute_bars_happy_path():
    hist = pd.DataFrame(
        {"Close": [100.0, 101.0, 99.0, 105.0]},
        index=pd.DatetimeIndex(
            ["2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05"]
        ),
    )
    out = _compute_daily_bars_frame(hist)

    assert list(out.columns) == ["close_adj", "return_1d"]
    assert out["close_adj"].tolist() == [100.0, 101.0, 99.0, 105.0]
    # First row: no prior close -> NaN
    assert pd.isna(out["return_1d"].iloc[0])
    # Subsequent: strict prior-close pct-change
    assert out["return_1d"].iloc[1] == pytest.approx(0.01)
    assert out["return_1d"].iloc[2] == pytest.approx(-0.0198, abs=1e-4)
    assert out["return_1d"].iloc[3] == pytest.approx(0.0606, abs=1e-4)


def test_compute_bars_empty_input_returns_canonical_empty():
    out = _compute_daily_bars_frame(pd.DataFrame())
    assert out.empty
    assert list(out.columns) == ["close_adj", "return_1d"]


def test_compute_bars_handles_tz_aware_index():
    """Yahoo sometimes returns a tz-aware DatetimeIndex; we strip tz."""
    hist = pd.DataFrame(
        {"Close": [500.0, 510.0]},
        index=pd.DatetimeIndex(["2024-01-02", "2024-01-03"], tz="America/New_York"),
    )
    out = _compute_daily_bars_frame(hist)
    # No tz on the output index (downstream joins use DATE, not TIMESTAMPTZ).
    assert out.index.tz is None
    assert out["close_adj"].tolist() == [500.0, 510.0]


# ---------------------------------------------------------------------------
# YFinanceReturnsProvider
# ---------------------------------------------------------------------------


def _mock_ticker(history_df: pd.DataFrame) -> MagicMock:
    mock = MagicMock()
    mock.history.return_value = history_df
    return mock


def test_fetch_daily_bars_passes_auto_adjust_true():
    """auto_adjust=True is the whole reason look-ahead-bias is avoided."""
    hist = pd.DataFrame(
        {"Close": [100.0, 102.0]},
        index=pd.DatetimeIndex(["2024-01-02", "2024-01-03"]),
    )
    mock_ticker = _mock_ticker(hist)
    with patch.object(returns_mod.yf, "Ticker", return_value=mock_ticker) as patched:
        provider = YFinanceReturnsProvider()
        provider.fetch_daily_bars("AAPL", date(2024, 1, 1), date(2024, 1, 5))

    patched.assert_called_once_with("AAPL")
    kwargs = mock_ticker.history.call_args.kwargs
    assert kwargs["auto_adjust"] is True, (
        "auto_adjust must be True -- split/dividend adjustment is the point"
    )
    # end is exclusive in yfinance; we push it one day past the caller's inclusive end.
    assert kwargs["start"] == "2024-01-01"
    assert kwargs["end"] == "2024-01-06"


def test_fetch_daily_bars_normalizes_slash_ticker():
    """
    OpenFIGI returns BRK/A; yfinance wants BRK-A. The provider must
    translate at the network boundary (reuses M1.5 helper).
    """
    hist = pd.DataFrame({"Close": [500.0]}, index=pd.DatetimeIndex(["2024-01-02"]))
    with patch.object(
        returns_mod.yf, "Ticker", return_value=_mock_ticker(hist)
    ) as patched:
        provider = YFinanceReturnsProvider()
        provider.fetch_daily_bars("BRK/A", date(2024, 1, 1), date(2024, 1, 5))

    patched.assert_called_once_with("BRK-A")


def test_fetch_daily_bars_empty_response_returns_empty_frame():
    """Delisted / invalid tickers produce an empty frame, not an exception."""
    with patch.object(
        returns_mod.yf, "Ticker", return_value=_mock_ticker(pd.DataFrame())
    ):
        provider = YFinanceReturnsProvider()
        out = provider.fetch_daily_bars("DELISTED", date(2024, 1, 1), date(2024, 1, 5))

    assert out.empty
    assert list(out.columns) == ["close_adj", "return_1d"]


def test_fetch_benchmark_uses_spy():
    """``fetch_benchmark`` is a thin wrapper pinned to SPY."""
    hist = pd.DataFrame(
        {"Close": [450.0, 451.0]},
        index=pd.DatetimeIndex(["2024-01-02", "2024-01-03"]),
    )
    with patch.object(
        returns_mod.yf, "Ticker", return_value=_mock_ticker(hist)
    ) as patched:
        provider = YFinanceReturnsProvider()
        out = provider.fetch_benchmark(date(2024, 1, 1), date(2024, 1, 5))

    patched.assert_called_once_with(BENCHMARK_TICKER)
    assert out["close_adj"].tolist() == [450.0, 451.0]


def test_provider_satisfies_protocol():
    """Runtime check that YFinanceReturnsProvider implements the Protocol."""
    from backend.app.services.providers.returns import ReturnsProvider

    provider = YFinanceReturnsProvider()
    assert isinstance(provider, ReturnsProvider)
