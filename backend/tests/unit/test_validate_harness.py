"""
Unit tests for ``backend.scripts.validate`` orchestrator helpers (M2.3).

Covers:

* ``compute_forward_returns`` fills ``return_Nd`` columns correctly and
  leaves the tail NaN where T+N runs off the end of the fetch window.
* ``pick_trading_day_row`` rolls a weekend action_date forward to the
  next open trading day.
* End-to-end ``compute_excess_returns_for_signals``: synthetic signals
  frame + synthetic ticker and SPY bars yield the mathematically
  expected forward and excess returns.
* Integration-lite: ``run_validation`` with a mocked ``ReturnsProvider``
  and a real in-process DuckDB persists ``signals_with_returns``
  correctly and is idempotent on a second run.
"""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from unittest.mock import MagicMock

import duckdb
import numpy as np
import pandas as pd
import pytest

from backend.scripts.validate import (
    FORWARD_HORIZONS,
    compute_excess_returns_for_signals,
    compute_forward_returns,
    pick_trading_day_row,
    run_validation,
)


def _make_bars(rows: list[tuple[str, float]]) -> pd.DataFrame:
    """Build a bars DataFrame from ``(date_str, close_adj)`` tuples."""
    df = pd.DataFrame(rows, columns=["date", "close_adj"])
    df.index = pd.to_datetime(df["date"])
    df["return_1d"] = df["close_adj"].pct_change()
    return df[["close_adj", "return_1d"]]


# ---------------------------------------------------------------------------
# compute_forward_returns
# ---------------------------------------------------------------------------


def test_compute_forward_returns_math():
    """
    With close=[100, 110, 121], return_1d is 10%/10%. Forward return
    on row 0 at horizon=2 is close[2]/close[0] - 1 = 21%.
    """
    bars = _make_bars(
        [("2024-09-30", 100.0), ("2024-10-01", 110.0), ("2024-10-02", 121.0)]
    )
    out = compute_forward_returns(bars, horizons=[1, 2])

    assert out["return_1d"].iloc[0] == pytest.approx(0.10)  # preserved from input
    assert out["return_1d"].iloc[1] == pytest.approx(0.10)
    # Forward columns:
    assert out["return_1d"].name == "return_1d"  # sanity
    # The new return_Nd columns we added:
    assert out.loc[out.index[0], "return_2d"] == pytest.approx(0.21)
    # Last row has no T+1 data, so return_1d (computed fresh) is NaN.
    # Also no T+2 data.
    assert pd.isna(out["return_2d"].iloc[-1])
    assert pd.isna(out["return_2d"].iloc[-2])  # only 1 row ahead, not 2


def test_compute_forward_returns_empty_input():
    empty = pd.DataFrame(columns=["close_adj", "return_1d"])
    out = compute_forward_returns(empty, horizons=[1, 5])
    assert out.empty
    # Canonical output columns present even on empty input.
    assert "return_5d" in out.columns


# ---------------------------------------------------------------------------
# pick_trading_day_row
# ---------------------------------------------------------------------------


def test_pick_trading_day_row_exact_match():
    bars = _make_bars([("2024-09-30", 100.0), ("2024-10-01", 101.0)])
    row = pick_trading_day_row(bars, pd.Timestamp("2024-09-30"))
    assert row is not None
    assert row["close_adj"] == 100.0
    assert row["reference_trading_date"] == pd.Timestamp("2024-09-30")


def test_pick_trading_day_row_rolls_weekend_forward():
    """
    Action date on a Saturday (2024-09-28) -> pick next open trading day
    (2024-09-30, Monday).
    """
    bars = _make_bars(
        [("2024-09-27", 99.0), ("2024-09-30", 100.0), ("2024-10-01", 101.0)]
    )
    row = pick_trading_day_row(bars, pd.Timestamp("2024-09-28"))
    assert row is not None
    assert row["reference_trading_date"] == pd.Timestamp("2024-09-30")
    assert row["close_adj"] == 100.0


def test_pick_trading_day_row_past_end_returns_none():
    """Action date after the last cached bar -> None."""
    bars = _make_bars([("2024-09-30", 100.0)])
    row = pick_trading_day_row(bars, pd.Timestamp("2025-01-01"))
    assert row is None


def test_pick_trading_day_row_empty_bars():
    empty = pd.DataFrame(columns=["close_adj", "return_1d"])
    assert pick_trading_day_row(empty, pd.Timestamp("2024-09-30")) is None


# ---------------------------------------------------------------------------
# compute_excess_returns_for_signals
# ---------------------------------------------------------------------------


def test_compute_excess_returns_end_to_end():
    """
    Ticker AAPL returns 20% over 2 days, SPY returns 10% over 2 days
    -> SPY-excess return at T+2 is 10%. Row has no mapped industry
    benchmark, so industry_benchmark falls back to SPY and the
    industry-excess equals the SPY-excess.
    """
    signals = pd.DataFrame(
        {
            "contract_transaction_unique_key": ["k1"],
            "ticker": ["AAPL"],
            "action_date": [pd.Timestamp("2024-09-30")],
        }
    )
    aapl_bars = _make_bars(
        [
            ("2024-09-30", 100.0),  # t
            ("2024-10-01", 110.0),  # t+1
            ("2024-10-02", 120.0),  # t+2
        ]
    )
    spy_bars = _make_bars(
        [
            ("2024-09-30", 450.0),
            ("2024-10-01", 455.0),
            ("2024-10-02", 495.0),  # +10% from t
        ]
    )

    out = compute_excess_returns_for_signals(
        signals,
        {"AAPL": aapl_bars},
        {"SPY": spy_bars},
        horizons=[1, 2],
    )
    r = out.iloc[0]
    assert r["reference_trading_date"] == pd.Timestamp("2024-09-30")
    assert r["return_1d"] == pytest.approx(0.10, abs=1e-6)
    assert r["return_2d"] == pytest.approx(0.20, abs=1e-6)
    assert r["spy_return_1d"] == pytest.approx(5.0 / 450.0, abs=1e-6)
    assert r["spy_return_2d"] == pytest.approx(0.10, abs=1e-6)
    assert r["excess_return_2d"] == pytest.approx(0.10, abs=1e-6)
    # No 'industry' column on the signal row -> falls back to SPY benchmark.
    assert r["industry_benchmark_ticker"] == "SPY"
    assert r["industry_excess_return_2d"] == pytest.approx(0.10, abs=1e-6)
    # action_date 2024-09-30 is the last day of FY24Q4.
    assert r["fiscal_quarter"] == "FY24Q4"


def test_compute_excess_returns_missing_ticker_nans_only():
    """If a ticker has no bars, all forward columns are NaN but the row
    is preserved in the output (no silent drops)."""
    signals = pd.DataFrame(
        {
            "contract_transaction_unique_key": ["k1"],
            "ticker": ["MISSING"],
            "action_date": [pd.Timestamp("2024-09-30")],
        }
    )
    spy_bars = _make_bars([("2024-09-30", 100.0), ("2024-10-01", 101.0)])
    out = compute_excess_returns_for_signals(
        signals, {}, {"SPY": spy_bars}, horizons=[1]
    )

    assert len(out) == 1
    assert pd.isna(out["return_1d"].iloc[0])
    assert pd.isna(out["excess_return_1d"].iloc[0])
    assert pd.isna(out["industry_excess_return_1d"].iloc[0])


def test_compute_excess_returns_uses_industry_benchmark():
    """
    A row tagged as Aerospace & Defense should have its
    industry-excess computed against ITA, not SPY. Verify both columns
    coexist and differ when the benchmarks do.
    """
    signals = pd.DataFrame(
        {
            "contract_transaction_unique_key": ["lmt1"],
            "ticker": ["LMT"],
            "industry": ["Aerospace & Defense"],
            "action_date": [pd.Timestamp("2024-09-30")],
        }
    )
    # LMT rallies 5% over 2 days
    lmt_bars = _make_bars(
        [("2024-09-30", 100.0), ("2024-10-01", 102.0), ("2024-10-02", 105.0)]
    )
    # SPY flat (0% over 2d)
    spy_bars = _make_bars(
        [("2024-09-30", 450.0), ("2024-10-01", 450.0), ("2024-10-02", 450.0)]
    )
    # ITA rallies 3% over 2 days (defense has a tailwind; LMT still beats it by 2%)
    ita_bars = _make_bars(
        [("2024-09-30", 200.0), ("2024-10-01", 202.0), ("2024-10-02", 206.0)]
    )

    out = compute_excess_returns_for_signals(
        signals,
        {"LMT": lmt_bars},
        {"SPY": spy_bars, "ITA": ita_bars},
        horizons=[2],
    )
    r = out.iloc[0]
    assert r["industry_benchmark_ticker"] == "ITA"
    # SPY-excess: 5% - 0% = 5%
    assert r["excess_return_2d"] == pytest.approx(0.05, abs=1e-6)
    # Industry-excess: 5% - 3% = 2%  (sector-neutralized)
    assert r["industry_excess_return_2d"] == pytest.approx(0.02, abs=1e-6)
    assert r["industry_benchmark_return_2d"] == pytest.approx(0.03, abs=1e-6)


def test_compute_excess_returns_fiscal_quarter_tag():
    """``fiscal_quarter`` must map each action_date to the right FY24 quarter."""
    signals = pd.DataFrame(
        {
            "contract_transaction_unique_key": ["q1", "q2", "q3", "q4"],
            "ticker": ["AAPL"] * 4,
            "action_date": [
                pd.Timestamp("2023-11-15"),  # FY24 Q1 (Oct-Dec)
                pd.Timestamp("2024-02-01"),  # FY24 Q2 (Jan-Mar)
                pd.Timestamp("2024-05-20"),  # FY24 Q3 (Apr-Jun)
                pd.Timestamp("2024-08-03"),  # FY24 Q4 (Jul-Sep)
            ],
        }
    )
    aapl_bars = _make_bars(
        [
            ("2023-11-15", 100.0),
            ("2024-02-01", 105.0),
            ("2024-05-20", 110.0),
            ("2024-08-03", 115.0),
            ("2024-12-01", 120.0),
        ]
    )
    spy_bars = aapl_bars.copy()
    out = compute_excess_returns_for_signals(
        signals, {"AAPL": aapl_bars}, {"SPY": spy_bars}, horizons=[1]
    )
    fq = dict(
        zip(out["contract_transaction_unique_key"], out["fiscal_quarter"])
    )
    assert fq == {
        "q1": "FY24Q1",
        "q2": "FY24Q2",
        "q3": "FY24Q3",
        "q4": "FY24Q4",
    }


# ---------------------------------------------------------------------------
# run_validation (integration-lite)
# ---------------------------------------------------------------------------


@pytest.fixture
def staged_db(tmp_path: Path):
    """
    Build two small DuckDB files (cleaned + cache) with just enough
    structure for run_validation to operate on:

    * cleaned.duckdb has ``signals_awards`` with 2 rows for AAPL.
    * cache.duckdb has the M2 schema via ``ensure_cache_tables``.

    Yields ``(cleaned_path, cache_path)``.
    """
    from backend.src.io import ensure_cache_tables, get_cleaned_conn

    cleaned_path = tmp_path / "cleaned.duckdb"
    cache_path = tmp_path / "cache.duckdb"

    conn = get_cleaned_conn(str(cleaned_path))
    conn.execute(f"ATTACH '{cache_path}' AS cache;")
    ensure_cache_tables(conn)

    # Minimal signals_awards: 2 rows, 1 ticker, known action dates.
    conn.execute("""
        CREATE OR REPLACE TABLE signals_awards AS SELECT * FROM (VALUES
          ('k1', 'piid1', 'AAPL', TRUE, 1000000000.0, DATE '2024-09-30', 0.001),
          ('k2', 'piid2', 'AAPL', TRUE, 1000000000.0, DATE '2024-10-03', 0.002)
        ) AS t(contract_transaction_unique_key, award_id_piid, ticker, is_public,
               market_cap, action_date, alpha_ratio)
    """)
    conn.close()
    return cleaned_path, cache_path


def test_run_validation_persists_signals_with_returns(staged_db):
    cleaned_path, cache_path = staged_db

    # Mocked provider: returns two synthetic bar frames, one for AAPL
    # and one for SPY. AAPL rises 10%/day for 5 days; SPY rises 1%/day.
    provider = MagicMock()

    def _synth(close_start, days=10, step=0.01, start_date="2024-09-25"):
        idx = pd.bdate_range(start=start_date, periods=days)
        closes = [close_start * (1.0 + step) ** i for i in range(days)]
        df = pd.DataFrame({"close_adj": closes}, index=idx)
        df["return_1d"] = df["close_adj"].pct_change()
        return df

    def fetch_daily_bars(ticker, start, end):
        if ticker == "SPY":
            return _synth(450.0, days=40, step=0.01)
        if ticker == "AAPL":
            return _synth(100.0, days=40, step=0.05)
        return pd.DataFrame(columns=["close_adj", "return_1d"])

    provider.fetch_daily_bars.side_effect = fetch_daily_bars
    provider.fetch_benchmark.side_effect = lambda s, e: fetch_daily_bars("SPY", s, e)

    n = run_validation(
        provider=provider,
        cleaned_db_path=str(cleaned_path),
        cache_db_path=str(cache_path),
        parquet_export_path=str(cleaned_path.parent / "signals_with_returns.parquet"),
    )
    assert n == 2

    # Re-open and check the table contents.
    conn = duckdb.connect(str(cleaned_path))
    conn.execute(f"ATTACH '{cache_path}' AS cache;")
    df = conn.execute(
        "SELECT * FROM signals_with_returns ORDER BY contract_transaction_unique_key"
    ).df()
    conn.close()

    # Expected new columns present.
    for h in FORWARD_HORIZONS:
        assert f"return_{h}d" in df.columns
        assert f"spy_return_{h}d" in df.columns
        assert f"excess_return_{h}d" in df.columns
    assert "reference_trading_date" in df.columns

    # At least one row should have non-NaN forward returns (plenty of
    # bars were provided by the fake provider).
    assert df["return_1d"].notna().any()
    assert df["excess_return_20d"].notna().any()


def test_run_validation_is_idempotent(staged_db):
    """
    Second invocation with the same inputs must not re-call the provider.

    The signals have action_date in late Sep / early Oct, so the fetch
    window is min - 5d through max + 95d (~2024-09-25 through 2025-01-06).
    Synthetic bars must span enough business days to fully cover that
    window, otherwise the cache-coverage check triggers a re-fetch.
    """
    cleaned_path, cache_path = staged_db

    provider = MagicMock()

    def _synth(close_start, days=100, step=0.01, start_date="2024-09-25"):
        idx = pd.bdate_range(start=start_date, periods=days)
        closes = [close_start * (1.0 + step) ** i for i in range(days)]
        df = pd.DataFrame({"close_adj": closes}, index=idx)
        df["return_1d"] = df["close_adj"].pct_change()
        return df

    def fetch_daily_bars(ticker, start, end):
        return _synth(450.0 if ticker == "SPY" else 100.0)

    provider.fetch_daily_bars.side_effect = fetch_daily_bars
    provider.fetch_benchmark.side_effect = lambda s, e: fetch_daily_bars("SPY", s, e)

    # First run -> populates cache.
    run_validation(
        provider=provider,
        cleaned_db_path=str(cleaned_path),
        cache_db_path=str(cache_path),
        parquet_export_path=None,
    )
    first_call_count = provider.fetch_daily_bars.call_count
    first_benchmark_calls = provider.fetch_benchmark.call_count
    assert first_call_count >= 1  # at least AAPL was fetched

    # Second run -> everything cached, so provider should NOT be called again.
    run_validation(
        provider=provider,
        cleaned_db_path=str(cleaned_path),
        cache_db_path=str(cache_path),
        parquet_export_path=None,
    )
    assert provider.fetch_daily_bars.call_count == first_call_count, (
        "fetch_daily_bars should not be called on the second run -- "
        "cache should cover the full requested window"
    )
    assert provider.fetch_benchmark.call_count == first_benchmark_calls
