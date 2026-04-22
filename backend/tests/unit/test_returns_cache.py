"""
Unit tests for the ``cache_returns`` DuckDB table + helpers (M2.2).

Covers:

* Round-trip: upsert a DataFrame of daily bars, fetch it back, get
  identical content.
* Idempotency: upserting the same bars twice leaves the cache unchanged
  (``ON CONFLICT DO NOTHING``).
* Range queries: ``get_cached_returns`` returns only rows within the
  requested ``[start, end]``.
* Benchmark flag round-trip: ``is_benchmark = TRUE`` survives the write.
* ``ensure_benchmark_pre_fetched`` short-circuits when the cache
  already covers the requested range.
"""

from __future__ import annotations

from datetime import date, datetime
from unittest.mock import MagicMock

import duckdb
import pandas as pd
import pytest

from backend.src.io import (
    ensure_benchmark_pre_fetched,
    ensure_cache_tables,
    get_cached_returns_df,
    upsert_cached_returns,
)


@pytest.fixture
def cache_conn(tmp_path):
    """
    A fresh in-memory cache connection with the real schema applied.

    We connect to a tmp-file DB and ATTACH it as ``cache`` so the
    helpers' fully-qualified ``cache.cache_returns`` references work.
    """
    db_path = tmp_path / "test_cache.duckdb"
    # Open the 'main' cleaned-conn-style handle first...
    conn = duckdb.connect(":memory:")
    # ...and ATTACH the test cache DB under the expected alias.
    conn.execute(f"ATTACH '{db_path}' AS cache")
    # cache_entity_hierarchy etc. need to be created too (the helper
    # creates them all in one call).
    ensure_cache_tables(conn)
    yield conn
    conn.close()


def _make_bars(rows: list[tuple[str, float, float | None]]) -> pd.DataFrame:
    """Build a bars DataFrame from ``(date_str, close_adj, return_1d)`` tuples."""
    df = pd.DataFrame(rows, columns=["date", "close_adj", "return_1d"])
    df.index = pd.to_datetime(df["date"])
    return df[["close_adj", "return_1d"]]


# ---------------------------------------------------------------------------
# Round-trip + idempotency
# ---------------------------------------------------------------------------


def test_upsert_and_read_back_returns(cache_conn):
    bars = _make_bars(
        [
            ("2024-09-30", 100.0, None),
            ("2024-10-01", 101.0, 0.01),
            ("2024-10-02", 102.01, 0.01),
        ]
    )
    n = upsert_cached_returns(cache_conn, "AAPL", bars)
    assert n == 3

    fetched = get_cached_returns_df(
        cache_conn, "AAPL", date(2024, 9, 30), date(2024, 10, 2)
    )
    assert len(fetched) == 3
    assert fetched["close_adj"].tolist() == [100.0, 101.0, 102.01]
    # return_1d roundtrip: the None on row 0 survives as NaN.
    assert pd.isna(fetched["return_1d"].iloc[0])
    assert fetched["return_1d"].iloc[1] == pytest.approx(0.01)


def test_upsert_is_idempotent_on_conflict(cache_conn):
    """
    Historical prices are immutable. A second upsert of the same
    ``(ticker, date)`` rows must be a no-op (0 new rows inserted).
    """
    bars = _make_bars([("2024-10-01", 101.0, 0.01)])
    first = upsert_cached_returns(cache_conn, "AAPL", bars)
    second = upsert_cached_returns(cache_conn, "AAPL", bars)
    assert first == 1
    assert second == 0

    # Cache still has exactly one row for that key.
    rows = cache_conn.execute(
        "SELECT COUNT(*) FROM cache.cache_returns WHERE ticker = 'AAPL'"
    ).fetchone()[0]
    assert rows == 1


def test_upsert_empty_frame_is_noop(cache_conn):
    """Empty DataFrame shouldn't error; should simply insert nothing."""
    empty = pd.DataFrame(columns=["close_adj", "return_1d"])
    n = upsert_cached_returns(cache_conn, "DELISTED", empty)
    assert n == 0


# ---------------------------------------------------------------------------
# Range queries
# ---------------------------------------------------------------------------


def test_get_cached_returns_respects_date_range(cache_conn):
    bars = _make_bars(
        [
            ("2024-09-30", 100.0, None),
            ("2024-10-01", 101.0, 0.01),
            ("2024-10-02", 102.01, 0.01),
            ("2024-10-03", 103.03, 0.01),
        ]
    )
    upsert_cached_returns(cache_conn, "MSFT", bars)

    # Inclusive range: Oct 1 -- Oct 2 should return 2 rows.
    mid = get_cached_returns_df(
        cache_conn, "MSFT", date(2024, 10, 1), date(2024, 10, 2)
    )
    assert len(mid) == 2
    assert mid["close_adj"].tolist() == [101.0, 102.01]


def test_get_cached_returns_miss_returns_empty_canonical(cache_conn):
    out = get_cached_returns_df(
        cache_conn, "NOCACHE", date(2024, 1, 1), date(2024, 1, 5)
    )
    assert out.empty
    assert list(out.columns) == ["close_adj", "return_1d"]


# ---------------------------------------------------------------------------
# Benchmark flag round-trip
# ---------------------------------------------------------------------------


def test_benchmark_flag_roundtrip(cache_conn):
    bars = _make_bars([("2024-10-01", 450.0, 0.001)])
    upsert_cached_returns(cache_conn, "SPY", bars, is_benchmark=True)
    row = cache_conn.execute(
        "SELECT ticker, is_benchmark, source FROM cache.cache_returns WHERE ticker = 'SPY'"
    ).fetchone()
    assert row is not None
    assert row[0] == "SPY"
    assert row[1] is True
    assert row[2] == "yfinance"


# ---------------------------------------------------------------------------
# ensure_benchmark_pre_fetched
# ---------------------------------------------------------------------------


def test_ensure_benchmark_fetches_when_cache_empty(cache_conn):
    """First-time benchmark fetch hits the provider."""
    provider = MagicMock()
    provider.fetch_benchmark.return_value = _make_bars(
        [("2024-10-01", 450.0, None), ("2024-10-02", 451.0, 0.00222)]
    )

    n = ensure_benchmark_pre_fetched(
        cache_conn, provider, date(2024, 10, 1), date(2024, 10, 2)
    )
    assert n == 2
    provider.fetch_benchmark.assert_called_once_with(
        date(2024, 10, 1), date(2024, 10, 2)
    )


def test_ensure_benchmark_short_circuits_when_covered(cache_conn):
    """
    If the cache already covers the requested range, no fetch is made.
    Critical: re-running the validation harness must not re-hit the API
    for SPY on every execution.
    """
    bars = _make_bars(
        [
            ("2024-09-30", 449.0, None),
            ("2024-10-01", 450.0, 0.00223),
            ("2024-10-02", 451.0, 0.00222),
            ("2024-10-03", 452.0, 0.00222),
        ]
    )
    upsert_cached_returns(cache_conn, "SPY", bars, is_benchmark=True)

    provider = MagicMock()
    # Asking for a sub-range -- cache already covers it.
    n = ensure_benchmark_pre_fetched(
        cache_conn, provider, date(2024, 10, 1), date(2024, 10, 2)
    )
    assert n == 0
    provider.fetch_benchmark.assert_not_called()


def test_ensure_benchmark_refetches_when_range_extends_beyond_cache(cache_conn):
    """Cache covers Oct 1-Oct 2, caller wants Oct 1-Oct 10 -> refetch."""
    bars = _make_bars(
        [
            ("2024-10-01", 450.0, None),
            ("2024-10-02", 451.0, 0.00222),
        ]
    )
    upsert_cached_returns(cache_conn, "SPY", bars, is_benchmark=True)

    provider = MagicMock()
    provider.fetch_benchmark.return_value = _make_bars(
        [
            ("2024-10-01", 450.0, None),  # overlaps (DO NOTHING)
            ("2024-10-02", 451.0, 0.00222),  # overlaps
            ("2024-10-10", 460.0, 0.01),  # new
        ]
    )

    n = ensure_benchmark_pre_fetched(
        cache_conn, provider, date(2024, 10, 1), date(2024, 10, 10)
    )
    # Only the new row (10/10) is inserted thanks to ON CONFLICT DO NOTHING.
    assert n == 1
    provider.fetch_benchmark.assert_called_once()
