"""
End-to-end integration test for the Milestone 2 validation harness (M2.6).

Ties the four M2 components together:

    1. Build a minimal ``signals_awards`` fixture in a temp DuckDB.
    2. Run ``validate.py::run_validation`` with a mocked ReturnsProvider.
    3. Run ``report.py::generate_report`` against the resulting table.
    4. Assert the HTML + Markdown + Parquet outputs exist and contain
       the expected sections / columns.

This test does NOT touch the network; the provider is mocked.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import duckdb
import numpy as np
import pandas as pd
import pytest

from backend.scripts.report import SIGNAL_CANDIDATES, generate_report
from backend.scripts.validate import FORWARD_HORIZONS, run_validation


def _synth_bars(
    close_start: float, days: int, step: float, start_date: str
) -> pd.DataFrame:
    idx = pd.bdate_range(start=start_date, periods=days)
    closes = [close_start * (1.0 + step) ** i for i in range(days)]
    df = pd.DataFrame({"close_adj": closes}, index=idx)
    df["return_1d"] = df["close_adj"].pct_change()
    return df


@pytest.fixture
def pipeline_env(tmp_path: Path):
    """
    Stage a minimal end-to-end environment: cleaned + cache DuckDBs with
    a 3-ticker, 6-row ``signals_awards`` table plus every column the
    report expects to find.
    """
    from backend.src.io import ensure_cache_tables, get_cleaned_conn

    cleaned_path = tmp_path / "cleaned.duckdb"
    cache_path = tmp_path / "cache.duckdb"
    parquet_path = tmp_path / "signals_with_returns.parquet"
    analysis_dir = tmp_path / "analysis"
    analysis_dir.mkdir()

    conn = get_cleaned_conn(str(cleaned_path))
    conn.execute(f"ATTACH '{cache_path}' AS cache;")
    ensure_cache_tables(conn)

    # Build a fixture signals_awards row by row so all the report's
    # expected columns are present with plausible values.
    rows = []
    tickers = ["AAPL", "MSFT", "GOOG"]
    action_dates = [
        "2024-09-03",
        "2024-09-04",
        "2024-09-05",
        "2024-09-06",
        "2024-09-09",
        "2024-09-10",
    ]
    for i, d in enumerate(action_dates):
        ticker = tickers[i % 3]
        rows.append(
            {
                "contract_transaction_unique_key": f"k{i:04d}",
                "award_id_piid": f"piid{i}",
                "ticker": ticker,
                "is_public": True,
                "market_cap": 1_000_000_000.0,
                "sector": "Industrials" if i % 2 == 0 else "Technology",
                "industry": "Aerospace",
                "action_date": pd.Timestamp(d),
                "transaction_type": "FUNDING_INCREASE",
                "is_primary_action": True,
                "signal_quality": "ok",
                "alpha_ratio": 0.0001 * (i + 1),
                "acv_alpha_ratio": 0.0001 * (i + 1),
                "contract_potential_yield": 0.00005 * (i + 1),
                "obligation_ratio": 0.01 * (i + 1),
                "moat_index": 1.0,
                "difference_between_obligated_and_potential": 1e5 * (i + 1),
            }
        )
    df = pd.DataFrame(rows)
    conn.register("fixture_signals", df)
    conn.execute(
        "CREATE OR REPLACE TABLE signals_awards AS SELECT * FROM fixture_signals"
    )
    conn.close()

    return {
        "cleaned": cleaned_path,
        "cache": cache_path,
        "parquet": parquet_path,
        "analysis": analysis_dir,
    }


def test_full_m2_harness_end_to_end(pipeline_env):
    """
    Run validate.py then report.py against the fixture. Assert all
    outputs exist and contain the expected content. Mocked provider.
    """
    # Build a mock provider returning 120 business days of synthetic bars.
    provider = MagicMock()

    def fetch_daily_bars(ticker, start, end):
        step = {
            "SPY": 0.002,
            "ITA": 0.0025,  # defense-industry benchmark
            "XLK": 0.003,   # tech-sector benchmark
            "AAPL": 0.005,
            "MSFT": 0.004,
            "GOOG": 0.003,
        }.get(ticker, 0.001)
        base = {
            "SPY": 450.0,
            "ITA": 200.0,
            "XLK": 180.0,
            "AAPL": 175.0,
            "MSFT": 420.0,
            "GOOG": 150.0,
        }.get(ticker, 100.0)
        # Span ~260 business days so the cache covers the expanded
        # LOOKFORWARD_DAYS (265 calendar) window after all six fixture
        # action dates in Sep 2024.
        return _synth_bars(base, days=260, step=step, start_date="2024-08-25")

    provider.fetch_daily_bars.side_effect = fetch_daily_bars
    # fetch_benchmark is invoked by ensure_benchmark_pre_fetched only when
    # the ticker == BENCHMARK_TICKER (SPY). Industry benchmarks (ITA, XLK)
    # route through fetch_daily_bars above with the specific ticker.
    provider.fetch_benchmark.side_effect = lambda s, e: fetch_daily_bars(
        "SPY", s, e
    )

    # --- Step 1: validate.py (returns join) ------------------------------
    n = run_validation(
        provider=provider,
        cleaned_db_path=str(pipeline_env["cleaned"]),
        cache_db_path=str(pipeline_env["cache"]),
        parquet_export_path=str(pipeline_env["parquet"]),
    )
    assert n == 6, f"expected 6 signals rows written, got {n}"
    assert pipeline_env["parquet"].exists()

    # --- Step 2: report.py against the joined table ----------------------
    conn = duckdb.connect(str(pipeline_env["cleaned"]))
    swr = conn.execute("SELECT * FROM signals_with_returns").df()
    conn.close()

    # Sanity: the expected return columns are present.
    for h in FORWARD_HORIZONS:
        assert f"return_{h}d" in swr.columns
        assert f"spy_return_{h}d" in swr.columns
        assert f"excess_return_{h}d" in swr.columns
        # M2.5: industry-neutralized columns should be present too.
        assert f"industry_benchmark_return_{h}d" in swr.columns
        assert f"industry_excess_return_{h}d" in swr.columns
    assert "reference_trading_date" in swr.columns
    assert "industry_benchmark_ticker" in swr.columns
    assert "fiscal_quarter" in swr.columns

    # Generate the report.
    out_html = pipeline_env["analysis"] / "validation_report_test.html"
    generate_report(swr, out_html, pipeline_version="integration-test-1.0")

    # --- Step 3: assertions on report artefacts --------------------------
    assert out_html.exists()
    html = out_html.read_text()
    assert "Alpha Validation Report" in html
    assert "integration-test-1.0" in html
    assert "1. Executive Summary" in html
    # M2.5: section 6 is now the industry IC breakdown; decision summary moved to 8.
    assert "6. Industry-level IC breakdown" in html
    assert "7. Per-quarter stability filter" in html
    assert "8. Decision Summary" in html
    for signal in SIGNAL_CANDIDATES:
        assert signal in html

    md = out_html.with_suffix(".md")
    assert md.exists()
    md_text = md.read_text()
    assert "Executive summary" in md_text
    assert "Decision criteria" in md_text


def test_full_m2_harness_is_idempotent(pipeline_env):
    """Second full run must not re-call the provider for returns."""
    provider = MagicMock()

    def fetch_daily_bars(ticker, start, end):
        return _synth_bars(
            450.0 if ticker == "SPY" else 100.0,
            days=260,
            step=0.001,
            start_date="2024-08-25",
        )

    provider.fetch_daily_bars.side_effect = fetch_daily_bars
    provider.fetch_benchmark.side_effect = lambda s, e: fetch_daily_bars("SPY", s, e)

    # First run populates cache.
    run_validation(
        provider=provider,
        cleaned_db_path=str(pipeline_env["cleaned"]),
        cache_db_path=str(pipeline_env["cache"]),
        parquet_export_path=None,
    )
    first_fetch_count = provider.fetch_daily_bars.call_count
    first_bench_count = provider.fetch_benchmark.call_count

    # Second run - no new network calls.
    run_validation(
        provider=provider,
        cleaned_db_path=str(pipeline_env["cleaned"]),
        cache_db_path=str(pipeline_env["cache"]),
        parquet_export_path=None,
    )
    assert provider.fetch_daily_bars.call_count == first_fetch_count
    assert provider.fetch_benchmark.call_count == first_bench_count
