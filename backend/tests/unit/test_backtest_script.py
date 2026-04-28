"""
Smoke tests for ``backend.scripts.backtest`` orchestration + rendering.

The engine itself is tested in ``test_backtest_engine.py``; here we
verify that the variant-runner, summary aggregation, and HTML/MD
rendering compose correctly against a synthetic DataFrame.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from backend.scripts.backtest import (
    PASS_MIN_SAME_SIGN_YEARS,
    PASS_NET_SHARPE,
    VARIANT_DEFINITIONS,
    _summary_row,
    render_html,
    render_markdown,
    run_all_variants,
)


def _fixture_event_signals(n_per_class: int = 100, seed: int = 0) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    classes = ["MAJOR_EXPANSION", "CONTRACTION", "MINOR_EXPANSION", "NON_EVENT"]
    rows = []
    for cls in classes:
        for i in range(n_per_class):
            sig = rng.standard_normal()
            ret = rng.standard_normal() * 0.02
            fy = f"FY2{i % 6}"
            rows.append(
                {
                    "ticker": f"T{i:03d}",
                    "event_class": cls,
                    "ceiling_change_pct_of_mcap": sig * 0.01,
                    "relative_ceiling_change": sig,
                    "ceiling_change_log_dollars": sig * 2,
                    "industry_excess_return_20d": ret,
                    "fiscal_quarter": f"{fy}Q{(i % 4) + 1}",
                    "reference_trading_date": pd.Timestamp("2024-01-01")
                    + pd.Timedelta(days=i),
                }
            )
    return pd.DataFrame(rows)


def test_run_all_variants_executes_full_matrix():
    df = _fixture_event_signals(n_per_class=120, seed=0)
    results = run_all_variants(df, return_col="industry_excess_return_20d")
    assert len(results) == len(VARIANT_DEFINITIONS)
    names = {r["name"] for r in results}
    assert names == {v["name"] for v in VARIANT_DEFINITIONS}


def test_summary_row_pass_logic():
    """When sharpe and year-stability both clear bars, pass_overall=True."""
    fake_result = {
        "config": {
            "signal_col": "x",
            "framing": "in_class",
            "target_class": "MAJOR_EXPANSION",
        },
        "summary": {
            "n_trades": 100,
            "gross_mean": 0.02,
            "net_mean": 0.018,
            "net_stddev": 0.05,
            "sharpe": 1.0,
            "hit_rate": 0.6,
        },
        "by_year": pd.DataFrame(
            {
                "fiscal_year": ["FY21", "FY22", "FY23", "FY24", "FY25", "FY26"],
                "net_mean": [0.01, 0.02, 0.005, 0.03, 0.015, 0.025],
            }
        ),
    }
    row = _summary_row("test", fake_result)
    assert row["pass_sharpe"] is True
    assert row["pass_yr_stability"] is True
    assert row["pass_overall"] is True


def test_summary_row_failing_sharpe():
    fake_result = {
        "config": {"signal_col": "x", "framing": "in_class"},
        "summary": {
            "n_trades": 100,
            "gross_mean": 0.001,
            "net_mean": 0.0005,
            "net_stddev": 0.05,
            "sharpe": 0.1,  # below PASS_NET_SHARPE
            "hit_rate": 0.5,
        },
        "by_year": pd.DataFrame(
            {
                "fiscal_year": ["FY21", "FY22", "FY23", "FY24", "FY25", "FY26"],
                "net_mean": [0.01, 0.01, 0.01, 0.01, 0.01, 0.01],
            }
        ),
    }
    row = _summary_row("fail-sharpe", fake_result)
    assert row["pass_sharpe"] is False
    assert row["pass_yr_stability"] is True
    assert row["pass_overall"] is False


def test_render_html_smoke():
    df = _fixture_event_signals(n_per_class=120, seed=1)
    results = run_all_variants(df, return_col="industry_excess_return_20d")
    summary_rows = [_summary_row(r["name"], r["result"]) for r in results]
    html = render_html(
        summary_rows=summary_rows,
        variant_results=results,
        metadata={
            "date_stamp": "20260428",
            "generated_at": "2026-04-28 12:00:00",
            "pipeline_version": "1.5.0",
            "row_count": len(df),
            "return_col": "industry_excess_return_20d",
            "txn_cost_bps": 15.0,
            "hold_days": 20,
        },
    )
    assert "Backtest Report" in html
    assert "Headline summary" in html
    assert "Per-variant detail" in html
    assert "Decision summary" in html
    assert "Pass criteria" in html
    # Every variant name should appear somewhere in the HTML.
    for v in VARIANT_DEFINITIONS:
        assert v["name"] in html


def test_render_markdown_smoke():
    df = _fixture_event_signals(n_per_class=120, seed=2)
    results = run_all_variants(df, return_col="industry_excess_return_20d")
    summary_rows = [_summary_row(r["name"], r["result"]) for r in results]
    md = render_markdown(
        summary_rows=summary_rows,
        variant_results=results,
        metadata={
            "date_stamp": "20260428",
            "generated_at": "2026-04-28 12:00:00",
            "pipeline_version": "1.5.0",
            "row_count": len(df),
            "return_col": "industry_excess_return_20d",
            "txn_cost_bps": 15.0,
            "hold_days": 20,
        },
    )
    assert "# Backtest Report" in md
    assert "## 1. Headline summary" in md
    assert "## 2. Per-variant detail" in md
    assert "## 3. Decision summary" in md
    for v in VARIANT_DEFINITIONS:
        assert v["name"] in md
