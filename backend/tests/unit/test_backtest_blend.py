"""
Smoke tests for ``backend.scripts.backtest_blend`` orchestration.

The composite math is unit-tested in ``test_composite_signals.py``;
the engine itself in ``test_backtest_engine.py``. Here we verify the
script wires them together and renders correctly.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from backend.scripts.backtest_blend import (
    COMPOSITE_COL,
    HOLDING_HORIZONS_DAYS,
    PASS_MIN_SAME_SIGN_YEARS,
    PASS_NET_SHARPE,
    _build_variant_definitions,
    _summary_row,
    attach_composite,
    render_html,
    render_markdown,
    run_all_variants,
)


def _fixture_blend_signals(n_per_class: int = 200, seed: int = 0) -> pd.DataFrame:
    """
    Build a synthetic frame with both component signals plus the
    columns the engine needs (event_class, fiscal_quarter, ticker,
    reference_trading_date, returns at T+20 and T+120).
    """
    rng = np.random.default_rng(seed)
    classes = ["MAJOR_EXPANSION", "CONTRACTION", "MINOR_EXPANSION", "NON_EVENT"]
    rows = []
    for cls in classes:
        for i in range(n_per_class):
            sig = rng.standard_normal()
            moat = rng.uniform(0, 1)
            ret_20 = rng.standard_normal() * 0.02
            ret_120 = rng.standard_normal() * 0.04
            rows.append(
                {
                    "ticker": f"T{i:03d}",
                    "event_class": cls,
                    "ceiling_change_pct_of_mcap": sig * 0.01,
                    "moat_index": moat,
                    "industry_excess_return_20d": ret_20,
                    "industry_excess_return_120d": ret_120,
                    "fiscal_quarter": f"FY2{i % 6}Q{(i % 4) + 1}",
                    "reference_trading_date": pd.Timestamp("2024-01-01")
                    + pd.Timedelta(days=i),
                }
            )
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Variant matrix
# ---------------------------------------------------------------------------


def test_variant_definitions_shape():
    variants = _build_variant_definitions()
    # 3 signals (composite, pct_of_mcap, moat_index) x 2 horizons = 6 variants.
    assert len(variants) == 3 * len(HOLDING_HORIZONS_DAYS)
    names = [v["name"] for v in variants]
    assert "composite-T20" in names
    assert "composite-T120" in names
    assert "pct_of_mcap-T20" in names
    assert "moat_index-T120" in names


def test_variant_definitions_have_unique_names():
    variants = _build_variant_definitions()
    names = [v["name"] for v in variants]
    assert len(names) == len(set(names))


# ---------------------------------------------------------------------------
# attach_composite
# ---------------------------------------------------------------------------


def test_attach_composite_only_within_target_class():
    df = _fixture_blend_signals(n_per_class=50)
    out = attach_composite(df, target_class="MAJOR_EXPANSION")
    assert COMPOSITE_COL in out.columns
    # Rows in MAJ should have a non-NaN composite.
    maj = out[out["event_class"] == "MAJOR_EXPANSION"]
    non_maj = out[out["event_class"] != "MAJOR_EXPANSION"]
    assert maj[COMPOSITE_COL].notna().all()
    # Rows outside MAJ should be NaN.
    assert non_maj[COMPOSITE_COL].isna().all()


def test_attach_composite_handles_empty_class():
    df = _fixture_blend_signals(n_per_class=50)
    out = attach_composite(df, target_class="NONEXISTENT")
    # All rows NaN; column still exists.
    assert COMPOSITE_COL in out.columns
    assert out[COMPOSITE_COL].isna().all()


def test_attach_composite_does_not_mutate_input():
    df = _fixture_blend_signals(n_per_class=20)
    cols_before = set(df.columns)
    _ = attach_composite(df)
    assert set(df.columns) == cols_before


def test_attach_composite_in_zero_one_range():
    df = _fixture_blend_signals(n_per_class=100)
    out = attach_composite(df)
    composite = out[out["event_class"] == "MAJOR_EXPANSION"][COMPOSITE_COL]
    assert composite.min() > 0
    assert composite.max() < 1


# ---------------------------------------------------------------------------
# run_all_variants smoke
# ---------------------------------------------------------------------------


def test_run_all_variants_executes_full_matrix():
    df = _fixture_blend_signals(n_per_class=200, seed=1)
    df = attach_composite(df)
    results = run_all_variants(df)
    assert len(results) == 6  # 3 signals x 2 horizons
    for r in results:
        assert "result" in r
        # All variants run on MAJOR_EXPANSION = 200 rows; with deciles
        # on, n_long + n_short = 2 * 200/10 * 2 (long+short) = 40.
        assert r["result"]["summary"]["n_trades"] > 0


# ---------------------------------------------------------------------------
# Summary row + pass logic
# ---------------------------------------------------------------------------


def test_summary_row_passing_case():
    fake_variant = {
        "name": "composite-T20",
        "kind": "composite",
        "hold_days": 20,
        "result": {
            "summary": {
                "n_trades": 1000,
                "gross_mean": 0.02,
                "net_mean": 0.018,
                "net_stddev": 0.05,
                "sharpe": 0.8,
                "hit_rate": 0.6,
            },
            "by_year": pd.DataFrame(
                {
                    "fiscal_year": ["FY21", "FY22", "FY23", "FY24", "FY25", "FY26"],
                    "net_mean": [0.01, 0.02, 0.005, 0.03, 0.015, 0.025],
                }
            ),
        },
    }
    row = _summary_row(fake_variant)
    assert row["pass_sharpe"] is True
    assert row["pass_yr_stability"] is True
    assert row["pass_overall"] is True
    assert row["kind"] == "composite"


def test_summary_row_failing_case():
    fake_variant = {
        "name": "composite-T20",
        "kind": "composite",
        "hold_days": 20,
        "result": {
            "summary": {
                "n_trades": 1000,
                "gross_mean": 0.005,
                "net_mean": 0.001,
                "net_stddev": 0.05,
                "sharpe": 0.1,
                "hit_rate": 0.5,
            },
            "by_year": pd.DataFrame(
                {
                    "fiscal_year": ["FY21", "FY22", "FY23", "FY24", "FY25"],
                    "net_mean": [0.01, -0.02, 0.005, 0.03, -0.015],
                }
            ),
        },
    }
    row = _summary_row(fake_variant)
    assert row["pass_sharpe"] is False
    # 3+/2- so max(positive, negative) = 3 < 5 -> fails year stability.
    assert row["pass_yr_stability"] is False
    assert row["pass_overall"] is False


# ---------------------------------------------------------------------------
# Rendering smoke
# ---------------------------------------------------------------------------


def test_render_html_smoke():
    df = _fixture_blend_signals(n_per_class=200, seed=2)
    df = attach_composite(df)
    results = run_all_variants(df)
    summary_rows = [_summary_row(v) for v in results]
    correlation_df = pd.DataFrame(
        {"a": [1.0, -0.4], "b": [-0.4, 1.0]},
        index=["a", "b"],
    )
    html = render_html(
        summary_rows=summary_rows,
        variant_results=results,
        correlation_df=correlation_df,
        metadata={
            "date_stamp": "20260428",
            "generated_at": "2026-04-28 12:00:00",
            "pipeline_version": "1.5.0",
            "row_count": len(df),
            "txn_cost_bps": 15.0,
        },
    )
    assert "Blended-Signal Backtest" in html
    assert "Component diagnostics" in html
    assert "Headline summary" in html
    assert "Per-variant detail" in html
    assert "Decision summary" in html
    # Every variant name should appear.
    for v in results:
        assert v["name"] in html


def test_render_markdown_smoke():
    df = _fixture_blend_signals(n_per_class=200, seed=3)
    df = attach_composite(df)
    results = run_all_variants(df)
    summary_rows = [_summary_row(v) for v in results]
    correlation_df = pd.DataFrame(
        {"a": [1.0, -0.4], "b": [-0.4, 1.0]},
        index=["a", "b"],
    )
    md = render_markdown(
        summary_rows=summary_rows,
        variant_results=results,
        correlation_df=correlation_df,
        metadata={
            "date_stamp": "20260428",
            "generated_at": "2026-04-28 12:00:00",
            "pipeline_version": "1.5.0",
            "row_count": len(df),
            "txn_cost_bps": 15.0,
        },
    )
    assert "# Blended-Signal Backtest" in md
    assert "## 1. Component diagnostics" in md
    assert "## 2. Headline summary" in md
    assert "## 4. Decision summary" in md
