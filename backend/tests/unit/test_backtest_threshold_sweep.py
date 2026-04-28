"""
Unit tests for ``backend.scripts.backtest_threshold_sweep``.

Focus: verify the in-place ``event_class`` reclassification is correct.
The orchestration / rendering glue is exercised by hand via the CLI
and shares structure with ``test_backtest_script.py``.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from backend.scripts.backtest_threshold_sweep import (
    SWEEP_THRESHOLDS,
    _build_summary_table,
    _format_threshold,
    _reclassify_major_expansion,
)


def test_format_threshold_dollars():
    assert _format_threshold(5e7) == "$50M"
    assert _format_threshold(1e8) == "$100M"
    assert _format_threshold(5e8) == "$500M"
    assert _format_threshold(1e9) == "$1B"
    assert _format_threshold(3e9) == "$3B"


def test_reclassify_promotes_to_major_at_lower_threshold():
    """A $75M FUNDING_INCREASE row should qualify under $50M but not under $100M."""
    df = pd.DataFrame(
        {
            "transaction_type": ["FUNDING_INCREASE"],
            "ceiling_change": [7.5e7],
            "event_class": ["MODERATE_EXPANSION"],
        }
    )
    out_50 = _reclassify_major_expansion(df, threshold=5e7)
    out_100 = _reclassify_major_expansion(df, threshold=1e8)
    assert out_50.iloc[0]["event_class"] == "MAJOR_EXPANSION"
    assert out_100.iloc[0]["event_class"] == "MODERATE_EXPANSION"


def test_reclassify_uses_strict_greater_than():
    """A row with ceiling_change exactly equal to the threshold should NOT qualify.
    This matches the production pipeline's '>' rule in transform.py."""
    df = pd.DataFrame(
        {
            "transaction_type": ["FUNDING_INCREASE"],
            "ceiling_change": [1e8],  # exactly at the threshold
            "event_class": ["MODERATE_EXPANSION"],
        }
    )
    out = _reclassify_major_expansion(df, threshold=1e8)
    assert out.iloc[0]["event_class"] == "MODERATE_EXPANSION"


def test_reclassify_demotes_at_higher_threshold():
    """A $200M row currently labelled MAJOR_EXPANSION should demote at $500M cutoff."""
    df = pd.DataFrame(
        {
            "transaction_type": ["FUNDING_INCREASE"],
            "ceiling_change": [2e8],
            "event_class": ["MAJOR_EXPANSION"],
        }
    )
    out = _reclassify_major_expansion(df, threshold=5e8)
    assert out.iloc[0]["event_class"] == "MODERATE_EXPANSION"


def test_reclassify_leaves_non_funding_increase_untouched():
    """CONTRACTION rows must not be reclassified regardless of ceiling_change."""
    df = pd.DataFrame(
        {
            "transaction_type": ["MODIFICATION"] * 3,
            "ceiling_change": [-1e7, -1e8, 1e8],
            "event_class": ["CONTRACTION", "CONTRACTION", "OTHER_MOD"],
        }
    )
    out = _reclassify_major_expansion(df, threshold=5e7)
    assert list(out["event_class"]) == ["CONTRACTION", "CONTRACTION", "OTHER_MOD"]


def test_reclassify_handles_null_ceiling_change():
    df = pd.DataFrame(
        {
            "transaction_type": ["FUNDING_INCREASE"],
            "ceiling_change": [np.nan],
            "event_class": ["NON_EVENT"],
        }
    )
    out = _reclassify_major_expansion(df, threshold=5e7)
    assert out.iloc[0]["event_class"] == "NON_EVENT"


def test_reclassify_does_not_mutate_input():
    df = pd.DataFrame(
        {
            "transaction_type": ["FUNDING_INCREASE"],
            "ceiling_change": [2e8],
            "event_class": ["MAJOR_EXPANSION"],
        }
    )
    _ = _reclassify_major_expansion(df, threshold=5e8)
    assert df.iloc[0]["event_class"] == "MAJOR_EXPANSION"  # original unchanged


def test_sweep_thresholds_are_ordered_ascending():
    """Sanity: report builds rely on thresholds being ascending."""
    assert SWEEP_THRESHOLDS == sorted(SWEEP_THRESHOLDS)


def test_build_summary_table_smoke():
    """Construct a fake sweep result with known stats and verify pass logic."""
    fake_entry = {
        "threshold": 1e8,
        "threshold_label": "$100M",
        "n_qualified_rows": 5000,
        "result": {
            "summary": {
                "n_trades": 1000,
                "gross_mean": 0.01,
                "net_mean": 0.008,
                "net_stddev": 0.05,
                "sharpe": 1.0,
                "hit_rate": 0.55,
            },
            "by_year": pd.DataFrame(
                {
                    "fiscal_year": ["FY21", "FY22", "FY23", "FY24", "FY25", "FY26"],
                    "net_mean": [0.01, 0.02, 0.005, 0.03, 0.015, 0.025],
                }
            ),
        },
    }
    df = _build_summary_table([fake_entry])
    row = df.iloc[0]
    # Pandas returns numpy.bool_; defend with bool() like the rest of the suite.
    assert bool(row["pass_sharpe"]) is True  # 1.0 >= 0.5
    assert bool(row["pass_yr_stability"]) is True  # 6 of 6 positive
    assert bool(row["pass_overall"]) is True
    assert row["n_pos_yrs"] == 6


def test_build_summary_table_failing_case():
    fake_entry = {
        "threshold": 1e9,
        "threshold_label": "$1B",
        "n_qualified_rows": 200,
        "result": {
            "summary": {
                "n_trades": 50,
                "gross_mean": 0.001,
                "net_mean": -0.001,
                "net_stddev": 0.05,
                "sharpe": -0.05,
                "hit_rate": 0.4,
            },
            "by_year": pd.DataFrame(
                {
                    "fiscal_year": ["FY21", "FY22", "FY23"],
                    "net_mean": [0.01, -0.02, 0.005],
                }
            ),
        },
    }
    df = _build_summary_table([fake_entry])
    row = df.iloc[0]
    assert bool(row["pass_sharpe"]) is False
    assert bool(row["pass_yr_stability"]) is False  # 2/1 split, max 2 < 5
    assert bool(row["pass_overall"]) is False
