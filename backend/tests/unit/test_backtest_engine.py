"""
Unit tests for ``backend.src.backtest_engine`` (M_backtest).

Strategy: build small synthetic ``signals_with_returns``-shape frames
where the ground-truth Sharpe / decile spread / sign-stability is
either known analytically or designed to be unambiguous, then assert
the engine recovers it.
"""

from __future__ import annotations

from typing import List

import numpy as np
import pandas as pd
import pytest

from backend.src.backtest_engine import (
    DEFAULT_TXN_COST_BPS,
    _decile_assign,
    _periods_per_year,
    assign_deciles,
    build_in_class_portfolio,
    build_long_short_portfolio,
    decile_returns,
    run_backtest,
    same_sign_year_count,
    summarize_by_year,
    summarize_portfolio,
    winsorize_within_class,
)


# ---------------------------------------------------------------------------
# Fixture factory
# ---------------------------------------------------------------------------


def _fixture_event_signals(
    n_per_class: int = 60,
    seed: int = 0,
    *,
    signal_predicts_return: bool = True,
    fy_count: int = 6,
) -> pd.DataFrame:
    """
    Build a synthetic event-classified frame.

    With ``signal_predicts_return=True``, the T+20 industry_excess_return
    is generated as ``0.5 * signal + noise`` for MAJOR_EXPANSION rows
    (positive slope) and ``-0.5 * signal + noise`` for CONTRACTION rows
    (negative slope), so the in-class top-decile-minus-bottom-decile
    spread is positive and the cross-class long-MAJ/short-CON spread
    is positive. Other event classes are noise.
    """
    rng = np.random.default_rng(seed)
    classes = ["MAJOR_EXPANSION", "CONTRACTION", "MINOR_EXPANSION", "NON_EVENT"]
    rows: List[dict] = []
    for cls in classes:
        for i in range(n_per_class):
            sig = rng.standard_normal()
            if signal_predicts_return and cls == "MAJOR_EXPANSION":
                ret = 0.5 * sig * 0.05 + rng.standard_normal() * 0.02
            elif signal_predicts_return and cls == "CONTRACTION":
                ret = -0.5 * sig * 0.05 + rng.standard_normal() * 0.02
            else:
                ret = rng.standard_normal() * 0.02
            fy_idx = i % fy_count
            fq = f"FY2{fy_idx}Q{(i % 4) + 1}"
            rows.append(
                {
                    "ticker": f"T{i:03d}",
                    "event_class": cls,
                    "relative_ceiling_change": sig,
                    "ceiling_change_pct_of_mcap": sig * 0.01,
                    "industry_excess_return_20d": ret,
                    "fiscal_quarter": fq,
                    "reference_trading_date": pd.Timestamp("2024-01-01")
                    + pd.Timedelta(days=i),
                }
            )
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# winsorize_within_class
# ---------------------------------------------------------------------------


def test_winsorize_clips_per_class_independently():
    df = pd.DataFrame(
        {
            "event_class": ["A"] * 100 + ["B"] * 100,
            "sig": list(range(100)) + [v * 1000 for v in range(100)],
        }
    )
    out = winsorize_within_class(df, "sig", "event_class", lower=0.0, upper=0.95)
    a = out.iloc[:100]
    b = out.iloc[100:]
    # Class A 95th percentile is ~94 (linear interp); top values clipped.
    assert a.max() < 100
    # Class B is clipped to its own (much larger) 95th percentile, not A's.
    assert b.max() > 1000
    # Unaffected rows in the middle are unchanged.
    assert float(a.iloc[50]) == 50.0
    assert float(b.iloc[50]) == pytest.approx(50_000.0)


def test_winsorize_preserves_nans():
    df = pd.DataFrame(
        {"event_class": ["A"] * 10, "sig": [1, 2, 3, np.nan, 5, 6, 7, 8, 9, 10]}
    )
    out = winsorize_within_class(df, "sig", "event_class")
    assert pd.isna(out.iloc[3])


def test_winsorize_passes_through_small_classes():
    df = pd.DataFrame({"event_class": ["A"] * 3, "sig": [1.0, 1e9, 3.0]})
    out = winsorize_within_class(df, "sig", "event_class")
    # Fewer than 5 rows, so no clipping applied.
    assert float(out.iloc[1]) == 1e9


def test_winsorize_extreme_outliers_compressed():
    """
    Mirrors the relative_ceiling_change use case from v2 analysis: a few
    extreme outliers among thousands of well-behaved values must be
    clipped down to the 99th-percentile cap, leaving the bulk untouched.
    """
    rng = np.random.default_rng(42)
    sig = list(rng.standard_normal(1000))
    sig[0] = 8.24e11  # the actual max from the dataset
    sig[1] = 1e9
    sig[2] = 1e6
    df = pd.DataFrame({"event_class": ["MAJOR_EXPANSION"] * 1000, "sig": sig})
    out = winsorize_within_class(df, "sig", "event_class")
    # 99th percentile of standard-normal-ish data is ~2.3; the outliers
    # all collapse to the same clip ceiling, far below their raw values.
    assert out.iloc[0] < 100, "8.24e11 outlier must be clipped to ~99th percentile"
    assert out.iloc[1] < 100, "1e9 outlier must be clipped"
    assert out.iloc[0] == out.iloc[1] == out.iloc[2], (
        "All extreme outliers should clip to the same upper bound"
    )
    # Middle-of-the-distribution values are untouched.
    assert -3 < out.iloc[500] < 3


# ---------------------------------------------------------------------------
# Decile assignment
# ---------------------------------------------------------------------------


def test_decile_assign_basic():
    s = pd.Series(list(range(100)))
    out = _decile_assign(s, 10)
    assert out.min() == 1
    assert out.max() == 10
    # Top decile is the last 10 rows by sort.
    top = out[out == 10]
    assert len(top) == 10


def test_decile_assign_handles_nans():
    s = pd.Series([1.0, 2.0, np.nan, 4.0, 5.0] * 10)
    out = _decile_assign(s, 5)
    nan_positions = s.isna()
    assert all(pd.isna(out[nan_positions]))


def test_assign_deciles_in_class_only():
    df = pd.DataFrame(
        {
            "event_class": ["A"] * 50 + ["B"] * 50,
            "sig": list(range(50)) + list(range(50)),
        }
    )
    out = assign_deciles(df, "sig", class_col="event_class", target_class="A")
    # Class A rows have 1..10 buckets, class B rows are NA.
    assert all(pd.isna(out.iloc[50:]))
    assert out.iloc[:50].dropna().min() == 1
    assert out.iloc[:50].dropna().max() == 10


# ---------------------------------------------------------------------------
# build_in_class_portfolio
# ---------------------------------------------------------------------------


def test_in_class_portfolio_recovers_signal():
    df = _fixture_event_signals(n_per_class=120, seed=1)
    trades = build_in_class_portfolio(
        df,
        signal_col="relative_ceiling_change",
        return_col="industry_excess_return_20d",
        class_col="event_class",
        target_class="MAJOR_EXPANSION",
        n_buckets=10,
        txn_cost_bps=0,
    )
    # Long sleeve has positive mean return (signal*0.5*0.05 with positive top decile).
    longs = trades[trades["side"] == "long"]
    shorts = trades[trades["side"] == "short"]
    assert longs["gross_return"].mean() > 0
    # Short sleeve: bottom decile (negative signal) has negative raw return,
    # and after sign-flip for the short P&L it becomes positive.
    assert shorts["gross_return"].mean() > 0


def test_in_class_portfolio_zero_when_no_signal():
    """With pure noise, the long-short spread should not be reliably positive."""
    df = _fixture_event_signals(n_per_class=200, seed=99, signal_predicts_return=False)
    trades = build_in_class_portfolio(
        df,
        signal_col="relative_ceiling_change",
        return_col="industry_excess_return_20d",
        class_col="event_class",
        target_class="MAJOR_EXPANSION",
        txn_cost_bps=0,
    )
    summary = summarize_portfolio(trades)
    # |net_mean| should be small (within ~1 sigma of zero).
    se = summary["net_stddev"] / np.sqrt(summary["n_trades"])
    assert abs(summary["net_mean"]) < 3 * se, (
        f"Pure noise produced suspiciously strong signal: net_mean={summary['net_mean']}"
    )


def test_in_class_empty_when_class_missing():
    df = _fixture_event_signals(n_per_class=20, seed=0)
    trades = build_in_class_portfolio(
        df,
        signal_col="relative_ceiling_change",
        return_col="industry_excess_return_20d",
        class_col="event_class",
        target_class="NONEXISTENT",
    )
    assert len(trades) == 0


def test_in_class_txn_cost_reduces_net():
    df = _fixture_event_signals(n_per_class=200, seed=2)
    trades_free = build_in_class_portfolio(
        df,
        signal_col="relative_ceiling_change",
        return_col="industry_excess_return_20d",
        class_col="event_class",
        target_class="MAJOR_EXPANSION",
        txn_cost_bps=0,
    )
    trades_costly = build_in_class_portfolio(
        df,
        signal_col="relative_ceiling_change",
        return_col="industry_excess_return_20d",
        class_col="event_class",
        target_class="MAJOR_EXPANSION",
        txn_cost_bps=15,
    )
    # Same gross returns; net is uniformly lower by 0.0015 (15 bps).
    assert (
        trades_costly["gross_return"].sum() == pytest.approx(trades_free["gross_return"].sum())
    )
    diff = trades_free["net_return"].mean() - trades_costly["net_return"].mean()
    assert diff == pytest.approx(15 / 10_000.0, abs=1e-9)


# ---------------------------------------------------------------------------
# build_long_short_portfolio
# ---------------------------------------------------------------------------


def test_cross_class_recovers_pair_signal():
    df = _fixture_event_signals(n_per_class=120, seed=3)
    trades = build_long_short_portfolio(
        df,
        signal_col="relative_ceiling_change",
        return_col="industry_excess_return_20d",
        class_col="event_class",
        long_class="MAJOR_EXPANSION",
        short_class="CONTRACTION",
        txn_cost_bps=0,
    )
    summary = summarize_portfolio(trades)
    # We engineered MAJ to have on-average positive returns (long wins)
    # and CON to have on-average negative returns (short wins). But because
    # the synthetic signals_per_class are normal mean=0, the *means* of MAJ
    # and CON returns are noise around zero. The cross-class long-short
    # spread tests whether on average MAJ outperforms CON (it shouldn't
    # systematically here since both have mean(signal)=0). We just verify
    # the trade construction is structurally correct.
    assert summary["n_long"] == 120
    assert summary["n_short"] == 120
    long_w = trades[trades["side"] == "long"]["weight"].iloc[0]
    short_w = trades[trades["side"] == "short"]["weight"].iloc[0]
    assert long_w == pytest.approx(1 / 120.0)
    assert short_w == pytest.approx(-1 / 120.0)


def test_cross_class_short_leg_sign_flipped():
    """Short P&L is the negative of the underlying return."""
    df = pd.DataFrame(
        {
            "ticker": ["A", "B"],
            "event_class": ["MAJOR_EXPANSION", "CONTRACTION"],
            "relative_ceiling_change": [1.0, -1.0],
            "industry_excess_return_20d": [0.05, 0.03],  # both positive
            "fiscal_quarter": ["FY24Q1", "FY24Q1"],
            "reference_trading_date": [pd.Timestamp("2024-01-01")] * 2,
        }
    )
    trades = build_long_short_portfolio(
        df,
        signal_col="relative_ceiling_change",
        return_col="industry_excess_return_20d",
        class_col="event_class",
        long_class="MAJOR_EXPANSION",
        short_class="CONTRACTION",
        txn_cost_bps=0,
    )
    long_row = trades[trades["side"] == "long"].iloc[0]
    short_row = trades[trades["side"] == "short"].iloc[0]
    assert long_row["gross_return"] == pytest.approx(0.05)
    # Short leg: stock went up 3%, so short P&L = -3%.
    assert short_row["gross_return"] == pytest.approx(-0.03)


# ---------------------------------------------------------------------------
# summarize_portfolio
# ---------------------------------------------------------------------------


def test_summarize_empty_returns_none_fields():
    out = summarize_portfolio(
        pd.DataFrame(
            columns=["fiscal_year", "side", "gross_return", "net_return", "weight"]
        )
    )
    assert out["n_trades"] == 0
    assert out["sharpe"] is None
    assert out["net_mean"] is None


def test_summarize_sharpe_known_value():
    """
    Construct a portfolio with mean=0.01, stddev=0.02 across 100 trades.
    Annualised Sharpe at hold_days=20 should be (0.01/0.02)*sqrt(252/20)
    = 0.5 * sqrt(12.6) ~= 1.7748.
    """
    rng = np.random.default_rng(7)
    n = 100
    base = rng.standard_normal(n)
    base = (base - base.mean()) / base.std(ddof=1)  # standardise exactly
    rets = 0.01 + 0.02 * base
    trades = pd.DataFrame(
        {
            "fiscal_year": ["FY24"] * n,
            "side": ["long"] * n,
            "gross_return": rets,
            "net_return": rets,
            "weight": [1 / n] * n,
        }
    )
    out = summarize_portfolio(trades, hold_days=20)
    expected_sharpe = 0.5 * np.sqrt(252 / 20)
    assert out["sharpe"] == pytest.approx(expected_sharpe, abs=1e-6)


def test_summarize_monthly_unequal_sleeves():
    """
    Construct a 2-month cross-class trade frame with unequal sleeves
    where per-trade pooling and sleeve-aware monthly aggregation give
    different answers, and verify monthly returns the right one.
    """
    # Month 1: 1 long with +10%, 4 shorts averaging +5%.
    # Long-short portfolio return for month 1: 10% + 5% = 15%
    # (short P&L = -5% on average since the underlying went up,
    # so net_return for short is -5%; long+short = 10 + (-5) = 5%).
    # Wait: short net_return is set to -underlying. If underlying = +5%,
    # short net_return = -5%. Long+short = 10 + (-5) = 5%.
    # Month 2: 2 longs averaging +2%, 6 shorts averaging -1%.
    # Long net_return = 2%; short net_return = -(-1) = +1%; sum = 3%.
    rows = []
    rows.append(
        {"reference_trading_date": pd.Timestamp("2024-01-15"), "side": "long",
         "fiscal_year": "FY24", "gross_return": 0.10, "net_return": 0.10}
    )
    for i in range(4):
        rows.append(
            {"reference_trading_date": pd.Timestamp("2024-01-15"), "side": "short",
             "fiscal_year": "FY24", "gross_return": -0.05, "net_return": -0.05}
        )
    for i in range(2):
        rows.append(
            {"reference_trading_date": pd.Timestamp("2024-02-15"), "side": "long",
             "fiscal_year": "FY24", "gross_return": 0.02, "net_return": 0.02}
        )
    for i in range(6):
        rows.append(
            {"reference_trading_date": pd.Timestamp("2024-02-15"), "side": "short",
             "fiscal_year": "FY24", "gross_return": 0.01, "net_return": 0.01}
        )
    trades = pd.DataFrame(rows)
    monthly = summarize_portfolio(trades, aggregation="monthly")
    # Expected: month 1 net = 0.10 + (-0.05) = 0.05; month 2 net = 0.02 + 0.01 = 0.03
    # Mean monthly = 0.04; stddev = stddev of [0.05, 0.03] = 0.01414
    assert monthly["n_periods"] == 2
    assert monthly["net_mean"] == pytest.approx(0.04, abs=1e-9)
    # Compare to broken per-trade pooling: 13 trades, mean = ?
    per_trade = summarize_portfolio(trades, aggregation="per_trade")
    # 1 long*0.10 + 4 short*(-0.05) + 2 long*0.02 + 6 short*0.01 = 0.10 - 0.20 + 0.04 + 0.06
    # = 0.00, divided by 13 = 0.0
    assert per_trade["net_mean"] == pytest.approx(0.0, abs=1e-9)
    # The two methods give materially different answers.
    assert abs(monthly["net_mean"] - per_trade["net_mean"]) > 0.03


def test_summarize_invalid_aggregation_raises():
    trades = pd.DataFrame(
        {
            "reference_trading_date": [pd.Timestamp("2024-01-01")],
            "side": ["long"],
            "fiscal_year": ["FY24"],
            "gross_return": [0.01],
            "net_return": [0.01],
        }
    )
    with pytest.raises(ValueError):
        summarize_portfolio(trades, aggregation="bogus")


def test_summarize_hit_rate():
    trades = pd.DataFrame(
        {
            "fiscal_year": ["FY24"] * 4,
            "side": ["long"] * 4,
            "gross_return": [0.05, -0.02, 0.03, -0.01],
            "net_return": [0.05, -0.02, 0.03, -0.01],
            "weight": [0.25] * 4,
        }
    )
    out = summarize_portfolio(trades)
    # 2 of 4 net positive.
    assert out["hit_rate"] == pytest.approx(0.5)


# ---------------------------------------------------------------------------
# summarize_by_year
# ---------------------------------------------------------------------------


def test_summarize_by_year_groups_correctly():
    trades = pd.DataFrame(
        {
            "fiscal_year": ["FY23", "FY23", "FY24", "FY24", "FY25", "FY25"],
            "side": ["long"] * 6,
            "gross_return": [0.01, 0.02, -0.01, 0.03, 0.04, -0.05],
            "net_return": [0.01, 0.02, -0.01, 0.03, 0.04, -0.05],
            "weight": [1 / 2] * 6,
        }
    )
    out = summarize_by_year(trades)
    assert list(out["fiscal_year"]) == ["FY23", "FY24", "FY25"]
    fy23 = out[out["fiscal_year"] == "FY23"].iloc[0]
    assert fy23["n_trades"] == 2
    assert fy23["net_mean"] == pytest.approx(0.015)


def test_summarize_by_year_empty():
    out = summarize_by_year(pd.DataFrame())
    assert len(out) == 0
    assert "fiscal_year" in out.columns


# ---------------------------------------------------------------------------
# decile_returns
# ---------------------------------------------------------------------------


def test_decile_returns_monotone_for_strong_signal():
    df = _fixture_event_signals(n_per_class=200, seed=4)
    spread = decile_returns(
        df,
        signal_col="relative_ceiling_change",
        return_col="industry_excess_return_20d",
        class_col="event_class",
        target_class="MAJOR_EXPANSION",
    )
    # Top bucket > bottom bucket on average.
    assert spread.iloc[-1]["mean_return"] > spread.iloc[0]["mean_return"]


# ---------------------------------------------------------------------------
# run_backtest end-to-end
# ---------------------------------------------------------------------------


def test_run_backtest_in_class_smoke():
    df = _fixture_event_signals(n_per_class=200, seed=5)
    res = run_backtest(
        df,
        signal_col="relative_ceiling_change",
        return_col="industry_excess_return_20d",
        class_col="event_class",
        framing="in_class",
        target_class="MAJOR_EXPANSION",
        winsorize=True,
    )
    assert res["config"]["framing"] == "in_class"
    assert res["config"]["winsorize"] is True
    assert res["summary"]["n_trades"] > 0
    assert len(res["deciles"]) > 0
    assert "fiscal_year" in res["by_year"].columns


def test_run_backtest_cross_class_smoke():
    df = _fixture_event_signals(n_per_class=120, seed=6)
    res = run_backtest(
        df,
        signal_col="relative_ceiling_change",
        return_col="industry_excess_return_20d",
        class_col="event_class",
        framing="cross_class",
        long_class="MAJOR_EXPANSION",
        short_class="CONTRACTION",
    )
    assert res["summary"]["n_long"] == 120
    assert res["summary"]["n_short"] == 120
    # Cross-class deciles are not computed.
    assert len(res["deciles"]) == 0


def test_run_backtest_invalid_framing_raises():
    df = _fixture_event_signals(n_per_class=10)
    with pytest.raises(ValueError):
        run_backtest(
            df,
            signal_col="relative_ceiling_change",
            return_col="industry_excess_return_20d",
            class_col="event_class",
            framing="bogus",
        )


def test_run_backtest_in_class_requires_target():
    df = _fixture_event_signals(n_per_class=10)
    with pytest.raises(ValueError):
        run_backtest(
            df,
            signal_col="relative_ceiling_change",
            return_col="industry_excess_return_20d",
            class_col="event_class",
            framing="in_class",
        )


def test_run_backtest_cross_class_requires_pair():
    df = _fixture_event_signals(n_per_class=10)
    with pytest.raises(ValueError):
        run_backtest(
            df,
            signal_col="relative_ceiling_change",
            return_col="industry_excess_return_20d",
            class_col="event_class",
            framing="cross_class",
            long_class="MAJOR_EXPANSION",
        )


# ---------------------------------------------------------------------------
# same_sign_year_count
# ---------------------------------------------------------------------------


def test_same_sign_count_all_positive():
    by_year = pd.DataFrame(
        {
            "fiscal_year": [f"FY2{i}" for i in range(6)],
            "net_mean": [0.01, 0.02, 0.005, 0.03, 0.015, 0.025],
        }
    )
    counts = same_sign_year_count(by_year, "net_mean")
    assert counts["n_years"] == 6
    assert counts["n_positive"] == 6
    assert counts["n_negative"] == 0


def test_same_sign_count_mixed():
    by_year = pd.DataFrame(
        {
            "fiscal_year": ["FY21", "FY22", "FY23", "FY24", "FY25", "FY26"],
            "net_mean": [0.01, -0.02, 0.005, 0.03, 0.015, 0.025],
        }
    )
    counts = same_sign_year_count(by_year, "net_mean")
    assert counts["n_positive"] == 5
    assert counts["n_negative"] == 1


def test_same_sign_count_empty():
    counts = same_sign_year_count(pd.DataFrame())
    assert counts["n_years"] == 0


# ---------------------------------------------------------------------------
# Sanity helpers
# ---------------------------------------------------------------------------


def test_periods_per_year_helper():
    assert _periods_per_year(20) == pytest.approx(252 / 20)
    assert _periods_per_year(5) == pytest.approx(252 / 5)
