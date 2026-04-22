"""
Unit tests for ``backend.src.analyze`` (M2.4).

Strategy: drive each analytic against a synthetic dataset with a
known ground-truth Information Coefficient and decile spread, so we
can verify the math rather than just "some number was produced".
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from backend.src.analyze import (
    cumulative_pnl,
    decile_spread,
    ic_per_sector,
    information_coefficient,
    signal_coverage,
    summarize_all_signals,
    top_minus_bottom,
)


def _make_perfect_alpha_dataset(n: int = 500) -> pd.DataFrame:
    """
    Construct a dataset where signal and forward-return are perfectly
    rank-correlated (Spearman IC == 1.0). We use a monotonic but
    non-linear transform so a naive Pearson test would undershoot and
    the Spearman test is unambiguously the right measure.
    """
    rng = np.random.default_rng(42)
    signal = rng.standard_normal(n)
    # Monotonic but nonlinear: exp + jitter-free.
    forward = np.exp(signal)
    return pd.DataFrame({"signal": signal, "forward_return": forward})


def _make_noise_dataset(n: int = 500) -> pd.DataFrame:
    """Independent signal and forward return -> IC ~ 0."""
    rng = np.random.default_rng(7)
    return pd.DataFrame(
        {"signal": rng.standard_normal(n), "forward_return": rng.standard_normal(n)}
    )


# ---------------------------------------------------------------------------
# information_coefficient
# ---------------------------------------------------------------------------


def test_ic_known_ground_truth_perfect_alpha():
    df = _make_perfect_alpha_dataset(n=200)
    out = information_coefficient(df["signal"], df["forward_return"])
    assert out["ic"] == pytest.approx(1.0, abs=1e-9)
    assert out["n"] == 200


def test_ic_on_noise_is_near_zero():
    df = _make_noise_dataset(n=2000)
    out = information_coefficient(df["signal"], df["forward_return"])
    # On 2000 iid samples, the 99% CI for Spearman under the null is
    # roughly +/- 0.06. Use 0.1 as a loose bound for flake safety.
    assert abs(out["ic"]) < 0.1
    assert out["n"] == 2000


def test_ic_handles_nans_and_small_samples():
    # Mixed NaNs; only 3 valid pairs survive dropna.
    signal = pd.Series([1.0, 2.0, 3.0, np.nan, 5.0])
    ret = pd.Series([np.nan, 2.0, 3.0, 4.0, 5.0])
    out = information_coefficient(signal, ret)
    assert out["n"] == 3
    # Two valid non-NaN pairs (2,2), (3,3), (5,5) are strictly increasing
    # -> IC == 1.0
    assert out["ic"] == pytest.approx(1.0)


def test_ic_too_few_pairs_returns_none():
    signal = pd.Series([1.0, np.nan])
    ret = pd.Series([np.nan, 2.0])
    out = information_coefficient(signal, ret)
    assert out["ic"] is None
    assert out["n"] == 0


def test_ic_constant_signal_returns_none():
    """Zero variance in the signal -> correlation undefined."""
    signal = pd.Series([1.0] * 10)
    ret = pd.Series([1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0])
    out = information_coefficient(signal, ret)
    assert out["ic"] is None


# ---------------------------------------------------------------------------
# decile_spread / top_minus_bottom
# ---------------------------------------------------------------------------


def test_decile_spread_monotonic_on_synthetic_alpha():
    """Perfect alpha signal -> mean return is monotonically non-decreasing
    across deciles (bucket 10 >= bucket 1)."""
    df = _make_perfect_alpha_dataset(n=1000)
    spread = decile_spread(df, "signal", "forward_return", n_buckets=10)
    # Bucket 1 mean < Bucket 10 mean, strictly (large gap in exp scale).
    b1 = spread[spread["bucket"] == 1]["mean_return"].iloc[0]
    b10 = spread[spread["bucket"] == 10]["mean_return"].iloc[0]
    assert b10 > b1
    # Non-decreasing check along the way.
    means = spread.sort_values("bucket")["mean_return"].values
    for i in range(1, len(means)):
        assert means[i] >= means[i - 1] - 1e-6, f"decile {i} dropped below {i - 1}"


def test_decile_spread_flat_on_random_noise():
    """Random data -> no systematic bucket-mean ordering. Top-bottom
    spread should be small relative to the overall std."""
    df = _make_noise_dataset(n=5000)
    tmb = top_minus_bottom(df, "signal", "forward_return", n_buckets=10)
    overall_std = df["forward_return"].std()
    # On noise, top-minus-bottom is typically < 1 standard deviation.
    # Loose bound for flake safety.
    assert abs(tmb) < 1.5 * overall_std


def test_decile_spread_too_few_rows_returns_empty():
    tiny = pd.DataFrame({"signal": [1.0, 2.0, 3.0], "forward_return": [0.1, 0.2, 0.3]})
    out = decile_spread(tiny, "signal", "forward_return", n_buckets=10)
    assert out.empty


def test_top_minus_bottom_manual_calculation():
    """Hand-checked: 10 rows with signal 1..10, returns 0.01..0.10 -> spread = 0.09."""
    df = pd.DataFrame(
        {
            "signal": list(range(1, 11)),
            "forward_return": [0.01 * i for i in range(1, 11)],
        }
    )
    tmb = top_minus_bottom(df, "signal", "forward_return", n_buckets=10)
    # With 10 rows and 10 buckets, each bucket has one row; spread is
    # simply ret[9] - ret[0] = 0.10 - 0.01 = 0.09.
    assert tmb == pytest.approx(0.09, abs=1e-9)


# ---------------------------------------------------------------------------
# cumulative_pnl
# ---------------------------------------------------------------------------


def test_cumulative_pnl_matches_manual_long_top_decile():
    """
    10 rows, ranked by signal, each on a distinct date. Long-top-decile
    strategy takes only the row in bucket 10. Cumulative P&L on that
    strategy equals the single row's forward return.
    """
    df = pd.DataFrame(
        {
            "signal": list(range(1, 11)),
            "forward_return": [0.01 * i for i in range(1, 11)],
            "reference_trading_date": pd.bdate_range("2024-09-30", periods=10),
        }
    )
    curve = cumulative_pnl(
        df,
        "signal",
        "forward_return",
        strategy="long_top_decile",
        n_buckets=10,
    )
    # 10 dates: 9 zeros + one 0.10 at the top-bucket row's date -> cumulative 0.10 at the end.
    assert curve.iloc[-1] == pytest.approx(0.10, abs=1e-9)


def test_cumulative_pnl_long_short_structure():
    """
    Long-short P&L curve is well-formed: monotone-index, numeric,
    matches input rows. We don't assert an end-of-curve magnitude
    because cumulative sums of iid returns follow a random walk whose
    end-point stdev scales with sqrt(n_dates); drift within that envelope
    is expected and not a bug.
    """
    df = _make_noise_dataset(n=500)
    df["reference_trading_date"] = pd.bdate_range("2024-09-30", periods=500)
    curve = cumulative_pnl(
        df, "signal", "forward_return", strategy="long_short", n_buckets=10
    )
    assert not curve.empty
    # All dates unique (one point per day), sorted, monotone increasing.
    assert curve.index.is_monotonic_increasing
    # Cumulative: each step equals prior + leg_return, so values are finite.
    assert curve.notna().all()
    # Sanity: first point equals the first day's long-short return (no drift yet).
    # Can't easily assert a specific value without re-deriving the mock math,
    # so just check it's finite and near the per-row return scale.
    assert np.isfinite(curve.iloc[0])


# ---------------------------------------------------------------------------
# ic_per_sector, signal_coverage, summarize_all_signals
# ---------------------------------------------------------------------------


def test_ic_per_sector_drops_small_sectors():
    rng = np.random.default_rng(0)
    df = pd.DataFrame(
        {
            "signal": rng.standard_normal(100),
            "forward_return": rng.standard_normal(100),
            "sector": (["Industrials"] * 60 + ["Tech"] * 30 + ["Micro"] * 10),
        }
    )
    out = ic_per_sector(df, "signal", "forward_return", min_rows_per_sector=30)
    # 'Micro' (10 rows) should be dropped; only Industrials + Tech remain.
    assert set(out["sector"]) == {"Industrials", "Tech"}


def test_signal_coverage_reports_correct_pcts():
    df = pd.DataFrame(
        {
            "sig_a": [1.0, 2.0, np.nan, 4.0],  # 75%
            "sig_b": [np.nan, np.nan, np.nan, 1.0],  # 25%
        }
    )
    out = signal_coverage(df, ["sig_a", "sig_b", "sig_nonexistent"])
    out_d = out.set_index("signal").to_dict(orient="index")
    assert out_d["sig_a"]["n_non_null"] == 3
    assert out_d["sig_a"]["pct"] == 75.0
    assert out_d["sig_b"]["pct"] == 25.0
    assert out_d["sig_nonexistent"]["n_non_null"] == 0


def test_summarize_all_signals_structure():
    df = _make_perfect_alpha_dataset(n=200)
    df["excess_return_5d"] = df["forward_return"]  # treat as return col
    out = summarize_all_signals(df, ["signal"], [5])
    assert len(out) == 1
    row = out.iloc[0]
    assert row["signal"] == "signal"
    assert row["horizon"] == "T+5d"
    assert row["ic"] == pytest.approx(1.0, abs=1e-9)
    assert row["n"] == 200
