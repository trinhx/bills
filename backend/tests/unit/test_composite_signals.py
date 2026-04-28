"""
Unit tests for ``backend.src.composite_signals`` (M_blend).

Strategy: build small DataFrames with analytically known percentile
ranks and verify the composite recovers the expected ordering and
values.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from backend.src.composite_signals import (
    SignalSpec,
    build_composite_score,
    component_correlations,
    percentile_rank,
)


# ---------------------------------------------------------------------------
# SignalSpec validation
# ---------------------------------------------------------------------------


def test_signal_spec_rejects_bad_direction():
    with pytest.raises(ValueError):
        SignalSpec(column="x", direction="up", weight=1.0)


def test_signal_spec_rejects_zero_weight():
    with pytest.raises(ValueError):
        SignalSpec(column="x", direction="asc", weight=0.0)


def test_signal_spec_rejects_negative_weight():
    with pytest.raises(ValueError):
        SignalSpec(column="x", direction="asc", weight=-0.5)


def test_signal_spec_is_frozen():
    spec = SignalSpec(column="x", direction="asc", weight=1.0)
    with pytest.raises(AttributeError):
        spec.column = "y"  # type: ignore[misc]


# ---------------------------------------------------------------------------
# percentile_rank
# ---------------------------------------------------------------------------


def test_percentile_rank_strictly_in_zero_one():
    s = pd.Series([1.0, 2.0, 3.0, 4.0, 5.0])
    out = percentile_rank(s)
    # n=5, denominator=6: lowest = 1/6, highest = 5/6.
    assert out.iloc[0] == pytest.approx(1 / 6)
    assert out.iloc[-1] == pytest.approx(5 / 6)
    assert out.min() > 0
    assert out.max() < 1


def test_percentile_rank_descending_flips_order():
    s = pd.Series([1.0, 2.0, 3.0])
    asc = percentile_rank(s, direction="asc")
    desc = percentile_rank(s, direction="desc")
    # asc: 1/4, 2/4, 3/4. desc: 1 - those = 3/4, 2/4, 1/4.
    assert asc.iloc[0] == pytest.approx(0.25)
    assert desc.iloc[0] == pytest.approx(0.75)
    assert desc.iloc[-1] == pytest.approx(0.25)


def test_percentile_rank_preserves_nan():
    s = pd.Series([1.0, np.nan, 3.0, 4.0])
    out = percentile_rank(s)
    assert pd.isna(out.iloc[1])
    # 3 non-null values; denominator = 4.
    assert out.iloc[0] == pytest.approx(1 / 4)
    assert out.iloc[2] == pytest.approx(2 / 4)
    assert out.iloc[3] == pytest.approx(3 / 4)


def test_percentile_rank_handles_ties():
    """Ties get the average rank."""
    s = pd.Series([1.0, 2.0, 2.0, 3.0])
    out = percentile_rank(s)
    # rank of the two 2.0 values is (2+3)/2 = 2.5; 4 non-null + 1 = 5.
    assert out.iloc[0] == pytest.approx(1 / 5)
    assert out.iloc[1] == pytest.approx(2.5 / 5)
    assert out.iloc[2] == pytest.approx(2.5 / 5)
    assert out.iloc[3] == pytest.approx(4 / 5)


def test_percentile_rank_rejects_bad_direction():
    s = pd.Series([1.0, 2.0])
    with pytest.raises(ValueError):
        percentile_rank(s, direction="up")


# ---------------------------------------------------------------------------
# build_composite_score: basic mechanics
# ---------------------------------------------------------------------------


def test_composite_two_signals_equal_weight():
    """
    Two identical signals: composite should equal the per-signal rank.
    """
    df = pd.DataFrame({"a": [1.0, 2.0, 3.0, 4.0], "b": [1.0, 2.0, 3.0, 4.0]})
    specs = [
        SignalSpec("a", direction="asc", weight=1.0),
        SignalSpec("b", direction="asc", weight=1.0),
    ]
    out = build_composite_score(df, specs)
    expected = percentile_rank(df["a"])
    pd.testing.assert_series_equal(out, expected, check_names=False)


def test_composite_opposite_signals_cancel():
    """
    Signal a ascending and signal a (same column) descending should
    average to exactly 0.5 for every row (the rank and its mirror sum
    to (n+1)/(n+1) = 1, divided by 2 = 0.5).
    """
    df = pd.DataFrame({"a": [1.0, 2.0, 3.0, 4.0]})
    specs = [
        SignalSpec("a", direction="asc", weight=1.0),
        SignalSpec("a", direction="desc", weight=1.0),
    ]
    out = build_composite_score(df, specs)
    # a_asc + a_desc = 1 always; /2 = 0.5.
    for v in out:
        assert v == pytest.approx(0.5)


def test_composite_orthogonal_signals_preserve_average_ordering():
    """
    With two perfectly anti-correlated signals (one's lowest is the
    other's highest), the composite should be exactly 0.5 for everyone
    -- they perfectly cancel.
    """
    df = pd.DataFrame({"a": [1.0, 2.0, 3.0, 4.0], "b": [4.0, 3.0, 2.0, 1.0]})
    specs = [
        SignalSpec("a", direction="asc", weight=1.0),
        SignalSpec("b", direction="asc", weight=1.0),
    ]
    out = build_composite_score(df, specs)
    for v in out:
        assert v == pytest.approx(0.5)


def test_composite_correlated_signals_rank_consistently():
    """Strongly correlated signals: top row in both = top composite."""
    df = pd.DataFrame(
        {
            "a": [1.0, 2.0, 3.0, 4.0, 5.0],
            "b": [1.1, 2.1, 3.1, 4.1, 5.1],  # strictly increasing
        }
    )
    specs = [
        SignalSpec("a", direction="asc"),
        SignalSpec("b", direction="asc"),
    ]
    out = build_composite_score(df, specs)
    # Composite is monotone increasing.
    assert list(out) == sorted(out)


# ---------------------------------------------------------------------------
# build_composite_score: NaN handling
# ---------------------------------------------------------------------------


def test_composite_require_all_drops_partial_rows():
    df = pd.DataFrame(
        {
            "a": [1.0, 2.0, np.nan, 4.0],
            "b": [1.0, np.nan, 3.0, 4.0],
        }
    )
    specs = [SignalSpec("a", direction="asc"), SignalSpec("b", direction="asc")]
    out = build_composite_score(df, specs, require_all_components=True)
    # Rows 0 and 3 have both; rows 1 and 2 are partial -> NaN.
    assert pd.isna(out.iloc[1])
    assert pd.isna(out.iloc[2])
    assert pd.notna(out.iloc[0])
    assert pd.notna(out.iloc[3])


def test_composite_partial_when_allowed():
    df = pd.DataFrame(
        {
            "a": [1.0, 2.0, np.nan, 4.0],
            "b": [1.0, np.nan, 3.0, 4.0],
        }
    )
    specs = [SignalSpec("a", direction="asc"), SignalSpec("b", direction="asc")]
    out = build_composite_score(df, specs, require_all_components=False)
    # All rows with at least one component should have a score.
    assert pd.notna(out.iloc[1])  # only 'a' present
    assert pd.notna(out.iloc[2])  # only 'b' present


# ---------------------------------------------------------------------------
# build_composite_score: validation
# ---------------------------------------------------------------------------


def test_composite_empty_specs_raises():
    df = pd.DataFrame({"a": [1.0, 2.0]})
    with pytest.raises(ValueError):
        build_composite_score(df, [])


def test_composite_missing_column_raises():
    df = pd.DataFrame({"a": [1.0, 2.0]})
    with pytest.raises(ValueError):
        build_composite_score(df, [SignalSpec("z", direction="asc")])


def test_composite_unequal_weights_normalised():
    """
    With weights 1.0 and 3.0, the second signal contributes 75% and
    the first 25%. Test by using identical components: the composite
    should equal the rank irrespective of weighting (because weighting
    different copies of the same thing collapses to the rank).
    """
    df = pd.DataFrame({"a": [1.0, 2.0, 3.0, 4.0], "b": [1.0, 2.0, 3.0, 4.0]})
    specs = [
        SignalSpec("a", direction="asc", weight=1.0),
        SignalSpec("b", direction="asc", weight=3.0),
    ]
    out = build_composite_score(df, specs)
    expected = percentile_rank(df["a"])
    pd.testing.assert_series_equal(out, expected, check_names=False)


# ---------------------------------------------------------------------------
# Sign alignment for the actual blend (smoke test)
# ---------------------------------------------------------------------------


def test_composite_sign_aligned_pct_of_mcap_and_moat():
    """
    The actual M_blend use case. pct_of_mcap is bullish ('asc'); moat
    is bearish ('desc'). A row that's high on pct_of_mcap and low on
    moat should get the highest composite. A row that's low on
    pct_of_mcap and high on moat should get the lowest.
    """
    df = pd.DataFrame(
        {
            "pct_of_mcap": [0.001, 0.002, 0.003, 0.004, 0.005],
            "moat_index": [1.0, 0.7, 0.5, 0.3, 0.1],  # descends
        }
    )
    specs = [
        SignalSpec("pct_of_mcap", direction="asc"),
        SignalSpec("moat_index", direction="desc"),
    ]
    out = build_composite_score(df, specs)
    # Row 4: highest pct_of_mcap (rank 5/6) and lowest moat (mirror -> 5/6).
    # Composite = (5/6 + 5/6) / 2 = 5/6.
    # Row 0: lowest pct_of_mcap (rank 1/6) and highest moat (mirror -> 1/6).
    # Composite = (1/6 + 1/6) / 2 = 1/6.
    assert out.iloc[0] == pytest.approx(1 / 6)
    assert out.iloc[-1] == pytest.approx(5 / 6)
    # Strictly increasing.
    assert list(out) == sorted(out)


# ---------------------------------------------------------------------------
# component_correlations
# ---------------------------------------------------------------------------


def test_component_correlations_diagonal_is_one():
    df = pd.DataFrame({"a": [1.0, 2.0, 3.0, 4.0, 5.0], "b": [5.0, 4.0, 3.0, 2.0, 1.0]})
    specs = [SignalSpec("a", direction="asc"), SignalSpec("b", direction="asc")]
    corr = component_correlations(df, specs)
    assert corr.loc["a", "a"] == pytest.approx(1.0)
    assert corr.loc["b", "b"] == pytest.approx(1.0)


def test_component_correlations_negative_for_anti_correlated():
    df = pd.DataFrame(
        {
            "a": [1.0, 2.0, 3.0, 4.0, 5.0],
            "b": [5.0, 4.0, 3.0, 2.0, 1.0],
        }
    )
    specs = [SignalSpec("a", direction="asc"), SignalSpec("b", direction="asc")]
    corr = component_correlations(df, specs)
    assert corr.loc["a", "b"] == pytest.approx(-1.0)


def test_component_correlations_handles_too_few_rows():
    df = pd.DataFrame({"a": [1.0], "b": [2.0]})
    specs = [SignalSpec("a", direction="asc"), SignalSpec("b", direction="asc")]
    corr = component_correlations(df, specs)
    assert pd.isna(corr.loc["a", "b"])
