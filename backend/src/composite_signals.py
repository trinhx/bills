"""
Pure functions for building composite (blended) signal scores.

This module supports the M_blend milestone: combining multiple
individually-validated event-driven signals into a single composite
score that we then feed into the existing ``backtest_engine`` as if it
were any other signal column.

Design conventions:

* All transformations are **rank-based** (percentile in ``[0, 1]``).
  Spearman ICs are scale-invariant, and our outlier audits (notably
  the 295 absurd-magnitude rows in ``relative_ceiling_change``) make
  raw-value averaging unsafe. Ranking neutralises both issues.
* **Within-class ranking only.** Composites are computed within the
  MAJOR_EXPANSION subset; cross-class composites would mix populations
  with structurally different distributions.
* **Sign-aligning is explicit.** The caller passes a
  ``SignalSpec(column, direction)`` per input where ``direction='asc'``
  means "high signal -> bullish" and ``direction='desc'`` means
  "high signal -> bearish" (we flip the rank). We do not auto-detect
  signs from the ICs because that would tune the composite to the
  in-sample data.
* **Equal-weight is the default.** Any other weighting introduces a
  tuning parameter that can silently fit noise. The function accepts
  weights but the blend script always passes equal weights.

The single public function is ``build_composite_score`` which takes a
DataFrame plus a list of ``SignalSpec`` and returns a ``pd.Series`` of
percentile-rank-blended scores in ``[0, 1]``, NaN-preserving.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import List, Optional, Sequence

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Types
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class SignalSpec:
    """
    One input to a composite blend.

    ``column`` is the source column name in the DataFrame. ``direction``
    must be ``'asc'`` (high value = bullish) or ``'desc'`` (high value =
    bearish, e.g. ``moat_index``). ``weight`` is the relative weight of
    this signal in the blend; weights are normalised to sum to 1 inside
    ``build_composite_score``.
    """

    column: str
    direction: str  # 'asc' or 'desc'
    weight: float = 1.0

    def __post_init__(self) -> None:
        if self.direction not in ("asc", "desc"):
            raise ValueError(
                f"direction must be 'asc' or 'desc', got {self.direction!r}"
            )
        if self.weight <= 0:
            raise ValueError(f"weight must be positive, got {self.weight}")


# ---------------------------------------------------------------------------
# Percentile-rank transformation
# ---------------------------------------------------------------------------


def percentile_rank(
    values: pd.Series, *, direction: str = "asc"
) -> pd.Series:
    """
    Convert a numeric Series to percentile ranks in ``[0, 1]``.

    NaN-preserving: NaN inputs map to NaN outputs. Ties get the average
    rank (matches ``method='average'`` in ``Series.rank``). With
    ``direction='desc'`` the ranks are flipped (``1 - rank``) so high
    values on the original scale become *low* composite contributions.

    The output is in ``[0, 1]`` *strictly excluding the endpoints* for
    n > 1: lowest value gets ``1/(n+1)``, highest gets ``n/(n+1)``.
    This is the standard "fractional rank" convention; it avoids
    putting any single observation at exactly 0 or 1 which would
    distort an equal-weight average if NaNs are present in other
    components.
    """
    if direction not in ("asc", "desc"):
        raise ValueError(f"direction must be 'asc' or 'desc', got {direction!r}")
    out = values.rank(method="average", na_option="keep") / (values.notna().sum() + 1)
    if direction == "desc":
        # Flip: keep NaNs as NaN, but the non-NaN ranks are mirrored.
        mask = out.notna()
        out.loc[mask] = 1.0 - out.loc[mask]
    return out


# ---------------------------------------------------------------------------
# Composite construction
# ---------------------------------------------------------------------------


def build_composite_score(
    df: pd.DataFrame,
    specs: Sequence[SignalSpec],
    *,
    require_all_components: bool = True,
) -> pd.Series:
    """
    Build a per-row composite score from multiple signals.

    Each input column is converted to a percentile rank
    (``percentile_rank``) with the spec's direction applied, then the
    ranks are weighted-averaged. Output is a ``pd.Series`` aligned to
    ``df.index`` containing values in ``[0, 1]`` for rows with all
    component values present (or with at least one component when
    ``require_all_components=False``), and NaN otherwise.

    With ``require_all_components=True`` (default), a row needs every
    component to be non-null to receive a composite score. This is the
    safer default: a partial composite implicitly imputes missing
    components, which can bias the bucket assignment.

    With ``require_all_components=False``, any row with at least one
    non-null component gets a score formed from whatever components are
    available, with weights re-normalised over the available subset.
    Use only when you understand the missing-data structure.
    """
    if not specs:
        raise ValueError("specs must contain at least one SignalSpec")
    for spec in specs:
        if spec.column not in df.columns:
            raise ValueError(f"column {spec.column!r} not in DataFrame")

    # Build the ranked-and-signed components matrix. Use per-spec keys
    # rather than column names so that the same column can appear twice
    # with different directions/weights without overwriting itself.
    components = pd.DataFrame(index=df.index)
    for i, spec in enumerate(specs):
        key = f"__c{i}_{spec.column}_{spec.direction}"
        components[key] = percentile_rank(df[spec.column], direction=spec.direction)

    weights = np.array([spec.weight for spec in specs], dtype=float)
    weights = weights / weights.sum()

    if require_all_components:
        valid_mask = components.notna().all(axis=1)
        out = pd.Series(np.nan, index=df.index)
        if valid_mask.any():
            sub = components.loc[valid_mask]
            out.loc[valid_mask] = (sub.values * weights).sum(axis=1)
        return out

    # Component-by-component, re-normalised weights for partial rows.
    out = pd.Series(np.nan, index=df.index)
    arr = components.values  # shape (n, k)
    valid = ~np.isnan(arr)  # bool mask
    any_valid = valid.any(axis=1)
    if not any_valid.any():
        return out
    # Multiply each row's components by weights, masking out NaN, then
    # re-normalise by the weight that's actually present.
    weighted = np.where(valid, arr * weights, 0.0)
    weight_sum = (valid * weights).sum(axis=1)
    safe = weight_sum > 0
    summed = weighted.sum(axis=1)
    out_vals = np.where(safe, summed / np.where(safe, weight_sum, 1.0), np.nan)
    out.loc[any_valid] = out_vals[any_valid]
    return out


# ---------------------------------------------------------------------------
# Diagnostics
# ---------------------------------------------------------------------------


def component_correlations(
    df: pd.DataFrame, specs: Sequence[SignalSpec]
) -> pd.DataFrame:
    """
    Pairwise Spearman rank correlation between components.

    Returns a DataFrame indexed by component column names with the same
    columns. Useful for verifying components are not redundant before
    you trust a composite. Computed on the rows where every component
    is non-null (i.e., the same population the composite is built on
    when ``require_all_components=True``).
    """
    cols = [spec.column for spec in specs]
    sub = df[cols].dropna()
    if len(sub) < 3:
        return pd.DataFrame(np.nan, index=cols, columns=cols)
    return sub.corr(method="spearman")
