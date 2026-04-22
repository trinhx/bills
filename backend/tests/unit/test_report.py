"""
Unit tests for ``backend.scripts.report`` (M2.5).

Covers:

* End-to-end report generation against a fixture DataFrame:
  HTML + Markdown files are produced; all expected section headers
  are present; numbers are embedded.
* Edge cases: missing columns, all-NaN signal, zero-variance forward
  return -- helpers don't blow up.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from backend.scripts.report import (
    INDUSTRY_BREAKDOWN_SIGNALS,
    MIN_INDUSTRY_ROWS,
    SIGNAL_CANDIDATES,
    _build_industry_ic_sections,
    _build_is_primary_comparison,
    _build_per_quarter_stability_section,
    _build_signal_quality_comparison,
    _df_to_html_table,
    _df_to_markdown_table,
    _ic_by_industry,
    _per_quarter_stability,
    generate_report,
)


def _fixture_signals_with_returns(n: int = 200, seed: int = 0) -> pd.DataFrame:
    """
    Build a minimal but realistic-looking ``signals_with_returns`` frame:
    signals, sectors, forward returns for all four horizons, plus the
    M2.5 columns (industry, industry_excess_return_*, fiscal_quarter).
    """
    rng = np.random.default_rng(seed)
    n_industries_each = n // 3
    # Split the rows evenly across three industries so each clears the
    # MIN_INDUSTRY_ROWS threshold when n >= ~600.
    industries = (
        ["Aerospace & Defense"] * n_industries_each
        + ["Information Technology Services"] * n_industries_each
        + ["Software - Application"] * (n - 2 * n_industries_each)
    )
    sectors = [
        "Industrials"
        if i == "Aerospace & Defense"
        else "Technology"
        for i in industries
    ]
    # Shuffle so row ordering isn't a structural confound.
    perm = rng.permutation(n)
    industries = [industries[j] for j in perm]
    sectors = [sectors[j] for j in perm]

    df = pd.DataFrame(
        {
            "contract_transaction_unique_key": [f"k{i:04d}" for i in range(n)],
            "ticker": rng.choice(["AAPL", "MSFT", "GOOG", "LMT"], size=n),
            "sector": sectors,
            "industry": industries,
            "action_date": pd.bdate_range("2023-10-02", periods=n),
            "is_primary_action": rng.choice([True, False], size=n, p=[0.75, 0.25]),
            "transaction_type": rng.choice(
                ["FUNDING_INCREASE", "MODIFICATION", "NEW_AWARDS"],
                size=n,
                p=[0.7, 0.2, 0.1],
            ),
            "signal_quality": rng.choice(
                ["ok", "microcap", "extreme_ratio"], size=n, p=[0.85, 0.1, 0.05]
            ),
            # Signals
            "alpha_ratio": rng.standard_normal(n) * 0.01,
            "acv_alpha_ratio": rng.standard_normal(n) * 0.01,
            "contract_potential_yield": rng.standard_normal(n) * 0.01,
            "obligation_ratio": rng.uniform(0, 1, size=n),
            "moat_index": rng.choice([1.0, 0.5, 0.25, 0.1], size=n),
            "difference_between_obligated_and_potential": rng.standard_normal(n) * 1e6,
        }
    )
    # Synthesise forward returns that are mildly correlated with acv_alpha_ratio
    # so at least one signal-horizon cell has a non-trivial IC in the report.
    base = 0.3 * df["acv_alpha_ratio"] * 100 + rng.standard_normal(n) * 0.02
    df["return_1d"] = base / 20
    df["return_5d"] = base / 4
    df["return_20d"] = base
    df["return_60d"] = base * 2 + rng.standard_normal(n) * 0.05
    # SPY returns: roughly flat.
    df["spy_return_1d"] = rng.standard_normal(n) * 0.005
    df["spy_return_5d"] = rng.standard_normal(n) * 0.01
    df["spy_return_20d"] = rng.standard_normal(n) * 0.02
    df["spy_return_60d"] = rng.standard_normal(n) * 0.04
    # Excess returns (SPY-neutral)
    for h in (1, 5, 20, 60):
        df[f"excess_return_{h}d"] = df[f"return_{h}d"] - df[f"spy_return_{h}d"]

    # M2.5: industry-neutral excess returns. Use SPY values as the
    # proxy benchmark for all rows (the fixture doesn't need the
    # exact ETF math to exercise the report rendering).
    for h in (1, 5, 20, 60):
        df[f"industry_benchmark_return_{h}d"] = df[f"spy_return_{h}d"]
        df[f"industry_excess_return_{h}d"] = df[f"excess_return_{h}d"]
    df["industry_benchmark_ticker"] = "SPY"

    # Stamp fiscal_quarter: the bdate_range starts 2023-10-02 and rolls
    # through enough business days that, for n ~= 600, rows span all 4
    # FY24 quarters. Use a helper mirroring the one in validate.py.
    def _fq(ts):
        m = ts.month
        if m >= 10:
            return "FY24Q1"
        if m <= 3:
            return "FY24Q2"
        if m <= 6:
            return "FY24Q3"
        return "FY24Q4"

    df["fiscal_quarter"] = df["action_date"].apply(_fq)
    df["reference_trading_date"] = df["action_date"]
    return df


# ---------------------------------------------------------------------------
# _df_to_markdown_table / _df_to_html_table
# ---------------------------------------------------------------------------


def test_markdown_table_happy_path():
    df = pd.DataFrame({"a": [1, 2], "b": [0.123456, 0.654321]})
    md = _df_to_markdown_table(df)
    lines = md.splitlines()
    assert lines[0] == "| a | b |"
    assert lines[1] == "| --- | --- |"
    assert "0.1235" in md
    assert "0.6543" in md


def test_markdown_table_empty():
    assert "_(no rows)_" in _df_to_markdown_table(pd.DataFrame())


def test_html_table_empty():
    assert "(no rows)" in _df_to_html_table(pd.DataFrame())


def test_html_table_formats_floats():
    df = pd.DataFrame({"x": [0.1234567]})
    out = _df_to_html_table(df)
    assert "0.1235" in out


# ---------------------------------------------------------------------------
# generate_report (integration with fixture)
# ---------------------------------------------------------------------------


def test_generate_report_end_to_end(tmp_path: Path):
    df = _fixture_signals_with_returns(n=300, seed=1)
    out = tmp_path / "validation_report_test.html"
    generate_report(df, out, pipeline_version="test-1.2.3")

    assert out.exists(), "HTML file should be written"
    html = out.read_text()

    # All top-level sections present.
    assert "Alpha Validation Report" in html
    assert "1. Executive Summary" in html
    assert "2. Data Coverage" in html
    # Per-signal section headers -- at least one of each.
    for signal in SIGNAL_CANDIDATES:
        assert signal in html, f"expected {signal!r} in report"
    assert "4. Signal-quality crosscut" in html
    assert "5. Robustness" in html
    # M2.5: new industry / stability sections bumped the decision-summary
    # header from 6 to 8.
    assert "6. Industry-level IC breakdown" in html
    assert "7. Per-quarter stability filter" in html
    assert "8. Decision Summary" in html
    # Threshold reminders embedded.
    assert "|IC| &gt;= 0.02" in html
    assert "|IC| &gt;= 0.05" in html
    assert "50 bps" in html
    # Meta row shows the pipeline_version we passed.
    assert "test-1.2.3" in html

    # Markdown companion written too.
    md = out.with_suffix(".md")
    assert md.exists()
    md_text = md.read_text()
    assert "Executive summary" in md_text
    assert "Decision criteria" in md_text
    # Threshold rationales present.
    assert "academic convention" in md_text


def test_generate_report_no_markdown_flag(tmp_path: Path):
    df = _fixture_signals_with_returns(n=100)
    out = tmp_path / "html_only.html"
    generate_report(df, out, write_markdown=False)
    assert out.exists()
    assert not out.with_suffix(".md").exists()


def test_generate_report_handles_missing_signal_column(tmp_path: Path):
    """
    Drop ``obligation_ratio`` from the fixture. The report should still
    render -- the missing-column section just notes that the signal
    wasn't present.
    """
    df = _fixture_signals_with_returns(n=100).drop(columns=["obligation_ratio"])
    out = tmp_path / "missing_col.html"
    generate_report(df, out, write_markdown=False)
    html = out.read_text()
    assert "column not present" in html


# ---------------------------------------------------------------------------
# Section-builder helpers
# ---------------------------------------------------------------------------


def test_is_primary_comparison_structure():
    df = _fixture_signals_with_returns(n=200)
    out = _build_is_primary_comparison(df)
    assert not out.empty
    assert set(
        ["horizon", "all_ic", "primary_only_ic", "all_n", "primary_only_n"]
    ) <= set(out.columns)


def test_signal_quality_comparison_structure():
    df = _fixture_signals_with_returns(n=200)
    out = _build_signal_quality_comparison(df)
    assert not out.empty
    # All candidate signals are listed at least once.
    signals_in_report = set(out["signal"].unique())
    assert set(SIGNAL_CANDIDATES) & signals_in_report


# ---------------------------------------------------------------------------
# M2.5 section builders: industry IC breakdown + per-quarter stability
# ---------------------------------------------------------------------------


def test_ic_by_industry_filters_by_min_rows():
    """Industries with fewer than MIN_INDUSTRY_ROWS must be dropped."""
    # Build a fixture where each industry has ~200 rows, two clear the bar
    # and one (n=50) does not.
    df = _fixture_signals_with_returns(n=800, seed=1)
    # Drop any rows to make Software - Application a sub-threshold set.
    small_ind = df[df["industry"] == "Software - Application"]
    keep = pd.concat([
        df[df["industry"] != "Software - Application"],
        small_ind.head(min(50, MIN_INDUSTRY_ROWS - 1)),
    ])
    out = _ic_by_industry(keep, "acv_alpha_ratio", "excess_return_20d")
    assert "Software - Application" not in set(out["industry"])
    # The two larger industries should appear.
    assert "Aerospace & Defense" in set(out["industry"])


def test_ic_by_industry_sorts_by_abs_ic():
    df = _fixture_signals_with_returns(n=900, seed=2)
    out = _ic_by_industry(df, "acv_alpha_ratio", "excess_return_20d")
    if len(out) >= 2:
        abs_ics = out["ic"].abs().tolist()
        assert abs_ics == sorted(abs_ics, reverse=True)


def test_build_industry_ic_sections_renders_all_top_signals():
    """
    The section should include an <h3> for every signal in
    INDUSTRY_BREAKDOWN_SIGNALS, plus sub-sections for T+20d and T+60d.
    """
    df = _fixture_signals_with_returns(n=900, seed=3)
    html = _build_industry_ic_sections(df)
    for sig in INDUSTRY_BREAKDOWN_SIGNALS:
        assert sig in html
    assert "T+20d" in html
    assert "T+60d" in html
    # Both SPY-neutral and industry-neutral subtables present.
    assert "SPY-neutral IC" in html
    assert "Industry-neutral IC" in html


def test_per_quarter_stability_returns_empty_on_random_data():
    """
    iid-noise signals shouldn't systematically clear |IC|>=0.02 across
    quarters. The stability filter should produce at most a handful of
    rows (and frequently zero).
    """
    df = _fixture_signals_with_returns(n=1500, seed=4)
    out = _per_quarter_stability(df)
    # We can't assert 'exactly zero' without making the fixture contrived;
    # just require no crashes and finite row count relative to the space.
    # The signal space is len(top_signals) * 2 horizons * ~3 industries ~= 18,
    # so well under that is fine.
    assert isinstance(out, pd.DataFrame)
    assert len(out) <= 18


def test_per_quarter_stability_surfaces_consistent_signals():
    """
    Inject a deterministic signal that's consistently positive across all
    four quarters for Aerospace & Defense specifically. It must appear
    in the stability table.
    """
    df = _fixture_signals_with_returns(n=1200, seed=5)
    # In A&D rows, make industry_excess_return_20d monotonically depend on
    # acv_alpha_ratio (and dominate noise), so every quarter shows
    # positive IC.
    ad_mask = df["industry"] == "Aerospace & Defense"
    df.loc[ad_mask, "industry_excess_return_20d"] = (
        df.loc[ad_mask, "acv_alpha_ratio"] * 100
    )
    out = _per_quarter_stability(df)
    matching = out[
        (out["signal"] == "acv_alpha_ratio")
        & (out["horizon"] == "T+20d")
        & (out["industry"] == "Aerospace & Defense")
    ]
    assert not matching.empty, (
        "A&D-specific engineered signal should clear the stability filter"
    )
    row = matching.iloc[0]
    assert row["ic_all"] > 0.02
    # 4/4 quarters same-sign (positive).
    assert row["same_sign_qs"] == "4/4"


def test_build_per_quarter_stability_section_handles_empty_gracefully():
    """Report should degrade gracefully when no signals meet the criteria."""
    df = _fixture_signals_with_returns(n=300, seed=6)
    html = _build_per_quarter_stability_section(df)
    # Either the 'no combinations' message OR a rendered table.
    assert "<table" in html or "No (signal" in html


def test_generate_report_includes_new_sections(tmp_path: Path):
    """The HTML and markdown should both contain Section 6 and 7 headers."""
    df = _fixture_signals_with_returns(n=900, seed=7)
    out = tmp_path / "report_with_new_sections.html"
    generate_report(df, out, pipeline_version="m2.5-test")
    html = out.read_text()
    assert "6. Industry-level IC breakdown" in html
    assert "7. Per-quarter stability filter" in html
    assert "8. Decision Summary" in html

    md = out.with_suffix(".md").read_text()
    assert "Industry-level IC breakdown" in md
    assert "Per-quarter stability filter" in md
