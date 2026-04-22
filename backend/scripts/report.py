"""
Phase 6 — HTML/Markdown report generator for the alpha-validation harness (M2.5).

Reads the ``signals_with_returns`` table produced by ``validate.py`` and
renders a self-contained HTML report + sibling Markdown file under
``backend/data/analysis/validation_report_<YYYYMMDD>.html``.

Report sections (see the M2 plan for details):

    1. Executive Summary   -- top-of-report signal x horizon IC / spread table
    2. Data Coverage       -- row counts per signal, per sector, per horizon
    3. Per-signal detail   -- one section per candidate signal, with plots
    4. Signal-quality      -- IC restricted to signal_quality = 'ok' rows
    5. Robustness          -- IC by transaction_type, by sector, CI on IC
    6. Decision summary    -- three threshold criteria, signals clearing each

Plots are embedded as base64-encoded PNGs so the HTML file is single-
self-contained: you can email/scp it around with no asset directory.

Idempotent: re-running overwrites only the report for today's date.
``--date 20250101`` override is supported for backfills.

Usage:
    uv run --env-file .env backend/scripts/report.py
    uv run --env-file .env backend/scripts/report.py --date 20260420
    uv run --env-file .env backend/scripts/report.py --output my_report.html
"""

from __future__ import annotations

import argparse
import base64
import io
import logging
from datetime import date, datetime
from html import escape as _html_escape
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import duckdb
import matplotlib

matplotlib.use("Agg")  # non-interactive backend for headless use
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from backend.src.analyze import (
    cumulative_pnl,
    decile_spread,
    ic_per_sector,
    information_coefficient,
    signal_coverage,
    summarize_all_signals,
    top_minus_bottom,
)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

SIGNAL_CANDIDATES: List[str] = [
    "alpha_ratio",
    "acv_alpha_ratio",
    "contract_potential_yield",
    "obligation_ratio",
    "moat_index",
    "difference_between_obligated_and_potential",
]

HORIZONS: List[int] = [1, 5, 20, 60]

# Return column prefix used by validate.py
EXCESS_PREFIX = "excess_return_"
RETURN_PREFIX = "return_"


# ---------------------------------------------------------------------------
# Plot helpers (all return base64 PNG strings)
# ---------------------------------------------------------------------------


def _fig_to_base64(fig: plt.Figure) -> str:
    """Serialise a matplotlib figure to a base64-encoded PNG data URI."""
    buf = io.BytesIO()
    fig.savefig(buf, format="png", bbox_inches="tight", dpi=120)
    plt.close(fig)
    buf.seek(0)
    return f"data:image/png;base64,{base64.b64encode(buf.read()).decode()}"


def plot_ic_by_horizon(df: pd.DataFrame, signal: str, horizons: Iterable[int]) -> str:
    """Bar chart: IC at each forward-return horizon."""
    xs: List[str] = []
    ys: List[float] = []
    ns: List[int] = []
    for h in horizons:
        ret_col = f"{EXCESS_PREFIX}{h}d"
        if ret_col not in df.columns:
            continue
        ic = information_coefficient(df[signal], df[ret_col])
        xs.append(f"T+{h}d")
        ys.append(0 if ic["ic"] is None else ic["ic"])
        ns.append(ic["n"])

    fig, ax = plt.subplots(figsize=(6, 3.2))
    colors = ["#2ecc71" if v >= 0 else "#e74c3c" for v in ys]
    ax.bar(xs, ys, color=colors, edgecolor="black", linewidth=0.5)
    for i, (v, n) in enumerate(zip(ys, ns)):
        ax.text(
            i, v, f"n={n}", ha="center", va="bottom" if v >= 0 else "top", fontsize=8
        )
    ax.axhline(0, color="black", linewidth=0.6)
    ax.set_ylabel("Information Coefficient (Spearman)")
    ax.set_title(f"{signal} — IC by horizon")
    ax.grid(axis="y", alpha=0.3)
    return _fig_to_base64(fig)


def plot_decile_spread(
    df: pd.DataFrame, signal: str, horizon: int, n_buckets: int = 10
) -> str:
    """Mean-forward-return per decile, with error bars."""
    ret_col = f"{EXCESS_PREFIX}{horizon}d"
    if ret_col not in df.columns:
        return ""
    spread = decile_spread(df, signal, ret_col, n_buckets=n_buckets)
    fig, ax = plt.subplots(figsize=(6, 3.2))
    if not spread.empty:
        ax.bar(
            spread["bucket"],
            spread["mean_return"],
            yerr=spread["std_return"] / np.sqrt(spread["n"].clip(lower=1)),
            color="#3498db",
            edgecolor="black",
            linewidth=0.5,
            capsize=3,
        )
    ax.axhline(0, color="black", linewidth=0.6)
    ax.set_xlabel(f"Signal decile (1 = low, {n_buckets} = high)")
    ax.set_ylabel(f"Mean excess_return_{horizon}d (+/- SE)")
    ax.set_title(f"{signal} — decile spread at T+{horizon}d")
    ax.grid(axis="y", alpha=0.3)
    return _fig_to_base64(fig)


def plot_cumulative_pnl(df: pd.DataFrame, signal: str, horizon: int) -> str:
    """Long-top-decile vs long-short cumulative P&L curves."""
    ret_col = f"{EXCESS_PREFIX}{horizon}d"
    if ret_col not in df.columns:
        return ""
    top = cumulative_pnl(df, signal, ret_col, strategy="long_top_decile")
    ls = cumulative_pnl(df, signal, ret_col, strategy="long_short")
    fig, ax = plt.subplots(figsize=(6, 3.2))
    if not top.empty:
        ax.plot(
            top.index,
            top.values,
            label="Long top decile",
            color="#2ecc71",
            linewidth=1.5,
        )
    if not ls.empty:
        ax.plot(
            ls.index,
            ls.values,
            label="Long-short",
            color="#9b59b6",
            linewidth=1.5,
            linestyle="--",
        )
    ax.axhline(0, color="black", linewidth=0.6)
    ax.set_xlabel("Reference trading date")
    ax.set_ylabel(f"Cumulative excess_return_{horizon}d")
    ax.set_title(f"{signal} — cumulative P&L at T+{horizon}d")
    ax.grid(alpha=0.3)
    # Only draw legend if at least one strategy contributed a curve; avoids
    # matplotlib's "No artists with labels" warning on sparse fixtures.
    if ax.get_legend_handles_labels()[0]:
        ax.legend(loc="best", fontsize=8)
    fig.autofmt_xdate()
    return _fig_to_base64(fig)


def plot_ic_heatmap_by_sector(
    df: pd.DataFrame, signal: str, horizons: Iterable[int]
) -> str:
    """Heatmap of IC by sector x horizon."""
    horizons = list(horizons)
    # Collect per-sector IC at each horizon.
    rows: Dict[str, Dict[str, Optional[float]]] = {}
    for h in horizons:
        ret_col = f"{EXCESS_PREFIX}{h}d"
        if ret_col not in df.columns:
            continue
        per = ic_per_sector(df, signal, ret_col, min_rows_per_sector=30)
        for _, r in per.iterrows():
            rows.setdefault(r["sector"], {})[f"T+{h}d"] = r["ic"]
    if not rows:
        return ""
    mat = pd.DataFrame(rows).T.reindex(columns=[f"T+{h}d" for h in horizons])
    fig, ax = plt.subplots(figsize=(6, max(2.5, 0.4 * len(mat) + 1)))
    im = ax.imshow(
        mat.values,
        cmap="RdYlGn",
        vmin=-0.15,
        vmax=0.15,
        aspect="auto",
    )
    ax.set_xticks(range(len(mat.columns)))
    ax.set_xticklabels(mat.columns)
    ax.set_yticks(range(len(mat.index)))
    ax.set_yticklabels(mat.index, fontsize=8)
    for i in range(mat.shape[0]):
        for j in range(mat.shape[1]):
            v = mat.values[i, j]
            if pd.notna(v):
                ax.text(
                    j,
                    i,
                    f"{v:.2f}",
                    ha="center",
                    va="center",
                    color="black",
                    fontsize=7,
                )
    ax.set_title(f"{signal} — IC by sector x horizon")
    fig.colorbar(im, ax=ax, label="IC")
    return _fig_to_base64(fig)


# ---------------------------------------------------------------------------
# HTML rendering
# ---------------------------------------------------------------------------


def _df_to_html_table(df: pd.DataFrame, float_fmt: str = "{:.4f}") -> str:
    """Pretty-print a DataFrame as HTML with numeric formatting."""
    if df is None or df.empty:
        return "<p><em>(no rows)</em></p>"
    # Copy so we don't mutate the caller's frame.
    pretty = df.copy()
    for col in pretty.columns:
        if pd.api.types.is_float_dtype(pretty[col]):
            pretty[col] = pretty[col].apply(
                lambda x: float_fmt.format(x) if pd.notna(x) else ""
            )
    return pretty.to_html(index=False, classes="data-table", border=0, escape=False)


def _df_to_markdown_table(df: pd.DataFrame, float_fmt: str = "{:.4f}") -> str:
    """
    Simple markdown-table renderer. Avoids the ``tabulate`` dependency
    that ``pandas.to_markdown`` requires. Produces the GFM-style pipe
    format. All cells are defensively coerced to strings, with NaN /
    ``None`` rendered as empty.
    """
    if df is None or df.empty:
        return "_(no rows)_\n"

    def _render_cell(x) -> str:
        if x is None or (isinstance(x, float) and pd.isna(x)):
            return ""
        if isinstance(x, float):
            return float_fmt.format(x)
        s = str(x)
        return "" if s in ("nan", "<NA>", "NaT") else s

    pretty = df.map(_render_cell) if hasattr(df, "map") else df.applymap(_render_cell)

    header = "| " + " | ".join(str(c) for c in pretty.columns) + " |"
    sep = "| " + " | ".join("---" for _ in pretty.columns) + " |"
    rows = ["| " + " | ".join(row) + " |" for row in pretty.values.tolist()]
    return "\n".join([header, sep, *rows])


def _decision_rows(summary: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """
    Build three DataFrames, one per decision criterion, listing the
    (signal, horizon) combinations that clear that threshold.
    """
    criteria = {}
    # Criterion 1: |IC| >= 0.02
    c1 = summary.dropna(subset=["ic"]).query("abs(ic) >= 0.02")
    criteria["|IC| >= 0.02"] = c1
    # Criterion 2: |IC| >= 0.05
    c2 = summary.dropna(subset=["ic"]).query("abs(ic) >= 0.05")
    criteria["|IC| >= 0.05"] = c2
    # Criterion 3: |IC| >= 0.02 AND |top_minus_bottom| >= 0.005 (50 bps)
    c3 = summary.dropna(subset=["ic", "top_minus_bottom"])
    c3 = c3.query("abs(ic) >= 0.02 and abs(top_minus_bottom) >= 0.005")
    criteria["|IC| >= 0.02 AND |top-minus-bottom| >= 50 bps"] = c3
    return criteria


_HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Alpha Validation Report — {date_stamp}</title>
<style>
  body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
          max-width: 1100px; margin: 2rem auto; padding: 0 1rem; color: #222; }}
  h1 {{ border-bottom: 2px solid #333; padding-bottom: 0.4rem; }}
  h2 {{ margin-top: 2rem; color: #333; border-bottom: 1px solid #ccc;
        padding-bottom: 0.3rem; }}
  h3 {{ margin-top: 1.5rem; color: #555; }}
  .data-table {{ border-collapse: collapse; margin: 0.5rem 0; }}
  .data-table th, .data-table td {{
    border: 1px solid #ccc; padding: 4px 10px; text-align: right; font-size: 0.9rem; }}
  .data-table th {{ background: #f2f2f2; }}
  .data-table td:first-child, .data-table th:first-child {{ text-align: left; }}
  .meta {{ color: #666; font-size: 0.9rem; }}
  .decision-box {{ background: #f8f8f8; border-left: 4px solid #3498db;
                   padding: 1rem 1.4rem; margin: 1rem 0; }}
  .caveats {{ background: #fff8e1; border-left: 4px solid #f1c40f;
              padding: 0.8rem 1.2rem; margin: 1rem 0; font-size: 0.9rem; }}
  img {{ max-width: 100%; height: auto; margin: 0.5rem 0; }}
  code {{ background: #eee; padding: 1px 4px; border-radius: 3px; }}
</style>
</head>
<body>
<h1>Alpha Validation Report</h1>
<p class="meta">
  Generated: {generated_at} &middot; Pipeline version: {pipeline_version}<br>
  Source: <code>cleaned.duckdb::signals_with_returns</code> &middot;
  {row_count:,} rows &middot; {unique_tickers} unique tickers &middot;
  action_date range [{action_date_min}, {action_date_max}]
</p>

<div class="caveats">
<strong>Known caveats for this run:</strong>
<ul>
  <li>Single-quarter window of action dates; short backtest means wide CIs on every IC.</li>
  <li>T+60 returns are only available for rows with action_date sufficiently far before the last cached bar; sample sizes shrink at longer horizons.</li>
  <li>~35% of raw signals rows are publicly resolved; alpha tests are conditional on public resolution.</li>
  <li>Benchmark is SPY only; no sector-factor neutralization yet (defense/aerospace dominates the universe).</li>
</ul>
</div>

<h2>1. Executive Summary</h2>
<p>IC and top-minus-bottom spread for every signal at every horizon.
   Look for both statistical significance (|IC|) and economic magnitude
   (|spread|). Cells with <code>n &lt; 100</code> should be treated
   cautiously.</p>
{executive_summary}

<h2>2. Data Coverage</h2>
<h3>Signal coverage</h3>
{signal_coverage}

<h3>Horizon sample sizes</h3>
{horizon_sample_sizes}

<h3>signal_quality distribution</h3>
{signal_quality_dist}

<h3>is_primary_action filter comparison</h3>
<p>IC for <code>acv_alpha_ratio</code> with/without the
<code>is_primary_action = True</code> filter. If the filter improves
IC, primary-action events are the signal-bearing subset.</p>
{is_primary_comparison}

{per_signal_sections}

<h2>4. Signal-quality crosscut</h2>
<p>Does restricting to <code>signal_quality = 'ok'</code> rows (drops
microcap/extreme_ratio/stale_shares) change the story?</p>
{signal_quality_comparison}

<h2>5. Robustness</h2>
<h3>IC by transaction_type</h3>
{ic_by_transaction_type}

<h2>6. Industry-level IC breakdown</h2>
<p>The sector-level IC above aggregates across many industries. Because
our universe is ~77% government-prime contractors split between
<em>Aerospace &amp; Defense</em> (Industrials) and <em>Information
Technology Services</em> (Technology), coarse sector averages can hide
industry-specific signal. The tables below drill down to the Yahoo
<code>industry</code> field for the top candidate signals.</p>
<p>Industries with fewer than {min_industry_rows} rows are omitted (too
noisy to report). "Ind-excess" columns use the row's industry-appropriate
benchmark (ITA for Aerospace &amp; Defense, XLK for IT Services, SPY for
everything else) instead of the broad SPY; "SPY-excess" is shown
side-by-side for comparison.</p>
{industry_ic_sections}

<h2>7. Per-quarter stability filter</h2>
<p>A workable signal should hold its sign across different market
regimes. This section identifies (signal, horizon, industry) combinations
where the IC is the same sign in at least 3 of 4 fiscal quarters AND
the overall |IC| clears the 0.02 threshold. These are the surviving
candidates.</p>
{per_quarter_stability}

<h2>8. Decision Summary</h2>
<p>Three threshold criteria are reported side-by-side. The report does
not auto-pick a verdict; the final call is the reviewer's.</p>

{decision_sections}

<div class="decision-box">
<strong>Reminder of threshold rationales:</strong>
<ol>
  <li><strong>|IC| &gt;= 0.02 in any segment</strong> — academic convention for a 'workable' alpha signal. Low bar, realistic for a short one-quarter sample. Surfaces directional evidence without overclaiming.</li>
  <li><strong>|IC| &gt;= 0.05 in any segment</strong> — stricter. A signal must be genuinely predictive to clear this. Risks false negatives on weak-but-real signals.</li>
  <li><strong>|IC| &gt;= 0.02 AND |top-minus-bottom| &gt;= 50 bps</strong> — two-factor test: statistical significance AND economic magnitude. More defensible go-signal but harder to clear.</li>
</ol>
</div>

</body>
</html>
"""


def _build_horizon_sample_sizes(df: pd.DataFrame) -> pd.DataFrame:
    """Per-horizon non-null return counts."""
    rows = []
    for h in HORIZONS:
        ret_col = f"{EXCESS_PREFIX}{h}d"
        if ret_col not in df.columns:
            continue
        rows.append(
            {
                "horizon": f"T+{h}d",
                "n_with_return": int(df[ret_col].notna().sum()),
                "pct_of_total": round(
                    100.0 * df[ret_col].notna().sum() / max(len(df), 1), 2
                ),
            }
        )
    return pd.DataFrame(rows)


def _build_is_primary_comparison(
    df: pd.DataFrame, signal: str = "acv_alpha_ratio"
) -> pd.DataFrame:
    """Compare IC with/without the is_primary_action filter."""
    rows = []
    if "is_primary_action" not in df.columns or signal not in df.columns:
        return pd.DataFrame()
    for h in HORIZONS:
        ret_col = f"{EXCESS_PREFIX}{h}d"
        if ret_col not in df.columns:
            continue
        all_ic = information_coefficient(df[signal], df[ret_col])
        sub = df[df["is_primary_action"] == True]
        prim_ic = information_coefficient(sub[signal], sub[ret_col])
        rows.append(
            {
                "horizon": f"T+{h}d",
                "all_ic": all_ic["ic"],
                "all_n": all_ic["n"],
                "primary_only_ic": prim_ic["ic"],
                "primary_only_n": prim_ic["n"],
            }
        )
    return pd.DataFrame(rows)


def _build_signal_quality_comparison(df: pd.DataFrame) -> pd.DataFrame:
    """For each signal, compare IC on all rows vs signal_quality='ok' rows."""
    rows = []
    for signal in SIGNAL_CANDIDATES:
        if signal not in df.columns:
            continue
        for h in HORIZONS:
            ret_col = f"{EXCESS_PREFIX}{h}d"
            if ret_col not in df.columns:
                continue
            all_ic = information_coefficient(df[signal], df[ret_col])
            ok = df[df.get("signal_quality", "") == "ok"]
            ok_ic = information_coefficient(ok[signal], ok[ret_col])
            rows.append(
                {
                    "signal": signal,
                    "horizon": f"T+{h}d",
                    "all_ic": all_ic["ic"],
                    "all_n": all_ic["n"],
                    "ok_ic": ok_ic["ic"],
                    "ok_n": ok_ic["n"],
                }
            )
    return pd.DataFrame(rows)


def _build_ic_by_transaction_type(df: pd.DataFrame) -> pd.DataFrame:
    """IC at T+20 broken down by transaction_type bucket."""
    if "transaction_type" not in df.columns or "acv_alpha_ratio" not in df.columns:
        return pd.DataFrame()
    rows = []
    for tt, sub in df.groupby("transaction_type", dropna=False):
        key = "(NULL)" if pd.isna(tt) else tt
        if "excess_return_20d" not in sub.columns:
            continue
        ic = information_coefficient(sub["acv_alpha_ratio"], sub["excess_return_20d"])
        if ic["n"] is None or ic["n"] < 20:
            continue
        rows.append({"transaction_type": key, "n": ic["n"], "ic_T+20d": ic["ic"]})
    if not rows:
        return pd.DataFrame(columns=["transaction_type", "n", "ic_T+20d"])
    return pd.DataFrame(rows).sort_values("n", ascending=False).reset_index(drop=True)


def _build_per_signal_section(df: pd.DataFrame, signal: str) -> str:
    """Render one per-signal HTML block with its plots."""
    if signal not in df.columns:
        return (
            f"<h3>{_html_escape(signal)}</h3>"
            f"<p><em>(column not present in data)</em></p>"
        )

    ic_bar = plot_ic_by_horizon(df, signal, HORIZONS)
    heatmap = plot_ic_heatmap_by_sector(df, signal, HORIZONS)
    decile20 = plot_decile_spread(df, signal, 20)
    pnl20 = plot_cumulative_pnl(df, signal, 20)

    parts: List[str] = [f"<h3>{_html_escape(signal)}</h3>"]
    parts.append(f'<p><img src="{ic_bar}" alt="IC by horizon"></p>')
    if heatmap:
        parts.append(f'<p><img src="{heatmap}" alt="IC by sector x horizon"></p>')
    if decile20:
        parts.append(f'<p><img src="{decile20}" alt="Decile spread T+20d"></p>')
    if pnl20:
        parts.append(f'<p><img src="{pnl20}" alt="Cumulative P&L T+20d"></p>')
    return "\n".join(parts)


# ---------------------------------------------------------------------------
# Industry-level and per-quarter breakdowns (M2.5)
# ---------------------------------------------------------------------------

#: Top candidate signals surfaced in the industry-breakdown section.
#: These are the ones the prior full-year report flagged as having the
#: largest |IC|; extending the analysis to every signal would bloat the
#: report without adding diagnostic value.
INDUSTRY_BREAKDOWN_SIGNALS: List[str] = [
    "difference_between_obligated_and_potential",
    "acv_alpha_ratio",
    "moat_index",
]

#: Industries with fewer rows than this are dropped from the industry
#: breakdown (too noisy to report IC on).
MIN_INDUSTRY_ROWS: int = 200


def _ic_by_industry(
    df: pd.DataFrame,
    signal_col: str,
    return_col: str,
    min_rows: int = MIN_INDUSTRY_ROWS,
) -> pd.DataFrame:
    """
    Spearman IC broken down by Yahoo ``industry``. Returns one row per
    industry with at least ``min_rows`` non-NaN pairs, sorted by |IC|.
    """
    if "industry" not in df.columns:
        return pd.DataFrame()
    rows = []
    for ind, sub in df.groupby("industry", dropna=False):
        key = "UNKNOWN" if pd.isna(ind) else ind
        ic = information_coefficient(sub[signal_col], sub[return_col])
        if ic["n"] is None or ic["n"] < min_rows:
            continue
        rows.append(
            {
                "industry": key,
                "n": ic["n"],
                "ic": ic["ic"],
                "p_value": ic["p_value"],
            }
        )
    if not rows:
        return pd.DataFrame(columns=["industry", "n", "ic", "p_value"])
    return pd.DataFrame(rows).sort_values(
        "ic", key=lambda s: s.abs(), ascending=False
    ).reset_index(drop=True)


def _build_industry_ic_sections(df: pd.DataFrame) -> str:
    """
    Render the Section 6 body: for each of the top candidate signals,
    show two side-by-side tables (SPY-neutral and industry-neutral IC
    by industry) at T+20 and T+60.
    """
    chunks: List[str] = []
    for sig in INDUSTRY_BREAKDOWN_SIGNALS:
        if sig not in df.columns:
            chunks.append(
                f"<h3>{_html_escape(sig)}</h3>"
                f"<p><em>(signal column not present)</em></p>"
            )
            continue
        chunks.append(f"<h3>{_html_escape(sig)}</h3>")
        for h in (20, 60):
            spy_col = f"excess_return_{h}d"
            ind_col = f"industry_excess_return_{h}d"
            chunks.append(f"<h4>T+{h}d</h4>")
            if spy_col in df.columns:
                spy_tbl = _ic_by_industry(df, sig, spy_col)
                chunks.append("<p><strong>SPY-neutral IC by industry:</strong></p>")
                chunks.append(_df_to_html_table(spy_tbl))
            if ind_col in df.columns:
                ind_tbl = _ic_by_industry(df, sig, ind_col)
                chunks.append(
                    "<p><strong>Industry-neutral IC by industry</strong> "
                    "(own-industry ETF is subtracted, so A&amp;D rows are "
                    "ITA-neutral, IT-Services rows are XLK-neutral, etc.):</p>"
                )
                chunks.append(_df_to_html_table(ind_tbl))
    return "\n".join(chunks)


def _per_quarter_stability(
    df: pd.DataFrame,
    signals: Iterable[str] = INDUSTRY_BREAKDOWN_SIGNALS,
    horizons: Iterable[int] = (20, 60),
    return_prefix: str = "industry_excess_return_",
    min_rows: int = MIN_INDUSTRY_ROWS,
    consistency_quarters: int = 3,
) -> pd.DataFrame:
    """
    For each (signal, horizon, industry) combo, compute the IC in each
    fiscal quarter AND the all-quarters IC. Retain only combos where:

    * The all-quarters |IC| >= 0.02, AND
    * The IC is same-signed in at least ``consistency_quarters`` of the
      four quarters.

    ``return_prefix`` lets the caller choose SPY-excess
    (``excess_return_``) or industry-excess (``industry_excess_return_``);
    we use industry-excess by default because that's the cleaner
    post-sector-factor view.
    """
    if "industry" not in df.columns or "fiscal_quarter" not in df.columns:
        return pd.DataFrame()

    # Auto-detect which fiscal quarters appear in the data so the filter
    # adapts naturally to multi-year datasets (FY2024 only, FY2024+FY2025,
    # etc.). Quarters are sorted so the output column order is stable.
    quarters = sorted(
        q for q in df["fiscal_quarter"].dropna().unique() if q != "UNKNOWN"
    )
    n_quarters = len(quarters)
    # If the default 3-of-4 threshold makes no sense (e.g. only 2 quarters
    # in the data), scale it down proportionally: keep at least 75% same-sign.
    required_same_sign = (
        consistency_quarters if n_quarters >= 4 else max(1, int(round(n_quarters * 0.75)))
    )

    rows: List[Dict] = []
    for signal in signals:
        if signal not in df.columns:
            continue
        for h in horizons:
            return_col = f"{return_prefix}{h}d"
            if return_col not in df.columns:
                continue
            for industry, sub in df.groupby("industry", dropna=False):
                if pd.isna(industry):
                    continue
                if len(sub) < min_rows:
                    continue
                overall = information_coefficient(sub[signal], sub[return_col])
                overall_ic = overall["ic"]
                if overall_ic is None or abs(overall_ic) < 0.02:
                    continue
                # Per-quarter IC
                q_ics = {}
                for q in quarters:
                    qsub = sub[sub["fiscal_quarter"] == q]
                    qic = information_coefficient(qsub[signal], qsub[return_col])
                    q_ics[q] = qic["ic"]
                # Count quarters with the same sign as the overall IC.
                target_sign = 1 if overall_ic > 0 else -1
                same_sign_count = sum(
                    1
                    for v in q_ics.values()
                    if v is not None
                    and ((v > 0 and target_sign > 0) or (v < 0 and target_sign < 0))
                )
                if same_sign_count < required_same_sign:
                    continue
                rows.append(
                    {
                        "signal": signal,
                        "horizon": f"T+{h}d",
                        "industry": industry,
                        "n": overall["n"],
                        "ic_all": overall_ic,
                        "same_sign_qs": f"{same_sign_count}/{n_quarters}",
                        **{f"ic_{q}": q_ics[q] for q in quarters},
                    }
                )
    if not rows:
        base_cols = ["signal", "horizon", "industry", "n", "ic_all", "same_sign_qs"]
        return pd.DataFrame(columns=base_cols + [f"ic_{q}" for q in quarters])
    return pd.DataFrame(rows).sort_values(
        "ic_all", key=lambda s: s.abs(), ascending=False
    ).reset_index(drop=True)


def _build_per_quarter_stability_section(df: pd.DataFrame) -> str:
    """Render the Section 7 body: the stability-filter table."""
    table = _per_quarter_stability(df)
    if table.empty:
        return (
            "<p><em>No (signal, horizon, industry) combinations meet the "
            "stability criteria on this dataset.</em></p>"
        )
    return _df_to_html_table(table)


def _build_decision_sections(summary: pd.DataFrame) -> str:
    """Render the three criteria tables for section 6."""
    criteria = _decision_rows(summary)
    chunks = []
    for label, matching in criteria.items():
        chunks.append(f"<h3>{_html_escape(label)}</h3>")
        if matching.empty:
            chunks.append(
                "<p><em>No signal/horizon combinations clear this threshold.</em></p>"
            )
        else:
            display = matching[
                ["signal", "horizon", "n", "ic", "top_minus_bottom"]
            ].copy()
            display = display.sort_values("ic", key=lambda s: s.abs(), ascending=False)
            chunks.append(_df_to_html_table(display))
    return "\n".join(chunks)


# ---------------------------------------------------------------------------
# Main report generation
# ---------------------------------------------------------------------------


def generate_report(
    df: pd.DataFrame,
    output_path: Path,
    *,
    pipeline_version: str = "unknown",
    generated_at: Optional[datetime] = None,
    write_markdown: bool = True,
) -> Path:
    """
    Render the full HTML report for ``df`` and write it to
    ``output_path``. Returns the output path.

    ``df`` must be the ``signals_with_returns`` table -- specifically it
    must contain the signal-candidate columns and the excess_return_Nd
    columns produced by ``validate.py``.
    """
    generated_at = generated_at or datetime.now()

    # Executive summary
    summary = summarize_all_signals(df, SIGNAL_CANDIDATES, HORIZONS)
    # Coverage + distributions
    coverage = signal_coverage(df, SIGNAL_CANDIDATES)
    horizon_ns = _build_horizon_sample_sizes(df)
    sq_dist = (
        df["signal_quality"]
        .value_counts(dropna=False)
        .head(15)
        .rename_axis("signal_quality")
        .reset_index(name="n")
        if "signal_quality" in df.columns
        else pd.DataFrame()
    )
    primary_comp = _build_is_primary_comparison(df)
    sq_cross = _build_signal_quality_comparison(df)
    ic_by_tt = _build_ic_by_transaction_type(df)

    # M2.5 sections: industry-level IC and per-quarter stability.
    industry_ic_html = _build_industry_ic_sections(df)
    per_quarter_stability_html = _build_per_quarter_stability_section(df)

    # Per-signal sections
    per_signal_html = []
    for signal in SIGNAL_CANDIDATES:
        per_signal_html.append("<h2>3. " + _html_escape(signal) + "</h2>")
        per_signal_html.append(_build_per_signal_section(df, signal))

    # Context
    action_dates = (
        pd.to_datetime(df["action_date"])
        if "action_date" in df.columns
        else pd.Series(dtype="datetime64[ns]")
    )
    ad_min = action_dates.min().date().isoformat() if not action_dates.empty else "?"
    ad_max = action_dates.max().date().isoformat() if not action_dates.empty else "?"
    unique_tickers = df["ticker"].nunique() if "ticker" in df.columns else 0

    html = _HTML_TEMPLATE.format(
        date_stamp=generated_at.strftime("%Y-%m-%d"),
        generated_at=generated_at.strftime("%Y-%m-%d %H:%M:%S"),
        pipeline_version=_html_escape(pipeline_version),
        row_count=len(df),
        unique_tickers=unique_tickers,
        action_date_min=ad_min,
        action_date_max=ad_max,
        executive_summary=_df_to_html_table(summary),
        signal_coverage=_df_to_html_table(coverage),
        horizon_sample_sizes=_df_to_html_table(horizon_ns),
        signal_quality_dist=_df_to_html_table(sq_dist)
        if not sq_dist.empty
        else "<p><em>(no signal_quality column)</em></p>",
        is_primary_comparison=_df_to_html_table(primary_comp),
        per_signal_sections="\n".join(per_signal_html),
        signal_quality_comparison=_df_to_html_table(sq_cross),
        ic_by_transaction_type=_df_to_html_table(ic_by_tt),
        industry_ic_sections=industry_ic_html,
        per_quarter_stability=per_quarter_stability_html,
        min_industry_rows=MIN_INDUSTRY_ROWS,
        decision_sections=_build_decision_sections(summary),
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(html, encoding="utf-8")

    # Companion markdown (executive summary + decision sections only).
    if write_markdown:
        md_path = output_path.with_suffix(".md")
        md = []
        md.append(f"# Alpha Validation Report — {generated_at.strftime('%Y-%m-%d')}\n")
        md.append(
            f"**Rows:** {len(df):,}  **Tickers:** {unique_tickers}  "
            f"**action_date:** [{ad_min}, {ad_max}]  "
            f"**pipeline_version:** {pipeline_version}\n"
        )
        md.append("## Executive summary\n")
        md.append(_df_to_markdown_table(summary))

        # M2.5: industry-level IC breakdown for top candidate signals,
        # at T+20 and T+60, showing both SPY-neutral and industry-neutral.
        md.append("\n\n## Industry-level IC breakdown\n")
        md.append(
            "Industries with fewer than "
            f"{MIN_INDUSTRY_ROWS} rows are omitted. 'Ind-excess' rows use "
            "the industry-appropriate benchmark (ITA for Aerospace & Defense, "
            "XLK for IT Services, SPY for everything else).\n"
        )
        for sig in INDUSTRY_BREAKDOWN_SIGNALS:
            if sig not in df.columns:
                continue
            for h in (20, 60):
                ind_col = f"industry_excess_return_{h}d"
                if ind_col not in df.columns:
                    continue
                tbl = _ic_by_industry(df, sig, ind_col)
                if tbl.empty:
                    continue
                md.append(f"\n### {sig} @ T+{h}d (industry-neutral)\n")
                md.append(_df_to_markdown_table(tbl))

        # M2.5: per-quarter stability filter
        md.append("\n\n## Per-quarter stability filter\n")
        md.append(
            "(signal, horizon, industry) combos with |IC| >= 0.02 AND "
            "same sign in >= 3 of 4 fiscal quarters. Uses "
            "industry-neutral excess returns.\n"
        )
        stability = _per_quarter_stability(df)
        if stability.empty:
            md.append(
                "\n_No combinations meet the stability criteria on this dataset._\n"
            )
        else:
            md.append(_df_to_markdown_table(stability))

        md.append("\n\n## Decision criteria\n")
        for label, matching in _decision_rows(summary).items():
            md.append(f"### {label}\n")
            if matching.empty:
                md.append("_No signal/horizon combinations clear this threshold._\n")
            else:
                md.append(
                    _df_to_markdown_table(
                        matching[
                            ["signal", "horizon", "n", "ic", "top_minus_bottom"]
                        ].sort_values("ic", key=lambda s: s.abs(), ascending=False)
                    )
                )
                md.append("\n")
        md.append(
            "\n## Threshold rationale reminders\n"
            "1. **|IC| ≥ 0.02**: academic convention, low bar, surfaces directional evidence.\n"
            "2. **|IC| ≥ 0.05**: stricter, reserved for genuinely predictive signals.\n"
            "3. **|IC| ≥ 0.02 AND |spread| ≥ 50 bps**: both statistical and economic significance.\n"
        )
        md_path.write_text("\n".join(md), encoding="utf-8")

    return output_path


def load_signals_with_returns(db_path: str) -> pd.DataFrame:
    conn = duckdb.connect(db_path, read_only=True)
    try:
        return conn.execute("SELECT * FROM signals_with_returns").df()
    finally:
        conn.close()


def setup_logging(debug: bool = False) -> logging.Logger:
    log_dir = Path("backend/data/logs")
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / "report.log"
    level = logging.DEBUG if debug else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.FileHandler(log_file), logging.StreamHandler()],
    )
    return logging.getLogger(__name__)


def main() -> None:
    from backend.app.core.version import PIPELINE_VERSION

    parser = argparse.ArgumentParser(
        description="Generate the alpha-validation HTML + Markdown report."
    )
    parser.add_argument(
        "--db",
        default="backend/data/cleaned/cleaned.duckdb",
        help="Path to the cleaned DuckDB (default: %(default)s)",
    )
    parser.add_argument(
        "--date",
        default=None,
        help="Override the date stamp in the output filename (YYYYMMDD).",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Explicit output path. If unset, backend/data/analysis/validation_report_<date>.html.",
    )
    parser.add_argument("--debug", action="store_true")
    parser.add_argument(
        "--no-markdown", action="store_true", help="Skip the .md sibling file."
    )
    args = parser.parse_args()

    logger = setup_logging(debug=args.debug)
    logger.info(f"Loading signals_with_returns from {args.db}")
    df = load_signals_with_returns(args.db)
    logger.info(f"Loaded {len(df):,} rows.")

    if args.output:
        out = Path(args.output)
    else:
        stamp = args.date or datetime.now().strftime("%Y%m%d")
        out = Path(f"backend/data/analysis/validation_report_{stamp}.html")

    generate_report(
        df,
        out,
        pipeline_version=PIPELINE_VERSION,
        write_markdown=not args.no_markdown,
    )
    logger.info(f"Wrote HTML: {out}")
    if not args.no_markdown:
        logger.info(f"Wrote Markdown: {out.with_suffix('.md')}")


if __name__ == "__main__":
    main()
