"""
M_backtest threshold-sensitivity sweep on the strongest variant.

Re-runs ``MAJ-in-class-pct_of_mcap`` (the highest-Sharpe variant from
the headline backtest) at multiple MAJOR_EXPANSION ceiling-change
cutoffs to test whether the Sharpe is threshold-sensitive. The base
pipeline classifies MAJOR_EXPANSION at $100M; this script re-derives
``event_class`` on the loaded DataFrame using a configurable cutoff
without re-running the upstream pipeline.

Goal of the sweep:

* If lower thresholds (e.g. $50M) yield similar/higher Sharpe with more
  trades, the signal is real but the n=5,340 slice was just slightly
  too small.
* If higher thresholds (e.g. $500M, $1B) yield higher Sharpe, the
  signal lives in the largest expansion events only -- a real but
  capacity-limited finding.
* If Sharpe is flat or worse at every threshold, the signal is
  genuinely too weak after costs to be tradable in this form.

Usage
-----

    uv run --env-file .env backend/scripts/backtest_threshold_sweep.py

Writes to
``backend/data/analysis/backtest_<date>_threshold_sweep/``.
"""

from __future__ import annotations

import argparse
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List

import duckdb
import numpy as np
import pandas as pd

from backend.scripts.report import _df_to_html_table, _df_to_markdown_table
from backend.src.backtest_engine import (
    DEFAULT_HOLD_DAYS,
    DEFAULT_TXN_COST_BPS,
    run_backtest,
    same_sign_year_count,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

#: Threshold sweep in ascending order. Each value is the minimum
#: ``ceiling_change`` (in dollars) to qualify as MAJOR_EXPANSION.
SWEEP_THRESHOLDS: List[float] = [
    5e7,    # $50M
    1e8,    # $100M (current production)
    2e8,    # $200M
    5e8,    # $500M
    1e9,    # $1B
]

#: Static configuration for the variant (matches the strongest M_backtest result).
SIGNAL_COL: str = "ceiling_change_pct_of_mcap"
RETURN_COL: str = "industry_excess_return_20d"

#: Pass criteria, mirroring backtest.py.
PASS_NET_SHARPE: float = 0.5
PASS_MIN_SAME_SIGN_YEARS: int = 5


# ---------------------------------------------------------------------------
# Data preparation
# ---------------------------------------------------------------------------


def _reclassify_major_expansion(df: pd.DataFrame, threshold: float) -> pd.DataFrame:
    """
    Re-derive ``event_class`` on the loaded DataFrame using a custom
    MAJOR_EXPANSION cutoff. All other classes are left as-is so the
    engine still has a coherent class column to read.

    A row qualifies as MAJOR_EXPANSION iff:
    transaction_type == 'FUNDING_INCREASE' AND ceiling_change > threshold.
    The strict ``>`` matches the production pipeline's
    ``transform.calculate_alpha_signals`` rule so $100M sweep output is
    directly comparable to the baseline ``backtest.py`` results.

    Rows that previously qualified as MAJOR_EXPANSION but no longer
    meet the higher cutoff are demoted to MODERATE_EXPANSION. This
    preserves rest-of-class behaviour for cross-class operations.
    """
    out = df.copy()
    is_funding_inc = out["transaction_type"] == "FUNDING_INCREASE"
    has_ceil = out["ceiling_change"].notna()
    is_major = is_funding_inc & has_ceil & (out["ceiling_change"] > threshold)
    is_moderate = (
        is_funding_inc & has_ceil & (~is_major) & (out["ceiling_change"] > 0)
    )
    # Only overwrite event_class on rows that fall into these reclassifiable
    # buckets; everything else keeps its existing label.
    out.loc[is_major, "event_class"] = "MAJOR_EXPANSION"
    out.loc[is_moderate & (out["event_class"] == "MAJOR_EXPANSION"), "event_class"] = (
        "MODERATE_EXPANSION"
    )
    return out


# ---------------------------------------------------------------------------
# Sweep execution
# ---------------------------------------------------------------------------


def _format_threshold(t: float) -> str:
    """Pretty-print a dollar threshold like ``$50M`` or ``$1B``."""
    if t >= 1e9:
        return f"${t / 1e9:.0f}B"
    if t >= 1e6:
        return f"${t / 1e6:.0f}M"
    return f"${t:,.0f}"


def run_sweep(df: pd.DataFrame) -> List[Dict[str, object]]:
    """
    Run the in-class backtest at every threshold and return a list of
    structured result dicts (one per threshold).
    """
    sweep_results: List[Dict[str, object]] = []
    for threshold in SWEEP_THRESHOLDS:
        logger.info("Threshold %s: reclassifying event_class", _format_threshold(threshold))
        reclass = _reclassify_major_expansion(df, threshold)
        n_qualified = int((reclass["event_class"] == "MAJOR_EXPANSION").sum())
        logger.info("  %d rows qualify as MAJOR_EXPANSION", n_qualified)
        result = run_backtest(
            reclass,
            signal_col=SIGNAL_COL,
            return_col=RETURN_COL,
            class_col="event_class",
            framing="in_class",
            target_class="MAJOR_EXPANSION",
            winsorize=False,
            txn_cost_bps=DEFAULT_TXN_COST_BPS,
            hold_days=DEFAULT_HOLD_DAYS,
        )
        sweep_results.append(
            {
                "threshold": threshold,
                "threshold_label": _format_threshold(threshold),
                "n_qualified_rows": n_qualified,
                "result": result,
            }
        )
    return sweep_results


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------


def _build_summary_table(sweep_results: List[Dict[str, object]]) -> pd.DataFrame:
    """One row per threshold; the headline comparison table."""
    rows: List[Dict[str, object]] = []
    for entry in sweep_results:
        s = entry["result"]["summary"]
        counts = same_sign_year_count(entry["result"]["by_year"], "net_mean")
        rows.append(
            {
                "threshold": entry["threshold_label"],
                "n_qualified": entry["n_qualified_rows"],
                "n_trades": s["n_trades"],
                "gross_mean": s["gross_mean"],
                "net_mean": s["net_mean"],
                "net_stddev": s["net_stddev"],
                "sharpe": s["sharpe"],
                "hit_rate": s["hit_rate"],
                "n_pos_yrs": counts["n_positive"],
                "n_neg_yrs": counts["n_negative"],
                "pass_sharpe": (
                    s["sharpe"] is not None and s["sharpe"] >= PASS_NET_SHARPE
                ),
                "pass_yr_stability": (
                    max(counts["n_positive"], counts["n_negative"])
                    >= PASS_MIN_SAME_SIGN_YEARS
                ),
            }
        )
    df = pd.DataFrame(rows)
    df["pass_overall"] = df["pass_sharpe"] & df["pass_yr_stability"]
    return df


def _format_pass_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in ("pass_sharpe", "pass_yr_stability", "pass_overall"):
        out[col] = out[col].apply(lambda b: "PASS" if b else "FAIL")
    return out


_HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Backtest Threshold Sweep &mdash; {date_stamp}</title>
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
  code {{ background: #eee; padding: 1px 4px; border-radius: 3px; }}
</style>
</head>
<body>
<h1>Backtest Threshold Sensitivity Sweep</h1>
<p class="meta">
  Generated: {generated_at} &middot; Pipeline version: {pipeline_version}<br>
  Variant: <code>{signal_col}</code> &times; in-class top vs bottom decile<br>
  Source: <code>cleaned.duckdb::signals_with_returns</code> &middot;
  {row_count:,} rows &middot; Return column: <code>{return_col}</code><br>
  Hold days: {hold_days} &middot; Round-trip cost: {txn_cost_bps:.0f} bps
</p>

<div class="decision-box">
<strong>Pass criteria:</strong>
<ul>
  <li>Net annualised Sharpe &geq; {pass_sharpe} (after {txn_cost_bps:.0f} bps round-trip cost)</li>
  <li>Net mean same-signed in &geq; {pass_years} fiscal years</li>
</ul>
</div>

<h2>1. Headline summary</h2>
{summary_table}

<h2>2. Per-threshold detail</h2>
{detail_sections}

<h2>3. Decision summary</h2>
<p>{decision_text}</p>

</body>
</html>
"""


def _build_detail_section_html(entry: Dict[str, object]) -> str:
    label = entry["threshold_label"]
    by_year = entry["result"]["by_year"]
    deciles = entry["result"]["deciles"]
    parts = [
        f"<h3>Threshold: {label}</h3>",
        f"<p>{entry['n_qualified_rows']:,} rows qualify as MAJOR_EXPANSION.</p>",
        "<h4>Per-fiscal-year</h4>",
        _df_to_html_table(by_year),
        "<h4>Decile spread</h4>",
        _df_to_html_table(deciles),
    ]
    return "\n".join(parts)


def _build_detail_section_md(entry: Dict[str, object]) -> str:
    label = entry["threshold_label"]
    by_year = entry["result"]["by_year"]
    deciles = entry["result"]["deciles"]
    lines = [
        f"### Threshold: {label}",
        "",
        f"{entry['n_qualified_rows']:,} rows qualify as MAJOR_EXPANSION.",
        "",
        "**Per-fiscal-year**",
        "",
        _df_to_markdown_table(by_year),
        "",
        "**Decile spread**",
        "",
        _df_to_markdown_table(deciles),
        "",
    ]
    return "\n".join(lines)


def _decision_text(summary_df: pd.DataFrame) -> str:
    n_pass = int(summary_df["pass_overall"].sum())
    n_total = len(summary_df)
    if n_pass == 0:
        # Find the threshold with the highest Sharpe for narrative.
        if not summary_df.empty and summary_df["sharpe"].notna().any():
            best = summary_df.loc[summary_df["sharpe"].idxmax()]
            return (
                f"<strong>0 of {n_total} thresholds pass.</strong> The highest "
                f"observed Sharpe is <strong>{best['sharpe']:+.3f}</strong> at "
                f"<code>{best['threshold']}</code> "
                f"(n_trades = {best['n_trades']:,}, "
                f"{best['n_pos_yrs']}+/{best['n_neg_yrs']}- years), still below "
                f"the {PASS_NET_SHARPE} bar. The signal does not become "
                "tradable at any threshold tested."
            )
        return (
            f"<strong>0 of {n_total} thresholds pass.</strong> No threshold "
            f"clears the Sharpe &geq; {PASS_NET_SHARPE} bar."
        )
    if n_pass == 1:
        passing = summary_df[summary_df["pass_overall"]].iloc[0]
        return (
            f"<strong>1 of {n_total} thresholds pass</strong>: "
            f"<code>{passing['threshold']}</code>. This is the only "
            "threshold where the signal is strong enough to overcome "
            "transaction costs while remaining year-stable. <em>Caveat: "
            "single passing threshold within a sweep that was specifically "
            "designed to find one is a classic overfitting signature; "
            "treat with care.</em>"
        )
    return f"<strong>{n_pass} of {n_total} thresholds pass.</strong>"


def render_html(
    *,
    summary_df: pd.DataFrame,
    sweep_results: List[Dict[str, object]],
    metadata: Dict[str, object],
) -> str:
    return _HTML_TEMPLATE.format(
        summary_table=_df_to_html_table(_format_pass_columns(summary_df)),
        detail_sections="\n".join(
            _build_detail_section_html(e) for e in sweep_results
        ),
        decision_text=_decision_text(summary_df),
        signal_col=SIGNAL_COL,
        pass_sharpe=PASS_NET_SHARPE,
        pass_years=PASS_MIN_SAME_SIGN_YEARS,
        **metadata,
    )


def render_markdown(
    *,
    summary_df: pd.DataFrame,
    sweep_results: List[Dict[str, object]],
    metadata: Dict[str, object],
) -> str:
    pretty = _format_pass_columns(summary_df)
    lines = [
        "# Backtest Threshold Sensitivity Sweep",
        "",
        f"Generated: {metadata['generated_at']}  ",
        f"Pipeline version: {metadata['pipeline_version']}  ",
        f"Variant: `{SIGNAL_COL}` x in-class top vs bottom decile  ",
        f"Source: `cleaned.duckdb::signals_with_returns` ({metadata['row_count']:,} rows)  ",
        f"Return column: `{metadata['return_col']}`  ",
        f"Hold days: {metadata['hold_days']}  ",
        f"Round-trip cost: {metadata['txn_cost_bps']:.0f} bps",
        "",
        "## Pass criteria",
        "",
        f"- Net annualised Sharpe >= {PASS_NET_SHARPE} (after {metadata['txn_cost_bps']:.0f} bps cost)",
        f"- Net mean same-signed in >= {PASS_MIN_SAME_SIGN_YEARS} fiscal years",
        "",
        "## 1. Headline summary",
        "",
        _df_to_markdown_table(pretty),
        "",
        "## 2. Per-threshold detail",
        "",
    ]
    for entry in sweep_results:
        lines.append(_build_detail_section_md(entry))
    lines.extend(["", "## 3. Decision summary", "", _decision_text(summary_df), ""])
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# I/O
# ---------------------------------------------------------------------------


def load_signals_with_returns(db_path: str) -> pd.DataFrame:
    conn = duckdb.connect(db_path, read_only=True)
    try:
        return conn.execute("SELECT * FROM signals_with_returns").df()
    finally:
        conn.close()


def setup_logging(debug: bool = False) -> logging.Logger:
    log_dir = Path("backend/data/logs")
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / "backtest_threshold_sweep.log"
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
        description=(
            "Run the M_backtest threshold-sensitivity sweep on the strongest "
            "in-class variant."
        )
    )
    parser.add_argument(
        "--db",
        default="backend/data/cleaned/cleaned.duckdb",
        help="Path to the cleaned DuckDB (default: %(default)s)",
    )
    parser.add_argument("--date", default=None, help="Date stamp (YYYYMMDD).")
    parser.add_argument("--output", default=None, help="Explicit output HTML path.")
    parser.add_argument("--debug", action="store_true")
    parser.add_argument(
        "--no-markdown", action="store_true", help="Skip the .md sibling file."
    )
    args = parser.parse_args()

    logger = setup_logging(debug=args.debug)
    logger.info(f"Loading signals_with_returns from {args.db}")
    df = load_signals_with_returns(args.db)
    logger.info(f"Loaded {len(df):,} rows.")

    stamp = args.date or datetime.now().strftime("%Y%m%d")
    if args.output:
        out_html = Path(args.output)
    else:
        slug = f"backtest_{stamp}_threshold_sweep"
        out_html = Path("backend/data/analysis") / slug / f"{slug}.html"
    out_html.parent.mkdir(parents=True, exist_ok=True)

    logger.info(f"Running threshold sweep over {len(SWEEP_THRESHOLDS)} cutoffs")
    sweep_results = run_sweep(df)
    summary_df = _build_summary_table(sweep_results)

    metadata = {
        "date_stamp": stamp,
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "pipeline_version": PIPELINE_VERSION,
        "row_count": len(df),
        "return_col": RETURN_COL,
        "txn_cost_bps": DEFAULT_TXN_COST_BPS,
        "hold_days": DEFAULT_HOLD_DAYS,
    }

    html = render_html(
        summary_df=summary_df,
        sweep_results=sweep_results,
        metadata=metadata,
    )
    out_html.write_text(html)
    logger.info(f"Wrote HTML: {out_html}")

    if not args.no_markdown:
        md = render_markdown(
            summary_df=summary_df,
            sweep_results=sweep_results,
            metadata=metadata,
        )
        out_md = out_html.with_suffix(".md")
        out_md.write_text(md)
        logger.info(f"Wrote Markdown: {out_md}")


if __name__ == "__main__":
    main()
