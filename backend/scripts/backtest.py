"""
Run the M_backtest event-driven backtest and emit HTML + Markdown reports.

This script orchestrates the pure-function ``backend.src.backtest_engine``
across a configurable matrix of (signal, framing) variants and writes a
single combined report covering all variants. It is the executable
entry point that the analysis pipeline calls; the engine itself stays
fully pandas-only and has no I/O.

Usage
-----

    uv run --env-file .env backend/scripts/backtest.py
    uv run --env-file .env backend/scripts/backtest.py --date 20260428_major_expansion
    uv run --env-file .env backend/scripts/backtest.py --output backend/data/analysis/foo.html

By default writes to:
``backend/data/analysis/backtest_<YYYYMMDD>_major_expansion/backtest_<YYYYMMDD>_major_expansion.{html,md}``.
"""

from __future__ import annotations

import argparse
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import duckdb
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

#: The three magnitude features evaluated by this milestone.
MAGNITUDE_SIGNALS: List[str] = [
    "ceiling_change_pct_of_mcap",
    "relative_ceiling_change",
    "ceiling_change_log_dollars",
]

#: Default forward-return column. T+20 industry-excess matches the IC
#: sweet spot identified in the v2 analysis.
DEFAULT_RETURN_COL: str = "industry_excess_return_20d"

#: Pass criteria. Net Sharpe must clear this; same-signed in this many years.
PASS_NET_SHARPE: float = 0.5
PASS_MIN_SAME_SIGN_YEARS: int = 5

#: Variant matrix.
#:
#: Three in-class variants (top vs bottom decile within MAJOR_EXPANSION),
#: one per magnitude signal -- this is where the signal_col actually
#: affects the trades. Plus one signal-agnostic cross-class variant
#: (long every MAJOR_EXPANSION row, short every CONTRACTION row); the
#: signal column doesn't enter cross-class selection so we only run it
#: once. signal_col is still set on the cross-class variant so the
#: trades DataFrame can carry the value for inspection.
VARIANT_DEFINITIONS: List[Dict[str, object]] = [
    {
        "name": "MAJ-in-class-pct_of_mcap",
        "signal_col": "ceiling_change_pct_of_mcap",
        "framing": "in_class",
        "target_class": "MAJOR_EXPANSION",
        "winsorize": False,
    },
    {
        "name": "MAJ-in-class-relative",
        "signal_col": "relative_ceiling_change",
        "framing": "in_class",
        "target_class": "MAJOR_EXPANSION",
        "winsorize": True,  # required because of 295 outlier rows
    },
    {
        "name": "MAJ-in-class-log_dollars",
        "signal_col": "ceiling_change_log_dollars",
        "framing": "in_class",
        "target_class": "MAJOR_EXPANSION",
        "winsorize": False,
    },
    {
        "name": "MAJ-vs-CON-cross-class",
        "signal_col": "ceiling_change_pct_of_mcap",  # carried for display only
        "framing": "cross_class",
        "long_class": "MAJOR_EXPANSION",
        "short_class": "CONTRACTION",
        "winsorize": False,
    },
]


# ---------------------------------------------------------------------------
# Result aggregation
# ---------------------------------------------------------------------------


def _summary_row(name: str, result: Dict[str, object]) -> Dict[str, object]:
    """Flatten a backtest result into a single row for the summary table."""
    s = result["summary"]
    cfg = result["config"]
    counts = same_sign_year_count(result["by_year"], "net_mean")
    pass_sharpe = (
        s["sharpe"] is not None and s["sharpe"] >= PASS_NET_SHARPE
    )
    n_dominant = max(counts["n_positive"], counts["n_negative"])
    pass_years = n_dominant >= PASS_MIN_SAME_SIGN_YEARS
    return {
        "variant": name,
        "signal": cfg["signal_col"],
        "framing": cfg["framing"],
        "n_trades": s["n_trades"],
        "gross_mean": s["gross_mean"],
        "net_mean": s["net_mean"],
        "net_stddev": s["net_stddev"],
        "sharpe": s["sharpe"],
        "hit_rate": s["hit_rate"],
        "n_pos_yrs": counts["n_positive"],
        "n_neg_yrs": counts["n_negative"],
        "pass_sharpe": pass_sharpe,
        "pass_yr_stability": pass_years,
        "pass_overall": pass_sharpe and pass_years,
    }


def run_all_variants(df: pd.DataFrame, return_col: str) -> List[Dict[str, object]]:
    """Execute every variant defined in ``VARIANT_DEFINITIONS``."""
    results: List[Dict[str, object]] = []
    for variant in VARIANT_DEFINITIONS:
        logger.info("Running variant: %s", variant["name"])
        kwargs = {k: v for k, v in variant.items() if k != "name"}
        try:
            res = run_backtest(df, return_col=return_col, class_col="event_class", **kwargs)
        except Exception as exc:  # surfaces config errors; engine itself doesn't raise on data
            logger.error("Variant %s failed: %s", variant["name"], exc)
            continue
        results.append({"name": variant["name"], "result": res})
    return results


# ---------------------------------------------------------------------------
# Report rendering
# ---------------------------------------------------------------------------


_HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Backtest Report &mdash; {date_stamp}</title>
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
  .pass {{ color: #27ae60; font-weight: bold; }}
  .fail {{ color: #c0392b; }}
  .decision-box {{ background: #f8f8f8; border-left: 4px solid #3498db;
                   padding: 1rem 1.4rem; margin: 1rem 0; }}
  code {{ background: #eee; padding: 1px 4px; border-radius: 3px; }}
</style>
</head>
<body>
<h1>Backtest Report</h1>
<p class="meta">
  Generated: {generated_at} &middot; Pipeline version: {pipeline_version}<br>
  Source: <code>cleaned.duckdb::signals_with_returns</code> &middot;
  {row_count:,} rows &middot; Return column: <code>{return_col}</code><br>
  Hold days: {hold_days} &middot; Round-trip cost: {txn_cost_bps:.0f} bps &middot;
  Buckets: 10 (deciles)
</p>

<div class="decision-box">
<strong>Pass criteria:</strong>
<ul>
  <li>Net annualised Sharpe &geq; {pass_sharpe} (after {txn_cost_bps:.0f} bps round-trip cost)</li>
  <li>Net mean same-signed in &geq; {pass_years} fiscal years</li>
</ul>
A variant must clear <strong>both</strong> bars to be considered tradable.
</div>

<h2>1. Headline summary</h2>
{summary_table}

<h2>2. Per-variant detail</h2>
{variant_sections}

<h2>3. Decision summary</h2>
<p>{decision_text}</p>
{passing_table}

</body>
</html>
"""


def _bool_cell(b: bool) -> str:
    return "PASS" if b else "FAIL"


def _format_summary_for_html(rows: List[Dict[str, object]]) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df["pass_sharpe"] = df["pass_sharpe"].apply(_bool_cell)
    df["pass_yr_stability"] = df["pass_yr_stability"].apply(_bool_cell)
    df["pass_overall"] = df["pass_overall"].apply(_bool_cell)
    return df


def _build_variant_section_html(name: str, result: Dict[str, object]) -> str:
    cfg = result["config"]
    summary = result["summary"]
    by_year = result["by_year"]
    deciles = result["deciles"]
    counts = same_sign_year_count(by_year, "net_mean")

    config_table = pd.DataFrame(
        [
            {"key": k, "value": str(v)}
            for k, v in cfg.items()
        ]
    )
    summary_df = pd.DataFrame([summary])
    parts = [
        f"<h3>{name}</h3>",
        "<h4>Configuration</h4>",
        _df_to_html_table(config_table),
        "<h4>Aggregate performance</h4>",
        _df_to_html_table(summary_df),
        f"<p>Same-signed years: <strong>{counts['n_positive']} positive / "
        f"{counts['n_negative']} negative</strong> out of {counts['n_years']}.</p>",
        "<h4>Per-fiscal-year</h4>",
        _df_to_html_table(by_year),
    ]
    if len(deciles) > 0:
        parts.append("<h4>Decile spread (in-class only)</h4>")
        parts.append(_df_to_html_table(deciles))
    return "\n".join(parts)


def _build_variant_section_md(name: str, result: Dict[str, object]) -> str:
    cfg = result["config"]
    summary = result["summary"]
    by_year = result["by_year"]
    deciles = result["deciles"]
    counts = same_sign_year_count(by_year, "net_mean")

    config_table = pd.DataFrame(
        [{"key": k, "value": str(v)} for k, v in cfg.items()]
    )
    summary_df = pd.DataFrame([summary])
    lines = [
        f"### {name}",
        "",
        "**Configuration**",
        "",
        _df_to_markdown_table(config_table),
        "",
        "**Aggregate performance**",
        "",
        _df_to_markdown_table(summary_df),
        "",
        f"Same-signed years: **{counts['n_positive']} positive / "
        f"{counts['n_negative']} negative** out of {counts['n_years']}.",
        "",
        "**Per-fiscal-year**",
        "",
        _df_to_markdown_table(by_year),
        "",
    ]
    if len(deciles) > 0:
        lines.extend(
            [
                "**Decile spread (in-class only)**",
                "",
                _df_to_markdown_table(deciles),
                "",
            ]
        )
    return "\n".join(lines)


def _decision_text(summary_rows: List[Dict[str, object]]) -> str:
    n_pass = sum(1 for r in summary_rows if r["pass_overall"])
    n_total = len(summary_rows)
    if n_pass == 0:
        return (
            f"<strong>0 of {n_total} variants pass.</strong> No event-driven "
            "magnitude signal clears both the net-Sharpe and 5-of-N "
            "fiscal-year same-sign bars at this configuration. The signal "
            "may exist (the IC analysis showed it does) but transaction "
            "costs and per-trade volatility erode it below tradability "
            "in this paper backtest."
        )
    elif n_pass == 1:
        passing = next(r for r in summary_rows if r["pass_overall"])
        return (
            f"<strong>1 of {n_total} variants pass</strong>: "
            f"<code>{passing['variant']}</code>. This is the only "
            "configuration where the signal is strong enough to overcome "
            "transaction costs while remaining year-stable."
        )
    return f"<strong>{n_pass} of {n_total} variants pass</strong>. See the table below."


def render_html(
    *,
    summary_rows: List[Dict[str, object]],
    variant_results: List[Dict[str, object]],
    metadata: Dict[str, object],
) -> str:
    sum_df = _format_summary_for_html(summary_rows)
    summary_table = _df_to_html_table(sum_df)
    variant_sections = "\n".join(
        _build_variant_section_html(v["name"], v["result"]) for v in variant_results
    )
    passing = [r for r in summary_rows if r["pass_overall"]]
    if passing:
        passing_df = _format_summary_for_html(passing)
        passing_table = _df_to_html_table(passing_df)
    else:
        passing_table = "<p><em>(no variants pass both criteria)</em></p>"
    return _HTML_TEMPLATE.format(
        summary_table=summary_table,
        variant_sections=variant_sections,
        decision_text=_decision_text(summary_rows),
        passing_table=passing_table,
        pass_sharpe=PASS_NET_SHARPE,
        pass_years=PASS_MIN_SAME_SIGN_YEARS,
        **metadata,
    )


def render_markdown(
    *,
    summary_rows: List[Dict[str, object]],
    variant_results: List[Dict[str, object]],
    metadata: Dict[str, object],
) -> str:
    sum_df = pd.DataFrame(summary_rows)
    if not sum_df.empty:
        for col in ("pass_sharpe", "pass_yr_stability", "pass_overall"):
            sum_df[col] = sum_df[col].apply(lambda b: "PASS" if b else "FAIL")
    lines: List[str] = [
        "# Backtest Report",
        "",
        f"Generated: {metadata['generated_at']}  ",
        f"Pipeline version: {metadata['pipeline_version']}  ",
        f"Source: `cleaned.duckdb::signals_with_returns` ({metadata['row_count']:,} rows)  ",
        f"Return column: `{metadata['return_col']}`  ",
        f"Hold days: {metadata['hold_days']}  ",
        f"Round-trip cost: {metadata['txn_cost_bps']:.0f} bps  ",
        f"Buckets: 10 (deciles)",
        "",
        "## Pass criteria",
        "",
        f"- Net annualised Sharpe >= {PASS_NET_SHARPE} (after {metadata['txn_cost_bps']:.0f} bps cost)",
        f"- Net mean same-signed in >= {PASS_MIN_SAME_SIGN_YEARS} fiscal years",
        "",
        "A variant must clear **both** bars to be considered tradable.",
        "",
        "## 1. Headline summary",
        "",
        _df_to_markdown_table(sum_df),
        "",
        "## 2. Per-variant detail",
        "",
    ]
    for v in variant_results:
        lines.append(_build_variant_section_md(v["name"], v["result"]))
    lines.extend(["", "## 3. Decision summary", "", _decision_text(summary_rows), ""])
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
    log_file = log_dir / "backtest.log"
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
        description="Run the M_backtest event-driven backtest variants."
    )
    parser.add_argument(
        "--db",
        default="backend/data/cleaned/cleaned.duckdb",
        help="Path to the cleaned DuckDB (default: %(default)s)",
    )
    parser.add_argument(
        "--date",
        default=None,
        help="Date stamp suffix for the output directory and filenames (YYYYMMDD).",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Explicit output HTML path. If unset, "
        "backend/data/analysis/backtest_<date>_major_expansion/...",
    )
    parser.add_argument(
        "--return-col",
        default=DEFAULT_RETURN_COL,
        help=f"Forward-return column to use (default: %(default)s)",
    )
    parser.add_argument(
        "--txn-cost-bps",
        type=float,
        default=DEFAULT_TXN_COST_BPS,
        help="Round-trip transaction cost in basis points (default: %(default)s)",
    )
    parser.add_argument(
        "--hold-days",
        type=int,
        default=DEFAULT_HOLD_DAYS,
        help="Holding period in trading days (default: %(default)s)",
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

    # Determine output paths.
    stamp = args.date or datetime.now().strftime("%Y%m%d")
    if args.output:
        out_html = Path(args.output)
    else:
        # Auto-create per-date subdirectory matching the analysis convention.
        slug = f"backtest_{stamp}_major_expansion"
        out_dir = Path("backend/data/analysis") / slug
        out_html = out_dir / f"{slug}.html"
    out_html.parent.mkdir(parents=True, exist_ok=True)

    # Patch variant txn_cost_bps and hold_days from CLI.
    for variant in VARIANT_DEFINITIONS:
        variant["txn_cost_bps"] = args.txn_cost_bps
        variant["hold_days"] = args.hold_days

    logger.info("Running %d backtest variants", len(VARIANT_DEFINITIONS))
    variant_results = run_all_variants(df, return_col=args.return_col)
    summary_rows = [_summary_row(v["name"], v["result"]) for v in variant_results]

    metadata = {
        "date_stamp": stamp,
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "pipeline_version": PIPELINE_VERSION,
        "row_count": len(df),
        "return_col": args.return_col,
        "txn_cost_bps": args.txn_cost_bps,
        "hold_days": args.hold_days,
    }

    html = render_html(
        summary_rows=summary_rows,
        variant_results=variant_results,
        metadata=metadata,
    )
    out_html.write_text(html)
    logger.info(f"Wrote HTML: {out_html}")

    if not args.no_markdown:
        md = render_markdown(
            summary_rows=summary_rows,
            variant_results=variant_results,
            metadata=metadata,
        )
        out_md = out_html.with_suffix(".md")
        out_md.write_text(md)
        logger.info(f"Wrote Markdown: {out_md}")


if __name__ == "__main__":
    main()
