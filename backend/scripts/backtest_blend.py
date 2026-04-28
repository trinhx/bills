"""
M_blend backtest: composite signal of pct_of_mcap + moat_index.

Tests whether combining two genuinely independent event-driven
signals into a single percentile-rank-blended composite produces a
tradable strategy. Single-signal backtests in M_backtest produced
net Sharpe of +0.24 (pct_of_mcap, T+20) -- below the +0.5 bar but
positive. The hypothesis here is that blending in moat_index (Spearman
rho = -0.42 with pct_of_mcap, opposite-sign IC) provides enough
independent signal to clear the bar.

Variant matrix (6 backtests):
* T+20 hold:  composite, pct_of_mcap-alone (baseline), moat-alone (baseline)
* T+120 hold: composite, pct_of_mcap-alone (baseline), moat-alone (baseline)

Single-signal baselines at each horizon let us attribute Sharpe gains
between "blend uplift" and "horizon shift".

Usage
-----

    uv run --env-file .env backend/scripts/backtest_blend.py

Writes to ``backend/data/analysis/backtest_<date>_blend/``.
"""

from __future__ import annotations

import argparse
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import duckdb
import numpy as np
import pandas as pd

from backend.scripts.report import _df_to_html_table, _df_to_markdown_table
from backend.src.backtest_engine import (
    DEFAULT_TXN_COST_BPS,
    run_backtest,
    same_sign_year_count,
)
from backend.src.composite_signals import (
    SignalSpec,
    build_composite_score,
    component_correlations,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

#: The two signals comprising the M_blend composite. pct_of_mcap is
#: bullish ('asc'); moat_index is bearish ('desc') because its IC is
#: negative -- sole-source major expansions underperform.
COMPOSITE_SPECS: List[SignalSpec] = [
    SignalSpec(column="ceiling_change_pct_of_mcap", direction="asc", weight=1.0),
    SignalSpec(column="moat_index", direction="desc", weight=1.0),
]

#: Holding-period variants. T+20 matches pct_of_mcap's IC peak;
#: T+120 matches moat_index's stronger long-horizon IC.
HOLDING_HORIZONS_DAYS: List[int] = [20, 120]

#: Synthetic column the script writes to the loaded DataFrame.
COMPOSITE_COL: str = "blend_composite"

#: Pass criteria, mirroring backtest.py.
PASS_NET_SHARPE: float = 0.5
PASS_MIN_SAME_SIGN_YEARS: int = 5


# ---------------------------------------------------------------------------
# Composite construction on the loaded DataFrame
# ---------------------------------------------------------------------------


def attach_composite(df: pd.DataFrame, target_class: str = "MAJOR_EXPANSION") -> pd.DataFrame:
    """
    Compute the percentile-rank composite **within the target class only**
    and attach it to ``df`` as a new column ``blend_composite``.

    Rows outside the target class get NaN composite, which is the
    correct behaviour for the in-class backtest (the engine drops
    NaN-signal rows when bucketing).
    """
    out = df.copy()
    # Initialise composite column with NaN (float) so the dtype is set
    # cleanly from the start.
    out[COMPOSITE_COL] = np.nan
    sub = out[out["event_class"] == target_class]
    if len(sub) == 0:
        return out
    composite = build_composite_score(sub, COMPOSITE_SPECS, require_all_components=True)
    out.loc[sub.index, COMPOSITE_COL] = composite.values
    return out


# ---------------------------------------------------------------------------
# Variant matrix
# ---------------------------------------------------------------------------


def _build_variant_definitions() -> List[Dict[str, object]]:
    """Generate the 6-row variant matrix: 3 signals x 2 horizons."""
    variants: List[Dict[str, object]] = []
    for h in HOLDING_HORIZONS_DAYS:
        return_col = f"industry_excess_return_{h}d"
        variants.extend(
            [
                {
                    "name": f"composite-T{h}",
                    "signal_col": COMPOSITE_COL,
                    "return_col": return_col,
                    "hold_days": h,
                    "kind": "composite",
                },
                {
                    "name": f"pct_of_mcap-T{h}",
                    "signal_col": "ceiling_change_pct_of_mcap",
                    "return_col": return_col,
                    "hold_days": h,
                    "kind": "baseline",
                },
                {
                    "name": f"moat_index-T{h}",
                    "signal_col": "moat_index",
                    "return_col": return_col,
                    "hold_days": h,
                    "kind": "baseline",
                },
            ]
        )
    return variants


def run_all_variants(df: pd.DataFrame) -> List[Dict[str, object]]:
    """Execute every variant and collect structured results."""
    variants = _build_variant_definitions()
    results: List[Dict[str, object]] = []
    for variant in variants:
        logger.info("Running variant: %s", variant["name"])
        try:
            res = run_backtest(
                df,
                signal_col=str(variant["signal_col"]),
                return_col=str(variant["return_col"]),
                class_col="event_class",
                framing="in_class",
                target_class="MAJOR_EXPANSION",
                winsorize=False,
                txn_cost_bps=DEFAULT_TXN_COST_BPS,
                hold_days=int(variant["hold_days"]),
            )
        except Exception as exc:
            logger.error("Variant %s failed: %s", variant["name"], exc)
            continue
        results.append({**variant, "result": res})
    return results


# ---------------------------------------------------------------------------
# Aggregation
# ---------------------------------------------------------------------------


def _summary_row(variant: Dict[str, object]) -> Dict[str, object]:
    res = variant["result"]
    s = res["summary"]
    counts = same_sign_year_count(res["by_year"], "net_mean")
    pass_sharpe = (s["sharpe"] is not None and s["sharpe"] >= PASS_NET_SHARPE)
    n_dominant = max(counts["n_positive"], counts["n_negative"])
    pass_years = n_dominant >= PASS_MIN_SAME_SIGN_YEARS
    return {
        "variant": variant["name"],
        "kind": variant["kind"],
        "horizon_days": variant["hold_days"],
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


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------


_HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Blended-Signal Backtest &mdash; {date_stamp}</title>
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
<h1>Blended-Signal Backtest (M_blend)</h1>
<p class="meta">
  Generated: {generated_at} &middot; Pipeline version: {pipeline_version}<br>
  Source: <code>cleaned.duckdb::signals_with_returns</code> &middot;
  {row_count:,} rows<br>
  Composite: equal-weight percentile-rank blend of
  <code>ceiling_change_pct_of_mcap</code> (asc) and
  <code>moat_index</code> (desc), within MAJOR_EXPANSION<br>
  Round-trip cost: {txn_cost_bps:.0f} bps &middot; Buckets: 10 (deciles)
</p>

<div class="decision-box">
<strong>Pass criteria:</strong>
<ul>
  <li>Net annualised Sharpe &geq; {pass_sharpe}</li>
  <li>Net mean same-signed in &geq; {pass_years} fiscal years</li>
</ul>
</div>

<h2>1. Component diagnostics</h2>
<p>Pairwise Spearman rank correlation of components within MAJOR_EXPANSION
(rows where both are non-null):</p>
{corr_table}

<h2>2. Headline summary</h2>
{summary_table}

<h2>3. Per-variant detail</h2>
{variant_sections}

<h2>4. Decision summary</h2>
<p>{decision_text}</p>
{passing_table}

</body>
</html>
"""


def _bool_cell(b: bool) -> str:
    return "PASS" if b else "FAIL"


def _format_summary(rows: List[Dict[str, object]]) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    for col in ("pass_sharpe", "pass_yr_stability", "pass_overall"):
        df[col] = df[col].apply(_bool_cell)
    return df


def _build_variant_section_html(variant: Dict[str, object]) -> str:
    res = variant["result"]
    cfg = res["config"]
    summary = res["summary"]
    by_year = res["by_year"]
    deciles = res["deciles"]
    counts = same_sign_year_count(by_year, "net_mean")
    config_table = pd.DataFrame(
        [{"key": k, "value": str(v)} for k, v in cfg.items()]
    )
    parts = [
        f"<h3>{variant['name']} ({variant['kind']})</h3>",
        "<h4>Configuration</h4>",
        _df_to_html_table(config_table),
        "<h4>Aggregate performance</h4>",
        _df_to_html_table(pd.DataFrame([summary])),
        f"<p>Same-signed years: <strong>{counts['n_positive']} positive / "
        f"{counts['n_negative']} negative</strong> out of {counts['n_years']}.</p>",
        "<h4>Per-fiscal-year</h4>",
        _df_to_html_table(by_year),
    ]
    if len(deciles) > 0:
        parts.append("<h4>Decile spread</h4>")
        parts.append(_df_to_html_table(deciles))
    return "\n".join(parts)


def _build_variant_section_md(variant: Dict[str, object]) -> str:
    res = variant["result"]
    cfg = res["config"]
    summary = res["summary"]
    by_year = res["by_year"]
    deciles = res["deciles"]
    counts = same_sign_year_count(by_year, "net_mean")
    config_table = pd.DataFrame(
        [{"key": k, "value": str(v)} for k, v in cfg.items()]
    )
    lines = [
        f"### {variant['name']} ({variant['kind']})",
        "",
        "**Configuration**",
        "",
        _df_to_markdown_table(config_table),
        "",
        "**Aggregate performance**",
        "",
        _df_to_markdown_table(pd.DataFrame([summary])),
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
            ["**Decile spread**", "", _df_to_markdown_table(deciles), ""]
        )
    return "\n".join(lines)


def _decision_text(summary_rows: List[Dict[str, object]]) -> str:
    n_pass = sum(1 for r in summary_rows if r["pass_overall"])
    n_total = len(summary_rows)
    if n_pass == 0:
        # Highlight the highest Sharpe composite for narrative.
        comps = [r for r in summary_rows if r["kind"] == "composite"]
        if comps:
            best_comp = max(
                (c for c in comps if c["sharpe"] is not None),
                key=lambda c: c["sharpe"],
                default=None,
            )
        else:
            best_comp = None
        if best_comp is not None:
            return (
                f"<strong>0 of {n_total} variants pass.</strong> The strongest "
                f"composite achieves net Sharpe = {best_comp['sharpe']:+.3f} at "
                f"{best_comp['variant']} ({best_comp['n_pos_yrs']}+/"
                f"{best_comp['n_neg_yrs']}- yrs), still below the "
                f"{PASS_NET_SHARPE} bar."
            )
        return (
            f"<strong>0 of {n_total} variants pass.</strong> The blended "
            f"signal does not clear both bars at any horizon tested."
        )
    if n_pass == 1:
        passing = next(r for r in summary_rows if r["pass_overall"])
        return (
            f"<strong>1 of {n_total} variants pass</strong>: "
            f"<code>{passing['variant']}</code>."
        )
    return f"<strong>{n_pass} of {n_total} variants pass.</strong>"


def render_html(
    *,
    summary_rows: List[Dict[str, object]],
    variant_results: List[Dict[str, object]],
    correlation_df: pd.DataFrame,
    metadata: Dict[str, object],
) -> str:
    summary_table = _df_to_html_table(_format_summary(summary_rows))
    corr_pretty = correlation_df.copy().reset_index().rename(columns={"index": "component"})
    corr_table = _df_to_html_table(corr_pretty, float_fmt="{:.4f}")
    variant_sections = "\n".join(
        _build_variant_section_html(v) for v in variant_results
    )
    passing = [r for r in summary_rows if r["pass_overall"]]
    if passing:
        passing_table = _df_to_html_table(_format_summary(passing))
    else:
        passing_table = "<p><em>(no variants pass both criteria)</em></p>"
    return _HTML_TEMPLATE.format(
        summary_table=summary_table,
        corr_table=corr_table,
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
    correlation_df: pd.DataFrame,
    metadata: Dict[str, object],
) -> str:
    pretty = _format_summary(summary_rows)
    corr_pretty = correlation_df.copy().reset_index().rename(columns={"index": "component"})
    lines = [
        "# Blended-Signal Backtest (M_blend)",
        "",
        f"Generated: {metadata['generated_at']}  ",
        f"Pipeline version: {metadata['pipeline_version']}  ",
        f"Source: `cleaned.duckdb::signals_with_returns` ({metadata['row_count']:,} rows)  ",
        "Composite: equal-weight percentile-rank blend of "
        "`ceiling_change_pct_of_mcap` (asc) and `moat_index` (desc), within MAJOR_EXPANSION  ",
        f"Round-trip cost: {metadata['txn_cost_bps']:.0f} bps  ",
        "Buckets: 10 (deciles)",
        "",
        "## Pass criteria",
        "",
        f"- Net annualised Sharpe >= {PASS_NET_SHARPE}",
        f"- Net mean same-signed in >= {PASS_MIN_SAME_SIGN_YEARS} fiscal years",
        "",
        "## 1. Component diagnostics",
        "",
        "Pairwise Spearman rank correlation of components within MAJOR_EXPANSION:",
        "",
        _df_to_markdown_table(corr_pretty),
        "",
        "## 2. Headline summary",
        "",
        _df_to_markdown_table(pretty),
        "",
        "## 3. Per-variant detail",
        "",
    ]
    for v in variant_results:
        lines.append(_build_variant_section_md(v))
    lines.extend(["", "## 4. Decision summary", "", _decision_text(summary_rows), ""])
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
    log_file = log_dir / "backtest_blend.log"
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
        description="Run the M_blend composite-signal backtest."
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

    logger.info("Building composite within MAJOR_EXPANSION")
    df = attach_composite(df, target_class="MAJOR_EXPANSION")
    n_composite = int(df[COMPOSITE_COL].notna().sum())
    logger.info(f"  {n_composite:,} rows have a composite score.")

    # Component diagnostics on the same population.
    maj_sub = df[df["event_class"] == "MAJOR_EXPANSION"]
    correlation_df = component_correlations(maj_sub, COMPOSITE_SPECS)

    logger.info("Running 6 backtest variants (3 signals x 2 horizons)")
    variant_results = run_all_variants(df)
    summary_rows = [_summary_row(v) for v in variant_results]

    stamp = args.date or datetime.now().strftime("%Y%m%d")
    if args.output:
        out_html = Path(args.output)
    else:
        slug = f"backtest_{stamp}_blend"
        out_html = Path("backend/data/analysis") / slug / f"{slug}.html"
    out_html.parent.mkdir(parents=True, exist_ok=True)

    metadata = {
        "date_stamp": stamp,
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "pipeline_version": PIPELINE_VERSION,
        "row_count": len(df),
        "txn_cost_bps": DEFAULT_TXN_COST_BPS,
    }

    html = render_html(
        summary_rows=summary_rows,
        variant_results=variant_results,
        correlation_df=correlation_df,
        metadata=metadata,
    )
    out_html.write_text(html)
    logger.info(f"Wrote HTML: {out_html}")

    if not args.no_markdown:
        md = render_markdown(
            summary_rows=summary_rows,
            variant_results=variant_results,
            correlation_df=correlation_df,
            metadata=metadata,
        )
        out_md = out_html.with_suffix(".md")
        out_md.write_text(md)
        logger.info(f"Wrote Markdown: {out_md}")


if __name__ == "__main__":
    main()
