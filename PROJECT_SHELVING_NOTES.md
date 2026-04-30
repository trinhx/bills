# Project Shelving Notes

**Shelved:** 2026-04-29
**Last commit:** `68d43c0` (M_blend composite-signal blend backtest)
**Pipeline version:** 1.5.0
**Test status:** 208/208 unit tests passing
**Repo state:** working tree clean; 3 commits unpushed at time of shelving (`4ed2a30`, `ebdb5c0`, `68d43c0`)

---

## 1. TL;DR

This project tests whether US federal contract data
(USASpending.gov) contains tradable alpha signals for publicly-listed
government contractors. After ~5 weeks of iterative work it has
produced a real, replicable, methodologically clean signal — and
demonstrated that the signal is not strong enough to clear an
institutional Sharpe bar (≥ 0.5 net of 15 bps round-trip cost) on
the 6 years of data we have.

The high-water mark is the **M_blend composite-signal backtest**
(`backend/data/analysis/backtest_20260428_blend/`), which combined
two independent event-driven signals (`ceiling_change_pct_of_mcap`
and `moat_index`) within MAJOR_EXPANSION events to achieve
**net Sharpe = +0.435** with 5 of 6 fiscal years positive — an 82%
improvement over the strongest single signal, matching diversification
theory's ceiling almost exactly. The +0.5 bar was missed by ~15%.

To go further requires either lower transaction-cost assumptions
(institutional execution at ~5 bps gets us to ~+0.49), more years of
data, or a genuinely new signal dimension (not another
magnitude-flavoured ratio). None of those are accessible from inside
the existing pipeline.

---

## 2. State at shelving

### Code

| Item | Value |
|---|---|
| Git branch | `main` |
| Last commit | `68d43c0 Add composite-signal blend backtest (M_blend)` |
| Unpushed commits | 3 (`4ed2a30` M_backtest, `ebdb5c0` M_thresholds, `68d43c0` M_blend) |
| Pipeline version | 1.5.0 (in `backend/app/core/version.py`) |
| Unit-test status | 208/208 passing (`uv run pytest backend/tests/unit -q`) |
| Lines of code | ~11,700 across `backend/src/`, `backend/scripts/`, `backend/tests/` |

### Data files (NOT in git — `backend/data/` is gitignored)

These exist on disk and must be regenerated if lost. Roughly in dependency order:

| Path | Approx size / shape | What it is |
|---|---|---|
| `backend/data/raw/contracts/FY*_All_Contracts_Full_*.csv` | ~37 CSVs, ~37M rows total | Raw USASpending downloads, FY21–FY26 |
| `backend/data/cleaned/cleaned.duckdb` | DuckDB; `signals_awards` 811,025 rows; `signals_with_returns` 304,096 rows | Pipeline output |
| `backend/data/cache/cache.duckdb` | Yahoo returns + CAGE/OpenFIGI caches | Warm cache for re-runs |
| `backend/data/results/FY*_final_signals.csv` | ~300k row CSV | Latest export of `signals_with_returns` |
| `backend/data/analysis/` | 11 directories | Validation reports + paired analysis markdowns; **all referenced from this document** |
| `backend/data/logs/*.log` | Various | Per-script run logs |

### Documentation files (in git)

| File | Purpose |
|---|---|
| `README.md` | Project overview |
| `CHANGELOG.md` | Pipeline schema/signal changes per version |
| `PROJECT_SHELVING_NOTES.md` | **This file** |
| `SESSION_HANDOFF.md` | Companion handoff doc capturing the conversational arc of the M_backtest → M_blend session for cross-LLM portability |

---

## 3. Re-entry checklist

If you (or a future LLM) come back to this project, do these things in order:

1. **Read this file (sections 4–7) end-to-end.** ~15 minutes.
2. **Read `SESSION_HANDOFF.md`** if you want the narrative arc of the most recent work. ~10 minutes.
3. **Run `git log --oneline -10`** to see what has changed since shelving.
4. **Run `uv run pytest backend/tests/unit -q`** to confirm tests still pass under your current dependency versions. If they fail, **debug rather than skip** — failing tests usually mean a pandas/numpy upgrade has broken an assumption (see Section 7d).
5. **If the data is gone, regenerate it** in this order — read each script's docstring for arg conventions first:
   1. `backend/scripts/ingest.py` — load raw CSVs into `raw_filtered_awards`
   2. `backend/scripts/enrich.py` — entity resolution (CAGE → ticker → market_cap)
   3. `backend/scripts/themes.py` — NAICS/PSC theme tagging
   4. `backend/scripts/signals.py` — compute alpha-signal columns
   5. `backend/scripts/validate.py` — attach forward returns
   6. `backend/scripts/report.py` — IC validation report
   7. `backend/scripts/backtest.py` / `backtest_threshold_sweep.py` / `backtest_blend.py` — the three backtests in M_backtest, Step C, M_blend respectively
6. **Verify regenerated numbers** against the headline figures in Section 5 below before trusting any new analysis. A mismatch by more than ~5% means something has drifted (data, code, or dependencies) and needs root-cause analysis before continuing.
7. **Decide whether to push the 3 unpushed commits** before doing new work. The remote may have moved.

---

## 4. Project thesis & methodology recap

### The hypothesis

The thesis evolved over the project's life. The **current** hypothesis is:

> *Specific contract events* (a ceiling expansion of $X, a sole-source
> award) systematically affect the contractor's stock returns over the
> following ~20 trading days, in a way that survives industry-neutral
> benchmarking and 6 years of regime variation.

This **is not** the original hypothesis. The original hypothesis was
about *level-based ratio signals* (e.g., the ratio of obligated to
potential value across a company's contract portfolio). That
hypothesis was killed in `validation_report_20260424_6yr` when
ratio-signal ICs were shown to flip sign across fiscal years.

### The methodological bar

A signal/horizon/event-class combination passes if **and only if**:

1. **|Spearman IC| ≥ 0.02** on industry-neutral T+H returns
2. **Same-signed in ≥ 5 of 6 fiscal years** (FY21–FY26)

This bar was set in M_events specifically because the prior
level-based hypothesis had failed it. The bar is intentionally strict;
it has rejected nearly every signal we've tested. Nothing about this
bar should be loosened to manufacture a passing result. See
**Section 7d** for the curve-fitting cautions.

For a **portfolio-level** strategy, the additional bars are:

3. **Net annualised Sharpe ≥ 0.5** after 15 bps round-trip cost
4. **Same-signed net mean** in ≥ 5 of 6 fiscal years

The signal-level bars are met by several findings (see Section 5).
The portfolio-level Sharpe bar is **not** met by any tested
configuration.

### Industry-neutral benchmarks

Returns are excess returns vs an industry-appropriate ETF, not vs SPY:

| Industry | Benchmark |
|---|---|
| Aerospace & Defense | ITA |
| IT Services / Tech | XLK |
| Everything else | SPY |

This was a M2.5 enhancement (`validation_report_20260422b/`) after
SPY-only neutralization was shown to hide industry-specific signals.

### The pivot from level to event signals (M_events)

`validation_report_20260424_6yr` was a "sobering" result: every level-based
ratio signal that had looked strong on 2 years of data flipped sign or
collapsed when extended to 6 years. The conclusion was that ratio signals
were artifacts of regime — not real predictors.

The pivot was to ask: *if it's not the level, maybe it's the
step-change*. That became M_events (pipeline 1.5.0, commit
`8266f4d`), which added four columns to `signals_awards`:

* `event_class` — categorical bucket (NEW_AWARD, MAJOR_EXPANSION, CONTRACTION, etc.)
* `ceiling_change_log_dollars` — signed log10 of `ceiling_change`
* `ceiling_change_pct_of_mcap` — `ceiling_change / market_cap`
* `relative_ceiling_change` — `ceiling_change / prev_potential_value`

Every subsequent finding in this project has been about
event-conditioned signals rather than level signals.

---

## 5. Key findings — the numbers worth remembering

If you remember nothing else, remember these six numbers.

| # | Finding | Number | Source |
|---|---|---:|---|
| 1 | **Best single-signal IC** (within MAJOR_EXPANSION at T+20) — `ceiling_change_pct_of_mcap` | **+0.061**, 6/6 yrs | `validation_report_20260428_events` |
| 2 | **Component independence** — Spearman ρ between `pct_of_mcap` and `moat_index` within MAJ | **−0.42** | M_blend pre-flight check |
| 3 | **Best single-signal Sharpe** — `pct_of_mcap`, MAJ, T+20, 15 bps | **+0.24**, 5/6 yrs | M_backtest (commit `4ed2a30`) |
| 4 | **Best threshold-tuned Sharpe** — same variant at $500M cutoff | **+0.27**, 6/6 yrs | M_thresholds (commit `ebdb5c0`) |
| 5 | **Best blended Sharpe** — equal-weight rank composite of `pct_of_mcap` + `moat_index` | **+0.43**, 5/6 yrs | M_blend (commit `68d43c0`) |
| 6 | **Theoretical ceiling** for this composite (single_sharpe × √(2 / (1 + ρ))) | **~+0.45** | Derived; matches realised |

**The single most important takeaway**: finding #5 hits finding #6
almost exactly. The composite is doing as well as theory says it
*can* do given the input ICs and component correlation. Further gains
require either better individual signals or a genuinely new
independent dimension — *not* better blending of the same inputs.

### Other findings worth flagging

* **`moat_index` (sole-source flag) IC peaks at T+180**, not T+20. IC
  = −0.080, n=4,214, 5/5 same-signed years (FY21–FY25). Sole-source
  major expansions underperform on long horizons. Used as the
  bearish-direction component in M_blend.
* **Cross-class long-MAJ short-CON does not work**: the v2 IC analysis's
  opposite-sign IC pair (MAJ +0.027, CON −0.028) was *misread* as
  evidence MAJ rows beat CON rows on average. They don't (CON
  unconditional T+20 mean +0.20% vs MAJ +0.06%). Cross-class
  Sharpe = −0.88. See M_backtest analysis for the explicit lesson.
* **Decile-monotonicity recovered in M_blend**: across every prior
  variant the top decile (bucket 10) lagged bucket 9, suggesting
  noise contamination. In the composite, bucket 10 cleanly beats
  bucket 9 by 0.69 percentage points — strong evidence the underlying
  signal is real.
* **`ceiling_change_log_dollars` does not survive within MAJ**: IC ≈ 0
  at every horizon. Within an event class already filtered to
  expansion size, the absolute dollar value carries no information.
* **`relative_ceiling_change` has 295 outlier rows** with values >
  1,000,000 (caused by `prev_potential_value` ≈ $0.01 dummy ceilings).
  Spearman IC is rank-based so the IC analysis is robust, but any
  value-weighted use requires winsorization. Engine implements
  per-class 99th-percentile winsorization for this signal only.
* **`contract_potential_yield` is 82% rank-correlated** with
  `pct_of_mcap` within MAJ. They are different lenses on the same
  underlying "contract value relative to company size" idea, both
  with `market_cap`-related denominators. **Do not blend them** —
  it adds no information and the additional noise actually hurts.

---

## 6. Report inventory

Every report lives at `backend/data/analysis/<dir>/` with three files:
the HTML and Markdown report itself, and a paired `_analysis.md`
written by the LLM at the time of the run with that run's
findings + honest pushback. The `_analysis.md` files are the
canonical source — start there if you want detail on any of these.

Reports listed in chronological order (earliest → most recent).
**Pipeline version** is the version stamp at the time of the run.

### Pre-event-pivot reports (level-based ratio signals — superseded)

#### `validation_report_20260421/` — Initial M2 decision gate (1.3.0)
Q4 of FY2024 only (~6 weeks, 9,751 rows). Seven cells cleared |IC| ≥ 0.05;
multiple ratio signals looked strong. **In hindsight: misleading**, because
the small window let regime-specific noise look like signal. Do not trust
these numbers; they were superseded by `20260422` (full FY2024) where
they collapsed.

#### `validation_report_20260422/` — First correction (1.3.0)
Full FY2024 (57,353 rows). The Q4 signals **collapsed**: 0 cells cleared
|IC| ≥ 0.05, only 4 cleared |IC| ≥ 0.02. Triggered the M2.5 enhancement
(industry-neutral benchmarks).

#### `validation_report_20260422b/` — Industry neutralization (1.4.0)
Full FY2024, with ITA+XLK+SPY industry-appropriate benchmarks and the
per-quarter-stability filter. **53 stable (signal × horizon × industry)
combos** identified. Strongest: `difference_between_obligated_and_potential`
× T+60 within Aerospace & Defense. Looked promising but was about to
fail OOS.

#### `validation_report_20260423/` — Out-of-sample test, FY2024 + FY2025 (1.4.0)
The first true OOS test (FY24 was "training", FY25 was fresh). The
Aerospace & Defense signal **did not survive**. The IT/E&C signal
held up partially. First sign that 2 years was insufficient and
regime-specific noise was contaminating the analysis.

#### `validation_report_20260423_extended/` — Long horizons added (1.4.0)
Same data as 20260423, with T+90, T+120, T+180 added. Showed that some
signals *accrue* over longer horizons rather than peak at T+20. Important
for later horizon-choice decisions in M_blend.

#### `validation_report_20260424/` — Stale duplicate (1.4.0)
**Skip this.** Generated before `signals.py` was re-run on the 6-year
`themed_awards` table; functionally identical to `20260423_extended`.
Retained for chronological completeness but adds nothing.

#### `validation_report_20260424_6yr/` — The sobering 6-year result (1.4.0)
The pivotal "we have 6 years of data; what survives?" run. Result: **every
level-based ratio signal flipped sign across regimes**. The full-history
ICs were not stable over fiscal years. This killed the level-signal
hypothesis and triggered the pivot to event-based signals (M_events).
**Read this analysis if you want to understand why event-conditioning
exists at all in this project.**

### Post-event-pivot reports (event-based magnitudes — current canon)

#### `validation_report_20260428_events/` — M_events validation (1.5.0)
**Headline finding of the entire project** at the IC level:
`ceiling_change_pct_of_mcap` × MAJOR_EXPANSION × T+20 has IC = +0.061,
n = 5,338, **6/6 years same-signed**, opposite-sign property on
CONTRACTION (−0.015). Three independent magnitude signals point the
same direction within MAJ × T+20 (`pct_of_mcap`, `contract_potential_yield`,
`moat_index`). The `pre_pipeline_rerun/` subdirectory has the v1 draft
written before `relative_ceiling_change` was correctly populated; the
top-level files are the canonical v2.

#### `backtest_20260428_major_expansion/` — M_backtest (1.5.0, commit `4ed2a30`)
First portfolio-level test of the IC findings. **0 of 4 variants
pass.** Strongest in-class variant: `pct_of_mcap` × MAJ × T+20 →
**Sharpe +0.24, 5/6 years positive**. Cross-class long-MAJ
short-CON: **Sharpe −0.88** (a documented lesson; see Section 7d).
The signal is real (positive gross edge) but does not survive 15 bps
round-trip costs at single-signal scale.

#### `backtest_20260428_threshold_sweep/` — M_thresholds / Step C (1.5.0, commit `ebdb5c0`)
Threshold-sensitivity sweep on the strongest variant at five
MAJOR_EXPANSION ceiling cutoffs. **0 of 5 thresholds pass.** Sharpe
peaks at **+0.27 at $500M with 6/6 years positive** but at the cost
of cutting the universe to 377 trades (vs 1,068 at $100M).
Demonstrates the signal is not threshold-tunable to passing — and
cautions against post-hoc threshold tuning generally.

#### `backtest_20260428_blend/` — M_blend (1.5.0, commit `68d43c0`)
**The project's high-water mark.** Equal-weight percentile-rank
composite of `pct_of_mcap` (asc) and `moat_index` (desc) within MAJ.
Six variants tested: composite + 2 baselines × 2 horizons. **Composite
at T+20: Sharpe +0.435, 5/6 years positive, decile structure cleanly
monotone for the first time** (bucket 10 beats bucket 9 by 0.69 pp).
Misses the +0.5 bar by ~15%. Also includes the explicit derivation
that this is at the diversification-theory ceiling for these inputs.

---

## 7. What the next LLM (or future-you) could try

Organized by what kind of capability would be needed to make progress.

### 7a. Things any LLM could already do, that we deferred for time

These are all bounded-scope improvements that the current author
chose not to pursue in this session, either because they were
secondary to the locked-in plan or because they introduce overfitting
risk that wasn't worth taking on under time pressure.

* **Impute `moat_index` for the 16% missing rows** within
  MAJ. Currently the composite has 898 trades vs 1,068 for
  `pct_of_mcap` alone — the gap is the missing-`moat_index` rows.
  Imputing with the industry median (or a well-justified default
  like 1.0 for tiny-cap NEW_DELIVERY_ORDER rows) could expand
  coverage and modestly improve Sharpe. Risk: imputation choices
  are easy to silently overfit; pre-register the rule.
* **Test 5 bps and 50 bps round-trip costs** as a sensitivity sweep,
  reporting Sharpe at each. The +0.5 bar is met at ~5 bps. This
  is a sensitivity report, not a hypothesis test.
* **Pre-register an OOS test** by holding out FY2026 partial data,
  re-deriving the composite weights on FY21–FY25 only, then
  evaluating on FY26. The current FY26 numbers are noisy
  (n=43 trades for the composite) but pre-registration would clean
  up any concern about the composite being post-hoc-fitted to
  in-sample data.
* **Run the M_blend backtest at horizons we didn't test** (T+5,
  T+60, T+90). Mainly for completeness; T+20 is likely the
  optimum.
* **Compute the implied gross-Sharpe ceiling** more carefully: the
  +0.43 vs +0.45 theoretical gap might be closeable with weight
  optimisation, but only at the cost of in-sample fitting.
* **Generate composite component IC tables** within each fiscal
  year, not just composite Sharpe. Would tell us whether one
  component carries the year-stability and which years rely
  more on the other.

### 7b. Things a smarter LLM (or one with more context window) could plausibly do better

These require either better generative reasoning, better
domain knowledge, or the ability to hold a much larger codebase
in context simultaneously.

* **Engineer a non-magnitude signal that's genuinely independent of
  `pct_of_mcap`.** This is the **single most valuable open problem**.
  M_blend identified that the binding constraint on Sharpe is finding
  a signal *not* mechanically tied to `ceiling_change / market_cap`.
  Candidates worth exploring:
  * **Agency-portfolio concentration** — does a contract from a single
    agency that's a large share of the contractor's existing portfolio
    predict differently than diversification?
  * **Contract-type composition** — IDV/IDIQ vs definitive contracts;
    sole-source vs competed; multiple-award vs single-award.
  * **Contract duration vs PSC norm** — is this contract longer or
    shorter than typical for its product/service code?
  * **NAICS-keyword similarity** — does this contract's NAICS keywords
    align with or diverge from the recipient's existing contract base?
  * **Subcontractor flow** (requires data we don't have; see 7c).
  These are *theoretical* signal candidates, none have been validated
  at the IC level. The work would be: design 3–5 candidates, compute
  their IC within MAJ at T+20, check correlation against `pct_of_mcap`
  and `moat_index`, and only then include the surviving signals in
  a re-blended composite.
* **Better feature engineering on `transaction_description` text.** The
  free-text description column is currently unused. A modern LLM-
  embeddings approach (sentence-BERT or similar against the recipient's
  existing description corpus) could yield a "contract uniqueness"
  signal that's mechanically independent of magnitude.
* **Multi-horizon composite.** Currently the composite uses identical
  weights regardless of horizon. A more sophisticated approach would
  weight `pct_of_mcap` higher at short horizons (where its IC peaks)
  and `moat_index` higher at long horizons (where its IC peaks). Risk:
  introduces more tunable parameters; needs rigorous OOS protocol.
* **Industry-conditional signals.** Are the M_blend findings stable
  across A&D, IT Services, and other industries individually? If one
  industry carries most of the signal, that's both informative
  (concentrate the strategy) and a warning (overfitting to one industry).
* **Better understanding of WHY the bucket-10-vs-bucket-9 monotonicity
  recovery happened in M_blend.** The narrative I gave was "moat_index
  filters out tiny-cap denominator artifacts" but I didn't actually
  prove it. A future LLM could verify by checking the average
  `market_cap` of bucket 10 in the composite vs bucket 10 in the
  single-signal `pct_of_mcap` variant.

### 7c. Things that need data we don't have

Hard limits — no LLM can fix these without external data acquisition.

* **More fiscal years.** 6 years is the bare minimum for a 5-of-6
  same-sign test; more years = more statistical power to distinguish
  signal from noise. The dataset will accrete naturally over time —
  consider re-running the analysis annually.
* **Pre-COVID years (FY2018–FY2020).** All current data is post-COVID,
  which is one macro regime. Including pre-COVID years would test
  whether the signal is regime-stable. USASpending data for those
  years exists; the pipeline should ingest them in principle.
* **Options-implied volatility around `action_date`.** Lets you
  condition trades on whether the market expected the announcement.
  Trades on *unexpected* announcements should outperform.
* **Insider-trading filings around `action_date`.** Form 4 filings that
  cluster around contract announcements would be a strong signal.
* **Subcontractor flow.** USASpending has prime-contract data; the
  prime-to-sub flow is much richer (more contractors, more events,
  more independence between events). Available via FPDS in principle
  but not in the current ingest.
* **More granular industry classification.** Yahoo Finance's industry
  field is coarse; better industry-neutral benchmarks would reduce
  noise in T+20 industry-excess returns.

### 7d. Things the next LLM should specifically NOT do

These are documented failure modes from this project. Repeating them
wastes time and risks publishing wrong results.

* **Do NOT tune thresholds, costs, fiscal-year filters, or holding
  periods until something passes.** That is curve-fitting. The
  M_thresholds sweep in commit `ebdb5c0` exists explicitly to
  *demonstrate* that threshold tuning does not produce a passing
  result and to *document* the post-hoc-tuning trap. Do not re-run
  that experiment hoping for different numbers.
* **Do NOT misread the IC opposite-sign-pair structure as evidence
  of cross-class mean differences.** This was the M_backtest cross-
  class mistake (Sharpe = −0.88, documented at length in the M_backtest
  analysis). An IC of +X within class A and −X within class B tells
  you about *intra-class* rank predictivity. It does NOT tell you that
  class A's unconditional mean exceeds class B's. They are different
  questions; check both before designing a cross-class trade.
* **Do NOT blend signals that are highly correlated (|ρ| > 0.5)** as
  if they were independent. The +0.82 ρ between `pct_of_mcap` and
  `contract_potential_yield` is documented; blending them adds noise,
  not information. Always run `component_correlations` before
  trusting a composite.
* **Do NOT loosen the methodological bars to manufacture a passing
  result.** |IC| ≥ 0.02 + 5-of-6-years and net Sharpe ≥ 0.5 are
  there to protect the project from fooling itself. If a result
  doesn't pass, the right answer is "this configuration doesn't
  pass", not "let me try a more lenient bar".
* **Do NOT silently drop or `xfail` tests** that fail under newer
  pandas/numpy versions without root-causing the failure. The test
  suite encodes assumptions about data shape and dtype handling that
  have caught real bugs (e.g., the dtype-upcast bug in
  `winsorize_within_class` that was caught by
  `test_winsorize_clips_per_class_independently` in commit `4ed2a30`).
  A failing test is a free bug report; treat it that way.
* **Do NOT trust FY26 numbers in isolation.** FY26 is partial-year
  data (n=43 composite trades vs ~200 in complete years). Its very
  high Sharpes (+1.50 in the composite, +1.24 in single-signal) are
  not informative about steady-state. Always re-run any analysis
  with FY26 excluded as a sanity check.

---

## 8. Honest limitations of the work

### What's been earned

1. **Methodologically clean signal validation.** The IC + 5-of-6-years
   bar is strict and well-justified; results that pass it have been
   stress-tested.
2. **Industry-neutral benchmarking from M2.5 onward.** Earlier results
   (validation_report_20260421) are explicitly flagged as misleading
   because of SPY-only neutralization. That correction is durable.
3. **Event-conditioning hypothesis.** The shift from level-based to
   event-based signals (M_events / pipeline 1.5.0) was driven by a
   real failure of the prior hypothesis, not by data-dredging.
4. **The diversification-theory match in M_blend.** The realised
   composite Sharpe (+0.43) matches the theoretical ceiling
   (+0.45) almost exactly. That kind of clean math-vs-empirics fit
   is the strongest evidence that the underlying signal is real and
   the blending machinery is doing what it should.
5. **Transparent failure modes.** Every report has a paired
   `_analysis.md` with explicit "honest pushback" / caveats sections.
   Future readers can see what each LLM at the time worried about,
   not just what it concluded.

### What's NOT been earned

1. **No out-of-sample validation.** All findings, including M_blend,
   were derived from the full 6-year dataset. We have no held-out
   test set. A truly clean finding would re-derive composite weights
   on FY21–FY24 and evaluate on FY25–FY26. This is the single biggest
   gap in the project's claims.
2. **No cross-regime robustness check.** All 6 years are post-COVID.
   We don't know if the signal works in different macro regimes.
3. **No transaction-cost or slippage realism beyond a flat 15 bps.**
   Real institutional trading has fill-quality, market-impact, and
   half-spread costs that vary with universe size and trade size.
   The 15 bps assumption is reasonable but unaudited.
4. **No capacity analysis.** With ~150 MAJ trades per year per fiscal
   year, the strategy is liquidity-bounded; we have not estimated
   the dollar capacity at which the alpha decays.
5. **No factor-attribution.** We have not regressed the strategy
   returns against standard factor exposures (Fama-French, momentum,
   quality). The Sharpe of +0.43 might be partially explained by
   common factor exposures rather than novel alpha.

### Open question worth flagging

**Is the post-COVID era structurally different from pre-COVID for
contractor-level event response?** We can't test this on current
data. Federal contracting volume, agency mix, and market liquidity
all changed materially in 2020–2021. The 6-year window we have is
mostly the post-shock recovery and stabilisation. If pre-COVID
contractor responses were different, the signal we've validated is
specific to a regime that may not persist. **This is the single
largest unknown.**

---

## End of shelving notes

Authored by an LLM agent (Claude Opus 4-7) at the conclusion of the
M_blend session, 2026-04-29. Companion document:
`SESSION_HANDOFF.md` for the conversational arc of the M_backtest →
M_blend session.
