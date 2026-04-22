# Changelog

All notable changes to the USASpending alpha pipeline output schema and
signal formulas are documented here. Downstream consumers should pin a
specific ``pipeline_version`` and consult this file before upgrading.

Format: [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) + semver-ish.
* MAJOR -- breaking formula changes or column removals
* MINOR -- new columns or new signals (additive)
* PATCH -- bug fixes that don't change the schema

The version string is defined in
``backend/app/core/version.py::PIPELINE_VERSION`` and is stamped on every
row of ``signals_awards``.

---

## [1.4.0] — Milestone 2.5: Industry Neutralization + Per-Quarter Stability

Additive enhancement to the M2 validation harness. Surfaces signals that
are hidden when the report aggregates at the Yahoo-sector level rather
than the finer-grained industry level, and filters to combinations that
are stable across fiscal quarters. No schema changes to ``signals_awards``.

### Added

* **Industry-specific benchmark ETFs** in the returns provider:
  * ``Aerospace & Defense`` industry → **ITA** (iShares U.S. Aerospace
    & Defense ETF)
  * ``Information Technology Services`` industry → **XLK** (Technology
    Select Sector SPDR)
  * All other industries → SPY (canonical broad-market fallback)
  Configured via a single ``INDUSTRY_BENCHMARK_MAP`` in
  ``backend/app/services/providers/returns.py`` and resolved per-row via
  the new ``industry_benchmark_ticker_for(industry)`` helper.
* **4 new columns on ``signals_with_returns``**:
  * ``industry_benchmark_return_{1,5,20,60}d`` — the forward return of
    the row's industry ETF
  * ``industry_excess_return_{1,5,20,60}d`` — ticker return minus its
    industry ETF return (industry-neutral alpha)
* **2 traceability columns**:
  * ``industry_benchmark_ticker`` — which ETF was subtracted for the row
  * ``fiscal_quarter`` — FY24Q1 / FY24Q2 / FY24Q3 / FY24Q4 tag
* **Section 6 of the report**: "Industry-level IC breakdown". For the
  top-candidate signals (``difference_between_obligated_and_potential``,
  ``acv_alpha_ratio``, ``moat_index``), shows SPY-neutral and
  industry-neutral IC side-by-side, broken down by Yahoo ``industry``
  (not the coarser ``sector``). Industries with < 200 rows are omitted.
* **Section 7 of the report**: "Per-quarter stability filter". Lists
  every (signal, horizon, industry) combo with ``|IC| >= 0.02`` AND the
  IC same-signed in >= 3 of 4 fiscal quarters. Uses industry-neutral
  returns. This is the definitive "survived the regime-change test"
  table.
* Section numbering bumped: Decision Summary moved from 6 → 8.
* Companion markdown output now includes the industry + stability sections.

### Changed

* ``compute_excess_returns_for_signals`` signature:
  ``benchmark_bars`` now takes a ``Dict[str, DataFrame]`` keyed by
  ticker (SPY + ITA + XLK) instead of a single ``spy_bars`` frame. This
  lets the function emit both SPY-neutral and industry-neutral excess
  returns in one pass.
* ``ensure_benchmark_pre_fetched`` in ``backend/src/io.py`` now routes
  through ``fetch_daily_bars(ticker, ...)`` for non-SPY benchmarks
  (industry ETFs), and keeps ``fetch_benchmark(...)`` for SPY itself.
  Tests that only mock ``fetch_benchmark`` remain compatible.

### Tests

* **10 new tests**:
  * ``test_compute_excess_returns_uses_industry_benchmark`` — verifies
    an A&D-tagged row has its industry-excess computed against ITA
    (2% vs a 5% raw return with 3% ITA drift) rather than SPY.
  * ``test_compute_excess_returns_fiscal_quarter_tag`` — verifies
    FY24Q1..Q4 mapping for dates across the fiscal year.
  * ``test_ic_by_industry_filters_by_min_rows`` + ``_sorts_by_abs_ic``
  * ``test_build_industry_ic_sections_renders_all_top_signals``
  * ``test_per_quarter_stability_returns_empty_on_random_data``
    (iid-noise fixture must not over-produce matches).
  * ``test_per_quarter_stability_surfaces_consistent_signals`` (an
    engineered always-positive A&D signal must clear 4/4 quarters).
  * ``test_build_per_quarter_stability_section_handles_empty_gracefully``
  * ``test_generate_report_includes_new_sections`` (HTML + markdown).

### Operational notes

* First full-year production run produced 57,353 rows with distribution:
  ITA = 32,229; SPY = 16,309; XLK = 8,815. 100% forward-return coverage
  at all four horizons. Cache hot from prior M2 run: only 321 new ITA
  bars + 321 new XLK bars were fetched (~7 seconds).
* The per-quarter stability filter immediately surfaced
  ``difference_between_obligated_and_potential`` at T+60d within
  Aerospace & Defense: **IC = −0.070, p < 0.0001, consistent sign in
  3 of 4 quarters**. This is the first signal on the full-year data that
  clears the ``|IC| >= 0.05`` threshold AND survives quarterly stability.

---

## [1.3.0] — Milestone 2: Alpha Validation Harness

Adds the forward-return validation stage (the decision gate for the
entire project). No changes to ``signals_awards`` schema; this
milestone is purely additive — new table, new scripts, new analytics,
new report.

### Added

* **``cache.cache_returns``** DuckDB table for split/dividend-adjusted
  daily bars, keyed by ``(ticker, date)``. Permanent cache (historical
  prices are immutable; we never overwrite a cached row).
* **``backend/app/services/providers/returns.py``** — ``ReturnsProvider``
  Protocol and ``YFinanceReturnsProvider`` concrete implementation
  using ``yf.Ticker.history(auto_adjust=True)``. Reuses the M1.5
  ``_normalize_ticker_for_yahoo`` helper so multi-class tickers
  (``BRK/A`` → ``BRK-A``) route correctly. Rate-limited via a new
  ``RETURNS_RATE_LIMITER`` (10 req / 10s).
* **``backend/scripts/validate.py``** — Phase 5 orchestrator that
  loads ``signals_awards`` rows where ``is_public`` AND ``market_cap``
  is present, pre-fetches SPY for the global date range, fetches
  per-ticker bars (cache-first), and joins forward + SPY-excess
  returns at T+1 / T+5 / T+20 / T+60 trading days. Writes
  ``signals_with_returns`` + Parquet export.
* **``backend/src/analyze.py``** — pure-function analytics:
  ``information_coefficient`` (Spearman, NaN-safe), ``ic_per_sector``,
  ``decile_spread``, ``top_minus_bottom``, ``cumulative_pnl``
  (long-top-decile / long-short strategies), ``signal_coverage``,
  ``summarize_all_signals``. No DuckDB or filesystem dependencies --
  unit-tested against synthetic datasets with known ground-truth IC.
* **``backend/scripts/report.py``** — idempotent HTML + Markdown
  report generator that reads ``signals_with_returns`` and renders:
  * Executive summary (signal × horizon IC / TMB table)
  * Data coverage (rows per signal, per horizon, ``signal_quality``
    distribution, ``is_primary_action`` filter comparison)
  * Per-signal detail sections with embedded PNG plots:
    IC by horizon, IC heatmap by sector × horizon, decile spread,
    cumulative P&L (long top decile + long-short)
  * Signal-quality crosscut (``signal_quality = 'ok'`` subset)
  * Robustness: IC by ``transaction_type``
  * Decision summary with three threshold criteria presented
    side-by-side (|IC| ≥ 0.02 / |IC| ≥ 0.05 / both IC AND 50 bps spread),
    leaving the verdict to the review session.

### Dependencies

* Added ``scipy`` (Spearman correlation, used by ``information_coefficient``).
* Added ``matplotlib`` (non-interactive ``Agg`` backend) for the
  embedded PNG plots in the HTML report.

### Tests

* **36 new tests** across four new test modules:
  * ``test_returns_provider.py`` (8): protocol conformance, auto_adjust,
    ticker normalisation, tz-handling, empty / delisted responses.
  * ``test_returns_cache.py`` (9): upsert idempotency, range queries,
    benchmark flag, ``ensure_benchmark_pre_fetched`` short-circuit.
  * ``test_validate_harness.py`` (10): forward-return math, trading-
    day-rollforward, missing-ticker NaN-fill, run_validation idempotency.
  * ``test_analyze.py`` (14): IC ground-truth (perfect alpha → 1.0;
    iid noise → ~0), decile monotonicity, cumulative P&L manual-calc.
  * ``test_report.py`` (9): full HTML + Markdown generation against a
    fixture, missing-column graceful handling.
  * ``test_validation_pipeline.py`` (2): end-to-end integration of
    validate.py + report.py; idempotency across a second run.

### Operational notes

* **First full production run** (FY2024, 27,927 signals_awards rows):
  * 9,751 eligible rows (public ticker + resolved market cap).
  * 326 unique tickers fetched; ~5 minutes wall-time one-off, then
    cached forever.
  * 100% forward-return coverage at all four horizons (the action
    date range 2024-08-19 → 2024-09-30 plus a 95-day look-forward
    window comfortably spans T+60 for every row).
  * Parquet and HTML/Markdown report written to
    ``backend/data/analysis/``.

---

## [1.2.0] — Milestone 1.6: CAGE Auth Resilience

Operational hardening for the CAGE scraper's notoriously short-lived
session cookies. No schema changes; same 65-field ``SignalsAward``
contract. Only ``pipeline_version`` values change on new rows.

### Added

* **Typed ``CageAuthExpiredError``** in ``cage_scraper.py``. Raised
  (rather than silently failing or calling ``sys.exit``) when:
  * required env vars are missing,
  * the live validation request hits the terms-of-use page,
  * a Cloudflare anti-bot challenge fires, or
  * an inline ``fetch_html`` call receives any of the above mid-run.
* **Inline auth-failure detection** inside every ``fetch_html`` call.
  Previously a mid-run cookie expiry would produce silently-parsed
  terms-page HTML and land as generic HTTP 500s in ``cache_failures``;
  now it raises ``CageAuthExpiredError`` and the pipeline can recover.
* **Sentinel exit code 42 (``CAGE_AUTH_EXIT_CODE``)** from ``enrich.py``
  on any CAGE auth-expiry (startup or mid-run). The cache is preserved
  before exit.
* **Interactive cookie-refresh prompt** in ``run_pipeline.sh``. When
  ``enrich.py`` exits with code 42, the script displays instructions
  and reads a curl command from stdin (Ctrl+D to commit, Ctrl+C to
  abort). ``.env`` is updated in place and the enrichment loop resumes
  with the same pass counter (cache-hit path skips already-resolved
  codes).
* **``backend/scripts/utils/update_cage_cookies.py``** — curl-parsing
  utility. Extracts ``PHPSESS`` and ``__RequestVerificationToken`` from
  a pasted curl command, writes a timestamped backup (``.env.backup-
  <YYYYMMDD-HHMMSS>``), upserts the two keys in place preserving every
  other line, then live-validates the new cookies against
  cage.dla.mil. Modes: ``--stdin`` (default), ``--curl-file``,
  ``--dry-run``, ``--no-backup``, ``--skip-validation``.
* **``backend/scripts/utils/invalidate_stale_cage_failures.py``** —
  one-shot utility to drop all ``provider = 'cage'`` rows from
  ``cache_failures``, so the next run retries them instead of waiting
  out the backoff window.
* **``--retry_cage_failures`` flag** on ``run_pipeline.sh`` — invokes
  the above utility before Phase 2. Useful after a manual cookie rotation.
* **Phase 2 retry loop counts CAGE too** — the backoff check now
  includes ``provider IN ('openfigi', 'cage')`` so a failing-then-fixed
  CAGE scrape doesn't let the loop exit prematurely.

### Changed

* ``_validate_credentials`` in ``cage_scraper.py`` no longer calls
  ``sys.exit``. It raises ``CageAuthExpiredError`` and lets the caller
  decide how to react (``enrich.py`` exits 42 so orchestrators can
  prompt for refresh).

### Tests

* **12 new tests** in ``test_cage_auth_expiry.py`` covering the three
  auth-failure signatures, ``fetch_html`` raising, and
  ``_validate_credentials`` error paths (no network calls; fully mocked).
* **18 new tests** in ``test_update_cage_cookies.py`` covering curl
  parsing (quoted/unquoted/double-quoted, malformed inputs, long token
  values) and ``.env`` manipulation (in-place update, backup creation,
  dry-run, missing-file handling).

### Operational notes

* **Post-deploy CAGE session rotation**: the one-shot
  ``invalidate_stale_cage_failures.py --execute`` drops the 26 stale
  CAGE-500 entries from the previous look-ahead-biased pilot run so
  they're retried on the next pipeline invocation.
* **Running against a live pipeline**: if CAGE cookies expire between
  the ``_validate_credentials`` startup check and the first in-loop
  fetch, the in-loop path will catch it too (``fetch_html`` also checks).

---

## [1.1.0] — Milestone 1.5: Pilot Remediation

Addresses ten issues surfaced by the first end-to-end pilot (5MB sample)
run under the M1 schema. No column additions or removals; same 65-field
`SignalsAward` contract. Values of `transaction_type`, `is_primary_action`,
`alpha_ratio`, `ceiling_change`, and the ``cache_market_cap`` provenance
columns materially shift.

### Fixed

* **`transaction_type` classification gap** — modifications with
  `$0 < federal_action_obligation < $5M` are now classified as
  `FUNDING_INCREASE` (and therefore `is_primary_action = True`). Pilot
  run showed ~60% of filtered rows (105 / 173) were landing on this
  branch with `transaction_type = NULL`; these are real funding actions,
  not clerical modifications.
* **`ceiling_change` computed from full piid history** — Phase 1 now
  runs the LAG window over the unfiltered CSV, then joins the lag
  column back onto the filtered relation. Previously the LAG ran
  post-filter, so rows that were first-in-filtered-sample had NULL
  `ceiling_change` (and therefore NULL `alpha_ratio` when
  `federal_action_obligation = 0`) despite having earlier siblings in
  the raw data. Phase 4 now passes the column through instead of
  recomputing it; falls back to in-function LAG for unit-test relations
  where the column isn't present upstream.
* **Yahoo ticker format normalisation** — `BRK/A` → `BRK-A`,
  `MOG/A` → `MOG-A` at the API boundary (yfinance uses hyphen, OpenFIGI
  returns slash). Eliminates 14 stuck `cache_failures` per pilot run
  for multi-class tickers. Cache keys preserve the OpenFIGI-native form.
* **OpenFIGI Bloomberg-ID rejection** — candidates matching
  `^\d{4,}[A-Z]?$` (e.g. `1446752D`) are filtered out before
  deterministic selection. These internal identifiers leaked through
  when OpenFIGI couldn't find a real listed equity and broke every
  downstream Yahoo / EDGAR call.
* **`enriched_awards` string-column type drift** — replaced pandas
  `conn.register()` + `persist_table` with an explicit
  `CREATE TABLE AS SELECT CAST(col AS VARCHAR) …` projection. Previously
  when every row had NULL for a string-domain column (e.g.
  `market_cap_quality` with a pre-M1 cache, or `transaction_type` with
  no classified rows), DuckDB's type inference landed the column as
  `INTEGER`. Downstream SQL (`IN (...)`, `ILIKE`, quality-flag
  composition) would then fail.

### Added

* **`backend/scripts/utils/invalidate_stale_market_cap.py`** — selective
  invalidation of pre-M1 `cache_market_cap` rows (those with
  `close_price IS NULL`). Necessary to surface the M1 look-ahead-bias
  fix after cache hits silently short-circuit the new provider code.
* **`--refresh_stale_market_cap` flag** on `run_pipeline.sh` — runs the
  above utility before Phase 2. Default off.
* **`backend/scripts/utils/invalidate_stale_resolutions.py`** — one-shot
  cleanup for stuck Yahoo failures with `/` in ticker and OpenFIGI
  Bloomberg-ID matches made obsolete by P1-4 / P1-5.
* **New unit tests**: `test_transaction_type_small_modification_is_funding_increase`,
  `test_ceiling_change_uses_full_piid_history`, `test_openfigi_rejects_bloomberg_id`,
  `test_ticker_format_normalized_for_yahoo`.
* **Integration test now asserts** `enriched_awards` stores
  `market_cap_quality`, `transaction_type`, `ticker`, `sector`, and
  `industry` as VARCHAR even when every row is NULL.

### Operational notes

* After upgrading, run
  `./run_pipeline.sh --source_dataset … --refresh_stale_market_cap` to
  drop pre-M1 look-ahead-biased market caps. The one-shot
  `invalidate_stale_resolutions.py` has already been executed against
  the local cache (14 Yahoo failures + 4 Bloomberg-ID OpenFIGI rows
  removed); next pipeline run will resolve them fresh.

---

## [1.0.0] — Milestone 1: Correctness Foundation

### Breaking changes
* **Market cap is now point-in-time.** `yahoo.py` was rewritten to use
  `yf.Ticker.get_shares_full(start, end)` to obtain the historical shares
  outstanding near `action_date`, instead of multiplying a historical
  close by the *current* share count. Eliminates the look-ahead bias that
  affected every historical `market_cap` value.
* `alpha_ratio` for $0-obligation modifications now falls back to
  `ceiling_change / market_cap` (previously 0.0). Same fallback semantics
  as `obligation_ratio`.
* `normalize_naics` now replaces the raw `naics_description` in place with
  the cleaned lookup value. The duplicate `naics_description_1` column has
  been removed.
* `join_entity_hierarchy` no longer leaks cache bookkeeping columns
  (`result_status`, `last_verified`) into downstream tables.

### Added
* **Spec-formula signal family** alongside existing signals (let the M2
  validation harness pick winners):
  * `duration_days` -- `end - action_date` in days, with a 30-day floor.
  * `acv_signal` -- `(federal_action_obligation / duration_days) * 365.25`.
  * `acv_alpha_ratio` -- `acv_signal / market_cap`.
  * `difference_between_obligated_and_potential` --
    `potential_total_value_of_award - total_dollars_obligated`.
* **Provenance columns on enriched/themed/signals output**:
  * `close_price` -- historical close on or prior to `action_date`.
  * `shares_outstanding` -- historical (or fallback current) share count.
  * `market_cap_quality` -- one of `ok` / `stale_shares` / `no_close` /
    `no_shares`. Lets researchers filter by data quality.
* **Row-level metadata**:
  * `is_primary_action` -- `True` for NEW_AWARDS / NEW_DELIVERY_ORDERS /
    FUNDING_INCREASE. Default filter for the aggregation layer (Phase 6).
  * `signal_quality` -- semicolon-delimited tags. Possible values:
    `ok` | `microcap` (market_cap < $50M) | `stale_shares` | `no_close` |
    `no_shares` | `missing_market_cap` | `extreme_ratio` (|alpha_ratio|
    exceeds 99th-percentile within sector).
  * `pipeline_version` -- semver-ish string matching this file.
  * `ingested_at` -- `CURRENT_TIMESTAMP` at the moment of Phase 4 projection.

### Fixed
* OpenFIGI "no match" results are now cached (previously re-queried on
  every run).
* `@with_retry` now accepts a configurable `retry_exceptions` tuple so
  yfinance-internal exceptions (JSONDecodeError, KeyError) trigger retry.
* Phase 1 ingestion now pins explicit dtypes for every column the pipeline
  filters on, casts, or uses in arithmetic.
* NAICS keywords lookup now LPAD-normalises the lookup code column so
  joins succeed across padding variations.
* Ingestion profile SQL is now parameterised, not string-interpolated.
* CAGE credential validation runs at startup, not silently.

### Internal
* Added `SignalsAward` dataclass fields for every new column.
* Integration test asserts final CSV columns exactly equal
  `SignalsAward` fields (regression guard for schema drift).
* New unit test `test_join_entity_hierarchy_excludes_cache_metadata`.
