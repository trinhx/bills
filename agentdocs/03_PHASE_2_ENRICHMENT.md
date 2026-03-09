# Phase 2: Data Processing & Enrichment (Entity + Market Data)

**Goal:** Resolve award recipients to their highest level owner via the DLA CAGE website, map owners to tradable tickers, and enrich each qualifying row with market cap/sector on the action date, while respecting API rate limits and using DuckDB-only caching.

## Non-negotiable performance rules
- Do **not** make network/API calls per row.
- Extract unique UEIs, resolve once via CAGE/APIs, cache, then join back.
- All caches persist in: `backend/data/cache/cache.duckdb` (schemas defined in `01_PROJECT_OVERVIEW.md`).

## Architecture boundaries (match AGENT-INSTRUCTIONS)
- `backend/src/transform.py`: pure functions (extract distinct keys, join cached results, compute derived fields that don't require IO).
- `backend/src/io.py`: DuckDB connections, reading/writing cache tables, reading/writing cleaned tables, exporting results.
- `backend/app/services/providers/`: **network side-effects only** (CAGE scraper HTTP calls, API retries, rate limiting); no DuckDB connections and no SQL joins here.

## Execution steps (TDD + staged integration)

1) Pure extraction/join functions (`backend/src/transform.py`)
- `extract_unique_uei(rel) -> DuckDBPyRelation` (distinct UEIs from `raw_filtered_awards`)
- Pure join helpers to merge cached tables onto the main relation:
  - Join UEI -> DLA CAGE entity hierarchy results (highest_level_owner_name, highest_level_cage_code, etc.)
  - Join highest_level_owner_name -> ticker/is_public (from OpenFIGI)
  - Join (ticker, action_date) -> market_cap/sector (from Yahoo Finance)
- All joins should be left joins; unresolved values become NULL.

2) API clients & Scrapers (network only) (`backend/app/services/providers/`)
Implement functions that:
- Accept identifiers (UEI / Owner Name / ticker) and return parsed results plus metadata needed for caching (status, payload hash).
- **Proactive Rate Limiting (CRITICAL):** Implement a proactive rate limiter for each provider/scraper to prevent blocking or 429s.
  - DLA CAGE Scraper: Establish safe parsing intervals (e.g., max 30-60 requests per minute) and respect website session parameters. A periodic cookie refresh may be required.
  - OpenFIGI: 250 requests per minute (approx 25 requests per 6 seconds) using the `X-OPENFIGI-APIKEY` loaded from the `.env` file. Batch up to 100 jobs per mapping request.
- **Reactive Retry/Backoff:** If a 429 is still encountered despite the rate limiter:
  - Catch 429, 5xx, timeouts, and scraper connection errors.
  - Apply exponential backoff + jitter.
  - Honor `Retry-After` headers if provided.
  - Cap attempts (e.g., 5); on terminal failure, return a structured failure record for `cache_failures`.
- Provider responsibilities:
  - DLA CAGE Scraper (`cage_scraper.py`): Input UEI -> Search -> Parse Details URI -> Fetch single Details page. Extract base entity and Highest Level Owner info directly from this page (no recursive traversal). If the "Highest Level Owner" block shows `<div>Information not Available</div>`, map the base entity fields to the highest-level owner fields.
  - OpenFIGI: `highest_level_owner_name` -> ticker. **Deterministic Selection Rule:** If OpenFIGI returns multiple tickers for a single Owner Name:
    1. Filter to `securityType = 'Common Stock'`.
    2. Prioritize US exchanges (NYSE, NASDAQ).
    3. If multiple US tickers still exist, select the primary ticker that appears first alphabetically to ensure deterministic caching.
  - Yahoo Finance (`yfinance`): ticker + action_date -> market_cap + sector 
    - **Market Cap Approximation Rule:** Yahoo Finance does not provide historical market cap directly. To calculate this:
      1. Fetch the historical `Close` price for the `action_date` (or the closest available prior trading day).
      2. Fetch the most recent `sharesOutstanding` from the ticker's `info` dictionary.
      3. Calculate `market_cap = historical_close * sharesOutstanding`. (Accept that shares outstanding may be slightly stale).
      4. Fetch `sector` from the `info` dictionary.

3) DuckDB cache IO (`backend/src/io.py`)
- Open cache DB: `backend/data/cache/cache.duckdb`
- Implement:
  - `ensure_cache_tables(conn)` creates the cache tables exactly as defined in `01_PROJECT_OVERVIEW.md`
  - `get_cached_*` functions for each cache table (lookup by primary key)
  - `upsert_cached_*` functions to persist successes
  - `upsert_failure(provider, key, ...)` for `cache_failures`
- **Cache-first behavior & Failure Backoff Logic:**
  - Before calling an API/scraper for a key, query the success cache tables. If it exists, use it.
  - If it does not exist, query the `cache_failures` table for that provider and key.
  - **Failure Backoff:** If a recent failure exists (e.g., `last_attempt_at` is within the last hour, or within `retry_after_seconds`), **skip the call entirely** and treat the key as unresolved (NULL) for this pipeline run. Do not hammer failing endpoints.
  - On network success: upsert cache row.
  - On terminal network failure: upsert failure row.

4) Tests (`backend/tests/unit/test_enrichment_retry_and_cache.py`)
- Unit-test the **proactive rate limiter** (mock time to ensure requests are delayed).
- Unit-test **reactive retry policy** with mocked responses (429 with Retry-After, then success; repeated 5xx then terminal failure).
- Unit-test the failure backoff logic (ensure an API call is skipped if `cache_failures` indicates a recent failure).
- Unit-test the DLA CAGE pure HTML parsing functions (ensuring single-pass extraction works correctly).
- Unit-test the OpenFIGI deterministic selection rule.
- Unit-test the Yahoo Finance Market Cap Approximation logic.
- Unit-test DuckDB cache upserts using `duckdb.connect(':memory:')` + table creation identical to cache schema.

5) Orchestrate (`backend/scripts/enrich.py`)
- Input: `backend/data/cleaned/cleaned.duckdb`, table `raw_filtered_awards`
- Steps:
  - Extract distinct UEIs
  - Resolve hierarchy via single-pass CAGE scraper + cache + failure backoff logic
  - Resolve tickers and market caps via enrichment clients
  - Join cached enrichments back to produce `enriched_awards` in `backend/data/cleaned/cleaned.duckdb`
- Output should include (at minimum): `highest_level_owner_name`, `is_public`, `ticker`, `market_cap`, `sector`, `last_verified_date`
- Append the `last_verified_date` column to the relation using DuckDB's `CURRENT_TIMESTAMP` immediately before persisting
- Do not compute Phase 4 signal math here (keep it for Phase 4).
- Output should include all Phase 2 columns defined in the overview: theme_llm, cage_business_name, cage_update_date, is_highest, highest_level_owner_name, highest_level_cage_code, highest_level_cage_update_date, is_public, ticker, market_cap, sector, last_verified_date
- If the CAGE scraper returns no results (e.g., UEI not found), upsert a row in cache_entity_hierarchy with result_status = 'not_found' and set all other fields to NULL. The cache‑first check must respect this status to avoid re‑scraping.

