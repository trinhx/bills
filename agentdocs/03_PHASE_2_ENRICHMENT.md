# Phase 2: Data Processing & Enrichment (Entity + Market Data)

**Goal:** Resolve award recipients to tradable tickers and enrich each qualifying row with market cap/sector on the action date, while respecting API rate limits and using DuckDB-only caching.

## Non-negotiable performance rules
- Do **not** make API calls per row.
- Extract unique UEIs, resolve once, cache, then join back.
- All caches persist in: `backend/data/cache/cache.duckdb` (schemas defined in `01_PROJECT_OVERVIEW.md`).

## Architecture boundaries (match AGENT-INSTRUCTIONS)
- `backend/src/transform.py`: pure functions (extract distinct keys, join cached results, compute derived fields that don't require IO).
- `backend/src/io.py`: DuckDB connections, reading/writing cache tables, reading/writing cleaned tables, exporting results.
- `backend/src/enrichment.py`: **network side-effects only** (HTTP calls, retries, rate limiting); no DuckDB connections and no SQL joins here.

## Execution steps (TDD + staged integration)

1) Pure extraction/join functions (`backend/src/transform.py`)
- `extract_unique_uei(rel) -> DuckDBPyRelation` (distinct UEIs from `raw_filtered_awards`)
- Pure join helpers to merge cached tables onto the main relation:
  - Join UEI -> LEI
  - LEI -> UPE LEI
  - UPE LEI -> ticker/is_public/parent_company_name
  - (ticker, action_date) -> market_cap/sector
- All joins should be left joins; unresolved values become NULL.

2) API clients (network only) (`backend/src/enrichment.py`)
Implement functions that:
- Accept identifiers (UEI / LEI / ticker) and return parsed results plus metadata needed for caching (status, payload hash).
- **Proactive Rate Limiting (CRITICAL):** Implement a proactive rate limiter (e.g., Token Bucket or Leaky Bucket) for each provider to prevent hitting 429s in the first place.
  - GLEIF: Hard limit of 60 requests per minute.
  - OpenFIGI: 250 requests per minute (approx 25 requests per 6 seconds) using the `X-OPENFIGI-APIKEY` loaded from the `.env` file. Batch up to 100 jobs per mapping request.
- **Reactive Retry/Backoff:** If a 429 is still encountered despite the rate limiter:
  - Catch 429, 5xx, and timeouts.
  - Apply exponential backoff + jitter.
  - Honor `Retry-After` headers if provided.
  - Cap attempts (e.g., 5); on terminal failure, return a structured failure record for `cache_failures`.
- Provider responsibilities:
  - GLEIF: UEI -> LEI, LEI -> UPE LEI
  - OpenFIGI: UPE LEI -> ticker. **Deterministic Selection Rule:** If OpenFIGI returns multiple tickers for a single LEI:
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
  - Before calling an API for a key, query the success cache tables. If it exists, use it.
  - If it does not exist, query the `cache_failures` table for that provider and key.
  - **Failure Backoff:** If a recent failure exists (e.g., `last_attempt_at` is within the last hour, or within `retry_after_seconds` if provided by the API), **skip the API call entirely** and treat the key as unresolved (NULL) for this pipeline run. Do not hammer failing endpoints.
  - On network success: upsert cache row.
  - On terminal network failure: upsert failure row.

4) Tests (`backend/tests/unit/test_enrichment_retry_and_cache.py`)
- Unit-test the **proactive rate limiter** (mock time to ensure requests are delayed to stay under X per minute).
- Unit-test **reactive retry policy** with mocked responses (429 with Retry-After, then success; repeated 5xx then terminal failure).
- Unit-test the failure backoff logic (ensure an API call is skipped if `cache_failures` indicates a recent failure).
- Unit-test the OpenFIGI deterministic selection rule.
- Unit-test the Yahoo Finance Market Cap Approximation logic.
- Unit-test DuckDB cache upserts using `duckdb.connect(':memory:')` + table creation identical to cache schema.

5) Orchestrate (`backend/scripts/enrich.py`)
- Input: `backend/data/cleaned/cleaned.duckdb`, table `raw_filtered_awards`
- Steps:
  - Extract distinct UEIs
  - Resolve via cache + failure backoff logic + enrichment clients
  - Join cached enrichments back to produce `enriched_awards` in `backend/data/cleaned/cleaned.duckdb`
- Output should include (at minimum): `parent_company_name`, `is_public`, `ticker`, `market_cap`, `sector`, `last_verified_date`
- Append the `last_verified_date` column to the relation using DuckDB's `CURRENT_TIMESTAMP` immediately before persisting
- Do not compute Phase 4 signal math here (keep it for Phase 4).

