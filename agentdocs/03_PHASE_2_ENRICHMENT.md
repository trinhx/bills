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
- Apply retry/backoff:
  - Retryable: 429, 5xx, timeouts
  - Exponential backoff + jitter
  - Honor `Retry-After`
  - Cap attempts (e.g., 5); on terminal failure, return a structured failure record for `cache_failures`
- Provider responsibilities:
  - GLEIF: UEI -> LEI, LEI -> UPE LEI
  - OpenFIGI: UPE LEI -> ticker (filter to common stock; capture exchange/security_type)
  - Yahoo Finance: ticker + action_date -> market_cap + sector

3) DuckDB cache IO (`backend/src/io.py`)
- Open cache DB: `backend/data/cache/cache.duckdb`
- Implement:
  - `ensure_cache_tables(conn)` creates the cache tables exactly as defined in `01_PROJECT_OVERVIEW.md`
  - `get_cached_*` functions for each cache table (lookup by primary key)
  - `upsert_cached_*` functions to persist successes
  - `upsert_failure(provider, key, ...)` for `cache_failures`
- Cache-first behavior:
  - Check cache before any API call
  - On success: upsert cache row
  - On failure: upsert failure row (so subsequent runs can short-circuit or apply cooldown policy)

4) Tests (`backend/tests/unit/test_enrichment_retry_and_cache.py`)
- Unit-test retry policy with mocked responses:
  - 429 with Retry-After, then success
  - repeated 5xx then terminal failure
- Unit-test DuckDB cache upserts using `duckdb.connect(':memory:')` + table creation identical to cache schema.

5) Orchestrate (`backend/scripts/enrich.py`)
- Input: `backend/data/cleaned/cleaned.duckdb`, table `raw_filtered_awards`
- Steps:
  - Extract distinct UEIs
  - Resolve via cache + enrichment clients
  - Join cached enrichments back to produce `enriched_awards` in `backend/data/cleaned/cleaned.duckdb`
- Output should include (at minimum): `parent_company_name`, `is_public`, `ticker`, `market_cap`, `sector`, `last_verified_date`
- Do not compute Phase 4 signal math here (keep it for Phase 4).
