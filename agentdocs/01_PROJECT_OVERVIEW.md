# Project Overview: USASpending Quantitative Analysis Pipeline

## 1. Project Objective
Build a high-performance Python data pipeline to identify alpha signals in publicly traded companies based on federal contract awards from USASpending.gov. The system will process large CSV datasets (~2GB) using DuckDB, resolve government award recipients to publicly traded parent companies, enrich the data with financial metrics, and compute quantitative signals based on "New Money" inflows relative to market capitalization.

## 2. Technical Stack & Architecture
- **Language:** Python
- **Environment:** `uv`
- **Data Modeling:** STRICTLY built-in Python `@dataclass`. No Pydantic.
- **Processing:** DuckDB (primary engine for CSV ingestion, transformations, and caching). Minimal Pandas/Polars only for small outputs if strictly necessary.

### 2.1 Dataclass usage (CRITICAL)
- Dataclasses are for: type hints, documentation, unit tests with tiny mock data, and structuring small API payloads/responses.
- **Do not** instantiate dataclasses for the full 2GB dataset (no "row objects" in Python lists). All large-scale data must remain in DuckDB as `DuckDBPyRelation`/SQL.

### 2.2 Repository directory structure (authoritative)
All logic must map to this exact repository structure. The `backend/data/cache/` directory should be created if it does not already exist.

```text
.
├── backend
│   ├── app
│   │   ├── api
│   │   │   └── endpoints
│   │   ├── core
│   │   ├── services
│   │   └── utils
│   ├── data
│   │   ├── analysis
│   │   ├── cache         # (Create this) DuckDB cache DBs (e.g., cache.duckdb)
│   │   ├── cleaned
│   │   ├── config
│   │   ├── logs
│   │   ├── out
│   │   ├── raw
│   │   │   ├── contracts
│   │   │   ├── lookups       # External CSV lookups (NAICS/PSC)
│   │   │   └── samples
│   │   └── results
│   ├── docs
│   ├── models
│   ├── prompts
│   ├── scripts
│   ├── src
│   └── tests
│       ├── integration
│       └── unit
├── deployment
└── frontend
```

## 3. Data Schemas

### 3.1 Base ingestion schema (target fields)
- `contract_transaction_unique_key` (TEXT) - Unique ID *(Required)*
- `award_id_piid` (TEXT) - Parent contract ID *(Required)*
- `federal_action_obligation` (NUMERIC) - New money obligated *(Required)*
- `total_dollars_obligated` (NUMERIC) - Total obligated to date *(Required)*
- `current_total_value_of_award` (NUMERIC)
- `potential_total_value_of_award` (NUMERIC)
- `action_date` (DATE) *(Required)*
- `solicitation_date` (DATE)
- `period_of_performance_start_date` (DATE)
- `period_of_performance_current_end_date` (DATE)
- `awarding_agency_name` (TEXT)
- `awarding_sub_agency_name` (TEXT)
- `recipient_parent_uei` (TEXT) - Primary key for entity *(Required)*
- `recipient_parent_name_raw` (TEXT) *(Required)*
- `product_or_service_code` (TEXT) *(Required)*
- `product_or_service_code_description` (TEXT)
- `naics_code` (TEXT) *(Required)*
- `naics_description` (TEXT)
- `number_of_offers_received` (NUMERIC)
- `transaction_description` (TEXT) *(Required)*
- `award_type` (TEXT)

### 3.2 Enriched, Thematic, and Calculated Schema (Final Output)

The following columns are added through successive phases to the base ingestion schema (Section 3.1), culminating in the `signals_awards` table:

**Phase 2 – Entity and Market Enrichment:**
- `parent_company_name` (TEXT) – Highest level parent (from GLEIF)
- `is_public` (BOOLEAN) – Whether the parent is publicly traded (from OpenFIGI)
- `ticker` (TEXT) – Public ticker (Exchange:Ticker) (from OpenFIGI)
- `market_cap` (DOUBLE) – Market capitalization at `action_date` (from Yahoo Finance)
- `sector` (TEXT) – Sector classification (from Yahoo Finance)
- `last_verified_date` (DATE) – Timestamp of enrichment (system)

**Phase 3 – Theme and Deliverable Classification (from lookup tables):**
- `naics_title` (TEXT) – Short industry title from NAICS lookup
- `naics_description` (TEXT) – Full NAICS description
- `psc_name` (TEXT) – Name of product/service from PSC lookup
- `psc_includes` (TEXT) – Description of items included from PSC lookup
- `psc_category` (TEXT) – Broad category (e.g., "Product", "Service")
- `psc_level_1_category` (TEXT) – Highest-level PSC category (e.g., "Research and Development")
- `deliverable` (TEXT) – Derived from `psc_level_1_category` with fallback to `psc_category`

**Phase 4 – Alpha Signals (calculated):**
- `alpha_ratio` (DOUBLE) – `federal_action_obligation / NULLIF(market_cap, 0)`
- `difference_between_obligated_and_potential` (NUMERIC) – `potential_total_value_of_award - total_dollars_obligated`
- `duration_days` (INTEGER) – `period_of_performance_current_end_date - action_date` (with 30‑day floor)
- `acv_signal` (DOUBLE) – Annualized contract value: `(federal_action_obligation / GREATEST(30, duration_days)) * 365.25`
- `acv_alpha_ratio` (DOUBLE) – `acv_signal / NULLIF(market_cap, 0)`


## 4. DuckDB persistence rules (Strict)

### 4.1 Cleaned DB (phase outputs)
- Phase outputs should be persisted to a DuckDB database file under:
  - `backend/data/cleaned/cleaned.duckdb`

Recommended tables:
- `raw_filtered_awards`
- `ingestion_profile`
- `enriched_awards` (after Phase 2)
- `themed_awards` (after Phase 3)
- `signals_awards` (after Phase 4)

### 4.2 Cache DB (API results)
- All caching persistence MUST be done in DuckDB:
  - `backend/data/cache/cache.duckdb`
- Do not use SQLite or JSON caches.

#### 4.2.1 Cache schemas (agent MUST implement exactly)
Create these tables (or equivalent) in `backend/data/cache/cache.duckdb`:

- `cache_gleif_uei_lei`
  `(uei TEXT PRIMARY KEY, lei TEXT, fetched_at TIMESTAMP, source_payload_hash TEXT, status TEXT)`

- `cache_gleif_lei_upe`
  `(lei TEXT PRIMARY KEY, upe_lei TEXT, fetched_at TIMESTAMP, source_payload_hash TEXT, status TEXT)`

- `cache_openfigi_lei_ticker`
  `(lei TEXT PRIMARY KEY, ticker TEXT, exchange TEXT, security_type TEXT, fetched_at TIMESTAMP, source_payload_hash TEXT, status TEXT)`

- `cache_market_cap`
  `(ticker TEXT, date DATE, market_cap DOUBLE, sector TEXT, fetched_at TIMESTAMP, source_payload_hash TEXT, status TEXT, PRIMARY KEY (ticker, date))`

- `cache_failures`
  `(provider TEXT, key TEXT, error_type TEXT, http_status INTEGER, message TEXT, retry_after_seconds INTEGER, attempts INTEGER, last_attempt_at TIMESTAMP, PRIMARY KEY (provider, key))`

## 5. API resilience & rate limiting (Strict)
- Implement a single retry/backoff policy shared across providers.
- Retry only on retryable conditions (HTTP 429, 5xx, timeouts) with exponential backoff + jitter; honor `Retry-After` if present.
- Hard-cap attempts (e.g., 5) and record failures in `cache_failures`.

Provider notes:
- **GLEIF:** conservative rate limiting (e.g., 60 req/min), 429 => pause and resume.
- **OpenFIGI:** conservative rate limiting, prefer batching/bulk endpoints, 429/5xx => backoff and resume.
- **Yahoo Finance / yfinance:** treat as throttled/unstable; batch where possible; cache aggressively; backoff on "Too Many Requests".

## 6. Logging and Monitoring
- All scripts must use Python's built-in `logging` module (configured at INFO level in production, DEBUG for development).
- Logs should be written to both console and a rotating file in `backend/data/logs/` (e.g., `pipeline.log`).
- Key events to log:
  - Start and end of each phase (with timestamps).
  - Count of input/output records at major steps.
  - Number of distinct keys processed (e.g., unique UEIs in Phase 2).
  - API call summaries (success/failure counts, cache hits/misses) – avoid logging per‑row details.
  - Warnings or errors (e.g., missing lookup files, unexpected API responses).
- Sensitive information (API keys, tokens) must never be logged.
- Use structured logging (JSON format) if downstream analysis of logs is anticipated.
