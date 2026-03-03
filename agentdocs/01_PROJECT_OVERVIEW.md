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

### 3.2 Enriched & calculated schema
- `parent_company_name` (GLEIF) - Highest level parent
- `is_public` (OpenFIGI) - Filter: Common Stock
- `ticker` (OpenFIGI) - Public ticker (Exchange:Ticker)
- `market_cap` (Yahoo Finance API) - Market cap at `action_date`
- `sector` (Yahoo Finance API)
- `alpha_ratio` (Calculated)
- `difference_between_obligated_and_potential` (Calculated)
- `duration_days` (Calculated)
- `acv_signal` (Calculated)
- `acv_alpha_ratio` (Calculated)
- `last_verified_date` (System)

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
