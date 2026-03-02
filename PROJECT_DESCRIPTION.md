# Project Description

## 1. Project Objective
Build a high-performance data pipeline and quantitative analysis tool to identify alpha signals in publicly traded companies based on [USASpending.gov](https://www.usaspending.gov) contract awards. The system must efficiently process ~2GB CSV drops with DuckDB, surface "New Money" inflows, resolve government award recipients to tradable tickers via GLEIF/OpenFIGI, and compute dual-track alpha ratios that compare historical market performance against federal award momentum.

## 2. Core Functional Requirements
- **High-Performance Ingestion:** Use DuckDB for lazy loading and memory-safe processing of 2GB+ CSV files on 16GB RAM hardware.
- **Long-Only Signal Filtering:** Only retain rows with positive `federal_action_obligation` values across award types `DEFINITIVE CONTRACT`, `DELIVERY ORDER`, and `PURCHASE ORDER`.
- **Entity Resolution Bridge:**
  - Map `recipient_parent_uei` to a Global Legal Entity Identifier (LEI) via the GLEIF API.
  - Perform a Level 2 hierarchy hop to surface each Ultimate Parent Entity (UPE).
- **Market Verification:** Call OpenFIGI to confirm the entity is publicly traded, filtering for common stock listings on NYSE and NASDAQ.
- **Local Persistence:** Maintain a DuckDB-backed cache of the UEI → LEI → Ticker mappings to minimize repeated API calls and latency.

## 3. Quantitative Signal Logic
For every qualifying transaction calculate two alpha metrics to contrast headline sentiment vs. fundamentals:

1. **Raw Alpha Ratio (Headline Impact)**  
   `Raw Alpha = federal_action_obligation / market_cap_at_action_date`

2. **ACV Alpha Ratio (Fundamental Impact)**  
   `ACV Alpha = ACV / market_cap_at_action_date`

- ACV is normalized with a 30-day floor derived from `(end_date - action_date)` to prevent underestimation for short-duration contracts.

## 4. Technical Stack
- **Databases:** DuckDB (primary processing, caching) plus SQLite for lightweight local entity storage when needed.
- **Languages / Tooling:** Python (DuckDB API, limited Pandas/Polars for light transformations), SQL, UV for environment and dependency management.
- **External APIs:**
  - **GLEIF:** Entity resolution from UEI to LEI with Level 2 hierarchy support.
  - **OpenFIGI:** Ticker verification, restricted to NYSE/NASDAQ common stock instruments.
  - **Yahoo Finance (or equivalent):** Historical market cap and sector metadata for alpha calculations.
