# USASpending Quantitative Analysis Pipeline

This project builds a high-performance data pipeline and quantitative analysis tool to identify potential alpha signals in publicly traded companies based on USASpending.gov contract awards.

At a high level, it:
- Ingests very large USASpending contract CSV drops (multi-GB) efficiently.
- Filters for meaningful “New Money” activity.
- Resolves award recipients to publicly traded tickers via entity resolution.
- Enriches awards with market context (e.g., market cap at award date).
- Generates quantitative signals to support research and downstream analysis.

## Running the Pipeline

The pipeline is organized into four sequential phases. Run each phase from the root of the repository. Make sure to set `PYTHONPATH=.` so python can locate the `backend` module.

### 1. Phase 1: Ingestion
Efficiently reads, filters, and processes large raw CSV data drops, creating a cleaned base dataset.
```bash
export PYTHONPATH=.
uv run backend/scripts/ingest.py --csv backend/data/raw/contracts/FY2024_All_Contracts_Full_20260207_1.csv
```

### 2. Phase 2: Enrichment
Pulls unique entity identifiers (UEIs) from the ingestion phase, resolving them to parent companies (CAGE Scraper), mapping to publicly traded tickers (OpenFIGI), and pulling historical market caps (Yahoo Finance).

**How Rate Limits and Caching Works (e.g., OpenFIGI)**
To respect API rate limits and improve performance, the enrichment phase heavily utilizes a persistent local DuckDB cache (`backend/data/cache/cache.duckdb`).
- **Cache-First Check**: Before making any external network request, the pipeline checks if a successful response for that specific input (like an OpenFIGI owner name) already exists in the local cache. If found, it uses the cached data and skips the API call entirely.
- **Failure & Backoff Cache**: If an API call previously failed (e.g., a 500 error), the failure is also cached along with a short 10-second cooldown window. The pipeline will wait 10 seconds and automatically retry any failures as part of the phase's built-in loop.
- **Resuming with New Data**: When loading a new or subsequent CSV file via `run_pipeline.sh`, the pipeline will automatically reuse the existing cache file. This ensures it instantly resolves any companies it has seen in previous runs without consuming precious API rate limits.

```bash
export PYTHONPATH=.
uv run backend/scripts/enrich.py
```

To verify the enriched data natively via DuckDB:
```bash
uv run python -c "import duckdb; print(duckdb.connect('backend/data/cleaned/cleaned.duckdb').execute('SELECT cage_business_name, cage_update_date, is_highest, highest_level_owner_name, highest_level_cage_code, highest_level_cage_update_date, ticker, sector, industry, market_cap FROM enriched_awards WHERE industry IS NOT NULL LIMIT 10').df().to_string())"
```

### 3. Phase 3: Theme Intelligence
Merges NAICS and PSC external lookup CSVs to append plain-text descriptions and build the standard `deliverable` mappings.
```bash
export PYTHONPATH=.
uv run backend/scripts/themes.py
```

To verify the themed data natively via DuckDB:
```bash
uv run python -c "import duckdb; print(duckdb.connect('backend/data/cleaned/cleaned.duckdb').execute('SELECT * FROM themed_awards LIMIT 10').df().to_string())"
```

### 4. Phase 4: Signal Generation
Computes the final alpha ratio mathematical indicators natively and exports the quantitative dataset directly to a CSV file.
```bash
export PYTHONPATH=.
uv run backend/scripts/signals.py --output backend/data/results/final_signals.csv
```

### Run All Phases Sequentially

Use the `run_pipeline.sh` script at the root of the repository to run all phases in one command. It stops immediately if any phase fails, and derives the output filename automatically from the input CSV name.

```bash
./run_pipeline.sh --source_dataset backend/data/raw/contracts/FY2024_All_Contracts_Full_20260207_1.csv
```

To also clear the enrichment cache before running (full fresh run):

```bash
./run_pipeline.sh --source_dataset backend/data/raw/contracts/FY2024_All_Contracts_Full_20260207_1.csv --clear_cache
```

Results are written to:
```
backend/data/results/<source_filename>_final_signals.csv
```

## Estimating Pipeline Run Time

To check how long it will take to complete running the full pipeline, accounting for API rate limits and existing cache hits, you can run the estimation tool:

```bash
uv run --env-file .env backend/scripts/estimate_pipeline.py --csv backend/data/raw/contracts/FY2024_All_Contracts_Full_20260207_1.csv
```

**Example Output:**
```
========================================
 Pipeline Estimation Tool
========================================
Scanning raw dataset...
CAGE Requests:
  Total Unique:      6140
  Pending:           5212
  Est. Duration:     2.17h

OpenFIGI Requests:
  Pending (derived): ~5883 (worst-case)
  Est. Duration:     4.09h

Yahoo Finance Requests:
  Pending (derived): ~29527 (worst-case)
  Est. Duration:     8.20h
========================================
 Total Est. Duration: 14.46h
========================================
```
