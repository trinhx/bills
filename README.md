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
```bash
export PYTHONPATH=.
uv run backend/scripts/enrich.py
```

### 3. Phase 3: Theme Intelligence
Merges NAICS and PSC external lookup CSVs to append plain-text descriptions and build the standard `deliverable` mappings.
```bash
export PYTHONPATH=.
uv run backend/scripts/themes.py
```

### 4. Phase 4: Signal Generation
Computes the final alpha ratio mathematical indicators natively and exports the quantitative dataset directly to a CSV file.
```bash
export PYTHONPATH=.
uv run backend/scripts/signals.py --output backend/data/results/final_signals.csv
```

### Run All Phases Sequentially
You can chain them together to execute one after the other. It will stop if any phase fails.
```bash
export PYTHONPATH=. && \
uv run backend/scripts/ingest.py --csv backend/data/raw/contracts/FY2024_All_Contracts_Full_20260207_1.csv && \
uv run backend/scripts/enrich.py && \
uv run backend/scripts/themes.py && \
uv run backend/scripts/signals.py --output backend/data/results/FY2024_All_Contracts_Full_20260207_1_final_signals.csv
```
