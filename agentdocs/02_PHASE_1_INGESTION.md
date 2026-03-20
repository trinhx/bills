# Phase 1: Data Ingestion & Modeling

**Goal:** Define `@dataclass` schemas (for typing/tests), lazily load the ~2GB CSV into DuckDB, apply base filters, and persist a filtered base table.

## CRITICAL constraints
- The 2GB dataset must remain in DuckDB (relations / SQL). Do not materialize it into Python objects.
- Dataclasses must **not** be instantiated per-row for the raw dataset. Dataclasses exist for typing, documentation, and unit tests with tiny mock data.

## Execution steps (TDD + pure functions)

1) Define models (`backend/models/contracts.py`)
- Create Python `@dataclass` models reflecting:
  - Base ingestion schema (Phase 1 columns)
  - Enriched/calculated schema (Phase 2 columns)
  - Theme/Classification schema (Phase 3 columns: `naics_title`, `naics_description`, `psc_name`, `psc_includes`, `psc_category`, `psc_level_1_category`, and `deliverable`)
- Models are for: type hints, test fixtures, and small API payloads; not for row-by-row ingestion.

2) Draft pure transform functions (`backend/src/transform.py`)
- Write pure functions that accept a `duckdb.DuckDBPyRelation` and return a `duckdb.DuckDBPyRelation` (or SQL string) with:
  - Column selection: base schema columns plus derived fields:
    - `transaction_type`: Derived from `contract_transaction_unique_key`, `parent_award_id_piid`, `federal_action_obligation`, and `transaction_description` to classify into `NEW_AWARDS`, `NEW_DELIVERY_ORDERS`, `MODIFICATION`, or `FUNDING_INCREASE`.
  - Filters applied during ingestion:
    - `federal_action_obligation >= 0`
    - `total_dollars_obligated >= 5000000`
    - `award_type IN ('DEFINITIVE CONTRACT', 'DELIVERY ORDER', 'PURCHASE ORDER')`
- No side effects: no file IO, no database connections, no logging inside these functions.

3) Write unit tests first (`backend/tests/unit/test_transform_ingestion.py`)
- Use `duckdb.connect(':memory:')`.
- Create a small in-memory table with boundary cases:
  - negative obligation, obligation = 0
  - total_dollars_obligated below/above 5,000,000
  - invalid award_type
- Assert the resulting relation contains only expected rows and expected columns.

4) Wire IO in `backend/src/io.py` (side-effects only)
Implement IO helpers that do not embed business logic:
- `get_cleaned_conn() -> duckdb.DuckDBPyConnection` opening `backend/data/cleaned/cleaned.duckdb`
- `scan_contracts_csv(conn, csv_path) -> duckdb.DuckDBPyRelation` using DuckDB CSV scanning (lazy)
- `persist_table(conn, rel, table_name)` to materialize a relation into a DuckDB table
- `write_profile(conn, profile_rel)` to persist `ingestion_profile`

5) Orchestrate (`backend/scripts/ingest.py`)
- Read from: `backend/data/raw/contracts/<file>.csv`
- Apply the pure Phase 1 transforms
- Persist to cleaned DB:
  - `raw_filtered_awards`
  - `ingestion_profile`
- Log (in the script): input row count, output row count, total obligation sum
- Do not build Phase 2 logic here.
