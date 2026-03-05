# Phase 3: Theme Intelligence

**Goal:** Normalize industry codes (NAICS/PSC) using external lookup tables to append detailed metadata as standalone columns (NAICS descriptions and PSC details), and derive an exact item classification (`deliverable`) from PSC.

## Architecture Boundaries
- Data transformations must be pure DuckDB relational operations in `backend/src/transform.py`.
- No database connections or file reading inside the transform functions. All IO happens in `backend/src/io.py` and the orchestrator script.
- Must utilize external lookup files for NAICS and PSC codes to append plain-English descriptions and categories directly as individual columns.
- **Model Updates Required:** You must update the data models (dataclasses in `backend/models/`) to support these new standalone NAICS and PSC columns.

## Required Lookup Files & Locations
The pipeline expects the following lookup files to be available in the raw data directory:
1. **NAICS Lookup:** `backend/data/raw/lookups/2022-NAICS-Description-Table.csv`
2. **PSC Lookup:** `backend/data/raw/lookups/Simplified_PSC_Lookup.csv`

### Lookup Table Schemas Expected
Your pure functions should expect the incoming DuckDB relations for the lookup tables to have the following strict schemas. 

**`2022-NAICS-Description-Table.csv` (All columns strictly required):**
- `naics_code` (TEXT) – 6-digit NAICS code, zero-padded.
- `naics_title` (TEXT) – Short title of the industry.
- `naics_description` (TEXT) – Full description.

**`Simplified_PSC_Lookup.csv` (All columns strictly required):**
- `psc_code` (TEXT) – PSC code (e.g., "R499").
- `psc_name` (TEXT) – Name of the product/service.
- `psc_includes` (TEXT) – Description of items included.
- `psc_category` (TEXT) – Broad category (e.g., "Product", "Service").
- `psc_level_1_category` (TEXT) – Highest-level category (e.g., "Research and Development", "Equipment").

## Execution Steps (TDD + Pure Functions)

1) Draft Pure Functions (`backend/src/transform.py`)
- Write pure functions that take a `duckdb.DuckDBPyRelation` (representing the enriched data from Phase 2) and `duckdb.DuckDBPyRelation` objects for the lookup tables, returning a modified relation:
  - **`normalize_naics()`**: Ensure `naics_code` is treated as a zero-padded string, strip whitespace, handle NULLs. Join with the NAICS lookup table to append `naics_title` and `naics_description` as their own distinct columns.
  - **`normalize_psc()`**: Ensure `product_or_service_code` is formatted cleanly. Join with the Simplified PSC lookup table on `psc_code` to append `psc_name`, `psc_includes`, `psc_category`, and `psc_level_1_category` as their own distinct columns.
  - **`derive_deliverable()`**: 
    - **Derive `deliverable` (TEXT):**
      1. Use `psc_level_1_category` from the PSC lookup if available.
      2. Else, use `psc_category` (Product/Service).
      3. Fallback to `NULL` if both are unavailable.

2) Write Unit Tests (`backend/tests/unit/test_themes.py`)
- Use `duckdb.connect(':memory:')`.
- Create a mock relation with messy data: NAICS codes missing leading zeros, whitespace in PSC codes.
- Create mock lookup tables for NAICS and PSC codes following the strict schemas defined above.
- Assert that the pure transform functions clean the data predictably, correctly execute the joins, accurately populate the individual NAICS and PSC columns, resolve the `deliverable` fallback logic correctly to `NULL` when missing, and handle all other NULLs gracefully.


3) Orchestrate (`backend/scripts/themes.py`)
- **Pre-flight Check:** Verify that both the NAICS and PSC lookup CSV files exist in `backend/data/raw/lookups/` before attempting to read them. Raise a clear `FileNotFoundError` with a descriptive message if either is missing.
- **IO Read:** Use `backend/src/io.py` to connect to `backend/data/cleaned/cleaned.duckdb` and load the `enriched_awards` table created in Phase 2. Also load the NAICS and PSC lookup CSVs as DuckDB relations.
- **Transform:** Apply the pure NAICS/PSC normalization, joins, and deliverable derivation functions to the relation.
- **IO Write:** Persist the output as a new table named `themed_awards` back into `backend/data/cleaned/cleaned.duckdb`.


## Target Output Schema (`themed_awards`)
The final output table must include all original enriched columns plus the newly appended and derived classification fields as individual columns:

| Column Name | Data Type | Source / Description |
| :--- | :--- | :--- |
| *(all `enriched_awards` columns)* | *various* | e.g., `ticker`, `market_cap`, `sector`, etc. |
| `naics_title` | TEXT | From NAICS lookup |
| `naics_description` | TEXT | From NAICS lookup |
| `psc_name` | TEXT | From PSC lookup |
| `psc_includes` | TEXT | From PSC lookup |
| `psc_category` | TEXT | From PSC lookup |
| `psc_level_1_category` | TEXT | From PSC lookup |
| `deliverable` | TEXT | Derived from `psc_level_1_category` with fallback to `psc_category`, else `NULL` |
