# Phase 3: Theme Intelligence

**Goal:** Normalize industry codes (NAICS/PSC) and combine them with Yahoo Finance sector data to derive actionable, standardized themes.

## Architecture Boundaries
- Data transformations must be pure DuckDB relational operations in `backend/src/transform.py`.
- No database connections or file reading inside the transform functions. All IO happens in `backend/src/io.py` and the orchestrator script.

## Execution Steps (TDD + Pure Functions)

1) Draft Pure Functions (`backend/src/transform.py`)
- Write pure functions that take a `duckdb.DuckDBPyRelation` (representing the enriched data from Phase 2) and return a modified relation with normalized codes:
  - **`normalize_naics()`**: Ensure `naics_code` is treated as a zero-padded string, strip whitespace, handle NULLs.
  - **`normalize_psc()`**: Ensure `product_or_service_code` is formatted cleanly.
  - **`coalesce_sector()`**: Merge or coalesce the `sector` column fetched from Yahoo Finance alongside the NAICS/PSC classifications to create a unified `theme` column.

2) Write Unit Tests (`backend/tests/unit/test_themes.py`)
- Use `duckdb.connect(':memory:')`.
- Create a mock relation with messy data: NAICS codes missing leading zeros, whitespace in PSC codes, missing Yahoo Finance sectors.
- Assert that the pure transform functions clean the data predictably and handle NULLs gracefully without failing.

3) Orchestrate (`backend/scripts/themes.py`)
- **IO Read:** Use `backend/src/io.py` to connect to `backend/data/cleaned/cleaned.duckdb` and load the `enriched_awards` table created in Phase 2.
- **Transform:** Apply the pure NAICS/PSC/Theme normalization functions to the relation.
- **IO Write:** Persist the output as a new table named `themed_awards` back into `backend/data/cleaned/cleaned.duckdb`.
