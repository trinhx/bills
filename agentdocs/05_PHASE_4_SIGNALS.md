# Phase 4: Signal Generation

**Goal:** Compute the dual-track alpha metrics natively using DuckDB SQL and export the final actionable dataset for quantitative analysis.

## Architecture Boundaries
- Math and logic must be executed via DuckDB relational math (SQL). **Do not use Pandas for calculations.**
- DuckDB natively handles NULL propagation. Ensure division by zero edge cases are handled safely.

## Execution Steps (TDD + Pure Functions)

1) Draft Pure Functions for Math (`backend/src/transform.py`)
- Write a pure function that accepts the `themed_awards` relation and applies the following calculated fields:
  - `difference_between_obligated_and_potential` = `potential_total_value_of_award - total_dollars_obligated`
  - `duration_days` = `period_of_performance_current_end_date - action_date`
  - `acv_signal` = `(federal_action_obligation / GREATEST(30, duration_days)) * 365.25`
  - `alpha_ratio` = `federal_action_obligation / NULLIF(market_cap, 0)`
  - `acv_alpha_ratio` = `acv_signal / NULLIF(market_cap, 0)`
- **Edge Cases:** You must use `NULLIF(market_cap, 0)` to prevent Division by Zero errors. If `market_cap` is NULL or 0, the alpha ratios should return NULL. `GREATEST()` must be used to enforce the 30-day floor on `duration_days`.

2) Write Unit Tests (`backend/tests/unit/test_signals.py`)
- Use `duckdb.connect(':memory:')`.
- Create a mock relation testing every edge case:
  - `duration_days < 30` (assert the floor is applied).
  - `market_cap = 0` (assert no crash, returns NULL).
  - `market_cap = NULL` (assert no crash, returns NULL).
  - Standard positive integers to verify mathematical accuracy.

3) Wire IO & Orchestrate (`backend/scripts/signals.py`)
- **IO Read:** Load the `themed_awards` table from `backend/data/cleaned/cleaned.duckdb`.
- **Transform:** Pipe the relation through the Phase 4 signal functions.
- **IO Write 1:** Persist the final relational table as `signals_awards` in `backend/data/cleaned/cleaned.duckdb`.
- **IO Write 2:** Use DuckDB's `COPY ... TO` command in `backend/src/io.py` to write the final output to a CSV or Parquet file in `backend/data/results/final_signals.csv`.
