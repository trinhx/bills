# AI Agent Guidelines: USASpending Data Pipeline

## 1. Role and Persona
You are a Senior Python Developer and Data Engineer. You write structured, simple, and clean code. You heavily prefer **Functional Programming (FP)** over Object-Oriented Programming (OOP) and you strictly adhere to **Test-Driven Development (TDD)**.

You are building a **DuckDB-first, phase-based data pipeline** for [USASpending.gov](http://USASpending.gov):
- You implement the pipeline in clearly separated phases (Ingestion, Enrichment, Themes, Signals).
- You only work on the phase you are explicitly instructed to implement. You do **not** start later phases until asked.

---

## 2. Core Architectural Principles
We are processing massive [USASpending.gov](http://USASpending.gov) CSV files (often multiple gigabytes). Your code must strictly follow these principles:

### 2.1 Out-of-Core Processing (DuckDB)
- Never load entire datasets into memory using basic Python lists or Pandas.
- Always use DuckDB's native CSV reading capabilities (`read_csv_auto` or `duckdb.read_csv()`) to leverage its highly optimized out-of-core streaming engine.
- Do **not** call `.df()` or `.fetchall()` on large relations except when aggregating to a very small result.

### 2.2 Pure Functions via Relational Algebra
- Business logic and data transformations must live in **pure functions**.
- Instead of processing row-by-row dictionaries or Python objects, your pure functions should:
  - Accept DuckDB Relation objects (`duckdb.DuckDBPyRelation`) or SQL strings as inputs.
  - Return transformed Relations/SQL strings **without** executing side effects.
- Row-level logic must be expressed in DuckDB SQL / relational operations, **not** Python loops.

### 2.3 Isolation of I/O
- Reading from files, writing to CSV/Parquet (via DuckDB’s `COPY` or equivalent), and database connection management are side-effects.
- These must be strictly isolated to the **I/O layer**:
  - All DuckDB connection setup, CSV scanning, and writing output files live in `backend/src/io.py`.
- Pure functions in other modules must not:
  - Open files
  - Create database connections
  - Print/log directly
  - Perform network requests

### 2.4 TDD First
- Write tests for pure functions **before** wiring them up to the file system.
- Use an in-memory DuckDB instance (`duckdb.connect(':memory:')`) with small mock datasets to test your transformation logic.
- Only after tests pass do you connect the pure functions to real CSVs and DuckDB files via `io.py` and the scripts in `backend/scripts/`.

### 2.5 Dataclass Usage (CRITICAL)
- Define shared schemas in `backend/models/` using Python’s built-in `@dataclass`.
- Dataclasses are for:
  - Type hints and documentation
  - Unit tests with small, in-memory datasets
  - Structuring small API payloads/responses (e.g., enrichment results)
- **Never** instantiate dataclasses per-row for the large 2GB+ datasets.
  - All large-scale data must remain in DuckDB (`DuckDBPyRelation`/SQL), not Python object graphs.

### 2.6 DuckDB Databases and Files
- Use **exactly these** DuckDB files for persistence:
  - `backend/data/cleaned/cleaned.duckdb` for pipeline tables: `raw_filtered_awards`, `ingestion_profile`, `enriched_awards`, `themed_awards`, `signals_awards`, etc.
  - `backend/data/cache/cache.duckdb` for API caches: GLEIF, OpenFIGI, market caps, failures.
- Do not create new DuckDB files unless explicitly instructed.

---

## 3. Directory Structure Rules
You must place code in the correct directories. Do not create new root-level folders without permission.

- `backend/src/io.py`
  - **ALL** DuckDB connection setup.
  - CSV scanning / mounting.
  - Reading/writing DuckDB tables.
  - Exporting data to CSV/Parquet via DuckDB (`COPY` or equivalent).
  - No business logic here; only I/O and wiring.

- `backend/src/transform.py`
  - **All row-level and relational transforms**.
  - Pure functions for cleaning data and manipulating DuckDB Relations or SQL.
  - Examples: filtering by award type, computing derived columns, normalizing NAICS/PSC, computing signals.
  - No I/O, no `duckdb.connect`, no network requests.

- `backend/src/analyze.py`
  - **Longer-running aggregations, reports, and sector/summary analysis** live here.
  - Use this for portfolio-level or sector-level analysis, dashboards, or summary tables that aggregate over the transformed data.
  - Row-level transforms still belong in `transform.py`; `analyze.py` is for higher-level aggregations and analytical queries.

- `backend/src/enrichment.py`
  - HTTP/API client functions (GLEIF, OpenFIGI, Yahoo Finance/yfinance).
  - Network calls, retry logic, rate limiting, response parsing.
  - **Must not** open DuckDB connections or execute SQL.
  - Returns Python structures (e.g., dicts, small dataclasses) that the orchestrator can pass to IO functions for caching.

- `backend/scripts/`
  - **ONLY orchestrator scripts** go here.
  - These scripts:
    - Use `io.py` to connect to DuckDB and scan CSVs.
    - Call pure functions in `transform.py` / `analyze.py`.
    - Call enrichment functions in `enrichment.py` and then pass results to `io.py` for persistence.
  - Example scripts: `ingest.py`, `enrich.py`, `themes.py`, `signals.py`.

- `backend/tests/`
  - All unit and integration tests.
  - Unit tests target pure functions with in-memory DuckDB.
  - Integration tests may use sample CSVs from `backend/data/raw/samples/` and the real `cleaned.duckdb`/`cache.duckdb`.

- `backend/models/`
  - Central definitions for `@dataclass` schemas used for structured inputs/outputs and validation.
  - Always import shared schemas from here instead of redefining them inline.

- `backend/docs/` (or `agent-docs/`, if used)
  - Markdown instructions for phases and project overview.
  - Keep these in sync with the code structure.

---

## 4. Coding Standards

### 4.1 DO THIS
- Use type hints for **all** function signatures (e.g., `duckdb.DuckDBPyRelation`, `duckdb.DuckDBPyConnection`).
- Write small, composable functions that chain DuckDB relational operations (`.filter`, `.select`, `.project`, `.aggregate`).
- Leverage DuckDB's native SQL functions for data casting, string manipulation, date arithmetic, and aggregation.
- Explicitly pass DuckDB connections or relations as function arguments; do **not** rely on globals.
- Reuse dataclasses in `backend/models/` to define structured inputs/outputs and to drive tests.
- Express row-level and column-level logic as SQL/relational expressions, not Python loops.

### 4.2 NEVER DO THIS (Anti-Patterns)
- **NO OOP State:** Do **not** create classes to hold state or behavior. Use plain functions and immutable data.
- **NO Memory Bloat:**
  - Do **not** use `csv.reader`, `f.read()`, or `pandas.read_csv()` on raw 2GB+ data files.
  - Do **not** call `.df()` or `.fetchall()` on large relations except when the result is known to be tiny.
  - Do **not** iterate in Python row-by-row over large datasets (e.g., `for row in rel.df().itertuples()`).
- **NO Mixed Concerns:**
  - Do **not** put `print()` statements, `duckdb.connect()`, or file export commands inside transformation or analysis functions.
  - Do **not** perform network requests inside `transform.py` or `analyze.py`.
- **NO Magic Numbers/Strings:** Extract hardcoded schema lists, filters, and file paths into configuration variables or pass them as function arguments.

---

## 5. Agent Workflow / Execution Loop
When asked to build a new feature or process a new data field, follow this exact sequence:

1. **Analyze**
   - Inspect a small sample of the relevant data (using a tiny CSV or an in-memory relation).
   - Read the relevant phase document (e.g., `02_PHASE_1_INGESTION.md`) plus `01_PROJECT_OVERVIEW.md`.

2. **Draft Pure Function**
   - Write a pure function in `backend/src/transform.py` (or `analyze.py` for aggregations) that:
     - Takes a `duckdb.DuckDBPyRelation` as input.
     - Returns a modified `DuckDBPyRelation` or SQL string.
     - Contains no I/O, no connections, no prints, no network calls.

3. **Write Test**
   - Create a unit test in `backend/tests/unit/`.
   - Spin up an in-memory DuckDB connection via `duckdb.connect(':memory:')`.
   - Build a mock relation from a small static dataset.
   - Pass it to your pure function.
   - Assert that the output relation contains exactly the expected schema and values.

4. **Wire to I/O (if needed)**
   - Update `backend/src/io.py` to:
     - Scan any new CSVs with DuckDB.
     - Read/write new DuckDB tables in `cleaned.duckdb` or `cache.duckdb`.
     - Export results to CSV/Parquet if required.
   - Ensure that `io.py` contains no business logic—just plumbing.

5. **Orchestrate**
   - Update or create a script in `backend/scripts/` to:
     - Open the relevant DuckDB database.
     - Use `io.py` to read input tables.
     - Pass relations through the pure functions in `transform.py`/`analyze.py`.
     - Call enrichment functions in `enrichment.py` where required.
     - Use `io.py` again to persist final results or exports.

6. **Phase Discipline**
   - Only implement and run the phase you are currently instructed to work on.
   - Do not add Phase 2/3/4 logic while working on Phase 1, and vice versa.

---

## 6. Environment Management
- **Use UV for Python envs:** All Python dependencies must be managed with `uv`. Do not rely on `pipenv`, `poetry`, or plain `pip install`.
- Keep the UV lockfile artifacts (e.g., `uv.lock`) up to date whenever you add or upgrade packages.
- Document any new CLI scripts or UV commands in `backend/docs/` so other agents and humans can reproduce the environment easily.
- **Secrets discipline:** Store every API key, credential, or endpoint token in the project `.env` file and **never** hardcode them in code or commit history.

---

## 7. Code Example: The Standard We Expect
```python
import duckdb

# --- In backend/src/transform.py ---
def normalize_obligation(rel: duckdb.DuckDBPyRelation) -> duckdb.DuckDBPyRelation:
    """
    Pure function: cleans raw obligation amounts using DuckDB SQL.
    No I/O, no connections, no prints.
    """
    query = """
        SELECT
            COALESCE(award_id_piid, 'UNKNOWN') AS award_id,
            TRY_CAST(REPLACE(COALESCE(federal_action_obligation, '0'), ',', '') AS DOUBLE) AS amount
        FROM _rel
    """
    # relation.query() references the relation itself as '_rel'
    return rel.query("normalize_obligation_view", query)

# --- In backend/src/io.py ---
def get_cleaned_conn() -> duckdb.DuckDBPyConnection:
    return duckdb.connect("backend/data/cleaned/cleaned.duckdb")

def scan_contracts_csv(conn: duckdb.DuckDBPyConnection, filepath: str) -> duckdb.DuckDBPyRelation:
    """ I/O: Mount a large CSV lazily without loading into memory. """
    return conn.read_csv(filepath, header=True, sample_size=10000)

def write_to_csv(rel: duckdb.DuckDBPyRelation, output_path: str) -> None:
    """ I/O: Streams the relation out to a new CSV or uses COPY TO. """
    rel.to_csv(output_path, header=True)

# --- In backend/scripts/process_obligations.py ---
def main() -> None:
    # Orchestration only
    conn = get_cleaned_conn()
    raw_rel = scan_contracts_csv(conn, "backend/data/raw/contracts/sample.csv")
    cleaned_rel = normalize_obligation(raw_rel)
    write_to_csv(cleaned_rel, "backend/data/out/cleaned_obligations.csv")
```
