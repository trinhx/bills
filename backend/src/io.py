import duckdb
import os
import pandas as pd
from pathlib import Path
from datetime import date, datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    # Used only for type hints; importing at module load time would create
    # a circular dependency with the provider module (providers use io
    # helpers indirectly via tests).
    from backend.app.services.providers.returns import ReturnsProvider  # noqa: F401


#: Default DuckDB memory budget for the pipeline.
#:
#: Phase 1 ingestion runs a ``LAG()`` window function partitioned on
#: ``award_id_piid`` across every input CSV. On a multi-year ingestion
#: (37 files, ~37M raw rows, 297 columns), DuckDB's default "80% of
#: system RAM" budget can trigger the Linux OOM-killer on 30-GB-RAM
#: workstations -- the process is terminated with no Python traceback,
#: leaving ``cleaned.duckdb`` in a partial / confusing state.
#:
#: We pin a 12 GB budget here (conservative relative to a 30 GB box,
#: leaving ~18 GB for the OS, Python interpreter, and provider caches).
#: DuckDB spills to disk (via the configured ``temp_directory``) when
#: workloads exceed this. Operators with larger machines can raise the
#: limit via the ``DUCKDB_MEMORY_LIMIT`` environment variable (e.g.
#: "24GB" on a 64-GB server).
_DEFAULT_MEMORY_LIMIT = "12GB"

#: Where DuckDB writes spill files when ``memory_limit`` is exceeded.
#: We colocate this with the cleaned DB so operators only need to free
#: one volume if disk pressure becomes an issue. Override via
#: ``DUCKDB_TEMP_DIR``.
_DEFAULT_TEMP_DIR = "backend/data/tmp"


def _apply_pragmas(conn: duckdb.DuckDBPyConnection) -> None:
    """
    Pin the memory budget and enable spill-to-disk so that long pipeline
    runs don't OOM-kill on multi-GB ingestions. Kept as a helper so
    cache connections and the cleaned DB get identical settings.
    """
    mem_limit = os.getenv("DUCKDB_MEMORY_LIMIT", _DEFAULT_MEMORY_LIMIT)
    temp_dir = os.getenv("DUCKDB_TEMP_DIR", _DEFAULT_TEMP_DIR)
    Path(temp_dir).mkdir(parents=True, exist_ok=True)
    # ``memory_limit`` is a soft cap that triggers spill-to-disk; pairing
    # with an explicit ``temp_directory`` avoids DuckDB writing spill
    # files next to the main database (confusing on disk) and lets
    # operators place the scratch volume on a faster drive if needed.
    conn.execute(f"SET memory_limit='{mem_limit}';")
    conn.execute(f"SET temp_directory='{temp_dir}';")
    conn.execute("SET wal_autocheckpoint='1000MB';")


def get_cleaned_conn(
    db_path: str = "backend/data/cleaned/cleaned.duckdb",
) -> duckdb.DuckDBPyConnection:
    """Open connection to the cleaned DuckDB database."""
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(db_path)
    _apply_pragmas(conn)
    return conn


def scan_contracts_csv(
    conn: duckdb.DuckDBPyConnection,
    csv_path: "str | Path | list[str | Path]",
) -> duckdb.DuckDBPyRelation:
    """
    Lazily scan one or more USASpending contracts CSVs using DuckDB.

    Accepts:
      * a single path (str or Path) to one CSV
      * a list of paths (mix of str and Path is fine)
      * a glob-style string like ``"data/FY2024_*.csv"`` (DuckDB expands it)

    DuckDB streams all files in parallel as a single relation. Window
    functions (e.g. Phase 1's ``prev_potential_value`` LAG on
    ``award_id_piid``) therefore see every row across every file, which
    is essential for the multi-file quarterly / yearly ingestions where
    modifications of the same piid may land in different files.

    Type inference notes:

    * DuckDB's default ``read_csv`` samples the first ~10K rows, which can
      mis-type columns that are mostly numeric/date but contain occasional
      blanks or text values deeper in a multi-GB file. We pin explicit
      types for every column the pipeline filters on, casts, or uses in
      arithmetic so that downstream SQL is deterministic. Remaining columns
      fall through to inference.
    * ``union_by_name=True`` is used for multi-file inputs to align columns
      by header name rather than positional order. USASpending's exports
      are supposed to have identical headers across split files, but this
      is a cheap safety net against future column reorderings.
    """
    dtype_overrides = {
        # Numerics we filter on or use in signals math
        "federal_action_obligation": "DOUBLE",
        "total_dollars_obligated": "DOUBLE",
        "current_total_value_of_award": "DOUBLE",
        "potential_total_value_of_award": "DOUBLE",
        "number_of_offers_received": "DOUBLE",
        # Dates used in window functions / date_diff
        "action_date": "DATE",
        "solicitation_date": "DATE",
        "period_of_performance_start_date": "DATE",
        "period_of_performance_current_end_date": "DATE",
        # Identifiers / keys we always treat as strings (some look numeric)
        "contract_transaction_unique_key": "VARCHAR",
        "award_id_piid": "VARCHAR",
        "parent_award_id_piid": "VARCHAR",
        "cage_code": "VARCHAR",
        "recipient_parent_uei": "VARCHAR",
        "product_or_service_code": "VARCHAR",
        "naics_code": "VARCHAR",
        "award_type": "VARCHAR",
    }

    # Normalise to what DuckDB's read_csv expects: a str, a list[str],
    # or a glob string (which DuckDB handles natively when passed as str).
    if isinstance(csv_path, (list, tuple)):
        csv_paths: "list[str]" = [str(p) for p in csv_path]
        return conn.read_csv(
            csv_paths,
            dtype=dtype_overrides,
            union_by_name=True,
        )
    return conn.read_csv(
        str(csv_path),
        dtype=dtype_overrides,
    )


def persist_table(
    conn: duckdb.DuckDBPyConnection, rel: duckdb.DuckDBPyRelation, table_name: str
) -> None:
    """Materialize a relation into a DuckDB table."""
    view_name = f"temp_view_{table_name}"
    rel.create_view(view_name, replace=True)
    conn.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM {view_name}")


def write_profile(
    conn: duckdb.DuckDBPyConnection, profile_rel: duckdb.DuckDBPyRelation
) -> None:
    """Persist ingestion_profile table."""
    persist_table(conn, profile_rel, "ingestion_profile")


# --- Phase 2 Caching IO ---


def get_cache_conn(
    db_path: str = "backend/data/cache/cache.duckdb",
) -> duckdb.DuckDBPyConnection:
    """Open connection to the cache DuckDB database."""
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(db_path)
    _apply_pragmas(conn)
    return conn


def ensure_cache_tables(conn: duckdb.DuckDBPyConnection) -> None:
    """Create cache tables if they do not exist."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS cache.cache_entity_hierarchy (
            cage_code TEXT PRIMARY KEY,
            cage_business_name TEXT,
            cage_update_date DATE,
            is_highest BOOLEAN,
            immediate_level_owner BOOLEAN,
            highest_level_owner_name TEXT,
            highest_level_cage_code TEXT,
            highest_level_cage_update_date DATE,
            result_status TEXT,
            last_verified TIMESTAMP
        )
    """)
    try:
        conn.execute(
            "ALTER TABLE cache.cache_entity_hierarchy ADD COLUMN IF NOT EXISTS immediate_level_owner BOOLEAN"
        )
    except duckdb.BinderException:
        pass
    conn.execute("""
        CREATE TABLE IF NOT EXISTS cache.cache_openfigi_ticker (
            highest_level_owner_name TEXT PRIMARY KEY,
            ticker TEXT,
            exchange TEXT,
            security_type TEXT,
            fetched_at TIMESTAMP,
            source_payload_hash TEXT,
            status TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS cache.cache_market_cap (
            ticker TEXT,
            date DATE,
            market_cap DOUBLE,
            close_price DOUBLE,
            shares_outstanding DOUBLE,
            market_cap_quality TEXT,
            sector TEXT,
            industry TEXT,
            fetched_at TIMESTAMP,
            source_payload_hash TEXT,
            status TEXT,
            PRIMARY KEY (ticker, date)
        )
    """)
    # Non-destructive migration for existing caches that pre-date the
    # M1 point-in-time columns. DuckDB ``ADD COLUMN IF NOT EXISTS`` is
    # idempotent.
    for col_def in (
        "close_price DOUBLE",
        "shares_outstanding DOUBLE",
        "market_cap_quality TEXT",
    ):
        try:
            conn.execute(
                f"ALTER TABLE cache.cache_market_cap ADD COLUMN IF NOT EXISTS {col_def}"
            )
        except duckdb.BinderException:
            pass
    conn.execute("""
        CREATE TABLE IF NOT EXISTS cache.cache_failures (
            provider TEXT,
            key TEXT,
            error_type TEXT,
            http_status INTEGER,
            message TEXT,
            retry_after_seconds INTEGER,
            attempts INTEGER,
            last_attempt_at TIMESTAMP,
            PRIMARY KEY (provider, key)
        )
    """)
    # M2.2: daily split-adjusted returns cache. Populated by the Milestone 2
    # validation harness. Historical prices are immutable; we never overwrite
    # a ``(ticker, date)`` row once it's been fetched.
    conn.execute("""
        CREATE TABLE IF NOT EXISTS cache.cache_returns (
            ticker        TEXT NOT NULL,
            date          DATE NOT NULL,
            close_adj     DOUBLE,
            return_1d     DOUBLE,
            fetched_at    TIMESTAMP,
            source        TEXT,
            is_benchmark  BOOLEAN DEFAULT FALSE,
            PRIMARY KEY (ticker, date)
        )
    """)


# Helper IO methods for Phase 2 Cache Access


def get_cached_entity_hierarchy(
    conn: duckdb.DuckDBPyConnection, cage_code: str
) -> dict | None:
    res = conn.execute(
        "SELECT * FROM cache.cache_entity_hierarchy WHERE cage_code = ?", [cage_code]
    ).fetchone()
    if res:
        cols = [desc[0] for desc in conn.description]
        return dict(zip(cols, res))
    return None


def upsert_cached_entity_hierarchy(conn: duckdb.DuckDBPyConnection, data: dict) -> None:
    _columns = {
        "cage_code",
        "cage_business_name",
        "cage_update_date",
        "is_highest",
        "immediate_level_owner",
        "highest_level_owner_name",
        "highest_level_cage_code",
        "highest_level_cage_update_date",
        "result_status",
        "last_verified",
    }
    data = {k: v for k, v in data.items() if k in _columns}
    keys = list(data.keys())
    placeholders = ", ".join(["?"] * len(keys))
    updates = ", ".join([f"{k}=EXCLUDED.{k}" for k in keys if k != "cage_code"])
    sql = f"""
        INSERT INTO cache.cache_entity_hierarchy ({", ".join(keys)})
        VALUES ({placeholders})
        ON CONFLICT(cage_code) DO UPDATE SET {updates}
    """
    conn.execute(sql, list(data.values()))


def get_cached_openfigi_ticker(
    conn: duckdb.DuckDBPyConnection, owner_name: str
) -> dict | None:
    res = conn.execute(
        "SELECT * FROM cache.cache_openfigi_ticker WHERE highest_level_owner_name = ?",
        [owner_name],
    ).fetchone()
    if res:
        cols = [desc[0] for desc in conn.description]
        return dict(zip(cols, res))
    return None


def upsert_cached_openfigi_ticker(conn: duckdb.DuckDBPyConnection, data: dict) -> None:
    keys = list(data.keys())
    placeholders = ", ".join(["?"] * len(keys))
    updates = ", ".join(
        [f"{k}=EXCLUDED.{k}" for k in keys if k != "highest_level_owner_name"]
    )
    sql = f"""
        INSERT INTO cache.cache_openfigi_ticker ({", ".join(keys)})
        VALUES ({placeholders})
        ON CONFLICT(highest_level_owner_name) DO UPDATE SET {updates}
    """
    conn.execute(sql, list(data.values()))


def get_cached_market_cap(
    conn: duckdb.DuckDBPyConnection, ticker: str, date_val: str
) -> dict | None:
    res = conn.execute(
        "SELECT * FROM cache.cache_market_cap WHERE ticker = ? AND date = ?",
        [ticker, date_val],
    ).fetchone()
    if res:
        cols = [desc[0] for desc in conn.description]
        return dict(zip(cols, res))
    return None


def upsert_cached_market_cap(conn: duckdb.DuckDBPyConnection, data: dict) -> None:
    keys = list(data.keys())
    placeholders = ", ".join(["?"] * len(keys))
    updates = ", ".join(
        [f"{k}=EXCLUDED.{k}" for k in keys if k not in ("ticker", "date")]
    )
    sql = f"""
        INSERT INTO cache.cache_market_cap ({", ".join(keys)})
        VALUES ({placeholders})
        ON CONFLICT(ticker, date) DO UPDATE SET {updates}
    """
    conn.execute(sql, list(data.values()))


def get_failure(
    conn: duckdb.DuckDBPyConnection, provider: str, key: str
) -> dict | None:
    res = conn.execute(
        "SELECT * FROM cache.cache_failures WHERE provider = ? AND key = ?",
        [provider, key],
    ).fetchone()
    if res:
        cols = [desc[0] for desc in conn.description]
        return dict(zip(cols, res))
    return None


def upsert_failure(
    conn: duckdb.DuckDBPyConnection,
    provider: str,
    key: str,
    error_type: str,
    http_status: int,
    message: str,
    retry_after_seconds: int,
    attempts: int,
) -> None:
    now = datetime.now()
    sql = """
        INSERT INTO cache.cache_failures 
        (provider, key, error_type, http_status, message, retry_after_seconds, attempts, last_attempt_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(provider, key) DO UPDATE SET
            error_type = EXCLUDED.error_type,
            http_status = EXCLUDED.http_status,
            message = EXCLUDED.message,
            retry_after_seconds = EXCLUDED.retry_after_seconds,
            attempts = EXCLUDED.attempts,
            last_attempt_at = EXCLUDED.last_attempt_at
    """
    conn.execute(
        sql,
        [
            provider,
            key,
            error_type,
            http_status,
            message,
            retry_after_seconds,
            attempts,
            now,
        ],
    )


def export_to_csv(
    conn: duckdb.DuckDBPyConnection, table_name: str, file_path: str
) -> None:
    """Export a table natively to CSV."""
    Path(file_path).parent.mkdir(parents=True, exist_ok=True)
    # Using specific headers and delimiters
    conn.execute(
        f"COPY (SELECT * FROM {table_name}) TO '{file_path}' (HEADER, DELIMITER ',')"
    )


# ---------------------------------------------------------------------------
# M2.2 — cache_returns helpers
# ---------------------------------------------------------------------------


def get_cached_returns_df(
    conn: duckdb.DuckDBPyConnection,
    ticker: str,
    start: date,
    end: date,
) -> pd.DataFrame:
    """
    Return the cached daily bars for ``ticker`` over the inclusive date
    range ``[start, end]`` as a pandas DataFrame indexed by date with
    ``close_adj`` and ``return_1d`` columns. Matches the output shape of
    :class:`ReturnsProvider.fetch_daily_bars`.

    Empty DataFrame (0 rows) means "nothing cached for that range" --
    callers treat that as a cache miss and re-fetch from the provider.
    """
    df = conn.execute(
        """
        SELECT date, close_adj, return_1d
        FROM cache.cache_returns
        WHERE ticker = ?
          AND date BETWEEN ? AND ?
        ORDER BY date
        """,
        [ticker, start, end],
    ).df()
    if df.empty:
        return pd.DataFrame(columns=["close_adj", "return_1d"])
    out = df.set_index("date")[["close_adj", "return_1d"]]
    out.index = pd.to_datetime(out.index)
    return out


def upsert_cached_returns(
    conn: duckdb.DuckDBPyConnection,
    ticker: str,
    bars: pd.DataFrame,
    *,
    source: str = "yfinance",
    is_benchmark: bool = False,
) -> int:
    """
    Insert a batch of daily bars into ``cache_returns``.

    Historical prices are immutable: ``ON CONFLICT DO NOTHING`` so a
    re-fetch never overwrites an existing (ticker, date) row. Returns
    the count of newly-inserted rows.

    ``bars`` is expected to be a DataFrame indexed by date (tz-naive
    preferred) with ``close_adj`` and ``return_1d`` columns --
    the shape emitted by ``YFinanceReturnsProvider.fetch_daily_bars``.
    """
    if bars is None or bars.empty:
        return 0

    # Assemble an explicit, typed record list rather than register()-
    # ing the DataFrame so DuckDB doesn't re-infer types on every call.
    now = datetime.now()
    records = []
    for idx, row in bars.iterrows():
        ts = pd.Timestamp(idx)
        if ts.tzinfo is not None:
            ts = ts.tz_localize(None)
        records.append(
            (
                ticker,
                ts.date(),
                float(row["close_adj"]) if pd.notna(row["close_adj"]) else None,
                float(row["return_1d"]) if pd.notna(row["return_1d"]) else None,
                now,
                source,
                is_benchmark,
            )
        )

    before = conn.execute(
        "SELECT COUNT(*) FROM cache.cache_returns WHERE ticker = ?", [ticker]
    ).fetchone()[0]

    conn.executemany(
        """
        INSERT INTO cache.cache_returns
            (ticker, date, close_adj, return_1d, fetched_at, source, is_benchmark)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(ticker, date) DO NOTHING
        """,
        records,
    )

    after = conn.execute(
        "SELECT COUNT(*) FROM cache.cache_returns WHERE ticker = ?", [ticker]
    ).fetchone()[0]
    return after - before


def ensure_benchmark_pre_fetched(
    conn: duckdb.DuckDBPyConnection,
    provider: "ReturnsProvider",
    start: date,
    end: date,
    benchmark_ticker: str = "SPY",
) -> int:
    """
    Fetch ``benchmark_ticker`` bars for ``[start, end]`` if the cache
    doesn't already cover the full range. Returns the number of
    newly-cached bars (0 if cache already satisfied the request).

    Routes via ``provider.fetch_benchmark`` when the requested ticker is
    the canonical broad benchmark (so tests that only mock
    ``fetch_benchmark`` keep working), and via ``provider.fetch_daily_bars``
    for every other benchmark (ITA, XLK, etc.). Either code path marks
    the resulting cache rows as ``is_benchmark = TRUE``.
    """
    existing = conn.execute(
        """
        SELECT MIN(date) AS min_d, MAX(date) AS max_d, COUNT(*) AS n
        FROM cache.cache_returns
        WHERE ticker = ? AND is_benchmark = TRUE
        """,
        [benchmark_ticker],
    ).fetchone()
    min_d, max_d, n = existing
    if (
        n
        and min_d is not None
        and max_d is not None
        and min_d <= start
        and max_d >= end
    ):
        return 0  # full coverage

    # Prefer the dedicated ``fetch_benchmark`` convenience when the
    # caller wants the canonical ticker (SPY); otherwise use the
    # general daily-bars API with the explicit ticker.
    from backend.app.services.providers.returns import BENCHMARK_TICKER

    if benchmark_ticker == BENCHMARK_TICKER:
        bars = provider.fetch_benchmark(start, end)
    else:
        bars = provider.fetch_daily_bars(benchmark_ticker, start, end)

    return upsert_cached_returns(
        conn, benchmark_ticker, bars, source="yfinance", is_benchmark=True
    )
