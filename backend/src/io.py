import duckdb
from pathlib import Path
from datetime import datetime

def get_cleaned_conn(db_path: str = "backend/data/cleaned/cleaned.duckdb") -> duckdb.DuckDBPyConnection:
    """Open connection to the cleaned DuckDB database."""
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(db_path)
    conn.execute("SET wal_autocheckpoint='1000MB';")
    return conn

def scan_contracts_csv(conn: duckdb.DuckDBPyConnection, csv_path: str) -> duckdb.DuckDBPyRelation:
    """Lazily scan contracts CSV using duckdb."""
    return conn.read_csv(csv_path)

def persist_table(conn: duckdb.DuckDBPyConnection, rel: duckdb.DuckDBPyRelation, table_name: str) -> None:
    """Materialize a relation into a DuckDB table."""
    view_name = f"temp_view_{table_name}"
    rel.create_view(view_name, replace=True)
    conn.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM {view_name}")

def write_profile(conn: duckdb.DuckDBPyConnection, profile_rel: duckdb.DuckDBPyRelation) -> None:
    """Persist ingestion_profile table."""
    persist_table(conn, profile_rel, "ingestion_profile")

# --- Phase 2 Caching IO ---

def get_cache_conn(db_path: str = "backend/data/cache/cache.duckdb") -> duckdb.DuckDBPyConnection:
    """Open connection to the cache DuckDB database."""
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(db_path)

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
        conn.execute("ALTER TABLE cache.cache_entity_hierarchy ADD COLUMN IF NOT EXISTS immediate_level_owner BOOLEAN")
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
            sector TEXT,
            industry TEXT,
            fetched_at TIMESTAMP,
            source_payload_hash TEXT,
            status TEXT,
            PRIMARY KEY (ticker, date)
        )
    """)
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

# Helper IO methods for Phase 2 Cache Access

def get_cached_entity_hierarchy(conn: duckdb.DuckDBPyConnection, cage_code: str) -> dict | None:
    res = conn.execute("SELECT * FROM cache.cache_entity_hierarchy WHERE cage_code = ?", [cage_code]).fetchone()
    if res:
        cols = [desc[0] for desc in conn.description]
        return dict(zip(cols, res))
    return None

def upsert_cached_entity_hierarchy(conn: duckdb.DuckDBPyConnection, data: dict) -> None:
    _columns = {
        "cage_code", "cage_business_name", "cage_update_date", "is_highest", "immediate_level_owner",
        "highest_level_owner_name", "highest_level_cage_code",
        "highest_level_cage_update_date", "result_status", "last_verified",
    }
    data = {k: v for k, v in data.items() if k in _columns}
    keys = list(data.keys())
    placeholders = ", ".join(["?"] * len(keys))
    updates = ", ".join([f"{k}=EXCLUDED.{k}" for k in keys if k != 'cage_code'])
    sql = f"""
        INSERT INTO cache.cache_entity_hierarchy ({", ".join(keys)})
        VALUES ({placeholders})
        ON CONFLICT(cage_code) DO UPDATE SET {updates}
    """
    conn.execute(sql, list(data.values()))

def get_cached_openfigi_ticker(conn: duckdb.DuckDBPyConnection, owner_name: str) -> dict | None:
    res = conn.execute("SELECT * FROM cache.cache_openfigi_ticker WHERE highest_level_owner_name = ?", [owner_name]).fetchone()
    if res:
        cols = [desc[0] for desc in conn.description]
        return dict(zip(cols, res))
    return None

def upsert_cached_openfigi_ticker(conn: duckdb.DuckDBPyConnection, data: dict) -> None:
    keys = list(data.keys())
    placeholders = ", ".join(["?"] * len(keys))
    updates = ", ".join([f"{k}=EXCLUDED.{k}" for k in keys if k != 'highest_level_owner_name'])
    sql = f"""
        INSERT INTO cache.cache_openfigi_ticker ({", ".join(keys)})
        VALUES ({placeholders})
        ON CONFLICT(highest_level_owner_name) DO UPDATE SET {updates}
    """
    conn.execute(sql, list(data.values()))

def get_cached_market_cap(conn: duckdb.DuckDBPyConnection, ticker: str, date_val: str) -> dict | None:
    res = conn.execute("SELECT * FROM cache.cache_market_cap WHERE ticker = ? AND date = ?", [ticker, date_val]).fetchone()
    if res:
        cols = [desc[0] for desc in conn.description]
        return dict(zip(cols, res))
    return None

def upsert_cached_market_cap(conn: duckdb.DuckDBPyConnection, data: dict) -> None:
    keys = list(data.keys())
    placeholders = ", ".join(["?"] * len(keys))
    updates = ", ".join([f"{k}=EXCLUDED.{k}" for k in keys if k not in ('ticker', 'date')])
    sql = f"""
        INSERT INTO cache.cache_market_cap ({", ".join(keys)})
        VALUES ({placeholders})
        ON CONFLICT(ticker, date) DO UPDATE SET {updates}
    """
    conn.execute(sql, list(data.values()))

def get_failure(conn: duckdb.DuckDBPyConnection, provider: str, key: str) -> dict | None:
    res = conn.execute("SELECT * FROM cache.cache_failures WHERE provider = ? AND key = ?", [provider, key]).fetchone()
    if res:
        cols = [desc[0] for desc in conn.description]
        return dict(zip(cols, res))
    return None

def upsert_failure(conn: duckdb.DuckDBPyConnection, provider: str, key: str, 
                   error_type: str, http_status: int, message: str, 
                   retry_after_seconds: int, attempts: int) -> None:
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
    conn.execute(sql, [provider, key, error_type, http_status, message, retry_after_seconds, attempts, now])

def export_to_csv(conn: duckdb.DuckDBPyConnection, table_name: str, file_path: str) -> None:
    """Export a table natively to CSV."""
    Path(file_path).parent.mkdir(parents=True, exist_ok=True)
    # Using specific headers and delimiters
    conn.execute(f"COPY (SELECT * FROM {table_name}) TO '{file_path}' (HEADER, DELIMITER ',')")


