import sys
import duckdb

sys.path.append(".")
from backend.src.io import ensure_cache_tables

cache_dir = "backend/data/cache"
bak_dir = f"{cache_dir}/bak"

try:
    conn = duckdb.connect()
    conn.execute(f"ATTACH '{cache_dir}/cache.duckdb' AS cache;")
    
    ensure_cache_tables(conn)
    
    queries = {
        "cache_entity_hierarchy": f"SELECT DISTINCT ON (cage_code) * FROM '{bak_dir}/cache_entity_hierarchy.parquet' ORDER BY cage_code, last_verified DESC",
        "cache_openfigi_ticker": f"SELECT DISTINCT ON (highest_level_owner_name) * FROM '{bak_dir}/cache_openfigi_ticker.parquet' ORDER BY highest_level_owner_name, fetched_at DESC",
        "cache_market_cap": f"SELECT DISTINCT ON (ticker, date) * FROM '{bak_dir}/cache_market_cap.parquet' ORDER BY ticker, date, fetched_at DESC",
        "cache_failures": f"SELECT DISTINCT ON (provider, key) * FROM '{bak_dir}/cache_failures.parquet' ORDER BY provider, key, last_attempt_at DESC"
    }
    
    for table, query in queries.items():
        try:
            print(f"Importing {table}...")
            conn.execute(f"INSERT INTO cache.{table} {query}")
            print(f"Success: Imported {table}")
        except Exception as e:
            print(f"Error importing {table}: {e}")
            
    conn.close()
    print("Recovery import complete.")
except Exception as e:
    print(f"Failed to connect to cache database: {e}")
