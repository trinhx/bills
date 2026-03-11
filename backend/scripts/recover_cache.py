import duckdb
import os
import shutil

cache_dir = "backend/data/cache"
bak_dir = f"{cache_dir}/bak"
os.makedirs(bak_dir, exist_ok=True)

try:
    conn = duckdb.connect(f"{cache_dir}/cache.duckdb")
    
    tables = [
        "cache.cache_entity_hierarchy", 
        "cache.cache_openfigi_ticker", 
        "cache.cache_market_cap", 
        "cache.cache_failures"
    ]
    
    for table in tables:
        table_name = table.split('.')[-1]
        try:
            print(f"Exporting {table_name}...")
            # We don't query the table directly, we let duckdb export it natively
            conn.execute(f"COPY {table_name} TO '{bak_dir}/{table_name}.parquet' (FORMAT PARQUET)")
            print(f"Success: Exported {table_name}")
        except Exception as e:
            print(f"Error exporting {table_name}: {e}")
            
    conn.close()
    print("Recovery export complete.")
except Exception as e:
    print(f"Failed to connect to cache database: {e}")
