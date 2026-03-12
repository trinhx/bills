import argparse
import logging
from pathlib import Path
import duckdb

import sys
sys.path.append(".")
from backend.src.io import get_cleaned_conn, scan_contracts_csv, ensure_cache_tables
from backend.src.transform import filter_and_select_phase1, extract_unique_cage_code

# Rate limits in seconds per request
CAGE_SECONDS_PER_REQ = 1.5
OPENFIGI_SECONDS_PER_REQ = 2.5
YAHOO_SECONDS_PER_REQ = 1.0

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(message)s"
    )
    return logging.getLogger(__name__)

def format_duration(seconds: float) -> str:
    if seconds < 60:
        return f"{seconds:.1f}s"
    minutes = seconds / 60
    if minutes < 60:
        return f"{minutes:.1f}m"
    hours = minutes / 60
    return f"{hours:.2f}h"

def main():
    parser = argparse.ArgumentParser(description="Estimate Pipeline Run Time")
    parser.add_argument("--csv", required=True, help="Path to source CSV")
    args = parser.parse_args()

    logger = setup_logging()
    
    csv_path = args.csv
    if not Path(csv_path).exists():
        logger.error(f"Input CSV not found: {csv_path}")
        return

    logger.info("========================================")
    logger.info(" Pipeline Estimation Tool")
    logger.info("========================================")

    cl_conn = get_cleaned_conn()
    cl_conn.execute("ATTACH 'backend/data/cache/cache.duckdb' AS cache;")
    ensure_cache_tables(cl_conn)

    # 1. Base CAGE Codes
    logger.info("Scanning raw dataset...")
    raw_rel = scan_contracts_csv(cl_conn, csv_path)
    filtered_rel = filter_and_select_phase1(raw_rel)
    
    cages_rel = extract_unique_cage_code(filtered_rel)
    total_cages = cages_rel.aggregate("count(*)").fetchone()[0]
    
    # Check cache for CAGE
    cached_cages = cl_conn.execute("SELECT COUNT(*) FROM cache.cache_entity_hierarchy").fetchone()[0]
    
    # We can't easily know precisely how many of our specific cages are cached without a join,
    # but let's do an exact anti-join to find purely uncached ones for accuracy.
    cages_rel.create_view("uncached_cages_view", replace=True)
    pending_cages = cl_conn.execute("""
        SELECT count(*) FROM uncached_cages_view u
        LEFT JOIN cache.cache_entity_hierarchy c ON u.cage_code = c.cage_code
        WHERE c.cage_code IS NULL
    """).fetchone()[0]

    cage_est_seconds = pending_cages * CAGE_SECONDS_PER_REQ

    logger.info(f"CAGE Requests:")
    logger.info(f"  Total Unique:      {total_cages}")
    logger.info(f"  Pending:           {pending_cages}")
    logger.info(f"  Est. Duration:     {format_duration(cage_est_seconds)}")

    # 2. OpenFIGI
    # Get total unique highest_level_owner_name from cached cage data,
    # plus assume 1 new owner for each pending cage.
    cached_owners = cl_conn.execute("""
        SELECT COUNT(DISTINCT highest_level_owner_name) 
        FROM cache.cache_entity_hierarchy
        WHERE highest_level_owner_name IS NOT NULL
    """).fetchone()[0]
    
    # Find how many owners are NOT in openfigi cache
    pending_owners_from_cache = cl_conn.execute("""
        SELECT COUNT(DISTINCT h.highest_level_owner_name)
        FROM cache.cache_entity_hierarchy h
        LEFT JOIN cache.cache_openfigi_ticker o ON h.highest_level_owner_name = o.highest_level_owner_name
        WHERE h.highest_level_owner_name IS NOT NULL
          AND o.highest_level_owner_name IS NULL
    """).fetchone()[0]
    
    # Add pending cages as potential new unique owners
    # Assuming worst case where every pending cage has a unique new highest level owner
    pending_openfigi = pending_owners_from_cache + pending_cages
    openfigi_est_seconds = pending_openfigi * OPENFIGI_SECONDS_PER_REQ

    logger.info(f"\nOpenFIGI Requests:")
    logger.info(f"  Pending (derived): ~{pending_openfigi} (worst-case)")
    logger.info(f"  Est. Duration:     {format_duration(openfigi_est_seconds)}")

    # 3. Yahoo Finance
    # For Yahoo, we need ticker + action_date.
    # We can approximate by looking at unique dates in the source data.
    unique_dates = cl_conn.execute(f"""
        SELECT COUNT(DISTINCT action_date) 
        FROM '{csv_path}'
    """).fetchone()[0]
    
    # And total unique tickers from openfigi cache
    unique_tickers = cl_conn.execute("""
        SELECT COUNT(DISTINCT ticker) 
        FROM cache.cache_openfigi_ticker
        WHERE ticker IS NOT NULL
    """).fetchone()[0]
    
    # We estimate worst-case missing Yahoo requests:
    pending_yahoo = (unique_tickers * unique_dates) - cl_conn.execute("SELECT COUNT(*) FROM cache.cache_market_cap").fetchone()[0]
    if pending_yahoo < 0:
        pending_yahoo = 0
        
    # Add potential new tickers from pending openfigi requests
    # Assume 10% of new openfigi requests result in a valid ticker
    potential_new_tickers = int(pending_openfigi * 0.10)
    pending_yahoo += (potential_new_tickers * unique_dates)

    yahoo_est_seconds = pending_yahoo * YAHOO_SECONDS_PER_REQ

    logger.info(f"\nYahoo Finance Requests:")
    logger.info(f"  Pending (derived): ~{pending_yahoo} (worst-case)")
    logger.info(f"  Est. Duration:     {format_duration(yahoo_est_seconds)}")
    
    # Total
    total_est_seconds = cage_est_seconds + openfigi_est_seconds + yahoo_est_seconds
    
    logger.info("========================================")
    logger.info(f" Total Est. Duration: {format_duration(total_est_seconds)}")
    logger.info("========================================")

    cl_conn.close()

if __name__ == "__main__":
    main()
