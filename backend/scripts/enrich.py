import argparse
import logging
from datetime import datetime
from pathlib import Path

from backend.src.io import (
    get_cleaned_conn, get_cache_conn, ensure_cache_tables, persist_table, 
    get_cached_entity_hierarchy, upsert_cached_entity_hierarchy,
    get_cached_openfigi_ticker, upsert_cached_openfigi_ticker,
    get_cached_market_cap, upsert_cached_market_cap,
    get_failure, upsert_failure
)
from backend.src.transform import extract_unique_cage_code, join_entity_hierarchy, join_openfigi, join_market_cap

from backend.app.services.providers.cage_scraper import enrich_cage_data, CAGE_HEADERS, CAGE_COOKIES
from backend.app.services.providers.openfigi import process_owner_name
from backend.app.services.providers.yahoo import fetch_yahoo_data

def setup_logging():
    log_dir = Path("backend/data/logs")
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / "enrichment.log"
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s",
                        handlers=[logging.FileHandler(log_file), logging.StreamHandler()])
    return logging.getLogger(__name__)

def is_failure_recent(failure: dict) -> bool:
    if not failure: return False
    retry_after = failure.get("retry_after_seconds", 3600)
    # Default backoff is 1 hour if no retry-after was provided
    if retry_after == 0:
        retry_after = 3600 
    
    last_attempt = failure.get("last_attempt_at")
    if not last_attempt: return False
    
    delta = (datetime.now() - last_attempt).total_seconds()
    return delta < retry_after

def main():
    logger = setup_logging()
    logger.info("Starting Phase 2 Enrichment")
    
    cl_conn = get_cleaned_conn()
    ca_conn = get_cache_conn()
    ensure_cache_tables(ca_conn)
    
    raw_rel = cl_conn.table("raw_filtered_awards")
    
    # 1. Resolve Hierarchy
    cages = extract_unique_cage_code(raw_rel).fetchall()
    cage_list = [row[0] for row in cages if row[0]]
    logger.info(f"Found {len(cage_list)} unique CAGE codes to process")
    
    for cage in cage_list[:5]:
        if get_cached_entity_hierarchy(ca_conn, cage): continue
        if is_failure_recent(get_failure(ca_conn, "cage", cage)): continue
        try:
            res = enrich_cage_data(cage, CAGE_HEADERS, CAGE_COOKIES)
            if res:
                res["cage_code"] = cage
                res["result_status"] = "success"
                res["last_verified"] = datetime.now()
                upsert_cached_entity_hierarchy(ca_conn, res)
            else:
                res = {
                    "cage_code": cage,
                    "cage_business_name": None, "cage_update_date": None,
                    "is_highest": None, "highest_level_owner_name": None, "highest_level_cage_code": None,
                    "highest_level_cage_update_date": None, "result_status": "not_found",
                    "last_verified": datetime.now()
                }
                upsert_cached_entity_hierarchy(ca_conn, res)
        except Exception as e:
            upsert_failure(ca_conn, "cage", cage, type(e).__name__, getattr(e, "status_code", 500), str(e), getattr(e, "retry_after", 0), 5)
            
    # Prepare relation for next step
    cl_conn.execute("ATTACH 'backend/data/cache/cache.duckdb' AS cache")
    hierarchy_rel = cl_conn.table("cache.cache_entity_hierarchy")
    joined_hierarchy = join_entity_hierarchy(raw_rel, hierarchy_rel)
    
    # 2. Resolve OpenFIGI (get unique owner names)
    owners = joined_hierarchy.aggregate("highest_level_owner_name").filter("highest_level_owner_name IS NOT NULL").fetchall()
    owner_list = [row[0] for row in owners if row[0]]
    logger.info(f"Found {len(owner_list)} unique owner names for OpenFIGI")
    
    for owner in owner_list[:5]:
        if get_cached_openfigi_ticker(ca_conn, owner): continue
        if is_failure_recent(get_failure(ca_conn, "openfigi", owner)): continue
        try:
            res = process_owner_name(owner)
            if res:
                res["highest_level_owner_name"] = owner
                res["fetched_at"] = datetime.now()
                res["source_payload_hash"] = "na"
                res["status"] = "success"
                upsert_cached_openfigi_ticker(ca_conn, res)
        except Exception as e:
             upsert_failure(ca_conn, "openfigi", owner, type(e).__name__, getattr(e, "status_code", 500), str(e), getattr(e, "retry_after", 0), 5)

    ticker_rel = cl_conn.table("cache.cache_openfigi_ticker")
    joined_tickers = join_openfigi(joined_hierarchy, ticker_rel)
    
    # Compute is_public derived logic directly
    joined_tickers = joined_tickers.select("*, ticker IS NOT NULL as is_public")
    
    # 3. Resolve Market Cap
    ticker_pairs = joined_tickers.aggregate("ticker, action_date").filter("ticker IS NOT NULL AND action_date IS NOT NULL").fetchall()
    logger.info(f"Found {len(ticker_pairs)} unique ticker-date pairs for Yahoo Finance")
    
    for t_row in ticker_pairs[:5]:
        t_tick = t_row[0]
        # action_date could be date object or string? duckdb fetchall returns datetime.date
        t_date = t_row[1].strftime("%Y-%m-%d") if hasattr(t_row[1], "strftime") else str(t_row[1])
        t_key = f"{t_tick}_{t_date}"
        if get_cached_market_cap(ca_conn, t_tick, t_date): continue
        if is_failure_recent(get_failure(ca_conn, "yahoo", t_key)): continue
        
        try:
            res = fetch_yahoo_data(t_tick, t_date)
            if res:
                res["ticker"] = t_tick
                res["date"] = t_date
                res["fetched_at"] = datetime.now()
                res["source_payload_hash"] = "na"
                res["status"] = "success"
                upsert_cached_market_cap(ca_conn, res)
        except Exception as e:
            upsert_failure(ca_conn, "yahoo", t_key, type(e).__name__, getattr(e, "status_code", 500), str(e), getattr(e, "retry_after", 0), 3)

    mc_rel = cl_conn.table("cache.cache_market_cap")
    joined_final = join_market_cap(joined_tickers, mc_rel)
    
    # Add final derived columns per Phase 2
    joined_final = joined_final.select("*, CURRENT_TIMESTAMP AS last_verified_date, NULL AS theme_llm")
    
    logger.info("Persisting enriched_awards table")
    persist_table(cl_conn, joined_final, "enriched_awards")
    output_count = cl_conn.table("enriched_awards").aggregate("count(*)").fetchone()[0]
    logger.info(f"Output row count for enriched_awards: {output_count}")
    
    logger.info("Phase 2 Enrichment completed successfully")

    ca_conn.close()
    cl_conn.close()

if __name__ == "__main__":
    main()
