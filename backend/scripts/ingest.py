import argparse
import logging
from pathlib import Path

from backend.src.io import get_cleaned_conn, scan_contracts_csv, persist_table, write_profile
from backend.src.transform import filter_and_select_phase1

def setup_logging():
    log_dir = Path("backend/data/logs")
    log_dir.mkdir(parents=True, exist_ok=True)
    
    log_file = log_dir / "pipeline.log"
    
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

def main():
    parser = argparse.ArgumentParser(description="Phase 1: Ingest Contracts CSV")
    parser.add_argument("--csv", required=True, help="Path to raw contracts CSV")
    args = parser.parse_args()

    logger = setup_logging()
    logger.info("Starting Phase 1 Ingestion")
    
    csv_path = args.csv
    if not Path(csv_path).exists():
        logger.error(f"Input CSV not found: {csv_path}")
        return

    conn = get_cleaned_conn()
    
    try:
        logger.info(f"Scanning CSV lazily: {csv_path}")
        raw_rel = scan_contracts_csv(conn, csv_path)
        
        input_count_rel = raw_rel.aggregate("count(*)")
        input_count = input_count_rel.fetchone()[0]
        logger.info(f"Input row count: {input_count}")
        
        logger.info("Applying Phase 1 transformations")
        filtered_rel = filter_and_select_phase1(raw_rel)
        
        logger.info("Persisting raw_filtered_awards table")
        persist_table(conn, filtered_rel, "raw_filtered_awards")
        
        final_table = conn.table("raw_filtered_awards")
        output_count = final_table.aggregate("count(*)").fetchone()[0]
        
        sum_obligation_row = final_table.aggregate("sum(federal_action_obligation)").fetchone()
        total_obligation = sum_obligation_row[0] if sum_obligation_row and sum_obligation_row[0] is not None else 0.0
        
        logger.info(f"Output row count: {output_count}")
        logger.info(f"Total federal action obligation: ${total_obligation:,.2f}")
        
        profile_sql = f"""
            SELECT 
                CURRENT_TIMESTAMP as run_time,
                '{csv_path}' as input_file,
                {input_count} as input_count,
                {output_count} as output_count,
                {total_obligation} as total_obligation
        """
        profile_rel = conn.sql(profile_sql)
        write_profile(conn, profile_rel)
        logger.info("Persisted ingestion_profile")
        
        logger.info("Phase 1 Ingestion completed successfully")

    except Exception as e:
        logger.exception("Error during ingestion phase")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
