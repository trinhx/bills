import logging
from pathlib import Path
from backend.src.io import get_cleaned_conn, persist_table
from backend.src.transform import normalize_naics, normalize_naics_keywords, normalize_psc, derive_deliverable

def setup_logging():
    log_dir = Path("backend/data/logs")
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / "themes.log"
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s",
                        handlers=[logging.FileHandler(log_file), logging.StreamHandler()])
    return logging.getLogger(__name__)

def main():
    logger = setup_logging()
    logger.info("Starting Phase 3 Theme Intelligence")
    
    # 1. Pre-flight Check
    naics_path = Path("backend/data/raw/lookups/2022-NAICS-Description-Table.csv")
    naics_kw_path = Path("backend/data/raw/lookups/2022-NAICS-Keywords.csv")
    psc_path = Path("backend/data/raw/lookups/Simplified_PSC_Lookup.csv")
    
    if not naics_path.exists():
        raise FileNotFoundError(f"Missing required NAICS lookup file at: {naics_path}")
    if not naics_kw_path.exists():
        raise FileNotFoundError(f"Missing required NAICS keywords lookup file at: {naics_kw_path}")
    if not psc_path.exists():
        raise FileNotFoundError(f"Missing required PSC lookup file at: {psc_path}")

    cl_conn = get_cleaned_conn()
    
    logger.info("Loading enriched awards and lookups")
    # 2. IO Read
    enriched_rel = cl_conn.table("enriched_awards")
    naics_rel = cl_conn.read_csv(str(naics_path))
    naics_kw_rel = cl_conn.read_csv(str(naics_kw_path), header=True)
    psc_rel = cl_conn.read_csv(str(psc_path))
    
    # 3. Transform
    logger.info("Applying NAICS normalization")
    themed_stage_1 = normalize_naics(enriched_rel, naics_rel)
    
    logger.info("Applying NAICS keywords normalization")
    themed_stage_2 = normalize_naics_keywords(themed_stage_1, naics_kw_rel)
    
    logger.info("Applying PSC normalization")
    themed_stage_3 = normalize_psc(themed_stage_2, psc_rel)
    
    logger.info("Deriving deliverables")
    themed_final = derive_deliverable(themed_stage_3)
    
    # 4. IO Write
    logger.info("Persisting themed_awards table")
    persist_table(cl_conn, themed_final, "themed_awards")
    output_count = cl_conn.table("themed_awards").aggregate("count(*)").fetchone()[0]
    
    logger.info(f"Phase 3 completed successfully. Output row count for themed_awards: {output_count}")
    cl_conn.close()

if __name__ == "__main__":
    main()
