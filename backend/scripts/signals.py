import logging
import argparse
from pathlib import Path
from backend.app.core.version import PIPELINE_VERSION
from backend.src.io import get_cleaned_conn, persist_table, export_to_csv
from backend.src.transform import calculate_alpha_signals, stamp_pipeline_metadata


def setup_logging():
    log_dir = Path("backend/data/logs")
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / "signals.log"
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.FileHandler(log_file), logging.StreamHandler()],
    )
    return logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description="Phase 4: Signal Generation")
    parser.add_argument(
        "--output",
        default="backend/data/results/final_signals.csv",
        help="Path to output CSV",
    )
    args = parser.parse_args()

    logger = setup_logging()
    logger.info(
        f"Starting Phase 4 Signal Generation (pipeline_version={PIPELINE_VERSION})"
    )

    cl_conn = get_cleaned_conn()

    logger.info("Loading themed_awards table")
    themed_rel = cl_conn.table("themed_awards")

    logger.info("Applying Alpha Signal Mathematics")
    signals_rel = calculate_alpha_signals(themed_rel)

    logger.info("Stamping pipeline_version and ingested_at")
    signals_rel = stamp_pipeline_metadata(signals_rel)

    logger.info("Persisting signals_awards natively")
    persist_table(cl_conn, signals_rel, "signals_awards")
    output_count = cl_conn.table("signals_awards").aggregate("count(*)").fetchone()[0]
    logger.info(
        f"Phase 4 Signal Generation completed. Output row count: {output_count}"
    )

    export_path = args.output
    logger.info(f"Exporting to CSV natively via DuckDB: {export_path}")
    export_to_csv(cl_conn, "signals_awards", export_path)
    logger.info("Export Complete.")

    cl_conn.close()


if __name__ == "__main__":
    main()
