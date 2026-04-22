"""
One-time cleanup for stale OpenFIGI and Yahoo cache entries made
obsolete by the M1.5 P1-4 and P1-5 fixes.

Specifically:

* **Yahoo 500 failures** for tickers containing ``/`` — these were stuck
  because yfinance can't handle the OpenFIGI-native separator. With
  P1-4 the provider now translates to hyphen form before calling
  yfinance. Drop the stuck failure entries so the next enrichment pass
  re-attempts (and succeeds).

* **OpenFIGI cached matches** whose ticker is a Bloomberg internal ID
  (pattern ``^\\d{4,}[A-Z]?$``) — these were persisted by the pre-P1-5
  selection logic. Drop them so the next enrichment pass re-queries
  OpenFIGI (which now rejects Bloomberg IDs) and likely caches as
  ``not_found`` instead.

This utility is safe to run multiple times; it's a no-op if the
offending rows are already gone.

Usage:
    uv run --env-file .env backend/scripts/utils/invalidate_stale_resolutions.py
    uv run --env-file .env backend/scripts/utils/invalidate_stale_resolutions.py --dry-run
"""

import argparse
import logging
import re
from pathlib import Path

import duckdb


_BLOOMBERG_ID_RE = r"^[0-9]{4,}[A-Z]?$"


def setup_logging() -> logging.Logger:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    return logging.getLogger(__name__)


def _report_counts(conn: duckdb.DuckDBPyConnection, logger: logging.Logger) -> None:
    yahoo_slash = conn.execute(
        "SELECT COUNT(*) FROM cache_failures WHERE provider = 'yahoo' AND key LIKE '%/%'"
    ).fetchone()[0]
    bbg_ids = conn.execute(
        f"SELECT COUNT(*) FROM cache_openfigi_ticker "
        f"WHERE ticker IS NOT NULL AND regexp_matches(ticker, '{_BLOOMBERG_ID_RE}')"
    ).fetchone()[0]
    logger.info(f"Candidates for invalidation:")
    logger.info(f"  yahoo failures with '/' in key:  {yahoo_slash}")
    logger.info(f"  openfigi Bloomberg-ID tickers:   {bbg_ids}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--cache-db",
        default="backend/data/cache/cache.duckdb",
        help="Path to the cache DuckDB file.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report what would be deleted without modifying the cache.",
    )
    args = parser.parse_args()
    logger = setup_logging()

    path = Path(args.cache_db)
    if not path.exists():
        logger.error(f"Cache DB not found: {path}")
        return

    conn = duckdb.connect(str(path), read_only=args.dry_run)
    try:
        logger.info(f"Target cache DB: {path}")
        _report_counts(conn, logger)

        if args.dry_run:
            logger.info("[DRY RUN] No modifications performed.")
            return

        # Delete Yahoo failures for slash-tickers.
        deleted_yahoo = conn.execute(
            "DELETE FROM cache_failures WHERE provider = 'yahoo' AND key LIKE '%/%'"
        ).fetchone()
        # Delete OpenFIGI rows whose ticker matches the Bloomberg-ID pattern.
        # Note: regexp_matches on NULL returns NULL, filtered out by WHERE.
        deleted_bbg = conn.execute(
            f"DELETE FROM cache_openfigi_ticker "
            f"WHERE ticker IS NOT NULL AND regexp_matches(ticker, '{_BLOOMBERG_ID_RE}')"
        ).fetchone()

        logger.info("Invalidation complete. Re-run the pipeline to re-resolve.")
        _report_counts(conn, logger)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
