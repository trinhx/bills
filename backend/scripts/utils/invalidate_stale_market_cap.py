"""
Invalidate stale ``cache_market_cap`` rows that pre-date Milestone 1.

The M1.1 rewrite of the Yahoo provider added point-in-time provenance
columns (``close_price``, ``shares_outstanding``, ``market_cap_quality``).
Cache rows written before that change have these columns populated as
NULL even though the ``market_cap`` value is present — and that
``market_cap`` was computed with the old, look-ahead-biased formula
(historical close * *current* shares outstanding).

The stale-detection predicate is:

    close_price IS NULL AND market_cap IS NOT NULL

This distinguishes **pre-M1 biased rows** (market_cap present, no
provenance) from **legitimate M1 'no_close' rows** (everything NULL;
yfinance genuinely had no bars for that ticker-date, e.g. IPOs before
their listing date, ADRs without Yahoo coverage). Deleting the latter
would waste API budget on the next run for zero information gain.

Any re-run of the pipeline against a pre-M1 cache will silently re-use
biased market caps because the cache-hit path short-circuits the API
call. This utility deletes every such row so the next enrichment pass
refetches with the corrected provider.

Selective: only deletes the pre-M1 bias pattern. Safe and idempotent.

Usage (from repo root):
    uv run --env-file .env backend/scripts/utils/invalidate_stale_market_cap.py

Integrated with the orchestrator via:
    ./run_pipeline.sh --refresh_stale_market_cap --source_dataset ...
"""
import argparse
import logging
from pathlib import Path

import duckdb


# Stale pre-M1 rows are the ones with a populated market_cap but no
# provenance columns. Rows where everything is NULL are legitimate M1-era
# "no_close" outcomes and MUST NOT be deleted.
_STALE_PREDICATE = "close_price IS NULL AND market_cap IS NOT NULL"


def setup_logging() -> logging.Logger:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    return logging.getLogger(__name__)


def invalidate(cache_db_path: str, dry_run: bool = False) -> int:
    """
    Delete stale pre-M1 rows from ``cache_market_cap``. Returns the number
    of rows deleted (or would-be-deleted if ``dry_run``).
    """
    conn = duckdb.connect(cache_db_path, read_only=dry_run)
    try:
        stale_count = conn.execute(
            f"SELECT COUNT(*) FROM cache_market_cap WHERE {_STALE_PREDICATE}"
        ).fetchone()[0]
        total = conn.execute("SELECT COUNT(*) FROM cache_market_cap").fetchone()[0]

        if dry_run:
            return stale_count

        if stale_count > 0:
            conn.execute(f"DELETE FROM cache_market_cap WHERE {_STALE_PREDICATE}")
        remaining = conn.execute("SELECT COUNT(*) FROM cache_market_cap").fetchone()[0]

        return stale_count, total, remaining
    finally:
        conn.close()


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

    logger.info(f"Target cache DB: {path}")
    logger.info(f"Stale-row predicate: {_STALE_PREDICATE}")
    if args.dry_run:
        n = invalidate(str(path), dry_run=True)
        logger.info(f"[DRY RUN] Would delete {n} pre-M1 biased rows.")
        return

    stale, total, remaining = invalidate(str(path), dry_run=False)
    logger.info(f"Deleted {stale} stale rows out of {total} total; {remaining} remain.")
    if stale == 0:
        logger.info("Cache was already clean — no M1 invalidation needed.")


if __name__ == "__main__":
    main()
