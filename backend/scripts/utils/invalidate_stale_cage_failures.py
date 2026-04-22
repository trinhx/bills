"""
Drop cached CAGE failure entries so the next enrichment pass retries them.

Motivation
----------
``cache_failures`` stores provider-level failures with a ``retry_after_seconds``
backoff. When a CAGE session cookie expired mid-run in a previous pipeline
invocation, every pending CAGE request was logged here with an HTTP-500-style
entry. These stick around until the backoff elapses -- often hours -- and
silently suppress retries on the next run. After rotating cookies the
operator usually wants those same CAGE codes to be retried *immediately*,
not at the end of some arbitrary backoff window.

This utility deletes every ``provider = 'cage'`` row from ``cache_failures``.
It does NOT touch successful CAGE entries in ``cache_entity_hierarchy``, and
it does NOT touch OpenFIGI / Yahoo failures.

Usage
-----
Dry-run (the default; reports count, doesn't delete)::

    uv run --env-file .env \\
        backend/scripts/utils/invalidate_stale_cage_failures.py

Actually delete::

    uv run --env-file .env \\
        backend/scripts/utils/invalidate_stale_cage_failures.py --execute

Integrated with ``run_pipeline.sh`` via the ``--retry_cage_failures`` flag,
which invokes this utility with ``--execute`` before Phase 2 starts.
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

import duckdb


def _setup_logging() -> logging.Logger:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    return logging.getLogger(__name__)


def count_cage_failures(conn: duckdb.DuckDBPyConnection) -> int:
    return conn.execute(
        "SELECT COUNT(*) FROM cache_failures WHERE provider = 'cage'"
    ).fetchone()[0]


def delete_cage_failures(conn: duckdb.DuckDBPyConnection) -> int:
    before = count_cage_failures(conn)
    conn.execute("DELETE FROM cache_failures WHERE provider = 'cage'")
    after = count_cage_failures(conn)
    return before - after


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--cache-db",
        default="backend/data/cache/cache.duckdb",
        help="Path to the cache DuckDB file (default: backend/data/cache/cache.duckdb)",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Actually delete. Without this, the utility is dry-run.",
    )
    args = parser.parse_args()
    logger = _setup_logging()

    path = Path(args.cache_db)
    if not path.exists():
        logger.error(f"Cache DB not found: {path}")
        return 2

    # Dry-run: open read-only. Execute: open read-write.
    conn = duckdb.connect(str(path), read_only=not args.execute)
    try:
        n = count_cage_failures(conn)
        logger.info(f"Found {n} cached CAGE failures.")
        if not args.execute:
            if n > 0:
                logger.info(
                    f"[DRY RUN] Would delete {n} rows (pass --execute to do it)."
                )
            return 0
        if n == 0:
            logger.info("Nothing to delete.")
            return 0
        deleted = delete_cage_failures(conn)
        logger.info(f"Deleted {deleted} CAGE failure rows; next run will retry them.")
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
