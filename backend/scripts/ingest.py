"""
Phase 1 — Ingestion.

Loads the raw USASpending contracts CSV(s), applies Phase 1 filters and
the full-history LAG that feeds ``ceiling_change`` / ``transaction_type``
classification, and persists the result to ``raw_filtered_awards``.

Supports three input modes (choose exactly one):

* ``--csv path/to/single.csv``  -- back-compat single-file path.
* ``--source-glob 'path/FY*.csv'`` -- DuckDB-native glob expansion. Any
  files matching the pattern are streamed in parallel as one relation.
* Passing multiple ``--csv`` flags is NOT supported; use ``--source-glob``.

Examples::

    uv run --env-file .env backend/scripts/ingest.py \\
        --csv backend/data/raw/contracts/FY2024_All_Contracts_Full_20260207_1.csv

    uv run --env-file .env backend/scripts/ingest.py \\
        --source-glob 'backend/data/raw/contracts/FY2024_All_Contracts_Full_20260207_*.csv'
"""
from __future__ import annotations

import argparse
import glob as _glob
import logging
from pathlib import Path
from typing import List

from backend.src.io import (
    get_cleaned_conn,
    persist_table,
    scan_contracts_csv,
)
from backend.src.transform import filter_and_select_phase1


def setup_logging():
    log_dir = Path("backend/data/logs")
    log_dir.mkdir(parents=True, exist_ok=True)

    log_file = log_dir / "pipeline.log"

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.FileHandler(log_file), logging.StreamHandler()],
    )
    return logging.getLogger(__name__)


def _resolve_inputs(
    single_csv: str | None, source_glob: str | None, logger: logging.Logger
) -> List[str]:
    """
    Turn the CLI flags into a sorted list of absolute CSV paths. Fails
    loudly (raises ``SystemExit``) if nothing resolved.

    Only one of ``single_csv`` or ``source_glob`` should be set; the
    argparse wiring enforces mutual exclusivity.
    """
    if single_csv:
        p = Path(single_csv).resolve()
        if not p.exists():
            logger.error(f"Input CSV not found: {p}")
            raise SystemExit(1)
        return [str(p)]

    if not source_glob:
        logger.error("No input specified; use --csv or --source-glob.")
        raise SystemExit(1)

    # Expand the glob. Sort for determinism (piid ordering within a file
    # is preserved; across-file ordering is set by the window function's
    # ORDER BY, so the glob's resolution order doesn't affect correctness
    # -- but a deterministic log message helps debugging).
    matches = sorted(_glob.glob(source_glob))
    if not matches:
        logger.error(f"--source-glob matched no files: {source_glob!r}")
        raise SystemExit(1)

    logger.info(f"--source-glob resolved to {len(matches)} file(s):")
    for m in matches:
        logger.info(f"  - {m}")
    return matches


def main():
    parser = argparse.ArgumentParser(description="Phase 1: Ingest Contracts CSV")
    input_group = parser.add_mutually_exclusive_group(required=True)
    input_group.add_argument(
        "--csv",
        help="Path to a single raw contracts CSV.",
    )
    input_group.add_argument(
        "--source-glob",
        help=(
            "Glob pattern matching multiple CSVs (e.g. "
            "'data/FY2024_All_Contracts_Full_*.csv'). DuckDB streams them "
            "as a single relation so window functions see the full history."
        ),
    )
    args = parser.parse_args()

    logger = setup_logging()
    logger.info("Starting Phase 1 Ingestion")

    inputs = _resolve_inputs(args.csv, args.source_glob, logger)

    conn = get_cleaned_conn()

    try:
        if len(inputs) == 1:
            logger.info(f"Scanning CSV lazily: {inputs[0]}")
            raw_rel = scan_contracts_csv(conn, inputs[0])
            input_file_label = inputs[0]
        else:
            logger.info(f"Scanning {len(inputs)} CSVs lazily (DuckDB multi-file)")
            raw_rel = scan_contracts_csv(conn, inputs)
            # For the ingestion_profile table we record a readable summary
            # rather than a 7-way concatenated path.
            input_file_label = f"<{len(inputs)} files> {inputs[0]} ... {inputs[-1]}"

        input_count_rel = raw_rel.aggregate("count(*)")
        input_count = input_count_rel.fetchone()[0]
        logger.info(f"Input row count: {input_count}")

        logger.info("Applying Phase 1 transformations")
        filtered_rel = filter_and_select_phase1(raw_rel)

        logger.info("Persisting raw_filtered_awards table")
        persist_table(conn, filtered_rel, "raw_filtered_awards")

        final_table = conn.table("raw_filtered_awards")
        output_count = final_table.aggregate("count(*)").fetchone()[0]

        sum_obligation_row = final_table.aggregate(
            "sum(federal_action_obligation)"
        ).fetchone()
        total_obligation = (
            sum_obligation_row[0]
            if sum_obligation_row and sum_obligation_row[0] is not None
            else 0.0
        )

        logger.info(f"Output row count: {output_count}")
        logger.info(f"Total federal action obligation: ${total_obligation:,.2f}")

        # Parameterise values rather than interpolating them into the SQL
        # string -- csv_path is user-supplied and a literal with a single
        # quote would otherwise break the query (or in adversarial
        # contexts, inject).
        conn.execute(
            """
            CREATE OR REPLACE TABLE ingestion_profile AS
            SELECT
                CURRENT_TIMESTAMP AS run_time,
                ? AS input_file,
                ? AS input_count,
                ? AS output_count,
                ? AS total_obligation
            """,
            [input_file_label, input_count, output_count, total_obligation],
        )
        logger.info("Persisted ingestion_profile")

        logger.info("Phase 1 Ingestion completed successfully")

    except Exception:
        logger.exception("Error during ingestion phase")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
