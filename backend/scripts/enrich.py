import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path
import pandas as pd

from backend.src.io import (
    get_cleaned_conn,
    get_cache_conn,
    ensure_cache_tables,
    get_cached_entity_hierarchy,
    upsert_cached_entity_hierarchy,
    get_cached_openfigi_ticker,
    upsert_cached_openfigi_ticker,
    get_cached_market_cap,
    upsert_cached_market_cap,
    get_failure,
    upsert_failure,
)
from backend.src.transform import (
    extract_unique_cage_code,
    join_entity_hierarchy,
    join_openfigi,
    join_market_cap,
)

from backend.app.services.providers.cage_scraper import (
    enrich_cage_data,
    CAGE_HEADERS,
    CAGE_COOKIES,
    CageAuthExpiredError,
    _validate_credentials as validate_cage_credentials,
)
from backend.app.services.providers.openfigi import process_owner_name
from backend.app.services.providers.yahoo import fetch_yahoo_data


# Sentinel exit code signalling to ``run_pipeline.sh`` that the CAGE
# session has expired and the interactive cookie-refresh prompt should
# fire. Chosen as 42 because it's memorable, not reserved by POSIX
# ``sysexits.h``, and documented here as the one place where
# ``sys.exit(42)`` is permitted.
CAGE_AUTH_EXIT_CODE: int = 42


def setup_logging(debug=False):
    log_dir = Path("backend/data/logs")
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / "enrichment.log"
    level = logging.DEBUG if debug else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.FileHandler(log_file), logging.StreamHandler()],
    )
    if debug:
        # Prevent urllib3 from flooding debug logs
        logging.getLogger("urllib3").setLevel(logging.INFO)
    return logging.getLogger(__name__)


def is_failure_recent(failure: dict) -> bool:
    if not failure:
        return False
    retry_after = failure.get("retry_after_seconds", 3600)
    # Default backoff is 10 seconds if no retry-after was provided
    if retry_after == 0:
        retry_after = 10  # Default backoff is 10s if no retry-after was provided
    last_attempt = failure.get("last_attempt_at")
    if not last_attempt:
        return False

    delta = (datetime.now() - last_attempt).total_seconds()
    return delta < retry_after


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    args = parser.parse_args()

    logger = setup_logging(debug=args.debug)
    logger.info("Starting Phase 2 Enrichment")

    # Fail fast on missing/expired CAGE scraper credentials before opening any
    # DB connections or issuing a single request. Without this, the first pass
    # would silently log failures for every CAGE code. We exit with the
    # ``CAGE_AUTH_EXIT_CODE`` sentinel so ``run_pipeline.sh`` can enter the
    # interactive cookie-refresh flow rather than treat this as a hard abort.
    try:
        validate_cage_credentials()
    except CageAuthExpiredError as auth_err:
        logger.error(f"[!] CAGE authentication failure at startup: {auth_err}")
        logger.error(
            "    Exiting with code "
            f"{CAGE_AUTH_EXIT_CODE}; orchestrator should prompt for fresh cookies."
        )
        sys.exit(CAGE_AUTH_EXIT_CODE)

    cl_conn = get_cleaned_conn()
    cl_conn.execute("ATTACH 'backend/data/cache/cache.duckdb' AS cache;")
    ensure_cache_tables(cl_conn)

    raw_rel = cl_conn.table("raw_filtered_awards")

    # 1. Resolve Hierarchy
    cages = extract_unique_cage_code(raw_rel).fetchall()
    cage_list = [row[0] for row in cages if row[0]]
    logger.info(f"Found {len(cage_list)} unique CAGE codes to process")

    for cage in cage_list:
        if get_cached_entity_hierarchy(cl_conn, cage):
            logger.debug(f"Cache hit for CAGE: {cage}")
            continue
        if is_failure_recent(get_failure(cl_conn, "cage", cage)):
            logger.debug(f"Recent failure for CAGE: {cage}, skipping")
            continue
        try:
            logger.info(f"Fetching CAGE data from API for: {cage}")
            res = enrich_cage_data(cage, CAGE_HEADERS, CAGE_COOKIES)
            if res:
                res["cage_code"] = cage
                res["result_status"] = "success"
                res["last_verified"] = datetime.now()
                upsert_cached_entity_hierarchy(cl_conn, res)
            else:
                res = {
                    "cage_code": cage,
                    "cage_business_name": None,
                    "cage_update_date": None,
                    "is_highest": None,
                    "immediate_level_owner": None,
                    "highest_level_owner_name": None,
                    "highest_level_cage_code": None,
                    "highest_level_cage_update_date": None,
                    "result_status": "not_found",
                    "last_verified": datetime.now(),
                }
                upsert_cached_entity_hierarchy(cl_conn, res)
        except CageAuthExpiredError as auth_err:
            # Mid-run cookie expiry: cache is preserved, but we can't keep
            # scraping. Exit with the sentinel so the orchestrator prompts
            # for fresh cookies. DO NOT write this to cache_failures --
            # it's not a per-CAGE failure, it's a global auth issue.
            logger.error(
                f"[!] CAGE session expired mid-run while processing {cage}: {auth_err}"
            )
            logger.error(
                "    Exiting with code "
                f"{CAGE_AUTH_EXIT_CODE}; cache is preserved, orchestrator will prompt for refresh."
            )
            try:
                cl_conn.close()
            except Exception:
                pass
            sys.exit(CAGE_AUTH_EXIT_CODE)
        except Exception as e:
            upsert_failure(
                cl_conn,
                "cage",
                cage,
                type(e).__name__,
                getattr(e, "status_code", 500),
                str(e),
                getattr(e, "retry_after", 0),
                5,
            )

    # Prepare relation for next step
    # Cache is already attached at the top of the script
    hierarchy_rel = cl_conn.table("cache.cache_entity_hierarchy")
    joined_hierarchy = join_entity_hierarchy(raw_rel, hierarchy_rel)

    # 2. Resolve OpenFIGI (get unique owner names)
    owners = (
        joined_hierarchy.aggregate("highest_level_owner_name")
        .filter("highest_level_owner_name IS NOT NULL")
        .fetchall()
    )
    owner_list = [row[0] for row in owners if row[0]]
    logger.info(f"Found {len(owner_list)} unique owner names for OpenFIGI")

    for owner in owner_list:
        if get_cached_openfigi_ticker(cl_conn, owner):
            logger.debug(f"Cache hit for OpenFIGI owner: {owner}")
            continue
        if is_failure_recent(get_failure(cl_conn, "openfigi", owner)):
            logger.debug(f"Recent failure for OpenFIGI owner: {owner}, skipping")
            continue
        try:
            logger.info(f"Fetching OpenFIGI data from API for owner: {owner}")
            res = process_owner_name(owner)
            if res:
                res["highest_level_owner_name"] = owner
                res["fetched_at"] = datetime.now()
                res["source_payload_hash"] = "na"
                res["status"] = "success"
                upsert_cached_openfigi_ticker(cl_conn, res)
            else:
                # Cache the "no match found" outcome so we don't re-query the
                # API for this owner on every subsequent run. The vast majority
                # of government contractors are private and will never match.
                upsert_cached_openfigi_ticker(
                    cl_conn,
                    {
                        "highest_level_owner_name": owner,
                        "ticker": None,
                        "exchange": None,
                        "security_type": None,
                        "fetched_at": datetime.now(),
                        "source_payload_hash": "na",
                        "status": "not_found",
                    },
                )
        except Exception as e:
            upsert_failure(
                cl_conn,
                "openfigi",
                owner,
                type(e).__name__,
                getattr(e, "status_code", 500),
                str(e),
                getattr(e, "retry_after", 0),
                5,
            )
    # Native DuckDB transactions handle standard inserts cleanly without manual commit blocks

    # Removed buggy checkpoint logic

    ticker_rel = cl_conn.table("cache.cache_openfigi_ticker")
    joined_tickers = join_openfigi(joined_hierarchy, ticker_rel)
    joined_tickers.create_view("joined_tickers", replace=True)

    # Compute derived logic directly
    joined_tickers = joined_tickers.select(
        "*, "
        "ticker IS NOT NULL as is_public, "
        "CASE "
        "WHEN number_of_offers_received = 1 THEN TRUE "
        "WHEN number_of_offers_received > 1 THEN FALSE "
        "ELSE NULL END AS sole_source_flag"
    )

    joined_tickers.create_view("joined_tickers_derived", replace=True)

    # 3. Resolve Market Cap
    ticker_pairs = (
        joined_tickers.aggregate("ticker, action_date")
        .filter("ticker IS NOT NULL AND action_date IS NOT NULL")
        .fetchall()
    )
    logger.info(f"Found {len(ticker_pairs)} unique ticker-date pairs for Yahoo Finance")

    for t_row in ticker_pairs:
        t_tick = t_row[0]
        # action_date could be date object or string? duckdb fetchall returns datetime.date
        t_date = (
            t_row[1].strftime("%Y-%m-%d")
            if hasattr(t_row[1], "strftime")
            else str(t_row[1])
        )
        t_key = f"{t_tick}_{t_date}"
        if get_cached_market_cap(cl_conn, t_tick, t_date):
            logger.debug(f"Cache hit for Yahoo ticker: {t_tick} on {t_date}")
            continue
        if is_failure_recent(get_failure(cl_conn, "yahoo", t_key)):
            logger.debug(
                f"Recent failure for Yahoo ticker: {t_tick} on {t_date}, skipping"
            )
            continue

        try:
            logger.info(
                f"Fetching Yahoo data from API for ticker: {t_tick} on {t_date}"
            )
            res = fetch_yahoo_data(t_tick, t_date)
            if res:
                res["ticker"] = t_tick
                res["date"] = t_date
                res["fetched_at"] = datetime.now()
                res["source_payload_hash"] = "na"
                res["status"] = "success"
                upsert_cached_market_cap(cl_conn, res)
        except Exception as e:
            upsert_failure(
                cl_conn,
                "yahoo",
                t_key,
                type(e).__name__,
                getattr(e, "status_code", 500),
                str(e),
                getattr(e, "retry_after", 0),
                3,
            )
    # Native DuckDB transactions handle standard inserts cleanly without manual commit blocks

    # Removed buggy checkpoint logic

    # Persist the joined_tickers relationship to the main db so we can retrieve it safely
    cl_conn.execute(
        "CREATE OR REPLACE TABLE temp_joined_tickers AS SELECT * FROM joined_tickers_derived"
    )

    # FATAL BUG WORKAROUND: DuckDB 1.1.0 crashes Python natively with "PendingQueryResult"
    # when attempting to convert ATTACHED mutated databases into Pandas dataframes.
    # We must cleanly close the connection to force the WAL flush to disk, then reopen it read-only!

    logger.info("Phase 2 Enrichment extraction completed successfully")

    if cl_conn:
        cl_conn.close()

    # --- Finalization Merge ---
    import pandas as pd

    logger.info(
        "Starting Phase 2 Finalization Merge (Workaround for DuckDB Python BinderException)"
    )
    cl_conn = get_cleaned_conn()
    cl_conn.execute("ATTACH 'backend/data/cache/cache.duckdb' AS cache (READ_ONLY)")

    df_tickers = cl_conn.execute("SELECT * FROM temp_joined_tickers").df()
    df_mc = cl_conn.execute("SELECT * FROM cache.cache_market_cap").df()

    df_mc = df_mc.rename(columns={"date": "action_date"})
    # Include the point-in-time provenance columns introduced in M1.2 so
    # they flow through to enriched_awards / themed_awards / signals_awards.
    mc_cols = [
        "ticker",
        "action_date",
        "market_cap",
        "close_price",
        "shares_outstanding",
        "market_cap_quality",
        "sector",
        "industry",
    ]
    # Older caches may not yet have the new columns; fill them with NULLs
    # defensively so the merge doesn't fail on a stale cache.
    for col in ("close_price", "shares_outstanding", "market_cap_quality"):
        if col not in df_mc.columns:
            df_mc[col] = None
    df_final = pd.merge(
        df_tickers,
        df_mc[mc_cols],
        on=["ticker", "action_date"],
        how="left",
    )

    df_final["last_verified_date"] = pd.Timestamp.now()
    df_final["theme_llm"] = None

    # String columns that DuckDB must persist as VARCHAR regardless of
    # what pandas inferred. When every value in a column is NULL, pandas
    # types it as float64; ``conn.register()`` then lands it in DuckDB
    # as INTEGER/DOUBLE. Downstream Phase 4 SQL (``IN (...)``, pattern
    # matches, quality-flag composition) breaks on the wrong type.
    #
    # Fix (M1.5 P2-3): build the persisted table via an explicit
    # ``CREATE TABLE AS SELECT`` that wraps every known string column in
    # ``CAST(col AS VARCHAR)``. This pins the type at SQL time so
    # pandas' best guess at the dtype is irrelevant.
    _string_columns = {
        "contract_transaction_unique_key",
        "award_id_piid",
        "parent_award_id_piid",
        "transaction_type",
        "awarding_agency_name",
        "awarding_sub_agency_name",
        "cage_code",
        "recipient_parent_uei",
        "recipient_parent_name",
        "recipient_parent_name_raw",
        "product_or_service_code",
        "product_or_service_code_description",
        "naics_code",
        "naics_description",
        "transaction_description",
        "award_type",
        "cage_business_name",
        "highest_level_owner_name",
        "highest_level_cage_code",
        "ticker",
        "market_cap_quality",
        "sector",
        "industry",
        "theme_llm",
    }
    # As a belt-and-braces measure, still coerce the pandas dtype to
    # ``object`` so DuckDB's own type inference doesn't trip on a NaN-
    # only float64 column before the CAST can kick in.
    for col in _string_columns:
        if col in df_final.columns:
            df_final[col] = (
                df_final[col].astype("object").where(df_final[col].notna(), None)
            )

    cl_conn.register("temp_final_df", df_final)

    # Build a CAST-wrapped projection: string columns -> VARCHAR, all
    # others -> pass-through. DuckDB identifiers are quoted with
    # double-quotes; our column names are clean ASCII but we quote them
    # anyway for safety.
    def _quote(ident: str) -> str:
        return '"' + ident.replace('"', '""') + '"'

    projections = []
    for col in df_final.columns:
        qcol = _quote(col)
        if col in _string_columns:
            projections.append(f"CAST({qcol} AS VARCHAR) AS {qcol}")
        else:
            projections.append(qcol)
    projection_sql = ", ".join(projections)

    logger.info("Persisting enriched_awards table (typed projection)")
    cl_conn.execute(
        f"CREATE OR REPLACE TABLE enriched_awards AS "
        f"SELECT {projection_sql} FROM temp_final_df"
    )
    output_count = cl_conn.table("enriched_awards").aggregate("count(*)").fetchone()[0]
    logger.info(f"Output row count for enriched_awards: {output_count}")

    cl_conn.execute("DROP TABLE IF EXISTS temp_joined_tickers")

    logger.info("Phase 2 Enrichment finalization completed successfully")
    cl_conn.close()


if __name__ == "__main__":
    main()
