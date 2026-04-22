#!/usr/bin/env bash
# run_pipeline.sh — Runs the full USASpending quantitative analysis pipeline.
#
# Usage:
#   ./run_pipeline.sh (--source_dataset <path> | --source_glob '<pattern>') \
#       [--clear_cache] \
#       [--refresh_stale_market_cap] \
#       [--retry_cage_failures]
#
# Input (exactly one is required):
#   --source_dataset PATH        Path to a single raw contracts CSV
#   --source_glob PATTERN        Shell-style glob matching multiple CSVs. All
#                                matching files are ingested as one relation so
#                                Phase 1's LAG window function sees the full
#                                piid history across files.
#
# Options:
#   --clear_cache                Delete the enrichment cache before running
#   --refresh_stale_market_cap   Invalidate pre-M1 market_cap cache rows so they
#                                get re-fetched with point-in-time shares
#   --retry_cage_failures        Drop all cached CAGE failures so they're retried
#                                this run (useful after a cookie refresh)
#
# Interactive cookie refresh:
#   If the CAGE session expires mid-run (exit code 42 from enrich.py), the
#   script prints a prompt and reads a fresh curl command from stdin. After
#   updating .env, the enrichment loop resumes -- cached results are kept.
#
# Examples:
#   ./run_pipeline.sh --source_dataset backend/data/raw/contracts/FY2024.csv
#   ./run_pipeline.sh --source_glob 'backend/data/raw/contracts/FY2024_*.csv' \
#                     --refresh_stale_market_cap --retry_cage_failures

set -euo pipefail

# Sentinel exit code emitted by enrich.py when CAGE cookies have expired.
# Must match CAGE_AUTH_EXIT_CODE in backend/scripts/enrich.py.
CAGE_AUTH_EXIT_CODE=42

# ── Graceful exit on Ctrl+C ───────────────────────────────────────────────────
trap '[[ $? -ne 0 ]] && echo "" && echo "[pipeline] Interrupted. Cached API results are preserved." && echo "[pipeline] Re-run without --clear_cache to resume from where you left off."' EXIT

# ── Argument parsing ──────────────────────────────────────────────────────────
SOURCE_DATASET=""
SOURCE_GLOB=""
CLEAR_CACHE=false
REFRESH_STALE_MCAP=false
RETRY_CAGE_FAILURES=false

USAGE="Usage: $0 (--source_dataset <path> | --source_glob '<pattern>') [--clear_cache] [--refresh_stale_market_cap] [--retry_cage_failures]"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --source_dataset)
            SOURCE_DATASET="$2"
            shift 2
            ;;
        --source_glob)
            SOURCE_GLOB="$2"
            shift 2
            ;;
        --clear_cache)
            CLEAR_CACHE=true
            shift
            ;;
        --refresh_stale_market_cap)
            REFRESH_STALE_MCAP=true
            shift
            ;;
        --retry_cage_failures)
            RETRY_CAGE_FAILURES=true
            shift
            ;;
        *)
            echo "Unknown argument: $1"
            echo "$USAGE"
            exit 1
            ;;
    esac
done

# Exactly one input mode is required.
if [[ -z "$SOURCE_DATASET" && -z "$SOURCE_GLOB" ]]; then
    echo "Error: must supply --source_dataset OR --source_glob."
    echo "$USAGE"
    exit 1
fi
if [[ -n "$SOURCE_DATASET" && -n "$SOURCE_GLOB" ]]; then
    echo "Error: --source_dataset and --source_glob are mutually exclusive."
    echo "$USAGE"
    exit 1
fi

# ── Derive output path from input ────────────────────────────────────────────
if [[ -n "$SOURCE_DATASET" ]]; then
    # Single-file: honour the existing filename convention.
    if [[ ! -f "$SOURCE_DATASET" ]]; then
        echo "Error: Source file not found: $SOURCE_DATASET"
        exit 1
    fi
    BASENAME=$(basename "$SOURCE_DATASET" .csv)
    OUTPUT_CSV="backend/data/results/${BASENAME}_final_signals.csv"
    INGEST_FLAGS=(--csv "$SOURCE_DATASET")
else
    # Multi-file: resolve glob once in shell, bail if nothing matched,
    # and derive the output name from the longest common basename prefix.
    # We use Python to compute LCP robustly (shell string-prefix logic is
    # error-prone with trailing separators like "_1", "_2", ...).
    #
    # nullglob ensures the pattern expands to nothing (not the literal
    # glob string) when there are no matches.
    shopt -s nullglob
    # shellcheck disable=SC2206  # word-splitting is intentional here
    MATCHED_FILES=( $SOURCE_GLOB )
    shopt -u nullglob
    if [[ ${#MATCHED_FILES[@]} -eq 0 ]]; then
        echo "Error: --source_glob matched no files: $SOURCE_GLOB"
        exit 1
    fi
    echo "[pipeline] --source_glob matched ${#MATCHED_FILES[@]} file(s):"
    for f in "${MATCHED_FILES[@]}"; do
        echo "           $f"
    done
    # Compute the longest-common-prefix of basenames (minus .csv), strip
    # trailing non-alphanumeric runs (so "FY2024_..._20260207_" becomes
    # "FY2024_..._20260207"), append our suffix.
    #
    # Edge case: when the LCP stops mid-year (e.g. "FY202" when the inputs
    # split on digit 4 vs 5), the raw LCP is too short to be a useful
    # filename. We detect this and build a more descriptive combined name
    # by extracting each basename's year-prefix and joining them.
    OUTPUT_BASE=$(uv run --env-file .env python -c "
import os, re, sys
from os.path import basename, splitext
files = sys.argv[1:]
bases = [splitext(basename(f))[0] for f in files]
pref = os.path.commonprefix(bases).rstrip('_-.')
# If LCP ends inside a 'FY####' token (e.g. 'FY202'), derive a combined
# token like 'FY2024_FY2025' from each file's leading FY#### prefix.
m_trunc = re.fullmatch(r'FY\d{0,3}', pref)
if m_trunc:
    years = sorted({
        m.group(0)
        for b in bases
        for m in [re.match(r'FY\d{4}', b)]
        if m
    })
    if years:
        # e.g. 'FY2024_FY2025' or 'FY2024' if only one year matched.
        pref = '_'.join(years)
if not pref:
    pref = 'all_sources'
print(pref)
" "${MATCHED_FILES[@]}")
    OUTPUT_CSV="backend/data/results/${OUTPUT_BASE}_final_signals.csv"
    INGEST_FLAGS=(--source-glob "$SOURCE_GLOB")
fi

echo "[pipeline] Output will be written to: $OUTPUT_CSV"

# ── Optional: clear cache ──────────────────────────────────────────────────────
if [[ "$CLEAR_CACHE" == true ]]; then
    echo ""
    echo "  ⚠️  WARNING: --clear_cache is set."
    echo "  This will permanently delete all cached API results (CAGE, OpenFIGI, Yahoo)."
    echo "  The pipeline will NOT be able to resume from where it left off."
    echo "  It will restart Phase 2 Enrichment from scratch."
    echo ""
    read -r -p "  Are you sure you want to continue? [y/N] " confirm
    if [[ ! "$confirm" =~ ^[Yy]$ ]]; then
        echo "[pipeline] Aborted."
        exit 0
    fi
    echo "[pipeline] Clearing enrichment cache..."
    rm -f backend/data/cache/cache.duckdb \
          backend/data/cache/cache.duckdb.wal \
          backend/data/cache/cache.duckdb.wal.bak
    echo "[pipeline] Cache cleared."
fi

# ── Optional: selective invalidation of pre-M1 market_cap rows ───────────────
# Deletes cache_market_cap rows where close_price IS NULL (pre-M1 look-ahead-
# biased entries). Mutually compatible with --clear_cache: this runs AFTER
# --clear_cache so it has no effect there (no rows exist).
if [[ "$REFRESH_STALE_MCAP" == true ]]; then
    echo ""
    echo "[pipeline] Invalidating stale market_cap rows (pre-M1 look-ahead-biased)..."
    uv run --env-file .env backend/scripts/utils/invalidate_stale_market_cap.py
fi

# ── Optional: retry stuck CAGE failures ──────────────────────────────────────
# Drops all cached CAGE failure entries so this run re-attempts them. Useful
# after rotating CAGE cookies -- otherwise the backoff window keeps them
# suppressed and the fresh session never gets exercised against them.
if [[ "$RETRY_CAGE_FAILURES" == true ]]; then
    echo ""
    echo "[pipeline] Dropping cached CAGE failures so this run retries them..."
    uv run --env-file .env backend/scripts/utils/invalidate_stale_cage_failures.py --execute
fi

# ── Run pipeline ──────────────────────────────────────────────────────────────
export PYTHONPATH=.
BACKOFF_SECONDS=10  # 10 seconds — user specified

echo "[pipeline] Starting Phase 1: Ingestion"
if [[ -n "$SOURCE_DATASET" ]]; then
    echo "[pipeline] Source: $SOURCE_DATASET"
else
    echo "[pipeline] Source glob: $SOURCE_GLOB"
fi
uv run --env-file .env backend/scripts/ingest.py "${INGEST_FLAGS[@]}"

# ── Interactive CAGE cookie refresh ──────────────────────────────────────────
# Called when enrich.py exits with CAGE_AUTH_EXIT_CODE (42). Prompts the user
# to paste a fresh curl command; updates .env in place; returns 0 on success.
refresh_cage_cookies() {
    echo ""
    echo "  ┌──────────────────────────────────────────────────────────────┐"
    echo "  │  [!] CAGE session expired mid-run. Cache is preserved.       │"
    echo "  │                                                              │"
    echo "  │  To resume, paste a fresh curl command:                      │"
    echo "  │    1. Open https://cage.dla.mil/search in your browser       │"
    echo "  │    2. If prompted, click 'I agree' on the terms page         │"
    echo "  │    3. DevTools → Network → right-click any request           │"
    echo "  │       → Copy → Copy as cURL                                  │"
    echo "  │    4. Paste below; press Ctrl+D when done (Ctrl+C to abort)  │"
    echo "  └──────────────────────────────────────────────────────────────┘"
    echo ""

    local tmp_curl
    tmp_curl=$(mktemp)
    trap 'rm -f "$tmp_curl"' RETURN

    # Read until EOF; Ctrl+D sends EOF.
    cat > "$tmp_curl"

    if [[ ! -s "$tmp_curl" ]]; then
        echo "[pipeline] No curl command received. Aborting."
        rm -f "$tmp_curl"
        return 1
    fi

    # Run the updater; it backs up .env, writes new values, and live-validates.
    if ! uv run --env-file .env backend/scripts/utils/update_cage_cookies.py \
            --curl-file "$tmp_curl"; then
        echo "[pipeline] Cookie refresh failed. Aborting."
        rm -f "$tmp_curl"
        return 1
    fi

    rm -f "$tmp_curl"
    echo "[pipeline] ✓ CAGE cookies updated. Resuming enrichment..."
    return 0
}

# ── Phase 2: Enrichment loop ──────────────────────────────────────────────────
# Retries in a loop until no provider failures remain within their backoff
# window. CAGE auth-expiry (exit code 42) triggers the interactive cookie
# refresh and the loop resumes from the same pass.
ENRICH_PASS=1
while true; do
    echo ""
    echo "[pipeline] Starting Phase 2: Enrichment (pass $ENRICH_PASS)"

    # Allow a controlled non-zero exit so we can detect CAGE_AUTH_EXIT_CODE.
    set +e
    uv run --env-file .env backend/scripts/enrich.py
    ENRICH_RC=$?
    set -e

    if [[ "$ENRICH_RC" -eq "$CAGE_AUTH_EXIT_CODE" ]]; then
        if refresh_cage_cookies; then
            # Don't increment pass counter: same logical pass, fresh session.
            continue
        else
            echo "[pipeline] Aborted: could not refresh CAGE cookies."
            exit 1
        fi
    elif [[ "$ENRICH_RC" -ne 0 ]]; then
        echo "[pipeline] enrich.py exited with code $ENRICH_RC. Aborting."
        exit "$ENRICH_RC"
    fi

    # Check for any failures still within their backoff window on either
    # provider. CAGE failures are included so a failing-then-fixed CAGE
    # scrape doesn't cause the loop to exit prematurely.
    PENDING=$(uv run --env-file .env python -c "
import duckdb
try:
    conn = duckdb.connect('backend/data/cache/cache.duckdb')
    result = conn.execute('''
        SELECT COUNT(*) FROM cache_failures
        WHERE provider IN ('openfigi', 'cage')
          AND last_attempt_at + INTERVAL (COALESCE(NULLIF(retry_after_seconds, 0), 10)) SECOND > now()
    ''').fetchone()
    conn.close()
    print(result[0])
except Exception:
    print(0)
" 2>/dev/null)

    if [[ "$PENDING" -eq 0 ]]; then
        echo "[pipeline] ✓ Phase 2 complete — no pending failures remain."
        break
    fi

    echo "[pipeline] $PENDING failure(s) still within backoff window."
    echo "[pipeline] Waiting ${BACKOFF_SECONDS} seconds before retrying..."
    for i in $(seq $BACKOFF_SECONDS -1 1); do
        printf "\r[pipeline] Next retry in %dm %ds ...  " $((i / 60)) $((i % 60))
        sleep 1
    done
    echo ""
    ENRICH_PASS=$((ENRICH_PASS + 1))
done

echo "[pipeline] Starting Phase 3: Theme Intelligence"
uv run --env-file .env backend/scripts/themes.py

echo "[pipeline] Starting Phase 4: Signal Generation"
echo "[pipeline] Output: $OUTPUT_CSV"
uv run --env-file .env backend/scripts/signals.py --output "$OUTPUT_CSV"

echo ""
echo "[pipeline] ✓ Pipeline complete."
echo "[pipeline] Results: $OUTPUT_CSV"
