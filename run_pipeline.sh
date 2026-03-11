#!/usr/bin/env bash
# run_pipeline.sh — Runs the full USASpending quantitative analysis pipeline.
#
# Usage:
#   ./run_pipeline.sh --source_dataset <path_to_csv> [--clear_cache]
#
# Options:
#   --source_dataset  Path to the raw contracts CSV file (required)
#   --clear_cache     Delete the enrichment cache before running (optional)
#
# Example:
#   ./run_pipeline.sh --source_dataset backend/data/raw/contracts/FY2024_All_Contracts_Full_20260207_1.csv
#   ./run_pipeline.sh --source_dataset backend/data/raw/contracts/FY2024_All_Contracts_Full_20260207_1.csv --clear_cache

set -euo pipefail

# ── Graceful exit on Ctrl+C ───────────────────────────────────────────────────
trap '[[ $? -ne 0 ]] && echo "" && echo "[pipeline] Interrupted. Cached API results are preserved." && echo "[pipeline] Re-run without --clear_cache to resume from where you left off."' EXIT

# ── Argument parsing ──────────────────────────────────────────────────────────
SOURCE_DATASET=""
CLEAR_CACHE=false

while [[ $# -gt 0 ]]; do
    case "$1" in
        --source_dataset)
            SOURCE_DATASET="$2"
            shift 2
            ;;
        --clear_cache)
            CLEAR_CACHE=true
            shift
            ;;
        *)
            echo "Unknown argument: $1"
            echo "Usage: $0 --source_dataset <path_to_csv> [--clear_cache]"
            exit 1
            ;;
    esac
done

if [[ -z "$SOURCE_DATASET" ]]; then
    echo "Error: --source_dataset is required."
    echo "Usage: $0 --source_dataset <path_to_csv> [--clear_cache]"
    exit 1
fi

if [[ ! -f "$SOURCE_DATASET" ]]; then
    echo "Error: Source file not found: $SOURCE_DATASET"
    exit 1
fi

# ── Derive output path from input filename ────────────────────────────────────
BASENAME=$(basename "$SOURCE_DATASET" .csv)
OUTPUT_CSV="backend/data/results/${BASENAME}_final_signals.csv"

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

# ── Run pipeline ──────────────────────────────────────────────────────────────
export PYTHONPATH=.
BACKOFF_SECONDS=1800  # 30 minutes — matches the failure backoff in enrich.py

echo "[pipeline] Starting Phase 1: Ingestion"
echo "[pipeline] Source: $SOURCE_DATASET"
uv run --env-file .env backend/scripts/ingest.py --csv "$SOURCE_DATASET"

# ── Phase 2: Enrichment loop ──────────────────────────────────────────────────
# Retries every 30 minutes until all pending failures have been resolved.
ENRICH_PASS=1
while true; do
    echo ""
    echo "[pipeline] Starting Phase 2: Enrichment (pass $ENRICH_PASS)"
    uv run --env-file .env backend/scripts/enrich.py

    # Check for any failures that are still within their backoff window
    PENDING=$(uv run --env-file .env python -c "
import duckdb, sys
from datetime import datetime
try:
    conn = duckdb.connect('backend/data/cache/cache.duckdb')
    result = conn.execute('''
        SELECT COUNT(*) FROM cache_failures
        WHERE provider = 'openfigi'
          AND (retry_after_seconds = 0 OR last_attempt_at + INTERVAL (COALESCE(retry_after_seconds, 1800)) SECOND > now())
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
    echo "[pipeline] Waiting 30 minutes before retrying..."
    for i in $(seq $BACKOFF_SECONDS -60 1); do
        printf "\r[pipeline] Next retry in %dm %ds ...  " $((i / 60)) $((i % 60))
        sleep 60
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
