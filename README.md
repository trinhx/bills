# USASpending Quantitative Analysis Pipeline

This project builds a high-performance data pipeline and quantitative analysis tool to identify potential alpha signals in publicly traded companies based on USASpending.gov contract awards.

At a high level, it:
- Ingests very large USASpending contract CSV drops (multi-GB) efficiently.
- Filters for meaningful “New Money” activity.
- Resolves award recipients to publicly traded tickers via entity resolution.
- Enriches awards with market context (e.g., market cap at award date).
- Generates quantitative signals to support research and downstream analysis.

## Why this exists

Government contracting can act as a real-world indicator of demand and budget allocation. This pipeline makes those award flows queryable and comparable to public market fundamentals, so you can rank companies and themes by award momentum relative to size.

## What it does (phases)

The backend pipeline is organized into four phases:

1) Data Ingestion
- Efficiently reads and filters large CSV drops.
- Produces a cleaned, queryable base dataset.

2) Data Processing & Enrichment
- Resolves contractor identifiers to ultimate parent entities and tradable tickers.
- Enriches transactions with market and company metadata.
- Uses persistent caching to minimize repeated API calls.

3) Theme Intelligence
- Classifies awards by themes/sectors using NAICS and product/service codes.
- Enables aggregated views of contracting activity by theme.

4) Signal Generation
- Computes actionable signals (including raw and ACV-based ratios).
- Produces a results dataset suitable for quantitative research, ranking, and alerting.

## Core design goals

- Performance on large datasets: Designed to process multi-GB CSV drops without loading everything into memory.
- Deterministic + testable: Data logic is structured to be reliable and verifiable before scaling up.
- Resilient enrichment: External API calls are rate-limited, retried safely, and cached to avoid rework.
- Clear separation of concerns: Ingestion, enrichment, classification, and signal computation are built as independent phases.

## Outputs

The pipeline produces:
- Cleaned intermediate datasets (phase outputs).
- A final results dataset containing enriched award records and computed alpha signals.

## How to work on this project

This repo is designed to be implemented and validated phase-by-phase. Complete and verify a phase before moving on to the next one to avoid compounding errors across the pipeline.

