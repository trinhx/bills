# USASpending Contract Awards Analysis Pipeline

This repository is designed to process and analyze massive CSV files downloaded from [USAspending.gov](https://www.usaspending.gov/).
Because these files are frequently gigabytes in size, this project strictly adheres to **Functional Programming (FP)** principles,
**Test-Driven Development (TDD)**, and lazy evaluation via Python generators. This architecture ensures we can process millions of rows
of contract data without exhausting system memory, while keeping business logic pure and fully testable. We use `uv` for lightning-fast
Python dependency and environment management.

## Directory Structure

```text
.
├── README.md
├── pyproject.toml  # Managed by uv
├── deployment/     # Docker, Terraform, etc.
├── frontend/       # UI (React/Vue/Next.js)
└── backend/
    ├── app/        # FastAPI / Web layer (API to serve contract data)
    │   ├── api/
    │   │   └── endpoints/
    │   ├── core/
    │   ├── services/
    │   └── utils/
    ├── data/       # Persistent data storage (CSVs)
    │   ├── analysis/
    │   ├── cleaned/
    │   ├── config/
    │   ├── logs/
    │   ├── out/
    │   ├── raw/    # Store massive USASpending ZIPs/CSVs here
    │   └── results/# Aggregated outputs (e.g., agency spending totals)
    ├── docs/
    ├── models/     # Pydantic models (e.g., ContractAward schema)
    ├── prompts/
    ├── scripts/    # Entry point runners
    │   └── process_contracts.py
    ├── src/        # Flattened functional core
    │   ├── analyze.py   # Pure functions for financial aggregations
    │   ├── contracts.py # USASpending-specific business logic
    │   ├── io.py        # File/DB operations (Streaming CSVs)
    │   └── transform.py # Pure functions for cleaning/normalizing fields
    └── tests/      # TDD focused tests
        ├── integration/
        └── unit/
            ├── test_contracts.py
            └── test_transform.py
```
