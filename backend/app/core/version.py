"""
Pipeline version identifier.

Stamped onto every row of ``signals_awards`` so downstream consumers can
detect schema/formula changes without inspecting column lists. Bump
``PIPELINE_VERSION`` whenever:

* A signal formula changes (numerator, denominator, floor, threshold).
* A column is renamed, added, or removed in any phase output.
* Entity-resolution rules change in a way that affects ticker/market_cap.
* Quality-flag semantics change.

Follow semver-ish: MAJOR.MINOR.PATCH
* MAJOR -- breaking formula changes or column removals
* MINOR -- new columns or new signals (additive)
* PATCH -- bug fixes that don't change the schema

Every bump must be recorded in CHANGELOG.md at the repo root.
"""

PIPELINE_VERSION: str = "1.4.0"
