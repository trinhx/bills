"""
Schema dataclasses for every stage of the USASpending alpha pipeline.

These dataclasses are the **single source of truth** for the columns
emitted at each phase. The integration tests assert that the final CSV
columns equal the fields declared in ``SignalsAward`` exactly — if you
add or rename a column in ``backend/src/transform.py``, you must also
update the corresponding dataclass here.

Dataclasses are NEVER instantiated for the full 2GB dataset; they exist
for typing, documentation, unit-test fixtures, and schema-contract
enforcement.

Column lineage:

* ``BaseAward``      -- after Phase 1 ingestion (filters + transaction_type)
* ``EnrichedAward``  -- + Phase 2 (CAGE hierarchy, OpenFIGI ticker, Yahoo mcap)
* ``ThemedAward``    -- + Phase 3 (NAICS/PSC lookups, deliverable)
* ``SignalsAward``   -- + Phase 4 (alpha signals, quality flags, metadata)
"""

from dataclasses import dataclass
from datetime import date, datetime
from typing import Optional


@dataclass
class BaseAward:
    """Phase 1 output schema (``raw_filtered_awards``)."""

    contract_transaction_unique_key: str
    award_id_piid: str
    parent_award_id_piid: Optional[str]
    transaction_type: Optional[str]
    federal_action_obligation: float
    total_dollars_obligated: float
    current_total_value_of_award: Optional[float]
    potential_total_value_of_award: Optional[float]
    action_date: date
    solicitation_date: Optional[date]
    period_of_performance_start_date: Optional[date]
    period_of_performance_current_end_date: Optional[date]
    awarding_agency_name: Optional[str]
    awarding_sub_agency_name: Optional[str]
    cage_code: str
    recipient_parent_uei: str
    recipient_parent_name: str
    recipient_parent_name_raw: str
    product_or_service_code: str
    product_or_service_code_description: Optional[str]
    naics_code: str
    # Phase 1 retains the raw CSV naics_description; Phase 3 replaces it
    # in place with the cleaned lookup value.
    naics_description: Optional[str]
    number_of_offers_received: Optional[float]
    transaction_description: str
    award_type: Optional[str]


@dataclass
class EnrichedAward(BaseAward):
    """Phase 2 output schema (``enriched_awards``)."""

    # CAGE scraper fields
    cage_business_name: Optional[str]
    cage_update_date: Optional[date]
    is_highest: Optional[bool]
    immediate_level_owner: Optional[bool]
    highest_level_owner_name: Optional[str]
    highest_level_cage_code: Optional[str]
    highest_level_cage_update_date: Optional[date]
    # OpenFIGI resolution
    is_public: Optional[bool]
    ticker: Optional[str]
    sole_source_flag: Optional[bool]
    # Yahoo Finance (point-in-time market data; M1.1/M1.2)
    market_cap: Optional[float]
    close_price: Optional[float]
    shares_outstanding: Optional[float]
    market_cap_quality: Optional[str]  # ok | stale_shares | no_close | no_shares
    sector: Optional[str]
    industry: Optional[str]
    # System metadata
    last_verified_date: Optional[date]
    theme_llm: Optional[str]


@dataclass
class ThemedAward(EnrichedAward):
    """Phase 3 output schema (``themed_awards``)."""

    naics_title: Optional[str]
    naics_keywords: Optional[str]
    psc_name: Optional[str]
    psc_includes: Optional[str]
    psc_level_1_category: Optional[str]
    deliverable: Optional[str]


@dataclass
class SignalsAward(ThemedAward):
    """
    Phase 4 output schema (``signals_awards``).

    Carries both the legacy ratio family (``alpha_ratio``,
    ``contract_potential_yield``) and the spec ACV family
    (``acv_signal``, ``acv_alpha_ratio``). The M2 validation harness will
    empirically pick winners.
    """

    # Legacy signal family
    contract_duration_years: Optional[float]
    remaining_contract_length_years: Optional[float]
    annualized_potential_value: Optional[float]
    contract_potential_yield: Optional[float]
    obligation_ratio: Optional[float]
    moat_index: Optional[float]
    alpha_ratio: Optional[float]
    ceiling_change: Optional[float]

    # Spec signal family (M1.3)
    duration_days: Optional[float]
    acv_signal: Optional[float]
    acv_alpha_ratio: Optional[float]
    difference_between_obligated_and_potential: Optional[float]

    # Event-magnitude features (M_events). Capture the *step-change*
    # at the moment of contract action rather than the level. See
    # ``calculate_alpha_signals`` docstring for full definitions.
    ceiling_change_log_dollars: Optional[float]
    ceiling_change_pct_of_mcap: Optional[float]
    relative_ceiling_change: Optional[float]
    event_class: Optional[str]

    # Row-level metadata (M1.4 / M1.5 / M1.7)
    is_primary_action: Optional[bool]
    signal_quality: Optional[str]
    pipeline_version: Optional[str]
    ingested_at: Optional[datetime]
