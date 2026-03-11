from dataclasses import dataclass
from datetime import date
from typing import Optional

@dataclass
class BaseAward:
    contract_transaction_unique_key: str
    award_id_piid: str
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
    # Note: Phase 1 docs have naics_description in base schema, and Phase 3 also mentions it as a lookup. We include it here.
    naics_description: Optional[str]
    number_of_offers_received: Optional[float]
    transaction_description: str
    award_type: Optional[str]

@dataclass
class EnrichedAward(BaseAward):
    cage_business_name: Optional[str]
    cage_update_date: Optional[date]
    is_highest: Optional[bool]
    highest_level_owner_name: Optional[str]
    highest_level_cage_code: Optional[str]
    highest_level_cage_update_date: Optional[date]
    is_public: Optional[bool]
    ticker: Optional[str]
    market_cap: Optional[float]
    sector: Optional[str]
    industry: Optional[str]
    last_verified_date: Optional[date]
    theme_llm: Optional[str]

@dataclass
class ThemedAward(EnrichedAward):
    naics_title: Optional[str]
    psc_name: Optional[str]
    psc_includes: Optional[str]
    psc_category: Optional[str]
    psc_level_1_category: Optional[str]
    deliverable: Optional[str]

@dataclass
class SignalsAward(ThemedAward):
    alpha_ratio: Optional[float]
    difference_between_obligated_and_potential: Optional[float]
    duration_days: Optional[int]
    acv_signal: Optional[float]
    acv_alpha_ratio: Optional[float]
