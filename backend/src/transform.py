import duckdb

def filter_and_select_phase1(rel: duckdb.DuckDBPyRelation) -> duckdb.DuckDBPyRelation:
    """
    Apply Phase 1 base filters and select required columns.
    Filters:
      - federal_action_obligation >= 0
      - total_dollars_obligated >= 5000000
      - award_type IN ('DEFINITIVE CONTRACT', 'DELIVERY ORDER', 'PURCHASE ORDER')
    """
    filtered = (
        rel
        .filter("federal_action_obligation >= 0")
        .filter("total_dollars_obligated >= 5000000")
        .filter("award_type IN ('DEFINITIVE CONTRACT', 'DELIVERY ORDER', 'PURCHASE ORDER')")
    )
    
    # Select columns matching Phase 1 base schema
    columns = [
        "contract_transaction_unique_key",
        "award_id_piid",
        "federal_action_obligation",
        "total_dollars_obligated",
        "current_total_value_of_award",
        "potential_total_value_of_award",
        "action_date",
        "solicitation_date",
        "period_of_performance_start_date",
        "period_of_performance_current_end_date",
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
        "number_of_offers_received",
        "transaction_description",
        "award_type"
    ]
    
    return filtered.select(*columns)


# --- Phase 2 Pure Transforms ---

def extract_unique_uei(rel: duckdb.DuckDBPyRelation) -> duckdb.DuckDBPyRelation:
    """Extract distinct UEIs from the phase 1 filtered awards."""
    return rel.aggregate("recipient_parent_uei").filter("recipient_parent_uei IS NOT NULL")

def join_entity_hierarchy(rel: duckdb.DuckDBPyRelation, hierarchy_rel: duckdb.DuckDBPyRelation) -> duckdb.DuckDBPyRelation:
    """
    Left join the raw awards relation with the cached entity hierarchy results.
    We join on recipient_parent_uei = uei.
    """
    # Expose necessary columns and avoid duplicates
    joined = rel.join(hierarchy_rel, rel.recipient_parent_uei == hierarchy_rel.uei, "left")
    return joined

def join_openfigi(rel: duckdb.DuckDBPyRelation, ticker_rel: duckdb.DuckDBPyRelation) -> duckdb.DuckDBPyRelation:
    """
    Left join the relation with the OpenFIGI ticker cache.
    We join on highest_level_owner_name = highest_level_owner_name.
    """
    joined = rel.join(
        ticker_rel, 
        rel.highest_level_owner_name == ticker_rel.highest_level_owner_name, 
        "left"
    )
    # the cache_openfigi_ticker might add a lot of cols. The downstream expects
    # 'ticker' and we can compute 'is_public' = ticker IS NOT NULL
    # is_public logic can be handled via SQL or project
    return joined
    
def join_market_cap(rel: duckdb.DuckDBPyRelation, mc_rel: duckdb.DuckDBPyRelation) -> duckdb.DuckDBPyRelation:
    """
    Left join the relation with the market cap cache.
    Join on ticker = ticker AND action_date = date.
    """
    joined = rel.join(
        mc_rel, 
        (rel.ticker == mc_rel.ticker) & (rel.action_date == mc_rel.date), 
        "left"
    )
    return joined

# --- Phase 3 Pure Transforms ---

def normalize_naics(rel: duckdb.DuckDBPyRelation, naics_lookup_rel: duckdb.DuckDBPyRelation) -> duckdb.DuckDBPyRelation:
    """
    Ensure naics_code is a zero-padded 6-digit string, strip whitespace, handle NULLs.
    Left join with NAICS lookup table to append naics_title and naics_description.
    """
    proj_expr = "* REPLACE (CASE WHEN naics_code IS NOT NULL THEN LPAD(TRIM(CAST(naics_code AS VARCHAR)), 6, '0') ELSE NULL END AS naics_code)"
    clean_rel = rel.project(proj_expr)
    
    joined = clean_rel.set_alias('l').join(
        naics_lookup_rel.set_alias('r'),
        "l.naics_code = r.naics_code",
        "left"
    )
    return joined.project('l.*, r.naics_title, r.naics_description')

def normalize_psc(rel: duckdb.DuckDBPyRelation, psc_lookup_rel: duckdb.DuckDBPyRelation) -> duckdb.DuckDBPyRelation:
    """
    Ensure product_or_service_code is formatted cleanly.
    Left join with PSC lookup table to append psc fields.
    """
    proj_expr = "* REPLACE (CASE WHEN product_or_service_code IS NOT NULL THEN UPPER(TRIM(CAST(product_or_service_code AS VARCHAR))) ELSE NULL END AS product_or_service_code)"
    clean_rel = rel.project(proj_expr)
    
    joined = clean_rel.set_alias('l').join(
        psc_lookup_rel.set_alias('r'),
        "l.product_or_service_code = r.psc_code",
        "left"
    )
    return joined.project('l.*, r.psc_name, r.psc_includes, r.psc_category, r.psc_level_1_category')

def derive_deliverable(rel: duckdb.DuckDBPyRelation) -> duckdb.DuckDBPyRelation:
    """
    Derive deliverable column: COALESCE(psc_level_1_category, psc_category).
    """
    proj_expr = "*, COALESCE(psc_level_1_category, psc_category) AS deliverable"
    return rel.project(proj_expr)

# --- Phase 4 Pure Transforms ---

def calculate_alpha_signals(rel: duckdb.DuckDBPyRelation) -> duckdb.DuckDBPyRelation:
    """
    Calculate quantitative signals:
    - difference_between_obligated_and_potential = potential_total_value_of_award - total_dollars_obligated
    - duration_days = period_of_performance_current_end_date - action_date
    - acv_signal = (federal_action_obligation / GREATEST(30, duration_days)) * 365.25
    - alpha_ratio = federal_action_obligation / NULLIF(market_cap, 0)
    - acv_alpha_ratio = acv_signal / NULLIF(market_cap, 0)
    """
    
def calculate_alpha_signals(rel: duckdb.DuckDBPyRelation) -> duckdb.DuckDBPyRelation:
    """
    Calculate quantitative signals:
    - difference_between_obligated_and_potential = potential_total_value_of_award - total_dollars_obligated
    - duration_days = period_of_performance_current_end_date - action_date
    - acv_signal = (federal_action_obligation / GREATEST(30, duration_days)) * 365.25
    - alpha_ratio = federal_action_obligation / NULLIF(market_cap, 0)
    - acv_alpha_ratio = acv_signal / NULLIF(market_cap, 0)
    """
    
    # Step 1: Intermediate columns
    proj1 = """
        *,
        (CAST(potential_total_value_of_award AS DOUBLE) - CAST(total_dollars_obligated AS DOUBLE)) AS difference_between_obligated_and_potential,
        date_diff('day', CAST(action_date AS DATE), CAST(period_of_performance_current_end_date AS DATE)) AS raw_duration_days
    """
    step1 = rel.project(proj1)
    
    # Step 2: Final computations using prior intermediates
    proj2 = """
        * EXCLUDE (raw_duration_days),
        CAST(raw_duration_days AS INTEGER) AS duration_days,
        (CAST(federal_action_obligation AS DOUBLE) / GREATEST(30, raw_duration_days)) * 365.25 AS acv_signal,
        CAST(federal_action_obligation AS DOUBLE) / NULLIF(market_cap, 0) AS alpha_ratio,
        ((CAST(federal_action_obligation AS DOUBLE) / GREATEST(30, raw_duration_days)) * 365.25) / NULLIF(market_cap, 0) AS acv_alpha_ratio
    """
    return step1.project(proj2)



