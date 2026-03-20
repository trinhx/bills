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
        "parent_award_id_piid",
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
    
    # Base projection
    selected = filtered.select(*columns)
    
    # Calculate transaction_type
    proj_expr = f"""*,
    CASE 
        WHEN list_extract(string_split(contract_transaction_unique_key, '_'), -2) IN ('0', '000') 
             AND list_extract(string_split(contract_transaction_unique_key, '_'), -1) IN ('0', '000') THEN
            CASE 
                WHEN parent_award_id_piid IS NULL OR parent_award_id_piid = '' OR parent_award_id_piid = '-NONE-' THEN 'NEW_AWARDS'
                ELSE 'NEW_DELIVERY_ORDERS'
            END
        WHEN list_extract(string_split(contract_transaction_unique_key, '_'), -2) NOT IN ('0', '000') THEN
            CASE
                WHEN federal_action_obligation >= 5000000 THEN 'MODIFICATION'
                WHEN federal_action_obligation = 0 
                     AND transaction_description NOT ILIKE '%No Cost%'
                     AND transaction_description NOT ILIKE '%Time Extension%'
                     AND transaction_description NOT ILIKE '%Administrative Change%'
                     AND transaction_description NOT ILIKE '%Correction%'
                     AND transaction_description NOT ILIKE '%Address Update%'
                     AND (
                         transaction_description ILIKE '%Add%'
                         OR transaction_description ILIKE '%Obligate%'
                         OR transaction_description ILIKE '%Increment%'
                         OR transaction_description ILIKE '%Funding%'
                         OR transaction_description ILIKE '%Production of 164 Bradley Vehicles%'
                         OR transaction_description ILIKE '%Procurement of Radar%'
                         OR transaction_description ILIKE '%UCA%'
                         OR transaction_description ILIKE '%Definitization%'
                         OR transaction_description ILIKE '%Letter Contract%'
                         OR transaction_description ILIKE '%ICS%'
                         OR transaction_description ILIKE '%Interim Contractor Support%'
                         OR transaction_description ILIKE '%CLS%'
                         OR transaction_description ILIKE '%Option%'
                         OR transaction_description ILIKE '%Incentive Fee%'
                     ) THEN 'FUNDING_INCREASE'
                ELSE NULL
            END
        ELSE NULL
    END as transaction_type
    """
    
    return selected.project(proj_expr)


# --- Phase 2 Pure Transforms ---

def extract_unique_cage_code(rel: duckdb.DuckDBPyRelation) -> duckdb.DuckDBPyRelation:
    """Extract distinct cage codes from the phase 1 filtered awards."""
    return rel.aggregate("cage_code").filter("cage_code IS NOT NULL")

def join_entity_hierarchy(rel: duckdb.DuckDBPyRelation, hierarchy_rel: duckdb.DuckDBPyRelation) -> duckdb.DuckDBPyRelation:
    """
    Left join the raw awards relation with the cached entity hierarchy results.
    We join on cage_code.
    """
    # Expose necessary columns and avoid duplicates
    joined = rel.join(hierarchy_rel, "cage_code", "left")
    return joined

def join_openfigi(rel: duckdb.DuckDBPyRelation, ticker_rel: duckdb.DuckDBPyRelation) -> duckdb.DuckDBPyRelation:
    """
    Left join the relation with the OpenFIGI ticker cache.
    We join on highest_level_owner_name = highest_level_owner_name.
    """
    # Alias the relations to prevent ambiguous column names (e.g., highest_level_owner_name)
    joined = rel.set_alias('l').join(
        ticker_rel.set_alias('r'), 
        "l.highest_level_owner_name = r.highest_level_owner_name", 
        "left"
    )
    
    # We must explicitly project the ticker column from the right table
    # so that it exists in the output schema!
    return joined.project('l.*, r.ticker')
    
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
    projection = """
        l.*,
        r.naics_title,
        TRIM(
            REPLACE(
                REPLACE(r.naics_description, 'Cross-References. Establishments primarily engaged in--', ''),
                'Cross-References.', ''
            )
        ) AS naics_description_1
    """
    return joined.project(projection)

def normalize_naics_keywords(rel: duckdb.DuckDBPyRelation, naics_kw_lookup_rel: duckdb.DuckDBPyRelation) -> duckdb.DuckDBPyRelation:
    """
    Aggregate NAICS keywords per code and left join to append naics_keywords.
    The lookup CSV has multiple keyword rows per NAICS code; we combine them
    with STRING_AGG to produce a single semicolon-delimited string per code.
    """
    agg = naics_kw_lookup_rel.project(
        '"2022 NAICS Code" AS naics_code, "2022 NAICS Keywords" AS kw'
    ).aggregate("naics_code, STRING_AGG(kw, '; ') AS naics_keywords")

    joined = rel.set_alias('l').join(
        agg.set_alias('r'),
        "l.naics_code = r.naics_code",
        "left"
    )
    return joined.project('l.*, r.naics_keywords')

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
    return joined.project('l.*, r.psc_name, r.psc_includes, r.psc_level_1_category')

def derive_deliverable(rel: duckdb.DuckDBPyRelation) -> duckdb.DuckDBPyRelation:
    """
    Derive deliverable column: psc_level_1_category.
    """
    proj_expr = "*, psc_level_1_category AS deliverable"
    return rel.project(proj_expr)

# --- Phase 4 Pure Transforms ---

def calculate_alpha_signals(rel: duckdb.DuckDBPyRelation) -> duckdb.DuckDBPyRelation:
    """
    Calculate quantitative signals:
    - contract_duration_years
    - remaining_contract_length_years
    - annualized_potential_value
    - contract_potential_yield
    - obligation_ratio
    - moat_index
    """
    
    # Step 1: Intermediate columns
    proj1 = """
        *,
        date_diff('day', CAST(period_of_performance_start_date AS DATE), CAST(period_of_performance_current_end_date AS DATE)) / 365.25 AS contract_duration_years,
        date_diff('day', CAST(action_date AS DATE), CAST(period_of_performance_current_end_date AS DATE)) / 365.25 AS remaining_contract_length_years
    """
    step1 = rel.project(proj1)
    
    # Step 2: More derived columns including scores
    proj2 = """
        *,
        CAST(potential_total_value_of_award AS DOUBLE) / NULLIF(contract_duration_years, 0) AS annualized_potential_value,
        CAST(federal_action_obligation AS DOUBLE) / NULLIF(current_total_value_of_award, 0) AS obligation_ratio,
        CASE
            WHEN number_of_offers_received = 1 THEN 1.0
            WHEN number_of_offers_received > 1 THEN 1.0 / number_of_offers_received
            WHEN number_of_offers_received IS NULL AND award_type = 'DELIVERY ORDER' THEN 0.5
            ELSE NULL
        END AS entrenchment_score,
        LEAST(contract_duration_years / 10.0, 1.0) AS exclusivity_score
    """
    step2 = step1.project(proj2)
    
    # Step 3: Final Computations
    proj3 = """
        * EXCLUDE (entrenchment_score, exclusivity_score),
        annualized_potential_value / NULLIF(market_cap, 0) AS contract_potential_yield,
        CAST(federal_action_obligation AS DOUBLE) / NULLIF(market_cap, 0) AS alpha_ratio,
        (exclusivity_score * 0.4) + (entrenchment_score * 0.6) AS moat_index
    """
    return step2.project(proj3)



