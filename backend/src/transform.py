import duckdb

from backend.app.core.version import PIPELINE_VERSION


def stamp_pipeline_metadata(rel: duckdb.DuckDBPyRelation) -> duckdb.DuckDBPyRelation:
    """
    Append ``pipeline_version`` and ``ingested_at`` columns to a relation.

    Stamped once at the end of Phase 4 so the schema contract includes them
    on every ``signals_awards`` row. Downstream consumers can detect
    schema/formula changes by inspecting ``pipeline_version``.

    ``ingested_at`` uses ``CURRENT_TIMESTAMP`` at the moment of projection.
    The same literal ``PIPELINE_VERSION`` value is used throughout the
    entire run (module constant).
    """
    # Parameterise via a safely-quoted literal rather than f-string
    # interpolation -- PIPELINE_VERSION is a const, but this is the safer
    # pattern for future-proofing if it ever becomes dynamic.
    escaped = PIPELINE_VERSION.replace("'", "''")
    proj = f"*, '{escaped}' AS pipeline_version, CURRENT_TIMESTAMP AS ingested_at"
    return rel.project(proj)


def filter_and_select_phase1(rel: duckdb.DuckDBPyRelation) -> duckdb.DuckDBPyRelation:
    """
    Apply Phase 1 base filters and select required columns.
    Filters:
      - federal_action_obligation >= 0
      - total_dollars_obligated >= 5000000
      - award_type IN ('DEFINITIVE CONTRACT', 'DELIVERY ORDER', 'PURCHASE ORDER')
    """
    # M1.5 P2-6: compute ``prev_potential_value`` BEFORE the $5M filter,
    # so the LAG sees the full award_id_piid history as reported by
    # USASpending, not just the rows that survive our filters. Previously
    # the LAG ran on the filtered relation, which caused rows that were
    # first-in-filtered-sample (but had real earlier siblings) to have
    # NULL ceiling_change and therefore NULL alpha_ratio. This also
    # stabilises classification: a FUNDING_INCREASE vs MODIFICATION
    # decision shouldn't change based on which sample size we ingested.
    #
    # The LAG only requires these four columns and has no size filters.
    # We project them first, compute the lag, then join back to the
    # filtered relation. Using an inner join on the primary key
    # ``contract_transaction_unique_key`` keeps everything in DuckDB.
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
        "award_type",
    ]

    # Step A: compute lag over the FULL relation, keyed by the primary key.
    # We only need the three columns the lag requires + the join key.
    lag_only = rel.project(
        "contract_transaction_unique_key, "
        "LAG(CAST(potential_total_value_of_award AS DOUBLE)) OVER ("
        "  PARTITION BY award_id_piid "
        "  ORDER BY action_date ASC, contract_transaction_unique_key ASC"
        ") AS prev_potential_value"
    )

    # Step B: apply the Phase 1 filters and select the base schema.
    filtered = (
        rel.filter("federal_action_obligation >= 0")
        .filter("total_dollars_obligated >= 5000000")
        .filter(
            "award_type IN ('DEFINITIVE CONTRACT', 'DELIVERY ORDER', 'PURCHASE ORDER')"
        )
    )
    selected = filtered.select(*columns)

    # Step C: inner-join the lag column back on the transaction key so
    # surviving rows retain the full-history ``prev_potential_value``.
    rel_with_lag = (
        selected.set_alias("l")
        .join(
            lag_only.set_alias("r"),
            "l.contract_transaction_unique_key = r.contract_transaction_unique_key",
            "left",
        )
        .project("l.*, r.prev_potential_value")
    )

    # Calculate transaction_type
    proj_expr = f"""*,
    CASE 
        WHEN list_extract(string_split(contract_transaction_unique_key, '_'), -3) IN ('0', '000') THEN
            CASE 
                WHEN parent_award_id_piid IS NULL OR parent_award_id_piid = '' OR parent_award_id_piid = '-NONE-' THEN 'NEW_AWARDS'
                ELSE 'NEW_DELIVERY_ORDERS'
            END
        WHEN list_extract(string_split(contract_transaction_unique_key, '_'), -3) NOT IN ('0', '000') THEN
            CASE
                WHEN federal_action_obligation >= 5000000 THEN 'MODIFICATION'
                WHEN federal_action_obligation = 0 THEN
                    CASE
                        WHEN transaction_description ILIKE '%No Cost%'
                             OR transaction_description ILIKE '%Time Extension%'
                             OR transaction_description ILIKE '%Administrative Change%'
                             OR transaction_description ILIKE '%Correction%'
                             OR transaction_description ILIKE '%Address Update%'
                             OR transaction_description ILIKE '%Revision%'
                             OR transaction_description ILIKE '%Clerical%'
                             OR transaction_description ILIKE '%SP30%' THEN NULL
                        WHEN (CAST(potential_total_value_of_award AS DOUBLE) - prev_potential_value) > 5000000 THEN 'FUNDING_INCREASE'
                        WHEN transaction_description ILIKE '%Add%'
                             OR transaction_description ILIKE '%Obligate%'
                             OR transaction_description ILIKE '%Increment%'
                             OR transaction_description ILIKE '%Funding%'
                             OR transaction_description ILIKE '%Production%'
                             OR transaction_description ILIKE '%Procurement%'
                             OR transaction_description ILIKE '%Manufacture%'
                             OR transaction_description ILIKE '%Purchase%'
                             OR transaction_description ILIKE '%Acquisition%'
                             OR transaction_description ILIKE '%Construction%'
                             OR transaction_description ILIKE '%Execute%'
                             OR transaction_description ILIKE '%UCA%'
                             OR transaction_description ILIKE '%Definitization%'
                             OR transaction_description ILIKE '%Letter Contract%'
                             OR transaction_description ILIKE '%Undefinitized%'
                             OR transaction_description ILIKE '%ICS%'
                             OR transaction_description ILIKE '%Interim Contractor Support%'
                             OR transaction_description ILIKE '%CLS%'
                             OR transaction_description ILIKE '%Option%'
                             OR transaction_description ILIKE '%Incentive Fee%'
                             OR transaction_description ILIKE '%Award Fee%'
                             OR transaction_description ILIKE '%Exercise%' THEN 'FUNDING_INCREASE'
                        ELSE NULL
                    END
                -- M1.5 P0-2: any positive obligation under the $5M threshold
                -- is treated as a FUNDING_INCREASE (primary action). The
                -- pilot run surfaced that ~60% of filtered rows were
                -- landing here with transaction_type=NULL (pre-fix).
                WHEN federal_action_obligation > 0 AND federal_action_obligation < 5000000 THEN 'FUNDING_INCREASE'
                ELSE NULL
            END
        ELSE NULL
    END as transaction_type,
    -- M1.5 P2-6: Persist ``ceiling_change`` here, computed from the
    -- full-history ``prev_potential_value`` (see the join above). This
    -- replaces the post-filter LAG that used to live in Phase 4 and
    -- guarantees ceiling_change reflects a piid's real modification
    -- history rather than an artefact of the sample filter.
    CAST(potential_total_value_of_award AS DOUBLE) - prev_potential_value AS ceiling_change
    """

    # M1.7 (events): we now keep ``prev_potential_value`` in the output so
    # ``calculate_alpha_signals`` can compute ``relative_ceiling_change`` =
    # ``ceiling_change / prev_potential_value`` without needing another LAG
    # window. It's a small intermediate column with no downstream consumer
    # outside Phase 4 -- benign in upstream tables.
    return rel_with_lag.project(proj_expr)


# --- Phase 2 Pure Transforms ---


def extract_unique_cage_code(rel: duckdb.DuckDBPyRelation) -> duckdb.DuckDBPyRelation:
    """Extract distinct cage codes from the phase 1 filtered awards."""
    return rel.aggregate("cage_code").filter("cage_code IS NOT NULL")


def join_entity_hierarchy(
    rel: duckdb.DuckDBPyRelation, hierarchy_rel: duckdb.DuckDBPyRelation
) -> duckdb.DuckDBPyRelation:
    """
    Left join the raw awards relation with the cached entity hierarchy results.
    We join on cage_code.

    Only projects domain fields onto the awards relation; excludes internal
    cache bookkeeping columns (``result_status``, ``last_verified``) so they
    never leak into downstream tables or the final CSV output.
    """
    joined = rel.set_alias("l").join(
        hierarchy_rel.set_alias("r"),
        "l.cage_code = r.cage_code",
        "left",
    )
    return joined.project(
        "l.*, "
        "r.cage_business_name, "
        "r.cage_update_date, "
        "r.is_highest, "
        "r.immediate_level_owner, "
        "r.highest_level_owner_name, "
        "r.highest_level_cage_code, "
        "r.highest_level_cage_update_date"
    )


def join_openfigi(
    rel: duckdb.DuckDBPyRelation, ticker_rel: duckdb.DuckDBPyRelation
) -> duckdb.DuckDBPyRelation:
    """
    Left join the relation with the OpenFIGI ticker cache.
    We join on highest_level_owner_name = highest_level_owner_name.
    """
    # Alias the relations to prevent ambiguous column names (e.g., highest_level_owner_name)
    joined = rel.set_alias("l").join(
        ticker_rel.set_alias("r"),
        "l.highest_level_owner_name = r.highest_level_owner_name",
        "left",
    )

    # We must explicitly project the ticker column from the right table
    # so that it exists in the output schema!
    return joined.project("l.*, r.ticker")


def join_market_cap(
    rel: duckdb.DuckDBPyRelation, mc_rel: duckdb.DuckDBPyRelation
) -> duckdb.DuckDBPyRelation:
    """
    Left join the relation with the market cap cache.
    Join on ticker = ticker AND action_date = date.
    """
    joined = rel.join(
        mc_rel, (rel.ticker == mc_rel.ticker) & (rel.action_date == mc_rel.date), "left"
    )
    return joined


# --- Phase 3 Pure Transforms ---


def normalize_naics(
    rel: duckdb.DuckDBPyRelation, naics_lookup_rel: duckdb.DuckDBPyRelation
) -> duckdb.DuckDBPyRelation:
    """
    Ensure naics_code is a zero-padded 6-digit string, strip whitespace, handle NULLs.
    Left join with NAICS lookup table and overwrite ``naics_description`` with
    the cleaned lookup value (stripping ``Cross-References.`` boilerplate).
    Appends ``naics_title`` from the lookup.

    We replace the raw CSV ``naics_description`` in place rather than adding
    a second column, so the output schema matches the documented data model
    and downstream consumers only see a single canonical description.
    """
    proj_expr = "* REPLACE (CASE WHEN naics_code IS NOT NULL THEN LPAD(TRIM(CAST(naics_code AS VARCHAR)), 6, '0') ELSE NULL END AS naics_code)"
    clean_rel = rel.project(proj_expr)

    joined = clean_rel.set_alias("l").join(
        naics_lookup_rel.set_alias("r"), "l.naics_code = r.naics_code", "left"
    )
    # Drop the raw naics_description from the left side (if present), then
    # re-emit using the cleaned lookup value under the same canonical name.
    # ``EXCLUDE`` errors on missing columns, so guard the clause.
    exclude_clause = (
        "EXCLUDE (naics_description)"
        if "naics_description" in clean_rel.columns
        else ""
    )
    projection = f"""
        l.* {exclude_clause},
        r.naics_title,
        TRIM(
            REPLACE(
                REPLACE(r.naics_description, 'Cross-References. Establishments primarily engaged in--', ''),
                'Cross-References.', ''
            )
        ) AS naics_description
    """
    return joined.project(projection)


def normalize_naics_keywords(
    rel: duckdb.DuckDBPyRelation, naics_kw_lookup_rel: duckdb.DuckDBPyRelation
) -> duckdb.DuckDBPyRelation:
    """
    Aggregate NAICS keywords per code and left join to append naics_keywords.
    The lookup CSV has multiple keyword rows per NAICS code; we combine them
    with STRING_AGG to produce a single semicolon-delimited string per code.

    The lookup NAICS code column is LPAD-normalized to 6 digits so it joins
    consistently with awards whose ``naics_code`` has already been zero-padded
    by ``normalize_naics``.
    """
    agg = naics_kw_lookup_rel.project(
        "LPAD(TRIM(CAST(\"2022 NAICS Code\" AS VARCHAR)), 6, '0') AS naics_code, "
        '"2022 NAICS Keywords" AS kw'
    ).aggregate("naics_code, STRING_AGG(kw, '; ') AS naics_keywords")

    joined = rel.set_alias("l").join(
        agg.set_alias("r"), "l.naics_code = r.naics_code", "left"
    )
    return joined.project("l.*, r.naics_keywords")


def normalize_psc(
    rel: duckdb.DuckDBPyRelation, psc_lookup_rel: duckdb.DuckDBPyRelation
) -> duckdb.DuckDBPyRelation:
    """
    Ensure product_or_service_code is formatted cleanly.
    Left join with PSC lookup table to append psc fields.
    """
    proj_expr = "* REPLACE (CASE WHEN product_or_service_code IS NOT NULL THEN UPPER(TRIM(CAST(product_or_service_code AS VARCHAR))) ELSE NULL END AS product_or_service_code)"
    clean_rel = rel.project(proj_expr)

    joined = clean_rel.set_alias("l").join(
        psc_lookup_rel.set_alias("r"), "l.product_or_service_code = r.psc_code", "left"
    )
    return joined.project("l.*, r.psc_name, r.psc_includes, r.psc_level_1_category")


def derive_deliverable(rel: duckdb.DuckDBPyRelation) -> duckdb.DuckDBPyRelation:
    """
    Derive deliverable column: psc_level_1_category.
    """
    proj_expr = "*, psc_level_1_category AS deliverable"
    return rel.project(proj_expr)


# --- Phase 4 Pure Transforms ---


#: Market-cap threshold below which a signal row is flagged ``microcap``.
#: Empirically chosen to match common quant-academic conventions ($50M).
#: Can be overridden by future config but is a constant today.
MICROCAP_THRESHOLD: float = 50_000_000.0


#: Event-class threshold for "MAJOR_EXPANSION" funding-increase events.
#: Picked from the empirical distribution of ``ceiling_change`` (~5,338
#: events at >$100M out of ~175k FUNDING_INCREASE rows in the resolved
#: universe). Above this we expect analyst attention; below it the
#: ceiling jump is likely incremental option exercise.
MAJOR_EXPANSION_THRESHOLD: float = 1e8  # $100M

#: Threshold separating "MODERATE_EXPANSION" from "MINOR_EXPANSION".
#: Calibrated against the same distribution: ~14.9k events > $10M total,
#: ~5.3k > $100M, leaving ~9.6k in the $10M-$100M moderate band.
MODERATE_EXPANSION_THRESHOLD: float = 1e7  # $10M


def calculate_alpha_signals(rel: duckdb.DuckDBPyRelation) -> duckdb.DuckDBPyRelation:
    """
    Compute quantitative signals on top of ``themed_awards``.

    Two parallel families of signals are emitted:

    **Legacy / current formulation** (ratio of lifetime contract value):

    * ``contract_duration_years``          -- end - start (years)
    * ``remaining_contract_length_years``  -- end - action_date (years)
    * ``annualized_potential_value``       -- potential_total / duration_years
    * ``contract_potential_yield``         -- annualized_potential / market_cap
    * ``obligation_ratio``                 -- obligation or ceiling_change / current_total
    * ``alpha_ratio``                      -- obligation or ceiling_change / market_cap
    * ``moat_index``                       -- 1 / offers (sole-source = 1.0)
    * ``ceiling_change``                   -- delta in potential_total over piid

    **Spec formulation** (new in M1; based on ``federal_action_obligation``,
    with a 30-day duration floor to prevent divide-by-tiny blow-ups):

    * ``duration_days``                           -- end - action_date (days, >= 30)
    * ``acv_signal``                              -- (fed_obligation / duration_days) * 365.25
    * ``acv_alpha_ratio``                         -- acv_signal / market_cap
    * ``difference_between_obligated_and_potential``
                                                  -- potential_total - total_obligated

    **Event-magnitude features (new in M_events)**:

    * ``ceiling_change_log_dollars`` -- signed ``log10(|ceiling_change|)``;
      positive when ceiling expands, negative when it shrinks. NULL for
      zero-or-NULL ceiling-change rows. Useful for reducing the long-tail
      sensitivity of dollar-amount-based ICs.
    * ``ceiling_change_pct_of_mcap`` -- ``ceiling_change / market_cap``.
      The "what fraction of the company's value just got announced"
      metric; the natural step-change analog of ``alpha_ratio``.
    * ``relative_ceiling_change``    -- ``ceiling_change / prev_potential_value``.
      The percent expansion of the ceiling. A 50% jump on a $100M
      contract is a different signal than a 5% jump on a $1B contract,
      even at the same dollar magnitude.
    * ``event_class`` (TEXT) -- discrete bucket combining
      ``transaction_type`` and ``ceiling_change`` magnitude:
      ``NEW_AWARD``, ``NEW_DELIVERY_ORDER``, ``MAJOR_EXPANSION``
      (FI > $100M), ``MODERATE_EXPANSION`` (FI $10M-$100M),
      ``MINOR_EXPANSION`` (FI < $10M positive), ``CONTRACTION``
      (negative ceiling_change), ``OTHER_MOD``, ``NON_EVENT``.

    **Row-level metadata** (new in M1):

    * ``is_primary_action`` (BOOL) -- True for NEW_AWARDS / NEW_DELIVERY_ORDERS /
      FUNDING_INCREASE. Default filter for the aggregation layer.
    * ``signal_quality`` (TEXT)    -- semicolon-delimited tags:
        ``ok`` | ``microcap`` | ``stale_shares`` | ``no_close`` | ``no_shares`` |
        ``missing_market_cap`` | ``extreme_ratio``. Order-independent; readers
        should split on ``;`` and match membership.

    Both families are preserved deliberately -- the Milestone 2 validation
    harness will empirically rank them; no signal is removed until then.
    """

    # M1.5 P2-6: ``ceiling_change`` is authoritatively emitted by Phase 1
    # using a FULL-history LAG (pre-filter), so the production path
    # already has this column when ``calculate_alpha_signals`` runs on
    # ``themed_awards``. We fall back to an in-function LAG only when
    # the column is absent -- keeps the unit-test mocks simple while
    # ensuring production never hits the post-filter artefact.
    if "ceiling_change" in rel.columns:
        step0 = rel
    else:
        step0 = rel.project(
            "*, "
            "CAST(potential_total_value_of_award AS DOUBLE) - "
            "LAG(CAST(potential_total_value_of_award AS DOUBLE)) OVER ("
            "  PARTITION BY award_id_piid "
            "  ORDER BY action_date ASC, contract_transaction_unique_key ASC"
            ") AS ceiling_change"
        )

    # Step 1: Duration columns (both years-based legacy and days-based spec).
    proj1 = """
        *,
        date_diff('day', CAST(period_of_performance_start_date AS DATE), CAST(period_of_performance_current_end_date AS DATE)) / 365.25 AS contract_duration_years,
        date_diff('day', CAST(action_date AS DATE), CAST(period_of_performance_current_end_date AS DATE)) / 365.25 AS remaining_contract_length_years,
        -- Spec: days from action_date to end-of-performance; NULL-safe.
        date_diff('day', CAST(action_date AS DATE), CAST(period_of_performance_current_end_date AS DATE)) AS duration_days_raw
    """
    step1 = step0.project(proj1)

    # Step 2: Legacy ratios + spec ACV + is_primary_action.
    # The 30-day floor via GREATEST is central to the spec: avoids
    # divide-by-tiny blow-ups for short/expired contracts that still
    # appear in modification records.
    proj2 = """
        *,
        CAST(potential_total_value_of_award AS DOUBLE) / NULLIF(contract_duration_years, 0) AS annualized_potential_value,
        CASE
            WHEN federal_action_obligation >= 1.0 THEN CAST(federal_action_obligation AS DOUBLE) / NULLIF(current_total_value_of_award, 0)
            ELSE CAST(ceiling_change AS DOUBLE) / NULLIF(current_total_value_of_award, 0)
        END AS obligation_ratio,
        -- Spec-formula columns (M1.3):
        GREATEST(30, COALESCE(duration_days_raw, 30)) AS duration_days,
        CAST(potential_total_value_of_award AS DOUBLE) - CAST(total_dollars_obligated AS DOUBLE) AS difference_between_obligated_and_potential,
        (CAST(federal_action_obligation AS DOUBLE) / GREATEST(30, COALESCE(duration_days_raw, 30))) * 365.25 AS acv_signal,
        -- Primary-action flag (M1.5). Everything else (MODIFICATION with
        -- $0 obligation, clerical or unclassified transactions) is
        -- non-primary. Explicit cast to VARCHAR guards against upstream
        -- type-inference drift (e.g. pandas round-trip turning an
        -- all-NULL column into INTEGER). COALESCE ensures NULL
        -- ``transaction_type`` (unclassified) maps to False rather than
        -- NULL, so downstream filters `WHERE is_primary_action` don't
        -- need a separate ``IS NOT NULL`` guard.
        COALESCE(
            CAST(transaction_type AS VARCHAR) IN ('NEW_AWARDS', 'NEW_DELIVERY_ORDERS', 'FUNDING_INCREASE'),
            FALSE
        ) AS is_primary_action
    """
    step2 = step1.project(proj2)

    # Step 3: Final ratios against market_cap + spec acv_alpha_ratio.
    proj3 = """
        * EXCLUDE (duration_days_raw),
        annualized_potential_value / NULLIF(market_cap, 0) AS contract_potential_yield,
        CASE
            WHEN federal_action_obligation >= 1.0 THEN CAST(federal_action_obligation AS DOUBLE) / NULLIF(market_cap, 0)
            ELSE CAST(ceiling_change AS DOUBLE) / NULLIF(market_cap, 0)
        END AS alpha_ratio,
        acv_signal / NULLIF(market_cap, 0) AS acv_alpha_ratio,
        1.0 / NULLIF(number_of_offers_received, 0) AS moat_index
    """
    step3 = step2.project(proj3)

    # Step 4: Extreme-ratio threshold per sector (99th percentile of
    # alpha_ratio among non-null rows). We compute it as a window so a
    # single pass over the relation produces the flag. COALESCE sector
    # so rows with NULL sector still get grouped (into 'UNKNOWN').
    proj4 = f"""
        *,
        QUANTILE_CONT(ABS(alpha_ratio), 0.99) OVER (
            PARTITION BY COALESCE(sector, 'UNKNOWN')
        ) AS _alpha_p99_sector
    """
    step4 = step3.project(proj4)

    # Step 5: Compose signal_quality from the various quality inputs.
    # Order-independent; semicolon-delimited; always non-empty.
    # Uses list_string_agg on an array of flag strings, filtered for NULL.
    #
    # CAST(market_cap_quality AS VARCHAR) is defensive against upstream
    # type-inference drift: when pandas round-trips an all-NULL column it
    # can land as DOUBLE/INTEGER, and ``= 'no_close'`` would then error.
    proj5 = f"""
        * EXCLUDE (_alpha_p99_sector),
        COALESCE(NULLIF(array_to_string(array_filter([
            CASE WHEN market_cap IS NULL THEN 'missing_market_cap' ELSE NULL END,
            CASE WHEN CAST(market_cap_quality AS VARCHAR) = 'no_close' THEN 'no_close' ELSE NULL END,
            CASE WHEN CAST(market_cap_quality AS VARCHAR) = 'no_shares' THEN 'no_shares' ELSE NULL END,
            CASE WHEN CAST(market_cap_quality AS VARCHAR) = 'stale_shares' THEN 'stale_shares' ELSE NULL END,
            CASE WHEN market_cap IS NOT NULL AND market_cap > 0 AND market_cap < {MICROCAP_THRESHOLD} THEN 'microcap' ELSE NULL END,
            CASE WHEN alpha_ratio IS NOT NULL AND _alpha_p99_sector IS NOT NULL
                      AND _alpha_p99_sector > 0
                      AND ABS(alpha_ratio) > _alpha_p99_sector
                 THEN 'extreme_ratio' ELSE NULL END
        ], x -> x IS NOT NULL), ';'), ''), 'ok') AS signal_quality
    """
    step5 = step4.project(proj5)

    # Step 6 (events): event-class + event-magnitude features.
    #
    # Three magnitude features and one categorical:
    #
    #   * ``ceiling_change_log_dollars`` -- signed log10 of |ceiling_change|.
    #     Positive when expansion, negative when contraction. NULL when
    #     ceiling_change is NULL or zero (no event to characterise).
    #     Compresses the long-tail dollar distribution so a single
    #     mega-contract doesn't dominate IC computations.
    #
    #   * ``ceiling_change_pct_of_mcap`` -- the announced delta as a
    #     fraction of market cap. Natural step-change analog of
    #     ``alpha_ratio``. NULL when market_cap is NULL or zero.
    #
    #   * ``relative_ceiling_change`` -- ``ceiling_change / prev_potential_value``,
    #     i.e. the percent expansion. NULL when ``prev_potential_value`` is
    #     absent (unit-test mocks) or zero.
    #
    #   * ``event_class`` -- the categorical bucket the event falls into.
    #     Used by the report's event-class IC tables.
    #
    # If ``prev_potential_value`` isn't in the relation (most unit-test
    # fixtures), we fall back to NULL ``relative_ceiling_change`` rather
    # than failing -- matches the schema-on-best-effort convention used
    # for ``ceiling_change`` itself.
    has_prev = "prev_potential_value" in step5.columns
    rel_ceil_expr = (
        "CAST(ceiling_change AS DOUBLE) / NULLIF(prev_potential_value, 0)"
        if has_prev
        else "CAST(NULL AS DOUBLE)"
    )
    # Always EXCLUDE prev_potential_value from the final output; it has
    # served its purpose computing ceiling_change and (optionally)
    # relative_ceiling_change. Excluding it keeps signals_awards lean.
    exclude_clause = "EXCLUDE (prev_potential_value)" if has_prev else ""
    proj6 = f"""
        * {exclude_clause},
        CASE
            WHEN ceiling_change IS NULL OR ceiling_change = 0 THEN NULL
            WHEN ceiling_change > 0 THEN LOG10(GREATEST(1.0, ceiling_change))
            ELSE -LOG10(GREATEST(1.0, ABS(ceiling_change)))
        END AS ceiling_change_log_dollars,
        CAST(ceiling_change AS DOUBLE) / NULLIF(market_cap, 0) AS ceiling_change_pct_of_mcap,
        {rel_ceil_expr} AS relative_ceiling_change,
        CASE
            WHEN CAST(transaction_type AS VARCHAR) = 'NEW_AWARDS' THEN 'NEW_AWARD'
            WHEN CAST(transaction_type AS VARCHAR) = 'NEW_DELIVERY_ORDERS' THEN 'NEW_DELIVERY_ORDER'
            WHEN CAST(transaction_type AS VARCHAR) = 'FUNDING_INCREASE'
                 AND ceiling_change IS NOT NULL
                 AND ceiling_change > {MAJOR_EXPANSION_THRESHOLD} THEN 'MAJOR_EXPANSION'
            WHEN CAST(transaction_type AS VARCHAR) = 'FUNDING_INCREASE'
                 AND ceiling_change IS NOT NULL
                 AND ceiling_change > {MODERATE_EXPANSION_THRESHOLD} THEN 'MODERATE_EXPANSION'
            WHEN CAST(transaction_type AS VARCHAR) = 'FUNDING_INCREASE'
                 AND ceiling_change IS NOT NULL
                 AND ceiling_change > 0 THEN 'MINOR_EXPANSION'
            WHEN ceiling_change IS NOT NULL AND ceiling_change < 0 THEN 'CONTRACTION'
            WHEN CAST(transaction_type AS VARCHAR) = 'MODIFICATION'
                 AND COALESCE(ceiling_change, 0) = 0 THEN 'OTHER_MOD'
            ELSE 'NON_EVENT'
        END AS event_class
    """
    return step5.project(proj6)
