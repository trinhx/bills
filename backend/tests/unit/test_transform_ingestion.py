import duckdb
import pytest
from backend.src.transform import filter_and_select_phase1


@pytest.fixture
def memory_db():
    conn = duckdb.connect(":memory:")
    yield conn
    conn.close()


def test_filter_and_select_phase1(memory_db):
    memory_db.execute("""
        CREATE TABLE raw_awards (
            contract_transaction_unique_key VARCHAR,
            award_id_piid VARCHAR,
            parent_award_id_piid VARCHAR,
            federal_action_obligation DOUBLE,
            total_dollars_obligated DOUBLE,
            current_total_value_of_award DOUBLE,
            potential_total_value_of_award DOUBLE,
            action_date DATE,
            solicitation_date DATE,
            period_of_performance_start_date DATE,
            period_of_performance_current_end_date DATE,
            awarding_agency_name VARCHAR,
            awarding_sub_agency_name VARCHAR,
            cage_code VARCHAR,
            recipient_parent_uei VARCHAR,
            recipient_parent_name VARCHAR,
            recipient_parent_name_raw VARCHAR,
            product_or_service_code VARCHAR,
            product_or_service_code_description VARCHAR,
            naics_code VARCHAR,
            naics_description VARCHAR,
            number_of_offers_received DOUBLE,
            transaction_description VARCHAR,
            award_type VARCHAR,
            extra_ignored_column VARCHAR
        )
    """)

    # Insert boundary cases
    memory_db.execute("""
        INSERT INTO raw_awards VALUES 
        -- Case 1: Valid NEW_AWARDS
        ('9700_9700_piid1_0_-NONE-_0', 'piid1', NULL, 1000.0, 5000000.0, NULL, NULL, '2023-01-01', NULL, NULL, NULL, 'Agency', 'Sub', 'CAGE1', 'UEI1', 'Name', 'RawName', 'PSC', 'PSC Desc', 'NAICS', 'NAICS Desc', 1, 'Desc', 'DEFINITIVE CONTRACT', 'extra'),
        -- Case 2: Negative obligation (should be filtered out)
        ('9700_9700_piid2_0_-NONE-_0', 'piid2', NULL, -100.0, 6000000.0, NULL, NULL, '2023-01-01', NULL, NULL, NULL, 'Agency', 'Sub', 'CAGE2', 'UEI2', 'Name', 'RawName', 'PSC', 'PSC Desc', 'NAICS', 'NAICS Desc', 1, 'Desc', 'DEFINITIVE CONTRACT', 'extra'),
        -- Case 3a: Previous transaction to establish potential_total_value_of_award
        ('9700_9700_piid3_0_-NONE-_0', 'piid3', NULL, 5000.0, 5000000.0, NULL, 1000000.0, '2022-01-01', NULL, NULL, NULL, 'Agency', 'Sub', 'CAGE3', 'UEI3', 'Name', 'RawName', 'PSC', 'PSC Desc', 'NAICS', 'NAICS Desc', 1, 'Base', 'DELIVERY ORDER', 'extra'),
        -- Case 3b: Zero obligation + potential value grew by 6M (> 5M) -> FUNDING_INCREASE
        ('9700_9700_piid3_mod1_-NONE-_0', 'piid3', NULL, 0.0, 5000000.0, NULL, 7000000.0, '2023-01-01', NULL, NULL, NULL, 'Agency', 'Sub', 'CAGE3', 'UEI3', 'Name', 'RawName', 'PSC', 'PSC Desc', 'NAICS', 'NAICS Desc', 1, 'Mod 1', 'DELIVERY ORDER', 'extra'),
        -- Case 3c: Zero obligation + potential value grew by 2M (< 5M) + No intent -> NULL
        ('9700_9700_piid3_mod2_-NONE-_0', 'piid3', NULL, 0.0, 5000000.0, NULL, 9000000.0, '2024-01-01', NULL, NULL, NULL, 'Agency', 'Sub', 'CAGE3', 'UEI3', 'Name', 'RawName', 'PSC', 'PSC Desc', 'NAICS', 'NAICS Desc', 1, 'Mod 2', 'DELIVERY ORDER', 'extra'),
        -- Case 3d: Zero obligation + potential value grew by 2M (< 5M) + Intent text -> FUNDING_INCREASE
        ('9700_9700_piid3_mod3_-NONE-_0', 'piid3', NULL, 0.0, 5000000.0, NULL, 11000000.0, '2025-01-01', NULL, NULL, NULL, 'Agency', 'Sub', 'CAGE3', 'UEI3', 'Name', 'RawName', 'PSC', 'PSC Desc', 'NAICS', 'NAICS Desc', 1, 'Intent: Procurement of items', 'DELIVERY ORDER', 'extra'),
        -- Case 3e: Zero obligation + potential value grew by 6M (> 5M) + No Action text -> NULL
        ('9700_9700_piid3_mod4_-NONE-_0', 'piid3', NULL, 0.0, 5000000.0, NULL, 19000000.0, '2026-01-01', NULL, NULL, NULL, 'Agency', 'Sub', 'CAGE3', 'UEI3', 'Name', 'RawName', 'PSC', 'PSC Desc', 'NAICS', 'NAICS Desc', 1, 'Administrative Change for Growth', 'DELIVERY ORDER', 'extra'),
        -- Case 4: Total dollars < 5M (should be filtered out)
        ('txn4_0_0_0_0_0', 'piid4', NULL, 1000.0, 4999999.99, NULL, NULL, '2023-01-01', NULL, NULL, NULL, 'Agency', 'Sub', 'CAGE4', 'UEI4', 'Name', 'RawName', 'PSC', 'PSC Desc', 'NAICS', 'NAICS Desc', 1, 'Desc', 'DELIVERY ORDER', 'extra'),
        -- Case 5: Invalid award_type (should be filtered out)
        ('txn5_0_0_0_0_0', 'piid5', NULL, 1000.0, 5000000.0, NULL, NULL, '2023-01-01', NULL, NULL, NULL, 'Agency', 'Sub', 'CAGE5', 'UEI5', 'Name', 'RawName', 'PSC', 'PSC Desc', 'NAICS', 'NAICS Desc', 1, 'Desc', 'INVALID TYPE', 'extra'),
        -- Case 6: NULL award_type (should be filtered out)
        ('txn6_0_0_0_0_0', 'piid6', NULL, 1000.0, 5000000.0, NULL, NULL, '2023-01-01', NULL, NULL, NULL, 'Agency', 'Sub', 'CAGE6', 'UEI6', 'Name', 'RawName', 'PSC', 'PSC Desc', 'NAICS', 'NAICS Desc', 1, 'Desc', NULL, 'extra'),
        -- Case 7: Valid MODIFICATION
        ('9700_9700_piid7_mod1_parent1_0', 'piid7', 'parent1', 5000000.0, 5000000.0, NULL, NULL, '2023-01-01', NULL, NULL, NULL, 'Agency', 'Sub', 'CAGE7', 'UEI7', 'Name', 'RawName', 'PSC', 'PSC Desc', 'NAICS', 'NAICS Desc', 1, 'Desc', 'DELIVERY ORDER', 'extra'),
        -- Case 8: NEW_DELIVERY_ORDERS
        ('9700_9700_piid8_000_parent2_000', 'piid8', 'parent2', 1000.0, 5000000.0, NULL, NULL, '2023-01-01', NULL, NULL, NULL, 'Agency', 'Sub', 'CAGE8', 'UEI8', 'Name', 'RawName', 'PSC', 'PSC Desc', 'NAICS', 'NAICS Desc', 1, 'Desc', 'DELIVERY ORDER', 'extra')
    """)

    rel = memory_db.table("raw_awards")
    filtered_rel = filter_and_select_phase1(rel)

    # Materialize explicitly, just map the df.
    df = filtered_rel.df()

    assert len(df) == 8

    columns = filtered_rel.columns
    assert "extra_ignored_column" not in columns
    assert "transaction_type" in columns

    # Let's map key to transaction_type for the assertions
    key_to_txn_type = dict(
        zip(df["contract_transaction_unique_key"], df["transaction_type"])
    )

    expected_survivors = [
        "9700_9700_piid1_0_-NONE-_0",
        "9700_9700_piid3_0_-NONE-_0",
        "9700_9700_piid3_mod1_-NONE-_0",
        "9700_9700_piid3_mod2_-NONE-_0",
        "9700_9700_piid3_mod3_-NONE-_0",
        "9700_9700_piid3_mod4_-NONE-_0",
        "9700_9700_piid7_mod1_parent1_0",
        "9700_9700_piid8_000_parent2_000",
    ]
    assert sorted(list(df["contract_transaction_unique_key"])) == sorted(
        expected_survivors
    )

    assert key_to_txn_type["9700_9700_piid1_0_-NONE-_0"] == "NEW_AWARDS"
    assert key_to_txn_type["9700_9700_piid3_0_-NONE-_0"] == "NEW_AWARDS"
    assert key_to_txn_type["9700_9700_piid3_mod1_-NONE-_0"] == "FUNDING_INCREASE"

    import pandas as pd

    assert (
        pd.isna(key_to_txn_type["9700_9700_piid3_mod2_-NONE-_0"])
        or key_to_txn_type["9700_9700_piid3_mod2_-NONE-_0"] == "NULL"
        or key_to_txn_type["9700_9700_piid3_mod2_-NONE-_0"] is None
    )
    assert key_to_txn_type["9700_9700_piid3_mod3_-NONE-_0"] == "FUNDING_INCREASE"
    assert (
        pd.isna(key_to_txn_type["9700_9700_piid3_mod4_-NONE-_0"])
        or key_to_txn_type["9700_9700_piid3_mod4_-NONE-_0"] == "NULL"
        or key_to_txn_type["9700_9700_piid3_mod4_-NONE-_0"] is None
    )

    assert key_to_txn_type["9700_9700_piid7_mod1_parent1_0"] == "MODIFICATION"
    assert key_to_txn_type["9700_9700_piid8_000_parent2_000"] == "NEW_DELIVERY_ORDERS"


def test_transaction_type_small_modification_is_funding_increase(memory_db):
    """
    M1.5 P0-2 regression guard.

    Modifications with ``$0 < federal_action_obligation < $5M`` must
    be classified as ``FUNDING_INCREASE``. Prior to the fix, these
    rows fell through to ``ELSE NULL`` — the pilot run showed 105 /
    173 rows (61%) being silently dropped this way.
    """
    memory_db.execute("""
        CREATE TABLE raw_awards (
            contract_transaction_unique_key VARCHAR,
            award_id_piid VARCHAR,
            parent_award_id_piid VARCHAR,
            federal_action_obligation DOUBLE,
            total_dollars_obligated DOUBLE,
            current_total_value_of_award DOUBLE,
            potential_total_value_of_award DOUBLE,
            action_date DATE,
            solicitation_date DATE,
            period_of_performance_start_date DATE,
            period_of_performance_current_end_date DATE,
            awarding_agency_name VARCHAR,
            awarding_sub_agency_name VARCHAR,
            cage_code VARCHAR,
            recipient_parent_uei VARCHAR,
            recipient_parent_name VARCHAR,
            recipient_parent_name_raw VARCHAR,
            product_or_service_code VARCHAR,
            product_or_service_code_description VARCHAR,
            naics_code VARCHAR,
            naics_description VARCHAR,
            number_of_offers_received DOUBLE,
            transaction_description VARCHAR,
            award_type VARCHAR
        )
    """)

    # All rows have mod-suffix P00001 (NOT IN '0','000'), so they go
    # through the modification-classification branch.
    memory_db.execute("""
        INSERT INTO raw_awards VALUES
        -- $888k obligation, description is "FUNDING MODIFICATION" (no
        -- keyword hits): pre-fix => NULL, post-fix => FUNDING_INCREASE
        ('7014_4732_piidA_P00001_GSX_0', 'piidA', 'parentA', 888300.0, 6000000.0, NULL, NULL, '2023-01-01', NULL, NULL, NULL, 'Agency', 'Sub', 'CAGE', 'UEI', 'Name', 'RawName', 'PSC', 'PSC Desc', '541512', 'NAICS Desc', 1, 'FUNDING MODIFICATION', 'DELIVERY ORDER'),
        -- $1 obligation (lower boundary): post-fix => FUNDING_INCREASE
        ('7014_4732_piidB_P00001_GSX_0', 'piidB', 'parentB', 1.0, 6000000.0, NULL, NULL, '2023-01-01', NULL, NULL, NULL, 'Agency', 'Sub', 'CAGE', 'UEI', 'Name', 'RawName', 'PSC', 'PSC Desc', '541512', 'NAICS Desc', 1, 'No keyword match here.', 'DELIVERY ORDER'),
        -- $4,999,999.99 (upper boundary, still < $5M): FUNDING_INCREASE
        ('7014_4732_piidC_P00001_GSX_0', 'piidC', 'parentC', 4999999.99, 6000000.0, NULL, NULL, '2023-01-01', NULL, NULL, NULL, 'Agency', 'Sub', 'CAGE', 'UEI', 'Name', 'RawName', 'PSC', 'PSC Desc', '541512', 'NAICS Desc', 1, 'Arbitrary text.', 'DELIVERY ORDER'),
        -- $5,000,000 exact (boundary): MODIFICATION (>= 5M branch)
        ('7014_4732_piidD_P00001_GSX_0', 'piidD', 'parentD', 5000000.0, 6000000.0, NULL, NULL, '2023-01-01', NULL, NULL, NULL, 'Agency', 'Sub', 'CAGE', 'UEI', 'Name', 'RawName', 'PSC', 'PSC Desc', '541512', 'NAICS Desc', 1, 'Mod', 'DELIVERY ORDER')
    """)

    rel = memory_db.table("raw_awards")
    df = filter_and_select_phase1(rel).df()
    key_to_tt = dict(zip(df["contract_transaction_unique_key"], df["transaction_type"]))
    assert key_to_tt["7014_4732_piidA_P00001_GSX_0"] == "FUNDING_INCREASE"
    assert key_to_tt["7014_4732_piidB_P00001_GSX_0"] == "FUNDING_INCREASE"
    assert key_to_tt["7014_4732_piidC_P00001_GSX_0"] == "FUNDING_INCREASE"
    assert key_to_tt["7014_4732_piidD_P00001_GSX_0"] == "MODIFICATION"


def test_ceiling_change_uses_full_piid_history(memory_db):
    """
    M1.5 P2-6 regression guard.

    ``filter_and_select_phase1`` must compute ``prev_potential_value``
    (and therefore ``ceiling_change``) using the FULL unfiltered piid
    history, not just rows that survive the $5M filter. This prevents
    first-in-filtered-sample rows from having NULL ceiling_change when
    the raw CSV has earlier siblings.

    Scenario: piid A has two rows.
      * Row 1 is a NEW_AWARD with total_dollars_obligated=$1M (filtered
        out) and potential_total=$4M.
      * Row 2 is a MODIFICATION with total_dollars_obligated=$10M
        (survives) and potential_total=$10M.

    Pre-fix: row 2's ceiling_change was NULL because the LAG ran over
    filtered data and row 2 was first-in-partition.

    Post-fix: row 2's ceiling_change is $10M - $4M = $6M, computed
    against the row that didn't survive the filter.
    """
    memory_db.execute("""
        CREATE TABLE raw_awards (
            contract_transaction_unique_key VARCHAR,
            award_id_piid VARCHAR,
            parent_award_id_piid VARCHAR,
            federal_action_obligation DOUBLE,
            total_dollars_obligated DOUBLE,
            current_total_value_of_award DOUBLE,
            potential_total_value_of_award DOUBLE,
            action_date DATE,
            solicitation_date DATE,
            period_of_performance_start_date DATE,
            period_of_performance_current_end_date DATE,
            awarding_agency_name VARCHAR,
            awarding_sub_agency_name VARCHAR,
            cage_code VARCHAR,
            recipient_parent_uei VARCHAR,
            recipient_parent_name VARCHAR,
            recipient_parent_name_raw VARCHAR,
            product_or_service_code VARCHAR,
            product_or_service_code_description VARCHAR,
            naics_code VARCHAR,
            naics_description VARCHAR,
            number_of_offers_received DOUBLE,
            transaction_description VARCHAR,
            award_type VARCHAR
        )
    """)

    memory_db.execute("""
        INSERT INTO raw_awards VALUES
        -- Row 1: earlier NEW_AWARD with potential=4M, total_obligated only $1M
        -- (filtered out by the >=$5M total filter), potential=4M
        ('key_base', 'piid_A', NULL, 1000000.0, 1000000.0, 4000000.0, 4000000.0,
         '2023-01-01', NULL, NULL, NULL, 'Agency', 'Sub', 'CAGE', 'UEI', 'Name',
         'RawName', 'PSC', 'PSC Desc', '541512', 'NAICS Desc', 1, 'Initial',
         'DEFINITIVE CONTRACT'),
        -- Row 2: later modification with potential=10M, total_obligated=10M
        -- (survives filter). Without P2-6, ceiling_change would be NULL.
        -- With P2-6, ceiling_change = 10M - 4M = 6M.
        ('key_mod',  'piid_A', 'piid_A', 1500000.0, 10000000.0, 10000000.0, 10000000.0,
         '2023-06-01', NULL, NULL, NULL, 'Agency', 'Sub', 'CAGE', 'UEI', 'Name',
         'RawName', 'PSC', 'PSC Desc', '541512', 'NAICS Desc', 1, 'Ceiling bump',
         'DEFINITIVE CONTRACT')
    """)

    df = filter_and_select_phase1(memory_db.table("raw_awards")).df()
    # Only row 2 survives the $5M filter.
    assert set(df["contract_transaction_unique_key"]) == {"key_mod"}, df
    # ceiling_change must reflect the full piid history: 10M - 4M.
    row = df[df["contract_transaction_unique_key"] == "key_mod"].iloc[0]
    assert row["ceiling_change"] == 6_000_000.0, (
        f"ceiling_change expected 6_000_000, got {row['ceiling_change']}"
    )


def test_transaction_type_mockup(memory_db):
    import os

    # Path to the mock CSV file
    csv_path = os.path.join(os.path.dirname(__file__), "mock_transaction_types.csv")

    # Load into duckdb relation
    rel = memory_db.read_csv(csv_path)

    # Apply the transform
    filtered_rel = filter_and_select_phase1(rel)

    # Convert to DataFrame to check results
    df = filtered_rel.df()

    # Also load the expected results to compare
    expected_df = (
        memory_db.read_csv(csv_path)
        .project("contract_transaction_unique_key, expected_transaction_type")
        .df()
    )
    expected_map = dict(
        zip(
            expected_df["contract_transaction_unique_key"],
            expected_df["expected_transaction_type"],
        )
    )

    # Check each output row
    for _, row in df.iterrows():
        key = row["contract_transaction_unique_key"]
        actual = row["transaction_type"]
        expected = expected_map.get(key)

        # Handle nan/NULL comparison
        import pandas as pd

        if pd.isna(expected) or expected == "NULL":
            assert pd.isna(actual) or actual == "NULL" or actual is None, (
                f"Expected NULL for {key}, got {actual}"
            )
        else:
            assert actual == expected, f"Expected {expected} for {key}, got {actual}"


# ---------------------------------------------------------------------------
# Multi-file ingestion (M_multi)
# ---------------------------------------------------------------------------


def _write_mini_contracts_csv(path, rows):
    """
    Write a minimal USASpending-shaped CSV with the subset of columns
    the ingestion path actually needs. The pipeline's scan_contracts_csv
    is liberal about columns it doesn't explicitly type, so we only
    write the ones Phase 1 cares about plus a couple of extras.
    """
    header = [
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
    lines = [",".join(header)]
    for r in rows:
        lines.append(",".join("" if v is None else str(v) for v in r))
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def test_scan_contracts_csv_accepts_list_of_paths(tmp_path, memory_db):
    """
    scan_contracts_csv should accept a list of CSV paths and union them
    into a single DuckDB relation. This is the foundation of multi-file
    ingestion; without it Phase 1's LAG cannot span file boundaries.
    """
    from backend.src.io import scan_contracts_csv

    # Two tiny CSVs, each with one row of the same piid.
    file_a = tmp_path / "shard_a.csv"
    file_b = tmp_path / "shard_b.csv"
    _write_mini_contracts_csv(
        file_a,
        [
            (
                "k_a",
                "piid_X",
                "",
                100.0,
                6000000.0,
                "",
                1000000.0,
                "2023-06-01",
                "",
                "",
                "",
                "Agency",
                "Sub",
                "CAGE",
                "UEI",
                "Name",
                "RawName",
                "PSC",
                "Desc",
                "541512",
                "NAICS Desc",
                1,
                "Desc",
                "DEFINITIVE CONTRACT",
            ),
        ],
    )
    _write_mini_contracts_csv(
        file_b,
        [
            (
                "k_b",
                "piid_Y",
                "",
                200.0,
                6000000.0,
                "",
                2000000.0,
                "2023-07-01",
                "",
                "",
                "",
                "Agency",
                "Sub",
                "CAGE",
                "UEI",
                "Name",
                "RawName",
                "PSC",
                "Desc",
                "541512",
                "NAICS Desc",
                1,
                "Desc",
                "DEFINITIVE CONTRACT",
            ),
        ],
    )

    rel = scan_contracts_csv(memory_db, [str(file_a), str(file_b)])
    df = rel.df()
    # Both rows from both files present in the unioned relation.
    assert set(df["contract_transaction_unique_key"]) == {"k_a", "k_b"}
    assert len(df) == 2


def test_filter_and_select_phase1_spans_multiple_files(tmp_path, memory_db):
    """
    Regression guard for the full-year multi-file ingestion.

    Phase 1 computes ``prev_potential_value`` via a LAG window. When the
    piid's history straddles two CSV files, the LAG must still see the
    earlier file's row so that ``ceiling_change`` is computed correctly.

    Scenario:
      * File A (older): piid_X @ 2023-06-01 with potential=$4M,
        total_obligated=$1M  (filtered out by the $5M threshold).
      * File B (newer): piid_X @ 2024-06-01 with potential=$10M,
        total_obligated=$10M (survives the filter).

    Expected: the surviving row has ceiling_change = 10M - 4M = 6M,
    which is only possible if the LAG saw the earlier file's row.
    """
    from backend.src.io import scan_contracts_csv

    file_old = tmp_path / "FY2023.csv"
    file_new = tmp_path / "FY2024.csv"

    # Older row: total_obligated=$1M, potential=$4M. Filtered out.
    _write_mini_contracts_csv(
        file_old,
        [
            (
                "k_old",
                "piid_X",
                "",
                1000000.0,
                1000000.0,
                4000000.0,
                4000000.0,
                "2023-06-01",
                "",
                "",
                "",
                "Agency",
                "Sub",
                "CAGE",
                "UEI",
                "Name",
                "RawName",
                "PSC",
                "Desc",
                "541512",
                "NAICS Desc",
                1,
                "Initial",
                "DEFINITIVE CONTRACT",
            ),
        ],
    )

    # Newer row: total_obligated=$10M, potential=$10M. Survives.
    _write_mini_contracts_csv(
        file_new,
        [
            (
                "k_new",
                "piid_X",
                "piid_X",
                1500000.0,
                10000000.0,
                10000000.0,
                10000000.0,
                "2024-06-01",
                "",
                "",
                "",
                "Agency",
                "Sub",
                "CAGE",
                "UEI",
                "Name",
                "RawName",
                "PSC",
                "Desc",
                "541512",
                "NAICS Desc",
                1,
                "Ceiling bump",
                "DEFINITIVE CONTRACT",
            ),
        ],
    )

    rel = scan_contracts_csv(memory_db, [str(file_old), str(file_new)])
    df = filter_and_select_phase1(rel).df()

    # Only the newer row survived.
    assert set(df["contract_transaction_unique_key"]) == {"k_new"}, df
    # ceiling_change must reflect the FULL-history LAG, spanning files.
    surv = df[df["contract_transaction_unique_key"] == "k_new"].iloc[0]
    assert surv["ceiling_change"] == 6_000_000.0, (
        f"ceiling_change expected 6_000_000 (10M - 4M across files); "
        f"got {surv['ceiling_change']}"
    )
