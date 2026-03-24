import duckdb
import pytest
from backend.src.transform import filter_and_select_phase1

@pytest.fixture
def memory_db():
    conn = duckdb.connect(':memory:')
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
    key_to_txn_type = dict(zip(df['contract_transaction_unique_key'], df['transaction_type']))
    
    expected_survivors = ['9700_9700_piid1_0_-NONE-_0', '9700_9700_piid3_0_-NONE-_0', '9700_9700_piid3_mod1_-NONE-_0', '9700_9700_piid3_mod2_-NONE-_0', '9700_9700_piid3_mod3_-NONE-_0', '9700_9700_piid3_mod4_-NONE-_0', '9700_9700_piid7_mod1_parent1_0', '9700_9700_piid8_000_parent2_000']
    assert sorted(list(df['contract_transaction_unique_key'])) == sorted(expected_survivors)
    
    assert key_to_txn_type['9700_9700_piid1_0_-NONE-_0'] == 'NEW_AWARDS'
    assert key_to_txn_type['9700_9700_piid3_0_-NONE-_0'] == 'NEW_AWARDS'
    assert key_to_txn_type['9700_9700_piid3_mod1_-NONE-_0'] == 'FUNDING_INCREASE'
    
    import pandas as pd
    assert pd.isna(key_to_txn_type['9700_9700_piid3_mod2_-NONE-_0']) or key_to_txn_type['9700_9700_piid3_mod2_-NONE-_0'] == 'NULL' or key_to_txn_type['9700_9700_piid3_mod2_-NONE-_0'] is None
    assert key_to_txn_type['9700_9700_piid3_mod3_-NONE-_0'] == 'FUNDING_INCREASE'
    assert pd.isna(key_to_txn_type['9700_9700_piid3_mod4_-NONE-_0']) or key_to_txn_type['9700_9700_piid3_mod4_-NONE-_0'] == 'NULL' or key_to_txn_type['9700_9700_piid3_mod4_-NONE-_0'] is None

    assert key_to_txn_type['9700_9700_piid7_mod1_parent1_0'] == 'MODIFICATION'
    assert key_to_txn_type['9700_9700_piid8_000_parent2_000'] == 'NEW_DELIVERY_ORDERS'

def test_transaction_type_mockup(memory_db):
    import os
    
    # Path to the mock CSV file
    csv_path = os.path.join(os.path.dirname(__file__), 'mock_transaction_types.csv')
    
    # Load into duckdb relation
    rel = memory_db.read_csv(csv_path)
    
    # Apply the transform
    filtered_rel = filter_and_select_phase1(rel)
    
    # Convert to DataFrame to check results
    df = filtered_rel.df()
    
    # Also load the expected results to compare
    expected_df = memory_db.read_csv(csv_path).project('contract_transaction_unique_key, expected_transaction_type').df()
    expected_map = dict(zip(expected_df['contract_transaction_unique_key'], expected_df['expected_transaction_type']))
    
    # Check each output row
    for _, row in df.iterrows():
        key = row['contract_transaction_unique_key']
        actual = row['transaction_type']
        expected = expected_map.get(key)
        
        # Handle nan/NULL comparison
        import pandas as pd
        if pd.isna(expected) or expected == 'NULL':
            assert pd.isna(actual) or actual == 'NULL' or actual is None, f"Expected NULL for {key}, got {actual}"
        else:
            assert actual == expected, f"Expected {expected} for {key}, got {actual}"
