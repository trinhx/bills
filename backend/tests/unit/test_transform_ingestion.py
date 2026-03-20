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
        ('9700_9700_piid1_0_0', 'piid1', NULL, 1000.0, 5000000.0, NULL, NULL, '2023-01-01', NULL, NULL, NULL, 'Agency', 'Sub', 'CAGE1', 'UEI1', 'Name', 'RawName', 'PSC', 'PSC Desc', 'NAICS', 'NAICS Desc', 1, 'Desc', 'DEFINITIVE CONTRACT', 'extra'),
        -- Case 2: Negative obligation (should be filtered out)
        ('9700_9700_piid2_0_0', 'piid2', NULL, -100.0, 6000000.0, NULL, NULL, '2023-01-01', NULL, NULL, NULL, 'Agency', 'Sub', 'CAGE2', 'UEI2', 'Name', 'RawName', 'PSC', 'PSC Desc', 'NAICS', 'NAICS Desc', 1, 'Desc', 'DEFINITIVE CONTRACT', 'extra'),
        -- Case 3: Zero obligation + valid transaction description (kept as FUNDING_INCREASE)
        ('9700_9700_piid3_mod1_txn', 'piid3', NULL, 0.0, 5000000.0, NULL, NULL, '2023-01-01', NULL, NULL, NULL, 'Agency', 'Sub', 'CAGE3', 'UEI3', 'Name', 'RawName', 'PSC', 'PSC Desc', 'NAICS', 'NAICS Desc', 1, 'Funding Add', 'DELIVERY ORDER', 'extra'),
        -- Case 4: Total dollars < 5M (should be filtered out)
        ('txn4', 'piid4', NULL, 1000.0, 4999999.99, NULL, NULL, '2023-01-01', NULL, NULL, NULL, 'Agency', 'Sub', 'CAGE4', 'UEI4', 'Name', 'RawName', 'PSC', 'PSC Desc', 'NAICS', 'NAICS Desc', 1, 'Desc', 'DELIVERY ORDER', 'extra'),
        -- Case 5: Invalid award_type (should be filtered out)
        ('txn5', 'piid5', NULL, 1000.0, 5000000.0, NULL, NULL, '2023-01-01', NULL, NULL, NULL, 'Agency', 'Sub', 'CAGE5', 'UEI5', 'Name', 'RawName', 'PSC', 'PSC Desc', 'NAICS', 'NAICS Desc', 1, 'Desc', 'INVALID TYPE', 'extra'),
        -- Case 6: NULL award_type (should be filtered out)
        ('txn6', 'piid6', NULL, 1000.0, 5000000.0, NULL, NULL, '2023-01-01', NULL, NULL, NULL, 'Agency', 'Sub', 'CAGE6', 'UEI6', 'Name', 'RawName', 'PSC', 'PSC Desc', 'NAICS', 'NAICS Desc', 1, 'Desc', NULL, 'extra'),
        -- Case 7: Valid MODIFICATION
        ('9700_9700_piid7_mod1_0', 'piid7', 'parent1', 5000000.0, 5000000.0, NULL, NULL, '2023-01-01', NULL, NULL, NULL, 'Agency', 'Sub', 'CAGE7', 'UEI7', 'Name', 'RawName', 'PSC', 'PSC Desc', 'NAICS', 'NAICS Desc', 1, 'Desc', 'DELIVERY ORDER', 'extra'),
        -- Case 8: NEW_DELIVERY_ORDERS
        ('9700_9700_piid8_000_000', 'piid8', 'parent2', 1000.0, 5000000.0, NULL, NULL, '2023-01-01', NULL, NULL, NULL, 'Agency', 'Sub', 'CAGE8', 'UEI8', 'Name', 'RawName', 'PSC', 'PSC Desc', 'NAICS', 'NAICS Desc', 1, 'Desc', 'DELIVERY ORDER', 'extra')
    """)
    
    rel = memory_db.table("raw_awards")
    filtered_rel = filter_and_select_phase1(rel)
    
    # Materialize explicitly, just map the df.
    df = filtered_rel.df()
    
    assert len(df) == 4
    
    columns = filtered_rel.columns
    assert "extra_ignored_column" not in columns
    assert "transaction_type" in columns
    
    # Let's map key to transaction_type for the assertions
    key_to_txn_type = dict(zip(df['contract_transaction_unique_key'], df['transaction_type']))
    
    expected_survivors = ['9700_9700_piid1_0_0', '9700_9700_piid3_mod1_txn', '9700_9700_piid7_mod1_0', '9700_9700_piid8_000_000']
    assert sorted(list(df['contract_transaction_unique_key'])) == sorted(expected_survivors)
    
    assert key_to_txn_type['9700_9700_piid1_0_0'] == 'NEW_AWARDS'
    assert key_to_txn_type['9700_9700_piid3_mod1_txn'] == 'FUNDING_INCREASE'
    assert key_to_txn_type['9700_9700_piid7_mod1_0'] == 'MODIFICATION'
    assert key_to_txn_type['9700_9700_piid8_000_000'] == 'NEW_DELIVERY_ORDERS'

