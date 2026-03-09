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
        -- Case 1: Valid
        ('txn1', 'piid1', 1000.0, 5000000.0, NULL, NULL, '2023-01-01', NULL, NULL, NULL, 'Agency', 'Sub', 'CAGE1', 'UEI1', 'Name', 'RawName', 'PSC', 'PSC Desc', 'NAICS', 'NAICS Desc', 1, 'Desc', 'DEFINITIVE CONTRACT', 'extra'),
        -- Case 2: Negative obligation (should be filtered out)
        ('txn2', 'piid2', -100.0, 6000000.0, NULL, NULL, '2023-01-01', NULL, NULL, NULL, 'Agency', 'Sub', 'CAGE2', 'UEI2', 'Name', 'RawName', 'PSC', 'PSC Desc', 'NAICS', 'NAICS Desc', 1, 'Desc', 'DEFINITIVE CONTRACT', 'extra'),
        -- Case 3: Zero obligation (should be kept because rule is >= 0)
        ('txn3', 'piid3', 0.0, 5000000.0, NULL, NULL, '2023-01-01', NULL, NULL, NULL, 'Agency', 'Sub', 'CAGE3', 'UEI3', 'Name', 'RawName', 'PSC', 'PSC Desc', 'NAICS', 'NAICS Desc', 1, 'Desc', 'DELIVERY ORDER', 'extra'),
        -- Case 4: Total dollars < 5M (should be filtered out)
        ('txn4', 'piid4', 1000.0, 4999999.99, NULL, NULL, '2023-01-01', NULL, NULL, NULL, 'Agency', 'Sub', 'CAGE4', 'UEI4', 'Name', 'RawName', 'PSC', 'PSC Desc', 'NAICS', 'NAICS Desc', 1, 'Desc', 'DELIVERY ORDER', 'extra'),
        -- Case 5: Invalid award_type (should be filtered out)
        ('txn5', 'piid5', 1000.0, 5000000.0, NULL, NULL, '2023-01-01', NULL, NULL, NULL, 'Agency', 'Sub', 'CAGE5', 'UEI5', 'Name', 'RawName', 'PSC', 'PSC Desc', 'NAICS', 'NAICS Desc', 1, 'Desc', 'INVALID TYPE', 'extra'),
        -- Case 6: NULL award_type (should be filtered out)
        ('txn6', 'piid6', 1000.0, 5000000.0, NULL, NULL, '2023-01-01', NULL, NULL, NULL, 'Agency', 'Sub', 'CAGE6', 'UEI6', 'Name', 'RawName', 'PSC', 'PSC Desc', 'NAICS', 'NAICS Desc', 1, 'Desc', NULL, 'extra')
    """)
    
    rel = memory_db.table("raw_awards")
    filtered_rel = filter_and_select_phase1(rel)
    
    results = filtered_rel.fetchall()
    
    # We expect txn1 and txn3 to survive
    assert len(results) == 2
    
    columns = filtered_rel.columns
    assert "extra_ignored_column" not in columns
    
    surviving_txns = sorted([row[0] for row in results])
    assert surviving_txns == ['txn1', 'txn3']
