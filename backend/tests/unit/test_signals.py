import pytest
import duckdb
from backend.src.transform import calculate_alpha_signals

@pytest.fixture
def memory_db():
    conn = duckdb.connect(':memory:')
    yield conn
    conn.close()

def test_calculate_alpha_signals(memory_db):
    memory_db.execute("""
        CREATE TABLE mock_themed (
            contract_transaction_unique_key VARCHAR,
            award_id_piid VARCHAR,
            potential_total_value_of_award DOUBLE,
            total_dollars_obligated DOUBLE,
            current_total_value_of_award DOUBLE,
            period_of_performance_start_date DATE,
            period_of_performance_current_end_date DATE,
            action_date DATE,
            federal_action_obligation DOUBLE,
            market_cap DOUBLE,
            number_of_offers_received DOUBLE,
            award_type VARCHAR
        )
    """)
    
    memory_db.execute("""
        INSERT INTO mock_themed VALUES 
        ('k1', 'id1', 5000, 5000, 10000, '2023-01-01', '2024-01-01', '2023-06-01', 3000, 1000, 1, 'DEFINITIVE CONTRACT'),
        ('k2', 'id1', 10000, 5000, 10000, '2023-01-01', '2024-01-01', '2023-07-02', 0, 1000, 1, 'DEFINITIVE CONTRACT'),
        ('k3', 'id2', 20000, 10000, 20000, '2023-01-01', '2024-01-01', '2023-01-01', 1000, 5000, 4, 'DEFINITIVE CONTRACT'),
        ('k4', 'id3', 1000, 1000, 1000, '2023-01-01', '2024-01-01', '2023-01-01', 500, 0, NULL, 'DELIVERY ORDER'),
        ('k5', 'id4', 2000, 2000, 2000, '2023-01-01', '2023-01-01', '2023-01-01', 500, NULL, NULL, 'DEFINITIVE CONTRACT')
    """)
    
    rel = memory_db.table("mock_themed")
    res_rel = calculate_alpha_signals(rel)
    results = res_rel.fetchall()
    
    res_dict = {row[0]: row for row in results}
    
    cel_idx = 12
    dur_idx = 13
    rem_idx = 14
    ann_idx = 15
    obl_idx = 16
    yield_idx = 17
    alpha_idx = 18
    moat_idx = 19
    
    # 1. duration = 365 / 365.25
    dur_val = 365 / 365.25
    
    # k1 check (fed > 0)
    assert res_dict['k1'][cel_idx] is None # First record
    assert abs(res_dict['k1'][dur_idx] - dur_val) < 0.001
    assert abs(res_dict['k1'][obl_idx] - 0.3) < 0.001 # 3000 / 10000
    
    # k2 check (fed = 0), ceiling_change = 10000 - 5000 = 5000. 
    # obligation = MAX(0, 5000) / 10000 = 0.5
    assert res_dict['k2'][cel_idx] == 5000
    assert abs(res_dict['k2'][obl_idx] - 0.5) < 0.001
    
    # Check moat_index handling
    # k1: offers=1.0 -> moat_index = 1.0
    assert abs(res_dict['k1'][moat_idx] - 1.0) < 0.001
    assert abs(res_dict['k2'][moat_idx] - 1.0) < 0.001
    
    # k3: offers=4.0 -> moat_index = 0.25
    assert abs(res_dict['k3'][moat_idx] - 0.25) < 0.001
    
    # k4: offers=NULL -> moat_index = NULL
    assert res_dict['k4'][moat_idx] is None
    
    # k5: offers=NULL -> moat_index = NULL
    assert res_dict['k5'][moat_idx] is None
    
    # Check zero divisions
    assert res_dict['k4'][yield_idx] is None  # market cap 0
    assert res_dict['k5'][yield_idx] is None  # market cap null
    assert res_dict['k5'][ann_idx] is None    # duration 0


