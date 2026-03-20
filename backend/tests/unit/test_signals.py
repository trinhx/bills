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
        ('id1', 10000, 5000, 10000, '2023-01-01', '2024-01-01', '2023-07-02', 3000, 1000, 1, 'DEFINITIVE CONTRACT'),
        ('id2', 20000, 10000, 20000, '2023-01-01', '2024-01-01', '2023-01-01', 1000, 5000, 4, 'DEFINITIVE CONTRACT'),
        ('id3', 1000, 1000, 1000, '2023-01-01', '2024-01-01', '2023-01-01', 500, 0, NULL, 'DELIVERY ORDER'),
        ('id4', 2000, 2000, 2000, '2023-01-01', '2023-01-01', '2023-01-01', 500, NULL, NULL, 'DEFINITIVE CONTRACT')
    """)
    
    rel = memory_db.table("mock_themed")
    res_rel = calculate_alpha_signals(rel)
    results = res_rel.fetchall()
    
    res_dict = {row[0]: row for row in results}
    
    dur_idx = 11
    rem_idx = 12
    ann_idx = 13
    obl_idx = 14
    yield_idx = 15
    alpha_idx = 16
    moat_idx = 17
    
    # 1. duration = 365 / 365.25
    dur_val = 365 / 365.25
    
    assert abs(res_dict['id1'][dur_idx] - dur_val) < 0.001
    assert abs(res_dict['id1'][obl_idx] - 0.3) < 0.001
    
    # Check entrenchment handling
    # id1: entrenchment=1.0, moat = 0.6 + (dur/10)*0.4
    assert abs(res_dict['id1'][moat_idx] - (0.6 + (dur_val/10.0)*0.4)) < 0.001
    
    # id2: entrenchment=0.25 (1/4), moat = 0.25*0.6 + (dur/10)*0.4
    assert abs(res_dict['id2'][moat_idx] - (0.15 + (dur_val/10.0)*0.4)) < 0.001
    
    # id3: entrenchment=0.5 (delivery order, null offers)
    assert abs(res_dict['id3'][moat_idx] - (0.3 + (dur_val/10.0)*0.4)) < 0.001
    
    # id4: entrenchment=NULL (not delivery order, null offers) -> moat=NULL
    assert res_dict['id4'][moat_idx] is None
    
    # Check zero divisions
    assert res_dict['id3'][yield_idx] is None  # market cap 0
    assert res_dict['id4'][yield_idx] is None  # market cap null
    assert res_dict['id4'][ann_idx] is None    # duration 0
