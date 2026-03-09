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
            period_of_performance_current_end_date DATE,
            action_date DATE,
            federal_action_obligation DOUBLE,
            market_cap DOUBLE
        )
    """)
    
    # 1. duration < 30 (uses floor 30 for acv divisor)
    # duration_days = date_diff: 2023-01-10 - 2023-01-01 = 9 days
    # floor = 30
    # acv_signal = (3000 / 30) * 365.25 = 36525
    # alpha_ratio = 3000 / 1000 = 3
    # acv_alpha_ratio = 36525 / 1000 = 36.525
    
    # 2. duration > 30 (e.g. 365 days)
    # acv_signal = (1000 / 365) * 365.25 ≈ 1000.68
    
    # 3. market_cap = 0 (alpha null)
    # 4. market_cap = null (alpha null)
    memory_db.execute("""
        INSERT INTO mock_themed VALUES 
        ('id1', 10000, 5000, '2023-01-10', '2023-01-01', 3000, 1000),
        ('id2', 20000, 10000, '2024-01-01', '2023-01-01', 1000, 5000),
        ('id3', 1000, 1000, '2023-02-01', '2023-01-01', 500, 0),
        ('id4', 2000, 2000, '2023-02-01', '2023-01-01', 500, NULL)
    """)
    
    rel = memory_db.table("mock_themed")
    res_rel = calculate_alpha_signals(rel)
    results = res_rel.fetchall()
    
    # piid, potential, total_obl, end, action, fed_obl, market_cap, diff, duration, acv, alpha, acv_alpha
    res_dict = {row[0]: row for row in results}
    
    # Check Difference
    assert res_dict['id1'][7] == 5000.0  # 10000 - 5000
    
    # Check Duration
    assert res_dict['id1'][8] == 9  # 10th - 1st
    assert res_dict['id2'][8] == 365 # 24th - 23rd
    
    # Check ACV (floor rule)
    assert res_dict['id1'][9] == 36525.0
    
    # Check Alpha
    assert res_dict['id1'][10] == 3.0
    assert res_dict['id1'][11] == 36.525
    
    # Check DivZero Prevention (market_cap = 0)
    assert res_dict['id3'][10] is None
    assert res_dict['id3'][11] is None
    
    # Check NULL mapping
    assert res_dict['id4'][10] is None
    assert res_dict['id4'][11] is None
