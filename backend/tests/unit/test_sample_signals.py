import pytest
import duckdb
import pandas as pd
from io import StringIO
from backend.src.transform import calculate_alpha_signals

# We select a tailored subset of the original data covering normal cases and
# edge cases. Schema extended in M1 with:
#   total_dollars_obligated (for spec difference_between_obligated_and_potential)
#   transaction_type        (for is_primary_action)
#   market_cap_quality, sector (for signal_quality flag composition)
ORIGINAL_DATA_CSV = """contract_transaction_unique_key,award_id_piid,potential_total_value_of_award,total_dollars_obligated,current_total_value_of_award,action_date,federal_action_obligation,market_cap,market_cap_quality,sector,number_of_offers_received,transaction_type,award_type,period_of_performance_start_date,period_of_performance_current_end_date
2036_4732_20346423F00005_P00008_GS35F281DA_0,20346423F00005,11624574.29,0,11609486.71,2024-09-30 0:00:00,0,21790818348,ok,Industrials,1,MODIFICATION,DELIVERY ORDER,2023-01-01 0:00:00,2025-06-30 0:00:00
9700_-NONE-_N0003021C1010_P00038_-NONE-_5,N0003021C1010,129895366,0,129895366,2024-09-30 0:00:00,0,73121269200,ok,Industrials,1,MODIFICATION,DEFINITIVE CONTRACT,2020-10-01 0:00:00,2024-09-30 0:00:00
9700_-NONE-_N0003021C1010_P00038_-NONE-_6,N0003021C1010,22611025.5,0,14552581.5,2024-09-30 0:00:00,0,73121269200,ok,Industrials,1,MODIFICATION,DEFINITIVE CONTRACT,2020-10-01 0:00:00,2024-04-19 0:00:00
9700_-NONE-_W56HZV18C0133_P00160_-NONE-_1,W56HZV18C0133,1081382693,263674118.5,817708574.5,2024-08-30 0:00:00,395410.72,50716605554,ok,Industrials,1,FUNDING_INCREASE,DEFINITIVE CONTRACT,2018-06-14 0:00:00,2025-05-31 0:00:00
9700_-NONE-_W56HZV18C0133_P00165_-NONE-_1,W56HZV18C0133,1083656054,263674118.5,819981935.9,2024-09-25 0:00:00,0.01,47883271969,ok,Industrials,1,MODIFICATION,DEFINITIVE CONTRACT,2018-06-14 0:00:00,2025-05-31 0:00:00
"""


@pytest.fixture
def memory_db():
    conn = duckdb.connect(":memory:")
    yield conn
    conn.close()


def test_pipeline_on_sample_data(memory_db):
    df = pd.read_csv(StringIO(ORIGINAL_DATA_CSV))
    memory_db.register("mock_themed", df)
    rel = memory_db.table("mock_themed")

    # Calculate signals
    res_rel = calculate_alpha_signals(rel)
    res_df = res_rel.df()

    # Let's map key to row for easy assertions
    res_df = res_df.set_index("contract_transaction_unique_key")

    # 1. Normal funding decrease edge case check (negative ceiling change)
    row_6 = res_df.loc["9700_-NONE-_N0003021C1010_P00038_-NONE-_6"]
    # previous value was 129895366, current is 22611025.5
    # ceiling_change = 22611025.5 - 129895366 = -107284340.5
    assert abs(row_6["ceiling_change"] - (-107284340.5)) < 1.0
    # fed=0, so obligation_ratio = ceiling_change / 14552581.5
    expected_obl = -107284340.5 / 14552581.5
    assert abs(row_6["obligation_ratio"] - expected_obl) < 1e-9

    # 2. Fed > 0 check (row_165)
    row_165 = res_df.loc["9700_-NONE-_W56HZV18C0133_P00165_-NONE-_1"]
    # fed = 0.01, so it shouldn't trigger >= 1.0 condition
    # ceiling_change = 1083656054 - 1081382693 = 2273361.0
    expected_obl_165 = 2273361.0 / 819981935.9
    assert abs(row_165["obligation_ratio"] - expected_obl_165) < 1e-9

    # 3. Normal fed > 0 check (row_160)
    row_160 = res_df.loc["9700_-NONE-_W56HZV18C0133_P00160_-NONE-_1"]
    assert abs(row_160["obligation_ratio"] - (395410.72 / 817708574.5)) < 1e-9
