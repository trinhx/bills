import pytest
import duckdb
from backend.src.transform import calculate_alpha_signals


@pytest.fixture
def memory_db():
    conn = duckdb.connect(":memory:")
    yield conn
    conn.close()


def test_calculate_alpha_signals(memory_db):
    # Schema extended in M1.3-M1.5 with:
    #   total_dollars_obligated (for spec difference_between_obligated_and_potential)
    #   transaction_type        (for is_primary_action)
    #   market_cap_quality      (for signal_quality)
    #   sector                  (for extreme_ratio percentile partition)
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
            market_cap_quality VARCHAR,
            sector VARCHAR,
            number_of_offers_received DOUBLE,
            transaction_type VARCHAR,
            award_type VARCHAR
        )
    """)

    memory_db.execute("""
        INSERT INTO mock_themed VALUES
        ('k1', 'id1', 5000, 5000, 10000, '2023-01-01', '2024-01-01', '2023-06-01', 3000, 1000, 'ok', 'Industrials', 1, 'NEW_AWARDS', 'DEFINITIVE CONTRACT'),
        ('k2', 'id1', 10000, 5000, 10000, '2023-01-01', '2024-01-01', '2023-07-02', 0, 1000, 'ok', 'Industrials', 1, 'MODIFICATION', 'DEFINITIVE CONTRACT'),
        ('k3', 'id2', 20000, 10000, 20000, '2023-01-01', '2024-01-01', '2023-01-01', 1000, 5000, 'ok', 'Industrials', 4, 'NEW_AWARDS', 'DEFINITIVE CONTRACT'),
        ('k4', 'id3', 1000, 1000, 1000, '2023-01-01', '2024-01-01', '2023-01-01', 500, 0, 'no_close', 'Technology', NULL, 'NEW_AWARDS', 'DELIVERY ORDER'),
        ('k5', 'id4', 2000, 2000, 2000, '2023-01-01', '2023-01-01', '2023-01-01', 500, NULL, NULL, NULL, NULL, 'NEW_AWARDS', 'DEFINITIVE CONTRACT')
    """)

    rel = memory_db.table("mock_themed")
    res_rel = calculate_alpha_signals(rel)
    df = res_rel.df().set_index("contract_transaction_unique_key")

    dur_val = 365 / 365.25  # duration in years from date_diff('day') / 365.25

    # k1 check (fed > 0)
    assert (
        df.loc["k1", "ceiling_change"] is None
        or df.loc["k1", "ceiling_change"] != df.loc["k1", "ceiling_change"]
    )  # NaN/None first record
    assert abs(df.loc["k1", "contract_duration_years"] - dur_val) < 0.001
    assert abs(df.loc["k1", "obligation_ratio"] - 0.3) < 0.001  # 3000 / 10000
    # fed=3000 (>=1), market_cap=1000 -> alpha_ratio = 3.0
    assert abs(df.loc["k1", "alpha_ratio"] - 3.0) < 0.001

    # k2 check (fed = 0), ceiling_change = 10000 - 5000 = 5000.
    # obligation_ratio = 5000 / 10000 = 0.5
    assert df.loc["k2", "ceiling_change"] == 5000
    assert abs(df.loc["k2", "obligation_ratio"] - 0.5) < 0.001
    # Regression guard: for $0 obligations, alpha_ratio must fall back to
    # ceiling_change / market_cap rather than silently being 0.
    # 5000 / 1000 = 5.0
    assert abs(df.loc["k2", "alpha_ratio"] - 5.0) < 0.001

    # Check moat_index handling
    assert abs(df.loc["k1", "moat_index"] - 1.0) < 0.001  # offers=1
    assert abs(df.loc["k2", "moat_index"] - 1.0) < 0.001
    assert abs(df.loc["k3", "moat_index"] - 0.25) < 0.001  # offers=4
    import pandas as pd

    assert pd.isna(df.loc["k4", "moat_index"])  # offers=NULL
    assert pd.isna(df.loc["k5", "moat_index"])

    # Zero-division safety
    assert pd.isna(df.loc["k4", "contract_potential_yield"])  # market_cap=0
    assert pd.isna(df.loc["k5", "contract_potential_yield"])  # market_cap=NULL
    assert pd.isna(df.loc["k5", "annualized_potential_value"])  # duration=0

    # --- M1.3 spec-formula assertions -----------------------------------
    # duration_days enforces a 30-day floor.
    # k5 has start == end == action_date -> duration_days_raw = 0 -> floored to 30.
    assert df.loc["k5", "duration_days"] == 30
    # k1: action_date 2023-06-01, end 2024-01-01 -> 214 days
    assert df.loc["k1", "duration_days"] == 214

    # difference_between_obligated_and_potential = potential - total_obligated
    # k1: 5000 - 5000 = 0
    assert df.loc["k1", "difference_between_obligated_and_potential"] == 0
    # k3: 20000 - 10000 = 10000
    assert df.loc["k3", "difference_between_obligated_and_potential"] == 10000

    # acv_signal = (federal_action_obligation / duration_days) * 365.25
    # k1: (3000 / 214) * 365.25 = 5120.327...
    assert abs(df.loc["k1", "acv_signal"] - (3000 / 214) * 365.25) < 0.01
    # k5: duration floored to 30, fed=500 -> (500/30)*365.25
    assert abs(df.loc["k5", "acv_signal"] - (500 / 30) * 365.25) < 0.01

    # acv_alpha_ratio = acv_signal / market_cap
    # k1: market_cap=1000 -> ~5.12
    assert abs(df.loc["k1", "acv_alpha_ratio"] - (3000 / 214) * 365.25 / 1000) < 0.001
    # k4: market_cap=0 -> NaN (NULLIF)
    assert pd.isna(df.loc["k4", "acv_alpha_ratio"])
    # k5: market_cap=NULL -> NaN
    assert pd.isna(df.loc["k5", "acv_alpha_ratio"])

    # --- M1.5 is_primary_action ----------------------------------------
    assert bool(df.loc["k1", "is_primary_action"]) is True  # NEW_AWARDS
    assert bool(df.loc["k2", "is_primary_action"]) is False  # MODIFICATION
    assert bool(df.loc["k3", "is_primary_action"]) is True
    assert bool(df.loc["k4", "is_primary_action"]) is True
    assert bool(df.loc["k5", "is_primary_action"]) is True

    # --- M1.4 signal_quality -------------------------------------------
    # k1: market_cap=1000 ($1k) -> microcap; quality=ok -> 'microcap'
    # Exact string not asserted (sector p99 may mark rows extreme), but
    # membership is.
    def _has(tag: str, sq: str) -> bool:
        return tag in sq.split(";")

    assert _has("microcap", df.loc["k1", "signal_quality"])
    # k4: market_cap_quality='no_close' -> expect that tag.
    assert _has("no_close", df.loc["k4", "signal_quality"])
    # k5: market_cap IS NULL -> missing_market_cap
    assert _has("missing_market_cap", df.loc["k5", "signal_quality"])


def test_is_primary_action_null_transaction_type_is_false(memory_db):
    """
    NULL ``transaction_type`` (unclassified by Phase 1) must map to
    ``False``, not NULL, so downstream filters ``WHERE is_primary_action``
    don't need a separate ``IS NOT NULL`` guard.
    """
    memory_db.execute("""
        CREATE TABLE themed (
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
            market_cap_quality VARCHAR,
            sector VARCHAR,
            number_of_offers_received DOUBLE,
            transaction_type VARCHAR,
            award_type VARCHAR
        )
    """)
    memory_db.execute("""
        INSERT INTO themed VALUES
          ('nk1', 'p1', 1000, 500, 1000, DATE '2023-01-01', DATE '2024-01-01',
           DATE '2023-06-01', 0, 1e9, 'ok', 'Industrials', 1, NULL, 'DEFINITIVE CONTRACT')
    """)
    res = calculate_alpha_signals(memory_db.table("themed"))
    df = res.df().set_index("contract_transaction_unique_key")
    # bool(False) is False; we want to ensure the cell is literally False,
    # not NULL/NaN.
    val = df.loc["nk1", "is_primary_action"]
    assert val is False or val == False  # noqa: E712
    import pandas as pd

    assert not pd.isna(val), f"is_primary_action must not be NULL, got {val!r}"
