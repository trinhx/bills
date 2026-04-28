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


# ---------------------------------------------------------------------------
# Event-class + event-magnitude features (M_events)
# ---------------------------------------------------------------------------


def _build_event_themed_table(memory_db):
    """
    Create a ``themed`` fixture that exercises every event_class branch
    so the tests can assert assignments precisely. Each row is named
    after the event_class it should map to.
    """
    memory_db.execute("""
        CREATE TABLE themed (
            contract_transaction_unique_key VARCHAR,
            award_id_piid VARCHAR,
            parent_award_id_piid VARCHAR,
            federal_action_obligation DOUBLE,
            total_dollars_obligated DOUBLE,
            current_total_value_of_award DOUBLE,
            potential_total_value_of_award DOUBLE,
            period_of_performance_start_date DATE,
            period_of_performance_current_end_date DATE,
            action_date DATE,
            market_cap DOUBLE,
            market_cap_quality VARCHAR,
            sector VARCHAR,
            number_of_offers_received DOUBLE,
            transaction_type VARCHAR,
            award_type VARCHAR,
            ceiling_change DOUBLE,
            prev_potential_value DOUBLE
        )
    """)
    # market_cap = 1e10 ($10B), so a $200M ceiling_change is 2% of mcap.
    memory_db.execute("""
        INSERT INTO themed VALUES
        -- NEW_AWARD: transaction_type = NEW_AWARDS regardless of ceiling_change
        ('NEW_AWARD',          'p1', NULL,      100000, 6e6, NULL, 5e8, '2023-01-01', '2024-01-01', '2023-06-01', 1e10, 'ok', 'Industrials', 1, 'NEW_AWARDS',          'DEFINITIVE CONTRACT', NULL,    NULL),
        -- NEW_DELIVERY_ORDER
        ('NEW_DELIVERY_ORDER', 'p2', 'parent2', 100000, 6e6, NULL, 5e7, '2023-01-01', '2024-01-01', '2023-06-01', 1e10, 'ok', 'Industrials', 1, 'NEW_DELIVERY_ORDERS', 'DELIVERY ORDER',      NULL,    NULL),
        -- MAJOR_EXPANSION: FUNDING_INCREASE with ceiling_change > $100M
        ('MAJOR_EXPANSION',    'p3', 'parent3',  50000, 6e6, NULL, 5e8, '2023-01-01', '2024-01-01', '2023-06-01', 1e10, 'ok', 'Industrials', 1, 'FUNDING_INCREASE',    'DEFINITIVE CONTRACT', 2e8,     3e8),
        -- MODERATE_EXPANSION: FUNDING_INCREASE with ceiling_change in ($10M, $100M]
        ('MODERATE_EXPANSION', 'p4', 'parent4',  50000, 6e6, NULL, 1e8, '2023-01-01', '2024-01-01', '2023-06-01', 1e10, 'ok', 'Industrials', 1, 'FUNDING_INCREASE',    'DEFINITIVE CONTRACT', 5e7,     5e7),
        -- MINOR_EXPANSION: FUNDING_INCREASE with positive ceiling_change <= $10M
        ('MINOR_EXPANSION',    'p5', 'parent5',  50000, 6e6, NULL, 1e7, '2023-01-01', '2024-01-01', '2023-06-01', 1e10, 'ok', 'Industrials', 1, 'FUNDING_INCREASE',    'DEFINITIVE CONTRACT', 5e6,     5e6),
        -- CONTRACTION: any negative ceiling_change
        ('CONTRACTION',        'p6', 'parent6',      0, 6e6, NULL, 5e7, '2023-01-01', '2024-01-01', '2023-06-01', 1e10, 'ok', 'Industrials', 1, 'FUNDING_INCREASE',    'DEFINITIVE CONTRACT', -1e7,    6e7),
        -- OTHER_MOD: MODIFICATION with zero ceiling_change
        ('OTHER_MOD',          'p7', 'parent7',      0, 6e6, NULL, 5e7, '2023-01-01', '2024-01-01', '2023-06-01', 1e10, 'ok', 'Industrials', 1, 'MODIFICATION',        'DEFINITIVE CONTRACT', 0,       5e7),
        -- NON_EVENT: NULL transaction_type, zero ceiling_change
        ('NON_EVENT',          'p8', 'parent8',      0, 6e6, NULL, 5e7, '2023-01-01', '2024-01-01', '2023-06-01', 1e10, 'ok', 'Industrials', 1, NULL,                  'DEFINITIVE CONTRACT', 0,       5e7)
    """)


def test_event_class_assigns_each_branch_correctly(memory_db):
    """Each fixture row must map to its namesake event_class."""
    _build_event_themed_table(memory_db)
    res = calculate_alpha_signals(memory_db.table("themed"))
    df = res.df().set_index("contract_transaction_unique_key")
    expected = {
        "NEW_AWARD": "NEW_AWARD",
        "NEW_DELIVERY_ORDER": "NEW_DELIVERY_ORDER",
        "MAJOR_EXPANSION": "MAJOR_EXPANSION",
        "MODERATE_EXPANSION": "MODERATE_EXPANSION",
        "MINOR_EXPANSION": "MINOR_EXPANSION",
        "CONTRACTION": "CONTRACTION",
        "OTHER_MOD": "OTHER_MOD",
        "NON_EVENT": "NON_EVENT",
    }
    for key, expected_class in expected.items():
        actual = df.loc[key, "event_class"]
        assert actual == expected_class, (
            f"row {key!r}: expected event_class={expected_class!r}, got {actual!r}"
        )


def test_ceiling_change_log_dollars_is_signed(memory_db):
    """
    log10 should be positive for expansions, negative for contractions,
    NULL for zero-or-NULL ceiling_change.
    """
    import pandas as pd

    _build_event_themed_table(memory_db)
    res = calculate_alpha_signals(memory_db.table("themed"))
    df = res.df().set_index("contract_transaction_unique_key")

    # MAJOR_EXPANSION: ceiling_change = 2e8 -> log10(2e8) ~= 8.301
    assert df.loc["MAJOR_EXPANSION", "ceiling_change_log_dollars"] == pytest.approx(
        8.30103, abs=1e-4
    )
    # MODERATE_EXPANSION: ceiling_change = 5e7 -> log10(5e7) ~= 7.699
    assert df.loc["MODERATE_EXPANSION", "ceiling_change_log_dollars"] == pytest.approx(
        7.69897, abs=1e-4
    )
    # CONTRACTION: ceiling_change = -1e7 -> -log10(1e7) = -7.0
    assert df.loc["CONTRACTION", "ceiling_change_log_dollars"] == pytest.approx(
        -7.0, abs=1e-4
    )
    # NEW_AWARD: ceiling_change is NULL -> log_dollars NULL
    assert pd.isna(df.loc["NEW_AWARD", "ceiling_change_log_dollars"])
    # OTHER_MOD: ceiling_change is 0 -> log_dollars NULL (zero-clipping)
    assert pd.isna(df.loc["OTHER_MOD", "ceiling_change_log_dollars"])


def test_ceiling_change_pct_of_mcap(memory_db):
    """
    ceiling_change_pct_of_mcap = ceiling_change / market_cap.
    NULL when market_cap is zero/NULL (NULLIF semantics).
    """
    import pandas as pd

    _build_event_themed_table(memory_db)
    res = calculate_alpha_signals(memory_db.table("themed"))
    df = res.df().set_index("contract_transaction_unique_key")

    # MAJOR_EXPANSION: 2e8 / 1e10 = 0.02
    assert df.loc["MAJOR_EXPANSION", "ceiling_change_pct_of_mcap"] == pytest.approx(0.02)
    # CONTRACTION: -1e7 / 1e10 = -0.001
    assert df.loc["CONTRACTION", "ceiling_change_pct_of_mcap"] == pytest.approx(-0.001)
    # OTHER_MOD: 0 / 1e10 = 0
    assert df.loc["OTHER_MOD", "ceiling_change_pct_of_mcap"] == pytest.approx(0.0)
    # NEW_AWARD: ceiling_change NULL -> pct NULL
    assert pd.isna(df.loc["NEW_AWARD", "ceiling_change_pct_of_mcap"])


def test_relative_ceiling_change_uses_prev_potential_value(memory_db):
    """
    relative_ceiling_change = ceiling_change / prev_potential_value.
    Verifies the divisor is the prior ceiling, not the current one.
    NULL when prev_potential_value is zero/NULL.
    """
    import pandas as pd

    _build_event_themed_table(memory_db)
    res = calculate_alpha_signals(memory_db.table("themed"))
    df = res.df().set_index("contract_transaction_unique_key")

    # MAJOR_EXPANSION: ceiling_change=2e8, prev=3e8 -> 2/3 ~= 0.6667
    assert df.loc["MAJOR_EXPANSION", "relative_ceiling_change"] == pytest.approx(
        2 / 3, abs=1e-4
    )
    # CONTRACTION: ceiling_change=-1e7, prev=6e7 -> -1/6 ~= -0.1667
    assert df.loc["CONTRACTION", "relative_ceiling_change"] == pytest.approx(
        -1 / 6, abs=1e-4
    )
    # NEW_AWARD: prev_potential_value NULL -> relative NULL
    assert pd.isna(df.loc["NEW_AWARD", "relative_ceiling_change"])


def test_event_columns_fall_back_to_null_when_prev_potential_value_absent(memory_db):
    """
    Unit-test mocks that don't carry ``prev_potential_value`` (most of
    them) should still get all 4 event columns -- but
    ``relative_ceiling_change`` will be NULL across the board because it
    has no divisor. This keeps the function backward-compatible with
    the existing test fixtures.
    """
    import pandas as pd

    memory_db.execute("""
        CREATE TABLE themed_minimal (
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
            award_type VARCHAR,
            ceiling_change DOUBLE
        )
    """)
    memory_db.execute("""
        INSERT INTO themed_minimal VALUES
          ('k1', 'p1', 5e8, 6e6, NULL, '2023-01-01', '2024-01-01', '2023-06-01', 100, 1e10, 'ok', 'Industrials', 1, 'FUNDING_INCREASE', 'DEFINITIVE CONTRACT', 2e8)
    """)
    res = calculate_alpha_signals(memory_db.table("themed_minimal"))
    df = res.df().set_index("contract_transaction_unique_key")

    # All 4 event columns are present in the output schema...
    for col in (
        "event_class",
        "ceiling_change_log_dollars",
        "ceiling_change_pct_of_mcap",
        "relative_ceiling_change",
    ):
        assert col in df.columns, f"missing column: {col}"
    # event_class is correctly classified
    assert df.loc["k1", "event_class"] == "MAJOR_EXPANSION"
    # ceiling_change_log_dollars and pct_of_mcap work fine
    assert df.loc["k1", "ceiling_change_log_dollars"] == pytest.approx(8.30103, abs=1e-4)
    assert df.loc["k1", "ceiling_change_pct_of_mcap"] == pytest.approx(0.02)
    # ...but relative_ceiling_change falls back to NULL because no prev
    assert pd.isna(df.loc["k1", "relative_ceiling_change"])
