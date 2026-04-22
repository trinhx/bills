"""Unit tests for pure-transform helpers in ``backend.src.transform``."""

import duckdb
import pytest

from backend.app.core.version import PIPELINE_VERSION
from backend.src.transform import join_entity_hierarchy, stamp_pipeline_metadata


@pytest.fixture
def memory_db():
    conn = duckdb.connect(":memory:")
    yield conn
    conn.close()


def test_join_entity_hierarchy_excludes_cache_metadata(memory_db):
    """
    Regression guard: ``join_entity_hierarchy`` must not leak the internal
    cache bookkeeping columns (``result_status``, ``last_verified``) onto
    the awards relation. These used to propagate all the way into the
    final CSV output.
    """
    memory_db.execute(
        """
        CREATE TABLE awards (
            award_id_piid VARCHAR,
            cage_code VARCHAR
        )
        """
    )
    memory_db.execute(
        "INSERT INTO awards VALUES ('a1', 'C1'), ('a2', 'C2'), ('a3', NULL)"
    )
    memory_db.execute(
        """
        CREATE TABLE hierarchy (
            cage_code VARCHAR,
            cage_business_name VARCHAR,
            cage_update_date DATE,
            is_highest BOOLEAN,
            immediate_level_owner BOOLEAN,
            highest_level_owner_name VARCHAR,
            highest_level_cage_code VARCHAR,
            highest_level_cage_update_date DATE,
            result_status VARCHAR,
            last_verified TIMESTAMP
        )
        """
    )
    memory_db.execute(
        """
        INSERT INTO hierarchy VALUES
          ('C1', 'ACME CORP', DATE '2023-01-01', TRUE, FALSE,
           'ACME CORP', 'C1', DATE '2023-01-01', 'success', TIMESTAMP '2023-01-01 12:00:00')
        """
    )

    awards_rel = memory_db.table("awards")
    hierarchy_rel = memory_db.table("hierarchy")
    joined = join_entity_hierarchy(awards_rel, hierarchy_rel)

    cols = joined.columns
    # Cache bookkeeping columns must not appear in the output schema.
    assert "result_status" not in cols, cols
    assert "last_verified" not in cols, cols

    # Domain columns must survive the join.
    for expected in (
        "award_id_piid",
        "cage_code",
        "cage_business_name",
        "cage_update_date",
        "is_highest",
        "immediate_level_owner",
        "highest_level_owner_name",
        "highest_level_cage_code",
        "highest_level_cage_update_date",
    ):
        assert expected in cols, f"missing {expected} in {cols}"

    # Row integrity: all 3 award rows preserved, left join fills NULL for misses.
    import pandas as pd

    df = joined.df().set_index("award_id_piid")
    assert df.loc["a1", "cage_business_name"] == "ACME CORP"
    assert pd.isna(df.loc["a2", "cage_business_name"])
    assert pd.isna(df.loc["a3", "cage_business_name"])


def test_stamp_pipeline_metadata_adds_version_and_timestamp(memory_db):
    """
    ``stamp_pipeline_metadata`` must append ``pipeline_version`` (literal
    from ``backend.app.core.version``) and ``ingested_at`` (timestamp) to
    every row without disturbing existing columns.
    """
    memory_db.execute("CREATE TABLE t (id INT, name VARCHAR)")
    memory_db.execute("INSERT INTO t VALUES (1, 'a'), (2, 'b')")

    stamped = stamp_pipeline_metadata(memory_db.table("t"))
    cols = stamped.columns
    assert "pipeline_version" in cols
    assert "ingested_at" in cols
    # Existing columns preserved and positioned first.
    assert cols[:2] == ["id", "name"]

    df = stamped.df()
    assert (df["pipeline_version"] == PIPELINE_VERSION).all()
    # ingested_at is a timestamp; all rows get the same value per row
    # (DuckDB evaluates CURRENT_TIMESTAMP once per statement).
    assert df["ingested_at"].notna().all()
