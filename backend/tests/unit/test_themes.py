import pytest
import duckdb
from backend.src.transform import (
    normalize_naics,
    normalize_naics_keywords,
    normalize_psc,
    derive_deliverable,
)


@pytest.fixture
def memory_db():
    conn = duckdb.connect(":memory:")
    yield conn
    conn.close()


def test_normalize_naics(memory_db):
    # Include ``naics_description`` on the awards side to exercise the
    # de-duplication behaviour: the pre-existing raw description must be
    # dropped and replaced by the cleaned lookup value under the same name.
    memory_db.execute("""
        CREATE TABLE mock_awards (
            award_id_piid VARCHAR,
            naics_code VARCHAR,
            naics_description VARCHAR
        )
    """)
    memory_db.execute("""
        INSERT INTO mock_awards VALUES 
        ('id1', '12345', 'RAW CSV DESC 1'),    -- missing leading zero
        ('id2', ' 123456 ', 'RAW CSV DESC 2'), -- whitespace
        ('id3', NULL, 'RAW CSV DESC 3')        -- null naics
    """)

    memory_db.execute("""
        CREATE TABLE mock_naics (
            naics_code VARCHAR,
            naics_title VARCHAR,
            naics_description VARCHAR
        )
    """)
    memory_db.execute("""
        INSERT INTO mock_naics VALUES 
        ('012345', 'Title1', 'Desc1 Cross-References.'),
        ('123456', 'Title2', 'Desc2 Cross-References. Establishments primarily engaged in--')
    """)

    rel_awards = memory_db.table("mock_awards")
    rel_naics = memory_db.table("mock_naics")

    res_rel = normalize_naics(rel_awards, rel_naics)

    # Contract: exactly one naics_description column, no _1 suffix anywhere.
    cols = res_rel.columns
    assert cols.count("naics_description") == 1, cols
    assert "naics_description_1" not in cols, cols
    assert "naics_title" in cols

    # Build a dict keyed by award_id_piid using column names (order-independent).
    import pandas as pd

    df = res_rel.df().set_index("award_id_piid")

    # id1 should have padded naics -> 012345 -> joins Title1, desc replaced
    assert df.loc["id1", "naics_code"] == "012345"
    assert df.loc["id1", "naics_title"] == "Title1"
    assert df.loc["id1", "naics_description"] == "Desc1"

    # id2 should have trimmed naics -> 123456 -> joins Title2, desc replaced
    assert df.loc["id2", "naics_code"] == "123456"
    assert df.loc["id2", "naics_title"] == "Title2"
    assert df.loc["id2", "naics_description"] == "Desc2"

    # id3: null naics yields no join match
    assert pd.isna(df.loc["id3", "naics_code"])
    assert pd.isna(df.loc["id3", "naics_title"])
    assert pd.isna(df.loc["id3", "naics_description"])


def test_normalize_psc_and_deliverable(memory_db):
    memory_db.execute("""
        CREATE TABLE mock_awards_psc (
            award_id_piid VARCHAR,
            product_or_service_code VARCHAR
        )
    """)
    memory_db.execute("""
        INSERT INTO mock_awards_psc VALUES 
        ('i1', ' R499 '), -- whitespace, lower
        ('i2', 'r500'),   -- lowercase
        ('i3', 'unkn')    -- no lookup
    """)

    memory_db.execute("""
        CREATE TABLE mock_psc (
            psc_code VARCHAR,
            psc_name VARCHAR,
            psc_includes VARCHAR,
            psc_level_1_category VARCHAR
        )
    """)
    memory_db.execute("""
        INSERT INTO mock_psc VALUES 
        ('R499', 'Support', 'Includes support', 'Professional Services'),
        ('R500', 'IT', 'Includes IT', NULL)
    """)

    rel_awards = memory_db.table("mock_awards_psc")
    rel_psc = memory_db.table("mock_psc")

    res_rel = normalize_psc(rel_awards, rel_psc)
    res_with_deliverable = derive_deliverable(res_rel)
    results = res_with_deliverable.fetchall()

    # cols: piid, psc_code, name, includes, lvl1_cat, deliverable
    res_dict = {row[0]: row for row in results}

    # i1: R499 -> level 1 available -> Professional Services
    assert res_dict["i1"][1] == "R499"
    assert res_dict["i1"][2] == "Support"
    assert res_dict["i1"][4] == "Professional Services"
    assert res_dict["i1"][5] == "Professional Services"  # deliverable

    # i2: R500 -> level 1 NULL -> missing lvl1_cat -> deliverable is NULL because psc_category removed
    assert res_dict["i2"][1] == "R500"
    assert res_dict["i2"][4] is None
    assert res_dict["i2"][5] is None  # deliverable fallback removed

    # i3: unkn -> UNKN -> no match -> NULL deliverable
    assert res_dict["i3"][1] == "UNKN"
    assert res_dict["i3"][2] is None
    assert res_dict["i3"][5] is None  # deliverable


def test_normalize_naics_keywords(memory_db):
    memory_db.execute("""
        CREATE TABLE mock_awards_kw (
            award_id_piid VARCHAR,
            naics_code VARCHAR
        )
    """)
    memory_db.execute("""
        INSERT INTO mock_awards_kw VALUES 
        ('id1', '012345'),
        ('id2', '123456'),
        ('id3', NULL)
    """)

    memory_db.execute("""
        CREATE TABLE mock_naics_kw (
            "2022 NAICS Code" VARCHAR,
            "2022 NAICS Title" VARCHAR,
            "2022 NAICS Keywords" VARCHAR
        )
    """)
    memory_db.execute("""
        INSERT INTO mock_naics_kw VALUES 
        ('012345', 'Title1', 'Keyword A'),
        ('012345', 'Title1', 'Keyword B'),
        ('123456', 'Title2', 'Keyword C')
    """)

    rel_awards = memory_db.table("mock_awards_kw")
    rel_kw = memory_db.table("mock_naics_kw")

    res_rel = normalize_naics_keywords(rel_awards, rel_kw)
    results = res_rel.fetchall()

    res_dict = {row[0]: row for row in results}

    # id1: 012345 -> two keywords aggregated
    kw1 = res_dict["id1"][2]
    assert "Keyword A" in kw1
    assert "Keyword B" in kw1
    assert "; " in kw1

    # id2: 123456 -> single keyword
    assert res_dict["id2"][2] == "Keyword C"

    # id3: NULL naics_code -> NULL keywords
    assert res_dict["id3"][2] is None
