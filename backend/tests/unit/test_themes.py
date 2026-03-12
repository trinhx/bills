import pytest
import duckdb
from backend.src.transform import normalize_naics, normalize_naics_keywords, normalize_psc, derive_deliverable

@pytest.fixture
def memory_db():
    conn = duckdb.connect(':memory:')
    yield conn
    conn.close()

def test_normalize_naics(memory_db):
    memory_db.execute("""
        CREATE TABLE mock_awards (
            award_id_piid VARCHAR,
            naics_code VARCHAR
        )
    """)
    memory_db.execute("""
        INSERT INTO mock_awards VALUES 
        ('id1', '12345'),    -- missing leading zero
        ('id2', ' 123456 '), -- whitespace
        ('id3', NULL)        -- null
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
        ('012345', 'Title1', 'Desc1'),
        ('123456', 'Title2', 'Desc2')
    """)
    
    rel_awards = memory_db.table("mock_awards")
    rel_naics = memory_db.table("mock_naics")
    
    res_rel = normalize_naics(rel_awards, rel_naics)
    results = res_rel.fetchall()
    
    res_dict = {row[0]: row for row in results}
    
    # id1 should have padded naics -> 012345 -> joins Title1
    assert res_dict['id1'][1] == '012345'
    assert res_dict['id1'][2] == 'Title1'
    assert res_dict['id1'][3] == 'Desc1'
    
    # id2 should have trimmed naics -> 123456 -> joins Title2
    assert res_dict['id2'][1] == '123456'
    assert res_dict['id2'][2] == 'Title2'
    assert res_dict['id2'][3] == 'Desc2'
    
    # id3 null
    assert res_dict['id3'][1] is None
    assert res_dict['id3'][2] is None
    assert res_dict['id3'][3] is None

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
            psc_category VARCHAR,
            psc_level_1_category VARCHAR
        )
    """)
    memory_db.execute("""
        INSERT INTO mock_psc VALUES 
        ('R499', 'Support', 'Includes support', 'Service', 'Professional Services'),
        ('R500', 'IT', 'Includes IT', 'Service', NULL)
    """)
    
    rel_awards = memory_db.table("mock_awards_psc")
    rel_psc = memory_db.table("mock_psc")
    
    res_rel = normalize_psc(rel_awards, rel_psc)
    res_with_deliverable = derive_deliverable(res_rel)
    results = res_with_deliverable.fetchall()
    
    # cols: piid, psc_code, name, includes, cat, lvl1_cat, deliverable
    res_dict = {row[0]: row for row in results}
    
    # i1: R499 -> level 1 available -> Professional Services
    assert res_dict['i1'][1] == 'R499'
    assert res_dict['i1'][2] == 'Support'
    assert res_dict['i1'][4] == 'Service'
    assert res_dict['i1'][5] == 'Professional Services'
    assert res_dict['i1'][6] == 'Professional Services' # deliverable
    
    # i2: R500 -> level 1 NULL -> Service
    assert res_dict['i2'][1] == 'R500'
    assert res_dict['i2'][4] == 'Service'
    assert res_dict['i2'][5] is None
    assert res_dict['i2'][6] == 'Service' # deliverable
    
    # i3: unkn -> UNKN -> no match -> NULL deliverable
    assert res_dict['i3'][1] == 'UNKN'
    assert res_dict['i3'][2] is None
    assert res_dict['i3'][6] is None # deliverable

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
    kw1 = res_dict['id1'][2]
    assert 'Keyword A' in kw1
    assert 'Keyword B' in kw1
    assert '; ' in kw1
    
    # id2: 123456 -> single keyword
    assert res_dict['id2'][2] == 'Keyword C'
    
    # id3: NULL naics_code -> NULL keywords
    assert res_dict['id3'][2] is None

