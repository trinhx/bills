import os
import pytest
import duckdb
from pathlib import Path
from unittest.mock import patch

from backend.src.io import ensure_cache_tables
from backend.src.transform import filter_and_select_phase1
from backend.scripts.enrich import main as enrich_main
from backend.scripts.themes import main as themes_main
from backend.scripts.signals import main as signals_main

# A small mock dataset representing three scenarios
MOCK_CSV_CONTENT = """contract_transaction_unique_key,award_id_piid,federal_action_obligation,total_dollars_obligated,current_total_value_of_award,potential_total_value_of_award,action_date,solicitation_date,period_of_performance_start_date,period_of_performance_current_end_date,awarding_agency_name,awarding_sub_agency_name,cage_code,recipient_parent_uei,recipient_parent_name,recipient_parent_name_raw,product_or_service_code,product_or_service_code_description,naics_code,naics_description,number_of_offers_received,transaction_description,award_type,extra_ignored_column
txn_happy,piid_happy,100.0,6000000.0,,,2023-01-05,,,,,Agency,Sub,CAGE_HAPPY,UEI_HAPPY,HAPPY PARENT CORP,HAPPY PARENT CORP,D302,IT AND TELECOM,541512,COMPUTER SYSTEMS DESIGN SERVICES,1,Desc,DEFINITIVE CONTRACT,extra
txn_429,piid_429,100.0,6000000.0,,,2023-01-05,,,,,Agency,Sub,CAGE_RETRY,UEI_RETRY,RETRY PARENT CORP,RETRY PARENT CORP,D302,IT AND TELECOM,541512,COMPUTER SYSTEMS DESIGN SERVICES,1,Desc,DEFINITIVE CONTRACT,extra
txn_nocage,piid_nocage,100.0,6000000.0,,,2023-01-05,,,,,Agency,Sub,CAGE_NOPARENT,UEI_NOPARENT,NOPARENT CORP,NOPARENT CORP,D302,IT AND TELECOM,541512,COMPUTER SYSTEMS DESIGN SERVICES,1,Desc,DEFINITIVE CONTRACT,extra
"""

class MockResponse:
    def __init__(self, status_code, json_data=None, text=None, headers=None, url="http://mock.url"):
        self.status_code = status_code
        self._json = json_data or {}
        self.text = text or ""
        self.headers = headers or {}
        self.url = url

    def json(self):
        return self._json

    def raise_for_status(self):
        import requests
        if self.status_code >= 400:
            raise requests.exceptions.HTTPError(f"{self.status_code} Error", response=self)

def mock_requests_get(url, *args, **kwargs):
    if "cage.dla.mil/Search/Results" in url:
        if "CAGE_HAPPY" in url:
            return MockResponse(200, text='<a href="/Search/Details?id=happy">Details</a>')
        elif "CAGE_RETRY" in url:
            return MockResponse(200, text='<a href="/Search/Details?id=retry">Details</a>')
        elif "CAGE_NOPARENT" in url:
            return MockResponse(200, text='<a href="/Search/Details?id=noparent">Details</a>')
        return MockResponse(200, text='<a href="/Search/Details?id=123">Details</a>')
        
    elif "cage.dla.mil/Search/Details" in url:
        if "id=retry" in url:
            return MockResponse(200, text='''
                <table class="detail-table"><tr><td class="detail-left-col">Legal Business Name</td><td class="detail-right-col">RETRY BUSINESS</td></tr></table>
                <div id="detail_topsection"><label>CAGE</label><span>CAGE_RETRY</span><label>CAGE Update Date</label><span>01/01/2023</span></div>
                <div id="ownership"><div class="subsection_header">Highest Level Owner</div><div class="data">
                   <label>Company Name</label><span>RETRY HIGHEST LEVEL OWNER</span>
                   <label>CAGE</label><span>MOCK_H_CAGE</span><label>CAGE Last Updated</label><span>01/01/2023</span>
                </div></div>
            ''')
        elif "id=noparent" in url:
            return MockResponse(200, text='''
                <table class="detail-table"><tr><td class="detail-left-col">Legal Business Name</td><td class="detail-right-col">NOPARENT BUSINESS</td></tr></table>
                <div id="detail_topsection"><label>CAGE</label><span>CAGE_NOPARENT</span><label>CAGE Update Date</label><span>01/01/2023</span></div>
                <div id="ownership"><div class="subsection_header">Highest Level Owner</div><div class="data">Information not Available</div></div>
            ''')
        else:
            return MockResponse(200, text='''
                <table class="detail-table"><tr><td class="detail-left-col">Legal Business Name</td><td class="detail-right-col">HAPPY BUSINESS</td></tr></table>
                <div id="detail_topsection"><label>CAGE</label><span>CAGE_HAPPY</span><label>CAGE Update Date</label><span>01/01/2023</span></div>
                <div id="ownership"><div class="subsection_header">Highest Level Owner</div><div class="data">
                   <label>Company Name</label><span>HAPPY HIGHEST LEVEL OWNER</span>
                   <label>CAGE</label><span>MOCK_H_CAGE</span><label>CAGE Last Updated</label><span>01/01/2023</span>
                </div></div>
            ''')
            
    elif "query2.finance.yahoo.com" in url:
        # Happy Path Mock
        if "CAGE_HAPPY" in url or "HAPPY" in url:
            return MockResponse(200, json_data={"quoteSummary": {"result": [{"price": {"marketCap": {"raw": 1000000000.0}}}]}})
        # Rate limit simulation for Yahoo too just in case
        return MockResponse(200, json_data={"quoteSummary": {"result": [{"price": {"marketCap": {"raw": 500000000.0}}}]}})
    
    return MockResponse(404, text="Not Found")


def mock_requests_post(url, *args, **kwargs):
    if "api.openfigi.com/v3/search" in url:
        query = kwargs.get('json', {}).get('query', '')
        if "HAPPY" in query:
            return MockResponse(200, json_data={"data": [{"ticker": "HPPY", "exchCode": "US", "securityType": "Common Stock"}]})
        elif "RETRY" in query:
            return MockResponse(429, headers={"Retry-After": "2"}, url=url)  # Mock 429
        elif "NOPARENT" in query:
            return MockResponse(200, json_data={"data": []}) # No mapping found
        else:
            return MockResponse(200, json_data={"data": [{"ticker": "MOCK", "exchCode": "US", "securityType": "Common Stock"}]})
    return MockResponse(404)


@pytest.fixture
def integration_env(tmp_path):
    """Sets up a temporary filesystem and mock databases for the pipeline."""
    csv_path = tmp_path / "raw_contracts.csv"
    csv_path.write_text(MOCK_CSV_CONTENT)
    
    db_path = str(tmp_path / "cleaned.duckdb")
    cache_path = str(tmp_path / "cache.duckdb")
    
    conn = duckdb.connect(db_path)
    
    # Run Phase 1 Ingestion: Use explicit typing instead of CSV inference to avoid dtype merge conflicts
    conn.execute("""
        CREATE TABLE raw_awards (
            contract_transaction_unique_key VARCHAR, award_id_piid VARCHAR, federal_action_obligation DOUBLE,
            total_dollars_obligated DOUBLE, current_total_value_of_award DOUBLE, potential_total_value_of_award DOUBLE,
            action_date DATE, solicitation_date DATE, period_of_performance_start_date DATE,
            period_of_performance_current_end_date DATE, awarding_agency_name VARCHAR, awarding_sub_agency_name VARCHAR,
            cage_code VARCHAR, recipient_parent_uei VARCHAR, recipient_parent_name VARCHAR,
            recipient_parent_name_raw VARCHAR, product_or_service_code VARCHAR, product_or_service_code_description VARCHAR,
            naics_code VARCHAR, naics_description VARCHAR, number_of_offers_received DOUBLE,
            transaction_description VARCHAR, award_type VARCHAR, extra_ignored_column VARCHAR
        )
    """)
    conn.execute("""
        INSERT INTO raw_awards VALUES 
        ('txn_happy', 'piid_happy', 100.0, 6000000.0, NULL, NULL, '2023-01-05', NULL, NULL, NULL, 'Agency', 'Sub', 'CAGE_HAPPY', 'UEI_HAPPY', 'HAPPY PARENT CORP', 'HAPPY PARENT CORP', 'D302', 'IT AND TELECOM', '541512', 'COMPUTER SYSTEMS DESIGN SERVICES', 1, 'Desc', 'DEFINITIVE CONTRACT', 'extra'),
        ('txn_429', 'piid_429', 100.0, 6000000.0, NULL, NULL, '2023-01-05', NULL, NULL, NULL, 'Agency', 'Sub', 'CAGE_RETRY', 'UEI_RETRY', 'RETRY PARENT CORP', 'RETRY PARENT CORP', 'D302', 'IT AND TELECOM', '541512', 'COMPUTER SYSTEMS DESIGN SERVICES', 1, 'Desc', 'DEFINITIVE CONTRACT', 'extra'),
        ('txn_nocage', 'piid_nocage', 100.0, 6000000.0, NULL, NULL, '2023-01-05', NULL, NULL, NULL, 'Agency', 'Sub', 'CAGE_NOPARENT', 'UEI_NOPARENT', 'NOPARENT CORP', 'NOPARENT CORP', 'D302', 'IT AND TELECOM', '541512', 'COMPUTER SYSTEMS DESIGN SERVICES', 1, 'Desc', 'DEFINITIVE CONTRACT', 'extra')
    """)
    rel = conn.table("raw_awards")
    filtered_rel = filter_and_select_phase1(rel)
    filtered_rel.create_view("filtered_view", replace=True)
    conn.execute("CREATE TABLE raw_filtered_awards AS SELECT * FROM filtered_view")
    
    # Initialize Cache DB using ATTACH just like production
    conn.execute(f"ATTACH '{cache_path}' AS cache;")
    ensure_cache_tables(conn)
    conn.execute("DETACH cache;")
    conn.close()
    
    yield db_path, cache_path


@patch("requests.Session.get", side_effect=mock_requests_get)
@patch("requests.Session.post", side_effect=mock_requests_post)
@patch("requests.post", side_effect=mock_requests_post)
@patch("requests.get", side_effect=mock_requests_get) # Cover standard requests.get
@patch("backend.app.services.providers.base.time.sleep") # Don't actually sleep in tests
@patch("backend.app.services.providers.openfigi.OPENFIGI_RATE_LIMITER.wait") # Skips proactive waiting
@patch("backend.scripts.enrich.CAGE_COOKIES", {"PHPSESS": "mock", "__RequestVerificationToken": "mock", "agree": "True"})
def test_end_to_end_pipeline(mock_rate_limiter, mock_sleep, mock_rget, mock_rpost, mock_sess_post, mock_sess_get, integration_env, tmp_path):
    db_path, cache_path = integration_env
    out_csv = tmp_path / "signals.csv"
    
    # Mock DuckDB connections to return our temp test files
    def mock_get_cleaned_conn(*args, **kwargs):
        return duckdb.connect(db_path)
        
    def mock_get_cache_conn(*args, **kwargs):
        return duckdb.connect(cache_path)
        
    # We must patch get_cleaned_conn everywhere it is used
    with patch("backend.scripts.enrich.get_cleaned_conn", side_effect=mock_get_cleaned_conn), \
         patch("backend.scripts.enrich.get_cache_conn", side_effect=mock_get_cache_conn), \
         patch("backend.scripts.themes.get_cleaned_conn", side_effect=mock_get_cleaned_conn), \
         patch("backend.scripts.signals.get_cleaned_conn", side_effect=mock_get_cleaned_conn), \
         patch("sys.argv", ["script.py"]):

        # For enrich, we need to mock ATTACH so it attaches the test cache DB, not the prod one
        real_execute = duckdb.DuckDBPyConnection.execute
        def mock_execute(self, query, *args, **kwargs):
            if "ATTACH 'backend/data/cache/cache.duckdb'" in query:
                return real_execute(self, f"ATTACH '{cache_path}' AS cache;", *args, **kwargs)
            return real_execute(self, query, *args, **kwargs)

        with patch.object(duckdb.DuckDBPyConnection, 'execute', new=mock_execute):
            # --- PHASE 2: ENRICHMENT ---
            enrich_main()
            
            # Verify Cache Enriched State
            cache_conn = duckdb.connect(cache_path)
            openfigi_rows = cache_conn.execute("SELECT * FROM cache.cache_openfigi_ticker").fetchall()
            failures = cache_conn.execute("SELECT provider, key, error_type, message, http_status, retry_after_seconds FROM cache.cache_failures").fetchall()
            print("OPENFIGI ROWS:", openfigi_rows)
            print("FAILURES ROWS:", failures)
            assert len(openfigi_rows) > 0
            
            # Rate Limit (429) should be logged in cache_failures with a retry_after_seconds > 0
            failures = cache_conn.execute("SELECT provider, key, http_status, retry_after_seconds FROM cache.cache_failures WHERE http_status=429").fetchall()
            assert len(failures) > 0, "Pipeline should have caught the 429 and logged it to failures table."
            for fail in failures:
                assert fail[3] > 0, "429 Failure must have a retry_after_seconds > 0 to prevent infinite 0-sec retry loop."
                
            cache_conn.close()

            # --- PHASE 3: THEMES ---
            # Themes needs enriched_awards; we renamed it to raw_filtered_awards in ingestion mock for some reason
            themes_main()
            
            # --- PHASE 4: SIGNALS ---
            with patch("sys.argv", ["script.py", "--output", str(out_csv)]):
                signals_main()
    
    assert out_csv.exists(), "Final signals CSV must be generated"
    
    # Load and verify CSV logic
    df = duckdb.read_csv(str(out_csv)).df()
    assert len(df) >= 3, "All 3 initial rows should make it to the final output, and may be multiplied by 1-to-many lookup joins."
    
    # Check that standard fields are present
    assert "contract_transaction_unique_key" in df.columns
    assert "market_cap" in df.columns
    assert "naics_keywords" in df.columns
    assert "alpha_ratio" in df.columns
    assert "acv_signal" in df.columns
