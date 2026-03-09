import pytest
import time
import requests
import duckdb
from unittest.mock import patch, MagicMock

from backend.app.services.providers.base import RateLimiter, with_retry, NetworkException
from backend.app.services.providers.openfigi import apply_deterministic_selection
from backend.app.services.providers.cage_scraper import parse_cage_details
from backend.src.io import ensure_cache_tables, upsert_cached_entity_hierarchy, get_cached_entity_hierarchy

def test_proactive_rate_limiter():
    limiter = RateLimiter(max_requests=2, time_window=0.5)
    
    start = time.time()
    limiter.wait() # 1st allowed immediately
    limiter.wait() # 2nd allowed immediately
    limiter.wait() # 3rd must wait ~0.5s
    end = time.time()
    
    elapsed = end - start
    assert elapsed >= 0.5, f"Limiter didn't block long enough: {elapsed}"

class MockResponse:
    def __init__(self, status_code, headers=None):
        self.status_code = status_code
        self.headers = headers or {}
        
    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.exceptions.HTTPError(response=self)

def test_reactive_retry_success_after_429():
    mock_calls = []
    
    @with_retry(max_attempts=3, base_delay=0.1)
    def flaky_func():
        mock_calls.append(1)
        if len(mock_calls) == 1:
            resp = MockResponse(429, headers={"Retry-After": "0"}) # 0 so test runs fast
            e = requests.exceptions.RequestException(response=resp)
            raise e
        return "success"
        
    result = flaky_func()
    assert result == "success"
    assert len(mock_calls) == 2

def test_reactive_retry_terminal_failure():
    @with_retry(max_attempts=2, base_delay=0.01)
    def failing_func():
        resp = MockResponse(500)
        e = requests.exceptions.RequestException(response=resp)
        raise e
        
    with pytest.raises(NetworkException) as excinfo:
        failing_func()
    assert excinfo.value.status_code == 500

def test_openfigi_deterministic_selection():
    mock_data = {
        "data": [
            {"ticker": "MSFT", "exchCode": "LSE", "securityType": "Common Stock"},
            {"ticker": "MSFT", "exchCode": "US", "securityType": "Common Stock"},
            {"ticker": "MSF", "exchCode": "US", "securityType": "Common Stock"}
        ]
    }
    # Should prioritize Common Stock, US, and then alphabetical (MSF < MSFT)
    res = apply_deterministic_selection(mock_data)
    assert res is not None
    assert res["ticker"] == "MSF"

def test_cage_scraper_parsing():
    html = """
    <table class="detail-table">
        <tr><td class="detail-left-col">Legal Business Name</td><td class="detail-right-col">TEST CORP</td></tr>
    </table>
    <div id="detail_topsection">
        <label>CAGE</label><span>12345</span>
        <label>CAGE Update Date</label><span>01/15/2023</span>
    </div>
    <div id="ownership">
        <div class="subsection_header">Highest Level Owner</div>
        <div class="data">
            <label>Company Name</label><span>PARENT CORP</span>
            <label>CAGE</label><span>67890</span>
            <label>CAGE Last Updated</label><span>02/20/2023</span>
        </div>
    </div>
    """
    res = parse_cage_details(html)
    assert res["cage_business_name"] == "TEST CORP"
    assert res["cage_update_date"] == "2023-01-15"
    assert res["is_highest"] is False
    assert res["highest_level_owner_name"] == "PARENT CORP"
    assert res["highest_level_cage_code"] == "67890"

def test_duckdb_cache_upsert():
    conn = duckdb.connect(':memory:')
    ensure_cache_tables(conn)
    
    data1 = {
        "uei": "UEI1",
        "cage_code": "C1",
        "cage_business_name": "N1",
        "cage_update_date": None,
        "is_highest": True,
        "highest_level_owner_name": "N1",
        "highest_level_cage_code": "C1",
        "highest_level_cage_update_date": None,
        "result_status": "success",
        "last_verified": "2023-01-01 12:00:00"
    }
    upsert_cached_entity_hierarchy(conn, data1)
    
    fetched = get_cached_entity_hierarchy(conn, "UEI1")
    assert fetched["cage_code"] == "C1"
    
    # Upsert with update
    data1["cage_code"] = "C2"
    upsert_cached_entity_hierarchy(conn, data1)
    fetched2 = get_cached_entity_hierarchy(conn, "UEI1")
    assert fetched2["cage_code"] == "C2"
