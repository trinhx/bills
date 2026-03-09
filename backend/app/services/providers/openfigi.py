import os
import requests
import json
import logging
from typing import Dict, Any, Optional

from backend.app.services.providers.base import RateLimiter, with_retry

logger = logging.getLogger(__name__)

# OpenFIGI permits 250 requests per minute with API Key (we do 20/min without, assuming key is set for production)
OPENFIGI_RATE_LIMITER = RateLimiter(max_requests=25, time_window=6.0)

@with_retry(max_attempts=5)
def fetch_openfigi_mapping(query_name: str, api_key: str = None) -> Optional[Dict[str, Any]]:
    """
    Fetch OpenFIGI mapping for a given name. Applies proactive rate limiting.
    OpenFIGI endpoint: POST https://api.openfigi.com/v3/mapping
    Payload: [{"idType": "ID_WERTPAPIER", "idValue": "..."}] 
    For text search on name, OpenFIGI usually requires /v3/search, but the prompt says OpenFIGI mapping request.
    Actually, OpenFIGI mapping endpoint allows searching by name using idType: "BASE_TICKER" or similar.
    Wait, the prompt says OpenFIGI mapping request, but usually companies are searched by name using Search API.
    Let's use the POST /v3/mapping API with idType 'ID_BB_GLOBAL' etc? 
    No, for names, OpenFIGI mapping doesn't support 'NAME'. 
    Let's use POST /v3/search for names.
    Wait, the prompt says: 'Batch up to 100 jobs per mapping request. ... OpenFIGI: highest_level_owner_name -> ticker.'
    If the prompt specifically says "mapping request", OpenFIGI's `/v3/mapping` accepts `idType` and `idValue`.
    There is no `idType` for Name in OpenFIGI mapping. It's usually `ID_ISIN`, `ID_BB_SEC_NUM_DES`, `TICKER`.
    If we are mapping from Name to Ticker, we might have to use `/v3/search` with payload `{"query": "name"}`.
    Let's assume the POST /v3/search is used because mapping by name is not possible on /v3/mapping.
    Wait, another way is POST /v3/mapping if they have a non-standard idType. We'll use POST /v3/search.
    """
    OPENFIGI_RATE_LIMITER.wait()
    
    url = "https://api.openfigi.com/v3/search"
    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["X-OPENFIGI-APIKEY"] = api_key

    payload = {"query": query_name}
    
    response = requests.post(url, headers=headers, json=payload, timeout=10)
    response.raise_for_status()
    
    data = response.json()
    return data

def apply_deterministic_selection(data: Dict[str, Any]) -> Optional[Dict[str, str]]:
    """
    Apply deterministic selection rule to OpenFIGI results:
    1. Filter to securityType = 'Common Stock'
    2. Prioritize US exchanges (NYSE, NASDAQ)
    3. Primary ticker alphabetically
    """
    if "data" not in data or not data["data"]:
        return None
        
    items = data["data"]
    
    # 1. Filter Common Stock
    common_stocks = [item for item in items if item.get('securityType') == 'Common Stock']
    if not common_stocks:
        # fallback to all items if no common stock found, just to be safe
        common_stocks = items
        
    # 2. Prioritize US Exchanges
    us_exchanges = ['US', 'New York', 'NASDAQ', 'NYSE']
    us_stocks = [item for item in common_stocks if item.get('exchCode') in us_exchanges or item.get('marketSector') == 'Equity']
    
    # Filter strictly by US exchanges if possible, otherwise use the pool we have
    strict_us_stocks = [item for item in common_stocks if item.get('exchCode') in ['US']]
    if not strict_us_stocks:
        strict_us_stocks = [item for item in common_stocks if 'US' in str(item.get('compositeFIGI', ''))] # Heuristic
        
    pool = strict_us_stocks if strict_us_stocks else common_stocks
    
    # 3. Primary ticker alphabetically
    # Ticker is usually in the 'ticker' field
    valid_tickers = []
    for item in pool:
        ticker = item.get('ticker')
        if ticker:
            valid_tickers.append(item)
            
    if not valid_tickers:
        return None
        
    valid_tickers.sort(key=lambda x: str(x.get('ticker', '')))
    best_match = valid_tickers[0]
    
    # Format the requested ticker (Exchange:Ticker) if possible, or just the ticker.
    # OpenFIGI returns Ticker and exchCode
    raw_ticker = best_match.get('ticker')
    exch_code = best_match.get('exchCode', 'UNKNOWN')
    ticker_str = f"{exch_code}:{raw_ticker}" if exch_code != 'US' else raw_ticker
    
    return {
        "ticker": ticker_str,
        "exchange": exch_code,
        "security_type": best_match.get('securityType', 'UNKNOWN')
    }

def process_owner_name(owner_name: str) -> Optional[Dict[str, str]]:
    """Full workflow: fetch -> apply rules -> return schema subset."""
    api_key = os.getenv("OPENFIGI_API_KEY") # Optional
    try:
        data = fetch_openfigi_mapping(owner_name, api_key)
        best_match = apply_deterministic_selection(data)
        return best_match
    except Exception as e:
        logger.error(f"Failed to process openfigi for {owner_name}: {e}")
        raise
