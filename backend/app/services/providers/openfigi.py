import os
import requests
import json
import logging
import re
from typing import Dict, Any, Optional

from backend.app.services.providers.base import RateLimiter, with_retry

logger = logging.getLogger(__name__)

# OpenFIGI permits 25 requests per minute with API Key (we do 24/min proactively to be safe)
OPENFIGI_RATE_LIMITER = RateLimiter(max_requests=2, time_window=5.0)

@with_retry(max_attempts=3, base_delay=5.0, max_delay=60.0)
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

    payload = {
        "query": query_name,
        "securityType": "Common Stock",
        "marketSecDes": "Equity",
        "exchCode": "US"
    }
    
    response = requests.post(url, headers=headers, json=payload, timeout=10)
    response.raise_for_status()
    
    data = response.json()
    return data

def normalize_company_name(name: str) -> str:
    """
    Sanitize federal vendor names to improve OpenFIGI matching.
    Strips noise like ', INC.' or ' CORPORATION' down to base equivalents.
    """
    name = name.upper().strip()
    
    # Remove all punctuation except amptersands and spaces
    name = re.sub(r'[^\w\s&]', ' ', name)
    
    # Normalize extra spaces
    name = re.sub(r'\s+', ' ', name).strip()
    
    # Common legal entity mappings based on OpenFIGI's database tendencies
    substitutions = [
        (r'\bINCORPORATED\b', 'INC'),
        (r'\bCORPORATION\b', 'CORP'),
        (r'\bCOMPANY\b', 'CO'),
        (r'\bLIMITED LIABILITY COMPANY\b', 'LLC'),
        (r'\bLIMITED LIABILITY CO\b', 'LLC'),
        (r'\bLIMITED PARTNERSHIP\b', 'LP'),
        (r'\bLIMITED\b', 'LTD'),
    ]
    
    for pattern, replacement in substitutions:
        name = re.sub(pattern, replacement, name)
        
    return name.strip()

def apply_deterministic_selection(data: Dict[str, Any]) -> Optional[Dict[str, str]]:
    """
    Since the API payload strictly requests Common Stock/Equity on the US exchange,
    we just need to take the first valid result returned by OpenFIGI.
    """
    if "data" not in data or not data["data"]:
        return None
        
    items = data["data"]
    
    # Filter out empty tickers just in case
    valid_tickers = [item for item in items if item.get('ticker')]
            
    if not valid_tickers:
        return None
        
    # Sort alphabetically as a fallback deterministic tie-breaker
    valid_tickers.sort(key=lambda x: str(x.get('ticker', '')))
    best_match = valid_tickers[0]
    
    ticker_str = best_match.get('ticker')
    exch_code = best_match.get('exchCode')
    
    return {
        "ticker": ticker_str,
        "exchange": exch_code,
        "security_type": best_match.get('securityType', 'UNKNOWN')
    }

def process_owner_name(owner_name: str) -> Optional[Dict[str, str]]:
    """Full workflow: fetch -> apply rules -> return schema subset."""
    api_key = os.getenv("OPENFIGI_API_KEY") # Optional
    try:
        query_name = normalize_company_name(owner_name)
        logger.debug(f"Normalized '{owner_name}' to -> '{query_name}' for OpenFIGI query")
        data = fetch_openfigi_mapping(query_name, api_key)
        best_match = apply_deterministic_selection(data)
        logger.debug(f"Extracted OpenFIGI fields for '{owner_name}': {best_match}")
        return best_match
    except Exception as e:
        logger.error(f"Failed to process openfigi for {owner_name}: {e}")
        raise
