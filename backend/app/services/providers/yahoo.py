import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
import logging
from typing import Dict, Any, Optional

from backend.app.services.providers.base import RateLimiter, with_retry

logger = logging.getLogger(__name__)

# Yahoo Finance is throttled/unstable, so we implement a conservative rate limit
YAHOO_RATE_LIMITER = RateLimiter(max_requests=10, time_window=10.0)

@with_retry(max_attempts=3, base_delay=2.0)
def fetch_yahoo_data(ticker: str, action_date: str) -> Optional[Dict[str, Any]]:
    """
    Fetch Yahoo Finance Market Cap approximation.
    1. historical Close price for the action_date (or the closest available prior trading day).
    2. most recent sharesOutstanding from the ticker's info dictionary.
    3. Calculate market_cap = historical_close * sharesOutstanding.
    4. Fetch sector from info.
    """
    YAHOO_RATE_LIMITER.wait()
    
    try:
        t = yf.Ticker(ticker)
        info = t.info
        
        # sharesOutstanding might not exist for some instruments
        shares_outstanding = info.get('sharesOutstanding', None)
        sector = info.get('sector', 'UNKNOWN')
        industry = info.get('industry', 'UNKNOWN')
        
        if not shares_outstanding:
            logger.warning(f"No sharesOutstanding found for {ticker}")
            return {"market_cap": None, "sector": sector, "industry": industry}
            
        # Parse action_date
        target_dt = pd.to_datetime(action_date).tz_localize(None)
        
        # We need the closest prior trading day. Fetch a 10-day window ending on action_date + 1 day
        start_dt = target_dt - timedelta(days=10)
        end_dt = target_dt + timedelta(days=1)
        
        hist = t.history(start=start_dt.strftime('%Y-%m-%d'), end=end_dt.strftime('%Y-%m-%d'))
        
        if hist.empty:
            logger.warning(f"No history found for {ticker} near {action_date}")
            return {"market_cap": None, "sector": sector, "industry": industry}
            
        # Filter to dates <= target_dt
        hist.index = hist.index.tz_localize(None)
        valid_history = hist[hist.index <= target_dt]
        
        if valid_history.empty:
            logger.warning(f"No history <= {action_date} found for {ticker}")
            return {"market_cap": None, "sector": sector, "industry": industry}
            
        # Get the closest prior trading day (last row of valid_history)
        closest_row = valid_history.iloc[-1]
        historical_close = closest_row['Close']
        
        market_cap = historical_close * shares_outstanding
        
        result = {
            "market_cap": float(market_cap),
            "sector": sector,
            "industry": industry
        }
        logger.debug(f"Extracted Yahoo fields for {ticker}: {result}")
        return result
    except Exception as e:
        logger.error(f"Error fetching Yahoo Finance data for {ticker}: {e}")
        # Reraise so @with_retry can handle it for explicit rate limiting errors, 
        # or we could catch 404/Not Found here. Let's just raise.
        raise
