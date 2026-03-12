"""
CAGE Data Enrichment Pipeline
-----------------------------
This module uses functional programming to enrich Phase 2 "CAGE fields" by scraping
HTML pages from https://cage.dla.mil/ (no public API).

Data flow (network -> parse -> schema dict):
1) CAGE search page:
   - fetch: GET /Search/Results?q={cage_code}&page=1
   - parse: parse_search_results(html) finds the first Details link (e.g., /Search/Details?id=...).
   - purpose: locate the entity Details page for the CAGE code (no schema fields populated here).
   - schema reference: 01_PROJECT_OVERVIEW.md

2) Entity Details page (single-pass, no traversal):
   - fetch: GET {details_uri}
   - parse: parse_cage_details(html) extracts and returns a dict matching the Phase 2 schema fields.
   - schema reference: 01_PROJECT_OVERVIEW.md

   A) Base entity fields (this row's CAGE record)
      Source: <table class="detail-table"> and <div id="detail_topsection"> blocks.
      - cage_business_name (TEXT)
          From "Legal Business Name" in the detail table.
      - cage_update_date (DATE)
          From "CAGE Update Date" in the detail_topsection; parsed/normalized to an ISO date string
          suitable for DuckDB DATE casting.

   B) Ownership / hierarchy fields (highest-level owner as reported on this page)
      Source: <div id="ownership"> -> "Highest Level Owner" subsection.

      Decision rule:
      - If "Highest Level Owner" shows "Information not Available", then the current entity is
        already the highest-level owner. In this case:
          - is_highest = True
          - highest_level_owner_name = cage_business_name
          - highest_level_cage_code = cage_code from detail_topsection
          - highest_level_cage_update_date = cage_update_date

      - Otherwise (Highest Level Owner info is present on the page), then:
          - is_highest (BOOLEAN)
              Set to False (this entity is NOT the highest-level owner).
          - highest_level_owner_name (TEXT)
              From ownership block label "Company Name".
          - highest_level_cage_code (TEXT)
              From ownership block label "CAGE" (text inside the span; ignore the href).
          - highest_level_cage_update_date (DATE)
              From ownership block label "CAGE Last Updated"; parsed/normalized for DuckDB DATE.

Notes:
- This module only produces the CAGE-related fields used in Phase 2 enrichment.
- It intentionally does not implement GLEIF logic and does not traverse additional Details pages.
- See also: 03_PHASE_2_ENRICHMENT.md for Phase 2 execution boundaries and caching rules.
"""

import re
import os
from datetime import datetime
from typing import Dict, Optional, Any
from bs4 import BeautifulSoup
import requests
import logging

logger = logging.getLogger(__name__)

# --- Configuration & Credentials ---
# In production, ensure these are loaded via your core config management
BASE_URL = "https://cage.dla.mil"

import sys

# Environment variables for session management
SESSION_COOKIE = os.getenv("CAGE_PHPSESS")
VERIFICATION_TOKEN = os.getenv("CAGE_VERIFICATION_TOKEN")


def _validate_credentials() -> None:
    """Validate CAGE credentials at runtime (not at import time)."""
    if not SESSION_COOKIE or not VERIFICATION_TOKEN:
        print("\n[!] ERROR: Missing CAGE scraper credentials.")
        print("    Please set CAGE_PHPSESS and CAGE_VERIFICATION_TOKEN in your .env file.")
        sys.exit(1)

    try:
        _test_cookies = {
            "PHPSESS": SESSION_COOKIE,
            "__RequestVerificationToken": VERIFICATION_TOKEN,
            "agree": "True",
        }
        _test_response = requests.get(f"{BASE_URL}/Search/Results?q=test&page=1", cookies=_test_cookies, timeout=10)
        _test_response.raise_for_status()
        if "agree" in _test_response.url.lower() or "verifying your identity" in _test_response.text.lower() or "Access Denied" in _test_response.text:
            print("\n[!] ERROR: CAGE session is invalid, expired, or blocked.")
            print("    Please capture fresh CAGE_PHPSESS and CAGE_VERIFICATION_TOKEN values from your browser and update the .env file.")
            sys.exit(1)
    except Exception as e:
        print(f"\n[!] ERROR: Network failure when validating CAGE scraper access: {e}")
        sys.exit(1)

CAGE_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Cache-Control": "max-age=0",
    "Connection": "keep-alive",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "same-origin",
    "Sec-Fetch-User": "?1",
    "Sec-GPC": "1",
    "Upgrade-Insecure-Requests": "1",
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36",
    "sec-ch-ua": '"Not:A-Brand";v="99", "Brave";v="145", "Chromium";v="145"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Linux"',
}

CAGE_COOKIES = {
    "PHPSESS": SESSION_COOKIE,
    "__RequestVerificationToken": VERIFICATION_TOKEN,
    "agree": "True",
}

# --- Pure Parsing Functions ---


def format_date(raw_date: str) -> str:
    """Converts MM/DD/YYYY to YYYY-MM-DD for DuckDB schema compatibility."""
    try:
        return datetime.strptime(raw_date, "%m/%d/%Y").strftime("%Y-%m-%d")
    except ValueError:
        return raw_date


def parse_search_results(html: str) -> Optional[str]:
    """Extracts the first 'Details' link from the search results page."""
    soup = BeautifulSoup(html, "html.parser")
    link = soup.find(
        "a",
        string=re.compile(r"Details", re.IGNORECASE),
        href=re.compile(r"/Search/Details\?id="),
    )
    return link["href"] if link else None


def parse_cage_details(html: str) -> Dict[str, Any]:
    """
    Extracts base CAGE info and Highest Level Owner info from a single details page.
    Maps exactly to the DuckDB schema fields.
    """
    soup = BeautifulSoup(html, "html.parser")

    # 1. Extract Base Entity Information
    cage_business_name, cage_code, cage_update_date = "", "", ""

    td_label = soup.find(
        "td",
        class_="detail-left-col",
        string=re.compile(r"Legal Business Name", re.IGNORECASE),
    )
    if td_label and td_label.find_next_sibling("td", class_="detail-right-col"):
        cage_business_name = td_label.find_next_sibling(
            "td", class_="detail-right-col"
        ).get_text(strip=True)

    top_section = soup.find("div", id="detail_topsection")
    if top_section:
        c_label = top_section.find("label", string=re.compile(r"^CAGE$", re.IGNORECASE))
        if c_label and c_label.find_next_sibling("span"):
            cage_code = c_label.find_next_sibling("span").get_text(strip=True)

        d_label = top_section.find(
            "label", string=re.compile(r"CAGE Update Date", re.IGNORECASE)
        )
        if d_label and d_label.find_next_sibling("span"):
            cage_update_date = format_date(
                d_label.find_next_sibling("span").get_text(strip=True)
            )

    # 2. Assume Base Entity is Highest Level initially
    is_highest = True
    immediate_level_owner = False
    highest_level_owner_name = cage_business_name
    highest_level_cage_code = cage_code
    highest_level_cage_update_date = cage_update_date

    # 3. Check Ownership Block to see if a parent exists
    ownership_div = soup.find("div", id="ownership")
    if ownership_div:
        highest_header = ownership_div.find(
            "div",
            class_="subsection_header",
            string=re.compile(r"Highest Level Owner", re.IGNORECASE),
        )
        if highest_header:
            data_div = highest_header.find_next_sibling("div", class_="data")

            # If information IS available, this entity has a higher parent
            if data_div and not data_div.find(
                string=re.compile(r"Information not Available", re.IGNORECASE)
            ):
                is_highest = False

                name_label = data_div.find(
                    "label", string=re.compile(r"Company Name", re.IGNORECASE)
                )
                if name_label and name_label.find_next_sibling("span"):
                    highest_level_owner_name = name_label.find_next_sibling(
                        "span"
                    ).get_text(strip=True)

                hcage_label = data_div.find(
                    "label", string=re.compile(r"^CAGE$", re.IGNORECASE)
                )
                if hcage_label and hcage_label.find_next_sibling("span"):
                    highest_level_cage_code = hcage_label.find_next_sibling(
                        "span"
                    ).get_text(strip=True)

                hdate_label = data_div.find(
                    "label", string=re.compile(r"CAGE Last Updated", re.IGNORECASE)
                )
                if hdate_label and hdate_label.find_next_sibling("span"):
                    highest_level_cage_update_date = format_date(
                        hdate_label.find_next_sibling("span").get_text(strip=True)
                    )
            elif data_div:
                # Highest level owner information is not available. Check for immediate level owner.
                immediate_header = ownership_div.find(
                    "div",
                    class_="subsection_header",
                    string=re.compile(r"Immediate(?: Level)? Owner", re.IGNORECASE),
                )
                if immediate_header:
                    imm_data_div = immediate_header.find_next_sibling("div", class_="data")
                    if imm_data_div and not imm_data_div.find(
                        string=re.compile(r"Information not Available", re.IGNORECASE)
                    ):
                        is_highest = False
                        immediate_level_owner = True

                        name_label = imm_data_div.find(
                            "label", string=re.compile(r"Company Name", re.IGNORECASE)
                        )
                        if name_label and name_label.find_next_sibling("span"):
                            highest_level_owner_name = name_label.find_next_sibling(
                                "span"
                            ).get_text(strip=True)

                        hcage_label = imm_data_div.find(
                            "label", string=re.compile(r"^CAGE$", re.IGNORECASE)
                        )
                        if hcage_label and hcage_label.find_next_sibling("span"):
                            highest_level_cage_code = hcage_label.find_next_sibling(
                                "span"
                            ).get_text(strip=True)

                        hdate_label = imm_data_div.find(
                            "label", string=re.compile(r"CAGE Last Updated", re.IGNORECASE)
                        )
                        if hdate_label and hdate_label.find_next_sibling("span"):
                            highest_level_cage_update_date = format_date(
                                hdate_label.find_next_sibling("span").get_text(strip=True)
                            )

    result = {
        "cage_business_name": cage_business_name,
        "cage_update_date": cage_update_date,
        "is_highest": is_highest,
        "immediate_level_owner": immediate_level_owner,
        "highest_level_owner_name": highest_level_owner_name,
        "highest_level_cage_code": highest_level_cage_code,
        "highest_level_cage_update_date": highest_level_cage_update_date,
    }
    
    logger.debug(f"Extracted CAGE fields: {result}")
    return result


# --- Side-Effect Functions (HTTP Requests) ---


def fetch_html(session: requests.Session, url: str) -> str:
    """Executes HTTP GET request and returns HTML text."""
    logger.debug(f"CAGE API Request: GET {url}")
    response = session.get(url, timeout=10)
    response.raise_for_status()
    return response.text


# --- Workflow Composition ---


def enrich_cage_data(
    cage_code: str, headers: Dict[str, str], cookies: Dict[str, str]
) -> Optional[Dict[str, Any]]:
    """Main orchestrator function for the single-pass CAGE enrichment pipeline."""
    with requests.Session() as session:
        session.headers.update(headers)
        session.cookies.update(cookies)

        # Step 1: Search CAGE code and extract the details URI
        search_html = fetch_html(session, f"{BASE_URL}/Search/Results?q={cage_code}&page=1")
        details_uri = parse_search_results(search_html)

        if not details_uri:
            return None

        # Step 2: Fetch the single details page and parse all hierarchy info
        details_html = fetch_html(session, f"{BASE_URL}{details_uri}")
        return parse_cage_details(details_html)


# --- Entry Point ---
if __name__ == "__main__":
    result = enrich_cage_data("KMEVRAKJVBD3", CAGE_HEADERS, CAGE_COOKIES)
    print(result)
