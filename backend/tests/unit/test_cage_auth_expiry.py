"""
Unit tests for the CAGE auth-expiry detection path (M1.6).

Covers:

* ``_is_auth_failure_response`` correctly classifies the three kinds of
  auth-failure responses CAGE returns on an expired session.
* ``fetch_html`` raises ``CageAuthExpiredError`` when the response looks
  like a terms-of-use redirect or Cloudflare challenge.
* ``_validate_credentials`` raises ``CageAuthExpiredError`` (not
  ``SystemExit``) on missing / invalid / blocked sessions.

No network calls; everything is mocked.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
import requests

from backend.app.services.providers import cage_scraper as cs


def _mock_response(
    *,
    url: str = "https://cage.dla.mil/Search/Results?q=test&page=1",
    text: str = "<html>ok</html>",
) -> MagicMock:
    """Build a ``requests.Response``-like mock."""
    resp = MagicMock(spec=requests.Response)
    resp.url = url
    resp.text = text
    resp.raise_for_status = MagicMock()
    return resp


# ---------------------------------------------------------------------------
# _is_auth_failure_response
# ---------------------------------------------------------------------------


def test_is_auth_failure_detects_agree_redirect():
    resp = _mock_response(url="https://cage.dla.mil/agree?return=/Search")
    assert cs._is_auth_failure_response(resp) is True


def test_is_auth_failure_detects_cloudflare_challenge():
    resp = _mock_response(
        text="<html><body>Verifying your identity, please wait...</body></html>"
    )
    assert cs._is_auth_failure_response(resp) is True


def test_is_auth_failure_detects_access_denied():
    resp = _mock_response(text="<html>Access Denied by Cloudflare</html>")
    assert cs._is_auth_failure_response(resp) is True


def test_is_auth_failure_allows_happy_path():
    """Normal CAGE search results page -- no auth-failure signatures."""
    resp = _mock_response(
        text="<html><a href='/Search/Details?id=123'>Details</a></html>"
    )
    assert cs._is_auth_failure_response(resp) is False


def test_is_auth_failure_none_response_is_failure():
    assert cs._is_auth_failure_response(None) is True


# ---------------------------------------------------------------------------
# fetch_html
# ---------------------------------------------------------------------------


def test_fetch_html_raises_cage_auth_expired_on_agree_redirect():
    """Terms-acceptance page must surface as CageAuthExpiredError, not silent failure."""
    session = MagicMock()
    session.get.return_value = _mock_response(
        url="https://cage.dla.mil/agree?x=1", text="<html>Please agree</html>"
    )
    with pytest.raises(cs.CageAuthExpiredError):
        cs.fetch_html(session, "https://cage.dla.mil/Search/Results?q=X&page=1")


def test_fetch_html_raises_cage_auth_expired_on_cloudflare():
    session = MagicMock()
    session.get.return_value = _mock_response(
        text="<html>Verifying your identity, please wait</html>"
    )
    with pytest.raises(cs.CageAuthExpiredError):
        cs.fetch_html(session, "https://cage.dla.mil/Search/Results?q=X&page=1")


def test_fetch_html_happy_path_returns_text():
    session = MagicMock()
    session.get.return_value = _mock_response(text="<html>ok results</html>")
    out = cs.fetch_html(session, "https://cage.dla.mil/Search/Results?q=X&page=1")
    assert "ok results" in out


# ---------------------------------------------------------------------------
# _validate_credentials
# ---------------------------------------------------------------------------


def test_validate_credentials_missing_env_raises_auth_error(monkeypatch):
    monkeypatch.setattr(cs, "SESSION_COOKIE", None, raising=False)
    monkeypatch.setattr(cs, "VERIFICATION_TOKEN", "abc", raising=False)
    with pytest.raises(cs.CageAuthExpiredError) as exc_info:
        cs._validate_credentials()
    assert "Missing CAGE scraper credentials" in str(exc_info.value)


def test_validate_credentials_rejects_agree_redirect(monkeypatch):
    monkeypatch.setattr(cs, "SESSION_COOKIE", "cookie-value", raising=False)
    monkeypatch.setattr(cs, "VERIFICATION_TOKEN", "token-value", raising=False)
    with patch.object(cs.requests, "get") as patched_get:
        patched_get.return_value = _mock_response(
            url="https://cage.dla.mil/agree", text="Please agree to terms"
        )
        with pytest.raises(cs.CageAuthExpiredError) as exc_info:
            cs._validate_credentials()
    assert "invalid, expired, or blocked" in str(exc_info.value)


def test_validate_credentials_wraps_network_errors(monkeypatch):
    monkeypatch.setattr(cs, "SESSION_COOKIE", "cookie", raising=False)
    monkeypatch.setattr(cs, "VERIFICATION_TOKEN", "token", raising=False)
    with patch.object(
        cs.requests, "get", side_effect=requests.exceptions.ConnectTimeout("boom")
    ):
        with pytest.raises(cs.CageAuthExpiredError) as exc_info:
            cs._validate_credentials()
    assert "Network failure" in str(exc_info.value)


def test_validate_credentials_happy_path_returns_none(monkeypatch):
    monkeypatch.setattr(cs, "SESSION_COOKIE", "cookie", raising=False)
    monkeypatch.setattr(cs, "VERIFICATION_TOKEN", "token", raising=False)
    with patch.object(cs.requests, "get") as patched_get:
        patched_get.return_value = _mock_response(
            url="https://cage.dla.mil/Search/Results?q=test&page=1",
            text="<html><a href='/Search/Details?id=1'>Details</a></html>",
        )
        # Should not raise.
        assert cs._validate_credentials() is None
