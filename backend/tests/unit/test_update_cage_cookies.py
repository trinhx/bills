"""
Unit tests for ``backend.scripts.utils.update_cage_cookies`` (M1.6.3).

Coverage:

* Curl parsing: extracts PHPSESS + __RequestVerificationToken from real-world
  curl strings (quoted, unquoted, double-quoted); rejects malformed inputs
  with a helpful error.
* .env manipulation: in-place update preserves existing keys and line order;
  creates a timestamped backup; appends keys that don't yet exist; refuses
  to touch files that don't exist.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from backend.scripts.utils.update_cage_cookies import (
    _upsert_env_lines,
    extract_cage_cookies,
    extract_cookie_string,
    parse_cookie_pairs,
    write_env_update,
)


# A representative real-world curl as emitted by Chrome/Brave DevTools.
# Note the trailing backslash-continuations and mix of quoted flag values.
REAL_CURL = r"""curl 'https://cage.dla.mil/Search/Results?q=1RTW6&page=1' \
  -H 'Accept: text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8' \
  -H 'Accept-Language: en-US,en;q=0.5' \
  -H 'Cache-Control: max-age=0' \
  -H 'Connection: keep-alive' \
  -b 'PHPSESS=abc123def456; __RequestVerificationToken=TOK-ZZZ; agree=True; TS01ee81a8=session-trace-data' \
  -H 'Referer: https://cage.dla.mil/search' \
  -H 'User-Agent: Mozilla/5.0' \
  -H 'sec-ch-ua: "Brave";v="147", "Not.A/Brand";v="8"' \
  -H 'sec-ch-ua-mobile: ?0' \
  -H 'sec-ch-ua-platform: "Linux"'
"""


# ---------------------------------------------------------------------------
# Cookie string extraction
# ---------------------------------------------------------------------------


def test_extract_cookie_string_from_real_curl():
    cookie_str = extract_cookie_string(REAL_CURL)
    assert cookie_str is not None
    assert "PHPSESS=abc123def456" in cookie_str
    assert "__RequestVerificationToken=TOK-ZZZ" in cookie_str
    # Headers must NOT leak into the cookie string.
    assert "Accept" not in cookie_str
    assert "Mozilla" not in cookie_str


def test_extract_cookie_string_supports_double_quoted_value():
    curl = 'curl https://example.com -b "PHPSESS=x; __RequestVerificationToken=y"'
    assert extract_cookie_string(curl) == "PHPSESS=x; __RequestVerificationToken=y"


def test_extract_cookie_string_supports_unquoted_token():
    curl = "curl https://example.com --cookie single_cookie=abc"
    assert extract_cookie_string(curl) == "single_cookie=abc"


def test_extract_cookie_string_returns_none_when_missing():
    assert extract_cookie_string("curl https://example.com -H 'Accept: */*'") is None


# ---------------------------------------------------------------------------
# Pair parsing
# ---------------------------------------------------------------------------


def test_parse_cookie_pairs_splits_on_first_equals():
    s = "K=a=b=c; J=ok"
    assert parse_cookie_pairs(s) == {"K": "a=b=c", "J": "ok"}


def test_parse_cookie_pairs_ignores_empty_and_malformed():
    s = "   ; K=v ; ; broken ; J=w; "
    got = parse_cookie_pairs(s)
    assert got == {"K": "v", "J": "w"}


# ---------------------------------------------------------------------------
# High-level extract_cage_cookies
# ---------------------------------------------------------------------------


def test_extract_cage_cookies_happy_path():
    phpsess, token = extract_cage_cookies(REAL_CURL)
    assert phpsess == "abc123def456"
    assert token == "TOK-ZZZ"


def test_extract_cage_cookies_raises_on_missing_cookie_flag():
    curl = "curl https://cage.dla.mil/search -H 'Accept: */*'"
    with pytest.raises(ValueError) as exc_info:
        extract_cage_cookies(curl)
    assert "Couldn't find a -b/--cookie flag" in str(exc_info.value)


def test_extract_cage_cookies_raises_on_missing_phpsess():
    curl = "curl https://cage.dla.mil/search -b '__RequestVerificationToken=xyz; agree=True'"
    with pytest.raises(ValueError) as exc_info:
        extract_cage_cookies(curl)
    assert "PHPSESS" in str(exc_info.value)


def test_extract_cage_cookies_raises_on_missing_token():
    curl = "curl https://cage.dla.mil/search -b 'PHPSESS=abc; agree=True'"
    with pytest.raises(ValueError) as exc_info:
        extract_cage_cookies(curl)
    assert "__RequestVerificationToken" in str(exc_info.value)


def test_extract_cage_cookies_tolerates_long_values():
    """Real __RequestVerificationToken values can be 150+ chars of base64."""
    long_tok = "a" * 200
    curl = f"curl https://x -b 'PHPSESS=short; __RequestVerificationToken={long_tok}; agree=True'"
    _, token = extract_cage_cookies(curl)
    assert token == long_tok


# ---------------------------------------------------------------------------
# .env in-place upsert
# ---------------------------------------------------------------------------


def test_upsert_env_lines_replaces_existing_keys():
    """Existing keys get replaced in place; values are always double-quoted."""
    lines = [
        "# Comment\n",
        "OPENFIGI_API_KEY=old\n",
        "CAGE_PHPSESS=old-session\n",
        "CAGE_VERIFICATION_TOKEN=old-token\n",
        "UNRELATED=yes\n",
    ]
    updated = _upsert_env_lines(
        lines, {"CAGE_PHPSESS": "new-session", "CAGE_VERIFICATION_TOKEN": "new-token"}
    )
    assert updated[0] == "# Comment\n"
    assert updated[1] == "OPENFIGI_API_KEY=old\n"
    assert updated[2] == 'CAGE_PHPSESS="new-session"\n'
    assert updated[3] == 'CAGE_VERIFICATION_TOKEN="new-token"\n'
    assert updated[4] == "UNRELATED=yes\n"


def test_upsert_env_lines_appends_missing_keys():
    lines = ["OTHER=x\n"]
    updated = _upsert_env_lines(
        lines, {"CAGE_PHPSESS": "v1", "CAGE_VERIFICATION_TOKEN": "v2"}
    )
    assert 'CAGE_PHPSESS="v1"\n' in updated
    assert 'CAGE_VERIFICATION_TOKEN="v2"\n' in updated
    # Existing content preserved.
    assert updated[0] == "OTHER=x\n"


def test_upsert_env_lines_preserves_export_prefix():
    """
    Many .env files use shell-export syntax (``export KEY=value``). The
    upsert must match those lines, preserve the ``export`` prefix, and
    NOT append duplicate plain-form entries at the end.
    """
    lines = [
        "export SUPABASE_URL=https://x\n",
        "export CAGE_PHPSESS=old-session\n",
        'export CAGE_VERIFICATION_TOKEN="old-token"\n',
    ]
    updated = _upsert_env_lines(
        lines,
        {"CAGE_PHPSESS": "new-session", "CAGE_VERIFICATION_TOKEN": "new-token"},
    )
    assert updated == [
        "export SUPABASE_URL=https://x\n",
        'export CAGE_PHPSESS="new-session"\n',
        'export CAGE_VERIFICATION_TOKEN="new-token"\n',
    ]
    # Regression guard: no duplicate plain-form entries.
    joined = "".join(updated)
    assert joined.count("CAGE_PHPSESS=") == 1
    assert joined.count("CAGE_VERIFICATION_TOKEN=") == 1


def test_upsert_env_lines_preserves_comments_and_blanks():
    lines = [
        "# Top comment\n",
        "\n",
        "CAGE_PHPSESS=x\n",
        "\n",
        "# Footer\n",
    ]
    updated = _upsert_env_lines(lines, {"CAGE_PHPSESS": "y"})
    assert updated == [
        "# Top comment\n",
        "\n",
        'CAGE_PHPSESS="y"\n',
        "\n",
        "# Footer\n",
    ]


def test_upsert_env_lines_escapes_shell_metacharacters():
    """
    Values are double-quoted and internal double quotes + shell meta-
    characters ($, backtick, backslash) are escaped so the resulting
    .env can be sourced by POSIX shells without side effects.
    """
    lines = ["CAGE_PHPSESS=old\n"]
    updated = _upsert_env_lines(
        lines, {"CAGE_PHPSESS": 'tricky "value" $HOME `cmd` \\ end'}
    )
    # Backslashes escaped first (\\ -> \\\\), then ", $, ` each get one \ added.
    expected = 'CAGE_PHPSESS="tricky \\"value\\" \\$HOME \\`cmd\\` \\\\ end"\n'
    assert updated[0] == expected


# ---------------------------------------------------------------------------
# write_env_update (file IO)
# ---------------------------------------------------------------------------


def test_write_env_update_creates_backup_and_writes(tmp_path: Path):
    env = tmp_path / ".env"
    env.write_text(
        "OPENFIGI_API_KEY=keep\nCAGE_PHPSESS=old\nCAGE_VERIFICATION_TOKEN=old\n"
    )

    backup = write_env_update(
        env,
        {"CAGE_PHPSESS": "new-phpsess", "CAGE_VERIFICATION_TOKEN": "new-token"},
    )

    assert backup is not None
    assert backup.exists()
    # Backup preserves original content verbatim.
    assert "CAGE_PHPSESS=old" in backup.read_text()
    # New .env has the updated values (double-quoted); other keys preserved.
    new_contents = env.read_text()
    assert "OPENFIGI_API_KEY=keep" in new_contents
    assert 'CAGE_PHPSESS="new-phpsess"' in new_contents
    assert 'CAGE_VERIFICATION_TOKEN="new-token"' in new_contents
    assert "CAGE_PHPSESS=old\n" not in new_contents


def test_write_env_update_dry_run_does_nothing(tmp_path: Path):
    env = tmp_path / ".env"
    original = "CAGE_PHPSESS=keep\nCAGE_VERIFICATION_TOKEN=keep\n"
    env.write_text(original)

    backup = write_env_update(
        env,
        {"CAGE_PHPSESS": "would-change"},
        dry_run=True,
    )

    assert backup is None
    # No backups created.
    assert list(tmp_path.glob(".env.backup-*")) == []
    # Original .env untouched.
    assert env.read_text() == original


def test_write_env_update_raises_when_env_missing(tmp_path: Path):
    env = tmp_path / "does-not-exist.env"
    with pytest.raises(FileNotFoundError):
        write_env_update(env, {"K": "V"})


def test_write_env_update_no_backup_flag(tmp_path: Path):
    env = tmp_path / ".env"
    env.write_text("CAGE_PHPSESS=old\nCAGE_VERIFICATION_TOKEN=old\n")

    backup = write_env_update(
        env,
        {"CAGE_PHPSESS": "new", "CAGE_VERIFICATION_TOKEN": "new"},
        no_backup=True,
    )

    assert backup is None
    assert list(tmp_path.glob(".env.backup-*")) == []
    assert 'CAGE_PHPSESS="new"' in env.read_text()
