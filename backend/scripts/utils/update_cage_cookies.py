"""
Update ``CAGE_PHPSESS`` and ``CAGE_VERIFICATION_TOKEN`` in ``.env`` from a
browser-exported curl command, then (optionally) live-validate the new
session against cage.dla.mil.

Why a curl-based input?
    When a CAGE session expires, the ordinary recovery ritual is:

        1. Open https://cage.dla.mil/search in your browser.
        2. Click through the ``agree`` terms-of-use page.
        3. Open DevTools → Network → right-click any request → Copy → Copy as cURL.
        4. Paste the curl into a terminal.

    The ``-b`` / ``--cookie`` flag holds a semicolon-separated cookie
    string containing ``PHPSESS=…; __RequestVerificationToken=…; agree=True``
    plus some Cloudflare anti-bot session cookies we don't need. This
    utility extracts the two values we DO need and writes them into
    ``.env`` in place, preserving every other line.

Safety:
    * A backup ``.env.backup-<YYYYMMDD-HHMMSS>`` is written before any
      modification. Backups are never auto-cleaned (see M1.6 plan).
    * The new cookies are live-tested by default (--skip-validation to
      bypass).
    * Non-cookie env keys (OPENFIGI_API_KEY, etc.) are preserved byte-
      for-byte.

Usage
-----
From stdin (the interactive run_pipeline.sh path)::

    pbpaste | uv run --env-file .env \\
        backend/scripts/utils/update_cage_cookies.py --stdin

From a saved curl file::

    uv run --env-file .env \\
        backend/scripts/utils/update_cage_cookies.py --curl-file /tmp/curl.txt

Dry run (print the intended changes, write nothing)::

    uv run backend/scripts/utils/update_cage_cookies.py \\
        --curl-file /tmp/curl.txt --dry-run
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import shutil
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple


# ---------------------------------------------------------------------------
# Curl parsing
# ---------------------------------------------------------------------------

# The curl `-b` / `--cookie` flag holds the entire cookie string. Accepts
# single, double, or unquoted forms. We intentionally don't try to parse
# the whole shell grammar -- we just find the value of -b / --cookie.
_COOKIE_FLAG_RE = re.compile(
    r"""(?ms)
    (?:^|\s)                                  # flag preceded by start or whitespace
    (?:-b|--cookie|--cookies?)                # the flag itself
    \s+
    (?:
        '([^']*)'                              # single-quoted value
      | "((?:[^"\\]|\\.)*)"                    # double-quoted (with escapes)
      | ([^\s'"]+)                             # unquoted single token
    )
    """,
    re.VERBOSE,
)


def extract_cookie_string(curl_text: str) -> Optional[str]:
    """Return the value passed to ``-b``/``--cookie`` in a curl command, or ``None``."""
    match = _COOKIE_FLAG_RE.search(curl_text)
    if not match:
        return None
    # exactly one of the three alternatives matches
    for group in match.groups():
        if group is not None:
            return group
    return None


def parse_cookie_pairs(cookie_string: str) -> Dict[str, str]:
    """
    Split a ``name=value; name=value`` cookie string into a dict.

    Values may contain ``=`` (e.g. base64 tokens) but NOT ``;`` (the pair
    separator). We split on the FIRST ``=`` only.
    """
    result: Dict[str, str] = {}
    for raw_pair in cookie_string.split(";"):
        pair = raw_pair.strip()
        if not pair or "=" not in pair:
            continue
        name, _, value = pair.partition("=")
        name = name.strip()
        value = value.strip()
        if name:
            result[name] = value
    return result


def extract_cage_cookies(curl_text: str) -> Tuple[str, str]:
    """
    Extract ``(PHPSESS, __RequestVerificationToken)`` from a curl command.

    Raises ``ValueError`` with a user-friendly message on any failure.
    """
    cookie_str = extract_cookie_string(curl_text)
    if not cookie_str:
        raise ValueError(
            "Couldn't find a -b/--cookie flag in the curl input. "
            "Make sure you copied the request as cURL (Brave/Chrome "
            "DevTools → Network → Copy → Copy as cURL)."
        )

    pairs = parse_cookie_pairs(cookie_str)
    phpsess = pairs.get("PHPSESS")
    token = pairs.get("__RequestVerificationToken")

    missing = []
    if not phpsess:
        missing.append("PHPSESS")
    if not token:
        missing.append("__RequestVerificationToken")
    if missing:
        raise ValueError(
            f"The curl cookie string is missing: {', '.join(missing)}. "
            "Make sure you're copying from a request that was made AFTER "
            "accepting the agree-to-terms page."
        )

    return phpsess, token


# ---------------------------------------------------------------------------
# .env manipulation
# ---------------------------------------------------------------------------

# Matches both plain ``KEY=value`` and shell-style ``export KEY=value`` lines.
# Captures (1) the optional ``export `` prefix and (2) the key name. Used to
# decide whether a line should be rewritten by the upsert logic.
_ENV_KEY_RE = re.compile(r"^(\s*(?:export\s+)?)([A-Z_][A-Z0-9_]*)\s*=")


def _shell_quote(value: str) -> str:
    """
    Return ``value`` wrapped in double quotes, with any embedded double
    quotes / backslashes / ``$`` / backticks escaped. Produces a literal
    that's safe to paste into a .env consumed by POSIX shells or by
    python-dotenv (both accept double-quoted values).
    """
    # Escape backslash first so we don't double-escape the ones we add later.
    escaped = value.replace("\\", "\\\\")
    escaped = escaped.replace('"', '\\"')
    escaped = escaped.replace("$", "\\$")
    escaped = escaped.replace("`", "\\`")
    return f'"{escaped}"'


def _upsert_env_lines(
    lines: List[str],
    updates: Dict[str, str],
) -> List[str]:
    """
    Return ``lines`` with ``updates`` applied in-place.

    * Existing keys are replaced on their current line (position preserved).
      The original ``export`` prefix (if any) is preserved so a shell-style
      .env stays shell-style.
    * Values are always written in double-quoted form so future tokens
      containing spaces, semicolons, or special characters can't break
      shell parsing.
    * Missing keys are appended at the end as ``KEY="value"`` pairs.
    """
    result = list(lines)
    seen: set[str] = set()
    for i, line in enumerate(result):
        m = _ENV_KEY_RE.match(line)
        if not m:
            continue
        prefix, key = m.group(1), m.group(2)
        if key in updates:
            result[i] = f"{prefix}{key}={_shell_quote(updates[key])}\n"
            seen.add(key)

    for key, value in updates.items():
        if key not in seen:
            # Ensure preceding line ends with newline before append.
            if result and not result[-1].endswith("\n"):
                result[-1] = result[-1] + "\n"
            result.append(f"{key}={_shell_quote(value)}\n")
    return result


def write_env_update(
    env_path: Path,
    updates: Dict[str, str],
    *,
    dry_run: bool = False,
    no_backup: bool = False,
) -> Optional[Path]:
    """
    Apply ``updates`` to ``env_path``. Returns the backup path (or ``None``
    in dry-run / no-backup mode).
    """
    if not env_path.exists():
        raise FileNotFoundError(f".env file not found at {env_path}")

    with env_path.open("r", encoding="utf-8") as fh:
        original = fh.readlines()
    updated = _upsert_env_lines(original, updates)

    if dry_run:
        return None

    backup_path: Optional[Path] = None
    if not no_backup:
        stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        backup_path = env_path.with_name(f"{env_path.name}.backup-{stamp}")
        shutil.copy2(env_path, backup_path)

    with env_path.open("w", encoding="utf-8") as fh:
        fh.writelines(updated)

    return backup_path


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _setup_logging() -> logging.Logger:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    return logging.getLogger(__name__)


def _read_curl(args: argparse.Namespace) -> str:
    if args.curl_file:
        path = Path(args.curl_file)
        if not path.exists():
            raise FileNotFoundError(f"curl file not found: {path}")
        return path.read_text(encoding="utf-8")
    return sys.stdin.read()


def _live_validate(phpsess: str, token: str, logger: logging.Logger) -> bool:
    """
    Briefly monkey-patch the CAGE credentials and run ``_validate_credentials``.
    We import lazily so the command still works when the CAGE module (or
    its network stack) isn't importable in a given environment.
    """
    try:
        from backend.app.services.providers import cage_scraper as cs
    except Exception as e:
        logger.warning(f"Could not import cage_scraper for validation: {e}")
        return False

    orig_sc = cs.SESSION_COOKIE
    orig_vt = cs.VERIFICATION_TOKEN
    orig_cookies = dict(cs.CAGE_COOKIES)
    try:
        cs.SESSION_COOKIE = phpsess
        cs.VERIFICATION_TOKEN = token
        cs.CAGE_COOKIES = {
            "PHPSESS": phpsess,
            "__RequestVerificationToken": token,
            "agree": "True",
        }
        try:
            cs._validate_credentials()
        except cs.CageAuthExpiredError as e:
            logger.error(f"[!] Live validation rejected the new cookies: {e}")
            return False
        logger.info("[✓] Live validation succeeded — new cookies accept the terms.")
        return True
    finally:
        cs.SESSION_COOKIE = orig_sc
        cs.VERIFICATION_TOKEN = orig_vt
        cs.CAGE_COOKIES = orig_cookies


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Update CAGE cookies in .env from a browser curl command.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    src = parser.add_mutually_exclusive_group()
    src.add_argument("--curl-file", help="Path to a file containing the curl command.")
    src.add_argument(
        "--stdin", action="store_true", help="Read curl command from stdin (default)."
    )
    parser.add_argument(
        "--env-file", default=".env", help="Target .env path (default: .env)"
    )
    parser.add_argument(
        "--no-backup", action="store_true", help="Skip writing .env.backup-<timestamp>."
    )
    parser.add_argument(
        "--skip-validation",
        action="store_true",
        help="Don't live-test the new cookies.",
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="Report intended changes; write nothing."
    )
    args = parser.parse_args(argv)

    logger = _setup_logging()

    try:
        curl_text = _read_curl(args)
    except Exception as e:
        logger.error(f"[!] Could not read curl input: {e}")
        return 2

    try:
        phpsess, token = extract_cage_cookies(curl_text)
    except ValueError as e:
        logger.error(f"[!] {e}")
        return 3

    logger.info(
        f"Parsed cookies (lengths): PHPSESS={len(phpsess)} chars, "
        f"__RequestVerificationToken={len(token)} chars."
    )

    env_path = Path(args.env_file)
    try:
        backup = write_env_update(
            env_path,
            {"CAGE_PHPSESS": phpsess, "CAGE_VERIFICATION_TOKEN": token},
            dry_run=args.dry_run,
            no_backup=args.no_backup,
        )
    except FileNotFoundError as e:
        logger.error(f"[!] {e}")
        return 4

    if args.dry_run:
        logger.info("[DRY RUN] .env would be updated; no files written.")
        return 0

    if backup is not None:
        logger.info(f"[✓] Backed up previous .env to: {backup}")
    logger.info(f"[✓] Wrote CAGE cookies to {env_path}.")

    if args.skip_validation:
        return 0

    # Import .env into the running process so _validate_credentials() can
    # see the new values. We do this AFTER writing so downstream imports
    # (which read os.getenv at module load) see the fresh creds.
    os.environ["CAGE_PHPSESS"] = phpsess
    os.environ["CAGE_VERIFICATION_TOKEN"] = token

    ok = _live_validate(phpsess, token, logger)
    return 0 if ok else 5


if __name__ == "__main__":
    sys.exit(main())
