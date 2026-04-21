"""Session and authentication management for LinkedIn CLI.

Handles cookie-based session persistence, login flow, and auth helpers.
"""

from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from enum import IntEnum
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from requests import Response, Session
from requests.cookies import cookiejar_from_dict
from requests.utils import dict_from_cookiejar

from linkedin_cli.config import (
    CONFIG_DIR,
    DEFAULT_TIMEOUT,
    DEFAULT_USER_AGENT,
    ENV_FILE,
)


class CliError(SystemExit):
    def __init__(self, message: str, code: int = 1):
        super().__init__(code)
        self.message = message


class ExitCode(IntEnum):
    OK = 0
    GENERAL = 1
    USAGE = 2
    AUTH = 3
    VALIDATION = 4
    NOT_FOUND = 5
    CONFLICT = 6
    RETRYABLE = 7


def fail(message: str, code: int = 1) -> None:
    raise CliError(message, code)


# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

SESSION_DIR = CONFIG_DIR
SESSION_FILE = SESSION_DIR / "session.json"


# ---------------------------------------------------------------------------
# Env helpers
# ---------------------------------------------------------------------------

def load_env_file(path: Path | None = None) -> None:
    """Load key=value pairs from an env file into os.environ (setdefault)."""
    env_path = path or ENV_FILE
    if not env_path.exists():
        return
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export "):].strip()
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if not key:
            continue
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
            value = value[1:-1]
        os.environ.setdefault(key, value)


def getenv_required(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        fail(f"Missing required environment variable: {name}", code=ExitCode.VALIDATION)
    return value


# ---------------------------------------------------------------------------
# Session construction and persistence
# ---------------------------------------------------------------------------

def build_session(user_agent: str | None = None) -> Session:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": user_agent or DEFAULT_USER_AGENT,
            "Accept-Language": "en-US,en;q=0.9",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
        }
    )
    return session


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def ensure_session_dir() -> None:
    SESSION_DIR.mkdir(parents=True, exist_ok=True, mode=0o700)
    try:
        SESSION_DIR.chmod(0o700)
    except OSError:
        pass


def save_session(session: Session, meta: dict[str, Any] | None = None) -> None:
    ensure_session_dir()
    payload = {
        "saved_at": now_iso(),
        "cookies": dict_from_cookiejar(session.cookies),
        "meta": meta or {},
    }
    SESSION_FILE.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    try:
        SESSION_FILE.chmod(0o600)
    except OSError:
        pass


def load_session(required: bool = True) -> tuple[Session | None, dict[str, Any]]:
    if not SESSION_FILE.exists():
        if required:
            fail(f"No saved LinkedIn session at {SESSION_FILE}. Run `login` first.", code=ExitCode.AUTH)
        return None, {}
    try:
        data = json.loads(SESSION_FILE.read_text(encoding="utf-8"))
    except Exception as exc:
        fail(f"Failed to read session file {SESSION_FILE}: {exc}")
    meta = data.get("meta") or {}
    session = build_session(meta.get("user_agent") or DEFAULT_USER_AGENT)
    session.cookies = cookiejar_from_dict(data.get("cookies") or {})
    return session, meta


# ---------------------------------------------------------------------------
# Cookie / CSRF helpers
# ---------------------------------------------------------------------------

def masked(value: str | None, keep: int = 4) -> str | None:
    if not value:
        return None
    if len(value) <= keep * 2:
        return "*" * len(value)
    return value[:keep] + "\u2026" + value[-keep:]


def cookie_value(session: Session, name: str) -> str | None:
    for cookie in session.cookies:
        if cookie.name == name:
            return cookie.value
    return None


def csrf_token_from_session(session: Session) -> str | None:
    jsessionid = cookie_value(session, "JSESSIONID")
    if not jsessionid:
        return None
    return jsessionid.strip('"')


def auth_summary(session: Session) -> dict[str, Any]:
    li_at = cookie_value(session, "li_at")
    jsessionid = cookie_value(session, "JSESSIONID")
    return {
        "has_li_at": bool(li_at),
        "li_at": masked(li_at),
        "has_jsessionid": bool(jsessionid),
        "jsessionid": masked(jsessionid),
        "csrf_token": masked(csrf_token_from_session(session)),
    }


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

def request(
    session: Session,
    method: str,
    url: str,
    *,
    expected_json: bool = False,
    **kwargs: Any,
) -> Response:
    kwargs.setdefault("timeout", DEFAULT_TIMEOUT)
    response = session.request(method.upper(), url, **kwargs)
    if response.status_code >= 400:
        snippet = response.text[:400].strip().replace("\n", " ") if response.text else ""
        fail(f"HTTP {response.status_code} for {url}\n{snippet}")
    if expected_json:
        content_type = response.headers.get("content-type", "")
        if "json" not in content_type:
            pass
    return response


# ---------------------------------------------------------------------------
# Login flow
# ---------------------------------------------------------------------------

def extract_hidden_inputs(form) -> dict[str, str]:
    payload: dict[str, str] = {}
    for tag in form.find_all("input"):
        name = tag.get("name")
        if not name:
            continue
        payload[name] = tag.get("value", "")
    return payload


def find_login_form(html: str):
    soup = BeautifulSoup(html, "html.parser")
    for form in soup.find_all("form"):
        action = form.get("action") or ""
        if "login-submit" in action:
            return form
    return None


def extract_error_message(html: str) -> str | None:
    soup = BeautifulSoup(html, "html.parser")
    selectors = [
        ".alert-content",
        "#error-for-username",
        "#error-for-password",
        '[role="alert"]',
        ".form__label--error",
    ]
    for selector in selectors:
        node = soup.select_one(selector)
        if node:
            text = " ".join(node.get_text(" ", strip=True).split())
            if text:
                return text
    title = soup.title.get_text(strip=True) if soup.title else ""
    if title and title.lower() not in {"linkedin login, sign in | linkedin"}:
        return title
    return None


def login_result(session: Session, response: Response) -> dict[str, Any]:
    url = response.url
    text = response.text or ""
    status = auth_summary(session)
    challenge = any(token in url for token in ["/checkpoint/", "/challenge/"])
    needs_login = "/login" in url or "session_password" in text
    logged_in = bool(status["has_li_at"]) and not needs_login and not challenge
    return {
        "logged_in": logged_in,
        "challenge": challenge,
        "final_url": url,
        "status": status,
        "error": extract_error_message(text),
    }


def linkedin_login(session: Session, username: str, password: str) -> dict[str, Any]:
    login_page = request(session, "GET", "https://www.linkedin.com/login")
    form = find_login_form(login_page.text)
    if not form:
        fail("Could not find LinkedIn login form. LinkedIn may have changed the page.")
    payload = extract_hidden_inputs(form)
    payload["session_key"] = username
    payload["session_password"] = password
    action = form.get("action") or "/checkpoint/lg/login-submit"
    submit_url = urljoin(login_page.url, action)
    response = session.post(
        submit_url,
        data=payload,
        headers={
            "Referer": login_page.url,
            "Origin": "https://www.linkedin.com",
            "Content-Type": "application/x-www-form-urlencoded",
        },
        allow_redirects=True,
        timeout=DEFAULT_TIMEOUT,
    )
    return login_result(session, response)


def _infer_browser_launcher(browser_name: str) -> tuple[Any, dict[str, Any]]:
    browser_name = browser_name.strip().lower()
    if browser_name in {"chrome", "chromium"}:
        return "chromium", {}
    if browser_name == "brave":
        return "chromium", {
            "executable_path": os.environ.get(
                "BRAVE_EXECUTABLE_PATH",
                "/Applications/Brave Browser.app/Contents/MacOS/Brave Browser",
            )
        }
    if browser_name == "firefox":
        return "firefox", {}
    # This branch validates user input from argparse first.
    raise ValueError(f"Unsupported browser: {browser_name}")


def _resolve_cookie_payload(browser_cookies: list[dict[str, Any]]) -> dict[str, str]:
    cookies: dict[str, str] = {}
    for cookie in browser_cookies:
        name = cookie.get("name")
        value = cookie.get("value")
        domain = cookie.get("domain", "")
        if not isinstance(name, str) or not isinstance(value, str):
            continue
        if "linkedin.com" not in domain:
            continue
        cookies[name] = value
    return cookies


def import_browser_session(
    browser_name: str,
    timeout: int = 300,
    user_agent: str | None = None,
) -> tuple[Session, dict[str, Any]]:
    try:
        from playwright.sync_api import sync_playwright
    except ImportError as exc:
        raise CliError(
            "Browser login requires playwright. Install it with: pip install playwright && python -m playwright install chromium firefox",
            code=ExitCode.GENERAL,
        ) from exc

    key, extra_launch_kwargs = _infer_browser_launcher(browser_name)
    _timeout = int(timeout)
    if _timeout <= 0:
        fail("Browser login timeout must be greater than zero", code=ExitCode.VALIDATION)
    launch_kwargs: dict[str, Any] = {"headless": False, "timeout": _timeout * 1000}
    launch_kwargs.update(extra_launch_kwargs)

    print(
        "Open the LinkedIn login flow in the browser. The command will finish when it detects an active li_at cookie "
        f"(timeout: {_timeout}s).",
        flush=True,
    )
    print("If already logged in, navigate to your LinkedIn home page to finish.", flush=True)

    with sync_playwright() as playwright:
        browser_type = getattr(playwright, key, None)
        if browser_type is None:
            fail(f"Unsupported browser launch target: {key}")
        try:
            browser = browser_type.launch(**launch_kwargs)
        except Exception as exc:
            raise CliError(
                f"Failed to launch {browser_name} for cookie capture. "
                "Make sure the browser is installed and available on your machine.",
                code=ExitCode.GENERAL,
            ) from exc
        context = browser.new_context(user_agent=user_agent)
        page = context.new_page()
        page.goto("https://www.linkedin.com/feed/")

        deadline = time.time() + _timeout
        while time.time() < deadline:
            is_connected = getattr(browser, "is_connected", None)
            if callable(is_connected) and not is_connected():
                fail("Browser closed before LinkedIn login completed", code=ExitCode.AUTH)
            cookies = context.cookies()
            cookie_map = _resolve_cookie_payload(cookies)
            if cookie_map.get("li_at"):
                session = build_session(user_agent)
                session.cookies = cookiejar_from_dict(cookie_map)
                if not csrf_token_from_session(session):
                    # Preserve existing request behavior while allowing manual cookie capture.
                    session.cookies.set("JSESSIONID", cookie_map.get("JSESSIONID", ""))
                captured = context.storage_state()
                browser.close()
                return session, {
                    "source": "browser_import",
                    "browser_name": browser_name,
                    "captured_count": len(cookie_map),
                    "captured_at": now_iso(),
                    "storage_state": captured.get("origins", []),
                }
            time.sleep(1)

        browser.close()
        fail(
            f"Timed out waiting for LinkedIn login after {_timeout}s. "
            "Use --timeout to extend or finish login and try again.",
            code=ExitCode.AUTH,
        )
