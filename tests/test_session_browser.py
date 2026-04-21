from __future__ import annotations

import sys
import types

import pytest

from linkedin_cli import session as session_mod


class _FakePage:
    def goto(self, _url: str) -> None:
        return None


class _FakeContext:
    def __init__(self, cookies_sequence: list[list[dict[str, str]]]) -> None:
        self._cookies_sequence = list(cookies_sequence)

    def new_page(self) -> _FakePage:
        return _FakePage()

    def cookies(self) -> list[dict[str, str]]:
        if self._cookies_sequence:
            return self._cookies_sequence.pop(0)
        return []

    def storage_state(self) -> dict[str, list[dict[str, str]]]:
        return {"origins": []}


class _FakeBrowser:
    def __init__(self, context: _FakeContext) -> None:
        self._context = context
        self.closed = False

    def new_context(self, user_agent: str | None = None) -> _FakeContext:
        self.user_agent = user_agent
        return self._context

    def close(self) -> None:
        self.closed = True


class _FakeBrowserType:
    def __init__(self, browser: _FakeBrowser) -> None:
        self._browser = browser
        self.launch_kwargs: dict[str, object] | None = None

    def launch(self, **kwargs: object) -> _FakeBrowser:
        self.launch_kwargs = kwargs
        return self._browser


class _FakePlaywrightManager:
    def __init__(self, browser_type: _FakeBrowserType) -> None:
        self._playwright = types.SimpleNamespace(
            chromium=browser_type,
            firefox=browser_type,
        )

    def __enter__(self) -> types.SimpleNamespace:
        return self._playwright

    def __exit__(self, exc_type, exc, tb) -> None:
        return None


def _install_fake_playwright(monkeypatch: pytest.MonkeyPatch, browser_type: _FakeBrowserType) -> None:
    fake_sync_api = types.ModuleType("playwright.sync_api")
    fake_sync_api.sync_playwright = lambda: _FakePlaywrightManager(browser_type)
    fake_playwright = types.ModuleType("playwright")
    fake_playwright.sync_api = fake_sync_api
    monkeypatch.setitem(sys.modules, "playwright", fake_playwright)
    monkeypatch.setitem(sys.modules, "playwright.sync_api", fake_sync_api)


def test_import_browser_session_honors_requested_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    context = _FakeContext(cookies_sequence=[[]])
    browser = _FakeBrowser(context)
    browser_type = _FakeBrowserType(browser)
    _install_fake_playwright(monkeypatch, browser_type)
    monkeypatch.setattr(session_mod.time, "sleep", lambda _seconds: None)
    times = iter([100.0, 106.0])
    monkeypatch.setattr(session_mod.time, "time", lambda: next(times))

    with pytest.raises(session_mod.CliError) as exc_info:
        session_mod.import_browser_session(browser_name="firefox", timeout=5)

    assert exc_info.value.code == session_mod.ExitCode.AUTH
    assert "after 5s" in exc_info.value.message
    assert browser_type.launch_kwargs == {"headless": False, "timeout": 5000}


def test_import_browser_session_captures_linkedin_cookies(monkeypatch: pytest.MonkeyPatch) -> None:
    context = _FakeContext(
        cookies_sequence=[
            [
                {"name": "li_at", "value": "token", "domain": ".www.linkedin.com"},
                {"name": "JSESSIONID", "value": '"ajax:123"', "domain": ".www.linkedin.com"},
            ]
        ]
    )
    browser = _FakeBrowser(context)
    browser_type = _FakeBrowserType(browser)
    _install_fake_playwright(monkeypatch, browser_type)
    monkeypatch.setattr(session_mod.time, "sleep", lambda _seconds: None)
    monkeypatch.setattr(session_mod.time, "time", lambda: 100.0)

    session, meta = session_mod.import_browser_session(browser_name="chrome", timeout=9, user_agent="ua-test")

    assert session_mod.cookie_value(session, "li_at") == "token"
    assert session_mod.csrf_token_from_session(session) == "ajax:123"
    assert meta["browser_name"] == "chrome"
    assert meta["captured_count"] == 2
    assert browser_type.launch_kwargs == {"headless": False, "timeout": 9000}


def test_save_session_restricts_cookie_file_permissions(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(session_mod, "SESSION_DIR", tmp_path)
    monkeypatch.setattr(session_mod, "SESSION_FILE", tmp_path / "session.json")
    session = session_mod.build_session("ua-test")
    session.cookies.set("li_at", "secret", domain=".linkedin.com")

    session_mod.save_session(session, {"user_agent": "ua-test"})

    assert (tmp_path.stat().st_mode & 0o777) == 0o700
    assert ((tmp_path / "session.json").stat().st_mode & 0o777) == 0o600
