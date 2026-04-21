from __future__ import annotations

import html
import json

import pytest
import requests

from linkedin_cli import voyager
from linkedin_cli.session import CliError, ExitCode


class _Response:
    def __init__(self, status_code: int = 200, payload: dict | None = None, text: str = "") -> None:
        self.status_code = status_code
        self._payload = payload if payload is not None else {}
        self.text = text
        self.url = "https://www.linkedin.com/test"
        self.headers = {"content-type": "application/json"}

    def json(self) -> dict:
        if self._payload is None:
            raise ValueError("bad json")
        return self._payload


def test_voyager_get_normalizes_paths_and_sends_contract_headers() -> None:
    session = requests.Session()
    session.cookies.set("JSESSIONID", '"ajax:123"', domain=".linkedin.com")
    calls: list[dict[str, object]] = []

    def fake_get(url: str, **kwargs):
        calls.append({"url": url, **kwargs})
        return _Response(payload={"ok": True})

    session.get = fake_get  # type: ignore[assignment]

    response = voyager.voyager_get(
        session,
        "identity/profiles/jane/profileView",
        params={"q": "memberIdentity"},
        referer="https://www.linkedin.com/in/jane/",
    )

    assert response.json() == {"ok": True}
    assert calls[0]["url"] == "https://www.linkedin.com/voyager/api/identity/profiles/jane/profileView"
    assert calls[0]["params"] == {"q": "memberIdentity"}
    assert calls[0]["allow_redirects"] is False
    assert calls[0]["headers"]["csrf-token"] == "ajax:123"  # type: ignore[index]
    assert calls[0]["headers"]["Referer"] == "https://www.linkedin.com/in/jane/"  # type: ignore[index]


def test_voyager_get_maps_api_prefix_and_auth_failures() -> None:
    session = requests.Session()
    calls: list[str] = []

    def fake_get(url: str, **_kwargs):
        calls.append(url)
        return _Response(status_code=401, text="unauthorized")

    session.get = fake_get  # type: ignore[assignment]

    with pytest.raises(CliError) as exc_info:
        voyager.voyager_get(session, "/api/me")

    assert calls == ["https://www.linkedin.com/voyager/api/me"]
    assert exc_info.value.code == ExitCode.AUTH


def test_voyager_get_includes_http_error_snippet() -> None:
    session = requests.Session()
    session.get = lambda *_args, **_kwargs: _Response(status_code=500, text="server down\nretry later")  # type: ignore[assignment]

    with pytest.raises(CliError) as exc_info:
        voyager.voyager_get(session, "/voyager/api/me")

    assert "HTTP 500" in exc_info.value.message
    assert "server down retry later" in exc_info.value.message


def test_voyager_get_treats_redirects_as_auth_failures() -> None:
    session = requests.Session()
    response = _Response(status_code=302, text="")
    response.headers = {"location": "https://www.linkedin.com/login"}
    session.get = lambda *_args, **_kwargs: response  # type: ignore[assignment]

    with pytest.raises(CliError) as exc_info:
        voyager.voyager_get(session, "/voyager/api/me")

    assert exc_info.value.code == ExitCode.AUTH
    assert "redirected the Voyager request" in exc_info.value.message


def test_parse_bootstrap_payloads_decodes_escaped_body_and_skips_invalid() -> None:
    body = {"included": [{"$type": "com.linkedin.voyager.dash.identity.profile.Profile", "publicIdentifier": "jane"}]}
    meta = {"request": "/voyager/api/identity/profiles/jane/profileView", "status": 200, "body": "bpr-guid-2"}
    page = f"""
    <html>
      <code id="datalet-bpr-guid-1">{html.escape(json.dumps(meta))}</code>
      <code id="bpr-guid-2">{html.escape(json.dumps(body))}</code>
      <code id="datalet-bpr-guid-bad">{{"body":"missing"}}</code>
    </html>
    """

    payloads = voyager.parse_bootstrap_payloads(page)

    assert payloads == [{"request": meta["request"], "status": 200, "body": body}]


def test_summarize_profile_and_company_bootstrap_payloads() -> None:
    profile_body = {
        "included": [
            {
                "$type": "com.linkedin.voyager.dash.identity.profile.Profile",
                "entityUrn": "urn:li:fsd_profile:jane",
                "objectUrn": "urn:li:member:1",
                "publicIdentifier": "jane",
                "firstName": "Jane",
                "lastName": "Doe",
                "headline": "Founder",
                "geoLocation": {"*geo": "urn:li:geo:1"},
            }
        ]
    }
    company_body = {
        "included": [
            {
                "$type": "com.linkedin.voyager.dash.organization.Company",
                "entityUrn": "urn:li:fsd_company:1",
                "name": "Acme",
                "url": "https://www.linkedin.com/company/acme/",
                "logoResolutionResult": {
                    "vectorImage": {
                        "rootUrl": "https://cdn.example/",
                        "artifacts": [
                            {"width": 100, "fileIdentifyingUrlPathSegment": "small.png"},
                            {"width": 300, "fileIdentifyingUrlPathSegment": "large.png"},
                        ],
                    }
                },
            }
        ]
    }
    profile_meta = {"request": "/voyager/api/identity/profiles/jane/profileView", "status": 200, "body": "profile-body"}
    company_meta = {"request": "/voyager/api/graphql?variables=(universalName:acme)", "status": 200, "body": "company-body"}
    page = f"""
    <code id="datalet-bpr-guid-1">{html.escape(json.dumps(profile_meta))}</code>
    <code id="profile-body">{html.escape(json.dumps(profile_body))}</code>
    <code id="datalet-bpr-guid-2">{html.escape(json.dumps(company_meta))}</code>
    <code id="company-body">{html.escape(json.dumps(company_body))}</code>
    """

    profile = voyager.summarize_profile_bootstrap(page, "jane")
    company = voyager.summarize_company_bootstrap(page, "acme")

    assert profile["first_name"] == "Jane"
    assert profile["entity_urn"] == "urn:li:fsd_profile:jane"
    assert company["name"] == "Acme"
    assert company["logo"] == "https://cdn.example/large.png"
