"""Voyager API helpers for LinkedIn CLI.

Provides authenticated access to LinkedIn's internal Voyager endpoints.
"""

from __future__ import annotations

import html as html_lib
import json
from typing import Any
from urllib.parse import urlparse

from bs4 import BeautifulSoup
from requests import Response, Session

from linkedin_cli.config import DEFAULT_TIMEOUT
from linkedin_cli.session import ExitCode, csrf_token_from_session, fail


def voyager_get(session: Session, path: str, params: dict[str, str] | None = None) -> Response:
    if not path.startswith("/"):
        path = "/" + path
    if not path.startswith("/voyager/api/"):
        if path.startswith("/api/"):
            path = "/voyager" + path
        else:
            path = "/voyager/api" + path
    csrf = csrf_token_from_session(session)
    headers = {
        "Accept": "application/vnd.linkedin.normalized+json+2.1",
        "X-RestLi-Protocol-Version": "2.0.0",
        "Referer": "https://www.linkedin.com/feed/",
    }
    if csrf:
        headers["csrf-token"] = csrf
    response = session.get(
        "https://www.linkedin.com" + path,
        params=params or {},
        headers=headers,
        timeout=DEFAULT_TIMEOUT,
    )
    if response.status_code == 401:
        fail("LinkedIn session is unauthorized. Run `login` again.", code=ExitCode.AUTH)
    if response.status_code == 403:
        fail(
            "LinkedIn rejected the Voyager request (403). The session may need verification or LinkedIn blocked the endpoint.",
            code=ExitCode.AUTH,
        )
    if response.status_code >= 400:
        snippet = response.text[:400].strip().replace("\n", " ") if response.text else ""
        fail(f"Voyager request failed with HTTP {response.status_code}: {snippet}")
    return response


def parse_json_response(response: Response) -> Any:
    try:
        return response.json()
    except Exception as exc:
        snippet = response.text[:500] if response.text else ""
        fail(f"Response was not valid JSON: {exc}\n{snippet}")


# ---------------------------------------------------------------------------
# Profile helpers
# ---------------------------------------------------------------------------

def normalize_profile_slug(value: str) -> str:
    value = value.strip()
    if not value:
        fail("Profile target is required")
    if value.startswith("http://") or value.startswith("https://"):
        path = urlparse(value).path.strip("/")
        parts = [part for part in path.split("/") if part]
        if len(parts) >= 2 and parts[0] == "in":
            return parts[1]
        fail(f"Could not extract LinkedIn profile slug from URL: {value}")
    return value.strip("/").removeprefix("in/")


def normalize_company_slug(value: str) -> str:
    value = value.strip()
    if not value:
        fail("Company target is required")
    if value.startswith("http://") or value.startswith("https://"):
        path = urlparse(value).path.strip("/")
        parts = [part for part in path.split("/") if part]
        if len(parts) >= 2 and parts[0] == "company":
            return parts[1]
        fail(f"Could not extract LinkedIn company slug from URL: {value}")
    return value.strip("/").removeprefix("company/")


# ---------------------------------------------------------------------------
# HTML / JSON-LD parsing
# ---------------------------------------------------------------------------

def find_json_ld_objects(html: str) -> list[Any]:
    soup = BeautifulSoup(html, "html.parser")
    objects: list[Any] = []
    for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
        raw = script.string or script.get_text() or ""
        raw = raw.strip()
        if not raw:
            continue
        try:
            parsed = json.loads(raw)
        except Exception:
            continue
        if isinstance(parsed, list):
            objects.extend(parsed)
        else:
            objects.append(parsed)
    return objects


def first_json_ld_of_type(objects: list[Any], target_type: str) -> dict[str, Any] | None:
    for obj in objects:
        if not isinstance(obj, dict):
            continue
        typ = obj.get("@type")
        if typ == target_type:
            return obj
        if isinstance(typ, list) and target_type in typ:
            return obj
    return None


def clean_text(text: str | None) -> str | None:
    if text is None:
        return None
    compact = " ".join(text.split())
    return compact or None


def parse_bootstrap_payloads(html: str) -> list[dict[str, Any]]:
    soup = BeautifulSoup(html, "html.parser")
    payloads: list[dict[str, Any]] = []
    for code in soup.find_all("code"):
        code_id = code.get("id") or ""
        if not code_id.startswith("datalet-bpr-guid-"):
            continue
        raw_meta = code.get_text().strip()
        if not raw_meta.startswith("{") or '"body"' not in raw_meta:
            continue
        try:
            meta = json.loads(raw_meta)
        except Exception:
            continue
        body_id = meta.get("body")
        if not body_id:
            continue
        body_node = soup.find("code", id=body_id)
        if body_node is None:
            continue
        raw_body = html_lib.unescape(body_node.get_text().strip())
        if not raw_body.startswith("{"):
            continue
        try:
            body = json.loads(raw_body)
        except Exception:
            continue
        payloads.append({"request": meta.get("request"), "status": meta.get("status"), "body": body})
    return payloads


def summarize_profile_bootstrap(html: str, slug: str) -> dict[str, Any] | None:
    for payload in parse_bootstrap_payloads(html):
        request_path = payload.get("request") or ""
        if slug not in request_path:
            continue
        body = payload.get("body") or {}
        included = body.get("included") or []
        for item in included:
            if not isinstance(item, dict):
                continue
            if item.get("publicIdentifier") != slug:
                continue
            item_type = item.get("$type") or ""
            if "Profile" not in item_type:
                continue
            return {
                "source": "bootstrap-graphql",
                "entity_urn": item.get("entityUrn"),
                "object_urn": item.get("objectUrn"),
                "public_identifier": item.get("publicIdentifier"),
                "first_name": item.get("firstName"),
                "last_name": item.get("lastName"),
                "headline": item.get("headline"),
                "location_urn": ((item.get("geoLocation") or {}).get("*geo")),
                "profile_type": item_type,
            }
    return None


def summarize_profile_html(html: str, url: str) -> dict[str, Any]:
    soup = BeautifulSoup(html, "html.parser")
    json_ld_objects = find_json_ld_objects(html)
    person = first_json_ld_of_type(json_ld_objects, "Person") or {}
    title = clean_text(soup.title.get_text(" ", strip=True) if soup.title else None)
    description = None
    desc_tag = soup.find("meta", attrs={"name": "description"})
    if desc_tag:
        description = clean_text(desc_tag.get("content"))
    canonical_tag = soup.find("link", attrs={"rel": "canonical"})
    canonical = canonical_tag.get("href") if canonical_tag else url
    image = None
    image_tag = soup.find("meta", attrs={"property": "og:image"})
    if image_tag:
        image = image_tag.get("content")
    return {
        "source": "html",
        "url": canonical,
        "title": title,
        "name": person.get("name"),
        "headline": description,
        "job_title": person.get("jobTitle"),
        "works_for": person.get("worksFor"),
        "same_as": person.get("sameAs"),
        "image": image,
    }


def _vector_image_best_url(image_obj: dict[str, Any] | None) -> str | None:
    if not image_obj:
        return None
    root = image_obj.get("rootUrl")
    artifacts = image_obj.get("artifacts") or []
    best = None
    for artifact in artifacts:
        if not isinstance(artifact, dict):
            continue
        width = artifact.get("width") or 0
        if best is None or width > (best.get("width") or 0):
            best = artifact
    if root and best and best.get("fileIdentifyingUrlPathSegment"):
        return root + best["fileIdentifyingUrlPathSegment"]
    return None


def summarize_company_bootstrap(html: str, slug: str) -> dict[str, Any] | None:
    for payload in parse_bootstrap_payloads(html):
        request_path = payload.get("request") or ""
        if f"universalName:{slug}" not in request_path and f"organizationUniversalName:{slug}" not in request_path:
            continue
        body = payload.get("body") or {}
        included = body.get("included") or []
        for item in included:
            if not isinstance(item, dict):
                continue
            item_type = item.get("$type") or ""
            item_url = item.get("url") or ""
            if "Company" not in item_type:
                continue
            if f"/company/{slug}/" not in item_url:
                continue
            return {
                "source": "bootstrap-graphql",
                "entity_urn": item.get("entityUrn"),
                "name": item.get("name"),
                "url": item_url,
                "logo": _vector_image_best_url((item.get("logoResolutionResult") or {}).get("vectorImage")),
                "company_type": item_type,
            }
    return None


def summarize_company_html(html: str, url: str) -> dict[str, Any]:
    soup = BeautifulSoup(html, "html.parser")
    json_ld_objects = find_json_ld_objects(html)
    org = first_json_ld_of_type(json_ld_objects, "Organization") or {}
    title = clean_text(soup.title.get_text(" ", strip=True) if soup.title else None)
    description = None
    desc_tag = soup.find("meta", attrs={"name": "description"})
    if desc_tag:
        description = clean_text(desc_tag.get("content"))
    canonical_tag = soup.find("link", attrs={"rel": "canonical"})
    canonical = canonical_tag.get("href") if canonical_tag else url
    image = None
    image_tag = soup.find("meta", attrs={"property": "og:image"})
    if image_tag:
        image = image_tag.get("content")
    return {
        "source": "html",
        "url": canonical,
        "title": title,
        "name": org.get("name"),
        "description": description,
        "logo": org.get("logo") or image,
        "website": org.get("url"),
        "same_as": org.get("sameAs"),
    }


def try_profile_voyager(session: Session, slug: str) -> dict[str, Any] | None:
    from linkedin_cli.session import CliError
    try:
        response = voyager_get(session, f"/voyager/api/identity/profiles/{slug}/profileView")
        data = parse_json_response(response)
    except CliError:
        return None
    summary = {
        "source": "voyager",
        "profile_urn": data.get("profile") or data.get("entityUrn"),
        "mini_profile": data.get("miniProfile"),
        "headline": data.get("headline"),
        "location": data.get("locationName"),
        "industry": data.get("industryName"),
        "geo": data.get("geoLocationName"),
    }
    return summary


def fetch_profile_summary(session: Session, slug: str) -> dict[str, Any]:
    from linkedin_cli.session import request as session_request
    voyager_summary = try_profile_voyager(session, slug)
    if voyager_summary:
        return voyager_summary
    response = session_request(session, "GET", f"https://www.linkedin.com/in/{slug}/")
    bootstrap_summary = summarize_profile_bootstrap(response.text, slug)
    if bootstrap_summary:
        return bootstrap_summary
    return summarize_profile_html(response.text, response.url)


def fetch_company_summary(session: Session, slug: str) -> dict[str, Any]:
    from linkedin_cli.session import request as session_request
    response = session_request(session, "GET", f"https://www.linkedin.com/company/{slug}/")
    bootstrap_summary = summarize_company_bootstrap(response.text, slug)
    if bootstrap_summary:
        return bootstrap_summary
    return summarize_company_html(response.text, response.url)


# ---------------------------------------------------------------------------
# URL extraction helpers
# ---------------------------------------------------------------------------

def extract_profile_slug_from_url(url: str) -> str | None:
    parsed = urlparse(url)
    parts = [part for part in parsed.path.strip("/").split("/") if part]
    if len(parts) >= 2 and parts[0] == "in":
        return parts[1]
    return None


def extract_company_slug_from_url(url: str) -> str | None:
    parsed = urlparse(url)
    parts = [part for part in parsed.path.strip("/").split("/") if part]
    if len(parts) >= 2 and parts[0] == "company":
        return parts[1]
    return None
