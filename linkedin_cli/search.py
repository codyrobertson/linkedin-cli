"""Public search backends and LinkedIn search result filtering."""

from __future__ import annotations

import os
from functools import partial
from urllib.parse import parse_qs, unquote, urlparse

import requests
from bs4 import BeautifulSoup

from linkedin_cli.config import DEFAULT_TIMEOUT, DEFAULT_USER_AGENT
from linkedin_cli.session import ExitCode, fail
from linkedin_cli.voyager import clean_text

DEFAULT_SEARXNG_BASE_URL = os.getenv("SEARXNG_BASE_URL", "http://127.0.0.1:8080").rstrip("/")


def decode_duckduckgo_result_url(url: str) -> str:
    normalized = "https:" + url if url.startswith("//") else url
    parsed = urlparse(normalized)
    if parsed.netloc.endswith("duckduckgo.com") and parsed.path == "/l/":
        target = parse_qs(parsed.query).get("uddg", [None])[0]
        if target:
            return unquote(target)
    return normalized


def ddg_html_search(query: str, limit: int = 10, timeout: int | None = None) -> list[dict[str, str]]:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": DEFAULT_USER_AGENT,
            "Accept-Language": "en-US,en;q=0.9",
        }
    )
    try:
        response = session.get(
            "https://html.duckduckgo.com/html/",
            params={"q": query},
            timeout=timeout or DEFAULT_TIMEOUT,
        )
    except requests.exceptions.Timeout:
        fail(f"DuckDuckGo search timed out for query `{query}`", code=ExitCode.RETRYABLE)
    except requests.RequestException as exc:
        fail(f"DuckDuckGo search failed for query `{query}`: {exc}", code=ExitCode.RETRYABLE)
    if response.status_code >= 400:
        fail(f"DuckDuckGo search failed with HTTP {response.status_code}")
    soup = BeautifulSoup(response.text, "html.parser")
    results: list[dict[str, str]] = []
    seen: set[str] = set()
    for node in soup.select("div.result"):
        link = node.select_one("a.result__a")
        if link is None:
            continue
        url = decode_duckduckgo_result_url(link.get("href") or "")
        if not url or url in seen:
            continue
        seen.add(url)
        snippet_node = node.select_one("a.result__snippet") or node.select_one("div.result__snippet")
        results.append(
            {
                "title": clean_text(link.get_text(" ", strip=True)) or url,
                "url": url,
                "snippet": clean_text(snippet_node.get_text(" ", strip=True) if snippet_node else "") or "",
            }
        )
        if len(results) >= limit:
            break
    return results


def searxng_search(
    query: str,
    limit: int = 10,
    timeout: int | None = None,
    *,
    base_url: str | None = None,
    engines: list[str] | None = None,
    time_range: str | None = None,
) -> list[dict[str, str]]:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": DEFAULT_USER_AGENT,
            "Accept": "application/json",
            "Accept-Language": "en-US,en;q=0.9",
        }
    )
    normalized_base_url = str(base_url or DEFAULT_SEARXNG_BASE_URL).rstrip("/")
    seen: set[str] = set()
    results: list[dict[str, str]] = []
    page = 1
    while len(results) < max(1, int(limit)):
        params: dict[str, str] = {
            "q": query,
            "format": "json",
            "pageno": str(page),
        }
        if engines:
            params["engines"] = ",".join(str(engine).strip() for engine in engines if str(engine).strip())
        if time_range:
            params["time_range"] = str(time_range).strip()
        try:
            response = session.get(
                f"{normalized_base_url}/search",
                params=params,
                timeout=timeout or DEFAULT_TIMEOUT,
            )
        except requests.exceptions.Timeout:
            fail(f"SearXNG search timed out for query `{query}`", code=ExitCode.RETRYABLE)
        except requests.RequestException as exc:
            fail(f"SearXNG search failed for query `{query}`: {exc}", code=ExitCode.RETRYABLE)
        if response.status_code >= 400:
            fail(f"SearXNG search failed with HTTP {response.status_code}", code=ExitCode.RETRYABLE)
        try:
            payload = response.json()
        except Exception as exc:
            fail(f"SearXNG search returned non-JSON payload for query `{query}`: {exc}", code=ExitCode.RETRYABLE)
        page_results = payload.get("results") or []
        if not isinstance(page_results, list) or not page_results:
            break
        added = 0
        for item in page_results:
            if not isinstance(item, dict):
                continue
            url = clean_text(str(item.get("url") or "")) or ""
            if not url or url in seen:
                continue
            seen.add(url)
            results.append(
                {
                    "title": clean_text(str(item.get("title") or "")) or url,
                    "url": url,
                    "snippet": clean_text(str(item.get("content") or item.get("snippet") or "")) or "",
                }
            )
            added += 1
            if len(results) >= max(1, int(limit)):
                break
        if added == 0:
            break
        page += 1
    return results[: max(1, int(limit))]


def resolve_public_search_fn(
    provider: str = "ddg",
    *,
    searxng_url: str | None = None,
    searxng_engines: list[str] | None = None,
) -> callable:
    normalized = str(provider or "ddg").strip().lower()
    if normalized == "ddg":
        return ddg_html_search
    if normalized == "searxng":
        return partial(searxng_search, base_url=searxng_url, engines=searxng_engines)
    fail("public search provider must be one of: ddg, searxng", code=ExitCode.VALIDATION)


def filter_linkedin_search_results(results: list[dict[str, str]], kind: str) -> list[dict[str, str]]:
    allowed = {
        "people": ["linkedin.com/in/"],
        "companies": ["linkedin.com/company/"],
        "posts": ["linkedin.com/posts/", "linkedin.com/feed/update/", "linkedin.com/pulse/"],
    }[kind]
    filtered: list[dict[str, str]] = []
    for result in results:
        url = result.get("url", "")
        if any(token in url for token in allowed):
            filtered.append(result)
    return filtered


def build_activity_queries(target: str, name: str | None = None) -> list[str]:
    queries: list[str] = []
    base_terms = [name, target]
    for term in base_terms:
        term = clean_text(term)
        if not term:
            continue
        queries.append(f'site:linkedin.com/posts "{term}"')
        queries.append(f'site:linkedin.com/feed/update "{term}"')
    deduped: list[str] = []
    seen: set[str] = set()
    for query in queries:
        if query not in seen:
            seen.add(query)
            deduped.append(query)
    return deduped
