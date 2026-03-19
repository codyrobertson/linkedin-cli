"""DuckDuckGo search and LinkedIn search result filtering."""

from __future__ import annotations

from urllib.parse import parse_qs, unquote, urlparse

import requests
from bs4 import BeautifulSoup

from linkedin_cli.config import DEFAULT_TIMEOUT, DEFAULT_USER_AGENT
from linkedin_cli.session import fail
from linkedin_cli.voyager import clean_text


def decode_duckduckgo_result_url(url: str) -> str:
    normalized = "https:" + url if url.startswith("//") else url
    parsed = urlparse(normalized)
    if parsed.netloc.endswith("duckduckgo.com") and parsed.path == "/l/":
        target = parse_qs(parsed.query).get("uddg", [None])[0]
        if target:
            return unquote(target)
    return normalized


def ddg_html_search(query: str, limit: int = 10) -> list[dict[str, str]]:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": DEFAULT_USER_AGENT,
            "Accept-Language": "en-US,en;q=0.9",
        }
    )
    response = session.get(
        "https://html.duckduckgo.com/html/",
        params={"q": query},
        timeout=DEFAULT_TIMEOUT,
    )
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
