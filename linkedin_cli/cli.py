"""LinkedIn CLI -- main entry point.

Commands:
  login     Authenticate with LINKEDIN_USERNAME / LINKEDIN_PASSWORD
  logout    Remove saved session
  status    Inspect current session health
  html      Fetch an authenticated LinkedIn URL
  voyager   Call a LinkedIn Voyager endpoint directly
  profile   Fetch and summarize a profile page
  company   Fetch and summarize a company page
  search    Search people, companies, or posts via web-indexed LinkedIn pages
  activity  Find likely public LinkedIn posts/activity for a target
  content   Harvest and inspect public LinkedIn post content
  post      Post-related commands (publish)
  snapshot  Snapshot authenticated user profile
  edit      Edit a profile field
  experience  Experience/position commands
  connect   Send a connection request
  follow    Follow a profile
  dm        Direct message commands
  schedule  Schedule a LinkedIn post
  action    Manage write actions

Environment variables:
  LINKEDIN_USERNAME
  LINKEDIN_PASSWORD
  LINKEDIN_USER_AGENT   (optional)
  LINKEDIN_CLI_HOME     (optional, default: ~/.config/linkedin-cli/)
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
import textwrap
import uuid as _uuid
import warnings
from pathlib import Path
from typing import Any

warnings.filterwarnings("ignore", message="urllib3 v2 only supports OpenSSL")

from bs4 import BeautifulSoup
from requests import Session

from linkedin_cli.config import MOBILE_USER_AGENT
from linkedin_cli.output import render_output
from linkedin_cli.session import (
    CliError,
    ExitCode,
    SESSION_FILE,
    auth_summary,
    build_session,
    fail,
    getenv_required,
    linkedin_login,
    import_browser_session,
    load_env_file,
    load_session,
    masked,
    now_iso,
    request,
    save_session,
)
from linkedin_cli.voyager import (
    clean_text,
    extract_company_slug_from_url,
    extract_profile_slug_from_url,
    fetch_company_summary,
    fetch_profile_summary,
    normalize_company_slug,
    normalize_profile_slug,
    parse_bootstrap_payloads,
    parse_json_response,
    summarize_company_bootstrap,
    summarize_company_html,
    summarize_profile_bootstrap,
    summarize_profile_html,
    try_profile_voyager,
    voyager_get,
)
from linkedin_cli.search import (
    build_activity_queries,
    ddg_html_search,
    filter_linkedin_search_results,
)


# Global flag set by --brief
_BRIEF_MODE = False
_OUTPUT_MODE = "json"


def pretty_print(data: Any) -> None:
    print(render_output(data, mode=_OUTPUT_MODE, brief=_BRIEF_MODE))


def parse_key_values(items: list[str]) -> dict[str, str]:
    out: dict[str, str] = {}
    for item in items:
        if "=" not in item:
            fail(f"Expected KEY=VALUE, got: {item}", code=ExitCode.VALIDATION)
        key, value = item.split("=", 1)
        out[key] = value
    return out


def _validate_positive_limit(limit: int, label: str = "limit") -> None:
    if limit <= 0:
        fail(f"{label.capitalize()} must be greater than zero", code=ExitCode.VALIDATION)


def _run_search(kind: str, query: str, limit: int, enrich: bool) -> dict[str, Any]:
    query = clean_text(query) or ""
    if not query:
        fail("Search query is required", code=ExitCode.VALIDATION)
    _validate_positive_limit(limit)

    if kind == "people":
        ddg_query = f"site:linkedin.com/in {query}"
    elif kind == "companies":
        ddg_query = f"site:linkedin.com/company {query}"
    else:
        ddg_query = f"site:linkedin.com/posts {query}"

    raw_results = ddg_html_search(ddg_query, limit=max(limit * 3, 10))
    filtered = filter_linkedin_search_results(raw_results, kind)[:limit]
    session, _ = load_session(required=False)
    session = session or build_session()
    enriched: list[dict[str, Any]] = []
    for result in filtered:
        item: dict[str, Any] = dict(result)
        try:
            if kind == "people":
                slug = extract_profile_slug_from_url(result["url"])
                item["slug"] = slug
                if slug and enrich:
                    item["summary"] = fetch_profile_summary(session, slug)
            elif kind == "companies":
                slug = extract_company_slug_from_url(result["url"])
                item["slug"] = slug
                if slug and enrich:
                    item["summary"] = fetch_company_summary(session, slug)
        except CliError as exc:
            item["enrichment_error"] = exc.message
        enriched.append(item)

    return {
        "source": "duckduckgo-html",
        "kind": kind,
        "query": query,
        "results": enriched,
    }


def _collect_activity_results(target: str, limit: int) -> dict[str, Any]:
    target = clean_text(target) or ""
    if not target:
        fail("Activity target is required", code=ExitCode.VALIDATION)
    _validate_positive_limit(limit)

    session, _ = load_session(required=False)
    session = session or build_session()
    profile_context: dict[str, Any] | None = None
    search_name: str | None = None
    slug = None
    try:
        slug = normalize_profile_slug(target)
    except CliError:
        slug = None
    if slug:
        try:
            profile_context = fetch_profile_summary(session, slug)
            search_name = clean_text(
                " ".join(
                    part
                    for part in [
                        profile_context.get("first_name") if isinstance(profile_context, dict) else None,
                        profile_context.get("last_name") if isinstance(profile_context, dict) else None,
                    ]
                    if part
                )
            )
        except CliError:
            profile_context = None
    queries = build_activity_queries(target=slug or target, name=search_name)
    collected: list[dict[str, str]] = []
    seen_urls: set[str] = set()
    for query in queries:
        results = filter_linkedin_search_results(ddg_html_search(query, limit=max(limit * 2, 10)), "posts")
        for result in results:
            url = result.get("url", "")
            if not url or url in seen_urls:
                continue
            seen_urls.add(url)
            enriched = dict(result)
            enriched["matched_query"] = query
            collected.append(enriched)
            if len(collected) >= limit:
                break
        if len(collected) >= limit:
            break
    return {
        "source": "duckduckgo-html",
        "target": target,
        "profile_context": profile_context,
        "results": collected,
    }


def _completion_script(shell: str) -> str:
    commands = [
        "login", "logout", "status", "doctor", "completion", "html", "voyager", "profile",
        "company", "search", "activity", "content", "post", "snapshot", "edit", "experience",
        "connect", "follow", "dm", "schedule", "action", "workflow", "discover",
    ]
    joined = " ".join(commands)
    if shell == "zsh":
        return textwrap.dedent(
            f"""
            #compdef linkedin
            _linkedin() {{
              local -a commands
              commands=({joined})
              _describe 'command' commands
            }}
            compdef _linkedin linkedin
            """
        ).strip()
    return textwrap.dedent(
        f"""
        _linkedin_complete() {{
          local cur="${{COMP_WORDS[COMP_CWORD]}}"
          COMPREPLY=( $(compgen -W "{joined}" -- "$cur") )
        }}
        complete -F _linkedin_complete linkedin
        """
    ).strip()


def _build_doctor_report() -> dict[str, Any]:
    from linkedin_cli.config import CONFIG_DIR, ENV_FILE
    from linkedin_cli.write.store import DB_PATH, init_db

    checks: list[dict[str, Any]] = []

    config_exists = CONFIG_DIR.exists()
    checks.append(
        {
            "name": "config_dir",
            "status": "ok" if config_exists else "warn",
            "detail": str(CONFIG_DIR),
        }
    )
    checks.append(
        {
            "name": "env_file",
            "status": "ok" if ENV_FILE.exists() else "warn",
            "detail": str(ENV_FILE),
        }
    )

    session_exists = SESSION_FILE.exists()
    checks.append(
        {
            "name": "session_file",
            "status": "ok" if session_exists else "warn",
            "detail": str(SESSION_FILE),
        }
    )

    try:
        init_db()
        checks.append({"name": "state_db", "status": "ok", "detail": str(DB_PATH)})
    except Exception as exc:
        checks.append({"name": "state_db", "status": "fail", "detail": str(exc)})

    ok = all(check["status"] == "ok" for check in checks)
    return {
        "ok": ok,
        "config_dir": str(CONFIG_DIR),
        "checks": checks,
    }


# ---------------------------------------------------------------------------
#  Account identity helpers (used by write commands)
# ---------------------------------------------------------------------------

def _get_account_id(session: Session) -> str:
    """Fetch the authenticated member ID via /voyager/api/me."""
    response = voyager_get(session, "/voyager/api/me")
    data = parse_json_response(response)
    me = data.get("data") or data
    member_id = me.get("plainId")
    if member_id:
        return str(member_id)
    for item in (data.get("included") or []):
        if isinstance(item, dict):
            urn = item.get("entityUrn") or ""
            if urn.startswith("urn:li:fs_miniProfile:"):
                return urn.split(":")[-1]
    fail("Could not determine account member ID from /voyager/api/me")


def _get_my_urn(session: Session) -> str:
    """Fetch the authenticated user fsd_profile URN via /voyager/api/me."""
    response = voyager_get(session, "/voyager/api/me")
    data = parse_json_response(response)
    for item in (data.get("included") or []):
        if isinstance(item, dict):
            urn = item.get("entityUrn") or ""
            if "fsd_profile" in urn:
                return urn
            dash = item.get("dashEntityUrn") or ""
            if "fsd_profile" in dash:
                return dash
    # Fallback: construct from miniProfile
    me = data.get("data") or data
    mini_urn = me.get("*miniProfile") or ""
    if mini_urn:
        parts = mini_urn.split(":")
        if len(parts) >= 4:
            return f"urn:li:fsd_profile:{parts[-1]}"
    fail("Could not determine fsd_profile URN from /voyager/api/me")


def _get_my_member_hash(session: Session) -> str:
    """Fetch the authenticated user's member hash (the part after fsd_profile:)."""
    urn = _get_my_urn(session)
    # URN format: urn:li:fsd_profile:HASH
    parts = urn.split(":")
    if len(parts) >= 4:
        return parts[-1]
    fail(f"Could not extract member hash from URN: {urn}")


def _resolve_profile_urn(session: Session, profile_input: str) -> str:
    """Resolve a profile URL or slug to a fsd_profile URN."""
    slug = normalize_profile_slug(profile_input)
    # Try voyager profile endpoint
    try:
        response = voyager_get(session, f"/voyager/api/identity/profiles/{slug}/profileView")
        data = parse_json_response(response)
        for item in (data.get("included") or []):
            if not isinstance(item, dict):
                continue
            urn = item.get("entityUrn") or ""
            if "fsd_profile" in urn:
                return urn
            if item.get("publicIdentifier") == slug:
                obj_urn = item.get("objectUrn") or item.get("entityUrn") or ""
                if obj_urn:
                    parts = obj_urn.split(":")
                    if len(parts) >= 4:
                        return f"urn:li:fsd_profile:{parts[-1]}"
    except CliError:
        pass
    # Try bootstrap HTML approach
    try:
        response2 = request(session, "GET", f"https://www.linkedin.com/in/{slug}/")
        bs = summarize_profile_bootstrap(response2.text, slug)
        if bs and bs.get("entity_urn"):
            urn = bs["entity_urn"]
            if "fsd_profile" in urn:
                return urn
            parts = urn.split(":")
            if len(parts) >= 4:
                return f"urn:li:fsd_profile:{parts[-1]}"
    except CliError:
        pass
    fail(f"Could not resolve profile URN for: {profile_input}")


def _resolve_mwlite_profile_context(session: Session, profile_input: str) -> dict[str, Any]:
    """Resolve mobile-web profile context for non-self mutations."""
    slug = normalize_profile_slug(profile_input)
    response = request(
        session,
        "GET",
        f"https://www.linkedin.com/in/{slug}/",
        headers={
            "User-Agent": MOBILE_USER_AGENT,
            "Accept-Language": "en-US,en;q=0.9",
        },
    )
    soup = BeautifulSoup(response.text, "html.parser")
    page_key_tag = soup.find("meta", attrs={"name": "pageKey"})
    action_container = soup.find(attrs={"data-member-urn": True, "data-vanity-name": True})
    if page_key_tag is None or action_container is None:
        fail(f"Could not resolve mobile profile context for: {profile_input}")

    action_text = " ".join(action_container.get_text(" ", strip=True).split())
    return {
        "slug": slug,
        "page_key": page_key_tag.get("content"),
        "member_urn": action_container.get("data-member-urn"),
        "vanity_name": action_container.get("data-vanity-name") or slug,
        "connection_state": action_text,
        "message_locked": bool(action_container.select_one("#trigger-upsell")),
    }


def _fetch_dm_conversations(session: Session, limit: int) -> list[dict[str, Any]]:
    response = voyager_get(session, "/voyager/api/messaging/conversations", params={"keyVersion": "LEGACY_INBOX"})
    data = parse_json_response(response)
    conversations: list[dict[str, Any]] = []
    included = data.get("included") or []
    entity_map: dict[str, dict[str, Any]] = {}
    for item in included:
        if isinstance(item, dict):
            urn = item.get("entityUrn") or item.get("dashEntityUrn") or ""
            if urn:
                entity_map[urn] = item

    for item in included:
        if not isinstance(item, dict):
            continue
        item_type = item.get("$type") or ""
        if "Conversation" not in item_type:
            continue
        urn = item.get("entityUrn") or ""
        if not urn:
            continue
        participants: list[dict[str, Any]] = []
        for participant_ref in (item.get("*participants") or item.get("participants") or []):
            participant = entity_map.get(participant_ref) if isinstance(participant_ref, str) else participant_ref
            if not isinstance(participant, dict):
                continue
            mini = participant.get("*miniProfile") or participant.get("miniProfile")
            mini_profile = entity_map.get(mini) if isinstance(mini, str) else mini
            if not isinstance(mini_profile, dict):
                continue
            display_name = " ".join(
                part
                for part in [mini_profile.get("firstName"), mini_profile.get("lastName")]
                if part
            ).strip()
            public_identifier = mini_profile.get("publicIdentifier")
            participants.append(
                {
                    "profile_key": public_identifier or display_name.lower().replace(" ", "-"),
                    "public_identifier": public_identifier,
                    "display_name": display_name or public_identifier or "Unknown",
                    "member_urn": mini_profile.get("entityUrn") or mini_profile.get("objectUrn"),
                }
            )
        conversations.append(
            {
                "conversation_urn": urn,
                "last_activity": item.get("lastActivityAt"),
                "participants": participants,
                "messages": [],
            }
        )

    convo_map = {conversation["conversation_urn"]: conversation for conversation in conversations}
    for item in included:
        if not isinstance(item, dict):
            continue
        item_type = item.get("$type") or ""
        if "Message" not in item_type or "Delivery" in item_type:
            continue
        conversation_ref = item.get("*conversation") or item.get("conversation") or item.get("*dashConversation") or ""
        if isinstance(conversation_ref, dict):
            conversation_ref = conversation_ref.get("entityUrn") or conversation_ref.get("dashEntityUrn") or ""
        if conversation_ref not in convo_map:
            continue
        convo_map[conversation_ref]["messages"].append(
            {
                "message_urn": item.get("entityUrn") or item.get("dashEntityUrn"),
                "sender_urn": item.get("*sender") or item.get("sender"),
                "created_at": item.get("deliveredAt") or item.get("createdAt"),
                "text": ((item.get("body") or {}).get("text") if isinstance(item.get("body"), dict) else None) or "",
            }
        )
    conversations.sort(key=lambda convo: convo.get("last_activity") or 0, reverse=True)
    return conversations[:limit]


# ---------------------------------------------------------------------------
#  Read-only command handlers
# ---------------------------------------------------------------------------

def cmd_login(args: argparse.Namespace) -> None:
    load_env_file()
    session: Session
    meta: dict[str, Any]

    if args.browser:
        session, meta = import_browser_session(
            browser_name=args.browser_name,
            timeout=args.timeout,
            user_agent=args.user_agent or None,
        )
    else:
        username = args.username or getenv_required("LINKEDIN_USERNAME")
        password = args.password or getenv_required("LINKEDIN_PASSWORD")
        session = build_session(args.user_agent)
        result = linkedin_login(session, username, password)
        if result["logged_in"]:
            meta = {
                "source": "form",
                "user_agent": session.headers.get("User-Agent"),
                "username_hint": masked(username),
                "login_url": result["final_url"],
                "last_login_at": now_iso(),
            }
            save_session(session, meta)
            pretty_print(
                {
                    "ok": True,
                    "message": "LinkedIn login succeeded",
                    "session_file": str(SESSION_FILE),
                    "final_url": result["final_url"],
                    "auth": result["status"],
                }
            )
            return
        if result["challenge"]:
            fail(
                "LinkedIn requested an additional verification step during login. "
                "The partial browserless login did not complete. Try again later or use `--browser`."
            )
        fail(result["error"] or "LinkedIn login failed")

    save_session(session, meta)
    pretty_print(
        {
            "ok": True,
            "message": "LinkedIn login succeeded",
            "session_file": str(SESSION_FILE),
            "meta": meta,
            "auth": auth_summary(session),
        }
    )


def cmd_logout(args: argparse.Namespace) -> None:
    if SESSION_FILE.exists():
        SESSION_FILE.unlink()
    pretty_print({"ok": True, "message": "LinkedIn session removed", "session_file": str(SESSION_FILE)})


def cmd_status(args: argparse.Namespace) -> None:
    session, meta = load_session(required=True)
    assert session is not None
    summary: dict[str, Any] = {
        "session_file": str(SESSION_FILE),
        "saved_meta": meta,
        "auth": auth_summary(session),
    }
    try:
        response = voyager_get(session, "/voyager/api/me")
        data = parse_json_response(response)
        summary["voyager_me_ok"] = True
        summary["voyager_me_status"] = response.status_code
        me_data = data.get("data") or {}
        included = data.get("included") or []
        mini_profile_urn = me_data.get("*miniProfile") or me_data.get("miniProfile")
        mini_profile = None
        for item in included:
            if not isinstance(item, dict):
                continue
            if item.get("entityUrn") == mini_profile_urn or item.get("dashEntityUrn") == mini_profile_urn:
                mini_profile = item
                break
        mini_profile = mini_profile or {}
        summary["account"] = {
            "entity_urn": mini_profile.get("entityUrn") or mini_profile.get("dashEntityUrn") or me_data.get("entityUrn"),
            "member_id": me_data.get("plainId"),
            "public_identifier": mini_profile.get("publicIdentifier"),
            "first_name": mini_profile.get("firstName"),
            "last_name": mini_profile.get("lastName"),
            "occupation": mini_profile.get("occupation"),
        }
    except CliError as exc:
        summary["voyager_me_ok"] = False
        summary["voyager_error"] = exc.message
        response = request(session, "GET", "https://www.linkedin.com/feed/")
        summary["feed_url"] = response.url
        summary["feed_title"] = BeautifulSoup(response.text, "html.parser").title.get_text(strip=True) if BeautifulSoup(response.text, "html.parser").title else None
    pretty_print(summary)


def cmd_doctor(args: argparse.Namespace) -> None:
    report = _build_doctor_report()
    pretty_print(report)
    if not report["ok"]:
        raise SystemExit(ExitCode.GENERAL)


def cmd_completion(args: argparse.Namespace) -> None:
    print(_completion_script(args.shell))


def cmd_html(args: argparse.Namespace) -> None:
    session, _ = load_session(required=not args.public)
    if session is None:
        session = build_session()
    response = request(session, "GET", args.url)
    if args.output:
        output_path = Path(args.output).expanduser()
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(response.text, encoding="utf-8")
        pretty_print({
            "ok": True,
            "status": response.status_code,
            "url": response.url,
            "output": str(output_path),
            "bytes": len(response.text),
        })
        return
    print(response.text)


def cmd_voyager(args: argparse.Namespace) -> None:
    session, _ = load_session(required=True)
    assert session is not None
    params = parse_key_values(args.param or [])
    response = voyager_get(session, args.path, params=params)
    data = parse_json_response(response)
    pretty_print(data)


def cmd_profile(args: argparse.Namespace) -> None:
    slug = normalize_profile_slug(args.target)
    url = f"https://www.linkedin.com/in/{slug}/"
    session, _ = load_session(required=False)
    if session is not None:
        voyager_summary = try_profile_voyager(session, slug)
        if voyager_summary:
            pretty_print({"slug": slug, "url": url, "summary": voyager_summary})
            return
    session = session or build_session()
    response = request(session, "GET", url)
    bootstrap_summary = summarize_profile_bootstrap(response.text, slug)
    if bootstrap_summary:
        pretty_print({"slug": slug, "url": response.url, "summary": bootstrap_summary})
        return
    summary = summarize_profile_html(response.text, response.url)
    pretty_print({"slug": slug, "url": response.url, "summary": summary})


def cmd_company(args: argparse.Namespace) -> None:
    slug = normalize_company_slug(args.target)
    url = f"https://www.linkedin.com/company/{slug}/"
    session, _ = load_session(required=False)
    session = session or build_session()
    response = request(session, "GET", url)
    bootstrap_summary = summarize_company_bootstrap(response.text, slug)
    if bootstrap_summary:
        pretty_print({"slug": slug, "url": response.url, "summary": bootstrap_summary})
        return
    summary = summarize_company_html(response.text, response.url)
    pretty_print({"slug": slug, "url": response.url, "summary": summary})


def cmd_search(args: argparse.Namespace) -> None:
    pretty_print(_run_search(args.kind, args.query, args.limit, args.enrich))


def cmd_activity(args: argparse.Namespace) -> None:
    pretty_print(_collect_activity_results(args.target, args.limit))


def cmd_content_harvest(args: argparse.Namespace) -> None:
    from linkedin_cli import content

    _validate_positive_limit(args.limit)
    _validate_positive_limit(args.per_query, label="per-query")
    _validate_positive_limit(args.search_timeout, label="search-timeout")
    _validate_positive_limit(args.fetch_workers, label="fetch-workers")
    _validate_positive_limit(args.query_workers, label="query-workers")
    content.init_content_db()
    query_terms = list(args.query or [])
    topics = list(args.topic or [])
    industries = list(args.industry or [])
    freshness_buckets = list(getattr(args, "freshness_bucket", []) or [])
    expansion = getattr(args, "expansion", "standard")
    backend = getattr(args, "backend", "auth-only")
    public_search = getattr(args, "public_search", "ddg")
    searxng_url = getattr(args, "searxng_url", None)
    searxng_engines = [engine for engine in list(getattr(args, "searxng_engine", []) or []) if engine]
    if not query_terms and not args.resume_job:
        query_terms = content.build_harvest_queries(industries=industries, topics=topics, expansion=expansion, freshness_buckets=freshness_buckets)
    if not args.resume_job:
        query_terms = content.prepare_backend_queries(query_terms, backend)
    if not query_terms and not args.resume_job:
        fail("Provide at least one --query or an --industry/--topic pair for content harvest", code=ExitCode.VALIDATION)
    if not args.resume_job:
        theoretical_capacity = len(query_terms) * int(args.per_query)
        if theoretical_capacity < int(args.limit):
            print(
                f"[content] configured query surface is small for limit={args.limit}: about {theoretical_capacity} raw result slots before dedupe; raise --per-query or use --expansion broad",
                file=sys.stderr,
                flush=True,
            )

    def emit_progress(event: dict[str, Any]) -> None:
        event_type = event.get("event")
        if event_type == "query_started":
            print(
                f"[content] query {event['query_index']}/{event['query_count']}: {event['query']}",
                file=sys.stderr,
                flush=True,
            )
        elif event_type == "query_results":
            print(f"[content] results: {event['result_count']} for {event['query']}", file=sys.stderr, flush=True)
        elif event_type == "query_page":
            print(
                f"[content] page advanced for {event['query']}: start={event['start']} page_count={event['page_count']}",
                file=sys.stderr,
                flush=True,
            )
        elif event_type == "query_backend_failed":
            print(
                f"[content] backend failed ({event.get('backend')}): {event.get('query')} :: {event.get('error')}",
                file=sys.stderr,
                flush=True,
            )
        elif event_type == "post_stored":
            print(
                f"[content] stored {event['stored_count']}/{event['limit']}: {event['url']}",
                file=sys.stderr,
                flush=True,
            )
        elif event_type in {"query_failed", "fetch_failed"}:
            print(f"[content] {event_type}: {event.get('error')}", file=sys.stderr, flush=True)
        elif event_type == "complete":
            print(
                f"[content] complete: {event['stored_count']} stored across {event['query_count']} queries",
                file=sys.stderr,
                flush=True,
            )

    summary = content.harvest_posts(
        industry=industries[0] if industries else None,
        industries=industries,
        topics=topics,
        limit=args.limit,
        per_query=args.per_query,
        query_terms=query_terms,
        search_timeout=args.search_timeout,
        progress=emit_progress,
        fetch_workers=args.fetch_workers,
        query_workers=args.query_workers,
        job_name=args.job_name,
        resume_job=args.resume_job,
        retry_budget=args.retry_budget,
        cooldown_seconds=args.cooldown_seconds,
        min_request_interval=args.min_request_interval,
        jitter_seconds=args.jitter_seconds,
        backend=backend,
        public_search=public_search,
        searxng_url=searxng_url,
        searxng_engines=searxng_engines,
    )
    if args.embed:
        embed_summary = content.embed_posts(limit=summary["stored_count"], model=args.embed_model, batch_size=args.embed_batch_size)
        summary["embedding"] = embed_summary
    pretty_print(summary)


def cmd_content_harvest_campaign(args: argparse.Namespace) -> None:
    from linkedin_cli import content

    _validate_positive_limit(args.limit)
    _validate_positive_limit(args.per_query, label="per-query")
    _validate_positive_limit(args.per_job_limit, label="per-job-limit")
    _validate_positive_limit(args.queries_per_job, label="queries-per-job")
    _validate_positive_limit(args.search_timeout, label="search-timeout")
    _validate_positive_limit(args.fetch_workers, label="fetch-workers")
    _validate_positive_limit(args.query_workers, label="query-workers")
    content.init_content_db()
    query_terms = list(args.query or [])
    topics = list(args.topic or [])
    industries = list(args.industry or [])
    freshness_buckets = list(getattr(args, "freshness_bucket", []) or [])
    expansion = getattr(args, "expansion", "standard")
    backend = getattr(args, "backend", "auth-only")
    public_search = getattr(args, "public_search", "ddg")
    searxng_url = getattr(args, "searxng_url", None)
    searxng_engines = [engine for engine in list(getattr(args, "searxng_engine", []) or []) if engine]
    if not query_terms:
        query_terms = content.build_harvest_queries(industries=industries, topics=topics, expansion=expansion, freshness_buckets=freshness_buckets)
    query_terms = content.prepare_backend_queries(query_terms, backend)
    if not query_terms:
        fail("Provide at least one --query or an --industry/--topic pair for content harvest-campaign", code=ExitCode.VALIDATION)
    theoretical_capacity = len(query_terms) * int(args.per_query)
    if theoretical_capacity < int(args.limit):
        print(
            f"[content] configured query surface is small for limit={args.limit}: about {theoretical_capacity} raw result slots before dedupe; raise --per-query or use --expansion broad",
            file=sys.stderr,
            flush=True,
        )

    def emit_progress(event: dict[str, Any]) -> None:
        event_type = event.get("event")
        if event_type == "campaign_started":
            print(
                f"[content] campaign started: {event['job_count']} jobs across {event['query_count']} queries"
                f" (speed={event.get('speed', 'balanced')} query_workers={event.get('query_workers')})",
                file=sys.stderr,
                flush=True,
            )
            deferred = event.get("deferred_post_processing") or {}
            if bool(deferred.get("materialize")) or bool(deferred.get("embed")) or int(deferred.get("retrain_every") or 0) > 0:
                print(
                    f"[content] deferred post-processing for speed: materialize={bool(deferred.get('materialize'))} "
                    f"embed={bool(deferred.get('embed'))} retrain_every={int(deferred.get('retrain_every') or 0)}",
                    file=sys.stderr,
                    flush=True,
                )
        elif event_type == "campaign_job_started":
            print(
                f"[content] campaign job {event['job_index']}/{event['job_count']}: {event['job_id']} ({event['query_count']} queries)",
                file=sys.stderr,
                flush=True,
            )
        elif event_type == "query_started":
            print(
                f"[content] query {event['query_index']}/{event['query_count']}: {event['query']}",
                file=sys.stderr,
                flush=True,
            )
        elif event_type == "query_results":
            print(f"[content] results: {event['result_count']} for {event['query']}", file=sys.stderr, flush=True)
        elif event_type == "query_page":
            print(
                f"[content] page advanced for {event['query']}: start={event['start']} page_count={event['page_count']}",
                file=sys.stderr,
                flush=True,
            )
        elif event_type == "query_backend_failed":
            print(
                f"[content] backend failed ({event.get('backend')}): {event.get('query')} :: {event.get('error')}",
                file=sys.stderr,
                flush=True,
            )
        elif event_type == "post_stored":
            print(
                f"[content] stored {event['stored_count']}/{event['limit']}: {event['url']}",
                file=sys.stderr,
                flush=True,
            )
        elif event_type in {"query_failed", "fetch_failed"}:
            print(f"[content] {event_type}: {event.get('error')}", file=sys.stderr, flush=True)
        elif event_type == "campaign_job_completed":
            print(
                f"[content] campaign job complete: {event['job_id']} stored {event['stored_count']} total {event['stored_total']}/{event['limit']}",
                file=sys.stderr,
                flush=True,
            )
        elif event_type == "campaign_materialized":
            print(f"[content] materialized {event['job_id']}: {event.get('rows_loaded', 0)} rows", file=sys.stderr, flush=True)
        elif event_type == "campaign_embedded":
            print(f"[content] embedded {event['job_id']}: {event.get('embedded_count', 0)} posts", file=sys.stderr, flush=True)
        elif event_type == "campaign_trained":
            print(
                f"[content] trained model after {event['job_id']}: {event.get('model_name')} ({event.get('sample_count', 0)} samples)",
                file=sys.stderr,
                flush=True,
            )
        elif event_type == "campaign_complete":
            print(
                f"[content] campaign complete: {event['stored_count']} stored across {event['job_count']} jobs",
                file=sys.stderr,
                flush=True,
            )

    summary = content.harvest_campaign(
        industry=industries[0] if industries else None,
        industries=industries,
        topics=topics,
        query_terms=query_terms,
        limit=args.limit,
        per_query=args.per_query,
        per_job_limit=args.per_job_limit,
        queries_per_job=args.queries_per_job,
        search_timeout=args.search_timeout,
        fetch_workers=args.fetch_workers,
        query_workers=args.query_workers,
        retry_budget=args.retry_budget,
        cooldown_seconds=args.cooldown_seconds,
        min_request_interval=args.min_request_interval,
        jitter_seconds=args.jitter_seconds,
        job_prefix=args.job_prefix,
        materialize=args.materialize,
        embed=args.embed,
        embed_model=args.embed_model,
        embed_batch_size=args.embed_batch_size,
        retrain_every=args.retrain_every,
        train_model_name=args.train_model_name,
        train_scope=args.train_scope,
        train_min_samples=args.train_min_samples,
        progress=emit_progress,
        backend=backend,
        expansion=expansion,
        freshness_buckets=freshness_buckets,
        resume=getattr(args, "resume", False),
        prune_min_yield=getattr(args, "prune_min_yield", None),
        prune_min_attempts=getattr(args, "prune_min_attempts", 2),
        stop_min_yield_rate=getattr(args, "stop_min_yield_rate", None),
        stop_window=getattr(args, "stop_window", 3),
        speed=getattr(args, "speed", "balanced"),
        public_search=public_search,
        searxng_url=searxng_url,
        searxng_engines=searxng_engines,
    )
    pretty_print(summary)


def cmd_content_train_warehouse_model(args: argparse.Namespace) -> None:
    from linkedin_cli import content_warehouse

    _validate_positive_limit(args.min_samples, label="min-samples")
    _validate_positive_limit(args.max_rows, label="max-rows")
    pretty_print(
        content_warehouse.train_warehouse_model(
            name=args.name,
            industries=list(args.industry or []),
            min_samples=args.min_samples,
            max_rows=args.max_rows,
        )
    )


def cmd_content_warehouse_model(args: argparse.Namespace) -> None:
    from linkedin_cli import content_warehouse

    pretty_print(content_warehouse.get_warehouse_model(args.name) or {})


def cmd_content_build_foundation_views(args: argparse.Namespace) -> None:
    from linkedin_cli import content_warehouse

    pretty_print(content_warehouse.build_foundation_views(industries=list(args.industry or [])))


def cmd_content_train_stacked_model(args: argparse.Namespace) -> None:
    from linkedin_cli import content_stack

    _validate_positive_limit(args.min_samples, label="min-samples")
    pretty_print(
        content_stack.train_stacked_model(
            model_name=args.name,
            artifact_dir=Path(args.artifact_dir) if args.artifact_dir else None,
            industries=list(args.industry or []),
            min_samples=args.min_samples,
            holdout_dir=Path(args.holdout_dir) if args.holdout_dir else None,
        )
    )


def cmd_content_rerank_target(args: argparse.Namespace) -> None:
    from linkedin_cli import content_stack

    _validate_positive_limit(args.limit, label="limit")
    target_profile = json.loads(Path(args.target_file).read_text(encoding="utf-8"))
    selected = content_stack.select_best_stacked_model() if not args.model_name else None
    model_name = args.model_name or str((selected or {}).get("model_name") or "")
    ranked = content_stack.rerank_for_target(
        posts=None,
        target_profile=target_profile,
        model_name=model_name,
        auto_calibrate_weights=bool(args.auto_calibrate_weights),
    )
    pretty_print(
        {
            "model_name": model_name,
            "selected_model": selected,
            "auto_calibrate_weights": bool(args.auto_calibrate_weights),
            "target_file": args.target_file,
            "target_profile": target_profile,
            "results": ranked[: args.limit],
            "count": min(len(ranked), args.limit),
        }
    )


def cmd_content_select_stacked_model(args: argparse.Namespace) -> None:
    from linkedin_cli import content_stack

    pretty_print(content_stack.select_best_stacked_model())


def cmd_content_audit_targets(args: argparse.Namespace) -> None:
    from linkedin_cli import content_stack
    from linkedin_cli import content_warehouse

    _validate_positive_limit(args.limit, label="limit")
    _validate_positive_limit(args.sample_size, label="sample-size")
    selected = content_stack.select_best_stacked_model() if not args.model_name else None
    model_name = args.model_name or str((selected or {}).get("model_name") or "")
    profiles: dict[str, dict[str, Any]] = {}
    for target_path in list(args.target_file or []):
        path = Path(target_path)
        profiles[path.stem] = json.loads(path.read_text(encoding="utf-8"))
    conn = content_warehouse._warehouse_connect(read_only=True)
    try:
        cursor = conn.execute("SELECT * FROM content_foundation_posts ORDER BY url ASC LIMIT ?", (int(args.sample_size),))
        columns = [str(column[0]) for column in (cursor.description or [])]
        posts = [dict(zip(columns, row)) for row in cursor.fetchall()]
    finally:
        conn.close()
    pretty_print(
        {
            "selected_model": selected,
            **content_stack.audit_target_profiles(
                profiles=profiles,
                model_name=model_name,
                posts=posts,
                limit=args.limit,
            ),
        }
    )


def cmd_content_list(args: argparse.Namespace) -> None:
    from linkedin_cli import content

    content.init_content_db()
    posts = content.list_posts(limit=args.limit, industry=args.industry, author=args.author, include_vectors=args.full)
    if not args.full:
        for post in posts:
            embedding_dim, fingerprint_dim = content.summarize_post_dimensions(post)
            post["embedding_dim"] = embedding_dim
            post["fingerprint_dim"] = fingerprint_dim
            post.pop("embedding", None)
            post.pop("fingerprint", None)
    pretty_print({"posts": posts})


def cmd_content_stats(args: argparse.Namespace) -> None:
    from linkedin_cli import content

    content.init_content_db()
    pretty_print(content.content_stats())


def cmd_content_embed(args: argparse.Namespace) -> None:
    from linkedin_cli import content

    content.init_content_db()
    _validate_positive_limit(args.limit)
    _validate_positive_limit(args.batch_size, label="batch-size")

    def emit_progress(event: dict[str, Any]) -> None:
        event_type = event.get("event")
        if event_type == "embed_batch_started":
            batch_end = int(event["batch_start"]) + int(event["batch_size"])
            print(
                f"[content] embedding batch {int(event['batch_start']) + 1}-{batch_end}/{event['total']} with {event['model']}",
                file=sys.stderr,
                flush=True,
            )
        elif event_type == "embed_post_stored":
            print(
                f"[content] embedded {event['embedded_count']}/{event['total']}: {event['url']}",
                file=sys.stderr,
                flush=True,
            )
        elif event_type == "embed_complete":
            print(
                f"[content] embedding complete: {event['embedded_count']}/{event['total']} with {event['model']}",
                file=sys.stderr,
                flush=True,
            )

    pretty_print(
        content.embed_posts(
            limit=args.limit,
            model=args.model,
            batch_size=args.batch_size,
            missing_only=not args.all,
            progress=emit_progress,
        )
    )


def _load_inline_or_file_text(text: str | None, text_file: str | None, *, label: str) -> str:
    loaded = text
    if text_file:
        path = Path(text_file).expanduser()
        if not path.exists():
            fail(f"{label} file not found: {path}", code=ExitCode.NOT_FOUND)
        loaded = path.read_text(encoding="utf-8")
    loaded = (loaded or "").strip()
    if not loaded:
        fail(f"{label} text is required", code=ExitCode.VALIDATION)
    return loaded


def _content_brief_from_args(args: argparse.Namespace) -> dict[str, str]:
    brief: dict[str, str] = {}
    for key in ("audience", "objective", "tone", "format", "length", "cta"):
        value = str(getattr(args, key, "") or "").strip()
        if value:
            brief[key] = value
    return brief


def cmd_content_patterns(args: argparse.Namespace) -> None:
    from linkedin_cli import content

    content.init_content_db()
    _validate_positive_limit(args.limit)
    pretty_print(
        content.ranked_patterns(
            limit=args.limit,
            industry=args.industry,
            topics=list(args.topic or []),
            author=args.author,
            owned_only=args.owned_only,
        )
    )


def cmd_content_score_draft(args: argparse.Namespace) -> None:
    from linkedin_cli import content

    content.init_content_db()
    text = _load_inline_or_file_text(args.text, args.text_file, label="Draft")
    pretty_print(content.score_draft(text=text, industry=args.industry, topics=list(args.topic or []), model=args.model))


def cmd_content_playbook(args: argparse.Namespace) -> None:
    from linkedin_cli import content

    content.init_content_db()
    _validate_positive_limit(args.limit)
    pretty_print(
        content.build_playbook(
            industry=args.industry,
            topics=list(args.topic or []),
            author=args.author,
            owned_only=args.owned_only,
            limit=args.limit,
        )
    )


def cmd_content_train_model(args: argparse.Namespace) -> None:
    from linkedin_cli import content

    content.init_content_db()
    pretty_print(
        content.train_outcome_model(
            name=args.name,
            scope=args.scope,
            min_samples=args.min_samples,
            industry=args.industry,
            topics=list(args.topic or []),
        )
    )


def cmd_content_model(args: argparse.Namespace) -> None:
    from linkedin_cli import content

    content.init_content_db()
    model = content.get_trained_model(name=args.name)
    if model is None:
        fail(f"Content model not found: {args.name}", code=ExitCode.NOT_FOUND)
    pretty_print(model)


def cmd_telemetry_sync(args: argparse.Namespace) -> None:
    from linkedin_cli import content
    from linkedin_cli.write import store

    store.init_db()
    content.init_content_db()
    urls = list(args.url or [])
    if args.owned_posts and not urls:
        urls = [
            str(post["url"])
            for post in content.list_posts(limit=100000)
            if post.get("owned_by_me")
        ]
    if not urls:
        fail("Provide at least one --url or use --owned-posts", code=ExitCode.VALIDATION)

    def emit_progress(event: dict[str, Any]) -> None:
        if event.get("event") == "telemetry_sync_started":
            print(
                f"[telemetry] syncing {event['index']}/{event['count']}: {event['url']}",
                file=sys.stderr,
                flush=True,
            )
        elif event.get("event") == "telemetry_synced":
            print(
                f"[telemetry] synced: {event['url']} reactions={event['reaction_count']} comments={event['comment_count']}",
                file=sys.stderr,
                flush=True,
            )

    pretty_print(content.sync_owned_post_telemetry(urls=urls, progress=emit_progress))


def cmd_telemetry_stats(args: argparse.Namespace) -> None:
    from linkedin_cli import content
    from linkedin_cli.write import store

    store.init_db()
    content.init_content_db()
    pretty_print(
        {
            "events": store.telemetry_stats(),
            "content": content.content_stats(),
        }
    )


def cmd_content_rewrite(args: argparse.Namespace) -> None:
    from linkedin_cli import content

    content.init_content_db()
    text = _load_inline_or_file_text(args.text, args.text_file, label="Draft")
    pretty_print(content.rewrite_draft(text=text, industry=args.industry, topics=list(args.topic or []), goal=args.goal, model=args.model))


def cmd_content_maximize(args: argparse.Namespace) -> None:
    from linkedin_cli import content

    content.init_content_db()
    text = _load_inline_or_file_text(args.text, args.text_file, label="Draft")
    pretty_print(
        content.maximize_draft(
            text=text,
            industry=args.industry,
            topics=list(args.topic or []),
            model=args.model,
            candidate_goals=list(args.goal or []),
        )
    )


def cmd_content_polish_and_score(args: argparse.Namespace) -> None:
    from linkedin_cli import content

    _validate_positive_limit(args.limit, label="limit")
    init_warning = None
    try:
        content.init_content_db()
    except sqlite3.DatabaseError as exc:
        init_warning = f"Local corpus DB unavailable: {exc}"
    text = _load_inline_or_file_text(args.text, args.text_file, label="Draft")
    target_profile = json.loads(Path(args.target_file).read_text(encoding="utf-8")) if args.target_file else None
    result = content.polish_and_score(
        text=text,
        industry=args.industry,
        topics=list(args.topic or []),
        model=args.model,
        candidate_goals=list(args.goal or []),
        stacked_model_name=args.stacked_model_name,
        target_profile=target_profile,
        auto_calibrate_weights=bool(args.auto_calibrate_weights),
        limit=args.limit,
        fresh=bool(args.fresh),
        long_form=bool(args.long_form),
    )
    if init_warning:
        result.setdefault("warnings", [])
        if init_warning not in result["warnings"]:
            result["warnings"].insert(0, init_warning)
        result["fallback_mode"] = result.get("fallback_mode") or "stacked_only"
    pretty_print(result)


def cmd_content_create(args: argparse.Namespace) -> None:
    from linkedin_cli import content

    content.init_content_db()
    prompt = _load_inline_or_file_text(args.prompt, args.prompt_file, label="Prompt")
    brief = _content_brief_from_args(args)
    pretty_print(
        content.create_drafts(
            prompt=prompt,
            industry=args.industry,
            topics=list(args.topic or []),
            model=args.model,
            candidate_goals=list(args.goal or []),
            candidate_count=args.count,
            generator=args.generator,
            speed=args.speed,
            brief=brief,
        )
    )


def cmd_content_choose(args: argparse.Namespace) -> None:
    from linkedin_cli import content

    content.init_content_db()
    prompt = _load_inline_or_file_text(args.prompt, args.prompt_file, label="Prompt")
    target_profile = json.loads(Path(args.target_file).read_text(encoding="utf-8")) if args.target_file else None
    brief = _content_brief_from_args(args)
    pretty_print(
        content.choose_draft(
            prompt=prompt,
            industry=args.industry,
            topics=list(args.topic or []),
            model=args.model,
            candidate_goals=list(args.goal or []),
            candidate_count=args.count,
            generator=args.generator,
            speed=args.speed,
            brief=brief,
            policy_name=args.policy_name,
            policy_alpha=args.alpha,
            log_decision=args.log_decision,
            context_key=args.context_key,
            polish_selected=bool(args.polish),
            stacked_model_name=args.stacked_model_name,
            target_profile=target_profile,
            auto_calibrate_weights=bool(args.auto_calibrate_weights),
            polish_limit=args.polish_limit,
        )
    )


def cmd_content_queue(args: argparse.Namespace) -> None:
    from linkedin_cli import content

    content.init_content_db()
    if args.prompt or args.prompt_file:
        prompt = _load_inline_or_file_text(args.prompt, args.prompt_file, label="Prompt")
        pretty_print(
            content.queue_drafts(
                prompt=prompt,
                industry=args.industry,
                topics=list(args.topic or []),
                model=args.model,
                candidate_goals=list(args.goal or []),
                candidate_count=args.count,
                generator=args.generator,
                speed=args.speed,
            )
        )
        return
    pretty_print(content.list_candidate_queue(limit=args.limit, status=args.status))


def cmd_content_show_candidate(args: argparse.Namespace) -> None:
    from linkedin_cli import content

    content.init_content_db()
    pretty_print(content.get_candidate(args.candidate_id))


def cmd_content_mark_published(args: argparse.Namespace) -> None:
    from linkedin_cli import content

    content.init_content_db()
    pretty_print(content.mark_candidate_published(args.candidate_id, post_url=args.post_url))


def cmd_content_trace_list(args: argparse.Namespace) -> None:
    from linkedin_cli import traces

    traces.init_trace_db()
    pretty_print({"traces": traces.list_traces(limit=args.limit, trace_type=args.trace_type, status=args.status)})


def cmd_content_trace_show(args: argparse.Namespace) -> None:
    from linkedin_cli import traces

    traces.init_trace_db()
    pretty_print(traces.get_trace(args.trace_id))


def cmd_content_trace_export(args: argparse.Namespace) -> None:
    from linkedin_cli import traces

    traces.init_trace_db()
    pretty_print(traces.export_trace(args.trace_id, output_path=args.output))


def cmd_content_tui(args: argparse.Namespace) -> None:
    from linkedin_cli import tui

    if getattr(args, "once", False):
        pretty_print(tui.build_content_dashboard_snapshot(limit=args.limit, trace_type=args.trace_type))
        return
    tui.run_content_tui(refresh_seconds=args.refresh, limit=args.limit, trace_type=args.trace_type)


def cmd_content_replay(args: argparse.Namespace) -> None:
    from linkedin_cli import replay_env

    pretty_print(replay_env.replay_trace(args.trace_id, policy_name=args.policy_name))


def cmd_content_autonomy_run(args: argparse.Namespace) -> None:
    from linkedin_cli import content

    content.init_content_db()
    prompt = _load_inline_or_file_text(args.prompt, args.prompt_file, label="Prompt")
    target_profile = json.loads(Path(args.target_file).read_text(encoding="utf-8")) if args.target_file else None
    brief = _content_brief_from_args(args)
    pretty_print(
        content.run_autonomy(
            prompt=prompt,
            industry=args.industry,
            topics=list(args.topic or []),
            model=args.model,
            candidate_goals=list(args.goal or []),
            candidate_count=args.count,
            generator=args.generator,
            speed=args.speed,
            brief=brief,
            decision_provider=args.decision_provider,
            policy_name=args.policy_name,
            policy_alpha=args.alpha,
            mode=args.mode,
            post_url=args.post_url,
            polish_selected=bool(args.polish_selected),
            stacked_model_name=args.stacked_model_name,
            target_profile=target_profile,
            auto_calibrate_weights=bool(args.auto_calibrate_weights),
            polish_limit=args.polish_limit,
        )
    )


def cmd_content_provider_set(args: argparse.Namespace) -> None:
    from linkedin_cli import llm_providers

    pretty_print(
        llm_providers.save_provider_config(
            provider_name=args.provider,
            api_key=args.api_key,
            model=args.model,
            base_url=args.base_url,
        )
    )


def cmd_content_provider_show(args: argparse.Namespace) -> None:
    from linkedin_cli import llm_providers

    config = dict(llm_providers.load_provider_config(args.provider))
    if config.get("api_key"):
        config["api_key"] = "***"
    pretty_print(config)


def cmd_content_train_policy(args: argparse.Namespace) -> None:
    from linkedin_cli import policy

    policy.init_policy_db()
    pretty_print(
        policy.train_policy(
            policy_name=args.policy_name,
            context_type=args.context_type,
            min_samples=args.min_samples,
            alpha=args.alpha,
            ridge=args.ridge,
        )
    )


def cmd_content_policy_report(args: argparse.Namespace) -> None:
    from linkedin_cli import policy

    policy.init_policy_db()
    pretty_print(policy.policy_report(policy_name=args.policy_name, context_type=args.context_type))


def cmd_content_sync_outcomes(args: argparse.Namespace) -> None:
    from linkedin_cli import content

    content.init_content_db()
    urls = list(args.url or [])
    if not urls:
        fail("Provide at least one --url for outcome sync", code=ExitCode.VALIDATION)

    def emit_progress(event: dict[str, Any]) -> None:
        if event.get("event") == "outcome_sync_started":
            print(
                f"[content] syncing outcome {event['index']}/{event['count']}: {event['url']}",
                file=sys.stderr,
                flush=True,
            )
        elif event.get("event") == "outcome_synced":
            print(
                f"[content] synced outcome: {event['url']} reactions={event['reaction_count']} comments={event['comment_count']}",
                file=sys.stderr,
                flush=True,
            )

    pretty_print(content.sync_post_outcomes(urls=urls, owned_by_me=args.owned, progress=emit_progress))


def cmd_content_retrieve(args: argparse.Namespace) -> None:
    from linkedin_cli import content

    content.init_content_db()
    _validate_positive_limit(args.limit)
    pretty_print(
        {
            "results": content.retrieve_posts(
                query_text=args.text,
                limit=args.limit,
                method=args.method,
                model=args.model,
                industry=args.industry,
                author=args.author,
                include_vectors=args.full,
            )
        }
    )


def cmd_content_similar(args: argparse.Namespace) -> None:
    from linkedin_cli import content

    content.init_content_db()
    _validate_positive_limit(args.limit)
    pretty_print({"results": content.similar_posts(url=args.url, limit=args.limit, method=args.method, include_vectors=args.full)})


def cmd_content_rebuild_index(args: argparse.Namespace) -> None:
    from linkedin_cli import content

    content.init_content_db()
    pretty_print(content.rebuild_retrieval_index(kind=args.kind, model=args.model))


def cmd_content_export_index(args: argparse.Namespace) -> None:
    from linkedin_cli import content

    content.init_content_db()
    pretty_print(content.export_retrieval_index(kind=args.kind, model=args.model, output_dir=args.output))


def cmd_content_harvest_jobs(args: argparse.Namespace) -> None:
    from linkedin_cli import content

    content.init_content_db()
    pretty_print({"jobs": content.list_harvest_jobs(limit=args.limit)})


def cmd_content_query_stats(args: argparse.Namespace) -> None:
    from linkedin_cli import content

    content.init_content_db()
    pretty_print({"queries": content.query_yield_stats(job_prefix=args.job_prefix, limit=args.limit)})


def cmd_content_materialize(args: argparse.Namespace) -> None:
    from linkedin_cli import content_warehouse

    pretty_print(content_warehouse.materialize_shards(job_id=args.job_id))


def cmd_content_warehouse_stats(args: argparse.Namespace) -> None:
    from linkedin_cli import content_warehouse

    pretty_print(content_warehouse.warehouse_stats(industry=args.industry))


def cmd_content_build_dataset(args: argparse.Namespace) -> None:
    from linkedin_cli import content_warehouse

    pretty_print(
        content_warehouse.build_training_dataset(
            output_dir=args.output,
            industries=list(args.industry or []),
            train_ratio=args.train_ratio,
            val_ratio=args.val_ratio,
        )
    )


def cmd_content_build_reward_dataset(args: argparse.Namespace) -> None:
    from linkedin_cli import content_warehouse

    pretty_print(
        content_warehouse.build_reward_dataset(
            output_dir=args.output,
            industries=list(args.industry or []),
            owned_only=args.owned_only,
            train_ratio=args.train_ratio,
            val_ratio=args.val_ratio,
        )
    )


def cmd_content_build_policy_dataset(args: argparse.Namespace) -> None:
    from linkedin_cli import content_warehouse

    pretty_print(
        content_warehouse.build_policy_dataset(
            output_dir=args.output,
            policy_name=args.policy_name,
            context_type=args.context_type,
            train_ratio=args.train_ratio,
            val_ratio=args.val_ratio,
        )
    )


def cmd_content_build_sft_dataset(args: argparse.Namespace) -> None:
    from linkedin_cli import qwen_training

    pretty_print(
        qwen_training.build_sft_dataset(
            output_dir=args.output,
            industry=args.industry,
            topics=args.topic,
            train_ratio=args.train_ratio,
            val_ratio=args.val_ratio,
        )
    )


def cmd_content_build_preference_dataset(args: argparse.Namespace) -> None:
    from linkedin_cli import qwen_training

    pretty_print(
        qwen_training.build_preference_dataset(
            output_dir=args.output,
            industry=args.industry,
            topics=args.topic,
            train_ratio=args.train_ratio,
            val_ratio=args.val_ratio,
        )
    )


def cmd_content_eval_dataset(args: argparse.Namespace) -> None:
    from linkedin_cli import evals

    pretty_print(evals.evaluate_dataset(dataset_dir=args.dataset_dir))


def cmd_content_eval_qwen(args: argparse.Namespace) -> None:
    from linkedin_cli import evals

    prompt = _load_inline_or_file_text(args.prompt, args.prompt_file, label="Prompt")
    pretty_print(
        evals.evaluate_qwen_generation(
            prompt=prompt,
            industry=args.industry,
            topics=list(args.topic or []),
            candidate_count=args.count,
            model=args.model,
            generator=args.generator,
        )
    )


def cmd_content_eval_policy(args: argparse.Namespace) -> None:
    from linkedin_cli import evals

    pretty_print(evals.evaluate_policy(policy_name=args.policy_name, context_type=args.context_type))


def cmd_content_eval_runtime(args: argparse.Namespace) -> None:
    from linkedin_cli import evals

    pretty_print(evals.evaluate_runtime(request_file=args.request_file, response_file=args.response_file))


def cmd_content_curate_corpus(args: argparse.Namespace) -> None:
    from linkedin_cli import corpus_curation

    pretty_print(
        corpus_curation.curate_corpus(
            industries=list(args.industry or []),
            min_quality=args.min_quality,
            near_duplicate_hamming=args.near_duplicate_hamming,
        )
    )


def cmd_content_curation_stats(args: argparse.Namespace) -> None:
    from linkedin_cli import corpus_curation

    pretty_print(corpus_curation.curation_stats())


def cmd_content_build_holdouts(args: argparse.Namespace) -> None:
    from linkedin_cli import corpus_curation

    pretty_print(
        corpus_curation.build_holdouts(
            output_dir=args.output,
            industries=list(args.industry or []),
            topics=list(args.topic or []),
            train_ratio=args.train_ratio,
            val_ratio=args.val_ratio,
            time_holdout_ratio=args.time_holdout_ratio,
            quota_per_industry=args.quota_per_industry,
            quota_per_topic=args.quota_per_topic,
            quota_per_format=args.quota_per_format,
            limit=args.limit,
        )
    )


def cmd_content_build_curated_sft(args: argparse.Namespace) -> None:
    from linkedin_cli import corpus_curation

    pretty_print(
        corpus_curation.build_curated_sft_dataset(
            output_dir=args.output,
            industries=list(args.industry or []),
            topics=list(args.topic or []),
            limit=args.limit,
            quota_per_industry=args.quota_per_industry,
            quota_per_topic=args.quota_per_topic,
            quota_per_format=args.quota_per_format,
            train_ratio=args.train_ratio,
            val_ratio=args.val_ratio,
        )
    )


def cmd_content_build_curated_preference(args: argparse.Namespace) -> None:
    from linkedin_cli import corpus_curation

    pretty_print(
        corpus_curation.build_curated_preference_dataset(
            output_dir=args.output,
            industries=list(args.industry or []),
            topics=list(args.topic or []),
            limit=args.limit,
            quota_per_industry=args.quota_per_industry,
            quota_per_topic=args.quota_per_topic,
            quota_per_format=args.quota_per_format,
            train_ratio=args.train_ratio,
            val_ratio=args.val_ratio,
        )
    )


def cmd_content_train_qwen(args: argparse.Namespace) -> None:
    from linkedin_cli import qwen_training

    planned = qwen_training.plan_training_run(
        phase=args.phase,
        dataset_dir=args.dataset_dir,
        base_model=args.base_model,
        output_name=args.output_name,
        runner=args.runner,
        wandb_project=args.wandb_project,
        wandb_entity=args.wandb_entity,
        modal_app_name=args.modal_app_name,
        learning_rate=args.learning_rate,
        epochs=args.epochs,
        lora_rank=args.lora_rank,
        per_device_batch_size=args.per_device_batch_size,
        gradient_accumulation_steps=args.gradient_accumulation_steps,
        dry_run=args.dry_run,
    )
    if args.dry_run:
        pretty_print(planned)
        return
    pretty_print(qwen_training.run_training_manifest(planned["manifest_path"], dry_run=False, runner=args.runner))


def cmd_content_qwen_runs(args: argparse.Namespace) -> None:
    from linkedin_cli import qwen_training

    pretty_print(qwen_training.list_qwen_runs(limit=args.limit))


def cmd_content_generate_benchmark_corpus(args: argparse.Namespace) -> None:
    from linkedin_cli import content_warehouse

    pretty_print(
        content_warehouse.generate_benchmark_corpus(
            job_id=args.job_id,
            row_count=args.rows,
            industries=list(args.industry or []),
            topics=list(args.topic or []),
        )
    )


def cmd_content_benchmark_warehouse(args: argparse.Namespace) -> None:
    from linkedin_cli import content_warehouse

    pretty_print(
        content_warehouse.benchmark_warehouse(
            job_id=args.job_id,
            dataset_output=args.dataset_output,
            industries=list(args.industry or []),
        )
    )


def cmd_content_benchmark_report(args: argparse.Namespace) -> None:
    from linkedin_cli import content_warehouse

    pretty_print({"reports": content_warehouse.benchmark_reports(limit=args.limit)})


def cmd_lead_autopilot_run(args: argparse.Namespace) -> None:
    from linkedin_cli import lead

    lead.init_lead_db()
    pretty_print(
        lead.run_autopilot(
            target_topics=args.topic,
            post_urls=args.post_url,
            all_owned=args.all_owned,
            limit=args.limit,
            state=args.state,
            min_fit=args.min_fit,
            min_reply=args.min_reply,
            min_deal=args.min_deal,
            sync_contacts=args.sync_contacts,
            dry_run=not args.execute,
        )
    )


def cmd_lead_rank(args: argparse.Namespace) -> None:
    from linkedin_cli import lead

    lead.init_lead_db()
    pretty_print({"results": lead.rank_leads(limit=args.limit)})


def cmd_lead_show(args: argparse.Namespace) -> None:
    from linkedin_cli import lead

    lead.init_lead_db()
    payload = lead.get_lead(args.profile)
    if not payload:
        fail(f"Lead not found for `{args.profile}`", code=ExitCode.VALIDATION)
    pretty_print(payload)


def cmd_comment_queue(args: argparse.Namespace) -> None:
    from linkedin_cli import comment

    comment.init_comment_db()
    session, _ = load_session(required=False)
    session = session or build_session()
    html = request(session, "GET", args.post_url).text
    queued = comment.queue_post_comments(args.post_url, html)
    queued["items"] = comment.list_comment_queue(post_url=args.post_url)
    pretty_print(queued)


def cmd_comment_draft(args: argparse.Namespace) -> None:
    from linkedin_cli import comment

    comment.init_comment_db()
    pretty_print(
        comment.draft_comment_reply(
            post_url=args.post_url,
            author_profile_key=args.profile,
            comment_id=args.comment_id,
            tone=args.tone,
        )
    )


def cmd_comment_execute(args: argparse.Namespace) -> None:
    from linkedin_cli import comment

    comment.init_comment_db()
    session, _ = load_session(required=True)
    assert session is not None
    text = _load_inline_or_file_text(args.text, args.text_file, label="Comment") if (args.text or args.text_file) else None
    pretty_print(
        comment.publish_post_comment(
            session=session,
            post_url=args.post_url,
            text=text,
            comment_id=args.comment_id,
            author_profile_key=args.profile,
            execute=args.execute,
            account_id=_get_account_id(session),
        )
    )


# ---------------------------------------------------------------------------
#  Write-system command handlers
# ---------------------------------------------------------------------------

def cmd_post_publish(args: argparse.Namespace) -> None:
    """Plan or execute a text or image post publish."""
    from linkedin_cli import content
    from linkedin_cli.write.store import init_db
    from linkedin_cli.write.plans import build_post_plan, build_image_post_plan
    from linkedin_cli.write.executor import execute_action

    session, _ = load_session(required=True)
    assert session is not None
    init_db()

    text = args.text
    if args.text_file:
        text_path = Path(args.text_file).expanduser()
        if not text_path.exists():
            fail(f"Text file not found: {text_path}")
        text = text_path.read_text(encoding="utf-8")
    if not text or not text.strip():
        fail("Post text is required. Use --text or --text-file.")

    account_id = _get_account_id(session)
    draft_analysis = None
    if args.score:
        content.init_content_db()
        draft_analysis = content.score_draft(text=text, model=args.score_model)

    if args.image:
        image_path = Path(args.image).expanduser().resolve()
        if not image_path.exists():
            fail(f"Image file not found: {image_path}")
        image_size = image_path.stat().st_size
        image_filename = image_path.name
        plan = build_image_post_plan(
            account_id=account_id,
            text=text,
            image_path=str(image_path),
            image_size=image_size,
            image_filename=image_filename,
            visibility=args.visibility,
        )
    else:
        plan = build_post_plan(account_id, text, visibility=args.visibility)

    action_id = f"act_{_uuid.uuid4().hex[:12]}"
    dry_run = not args.execute

    result = execute_action(
        session=session,
        action_id=action_id,
        plan=plan,
        account_id=account_id,
        dry_run=dry_run,
    )
    if draft_analysis is not None:
        result["draft_analysis"] = draft_analysis
    pretty_print(result)


def cmd_profile_snapshot(args: argparse.Namespace) -> None:
    """Snapshot the authenticated user profile."""
    session, _ = load_session(required=True)
    assert session is not None

    response = voyager_get(session, "/voyager/api/me")
    data = parse_json_response(response)

    if args.output:
        output_path = Path(args.output).expanduser()
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
        pretty_print({"ok": True, "message": "Profile snapshot saved", "output": str(output_path)})
    else:
        pretty_print(data)


def cmd_profile_edit(args: argparse.Namespace) -> None:
    """Plan or execute a profile field edit."""
    from linkedin_cli.write.store import init_db
    from linkedin_cli.write.plans import build_profile_edit_plan
    from linkedin_cli.write.executor import execute_action

    session, _ = load_session(required=True)
    assert session is not None
    init_db()

    field = args.field
    value = args.value
    if args.file:
        val_path = Path(args.file).expanduser()
        if not val_path.exists():
            fail(f"File not found: {val_path}")
        value = val_path.read_text(encoding="utf-8")
    if not value or not value.strip():
        fail(f"Value for {field} is required. Use --value or --file.")

    account_id = _get_account_id(session)
    member_hash = _get_my_member_hash(session)
    plan = build_profile_edit_plan(account_id, field, value, member_hash=member_hash)
    action_id = f"act_{_uuid.uuid4().hex[:12]}"
    dry_run = not args.execute

    result = execute_action(
        session=session,
        action_id=action_id,
        plan=plan,
        account_id=account_id,
        dry_run=dry_run,
    )
    pretty_print(result)


def cmd_experience_add(args: argparse.Namespace) -> None:
    """Plan or execute adding an experience/position entry."""
    from linkedin_cli.write.store import init_db
    from linkedin_cli.write.plans import build_experience_plan
    from linkedin_cli.write.executor import execute_action

    session, _ = load_session(required=True)
    assert session is not None
    init_db()

    account_id = _get_account_id(session)

    # Parse start date
    start_month = None
    start_year = None
    if args.start:
        parts = args.start.split("/")
        if len(parts) == 2:
            start_month = int(parts[0])
            start_year = int(parts[1])
        else:
            fail("Start date must be in MM/YYYY format")

    # Parse end date
    end_month = None
    end_year = None
    if args.end:
        parts = args.end.split("/")
        if len(parts) == 2:
            end_month = int(parts[0])
            end_year = int(parts[1])
        else:
            fail("End date must be in MM/YYYY format")

    plan = build_experience_plan(
        account_id=account_id,
        title=args.title,
        company=args.company,
        description=args.description,
        location=args.location,
        start_month=start_month,
        start_year=start_year,
        end_month=end_month,
        end_year=end_year,
    )
    action_id = f"act_{_uuid.uuid4().hex[:12]}"
    dry_run = not args.execute

    result = execute_action(
        session=session,
        action_id=action_id,
        plan=plan,
        account_id=account_id,
        dry_run=dry_run,
    )
    pretty_print(result)


def cmd_connect(args: argparse.Namespace) -> None:
    """Plan or execute a connection request."""
    from linkedin_cli import discovery
    from linkedin_cli.write.store import init_db
    from linkedin_cli.write.plans import build_connect_plan
    from linkedin_cli.write.executor import execute_action

    session, _ = load_session(required=True)
    assert session is not None
    init_db()

    account_id = _get_account_id(session)
    context = _resolve_mwlite_profile_context(session, args.profile)

    plan = build_connect_plan(
        account_id=account_id,
        vanity_name=context["vanity_name"],
        page_key=context["page_key"],
        member_urn=context["member_urn"],
        message=args.message,
    )
    action_id = f"act_{_uuid.uuid4().hex[:12]}"
    dry_run = not args.execute

    result = execute_action(
        session=session,
        action_id=action_id,
        plan=plan,
        account_id=account_id,
        dry_run=dry_run,
    )
    if result.get("status") == "succeeded":
        slug = normalize_profile_slug(args.profile)
        discovery.init_discovery_db()
        discovery.upsert_prospect(slug, slug, public_identifier=slug, profile_url=f"https://www.linkedin.com/in/{slug}/")
        discovery.record_action_feedback(
            action_type="connect",
            profile_key=slug,
            succeeded=True,
            metadata={"action_id": action_id},
        )
    pretty_print(result)


def cmd_follow(args: argparse.Namespace) -> None:
    """Plan or execute a follow action."""
    from linkedin_cli import discovery
    from linkedin_cli.write.store import init_db
    from linkedin_cli.write.plans import build_follow_plan
    from linkedin_cli.write.executor import execute_action

    session, _ = load_session(required=True)
    assert session is not None
    init_db()

    account_id = _get_account_id(session)
    context = _resolve_mwlite_profile_context(session, args.profile)

    plan = build_follow_plan(
        account_id=account_id,
        target_member_urn=context["member_urn"],
        page_key=context["page_key"],
        vanity_name=context["vanity_name"],
    )
    action_id = f"act_{_uuid.uuid4().hex[:12]}"
    dry_run = not args.execute

    result = execute_action(
        session=session,
        action_id=action_id,
        plan=plan,
        account_id=account_id,
        dry_run=dry_run,
    )
    if result.get("status") == "succeeded":
        slug = normalize_profile_slug(args.profile)
        discovery.init_discovery_db()
        discovery.upsert_prospect(slug, slug, public_identifier=slug, profile_url=f"https://www.linkedin.com/in/{slug}/")
        discovery.record_action_feedback(
            action_type="follow",
            profile_key=slug,
            succeeded=True,
            metadata={"action_id": action_id},
        )
    pretty_print(result)


def cmd_dm_list(args: argparse.Namespace) -> None:
    """List recent DM conversations."""
    session, _ = load_session(required=True)
    assert session is not None
    conversations = _fetch_dm_conversations(session, args.limit)
    pretty_print(
        {
            "conversations": [
                {
                    "urn": conversation["conversation_urn"],
                    "participants": [item["display_name"] for item in conversation["participants"]] or ["Unknown"],
                    "last_activity": conversation["last_activity"],
                }
                for conversation in conversations
            ]
        }
    )


def cmd_dm_send(args: argparse.Namespace) -> None:
    """Plan or execute sending a DM."""
    from linkedin_cli import discovery
    from linkedin_cli.write.store import init_db
    from linkedin_cli.write.plans import build_dm_plan
    from linkedin_cli.write.executor import execute_action

    session, _ = load_session(required=True)
    assert session is not None
    init_db()

    account_id = _get_account_id(session)

    message = args.message
    if args.message_file:
        msg_path = Path(args.message_file).expanduser()
        if not msg_path.exists():
            fail(f"Message file not found: {msg_path}")
        message = msg_path.read_text(encoding="utf-8")
    if not message or not message.strip():
        fail("Message text is required. Use --message or --message-file.")

    conversation_urn = args.conversation
    recipient_urn = None
    if args.to:
        context = _resolve_mwlite_profile_context(session, args.to)
        if not conversation_urn and context.get("message_locked"):
            fail("LinkedIn has direct messaging locked for this profile from your account. Send a connection request first or use InMail/Premium.")
        recipient_urn = _resolve_profile_urn(session, args.to)

    if not conversation_urn and not recipient_urn:
        fail("Either --conversation URN or --to profile is required.")

    # Fetch the mailbox URN dynamically
    mailbox_urn = _get_my_urn(session)

    plan = build_dm_plan(
        account_id=account_id,
        conversation_urn=conversation_urn,
        recipient_urn=recipient_urn,
        message_text=message,
        mailbox_urn=mailbox_urn,
    )
    action_id = f"act_{_uuid.uuid4().hex[:12]}"
    dry_run = not args.execute

    result = execute_action(
        session=session,
        action_id=action_id,
        plan=plan,
        account_id=account_id,
        dry_run=dry_run,
    )
    if result.get("status") == "succeeded" and args.to:
        slug = normalize_profile_slug(args.to)
        discovery.init_discovery_db()
        discovery.upsert_prospect(slug, slug, public_identifier=slug, profile_url=f"https://www.linkedin.com/in/{slug}/")
        discovery.record_action_feedback(
            action_type="dm.send",
            profile_key=slug,
            succeeded=True,
            metadata={"action_id": action_id},
        )
    pretty_print(result)


def cmd_schedule(args: argparse.Namespace) -> None:
    """Schedule a LinkedIn post for future publishing."""
    from linkedin_cli.write.store import init_db, create_action
    from linkedin_cli.write.plans import build_scheduled_post_plan

    session, _ = load_session(required=True)
    assert session is not None
    init_db()

    account_id = _get_account_id(session)

    text = args.text
    if args.text_file:
        text_path = Path(args.text_file).expanduser()
        if not text_path.exists():
            fail(f"Text file not found: {text_path}")
        text = text_path.read_text(encoding="utf-8")
    if not text or not text.strip():
        fail("Post text is required. Use --text or --text-file.")
    if args.image:
        fail("Scheduled image posts are not supported yet. Use `post publish --image --execute` for immediate image publishing.")

    plan = build_scheduled_post_plan(
        account_id=account_id,
        text=text,
        scheduled_at=args.at,
        visibility=args.visibility,
        image_path=args.image,
    )
    action_id = f"act_{_uuid.uuid4().hex[:12]}"
    create_action(
        action_id=action_id,
        action_type="post.scheduled",
        target_key="me",
        idempotency_key=plan["idempotency_key"],
        plan=plan,
        account_id=account_id,
        dry_run=False,
        scheduled_at=args.at,
    )
    pretty_print({
        "status": "scheduled",
        "action_id": action_id,
        "scheduled_at": args.at,
        "message": f"Post scheduled for {args.at}. The scheduler will publish it when the time comes.",
    })


def cmd_action_list(args: argparse.Namespace) -> None:
    """List recent actions from the store."""
    from linkedin_cli.write.store import init_db, list_actions

    init_db()
    actions = list_actions(state=args.state, limit=args.limit)
    for a in actions:
        a.pop("plan_json", None)
    pretty_print({"actions": actions, "count": len(actions)})


def cmd_action_show(args: argparse.Namespace) -> None:
    """Show details for a specific action."""
    from linkedin_cli.write.store import init_db, get_action

    init_db()
    action = get_action(args.action_id)
    if not action:
        fail(f"Action not found: {args.action_id}", code=ExitCode.NOT_FOUND)
    pretty_print(action)


def cmd_action_retry(args: argparse.Namespace) -> None:
    """Retry a failed action."""
    from linkedin_cli.write.store import init_db, get_action, update_state
    from linkedin_cli.write.executor import execute_action

    session, _ = load_session(required=True)
    assert session is not None
    init_db()

    action = get_action(args.action_id)
    if not action:
        fail(f"Action not found: {args.action_id}", code=ExitCode.NOT_FOUND)
    if action["state"] not in ("failed", "unknown_remote_state"):
        state = action["state"]
        fail(
            f"Action is in state '{state}' -- only failed or unknown_remote_state actions can be retried",
            code=ExitCode.CONFLICT,
        )

    plan = action.get("plan")
    if not plan:
        fail("Action has no stored plan -- cannot retry", code=ExitCode.CONFLICT)

    update_state(args.action_id, "planned")

    result = execute_action(
        session=session,
        action_id=args.action_id,
        plan=plan,
        account_id=action["account_id"],
        dry_run=False,
    )
    pretty_print(result)


def cmd_action_reconcile(args: argparse.Namespace) -> None:
    from linkedin_cli.write.reconcile import reconcile_action
    from linkedin_cli.write.store import get_action, init_db

    session, _ = load_session(required=True)
    assert session is not None
    init_db()

    action = get_action(args.action_id)
    if not action:
        fail(f"Action not found: {args.action_id}", code=ExitCode.NOT_FOUND)

    pretty_print(reconcile_action(session, args.action_id))


def cmd_action_cancel(args: argparse.Namespace) -> None:
    from linkedin_cli.write.store import cancel_action, init_db

    init_db()
    try:
        action = cancel_action(args.action_id, reason=args.reason or "user requested cancel")
    except ValueError:
        fail(f"Action not found: {args.action_id}", code=ExitCode.NOT_FOUND)
    pretty_print({"status": "canceled", "action": action})


def cmd_action_artifacts(args: argparse.Namespace) -> None:
    from linkedin_cli.write.store import get_action, init_db, list_artifacts

    init_db()
    action = get_action(args.action_id)
    if not action:
        fail(f"Action not found: {args.action_id}", code=ExitCode.NOT_FOUND)
    pretty_print({"action_id": args.action_id, "artifacts": list_artifacts(args.action_id)})


def cmd_action_health(args: argparse.Namespace) -> None:
    from linkedin_cli.write.guards import action_health_report
    from linkedin_cli.write.store import init_db

    init_db()
    pretty_print(action_health_report(stale_minutes=args.stale_minutes))


def cmd_workflow_search_save(args: argparse.Namespace) -> None:
    from linkedin_cli import workflow

    workflow.init_workflow_db()
    pretty_print(
        workflow.save_search(
            name=args.name,
            kind=args.kind,
            query=args.query,
            limit=args.limit,
            enrich=args.enrich,
        )
    )


def cmd_workflow_search_list(args: argparse.Namespace) -> None:
    from linkedin_cli import workflow

    workflow.init_workflow_db()
    pretty_print({"saved_searches": workflow.list_saved_searches()})


def cmd_workflow_search_run(args: argparse.Namespace) -> None:
    from linkedin_cli import discovery, workflow

    workflow.init_workflow_db()
    saved = workflow.get_saved_search(args.name)
    if not saved:
        fail(f"Saved search not found: {args.name}", code=ExitCode.NOT_FOUND)
    payload = _run_search(saved["kind"], saved["query"], saved["result_limit"], bool(saved["enrich"]))
    if args.ingest_discovery:
        discovery.init_discovery_db()
        discovery.ingest_search_results(
            kind=payload["kind"],
            query=payload["query"],
            results=payload["results"],
            source_label=f"saved:{args.name}",
        )
    synced_contacts: list[dict[str, Any]] = []
    if args.save_contacts:
        synced_contacts = workflow.sync_contacts_from_search_results(payload["results"])
    if args.ingest_discovery or args.save_contacts:
        payload["automation"] = {
            "ingested_discovery": bool(args.ingest_discovery),
            "saved_contacts": len(synced_contacts),
        }
    pretty_print(payload)


def cmd_workflow_search_delete(args: argparse.Namespace) -> None:
    from linkedin_cli import workflow

    workflow.init_workflow_db()
    if not workflow.delete_saved_search(args.name):
        fail(f"Saved search not found: {args.name}", code=ExitCode.NOT_FOUND)
    pretty_print({"status": "deleted", "name": args.name})


def cmd_workflow_template_save(args: argparse.Namespace) -> None:
    from linkedin_cli import workflow

    workflow.init_workflow_db()
    pretty_print(workflow.save_template(args.name, args.kind, args.body))


def cmd_workflow_template_list(args: argparse.Namespace) -> None:
    from linkedin_cli import workflow

    workflow.init_workflow_db()
    pretty_print({"templates": workflow.list_templates(kind=args.kind)})


def cmd_workflow_template_show(args: argparse.Namespace) -> None:
    from linkedin_cli import workflow

    workflow.init_workflow_db()
    template = workflow.get_template(args.name)
    if not template:
        fail(f"Template not found: {args.name}", code=ExitCode.NOT_FOUND)
    pretty_print(template)


def cmd_workflow_template_render(args: argparse.Namespace) -> None:
    from linkedin_cli import workflow

    workflow.init_workflow_db()
    variables = parse_key_values(args.var or [])
    pretty_print({"name": args.name, "body": workflow.render_template(args.name, variables)})


def cmd_workflow_template_delete(args: argparse.Namespace) -> None:
    from linkedin_cli import workflow

    workflow.init_workflow_db()
    if not workflow.delete_template(args.name):
        fail(f"Template not found: {args.name}", code=ExitCode.NOT_FOUND)
    pretty_print({"status": "deleted", "name": args.name})


def cmd_workflow_contact_upsert(args: argparse.Namespace) -> None:
    from linkedin_cli import workflow

    workflow.init_workflow_db()
    tags = [tag.strip() for tag in (args.tags or "").split(",") if tag.strip()]
    pretty_print(
        workflow.upsert_contact(
            profile_key=args.profile,
            display_name=args.name,
            stage=args.stage,
            tags=tags,
            notes=args.notes or "",
        )
    )


def cmd_workflow_contact_list(args: argparse.Namespace) -> None:
    from linkedin_cli import workflow

    workflow.init_workflow_db()
    pretty_print({"contacts": workflow.list_contacts(stage=args.stage, tag=args.tag)})


def cmd_workflow_contact_show(args: argparse.Namespace) -> None:
    from linkedin_cli import workflow

    workflow.init_workflow_db()
    contact = workflow.get_contact(args.profile)
    if not contact:
        fail(f"Contact not found: {args.profile}", code=ExitCode.NOT_FOUND)
    pretty_print(contact)


def cmd_workflow_contact_delete(args: argparse.Namespace) -> None:
    from linkedin_cli import workflow

    workflow.init_workflow_db()
    if not workflow.delete_contact(args.profile):
        fail(f"Contact not found: {args.profile}", code=ExitCode.NOT_FOUND)
    pretty_print({"status": "deleted", "profile": args.profile})


def cmd_workflow_contact_export(args: argparse.Namespace) -> None:
    from linkedin_cli import workflow

    workflow.init_workflow_db()
    path = workflow.export_contacts_csv(Path(args.output).expanduser())
    pretty_print({"status": "exported", "path": str(path)})


def cmd_workflow_contact_import(args: argparse.Namespace) -> None:
    from linkedin_cli import workflow

    workflow.init_workflow_db()
    count = workflow.import_contacts_csv(Path(args.input).expanduser())
    pretty_print({"status": "imported", "count": count, "path": str(Path(args.input).expanduser())})


def cmd_workflow_contact_sync_discovery(args: argparse.Namespace) -> None:
    from linkedin_cli import discovery, workflow

    workflow.init_workflow_db()
    discovery.init_discovery_db()
    queue = discovery.list_queue(limit=args.limit, state=args.state)
    if args.min_score is not None:
        queue = [item for item in queue if float(item.get("score") or 0.0) >= args.min_score]
    synced = workflow.sync_contacts_from_queue(queue)
    pretty_print({"status": "synced", "queue_count": len(queue), "contact_count": len(synced), "contacts": synced})


def cmd_workflow_inbox_upsert(args: argparse.Namespace) -> None:
    from linkedin_cli import workflow

    workflow.init_workflow_db()
    pretty_print(
        workflow.upsert_inbox_item(
            conversation_urn=args.conversation,
            state=args.state,
            priority=args.priority,
            notes=args.notes or "",
        )
    )


def cmd_workflow_inbox_list(args: argparse.Namespace) -> None:
    from linkedin_cli import workflow

    workflow.init_workflow_db()
    pretty_print({"inbox": workflow.list_inbox_items(state=args.state)})


def cmd_workflow_inbox_show(args: argparse.Namespace) -> None:
    from linkedin_cli import workflow

    workflow.init_workflow_db()
    item = workflow.get_inbox_item(args.conversation)
    if not item:
        fail(f"Inbox item not found: {args.conversation}", code=ExitCode.NOT_FOUND)
    pretty_print(item)


def cmd_workflow_inbox_delete(args: argparse.Namespace) -> None:
    from linkedin_cli import workflow

    workflow.init_workflow_db()
    if not workflow.delete_inbox_item(args.conversation):
        fail(f"Inbox item not found: {args.conversation}", code=ExitCode.NOT_FOUND)
    pretty_print({"status": "deleted", "conversation": args.conversation})


def cmd_discover_ingest_search(args: argparse.Namespace) -> None:
    from linkedin_cli import discovery, workflow

    store_payload: dict[str, Any]
    discovery.init_discovery_db()

    if args.saved:
        workflow.init_workflow_db()
        saved = workflow.get_saved_search(args.saved)
        if not saved:
            fail(f"Saved search not found: {args.saved}", code=ExitCode.NOT_FOUND)
        store_payload = _run_search(saved["kind"], saved["query"], saved["result_limit"], bool(saved["enrich"]))
        source_label = f"saved:{args.saved}"
    else:
        if not args.query:
            fail("Either --query or --saved is required", code=ExitCode.VALIDATION)
        store_payload = _run_search(args.kind, args.query, args.limit, args.enrich)
        source_label = args.query

    created = discovery.ingest_search_results(
        kind=store_payload["kind"],
        query=store_payload["query"],
        results=store_payload["results"],
        source_label=source_label,
    )
    pretty_print(
        {
            "status": "ingested",
            "created": created,
            "kind": store_payload["kind"],
            "query": store_payload["query"],
            "queue_size": discovery.queue_stats()["prospect_count"],
        }
    )


def cmd_discover_ingest_inbox(args: argparse.Namespace) -> None:
    from linkedin_cli import discovery

    session, _ = load_session(required=True)
    assert session is not None
    discovery.init_discovery_db()
    conversations = _fetch_dm_conversations(session, args.limit)
    created = discovery.ingest_inbox_conversations(conversations, self_member_urn=_get_my_urn(session))
    pretty_print(
        {
            "status": "ingested",
            "created": created,
            "conversation_count": len(conversations),
            "queue_size": discovery.queue_stats()["prospect_count"],
        }
    )


def cmd_discover_ingest_engagement(args: argparse.Namespace) -> None:
    from linkedin_cli import discovery

    discovery.init_discovery_db()
    activity = _collect_activity_results(args.target, args.limit)
    session, _ = load_session(required=False)
    session = session or build_session()

    ingested: list[dict[str, Any]] = []
    commenter_total = 0
    liker_total = 0
    reposter_total = 0
    for result in activity["results"]:
        response = request(session, "GET", result["url"])
        summary = discovery.ingest_public_post_engagement(
            target_key=args.target,
            post_url=result["url"],
            html=response.text,
        )
        ingested.append(summary)
        commenter_total += int(summary.get("commenter_count") or 0)
        liker_total += int(summary.get("liker_count") or 0)
        reposter_total += int(summary.get("reposter_count") or 0)

    pretty_print(
        {
            "status": "ingested",
            "target": args.target,
            "posts": ingested,
            "post_count": len(ingested),
            "commenter_count": commenter_total,
            "liker_count": liker_total,
            "reposter_count": reposter_total,
            "queue_size": discovery.queue_stats()["prospect_count"],
        }
    )


def cmd_discover_ingest_profile_views(args: argparse.Namespace) -> None:
    from linkedin_cli import discovery

    session, _ = load_session(required=True)
    assert session is not None
    discovery.init_discovery_db()
    payload = discovery.fetch_profile_view_analytics(session)
    parsed = discovery.parse_profile_view_analytics_payload(payload)
    used_html_fallback = False
    if args.html_fallback and int(parsed.get("available_viewer_count") or 0) == 0:
        response = request(session, "GET", "https://www.linkedin.com/me/profile-views")
        for bootstrap in parse_bootstrap_payloads(response.text):
            request_path = str(bootstrap.get("request") or "")
            if "voyagerPremiumDashAnalyticsView" not in request_path:
                continue
            body = bootstrap.get("body")
            if isinstance(body, dict):
                payload = body
                parsed = discovery.parse_profile_view_analytics_payload(payload)
                used_html_fallback = True
                break
    summary = discovery.ingest_profile_view_analytics("me", payload)
    summary["used_html_fallback"] = used_html_fallback
    summary["queue_size"] = discovery.queue_stats()["prospect_count"]
    pretty_print(summary)


def cmd_discover_signal_add(args: argparse.Namespace) -> None:
    from linkedin_cli import discovery

    discovery.init_discovery_db()
    if not discovery.get_prospect(args.profile):
        discovery.upsert_prospect(args.profile, args.profile, public_identifier=args.profile)
    prospect = discovery.add_signal(
        args.profile,
        signal_type=args.type,
        source=args.source,
        notes=args.notes,
    )
    pretty_print(prospect)


def cmd_discover_state_set(args: argparse.Namespace) -> None:
    from linkedin_cli import discovery

    discovery.init_discovery_db()
    prospect = discovery.get_prospect(args.profile)
    if not prospect:
        fail(f"Prospect not found: {args.profile}", code=ExitCode.NOT_FOUND)
    pretty_print(discovery.set_prospect_state(args.profile, args.state))


def cmd_discover_queue(args: argparse.Namespace) -> None:
    from linkedin_cli import discovery

    discovery.init_discovery_db()
    queue = discovery.list_queue(limit=args.limit, state=args.state)
    if not args.why:
        queue = [
            {
                "profile_key": item["profile_key"],
                "display_name": item["display_name"],
                "state": item["state"],
                "score": item["score"],
                "headline": item.get("headline"),
                "source_count": item.get("source_count"),
                "signal_count": item.get("signal_count"),
            }
            for item in queue
        ]
    pretty_print({"queue": queue, "count": len(queue)})


def cmd_discover_show(args: argparse.Namespace) -> None:
    from linkedin_cli import discovery

    discovery.init_discovery_db()
    prospect = discovery.get_prospect(args.profile)
    if not prospect:
        fail(f"Prospect not found: {args.profile}", code=ExitCode.NOT_FOUND)
    pretty_print(prospect)


def cmd_discover_stats(args: argparse.Namespace) -> None:
    from linkedin_cli import discovery

    discovery.init_discovery_db()
    pretty_print(discovery.queue_stats())


# ---------------------------------------------------------------------------
#  Parser
# ---------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="linkedin",
        description="Unofficial LinkedIn CLI -- session-based authentication, Voyager API access, and safe write system",
        epilog=textwrap.dedent(
            """
            examples:
              linkedin doctor
              linkedin --table action list
              linkedin search people "founder fintech"
              linkedin workflow search save --name founders --kind people --query "fintech founder"
              linkedin discover ingest-search --kind people --query "founder fintech"
            """
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.set_defaults(output_mode="json")
    output_group = parser.add_mutually_exclusive_group()
    output_group.add_argument("--json", dest="output_mode", action="store_const", const="json", help="Render structured JSON output")
    output_group.add_argument("--table", dest="output_mode", action="store_const", const="table", help="Render tabular output when possible")
    output_group.add_argument("--quiet", dest="output_mode", action="store_const", const="quiet", help="Render concise values only")
    parser.add_argument("--brief", action="store_true", help="Compact JSON for agent consumption (fewer tokens)")
    sub = parser.add_subparsers(dest="command", required=True)

    p_login = sub.add_parser("login", help="Log into LinkedIn using web form auth")
    p_login.add_argument("--username", help="LinkedIn username/email (defaults to LINKEDIN_USERNAME)")
    p_login.add_argument("--password", help="LinkedIn password (defaults to LINKEDIN_PASSWORD)")
    p_login.add_argument("--browser", action="store_true", help="Capture session cookies from a real browser")
    p_login.add_argument(
        "--browser-name",
        default="chrome",
        choices=["chrome", "firefox", "brave", "chromium"],
        help="Browser to use when capturing cookies (default: chrome)",
    )
    p_login.add_argument("--timeout", type=int, default=180, help="Seconds to wait for browser login completion")
    p_login.add_argument("--user-agent", help="Override browser user agent")
    p_login.set_defaults(func=cmd_login)

    p_logout = sub.add_parser("logout", help="Delete saved LinkedIn session")
    p_logout.set_defaults(func=cmd_logout)

    p_status = sub.add_parser("status", help="Inspect current LinkedIn session")
    p_status.set_defaults(func=cmd_status)

    p_doctor = sub.add_parser("doctor", help="Check config, session, and local state health")
    p_doctor.set_defaults(func=cmd_doctor)

    p_completion = sub.add_parser("completion", help="Print a basic shell completion script")
    p_completion.add_argument("shell", choices=["bash", "zsh"], help="Shell to generate completion for")
    p_completion.set_defaults(func=cmd_completion)

    p_html = sub.add_parser("html", help="Fetch a LinkedIn URL with the saved session")
    p_html.add_argument("url", help="Absolute LinkedIn URL")
    p_html.add_argument("--public", action="store_true", help="Do not require a saved session")
    p_html.add_argument("--output", help="Write HTML to a file instead of stdout")
    p_html.set_defaults(func=cmd_html)

    p_voyager = sub.add_parser("voyager", help="Call a LinkedIn Voyager endpoint")
    p_voyager.add_argument("path", help="Voyager path, e.g. /voyager/api/me or /identity/profiles/foo/profileView")
    p_voyager.add_argument("--param", action="append", default=[], help="Query param as KEY=VALUE (repeatable)")
    p_voyager.set_defaults(func=cmd_voyager)

    p_profile = sub.add_parser("profile", help="Fetch and summarize a LinkedIn profile")
    p_profile.add_argument("target", help="Profile URL or public identifier")
    p_profile.set_defaults(func=cmd_profile)

    p_company = sub.add_parser("company", help="Fetch and summarize a LinkedIn company")
    p_company.add_argument("target", help="Company URL or company slug")
    p_company.set_defaults(func=cmd_company)

    p_search = sub.add_parser("search", help="Search LinkedIn entities via web-indexed LinkedIn pages")
    p_search.add_argument("kind", choices=["people", "companies", "posts"], help="What to search for")
    p_search.add_argument("query", help="Search query")
    p_search.add_argument("--limit", type=int, default=5, help="Maximum number of results")
    p_search.add_argument("--enrich", action="store_true", help="Enrich people/company results with LinkedIn-authenticated profile or company fetches")
    p_search.set_defaults(func=cmd_search)

    p_activity = sub.add_parser("activity", help="Find likely public LinkedIn posts/activity for a person")
    p_activity.add_argument("target", help="Profile URL, public identifier, or name")
    p_activity.add_argument("--limit", type=int, default=5, help="Maximum number of posts/results")
    p_activity.set_defaults(func=cmd_activity)

    p_telemetry = sub.add_parser("telemetry", help="Sync and inspect local growth telemetry")
    telemetry_sub = p_telemetry.add_subparsers(dest="telemetry_command", required=True)
    p_telemetry_sync = telemetry_sub.add_parser("sync", help="Sync owned-post telemetry into local storage")
    p_telemetry_sync.add_argument("--url", action="append", default=[], help="Owned post URL to sync (repeatable)")
    p_telemetry_sync.add_argument("--owned-posts", action="store_true", help="Sync all posts already marked as owned")
    p_telemetry_sync.set_defaults(func=cmd_telemetry_sync)
    p_telemetry_stats = telemetry_sub.add_parser("stats", help="Show telemetry and content corpus summary stats")
    p_telemetry_stats.set_defaults(func=cmd_telemetry_stats)

    p_content = sub.add_parser("content", help="Harvest and inspect public LinkedIn post content")
    content_sub = p_content.add_subparsers(dest="content_command", required=True)

    p_content_harvest = content_sub.add_parser("harvest", help="Harvest public LinkedIn posts into local storage")
    p_content_harvest.add_argument("--industry", action="append", default=[], help="Industry or market label to attach to harvested posts (repeatable)")
    p_content_harvest.add_argument("--topic", action="append", default=[], help="Topic term to expand into LinkedIn post queries")
    p_content_harvest.add_argument("--query", action="append", default=[], help="Raw search query to run (repeatable)")
    p_content_harvest.add_argument("--limit", type=int, default=100, help="Maximum posts to store")
    p_content_harvest.add_argument("--per-query", type=int, default=25, help="Maximum search results to inspect per query")
    p_content_harvest.add_argument("--backend", choices=["auth-only", "hybrid", "public-only"], default="auth-only", help="Search backend mode (default: auth-only)")
    p_content_harvest.add_argument("--public-search", choices=["ddg", "searxng"], default="ddg", help="Public discovery provider used by hybrid/public-only backends")
    p_content_harvest.add_argument("--searxng-url", help="Optional local SearXNG base URL, e.g. http://127.0.0.1:8080")
    p_content_harvest.add_argument("--searxng-engine", action="append", default=[], help="Optional SearXNG engine name filter (repeatable)")
    p_content_harvest.add_argument("--expansion", choices=["standard", "broad", "exhaustive", "recursive"], default="standard", help="How aggressively to expand industry/topic queries")
    p_content_harvest.add_argument("--freshness-bucket", action="append", default=[], choices=["recent", "month", "quarter", "year"], help="Add time-window keyword expansions")
    p_content_harvest.add_argument("--search-timeout", type=int, default=30, help="Timeout in seconds for each search request")
    p_content_harvest.add_argument("--fetch-workers", type=int, default=6, help="Concurrent workers for post page fetches")
    p_content_harvest.add_argument("--query-workers", type=int, default=4, help="Concurrent workers for query result resolution")
    p_content_harvest.add_argument("--job-name", help="Persist harvest progress under a reusable job id")
    p_content_harvest.add_argument("--resume-job", help="Resume a previously interrupted harvest job id")
    p_content_harvest.add_argument("--retry-budget", type=int, default=2, help="Maximum retry attempts for retryable search failures")
    p_content_harvest.add_argument("--cooldown-seconds", type=float, default=1.5, help="Base cooldown between retry attempts")
    p_content_harvest.add_argument("--min-request-interval", type=float, default=0.25, help="Minimum pause between post fetch/store steps")
    p_content_harvest.add_argument("--jitter-seconds", type=float, default=0.35, help="Extra randomized jitter added to cooldown and pacing")
    p_content_harvest.add_argument("--embed", action="store_true", help="Generate embeddings for the newly stored posts after harvest")
    p_content_harvest.add_argument(
        "--embed-model",
        default="fastembed:BAAI/bge-small-en-v1.5",
        help="Embedding model to use with --embed (default: fastembed:BAAI/bge-small-en-v1.5; use local-hash-v1 or text-embedding-3-small as needed)",
    )
    p_content_harvest.add_argument("--embed-batch-size", type=int, default=25, help="Batch size for embedding requests")
    p_content_harvest.set_defaults(func=cmd_content_harvest)

    p_content_campaign = content_sub.add_parser("harvest-campaign", help="Run a large local acquisition campaign as many resumable harvest jobs")
    p_content_campaign.add_argument("--industry", action="append", default=[], help="Industry or market label to attach to harvested posts (repeatable)")
    p_content_campaign.add_argument("--topic", action="append", default=[], help="Topic term to expand into LinkedIn post queries")
    p_content_campaign.add_argument("--query", action="append", default=[], help="Raw search query to run (repeatable)")
    p_content_campaign.add_argument("--limit", type=int, default=1000, help="Maximum posts to store across the full campaign")
    p_content_campaign.add_argument("--per-query", type=int, default=25, help="Maximum search results to inspect per query")
    p_content_campaign.add_argument("--backend", choices=["auth-only", "hybrid", "public-only"], default="auth-only", help="Search backend mode (default: auth-only)")
    p_content_campaign.add_argument("--public-search", choices=["ddg", "searxng"], default="ddg", help="Public discovery provider used by hybrid/public-only backends")
    p_content_campaign.add_argument("--searxng-url", help="Optional local SearXNG base URL, e.g. http://127.0.0.1:8080")
    p_content_campaign.add_argument("--searxng-engine", action="append", default=[], help="Optional SearXNG engine name filter (repeatable)")
    p_content_campaign.add_argument("--expansion", choices=["standard", "broad", "exhaustive", "recursive"], default="standard", help="How aggressively to expand industry/topic queries")
    p_content_campaign.add_argument("--freshness-bucket", action="append", default=[], choices=["recent", "month", "quarter", "year"], help="Add time-window keyword expansions")
    p_content_campaign.add_argument("--per-job-limit", type=int, default=1000, help="Maximum posts to store per harvest job")
    p_content_campaign.add_argument("--queries-per-job", type=int, default=24, help="How many queries to pack into each harvest job")
    p_content_campaign.add_argument("--speed", choices=["balanced", "max"], default="balanced", help="balanced keeps inline post-processing; max prioritizes throughput and defers slow post-processing")
    p_content_campaign.add_argument("--search-timeout", type=int, default=30, help="Timeout in seconds for each search request")
    p_content_campaign.add_argument("--fetch-workers", type=int, default=6, help="Concurrent workers for post page fetches")
    p_content_campaign.add_argument("--query-workers", type=int, default=4, help="Concurrent workers for query result resolution")
    p_content_campaign.add_argument("--retry-budget", type=int, default=2, help="Maximum retry attempts for retryable search failures")
    p_content_campaign.add_argument("--cooldown-seconds", type=float, default=1.5, help="Base cooldown between retry attempts")
    p_content_campaign.add_argument("--min-request-interval", type=float, default=0.25, help="Minimum pause between post fetch/store steps")
    p_content_campaign.add_argument("--jitter-seconds", type=float, default=0.35, help="Extra randomized jitter added to cooldown and pacing")
    p_content_campaign.add_argument("--job-prefix", default="campaign", help="Prefix used to name per-batch harvest jobs")
    p_content_campaign.add_argument("--resume", action="store_true", help="Resume or reuse jobs under the same job prefix")
    p_content_campaign.add_argument("--prune-min-yield", type=float, help="Prune historically low-yield queries below this stored/result ratio")
    p_content_campaign.add_argument("--prune-min-attempts", type=int, default=2, help="Minimum historical attempts before pruning a query")
    p_content_campaign.add_argument("--stop-min-yield-rate", type=float, help="Stop early when recent jobs fall below this unique-yield ratio")
    p_content_campaign.add_argument("--stop-window", type=int, default=3, help="How many recent jobs to consider for stop rules")
    p_content_campaign.add_argument("--materialize", action="store_true", help="Materialize each completed job into the local DuckDB warehouse")
    p_content_campaign.add_argument("--embed", action="store_true", help="Generate embeddings for newly stored posts after each job")
    p_content_campaign.add_argument(
        "--embed-model",
        default="fastembed:BAAI/bge-small-en-v1.5",
        help="Embedding model to use with --embed (default: fastembed:BAAI/bge-small-en-v1.5)",
    )
    p_content_campaign.add_argument("--embed-batch-size", type=int, default=25, help="Batch size for embedding requests")
    p_content_campaign.add_argument("--retrain-every", type=int, default=0, help="Retrain the content model every N completed jobs (0 disables)")
    p_content_campaign.add_argument("--train-model-name", default="default", help="Stored model name for campaign retrains")
    p_content_campaign.add_argument("--train-scope", choices=["auto", "owned", "all"], default="all", help="Training scope when retraining during the campaign")
    p_content_campaign.add_argument("--train-min-samples", type=int, default=100, help="Minimum posts required before a campaign retrain runs")
    p_content_campaign.set_defaults(func=cmd_content_harvest_campaign)

    p_content_list = content_sub.add_parser("list", help="List harvested posts from local storage")
    p_content_list.add_argument("--limit", type=int, default=20, help="Maximum posts to show")
    p_content_list.add_argument("--industry", help="Filter by stored industry label")
    p_content_list.add_argument("--author", help="Filter by author name substring")
    p_content_list.add_argument("--full", action="store_true", help="Include raw embedding and fingerprint arrays")
    p_content_list.set_defaults(func=cmd_content_list)

    p_content_stats = content_sub.add_parser("stats", help="Show harvest summary metrics from local storage")
    p_content_stats.set_defaults(func=cmd_content_stats)

    p_content_patterns = content_sub.add_parser("patterns", help="Rank the hooks, topics, and structures that perform best")
    p_content_patterns.add_argument("--limit", type=int, default=10, help="Maximum rows per section")
    p_content_patterns.add_argument("--industry", help="Optional industry filter")
    p_content_patterns.add_argument("--topic", action="append", default=[], help="Optional topic filter (repeatable)")
    p_content_patterns.add_argument("--author", help="Optional author filter")
    p_content_patterns.add_argument("--owned-only", action="store_true", help="Only analyze posts marked as your own")
    p_content_patterns.set_defaults(func=cmd_content_patterns)

    p_content_embed = content_sub.add_parser("embed", help="Generate embeddings for harvested posts")
    p_content_embed.add_argument("--limit", type=int, default=100, help="Maximum posts to embed")
    p_content_embed.add_argument(
        "--model",
        default="fastembed:BAAI/bge-small-en-v1.5",
        help="Embedding model name (default: fastembed:BAAI/bge-small-en-v1.5; use local-hash-v1 or text-embedding-3-small as needed)",
    )
    p_content_embed.add_argument("--batch-size", type=int, default=25, help="Embedding batch size")
    p_content_embed.add_argument("--all", action="store_true", help="Re-embed posts even if an embedding already exists")
    p_content_embed.set_defaults(func=cmd_content_embed)

    p_content_train = content_sub.add_parser("train-model", help="Train a local outcome model from synced post performance")
    p_content_train.add_argument("--name", default="default", help="Model name to store (default: default)")
    p_content_train.add_argument("--scope", choices=["auto", "owned", "all"], default="auto", help="Training corpus scope")
    p_content_train.add_argument("--min-samples", type=int, default=5, help="Minimum posts required to fit a model")
    p_content_train.add_argument("--industry", help="Optional industry slice for training")
    p_content_train.add_argument("--topic", action="append", default=[], help="Optional topic slice for training (repeatable)")
    p_content_train.set_defaults(func=cmd_content_train_model)

    p_content_model = content_sub.add_parser("model", help="Show a stored local outcome model")
    p_content_model.add_argument("--name", default="default", help="Model name to inspect")
    p_content_model.set_defaults(func=cmd_content_model)

    p_content_playbook = content_sub.add_parser("playbook", help="Show the learned hook, structure, and rewrite playbook from the corpus")
    p_content_playbook.add_argument("--limit", type=int, default=8, help="Maximum rows per learned section")
    p_content_playbook.add_argument("--industry", help="Optional industry filter")
    p_content_playbook.add_argument("--topic", action="append", default=[], help="Optional topic filter (repeatable)")
    p_content_playbook.add_argument("--author", help="Optional author filter")
    p_content_playbook.add_argument("--owned-only", action="store_true", help="Only analyze posts marked as your own")
    p_content_playbook.set_defaults(func=cmd_content_playbook)

    p_content_score = content_sub.add_parser("score-draft", help="Score a draft against the learned content library")
    p_content_score.add_argument("--text", help="Draft text")
    p_content_score.add_argument("--text-file", help="Read draft text from file")
    p_content_score.add_argument("--industry", help="Optional industry filter for the reference corpus")
    p_content_score.add_argument("--topic", action="append", default=[], help="Optional topic filter for the reference corpus (repeatable)")
    p_content_score.add_argument("--model", default="fastembed:BAAI/bge-small-en-v1.5", help="Embedding model used to encode the draft")
    p_content_score.set_defaults(func=cmd_content_score_draft)

    p_content_rewrite = content_sub.add_parser("rewrite", help="Rewrite a draft to better match winning patterns")
    p_content_rewrite.add_argument("--text", help="Draft text")
    p_content_rewrite.add_argument("--text-file", help="Read draft text from file")
    p_content_rewrite.add_argument("--industry", help="Optional industry filter for the reference corpus")
    p_content_rewrite.add_argument("--topic", action="append", default=[], help="Optional topic filter for the reference corpus (repeatable)")
    p_content_rewrite.add_argument("--goal", choices=["engagement", "instructional", "authority", "contrarian"], default="engagement", help="Rewrite goal")
    p_content_rewrite.add_argument("--model", default="fastembed:BAAI/bge-small-en-v1.5", help="Embedding model used to score the rewrite")
    p_content_rewrite.set_defaults(func=cmd_content_rewrite)

    p_content_maximize = content_sub.add_parser("maximize", help="Try several learned rewrite strategies and keep the highest-scoring draft")
    p_content_maximize.add_argument("--text", help="Draft text")
    p_content_maximize.add_argument("--text-file", help="Read draft text from file")
    p_content_maximize.add_argument("--industry", help="Optional industry filter for the reference corpus")
    p_content_maximize.add_argument("--topic", action="append", default=[], help="Optional topic filter for the reference corpus (repeatable)")
    p_content_maximize.add_argument("--goal", action="append", default=[], choices=["engagement", "instructional", "authority", "contrarian"], help="Candidate rewrite goal to evaluate (repeatable)")
    p_content_maximize.add_argument("--model", default="fastembed:BAAI/bge-small-en-v1.5", help="Embedding model used to score candidates")
    p_content_maximize.set_defaults(func=cmd_content_maximize)

    p_content_polish = content_sub.add_parser("polish-and-score", help="Generate polished rewrites for a draft and score them with the stacked model")
    p_content_polish.add_argument("--text", help="Draft text")
    p_content_polish.add_argument("--text-file", help="Read draft text from file")
    p_content_polish.add_argument("--industry", help="Optional industry filter for the reference corpus")
    p_content_polish.add_argument("--topic", action="append", default=[], help="Optional topic filter for the reference corpus (repeatable)")
    p_content_polish.add_argument("--goal", action="append", default=[], choices=["engagement", "instructional", "authority", "contrarian"], help="Rewrite goals to evaluate (repeatable)")
    p_content_polish.add_argument("--model", default="fastembed:BAAI/bge-small-en-v1.5", help="Embedding model used to score local draft variants")
    p_content_polish.add_argument("--stacked-model-name", help="Stored stacked model name (defaults to automatic best-model selection)")
    p_content_polish.add_argument("--target-file", help="Optional JSON target profile used for target-aware stacked scoring")
    p_content_polish.add_argument("--limit", type=int, default=3, help="Maximum ranked variants to return")
    p_content_polish.add_argument("--fresh", action="store_true", help="Add fresh prompt-generated variants in addition to rewrites")
    p_content_polish.add_argument("--long-form", action="store_true", help="Expand fresh variants into longer-form copy")
    p_content_polish.add_argument("--no-calibrate-weights", dest="auto_calibrate_weights", action="store_false", help="Disable quality-based head weight calibration")
    p_content_polish.set_defaults(auto_calibrate_weights=True)
    p_content_polish.set_defaults(func=cmd_content_polish_and_score)

    p_content_create = content_sub.add_parser("create", help="Generate multiple scored post candidates from a prompt and slice playbook")
    p_content_create.add_argument("--prompt", help="Prompt seed for the content generator")
    p_content_create.add_argument("--prompt-file", help="Read prompt seed from file")
    p_content_create.add_argument("--industry", help="Optional industry filter for the reference corpus")
    p_content_create.add_argument("--topic", action="append", default=[], help="Optional topic filter for the reference corpus (repeatable)")
    p_content_create.add_argument("--goal", action="append", default=[], choices=["engagement", "instructional", "authority", "contrarian", "launch"], help="Candidate generation goal (repeatable)")
    p_content_create.add_argument("--count", type=int, default=8, help="Number of candidates to generate")
    p_content_create.add_argument("--model", default="fastembed:BAAI/bge-small-en-v1.5", help="Embedding model used to score candidates")
    p_content_create.add_argument("--generator", choices=["auto", "heuristic", "cerebras"], default="auto", help="Candidate generation backend")
    p_content_create.add_argument("--speed", choices=["balanced", "max"], default="balanced", help="balanced uses fuller playbook context; max trims generation context for faster candidate creation")
    p_content_create.add_argument("--audience", help="Who the post is for, e.g. engineering leaders")
    p_content_create.add_argument("--objective", help="What the post should achieve, e.g. drive demos")
    p_content_create.add_argument("--tone", help="Desired voice, e.g. operator, direct, authoritative")
    p_content_create.add_argument("--format", help="Desired post format, e.g. story or operator")
    p_content_create.add_argument("--length", choices=["short", "medium", "long"], help="Desired post length")
    p_content_create.add_argument("--cta", help="Preferred closing CTA line")
    p_content_create.set_defaults(func=cmd_content_create)

    p_content_choose = content_sub.add_parser("choose", help="Generate candidates from a prompt and choose the strongest draft")
    p_content_choose.add_argument("--prompt", help="Prompt seed for the content generator")
    p_content_choose.add_argument("--prompt-file", help="Read prompt seed from file")
    p_content_choose.add_argument("--industry", help="Optional industry filter for the reference corpus")
    p_content_choose.add_argument("--topic", action="append", default=[], help="Optional topic filter for the reference corpus (repeatable)")
    p_content_choose.add_argument("--goal", action="append", default=[], choices=["engagement", "instructional", "authority", "contrarian", "launch"], help="Candidate generation goal (repeatable)")
    p_content_choose.add_argument("--count", type=int, default=8, help="Number of candidates to generate before choosing")
    p_content_choose.add_argument("--model", default="fastembed:BAAI/bge-small-en-v1.5", help="Embedding model used to score candidates")
    p_content_choose.add_argument("--generator", choices=["auto", "heuristic", "cerebras"], default="auto", help="Candidate generation backend")
    p_content_choose.add_argument("--speed", choices=["balanced", "max"], default="balanced", help="balanced uses fuller playbook context; max trims generation context for faster candidate creation")
    p_content_choose.add_argument("--audience", help="Who the post is for, e.g. ops leaders")
    p_content_choose.add_argument("--objective", help="What the post should achieve, e.g. book calls")
    p_content_choose.add_argument("--tone", help="Desired voice, e.g. operator, direct, authoritative")
    p_content_choose.add_argument("--format", help="Desired post format, e.g. story or operator")
    p_content_choose.add_argument("--length", choices=["short", "medium", "long"], help="Desired post length")
    p_content_choose.add_argument("--cta", help="Preferred closing CTA line")
    p_content_choose.add_argument("--policy-name", help="Optional stored policy name to use for action selection")
    p_content_choose.add_argument("--alpha", type=float, default=0.2, help="Exploration weight for policy selection")
    p_content_choose.add_argument("--log-decision", action="store_true", help="Log the choose decision into the policy store")
    p_content_choose.add_argument("--context-key", help="Optional stable context key for policy logging")
    p_content_choose.add_argument("--polish", action="store_true", help="Polish the selected candidate and score rewritten variants")
    p_content_choose.add_argument("--stacked-model-name", help="Stored stacked model name used for polishing")
    p_content_choose.add_argument("--target-file", help="Optional JSON target profile used for polishing")
    p_content_choose.add_argument("--polish-limit", type=int, default=3, help="Maximum polished variants to return")
    p_content_choose.add_argument("--no-calibrate-weights", dest="auto_calibrate_weights", action="store_false", help="Disable quality-based head weight calibration during polishing")
    p_content_choose.set_defaults(auto_calibrate_weights=True)
    p_content_choose.set_defaults(func=cmd_content_choose)

    p_content_queue = content_sub.add_parser("queue", help="Persist generated content candidates or list the candidate queue")
    p_content_queue.add_argument("--prompt", help="Prompt seed for generating queued candidates")
    p_content_queue.add_argument("--prompt-file", help="Read prompt seed from file")
    p_content_queue.add_argument("--industry", help="Optional industry filter for the reference corpus")
    p_content_queue.add_argument("--topic", action="append", default=[], help="Optional topic filter for the reference corpus (repeatable)")
    p_content_queue.add_argument("--goal", action="append", default=[], choices=["engagement", "instructional", "authority", "contrarian", "launch"], help="Candidate generation goal (repeatable)")
    p_content_queue.add_argument("--count", type=int, default=8, help="Number of candidates to generate before queueing")
    p_content_queue.add_argument("--model", default="fastembed:BAAI/bge-small-en-v1.5", help="Embedding model used to score queued candidates")
    p_content_queue.add_argument("--generator", choices=["auto", "heuristic", "cerebras"], default="auto", help="Candidate generation backend")
    p_content_queue.add_argument("--speed", choices=["balanced", "max"], default="balanced", help="balanced uses fuller playbook context; max trims generation context for faster candidate creation")
    p_content_queue.add_argument("--limit", type=int, default=20, help="Maximum queued candidates to list when no prompt is provided")
    p_content_queue.add_argument("--status", choices=["queued", "published"], help="Optional queue status filter when listing")
    p_content_queue.set_defaults(func=cmd_content_queue)

    p_content_show_candidate = content_sub.add_parser("show-candidate", help="Show a stored content candidate by id")
    p_content_show_candidate.add_argument("--candidate-id", required=True, help="Stored candidate id")
    p_content_show_candidate.set_defaults(func=cmd_content_show_candidate)

    p_content_mark_published = content_sub.add_parser("mark-published", help="Mark a stored candidate as published")
    p_content_mark_published.add_argument("--candidate-id", required=True, help="Stored candidate id")
    p_content_mark_published.add_argument("--post-url", help="Optional LinkedIn post URL for the published candidate")
    p_content_mark_published.set_defaults(func=cmd_content_mark_published)

    p_content_trace_list = content_sub.add_parser("trace-list", help="List stored autonomy traces")
    p_content_trace_list.add_argument("--limit", type=int, default=20, help="Maximum traces to show")
    p_content_trace_list.add_argument("--trace-type", help="Optional trace type filter")
    p_content_trace_list.add_argument("--status", help="Optional trace status filter")
    p_content_trace_list.set_defaults(func=cmd_content_trace_list)

    p_content_trace_show = content_sub.add_parser("trace-show", help="Show a stored autonomy trace")
    p_content_trace_show.add_argument("--trace-id", required=True, help="Trace id")
    p_content_trace_show.set_defaults(func=cmd_content_trace_show)

    p_content_trace_export = content_sub.add_parser("trace-export", help="Export a stored autonomy trace")
    p_content_trace_export.add_argument("--trace-id", required=True, help="Trace id")
    p_content_trace_export.add_argument("--output", help="Optional output path")
    p_content_trace_export.set_defaults(func=cmd_content_trace_export)

    p_content_tui = content_sub.add_parser("tui", help="Open a live terminal dashboard for traces, candidates, decisions, and rewards")
    p_content_tui.add_argument("--refresh", type=float, default=2.0, help="Refresh interval in seconds")
    p_content_tui.add_argument("--limit", type=int, default=10, help="Maximum recent rows per section")
    p_content_tui.add_argument("--trace-type", help="Optional trace type filter")
    p_content_tui.add_argument("--once", action="store_true", help="Print one snapshot instead of launching the interactive TUI")
    p_content_tui.set_defaults(func=cmd_content_tui)

    p_content_replay = content_sub.add_parser("replay", help="Replay a stored trace through the local replay environment")
    p_content_replay.add_argument("--trace-id", required=True, help="Trace id")
    p_content_replay.add_argument("--policy-name", help="Optional policy override")
    p_content_replay.set_defaults(func=cmd_content_replay)

    p_content_autonomy = content_sub.add_parser("autonomy-run", help="Run the traced content autonomy loop")
    p_content_autonomy.add_argument("--prompt", help="Prompt seed for the content generator")
    p_content_autonomy.add_argument("--prompt-file", help="Read prompt seed from file")
    p_content_autonomy.add_argument("--industry", help="Optional industry filter")
    p_content_autonomy.add_argument("--topic", action="append", default=[], help="Optional topic filter (repeatable)")
    p_content_autonomy.add_argument("--goal", action="append", default=[], choices=["engagement", "instructional", "authority", "contrarian", "launch"], help="Candidate generation goal (repeatable)")
    p_content_autonomy.add_argument("--count", type=int, default=8, help="Number of candidates to generate")
    p_content_autonomy.add_argument("--model", default="fastembed:BAAI/bge-small-en-v1.5", help="Embedding model used to score candidates")
    p_content_autonomy.add_argument("--generator", choices=["auto", "heuristic", "cerebras"], default="auto", help="Candidate generation backend")
    p_content_autonomy.add_argument("--speed", choices=["balanced", "max"], default="balanced", help="balanced uses fuller playbook context; max trims generation context for faster candidate creation")
    p_content_autonomy.add_argument("--audience", help="Who the post is for, e.g. platform teams")
    p_content_autonomy.add_argument("--objective", help="What the post should achieve, e.g. attract inbound leads")
    p_content_autonomy.add_argument("--tone", help="Desired voice, e.g. operator, direct, authoritative")
    p_content_autonomy.add_argument("--format", help="Desired post format, e.g. story or operator")
    p_content_autonomy.add_argument("--length", choices=["short", "medium", "long"], help="Desired post length")
    p_content_autonomy.add_argument("--cta", help="Preferred closing CTA line")
    p_content_autonomy.add_argument("--decision-provider", choices=["local-policy", "cerebras"], default="local-policy", help="Decision layer used to choose the action from the typed runtime request")
    p_content_autonomy.add_argument("--policy-name", default="content-default", help="Stored policy name to use")
    p_content_autonomy.add_argument("--alpha", type=float, default=0.2, help="Exploration weight for policy selection")
    p_content_autonomy.add_argument("--mode", choices=["review", "limited"], default="review", help="Autonomy mode")
    p_content_autonomy.add_argument("--post-url", help="When mode=limited, mark the chosen candidate as published at this URL")
    p_content_autonomy.add_argument("--polish-selected", action="store_true", help="Polish the selected candidate and score rewritten variants")
    p_content_autonomy.add_argument("--stacked-model-name", help="Stored stacked model name used for polishing")
    p_content_autonomy.add_argument("--target-file", help="Optional JSON target profile used for polishing")
    p_content_autonomy.add_argument("--polish-limit", type=int, default=3, help="Maximum polished variants to return")
    p_content_autonomy.add_argument("--no-calibrate-weights", dest="auto_calibrate_weights", action="store_false", help="Disable quality-based head weight calibration during polishing")
    p_content_autonomy.set_defaults(auto_calibrate_weights=True)
    p_content_autonomy.set_defaults(func=cmd_content_autonomy_run)

    p_content_provider_set = content_sub.add_parser("provider-set", help="Store local config for an external content generation provider")
    p_content_provider_set.add_argument("--provider", choices=["cerebras"], default="cerebras", help="Provider name")
    p_content_provider_set.add_argument("--api-key", required=True, help="Provider API key")
    p_content_provider_set.add_argument("--model", required=True, help="Provider model name")
    p_content_provider_set.add_argument("--base-url", help="Optional API base URL override")
    p_content_provider_set.set_defaults(func=cmd_content_provider_set)

    p_content_provider_show = content_sub.add_parser("provider-show", help="Show local config for an external content generation provider")
    p_content_provider_show.add_argument("--provider", choices=["cerebras"], default="cerebras", help="Provider name")
    p_content_provider_show.set_defaults(func=cmd_content_provider_show)

    p_content_train_policy = content_sub.add_parser("train-policy", help="Train a local contextual policy from logged decisions and rewards")
    p_content_train_policy.add_argument("--policy-name", required=True, help="Stored policy name")
    p_content_train_policy.add_argument("--context-type", help="Optional context type filter, e.g. content_publish")
    p_content_train_policy.add_argument("--min-samples", type=int, default=25, help="Minimum rewarded decisions required for training")
    p_content_train_policy.add_argument("--alpha", type=float, default=0.2, help="Exploration weight to store with the policy")
    p_content_train_policy.add_argument("--ridge", type=float, default=0.01, help="Ridge regularization strength")
    p_content_train_policy.set_defaults(func=cmd_content_train_policy)

    p_content_policy_report = content_sub.add_parser("policy-report", help="Show offline evaluation and reward stats for a stored policy")
    p_content_policy_report.add_argument("--policy-name", required=True, help="Stored policy name")
    p_content_policy_report.add_argument("--context-type", help="Optional context type filter, e.g. content_publish")
    p_content_policy_report.set_defaults(func=cmd_content_policy_report)

    p_content_sync_outcomes = content_sub.add_parser("sync-outcomes", help="Refresh post outcomes and mark your own winning posts")
    p_content_sync_outcomes.add_argument("--url", action="append", default=[], help="Stored LinkedIn post URL to sync (repeatable)")
    p_content_sync_outcomes.add_argument("--owned", action="store_true", help="Mark the synced posts as your own posts")
    p_content_sync_outcomes.set_defaults(func=cmd_content_sync_outcomes)

    p_content_retrieve = content_sub.add_parser("retrieve", help="Retrieve the best matching posts for a free-text query")
    p_content_retrieve.add_argument("--text", required=True, help="Query text")
    p_content_retrieve.add_argument("--limit", type=int, default=10, help="Maximum matches to show")
    p_content_retrieve.add_argument("--method", choices=["hybrid", "semantic", "fingerprint", "lexical"], default="hybrid", help="Retrieval strategy")
    p_content_retrieve.add_argument("--model", default="fastembed:BAAI/bge-small-en-v1.5", help="Embedding model used to encode the query")
    p_content_retrieve.add_argument("--industry", help="Optional industry filter")
    p_content_retrieve.add_argument("--author", help="Optional author filter")
    p_content_retrieve.add_argument("--full", action="store_true", help="Include raw embedding and fingerprint arrays in results")
    p_content_retrieve.set_defaults(func=cmd_content_retrieve)

    p_content_similar = content_sub.add_parser("similar", help="Retrieve posts similar to a stored post URL")
    p_content_similar.add_argument("--url", required=True, help="Stored post URL")
    p_content_similar.add_argument("--limit", type=int, default=10, help="Maximum matches to show")
    p_content_similar.add_argument("--method", choices=["hybrid", "semantic", "fingerprint", "lexical"], default="hybrid", help="Retrieval strategy")
    p_content_similar.add_argument("--full", action="store_true", help="Include raw embedding and fingerprint arrays in results")
    p_content_similar.set_defaults(func=cmd_content_similar)

    p_content_rebuild = content_sub.add_parser("rebuild-index", help="Rebuild persisted retrieval indexes for harvested posts")
    p_content_rebuild.add_argument("--kind", choices=["all", "semantic", "fingerprint"], default="all", help="Index family to rebuild")
    p_content_rebuild.add_argument("--model", help="Semantic embedding model to rebuild (semantic only)")
    p_content_rebuild.set_defaults(func=cmd_content_rebuild_index)

    p_content_export = content_sub.add_parser("export-index", help="Export vectors and payloads for an external ANN backend")
    p_content_export.add_argument("--kind", choices=["all", "semantic", "fingerprint"], default="all", help="Index family to export")
    p_content_export.add_argument("--model", help="Semantic embedding model to export (semantic only)")
    p_content_export.add_argument("--output", help="Output directory for exported JSONL files")
    p_content_export.set_defaults(func=cmd_content_export_index)

    p_content_jobs = content_sub.add_parser("harvest-jobs", help="List persisted content harvest jobs")
    p_content_jobs.add_argument("--limit", type=int, default=20, help="Maximum jobs to show")
    p_content_jobs.set_defaults(func=cmd_content_harvest_jobs)
    p_content_query_stats = content_sub.add_parser("query-stats", help="Show historical per-query yield stats")
    p_content_query_stats.add_argument("--job-prefix", help="Optional job prefix filter")
    p_content_query_stats.add_argument("--limit", type=int, default=20, help="Maximum query rows to show")
    p_content_query_stats.set_defaults(func=cmd_content_query_stats)

    p_content_materialize = content_sub.add_parser("materialize", help="Materialize local content shard files into the local DuckDB warehouse")
    p_content_materialize.add_argument("--job-id", help="Optional harvest job id to materialize")
    p_content_materialize.set_defaults(func=cmd_content_materialize)

    p_content_warehouse_stats = content_sub.add_parser("warehouse-stats", help="Show stats from the local DuckDB content warehouse")
    p_content_warehouse_stats.add_argument("--industry", help="Optional industry filter")
    p_content_warehouse_stats.set_defaults(func=cmd_content_warehouse_stats)

    p_content_build_dataset = content_sub.add_parser("build-dataset", help="Build local train/val/test datasets from the DuckDB warehouse")
    p_content_build_dataset.add_argument("--output", required=True, help="Directory where train/val/test JSONL files should be written")
    p_content_build_dataset.add_argument("--industry", action="append", default=[], help="Optional industry filter (repeatable)")
    p_content_build_dataset.add_argument("--train-ratio", type=float, default=0.8, help="Training split ratio")
    p_content_build_dataset.add_argument("--val-ratio", type=float, default=0.1, help="Validation split ratio")
    p_content_build_dataset.set_defaults(func=cmd_content_build_dataset)

    p_content_build_reward_dataset = content_sub.add_parser("build-reward-dataset", help="Build local reward datasets from the DuckDB warehouse")
    p_content_build_reward_dataset.add_argument("--output", required=True, help="Directory where train/val/test JSONL files should be written")
    p_content_build_reward_dataset.add_argument("--industry", action="append", default=[], help="Optional industry filter (repeatable)")
    p_content_build_reward_dataset.add_argument("--owned-only", action="store_true", help="Only include owned posts")
    p_content_build_reward_dataset.add_argument("--train-ratio", type=float, default=0.8, help="Training split ratio")
    p_content_build_reward_dataset.add_argument("--val-ratio", type=float, default=0.1, help="Validation split ratio")
    p_content_build_reward_dataset.set_defaults(func=cmd_content_build_reward_dataset)

    p_content_build_policy_dataset = content_sub.add_parser("build-policy-dataset", help="Build local policy decision datasets from logged rewards")
    p_content_build_policy_dataset.add_argument("--output", required=True, help="Directory where train/val/test JSONL files should be written")
    p_content_build_policy_dataset.add_argument("--policy-name", required=True, help="Stored policy name")
    p_content_build_policy_dataset.add_argument("--context-type", help="Optional context type filter")
    p_content_build_policy_dataset.add_argument("--train-ratio", type=float, default=0.8, help="Training split ratio")
    p_content_build_policy_dataset.add_argument("--val-ratio", type=float, default=0.1, help="Validation split ratio")
    p_content_build_policy_dataset.set_defaults(func=cmd_content_build_policy_dataset)

    p_content_build_sft_dataset = content_sub.add_parser("build-sft-dataset", help="Build local SFT datasets for Qwen content tuning")
    p_content_build_sft_dataset.add_argument("--output", required=True, help="Directory where train/val/test JSONL files should be written")
    p_content_build_sft_dataset.add_argument("--industry", help="Optional industry filter")
    p_content_build_sft_dataset.add_argument("--topic", action="append", default=[], help="Optional topic filter (repeatable)")
    p_content_build_sft_dataset.add_argument("--train-ratio", type=float, default=0.8, help="Training split ratio")
    p_content_build_sft_dataset.add_argument("--val-ratio", type=float, default=0.1, help="Validation split ratio")
    p_content_build_sft_dataset.set_defaults(func=cmd_content_build_sft_dataset)

    p_content_build_preference_dataset = content_sub.add_parser("build-preference-dataset", help="Build local preference datasets for Qwen content tuning")
    p_content_build_preference_dataset.add_argument("--output", required=True, help="Directory where train/val/test JSONL files should be written")
    p_content_build_preference_dataset.add_argument("--industry", help="Optional industry filter")
    p_content_build_preference_dataset.add_argument("--topic", action="append", default=[], help="Optional topic filter (repeatable)")
    p_content_build_preference_dataset.add_argument("--train-ratio", type=float, default=0.8, help="Training split ratio")
    p_content_build_preference_dataset.add_argument("--val-ratio", type=float, default=0.1, help="Validation split ratio")
    p_content_build_preference_dataset.set_defaults(func=cmd_content_build_preference_dataset)

    p_content_eval_dataset = content_sub.add_parser("eval-dataset", help="Evaluate a local dataset artifact and persist a report")
    p_content_eval_dataset.add_argument("--dataset-dir", required=True, help="Dataset directory containing train/val/test JSONL files")
    p_content_eval_dataset.set_defaults(func=cmd_content_eval_dataset)

    p_content_eval_qwen = content_sub.add_parser("eval-qwen", help="Evaluate candidate generation quality for a prompt slice")
    p_content_eval_qwen.add_argument("--prompt", help="Prompt seed text")
    p_content_eval_qwen.add_argument("--prompt-file", help="Path to a file containing the prompt")
    p_content_eval_qwen.add_argument("--industry", help="Optional industry filter")
    p_content_eval_qwen.add_argument("--topic", action="append", default=[], help="Optional topic filter (repeatable)")
    p_content_eval_qwen.add_argument("--count", type=int, default=8, help="Candidate count to evaluate")
    p_content_eval_qwen.add_argument("--model", default="local-hash-v1", help="Scoring model")
    p_content_eval_qwen.add_argument("--generator", choices=["heuristic", "qwen-local"], default="heuristic", help="Generator family to evaluate")
    p_content_eval_qwen.set_defaults(func=cmd_content_eval_qwen)

    p_content_eval_policy = content_sub.add_parser("eval-policy", help="Persist an offline evaluation report for a stored policy")
    p_content_eval_policy.add_argument("--policy-name", required=True, help="Stored policy name")
    p_content_eval_policy.add_argument("--context-type", help="Optional context type filter")
    p_content_eval_policy.set_defaults(func=cmd_content_eval_policy)

    p_content_eval_runtime = content_sub.add_parser("eval-runtime", help="Validate a runtime request/response pair and persist a report")
    p_content_eval_runtime.add_argument("--request-file", required=True, help="Path to the runtime request JSON file")
    p_content_eval_runtime.add_argument("--response-file", required=True, help="Path to the runtime response JSON file")
    p_content_eval_runtime.set_defaults(func=cmd_content_eval_runtime)

    p_content_curate = content_sub.add_parser("curate-corpus", help="Score, dedupe, and label the warehouse corpus")
    p_content_curate.add_argument("--industry", action="append", default=[], help="Optional industry filter (repeatable)")
    p_content_curate.add_argument("--min-quality", type=float, default=0.0, help="Minimum quality score required to keep a row")
    p_content_curate.add_argument("--near-duplicate-hamming", type=int, default=4, help="Maximum simhash Hamming distance for near-duplicate suppression")
    p_content_curate.set_defaults(func=cmd_content_curate_corpus)

    p_content_curation_stats = content_sub.add_parser("curation-stats", help="Show curated corpus stats")
    p_content_curation_stats.set_defaults(func=cmd_content_curation_stats)

    p_content_holdouts = content_sub.add_parser("build-holdouts", help="Build balanced train/val/test holdouts from the curated corpus")
    p_content_holdouts.add_argument("--output", required=True, help="Directory where train/val/test JSONL files should be written")
    p_content_holdouts.add_argument("--industry", action="append", default=[], help="Optional industry filter (repeatable)")
    p_content_holdouts.add_argument("--topic", action="append", default=[], help="Optional topic filter (repeatable)")
    p_content_holdouts.add_argument("--limit", type=int, default=50000, help="Maximum curated rows to sample")
    p_content_holdouts.add_argument("--quota-per-industry", type=int, help="Optional cap per industry")
    p_content_holdouts.add_argument("--quota-per-topic", type=int, help="Optional cap per topic")
    p_content_holdouts.add_argument("--quota-per-format", type=int, help="Optional cap per structure")
    p_content_holdouts.add_argument("--train-ratio", type=float, default=0.8, help="Training split ratio")
    p_content_holdouts.add_argument("--val-ratio", type=float, default=0.1, help="Validation split ratio")
    p_content_holdouts.add_argument("--time-holdout-ratio", type=float, default=0.1, help="Recent chronological holdout ratio written to time_holdout.jsonl")
    p_content_holdouts.set_defaults(func=cmd_content_build_holdouts)

    p_content_curated_sft = content_sub.add_parser("build-curated-sft", help="Build a quota-aware SFT dataset from the curated corpus")
    p_content_curated_sft.add_argument("--output", required=True, help="Directory where train/val/test JSONL files should be written")
    p_content_curated_sft.add_argument("--industry", action="append", default=[], help="Optional industry filter (repeatable)")
    p_content_curated_sft.add_argument("--topic", action="append", default=[], help="Optional topic filter (repeatable)")
    p_content_curated_sft.add_argument("--limit", type=int, default=50000, help="Maximum curated rows to sample")
    p_content_curated_sft.add_argument("--quota-per-industry", type=int, help="Optional cap per industry")
    p_content_curated_sft.add_argument("--quota-per-topic", type=int, help="Optional cap per topic")
    p_content_curated_sft.add_argument("--quota-per-format", type=int, help="Optional cap per structure")
    p_content_curated_sft.add_argument("--train-ratio", type=float, default=0.8, help="Training split ratio")
    p_content_curated_sft.add_argument("--val-ratio", type=float, default=0.1, help="Validation split ratio")
    p_content_curated_sft.set_defaults(func=cmd_content_build_curated_sft)

    p_content_curated_pref = content_sub.add_parser("build-curated-preference", help="Build a quota-aware preference dataset from the curated corpus")
    p_content_curated_pref.add_argument("--output", required=True, help="Directory where train/val/test JSONL files should be written")
    p_content_curated_pref.add_argument("--industry", action="append", default=[], help="Optional industry filter (repeatable)")
    p_content_curated_pref.add_argument("--topic", action="append", default=[], help="Optional topic filter (repeatable)")
    p_content_curated_pref.add_argument("--limit", type=int, default=50000, help="Maximum curated rows to sample")
    p_content_curated_pref.add_argument("--quota-per-industry", type=int, help="Optional cap per industry")
    p_content_curated_pref.add_argument("--quota-per-topic", type=int, help="Optional cap per topic")
    p_content_curated_pref.add_argument("--quota-per-format", type=int, help="Optional cap per structure")
    p_content_curated_pref.add_argument("--train-ratio", type=float, default=0.8, help="Training split ratio")
    p_content_curated_pref.add_argument("--val-ratio", type=float, default=0.1, help="Validation split ratio")
    p_content_curated_pref.set_defaults(func=cmd_content_build_curated_preference)

    p_content_train_qwen = content_sub.add_parser("train-qwen", help="Plan or run a local Qwen SFT/preference training job")
    p_content_train_qwen.add_argument("--phase", choices=["sft", "preference"], required=True, help="Training phase")
    p_content_train_qwen.add_argument("--dataset-dir", required=True, help="Dataset directory containing train/val/test JSONL files")
    p_content_train_qwen.add_argument("--base-model", default="Qwen/Qwen2.5-3B-Instruct", help="Local base model name or path")
    p_content_train_qwen.add_argument("--output-name", help="Optional stable run directory name")
    p_content_train_qwen.add_argument("--runner", choices=["local", "modal"], default="local", help="Training runner backend")
    p_content_train_qwen.add_argument("--wandb-project", help="Optional W&B project for run tracking")
    p_content_train_qwen.add_argument("--wandb-entity", help="Optional W&B entity/team")
    p_content_train_qwen.add_argument("--modal-app-name", help="Optional Modal app name")
    p_content_train_qwen.add_argument("--learning-rate", type=float, default=2e-4, help="Learning rate")
    p_content_train_qwen.add_argument("--epochs", type=float, default=1.0, help="Epoch count")
    p_content_train_qwen.add_argument("--lora-rank", type=int, default=16, help="LoRA rank")
    p_content_train_qwen.add_argument("--per-device-batch-size", type=int, default=2, help="Per-device batch size")
    p_content_train_qwen.add_argument("--gradient-accumulation-steps", type=int, default=8, help="Gradient accumulation steps")
    p_content_train_qwen.add_argument("--dry-run", action="store_true", help="Only write the manifest and command")
    p_content_train_qwen.set_defaults(func=cmd_content_train_qwen)

    p_content_qwen_runs = content_sub.add_parser("qwen-runs", help="List recorded local Qwen training runs")
    p_content_qwen_runs.add_argument("--limit", type=int, default=20, help="Maximum runs to show")
    p_content_qwen_runs.set_defaults(func=cmd_content_qwen_runs)

    p_content_train_warehouse = content_sub.add_parser("train-warehouse-model", help="Train a local model from the DuckDB warehouse corpus")
    p_content_train_warehouse.add_argument("--name", default="warehouse-default", help="Stored model name")
    p_content_train_warehouse.add_argument("--industry", action="append", default=[], help="Optional industry filter (repeatable)")
    p_content_train_warehouse.add_argument("--min-samples", type=int, default=100, help="Minimum rows required to fit a model")
    p_content_train_warehouse.add_argument("--max-rows", type=int, default=100000, help="Maximum warehouse rows to load for training")
    p_content_train_warehouse.set_defaults(func=cmd_content_train_warehouse_model)

    p_content_warehouse_model = content_sub.add_parser("warehouse-model", help="Show a stored local warehouse-trained model")
    p_content_warehouse_model.add_argument("--name", default="warehouse-default", help="Stored model name")
    p_content_warehouse_model.set_defaults(func=cmd_content_warehouse_model)

    p_content_build_foundation = content_sub.add_parser("build-foundation-views", help="Build or refresh warehouse-backed stacked ranking views")
    p_content_build_foundation.add_argument("--industry", action="append", default=[], help="Optional industry filter (repeatable)")
    p_content_build_foundation.set_defaults(func=cmd_content_build_foundation_views)

    p_content_train_stacked = content_sub.add_parser("train-stacked-model", help="Train the local stacked content ranking foundation artifact")
    p_content_train_stacked.add_argument("--name", default="foundation-v1", help="Stored model name")
    p_content_train_stacked.add_argument("--industry", action="append", default=[], help="Optional industry filter (repeatable)")
    p_content_train_stacked.add_argument("--min-samples", type=int, default=100, help="Minimum foundation rows required to fit the model")
    p_content_train_stacked.add_argument("--artifact-dir", help="Optional artifact output directory")
    p_content_train_stacked.add_argument("--holdout-dir", help="Optional holdout directory with train/val/test/time_holdout JSONL files")
    p_content_train_stacked.set_defaults(func=cmd_content_train_stacked_model)

    p_content_select_stacked = content_sub.add_parser("select-stacked-model", help="Select the strongest stacked content model from stored holdout metrics")
    p_content_select_stacked.set_defaults(func=cmd_content_select_stacked_model)

    p_content_rerank_target = content_sub.add_parser("rerank-target", help="Rerank warehouse posts for a target profile using a stacked model")
    p_content_rerank_target.add_argument("--model-name", help="Stored stacked model name (defaults to automatic best-model selection)")
    p_content_rerank_target.add_argument("--target-file", required=True, help="Path to a JSON target profile")
    p_content_rerank_target.add_argument("--limit", type=int, default=20, help="Maximum ranked rows to show")
    p_content_rerank_target.add_argument("--no-calibrate-weights", dest="auto_calibrate_weights", action="store_false", help="Disable quality-based head weight calibration")
    p_content_rerank_target.set_defaults(auto_calibrate_weights=True)
    p_content_rerank_target.set_defaults(func=cmd_content_rerank_target)

    p_content_audit_targets = content_sub.add_parser("audit-targets", help="Compare calibrated vs raw reranks across one or more target profiles")
    p_content_audit_targets.add_argument("--target-file", action="append", required=True, help="Path to a JSON target profile (repeatable)")
    p_content_audit_targets.add_argument("--model-name", help="Stored stacked model name (defaults to automatic best-model selection)")
    p_content_audit_targets.add_argument("--limit", type=int, default=10, help="Maximum ranked rows to include per profile")
    p_content_audit_targets.add_argument("--sample-size", type=int, default=5000, help="Number of warehouse posts to audit against")
    p_content_audit_targets.set_defaults(func=cmd_content_audit_targets)

    p_content_generate_bench = content_sub.add_parser("generate-benchmark-corpus", help="Generate a synthetic local content shard corpus for warehouse benchmarking")
    p_content_generate_bench.add_argument("--job-id", required=True, help="Benchmark corpus job id")
    p_content_generate_bench.add_argument("--rows", type=int, required=True, help="Number of synthetic rows to generate")
    p_content_generate_bench.add_argument("--industry", action="append", default=[], help="Industry labels to cycle through")
    p_content_generate_bench.add_argument("--topic", action="append", default=[], help="Topic labels to cycle through")
    p_content_generate_bench.set_defaults(func=cmd_content_generate_benchmark_corpus)

    p_content_benchmark = content_sub.add_parser("benchmark-warehouse", help="Benchmark local materialize, stats, and dataset build steps for a shard corpus")
    p_content_benchmark.add_argument("--job-id", required=True, help="Benchmark corpus job id")
    p_content_benchmark.add_argument("--dataset-output", required=True, help="Directory for benchmark dataset output")
    p_content_benchmark.add_argument("--industry", action="append", default=[], help="Optional industry filter for dataset/stats")
    p_content_benchmark.set_defaults(func=cmd_content_benchmark_warehouse)

    p_content_benchmark_report = content_sub.add_parser("benchmark-report", help="List saved local warehouse benchmark reports")
    p_content_benchmark_report.add_argument("--limit", type=int, default=20, help="Maximum reports to show")
    p_content_benchmark_report.set_defaults(func=cmd_content_benchmark_report)

    p_lead = sub.add_parser("lead", help="Lead ranking and autopilot commands")
    lead_sub = p_lead.add_subparsers(dest="lead_command", required=True)

    p_lead_autopilot = lead_sub.add_parser("autopilot", help="Lead autopilot actions")
    lead_autopilot_sub = p_lead_autopilot.add_subparsers(dest="lead_autopilot_command", required=True)
    p_lead_autopilot_run = lead_autopilot_sub.add_parser("run", help="Enrich, score, and route discovery prospects")
    p_lead_autopilot_run.add_argument("--limit", type=int, default=25, help="Maximum prospects to evaluate")
    p_lead_autopilot_run.add_argument("--state", choices=["new", "watch", "ready", "contacted", "waiting", "engaged", "won", "cold", "do_not_contact"], help="Optional discovery state filter")
    p_lead_autopilot_run.add_argument("--topic", action="append", default=[], help="Target topic keyword (repeatable)")
    p_lead_autopilot_run.add_argument("--post-url", action="append", default=[], help="Post URL to ingest visible engagers from (repeatable)")
    p_lead_autopilot_run.add_argument("--all-owned", action="store_true", help="Ingest visible engagers from all owned posts before ranking")
    p_lead_autopilot_run.add_argument("--min-fit", type=float, default=0.45, help="Minimum fit score for a ready recommendation")
    p_lead_autopilot_run.add_argument("--min-reply", type=float, default=0.35, help="Minimum reply likelihood for a ready recommendation")
    p_lead_autopilot_run.add_argument("--min-deal", type=float, default=0.25, help="Minimum deal likelihood for a ready recommendation")
    p_lead_autopilot_run.add_argument("--sync-contacts", action="store_true", help="Sync recommended leads into the contact store")
    p_lead_autopilot_run.add_argument("--execute", action="store_true", help="Persist recommended queue-state changes (default is dry-run)")
    p_lead_autopilot_run.set_defaults(func=cmd_lead_autopilot_run)

    p_lead_rank = lead_sub.add_parser("rank", help="List the highest-ranked leads")
    p_lead_rank.add_argument("--limit", type=int, default=25, help="Maximum leads to show")
    p_lead_rank.set_defaults(func=cmd_lead_rank)

    p_lead_show = lead_sub.add_parser("show", help="Show a lead with enrichment details")
    p_lead_show.add_argument("--profile", required=True, help="Prospect profile key")
    p_lead_show.set_defaults(func=cmd_lead_show)

    p_comment = sub.add_parser("comment", help="Public comment queue and drafting commands")
    comment_sub = p_comment.add_subparsers(dest="comment_command", required=True)
    p_comment_queue = comment_sub.add_parser("queue", help="Fetch a post and persist visible public comments into the queue")
    p_comment_queue.add_argument("--post-url", required=True, help="LinkedIn post URL")
    p_comment_queue.set_defaults(func=cmd_comment_queue)

    p_comment_draft = comment_sub.add_parser("draft", help="Draft a reply for a queued comment")
    p_comment_draft.add_argument("--post-url", required=True, help="LinkedIn post URL")
    p_comment_draft.add_argument("--profile", help="Comment author profile key")
    p_comment_draft.add_argument("--comment-id", help="Exact queued comment ID")
    p_comment_draft.add_argument("--tone", choices=["expert", "warm", "contrarian", "neutral"], default="expert", help="Reply tone")
    p_comment_draft.set_defaults(func=cmd_comment_draft)

    p_comment_execute = comment_sub.add_parser("execute", help="Post a public comment on the post thread from text or a queued draft")
    p_comment_execute.add_argument("--post-url", required=True, help="LinkedIn post URL")
    p_comment_execute.add_argument("--text", help="Comment text to publish")
    p_comment_execute.add_argument("--text-file", help="Read comment text from file")
    p_comment_execute.add_argument("--profile", help="Comment author profile key for selecting a queued draft")
    p_comment_execute.add_argument("--comment-id", help="Exact queued comment ID to publish from draft_reply")
    p_comment_execute.add_argument("--execute", action="store_true", help="Actually execute (default is dry-run)")
    p_comment_execute.set_defaults(func=cmd_comment_execute)

    # --- Write system commands ---

    # post publish
    p_post = sub.add_parser("post", help="Post-related commands")
    post_sub = p_post.add_subparsers(dest="post_command", required=True)
    p_post_publish = post_sub.add_parser("publish", help="Publish a text post")
    p_post_publish.add_argument("--text", help="Post text content")
    p_post_publish.add_argument("--text-file", help="Read post text from file")
    p_post_publish.add_argument("--image", help="Path to image file to include in post")
    p_post_publish.add_argument("--visibility", choices=["anyone", "connections"], default="anyone", help="Post visibility (default: anyone)")
    p_post_publish.add_argument("--score", action="store_true", help="Score the draft against the local content library before publishing")
    p_post_publish.add_argument("--score-model", default="fastembed:BAAI/bge-small-en-v1.5", help="Embedding model used when --score is enabled")
    p_post_publish.add_argument("--execute", action="store_true", help="Actually execute (default is dry-run)")
    p_post_publish.set_defaults(func=cmd_post_publish)

    # profile snapshot
    p_snapshot = sub.add_parser("snapshot", help="Snapshot authenticated user profile")
    p_snapshot.add_argument("--me", action="store_true", default=True, help="Snapshot own profile (default)")
    p_snapshot.add_argument("--output", help="Save snapshot to JSON file")
    p_snapshot.set_defaults(func=cmd_profile_snapshot)

    # profile edit
    p_edit = sub.add_parser("edit", help="Edit a profile field")
    p_edit.add_argument("field", choices=["headline", "about", "website", "location"], help="Profile field to edit")
    p_edit.add_argument("--value", help="New field value")
    p_edit.add_argument("--file", help="Read value from file")
    p_edit.add_argument("--execute", action="store_true", help="Actually execute (default is dry-run)")
    p_edit.set_defaults(func=cmd_profile_edit)

    # experience add
    p_exp = sub.add_parser("experience", help="Experience/position commands")
    exp_sub = p_exp.add_subparsers(dest="experience_command", required=True)
    p_exp_add = exp_sub.add_parser("add", help="Add a new experience/position entry")
    p_exp_add.add_argument("--title", required=True, help="Job title")
    p_exp_add.add_argument("--company", required=True, help="Company name")
    p_exp_add.add_argument("--description", help="Role description")
    p_exp_add.add_argument("--location", help="Location (e.g. San Francisco, CA)")
    p_exp_add.add_argument("--start", help="Start date in MM/YYYY format")
    p_exp_add.add_argument("--end", help="End date in MM/YYYY format (omit for current position)")
    p_exp_add.add_argument("--execute", action="store_true", help="Actually execute (default is dry-run)")
    p_exp_add.set_defaults(func=cmd_experience_add)

    # connect
    p_connect = sub.add_parser("connect", help="Send a connection request")
    p_connect.add_argument("--profile", required=True, help="Profile URL or slug")
    p_connect.add_argument("--message", help="Custom invitation message")
    p_connect.add_argument("--execute", action="store_true", help="Actually execute (default is dry-run)")
    p_connect.set_defaults(func=cmd_connect)

    # follow
    p_follow = sub.add_parser("follow", help="Follow a profile")
    p_follow.add_argument("--profile", required=True, help="Profile URL or slug")
    p_follow.add_argument("--execute", action="store_true", help="Actually execute (default is dry-run)")
    p_follow.set_defaults(func=cmd_follow)

    # dm commands
    p_dm = sub.add_parser("dm", help="Direct message commands")
    dm_sub = p_dm.add_subparsers(dest="dm_command", required=True)
    p_dm_list = dm_sub.add_parser("list", help="List recent conversations")
    p_dm_list.add_argument("--limit", type=int, default=10, help="Max conversations to show")
    p_dm_list.set_defaults(func=cmd_dm_list)
    p_dm_send = dm_sub.add_parser("send", help="Send a direct message")
    p_dm_send.add_argument("--to", help="Profile URL or slug (for new conversation)")
    p_dm_send.add_argument("--conversation", help="Conversation URN (for reply)")
    p_dm_send.add_argument("--message", help="Message text")
    p_dm_send.add_argument("--message-file", help="Read message from file")
    p_dm_send.add_argument("--execute", action="store_true", help="Actually send (default is dry-run)")
    p_dm_send.set_defaults(func=cmd_dm_send)

    # schedule command
    p_schedule = sub.add_parser("schedule", help="Schedule a LinkedIn post")
    p_schedule.add_argument("--text", help="Post text content")
    p_schedule.add_argument("--text-file", help="Read post text from file")
    p_schedule.add_argument("--image", help="Path to image file")
    p_schedule.add_argument("--at", required=True, help="ISO datetime to publish (e.g. 2026-03-17T09:00:00)")
    p_schedule.add_argument("--visibility", choices=["anyone", "connections"], default="anyone")
    p_schedule.set_defaults(func=cmd_schedule)

    # action list
    p_action = sub.add_parser("action", help="Manage write actions")
    action_sub = p_action.add_subparsers(dest="action_command", required=True)
    p_action_list = action_sub.add_parser("list", help="List recent actions")
    p_action_list.add_argument("--state", choices=["planned", "dry_run", "executing", "succeeded", "failed", "unknown_remote_state", "retry_scheduled", "blocked", "duplicate_skipped", "canceled"], help="Filter by state")
    p_action_list.add_argument("--limit", type=int, default=20, help="Max results")
    p_action_list.set_defaults(func=cmd_action_list)

    # action show
    p_action_show = action_sub.add_parser("show", help="Show action details")
    p_action_show.add_argument("action_id", help="Action ID")
    p_action_show.set_defaults(func=cmd_action_show)

    # action retry
    p_action_retry = action_sub.add_parser("retry", help="Retry a failed action")
    p_action_retry.add_argument("action_id", help="Action ID to retry")
    p_action_retry.set_defaults(func=cmd_action_retry)

    p_action_reconcile = action_sub.add_parser("reconcile", help="Reconcile uncertain action state against LinkedIn")
    p_action_reconcile.add_argument("action_id", help="Action ID to reconcile")
    p_action_reconcile.set_defaults(func=cmd_action_reconcile)

    p_action_cancel = action_sub.add_parser("cancel", help="Cancel a pending or retryable action")
    p_action_cancel.add_argument("action_id", help="Action ID to cancel")
    p_action_cancel.add_argument("--reason", help="Optional cancellation reason")
    p_action_cancel.set_defaults(func=cmd_action_cancel)

    p_action_artifacts = action_sub.add_parser("artifacts", help="List persisted artifacts for an action")
    p_action_artifacts.add_argument("action_id", help="Action ID")
    p_action_artifacts.set_defaults(func=cmd_action_artifacts)

    p_action_health = action_sub.add_parser("health", help="Summarize stuck, uncertain, due retry, and overdue scheduled actions")
    p_action_health.add_argument("--stale-minutes", type=int, default=None, help="Minutes before an executing action is treated as stuck")
    p_action_health.set_defaults(func=cmd_action_health)

    p_workflow = sub.add_parser("workflow", help="Manage local searches, templates, contacts, and inbox state")
    workflow_sub = p_workflow.add_subparsers(dest="workflow_command", required=True)

    p_workflow_search = workflow_sub.add_parser("search", help="Manage saved searches")
    workflow_search_sub = p_workflow_search.add_subparsers(dest="workflow_search_command", required=True)
    p_workflow_search_save = workflow_search_sub.add_parser("save", help="Save or update a search")
    p_workflow_search_save.add_argument("--name", required=True, help="Saved search name")
    p_workflow_search_save.add_argument("--kind", choices=["people", "companies", "posts"], required=True, help="Search kind")
    p_workflow_search_save.add_argument("--query", required=True, help="Search query")
    p_workflow_search_save.add_argument("--limit", type=int, default=5, help="Default result limit")
    p_workflow_search_save.add_argument("--enrich", action="store_true", help="Enable enrichment when running the saved search")
    p_workflow_search_save.set_defaults(func=cmd_workflow_search_save)

    p_workflow_search_list = workflow_search_sub.add_parser("list", help="List saved searches")
    p_workflow_search_list.set_defaults(func=cmd_workflow_search_list)

    p_workflow_search_run = workflow_search_sub.add_parser("run", help="Run a saved search")
    p_workflow_search_run.add_argument("name", help="Saved search name")
    p_workflow_search_run.add_argument("--ingest-discovery", action="store_true", help="Also ingest the results into the discovery queue")
    p_workflow_search_run.add_argument("--save-contacts", action="store_true", help="Also sync people results into the local contact store")
    p_workflow_search_run.set_defaults(func=cmd_workflow_search_run)

    p_workflow_search_delete = workflow_search_sub.add_parser("delete", help="Delete a saved search")
    p_workflow_search_delete.add_argument("name", help="Saved search name")
    p_workflow_search_delete.set_defaults(func=cmd_workflow_search_delete)

    p_workflow_template = workflow_sub.add_parser("template", help="Manage reusable content templates")
    workflow_template_sub = p_workflow_template.add_subparsers(dest="workflow_template_command", required=True)
    p_workflow_template_save = workflow_template_sub.add_parser("save", help="Save or update a template")
    p_workflow_template_save.add_argument("--name", required=True, help="Template name")
    p_workflow_template_save.add_argument("--kind", choices=["dm", "post", "generic"], required=True, help="Template kind")
    p_workflow_template_save.add_argument("--body", required=True, help="Template body")
    p_workflow_template_save.set_defaults(func=cmd_workflow_template_save)

    p_workflow_template_list = workflow_template_sub.add_parser("list", help="List templates")
    p_workflow_template_list.add_argument("--kind", choices=["dm", "post", "generic"], help="Filter by template kind")
    p_workflow_template_list.set_defaults(func=cmd_workflow_template_list)

    p_workflow_template_show = workflow_template_sub.add_parser("show", help="Show a template")
    p_workflow_template_show.add_argument("name", help="Template name")
    p_workflow_template_show.set_defaults(func=cmd_workflow_template_show)

    p_workflow_template_render = workflow_template_sub.add_parser("render", help="Render a template with KEY=VALUE variables")
    p_workflow_template_render.add_argument("name", help="Template name")
    p_workflow_template_render.add_argument("--var", action="append", default=[], help="Template variable as KEY=VALUE")
    p_workflow_template_render.set_defaults(func=cmd_workflow_template_render)

    p_workflow_template_delete = workflow_template_sub.add_parser("delete", help="Delete a template")
    p_workflow_template_delete.add_argument("name", help="Template name")
    p_workflow_template_delete.set_defaults(func=cmd_workflow_template_delete)

    p_workflow_contact = workflow_sub.add_parser("contact", help="Manage local contact and lead records")
    workflow_contact_sub = p_workflow_contact.add_subparsers(dest="workflow_contact_command", required=True)
    p_workflow_contact_upsert = workflow_contact_sub.add_parser("upsert", help="Create or update a contact")
    p_workflow_contact_upsert.add_argument("--profile", required=True, help="Profile slug or local contact key")
    p_workflow_contact_upsert.add_argument("--name", required=True, help="Display name")
    p_workflow_contact_upsert.add_argument("--stage", choices=["new", "active", "qualified", "won", "archived"], default="new", help="Lead stage")
    p_workflow_contact_upsert.add_argument("--tags", help="Comma-separated tags")
    p_workflow_contact_upsert.add_argument("--notes", help="Freeform notes")
    p_workflow_contact_upsert.set_defaults(func=cmd_workflow_contact_upsert)

    p_workflow_contact_list = workflow_contact_sub.add_parser("list", help="List contacts")
    p_workflow_contact_list.add_argument("--stage", choices=["new", "active", "qualified", "won", "archived"], help="Filter by stage")
    p_workflow_contact_list.add_argument("--tag", help="Filter by tag")
    p_workflow_contact_list.set_defaults(func=cmd_workflow_contact_list)

    p_workflow_contact_show = workflow_contact_sub.add_parser("show", help="Show a contact")
    p_workflow_contact_show.add_argument("profile", help="Profile slug or local contact key")
    p_workflow_contact_show.set_defaults(func=cmd_workflow_contact_show)

    p_workflow_contact_delete = workflow_contact_sub.add_parser("delete", help="Delete a contact")
    p_workflow_contact_delete.add_argument("profile", help="Profile slug or local contact key")
    p_workflow_contact_delete.set_defaults(func=cmd_workflow_contact_delete)

    p_workflow_contact_export = workflow_contact_sub.add_parser("export", help="Export contacts to CSV")
    p_workflow_contact_export.add_argument("--output", required=True, help="CSV output path")
    p_workflow_contact_export.set_defaults(func=cmd_workflow_contact_export)

    p_workflow_contact_import = workflow_contact_sub.add_parser("import", help="Import contacts from CSV")
    p_workflow_contact_import.add_argument("--input", required=True, help="CSV input path")
    p_workflow_contact_import.set_defaults(func=cmd_workflow_contact_import)

    p_workflow_contact_sync = workflow_contact_sub.add_parser("sync-discovery", help="Sync top discovery prospects into the contact store")
    p_workflow_contact_sync.add_argument("--limit", type=int, default=25, help="Maximum discovery prospects to sync")
    p_workflow_contact_sync.add_argument("--state", choices=["new", "watch", "ready", "contacted", "waiting", "engaged", "won", "cold", "do_not_contact"], help="Optional discovery state filter")
    p_workflow_contact_sync.add_argument("--min-score", type=float, help="Optional minimum discovery score")
    p_workflow_contact_sync.set_defaults(func=cmd_workflow_contact_sync_discovery)

    p_workflow_inbox = workflow_sub.add_parser("inbox", help="Manage local inbox triage state")
    workflow_inbox_sub = p_workflow_inbox.add_subparsers(dest="workflow_inbox_command", required=True)

    p_workflow_inbox_upsert = workflow_inbox_sub.add_parser("upsert", help="Create or update inbox triage state")
    p_workflow_inbox_upsert.add_argument("--conversation", required=True, help="Conversation URN")
    p_workflow_inbox_upsert.add_argument("--state", choices=["new", "follow_up", "waiting", "closed"], default="new", help="Inbox state")
    p_workflow_inbox_upsert.add_argument("--priority", choices=["low", "medium", "high"], default="medium", help="Follow-up priority")
    p_workflow_inbox_upsert.add_argument("--notes", help="Freeform triage notes")
    p_workflow_inbox_upsert.set_defaults(func=cmd_workflow_inbox_upsert)

    p_workflow_inbox_list = workflow_inbox_sub.add_parser("list", help="List inbox triage items")
    p_workflow_inbox_list.add_argument("--state", choices=["new", "follow_up", "waiting", "closed"], help="Filter by inbox state")
    p_workflow_inbox_list.set_defaults(func=cmd_workflow_inbox_list)

    p_workflow_inbox_show = workflow_inbox_sub.add_parser("show", help="Show an inbox triage item")
    p_workflow_inbox_show.add_argument("conversation", help="Conversation URN")
    p_workflow_inbox_show.set_defaults(func=cmd_workflow_inbox_show)

    p_workflow_inbox_delete = workflow_inbox_sub.add_parser("delete", help="Delete an inbox triage item")
    p_workflow_inbox_delete.add_argument("conversation", help="Conversation URN")
    p_workflow_inbox_delete.set_defaults(func=cmd_workflow_inbox_delete)

    p_discover = sub.add_parser("discover", help="Build and inspect the unified prospect discovery queue")
    discover_sub = p_discover.add_subparsers(dest="discover_command", required=True)

    p_discover_ingest_search = discover_sub.add_parser("ingest-search", help="Ingest people from a query or saved search")
    p_discover_ingest_search.add_argument("--kind", choices=["people", "companies", "posts"], default="people", help="Discovery kind")
    p_discover_ingest_search.add_argument("--query", help="Search query")
    p_discover_ingest_search.add_argument("--saved", help="Saved search name from workflow search save")
    p_discover_ingest_search.add_argument("--limit", type=int, default=10, help="Maximum search results to ingest")
    p_discover_ingest_search.add_argument("--enrich", action="store_true", help="Enrich search results before ingesting")
    p_discover_ingest_search.set_defaults(func=cmd_discover_ingest_search)

    p_discover_ingest_inbox = discover_sub.add_parser("ingest-inbox", help="Ingest recent inbox participants into the queue")
    p_discover_ingest_inbox.add_argument("--limit", type=int, default=10, help="Maximum conversations to inspect")
    p_discover_ingest_inbox.set_defaults(func=cmd_discover_ingest_inbox)

    p_discover_ingest_engagement = discover_sub.add_parser("ingest-engagement", help="Ingest public commenters and engagement from recent public posts")
    p_discover_ingest_engagement.add_argument("--target", required=True, help="Profile slug, profile URL, or actor name to inspect")
    p_discover_ingest_engagement.add_argument("--limit", type=int, default=5, help="Maximum public post URLs to inspect")
    p_discover_ingest_engagement.set_defaults(func=cmd_discover_ingest_engagement)

    p_discover_ingest_profile_views = discover_sub.add_parser("ingest-profile-views", help="Ingest profile-view telemetry from the authenticated analytics API")
    p_discover_ingest_profile_views.add_argument("--html-fallback", action="store_true", help="Fallback to the authenticated page bootstrap when the API withholds viewer identities")
    p_discover_ingest_profile_views.set_defaults(func=cmd_discover_ingest_profile_views)

    p_discover_signal = discover_sub.add_parser("signal", help="Add engagement signals to a prospect")
    discover_signal_sub = p_discover_signal.add_subparsers(dest="discover_signal_command", required=True)
    p_discover_signal_add = discover_signal_sub.add_parser("add", help="Attach a manual or public engagement signal")
    p_discover_signal_add.add_argument("--profile", required=True, help="Prospect key or public identifier")
    p_discover_signal_add.add_argument(
        "--type",
        choices=["replied_dm", "inbound_dm", "active_thread", "accepted", "manual_positive", "commented", "profile_view", "followed", "liked", "reposted", "outreach_sent", "follow_up_sent", "connection_requested"],
        required=True,
        help="Signal type",
    )
    p_discover_signal_add.add_argument("--source", choices=["manual", "public", "inbox", "workflow"], required=True, help="Signal source")
    p_discover_signal_add.add_argument("--notes", help="Optional signal notes")
    p_discover_signal_add.set_defaults(func=cmd_discover_signal_add)

    p_discover_state = discover_sub.add_parser("state", help="Manage queue state for prospects")
    discover_state_sub = p_discover_state.add_subparsers(dest="discover_state_command", required=True)
    p_discover_state_set = discover_state_sub.add_parser("set", help="Update the queue state for a prospect")
    p_discover_state_set.add_argument("profile", help="Prospect key or public identifier")
    p_discover_state_set.add_argument("--state", choices=["new", "watch", "ready", "contacted", "waiting", "engaged", "won", "cold", "do_not_contact"], required=True, help="Queue state")
    p_discover_state_set.set_defaults(func=cmd_discover_state_set)

    p_discover_queue = discover_sub.add_parser("queue", help="List ranked prospects from the discovery queue")
    p_discover_queue.add_argument("--state", choices=["new", "watch", "ready", "contacted", "waiting", "engaged", "won", "cold", "do_not_contact"], help="Filter by queue state")
    p_discover_queue.add_argument("--limit", type=int, default=20, help="Maximum ranked prospects to show")
    p_discover_queue.add_argument("--why", action="store_true", help="Show score breakdown fields")
    p_discover_queue.set_defaults(func=cmd_discover_queue)

    p_discover_show = discover_sub.add_parser("show", help="Show a prospect with sources and signals")
    p_discover_show.add_argument("profile", help="Prospect key or public identifier")
    p_discover_show.set_defaults(func=cmd_discover_show)

    p_discover_stats = discover_sub.add_parser("stats", help="Show queue source/signal summary stats")
    p_discover_stats.set_defaults(func=cmd_discover_stats)

    return parser


def main() -> None:
    load_env_file()
    parser = build_parser()
    args = parser.parse_args()
    global _BRIEF_MODE, _OUTPUT_MODE
    if args.brief:
        _BRIEF_MODE = True
    _OUTPUT_MODE = args.output_mode
    try:
        args.func(args)
    except CliError as exc:
        print(exc.message, file=sys.stderr)
        raise SystemExit(exc.code)
