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
import sys
import uuid as _uuid
import warnings
from pathlib import Path
from typing import Any

warnings.filterwarnings("ignore", message="urllib3 v2 only supports OpenSSL")

from bs4 import BeautifulSoup
from requests import Session

from linkedin_cli.config import MOBILE_USER_AGENT
from linkedin_cli.session import (
    CliError,
    SESSION_FILE,
    auth_summary,
    build_session,
    fail,
    getenv_required,
    linkedin_login,
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


def pretty_print(data: Any) -> None:
    if _BRIEF_MODE:
        # Compact: strip verbose keys, one-line JSON
        if isinstance(data, dict):
            data = _strip_verbose(data)
        print(json.dumps(data, ensure_ascii=False, separators=(",", ":")))
    else:
        print(json.dumps(data, indent=2, ensure_ascii=False))


def _strip_verbose(d: dict) -> dict:
    """Remove keys that waste tokens in agent context."""
    skip_keys = {
        "$type", "$recipeTypes", "$anti_abuse_metadata", "trackingId",
        "versionTag", "entityUrn", "dashEntityUrn", "objectUrn",
        "plan_json", "risk_flags", "next_attempt_at", "desired_fingerprint",
    }
    out = {}
    for k, v in d.items():
        if k in skip_keys:
            continue
        if isinstance(v, dict):
            v = _strip_verbose(v)
        elif isinstance(v, list):
            v = [_strip_verbose(i) if isinstance(i, dict) else i for i in v]
        out[k] = v
    return out


def parse_key_values(items: list[str]) -> dict[str, str]:
    out: dict[str, str] = {}
    for item in items:
        if "=" not in item:
            fail(f"Expected KEY=VALUE, got: {item}")
        key, value = item.split("=", 1)
        out[key] = value
    return out


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


# ---------------------------------------------------------------------------
#  Read-only command handlers
# ---------------------------------------------------------------------------

def cmd_login(args: argparse.Namespace) -> None:
    load_env_file()
    username = args.username or getenv_required("LINKEDIN_USERNAME")
    password = args.password or getenv_required("LINKEDIN_PASSWORD")
    session = build_session(args.user_agent)
    result = linkedin_login(session, username, password)
    if result["logged_in"]:
        meta = {
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
            "The partial browserless login did not complete. Try again later or use a verified session cookie import workflow."
        )
    fail(result["error"] or "LinkedIn login failed")


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
    kind = args.kind
    query = clean_text(args.query) or ""
    if not query:
        fail("Search query is required")
    limit = args.limit
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
                if slug and args.enrich:
                    item["summary"] = fetch_profile_summary(session, slug)
            elif kind == "companies":
                slug = extract_company_slug_from_url(result["url"])
                item["slug"] = slug
                if slug and args.enrich:
                    item["summary"] = fetch_company_summary(session, slug)
        except CliError as exc:
            item["enrichment_error"] = exc.message
        enriched.append(item)
    pretty_print(
        {
            "source": "duckduckgo-html",
            "kind": kind,
            "query": query,
            "results": enriched,
        }
    )


def cmd_activity(args: argparse.Namespace) -> None:
    target = clean_text(args.target) or ""
    if not target:
        fail("Activity target is required")
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
        results = filter_linkedin_search_results(ddg_html_search(query, limit=max(args.limit * 2, 10)), "posts")
        for result in results:
            url = result.get("url", "")
            if not url or url in seen_urls:
                continue
            seen_urls.add(url)
            enriched = dict(result)
            enriched["matched_query"] = query
            collected.append(enriched)
            if len(collected) >= args.limit:
                break
        if len(collected) >= args.limit:
            break
    pretty_print(
        {
            "source": "duckduckgo-html",
            "target": target,
            "profile_context": profile_context,
            "results": collected,
        }
    )


# ---------------------------------------------------------------------------
#  Write-system command handlers
# ---------------------------------------------------------------------------

def cmd_post_publish(args: argparse.Namespace) -> None:
    """Plan or execute a text or image post publish."""
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
    pretty_print(result)


def cmd_follow(args: argparse.Namespace) -> None:
    """Plan or execute a follow action."""
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
    pretty_print(result)


def cmd_dm_list(args: argparse.Namespace) -> None:
    """List recent DM conversations."""
    session, _ = load_session(required=True)
    assert session is not None
    response = voyager_get(session, "/voyager/api/messaging/conversations", params={"keyVersion": "LEGACY_INBOX"})
    data = parse_json_response(response)
    convos = []
    included = data.get("included") or []
    # Build a lookup of included entities
    entity_map = {}
    for item in included:
        if isinstance(item, dict):
            urn = item.get("entityUrn") or item.get("dashEntityUrn") or ""
            if urn:
                entity_map[urn] = item
    # Extract conversation summaries
    for item in included:
        if not isinstance(item, dict):
            continue
        item_type = item.get("$type") or ""
        if "Conversation" not in item_type:
            continue
        urn = item.get("entityUrn") or ""
        if not urn:
            continue
        last_msg = item.get("lastActivityAt")
        participants = []
        for p in (item.get("*participants") or item.get("participants") or []):
            if isinstance(p, str) and p in entity_map:
                pp = entity_map[p]
                mini = pp.get("*miniProfile") or pp.get("miniProfile")
                if isinstance(mini, str) and mini in entity_map:
                    mp = entity_map[mini]
                    participants.append(f"{mp.get('firstName', '')} {mp.get('lastName', '')}".strip())
                elif isinstance(mini, dict):
                    participants.append(f"{mini.get('firstName', '')} {mini.get('lastName', '')}".strip())
        convos.append({
            "urn": urn,
            "participants": participants or ["Unknown"],
            "last_activity": last_msg,
        })
    convos.sort(key=lambda c: c.get("last_activity") or 0, reverse=True)
    pretty_print({"conversations": convos[:args.limit]})


def cmd_dm_send(args: argparse.Namespace) -> None:
    """Plan or execute sending a DM."""
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
        fail(f"Action not found: {args.action_id}")
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
        fail(f"Action not found: {args.action_id}")
    if action["state"] not in ("failed", "unknown_remote_state"):
        state = action["state"]
        fail(f"Action is in state '{state}' -- only failed or unknown_remote_state actions can be retried")

    plan = action.get("plan")
    if not plan:
        fail("Action has no stored plan -- cannot retry")

    update_state(args.action_id, "planned")

    result = execute_action(
        session=session,
        action_id=args.action_id,
        plan=plan,
        account_id=action["account_id"],
        dry_run=False,
    )
    pretty_print(result)


# ---------------------------------------------------------------------------
#  Parser
# ---------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="linkedin",
        description="Unofficial LinkedIn CLI -- session-based authentication, Voyager API access, and safe write system",
    )
    parser.add_argument("--brief", action="store_true", help="Compact output for agent consumption (fewer tokens)")
    sub = parser.add_subparsers(dest="command", required=True)

    p_login = sub.add_parser("login", help="Log into LinkedIn using web form auth")
    p_login.add_argument("--username", help="LinkedIn username/email (defaults to LINKEDIN_USERNAME)")
    p_login.add_argument("--password", help="LinkedIn password (defaults to LINKEDIN_PASSWORD)")
    p_login.add_argument("--user-agent", help="Override browser user agent")
    p_login.set_defaults(func=cmd_login)

    p_logout = sub.add_parser("logout", help="Delete saved LinkedIn session")
    p_logout.set_defaults(func=cmd_logout)

    p_status = sub.add_parser("status", help="Inspect current LinkedIn session")
    p_status.set_defaults(func=cmd_status)

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

    # --- Write system commands ---

    # post publish
    p_post = sub.add_parser("post", help="Post-related commands")
    post_sub = p_post.add_subparsers(dest="post_command", required=True)
    p_post_publish = post_sub.add_parser("publish", help="Publish a text post")
    p_post_publish.add_argument("--text", help="Post text content")
    p_post_publish.add_argument("--text-file", help="Read post text from file")
    p_post_publish.add_argument("--image", help="Path to image file to include in post")
    p_post_publish.add_argument("--visibility", choices=["anyone", "connections"], default="anyone", help="Post visibility (default: anyone)")
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
    p_action_list.add_argument("--state", choices=["planned", "dry_run", "executing", "succeeded", "failed", "unknown_remote_state", "retry_scheduled", "blocked", "duplicate_skipped"], help="Filter by state")
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

    return parser


def main() -> None:
    load_env_file()
    parser = build_parser()
    args = parser.parse_args()
    global _BRIEF_MODE
    if args.brief:
        _BRIEF_MODE = True
    try:
        args.func(args)
    except CliError as exc:
        print(exc.message, file=sys.stderr)
        raise SystemExit(exc.code)
