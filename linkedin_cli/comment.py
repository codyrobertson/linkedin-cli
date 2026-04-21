"""Persisted public comment queue and reply drafting."""

from __future__ import annotations

import hashlib
import json
import re
from typing import Any

from bs4 import BeautifulSoup
from requests import Session

from linkedin_cli import discovery, lead
from linkedin_cli.config import DEFAULT_TIMEOUT
from linkedin_cli.session import csrf_token_from_session, request
from linkedin_cli.voyager import parse_bootstrap_payloads
from linkedin_cli.write import store


VALID_COMMENT_STATES = {"queued", "drafted", "replied", "ignored"}
COMMENT_SIGNAL_QUERY_ID = "inSessionRelevanceVoyagerFeedDashClientSignal.c1c9c08097afa4e02954945e9df54091"
COMMENT_PEM_METADATA = "Voyager - Feed - Comments=create-a-comment"


def init_comment_db() -> None:
    discovery.init_discovery_db()
    lead.init_lead_db()
    conn = store._connect()
    try:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS comment_queue (
                comment_id TEXT PRIMARY KEY,
                post_url TEXT NOT NULL,
                author_profile_key TEXT NOT NULL,
                author_name TEXT NOT NULL,
                body TEXT NOT NULL,
                state TEXT NOT NULL DEFAULT 'queued',
                priority REAL NOT NULL DEFAULT 0,
                draft_reply TEXT,
                metadata_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_comment_queue_post
                ON comment_queue(post_url, updated_at DESC);
            """
        )
        conn.commit()
    finally:
        conn.close()


def _comment_id(post_url: str, author_profile_key: str, body: str) -> str:
    digest = hashlib.sha256(f"{post_url}|{author_profile_key}|{body}".encode("utf-8")).hexdigest()[:16]
    return f"cmt_{digest}"


def extract_post_comments(post_url: str, html: str) -> list[dict[str, Any]]:
    comments: list[dict[str, Any]] = []
    seen: set[str] = set()
    for payload in parse_bootstrap_payloads(html):
        body = payload.get("body") or {}
        for item in body.get("included") or []:
            if not isinstance(item, dict) or item.get("$type") != "com.linkedin.voyager.dash.social.Comment":
                continue
            commentary = item.get("commentary") or {}
            commenter = item.get("commenter") or {}
            name = str(((commenter.get("title") or {}).get("text")) or "").strip()
            url = commenter.get("navigationUrl")
            body_text = " ".join(str(commentary.get("text") or "").split()).strip()
            comment_urn = str(item.get("urn") or "").strip() or None
            if not name or not body_text:
                continue
            record = discovery._commenter_record(name, url, "person")  # type: ignore[attr-defined]
            comment_id = _comment_id(post_url, record["profile_key"], body_text)
            if comment_id in seen:
                continue
            seen.add(comment_id)
            comments.append(
                {
                    "comment_id": comment_id,
                    "post_url": post_url,
                    "author_profile_key": record["profile_key"],
                    "author_name": record["display_name"],
                    "author_public_identifier": record["public_identifier"],
                    "author_profile_url": record["profile_url"],
                    "body": body_text,
                    "comment_urn": comment_urn,
                }
            )
    objects = discovery._parse_json_ld_objects(html)  # type: ignore[attr-defined]
    post_object = next(
        (item for item in objects if isinstance(item, dict) and item.get("@type") == "SocialMediaPosting"),
        {},
    )
    for comment in post_object.get("comment") or []:
        if not isinstance(comment, dict):
            continue
        author = comment.get("author") or {}
        if not isinstance(author, dict):
            continue
        name = str(author.get("name") or "").strip()
        url = author.get("url")
        body = " ".join(str(comment.get("text") or "").split()).strip()
        if not name or not body:
            continue
        record = discovery._commenter_record(name, url, "company" if author.get("@type") == "Organization" else "person")  # type: ignore[attr-defined]
        comment_id = _comment_id(post_url, record["profile_key"], body)
        if comment_id in seen:
            continue
        seen.add(comment_id)
        comments.append(
            {
                "comment_id": comment_id,
                "post_url": post_url,
                "author_profile_key": record["profile_key"],
                "author_name": record["display_name"],
                "author_public_identifier": record["public_identifier"],
                "author_profile_url": record["profile_url"],
                "body": body,
                "comment_urn": None,
            }
        )
    soup = BeautifulSoup(html, "html.parser")
    for node in soup.select("[data-tracking-control-name*=public_post_comment_text], [data-control-name*=comment_text]"):
        body = " ".join(node.get_text(" ", strip=True).split())
        if not body:
            continue
    return comments


def queue_post_comments(post_url: str, html: str) -> dict[str, Any]:
    init_comment_db()
    comments = extract_post_comments(post_url, html)
    for item in comments:
        discovery.upsert_prospect(
            profile_key=item["author_profile_key"],
            display_name=item["author_name"],
            public_identifier=item.get("author_public_identifier"),
            profile_url=item.get("author_profile_url"),
            entity_type="person",
        )
    now = store._now_iso()
    conn = store._connect()
    queued_count = 0
    try:
        for item in comments:
            existing_row = conn.execute(
                "SELECT metadata_json FROM comment_queue WHERE comment_id = ?",
                (item["comment_id"],),
            ).fetchone()
            metadata = json.loads((existing_row["metadata_json"] if existing_row else "{}") or "{}")
            metadata.update(
                {k: item[k] for k in ("author_public_identifier", "author_profile_url", "comment_urn") if item.get(k)}
            )
            conn.execute(
                """
                INSERT INTO comment_queue
                (comment_id, post_url, author_profile_key, author_name, body, state, priority, draft_reply, metadata_json, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, 'queued', 0, NULL, ?, ?, ?)
                ON CONFLICT(comment_id) DO UPDATE SET
                    author_name = excluded.author_name,
                    body = excluded.body,
                    metadata_json = excluded.metadata_json,
                    updated_at = excluded.updated_at
                """,
                (
                    item["comment_id"],
                    post_url,
                    item["author_profile_key"],
                    item["author_name"],
                    item["body"],
                    json.dumps(metadata, ensure_ascii=False, sort_keys=True),
                    now,
                    now,
                ),
            )
            queued_count += 1
        conn.commit()
    finally:
        conn.close()
    return {"post_url": post_url, "queued_count": queued_count, "comments": comments}


def list_comment_queue(post_url: str | None = None, state: str | None = None) -> list[dict[str, Any]]:
    init_comment_db()
    conn = store._connect()
    try:
        sql = "SELECT * FROM comment_queue"
        where: list[str] = []
        params: list[Any] = []
        if post_url:
            where.append("post_url = ?")
            params.append(post_url)
        if state:
            where.append("state = ?")
            params.append(state)
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY priority DESC, updated_at DESC"
        rows = conn.execute(sql, tuple(params)).fetchall()
        return [_row_to_dict(row) for row in rows]
    finally:
        conn.close()


def draft_comment_reply(
    *,
    post_url: str,
    author_profile_key: str | None = None,
    comment_id: str | None = None,
    tone: str = "expert",
) -> dict[str, Any]:
    init_comment_db()
    if not author_profile_key and not comment_id:
        raise ValueError("Provide author_profile_key or comment_id")
    conn = store._connect()
    try:
        if comment_id:
            row = conn.execute("SELECT * FROM comment_queue WHERE comment_id = ?", (comment_id,)).fetchone()
        else:
            row = conn.execute(
                "SELECT * FROM comment_queue WHERE post_url = ? AND author_profile_key = ? ORDER BY updated_at DESC LIMIT 1",
                (post_url, author_profile_key),
            ).fetchone()
        if row is None:
            raise ValueError("Comment queue item not found")
        item = _row_to_dict(row)
        lead_payload = lead.get_lead(item["author_profile_key"]) or {}
        body = str(item["body"] or "")
        overlap = ", ".join((lead_payload.get("lead_features") or {}).get("topic_overlap_terms") or [])
        if tone == "expert":
            reply = f"Thanks {item['author_name']} — the useful version is mapping one workflow, instrumenting it, then letting the agent own that path. {('That fits ' + overlap + ' especially well.') if overlap else ''}".strip()
        elif tone == "warm":
            reply = f"Appreciate that, {item['author_name']}. I’ve found the win comes from starting with one real workflow instead of a generic demo."
        elif tone == "contrarian":
            reply = f"Most teams overcomplicate this, {item['author_name']}. The boring workflow fix usually beats the flashy agent demo."
        else:
            reply = f"Thanks {item['author_name']}. The core issue is workflow ownership, not just model quality."
        conn.execute(
            "UPDATE comment_queue SET draft_reply = ?, state = 'drafted', updated_at = ? WHERE comment_id = ?",
            (reply, store._now_iso(), item["comment_id"]),
        )
        conn.commit()
        item["draft_reply"] = reply
        item["state"] = "drafted"
        item["tone"] = tone
        item["lead"] = lead_payload
        return item
    finally:
        conn.close()


def publish_post_comment(
    *,
    session: Session,
    post_url: str,
    text: str | None = None,
    comment_id: str | None = None,
    author_profile_key: str | None = None,
    execute: bool = False,
    account_id: str | None = None,
) -> dict[str, Any]:
    init_comment_db()
    comment_item: dict[str, Any] | None = None
    if comment_id or author_profile_key:
        conn = store._connect()
        try:
            if comment_id:
                row = conn.execute("SELECT * FROM comment_queue WHERE comment_id = ?", (comment_id,)).fetchone()
            else:
                row = conn.execute(
                    "SELECT * FROM comment_queue WHERE post_url = ? AND author_profile_key = ? ORDER BY updated_at DESC LIMIT 1",
                    (post_url, author_profile_key),
                ).fetchone()
            if row is None:
                raise ValueError("Comment queue item not found")
            comment_item = _row_to_dict(row)
        finally:
            conn.close()
    reply_text = " ".join((text or comment_item.get("draft_reply") or "").split()) if comment_item else " ".join((text or "").split())
    if not reply_text:
        raise ValueError("Comment text is required or the queued item must have a draft_reply")

    activity_match = re.search(r"activity-(\d+)", post_url)
    activity_urn = f"urn:li:activity:{activity_match.group(1)}" if activity_match else None
    target_thread_urn = None
    delivery_mode = "post_thread_comment"
    event_type = "comment_posted"
    signal = True
    if comment_item and comment_item.get("metadata", {}).get("comment_urn"):
        target_thread_urn = str(comment_item["metadata"]["comment_urn"])
        delivery_mode = "comment_reply"
        event_type = "comment_reply_posted"
        signal = False
    if target_thread_urn is None:
        html = request(session, "GET", post_url).text
        if comment_item is not None:
            queue_post_comments(post_url, html)
            comment_item = _load_comment_item(
                post_url=post_url,
                comment_id=comment_id,
                author_profile_key=author_profile_key,
            )
            if comment_item and comment_item.get("metadata", {}).get("comment_urn"):
                target_thread_urn = str(comment_item["metadata"]["comment_urn"])
                delivery_mode = "comment_reply"
                event_type = "comment_reply_posted"
                signal = False
                reply_text = " ".join((text or comment_item.get("draft_reply") or "").split())
        if target_thread_urn is None:
            context = extract_comment_context(post_url, html)
            target_thread_urn = context["thread_urn"]
            activity_urn = context.get("activity_urn") or activity_urn
    from linkedin_cli.write.executor import execute_action
    from linkedin_cli.write.plans import build_comment_plan

    effective_account_id = account_id or "me"
    plan = build_comment_plan(
        account_id=effective_account_id,
        post_url=post_url,
        thread_urn=target_thread_urn,
        text=reply_text,
        delivery_mode=delivery_mode,
        activity_urn=activity_urn,
        source_comment_id=comment_item["comment_id"] if comment_item else None,
    )
    headers = _comment_headers(
        session,
        referer=post_url,
        reply=(delivery_mode == "comment_reply"),
    )
    live_request = dict(plan["live_request"])
    live_request["headers"] = headers
    action_id = f"act_{hashlib.sha256(plan['idempotency_key'].encode('utf-8')).hexdigest()[:12]}"
    exec_result = execute_action(
        session=session,
        action_id=action_id,
        plan=plan,
        account_id=effective_account_id,
        dry_run=not execute,
    )
    if not execute:
        exec_result["request"] = live_request
        exec_result["source_comment"] = comment_item
        return exec_result

    live_result = dict(exec_result.get("result") or {})
    result: dict[str, Any] = dict(exec_result)
    live_request = {
        **live_request,
        "headers": headers,
    }
    result["http_status"] = live_result.get("http_status")
    result["request"] = live_request
    result["source_comment"] = comment_item
    result["remote_ref"] = live_result.get("remote_ref")
    result["signal"] = None
    if "response_data" in live_result:
        result["response_data"] = live_result["response_data"]
    if "response_text" in live_result:
        result["response_text"] = live_result["response_text"]
    if live_result.get("error"):
        result["error"] = live_result["error"]
    if exec_result.get("status") != "succeeded":
        return result

    if signal and activity_urn:
        result["signal"] = _emit_comment_signal(session, activity_urn=activity_urn, referer=post_url)
    if comment_item:
        _mark_comment_replied(comment_item["comment_id"], result["remote_ref"], delivery_mode=delivery_mode)
    dedupe_key = f"comment_posted:{post_url}:{hashlib.sha256(reply_text.encode('utf-8')).hexdigest()[:12]}"
    store.append_telemetry_event(
        entity_kind="post",
        entity_key=post_url,
        event_type=event_type,
        dedupe_key=dedupe_key,
        payload={
            "thread_urn": target_thread_urn,
            "activity_urn": activity_urn,
            "remote_ref": result["remote_ref"],
            "source_comment_id": comment_item["comment_id"] if comment_item else None,
            "delivery_mode": delivery_mode,
        },
        source="comment.execute",
    )
    try:
        from linkedin_cli import traces

        traces.attribute_telemetry_event(
            entity_kind="post",
            entity_key=post_url,
            event_type=event_type,
            event_time=store._now_iso(),
            payload={
                "thread_urn": target_thread_urn,
                "activity_urn": activity_urn,
                "remote_ref": result["remote_ref"],
                "source_comment_id": comment_item["comment_id"] if comment_item else None,
                "delivery_mode": delivery_mode,
            },
            source="comment.execute",
            source_event_id=dedupe_key,
        )
    except Exception:
        pass
    return result


def _load_comment_item(
    *,
    post_url: str,
    comment_id: str | None,
    author_profile_key: str | None,
) -> dict[str, Any] | None:
    conn = store._connect()
    try:
        if comment_id:
            row = conn.execute("SELECT * FROM comment_queue WHERE comment_id = ?", (comment_id,)).fetchone()
        elif author_profile_key:
            row = conn.execute(
                "SELECT * FROM comment_queue WHERE post_url = ? AND author_profile_key = ? ORDER BY updated_at DESC LIMIT 1",
                (post_url, author_profile_key),
            ).fetchone()
        else:
            row = None
        return _row_to_dict(row) if row is not None else None
    finally:
        conn.close()


def extract_comment_context(post_url: str, html: str) -> dict[str, Any]:
    thread_urn = None
    can_post = None
    for payload in parse_bootstrap_payloads(html):
        body = payload.get("body") or {}
        for item in body.get("included") or []:
            if not isinstance(item, dict):
                continue
            item_type = item.get("$type") or ""
            if item_type == "com.linkedin.voyager.dash.social.SocialDetail":
                entity_urn = str(item.get("entityUrn") or "")
                match = re.search(r"\((urn:li:ugcPost:\d+),", entity_urn)
                if match:
                    thread_urn = match.group(1)
            elif item_type == "com.linkedin.voyager.dash.social.SocialPermissions":
                can_post = bool(item.get("canPostComments"))
    if not thread_urn:
        raise ValueError(f"Could not determine the post thread URN for {post_url}")
    activity_match = re.search(r"activity-(\d+)", post_url)
    activity_urn = f"urn:li:activity:{activity_match.group(1)}" if activity_match else None
    return {
        "thread_urn": thread_urn,
        "activity_urn": activity_urn,
        "can_post_comments": can_post,
    }


def _comment_headers(session: Session, *, referer: str, reply: bool = False) -> dict[str, str]:
    csrf = csrf_token_from_session(session)
    headers = {
        "Accept": "application/vnd.linkedin.normalized+json+2.1",
        "Content-Type": "application/json; charset=UTF-8",
        "X-RestLi-Protocol-Version": "2.0.0",
        "Referer": referer.rstrip("/") + "/",
        "Origin": "https://www.linkedin.com",
        "X-Li-Lang": "en_US",
        "X-Li-DeCo-Include-Micro-Schema": "true",
        "X-Li-Pem-Metadata": "Voyager - Feed - Comments=create-a-comment-reply" if reply else COMMENT_PEM_METADATA,
    }
    if csrf:
        headers["csrf-token"] = csrf
    return headers


def _emit_comment_signal(session: Session, *, activity_urn: str, referer: str) -> dict[str, Any] | None:
    headers = _comment_headers(session, referer=referer)
    headers.pop("X-Li-DeCo-Include-Micro-Schema", None)
    signal_body = {
        "variables": {
            "backendUpdateUrn": activity_urn,
            "actionType": "submitComment",
        },
        "queryId": COMMENT_SIGNAL_QUERY_ID,
        "includeWebMetadata": True,
    }
    response = session.post(
        f"https://www.linkedin.com/voyager/api/graphql?action=execute&queryId={COMMENT_SIGNAL_QUERY_ID}",
        json=signal_body,
        headers=headers,
        timeout=DEFAULT_TIMEOUT,
    )
    payload: dict[str, Any] = {"http_status": response.status_code}
    if response.status_code < 400:
        try:
            payload["response_data"] = response.json()
        except Exception:
            payload["response_text"] = response.text[:500]
    else:
        payload["error"] = response.text[:500]
    return payload


def _mark_comment_replied(comment_id: str, remote_ref: str | None, *, delivery_mode: str) -> None:
    conn = store._connect()
    try:
        row = conn.execute("SELECT metadata_json FROM comment_queue WHERE comment_id = ?", (comment_id,)).fetchone()
        metadata = json.loads((row["metadata_json"] if row else "{}") or "{}")
        metadata.update(
            {
                "remote_ref": remote_ref,
                "delivery_mode": delivery_mode,
                "replied_at": store._now_iso(),
            }
        )
        conn.execute(
            "UPDATE comment_queue SET state = 'replied', metadata_json = ?, updated_at = ? WHERE comment_id = ?",
            (json.dumps(metadata, ensure_ascii=False, sort_keys=True), store._now_iso(), comment_id),
        )
        conn.commit()
    finally:
        conn.close()


def _row_to_dict(row: Any) -> dict[str, Any]:
    data = dict(row)
    data["metadata"] = json.loads(data.pop("metadata_json") or "{}")
    return data
