"""Post-write verification for LinkedIn actions.

Checks whether a write operation actually landed by re-reading
the relevant data from LinkedIn.
"""

from __future__ import annotations

import re
from typing import Any

from requests import Session

from linkedin_cli.session import request as session_request
from linkedin_cli.voyager import find_json_ld_objects, parse_bootstrap_payloads, parse_json_response, voyager_get
from linkedin_cli.write.store import get_action, update_state, write_artifact


def _normalize_for_compare(text: str) -> str:
    """Normalize text for fuzzy comparison: lowercase, strip, collapse whitespace."""
    text = text.strip().lower()
    text = re.sub(r"\s+", " ", text)
    return text


def reconcile_post(session: Session, action_id: str, plan: dict[str, Any]) -> dict[str, Any]:
    """Fetch recent feed items and check if post text matches.

    Looks at the authenticated user's recent posts via Voyager and
    compares normalized text to find the published post.

    Returns reconciliation result dict.
    """
    desired_text = plan.get("desired", {}).get("text", "")
    normalized_desired = _normalize_for_compare(desired_text)

    if not normalized_desired:
        return {"reconciled": False, "reason": "No desired text in plan"}

    try:
        # Fetch recent feed activity for the authenticated user
        response = voyager_get(
            session,
            "/voyager/api/feed/normalizedUgcPosts",
            params={"count": "10", "start": "0"},
        )
        data = parse_json_response(response)
    except Exception as exc:
        return {"reconciled": False, "reason": f"Failed to fetch feed: {exc}"}

    # Search through included items for matching text
    included = data.get("included") or []
    for item in included:
        if not isinstance(item, dict):
            continue
        # Check for share commentary text
        commentary = (
            item.get("commentary", {}).get("text", "")
            if isinstance(item.get("commentary"), dict)
            else ""
        )
        if not commentary:
            # Try alternative path in the response
            specific = item.get("specificContent", {})
            if isinstance(specific, dict):
                share_content = specific.get("com.linkedin.ugc.ShareContent", {})
                if isinstance(share_content, dict):
                    commentary = share_content.get("shareCommentary", {}).get("text", "")

        if not commentary:
            continue

        normalized_actual = _normalize_for_compare(commentary)
        if normalized_actual == normalized_desired:
            # Found a match
            post_urn = item.get("entityUrn") or item.get("activityUrn") or item.get("urn")
            action = get_action(action_id)
            if action and action.get("state") != "succeeded":
                update_state(action_id, "succeeded", remote_ref=post_urn)
            return {
                "reconciled": True,
                "matched_urn": post_urn,
                "message": "Post text matches a recent feed item",
            }

    return {
        "reconciled": False,
        "reason": "No matching post found in recent feed items",
        "checked_items": len(included),
    }


def reconcile_profile_edit(
    session: Session, action_id: str, plan: dict[str, Any]
) -> dict[str, Any]:
    """Re-fetch profile and check if the edited field matches desired value.

    Returns reconciliation result dict.
    """
    field = plan.get("desired", {}).get("field", "")
    desired_value = plan.get("desired", {}).get("value", "")

    if not field or not desired_value:
        return {"reconciled": False, "reason": "Missing field or value in plan"}

    normalized_desired = _normalize_for_compare(desired_value)

    try:
        response = voyager_get(session, "/voyager/api/me")
        data = parse_json_response(response)
    except Exception as exc:
        return {"reconciled": False, "reason": f"Failed to fetch profile: {exc}"}

    # Navigate the profile data to find the field
    included = data.get("included") or []

    actual_value = None

    # Check in the main profile data
    for item in included:
        if not isinstance(item, dict):
            continue
        item_type = item.get("$type", "")
        if "Profile" not in item_type and "MiniProfile" not in item_type:
            continue

        if field == "headline":
            actual_value = item.get("headline") or item.get("occupation")
        elif field == "about":
            actual_value = item.get("summary")
        elif field == "location":
            actual_value = item.get("locationName") or item.get("geoLocationName")
        elif field == "website":
            # Websites are nested, check if our URL appears
            websites = item.get("websites") or []
            for site in websites:
                if isinstance(site, dict):
                    url = site.get("url", "")
                    if _normalize_for_compare(url) == normalized_desired:
                        actual_value = url
                        break

        if actual_value:
            break

    if actual_value and _normalize_for_compare(actual_value) == normalized_desired:
        action = get_action(action_id)
        if action and action.get("state") != "succeeded":
            update_state(action_id, "succeeded")
        return {
            "reconciled": True,
            "field": field,
            "actual_value": actual_value,
            "message": f"Profile {field} matches desired value",
        }

    return {
        "reconciled": False,
        "field": field,
        "actual_value": actual_value,
        "desired_value": desired_value,
        "reason": f"Profile {field} does not match desired value",
    }


def _iter_comment_texts_from_value(value: Any) -> list[tuple[str, str | None]]:
    matches: list[tuple[str, str | None]] = []
    if isinstance(value, dict):
        value_type = value.get("@type") or value.get("$type") or ""
        text = None
        if "Comment" in str(value_type):
            commentary = value.get("commentary")
            if isinstance(commentary, dict):
                text = commentary.get("text")
            text = text or value.get("text")
        if text:
            remote_ref = value.get("entityUrn") or value.get("urn") or value.get("@id")
            matches.append((str(text), str(remote_ref) if remote_ref else None))
        for child in value.values():
            matches.extend(_iter_comment_texts_from_value(child))
    elif isinstance(value, list):
        for item in value:
            matches.extend(_iter_comment_texts_from_value(item))
    return matches


def reconcile_comment(session: Session, action_id: str, plan: dict[str, Any]) -> dict[str, Any]:
    """Re-fetch a public post page and check whether the planned comment text appears."""
    desired = plan.get("desired") or {}
    post_url = str(desired.get("post_url") or plan.get("live_request", {}).get("post_url") or "")
    desired_text = str(desired.get("text") or plan.get("live_request", {}).get("body", {}).get("commentary", {}).get("text") or "")
    normalized_desired = _normalize_for_compare(desired_text)
    if not post_url:
        return {"reconciled": False, "reason": "No post_url in comment plan"}
    if not normalized_desired:
        return {"reconciled": False, "reason": "No desired comment text in plan"}

    try:
        response = session_request(session, "GET", post_url)
    except Exception as exc:
        return {"reconciled": False, "reason": f"Failed to fetch post page: {exc}"}

    found_texts: list[tuple[str, str | None]] = []
    for obj in find_json_ld_objects(response.text):
        found_texts.extend(_iter_comment_texts_from_value(obj))
    for payload in parse_bootstrap_payloads(response.text):
        found_texts.extend(_iter_comment_texts_from_value((payload.get("body") or {}).get("included") or []))

    for text, remote_ref in found_texts:
        if _normalize_for_compare(text) == normalized_desired:
            action = get_action(action_id)
            if action and action.get("state") != "succeeded":
                update_state(action_id, "succeeded", remote_ref=remote_ref)
            return {
                "reconciled": True,
                "matched_urn": remote_ref,
                "message": "Comment text matches the fetched post page",
            }

    return {
        "reconciled": False,
        "reason": "No matching comment found on fetched post page",
        "checked_items": len(found_texts),
    }


def reconcile_dm(session: Session, action_id: str, plan: dict[str, Any]) -> dict[str, Any]:
    """Fetch recent conversations and check whether the planned DM text appears."""
    desired = plan.get("desired") or {}
    desired_text = str(desired.get("message_text") or plan.get("live_request", {}).get("body", {}).get("message", {}).get("body", {}).get("text") or "")
    normalized_desired = _normalize_for_compare(desired_text)
    conversation_urn = desired.get("conversation_urn")
    if not normalized_desired:
        return {"reconciled": False, "reason": "No desired DM text in plan"}

    try:
        response = voyager_get(
            session,
            "/voyager/api/messaging/conversations",
            params={"createdBefore": "0", "keyVersion": "LEGACY_INBOX"},
        )
        data = parse_json_response(response)
    except Exception as exc:
        return {"reconciled": False, "reason": f"Failed to fetch conversations: {exc}"}

    checked = 0
    for item in data.get("included") or []:
        if not isinstance(item, dict):
            continue
        item_type = item.get("$type") or ""
        if "Message" not in item_type or "Delivery" in item_type:
            continue
        if conversation_urn:
            refs = [
                item.get("*conversation"),
                item.get("conversation"),
                item.get("conversationUrn"),
                item.get("entityUrn"),
            ]
            if not any(conversation_urn in str(ref or "") for ref in refs):
                continue
        body = item.get("body") or {}
        text = body.get("text") if isinstance(body, dict) else str(body)
        if not text:
            continue
        checked += 1
        if _normalize_for_compare(str(text)) == normalized_desired:
            message_urn = item.get("entityUrn") or item.get("dashEntityUrn")
            action = get_action(action_id)
            if action and action.get("state") != "succeeded":
                update_state(action_id, "succeeded", remote_ref=message_urn)
            return {
                "reconciled": True,
                "matched_urn": message_urn,
                "message": "DM text matches a recent conversation message",
            }

    return {
        "reconciled": False,
        "reason": "No matching DM found in recent conversations",
        "checked_items": checked,
    }


def _profile_action_text(session: Session, vanity_name: str) -> str:
    response = session_request(
        session,
        "GET",
        f"https://www.linkedin.com/in/{vanity_name}/",
    )
    text_parts: list[str] = []
    for payload in parse_bootstrap_payloads(response.text):
        text_parts.append(str(payload.get("body") or ""))
    json_ld = find_json_ld_objects(response.text)
    if json_ld:
        text_parts.append(str(json_ld))
    text_parts.append(response.text)
    return _normalize_for_compare(" ".join(text_parts))


def reconcile_connect(session: Session, action_id: str, plan: dict[str, Any]) -> dict[str, Any]:
    desired = plan.get("desired") or {}
    vanity_name = str(desired.get("vanity_name") or "").strip()
    if not vanity_name:
        return {"reconciled": False, "reason": "No vanity_name in connect plan"}
    try:
        action_text = _profile_action_text(session, vanity_name)
    except Exception as exc:
        return {"reconciled": False, "reason": f"Failed to fetch profile page: {exc}"}
    matched = any(marker in action_text for marker in ("pending", "message", "connected", "1st"))
    if matched:
        action = get_action(action_id)
        if action and action.get("state") != "succeeded":
            update_state(action_id, "succeeded", remote_ref=desired.get("target_urn"))
        return {
            "reconciled": True,
            "matched_urn": desired.get("target_urn"),
            "message": "Connection state indicates invite was sent or profile is connected",
        }
    return {
        "reconciled": False,
        "reason": "Profile page did not show pending/connected state",
    }


def reconcile_follow(session: Session, action_id: str, plan: dict[str, Any]) -> dict[str, Any]:
    desired = plan.get("desired") or {}
    vanity_name = str(desired.get("vanity_name") or "").strip()
    if not vanity_name:
        return {"reconciled": False, "reason": "No vanity_name in follow plan"}
    try:
        action_text = _profile_action_text(session, vanity_name)
    except Exception as exc:
        return {"reconciled": False, "reason": f"Failed to fetch profile page: {exc}"}
    matched = any(marker in action_text for marker in ("following", "unfollow"))
    if matched:
        action = get_action(action_id)
        if action and action.get("state") != "succeeded":
            update_state(action_id, "succeeded", remote_ref=desired.get("target_urn"))
        return {
            "reconciled": True,
            "matched_urn": desired.get("target_urn"),
            "message": "Profile page indicates active follow state",
        }
    return {
        "reconciled": False,
        "reason": "Profile page did not show following state",
    }


def _localized_value(value: Any) -> str:
    if isinstance(value, str):
        return value
    if isinstance(value, dict):
        localized = value.get("localized")
        if isinstance(localized, dict):
            for candidate in localized.values():
                if candidate:
                    return str(candidate)
        for key in ("text", "name", "value"):
            if value.get(key):
                return str(value[key])
    return ""


def reconcile_experience(session: Session, action_id: str, plan: dict[str, Any]) -> dict[str, Any]:
    desired = plan.get("desired") or {}
    title = _normalize_for_compare(str(desired.get("title") or ""))
    company = _normalize_for_compare(str(desired.get("company") or ""))
    if not title or not company:
        return {"reconciled": False, "reason": "Missing title or company in experience plan"}
    try:
        response = voyager_get(session, "/voyager/api/me")
        data = parse_json_response(response)
    except Exception as exc:
        return {"reconciled": False, "reason": f"Failed to fetch profile: {exc}"}

    checked = 0
    for item in data.get("included") or []:
        if not isinstance(item, dict):
            continue
        item_type = str(item.get("$type") or "")
        serialized = _normalize_for_compare(str(item))
        title_value = _normalize_for_compare(_localized_value(item.get("title")))
        company_value = _normalize_for_compare(
            _localized_value(item.get("companyName"))
            or _localized_value(item.get("company"))
            or _localized_value(item.get("companyUrnResolutionResult"))
        )
        if "Position" in item_type or title in serialized or company in serialized:
            checked += 1
        if (title_value == title or title in serialized) and (company_value == company or company in serialized):
            remote_ref = item.get("entityUrn") or item.get("urn")
            action = get_action(action_id)
            if action and action.get("state") != "succeeded":
                update_state(action_id, "succeeded", remote_ref=remote_ref)
            return {
                "reconciled": True,
                "matched_urn": remote_ref,
                "message": "Experience entry matches profile data",
            }

    return {
        "reconciled": False,
        "reason": "No matching experience entry found in profile data",
        "checked_items": checked,
    }


def reconcile_action(session: Session, action_id: str) -> dict[str, Any]:
    """Dispatch reconciliation for a stored action and persist the evidence."""
    action = get_action(action_id)
    if action is None:
        raise ValueError(f"Action not found: {action_id}")

    plan = action.get("plan") or {}
    action_type = action.get("action_type") or ""

    if action_type in {"post.publish", "post.image_publish", "post.scheduled"}:
        result = reconcile_post(session, action_id, plan)
    elif action_type.startswith("profile.edit"):
        result = reconcile_profile_edit(session, action_id, plan)
    elif action_type == "comment.post":
        result = reconcile_comment(session, action_id, plan)
    elif action_type == "dm.send":
        result = reconcile_dm(session, action_id, plan)
    elif action_type == "connect":
        result = reconcile_connect(session, action_id, plan)
    elif action_type == "follow":
        result = reconcile_follow(session, action_id, plan)
    elif action_type == "experience.add":
        result = reconcile_experience(session, action_id, plan)
    else:
        result = {
            "reconciled": False,
            "reason": f"No reconciler is implemented for action type: {action_type}",
        }

    write_artifact(
        action_id,
        "reconcile",
        {
            "action_id": action_id,
            "action_type": action_type,
            "result": result,
        },
    )
    return result
