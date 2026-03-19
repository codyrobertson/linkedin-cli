"""Post-write verification for LinkedIn actions.

Checks whether a write operation actually landed by re-reading
the relevant data from LinkedIn.
"""

from __future__ import annotations

import re
from typing import Any

from requests import Session

from linkedin_cli.voyager import parse_json_response, voyager_get
from linkedin_cli.write.store import get_action, update_state


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
