"""Action plan builders for LinkedIn write operations.

Each builder normalizes inputs, computes an idempotency key, and returns
a plan dict that the executor understands.
"""

from __future__ import annotations

import hashlib
import re
import uuid
from typing import Any


def idempotency_key(*parts: str) -> str:
    """Compute sha256 idempotency key from ordered string parts."""
    canonical = "\n".join(parts)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _normalize_text(text: str) -> str:
    """Normalize post/about text: strip, normalize line endings, collapse trailing whitespace."""
    text = text.strip()
    # Normalize line endings
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    # Collapse trailing whitespace on each line
    text = re.sub(r"[ \t]+$", "", text, flags=re.MULTILINE)
    return text


def build_post_plan(account_id: str, text: str, visibility: str = "anyone") -> dict[str, Any]:
    """Build an action plan for publishing a text post.

    Args:
        account_id: The member URN or ID (e.g. "urn:li:person:ABC123" or just the member_id)
        text: Post text content
        visibility: "anyone" (public) or "connections"

    Returns:
        Plan dict with action_type, target, desired state, and live request details.
    """
    normalized_text = _normalize_text(text)
    if not normalized_text:
        raise ValueError("Post text cannot be empty")

    # Map visibility to LinkedIn enum
    vis_map = {
        "anyone": "ANYONE",
        "public": "PUBLIC",
        "connections": "CONNECTIONS",
    }
    li_visibility = vis_map.get(visibility.lower(), "PUBLIC")

    idem_key = idempotency_key(account_id, "post.publish", normalized_text, visibility.lower())

    # Build the author URN
    if account_id.startswith("urn:li:"):
        author_urn = account_id
    else:
        author_urn = f"urn:li:person:{account_id}"

    plan = {
        "action_type": "post.publish",
        "account_id": account_id,
        "idempotency_key": idem_key,
        "target_key": "me",
        "desired": {
            "text": normalized_text,
            "visibility": visibility.lower(),
        },
        "live_request": {
            "method": "POST",
            "path": "/voyager/api/graphql?action=execute&queryId=voyagerContentcreationDashShares.279996efa5064c01775d5aff003d9377",
            "body": {
                "variables": {
                    "post": {
                        "allowedCommentersScope": "ALL",
                        "intendedShareLifeCycleState": "PUBLISHED",
                        "origin": "FEED",
                        "visibilityDataUnion": {
                            "visibilityType": li_visibility,
                        },
                        "commentary": {
                            "text": normalized_text,
                            "attributesV2": [],
                        },
                    }
                },
                "queryId": "voyagerContentcreationDashShares.279996efa5064c01775d5aff003d9377",
                "includeWebMetadata": True,
            },
        },
        "reconcile": {
            "strategy": "feed_text_match",
            "window_minutes": 10,
        },
    }
    return plan


def build_image_post_plan(
    account_id: str,
    text: str,
    image_path: str,
    image_size: int,
    image_filename: str,
    visibility: str = "anyone",
) -> dict[str, Any]:
    """Build an action plan for publishing a post with an image.

    This is a multi-step plan:
      1. Register upload to get upload URL and image URN
      2. Upload image bytes to the upload URL
      3. Publish the post referencing the image URN
    """
    normalized_text = _normalize_text(text)
    if not normalized_text:
        raise ValueError("Post text cannot be empty")

    vis_map = {
        "anyone": "ANYONE",
        "public": "PUBLIC",
        "connections": "CONNECTIONS",
    }
    li_visibility = vis_map.get(visibility.lower(), "PUBLIC")

    idem_key = idempotency_key(account_id, "post.image_publish", normalized_text, image_path)

    if account_id.startswith("urn:li:"):
        author_urn = account_id
    else:
        author_urn = f"urn:li:person:{account_id}"

    plan = {
        "action_type": "post.image_publish",
        "account_id": account_id,
        "idempotency_key": idem_key,
        "target_key": "me",
        "desired": {
            "text": normalized_text,
            "visibility": visibility.lower(),
            "image_path": image_path,
            "image_filename": image_filename,
        },
        "live_request": {
            "steps": [
                {
                    "name": "register_upload",
                    "method": "POST",
                    "path": "/voyager/api/voyagerMediaUploadMetadata?action=upload",
                    "body": {
                        "fileSize": image_size,
                        "filename": image_filename,
                        "mediaUploadType": "IMAGE_SHARING",
                    },
                },
                {
                    "name": "upload_image",
                    "method": "PUT",
                    "url_from": "register_upload.uploadUrl",
                    "body_type": "binary",
                    "file_path": image_path,
                },
                {
                    "name": "publish_post",
                    "method": "POST",
                    "path": "/voyager/api/graphql?action=execute&queryId=voyagerContentcreationDashShares.279996efa5064c01775d5aff003d9377",
                    "body": {
                        "author": author_urn,
                        "lifecycleState": "PUBLISHED",
                        "visibility": {
                            "com.linkedin.ugc.MemberNetworkVisibility": li_visibility,
                        },
                        "specificContent": {
                            "com.linkedin.ugc.ShareContent": {
                                "shareCommentary": {"text": normalized_text},
                                "shareMediaCategory": "IMAGE",
                                "media": [
                                    {
                                        "status": "READY",
                                        "media_urn_from": "register_upload.urn",
                                    }
                                ],
                            }
                        },
                    },
                },
            ],
        },
        "reconcile": {
            "strategy": "feed_text_match",
            "window_minutes": 10,
        },
    }
    return plan


def build_profile_edit_plan(
    account_id: str,
    field: str,
    value: str,
    member_hash: str | None = None,
) -> dict[str, Any]:
    """Build an action plan for editing a profile field via GraphQL.

    Args:
        account_id: The member URN or ID
        field: One of "headline", "about", "website", "location"
        value: The desired new value
        member_hash: The fsd_profile member hash. If None, it must be
                     provided by the caller (fetched from the session).

    Returns:
        Plan dict for the profile edit.
    """
    allowed_fields = {"headline", "about", "website", "location"}
    if field not in allowed_fields:
        raise ValueError(f"Unsupported profile field: {field}. Must be one of {allowed_fields}")

    normalized_value = _normalize_text(value)
    if not normalized_value:
        raise ValueError(f"Profile {field} value cannot be empty")

    idem_key = idempotency_key(account_id, f"profile.edit.{field}", normalized_value)

    if not member_hash:
        raise ValueError(
            "member_hash is required for profile edit plans. "
            "Fetch it from the session via _get_my_member_hash()."
        )
    profile_urn = f"urn:li:fsd_profile:{member_hash}"

    # Build formElementInputs based on field type
    if field == "headline":
        form_element_urn = f"urn:li:fsd_profileEditFormElement:(TOP_CARD,{profile_urn},/headline)"
        input_values = [{"textInputValue": normalized_value}]
    elif field == "about":
        form_element_urn = f"urn:li:fsd_profileEditFormElement:(SUMMARY,{profile_urn},/summary)"
        input_values = [{"textInputValue": normalized_value}]
    elif field == "website":
        form_element_urn = f"urn:li:fsd_profileEditFormElement:(CONTACT_INFO,{profile_urn},/websiteUrl)"
        input_values = [{"textInputValue": normalized_value}]
    elif field == "location":
        form_element_urn = f"urn:li:fsd_profileEditFormElement:(TOP_CARD,{profile_urn},/geoLocation)"
        input_values = [{"locationInputValue": normalized_value}]
    else:
        form_element_urn = ""
        input_values = []

    tracking_id = str(uuid.uuid4())

    plan = {
        "action_type": f"profile.edit.{field}",
        "account_id": account_id,
        "idempotency_key": idem_key,
        "target_key": f"profile.{field}",
        "desired": {
            "field": field,
            "value": normalized_value,
        },
        "live_request": {
            "method": "POST",
            "path": "/voyager/api/graphql?action=execute&queryId=voyagerIdentityDashProfileEditFormPages.56e440de740281eb97a4f2219a98e71a",
            "body": {
                "variables": {
                    "formElementInputs": [
                        {
                            "formElementUrn": form_element_urn,
                            "formElementInputValues": input_values,
                        }
                    ],
                    "trackingId": tracking_id,
                },
                "queryId": "voyagerIdentityDashProfileEditFormPages.56e440de740281eb97a4f2219a98e71a",
                "includeWebMetadata": True,
            },
        },
        "reconcile": {
            "strategy": "profile_field_match",
            "field": field,
        },
    }
    return plan


def build_experience_plan(
    account_id: str,
    title: str,
    company: str,
    description: str | None = None,
    location: str | None = None,
    start_month: int | None = None,
    start_year: int | None = None,
    end_month: int | None = None,
    end_year: int | None = None,
) -> dict[str, Any]:
    """Build an action plan for adding a position/experience entry."""
    title = _normalize_text(title)
    company = _normalize_text(company)
    if not title:
        raise ValueError("Job title cannot be empty")
    if not company:
        raise ValueError("Company name cannot be empty")

    idem_parts = [account_id, "experience.add", title, company]
    if start_year:
        idem_parts.append(str(start_year))
    if start_month:
        idem_parts.append(str(start_month))
    idem_key = idempotency_key(*idem_parts)

    # Build the request body
    body: dict[str, Any] = {
        "companyName": {"localized": {"en_US": company}},
        "title": {"localized": {"en_US": title}},
    }

    if description:
        body["description"] = {"localized": {"en_US": _normalize_text(description)}}

    if location:
        body["locationName"] = {"localized": {"en_US": _normalize_text(location)}}

    # Date range
    date_range: dict[str, Any] = {}
    if start_month or start_year:
        start: dict[str, int] = {}
        if start_month:
            start["month"] = start_month
        if start_year:
            start["year"] = start_year
        date_range["start"] = start

    if end_month or end_year:
        end: dict[str, int] = {}
        if end_month:
            end["month"] = end_month
        if end_year:
            end["year"] = end_year
        date_range["end"] = end

    if date_range:
        body["dateRange"] = date_range

    # Desired state for tracking
    desired: dict[str, Any] = {
        "title": title,
        "company": company,
    }
    if description:
        desired["description"] = _normalize_text(description)
    if location:
        desired["location"] = _normalize_text(location)
    if start_month:
        desired["start_month"] = start_month
    if start_year:
        desired["start_year"] = start_year
    if end_month:
        desired["end_month"] = end_month
    if end_year:
        desired["end_year"] = end_year

    plan = {
        "action_type": "experience.add",
        "account_id": account_id,
        "idempotency_key": idem_key,
        "target_key": f"experience.{company}.{title}",
        "desired": desired,
        "live_request": {
            "method": "POST",
            "path": "/voyager/api/identity/dash/profilePositions",
            "body": body,
        },
        "reconcile": {
            "strategy": "experience_match",
            "title": title,
            "company": company,
        },
    }
    return plan


def build_connect_plan(
    account_id: str,
    vanity_name: str,
    page_key: str,
    member_urn: str,
    message: str | None = None,
) -> dict[str, Any]:
    """Build an action plan for sending a connection request."""
    if not vanity_name:
        raise ValueError("Vanity name is required for connection request")
    if not page_key:
        raise ValueError("page_key is required for connection request")

    idem_key = idempotency_key(account_id, "connect", vanity_name)

    body: dict[str, Any] = {
        "queryId": "760d8d38a41717577499706ae0030e47",
        "variables": {"inviteeVanityName": vanity_name},
        "pageKey": page_key,
    }

    desired: dict[str, Any] = {
        "target_urn": member_urn,
        "vanity_name": vanity_name,
    }
    if message:
        desired["message"] = _normalize_text(message)

    plan = {
        "action_type": "connect",
        "account_id": account_id,
        "idempotency_key": idem_key,
        "target_key": f"connect.{vanity_name}",
        "desired": desired,
        "live_request": {
            "method": "POST",
            "path": "/mwlite/profile/api/non-self/runQuery",
            "body": body,
            "mode": "mwlite_graphql",
            "referer": f"https://www.linkedin.com/in/{vanity_name}/",
        },
        "reconcile": {
            "strategy": "none",
        },
    }
    return plan


def build_follow_plan(
    account_id: str,
    target_member_urn: str,
    page_key: str,
    vanity_name: str,
) -> dict[str, Any]:
    """Build an action plan for following a profile via the mwlite profile endpoint."""
    if not target_member_urn:
        raise ValueError("Target member URN is required for follow")
    if not page_key:
        raise ValueError("page_key is required for follow")

    idem_key = idempotency_key(account_id, "follow", target_member_urn)

    body = {
        "queryId": "dba87726eac3e0c36630116a64305104",
        "variables": {
            "followState": "FOLLOW_ACTIVE",
            "to": target_member_urn,
        },
        "pageKey": page_key,
    }

    plan = {
        "action_type": "follow",
        "account_id": account_id,
        "idempotency_key": idem_key,
        "target_key": f"follow.{target_member_urn}",
        "desired": {
            "target_urn": target_member_urn,
            "vanity_name": vanity_name,
        },
        "live_request": {
            "method": "POST",
            "path": "/mwlite/profile/api/non-self/runQuery",
            "body": body,
            "mode": "mwlite_graphql",
            "referer": f"https://www.linkedin.com/in/{vanity_name}/",
        },
        "reconcile": {
            "strategy": "none",
        },
    }
    return plan


def build_dm_plan(
    account_id: str,
    conversation_urn: str | None = None,
    recipient_urn: str | None = None,
    message_text: str = "",
    mailbox_urn: str | None = None,
) -> dict[str, Any]:
    """Build an action plan for sending a LinkedIn DM.

    Either conversation_urn (reply to existing thread) or recipient_urn
    (start new conversation) must be provided.

    Args:
        account_id: The member account ID.
        conversation_urn: Existing conversation URN for replies.
        recipient_urn: Recipient fsd_profile URN for new conversations.
        message_text: The message body.
        mailbox_urn: The authenticated user's fsd_profile URN (mailbox).
                     Must be provided by the caller.
    """
    normalized_text = " ".join(message_text.strip().split())

    target_key = conversation_urn or recipient_urn or "unknown"
    key_input = f"{account_id}|dm.send|{target_key}|{normalized_text}"
    idem_key = hashlib.sha256(key_input.encode()).hexdigest()

    if not mailbox_urn:
        raise ValueError(
            "mailbox_urn is required for DM plans. "
            "Fetch it via _get_my_urn() from the session."
        )

    # Generate a unique origin token
    origin_token = str(uuid.uuid4())
    tracking_id = str(uuid.uuid4())

    # Build recipient URN list
    if recipient_urn:
        if recipient_urn.startswith("urn:li:fsd_profile:"):
            host_recipient_urns = [recipient_urn]
        else:
            host_recipient_urns = [f"urn:li:fsd_profile:{recipient_urn}"]
    elif conversation_urn:
        host_recipient_urns = []
    else:
        host_recipient_urns = []

    path = "/voyager/api/voyagerMessagingDashMessengerMessages?action=createMessage"
    body = {
        "message": {
            "body": {
                "attributes": [],
                "text": message_text.strip(),
            },
            "originToken": origin_token,
            "renderContentUnions": [],
        },
        "mailboxUrn": mailbox_urn,
        "trackingId": tracking_id,
        "dedupeByClientGeneratedToken": False,
        "hostRecipientUrns": host_recipient_urns,
    }

    return {
        "action_type": "dm.send",
        "account_id": account_id,
        "idempotency_key": idem_key,
        "target_key": target_key,
        "desired": {
            "message_text": message_text.strip(),
            "conversation_urn": conversation_urn,
            "recipient_urn": recipient_urn,
        },
        "live_request": {
            "method": "POST",
            "path": path,
            "body": body,
        },
        "reconcile": {
            "strategy": "conversation_text_match",
            "window_minutes": 10,
        },
    }


def build_scheduled_post_plan(
    account_id: str,
    text: str,
    scheduled_at: str,
    visibility: str = "anyone",
    image_path: str | None = None,
) -> dict[str, Any]:
    """Build an action plan for a scheduled LinkedIn post."""
    normalized_text = " ".join(text.strip().split())
    key_input = f"{account_id}|post.scheduled|{normalized_text}|{visibility}|{scheduled_at}"
    idem_key = hashlib.sha256(key_input.encode()).hexdigest()

    vis_map = {
        "anyone": "ANYONE",
        "connections": "CONNECTIONS",
    }

    body = {
        "author": f"urn:li:person:{account_id}",
        "lifecycleState": "PUBLISHED",
        "visibility": {
            "com.linkedin.ugc.MemberNetworkVisibility": vis_map.get(visibility, "PUBLIC"),
        },
        "specificContent": {
            "com.linkedin.ugc.ShareContent": {
                "shareCommentary": {"text": text.strip()},
                "shareMediaCategory": "NONE",
            }
        },
    }

    return {
        "action_type": "post.scheduled",
        "account_id": account_id,
        "idempotency_key": idem_key,
        "target_key": "me",
        "scheduled_at": scheduled_at,
        "desired": {
            "text": text.strip(),
            "visibility": visibility,
            "image_path": image_path,
            "scheduled_at": scheduled_at,
        },
        "live_request": {
            "method": "POST",
            "path": "/voyager/api/graphql?action=execute&queryId=voyagerContentcreationDashShares.279996efa5064c01775d5aff003d9377",
            "body": {
                "variables": {
                    "post": {
                        "allowedCommentersScope": "ALL",
                        "intendedShareLifeCycleState": "PUBLISHED",
                        "origin": "FEED",
                        "visibilityDataUnion": {
                            "visibilityType": vis_map.get(visibility, "ANYONE"),
                        },
                        "commentary": {
                            "text": text.strip(),
                            "attributesV2": [],
                        },
                    }
                },
                "queryId": "voyagerContentcreationDashShares.279996efa5064c01775d5aff003d9377",
                "includeWebMetadata": True,
            },
        },
        "reconcile": {
            "strategy": "feed_text_match",
            "window_minutes": 15,
        },
    }
