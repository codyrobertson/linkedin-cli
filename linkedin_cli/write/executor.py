"""Write executor for LinkedIn actions.

Handles the full lifecycle: idempotency check, warm-up, jitter,
execute, record attempt, update state.
"""

from __future__ import annotations

import fcntl
import json
import random
import time
from datetime import datetime, timedelta, timezone
from typing import Any

import requests
from requests import Session

from linkedin_cli.config import CONFIG_DIR, DEFAULT_TIMEOUT, MOBILE_USER_AGENT
from linkedin_cli.session import csrf_token_from_session
from linkedin_cli.voyager import voyager_get
from linkedin_cli.write.store import (
    create_action,
    find_by_idempotency_key,
    get_action,
    init_db,
    list_artifacts,
    record_attempt,
    update_state,
    write_artifact,
)


LOCK_DIR = CONFIG_DIR / "locks"
LOCK_FILE = LOCK_DIR / "account.lock"
RETRYABLE_HTTP_STATUSES = {408, 425, 429, 500, 502, 503, 504}


def _next_retry_timestamp(attempt_no: int) -> str:
    minutes = [3, 12, 45][min(max(attempt_no - 1, 0), 2)]
    return (datetime.now(timezone.utc) + timedelta(minutes=minutes)).isoformat()


def _is_transport_uncertain(exc: Exception) -> bool:
    if isinstance(exc, (TimeoutError, requests.Timeout)):
        return True
    message = str(exc).lower()
    return "timed out" in message or "timeout" in message


def _build_result(status: str, message: str, action_id: str, **extra: Any) -> dict[str, Any]:
    payload = {
        "status": status,
        "message": message,
        "action": get_action(action_id),
        "artifacts": list_artifacts(action_id),
    }
    payload.update(extra)
    return payload


def _extract_post_remote_ref(resp_data: dict[str, Any]) -> str | None:
    create_data = (resp_data.get("data", {}).get("data", {})
                   .get("createContentcreationDashShares", {}))
    share_urn = create_data.get("resourceKey") or create_data.get("*entity")
    activity_urn = None
    for item in resp_data.get("included", []):
        if isinstance(item, dict):
            urn = item.get("activityUrn") or item.get("entityUrn") or ""
            if "activity" in urn:
                activity_urn = urn
                break
    return (
        share_urn
        or activity_urn
        or resp_data.get("value", {}).get("activityUrn")
        or resp_data.get("data", {}).get("activityUrn")
        or resp_data.get("activityUrn")
    )


def _acquire_lock() -> Any:
    """Acquire single-account file lock. Returns the lock file handle."""
    LOCK_DIR.mkdir(parents=True, exist_ok=True)
    lock_fh = open(LOCK_FILE, "w")
    try:
        fcntl.flock(lock_fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except (IOError, OSError):
        lock_fh.close()
        raise RuntimeError(
            "Another LinkedIn write operation is in progress. "
            "Only one write at a time per account is allowed."
        )
    return lock_fh


def _release_lock(lock_fh: Any) -> None:
    """Release the account lock."""
    try:
        fcntl.flock(lock_fh, fcntl.LOCK_UN)
        lock_fh.close()
    except Exception:
        pass


def _voyager_headers(session: Session, share: bool = False) -> dict[str, str]:
    """Build standard Voyager headers for write requests."""
    csrf = csrf_token_from_session(session)
    headers = {
        "Accept": "application/vnd.linkedin.normalized+json+2.1",
        "Content-Type": "application/json; charset=UTF-8",
        "X-RestLi-Protocol-Version": "2.0.0",
        "Referer": "https://www.linkedin.com/feed/",
        "Origin": "https://www.linkedin.com",
        "X-Li-Lang": "en_US",
        "X-Li-Track": '{"clientVersion":"1.13.42872","mpVersion":"1.13.42872","osName":"web","timezoneOffset":-7,"timezone":"America/Phoenix","deviceFormFactor":"DESKTOP","mpName":"voyager-web","displayDensity":1,"displayWidth":3440,"displayHeight":1440}',
    }
    if csrf:
        headers["csrf-token"] = csrf
    if share:
        headers["X-Li-Pem-Metadata"] = "Voyager - Sharing - CreateShare=sharing-create-content"
    return headers


def _mwlite_headers(session: Session, referer: str) -> dict[str, str]:
    """Build headers for the mobile-web mutation endpoint used by profile actions."""
    csrf = csrf_token_from_session(session)
    headers = {
        "User-Agent": MOBILE_USER_AGENT,
        "Accept": "*/*",
        "Content-Type": "application/json",
        "Referer": referer,
        "Origin": "https://www.linkedin.com",
    }
    if csrf:
        headers["Csrf-Token"] = csrf
    return headers


def execute_action(
    session: Session,
    action_id: str,
    plan: dict[str, Any],
    account_id: str,
    dry_run: bool = True,
) -> dict[str, Any]:
    """Main execution entry point.

    1. Check idempotency (skip if duplicate)
    2. If dry_run, persist and return
    3. Warm-up GET
    4. Add 2-5s jitter delay
    5. Execute the POST/PUT
    6. Record attempt
    7. Update state

    Returns the final action dict.
    """
    init_db()

    action_type = plan["action_type"]
    idem_key = plan["idempotency_key"]
    target_key = plan["target_key"]

    # 1. Idempotency check
    existing = find_by_idempotency_key(account_id, idem_key)
    if existing and existing["action_id"] != action_id:
        skip_states = {"planned", "executing", "retry_scheduled", "unknown_remote_state", "succeeded"}
        if existing["state"] == "dry_run" and dry_run:
            return {
                "status": "duplicate_skipped",
                "message": "Action already exists as a dry run",
                "existing_action": existing,
            }
        if existing["state"] in skip_states:
            return {
                "status": "duplicate_skipped",
                "message": f"Action already exists in state '{existing['state']}'",
                "existing_action": existing,
            }

    # 2. Create or reuse the action record
    if existing:
        action_id = existing["action_id"]
        action = existing
        update_state(
            action_id,
            "dry_run" if dry_run else "planned",
            last_error=None,
            remote_ref=None,
            dry_run=dry_run,
            plan=plan,
        )
        write_artifact(action_id, "plan", plan)
    else:
        action = create_action(
            action_id=action_id,
            action_type=action_type,
            account_id=account_id,
            target_key=target_key,
            idempotency_key=idem_key,
            plan=plan,
            dry_run=dry_run,
        )

    if dry_run:
        return _build_result(
            "dry_run",
            "Action planned but not executed. Pass --execute for live.",
            action_id,
        )

    # Acquire write lock for live execution
    lock_fh = _acquire_lock()
    try:
        from linkedin_cli.write.guards import evaluate_write_guard

        guard = evaluate_write_guard(account_id, action_type)
        if not guard.get("allowed"):
            reason = str(guard.get("reason") or "Write blocked by local guardrail")
            update_state(
                action_id,
                "blocked",
                last_error=reason,
                risk_flags=guard.get("risk_flags") or [],
                dry_run=False,
            )
            write_artifact(action_id, "last_result", {"status": "blocked", "guard": guard})
            return _build_result("blocked", reason, action_id, guard=guard)

        # Update state to executing
        update_state(action_id, "executing")

        # 3. Warm-up GET
        try:
            if action_type in ("post.publish", "post.image_publish", "post.scheduled"):
                voyager_get(session, "/voyager/api/me")
            elif action_type.startswith("profile.edit"):
                voyager_get(session, "/voyager/api/me")
            elif action_type == "experience.add":
                voyager_get(session, "/voyager/api/me")
            elif action_type in ("connect", "follow"):
                voyager_get(session, "/voyager/api/me")
            elif action_type == "comment.post":
                voyager_get(session, "/voyager/api/me")
        except Exception:
            pass  # Warm-up failure is non-fatal

        # 4. Jitter delay
        jitter = random.uniform(2.0, 5.0)
        time.sleep(jitter)

        # 5. Execute
        try:
            if action_type == "post.publish":
                result = _post_publish(session, plan)
            elif action_type == "post.image_publish":
                result = _image_post_publish(session, plan)
            elif action_type.startswith("profile.edit"):
                result = _profile_edit(session, plan)
            elif action_type == "experience.add":
                result = _experience_add(session, plan)
            elif action_type == "connect":
                result = _connect_request(session, plan)
            elif action_type == "follow":
                result = _follow(session, plan)
            elif action_type == "dm.send":
                result = _dm_send(session, plan)
            elif action_type == "comment.post":
                result = _comment_post(session, plan)
            elif action_type == "post.scheduled":
                result = _post_publish(session, plan)
            else:
                update_state(action_id, "failed", last_error=f"Unknown action type: {action_type}")
                return _build_result("failed", f"Unknown action type: {action_type}", action_id)
        except Exception as exc:
            error_msg = str(exc)
            attempt_no = (action.get("attempt_count") or 0) + 1
            # 6. Record failed attempt
            record_attempt(
                action_id=action_id,
                attempt_no=attempt_no,
                method=plan.get("live_request", {}).get("method", "POST"),
                path=plan.get("live_request", {}).get("path", ""),
                status=None,
                outcome="transport_error",
                error=error_msg,
            )
            if _is_transport_uncertain(exc):
                update_state(action_id, "unknown_remote_state", last_error=error_msg)
                write_artifact(action_id, "last_result", {"status": "unknown_remote_state", "error": error_msg})
                return _build_result(
                    "unknown_remote_state",
                    "Transport failed after request uncertainty. Reconcile before retrying.",
                    action_id,
                )

            next_attempt_at = _next_retry_timestamp(attempt_no)
            update_state(
                action_id,
                "retry_scheduled",
                last_error=error_msg,
                next_attempt_at=next_attempt_at,
            )
            write_artifact(
                action_id,
                "last_result",
                {"status": "retry_scheduled", "error": error_msg, "next_attempt_at": next_attempt_at},
            )
            return _build_result(
                "retry_scheduled",
                error_msg,
                action_id,
                next_attempt_at=next_attempt_at,
            )

        # 6. Record successful attempt
        http_status = result.get("http_status")
        record_attempt(
            action_id=action_id,
            attempt_no=(action.get("attempt_count") or 0) + 1,
            method=plan.get("live_request", {}).get("method", "POST"),
            path=plan.get("live_request", {}).get("path", ""),
            status=http_status,
            outcome="success" if (http_status and http_status < 400) else "http_error",
            error=result.get("error"),
        )
        write_artifact(action_id, "last_result", result)

        # 7. Update state
        if http_status and http_status < 400:
            update_state(
                action_id,
                "succeeded",
                remote_ref=result.get("remote_ref"),
            )
            return _build_result(
                "succeeded",
                "Action executed successfully",
                action_id,
                result=result,
            )

        if http_status in RETRYABLE_HTTP_STATUSES:
            next_attempt_at = _next_retry_timestamp((action.get("attempt_count") or 0) + 1)
            update_state(
                action_id,
                "retry_scheduled",
                last_error=result.get("error") or f"HTTP {http_status}",
                next_attempt_at=next_attempt_at,
            )
            return _build_result(
                "retry_scheduled",
                result.get("error") or f"HTTP {http_status}",
                action_id,
                result=result,
                next_attempt_at=next_attempt_at,
            )

        else:
            update_state(
                action_id,
                "failed",
                last_error=result.get("error") or f"HTTP {http_status}",
            )
            return _build_result(
                "failed",
                result.get("error") or f"HTTP {http_status}",
                action_id,
                result=result,
            )
    finally:
        _release_lock(lock_fh)


def _post_publish(session: Session, plan: dict[str, Any]) -> dict[str, Any]:
    """POST to voyager feed endpoint to publish a text post."""
    live_req = plan["live_request"]
    url = "https://www.linkedin.com" + live_req["path"]
    body = live_req["body"]
    headers = _voyager_headers(session, share=True)

    response = session.post(
        url,
        json=body,
        headers=headers,
        timeout=DEFAULT_TIMEOUT,
    )

    result: dict[str, Any] = {
        "http_status": response.status_code,
        "remote_ref": None,
        "error": None,
    }

    if response.status_code < 400:
        try:
            resp_data = response.json()
            result["remote_ref"] = _extract_post_remote_ref(resp_data)
        except Exception:
            result["response_text"] = response.text[:500]
    else:
        snippet = response.text[:500].strip().replace("\n", " ") if response.text else ""
        result["error"] = f"HTTP {response.status_code}: {snippet}"

    return result


def _profile_edit(session: Session, plan: dict[str, Any]) -> dict[str, Any]:
    """POST to voyager GraphQL endpoint to edit a profile field."""
    live_req = plan["live_request"]
    url = "https://www.linkedin.com" + live_req["path"]
    body = live_req["body"]
    headers = _voyager_headers(session)
    headers["X-Li-Pem-Metadata"] = "Voyager - Identity - ProfileEditFormPages=identity-profile-edit-form-page"

    response = session.post(
        url,
        json=body,
        headers=headers,
        timeout=DEFAULT_TIMEOUT,
    )

    result: dict[str, Any] = {
        "http_status": response.status_code,
        "remote_ref": None,
        "error": None,
    }

    if response.status_code < 400:
        try:
            result["response_data"] = response.json()
        except Exception:
            result["response_text"] = response.text[:500]
    else:
        snippet = response.text[:500].strip().replace(chr(10), ' ') if response.text else ""
        result["error"] = f"HTTP {response.status_code}: {snippet}"

    return result


def _experience_add(session: Session, plan: dict[str, Any]) -> dict[str, Any]:
    """POST to voyager endpoint to add a profile position."""
    live_req = plan["live_request"]
    url = "https://www.linkedin.com" + live_req["path"]
    body = live_req["body"]
    headers = _voyager_headers(session)

    response = session.post(
        url,
        json=body,
        headers=headers,
        timeout=DEFAULT_TIMEOUT,
    )

    result: dict[str, Any] = {
        "http_status": response.status_code,
        "remote_ref": None,
        "error": None,
    }

    if response.status_code < 400:
        try:
            resp_data = response.json()
            result["response_data"] = resp_data
            result["remote_ref"] = (
                resp_data.get("entityUrn")
                or resp_data.get("data", {}).get("entityUrn")
                or resp_data.get("value", {}).get("entityUrn")
            )
        except Exception:
            result["response_text"] = response.text[:500]
    else:
        snippet = response.text[:500].strip().replace("\n", " ") if response.text else ""
        result["error"] = f"HTTP {response.status_code}: {snippet}"

    return result


def _connect_request(session: Session, plan: dict[str, Any]) -> dict[str, Any]:
    """POST a connection invitation using the mwlite profile mutation endpoint."""
    live_req = plan["live_request"]
    url = "https://www.linkedin.com" + live_req["path"]
    body = live_req["body"]
    headers = _mwlite_headers(session, live_req.get("referer", "https://www.linkedin.com/"))

    response = session.post(
        url,
        json=body,
        headers=headers,
        timeout=DEFAULT_TIMEOUT,
    )

    result: dict[str, Any] = {
        "http_status": response.status_code,
        "remote_ref": None,
        "error": None,
    }

    if response.status_code < 400:
        try:
            resp_data = response.json()
            result["response_data"] = resp_data
            response_code = ((resp_data.get("graphQL") or {}).get("addConnection") or {}).get("responseCode")
            result["remote_ref"] = response_code
            if response_code not in {"CREATED_201", "OK_200"}:
                result["error"] = f"LinkedIn responseCode: {response_code}"
                result["http_status"] = 409
        except Exception:
            result["response_text"] = response.text[:500]
    else:
        snippet = response.text[:500].strip().replace("\n", " ") if response.text else ""
        result["error"] = f"HTTP {response.status_code}: {snippet}"

    return result


def _follow(session: Session, plan: dict[str, Any]) -> dict[str, Any]:
    """POST a follow action using the mwlite profile mutation endpoint."""
    live_req = plan["live_request"]
    url = "https://www.linkedin.com" + live_req["path"]
    body = live_req["body"]
    headers = _mwlite_headers(session, live_req.get("referer", "https://www.linkedin.com/"))

    response = session.post(
        url,
        json=body,
        headers=headers,
        timeout=DEFAULT_TIMEOUT,
    )

    result: dict[str, Any] = {
        "http_status": response.status_code,
        "remote_ref": None,
        "error": None,
    }

    if response.status_code < 400:
        try:
            resp_data = response.json()
            result["response_data"] = resp_data
            response_code = ((resp_data.get("graphQL") or {}).get("updateFollowState") or {}).get("responseCode")
            result["remote_ref"] = response_code
            if response_code not in {"CREATED_201", "OK_200"}:
                result["error"] = f"LinkedIn responseCode: {response_code}"
                result["http_status"] = 409
        except Exception:
            result["response_text"] = response.text[:500]
    else:
        snippet = response.text[:500].strip().replace("\n", " ") if response.text else ""
        result["error"] = f"HTTP {response.status_code}: {snippet}"

    return result


def _dm_send(session: Session, plan: dict[str, Any]) -> dict[str, Any]:
    """POST to voyager messaging endpoint to send a DM."""
    live_req = plan["live_request"]
    url = "https://www.linkedin.com" + live_req["path"]
    body = live_req["body"]
    headers = _voyager_headers(session)
    headers["X-Li-Pem-Metadata"] = "Voyager - Messaging - MessengerMessages=messaging-messenger-messages"

    response = session.post(
        url,
        json=body,
        headers=headers,
        timeout=DEFAULT_TIMEOUT,
    )

    result: dict[str, Any] = {
        "http_status": response.status_code,
        "remote_ref": None,
        "error": None,
    }

    if response.status_code < 400:
        try:
            resp_data = response.json()
            result["response_data"] = resp_data
            result["remote_ref"] = (
                resp_data.get("data", {}).get("value", {}).get("entityUrn")
                or resp_data.get("value", {}).get("entityUrn")
            )
        except Exception:
            result["response_text"] = response.text[:500]
    else:
        snippet = response.text[:500].strip().replace("\n", " ") if response.text else ""
        result["error"] = f"HTTP {response.status_code}: {snippet}"

    return result


def _comment_post(session: Session, plan: dict[str, Any]) -> dict[str, Any]:
    """POST a public comment through the same action lifecycle as other writes."""
    from linkedin_cli.comment import _comment_headers

    live_req = plan["live_request"]
    url = "https://www.linkedin.com" + live_req["path"]
    body = live_req["body"]
    headers = _comment_headers(
        session,
        referer=live_req.get("post_url", "https://www.linkedin.com/feed/"),
        reply=(live_req.get("delivery_mode") == "comment_reply"),
    )

    response = session.post(
        url,
        json=body,
        headers=headers,
        timeout=DEFAULT_TIMEOUT,
    )

    result: dict[str, Any] = {
        "http_status": response.status_code,
        "remote_ref": None,
        "error": None,
    }

    if response.status_code < 400:
        try:
            response_data = response.json()
            result["response_data"] = response_data
            result["remote_ref"] = (
                response_data.get("entityUrn")
                or response_data.get("data", {}).get("entityUrn")
                or response_data.get("data", {}).get("urn")
            )
        except Exception:
            result["response_text"] = response.text[:500]
    else:
        snippet = response.text[:500].strip().replace("\n", " ") if response.text else ""
        result["error"] = f"HTTP {response.status_code}: {snippet}"

    return result


def _image_post_publish(session: Session, plan: dict[str, Any]) -> dict[str, Any]:
    """Multi-step image post: register upload, upload image, publish post."""
    steps = plan["live_request"]["steps"]
    headers = _voyager_headers(session)

    # Step 1: Register upload
    reg_step = steps[0]
    reg_url = "https://www.linkedin.com" + reg_step["path"]
    reg_response = session.post(
        reg_url,
        json=reg_step["body"],
        headers=headers,
        timeout=DEFAULT_TIMEOUT,
    )

    if reg_response.status_code >= 400:
        snippet = reg_response.text[:500].strip().replace("\n", " ") if reg_response.text else ""
        return {
            "http_status": reg_response.status_code,
            "remote_ref": None,
            "error": f"Upload registration failed - HTTP {reg_response.status_code}: {snippet}",
            "step": "register_upload",
        }

    try:
        reg_data = reg_response.json()
    except Exception:
        return {
            "http_status": reg_response.status_code,
            "remote_ref": None,
            "error": "Upload registration returned non-JSON response",
            "step": "register_upload",
        }

    # Extract upload URL and URN from registration response
    upload_url = (
        reg_data.get("value", {}).get("uploadUrl")
        or reg_data.get("data", {}).get("value", {}).get("uploadUrl")
        or reg_data.get("uploadUrl")
    )
    image_urn = (
        reg_data.get("value", {}).get("urn")
        or reg_data.get("data", {}).get("value", {}).get("urn")
        or reg_data.get("urn")
    )

    if not upload_url:
        return {
            "http_status": reg_response.status_code,
            "remote_ref": None,
            "error": f"No uploadUrl in registration response: {json.dumps(reg_data)[:300]}",
            "step": "register_upload",
        }
    if not image_urn:
        return {
            "http_status": reg_response.status_code,
            "remote_ref": None,
            "error": f"No media urn in registration response: {json.dumps(reg_data)[:300]}",
            "step": "register_upload",
        }

    # Step 2: Upload image binary
    image_path = steps[1]["file_path"]
    try:
        with open(image_path, "rb") as f:
            image_data = f.read()
    except Exception as exc:
        return {
            "http_status": None,
            "remote_ref": None,
            "error": f"Failed to read image file {image_path}: {exc}",
            "step": "upload_image",
        }

    upload_headers = dict(headers)
    upload_headers["Content-Type"] = "application/octet-stream"
    upload_response = session.put(
        upload_url,
        data=image_data,
        headers=upload_headers,
        timeout=60,
    )

    if upload_response.status_code >= 400:
        snippet = upload_response.text[:500].strip().replace("\n", " ") if upload_response.text else ""
        return {
            "http_status": upload_response.status_code,
            "remote_ref": None,
            "error": f"Image upload failed - HTTP {upload_response.status_code}: {snippet}",
            "step": "upload_image",
        }

    # Small delay between upload and publish
    time.sleep(1.0)

    # Step 3: Publish post with image
    pub_step = steps[2]
    pub_body = json.loads(json.dumps(pub_step["body"]))  # deep copy

    # Replace the media URN placeholder
    media_list = (
        pub_body.get("variables", {})
        .get("post", {})
        .get("media", [])
    )
    for media_item in media_list:
        if "media_urn_from" in media_item:
            del media_item["media_urn_from"]
            media_item["media"] = image_urn

    legacy_media_list = (
        pub_body.get("specificContent", {})
        .get("com.linkedin.ugc.ShareContent", {})
        .get("media", [])
    )
    for media_item in legacy_media_list:
        if "media_urn_from" in media_item:
            del media_item["media_urn_from"]
            media_item["media"] = image_urn

    pub_url = "https://www.linkedin.com" + pub_step["path"]
    pub_response = session.post(
        pub_url,
        json=pub_body,
        headers=_voyager_headers(session, share=True),
        timeout=DEFAULT_TIMEOUT,
    )

    result: dict[str, Any] = {
        "http_status": pub_response.status_code,
        "remote_ref": None,
        "error": None,
        "image_urn": image_urn,
    }

    if pub_response.status_code < 400:
        try:
            resp_data = pub_response.json()
            result["remote_ref"] = _extract_post_remote_ref(resp_data)
            result["response_data"] = resp_data
        except Exception:
            result["response_text"] = pub_response.text[:500]
    else:
        snippet = pub_response.text[:500].strip().replace("\n", " ") if pub_response.text else ""
        result["error"] = f"Post publish failed - HTTP {pub_response.status_code}: {snippet}"
        result["step"] = "publish_post"

    return result
