"""LinkedIn DM poller -- checks for new messages and forwards to Discord.

Designed to run as a cron job every 2-5 minutes.
Tracks last-seen message timestamp to avoid duplicates.

Usage:
    python3 -m linkedin_cli.integrations.dm_poller check [--quiet]
"""

from __future__ import annotations

import json
import os
import sys
import tempfile
import urllib.error
import urllib.request
from datetime import datetime, timezone

from linkedin_cli.config import CONFIG_DIR

STATE_PATH = CONFIG_DIR / "dm_poll_state.json"
DEFAULT_WEBHOOK_ENV = "DISCORD_INBOX_CHANNEL"


def _load_env_from_config() -> None:
    """Load .env from config directory if present."""
    env_path = CONFIG_DIR / ".env"
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip().strip("'\""))


def load_state() -> dict:
    if STATE_PATH.exists():
        return json.loads(STATE_PATH.read_text())
    return {"last_checked_at": None, "last_message_ts": 0}


def save_state(state: dict) -> None:
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=str(STATE_PATH.parent), suffix=".tmp")
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(state, f, indent=2)
        os.replace(tmp, STATE_PATH)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def fetch_recent_messages(session, count=20):
    """Fetch recent messaging conversations via Voyager."""
    from linkedin_cli.voyager import parse_json_response, voyager_get

    try:
        resp = voyager_get(
            session,
            "/voyager/api/messaging/conversations",
            params={"createdBefore": "0", "keyVersion": "LEGACY_INBOX"},
        )
        return parse_json_response(resp)
    except BaseException as exc:
        return {"_fetch_error": getattr(exc, "message", str(exc))}


def extract_new_messages(data, last_ts):
    """Extract messages newer than last_ts from Voyager response."""
    if not data:
        return []

    messages = []
    included = data.get("included") or []

    for item in included:
        if not isinstance(item, dict):
            continue
        item_type = item.get("$type") or ""
        if "Message" not in item_type or "Delivery" in item_type:
            continue

        created = item.get("deliveredAt") or item.get("createdAt") or 0
        if created <= last_ts:
            continue

        body = item.get("body") or {}
        text = body.get("text") or "" if isinstance(body, dict) else str(body)
        sender_urn = item.get("*sender") or item.get("sender") or ""

        if not text:
            continue

        messages.append({
            "text": text[:500],
            "sender_urn": str(sender_urn)[:100],
            "created_at": created,
            "message_urn": (item.get("entityUrn") or "")[:100],
        })

    return sorted(messages, key=lambda m: m["created_at"])


def notify_discord(messages, webhook_url=None):
    """Send new DM notifications to Discord."""
    if not messages:
        return {"attempted": 0, "delivered": 0, "failed": 0}

    url = webhook_url or os.getenv(DEFAULT_WEBHOOK_ENV, "") or os.getenv("EMAIL_WEBHOOK_URL", "")
    if not url:
        return {"attempted": 0, "delivered": 0, "failed": 0}

    summary = {"attempted": 0, "delivered": 0, "failed": 0}
    for msg in messages[-5:]:  # Cap at 5 per poll
        summary["attempted"] += 1
        content = (
            f"**New LinkedIn DM**\n"
            f"**From:** `{msg['sender_urn']}`\n"
            f"**Message:** {msg['text'][:300]}"
        )
        payload = json.dumps({
            "content": content,
            "username": "LinkedIn DMs",
            "allowed_mentions": {"parse": []},
        }).encode()

        req = urllib.request.Request(
            url,
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            urllib.request.urlopen(req, timeout=10)
        except Exception as e:
            summary["failed"] += 1
            print(f"Discord notify failed: {e}", file=sys.stderr)
        else:
            summary["delivered"] += 1
    return summary


def check(quiet=False):
    from linkedin_cli.session import load_env_file, load_session

    load_env_file()
    _load_env_from_config()

    session, _ = load_session(required=True)
    if not session:
        return {"ok": False, "error": "No LinkedIn session"}

    state = load_state()
    last_ts = state.get("last_message_ts", 0)

    data = fetch_recent_messages(session)
    if data is None:
        error_payload = {"ok": False, "error": "Failed to fetch messages"}
        print(json.dumps(error_payload), file=sys.stderr)
        raise SystemExit(1)
    if isinstance(data, dict) and data.get("_fetch_error"):
        error_payload = {"ok": False, "error": data["_fetch_error"]}
        print(json.dumps(error_payload), file=sys.stderr)
        raise SystemExit(1)

    new_msgs = extract_new_messages(data, last_ts)

    if new_msgs:
        # Update state with newest timestamp
        newest_ts = max(m["created_at"] for m in new_msgs)
        state["last_message_ts"] = newest_ts
        state["last_checked_at"] = datetime.now(timezone.utc).isoformat()
        save_state(state)

        # Notify Discord
        notify_summary = notify_discord(new_msgs)

        if not quiet:
            print(json.dumps({"ok": True, "new_messages": len(new_msgs), "items": new_msgs, "notify": notify_summary}))
    else:
        state["last_checked_at"] = datetime.now(timezone.utc).isoformat()
        save_state(state)
        if not quiet:
            print(json.dumps({"ok": True, "new_messages": 0}))


if __name__ == "__main__":
    quiet = "--quiet" in sys.argv
    check(quiet=quiet)
