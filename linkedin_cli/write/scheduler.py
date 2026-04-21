"""LinkedIn content scheduler.

Checks for actions with action_type='post.scheduled' that are due,
and executes them. Designed to be called from a cron job.

Usage:
    python3 -m linkedin_cli.write.scheduler tick
"""

from __future__ import annotations

import json
import logging
import sqlite3
from datetime import datetime, timezone

from linkedin_cli.config import CONFIG_DIR

logger = logging.getLogger(__name__)

DB_PATH = CONFIG_DIR / "state.sqlite"


def _local_timezone():
    return datetime.now().astimezone().tzinfo


def _parse_scheduled_at(value: str, *, local_tz=None) -> datetime:
    text = str(value or "").strip()
    if not text:
        raise ValueError("scheduled_at is empty")
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=local_tz or _local_timezone())
    return parsed.astimezone(timezone.utc)


def get_due_scheduled_posts(now: datetime | None = None) -> list[dict]:
    """Find scheduled posts that are due for execution."""
    if not DB_PATH.exists():
        return []

    current = now or datetime.now(timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=_local_timezone())
    current_utc = current.astimezone(timezone.utc)
    local_tz = _local_timezone()
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    try:
        cursor = conn.execute(
            """
            SELECT * FROM actions
            WHERE action_type = 'post.scheduled'
              AND state IN ('planned', 'dry_run')
              AND scheduled_at IS NOT NULL
            ORDER BY scheduled_at ASC
            """,
        )
        rows = cursor.fetchall()
        due: list[tuple[datetime, dict]] = []
        for row in rows:
            item = dict(row)
            try:
                scheduled_at = _parse_scheduled_at(str(item.get("scheduled_at") or ""), local_tz=local_tz)
            except Exception as exc:
                logger.warning("Skipping invalid scheduled_at for action %s: %s", item.get("action_id"), exc)
                continue
            if scheduled_at <= current_utc:
                due.append((scheduled_at, item))
        due.sort(key=lambda pair: pair[0])
        return [item for _, item in due]
    finally:
        conn.close()


def tick(dry_run: bool = False) -> dict:
    """Check for due scheduled posts and execute them.

    Returns summary of what was processed.
    """
    from linkedin_cli.session import load_env_file, load_session
    from linkedin_cli.write.executor import execute_action
    from linkedin_cli.write.store import init_db, update_state

    load_env_file()
    init_db()

    due = get_due_scheduled_posts()
    if not due:
        return {"ok": True, "due": 0, "executed": 0}

    session, _ = load_session(required=True)
    if session is None:
        return {"ok": False, "error": "No LinkedIn session"}

    executed = []
    errors = []

    for action in due:
        action_id = action["action_id"]
        plan = json.loads(action["plan_json"]) if action.get("plan_json") else {}
        account_id = action.get("account_id", "")

        if dry_run:
            executed.append({"action_id": action_id, "status": "would_execute"})
            continue

        try:
            # Mark as no longer dry_run so executor will actually POST
            update_state(action_id, "planned")
            result = execute_action(
                session=session,
                action_id=action_id,
                plan=plan,
                account_id=account_id,
                dry_run=False,
            )
            executed.append({"action_id": action_id, "result": result})
        except Exception as e:
            errors.append({"action_id": action_id, "error": str(e)})

    return {
        "ok": len(errors) == 0,
        "due": len(due),
        "executed": len(executed),
        "errors": errors,
        "items": executed,
    }


if __name__ == "__main__":
    import sys

    dry = "--dry-run" in sys.argv
    result = tick(dry_run=dry)
    print(json.dumps(result, indent=2))
