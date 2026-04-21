"""Operational guardrails for long-running LinkedIn write agents."""

from __future__ import annotations

import os
from datetime import datetime, time as clock_time, timedelta, timezone
from typing import Any

from linkedin_cli.write import store

DEFAULT_MAX_HOURLY = 20
DEFAULT_MAX_DAILY = 100
DEFAULT_STALE_MINUTES = 30


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _guards_enabled() -> bool:
    return os.getenv("LINKEDIN_WRITE_GUARDS", "1").strip().lower() not in {"0", "false", "off", "no"}


def _parse_clock(value: str) -> clock_time:
    hour, minute = value.strip().split(":", 1)
    return clock_time(hour=int(hour), minute=int(minute))


def _in_quiet_hours(now: datetime, quiet_hours: str | None) -> bool:
    raw = str(quiet_hours or "").strip()
    if not raw:
        return False
    try:
        start_raw, end_raw = raw.split("-", 1)
        start = _parse_clock(start_raw)
        end = _parse_clock(end_raw)
    except Exception:
        return False
    current = now.astimezone().time().replace(second=0, microsecond=0)
    if start <= end:
        return start <= current < end
    return current >= start or current < end


def _count_live_actions(account_id: str, since: datetime) -> int:
    conn = store._connect()
    try:
        row = conn.execute(
            """
            SELECT COUNT(*)
            FROM actions
            WHERE account_id = ?
              AND dry_run = 0
              AND state IN ('executing', 'unknown_remote_state', 'retry_scheduled', 'succeeded', 'failed')
              AND updated_at >= ?
            """,
            (account_id, since.astimezone(timezone.utc).isoformat()),
        ).fetchone()
        return int((row[0] if row else 0) or 0)
    finally:
        conn.close()


def evaluate_write_guard(
    account_id: str,
    action_type: str,
    *,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Return whether a live write is allowed under local long-run guardrails."""
    current = now or datetime.now(timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    current = current.astimezone(timezone.utc)
    if not _guards_enabled():
        return {"allowed": True, "risk_flags": [], "details": {"enabled": False}}

    risk_flags: list[str] = []
    details: dict[str, Any] = {
        "enabled": True,
        "action_type": action_type,
        "account_id": account_id,
    }

    quiet_hours = os.getenv("LINKEDIN_WRITE_QUIET_HOURS", "").strip()
    if _in_quiet_hours(current, quiet_hours):
        risk_flags.append("quiet_hours")
        return {
            "allowed": False,
            "reason": f"Write blocked by LINKEDIN_WRITE_QUIET_HOURS={quiet_hours}",
            "risk_flags": risk_flags,
            "details": details,
        }

    max_hourly = max(0, _env_int("LINKEDIN_WRITE_MAX_HOURLY", DEFAULT_MAX_HOURLY))
    max_daily = max(0, _env_int("LINKEDIN_WRITE_MAX_DAILY", DEFAULT_MAX_DAILY))
    hourly_count = _count_live_actions(account_id, current - timedelta(hours=1))
    daily_count = _count_live_actions(account_id, current - timedelta(hours=24))
    details.update(
        {
            "max_hourly": max_hourly,
            "max_daily": max_daily,
            "hourly_count": hourly_count,
            "daily_count": daily_count,
        }
    )
    if max_hourly and hourly_count >= max_hourly:
        risk_flags.append("hourly_write_budget")
        return {
            "allowed": False,
            "reason": f"Hourly write budget reached for account {account_id}: {hourly_count}/{max_hourly}",
            "risk_flags": risk_flags,
            "details": details,
        }
    if max_daily and daily_count >= max_daily:
        risk_flags.append("daily_write_budget")
        return {
            "allowed": False,
            "reason": f"Daily write budget reached for account {account_id}: {daily_count}/{max_daily}",
            "risk_flags": risk_flags,
            "details": details,
        }
    return {"allowed": True, "risk_flags": risk_flags, "details": details}


def _parse_iso(value: str | None) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _row_to_public_action(row: Any) -> dict[str, Any]:
    item = dict(row)
    item.pop("plan_json", None)
    return item


def action_health_report(*, now: datetime | None = None, stale_minutes: int | None = None) -> dict[str, Any]:
    """Summarize action states that need operator attention."""
    current = now or datetime.now(timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    current = current.astimezone(timezone.utc)
    stale_after = max(1, int(stale_minutes if stale_minutes is not None else _env_int("LINKEDIN_WRITE_HEALTH_STALE_MINUTES", DEFAULT_STALE_MINUTES)))
    stale_cutoff = current - timedelta(minutes=stale_after)

    conn = store._connect()
    try:
        executing_rows = conn.execute(
            "SELECT * FROM actions WHERE state = 'executing' ORDER BY updated_at ASC"
        ).fetchall()
        unknown_rows = conn.execute(
            "SELECT * FROM actions WHERE state = 'unknown_remote_state' ORDER BY updated_at ASC"
        ).fetchall()
        retry_rows = conn.execute(
            "SELECT * FROM actions WHERE state = 'retry_scheduled' ORDER BY next_attempt_at ASC"
        ).fetchall()
        scheduled_rows = conn.execute(
            """
            SELECT * FROM actions
            WHERE action_type = 'post.scheduled'
              AND state IN ('planned', 'dry_run')
              AND scheduled_at IS NOT NULL
            ORDER BY scheduled_at ASC
            """
        ).fetchall()
    finally:
        conn.close()

    stuck_executing = [
        _row_to_public_action(row)
        for row in executing_rows
        if (_parse_iso(row["updated_at"]) or current) <= stale_cutoff
    ]
    due_retries = [
        _row_to_public_action(row)
        for row in retry_rows
        if (parsed := _parse_iso(row["next_attempt_at"])) is not None and parsed <= current
    ]
    overdue_scheduled = [
        _row_to_public_action(row)
        for row in scheduled_rows
        if (parsed := _parse_iso(row["scheduled_at"])) is not None and parsed <= current
    ]
    unknown_remote_state = [_row_to_public_action(row) for row in unknown_rows]

    status = "ok"
    if stuck_executing or unknown_remote_state:
        status = "needs_attention"
    elif due_retries or overdue_scheduled:
        status = "work_pending"

    return {
        "status": status,
        "checked_at": current.isoformat(),
        "stale_minutes": stale_after,
        "counts": {
            "stuck_executing": len(stuck_executing),
            "unknown_remote_state": len(unknown_remote_state),
            "due_retries": len(due_retries),
            "overdue_scheduled": len(overdue_scheduled),
        },
        "stuck_executing": stuck_executing,
        "unknown_remote_state": unknown_remote_state,
        "due_retries": due_retries,
        "overdue_scheduled": overdue_scheduled,
    }
