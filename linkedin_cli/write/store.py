"""SQLite action store for LinkedIn write operations.

Provides durable persistence for action plans, state transitions,
and attempt tracking.
"""

from __future__ import annotations

import json
import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from linkedin_cli.config import CONFIG_DIR

DB_PATH = CONFIG_DIR / "state.sqlite"
ARTIFACTS_DIR = CONFIG_DIR / "artifacts"
SQLITE_TIMEOUT_SECONDS = 120.0
SQLITE_BUSY_TIMEOUT_MS = 120_000

# Valid state transitions
VALID_STATES = {
    "planned",
    "dry_run",
    "executing",
    "unknown_remote_state",
    "retry_scheduled",
    "succeeded",
    "failed",
    "duplicate_skipped",
    "blocked",
    "canceled",
}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _connect() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH), timeout=SQLITE_TIMEOUT_SECONDS)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute(f"PRAGMA busy_timeout={SQLITE_BUSY_TIMEOUT_MS}")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db() -> None:
    """Create tables if they don't exist."""
    conn = _connect()
    try:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS actions (
                action_id TEXT PRIMARY KEY,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                account_id TEXT NOT NULL,
                action_type TEXT NOT NULL,
                target_key TEXT NOT NULL,
                idempotency_key TEXT NOT NULL,
                desired_fingerprint TEXT,
                state TEXT NOT NULL DEFAULT 'planned',
                dry_run INTEGER NOT NULL DEFAULT 1,
                plan_json TEXT,
                last_error TEXT,
                remote_ref TEXT,
                attempt_count INTEGER NOT NULL DEFAULT 0,
                next_attempt_at TEXT,
                risk_flags TEXT,
                scheduled_at TEXT
            );

            CREATE UNIQUE INDEX IF NOT EXISTS idx_actions_idempotency
                ON actions(account_id, idempotency_key);

            CREATE INDEX IF NOT EXISTS idx_actions_state
                ON actions(state);

            CREATE INDEX IF NOT EXISTS idx_actions_created
                ON actions(created_at);

            CREATE INDEX IF NOT EXISTS idx_actions_account_updated
                ON actions(account_id, updated_at);

            CREATE TABLE IF NOT EXISTS attempts (
                attempt_id TEXT PRIMARY KEY,
                action_id TEXT NOT NULL,
                attempt_no INTEGER NOT NULL,
                started_at TEXT NOT NULL,
                finished_at TEXT,
                request_method TEXT,
                request_path TEXT,
                http_status INTEGER,
                outcome TEXT,
                error TEXT,
                FOREIGN KEY (action_id) REFERENCES actions(action_id)
            );

            CREATE INDEX IF NOT EXISTS idx_attempts_action
                ON attempts(action_id);

            CREATE TABLE IF NOT EXISTS telemetry_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                entity_kind TEXT NOT NULL,
                entity_key TEXT NOT NULL,
                event_type TEXT NOT NULL,
                event_time TEXT NOT NULL,
                source TEXT NOT NULL,
                payload_json TEXT NOT NULL DEFAULT '{}',
                dedupe_key TEXT UNIQUE
            );

            CREATE INDEX IF NOT EXISTS idx_telemetry_events_time
                ON telemetry_events(event_time DESC);

            CREATE INDEX IF NOT EXISTS idx_telemetry_events_kind
                ON telemetry_events(entity_kind, event_type);
        """)
        conn.commit()
    finally:
        conn.close()


def append_telemetry_event(
    entity_kind: str,
    entity_key: str,
    event_type: str,
    dedupe_key: str | None,
    payload: dict[str, Any] | None,
    *,
    source: str,
    event_time: str | None = None,
) -> dict[str, Any]:
    now = event_time or _now_iso()
    serialized = json.dumps(payload or {}, ensure_ascii=False, sort_keys=True)
    conn = _connect()
    try:
        conn.execute(
            """
            INSERT INTO telemetry_events
            (entity_kind, entity_key, event_type, event_time, source, payload_json, dedupe_key)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(dedupe_key) DO NOTHING
            """,
            (entity_kind, entity_key, event_type, now, source, serialized, dedupe_key),
        )
        conn.commit()
        if dedupe_key:
            row = conn.execute("SELECT * FROM telemetry_events WHERE dedupe_key = ?", (dedupe_key,)).fetchone()
        else:
            row = conn.execute("SELECT * FROM telemetry_events ORDER BY id DESC LIMIT 1").fetchone()
        assert row is not None
        result = dict(row)
        result["payload"] = json.loads(result.pop("payload_json") or "{}")
        return result
    finally:
        conn.close()


def list_telemetry_events(limit: int = 100) -> list[dict[str, Any]]:
    conn = _connect()
    try:
        rows = conn.execute(
            "SELECT * FROM telemetry_events ORDER BY event_time DESC, id DESC LIMIT ?",
            (max(1, int(limit)),),
        ).fetchall()
        events: list[dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            item["payload"] = json.loads(item.pop("payload_json") or "{}")
            events.append(item)
        return events
    finally:
        conn.close()


def telemetry_stats() -> dict[str, Any]:
    conn = _connect()
    try:
        event_count = int(conn.execute("SELECT COUNT(*) FROM telemetry_events").fetchone()[0] or 0)
        rows = conn.execute(
            """
            SELECT entity_kind, event_type, COUNT(*) AS count
            FROM telemetry_events
            GROUP BY entity_kind, event_type
            ORDER BY count DESC
            """
        ).fetchall()
        by_event_type: dict[str, int] = {}
        by_entity_kind: dict[str, int] = {}
        for row in rows:
            entity_kind = str(row["entity_kind"])
            event_type = str(row["event_type"])
            count = int(row["count"] or 0)
            by_event_type[event_type] = by_event_type.get(event_type, 0) + count
            by_entity_kind[entity_kind] = by_entity_kind.get(entity_kind, 0) + count
        return {
            "event_count": event_count,
            "by_event_type": by_event_type,
            "by_entity_kind": by_entity_kind,
        }
    finally:
        conn.close()


def create_action(
    action_id: str,
    action_type: str,
    account_id: str,
    target_key: str,
    idempotency_key: str,
    plan: dict[str, Any],
    dry_run: bool = True,
    scheduled_at: str | None = None,
) -> dict[str, Any]:
    """Insert a new action in 'planned' state. Returns the action row as dict."""
    now = _now_iso()
    state = "dry_run" if dry_run else "planned"
    conn = _connect()
    try:
        conn.execute(
            """INSERT INTO actions
               (action_id, created_at, updated_at, account_id, action_type,
                target_key, idempotency_key, state, dry_run, plan_json, scheduled_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                action_id,
                now,
                now,
                account_id,
                action_type,
                target_key,
                idempotency_key,
                state,
                1 if dry_run else 0,
                json.dumps(plan, ensure_ascii=False),
                scheduled_at,
            ),
        )
        conn.commit()
        write_artifact(action_id, "plan", plan)
        result = get_action(action_id, conn=conn)
        assert result is not None
        return result
    finally:
        conn.close()


def get_action(action_id: str, *, conn: sqlite3.Connection | None = None) -> Optional[dict[str, Any]]:
    """Get action by ID. Returns dict or None."""
    own_conn = conn is None
    if own_conn:
        conn = _connect()
    try:
        row = conn.execute("SELECT * FROM actions WHERE action_id = ?", (action_id,)).fetchone()
        if row is None:
            return None
        d = dict(row)
        # Parse plan_json back to dict
        if d.get("plan_json"):
            try:
                d["plan"] = json.loads(d["plan_json"])
            except Exception:
                d["plan"] = None
        else:
            d["plan"] = None
        return d
    finally:
        if own_conn:
            conn.close()


def find_by_idempotency_key(account_id: str, idempotency_key: str) -> Optional[dict[str, Any]]:
    """Check for existing action with same idempotency key."""
    conn = _connect()
    try:
        row = conn.execute(
            "SELECT * FROM actions WHERE account_id = ? AND idempotency_key = ?",
            (account_id, idempotency_key),
        ).fetchone()
        if row is None:
            return None
        d = dict(row)
        if d.get("plan_json"):
            try:
                d["plan"] = json.loads(d["plan_json"])
            except Exception:
                d["plan"] = None
        else:
            d["plan"] = None
        return d
    finally:
        conn.close()


def update_state(action_id: str, state: str, **kwargs: Any) -> dict[str, Any]:
    """Update action state and optional fields (last_error, remote_ref, attempt_count, etc.)."""
    if state not in VALID_STATES:
        raise ValueError(f"Invalid state: {state}. Must be one of {VALID_STATES}")
    conn = _connect()
    try:
        sets = ["state = ?", "updated_at = ?"]
        params: list[Any] = [state, _now_iso()]
        for key in ("last_error", "remote_ref", "attempt_count", "next_attempt_at", "risk_flags", "dry_run"):
            if key in kwargs:
                sets.append(f"{key} = ?")
                val = kwargs[key]
                if key == "risk_flags" and isinstance(val, (list, dict)):
                    val = json.dumps(val)
                elif key == "dry_run":
                    val = 1 if bool(val) else 0
                params.append(val)
        if "plan" in kwargs:
            sets.append("plan_json = ?")
            params.append(json.dumps(kwargs["plan"], ensure_ascii=False))
        params.append(action_id)
        conn.execute(
            f"UPDATE actions SET {', '.join(sets)} WHERE action_id = ?",
            params,
        )
        conn.commit()
        result = get_action(action_id, conn=conn)
        assert result is not None
        return result
    finally:
        conn.close()


def cancel_action(action_id: str, reason: str | None = None) -> dict[str, Any]:
    """Cancel an action that has not reached a terminal remote-success state."""
    action = get_action(action_id)
    if action is None:
        raise ValueError(f"Action not found: {action_id}")
    if action["state"] in {"succeeded", "canceled"}:
        return action
    canceled = update_state(action_id, "canceled", last_error=reason)
    write_artifact(
        action_id,
        "cancel",
        {
            "action_id": action_id,
            "reason": reason,
            "previous_state": action["state"],
            "state": canceled["state"],
        },
    )
    return canceled


def record_attempt(
    action_id: str,
    attempt_no: int,
    method: str,
    path: str,
    status: Optional[int],
    outcome: str,
    error: Optional[str] = None,
) -> str:
    """Log an execution attempt. Returns attempt_id."""
    attempt_id = f"att_{uuid.uuid4().hex[:12]}"
    now = _now_iso()
    conn = _connect()
    try:
        conn.execute(
            """INSERT INTO attempts
               (attempt_id, action_id, attempt_no, started_at, finished_at,
                request_method, request_path, http_status, outcome, error)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (attempt_id, action_id, attempt_no, now, now, method, path, status, outcome, error),
        )
        # Also bump attempt_count on the action
        conn.execute(
            "UPDATE actions SET attempt_count = attempt_count + 1, updated_at = ? WHERE action_id = ?",
            (now, action_id),
        )
        conn.commit()
        return attempt_id
    finally:
        conn.close()


def list_actions(state: Optional[str] = None, limit: int = 20) -> list[dict[str, Any]]:
    """List recent actions, optionally filtered by state."""
    conn = _connect()
    try:
        if state:
            rows = conn.execute(
                "SELECT * FROM actions WHERE state = ? ORDER BY created_at DESC LIMIT ?",
                (state, limit),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM actions ORDER BY created_at DESC LIMIT ?",
                (limit,),
            ).fetchall()
        results = []
        for row in rows:
            d = dict(row)
            if d.get("plan_json"):
                try:
                    d["plan"] = json.loads(d["plan_json"])
                except Exception:
                    d["plan"] = None
            else:
                d["plan"] = None
            results.append(d)
        return results
    finally:
        conn.close()


def artifact_dir(action_id: str) -> Path:
    return ARTIFACTS_DIR / action_id


def write_artifact(action_id: str, kind: str, payload: Any) -> Path:
    """Persist a JSON artifact for an action and return its path."""
    target_dir = artifact_dir(action_id)
    target_dir.mkdir(parents=True, exist_ok=True)
    path = target_dir / f"{kind}.json"
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    return path


def list_artifacts(action_id: str) -> list[dict[str, Any]]:
    """List JSON artifacts stored for an action."""
    target_dir = artifact_dir(action_id)
    if not target_dir.exists():
        return []
    artifacts: list[dict[str, Any]] = []
    for path in sorted(target_dir.glob("*.json")):
        artifacts.append(
            {
                "kind": path.stem,
                "path": str(path),
                "bytes": path.stat().st_size,
            }
        )
    return artifacts
