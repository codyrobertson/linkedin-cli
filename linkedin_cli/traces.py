"""Structured autonomy traces, replay artifacts, and reward attribution windows."""

from __future__ import annotations

import hashlib
import json
import math
import uuid
from pathlib import Path
from typing import Any

from linkedin_cli import policy, runtime_contract
from linkedin_cli.session import ExitCode, fail
from linkedin_cli.write import store


TRACE_STATUS_RUNNING = "running"
TRACE_STATUS_COMPLETED = "completed"
TRACE_STATUS_FAILED = "failed"
TRACE_STATUS_CANCELED = "canceled"
WINDOW_STATUS_OPEN = "open"
WINDOW_STATUS_CLOSED = "closed"
DEFAULT_REWARD_WINDOW_HOURS = {
    "publish_post": 72,
    "reply_comment": 48,
    "send_dm": 336,
    "connect": 336,
    "follow": 168,
    "queue_only": 0,
    "noop": 0,
}
PROSPECT_SIGNAL_REWARD_WEIGHTS = {
    "replied_dm": 3.0,
    "inbound_dm": 1.5,
    "accepted": 2.0,
    "manual_positive": 4.0,
    "profile_view": 0.8,
    "commented": 1.0,
    "liked": 0.4,
    "reposted": 1.2,
    "followed": 0.5,
}


def _json(value: Any) -> str:
    return json.dumps(value or {}, ensure_ascii=False, sort_keys=True)


def _ensure_column(conn: Any, table: str, column: str, ddl: str) -> None:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    existing = {row["name"] if hasattr(row, "keys") else row[1] for row in rows}
    if column not in existing:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {ddl}")


def init_trace_db() -> None:
    conn = store._connect()
    try:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS autonomy_traces (
                trace_id TEXT PRIMARY KEY,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                trace_type TEXT NOT NULL,
                status TEXT NOT NULL,
                request_kind TEXT NOT NULL,
                context_key TEXT,
                root_entity_kind TEXT,
                root_entity_key TEXT,
                reward_spec_json TEXT NOT NULL DEFAULT '{}',
                metadata_json TEXT NOT NULL DEFAULT '{}',
                summary_json TEXT NOT NULL DEFAULT '{}'
            );

            CREATE INDEX IF NOT EXISTS idx_autonomy_traces_lookup
                ON autonomy_traces(trace_type, status, created_at DESC);

            CREATE TABLE IF NOT EXISTS autonomy_trace_steps (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                trace_id TEXT NOT NULL,
                step_index INTEGER NOT NULL,
                step_kind TEXT NOT NULL,
                step_name TEXT NOT NULL,
                status TEXT NOT NULL,
                started_at TEXT NOT NULL,
                finished_at TEXT,
                input_json TEXT NOT NULL DEFAULT '{}',
                output_json TEXT NOT NULL DEFAULT '{}',
                metadata_json TEXT NOT NULL DEFAULT '{}',
                artifact_relpath TEXT,
                FOREIGN KEY (trace_id) REFERENCES autonomy_traces(trace_id) ON DELETE CASCADE
            );

            CREATE UNIQUE INDEX IF NOT EXISTS idx_autonomy_trace_steps_unique
                ON autonomy_trace_steps(trace_id, step_index);

            CREATE TABLE IF NOT EXISTS autonomy_reward_windows (
                window_id TEXT PRIMARY KEY,
                trace_id TEXT NOT NULL,
                decision_id TEXT,
                action_type TEXT NOT NULL,
                entity_kind TEXT NOT NULL,
                entity_key TEXT NOT NULL,
                status TEXT NOT NULL,
                opened_at TEXT NOT NULL,
                closes_at TEXT NOT NULL,
                closed_at TEXT,
                reward_spec_json TEXT NOT NULL DEFAULT '{}',
                baseline_json TEXT NOT NULL DEFAULT '{}',
                last_observed_json TEXT NOT NULL DEFAULT '{}',
                metadata_json TEXT NOT NULL DEFAULT '{}',
                FOREIGN KEY (trace_id) REFERENCES autonomy_traces(trace_id) ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS idx_autonomy_reward_windows_lookup
                ON autonomy_reward_windows(entity_kind, entity_key, status, closes_at DESC);

            CREATE TABLE IF NOT EXISTS autonomy_reward_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                window_id TEXT NOT NULL,
                trace_id TEXT NOT NULL,
                decision_id TEXT,
                source_event_id TEXT,
                reward_type TEXT NOT NULL,
                reward_value REAL NOT NULL,
                event_time TEXT NOT NULL,
                payload_json TEXT NOT NULL DEFAULT '{}',
                dedupe_key TEXT,
                FOREIGN KEY (window_id) REFERENCES autonomy_reward_windows(window_id) ON DELETE CASCADE,
                FOREIGN KEY (trace_id) REFERENCES autonomy_traces(trace_id) ON DELETE CASCADE
            );

            CREATE UNIQUE INDEX IF NOT EXISTS idx_autonomy_reward_events_dedupe
                ON autonomy_reward_events(dedupe_key);
            """
        )
        _ensure_column(conn, "autonomy_trace_steps", "artifact_relpath", "TEXT")
        conn.commit()
    finally:
        conn.close()


def trace_dir(trace_id: str) -> Path:
    return store.ARTIFACTS_DIR / "traces" / trace_id


def write_trace_artifact(trace_id: str, step_index: int, kind: str, payload: Any) -> str:
    target_dir = trace_dir(trace_id)
    target_dir.mkdir(parents=True, exist_ok=True)
    path = target_dir / f"{step_index:03d}-{kind}.json"
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    return str(path.relative_to(store.ARTIFACTS_DIR))


def _trace_row_to_dict(row: Any) -> dict[str, Any]:
    item = dict(row)
    item["reward_spec"] = json.loads(item.pop("reward_spec_json") or "{}")
    item["metadata"] = json.loads(item.pop("metadata_json") or "{}")
    item["summary"] = json.loads(item.pop("summary_json") or "{}")
    return item


def _step_row_to_dict(row: Any) -> dict[str, Any]:
    item = dict(row)
    item["input"] = json.loads(item.pop("input_json") or "{}")
    item["output"] = json.loads(item.pop("output_json") or "{}")
    item["metadata"] = json.loads(item.pop("metadata_json") or "{}")
    return item


def _window_row_to_dict(row: Any) -> dict[str, Any]:
    item = dict(row)
    item["reward_spec"] = json.loads(item.pop("reward_spec_json") or "{}")
    item["baseline"] = json.loads(item.pop("baseline_json") or "{}")
    item["last_observed"] = json.loads(item.pop("last_observed_json") or "{}")
    item["metadata"] = json.loads(item.pop("metadata_json") or "{}")
    return item


def _reward_event_row_to_dict(row: Any) -> dict[str, Any]:
    item = dict(row)
    item["payload"] = json.loads(item.pop("payload_json") or "{}")
    return item


def start_trace(
    *,
    trace_type: str,
    request_kind: str,
    context_key: str | None = None,
    root_entity_kind: str | None = None,
    root_entity_key: str | None = None,
    reward_spec: dict[str, Any] | None = None,
    metadata: dict[str, Any] | None = None,
    trace_id: str | None = None,
) -> dict[str, Any]:
    init_trace_db()
    now = store._now_iso()
    trace_id = trace_id or f"tr_{uuid.uuid4().hex[:16]}"
    conn = store._connect()
    try:
        conn.execute(
            """
            INSERT INTO autonomy_traces
            (trace_id, created_at, updated_at, trace_type, status, request_kind, context_key, root_entity_kind, root_entity_key,
             reward_spec_json, metadata_json, summary_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '{}')
            """,
            (
                trace_id,
                now,
                now,
                str(trace_type or "").strip() or "generic",
                TRACE_STATUS_RUNNING,
                str(request_kind or "").strip() or "generic",
                context_key,
                root_entity_kind,
                root_entity_key,
                _json(reward_spec or runtime_contract.CONTENT_REWARD_SPEC),
                _json(metadata),
            ),
        )
        conn.commit()
        row = conn.execute("SELECT * FROM autonomy_traces WHERE trace_id = ?", (trace_id,)).fetchone()
        assert row is not None
        return _trace_row_to_dict(row)
    finally:
        conn.close()


def append_trace_step(
    trace_id: str,
    *,
    step_kind: str,
    step_name: str,
    status: str = "completed",
    input_payload: dict[str, Any] | None = None,
    output_payload: dict[str, Any] | None = None,
    metadata: dict[str, Any] | None = None,
    artifact_kind: str | None = None,
    artifact_payload: Any | None = None,
    started_at: str | None = None,
    finished_at: str | None = None,
) -> dict[str, Any]:
    init_trace_db()
    now = store._now_iso()
    started = started_at or now
    finished = finished_at or now
    conn = store._connect()
    try:
        trace = conn.execute("SELECT 1 FROM autonomy_traces WHERE trace_id = ?", (trace_id,)).fetchone()
        if trace is None:
            fail(f"Trace not found: {trace_id}", code=ExitCode.NOT_FOUND)
        next_index = int(
            conn.execute("SELECT COALESCE(MAX(step_index), 0) + 1 FROM autonomy_trace_steps WHERE trace_id = ?", (trace_id,)).fetchone()[0]
            or 1
        )
        artifact_relpath = None
        if artifact_kind:
            artifact_relpath = write_trace_artifact(trace_id, next_index, artifact_kind, artifact_payload if artifact_payload is not None else output_payload)
        conn.execute(
            """
            INSERT INTO autonomy_trace_steps
            (trace_id, step_index, step_kind, step_name, status, started_at, finished_at, input_json, output_json, metadata_json, artifact_relpath)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                trace_id,
                next_index,
                step_kind,
                step_name,
                status,
                started,
                finished,
                _json(input_payload),
                _json(output_payload),
                _json(metadata),
                artifact_relpath,
            ),
        )
        conn.execute("UPDATE autonomy_traces SET updated_at = ? WHERE trace_id = ?", (finished, trace_id))
        conn.commit()
        row = conn.execute(
            "SELECT * FROM autonomy_trace_steps WHERE trace_id = ? AND step_index = ?",
            (trace_id, next_index),
        ).fetchone()
        assert row is not None
        return _step_row_to_dict(row)
    finally:
        conn.close()


def finalize_trace(
    trace_id: str,
    *,
    status: str,
    summary: dict[str, Any] | None = None,
    metadata_patch: dict[str, Any] | None = None,
) -> dict[str, Any]:
    init_trace_db()
    now = store._now_iso()
    conn = store._connect()
    try:
        row = conn.execute("SELECT * FROM autonomy_traces WHERE trace_id = ?", (trace_id,)).fetchone()
        if row is None:
            fail(f"Trace not found: {trace_id}", code=ExitCode.NOT_FOUND)
        current = _trace_row_to_dict(row)
        merged_metadata = dict(current.get("metadata") or {})
        merged_metadata.update(metadata_patch or {})
        merged_summary = dict(current.get("summary") or {})
        merged_summary.update(summary or {})
        conn.execute(
            """
            UPDATE autonomy_traces
            SET updated_at = ?, status = ?, metadata_json = ?, summary_json = ?
            WHERE trace_id = ?
            """,
            (now, status, _json(merged_metadata), _json(merged_summary), trace_id),
        )
        conn.commit()
        refreshed = conn.execute("SELECT * FROM autonomy_traces WHERE trace_id = ?", (trace_id,)).fetchone()
        assert refreshed is not None
        return get_trace(trace_id)
    finally:
        conn.close()


def list_traces(limit: int = 20, *, trace_type: str | None = None, status: str | None = None) -> list[dict[str, Any]]:
    init_trace_db()
    where: list[str] = []
    params: list[Any] = []
    if trace_type:
        where.append("trace_type = ?")
        params.append(trace_type)
    if status:
        where.append("status = ?")
        params.append(status)
    where_sql = f"WHERE {' AND '.join(where)}" if where else ""
    conn = store._connect()
    try:
        rows = conn.execute(
            f"SELECT * FROM autonomy_traces {where_sql} ORDER BY updated_at DESC, created_at DESC LIMIT ?",
            (*params, max(1, int(limit))),
        ).fetchall()
        return [_trace_row_to_dict(row) for row in rows]
    finally:
        conn.close()


def get_trace(trace_id: str, *, include_steps: bool = True, include_reward_windows: bool = True, include_reward_events: bool = True) -> dict[str, Any]:
    init_trace_db()
    conn = store._connect()
    try:
        row = conn.execute("SELECT * FROM autonomy_traces WHERE trace_id = ?", (trace_id,)).fetchone()
        if row is None:
            fail(f"Trace not found: {trace_id}", code=ExitCode.NOT_FOUND)
        payload = _trace_row_to_dict(row)
        if include_steps:
            step_rows = conn.execute(
                "SELECT * FROM autonomy_trace_steps WHERE trace_id = ? ORDER BY step_index ASC",
                (trace_id,),
            ).fetchall()
            payload["steps"] = [_step_row_to_dict(item) for item in step_rows]
        if include_reward_windows:
            window_rows = conn.execute(
                "SELECT * FROM autonomy_reward_windows WHERE trace_id = ? ORDER BY opened_at ASC",
                (trace_id,),
            ).fetchall()
            payload["reward_windows"] = [_window_row_to_dict(item) for item in window_rows]
        if include_reward_events:
            event_rows = conn.execute(
                "SELECT * FROM autonomy_reward_events WHERE trace_id = ? ORDER BY event_time ASC, id ASC",
                (trace_id,),
            ).fetchall()
            payload["reward_events"] = [_reward_event_row_to_dict(item) for item in event_rows]
        return payload
    finally:
        conn.close()


def export_trace(trace_id: str, *, output_path: str | Path | None = None) -> dict[str, Any]:
    payload = get_trace(trace_id)
    target = Path(output_path) if output_path else (trace_dir(trace_id) / "trace-export.json")
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    return {"trace_id": trace_id, "output_path": str(target), "trace": payload}


def _reward_hours_for_action(action_type: str, reward_window_hours: int | None = None) -> int:
    if reward_window_hours is not None:
        return max(0, int(reward_window_hours))
    return int(DEFAULT_REWARD_WINDOW_HOURS.get(str(action_type or ""), 72))


def open_reward_window(
    *,
    trace_id: str,
    action_type: str,
    entity_kind: str,
    entity_key: str,
    decision_id: str | None = None,
    reward_window_hours: int | None = None,
    opened_at: str | None = None,
    baseline: dict[str, Any] | None = None,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    init_trace_db()
    window_id = f"rw_{uuid.uuid4().hex[:16]}"
    opened = opened_at or store._now_iso()
    hours = _reward_hours_for_action(action_type, reward_window_hours)
    closes_at = store._now_iso()
    if hours > 0:
        from datetime import datetime, timedelta, timezone

        opened_dt = datetime.fromisoformat(opened.replace("Z", "+00:00"))
        closes_at = (opened_dt + timedelta(hours=hours)).astimezone(timezone.utc).isoformat()
    conn = store._connect()
    try:
        conn.execute(
            """
            INSERT INTO autonomy_reward_windows
            (window_id, trace_id, decision_id, action_type, entity_kind, entity_key, status, opened_at, closes_at, reward_spec_json,
             baseline_json, last_observed_json, metadata_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                window_id,
                trace_id,
                decision_id,
                action_type,
                entity_kind,
                entity_key,
                WINDOW_STATUS_OPEN if hours > 0 else WINDOW_STATUS_CLOSED,
                opened,
                closes_at,
                _json(runtime_contract.CONTENT_REWARD_SPEC),
                _json(baseline),
                _json(baseline),
                _json(metadata),
            ),
        )
        conn.commit()
        row = conn.execute("SELECT * FROM autonomy_reward_windows WHERE window_id = ?", (window_id,)).fetchone()
        assert row is not None
        return _window_row_to_dict(row)
    finally:
        conn.close()


def list_reward_windows(*, trace_id: str | None = None, status: str | None = None, limit: int = 50) -> list[dict[str, Any]]:
    init_trace_db()
    where: list[str] = []
    params: list[Any] = []
    if trace_id:
        where.append("trace_id = ?")
        params.append(trace_id)
    if status:
        where.append("status = ?")
        params.append(status)
    where_sql = f"WHERE {' AND '.join(where)}" if where else ""
    conn = store._connect()
    try:
        rows = conn.execute(
            f"SELECT * FROM autonomy_reward_windows {where_sql} ORDER BY opened_at DESC LIMIT ?",
            (*params, max(1, int(limit))),
        ).fetchall()
        return [_window_row_to_dict(row) for row in rows]
    finally:
        conn.close()


def close_expired_reward_windows(*, now: str | None = None) -> int:
    init_trace_db()
    current = now or store._now_iso()
    conn = store._connect()
    try:
        result = conn.execute(
            """
            UPDATE autonomy_reward_windows
            SET status = ?, closed_at = COALESCE(closed_at, ?)
            WHERE status = ? AND closes_at <= ?
            """,
            (WINDOW_STATUS_CLOSED, current, WINDOW_STATUS_OPEN, current),
        )
        conn.commit()
        return int(result.rowcount or 0)
    finally:
        conn.close()


def _log1p_count(value: Any) -> float:
    return round(math.log1p(max(0.0, float(value or 0.0))), 6)


def _telemetry_reward_from_snapshot(snapshot: dict[str, Any]) -> dict[str, Any]:
    return runtime_contract.compute_content_reward(
        {
            "reaction_log1p": _log1p_count(snapshot.get("reaction_count")),
            "comment_log1p": _log1p_count(snapshot.get("comment_count")),
            "repost_log1p": _log1p_count(snapshot.get("repost_count")),
            "profile_view_log1p": _log1p_count(snapshot.get("profile_view_count")),
            "dm_reply": float(snapshot.get("dm_reply") or 0.0),
            "meeting_booked": float(snapshot.get("meeting_booked") or 0.0),
            "negative_feedback": float(snapshot.get("negative_feedback") or 0.0),
        }
    )


def _prospect_signal_reward(signal_type: str, metadata: dict[str, Any] | None = None) -> float:
    signal = str(signal_type or "").strip()
    reward = float(PROSPECT_SIGNAL_REWARD_WEIGHTS.get(signal, 0.0))
    details = metadata or {}
    if signal == "manual_positive" and details.get("meeting_booked"):
        reward += 6.0
    return round(reward, 6)


def attribute_prospect_signal(
    *,
    profile_key: str,
    signal_type: str,
    source: str,
    metadata: dict[str, Any] | None = None,
    dedupe_key: str | None = None,
    event_time: str | None = None,
) -> dict[str, Any]:
    init_trace_db()
    current_time = event_time or store._now_iso()
    close_expired_reward_windows(now=current_time)
    reward_value = _prospect_signal_reward(signal_type, metadata)
    if reward_value == 0.0:
        return {"attributed_count": 0, "events": []}
    conn = store._connect()
    attributed: list[dict[str, Any]] = []
    try:
        window_rows = conn.execute(
            """
            SELECT * FROM autonomy_reward_windows
            WHERE entity_kind = 'prospect' AND entity_key = ? AND opened_at <= ? AND closes_at >= ?
            ORDER BY opened_at ASC
            """,
            (profile_key, current_time, current_time),
        ).fetchall()
        for row in window_rows:
            window = _window_row_to_dict(row)
            detail = {
                "source": source,
                "signal_type": signal_type,
                "metadata": metadata or {},
            }
            event_dedupe_key = (
                f"{window['window_id']}:prospect_signal:"
                f"{dedupe_key or hashlib.sha1(_json(detail).encode('utf-8')).hexdigest()[:12]}"
            )
            conn.execute(
                """
                INSERT INTO autonomy_reward_events
                (window_id, trace_id, decision_id, source_event_id, reward_type, reward_value, event_time, payload_json, dedupe_key)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(dedupe_key) DO NOTHING
                """,
                (
                    window["window_id"],
                    window["trace_id"],
                    window.get("decision_id"),
                    dedupe_key,
                    "prospect_signal",
                    float(reward_value),
                    current_time,
                    _json(detail),
                    event_dedupe_key,
                ),
            )
            conn.commit()
            event_row = conn.execute(
                "SELECT * FROM autonomy_reward_events WHERE dedupe_key = ?",
                (event_dedupe_key,),
            ).fetchone()
            if event_row is None:
                continue
            reward_event = _reward_event_row_to_dict(event_row)
            attributed.append(reward_event)
            if window.get("decision_id"):
                policy.record_reward(
                    str(window["decision_id"]),
                    reward_type="prospect_signal",
                    reward_value=float(reward_value),
                    payload=reward_event["payload"],
                    dedupe_key=f"policy:{event_dedupe_key}",
                    trace_id=window["trace_id"],
                    window_id=window["window_id"],
                    reward_source="trace_reward_window",
                    event_time=current_time,
                )
        return {"attributed_count": len(attributed), "events": attributed}
    finally:
        conn.close()


def attribute_telemetry_event(
    *,
    entity_kind: str,
    entity_key: str,
    event_type: str,
    event_time: str,
    payload: dict[str, Any] | None,
    source: str,
    source_event_id: str | int | None = None,
) -> dict[str, Any]:
    init_trace_db()
    close_expired_reward_windows(now=event_time)
    conn = store._connect()
    attributed: list[dict[str, Any]] = []
    try:
        window_rows = conn.execute(
            """
            SELECT * FROM autonomy_reward_windows
            WHERE entity_kind = ? AND entity_key = ? AND opened_at <= ? AND closes_at >= ?
            ORDER BY opened_at ASC
            """,
            (entity_kind, entity_key, event_time, event_time),
        ).fetchall()
        for row in window_rows:
            window = _window_row_to_dict(row)
            reward_payload = payload or {}
            reward_value = 0.0
            detail: dict[str, Any] = {"source": source, "event_type": event_type}
            if event_type == "reaction_snapshot":
                previous_snapshot = window.get("last_observed") or window.get("baseline") or {}
                previous_reward = _telemetry_reward_from_snapshot(previous_snapshot)
                current_reward = _telemetry_reward_from_snapshot(reward_payload)
                reward_value = round(
                    float(current_reward["total_reward"]) - float(previous_reward["total_reward"]),
                    6,
                )
                detail.update(
                    {
                        "previous_snapshot": previous_snapshot,
                        "current_snapshot": reward_payload,
                        "current_reward": current_reward,
                        "previous_reward": previous_reward,
                    }
                )
            else:
                continue
            dedupe_key = (
                f"{window['window_id']}:{event_type}:"
                f"{source_event_id if source_event_id is not None else hashlib.sha1(_json(reward_payload).encode('utf-8')).hexdigest()[:12]}"
            )
            conn.execute(
                """
                INSERT INTO autonomy_reward_events
                (window_id, trace_id, decision_id, source_event_id, reward_type, reward_value, event_time, payload_json, dedupe_key)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(dedupe_key) DO NOTHING
                """,
                (
                    window["window_id"],
                    window["trace_id"],
                    window.get("decision_id"),
                    str(source_event_id) if source_event_id is not None else None,
                    "telemetry_window_delta",
                    float(reward_value),
                    event_time,
                    _json(detail),
                    dedupe_key,
                ),
            )
            conn.execute(
                "UPDATE autonomy_reward_windows SET last_observed_json = ? WHERE window_id = ?",
                (_json(reward_payload), window["window_id"]),
            )
            conn.commit()
            event_row = conn.execute(
                "SELECT * FROM autonomy_reward_events WHERE dedupe_key = ?",
                (dedupe_key,),
            ).fetchone()
            if event_row is None:
                continue
            reward_event = _reward_event_row_to_dict(event_row)
            attributed.append(reward_event)
            if window.get("decision_id"):
                policy.record_reward(
                    str(window["decision_id"]),
                    reward_type="telemetry_window_delta",
                    reward_value=float(reward_value),
                    payload=reward_event["payload"],
                    dedupe_key=f"policy:{dedupe_key}",
                    trace_id=window["trace_id"],
                    window_id=window["window_id"],
                    reward_source="trace_reward_window",
                    event_time=event_time,
                )
        return {"attributed_count": len(attributed), "events": attributed}
    finally:
        conn.close()
