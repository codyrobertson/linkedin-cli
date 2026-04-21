"""Terminal dashboard for content autonomy, traces, and rewards."""

from __future__ import annotations

import curses
import json
import textwrap
import time
from datetime import datetime
from typing import Any

from linkedin_cli import content, corpus_curation, policy, traces
from linkedin_cli.session import ExitCode, fail
from linkedin_cli.write import store


VIEW_ORDER = ["overview", "traces", "queue", "decisions", "rewards"]
VIEW_LABELS = {
    "overview": "Overview",
    "traces": "Traces",
    "queue": "Queue",
    "decisions": "Decisions",
    "rewards": "Rewards",
}


def _safe_json(value: str | None, default: Any) -> Any:
    try:
        return json.loads(value or "")
    except Exception:
        return default


def _trim(value: Any, width: int) -> str:
    text = str(value or "")
    if width <= 1:
        return text[:width]
    return text if len(text) <= width else text[: max(0, width - 1)] + "…"


def _time_label(value: str | None) -> str:
    raw = str(value or "").strip()
    if not raw:
        return "-"
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        return parsed.strftime("%m-%d %H:%M")
    except Exception:
        return raw[:16]


def _count_query(conn: Any, sql: str, params: tuple[Any, ...] = ()) -> int:
    try:
        row = conn.execute(sql, params).fetchone()
        return int((row[0] if row else 0) or 0)
    except Exception:
        return 0


def _recent_policy_decisions(limit: int) -> list[dict[str, Any]]:
    policy.init_policy_db()
    conn = store._connect()
    try:
        rows = conn.execute(
            """
            SELECT decision_id, created_at, policy_name, context_type, context_key, chosen_action_id,
                   chosen_score, propensity, metadata_json
            FROM policy_decisions
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (int(limit),),
        ).fetchall()
        decisions: list[dict[str, Any]] = []
        for row in rows:
            metadata = _safe_json(row["metadata_json"] if hasattr(row, "keys") else row[8], {})
            decisions.append(
                {
                    "decision_id": row["decision_id"] if hasattr(row, "keys") else row[0],
                    "created_at": row["created_at"] if hasattr(row, "keys") else row[1],
                    "policy_name": row["policy_name"] if hasattr(row, "keys") else row[2],
                    "context_type": row["context_type"] if hasattr(row, "keys") else row[3],
                    "context_key": row["context_key"] if hasattr(row, "keys") else row[4],
                    "chosen_action_id": row["chosen_action_id"] if hasattr(row, "keys") else row[5],
                    "chosen_score": float((row["chosen_score"] if hasattr(row, "keys") else row[6]) or 0.0),
                    "propensity": float((row["propensity"] if hasattr(row, "keys") else row[7]) or 0.0),
                    "decision_source": str(metadata.get("decision_source") or "local-policy"),
                    "metadata": metadata,
                }
            )
        return decisions
    finally:
        conn.close()


def _recent_reward_events(limit: int) -> list[dict[str, Any]]:
    policy.init_policy_db()
    conn = store._connect()
    try:
        rows = conn.execute(
            """
            SELECT id, event_time, decision_id, reward_type, reward_value, reward_source, trace_id, payload_json
            FROM policy_rewards
            ORDER BY event_time DESC, id DESC
            LIMIT ?
            """,
            (int(limit),),
        ).fetchall()
        events: list[dict[str, Any]] = []
        for row in rows:
            events.append(
                {
                    "id": row["id"] if hasattr(row, "keys") else row[0],
                    "event_time": row["event_time"] if hasattr(row, "keys") else row[1],
                    "decision_id": row["decision_id"] if hasattr(row, "keys") else row[2],
                    "reward_type": row["reward_type"] if hasattr(row, "keys") else row[3],
                    "reward_value": float((row["reward_value"] if hasattr(row, "keys") else row[4]) or 0.0),
                    "reward_source": row["reward_source"] if hasattr(row, "keys") else row[5],
                    "trace_id": row["trace_id"] if hasattr(row, "keys") else row[6],
                    "payload": _safe_json(row["payload_json"] if hasattr(row, "keys") else row[7], {}),
                }
            )
        return events
    finally:
        conn.close()


def build_content_dashboard_snapshot(*, limit: int = 10, trace_type: str | None = None) -> dict[str, Any]:
    store.init_db()
    content.init_content_db()
    traces.init_trace_db()
    policy.init_policy_db()
    trace_rows = traces.list_traces(limit=limit, trace_type=trace_type)
    candidate_rows = content.list_candidate_queue(limit=limit)
    decision_rows = _recent_policy_decisions(limit)
    reward_rows = _recent_reward_events(limit)
    overview = {
        "post_count": 0,
        "queued_candidate_count": 0,
        "trace_count": 0,
        "reward_count": 0,
        "decision_count": 0,
        "curated_count": 0,
    }
    conn = store._connect()
    try:
        overview["queued_candidate_count"] = _count_query(conn, "SELECT COUNT(*) FROM content_candidates WHERE status = 'queued'")
        if trace_type:
            overview["trace_count"] = _count_query(conn, "SELECT COUNT(*) FROM autonomy_traces WHERE trace_type = ?", (trace_type,))
        else:
            overview["trace_count"] = _count_query(conn, "SELECT COUNT(*) FROM autonomy_traces")
        overview["reward_count"] = _count_query(conn, "SELECT COUNT(*) FROM policy_rewards")
        overview["decision_count"] = _count_query(conn, "SELECT COUNT(*) FROM policy_decisions")
    finally:
        conn.close()
    content_stats = content.content_stats()
    overview["post_count"] = int(content_stats.get("post_count") or 0)
    try:
        curation = corpus_curation.curation_stats()
    except Exception:
        curation = {"curated_count": 0, "by_industry": {}, "duplicate_counts": {}}
    overview["curated_count"] = int(curation.get("curated_count") or 0)
    return {
        "generated_at": store._now_iso(),
        "overview": overview,
        "content_stats": content_stats,
        "curation_stats": curation,
        "traces": trace_rows,
        "candidates": candidate_rows,
        "policy_decisions": decision_rows,
        "reward_events": reward_rows,
    }


def _section_lines(snapshot: dict[str, Any], view: str, width: int) -> list[str]:
    lines: list[str] = []
    overview = snapshot.get("overview") or {}
    if view == "overview":
        lines.extend(
            [
                f"posts: {overview.get('post_count', 0)}",
                f"curated: {overview.get('curated_count', 0)}",
                f"queued candidates: {overview.get('queued_candidate_count', 0)}",
                f"traces: {overview.get('trace_count', 0)}",
                f"decisions: {overview.get('decision_count', 0)}",
                f"rewards: {overview.get('reward_count', 0)}",
                "",
                "Top industries:",
            ]
        )
        for industry, count in list((snapshot.get("curation_stats") or {}).get("by_industry", {}).items())[:8]:
            lines.append(f"  {industry}: {count}")
        lines.append("")
        lines.append("Recent traces:")
        for trace in snapshot.get("traces") or []:
            lines.append(
                f"  {_time_label(trace.get('updated_at') or trace.get('created_at'))}  {trace.get('trace_id')}  {trace.get('status')}  {trace.get('trace_type')}"
            )
    elif view == "traces":
        for trace in snapshot.get("traces") or []:
            summary = trace.get("summary") or {}
            lines.append(
                f"{_time_label(trace.get('updated_at') or trace.get('created_at'))}  {trace.get('trace_id')}  {trace.get('status')}  {trace.get('trace_type')}"
            )
            lines.append(f"  request: {trace.get('request_kind')}  chosen: {summary.get('chosen_candidate_id') or '-'}")
            lines.append(f"  summary: {_trim(summary, max(20, width - 12))}")
            lines.append("")
    elif view == "queue":
        for candidate in snapshot.get("candidates") or []:
            lines.append(
                f"#{candidate.get('rank', '-')}  {candidate.get('candidate_id')}  {candidate.get('status')}  {candidate.get('goal')}"
            )
            lines.append(f"  score: {float((candidate.get('score') or {}).get('predicted_outcome_score') or 0.0):.4f}")
            lines.append(f"  {_trim(candidate.get('text'), max(20, width - 4))}")
            lines.append("")
    elif view == "decisions":
        for decision in snapshot.get("policy_decisions") or []:
            lines.append(
                f"{_time_label(decision.get('created_at'))}  {decision.get('decision_id')}  {decision.get('decision_source')}"
            )
            lines.append(
                f"  action: {decision.get('chosen_action_id')}  score: {float(decision.get('chosen_score') or 0.0):.4f}  propensity: {float(decision.get('propensity') or 0.0):.4f}"
            )
            lines.append(f"  context: {decision.get('context_type')} / {decision.get('context_key')}")
            lines.append("")
    elif view == "rewards":
        for event in snapshot.get("reward_events") or []:
            lines.append(
                f"{_time_label(event.get('event_time'))}  {event.get('reward_type')}  {float(event.get('reward_value') or 0.0):.4f}"
            )
            lines.append(f"  source: {event.get('reward_source') or '-'}  decision: {event.get('decision_id') or '-'}")
            lines.append(f"  trace: {event.get('trace_id') or '-'}")
            lines.append("")
    if not lines:
        lines.append("No data yet.")
    normalized: list[str] = []
    for line in lines:
        if not line:
            normalized.append("")
            continue
        wrapped = textwrap.wrap(str(line), width=max(10, width - 1)) or [""]
        normalized.extend(wrapped)
    return normalized


def _draw_dashboard(screen: Any, snapshot: dict[str, Any], view: str, refresh_seconds: float) -> None:
    screen.erase()
    height, width = screen.getmaxyx()
    title = "LinkedIn Content Autonomy TUI"
    subtitle = f"view={VIEW_LABELS.get(view, view)}  refresh={refresh_seconds:.1f}s  q quit  r refresh  1-5 switch"
    screen.addnstr(0, 0, title, max(1, width - 1), curses.A_BOLD)
    screen.addnstr(1, 0, subtitle, max(1, width - 1))
    screen.hline(2, 0, "-", max(1, width - 1))
    lines = _section_lines(snapshot, view, width)
    row = 3
    for line in lines[: max(0, height - 5)]:
        screen.addnstr(row, 0, line, max(1, width - 1))
        row += 1
    footer = f"updated {snapshot.get('generated_at') or '-'}"
    screen.hline(max(3, height - 2), 0, "-", max(1, width - 1))
    screen.addnstr(max(4, height - 1), 0, footer, max(1, width - 1))
    screen.refresh()


def run_content_tui(*, refresh_seconds: float = 2.0, limit: int = 10, trace_type: str | None = None) -> None:
    if refresh_seconds <= 0:
        fail("refresh_seconds must be positive", code=ExitCode.VALIDATION)

    def _main(screen: Any) -> None:
        try:
            curses.curs_set(0)
        except curses.error:
            pass
        screen.nodelay(True)
        view = "overview"
        snapshot = build_content_dashboard_snapshot(limit=limit, trace_type=trace_type)
        last_refresh = time.monotonic()
        while True:
            _draw_dashboard(screen, snapshot, view, refresh_seconds)
            key = screen.getch()
            if key in (ord("q"), ord("Q")):
                break
            if key in (ord("r"), ord("R")):
                snapshot = build_content_dashboard_snapshot(limit=limit, trace_type=trace_type)
                last_refresh = time.monotonic()
                continue
            if key in (ord("1"), ord("2"), ord("3"), ord("4"), ord("5")):
                view = VIEW_ORDER[int(chr(key)) - 1]
            now = time.monotonic()
            if now - last_refresh >= refresh_seconds:
                snapshot = build_content_dashboard_snapshot(limit=limit, trace_type=trace_type)
                last_refresh = now
            time.sleep(0.1)

    curses.wrapper(_main)
