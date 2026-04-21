"""Local workflow persistence for saved searches, templates, and contacts."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from string import Formatter
from typing import Any

from linkedin_cli.write import store


VALID_SEARCH_KINDS = {"people", "companies", "posts"}
VALID_TEMPLATE_KINDS = {"dm", "post", "generic"}
VALID_CONTACT_STAGES = {"new", "active", "qualified", "won", "archived"}
VALID_INBOX_STATES = {"new", "follow_up", "waiting", "closed"}
VALID_INBOX_PRIORITIES = {"low", "medium", "high"}


def init_workflow_db() -> None:
    conn = store._connect()
    try:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS saved_searches (
                name TEXT PRIMARY KEY,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                kind TEXT NOT NULL,
                query TEXT NOT NULL,
                result_limit INTEGER NOT NULL DEFAULT 5,
                enrich INTEGER NOT NULL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS templates (
                name TEXT PRIMARY KEY,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                kind TEXT NOT NULL,
                body TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS contacts (
                profile_key TEXT PRIMARY KEY,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                display_name TEXT NOT NULL,
                stage TEXT NOT NULL,
                tags_json TEXT NOT NULL DEFAULT '[]',
                notes TEXT NOT NULL DEFAULT ''
            );

            CREATE TABLE IF NOT EXISTS inbox_triage (
                conversation_urn TEXT PRIMARY KEY,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                state TEXT NOT NULL,
                priority TEXT NOT NULL,
                notes TEXT NOT NULL DEFAULT ''
            );
            """
        )
        conn.commit()
    finally:
        conn.close()


def _row_to_dict(row: Any) -> dict[str, Any]:
    result = dict(row)
    if "tags_json" in result:
        result["tags"] = json.loads(result.pop("tags_json") or "[]")
    return result


def save_search(name: str, kind: str, query: str, limit: int = 5, enrich: bool = False) -> dict[str, Any]:
    if kind not in VALID_SEARCH_KINDS:
        raise ValueError(f"Unsupported search kind: {kind}")
    now = store._now_iso()
    conn = store._connect()
    try:
        conn.execute(
            """
            INSERT INTO saved_searches (name, created_at, updated_at, kind, query, result_limit, enrich)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(name) DO UPDATE SET
                updated_at = excluded.updated_at,
                kind = excluded.kind,
                query = excluded.query,
                result_limit = excluded.result_limit,
                enrich = excluded.enrich
            """,
            (name, now, now, kind, query, limit, 1 if enrich else 0),
        )
        conn.commit()
        row = conn.execute("SELECT * FROM saved_searches WHERE name = ?", (name,)).fetchone()
        return _row_to_dict(row)
    finally:
        conn.close()


def list_saved_searches() -> list[dict[str, Any]]:
    conn = store._connect()
    try:
        rows = conn.execute("SELECT * FROM saved_searches ORDER BY updated_at DESC").fetchall()
        return [_row_to_dict(row) for row in rows]
    finally:
        conn.close()


def get_saved_search(name: str) -> dict[str, Any] | None:
    conn = store._connect()
    try:
        row = conn.execute("SELECT * FROM saved_searches WHERE name = ?", (name,)).fetchone()
        return _row_to_dict(row) if row else None
    finally:
        conn.close()


def delete_saved_search(name: str) -> bool:
    conn = store._connect()
    try:
        cur = conn.execute("DELETE FROM saved_searches WHERE name = ?", (name,))
        conn.commit()
        return cur.rowcount > 0
    finally:
        conn.close()


def save_template(name: str, kind: str, body: str) -> dict[str, Any]:
    if kind not in VALID_TEMPLATE_KINDS:
        raise ValueError(f"Unsupported template kind: {kind}")
    now = store._now_iso()
    conn = store._connect()
    try:
        conn.execute(
            """
            INSERT INTO templates (name, created_at, updated_at, kind, body)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(name) DO UPDATE SET
                updated_at = excluded.updated_at,
                kind = excluded.kind,
                body = excluded.body
            """,
            (name, now, now, kind, body),
        )
        conn.commit()
        row = conn.execute("SELECT * FROM templates WHERE name = ?", (name,)).fetchone()
        return _row_to_dict(row)
    finally:
        conn.close()


def list_templates(kind: str | None = None) -> list[dict[str, Any]]:
    conn = store._connect()
    try:
        if kind:
            rows = conn.execute("SELECT * FROM templates WHERE kind = ? ORDER BY updated_at DESC", (kind,)).fetchall()
        else:
            rows = conn.execute("SELECT * FROM templates ORDER BY updated_at DESC").fetchall()
        return [_row_to_dict(row) for row in rows]
    finally:
        conn.close()


def get_template(name: str) -> dict[str, Any] | None:
    conn = store._connect()
    try:
        row = conn.execute("SELECT * FROM templates WHERE name = ?", (name,)).fetchone()
        return _row_to_dict(row) if row else None
    finally:
        conn.close()


def render_template(name: str, variables: dict[str, str]) -> str:
    template = get_template(name)
    if not template:
        raise ValueError(f"Template not found: {name}")
    body = template["body"]
    required = {
        field_name
        for _, field_name, _, _ in Formatter().parse(body)
        if field_name
    }
    missing = sorted(required - set(variables))
    if missing:
        raise ValueError(f"Missing template variables: {', '.join(missing)}")
    return body.format(**variables)


def delete_template(name: str) -> bool:
    conn = store._connect()
    try:
        cur = conn.execute("DELETE FROM templates WHERE name = ?", (name,))
        conn.commit()
        return cur.rowcount > 0
    finally:
        conn.close()


def upsert_contact(
    profile_key: str,
    display_name: str,
    stage: str = "new",
    tags: list[str] | None = None,
    notes: str = "",
) -> dict[str, Any]:
    if stage not in VALID_CONTACT_STAGES:
        raise ValueError(f"Unsupported contact stage: {stage}")
    tags = sorted(set(tags or []))
    now = store._now_iso()
    conn = store._connect()
    try:
        conn.execute(
            """
            INSERT INTO contacts (profile_key, created_at, updated_at, display_name, stage, tags_json, notes)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(profile_key) DO UPDATE SET
                updated_at = excluded.updated_at,
                display_name = excluded.display_name,
                stage = excluded.stage,
                tags_json = excluded.tags_json,
                notes = excluded.notes
            """,
            (profile_key, now, now, display_name, stage, json.dumps(tags), notes),
        )
        conn.commit()
        row = conn.execute("SELECT * FROM contacts WHERE profile_key = ?", (profile_key,)).fetchone()
        return _row_to_dict(row)
    finally:
        conn.close()


def _stage_from_discovery_state(state: str | None) -> str:
    if state in {"engaged"}:
        return "qualified"
    if state in {"won"}:
        return "won"
    if state in {"contacted", "waiting", "watch"}:
        return "active"
    if state in {"cold", "do_not_contact"}:
        return "archived"
    return "new"


def sync_contacts_from_queue(prospects: list[dict[str, Any]]) -> list[dict[str, Any]]:
    synced: list[dict[str, Any]] = []
    for prospect in prospects:
        if prospect.get("entity_type") not in {None, "person"}:
            continue
        profile_key = str(prospect.get("public_identifier") or prospect.get("profile_key") or "").strip()
        display_name = str(prospect.get("display_name") or profile_key).strip()
        if not profile_key or not display_name:
            continue
        tags: list[str] = []
        if prospect.get("company"):
            tags.append(str(prospect["company"]))
        if prospect.get("state"):
            tags.append(f"queue:{prospect['state']}")
        score = prospect.get("score")
        notes = f"Discovery sync score={round(float(score or 0.0), 4)}"
        synced.append(
            upsert_contact(
                profile_key=profile_key,
                display_name=display_name,
                stage=_stage_from_discovery_state(prospect.get("state")),
                tags=tags,
                notes=notes,
            )
        )
    return synced


def sync_contacts_from_search_results(results: list[dict[str, Any]]) -> list[dict[str, Any]]:
    synced: list[dict[str, Any]] = []
    for result in results:
        url = str(result.get("url") or "")
        if "/in/" not in url:
            continue
        slug = url.rstrip("/").split("/in/")[-1].split("/", 1)[0]
        display_name = str((result.get("title") or "").split(" - ")[0].strip() or slug).strip()
        if not slug or not display_name:
            continue
        notes = str(result.get("snippet") or "")
        synced.append(
            upsert_contact(
                profile_key=slug,
                display_name=display_name,
                stage="new",
                tags=["search"],
                notes=notes,
            )
        )
    return synced


def list_contacts(stage: str | None = None, tag: str | None = None) -> list[dict[str, Any]]:
    conn = store._connect()
    try:
        rows = conn.execute("SELECT * FROM contacts ORDER BY updated_at DESC").fetchall()
        contacts = [_row_to_dict(row) for row in rows]
        if stage:
            contacts = [contact for contact in contacts if contact["stage"] == stage]
        if tag:
            contacts = [contact for contact in contacts if tag in contact.get("tags", [])]
        return contacts
    finally:
        conn.close()


def get_contact(profile_key: str) -> dict[str, Any] | None:
    conn = store._connect()
    try:
        row = conn.execute("SELECT * FROM contacts WHERE profile_key = ?", (profile_key,)).fetchone()
        return _row_to_dict(row) if row else None
    finally:
        conn.close()


def delete_contact(profile_key: str) -> bool:
    conn = store._connect()
    try:
        cur = conn.execute("DELETE FROM contacts WHERE profile_key = ?", (profile_key,))
        conn.commit()
        return cur.rowcount > 0
    finally:
        conn.close()


def export_contacts_csv(path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    contacts = list_contacts()
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=["profile_key", "display_name", "stage", "tags", "notes"])
        writer.writeheader()
        for contact in contacts:
            writer.writerow(
                {
                    "profile_key": contact["profile_key"],
                    "display_name": contact["display_name"],
                    "stage": contact["stage"],
                    "tags": ",".join(contact.get("tags", [])),
                    "notes": contact.get("notes", ""),
                }
            )
    return path


def import_contacts_csv(path: Path) -> int:
    count = 0
    with path.open("r", encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            upsert_contact(
                profile_key=(row.get("profile_key") or "").strip(),
                display_name=(row.get("display_name") or "").strip(),
                stage=(row.get("stage") or "new").strip() or "new",
                tags=[tag.strip() for tag in (row.get("tags") or "").split(",") if tag.strip()],
                notes=(row.get("notes") or "").strip(),
            )
            count += 1
    return count


def upsert_inbox_item(
    conversation_urn: str,
    state: str = "new",
    priority: str = "medium",
    notes: str = "",
) -> dict[str, Any]:
    if state not in VALID_INBOX_STATES:
        raise ValueError(f"Unsupported inbox state: {state}")
    if priority not in VALID_INBOX_PRIORITIES:
        raise ValueError(f"Unsupported inbox priority: {priority}")

    now = store._now_iso()
    conn = store._connect()
    try:
        conn.execute(
            """
            INSERT INTO inbox_triage (conversation_urn, created_at, updated_at, state, priority, notes)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(conversation_urn) DO UPDATE SET
                updated_at = excluded.updated_at,
                state = excluded.state,
                priority = excluded.priority,
                notes = excluded.notes
            """,
            (conversation_urn, now, now, state, priority, notes),
        )
        conn.commit()
        row = conn.execute("SELECT * FROM inbox_triage WHERE conversation_urn = ?", (conversation_urn,)).fetchone()
        return _row_to_dict(row)
    finally:
        conn.close()


def list_inbox_items(state: str | None = None) -> list[dict[str, Any]]:
    conn = store._connect()
    try:
        if state:
            rows = conn.execute(
                "SELECT * FROM inbox_triage WHERE state = ? ORDER BY updated_at DESC",
                (state,),
            ).fetchall()
        else:
            rows = conn.execute("SELECT * FROM inbox_triage ORDER BY updated_at DESC").fetchall()
        return [_row_to_dict(row) for row in rows]
    finally:
        conn.close()


def get_inbox_item(conversation_urn: str) -> dict[str, Any] | None:
    conn = store._connect()
    try:
        row = conn.execute(
            "SELECT * FROM inbox_triage WHERE conversation_urn = ?",
            (conversation_urn,),
        ).fetchone()
        return _row_to_dict(row) if row else None
    finally:
        conn.close()


def delete_inbox_item(conversation_urn: str) -> bool:
    conn = store._connect()
    try:
        cur = conn.execute("DELETE FROM inbox_triage WHERE conversation_urn = ?", (conversation_urn,))
        conn.commit()
        return cur.rowcount > 0
    finally:
        conn.close()
