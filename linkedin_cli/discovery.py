"""Unified discovery queue for prospects, sources, signals, and feedback."""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from hashlib import sha256
from typing import Any
from urllib.parse import urlparse

from bs4 import BeautifulSoup

from linkedin_cli.voyager import parse_json_response, voyager_get
from linkedin_cli.write import store


SOURCE_WEIGHTS = {
    "search.query": 3.0,
    "search.saved": 4.0,
    "inbox": 2.0,
    "public.engagement": 3.0,
    "profile.views": 4.0,
    "workflow.contact": 2.0,
    "manual": 1.0,
}

SIGNAL_WEIGHTS = {
    "replied_dm": 10.0,
    "inbound_dm": 8.0,
    "active_thread": 8.0,
    "accepted": 7.0,
    "manual_positive": 6.0,
    "commented": 5.0,
    "profile_view": 4.0,
    "followed": 3.0,
    "liked": 2.0,
    "reposted": 4.0,
    "outreach_sent": 2.0,
    "follow_up_sent": 3.0,
    "connection_requested": 1.0,
    "public_post": 0.5,
}

DISCOVERY_STATES = {
    "new",
    "watch",
    "ready",
    "contacted",
    "waiting",
    "engaged",
    "won",
    "cold",
    "do_not_contact",
}

POSITIVE_SIGNAL_TYPES = {"replied_dm", "accepted", "manual_positive"}
POSITIVE_STATES = {"engaged", "won"}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _connect():
    return store._connect()


def _ensure_column(conn: Any, table: str, column: str, ddl: str) -> None:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    existing = {row["name"] if isinstance(row, dict) else row[1] for row in rows}
    if column not in existing:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {ddl}")


def _canonical_profile_url(url: str | None) -> str | None:
    if not url:
        return None
    parsed = urlparse(url)
    path = parsed.path.rstrip("/")
    if not path:
        return None
    return f"{parsed.scheme or 'https'}://{parsed.netloc.lower()}{path}"


def _slug_from_url(url: str | None) -> str | None:
    canonical = _canonical_profile_url(url)
    if not canonical:
        return None
    parsed = urlparse(canonical)
    parts = [part for part in parsed.path.strip("/").split("/") if part]
    if len(parts) >= 2 and parts[0] in {"in", "company"}:
        return parts[1]
    return None


def _entity_type_from_url(url: str | None) -> str:
    canonical = _canonical_profile_url(url) or ""
    if "/company/" in canonical:
        return "company"
    if "/posts/" in canonical or "/feed/update/" in canonical or "/pulse/" in canonical:
        return "post"
    return "person"


def _normalize_display_name(name: str | None) -> str | None:
    if not name:
        return None
    return " ".join(name.split()).strip().lower() or None


def _key_from_name(name: str) -> str:
    cleaned = re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")
    return cleaned or f"prospect-{abs(hash(name))}"


def _post_key_from_url(url: str) -> str:
    parsed = urlparse(url)
    token = parsed.path.strip("/").replace("/", ":") or sha256(url.encode("utf-8")).hexdigest()[:12]
    return f"post:{token}"


def _company_key(slug_or_name: str) -> str:
    return f"company:{slug_or_name}"


def _parse_json_ld_objects(html: str) -> list[Any]:
    soup = BeautifulSoup(html, "html.parser")
    objects: list[Any] = []
    for tag in soup.find_all("script", attrs={"type": "application/ld+json"}):
        raw = tag.string or tag.get_text() or ""
        raw = raw.strip()
        if not raw:
            continue
        try:
            parsed = json.loads(raw)
        except Exception:
            continue
        if isinstance(parsed, list):
            objects.extend(parsed)
        else:
            objects.append(parsed)
    return objects


def _view_text(value: Any) -> str | None:
    if isinstance(value, str):
        compact = " ".join(value.split()).strip()
        return compact or None
    if isinstance(value, dict):
        for key in ("text", "accessibilityText", "displayName", "url"):
            extracted = _view_text(value.get(key))
            if extracted:
                return extracted
    return None


def _metadata_json(metadata: dict[str, Any] | None) -> str:
    return json.dumps(metadata or {}, sort_keys=True, ensure_ascii=False)


def _dedupe_key(prefix: str, *parts: str) -> str:
    body = "|".join(part.strip() for part in parts if part and part.strip())
    return f"{prefix}:{sha256(body.encode('utf-8')).hexdigest()[:16]}"


def _row_to_dict(row: Any) -> dict[str, Any]:
    data = dict(row)
    if "metadata_json" in data:
        try:
            data["metadata"] = json.loads(data["metadata_json"] or "{}")
        except Exception:
            data["metadata"] = {}
    return data


def init_discovery_db() -> None:
    conn = _connect()
    try:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS prospects (
                profile_key TEXT PRIMARY KEY,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                entity_type TEXT NOT NULL DEFAULT 'person',
                display_name TEXT NOT NULL,
                public_identifier TEXT,
                member_urn TEXT,
                profile_url TEXT,
                headline TEXT,
                company TEXT,
                location TEXT,
                state TEXT NOT NULL DEFAULT 'new',
                score REAL NOT NULL DEFAULT 0,
                fit_score REAL NOT NULL DEFAULT 0,
                intent_score REAL NOT NULL DEFAULT 0,
                freshness_score REAL NOT NULL DEFAULT 0,
                saturation_score REAL NOT NULL DEFAULT 0,
                staleness_score REAL NOT NULL DEFAULT 0,
                learned_score REAL NOT NULL DEFAULT 0,
                last_seen_at TEXT,
                last_signal_at TEXT
            );

            CREATE TABLE IF NOT EXISTS prospect_sources (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                prospect_key TEXT NOT NULL,
                source_type TEXT NOT NULL,
                source_value TEXT NOT NULL,
                dedupe_key TEXT,
                metadata_json TEXT,
                created_at TEXT NOT NULL,
                FOREIGN KEY (prospect_key) REFERENCES prospects(profile_key)
            );

            CREATE TABLE IF NOT EXISTS prospect_signals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                prospect_key TEXT NOT NULL,
                signal_type TEXT NOT NULL,
                source TEXT NOT NULL,
                weight REAL NOT NULL,
                confidence REAL NOT NULL DEFAULT 1.0,
                dedupe_key TEXT,
                notes TEXT,
                metadata_json TEXT,
                created_at TEXT NOT NULL,
                FOREIGN KEY (prospect_key) REFERENCES prospects(profile_key)
            );

            CREATE TABLE IF NOT EXISTS prospect_aliases (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                alias_type TEXT NOT NULL,
                alias_value TEXT NOT NULL,
                prospect_key TEXT NOT NULL,
                created_at TEXT NOT NULL,
                UNIQUE(alias_type, alias_value),
                FOREIGN KEY (prospect_key) REFERENCES prospects(profile_key)
            );
            """
        )

        _ensure_column(conn, "prospects", "entity_type", "TEXT NOT NULL DEFAULT 'person'")
        _ensure_column(conn, "prospects", "member_urn", "TEXT")
        _ensure_column(conn, "prospects", "learned_score", "REAL NOT NULL DEFAULT 0")
        _ensure_column(conn, "prospect_sources", "dedupe_key", "TEXT")
        _ensure_column(conn, "prospect_signals", "dedupe_key", "TEXT")
        conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_prospect_sources_dedupe ON prospect_sources(prospect_key, dedupe_key)"
        )
        conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_prospect_signals_dedupe ON prospect_signals(prospect_key, dedupe_key)"
        )
        conn.commit()
    finally:
        conn.close()


def _register_alias(conn: Any, alias_type: str, alias_value: str | None, prospect_key: str) -> None:
    if not alias_value:
        return
    try:
        conn.execute(
            """
            INSERT INTO prospect_aliases (alias_type, alias_value, prospect_key, created_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(alias_type, alias_value) DO UPDATE SET
                prospect_key = excluded.prospect_key
            """,
            (alias_type, alias_value, prospect_key, _now_iso()),
        )
    except Exception:
        pass


def resolve_profile_key(
    public_identifier: str | None = None,
    member_urn: str | None = None,
    profile_url: str | None = None,
    display_name: str | None = None,
    entity_type: str = "person",
) -> str | None:
    canonical_url = _canonical_profile_url(profile_url)
    normalized_name = _normalize_display_name(display_name)
    candidates = [
        ("member_urn", member_urn),
        ("public_identifier", public_identifier),
        ("profile_url", canonical_url),
    ]
    if normalized_name and entity_type == "person":
        candidates.append(("display_name", normalized_name))

    conn = _connect()
    try:
        for alias_type, alias_value in candidates:
            if not alias_value:
                continue
            row = conn.execute(
                "SELECT prospect_key FROM prospect_aliases WHERE alias_type = ? AND alias_value = ?",
                (alias_type, alias_value),
            ).fetchone()
            if row:
                return row["prospect_key"]
        return None
    finally:
        conn.close()


def upsert_prospect(
    profile_key: str,
    display_name: str,
    public_identifier: str | None = None,
    profile_url: str | None = None,
    headline: str | None = None,
    company: str | None = None,
    location: str | None = None,
    state: str = "new",
    member_urn: str | None = None,
    entity_type: str = "person",
) -> dict[str, Any]:
    if state not in DISCOVERY_STATES:
        raise ValueError(f"Unsupported discovery state: {state}")

    init_discovery_db()
    canonical_url = _canonical_profile_url(profile_url)
    resolved = resolve_profile_key(
        public_identifier=public_identifier,
        member_urn=member_urn,
        profile_url=canonical_url,
        display_name=display_name,
        entity_type=entity_type,
    )
    canonical_key = resolved or profile_key
    now = _now_iso()
    conn = _connect()
    try:
        conn.execute(
            """
            INSERT INTO prospects (
                profile_key, created_at, updated_at, entity_type, display_name, public_identifier,
                member_urn, profile_url, headline, company, location, state, last_seen_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(profile_key) DO UPDATE SET
                updated_at = excluded.updated_at,
                entity_type = excluded.entity_type,
                display_name = excluded.display_name,
                public_identifier = COALESCE(excluded.public_identifier, prospects.public_identifier),
                member_urn = COALESCE(excluded.member_urn, prospects.member_urn),
                profile_url = COALESCE(excluded.profile_url, prospects.profile_url),
                headline = COALESCE(excluded.headline, prospects.headline),
                company = COALESCE(excluded.company, prospects.company),
                location = COALESCE(excluded.location, prospects.location),
                state = CASE
                    WHEN excluded.state = 'new' THEN prospects.state
                    ELSE excluded.state
                END,
                last_seen_at = excluded.last_seen_at
            """,
            (
                canonical_key,
                now,
                now,
                entity_type,
                display_name,
                public_identifier,
                member_urn,
                canonical_url,
                headline,
                company,
                location,
                state,
                now,
            ),
        )
        _register_alias(conn, "public_identifier", public_identifier, canonical_key)
        _register_alias(conn, "member_urn", member_urn, canonical_key)
        _register_alias(conn, "profile_url", canonical_url, canonical_key)
        if entity_type == "person":
            _register_alias(conn, "display_name", _normalize_display_name(display_name), canonical_key)
        conn.commit()
    finally:
        conn.close()

    _recalculate_scores(canonical_key)
    return get_prospect(canonical_key) or {}


def set_prospect_state(profile_key: str, state: str) -> dict[str, Any]:
    if state not in DISCOVERY_STATES:
        raise ValueError(f"Unsupported discovery state: {state}")
    conn = _connect()
    try:
        conn.execute(
            "UPDATE prospects SET state = ?, updated_at = ? WHERE profile_key = ?",
            (state, _now_iso(), profile_key),
        )
        conn.commit()
    finally:
        conn.close()
    _recalculate_scores(profile_key)
    return get_prospect(profile_key) or {}


def add_source(
    prospect_key: str,
    source_type: str,
    source_value: str,
    metadata: dict[str, Any] | None = None,
    dedupe_key: str | None = None,
) -> dict[str, Any]:
    init_discovery_db()
    metadata = metadata or {}
    dedupe_key = dedupe_key or _dedupe_key("source", prospect_key, source_type, source_value, _metadata_json(metadata))
    conn = _connect()
    try:
        conn.execute(
            """
            INSERT OR IGNORE INTO prospect_sources
            (prospect_key, source_type, source_value, dedupe_key, metadata_json, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (prospect_key, source_type, source_value, dedupe_key, _metadata_json(metadata), _now_iso()),
        )
        conn.commit()
    finally:
        conn.close()
    _recalculate_scores(prospect_key)
    return get_prospect(prospect_key) or {}


def add_signal(
    prospect_key: str,
    signal_type: str,
    source: str,
    weight: float | None = None,
    confidence: float = 1.0,
    notes: str | None = None,
    metadata: dict[str, Any] | None = None,
    dedupe_key: str | None = None,
) -> dict[str, Any]:
    init_discovery_db()
    metadata = metadata or {}
    actual_weight = weight if weight is not None else SIGNAL_WEIGHTS.get(signal_type, 1.0)
    dedupe_key = dedupe_key or _dedupe_key(
        "signal",
        prospect_key,
        signal_type,
        source,
        notes or "",
        _metadata_json(metadata),
    )
    conn = _connect()
    try:
        conn.execute(
            """
            INSERT OR IGNORE INTO prospect_signals
            (prospect_key, signal_type, source, weight, confidence, dedupe_key, notes, metadata_json, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                prospect_key,
                signal_type,
                source,
                actual_weight,
                confidence,
                dedupe_key,
                notes,
                _metadata_json(metadata),
                _now_iso(),
            ),
        )
        conn.commit()
    finally:
        conn.close()
    _recalculate_scores(prospect_key)
    try:
        from linkedin_cli import traces

        traces.attribute_prospect_signal(
            profile_key=prospect_key,
            signal_type=signal_type,
            source=source,
            metadata=metadata,
            dedupe_key=dedupe_key,
        )
    except Exception:
        pass
    return get_prospect(prospect_key) or {}


def record_action_feedback(
    action_type: str,
    profile_key: str,
    succeeded: bool,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if not succeeded:
        return get_prospect(profile_key) or {}
    metadata = metadata or {}
    if action_type == "dm.send":
        add_signal(
            profile_key,
            signal_type="outreach_sent",
            source="action",
            metadata=metadata,
            dedupe_key=_dedupe_key("action", profile_key, action_type, metadata.get("action_id", _now_iso())),
        )
        set_prospect_state(profile_key, "waiting")
    elif action_type == "connect":
        add_signal(
            profile_key,
            signal_type="connection_requested",
            source="action",
            metadata=metadata,
            dedupe_key=_dedupe_key("action", profile_key, action_type, metadata.get("action_id", _now_iso())),
        )
        set_prospect_state(profile_key, "contacted")
    elif action_type == "follow":
        add_signal(
            profile_key,
            signal_type="followed",
            source="action",
            metadata=metadata,
            dedupe_key=_dedupe_key("action", profile_key, action_type, metadata.get("action_id", _now_iso())),
        )
        set_prospect_state(profile_key, "watch")
    return get_prospect(profile_key) or {}


def get_prospect(profile_key: str) -> dict[str, Any] | None:
    conn = _connect()
    try:
        row = conn.execute("SELECT * FROM prospects WHERE profile_key = ?", (profile_key,)).fetchone()
        if row is None:
            return None
        prospect = _row_to_dict(row)
        sources = conn.execute(
            """
            SELECT source_type, source_value, dedupe_key, metadata_json, created_at
            FROM prospect_sources
            WHERE prospect_key = ?
            ORDER BY id ASC
            """,
            (profile_key,),
        ).fetchall()
        signals = conn.execute(
            """
            SELECT signal_type, source, weight, confidence, dedupe_key, notes, metadata_json, created_at
            FROM prospect_signals
            WHERE prospect_key = ?
            ORDER BY id DESC
            """,
            (profile_key,),
        ).fetchall()
        prospect["sources"] = [_row_to_dict(source) for source in sources]
        prospect["signals"] = [_row_to_dict(signal) for signal in signals]
        prospect["source_count"] = len(prospect["sources"])
        prospect["signal_count"] = len(prospect["signals"])
        return prospect
    finally:
        conn.close()


def list_queue(limit: int = 20, state: str | None = None) -> list[dict[str, Any]]:
    conn = _connect()
    try:
        sql = """
            SELECT
                p.*,
                (SELECT COUNT(*) FROM prospect_sources s WHERE s.prospect_key = p.profile_key) AS source_count,
                (SELECT COUNT(*) FROM prospect_signals g WHERE g.prospect_key = p.profile_key) AS signal_count
            FROM prospects p
        """
        params: list[Any] = []
        if state:
            sql += " WHERE p.state = ?"
            params.append(state)
        sql += " ORDER BY p.score DESC, p.updated_at DESC LIMIT ?"
        params.append(limit)
        rows = conn.execute(sql, params).fetchall()
        return [_row_to_dict(row) for row in rows]
    finally:
        conn.close()


def queue_stats() -> dict[str, Any]:
    conn = _connect()
    try:
        prospects = conn.execute("SELECT COUNT(*) AS count FROM prospects").fetchone()
        avg_score = conn.execute("SELECT AVG(score) AS avg_score FROM prospects").fetchone()
        sources = conn.execute(
            "SELECT source_type, COUNT(*) AS count FROM prospect_sources GROUP BY source_type ORDER BY count DESC"
        ).fetchall()
        signals = conn.execute(
            "SELECT signal_type, COUNT(*) AS count FROM prospect_signals GROUP BY signal_type ORDER BY count DESC"
        ).fetchall()
        states = conn.execute(
            "SELECT state, COUNT(*) AS count FROM prospects GROUP BY state ORDER BY count DESC"
        ).fetchall()

        outreach_sent = conn.execute(
            "SELECT COUNT(*) AS count FROM prospect_signals WHERE signal_type = 'outreach_sent'"
        ).fetchone()["count"] or 0
        replied = conn.execute(
            "SELECT COUNT(*) AS count FROM prospect_signals WHERE signal_type = 'replied_dm'"
        ).fetchone()["count"] or 0
        connection_requested = conn.execute(
            "SELECT COUNT(*) AS count FROM prospect_signals WHERE signal_type = 'connection_requested'"
        ).fetchone()["count"] or 0
        accepted = conn.execute(
            "SELECT COUNT(*) AS count FROM prospect_signals WHERE signal_type = 'accepted'"
        ).fetchone()["count"] or 0

        source_conversion: dict[str, Any] = {}
        for row in sources:
            source_type = row["source_type"]
            total = row["count"] or 0
            success = conn.execute(
                """
                SELECT COUNT(DISTINCT s.prospect_key) AS count
                FROM prospect_sources s
                JOIN prospects p ON p.profile_key = s.prospect_key
                WHERE s.source_type = ?
                  AND (
                    p.state IN ('engaged', 'won') OR EXISTS (
                      SELECT 1 FROM prospect_signals g
                      WHERE g.prospect_key = s.prospect_key
                        AND g.signal_type IN ('replied_dm', 'accepted', 'manual_positive')
                    )
                  )
                """,
                (source_type,),
            ).fetchone()["count"] or 0
            source_conversion[source_type] = {
                "total": int(total),
                "success_count": int(success),
                "success_rate": round(float(success) / float(total), 4) if total else 0.0,
            }

        template_performance: dict[str, dict[str, Any]] = {}
        template_rows = conn.execute(
            """
            SELECT prospect_key, metadata_json
            FROM prospect_signals
            WHERE signal_type = 'outreach_sent'
            """
        ).fetchall()
        for row in template_rows:
            try:
                metadata = json.loads(row["metadata_json"] or "{}")
            except Exception:
                metadata = {}
            template_name = metadata.get("template_name")
            if not template_name:
                continue
            stats = template_performance.setdefault(template_name, {"sent_count": 0, "reply_count": 0})
            stats["sent_count"] += 1
            reply = conn.execute(
                """
                SELECT COUNT(*) AS count
                FROM prospect_signals
                WHERE prospect_key = ? AND signal_type = 'replied_dm'
                """,
                (row["prospect_key"],),
            ).fetchone()["count"] or 0
            if reply:
                stats["reply_count"] += 1

        return {
            "prospect_count": int(prospects["count"] or 0),
            "average_score": round(float(avg_score["avg_score"] or 0.0), 4),
            "sources_by_type": {row["source_type"]: row["count"] for row in sources},
            "signals_by_type": {row["signal_type"]: row["count"] for row in signals},
            "states": {row["state"]: row["count"] for row in states},
            "reply_rate": round(float(replied) / float(outreach_sent), 4) if outreach_sent else 0.0,
            "acceptance_rate": round(float(accepted) / float(connection_requested), 4) if connection_requested else 0.0,
            "source_conversion": source_conversion,
            "template_performance": template_performance,
        }
    finally:
        conn.close()


def ingest_search_results(
    kind: str,
    query: str,
    results: list[dict[str, Any]],
    source_label: str,
) -> int:
    created = 0
    source_type = "search.saved" if source_label.startswith("saved:") else "search.query"
    for result in results:
        url = result.get("url")
        slug = result.get("slug") or _slug_from_url(url)
        title = (result.get("title") or "").split(" - ")[0].strip() or result.get("title") or ""
        summary = result.get("summary") if isinstance(result.get("summary"), dict) else {}

        if kind == "people":
            profile_key = slug or resolve_profile_key(profile_url=url, display_name=title) or _key_from_name(title or "unknown-person")
            upsert_prospect(
                profile_key=profile_key,
                display_name=summary.get("name")
                or " ".join(part for part in [summary.get("first_name"), summary.get("last_name")] if part)
                or title
                or profile_key,
                public_identifier=slug,
                profile_url=url,
                headline=summary.get("headline") or result.get("snippet"),
                member_urn=summary.get("member_urn"),
                entity_type="person",
            )
        elif kind == "companies":
            company_key = _company_key(slug or _key_from_name(title or "company"))
            upsert_prospect(
                profile_key=company_key,
                display_name=title or company_key,
                public_identifier=slug,
                profile_url=url,
                headline=result.get("snippet"),
                entity_type="company",
            )
            profile_key = company_key
        elif kind == "posts":
            post_key = _post_key_from_url(url or result.get("title") or _now_iso())
            upsert_prospect(
                profile_key=post_key,
                display_name=title or post_key,
                profile_url=url,
                headline=result.get("snippet"),
                entity_type="post",
            )
            add_signal(
                post_key,
                signal_type="public_post",
                source="search",
                metadata={"query": query, "url": url},
                dedupe_key=_dedupe_key("search-post", post_key, query),
            )
            profile_key = post_key
        else:
            continue

        add_source(
            profile_key,
            source_type=source_type,
            source_value=query,
            metadata={"label": source_label, "url": url},
            dedupe_key=_dedupe_key("source", profile_key, source_type, query, url or ""),
        )
        created += 1
    return created


def _commenter_record(name: str, url: str | None, entity_type: str | None = None) -> dict[str, Any]:
    guessed_type = entity_type or _entity_type_from_url(url)
    slug = _slug_from_url(url)
    if guessed_type == "company":
        profile_key = _company_key(slug or _key_from_name(name))
    else:
        profile_key = slug or _key_from_name(name)
    return {
        "profile_key": profile_key,
        "display_name": name,
        "public_identifier": slug,
        "profile_url": url,
        "entity_type": guessed_type,
    }


PROFILE_VIEW_ANALYTICS_QUERY_ID = "voyagerPremiumDashAnalyticsView.ce06d2faf5a200e49defacd432aff6b8"
PROFILE_VIEW_ANALYTICS_VARIABLES = "(analyticsEntityUrn:(activityUrn:urn%3Ali%3Adummy%3A-1),query:(),surfaceType:WVMP)"


def fetch_profile_view_analytics(session: Any) -> dict[str, Any]:
    path = (
        "/voyager/api/graphql?includeWebMetadata=true&variables="
        f"{PROFILE_VIEW_ANALYTICS_VARIABLES}&queryId={PROFILE_VIEW_ANALYTICS_QUERY_ID}"
    )
    response = voyager_get(session, path, referer="https://www.linkedin.com/me/profile-views")
    return parse_json_response(response)


def parse_profile_view_analytics_payload(payload: dict[str, Any]) -> dict[str, Any]:
    included = payload.get("included") or []
    viewer_count = 0
    view_title = None
    profiles_by_urn: dict[str, dict[str, Any]] = {}
    fallback_profiles: list[dict[str, Any]] = []

    for item in included:
        if not isinstance(item, dict):
            continue
        item_type = str(item.get("$type") or "")
        if "edgeinsightsanalytics.Card" in item_type:
            items = ((((item.get("component") or {}).get("summary") or {}).get("keyMetrics") or {}).get("items") or [])
            for metric in items:
                title_text = _view_text((metric or {}).get("title")) or ""
                if title_text.isdigit():
                    viewer_count = max(viewer_count, int(title_text))
        elif "edgeinsightsanalytics.View" in item_type and not view_title:
            view_title = _view_text(item.get("title"))
        elif "Profile" in item_type:
            display_name = " ".join(part for part in [str(item.get("firstName") or "").strip(), str(item.get("lastName") or "").strip()] if part).strip()
            profile = {
                "entity_urn": item.get("entityUrn"),
                "display_name": display_name or _view_text(item.get("title")),
                "public_identifier": item.get("publicIdentifier"),
                "profile_url": _canonical_profile_url(item.get("navigationUrl")),
                "headline": _view_text(item.get("headline")),
            }
            entity_urn = str(item.get("entityUrn") or "")
            if entity_urn:
                profiles_by_urn[entity_urn] = profile
            if item.get("wvmpProfileActions") is not None:
                fallback_profiles.append(profile)

    viewers: list[dict[str, Any]] = []
    seen_keys: set[str] = set()
    for item in included:
        if not isinstance(item, dict):
            continue
        item_type = str(item.get("$type") or "")
        if "AnalyticsEntityLockup" not in item_type or item.get("blurred") is True:
            continue
        entity_lockup = item.get("entityLockup") or {}
        action_data = entity_lockup.get("actionData") or {}
        profile = profiles_by_urn.get(str(action_data.get("entityProfile") or ""))
        profile_url = _canonical_profile_url(_view_text(entity_lockup.get("navigationUrl")) or (profile or {}).get("profile_url"))
        public_identifier = (profile or {}).get("public_identifier") or _slug_from_url(profile_url)
        display_name = _view_text(entity_lockup.get("title")) or (profile or {}).get("display_name")
        if not display_name:
            continue
        viewer = {
            "entity_urn": (profile or {}).get("entity_urn"),
            "display_name": display_name,
            "public_identifier": public_identifier,
            "profile_url": profile_url,
            "headline": _view_text(entity_lockup.get("subtitle")) or (profile or {}).get("headline"),
        }
        profile_key = public_identifier or resolve_profile_key(profile_url=profile_url, display_name=display_name) or _key_from_name(display_name)
        viewer["profile_key"] = profile_key
        if profile_key in seen_keys:
            continue
        seen_keys.add(profile_key)
        viewers.append(viewer)

    if not viewers:
        for profile in fallback_profiles:
            display_name = str(profile.get("display_name") or "").strip()
            if not display_name:
                continue
            public_identifier = profile.get("public_identifier")
            profile_url = _canonical_profile_url(profile.get("profile_url"))
            profile_key = public_identifier or resolve_profile_key(profile_url=profile_url, display_name=display_name) or _key_from_name(display_name)
            if profile_key in seen_keys:
                continue
            seen_keys.add(profile_key)
            viewers.append({**profile, "profile_key": profile_key})

    return {
        "viewer_count": viewer_count,
        "available_viewer_count": len(viewers),
        "view_title": view_title,
        "viewers": viewers,
    }


def ingest_profile_view_analytics(target_key: str, payload: dict[str, Any]) -> dict[str, Any]:
    init_discovery_db()
    parsed = parse_profile_view_analytics_payload(payload)
    ingested_count = 0
    for viewer in parsed["viewers"]:
        profile_key = str(viewer.get("profile_key") or "")
        display_name = str(viewer.get("display_name") or "").strip()
        if not profile_key or not display_name:
            continue
        upsert_prospect(
            profile_key=profile_key,
            display_name=display_name,
            public_identifier=viewer.get("public_identifier"),
            member_urn=viewer.get("entity_urn"),
            profile_url=viewer.get("profile_url"),
            headline=viewer.get("headline"),
            entity_type="person",
        )
        add_source(
            profile_key,
            source_type="profile.views",
            source_value=target_key,
            metadata={"viewer_count": parsed.get("viewer_count"), "view_title": parsed.get("view_title")},
            dedupe_key=_dedupe_key("profile-view-source", profile_key, target_key, str(parsed.get("viewer_count") or 0)),
        )
        add_signal(
            profile_key,
            signal_type="profile_view",
            source="analytics",
            metadata={"target_key": target_key, "view_title": parsed.get("view_title")},
            dedupe_key=_dedupe_key("profile-view", profile_key, target_key),
        )
        ingested_count += 1
    return {
        "target_key": target_key,
        "viewer_count": parsed.get("viewer_count", 0),
        "available_viewer_count": parsed.get("available_viewer_count", 0),
        "ingested_count": ingested_count,
        "view_title": parsed.get("view_title"),
        "viewers": parsed.get("viewers", []),
    }


def ingest_public_post_engagement(target_key: str, post_url: str, html: str) -> dict[str, Any]:
    init_discovery_db()
    soup = BeautifulSoup(html, "html.parser")
    objects = _parse_json_ld_objects(html)
    post_object = next(
        (
            item
            for item in objects
            if isinstance(item, dict) and item.get("@type") == "SocialMediaPosting"
        ),
        {},
    )
    reaction_count = 0
    comment_count = 0
    for stat in post_object.get("interactionStatistic") or []:
        if not isinstance(stat, dict):
            continue
        interaction = str(stat.get("interactionType") or "")
        count = int(stat.get("userInteractionCount") or 0)
        if "LikeAction" in interaction:
            reaction_count = count
        if "CommentAction" in interaction:
            comment_count = count

    post_key = _post_key_from_url(post_url)
    upsert_prospect(
        profile_key=post_key,
        display_name=post_object.get("headline") or (soup.title.get_text(" ", strip=True) if soup.title else post_key),
        profile_url=post_url,
        headline=post_object.get("headline"),
        entity_type="post",
    )
    add_source(
        post_key,
        source_type="public.engagement",
        source_value=target_key,
        metadata={"post_url": post_url, "reaction_count": reaction_count, "comment_count": comment_count},
        dedupe_key=_dedupe_key("public-post", target_key, post_url),
    )
    add_signal(
        post_key,
        signal_type="liked",
        source="public",
        weight=min(float(reaction_count) * 0.1, 5.0) if reaction_count else 0.0,
        metadata={"post_url": post_url, "reaction_count": reaction_count, "comment_count": comment_count},
        dedupe_key=_dedupe_key("public-reactions", post_url, str(reaction_count), str(comment_count)),
    )

    commenters: dict[str, dict[str, Any]] = {}
    likers: dict[str, dict[str, Any]] = {}
    reposters: dict[str, dict[str, Any]] = {}
    for comment in post_object.get("comment") or []:
        if not isinstance(comment, dict):
            continue
        author = comment.get("author") or {}
        if not isinstance(author, dict):
            continue
        name = author.get("name")
        author_url = author.get("url")
        author_type = "company" if author.get("@type") == "Organization" else "person"
        if not name:
            continue
        record = _commenter_record(name, author_url, author_type)
        commenters[record["profile_key"]] = record

    for anchor in soup.select("a[data-tracking-control-name*=public_post_comment_actor-name]"):
        name = " ".join(anchor.get_text(" ", strip=True).split())
        href = anchor.get("href")
        if not name:
            continue
        record = _commenter_record(name, href)
        commenters.setdefault(record["profile_key"], record)

    for anchor in soup.select("a[data-tracking-control-name*=reaction_actor], a[data-tracking-control-name*=reactor], a[data-control-name*=reaction]"):
        name = " ".join(anchor.get_text(" ", strip=True).split())
        href = anchor.get("href")
        if not name:
            continue
        record = _commenter_record(name, href)
        likers.setdefault(record["profile_key"], record)

    for anchor in soup.select("a[data-tracking-control-name*=repost_actor], a[data-control-name*=reshare], a[data-control-name*=repost]"):
        name = " ".join(anchor.get_text(" ", strip=True).split())
        href = anchor.get("href")
        if not name:
            continue
        record = _commenter_record(name, href)
        reposters.setdefault(record["profile_key"], record)

    for profile_key, commenter in commenters.items():
        upsert_prospect(
            profile_key=profile_key,
            display_name=commenter["display_name"],
            public_identifier=commenter["public_identifier"],
            profile_url=commenter["profile_url"],
            entity_type=commenter["entity_type"],
        )
        add_source(
            profile_key,
            source_type="public.engagement",
            source_value=post_url,
            metadata={"target_key": target_key, "post_url": post_url},
            dedupe_key=_dedupe_key("public-source", profile_key, post_url),
        )
        add_signal(
            profile_key,
            signal_type="commented",
            source="public",
            metadata={
                "target_key": target_key,
                "post_url": post_url,
                "reaction_count": reaction_count,
                "comment_count": comment_count,
            },
            dedupe_key=_dedupe_key("public-comment", profile_key, post_url),
        )

    for profile_key, liker in likers.items():
        upsert_prospect(
            profile_key=profile_key,
            display_name=liker["display_name"],
            public_identifier=liker["public_identifier"],
            profile_url=liker["profile_url"],
            entity_type=liker["entity_type"],
        )
        add_source(
            profile_key,
            source_type="public.engagement",
            source_value=post_url,
            metadata={"target_key": target_key, "post_url": post_url, "engagement": "liked"},
            dedupe_key=_dedupe_key("public-like-source", profile_key, post_url),
        )
        add_signal(
            profile_key,
            signal_type="liked",
            source="public",
            metadata={"target_key": target_key, "post_url": post_url},
            dedupe_key=_dedupe_key("public-like", profile_key, post_url),
        )

    for profile_key, reposter in reposters.items():
        upsert_prospect(
            profile_key=profile_key,
            display_name=reposter["display_name"],
            public_identifier=reposter["public_identifier"],
            profile_url=reposter["profile_url"],
            entity_type=reposter["entity_type"],
        )
        add_source(
            profile_key,
            source_type="public.engagement",
            source_value=post_url,
            metadata={"target_key": target_key, "post_url": post_url, "engagement": "reposted"},
            dedupe_key=_dedupe_key("public-repost-source", profile_key, post_url),
        )
        add_signal(
            profile_key,
            signal_type="reposted",
            source="public",
            metadata={"target_key": target_key, "post_url": post_url},
            dedupe_key=_dedupe_key("public-repost", profile_key, post_url),
        )

    return {
        "target_key": target_key,
        "post_key": post_key,
        "post_url": post_url,
        "reaction_count": reaction_count,
        "comment_count": comment_count,
        "commenter_count": len(commenters),
        "liker_count": len(likers),
        "reposter_count": len(reposters),
    }


def ingest_inbox_conversations(
    conversations: list[dict[str, Any]],
    self_member_urn: str | None = None,
) -> int:
    created = 0
    for conversation in conversations:
        conversation_urn = conversation.get("conversation_urn") or conversation.get("urn") or ""
        messages = sorted(conversation.get("messages") or [], key=lambda item: item.get("created_at") or 0)
        for participant in conversation.get("participants", []):
            profile_key = participant.get("profile_key") or participant.get("public_identifier")
            display_name = participant.get("display_name") or participant.get("name") or profile_key
            if not profile_key or not display_name:
                continue
            upsert_prospect(
                profile_key=profile_key,
                display_name=display_name,
                public_identifier=participant.get("public_identifier"),
                member_urn=participant.get("member_urn"),
                entity_type=participant.get("entity_type") or "person",
            )
            add_source(
                profile_key,
                source_type="inbox",
                source_value=conversation_urn,
                metadata={"conversation_urn": conversation_urn},
                dedupe_key=_dedupe_key("inbox-source", profile_key, conversation_urn),
            )
            add_signal(
                profile_key,
                signal_type="active_thread",
                source="inbox",
                metadata={"conversation_urn": conversation_urn},
                dedupe_key=_dedupe_key("active-thread", profile_key, conversation_urn),
            )

            participant_urn = participant.get("member_urn")
            if messages and self_member_urn and participant_urn:
                seen_self = False
                for message in messages:
                    sender_urn = message.get("sender_urn")
                    message_urn = message.get("message_urn") or f"{conversation_urn}:{message.get('created_at')}"
                    if sender_urn == self_member_urn:
                        seen_self = True
                        continue
                    if sender_urn == participant_urn:
                        add_signal(
                            profile_key,
                            signal_type="inbound_dm",
                            source="inbox",
                            notes=message.get("text"),
                            metadata={"conversation_urn": conversation_urn, "message_urn": message_urn},
                            dedupe_key=_dedupe_key("inbound", profile_key, message_urn),
                        )
                        if seen_self:
                            add_signal(
                                profile_key,
                                signal_type="replied_dm",
                                source="inbox",
                                notes=message.get("text"),
                                metadata={"conversation_urn": conversation_urn, "message_urn": message_urn},
                                dedupe_key=_dedupe_key("reply", profile_key, message_urn),
                            )
            created += 1
    return created


def _signal_success_rate(conn: Any, signal_type: str) -> float:
    total = conn.execute(
        "SELECT COUNT(DISTINCT prospect_key) AS count FROM prospect_signals WHERE signal_type = ?",
        (signal_type,),
    ).fetchone()["count"] or 0
    if not total:
        return 0.0
    success = conn.execute(
        """
        SELECT COUNT(DISTINCT g.prospect_key) AS count
        FROM prospect_signals g
        JOIN prospects p ON p.profile_key = g.prospect_key
        WHERE g.signal_type = ?
          AND (
            p.state IN ('engaged', 'won') OR EXISTS (
              SELECT 1 FROM prospect_signals g2
              WHERE g2.prospect_key = g.prospect_key
                AND g2.signal_type IN ('replied_dm', 'accepted', 'manual_positive')
            )
          )
        """,
        (signal_type,),
    ).fetchone()["count"] or 0
    return float(success) / float(total)


def _source_success_rate(conn: Any, source_type: str) -> float:
    total = conn.execute(
        "SELECT COUNT(DISTINCT prospect_key) AS count FROM prospect_sources WHERE source_type = ?",
        (source_type,),
    ).fetchone()["count"] or 0
    if not total:
        return 0.0
    success = conn.execute(
        """
        SELECT COUNT(DISTINCT s.prospect_key) AS count
        FROM prospect_sources s
        JOIN prospects p ON p.profile_key = s.prospect_key
        WHERE s.source_type = ?
          AND (
            p.state IN ('engaged', 'won') OR EXISTS (
              SELECT 1 FROM prospect_signals g
              WHERE g.prospect_key = s.prospect_key
                AND g.signal_type IN ('replied_dm', 'accepted', 'manual_positive')
            )
          )
        """,
        (source_type,),
    ).fetchone()["count"] or 0
    return float(success) / float(total)


def _recalculate_scores(profile_key: str) -> None:
    conn = _connect()
    try:
        prospect = conn.execute("SELECT * FROM prospects WHERE profile_key = ?", (profile_key,)).fetchone()
        if prospect is None:
            return

        sources = conn.execute(
            "SELECT source_type FROM prospect_sources WHERE prospect_key = ?",
            (profile_key,),
        ).fetchall()
        signals = conn.execute(
            "SELECT signal_type, weight, confidence, created_at FROM prospect_signals WHERE prospect_key = ?",
            (profile_key,),
        ).fetchall()

        fit_score = sum(SOURCE_WEIGHTS.get(row["source_type"], 1.0) for row in sources)
        intent_score = sum(float(row["weight"]) * float(row["confidence"]) for row in signals)

        last_signal_at = prospect["last_signal_at"]
        if signals:
            last_signal_at = max(row["created_at"] for row in signals)

        freshness_score = 0.0
        staleness_score = 0.0
        if last_signal_at:
            signal_dt = datetime.fromisoformat(last_signal_at)
            age_days = max((datetime.now(timezone.utc) - signal_dt).total_seconds() / 86400.0, 0)
            if age_days <= 7:
                freshness_score = 4.0
            elif age_days <= 30:
                freshness_score = 2.0
            if age_days > 30:
                staleness_score = 3.0

        outreach_count = sum(
            1
            for row in signals
            if row["signal_type"] in {"outreach_sent", "follow_up_sent", "connection_requested"}
        )
        saturation_score = float(outreach_count * 2)

        source_bonus = sum(_source_success_rate(conn, row["source_type"]) * 2.0 for row in sources)
        signal_bonus = sum(
            _signal_success_rate(conn, row["signal_type"]) * 1.0
            for row in signals
            if row["signal_type"] not in POSITIVE_SIGNAL_TYPES
        )
        learned_score = round(source_bonus + signal_bonus, 2)

        total_score = round(
            fit_score + intent_score + freshness_score - saturation_score - staleness_score + learned_score,
            2,
        )

        conn.execute(
            """
            UPDATE prospects
            SET
                updated_at = ?,
                score = ?,
                fit_score = ?,
                intent_score = ?,
                freshness_score = ?,
                saturation_score = ?,
                staleness_score = ?,
                learned_score = ?,
                last_signal_at = ?
            WHERE profile_key = ?
            """,
            (
                _now_iso(),
                total_score,
                float(fit_score),
                float(intent_score),
                freshness_score,
                saturation_score,
                staleness_score,
                learned_score,
                last_signal_at,
                profile_key,
            ),
        )
        conn.commit()
    finally:
        conn.close()
