"""Lead enrichment, ranking, and autopilot helpers."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

from linkedin_cli import modeling
from linkedin_cli.session import build_session, load_session, request
from linkedin_cli.write import store


LEAD_FEATURE_VERSION = "lead-v2"
REPLY_MODEL_NAME = "reply_likelihood"
DEAL_MODEL_NAME = "deal_likelihood"


def init_lead_db() -> None:
    from linkedin_cli import discovery

    discovery.init_discovery_db()
    conn = store._connect()
    try:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS lead_features (
                profile_key TEXT PRIMARY KEY,
                feature_version TEXT NOT NULL,
                fit_score REAL NOT NULL DEFAULT 0,
                reply_likelihood REAL NOT NULL DEFAULT 0,
                deal_likelihood REAL NOT NULL DEFAULT 0,
                features_json TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS lead_labels (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                profile_key TEXT NOT NULL,
                label_type TEXT NOT NULL,
                label_value REAL NOT NULL,
                label_time TEXT NOT NULL,
                metadata_json TEXT NOT NULL DEFAULT '{}'
            );
            """
        )
        _ensure_prospect_columns(conn)
        conn.commit()
    finally:
        conn.close()
    modeling.init_modeling_db()


def _ensure_column(conn: Any, table: str, column: str, ddl: str) -> None:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    existing = {row["name"] if isinstance(row, dict) else row[1] for row in rows}
    if column not in existing:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {ddl}")


def _ensure_prospect_columns(conn: Any) -> None:
    _ensure_column(conn, "prospects", "reply_likelihood", "REAL NOT NULL DEFAULT 0")
    _ensure_column(conn, "prospects", "deal_likelihood", "REAL NOT NULL DEFAULT 0")
    _ensure_column(conn, "prospects", "last_enriched_at", "TEXT")


def _tokens(*values: str) -> list[str]:
    tokens: list[str] = []
    for value in values:
        for token in re.findall(r"[a-z0-9][a-z0-9+#.-]{1,}", (value or "").lower()):
            if token not in tokens:
                tokens.append(token)
    return tokens


def build_lead_features(
    *,
    profile_key: str,
    profile: dict[str, Any] | None,
    company: dict[str, Any] | None,
    signals: list[dict[str, Any]] | None,
    target_topics: list[str] | None = None,
) -> dict[str, Any]:
    profile = profile or {}
    company = company or {}
    signals = signals or []
    target_topics = [topic.strip().lower() for topic in (target_topics or []) if topic and topic.strip()]
    text_tokens = _tokens(
        str(profile.get("display_name") or ""),
        str(profile.get("headline") or ""),
        str(company.get("name") or ""),
        str(company.get("description") or ""),
        str(company.get("industry") or ""),
        str(profile.get("location") or ""),
    )
    overlap = sorted({topic for topic in target_topics if topic in text_tokens})
    signal_counts: dict[str, int] = {}
    for signal in signals:
        key = str(signal.get("signal_type") or "unknown")
        signal_counts[key] = signal_counts.get(key, 0) + 1
    founder_keyword = any(token in text_tokens for token in ("founder", "ceo", "owner", "cofounder"))
    ai_keyword = any(token in text_tokens for token in ("ai", "llm", "agent", "workflow", "automation"))
    fit_score = round(
        min(
            0.99,
            (0.14 * len(overlap))
            + (0.22 * signal_counts.get("commented", 0))
            + (0.18 * signal_counts.get("profile_view", 0))
            + (0.26 * signal_counts.get("replied_dm", 0))
            + (0.08 * signal_counts.get("liked", 0))
            + (0.06 * signal_counts.get("reposted", 0))
            + (0.08 if founder_keyword else 0.0)
            + (0.06 if ai_keyword else 0.0),
        ),
        4,
    )
    numeric_features = {
        "fit_score": fit_score,
        "topic_overlap_count": float(len(overlap)),
        "commented": float(signal_counts.get("commented", 0)),
        "profile_view": float(signal_counts.get("profile_view", 0)),
        "replied_dm": float(signal_counts.get("replied_dm", 0)),
        "liked": float(signal_counts.get("liked", 0)),
        "reposted": float(signal_counts.get("reposted", 0)),
        "accepted": float(signal_counts.get("accepted", 0)),
        "outreach_sent": float(signal_counts.get("outreach_sent", 0)),
        "active_thread": float(signal_counts.get("active_thread", 0)),
        "has_founder_keyword": 1.0 if founder_keyword else 0.0,
        "has_ai_keyword": 1.0 if ai_keyword else 0.0,
    }
    features = {
        "profile_key": profile_key,
        "topic_overlap_terms": overlap,
        "signal_counts": signal_counts,
        "headline_tokens": text_tokens,
        "has_founder_keyword": founder_keyword,
        "has_ai_keyword": ai_keyword,
        "numeric": numeric_features,
    }
    return {
        "profile_key": profile_key,
        "fit_score": fit_score,
        "features": features,
    }


def upsert_lead_features(
    profile_key: str,
    features: dict[str, Any],
    fit_score: float,
    *,
    reply_likelihood: float = 0.0,
    deal_likelihood: float = 0.0,
    feature_version: str = LEAD_FEATURE_VERSION,
) -> dict[str, Any]:
    init_lead_db()
    now = store._now_iso()
    conn = store._connect()
    try:
        conn.execute(
            """
            INSERT INTO lead_features (profile_key, feature_version, fit_score, reply_likelihood, deal_likelihood, features_json, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(profile_key) DO UPDATE SET
                feature_version = excluded.feature_version,
                fit_score = excluded.fit_score,
                reply_likelihood = excluded.reply_likelihood,
                deal_likelihood = excluded.deal_likelihood,
                features_json = excluded.features_json,
                updated_at = excluded.updated_at
            """,
            (
                profile_key,
                feature_version,
                float(fit_score),
                float(reply_likelihood),
                float(deal_likelihood),
                json.dumps(features, ensure_ascii=False, sort_keys=True),
                now,
            ),
        )
        conn.execute(
            """
            UPDATE prospects
            SET fit_score = ?, reply_likelihood = ?, deal_likelihood = ?, last_enriched_at = ?, updated_at = ?
            WHERE profile_key = ?
            """,
            (float(fit_score), float(reply_likelihood), float(deal_likelihood), now, now, profile_key),
        )
        conn.commit()
        row = conn.execute("SELECT * FROM lead_features WHERE profile_key = ?", (profile_key,)).fetchone()
        assert row is not None
        return _row_to_dict(row)
    finally:
        conn.close()


def get_lead_features(profile_key: str) -> dict[str, Any] | None:
    init_lead_db()
    conn = store._connect()
    try:
        row = conn.execute("SELECT * FROM lead_features WHERE profile_key = ?", (profile_key,)).fetchone()
        return _row_to_dict(row) if row else None
    finally:
        conn.close()


def get_lead(profile_key: str) -> dict[str, Any] | None:
    from linkedin_cli import discovery

    prospect = discovery.get_prospect(profile_key)
    features = get_lead_features(profile_key)
    if not prospect and not features:
        return None
    payload = dict(prospect or {})
    if features:
        payload.update(
            {
                "fit_score": features["fit_score"],
                "reply_likelihood": features["reply_likelihood"],
                "deal_likelihood": features["deal_likelihood"],
                "lead_features": features["features"],
                "feature_version": features["feature_version"],
                "lead_updated_at": features["updated_at"],
            }
        )
    return payload


def _artifact_dir() -> Path:
    return store.ARTIFACTS_DIR / "models"


def _label_value(prospect: dict[str, Any], label_type: str) -> float:
    signals = {str(signal.get("signal_type") or "") for signal in prospect.get("signals") or []}
    state = str(prospect.get("state") or "")
    if label_type == REPLY_MODEL_NAME:
        return 1.0 if {"replied_dm", "inbound_dm"} & signals else 0.0
    if label_type == DEAL_MODEL_NAME:
        if state in {"engaged", "won"} or {"accepted", "manual_positive"} & signals:
            return 1.0
        return 0.0
    return 0.0


def _train_from_prospects(prospects: list[dict[str, Any]], target_topics: list[str] | None = None) -> dict[str, dict[str, Any]]:
    artifacts: dict[str, dict[str, Any]] = {}
    tasks = (REPLY_MODEL_NAME, DEAL_MODEL_NAME)
    for task in tasks:
        samples: list[dict[str, Any]] = []
        for prospect in prospects:
            built = build_lead_features(
                profile_key=str(prospect.get("profile_key") or ""),
                profile=prospect,
                company={"name": prospect.get("company"), "description": prospect.get("headline")},
                signals=prospect.get("signals") or [],
                target_topics=target_topics,
            )
            samples.append({"features": built["features"]["numeric"], "label": _label_value(prospect, task)})
        labels = {int(sample["label"]) for sample in samples}
        if len(samples) >= 6 and labels == {0, 1}:
            artifacts[task] = modeling.train_model(
                task=task,
                samples=samples,
                artifact_dir=_artifact_dir(),
                model_name=task,
            )
    return artifacts


def _ingest_post_engagers(
    *,
    post_urls: list[str],
    fetch_html: Any | None,
) -> int:
    from linkedin_cli import discovery

    ingested = 0
    if not post_urls:
        return ingested
    session = None
    html_fetch = fetch_html
    if html_fetch is None:
        session, _ = load_session(required=False)
        session = session or build_session()

        def html_fetch(url: str) -> str:
            assert session is not None
            return request(session, "GET", url).text

    for post_url in post_urls:
        html = html_fetch(post_url)
        discovery.ingest_public_post_engagement("lead.autopilot", post_url, html)
        ingested += 1
    return ingested


def _owned_post_urls() -> list[str]:
    from linkedin_cli import content

    posts = content.list_posts(limit=100000)
    return [str(post["url"]) for post in posts if post.get("owned_by_me")]


def _prospects_for_post_urls(post_urls: list[str], limit: int) -> list[dict[str, Any]]:
    from linkedin_cli import discovery

    if not post_urls:
        return []
    conn = store._connect()
    try:
        placeholders = ",".join("?" for _ in post_urls)
        rows = conn.execute(
            f"""
            SELECT DISTINCT p.profile_key
            FROM prospects p
            JOIN prospect_sources s ON s.prospect_key = p.profile_key
            WHERE p.entity_type = 'person'
              AND s.source_type = 'public.engagement'
              AND s.source_value IN ({placeholders})
            ORDER BY p.updated_at DESC
            LIMIT ?
            """,
            (*post_urls, max(1, int(limit))),
        ).fetchall()
    finally:
        conn.close()
    prospects: list[dict[str, Any]] = []
    for row in rows:
        prospect = discovery.get_prospect(str(row["profile_key"]))
        if prospect:
            prospects.append(prospect)
    return prospects


def _predict(task: str, numeric_features: dict[str, float], fallback: float) -> float:
    try:
        return modeling.predict_probability(task, numeric_features)
    except Exception:
        return round(float(fallback), 6)


def _recommended_state(fit_score: float, reply_likelihood: float, deal_likelihood: float, *, min_fit: float, min_reply: float, min_deal: float) -> str:
    if fit_score >= min_fit and (reply_likelihood >= min_reply or deal_likelihood >= min_deal):
        return "ready"
    if fit_score >= (min_fit * 0.6) or reply_likelihood >= (min_reply * 0.7):
        return "watch"
    return "new"


def _contact_stage(recommended_state: str, deal_likelihood: float, reply_likelihood: float) -> str:
    if recommended_state == "ready" and (deal_likelihood >= 0.3 or reply_likelihood >= 0.5):
        return "qualified"
    if recommended_state in {"ready", "watch"}:
        return "active"
    return "new"


def run_autopilot(
    *,
    target_topics: list[str] | None = None,
    prospects: list[dict[str, Any]] | None = None,
    post_urls: list[str] | None = None,
    all_owned: bool = False,
    fetch_html: Any | None = None,
    limit: int = 25,
    state: str | None = None,
    min_fit: float = 0.45,
    min_reply: float = 0.35,
    min_deal: float = 0.25,
    sync_contacts: bool = False,
    dry_run: bool = True,
) -> dict[str, Any]:
    from linkedin_cli import discovery, workflow

    init_lead_db()
    effective_post_urls = list(post_urls or [])
    if all_owned:
        effective_post_urls.extend(_owned_post_urls())
    effective_post_urls = list(dict.fromkeys(effective_post_urls))
    ingested_posts = _ingest_post_engagers(post_urls=effective_post_urls, fetch_html=fetch_html) if effective_post_urls else 0

    if prospects is not None:
        source_prospects = prospects
    elif effective_post_urls:
        source_prospects = _prospects_for_post_urls(effective_post_urls, limit=max(1, int(limit)))
    else:
        source_prospects = discovery.list_queue(limit=max(1, int(limit)), state=state)
    hydrated = [discovery.get_prospect(str(item.get("profile_key") or "")) for item in source_prospects]
    hydrated = [item for item in hydrated if item and item.get("entity_type") in {None, "person"}]
    if not hydrated:
        return {
            "count": 0,
            "ingested_posts": ingested_posts,
            "routed": {"ready": 0, "watch": 0, "new": 0},
            "recommendations": [],
        }

    _train_from_prospects(hydrated, target_topics=target_topics)

    recommendations: list[dict[str, Any]] = []
    routed = {"ready": 0, "watch": 0, "new": 0}
    for prospect in hydrated:
        profile_key = str(prospect.get("profile_key") or prospect.get("public_identifier") or "").strip()
        if not profile_key:
            continue
        built = build_lead_features(
            profile_key=profile_key,
            profile=prospect,
            company={"name": prospect.get("company"), "description": prospect.get("headline")},
            signals=prospect.get("signals") or [],
            target_topics=target_topics,
        )
        numeric = built["features"]["numeric"]
        reply_likelihood = _predict(
            REPLY_MODEL_NAME,
            numeric,
            min(0.95, built["fit_score"] + (0.22 * numeric.get("replied_dm", 0.0)) + (0.08 * numeric.get("commented", 0.0))),
        )
        deal_likelihood = _predict(
            DEAL_MODEL_NAME,
            numeric,
            min(0.9, (0.7 * built["fit_score"]) + (0.18 if numeric.get("has_founder_keyword") else 0.0)),
        )
        recommended_state = _recommended_state(
            built["fit_score"],
            reply_likelihood,
            deal_likelihood,
            min_fit=min_fit,
            min_reply=min_reply,
            min_deal=min_deal,
        )
        routed[recommended_state] += 1
        if not dry_run:
            current_state = str(prospect.get("state") or "new")
            if current_state not in {"engaged", "won", "do_not_contact"} and recommended_state != current_state:
                discovery.set_prospect_state(profile_key, recommended_state)
        upsert_lead_features(
            profile_key,
            built["features"],
            built["fit_score"],
            reply_likelihood=reply_likelihood,
            deal_likelihood=deal_likelihood,
        )
        if sync_contacts:
            workflow.upsert_contact(
                profile_key=profile_key,
                display_name=str(prospect.get("display_name") or profile_key),
                stage=_contact_stage(recommended_state, deal_likelihood, reply_likelihood),
                tags=[
                    *[f"topic:{topic}" for topic in built["features"]["topic_overlap_terms"]],
                    f"queue:{recommended_state}",
                ],
                notes=f"fit={built['fit_score']:.4f} reply={reply_likelihood:.4f} deal={deal_likelihood:.4f}",
            )
        recommendations.append(
            {
                "profile_key": profile_key,
                "display_name": prospect.get("display_name"),
                "fit_score": built["fit_score"],
                "reply_likelihood": reply_likelihood,
                "deal_likelihood": deal_likelihood,
                "recommended_state": recommended_state,
                "features": built["features"],
                "dry_run": dry_run,
            }
        )
    recommendations.sort(
        key=lambda item: (item["deal_likelihood"], item["reply_likelihood"], item["fit_score"]),
        reverse=True,
    )
    return {
        "count": len(recommendations),
        "ingested_posts": ingested_posts,
        "routed": routed,
        "recommendations": recommendations,
    }


def rank_leads(limit: int = 25) -> list[dict[str, Any]]:
    init_lead_db()
    conn = store._connect()
    try:
        rows = conn.execute(
            """
            SELECT
                p.profile_key,
                p.display_name,
                p.state,
                p.company,
                p.fit_score,
                p.reply_likelihood,
                p.deal_likelihood,
                p.last_enriched_at
            FROM prospects p
            LEFT JOIN lead_features lf ON lf.profile_key = p.profile_key
            WHERE p.entity_type = 'person'
            ORDER BY p.deal_likelihood DESC, p.reply_likelihood DESC, p.fit_score DESC, p.updated_at DESC
            LIMIT ?
            """,
            (max(1, int(limit)),),
        ).fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()


def _row_to_dict(row: Any) -> dict[str, Any]:
    result = dict(row)
    result["features"] = json.loads(result.pop("features_json") or "{}")
    return result
