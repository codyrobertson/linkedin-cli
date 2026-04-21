"""Public LinkedIn post harvesting and lightweight content analysis."""

from __future__ import annotations

import json
import os
import re
import math
import hashlib
import sqlite3
import threading
import time
import random
import uuid
from datetime import datetime, timezone
from pathlib import Path
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, as_completed, wait
from collections import Counter, defaultdict
from functools import lru_cache
from typing import Any, Callable
from urllib.parse import quote
from urllib.parse import urlsplit, urlunsplit

from bs4 import BeautifulSoup
import requests

from linkedin_cli import retrieval_index
from linkedin_cli.config import DEFAULT_TIMEOUT
from linkedin_cli.search import ddg_html_search, filter_linkedin_search_results, resolve_public_search_fn
from linkedin_cli.session import CliError, ExitCode, build_session, fail, load_session, request
from linkedin_cli.voyager import clean_text, parse_json_response, voyager_get
from linkedin_cli.write import store

FINGERPRINT_VERSION = "content-fingerprint-v1"
OUTCOME_MODEL_NAME = "default"
FINGERPRINT_DIMENSIONS = (
    "hook_strength",
    "specificity",
    "novelty",
    "authority",
    "narrative",
    "instructional",
    "contrarian",
    "emotionality",
    "engagement_cta",
    "listiness",
    "proof",
    "density",
    "hashtag_load",
)

STOPWORDS = {
    "a", "an", "and", "are", "as", "at", "be", "been", "but", "by", "for", "from", "had", "has", "have",
    "how", "i", "if", "in", "into", "is", "it", "its", "just", "my", "not", "of", "on", "or", "our",
    "so", "that", "the", "their", "them", "there", "this", "to", "was", "we", "were", "what", "when",
    "which", "who", "why", "will", "with", "you", "your",
}

ANN_SIGNATURE_BITS = 64
ANN_BAND_BITS = 8
ANN_SHORTLIST_MULTIPLIER = 12
RETRIEVAL_INDEX_VERSION = "2026-03-26"
HARVEST_JOB_STATUS_PENDING = "pending"
HARVEST_JOB_STATUS_RUNNING = "running"
HARVEST_JOB_STATUS_COMPLETED = "completed"
HARVEST_JOB_STATUS_FAILED = "failed"
_CONTENT_DB_INIT_LOCK = threading.Lock()
_CONTENT_DB_INITIALIZED_PATHS: set[str] = set()
_SQLITE_LOCK_RETRY_ATTEMPTS = 8
_SQLITE_LOCK_RETRY_BASE_SECONDS = 0.25
_HARVEST_STALE_QUERY_SECONDS = 30 * 60


KNOWN_EMBEDDING_DIMS = {
    "fastembed:BAAI/bge-small-en-v1.5": 384,
    "local-hash-v1": 256,
}
STRUCTURE_FEATURES = ("list", "question", "how_to", "insight")
HOOK_TYPE_FEATURES = ("question", "number", "directive", "contrarian", "personal", "statement")
INDUSTRY_RELEVANCE_HINTS = {
    "ai": ("ai", "artificial intelligence", "agent", "agents", "agentic", "llm", "llms", "gpt", "openai", "anthropic", "genai"),
    "fintech": ("fintech", "payments", "banking", "finance", "financial", "insurtech", "lending", "wealthtech"),
    "cybersecurity": ("cybersecurity", "cyber", "security", "infosec", "threat", "soc", "vulnerability"),
    "healthcare": ("healthcare", "health", "clinical", "patient", "medtech", "payer", "provider"),
    "sales": ("sales", "revops", "outbound", "prospecting", "sdr", "ae", "pipeline"),
    "marketing": ("marketing", "growth", "demand gen", "brand", "ads", "campaign", "attribution"),
    "devtools": ("devtools", "developer", "developers", "engineering", "software", "api", "platform"),
    "cloud": ("cloud", "aws", "azure", "gcp", "kubernetes", "infra", "infrastructure"),
    "data": ("data", "analytics", "warehouse", "etl", "pipeline", "lakehouse", "sql", "vector"),
    "recruiting": ("recruiting", "recruiter", "talent", "candidate", "hiring", "sourcing"),
}


def _ensure_column(conn: Any, table: str, column: str, ddl: str) -> None:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    existing = {row["name"] if isinstance(row, dict) else row[1] for row in rows}
    if column not in existing:
        try:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {ddl}")
        except sqlite3.OperationalError as exc:
            if "duplicate column name" not in str(exc).lower():
                raise


def _backfill_fingerprints(conn: Any) -> None:
    rows = conn.execute(
        """
        SELECT url, text, title
        FROM harvested_posts
        WHERE fingerprint_json IS NULL OR fingerprint_version IS NULL
        LIMIT 5000
        """
    ).fetchall()
    if not rows:
        return
    now = store._now_iso()
    for row in rows:
        payload = dict(row)
        fingerprint = _compute_content_fingerprint(str(payload.get("text") or ""), title=str(payload.get("title") or ""))
        conn.execute(
            """
            UPDATE harvested_posts
            SET fingerprint_version = ?, fingerprint_dimensions = ?, fingerprint_json = ?, fingerprinted_at = ?
            WHERE url = ?
            """,
            (FINGERPRINT_VERSION, len(fingerprint), json.dumps(fingerprint), now, payload["url"]),
        )


def _backfill_dimensions(conn: Any) -> None:
    rows = conn.execute(
        """
        SELECT url, embedding_model, embedding_json, fingerprint_version, fingerprint_json
        FROM harvested_posts
        WHERE embedding_dimensions IS NULL OR (fingerprint_version IS NOT NULL AND fingerprint_dimensions IS NULL)
        LIMIT 5000
        """
    ).fetchall()
    if not rows:
        return
    for row in rows:
        payload = dict(row)
        embedding_dimensions = None
        embedding_json = payload.get("embedding_json")
        if embedding_json:
            try:
                embedding_dimensions = len(json.loads(embedding_json))
            except Exception:
                embedding_dimensions = _embedding_dimension_for_model(str(payload.get("embedding_model") or ""))
        fingerprint_dimensions = None
        fingerprint_json = payload.get("fingerprint_json")
        if payload.get("fingerprint_version"):
            fingerprint_dimensions = len(FINGERPRINT_DIMENSIONS)
            if fingerprint_json:
                try:
                    fingerprint_dimensions = len(json.loads(fingerprint_json))
                except Exception:
                    fingerprint_dimensions = len(FINGERPRINT_DIMENSIONS)
        conn.execute(
            """
            UPDATE harvested_posts
            SET embedding_dimensions = COALESCE(embedding_dimensions, ?),
                fingerprint_dimensions = COALESCE(fingerprint_dimensions, ?)
            WHERE url = ?
            """,
            (embedding_dimensions, fingerprint_dimensions, payload["url"]),
        )


def _backfill_outcome_scores(conn: Any) -> None:
    rows = conn.execute(
        """
        SELECT url, reaction_count, comment_count, word_count
        FROM harvested_posts
        WHERE outcome_score IS NULL OR outcome_score = 0
        LIMIT 5000
        """
    ).fetchall()
    for row in rows:
        payload = dict(row)
        conn.execute(
            """
            UPDATE harvested_posts
            SET outcome_score = ?
            WHERE url = ?
            """,
            (
                _engagement_score(
                    int(payload.get("reaction_count") or 0),
                    int(payload.get("comment_count") or 0),
                    int(payload.get("word_count") or 0),
                ),
                payload["url"],
            ),
        )


def _index_record_for_row(conn: Any, row: dict[str, Any]) -> None:
    now = store._now_iso()
    url = str(row.get("url") or "")
    if not url:
        return
    conn.execute("DELETE FROM retrieval_bands WHERE url = ?", (url,))
    fingerprint = row.get("fingerprint")
    if isinstance(fingerprint, list) and fingerprint:
        for band_no, band_hash in _band_hashes(_chunk_signature_bits([float(value) for value in fingerprint])):
            conn.execute(
                """
                INSERT OR REPLACE INTO retrieval_bands (url, index_kind, model_key, band_no, band_hash, updated_at)
                VALUES (?, 'fingerprint', ?, ?, ?, ?)
                """,
                (url, FINGERPRINT_VERSION, band_no, band_hash, now),
            )
    embedding = row.get("embedding")
    model = str(row.get("embedding_model") or "")
    if isinstance(embedding, list) and embedding and model:
        for band_no, band_hash in _band_hashes(_chunk_signature_bits([float(value) for value in embedding])):
            conn.execute(
                """
                INSERT OR REPLACE INTO retrieval_bands (url, index_kind, model_key, band_no, band_hash, updated_at)
                VALUES (?, 'semantic', ?, ?, ?, ?)
                """,
                (url, model, band_no, band_hash, now),
            )


def _ensure_retrieval_index(conn: Any, limit: int = 5000) -> None:
    rows = conn.execute(
        """
        SELECT * FROM harvested_posts
        ORDER BY updated_at DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    for row in rows:
        payload = _row_to_dict(row)
        fingerprint_bands = conn.execute(
            "SELECT COUNT(*) AS count FROM retrieval_bands WHERE url = ? AND index_kind = 'fingerprint'",
            (payload["url"],),
        ).fetchone()["count"] or 0
        semantic_bands = conn.execute(
            "SELECT COUNT(*) AS count FROM retrieval_bands WHERE url = ? AND index_kind = 'semantic'",
            (payload["url"],),
        ).fetchone()["count"] or 0
        needs_fingerprint = bool(payload.get("fingerprint")) and fingerprint_bands == 0
        needs_semantic = bool(payload.get("embedding")) and semantic_bands == 0
        if needs_fingerprint or needs_semantic:
            _index_record_for_row(conn, payload)


def _normalize_industries(industries: list[str] | None = None, industry: str | None = None) -> list[str]:
    values = [clean_text(value) for value in (industries or []) if clean_text(value)]
    if industry and clean_text(industry):
        values.append(clean_text(industry))
    return list(dict.fromkeys(values))


def _normalize_topics(topics: list[str] | None = None, topic: str | None = None) -> list[str]:
    values = [clean_text(value) for value in (topics or []) if clean_text(value)]
    if topic and clean_text(topic):
        values.append(clean_text(topic))
    return list(dict.fromkeys(values))


def _phrase_tokens(value: str) -> list[str]:
    return [token for token in re.findall(r"[a-z0-9][a-z0-9+#.-]{1,}", clean_text(value).lower()) if token and token not in STOPWORDS]


def _phrase_in_text(text: str, phrase: str) -> bool:
    normalized_text = clean_text(text).lower()
    normalized_phrase = clean_text(phrase).lower()
    if not normalized_text or not normalized_phrase:
        return False
    if " " in normalized_phrase or "-" in normalized_phrase:
        return normalized_phrase in normalized_text
    return re.search(rf"(?<![a-z0-9]){re.escape(normalized_phrase)}(?![a-z0-9])", normalized_text) is not None


def _industry_terms(industry: str) -> list[str]:
    normalized = clean_text(industry).lower()
    if not normalized:
        return []
    return list(dict.fromkeys((normalized, *INDUSTRY_RELEVANCE_HINTS.get(normalized, ()))))


def _query_scope(query: str, industries: list[str] | None = None, topics: list[str] | None = None) -> tuple[list[str], list[str]]:
    normalized_query = clean_text(
        query.replace('"', "").replace("site:linkedin.com/posts", "").replace("site:linkedin.com/feed/update", "")
    ).lower()
    normalized_industries = _normalize_industries(industries)
    normalized_topics = _normalize_topics(topics)
    matched_industries = [value for value in normalized_industries if _phrase_in_text(normalized_query, value)]
    matched_topics = [value for value in normalized_topics if _phrase_in_text(normalized_query, value)]
    if matched_topics:
        return matched_industries, matched_topics

    residual = normalized_query
    for industry_value in matched_industries:
        residual = re.sub(rf"(?<![a-z0-9]){re.escape(industry_value.lower())}(?![a-z0-9])", " ", residual)
    residual_topics: list[str] = []
    for token in _extract_topic_tokens(residual, limit=6):
        if token not in residual_topics:
            residual_topics.append(token)
    return matched_industries, residual_topics


def _post_corpus_text(post: dict[str, Any]) -> str:
    metadata = post.get("metadata") if isinstance(post.get("metadata"), dict) else {}
    metadata_parts = []
    for key in ("query_industries", "query_topics"):
        values = metadata.get(key)
        if isinstance(values, list):
            metadata_parts.extend(str(value) for value in values)
    return "\n".join(
        str(part or "")
        for part in (
            post.get("title"),
            post.get("hook"),
            post.get("text"),
            post.get("source_query"),
            " ".join(str(value) for value in post.get("industries") or []),
            " ".join(metadata_parts),
        )
        if part
    )


def _model_matches_slice(model: dict[str, Any] | None, industry: str | None = None, topics: list[str] | None = None) -> bool:
    if not model:
        return False
    metadata = model.get("metadata") if isinstance(model.get("metadata"), dict) else {}
    requested_industry = (clean_text(industry or "") or "").lower()
    requested_topics = sorted(value.lower() for value in _normalize_topics(topics))
    model_industry = (clean_text(str(metadata.get("industry") or "")) or "").lower()
    model_topics = sorted(clean_text(str(value)).lower() for value in (metadata.get("topics") or []) if clean_text(str(value)))
    if requested_industry and model_industry and requested_industry != model_industry:
        return False
    if requested_topics and model_topics and requested_topics != model_topics:
        return False
    return True


def _post_relevance_score(post: dict[str, Any], *, industry: str | None = None, topics: list[str] | None = None) -> float:
    normalized_industry = (clean_text(industry or "") or "").lower()
    normalized_topics = _normalize_topics(topics)
    if not normalized_industry and not normalized_topics:
        return 1.0

    text = _post_corpus_text(post)
    lowered = (clean_text(text) or "").lower()
    tokens = set(_extract_topic_tokens(lowered, limit=48))
    stored_industries = {clean_text(str(value)).lower() for value in (post.get("industries") or []) if clean_text(str(value))}
    metadata = post.get("metadata") if isinstance(post.get("metadata"), dict) else {}
    stored_industries.update(
        clean_text(str(value)).lower()
        for value in (metadata.get("query_industries") or [])
        if clean_text(str(value))
    )

    industry_score = 1.0
    if normalized_industry:
        alias_terms = _industry_terms(normalized_industry)
        alias_hits = [term for term in alias_terms if _phrase_in_text(lowered, term)]
        alias_score = 1.0 if alias_hits else 0.0
        if not alias_hits:
            alias_tokens = {token for term in alias_terms for token in _phrase_tokens(term)}
            overlap = len(alias_tokens & tokens)
            alias_score = min(1.0, overlap / max(1, min(3, len(alias_tokens))))
        tagged_prior = 0.2 if normalized_industry in stored_industries else 0.0
        industry_score = max(alias_score, tagged_prior)
        if industry_score < 0.35:
            return 0.02

    topic_score = 1.0
    if normalized_topics:
        scored_topics: list[float] = []
        for topic in normalized_topics:
            phrase = clean_text(topic).lower()
            if not phrase:
                continue
            phrase_hit = 1.0 if _phrase_in_text(lowered, phrase) else 0.0
            topic_tokens = set(_phrase_tokens(phrase))
            overlap = len(topic_tokens & tokens) / max(1, len(topic_tokens)) if topic_tokens else 0.0
            negated = bool(re.search(rf"\b(?:not|nothing|without|never)\b[^.?!]{{0,40}}{re.escape(phrase)}", lowered))
            score = max(phrase_hit, overlap)
            if negated:
                score *= 0.05
            scored_topics.append(score)
        topic_score = max(scored_topics) if scored_topics else 0.0
        if topic_score < 0.35:
            return 0.02

    if normalized_industry and normalized_topics:
        return round((industry_score * 0.6) + (topic_score * 0.4), 6)
    return round(industry_score if normalized_industry else topic_score, 6)


def _filter_posts_by_relevance(
    posts: list[dict[str, Any]],
    *,
    industry: str | None = None,
    topics: list[str] | None = None,
    min_score: float = 0.35,
) -> list[dict[str, Any]]:
    if not clean_text(industry or "") and not _normalize_topics(topics):
        for post in posts:
            post["relevance_score"] = 1.0
        return posts
    filtered: list[dict[str, Any]] = []
    for post in posts:
        relevance = _post_relevance_score(post, industry=industry, topics=topics)
        post["relevance_score"] = relevance
        if relevance >= min_score:
            filtered.append(post)
    return filtered


def _backfill_post_industries(conn: Any) -> None:
    rows = conn.execute(
        """
        SELECT url, industry, updated_at
        FROM harvested_posts
        WHERE COALESCE(industry, '') != ''
        """
    ).fetchall()
    if not rows:
        return
    for row in rows:
        payload = dict(row)
        conn.execute(
            """
            INSERT OR IGNORE INTO harvested_post_industries (url, industry, created_at, updated_at)
            VALUES (?, ?, ?, ?)
            """,
            (
                payload["url"],
                str(payload["industry"]),
                payload.get("updated_at") or store._now_iso(),
                payload.get("updated_at") or store._now_iso(),
            ),
        )


def _industries_by_url(conn: Any, urls: list[str]) -> dict[str, list[str]]:
    if not urls:
        return {}
    placeholders = ",".join("?" for _ in urls)
    rows = conn.execute(
        f"""
        SELECT url, industry
        FROM harvested_post_industries
        WHERE url IN ({placeholders})
        ORDER BY industry ASC
        """,
        tuple(urls),
    ).fetchall()
    mapping: dict[str, list[str]] = defaultdict(list)
    for row in rows:
        mapping[str(row["url"])].append(str(row["industry"]))
    return mapping


def _attach_industries(conn: Any, posts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not posts:
        return posts
    mapping = _industries_by_url(conn, [str(post["url"]) for post in posts if post.get("url")])
    for post in posts:
        industries = mapping.get(str(post.get("url") or ""), [])
        if not industries:
            legacy = clean_text(str(post.get("industry") or ""))
            industries = [legacy] if legacy else []
        post["industries"] = industries
        post["industry"] = industries[0] if industries else None
    return posts


def _sync_post_industries(conn: Any, url: str, industries: list[str], *, now: str) -> None:
    conn.execute("DELETE FROM harvested_post_industries WHERE url = ?", (url,))
    for industry in industries:
        conn.execute(
            """
            INSERT INTO harvested_post_industries (url, industry, created_at, updated_at)
            VALUES (?, ?, ?, ?)
            """,
            (url, industry, now, now),
        )


def _backfill_query_context(conn: Any) -> None:
    rows = conn.execute(
        """
        SELECT url, industry, source_query, metadata_json, updated_at
        FROM harvested_posts
        WHERE COALESCE(source_query, '') != ''
        """
    ).fetchall()
    if not rows:
        return
    mapping = _industries_by_url(conn, [str(row["url"]) for row in rows])
    now = store._now_iso()
    for row in rows:
        payload = dict(row)
        url = str(payload["url"])
        source_query = str(payload.get("source_query") or "")
        legacy_industries = mapping.get(url) or _normalize_industries(industry=payload.get("industry"))
        metadata = json.loads(payload.get("metadata_json") or "{}")
        query_industries, query_topics = _query_scope(source_query, legacy_industries)
        existing_query_industries = _normalize_industries(metadata.get("query_industries"))
        existing_query_topics = _normalize_topics(metadata.get("query_topics"))
        should_repair_industries = bool(query_industries) and (
            existing_query_industries != query_industries or len(legacy_industries) > max(1, len(query_industries))
        )
        should_repair_topics = existing_query_topics != query_topics
        if not should_repair_industries and not should_repair_topics:
            continue
        metadata["query_industries"] = query_industries
        metadata["query_topics"] = query_topics
        if should_repair_industries:
            _sync_post_industries(conn, url, query_industries, now=payload.get("updated_at") or now)
        conn.execute(
            """
            UPDATE harvested_posts
            SET industry = ?, metadata_json = ?, updated_at = ?
            WHERE url = ?
            """,
            (
                query_industries[0] if query_industries else payload.get("industry"),
                json.dumps(metadata, ensure_ascii=False, sort_keys=True),
                payload.get("updated_at") or now,
                url,
            ),
        )


def init_content_db(*, force: bool = False) -> None:
    db_key = str(store.DB_PATH.resolve())
    with _CONTENT_DB_INIT_LOCK:
        if not force and db_key in _CONTENT_DB_INITIALIZED_PATHS:
            return

    conn = store._connect()
    try:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS harvested_posts (
                url TEXT PRIMARY KEY,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                industry TEXT,
                source_query TEXT,
                title TEXT,
                author_name TEXT,
                author_url TEXT,
                published_at TEXT,
                text TEXT,
                hook TEXT,
                structure TEXT,
                word_count INTEGER NOT NULL DEFAULT 0,
                reaction_count INTEGER NOT NULL DEFAULT 0,
                comment_count INTEGER NOT NULL DEFAULT 0,
                repost_count INTEGER NOT NULL DEFAULT 0,
                owned_by_me INTEGER NOT NULL DEFAULT 0,
                outcome_score REAL NOT NULL DEFAULT 0,
                last_synced_at TEXT,
                embedding_model TEXT,
                embedding_dimensions INTEGER,
                embedding_json TEXT,
                embedded_at TEXT,
                fingerprint_version TEXT,
                fingerprint_dimensions INTEGER,
                fingerprint_json TEXT,
                fingerprinted_at TEXT,
                metadata_json TEXT NOT NULL DEFAULT '{}'
            );

            CREATE TABLE IF NOT EXISTS content_outcome_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                url TEXT NOT NULL,
                synced_at TEXT NOT NULL,
                reaction_count INTEGER NOT NULL DEFAULT 0,
                comment_count INTEGER NOT NULL DEFAULT 0,
                outcome_score REAL NOT NULL DEFAULT 0,
                owned_by_me INTEGER NOT NULL DEFAULT 0,
                metadata_json TEXT NOT NULL DEFAULT '{}',
                FOREIGN KEY (url) REFERENCES harvested_posts(url)
            );

            CREATE INDEX IF NOT EXISTS idx_content_outcome_snapshots_url
                ON content_outcome_snapshots(url, synced_at DESC);

            CREATE TABLE IF NOT EXISTS retrieval_bands (
                url TEXT NOT NULL,
                index_kind TEXT NOT NULL,
                model_key TEXT NOT NULL,
                band_no INTEGER NOT NULL,
                band_hash TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (url, index_kind, model_key, band_no),
                FOREIGN KEY (url) REFERENCES harvested_posts(url)
            );

            CREATE INDEX IF NOT EXISTS idx_retrieval_bands_lookup
                ON retrieval_bands(index_kind, model_key, band_no, band_hash);

            CREATE INDEX IF NOT EXISTS idx_harvested_posts_updated_at
                ON harvested_posts(updated_at DESC);
            CREATE INDEX IF NOT EXISTS idx_harvested_posts_industry
                ON harvested_posts(industry);

            CREATE TABLE IF NOT EXISTS harvested_post_industries (
                url TEXT NOT NULL,
                industry TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (url, industry),
                FOREIGN KEY (url) REFERENCES harvested_posts(url) ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS idx_harvested_post_industries_industry
                ON harvested_post_industries(industry, updated_at DESC);

            CREATE TABLE IF NOT EXISTS content_models (
                model_name TEXT PRIMARY KEY,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                scope TEXT NOT NULL,
                sample_count INTEGER NOT NULL DEFAULT 0,
                feature_names_json TEXT NOT NULL,
                coefficients_json TEXT NOT NULL,
                intercept REAL NOT NULL DEFAULT 0,
                metrics_json TEXT NOT NULL DEFAULT '{}',
                metadata_json TEXT NOT NULL DEFAULT '{}'
            );

            CREATE TABLE IF NOT EXISTS content_candidates (
                candidate_id TEXT PRIMARY KEY,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                prompt TEXT NOT NULL,
                industry TEXT,
                topics_json TEXT NOT NULL DEFAULT '[]',
                goal TEXT NOT NULL,
                text TEXT NOT NULL,
                rank INTEGER NOT NULL DEFAULT 0,
                chosen INTEGER NOT NULL DEFAULT 0,
                status TEXT NOT NULL DEFAULT 'queued',
                generator_source TEXT NOT NULL,
                model_name TEXT,
                score_json TEXT NOT NULL DEFAULT '{}',
                references_json TEXT NOT NULL DEFAULT '{}',
                post_url TEXT,
                metadata_json TEXT NOT NULL DEFAULT '{}'
            );

            CREATE INDEX IF NOT EXISTS idx_content_candidates_status
                ON content_candidates(status, updated_at DESC);

            CREATE INDEX IF NOT EXISTS idx_content_candidates_prompt
                ON content_candidates(created_at DESC, prompt);

            CREATE TABLE IF NOT EXISTS owned_post_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                url TEXT NOT NULL,
                captured_at TEXT NOT NULL,
                reaction_count INTEGER NOT NULL DEFAULT 0,
                comment_count INTEGER NOT NULL DEFAULT 0,
                repost_count INTEGER NOT NULL DEFAULT 0,
                payload_json TEXT NOT NULL DEFAULT '{}',
                FOREIGN KEY (url) REFERENCES harvested_posts(url)
            );

            CREATE TABLE IF NOT EXISTS content_harvest_jobs (
                job_id TEXT PRIMARY KEY,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                status TEXT NOT NULL,
                limit_value INTEGER NOT NULL DEFAULT 0,
                per_query INTEGER NOT NULL DEFAULT 0,
                search_timeout INTEGER,
                fetch_workers INTEGER NOT NULL DEFAULT 1,
                query_workers INTEGER NOT NULL DEFAULT 1,
                stored_count INTEGER NOT NULL DEFAULT 0,
                unique_url_count INTEGER NOT NULL DEFAULT 0,
                industries_json TEXT NOT NULL DEFAULT '[]',
                queries_json TEXT NOT NULL DEFAULT '[]',
                metadata_json TEXT NOT NULL DEFAULT '{}'
            );

            CREATE TABLE IF NOT EXISTS content_harvest_job_queries (
                job_id TEXT NOT NULL,
                query_index INTEGER NOT NULL,
                query TEXT NOT NULL,
                status TEXT NOT NULL,
                stored_count INTEGER NOT NULL DEFAULT 0,
                result_count INTEGER NOT NULL DEFAULT 0,
                last_error TEXT,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (job_id, query_index),
                FOREIGN KEY (job_id) REFERENCES content_harvest_jobs(job_id) ON DELETE CASCADE
            );
            """
        )
        _backfill_post_industries(conn)
        _backfill_query_context(conn)
        _ensure_column(conn, "harvested_posts", "owned_by_me", "INTEGER NOT NULL DEFAULT 0")
        _ensure_column(conn, "harvested_posts", "outcome_score", "REAL NOT NULL DEFAULT 0")
        _ensure_column(conn, "harvested_posts", "last_synced_at", "TEXT")
        _ensure_column(conn, "harvested_posts", "repost_count", "INTEGER NOT NULL DEFAULT 0")
        _ensure_column(conn, "harvested_posts", "embedding_model", "TEXT")
        _ensure_column(conn, "harvested_posts", "embedding_dimensions", "INTEGER")
        _ensure_column(conn, "harvested_posts", "embedding_json", "TEXT")
        _ensure_column(conn, "harvested_posts", "embedded_at", "TEXT")
        _ensure_column(conn, "harvested_posts", "fingerprint_version", "TEXT")
        _ensure_column(conn, "harvested_posts", "fingerprint_dimensions", "INTEGER")
        _ensure_column(conn, "harvested_posts", "fingerprint_json", "TEXT")
        _ensure_column(conn, "harvested_posts", "fingerprinted_at", "TEXT")
        _backfill_fingerprints(conn)
        _backfill_dimensions(conn)
        _backfill_outcome_scores(conn)
        _ensure_retrieval_index(conn)
        conn.commit()
        _CONTENT_DB_INITIALIZED_PATHS.add(db_key)
    finally:
        conn.close()


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
        except (Exception, CliError):
            continue
        if isinstance(parsed, list):
            objects.extend(parsed)
        else:
            objects.append(parsed)
    return objects


def _first_nonempty_line(text: str) -> str:
    for line in text.splitlines():
        cleaned = clean_text(line)
        if cleaned:
            return cleaned
    return ""


def _normalized_post_text(value: Any) -> str:
    if not isinstance(value, str):
        return ""
    lines = [clean_text(line) for line in value.splitlines()]
    lines = [line for line in lines if line]
    return "\n".join(lines)


def _sentence_hook(text: str) -> str:
    first_line = _first_nonempty_line(text)
    if not first_line:
        return ""
    match = re.match(r"(.+?[.!?])(?:\s|$)", first_line)
    if match:
        return match.group(1).strip()
    return first_line


def _classify_structure(text: str) -> str:
    stripped_lines = [line.strip() for line in text.splitlines() if line.strip()]
    if any(re.match(r"^(\d+\.|-|\*)\s+", line) for line in stripped_lines):
        return "list"
    first_line = stripped_lines[0] if stripped_lines else ""
    if "?" in first_line:
        return "question"
    if any(token in text.lower() for token in ["here's how", "how to", "framework", "playbook"]):
        return "how_to"
    return "insight"


def _interaction_counts(post_object: dict[str, Any]) -> tuple[int, int]:
    reaction_count = 0
    comment_count = 0
    for stat in post_object.get("interactionStatistic") or []:
        if not isinstance(stat, dict):
            continue
        stat_type = str(stat.get("interactionType") or "").lower()
        count = int(stat.get("userInteractionCount") or 0)
        if "comment" in stat_type:
            comment_count = count
        elif "like" in stat_type or not reaction_count:
            reaction_count = count
    return reaction_count, comment_count


def extract_post_record(url: str, html: str, source_query: str | None = None) -> dict[str, Any]:
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
    author = post_object.get("author") if isinstance(post_object.get("author"), dict) else {}
    title = clean_text(post_object.get("headline") or "")
    text = _normalized_post_text(post_object.get("articleBody") or post_object.get("text") or "")
    if not text:
        text = clean_text((soup.find("meta", attrs={"property": "og:description"}) or {}).get("content") or "") or title
    if not title:
        title = clean_text(soup.title.get_text(" ", strip=True) if soup.title else "") or clean_text(text)
    reaction_count, comment_count = _interaction_counts(post_object)
    metadata = {
        "post_id": post_object.get("@id") or url,
        "source_query": source_query,
    }
    return {
        "url": url,
        "source_query": source_query,
        "title": title,
        "author_name": clean_text(author.get("name") or "") or None,
        "author_url": author.get("url"),
        "published_at": post_object.get("datePublished"),
        "text": text,
        "hook": _sentence_hook(text or title),
        "structure": _classify_structure(text or title),
        "word_count": len((text or "").split()),
        "reaction_count": reaction_count,
        "comment_count": comment_count,
        "repost_count": 0,
        "metadata": metadata,
    }


def _bounded_ratio(value: float, scale: float = 1.0) -> float:
    if scale <= 0:
        return 0.0
    return round(max(0.0, min(value / scale, 1.0)), 6)


def _engagement_score(reaction_count: int, comment_count: int, word_count: int = 0) -> float:
    base = math.log1p(max(int(reaction_count), 0)) + (1.7 * math.log1p(max(int(comment_count), 0)))
    density_bonus = min(max(word_count, 0) / 180.0, 1.0) * 0.35
    return round(base + density_bonus, 6)


def _extract_topic_tokens(text: str, limit: int = 12) -> list[str]:
    tokens = re.findall(r"[a-z0-9][a-z0-9+#.-]{2,}", (text or "").lower())
    ranked: list[str] = []
    seen: set[str] = set()
    for token in tokens:
        if token in STOPWORDS or token.isdigit():
            continue
        normalized = token.strip(".-")
        if not normalized or normalized in STOPWORDS or normalized in seen:
            continue
        seen.add(normalized)
        ranked.append(normalized)
        if len(ranked) >= limit:
            break
    return ranked


def _hook_type(text: str) -> str:
    hook = _sentence_hook(text)
    lowered = hook.lower()
    if not hook:
        return "unknown"
    if "?" in hook:
        return "question"
    if re.match(r"^\d", hook):
        return "number"
    if re.match(r"^(stop|start|never|always|don't|do not)\b", lowered):
        return "directive"
    if any(token in lowered for token in ("but", "actually", "wrong", "myth", "overrated", "underestimated")):
        return "contrarian"
    if re.match(r"^(i |we |my |our )", lowered):
        return "personal"
    return "statement"


def _chunk_signature_bits(vector: list[float], bits: int = ANN_SIGNATURE_BITS) -> str:
    if not vector:
        return ""
    chunk_size = max(1, math.ceil(len(vector) / bits))
    flags: list[str] = []
    for start in range(0, len(vector), chunk_size):
        chunk = vector[start : start + chunk_size]
        flags.append("1" if sum(float(value) for value in chunk) >= 0 else "0")
        if len(flags) >= bits:
            break
    while len(flags) < bits:
        flags.append("0")
    return "".join(flags[:bits])


def _band_hashes(signature: str, band_bits: int = ANN_BAND_BITS) -> list[tuple[int, str]]:
    if not signature:
        return []
    hashes: list[tuple[int, str]] = []
    for band_no, start in enumerate(range(0, len(signature), band_bits)):
        band = signature[start : start + band_bits]
        if len(band) < band_bits:
            band = band.ljust(band_bits, "0")
        hashes.append((band_no, band))
    return hashes


def _compute_content_fingerprint(text: str, title: str = "") -> list[float]:
    combined = clean_text("\n".join(part for part in [title, text] if part)) or ""
    normalized = combined.lower()
    first_line = _first_nonempty_line(combined) or title or combined
    lines = [line.strip() for line in combined.splitlines() if line.strip()]
    tokens = re.findall(r"\b[\w#@%'-]+\b", normalized)
    unique_tokens = len(set(tokens))
    hashtag_count = len(re.findall(r"#\w+", combined))
    number_count = len(re.findall(r"\b\d+(?:\.\d+)?%?\b", combined))
    question_count = combined.count("?")
    exclaim_count = combined.count("!")

    dimensions = [
        _bounded_ratio((len(first_line) <= 120) + (1 if any(char in first_line for char in "?!") else 0) + (1 if number_count else 0), 3),
        _bounded_ratio(number_count + len(re.findall(r"\b[a-z]{5,}\b", normalized)), 18),
        _bounded_ratio(len(re.findall(r"\b(new|launch|launched|announce|announced|today|just|changed|shift)\b", normalized)), 4),
        _bounded_ratio(len(re.findall(r"\b(founder|ceo|cto|expert|leader|revenue|enterprise|case study|research|study)\b", normalized)), 4),
        _bounded_ratio(len(re.findall(r"\b(i|we|my|our|when|today|yesterday|last)\b", normalized)), 8),
        _bounded_ratio(len(re.findall(r"\b(how|step|steps|framework|playbook|guide|here's how|process)\b", normalized)), 5),
        _bounded_ratio(len(re.findall(r"\b(but|however|wrong|myth|overrated|stop|actually|not)\b", normalized)), 8),
        _bounded_ratio(exclaim_count + len(re.findall(r"\b(wild|huge|massive|crazy|love|hate|fear|pain)\b", normalized)), 6),
        _bounded_ratio(question_count + len(re.findall(r"\b(comment|reply|thoughts|agree|what do you think|dm me)\b", normalized)), 4),
        _bounded_ratio(sum(1 for line in lines if re.match(r"^(\d+\.|[-*•→])\s*", line)), 4),
        _bounded_ratio(number_count + len(re.findall(r"\b(data|study|proof|results|measured|benchmark|case study)\b", normalized)), 6),
        _bounded_ratio(unique_tokens / max(len(tokens), 1), 0.75),
        _bounded_ratio(hashtag_count, 6),
    ]
    return dimensions


def _content_feature_names() -> list[str]:
    names = list(FINGERPRINT_DIMENSIONS)
    names.append("word_count_scaled")
    names.extend(f"structure:{value}" for value in STRUCTURE_FEATURES)
    names.extend(f"hook_type:{value}" for value in HOOK_TYPE_FEATURES)
    return names


def _content_feature_vector(
    *,
    text: str,
    title: str = "",
    word_count: int | None = None,
    fingerprint: list[float] | None = None,
) -> list[float]:
    body = _normalized_post_text(text)
    computed = fingerprint or _compute_content_fingerprint(body, title=title)
    structure = _classify_structure(body or title)
    hook_type = _hook_type(_sentence_hook(body or title) or title)
    features = list(computed)
    features.append(_bounded_ratio(float(word_count if word_count is not None else len((body or title).split())), 320))
    features.extend(1.0 if structure == value else 0.0 for value in STRUCTURE_FEATURES)
    features.extend(1.0 if hook_type == value else 0.0 for value in HOOK_TYPE_FEATURES)
    return features


def _dot(left: list[float], right: list[float]) -> float:
    return sum(float(a) * float(b) for a, b in zip(left, right))


def _fit_linear_model(
    rows: list[list[float]],
    targets: list[float],
    *,
    learning_rate: float = 0.08,
    epochs: int = 600,
    ridge: float = 0.02,
) -> tuple[list[float], float]:
    if not rows:
        return [], 0.0
    feature_count = len(rows[0])
    coefficients = [0.0] * feature_count
    intercept = sum(targets) / len(targets)
    sample_count = float(len(rows))
    for _ in range(max(50, epochs)):
        gradient = [0.0] * feature_count
        intercept_gradient = 0.0
        for row, target in zip(rows, targets):
            prediction = intercept + _dot(coefficients, row)
            error = prediction - target
            intercept_gradient += error
            for index, value in enumerate(row):
                gradient[index] += error * float(value)
        intercept -= learning_rate * (intercept_gradient / sample_count)
        for index in range(feature_count):
            regularized = (gradient[index] / sample_count) + (ridge * coefficients[index])
            coefficients[index] -= learning_rate * regularized
    return coefficients, intercept


def _score_linear_model(rows: list[list[float]], targets: list[float], coefficients: list[float], intercept: float) -> dict[str, float]:
    if not rows:
        return {"mae": 0.0, "rmse": 0.0, "r2": 0.0}
    predictions = [intercept + _dot(coefficients, row) for row in rows]
    absolute_errors = [abs(prediction - target) for prediction, target in zip(predictions, targets)]
    squared_errors = [(prediction - target) ** 2 for prediction, target in zip(predictions, targets)]
    mean_target = sum(targets) / len(targets)
    total_variance = sum((target - mean_target) ** 2 for target in targets)
    r2 = 0.0 if total_variance <= 0 else 1.0 - (sum(squared_errors) / total_variance)
    return {
        "mae": round(sum(absolute_errors) / len(absolute_errors), 4),
        "rmse": round(math.sqrt(sum(squared_errors) / len(squared_errors)), 4),
        "r2": round(r2, 4),
    }


def _model_row_to_dict(row: Any) -> dict[str, Any]:
    data = dict(row)
    data["feature_names"] = json.loads(data.pop("feature_names_json", "[]") or "[]")
    data["coefficients"] = json.loads(data.pop("coefficients_json", "[]") or "[]")
    data["metrics"] = json.loads(data.pop("metrics_json", "{}") or "{}")
    data["metadata"] = json.loads(data.pop("metadata_json", "{}") or "{}")
    return data


def _upsert_post_conn(conn: Any, record: dict[str, Any], *, now: str | None = None) -> dict[str, Any]:
    timestamp = now or store._now_iso()
    text = str(record.get("text") or "")
    title = str(record.get("title") or "")
    fingerprint = _compute_content_fingerprint(text, title=title)
    industries = _normalize_industries(record.get("industries"), record.get("industry"))
    primary_industry = industries[0] if industries else None
    conn.execute(
        """
        INSERT INTO harvested_posts (
            url, created_at, updated_at, industry, source_query, title, author_name, author_url,
            published_at, text, hook, structure, word_count, reaction_count, comment_count, repost_count,
            owned_by_me, outcome_score, last_synced_at,
            fingerprint_version, fingerprint_dimensions, fingerprint_json, fingerprinted_at, metadata_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(url) DO UPDATE SET
            updated_at = excluded.updated_at,
            industry = excluded.industry,
            source_query = excluded.source_query,
            title = excluded.title,
            author_name = excluded.author_name,
            author_url = excluded.author_url,
            published_at = excluded.published_at,
            text = excluded.text,
            hook = excluded.hook,
            structure = excluded.structure,
            word_count = excluded.word_count,
            reaction_count = CASE
                WHEN excluded.reaction_count > harvested_posts.reaction_count THEN excluded.reaction_count
                ELSE harvested_posts.reaction_count
            END,
            comment_count = CASE
                WHEN excluded.comment_count > harvested_posts.comment_count THEN excluded.comment_count
                ELSE harvested_posts.comment_count
            END,
            repost_count = CASE
                WHEN excluded.repost_count > harvested_posts.repost_count THEN excluded.repost_count
                ELSE harvested_posts.repost_count
            END,
            owned_by_me = CASE
                WHEN excluded.owned_by_me = 1 THEN 1
                ELSE harvested_posts.owned_by_me
            END,
            outcome_score = CASE
                WHEN excluded.outcome_score > harvested_posts.outcome_score THEN excluded.outcome_score
                ELSE harvested_posts.outcome_score
            END,
            last_synced_at = COALESCE(excluded.last_synced_at, harvested_posts.last_synced_at),
            fingerprint_version = excluded.fingerprint_version,
            fingerprint_dimensions = excluded.fingerprint_dimensions,
            fingerprint_json = excluded.fingerprint_json,
            fingerprinted_at = excluded.fingerprinted_at,
            embedding_model = NULL,
            embedding_dimensions = NULL,
            embedding_json = NULL,
            embedded_at = NULL,
            metadata_json = excluded.metadata_json
        """,
        (
            record["url"],
            timestamp,
            timestamp,
            primary_industry,
            record.get("source_query"),
            record.get("title"),
            record.get("author_name"),
            record.get("author_url"),
            record.get("published_at"),
            record.get("text"),
            record.get("hook"),
            record.get("structure"),
            int(record.get("word_count") or 0),
            int(record.get("reaction_count") or 0),
            int(record.get("comment_count") or 0),
            int(record.get("repost_count") or 0),
            1 if record.get("owned_by_me") else 0,
            _engagement_score(
                int(record.get("reaction_count") or 0),
                int(record.get("comment_count") or 0),
                int(record.get("word_count") or 0),
            ),
            record.get("last_synced_at"),
            FINGERPRINT_VERSION,
            len(fingerprint),
            json.dumps(fingerprint),
            timestamp,
            json.dumps(record.get("metadata") or {}, ensure_ascii=False, sort_keys=True),
        ),
    )
    _sync_post_industries(conn, str(record["url"]), industries, now=timestamp)
    row = conn.execute("SELECT * FROM harvested_posts WHERE url = ?", (record["url"],)).fetchone()
    assert row is not None
    stored = _attach_industries(conn, [_row_to_dict(row)])[0]
    _index_record_for_row(conn, stored)
    return stored


def _is_sqlite_locked_error(exc: sqlite3.OperationalError) -> bool:
    return "database is locked" in str(exc).lower()


def _sqlite_lock_retry_sleep(attempt: int) -> None:
    delay = min(3.0, _SQLITE_LOCK_RETRY_BASE_SECONDS * (2 ** attempt))
    time.sleep(delay + random.uniform(0.0, 0.1))


def upsert_post(record: dict[str, Any]) -> dict[str, Any]:
    init_content_db()
    for attempt in range(_SQLITE_LOCK_RETRY_ATTEMPTS + 1):
        conn = store._connect()
        try:
            stored = _upsert_post_conn(conn, record)
            conn.commit()
            return stored
        except sqlite3.OperationalError as exc:
            try:
                conn.rollback()
            except Exception:
                pass
            if not _is_sqlite_locked_error(exc) or attempt >= _SQLITE_LOCK_RETRY_ATTEMPTS:
                raise
        finally:
            conn.close()
        _sqlite_lock_retry_sleep(attempt)
    raise AssertionError("unreachable")


def _row_to_dict(row: Any) -> dict[str, Any]:
    result = dict(row)
    result["metadata"] = json.loads(result.pop("metadata_json", None) or "{}")
    embedding_json = result.pop("embedding_json", None)
    result["embedding"] = json.loads(embedding_json) if embedding_json else None
    fingerprint_json = result.pop("fingerprint_json", None)
    result["fingerprint"] = json.loads(fingerprint_json) if fingerprint_json else None
    return result


def _candidate_row_to_dict(row: Any) -> dict[str, Any]:
    result = dict(row)
    result["topics"] = json.loads(result.pop("topics_json", None) or "[]")
    result["score"] = json.loads(result.pop("score_json", None) or "{}")
    result["references"] = json.loads(result.pop("references_json", None) or "{}")
    result["metadata"] = json.loads(result.pop("metadata_json", None) or "{}")
    result["chosen"] = bool(result.get("chosen"))
    return result


def _embedding_dimension_for_model(model: str | None) -> int | None:
    if not model:
        return None
    exact = KNOWN_EMBEDDING_DIMS.get(model)
    if exact is not None:
        return exact
    if model.startswith("local-hash"):
        return 256
    return None


def summarize_post_dimensions(post: dict[str, Any]) -> tuple[int | None, int | None]:
    embedding = post.get("embedding")
    fingerprint = post.get("fingerprint")
    embedding_dim = len(embedding) if isinstance(embedding, list) else post.get("embedding_dimensions")
    if embedding_dim is None:
        embedding_dim = _embedding_dimension_for_model(str(post.get("embedding_model") or ""))
    fingerprint_dim = len(fingerprint) if isinstance(fingerprint, list) else post.get("fingerprint_dimensions")
    if fingerprint_dim is None and post.get("fingerprint_version"):
        fingerprint_dim = len(FINGERPRINT_DIMENSIONS)
    return embedding_dim, fingerprint_dim


def list_posts(
    limit: int = 20,
    industry: str | None = None,
    author: str | None = None,
    include_vectors: bool = True,
) -> list[dict[str, Any]]:
    init_content_db()
    limit = max(1, int(limit))
    conn = store._connect()
    try:
        columns = "*" if include_vectors else (
            "url, created_at, updated_at, industry, source_query, title, author_name, author_url, "
            "published_at, text, hook, structure, word_count, reaction_count, comment_count, "
            "owned_by_me, outcome_score, last_synced_at, "
            "embedding_model, embedding_dimensions, embedded_at, "
            "fingerprint_version, fingerprint_dimensions, fingerprinted_at, metadata_json"
        )
        where: list[str] = []
        params: list[Any] = []
        if industry:
            where.append(
                "EXISTS (SELECT 1 FROM harvested_post_industries hpi WHERE hpi.url = harvested_posts.url AND hpi.industry = ?)"
            )
            params.append(industry)
        if author:
            where.append("LOWER(COALESCE(author_name, '')) LIKE ?")
            params.append(f"%{author.lower()}%")
        where_sql = f" WHERE {' AND '.join(where)}" if where else ""
        rows = conn.execute(
            f"SELECT {columns} FROM harvested_posts{where_sql} ORDER BY updated_at DESC LIMIT ?",
            (*params, limit),
        ).fetchall()
        return _attach_industries(conn, [_row_to_dict(row) for row in rows])
    finally:
        conn.close()


def _load_posts_by_urls(urls: list[str]) -> list[dict[str, Any]]:
    if not urls:
        return []
    conn = store._connect()
    try:
        placeholders = ",".join("?" for _ in urls)
        rows = conn.execute(
            f"SELECT * FROM harvested_posts WHERE url IN ({placeholders})",
            tuple(urls),
        ).fetchall()
        return _attach_industries(conn, [_row_to_dict(row) for row in rows])
    finally:
        conn.close()


def _ann_candidates(
    *,
    query_embedding: list[float] | None,
    model: str | None,
    query_fingerprint: list[float] | None,
    shortlist_size: int,
) -> list[str]:
    conn = store._connect()
    try:
        _ensure_retrieval_index(conn)
        scores: dict[str, float] = defaultdict(float)

        def accumulate(index_kind: str, model_key: str, vector: list[float] | None, weight: float) -> None:
            if not vector:
                return
            for band_no, band_hash in _band_hashes(_chunk_signature_bits(vector)):
                rows = conn.execute(
                    """
                    SELECT url FROM retrieval_bands
                    WHERE index_kind = ? AND model_key = ? AND band_no = ? AND band_hash = ?
                    """,
                    (index_kind, model_key, band_no, band_hash),
                ).fetchall()
                for row in rows:
                    scores[row["url"]] += weight

        if query_embedding and model:
            accumulate("semantic", model, query_embedding, 1.5)
        if query_fingerprint:
            accumulate("fingerprint", FINGERPRINT_VERSION, query_fingerprint, 1.0)

        ranked = sorted(scores.items(), key=lambda item: item[1], reverse=True)
        return [url for url, _score in ranked[:shortlist_size]]
    finally:
        conn.close()


def _sanitize_index_key(value: str) -> str:
    return re.sub(r"[^a-z0-9._-]+", "-", value.lower()).strip("-") or "default"


def _index_dir(kind: str, model_key: str) -> Path:
    return store.ARTIFACTS_DIR / "retrieval" / "content" / _sanitize_index_key(kind) / _sanitize_index_key(model_key)


def _index_items(kind: str, model_key: str) -> list[dict[str, Any]]:
    posts = list_posts(limit=100000)
    items: list[dict[str, Any]] = []
    for post in posts:
        vector: list[float] | None = None
        if kind == "semantic":
            if str(post.get("embedding_model") or "") != model_key:
                continue
            vector = post.get("embedding")
        elif kind == "fingerprint":
            if str(post.get("fingerprint_version") or FINGERPRINT_VERSION) != model_key:
                continue
            vector = post.get("fingerprint")
        if not isinstance(vector, list) or not vector:
            continue
        items.append(
            {
                "id": str(post["url"]),
                "vector": vector,
                "payload": {
                    "url": str(post["url"]),
                    "author_name": post.get("author_name"),
                    "author_url": post.get("author_url"),
                    "industry": post.get("industry"),
                    "industries": post.get("industries") or [],
                    "updated_at": post.get("updated_at"),
                    "owned_by_me": bool(post.get("owned_by_me")),
                },
            }
        )
    return items


def _rebuild_single_index(kind: str, model_key: str) -> dict[str, Any]:
    items = _index_items(kind, model_key)
    index = retrieval_index.build_index(items, index_name=f"{kind}:{model_key}")
    index_dir = _index_dir(kind, model_key)
    retrieval_index.save_index(index, index_dir)
    return {
        "kind": kind,
        "model_key": model_key,
        "engine": index["engine"],
        "count": index["count"],
        "dimension": index["dimension"],
        "path": str(index_dir),
        "version": RETRIEVAL_INDEX_VERSION,
    }


def rebuild_retrieval_index(kind: str = "all", model: str | None = None) -> dict[str, Any]:
    init_content_db()
    posts = list_posts(limit=100000)
    semantic_models = sorted({str(post.get("embedding_model") or "") for post in posts if post.get("embedding")})
    targets: list[tuple[str, str]] = []
    if kind in {"all", "semantic"}:
        if model:
            targets.append(("semantic", model))
        else:
            targets.extend(("semantic", model_name) for model_name in semantic_models)
    if kind in {"all", "fingerprint"}:
        targets.append(("fingerprint", FINGERPRINT_VERSION))
    rebuilt: list[dict[str, Any]] = []
    for index_kind, model_key in targets:
        rebuilt.append(_rebuild_single_index(index_kind, model_key))
    return {
        "rebuilt": rebuilt,
        "count": len(rebuilt),
    }


def export_retrieval_index(kind: str = "all", model: str | None = None, output_dir: str | Path | None = None) -> dict[str, Any]:
    init_content_db()
    base_dir = Path(output_dir) if output_dir else (store.ARTIFACTS_DIR / "retrieval-export" / "content")
    base_dir.mkdir(parents=True, exist_ok=True)
    posts = list_posts(limit=100000)
    semantic_models = sorted({str(post.get("embedding_model") or "") for post in posts if post.get("embedding")})
    targets: list[tuple[str, str]] = []
    if kind in {"all", "semantic"}:
        if model:
            targets.append(("semantic", model))
        else:
            targets.extend(("semantic", model_name) for model_name in semantic_models)
    if kind in {"all", "fingerprint"}:
        targets.append(("fingerprint", FINGERPRINT_VERSION))
    exported: list[dict[str, Any]] = []
    for index_kind, model_key in targets:
        items = _index_items(index_kind, model_key)
        path = base_dir / f"{_sanitize_index_key(index_kind)}-{_sanitize_index_key(model_key)}.jsonl"
        retrieval_index.export_jsonl(items, path)
        exported.append(
            {
                "kind": index_kind,
                "model_key": model_key,
                "count": len(items),
                "path": str(path),
            }
        )
    return {"output_dir": str(base_dir), "exports": exported, "count": len(exported)}


def list_harvest_jobs(limit: int = 20) -> list[dict[str, Any]]:
    init_content_db()
    conn = store._connect()
    try:
        rows = conn.execute(
            """
            SELECT * FROM content_harvest_jobs
            ORDER BY updated_at DESC
            LIMIT ?
            """,
            (max(1, int(limit)),),
        ).fetchall()
        jobs = [_harvest_job_to_dict(row) for row in rows]
        for job in jobs:
            job["queries_total"] = len(job.get("queries") or [])
        return jobs
    finally:
        conn.close()


def _load_vector_index(kind: str, model_key: str) -> dict[str, Any] | None:
    path = _index_dir(kind, model_key)
    if not (path / "manifest.json").exists():
        return None
    try:
        return retrieval_index.load_index(path)
    except Exception:
        return None


def _vector_index_candidates(kind: str, model_key: str, vector: list[float] | None, shortlist_size: int) -> list[str]:
    if not vector or not model_key:
        return []
    index = _load_vector_index(kind, model_key)
    if index is None:
        summary = _rebuild_single_index(kind, model_key)
        if int(summary.get("count") or 0) <= 0:
            return []
        index = _load_vector_index(kind, model_key)
    if index is None:
        return []
    results = retrieval_index.query(index, vector, limit=shortlist_size)
    return [str(item["id"]) for item in results]


def content_stats() -> dict[str, Any]:
    posts = list_posts(limit=100000)
    industries = Counter()
    for post in posts:
        values = post.get("industries") or ([post.get("industry")] if post.get("industry") else [])
        if not values:
            industries["unknown"] += 1
            continue
        for value in values:
            industries[str(value)] += 1
    structures = Counter(post.get("structure") or "unknown" for post in posts)
    authors = Counter(post.get("author_name") or "unknown" for post in posts)
    fingerprinted = [post for post in posts if post.get("fingerprint")]
    fingerprint_averages = {}
    if fingerprinted:
        for index, name in enumerate(FINGERPRINT_DIMENSIONS):
            fingerprint_averages[name] = round(
                sum(float(post["fingerprint"][index]) for post in fingerprinted) / len(fingerprinted),
                4,
            )
    return {
        "post_count": len(posts),
        "industries": dict(industries),
        "structures": dict(structures),
        "top_authors": dict(authors.most_common(10)),
        "embedded_count": sum(1 for post in posts if post.get("embedding")),
        "fingerprinted_count": len(fingerprinted),
        "owned_count": sum(1 for post in posts if post.get("owned_by_me")),
        "outcome_synced_count": sum(1 for post in posts if post.get("last_synced_at")),
        "fingerprint_dimensions": list(FINGERPRINT_DIMENSIONS),
        "fingerprint_averages": fingerprint_averages,
        "average_word_count": round(sum(post.get("word_count") or 0 for post in posts) / len(posts), 2) if posts else 0.0,
        "average_outcome_score": round(sum(float(post.get("outcome_score") or 0.0) for post in posts) / len(posts), 4) if posts else 0.0,
    }


def get_trained_model(name: str = OUTCOME_MODEL_NAME) -> dict[str, Any] | None:
    init_content_db()
    conn = store._connect()
    try:
        row = conn.execute("SELECT * FROM content_models WHERE model_name = ?", (name,)).fetchone()
        return _model_row_to_dict(row) if row else None
    finally:
        conn.close()


def train_outcome_model(
    *,
    name: str = OUTCOME_MODEL_NAME,
    scope: str = "auto",
    min_samples: int = 5,
    industry: str | None = None,
    topics: list[str] | None = None,
    learning_rate: float = 0.08,
    epochs: int = 600,
    ridge: float = 0.02,
) -> dict[str, Any]:
    init_content_db()
    normalized_topics = _normalize_topics(topics)
    posts = list_posts(limit=100000, industry=industry)
    posts = _filter_posts_by_relevance(posts, industry=industry, topics=normalized_topics)
    owned_posts = [
        post for post in posts
        if post.get("owned_by_me") and (post.get("last_synced_at") or float(post.get("outcome_score") or 0.0) > 0)
    ]
    all_posts = [post for post in posts if float(post.get("outcome_score") or 0.0) > 0]
    if scope not in {"auto", "owned", "all"}:
        fail("Model scope must be one of: auto, owned, all", code=ExitCode.VALIDATION)
    if scope == "owned":
        training_posts = owned_posts
        resolved_scope = "owned"
    elif scope == "all":
        training_posts = all_posts
        resolved_scope = "all"
    else:
        training_posts = owned_posts if len(owned_posts) >= min_samples else all_posts
        resolved_scope = "owned" if training_posts is owned_posts else "all"
    if len(training_posts) < min_samples:
        return {
            "trained": False,
            "model_name": name,
            "scope": resolved_scope,
            "sample_count": len(training_posts),
            "min_samples": min_samples,
            "industry": industry,
            "topics": normalized_topics,
            "reason": "Not enough outcome-synced posts to train a model.",
        }

    rows: list[list[float]] = []
    targets: list[float] = []
    for post in training_posts:
        text = str(post.get("text") or "")
        title = str(post.get("title") or "")
        fingerprint = post.get("fingerprint")
        if not isinstance(fingerprint, list) or not fingerprint:
            fingerprint = _compute_content_fingerprint(text, title=title)
        rows.append(
            _content_feature_vector(
                text=text,
                title=title,
                word_count=int(post.get("word_count") or 0),
                fingerprint=[float(value) for value in fingerprint],
            )
        )
        targets.append(float(post.get("outcome_score") or 0.0))

    coefficients, intercept = _fit_linear_model(
        rows,
        targets,
        learning_rate=learning_rate,
        epochs=epochs,
        ridge=ridge,
    )
    metrics = _score_linear_model(rows, targets, coefficients, intercept)
    now = store._now_iso()
    conn = store._connect()
    try:
        conn.execute(
            """
            INSERT INTO content_models (
                model_name, created_at, updated_at, scope, sample_count,
                feature_names_json, coefficients_json, intercept, metrics_json, metadata_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(model_name) DO UPDATE SET
                updated_at = excluded.updated_at,
                scope = excluded.scope,
                sample_count = excluded.sample_count,
                feature_names_json = excluded.feature_names_json,
                coefficients_json = excluded.coefficients_json,
                intercept = excluded.intercept,
                metrics_json = excluded.metrics_json,
                metadata_json = excluded.metadata_json
            """,
            (
                name,
                now,
                now,
                resolved_scope,
                len(training_posts),
                json.dumps(_content_feature_names(), ensure_ascii=False),
                json.dumps(coefficients, ensure_ascii=False),
                intercept,
                json.dumps(metrics, ensure_ascii=False, sort_keys=True),
                json.dumps(
                    {
                        "learning_rate": learning_rate,
                        "epochs": epochs,
                        "ridge": ridge,
                        "target_mean": round(sum(targets) / len(targets), 4),
                        "industry": clean_text(industry) or None,
                        "topics": normalized_topics,
                        "relevance_min": 0.35 if (industry or normalized_topics) else 0.0,
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                ),
            ),
        )
        conn.commit()
    finally:
        conn.close()
    model = get_trained_model(name)
    return {
        "trained": True,
        "model_name": name,
        "scope": resolved_scope,
        "industry": clean_text(industry) or None,
        "topics": normalized_topics,
        "sample_count": len(training_posts),
        "feature_count": len(_content_feature_names()),
        "metrics": metrics,
        "model": model,
    }


def ranked_patterns(
    *,
    limit: int = 10,
    industry: str | None = None,
    topics: list[str] | None = None,
    author: str | None = None,
    owned_only: bool = False,
) -> dict[str, Any]:
    posts = list_posts(limit=100000, industry=industry, author=author)
    if owned_only:
        posts = [post for post in posts if post.get("owned_by_me")]
    posts = _filter_posts_by_relevance(posts, industry=industry, topics=topics)
    if not posts:
        return {
            "post_count": 0,
            "top_posts": [],
            "structures": [],
            "hook_types": [],
            "topics": [],
            "hook_examples": [],
        }

    def post_weight(post: dict[str, Any]) -> float:
        base = float(post.get("outcome_score") or 0.0)
        if post.get("owned_by_me"):
            base *= 1.25
        return base * max(0.35, float(post.get("relevance_score") or 1.0))

    structure_scores: dict[str, list[float]] = defaultdict(list)
    hook_type_scores: dict[str, list[float]] = defaultdict(list)
    topic_scores: dict[str, list[float]] = defaultdict(list)
    hook_examples: list[dict[str, Any]] = []

    for post in posts:
        weight = post_weight(post)
        structure_scores[str(post.get("structure") or "unknown")].append(weight)
        hook_type_scores[_hook_type(str(post.get("hook") or post.get("title") or ""))].append(weight)
        for topic in _extract_topic_tokens(" ".join(str(post.get(key) or "") for key in ("title", "hook", "text")), limit=8):
            topic_scores[topic].append(weight)
        hook_examples.append(
            {
                "url": post.get("url"),
                "hook": post.get("hook") or post.get("title"),
                "author_name": post.get("author_name"),
                "outcome_score": round(weight, 4),
                "reaction_count": post.get("reaction_count"),
                "comment_count": post.get("comment_count"),
            }
        )

    def summarize(score_map: dict[str, list[float]], *, min_count: int = 1) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for key, values in score_map.items():
            if len(values) < min_count:
                continue
            rows.append(
                {
                    "name": key,
                    "count": len(values),
                    "average_outcome_score": round(sum(values) / len(values), 4),
                    "total_outcome_score": round(sum(values), 4),
                }
            )
        rows.sort(key=lambda item: (item["average_outcome_score"], item["count"]), reverse=True)
        return rows[:limit]

    hook_examples.sort(key=lambda item: item["outcome_score"], reverse=True)
    top_posts = sorted(
        (
            {
                "url": post.get("url"),
                "title": post.get("title"),
                "hook": post.get("hook"),
                "author_name": post.get("author_name"),
                "structure": post.get("structure"),
                "outcome_score": round(post_weight(post), 4),
                "reaction_count": post.get("reaction_count"),
                "comment_count": post.get("comment_count"),
                "owned_by_me": bool(post.get("owned_by_me")),
                "relevance_score": round(float(post.get("relevance_score") or 1.0), 4),
            }
            for post in posts
        ),
        key=lambda item: item["outcome_score"],
        reverse=True,
    )[:limit]
    return {
        "post_count": len(posts),
        "top_posts": top_posts,
        "structures": summarize(structure_scores),
        "hook_types": summarize(hook_type_scores),
        "topics": summarize(topic_scores, min_count=2) or summarize(topic_scores),
        "hook_examples": hook_examples[:limit],
    }


def _friendly_feature_name(name: str) -> str:
    return name.replace("_", " ")


def _learned_signal_rankings(model: dict[str, Any] | None, limit: int = 8) -> dict[str, list[dict[str, Any]]]:
    if not model or not model.get("coefficients"):
        return {"top_positive": [], "top_negative": []}
    pairs = [
        {"feature": str(feature), "label": _friendly_feature_name(str(feature)), "weight": round(float(weight), 6)}
        for feature, weight in zip(model.get("feature_names") or [], model.get("coefficients") or [])
    ]
    return {
        "top_positive": sorted(pairs, key=lambda item: item["weight"], reverse=True)[:limit],
        "top_negative": sorted(pairs, key=lambda item: item["weight"])[:limit],
    }


def _hook_template_from_example(example: dict[str, Any], topic_hint: str | None = None) -> dict[str, Any]:
    hook = str(example.get("hook") or "").strip()
    hook_type = _hook_type(hook)
    topic = topic_hint or "the workflow"
    if hook_type == "personal":
        template = "I learned something the hard way about {topic}. Here is what changed."
        rationale = "Personal milestone hooks overperform in the current corpus when they lead into a concrete lesson."
    elif hook_type == "number" or re.search(r"\b\d[\d,]*(?:\.\d+)?\b", hook):
        template = "{number}-style result: what changed after fixing {topic}."
        rationale = "Specific numbers plus a clear before/after frame consistently show up in high-outcome posts."
    elif hook_type == "contrarian" or any(token in hook.lower() for token in ("won't", "wrong", "kill", "overrated", "actually")):
        template = "{common belief} is wrong. {topic} changes the leverage point."
        rationale = "Contrarian hooks work best when they challenge an obvious belief and then explain the shift."
    elif hook.lower().startswith("today"):
        template = "Today, we changed one thing in {topic} that removes the painful handoff."
        rationale = "Time-anchored launch or reveal hooks perform when they imply immediate stakes."
    else:
        template = "{specific claim about topic}. Here is what happened when we tested it."
        rationale = "High-performing statement hooks are specific claims followed quickly by evidence."
    return {
        "template": template.replace("{topic}", topic),
        "hook_type": hook_type,
        "based_on": {
            "url": example.get("url"),
            "hook": hook,
            "author_name": example.get("author_name"),
            "outcome_score": example.get("outcome_score"),
        },
        "rationale": rationale,
    }


def build_playbook(
    *,
    industry: str | None = None,
    topics: list[str] | None = None,
    author: str | None = None,
    owned_only: bool = False,
    limit: int = 8,
    model_name: str = OUTCOME_MODEL_NAME,
) -> dict[str, Any]:
    normalized_topics = _normalize_topics(topics)
    patterns = ranked_patterns(limit=limit, industry=industry, topics=normalized_topics, author=author, owned_only=owned_only)
    trained_model = get_trained_model(name=model_name)
    if not _model_matches_slice(trained_model, industry=industry, topics=normalized_topics):
        trained_model = None
    signals = _learned_signal_rankings(trained_model, limit=limit)
    learned_topics = [str(item["name"]) for item in patterns.get("topics") or [] if item.get("name")]
    topic_hint = learned_topics[0] if learned_topics else "your workflow"
    hook_templates: list[dict[str, Any]] = []
    seen_templates: set[str] = set()
    for example in patterns.get("hook_examples") or patterns.get("top_posts") or []:
        template = _hook_template_from_example(example, topic_hint=topic_hint)
        key = template["template"]
        if key in seen_templates:
            continue
        seen_templates.add(key)
        hook_templates.append(template)
        if len(hook_templates) >= limit:
            break
    structure_blueprints: list[dict[str, Any]] = []
    for item in patterns.get("structures") or []:
        name = str(item.get("name") or "")
        if name == "list":
            blueprint = "Hook with a sharp claim, 3 numbered proof-backed steps, then one direct CTA."
        elif name == "question":
            blueprint = "Use only when the question is backed by a concrete stake or surprising answer."
        elif name == "how_to":
            blueprint = "Lead with the result first, then compress the process into steps instead of generic teaching."
        else:
            blueprint = "Lead with a claim, add evidence fast, and end with what the reader should do or believe next."
        structure_blueprints.append(
            {
                "structure": name,
                "average_outcome_score": item.get("average_outcome_score"),
                "blueprint": blueprint,
            }
        )
    rewrite_rules: list[str] = []
    negative_features = {item["feature"] for item in signals.get("top_negative") or []}
    positive_features = {item["feature"] for item in signals.get("top_positive") or []}
    if "hashtag_load" in negative_features:
        rewrite_rules.append("Cut hashtags to zero or one. The corpus penalizes hashtag-heavy posts.")
    if "structure:question" in negative_features:
        rewrite_rules.append("Do not open with a vague question. Replace it with a directional claim or tension-filled observation.")
    if "word_count_scaled" in positive_features:
        rewrite_rules.append("Do not underwrite the body. Add enough detail to prove the claim instead of stopping at the hook.")
    if "proof" in positive_features:
        rewrite_rules.append("Add one metric, result, case, or concrete observation before the CTA.")
    if "narrative" in positive_features:
        rewrite_rules.append("Introduce a narrative turn: what changed, what broke, or what you learned.")
    if "engagement_cta" in positive_features:
        rewrite_rules.append("End with one explicit response prompt tied to the claim, not a generic 'thoughts?'.")
    if "contrarian" in positive_features:
        rewrite_rules.append("When the claim is strong enough, frame it against a common belief rather than explaining softly.")
    return {
        "post_count": patterns.get("post_count", 0),
        "industry": industry,
        "topics": normalized_topics,
        "learned_signals": signals,
        "winning_structures": structure_blueprints,
        "winning_topics": patterns.get("topics") or [],
        "hook_templates": hook_templates,
        "rewrite_rules": rewrite_rules,
        "top_examples": patterns.get("top_posts") or [],
    }


def score_draft(
    *,
    text: str,
    industry: str | None = None,
    topics: list[str] | None = None,
    model: str | None = None,
    embed_fn: Callable[[list[str], str], list[list[float]]] | None = None,
) -> dict[str, Any]:
    normalized = _normalized_post_text(text)
    if not normalized:
        fail("Draft text is required", code=ExitCode.VALIDATION)
    normalized_topics = _normalize_topics(topics)
    patterns = ranked_patterns(limit=8, industry=industry, topics=normalized_topics)
    similar = retrieve_posts(
        query_text=normalized,
        limit=5,
        method="hybrid",
        model=model,
        industry=industry,
        embed_fn=embed_fn,
    )
    similar = _filter_posts_by_relevance(similar, industry=industry, topics=normalized_topics)
    fingerprint = _compute_content_fingerprint(normalized, title=_sentence_hook(normalized))
    hook = _sentence_hook(normalized)
    hook_type = _hook_type(hook)
    top_structure = (patterns.get("structures") or [{}])[0].get("name")
    topic_hits = [topic for topic in _extract_topic_tokens(normalized) if topic in {item["name"] for item in patterns.get("topics") or []}]
    similar_avg = round(sum(float(item.get("outcome_score") or 0.0) for item in similar) / len(similar), 4) if similar else 0.0
    pattern_bonus = 0.0
    if top_structure and _classify_structure(normalized) == top_structure:
        pattern_bonus += 0.35
    if hook_type in {item["name"] for item in patterns.get("hook_types") or []}:
        pattern_bonus += 0.25
    pattern_bonus += min(len(topic_hits), 3) * 0.1
    heuristic_prediction = round((similar_avg * 0.65) + pattern_bonus + (sum(fingerprint) / max(len(fingerprint), 1)), 4)
    trained_model = get_trained_model()
    model_prediction = None
    prediction_source = "heuristic"
    predicted = heuristic_prediction
    if trained_model and trained_model.get("coefficients"):
        if not _model_matches_slice(trained_model, industry=industry, topics=normalized_topics):
            trained_model = None
    if trained_model and trained_model.get("coefficients"):
        draft_features = _content_feature_vector(
            text=normalized,
            title=hook,
            word_count=len(normalized.split()),
            fingerprint=fingerprint,
        )
        model_prediction = round(
            float(trained_model.get("intercept") or 0.0) + _dot([float(value) for value in trained_model.get("coefficients") or []], draft_features),
            4,
        )
        blend_weight = 0.6 if int(trained_model.get("sample_count") or 0) >= 5 else 0.4
        predicted = round((model_prediction * blend_weight) + (heuristic_prediction * (1.0 - blend_weight)), 4)
        prediction_source = "trained_blend"

    recommendations: list[str] = []
    if len(hook) > 120:
        recommendations.append("Shorten the opening hook to one sharp sentence under 120 characters.")
    if _classify_structure(normalized) == "insight" and top_structure == "list":
        recommendations.append("Restructure the body into numbered steps to match higher-performing instructional posts.")
    if fingerprint[FINGERPRINT_DIMENSIONS.index("engagement_cta")] < 0.2:
        recommendations.append("Add a specific CTA or question so the post asks readers to respond.")
    if fingerprint[FINGERPRINT_DIMENSIONS.index("proof")] < 0.2:
        recommendations.append("Anchor the post with proof: a number, case study, or concrete result.")
    if not topic_hits and patterns.get("topics"):
        recommendations.append(f"Work in one of the winning topics already in your corpus, such as `{patterns['topics'][0]['name']}`.")

    return {
        "hook": hook,
        "hook_type": hook_type,
        "structure": _classify_structure(normalized),
        "word_count": len(normalized.split()),
        "predicted_outcome_score": predicted,
        "heuristic_prediction": heuristic_prediction,
        "model_prediction": model_prediction,
        "prediction_source": prediction_source,
        "similar_posts": similar,
        "pattern_matches": {
            "top_structure": top_structure,
            "matched_topics": topic_hits,
            "top_hook_types": [item["name"] for item in patterns.get("hook_types") or []][:3],
        },
        "fingerprint": dict(zip(FINGERPRINT_DIMENSIONS, fingerprint)),
        "recommendations": recommendations,
    }


def rewrite_draft(
    *,
    text: str,
    industry: str | None = None,
    topics: list[str] | None = None,
    goal: str = "engagement",
    model: str | None = None,
    embed_fn: Callable[[list[str], str], list[list[float]]] | None = None,
) -> dict[str, Any]:
    analysis = score_draft(text=text, industry=industry, topics=topics, model=model, embed_fn=embed_fn)
    normalized = _normalized_post_text(text)
    lines = [line.strip() for line in normalized.splitlines() if line.strip()]
    body = " ".join(lines[1:]) if len(lines) > 1 else normalized
    draft_topics = _extract_topic_tokens(normalized, limit=4)
    topic = (analysis.get("pattern_matches") or {}).get("matched_topics") or draft_topics
    lead_topic = " ".join(draft_topics[:2]).strip() or (topic[0] if topic else None) or "your workflow"
    body_sentences = [
        clean_text(sentence)
        for sentence in re.split(r"[.!?]+", body)
        if clean_text(sentence)
    ]
    if goal == "instructional":
        hook = analysis["hook"]
        if not re.match(r"^\d", hook):
            normalized_hook = re.sub(r"\bare saving\b", "save", hook, flags=re.IGNORECASE)
            normalized_hook = re.sub(r"\bare\b", "", normalized_hook, flags=re.IGNORECASE)
            normalized_hook = re.sub(r"\s+", " ", normalized_hook).strip(" .")
            hook = f"3 ways {normalized_hook if normalized_hook else lead_topic}."
        bullets = body_sentences[:3]
        if len(bullets) < 3:
            bullets = [
                "Start with one workflow that already repeats every week",
                "Remove one manual handoff so the process actually gets faster",
                "Measure the hours saved so the result is obvious",
            ]
        rewritten = "\n".join([hook, "", *(f"{index}. {item.rstrip('.')}." for index, item in enumerate(bullets, start=1)), "", "Which step would you test first?"])
    elif goal == "authority":
        hook = analysis["hook"]
        if "result" not in hook.lower() and "case study" not in hook.lower():
            hook = f"Case study: {hook.rstrip('.')}."
        rewritten = "\n".join([hook, "", body.strip(), "", "The useful question is what this changes in your workflow."])
    elif goal == "contrarian":
        hook = analysis["hook"]
        if "actually" not in hook.lower():
            hook = f"{hook.rstrip('.')} Actually, most teams are optimizing the wrong part."
        rewritten = "\n".join([hook, "", body.strip(), "", "If you disagree, say where the real leverage is."])
    else:
        hook = analysis["hook"]
        if len(hook) > 120:
            hook = hook[:117].rstrip(" .,") + "..."
        rewritten = "\n".join([hook, "", body.strip(), "", "What are you seeing in your own pipeline?"])

    rewritten = _normalized_post_text(rewritten)
    rescored = score_draft(text=rewritten, industry=industry, topics=topics, model=model, embed_fn=embed_fn)
    return {
        "goal": goal,
        "original": normalized,
        "rewritten": rewritten,
        "score_before": analysis["predicted_outcome_score"],
        "score_after": rescored["predicted_outcome_score"],
        "recommendations": analysis["recommendations"],
        "rewrite_notes": [
            "Sharper first-line hook",
            "More explicit structure",
            "Clearer CTA anchored to engagement",
        ],
    }


def _content_reference_urls(playbook: dict[str, Any], exemplars: list[dict[str, Any]], limit: int = 5) -> list[str]:
    urls: list[str] = []
    for item in list(playbook.get("top_examples") or []) + list(exemplars or []):
        url = clean_text((item or {}).get("url")) if isinstance(item, dict) else None
        if url and url not in urls:
            urls.append(url)
        if len(urls) >= limit:
            break
    return urls


def _draft_policy_action(candidate: dict[str, Any]) -> dict[str, Any]:
    score = candidate.get("score") or {}
    fingerprint = score.get("fingerprint") or {}
    predicted = float(score.get("predicted_outcome_score") or 0.0)
    heuristic = float(score.get("heuristic_prediction") or 0.0)
    model_prediction = float(score.get("model_prediction") or predicted)
    features = [
        predicted,
        heuristic,
        model_prediction,
        *[float(fingerprint.get(name) or 0.0) for name in FINGERPRINT_DIMENSIONS],
    ]
    return {
        "action_id": str(candidate.get("candidate_id") or f"rank_{candidate.get('rank') or 0}"),
        "label": candidate.get("goal") or "candidate",
        "features": features,
        "score": predicted,
        "metadata": {"rank": candidate.get("rank"), "goal": candidate.get("goal")},
    }


def _candidate_topic_hint(prompt: str, topics: list[str] | None = None) -> str:
    normalized_topics = _normalize_topics(topics)
    if normalized_topics:
        return " and ".join(normalized_topics[:2])
    prompt_topics = _extract_topic_tokens(_prompt_subject_text(prompt), limit=4)
    if prompt_topics:
        return " ".join(prompt_topics[:2])
    return "the workflow"


def _prompt_subject_text(prompt: str) -> str:
    normalized = _normalized_post_text(prompt)
    lowered = normalized.lower()
    patterns = (
        r"^(?:create|write|draft|generate)\s+(?:a\s+)?(?:linkedin\s+post|post|draft|thread)\s+(?:about|on|for)\s+",
        r"^(?:create|write|draft|generate)\s+(?:something\s+)?(?:about|on)\s+",
    )
    for pattern in patterns:
        updated = re.sub(pattern, "", lowered, flags=re.IGNORECASE).strip()
        if updated != lowered:
            return clean_text(updated)
    return normalized


def _trim_sentence(value: str, max_chars: int = 140) -> str:
    normalized = clean_text(value) or ""
    if len(normalized) <= max_chars:
        return normalized.rstrip(".")
    return normalized[: max_chars - 3].rstrip(" .,") + "..."


def _candidate_body_sentence(prompt: str, fallback: str) -> str:
    original = _normalized_post_text(prompt)
    normalized = _normalized_post_text(_prompt_subject_text(prompt))
    sentences = [clean_text(sentence) for sentence in re.split(r"[.!?]+", normalized) if clean_text(sentence)]
    if sentences:
        first = sentences[0].rstrip(".")
        prompt_was_instruction = normalized and original and normalized != original
        if prompt_was_instruction and re.match(r"^(why|how|what|when|where)\b", first, flags=re.IGNORECASE):
            lowered = first.lower()
            if "handoff" in lowered and ("break" in lowered or "fail" in lowered):
                return "The failure usually starts at the handoff, not in the model"
            if "break" in lowered or "fail" in lowered:
                return "The failure usually starts in the operating layer, not the model"
            return fallback.rstrip(".")
        return first
    return fallback.rstrip(".")


def _normalize_content_brief(brief: dict[str, Any] | None = None) -> dict[str, str]:
    payload = dict(brief or {})
    normalized: dict[str, str] = {}
    for key in ("audience", "objective", "tone", "format", "length", "cta"):
        value = clean_text(payload.get(key))
        if value:
            normalized[key] = value
    return normalized


def _normalize_generation_speed(speed: str | None = None) -> str:
    normalized = clean_text(speed or "balanced").lower() or "balanced"
    if normalized not in {"balanced", "max"}:
        fail("speed must be one of: balanced, max", code=ExitCode.VALIDATION)
    return normalized


def _brief_cta_line(brief: dict[str, str], topic_hint: str) -> str:
    explicit = clean_text(brief.get("cta"))
    if explicit:
        return explicit.rstrip(".") + "."
    objective = clean_text(brief.get("objective") or "").lower()
    if "demo" in objective:
        return "If you want the operator pattern behind this, reply with `pattern`."
    if "lead" in objective or "inbound" in objective or "pipeline" in objective:
        return "If this is on your roadmap, DM me `workflow` and I will send the operating checklist."
    if "call" in objective or "meeting" in objective:
        return "If you want me to audit the system, comment `audit`."
    return "What is still breaking in your stack?"


def _audience_stake_line(audience: str, topic_hint: str) -> str:
    audience_label = clean_text(audience)
    lowered = audience_label.lower()
    if "engineering" in lowered or "platform" in lowered:
        return f"{audience_label.capitalize()} feel this as reliability debt: the demo passes, but the system still falls apart between owners, queues, and services."
    if "ops" in lowered or "operations" in lowered:
        return f"{audience_label.capitalize()} end up doing the cleanup work after every broken handoff, retry, and exception path."
    if "leader" in lowered or "executive" in lowered:
        return f"{audience_label.capitalize()} feel this first in missed throughput, slower execution, and reporting they cannot trust."
    return f"This matters most for {audience_label} because they inherit the failure when the handoff is vague, manual, or impossible to measure."


def _goal_story_bridge(goal: str, topic_hint: str) -> str:
    if goal == "launch":
        return f"On paper, the rollout looked done. In production, the handoff failed first."
    if goal == "authority":
        return "The first real failure is usually not the model. It is the operating seam between steps."
    if goal == "contrarian":
        return f"The popular story here overweights the model and underweights the system around it."
    if goal == "instructional":
        return f"The useful work starts when you name the exact break instead of admiring the demo."
    return f"The pattern I keep seeing is the same: the demo looks clean, then the handoff breaks under real operating pressure."


def _apply_content_brief(
    *,
    text: str,
    prompt: str,
    goal: str,
    brief: dict[str, str],
    topics: list[str] | None = None,
) -> str:
    if not brief:
        return _normalized_post_text(text)
    normalized = _normalized_post_text(text)
    topic_hint = _candidate_topic_hint(prompt, topics)
    lines = [segment.strip() for segment in normalized.splitlines() if segment.strip()]
    if not lines:
        lines = [normalized]
    lead = lines[0]
    remainder = lines[1:]
    audience = clean_text(brief.get("audience"))
    objective = clean_text(brief.get("objective"))
    tone = clean_text(brief.get("tone")).lower()
    format_hint = clean_text(brief.get("format")).lower()
    length_hint = clean_text(brief.get("length")).lower()
    cta_line = _brief_cta_line(brief, topic_hint)

    if format_hint == "story":
        story_bridge = _goal_story_bridge(goal, topic_hint)
        lines = [lead, story_bridge, *remainder]
    elif format_hint == "operator":
        operator_bridge = "If you own this system, the useful question is not whether the model looks smart. It is whether the handoff path survives a normal week."
        lines = [lead, operator_bridge, *remainder]

    if audience:
        audience_line = _audience_stake_line(audience, topic_hint)
        if audience_line not in lines:
            lines.insert(min(2, len(lines)), audience_line)

    if objective:
        objective_line = f"The point is not more AI theater. It is to {objective} by making the system reliable enough to trust."
        if objective_line not in lines:
            lines.append(objective_line)

    if tone == "operator":
        lines.append("That means naming the owner, tightening the handoff, and proving the result in operator terms instead of presentation terms.")
    elif tone in {"direct", "authoritative"}:
        lines.append("The blunt version: if it only works in the happy path, it does not work yet.")

    if length_hint == "long":
        expanded = _expand_long_form_candidate(text="\n".join(lines), prompt=prompt, goal=goal, topics=topics)
        lines = [segment.strip() for segment in expanded.splitlines() if segment.strip()]
    elif length_hint == "short" and len(lines) > 4:
        lines = lines[:4]

    if lines:
        lines[-1] = cta_line
    return _normalized_post_text("\n".join(lines))


def _heuristic_rewrite_without_corpus(*, text: str, goal: str, topics: list[str] | None = None) -> str:
    normalized = _normalized_post_text(text)
    lines = [line.strip() for line in normalized.splitlines() if line.strip()]
    hook = _sentence_hook(normalized)
    body = " ".join(lines[1:]) if len(lines) > 1 else normalized
    normalized_topics = _normalize_topics(topics)
    draft_topics = _extract_topic_tokens(normalized, limit=4)
    lead_topic = " ".join(draft_topics[:2]).strip() or (" ".join(normalized_topics[:2]).strip() if normalized_topics else "") or "the workflow"
    body_sentences = [clean_text(sentence) for sentence in re.split(r"[.!?]+", body) if clean_text(sentence)]

    if goal == "instructional":
        opening = hook if re.match(r"^\d", hook) else f"3 ways {hook.rstrip('.').lower()}"
        bullets = body_sentences[:3]
        if len(bullets) < 3:
            bullets = [
                f"Start with one repeated part of {lead_topic}",
                "Remove one manual handoff that creates delay",
                "Measure the hours saved so the result is obvious",
            ]
        return _normalized_post_text(
            "\n".join([opening.rstrip(".") + ".", "", *(f"{index}. {item.rstrip('.')}." for index, item in enumerate(bullets, start=1)), "", "Which step would you test first?"])
        )
    if goal == "authority":
        opening = hook if "case study" in hook.lower() else f"Case study: {hook.rstrip('.')}"
        return _normalized_post_text(
            "\n".join([opening.rstrip(".") + ".", "", body.rstrip(".") + ".", "", f"The useful question is what this changes in {lead_topic}."])
        )
    if goal == "contrarian":
        opening = hook if "actually" in hook.lower() else f"{hook.rstrip('.')} Actually, most teams are optimizing the wrong part."
        return _normalized_post_text(
            "\n".join([opening, "", body.rstrip(".") + ".", "", f"What belief about {lead_topic} are you still seeing people defend?"])
        )
    opening = hook[:117].rstrip(" .,") + "..." if len(hook) > 120 else hook
    return _normalized_post_text(
        "\n".join([opening.rstrip(".") + ".", "", body.rstrip(".") + ".", "", f"What are you seeing in your own {lead_topic} stack?"])
    )


def _render_candidate_text(
    *,
    prompt: str,
    goal: str,
    playbook: dict[str, Any],
    exemplars: list[dict[str, Any]],
    topics: list[str] | None = None,
    variant_index: int = 0,
    brief: dict[str, str] | None = None,
) -> str:
    topic_hint = _candidate_topic_hint(prompt, topics)
    top_hook_template = (playbook.get("hook_templates") or [{}])[variant_index % max(1, len(playbook.get("hook_templates") or [{}]))]
    top_rules = list(playbook.get("rewrite_rules") or [])
    body_seed = _candidate_body_sentence(
        prompt,
        fallback=f"Most teams still handle {topic_hint} with brittle handoffs and no proof.",
    )
    hook_type = (clean_text((top_hook_template or {}).get("hook_type")) or "").lower()
    if hook_type == "personal":
        fresh_hook = f"I learned the hard way that {topic_hint} breaks in the handoff, not the demo."
    elif hook_type == "contrarian":
        fresh_hook = f"Most teams are fixing the wrong part of {topic_hint}."
    elif hook_type == "question":
        fresh_hook = f"Why does {topic_hint} still fail when the demo looked perfect?"
    else:
        fresh_hook = f"{topic_hint.capitalize()} breaks when nobody owns the handoff."
    proof_line = "We mapped the handoff, removed the break, and measured what changed."
    if top_rules:
        first_rule = clean_text(top_rules[min(variant_index, len(top_rules) - 1)]) or ""
        if "proof" in first_rule.lower():
            proof_line = "The only useful version is the one with proof: a result, a number, or a visible before-and-after."
        elif "narrative" in first_rule.lower():
            proof_line = "The turning point was not the model. It was the handoff we finally stopped tolerating."
        elif "cta" in first_rule.lower():
            proof_line = "The interesting question is which part of the workflow still breaks when nobody is watching."

    rendered: str
    if goal == "instructional":
        hook = _trim_sentence(f"How we made {topic_hint} less brittle without adding more process")
        steps = [
            "Start with the exact break, not the abstract strategy.",
            "Remove one handoff so the system gets simpler before it gets smarter.",
            "Measure one concrete result so the improvement is obvious to the operator.",
        ]
        rendered = "\n".join([hook, "", f"{body_seed}.", "", *(f"{index}. {step}" for index, step in enumerate(steps, start=1)), "", "Which step is still failing in your stack?"])
    elif goal == "authority":
        hook = _trim_sentence(f"Case study: {body_seed}")
        rendered = "\n".join(
            [
                hook,
                "",
                f"{proof_line}",
                "The common mistake is treating this like a prompt problem instead of an operating problem.",
                "The highest-leverage fix is usually hiding in one ugly operating break.",
                "",
                "If you own this system, the real question is whether the result survives contact with a real operator.",
            ]
        )
    elif goal == "contrarian":
        hook = _trim_sentence(f"Most advice about {topic_hint} is wrong.")
        rendered = "\n".join(
            [
                hook,
                "",
                f"{body_seed}.",
                "The model is rarely the bottleneck. The hidden cost is the handoff everyone ignores.",
                proof_line,
                "",
                f"What belief about {topic_hint} are you still seeing people defend?",
            ]
        )
    elif goal == "launch":
        hook = _trim_sentence(f"Today we changed one part of {topic_hint} that operators actually feel.")
        rendered = "\n".join(
            [
                hook,
                "",
                f"{body_seed}.",
                proof_line,
                "We did not add a dashboard. We removed the break that made the system unreliable.",
                "",
                "If you want the pattern, say `pattern` and I’ll break it down.",
            ]
        )
    else:
        hook = _trim_sentence(fresh_hook)
        rendered = "\n".join(
            [
                hook,
                "",
                f"{body_seed}.",
                proof_line,
                "The teams that win here treat the operating layer like a system, not a demo.",
                "",
                "What is still breaking in your stack?",
            ]
        )
    return _apply_content_brief(
        text=rendered,
        prompt=prompt,
        goal=goal,
        brief=_normalize_content_brief(brief),
        topics=topics,
    )


def _expand_long_form_candidate(*, text: str, prompt: str, goal: str, topics: list[str] | None = None) -> str:
    normalized = _normalized_post_text(text)
    topic_hint = _candidate_topic_hint(prompt, topics)
    body_seed = _candidate_body_sentence(
        prompt,
        fallback=f"Most teams still handle {topic_hint} with brittle handoffs and no proof.",
    )
    additions = [
        "What actually matters:",
        "The deeper issue is not the model quality. It is the operating layer around the work: the handoff, the owner, and the proof that the system got better.",
        "In practice, the teams that make this work do three things differently. They start with the exact break, remove one hidden manual step, and measure the result in terms an operator actually trusts.",
    ]
    if goal == "authority":
        additions.append(
            "The mistake I keep seeing is teams treating this like a prompt exercise instead of a systems problem. The prompt matters far less than the routing, the exception path, and whether the result survives a real week of usage."
        )
    elif goal == "instructional":
        additions.append(
            "If you want a practical starting point, map the current process, identify the single most expensive handoff, and define the before-and-after metric before you touch the tooling."
        )
    elif goal == "contrarian":
        additions.append(
            "That is why a lot of loud advice here is directionally wrong. It rewards novelty in the demo instead of reliability in the operating flow."
        )
    else:
        additions.append(
            f"The useful question is whether {body_seed.lower()} long after the launch post. If the handoff path still breaks under normal load, the story was stronger than the system."
        )
    additions.extend(
        [
            "What to change next:",
            "Name the owner of the handoff, remove one hidden approval step, and define the single metric that proves the system improved.",
            "If the handoff path cannot survive retries, exceptions, and a normal week of operator usage, it is still a demo instead of a system.",
        ]
    )
    return _normalized_post_text("\n".join([normalized, *additions]))


def create_drafts(
    *,
    prompt: str,
    industry: str | None = None,
    topics: list[str] | None = None,
    model: str | None = None,
    candidate_goals: list[str] | None = None,
    candidate_count: int = 8,
    generator: str = "auto",
    speed: str = "balanced",
    brief: dict[str, Any] | None = None,
    embed_fn: Callable[[list[str], str], list[list[float]]] | None = None,
) -> dict[str, Any]:
    normalized_prompt = _normalized_post_text(prompt)
    if not normalized_prompt:
        fail("Prompt text is required", code=ExitCode.VALIDATION)
    normalized_topics = _normalize_topics(topics)
    normalized_brief = _normalize_content_brief(brief)
    normalized_speed = _normalize_generation_speed(speed)
    playbook_limit = max(6, candidate_count)
    exemplar_limit = max(5, min(candidate_count * 2, 12))
    if normalized_speed == "max":
        playbook_limit = 4
        exemplar_limit = 3
    playbook = build_playbook(industry=industry, topics=normalized_topics, limit=playbook_limit)
    exemplars = retrieve_posts(
        query_text=normalized_prompt,
        limit=exemplar_limit,
        method="hybrid",
        model=model,
        industry=industry,
        embed_fn=embed_fn,
    )
    exemplars = _filter_posts_by_relevance(exemplars, industry=industry, topics=normalized_topics)
    goal_sequence = list(dict.fromkeys(candidate_goals or ["engagement", "instructional", "authority", "contrarian", "launch"]))
    if not goal_sequence:
        goal_sequence = ["engagement"]
    reference_urls = _content_reference_urls(playbook, exemplars)
    raw_candidates: list[dict[str, Any]] = []
    requested_generator = str(generator or "auto").strip().lower()
    normalized_generator = requested_generator
    if normalized_generator == "auto":
        from linkedin_cli import llm_providers

        normalized_generator = "cerebras" if llm_providers.is_provider_configured("cerebras") else "heuristic"
    if normalized_generator == "heuristic":
        for index in range(max(1, candidate_count)):
            goal = goal_sequence[index % len(goal_sequence)]
            text = _render_candidate_text(
                prompt=normalized_prompt,
                goal=goal,
                playbook=playbook,
                exemplars=exemplars,
                topics=normalized_topics,
                variant_index=index,
            )
            raw_candidates.append(
                {
                    "candidate_id": f"draft-{index + 1:02d}",
                    "goal": goal,
                    "text": text,
                    "generator": {
                        "source": "playbook-heuristic-v1",
                        "industry": industry,
                        "topics": normalized_topics,
                    },
                }
            )
    else:
        from linkedin_cli import llm_providers

        try:
            generated = llm_providers.generate_candidates_via_provider(
                provider_name=normalized_generator,
                prompt=normalized_prompt,
                industry=industry,
                topics=normalized_topics,
                candidate_goals=goal_sequence,
                candidate_count=max(1, candidate_count),
                playbook=playbook,
                exemplars=exemplars,
                brief=normalized_brief,
            )
            raw_candidates = list(generated.get("candidates") or [])
        except (Exception, CliError):
            if requested_generator != "auto":
                raise
            normalized_generator = "heuristic"
            for index in range(max(1, candidate_count)):
                goal = goal_sequence[index % len(goal_sequence)]
                text = _render_candidate_text(
                    prompt=normalized_prompt,
                    goal=goal,
                    playbook=playbook,
                    exemplars=exemplars,
                    topics=normalized_topics,
                    variant_index=index,
                )
                raw_candidates.append(
                    {
                        "candidate_id": f"draft-{index + 1:02d}",
                        "goal": goal,
                        "text": text,
                        "generator": {
                            "source": "playbook-heuristic-v1",
                            "industry": industry,
                            "topics": normalized_topics,
                            "fallback_from": requested_generator,
                        },
                    }
                )
    from linkedin_cli import llm_providers
    top_example_hooks = [
        clean_text((item or {}).get("hook"))
        for item in (playbook.get("top_examples") or [])[:3]
        if clean_text((item or {}).get("hook"))
    ]
    candidates: list[dict[str, Any]] = []
    for index, candidate in enumerate(raw_candidates[: max(1, candidate_count)], start=1):
        text = str(candidate.get("text") or "").strip()
        if not text:
            continue
        goal = str(candidate.get("goal") or goal_sequence[(index - 1) % len(goal_sequence)]).strip().lower()
        if normalized_brief:
            text = _apply_content_brief(text=text, prompt=normalized_prompt, goal=goal, brief=normalized_brief, topics=normalized_topics)
        originality_issues = llm_providers.validate_candidate_originality(text=text, prompt=normalized_prompt, exemplars=exemplars)
        if originality_issues:
            fallback_text = _render_candidate_text(
                prompt=normalized_prompt,
                goal=goal,
                playbook={"hook_templates": [], "rewrite_rules": playbook.get("rewrite_rules") or [], "top_examples": []},
                exemplars=[],
                topics=normalized_topics,
                variant_index=index - 1,
                brief=normalized_brief,
            )
            text = fallback_text
        score = score_draft(text=text, industry=industry, topics=normalized_topics, model=model, embed_fn=embed_fn)
        candidates.append(
            {
                "candidate_id": str(candidate.get("candidate_id") or f"draft-{index:02d}"),
                "goal": goal,
                "text": text,
                "score": score,
                "references": {
                    "top_example_urls": reference_urls,
                    "top_example_hooks": top_example_hooks,
                    "brief": dict(normalized_brief),
                },
                "generator": dict(candidate.get("generator") or {"source": "playbook-heuristic-v1"}),
            }
        )
    ranked = sorted(
        candidates,
        key=lambda item: (
            float((item.get("score") or {}).get("predicted_outcome_score") or 0.0),
            -len(str(item.get("text") or "")),
        ),
        reverse=True,
    )
    for rank, candidate in enumerate(ranked, start=1):
        candidate["rank"] = rank
    return {
        "prompt": normalized_prompt,
        "industry": industry,
        "topics": normalized_topics,
        "brief": normalized_brief,
        "speed": normalized_speed,
        "generator_requested": requested_generator,
        "generator": normalized_generator,
        "playbook": playbook,
        "exemplars": exemplars,
        "candidates": ranked,
    }


def choose_draft(
    *,
    prompt: str,
    industry: str | None = None,
    topics: list[str] | None = None,
    model: str | None = None,
    candidate_goals: list[str] | None = None,
    candidate_count: int = 8,
    generator: str = "auto",
    speed: str = "balanced",
    brief: dict[str, Any] | None = None,
    embed_fn: Callable[[list[str], str], list[list[float]]] | None = None,
    policy_name: str | None = None,
    policy_alpha: float = 0.2,
    log_decision: bool = False,
    context_key: str | None = None,
    polish_selected: bool = False,
    stacked_model_name: str | None = None,
    target_profile: dict[str, Any] | None = None,
    auto_calibrate_weights: bool = True,
    polish_limit: int = 3,
) -> dict[str, Any]:
    created = create_drafts(
        prompt=prompt,
        industry=industry,
        topics=topics,
        model=model,
        candidate_goals=candidate_goals,
        candidate_count=candidate_count,
        generator=generator,
        speed=speed,
        brief=brief,
        embed_fn=embed_fn,
    )
    candidates = list(created.get("candidates") or [])
    if not candidates:
        fail("No draft candidates were generated", code=ExitCode.GENERAL)
    best_candidate = candidates[0]
    policy_decision = None
    if policy_name:
        from linkedin_cli import policy

        policy.init_policy_db()
        actions = [_draft_policy_action(candidate) for candidate in candidates]
        policy_decision = policy.choose_action_linucb(
            policy_name=policy_name,
            context_type="content_publish",
            context_key=context_key or hashlib.sha1(created["prompt"].encode("utf-8")).hexdigest()[:16],
            context_features=[1.0, float(len(created.get("topics") or [])), float(len(created["prompt"].split()))],
            actions=actions,
            alpha=policy_alpha,
            log_decision=log_decision,
            metadata={"industry": industry, "topics": created.get("topics") or []},
        )
        selected_action = policy_decision["chosen_action_id"]
        best_candidate = next((candidate for candidate in candidates if str(candidate.get("candidate_id") or f"rank_{candidate.get('rank') or 0}") == selected_action), best_candidate)
    best_score = best_candidate.get("score") or {}
    playbook = created.get("playbook") or {}
    polished_choice = None
    if polish_selected and best_candidate and str(best_candidate.get("text") or "").strip():
        polished_choice = polish_and_score(
            text=str(best_candidate.get("text") or ""),
            industry=industry,
            topics=created.get("topics") or [],
            model=model,
            candidate_goals=candidate_goals,
            stacked_model_name=stacked_model_name,
            target_profile=target_profile,
            auto_calibrate_weights=auto_calibrate_weights,
            limit=polish_limit,
            embed_fn=embed_fn,
        )
    return {
        "prompt": created["prompt"],
        "industry": created.get("industry"),
        "topics": created.get("topics") or [],
        "brief": dict(created.get("brief") or {}),
        "speed": created.get("speed") or "balanced",
        "best_candidate": best_candidate,
        "polished_choice": polished_choice,
        "candidates": candidates,
        "policy_decision": policy_decision,
        "why_it_won": {
            "learned_signals": playbook.get("learned_signals") or {},
            "recommendations": best_score.get("recommendations") or [],
            "matched_topics": ((best_score.get("pattern_matches") or {}).get("matched_topics") or []),
            "top_structure": ((best_score.get("pattern_matches") or {}).get("top_structure")),
        },
        "references": {
            "top_example_urls": _content_reference_urls(playbook, created.get("exemplars") or []),
            "hook_templates": [item.get("template") for item in (playbook.get("hook_templates") or [])[:3] if item.get("template")],
        },
    }


def queue_drafts(
    *,
    prompt: str,
    industry: str | None = None,
    topics: list[str] | None = None,
    model: str | None = None,
    candidate_goals: list[str] | None = None,
    candidate_count: int = 8,
    generator: str = "auto",
    speed: str = "balanced",
    brief: dict[str, Any] | None = None,
    embed_fn: Callable[[list[str], str], list[list[float]]] | None = None,
) -> dict[str, Any]:
    created = create_drafts(
        prompt=prompt,
        industry=industry,
        topics=topics,
        model=model,
        candidate_goals=candidate_goals,
        candidate_count=candidate_count,
        generator=generator,
        speed=speed,
        brief=brief,
        embed_fn=embed_fn,
    )
    persisted = _persist_candidates(created, model=model)
    best_candidate = next((item for item in persisted if int(item.get("rank") or 0) == 1), None)
    return {
        "prompt": created["prompt"],
        "industry": created.get("industry"),
        "topics": created.get("topics") or [],
        "speed": created.get("speed") or "balanced",
        "candidate_count": len(persisted),
        "best_candidate": best_candidate,
        "candidates": persisted,
    }


def _persist_candidates(created: dict[str, Any], *, model: str | None = None) -> list[dict[str, Any]]:
    now = store._now_iso()
    conn = store._connect()
    try:
        persisted: list[dict[str, Any]] = []
        for candidate in created.get("candidates") or []:
            candidate_id = f"cand_{uuid.uuid4().hex[:12]}"
            conn.execute(
                """
                INSERT INTO content_candidates
                (candidate_id, created_at, updated_at, prompt, industry, topics_json, goal, text, rank, chosen, status,
                 generator_source, model_name, score_json, references_json, post_url, metadata_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'queued', ?, ?, ?, ?, NULL, ?)
                """,
                (
                    candidate_id,
                    now,
                    now,
                    created["prompt"],
                    created.get("industry"),
                    json.dumps(created.get("topics") or [], ensure_ascii=False, sort_keys=True),
                    candidate.get("goal"),
                    candidate.get("text"),
                    int(candidate.get("rank") or 0),
                    1 if int(candidate.get("rank") or 0) == 1 else 0,
                    ((candidate.get("generator") or {}).get("source") or "playbook-heuristic-v1"),
                    model,
                    json.dumps(candidate.get("score") or {}, ensure_ascii=False, sort_keys=True),
                    json.dumps(candidate.get("references") or {}, ensure_ascii=False, sort_keys=True),
                    json.dumps(
                        {
                            "industry": created.get("industry"),
                            "topics": created.get("topics") or [],
                            "playbook_post_count": (created.get("playbook") or {}).get("post_count"),
                        },
                        ensure_ascii=False,
                        sort_keys=True,
                    ),
                ),
            )
            candidate["candidate_id"] = candidate_id
            persisted.append(candidate)
        conn.commit()
        return persisted
    finally:
        conn.close()


def list_candidate_queue(limit: int = 20, status: str | None = None) -> list[dict[str, Any]]:
    init_content_db()
    conn = store._connect()
    try:
        where = ""
        params: list[Any] = []
        if status:
            where = "WHERE status = ?"
            params.append(status)
        rows = conn.execute(
            f"SELECT * FROM content_candidates {where} ORDER BY chosen DESC, updated_at DESC LIMIT ?",
            (*params, max(1, int(limit))),
        ).fetchall()
        return [_candidate_row_to_dict(row) for row in rows]
    finally:
        conn.close()


def get_candidate(candidate_id: str) -> dict[str, Any]:
    init_content_db()
    conn = store._connect()
    try:
        row = conn.execute("SELECT * FROM content_candidates WHERE candidate_id = ?", (candidate_id,)).fetchone()
        if row is None:
            fail(f"Content candidate not found: {candidate_id}", code=ExitCode.NOT_FOUND)
        return _candidate_row_to_dict(row)
    finally:
        conn.close()


def mark_candidate_published(
    candidate_id: str,
    *,
    post_url: str | None = None,
    trace_id: str | None = None,
    decision_id: str | None = None,
    reward_window_hours: int | None = None,
) -> dict[str, Any]:
    init_content_db()
    now = store._now_iso()
    conn = store._connect()
    try:
        row = conn.execute("SELECT * FROM content_candidates WHERE candidate_id = ?", (candidate_id,)).fetchone()
        if row is None:
            fail(f"Content candidate not found: {candidate_id}", code=ExitCode.NOT_FOUND)
        conn.execute(
            """
            UPDATE content_candidates
            SET status = 'published',
                post_url = COALESCE(?, post_url),
                updated_at = ?
            WHERE candidate_id = ?
            """,
            (post_url, now, candidate_id),
        )
        conn.commit()
        refreshed = conn.execute("SELECT * FROM content_candidates WHERE candidate_id = ?", (candidate_id,)).fetchone()
        assert refreshed is not None
        result = _candidate_row_to_dict(refreshed)
        if result.get("post_url"):
            from linkedin_cli import traces

            traces.init_trace_db()
            effective_trace_id = trace_id
            if not effective_trace_id:
                manual_trace = traces.start_trace(
                    trace_type="manual_publish",
                    request_kind="content_publish",
                    context_key=f"manual:{candidate_id}",
                    root_entity_kind="content_candidate",
                    root_entity_key=candidate_id,
                    metadata={"candidate_id": candidate_id, "manual": True},
                )
                effective_trace_id = manual_trace["trace_id"]
            result["reward_window"] = traces.open_reward_window(
                trace_id=str(effective_trace_id),
                decision_id=decision_id,
                action_type="publish_post",
                entity_kind="post",
                entity_key=str(result["post_url"]),
                reward_window_hours=reward_window_hours,
                baseline={"reaction_count": 0, "comment_count": 0, "repost_count": 0},
                metadata={"candidate_id": candidate_id},
            )
        return result
    finally:
        conn.close()


def _candidate_runtime_actions(candidates: list[dict[str, Any]]) -> list[dict[str, Any]]:
    actions: list[dict[str, Any]] = []
    for candidate in candidates:
        score = candidate.get("score") or {}
        action_id = str(candidate.get("candidate_id") or f"rank_{candidate.get('rank') or 0}")
        actions.append(
            {
                "action_id": action_id,
                "action_type": "publish_post",
                "label": f"publish {candidate.get('goal') or 'candidate'}",
                "payload": {
                    "candidate_id": action_id,
                    "text": candidate.get("text") or "",
                },
                "score_snapshot": score,
                "metadata": {
                    "goal": candidate.get("goal"),
                    "rank": candidate.get("rank"),
                    "policy_features": (_draft_policy_action(candidate).get("features") or []),
                },
            }
        )
    actions.append(
        {
            "action_id": "noop",
            "action_type": "noop",
            "label": "do nothing",
            "payload": {},
            "score_snapshot": {"predicted_outcome_score": 0.0},
            "metadata": {"policy_features": [0.0] * (3 + len(FINGERPRINT_DIMENSIONS))},
        }
    )
    return actions


def _resolve_autonomy_account_id(session: Any) -> str:
    try:
        response = voyager_get(session, "/voyager/api/me")
        data = parse_json_response(response)
        me = data.get("data") or data
        member_id = me.get("plainId")
        if member_id:
            return str(member_id)
        for item in (data.get("included") or []):
            if isinstance(item, dict):
                urn = item.get("entityUrn") or ""
                if urn.startswith("urn:li:fs_miniProfile:"):
                    return urn.split(":")[-1]
    except Exception:
        pass
    return "me"


def _remote_ref_to_post_url(remote_ref: str | None) -> str | None:
    if not remote_ref:
        return None
    if remote_ref.startswith("urn:li:activity:") or remote_ref.startswith("urn:li:share:"):
        return f"https://www.linkedin.com/feed/update/{remote_ref}/"
    return remote_ref


def run_autonomy(
    *,
    prompt: str,
    industry: str | None = None,
    topics: list[str] | None = None,
    model: str | None = None,
    candidate_goals: list[str] | None = None,
    candidate_count: int = 8,
    generator: str = "auto",
    speed: str = "balanced",
    brief: dict[str, Any] | None = None,
    decision_provider: str = "local-policy",
    policy_name: str = "content-default",
    policy_alpha: float = 0.2,
    mode: str = "review",
    post_url: str | None = None,
    polish_selected: bool = False,
    stacked_model_name: str | None = None,
    target_profile: dict[str, Any] | None = None,
    auto_calibrate_weights: bool = True,
    polish_limit: int = 3,
) -> dict[str, Any]:
    from linkedin_cli import policy, runtime_contract, traces

    init_content_db()
    traces.init_trace_db()
    normalized_topics = _normalize_topics(topics)
    context_key = hashlib.sha1(
        json.dumps({"prompt": prompt, "industry": industry, "topics": normalized_topics}, ensure_ascii=False, sort_keys=True).encode("utf-8")
    ).hexdigest()[:16]
    trace = traces.start_trace(
        trace_type="content_autonomy",
        request_kind="content_publish",
        context_key=context_key,
        root_entity_kind="content_prompt",
        root_entity_key=context_key,
        metadata={"industry": industry, "topics": normalized_topics, "policy_name": policy_name, "policy_alpha": policy_alpha, "mode": mode},
    )
    try:
        created = create_drafts(
            prompt=prompt,
            industry=industry,
            topics=normalized_topics,
            model=model,
            candidate_goals=candidate_goals,
            candidate_count=candidate_count,
            generator=generator,
            speed=speed,
            brief=brief,
        )
        traces.append_trace_step(
            trace["trace_id"],
            step_kind="generation",
            step_name="generate_candidates",
            output_payload={
                "prompt": created["prompt"],
                "industry": created.get("industry"),
                "topics": created.get("topics") or [],
                "candidate_count": len(created.get("candidates") or []),
                "generator": generator,
                "speed": created.get("speed") or speed,
                "decision_provider": decision_provider,
            },
            artifact_kind="candidates",
            artifact_payload=created,
        )
        candidates = _persist_candidates(created, model=model)
        best_candidate = next((item for item in candidates if int(item.get("rank") or 0) == 1), candidates[0] if candidates else None)
        actions = _candidate_runtime_actions(candidates)
        request_payload = runtime_contract.build_runtime_request(
            task_type="content_publish",
            objective=f"Choose the strongest {industry or 'general'} content candidate to publish or hold.",
            context={
                "industry": industry,
                "topics": normalized_topics,
                "policy_name": policy_name,
                "mode": mode,
                "speed": created.get("speed") or speed,
            },
            actions=actions,
        )
        traces.append_trace_step(
            trace["trace_id"],
            step_kind="decision",
            step_name="runtime_request",
            output_payload=request_payload,
            artifact_kind="runtime-request",
            artifact_payload=request_payload,
        )
        policy.init_policy_db()
        scored_actions = []
        for action in actions:
            metadata = action.get("metadata") or {}
            scored_actions.append(
                {
                    "action_id": action["action_id"],
                    "label": action["label"],
                    "features": list(metadata.get("policy_features") or [0.0, 0.0, 0.0, 0.0]),
                    "score": float((action.get("score_snapshot") or {}).get("predicted_outcome_score") or 0.0),
                    "metadata": metadata,
                }
            )
        context_features = [1.0, float(len(normalized_topics)), float(len(prompt.split())), 1.0 if industry else 0.0]
        normalized_decision_provider = str(decision_provider or "local-policy").strip().lower()
        provider_decision = None
        if normalized_decision_provider == "local-policy":
            policy_decision = policy.choose_action_linucb(
                policy_name=policy_name,
                context_type="content_publish",
                context_key=context_key,
                context_features=context_features,
                actions=scored_actions,
                alpha=policy_alpha,
                log_decision=True,
                metadata={"trace_id": trace["trace_id"], "industry": industry, "topics": normalized_topics, "decision_source": "local-policy"},
            )
            chosen_id = policy_decision["chosen_action_id"]
            chosen_action = next((item for item in actions if item["action_id"] == chosen_id), actions[0])
            response_payload = runtime_contract.parse_runtime_response(
                {
                    "request_id": request_payload["request_id"],
                    "request_fingerprint": request_payload["request_fingerprint"],
                    "chosen_action_id": chosen_action["action_id"],
                    "action_type": chosen_action["action_type"],
                    "execute": bool(mode == "limited"),
                    "rationale": "Selected by the local contextual policy over the scored candidate set.",
                    "payload": dict(chosen_action.get("payload") or {}),
                },
                request=request_payload,
            )
        else:
            from linkedin_cli import llm_providers

            provider_decision = llm_providers.decide_runtime_action_via_provider(
                provider_name=normalized_decision_provider,
                request=request_payload,
            )
            response_payload = dict(provider_decision["response"])
            response_payload["execute"] = bool(mode == "limited" and response_payload.get("execute"))
            chosen_id = response_payload["chosen_action_id"]
            chosen_action = next((item for item in actions if item["action_id"] == chosen_id), actions[0])
            policy_decision = policy.log_policy_decision(
                policy_name=policy_name,
                context_type="content_publish",
                context_key=context_key,
                context_features=context_features,
                available_actions=scored_actions,
                chosen_action_id=chosen_id,
                chosen_score=float((chosen_action.get("score_snapshot") or {}).get("predicted_outcome_score") or 0.0),
                propensity=1.0,
                metadata={
                    "trace_id": trace["trace_id"],
                    "industry": industry,
                    "topics": normalized_topics,
                    "decision_source": f"provider:{normalized_decision_provider}",
                    "provider_model": provider_decision.get("model"),
                },
            )
            traces.append_trace_step(
                trace["trace_id"],
                step_kind="decision",
                step_name="provider_completion",
                output_payload={
                    "provider": provider_decision.get("provider"),
                    "model": provider_decision.get("model"),
                    "chosen_action_id": chosen_id,
                },
                artifact_kind="provider-completion",
                artifact_payload={
                    "messages": provider_decision.get("messages") or [],
                    "raw_content": provider_decision.get("raw_content"),
                    "raw_response": provider_decision.get("raw_response") or {},
                },
            )
        traces.append_trace_step(
            trace["trace_id"],
            step_kind="decision",
            step_name="runtime_response",
            output_payload=response_payload,
            metadata={"decision_id": policy_decision.get("decision_id")},
            artifact_kind="runtime-response",
            artifact_payload=response_payload,
        )
        chosen_candidate = next((item for item in candidates if item.get("candidate_id") == chosen_id), best_candidate)
        polished_choice = None
        if polish_selected and chosen_candidate and str(chosen_candidate.get("text") or "").strip():
            polished_choice = polish_and_score(
                text=str(chosen_candidate.get("text") or ""),
                industry=industry,
                topics=normalized_topics,
                model=model,
                candidate_goals=candidate_goals,
                stacked_model_name=stacked_model_name,
                target_profile=target_profile,
                auto_calibrate_weights=auto_calibrate_weights,
                limit=polish_limit,
            )
        execution_summary: dict[str, Any] = {
            "mode": mode,
            "queued_candidate_ids": [item["candidate_id"] for item in candidates],
            "chosen_candidate_id": chosen_candidate.get("candidate_id") if chosen_candidate else None,
            "decision_id": policy_decision.get("decision_id"),
        }
        if polished_choice:
            execution_summary["polished_best_rank"] = int((polished_choice.get("best_variant") or {}).get("rank") or 0)
        if mode == "limited" and chosen_candidate and chosen_action["action_type"] == "publish_post":
            if post_url:
                published = mark_candidate_published(
                    str(chosen_candidate["candidate_id"]),
                    post_url=post_url,
                    trace_id=trace["trace_id"],
                    decision_id=policy_decision.get("decision_id"),
                )
                execution_summary["published_candidate"] = published
            else:
                from linkedin_cli.write.executor import execute_action
                from linkedin_cli.write.plans import build_post_plan

                session, _ = load_session(required=True)
                if session is None:
                    fail("No LinkedIn session available for limited autonomy publish", code=ExitCode.AUTH)
                account_id = _resolve_autonomy_account_id(session)
                plan = build_post_plan(
                    account_id=account_id,
                    text=str(chosen_candidate.get("text") or ""),
                    visibility="anyone",
                )
                action_id = f"act_{uuid.uuid4().hex[:12]}"
                publish_result = execute_action(
                    session=session,
                    action_id=action_id,
                    plan=plan,
                    account_id=account_id,
                    dry_run=False,
                )
                live_result = publish_result.get("result") or {}
                remote_ref = (
                    live_result.get("remote_ref")
                    or (publish_result.get("action") or {}).get("remote_ref")
                )
                publish_url = _remote_ref_to_post_url(str(remote_ref)) if remote_ref else None
                execution_summary["publish_action"] = {
                    "action_id": action_id,
                    "status": publish_result.get("status"),
                    "remote_ref": remote_ref,
                    "post_url": publish_url,
                }
                if publish_result.get("status") == "succeeded" and publish_url:
                    published = mark_candidate_published(
                        str(chosen_candidate["candidate_id"]),
                        post_url=publish_url,
                        trace_id=trace["trace_id"],
                        decision_id=policy_decision.get("decision_id"),
                    )
                    execution_summary["published_candidate"] = published
        execution_status = "queued"
        if execution_summary.get("published_candidate"):
            execution_status = "executed"
        elif execution_summary.get("publish_action"):
            execution_status = str(execution_summary["publish_action"].get("status") or "execution_attempted")
        execution_result = runtime_contract.build_execution_result(
            request=request_payload,
            response=response_payload,
            status=execution_status,
            artifact_refs=[f"trace:{trace['trace_id']}"],
            telemetry={"reward_window_hours": 72 if execution_summary.get("published_candidate") else 0},
        )
        execution_result["summary"] = execution_summary
        traces.append_trace_step(
            trace["trace_id"],
            step_kind="execution",
            step_name="execute_action",
            output_payload=execution_result,
            artifact_kind="execution-result",
            artifact_payload=execution_result,
        )
        finalized = traces.finalize_trace(
            trace["trace_id"],
            status=traces.TRACE_STATUS_COMPLETED,
            summary={
                "candidate_count": len(candidates),
                "queued_candidate_ids": execution_summary["queued_candidate_ids"],
                "chosen_candidate_id": execution_summary["chosen_candidate_id"],
                "decision_id": policy_decision.get("decision_id"),
            },
            metadata_patch={"context_features": context_features, "context_type": "content_publish"},
        )
        return {
            "trace_id": finalized["trace_id"],
            "status": finalized["status"],
            "mode": mode,
            "request": request_payload,
            "response": response_payload,
            "execution": execution_result,
            "best_candidate": chosen_candidate,
            "polished_choice": polished_choice,
            "decision": policy_decision,
            "queued_candidates": candidates,
        }
    except Exception as exc:
        traces.append_trace_step(
            trace["trace_id"],
            step_kind="error",
            step_name="failure",
            status="failed",
            output_payload={"error": str(exc)},
            artifact_kind="failure",
            artifact_payload={"error": str(exc)},
        )
        traces.finalize_trace(trace["trace_id"], status=traces.TRACE_STATUS_FAILED, summary={"error": str(exc)})
        raise


def maximize_draft(
    *,
    text: str,
    industry: str | None = None,
    topics: list[str] | None = None,
    model: str | None = None,
    candidate_goals: list[str] | None = None,
    embed_fn: Callable[[list[str], str], list[list[float]]] | None = None,
) -> dict[str, Any]:
    normalized = _normalized_post_text(text)
    baseline = score_draft(text=normalized, industry=industry, topics=topics, model=model, embed_fn=embed_fn)
    playbook = build_playbook(industry=industry, topics=topics, limit=6)
    goals = list(dict.fromkeys(candidate_goals or ["engagement", "instructional", "authority", "contrarian"]))
    variants: list[dict[str, Any]] = []
    for goal in goals:
        rewrite = rewrite_draft(text=normalized, industry=industry, topics=topics, goal=goal, model=model, embed_fn=embed_fn)
        variants.append(
            {
                "goal": goal,
                "rewritten": rewrite["rewritten"],
                "score_before": rewrite["score_before"],
                "score_after": rewrite["score_after"],
                "lift": round(float(rewrite["score_after"]) - float(rewrite["score_before"]), 4),
                "rewrite_notes": rewrite["rewrite_notes"],
            }
        )
    best_variant = max(variants, key=lambda item: (float(item["score_after"]), float(item["lift"]))) if variants else None
    return {
        "baseline": baseline,
        "playbook": playbook,
        "variants": sorted(variants, key=lambda item: (float(item["score_after"]), float(item["lift"])), reverse=True),
        "best_variant": best_variant,
        "best_score": float(best_variant["score_after"]) if best_variant else float(baseline["predicted_outcome_score"]),
    }


def polish_and_score(
    *,
    text: str,
    industry: str | None = None,
    topics: list[str] | None = None,
    model: str | None = None,
    candidate_goals: list[str] | None = None,
    stacked_model_name: str | None = None,
    target_profile: dict[str, Any] | None = None,
    auto_calibrate_weights: bool = True,
    limit: int = 3,
    fresh: bool = False,
    long_form: bool = False,
    embed_fn: Callable[[list[str], str], list[list[float]]] | None = None,
) -> dict[str, Any]:
    from linkedin_cli import content_stack

    normalized = _normalized_post_text(text)
    if not normalized:
        fail("Draft text is required", code=ExitCode.VALIDATION)
    normalized_topics = _normalize_topics(topics)
    limit = max(1, int(limit))
    goals = list(dict.fromkeys(candidate_goals or ["engagement", "instructional", "authority", "contrarian"]))
    selected_model = content_stack.select_best_stacked_model() if not stacked_model_name else None
    resolved_model_name = str(stacked_model_name or (selected_model or {}).get("model_name") or "").strip()
    warnings: list[str] = []
    fallback_mode: str | None = None

    try:
        baseline_local = score_draft(text=normalized, industry=industry, topics=normalized_topics, model=model, embed_fn=embed_fn)
        maximized = maximize_draft(
            text=normalized,
            industry=industry,
            topics=normalized_topics,
            model=model,
            candidate_goals=goals,
            embed_fn=embed_fn,
        )
        raw_variants: list[dict[str, Any]] = [
            {
                "goal": "original",
                "source": "original",
                "text": normalized,
                "rewrite_notes": [],
            }
        ]
        for item in list(maximized.get("variants") or []):
            rewritten = _normalized_post_text(str(item.get("rewritten") or ""))
            if not rewritten:
                continue
            raw_variants.append(
                {
                    "goal": str(item.get("goal") or "rewrite"),
                    "source": "rewrite",
                    "text": rewritten,
                    "rewrite_notes": list(item.get("rewrite_notes") or []),
                }
            )
        if fresh:
            created = create_drafts(
                prompt=normalized,
                industry=industry,
                topics=normalized_topics,
                model=model,
                candidate_goals=goals,
                candidate_count=max(limit, len(goals)),
                generator="auto",
                embed_fn=embed_fn,
            )
            for item in list(created.get("candidates") or []):
                fresh_text = _normalized_post_text(str(item.get("text") or ""))
                if not fresh_text:
                    continue
                if long_form:
                    fresh_text = _expand_long_form_candidate(
                        text=fresh_text,
                        prompt=normalized,
                        goal=str(item.get("goal") or "engagement"),
                        topics=normalized_topics,
                    )
                raw_variants.append(
                    {
                        "goal": str(item.get("goal") or "fresh"),
                        "source": "fresh",
                        "text": fresh_text,
                        "rewrite_notes": [
                            "Fresh candidate generated from the prompt and playbook.",
                            "Expanded into a longer-form version." if long_form else "Kept in compact form.",
                        ],
                    }
                )
    except sqlite3.DatabaseError as exc:
        baseline_local = None
        fallback_mode = "stacked_only"
        warnings.append(f"Local corpus scoring unavailable: {exc}")
        raw_variants = [{"goal": "original", "source": "original", "text": normalized, "rewrite_notes": []}]
        for goal in goals:
            raw_variants.append(
                {
                    "goal": goal,
                    "source": "rewrite",
                    "text": _heuristic_rewrite_without_corpus(text=normalized, goal=goal, topics=normalized_topics),
                    "rewrite_notes": [
                        "Corpus DB unavailable, used heuristic rewrite fallback.",
                        "Stacked scoring remains active.",
                    ],
                }
            )

    baseline_stacked = content_stack.score_text_for_target(
        text=normalized,
        industry=industry,
        topics=normalized_topics,
        target_profile=target_profile,
        model_name=resolved_model_name,
        auto_calibrate_weights=auto_calibrate_weights,
    )

    seen_texts: set[str] = set()
    ranked_variants: list[dict[str, Any]] = []
    for candidate in raw_variants:
        variant_text = str(candidate.get("text") or "")
        if not variant_text or variant_text in seen_texts:
            continue
        seen_texts.add(variant_text)
        if baseline_local is None:
            local_score = None
        else:
            local_score = baseline_local if candidate["source"] == "original" else score_draft(
                text=variant_text,
                industry=industry,
                topics=normalized_topics,
                model=model,
                embed_fn=embed_fn,
            )
        stacked_score = content_stack.score_text_for_target(
            text=variant_text,
            industry=industry,
            topics=normalized_topics,
            target_profile=target_profile,
            model_name=resolved_model_name,
            auto_calibrate_weights=auto_calibrate_weights,
        )
        ranked_variants.append(
            {
                "goal": candidate["goal"],
                "source": candidate["source"],
                "text": variant_text,
                "rewrite_notes": list(candidate.get("rewrite_notes") or []),
                "local_score": local_score,
                "stacked_score": stacked_score,
                "final_score": float(stacked_score.get("final_score") or 0.0),
            }
        )

    ranked_variants.sort(
        key=lambda item: (
            float(item.get("final_score") or 0.0),
            float(((item.get("local_score") or {}).get("predicted_outcome_score") or 0.0)),
            str(item.get("text") or ""),
        ),
        reverse=True,
    )
    for index, item in enumerate(ranked_variants, start=1):
        item["rank"] = index
    limited = ranked_variants[:limit]
    return {
        "text": normalized,
        "industry": industry,
        "topics": normalized_topics,
        "model_name": resolved_model_name,
        "selected_model": selected_model,
        "target_profile": dict(target_profile or {}),
        "auto_calibrate_weights": bool(auto_calibrate_weights),
        "fresh": bool(fresh),
        "long_form": bool(long_form),
        "fallback_mode": fallback_mode,
        "warnings": warnings,
        "baseline": {
            "local_score": baseline_local,
            "stacked_score": baseline_stacked,
        },
        "variants": limited,
        "best_variant": limited[0] if limited else None,
        "count": len(limited),
    }


def sync_post_outcomes(
    *,
    urls: list[str],
    owned_by_me: bool = False,
    fetch_html: Callable[[str], str] | None = None,
    progress: Callable[[dict[str, Any]], None] | None = None,
) -> dict[str, Any]:
    init_content_db()
    fetch_html = fetch_html or _default_fetch_html
    synced: list[dict[str, Any]] = []
    now = store._now_iso()
    conn = store._connect()
    try:
        for index, url in enumerate(urls, start=1):
            if progress:
                progress({"event": "outcome_sync_started", "url": url, "index": index, "count": len(urls)})
            html = fetch_html(url)
            record = extract_post_record(url=url, html=html, source_query=None)
            record["owned_by_me"] = owned_by_me
            record["last_synced_at"] = now
            stored = _upsert_post_conn(conn, record, now=now)
            outcome_score = _engagement_score(
                int(stored.get("reaction_count") or 0),
                int(stored.get("comment_count") or 0),
                int(stored.get("word_count") or 0),
            )
            conn.execute(
                """
                UPDATE harvested_posts
                SET owned_by_me = CASE WHEN ? = 1 THEN 1 ELSE owned_by_me END,
                    outcome_score = ?,
                    last_synced_at = ?
                WHERE url = ?
                """,
                (1 if owned_by_me else 0, outcome_score, now, url),
            )
            conn.execute(
                """
                INSERT INTO content_outcome_snapshots
                (url, synced_at, reaction_count, comment_count, outcome_score, owned_by_me, metadata_json)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    url,
                    now,
                    int(stored.get("reaction_count") or 0),
                    int(stored.get("comment_count") or 0),
                    outcome_score,
                    1 if owned_by_me else 0,
                    json.dumps({"source": "sync_post_outcomes"}, ensure_ascii=False, sort_keys=True),
                ),
            )
            synced.append(
                {
                    "url": url,
                    "reaction_count": stored.get("reaction_count"),
                    "comment_count": stored.get("comment_count"),
                    "outcome_score": outcome_score,
                    "owned_by_me": bool(owned_by_me or stored.get("owned_by_me")),
                }
            )
            if progress:
                progress({"event": "outcome_synced", **synced[-1]})
        conn.commit()
    finally:
        conn.close()
    return {"synced_count": len(synced), "posts": synced}


def sync_owned_post_telemetry(
    *,
    urls: list[str],
    fetch_html: Callable[[str], str] | None = None,
    progress: Callable[[dict[str, Any]], None] | None = None,
) -> dict[str, Any]:
    init_content_db()
    fetch_html = fetch_html or _default_fetch_html
    now = store._now_iso()
    snapshots: list[dict[str, Any]] = []
    events_written = 0
    conn = store._connect()
    try:
        for index, url in enumerate(urls, start=1):
            if progress:
                progress({"event": "telemetry_sync_started", "url": url, "index": index, "count": len(urls)})
            html = fetch_html(url)
            record = extract_post_record(url=url, html=html, source_query=None)
            record["owned_by_me"] = True
            record["last_synced_at"] = now
            stored = _upsert_post_conn(conn, record, now=now)
            payload = {
                "reaction_count": int(stored.get("reaction_count") or 0),
                "comment_count": int(stored.get("comment_count") or 0),
                "repost_count": int(stored.get("repost_count") or 0),
                "outcome_score": float(stored.get("outcome_score") or 0.0),
            }
            conn.execute(
                """
                INSERT INTO owned_post_snapshots
                (url, captured_at, reaction_count, comment_count, repost_count, payload_json)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    url,
                    now,
                    payload["reaction_count"],
                    payload["comment_count"],
                    payload["repost_count"],
                    json.dumps(payload, ensure_ascii=False, sort_keys=True),
                ),
            )
            conn.commit()
            telemetry_event = store.append_telemetry_event(
                entity_kind="post",
                entity_key=url,
                event_type="reaction_snapshot",
                dedupe_key=f"reaction-snapshot:{url}:{now}",
                payload=payload,
                source="content.sync_owned_post_telemetry",
                event_time=now,
            )
            try:
                from linkedin_cli import traces

                traces.attribute_telemetry_event(
                    entity_kind="post",
                    entity_key=url,
                    event_type="reaction_snapshot",
                    event_time=now,
                    payload=payload,
                    source="content.sync_owned_post_telemetry",
                    source_event_id=telemetry_event.get("id"),
                )
            except Exception:
                # Reward attribution should not block telemetry capture.
                pass
            events_written += 1
            snapshot = {"url": url, "captured_at": now, **payload}
            snapshots.append(snapshot)
            if progress:
                progress({"event": "telemetry_synced", **snapshot})
        conn.commit()
    finally:
        conn.close()
    return {
        "synced_count": len(snapshots),
        "events_written": events_written,
        "snapshots": snapshots,
    }


def build_harvest_queries(
    industry: str | None = None,
    topics: list[str] | None = None,
    industries: list[str] | None = None,
    expansion: str = "standard",
    freshness_buckets: list[str] | None = None,
) -> list[str]:
    topics = [clean_text(topic) for topic in (topics or []) if clean_text(topic)]
    normalized_industries = [clean_text(value) for value in (industries or []) if clean_text(value)]
    if industry and clean_text(industry):
        normalized_industries.append(clean_text(industry))
    normalized_industries = list(dict.fromkeys(normalized_industries))
    recursive_mode = expansion == "recursive"
    effective_expansion = "exhaustive" if recursive_mode else expansion
    combined_terms: list[str] = []
    if normalized_industries and topics:
        combined_terms.extend([f"{industry_value} {topic}" for industry_value in normalized_industries for topic in topics])
    base_terms: list[str] = list(combined_terms)
    expanded_pair_terms: list[str] = []
    if effective_expansion in {"broad", "exhaustive"}:
        modifiers = [
            "case study",
            "benchmark",
            "playbook",
            "implementation",
            "lessons",
            "template",
            "example",
            "startup",
            "enterprise",
            "founder",
            "operator",
            "2026",
        ]
        if effective_expansion == "exhaustive":
            modifiers.extend(
                [
                    "customer story",
                    "roi",
                    "gtm",
                    "open source",
                    "security",
                    "compliance",
                    "architecture",
                    "stack",
                    "demo",
                    "best practices",
                    "hiring",
                    "launch",
                    "series a",
                    "seed",
                ]
            )
        seeds = combined_terms or base_terms
        for term in seeds:
            if not term:
                continue
            expanded_pair_terms.extend(f"{term} {modifier}" for modifier in modifiers)
        if effective_expansion == "exhaustive":
            for industry_value in normalized_industries:
                for topic in topics:
                    expanded_pair_terms.extend(
                        [
                            f"{topic} {industry_value}",
                            f"{topic} {industry_value} case study",
                            f"{topic} {industry_value} benchmark",
                            f"{topic} {industry_value} playbook",
                            f"{topic} {industry_value} roi",
                        ]
                    )
    base_terms.extend(expanded_pair_terms)
    base_terms.extend(normalized_industries)
    base_terms.extend(topics)
    freshness_terms: list[str] = []
    for term in combined_terms or base_terms:
        if not term:
            continue
        freshness_terms.extend(f"{term} {modifier}" for modifier in _freshness_modifiers(freshness_buckets))
    base_terms.extend(freshness_terms)
    recursive_terms: list[tuple[str, bool]] = []
    recursive_entity_terms: list[tuple[str, str]] = []
    if recursive_mode:
        for term in combined_terms or base_terms:
            cleaned = clean_text(term)
            if cleaned:
                recursive_terms.append((cleaned, False))
        for industry_value in normalized_industries:
            for topic in topics:
                recursive_terms.append((f"{topic} {industry_value}", False))
        for entity in _recursive_harvest_entities(normalized_industries, topics):
            label = clean_text(str(entity.get("label") or ""))
            if not label:
                continue
            if entity.get("kind") == "company":
                for industry_value in normalized_industries or topics:
                    recursive_entity_terms.append((label, industry_value))
            for topic in topics or normalized_industries:
                recursive_entity_terms.append((label, topic))
    queries: list[str] = []

    def append_term_queries(target: list[str], term: str, *, quoted: bool, surface: str) -> None:
        if not term:
            return
        if quoted:
            target.append(f'{surface} "{term}"')
        else:
            target.append(f"{surface} {term}")

    def append_entity_queries(target: list[str], label: str, context: str, *, surface: str) -> None:
        if not label or not context:
            return
        target.append(f'{surface} "{label}" {context}')

    if recursive_mode:
        priority_post_queries: list[str] = []
        baseline_post_queries: list[str] = []
        priority_feed_queries: list[str] = []
        baseline_feed_queries: list[str] = []
        for term, quoted in recursive_terms:
            append_term_queries(priority_post_queries, term, quoted=quoted, surface="site:linkedin.com/posts")
            append_term_queries(priority_feed_queries, term, quoted=quoted, surface="site:linkedin.com/feed/update")
        for label, context in recursive_entity_terms:
            append_entity_queries(priority_post_queries, label, context, surface="site:linkedin.com/posts")
            append_entity_queries(priority_feed_queries, label, context, surface="site:linkedin.com/feed/update")
        for term in base_terms:
            append_term_queries(baseline_post_queries, term, quoted=True, surface="site:linkedin.com/posts")
            append_term_queries(baseline_feed_queries, term, quoted=True, surface="site:linkedin.com/feed/update")
        queries.extend(priority_post_queries)
        queries.extend(baseline_post_queries)
        queries.extend(priority_feed_queries)
        queries.extend(baseline_feed_queries)
    else:
        for term in base_terms:
            append_term_queries(queries, term, quoted=True, surface="site:linkedin.com/posts")
            append_term_queries(queries, term, quoted=True, surface="site:linkedin.com/feed/update")
    deduped: list[str] = []
    seen: set[str] = set()
    for query in queries:
        if query not in seen:
            seen.add(query)
            deduped.append(query)
    return deduped


def _recursive_harvest_entities(
    industries: list[str] | None,
    topics: list[str] | None,
    limit: int = 12,
) -> list[dict[str, str]]:
    init_content_db()
    normalized_industries = [clean_text(value).lower() for value in (industries or []) if clean_text(value)]
    topic_tokens = {token for topic in (topics or []) for token in _retrieval_tokens(str(topic or ""))}
    conn = store._connect()
    try:
        rows = conn.execute(
            """
            SELECT p.author_name, p.author_url, p.source_query, p.title, p.text, p.outcome_score, p.reaction_count, p.comment_count
            FROM harvested_posts AS p
            ORDER BY
                COALESCE(p.outcome_score, 0) DESC,
                COALESCE(p.reaction_count, 0) DESC,
                COALESCE(p.comment_count, 0) DESC,
                p.updated_at DESC
            LIMIT 1500
            """
        ).fetchall()
    finally:
        conn.close()
    entities: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for row in rows:
        payload = _row_to_dict(row)
        author_name = clean_text(str(payload.get("author_name") or ""))
        if not author_name or len(author_name) < 3:
            continue
        combined_text = " ".join(
            [
                str(payload.get("source_query") or ""),
                str(payload.get("title") or ""),
                str(payload.get("text") or "")[:400],
            ]
        ).lower()
        if normalized_industries and not any(industry in combined_text for industry in normalized_industries):
            continue
        if topic_tokens and not (topic_tokens & _retrieval_tokens(combined_text)):
            continue
        author_url = str(payload.get("author_url") or "")
        kind = "company" if "/company/" in author_url else "author"
        key = (kind, author_name.lower())
        if key in seen:
            continue
        seen.add(key)
        entities.append({"label": author_name, "kind": kind})
        if len(entities) >= max(1, int(limit)):
            break
    return entities


def _freshness_modifiers(freshness_buckets: list[str] | None = None) -> list[str]:
    if not freshness_buckets:
        return []
    now = datetime.now(timezone.utc)
    quarter = ((now.month - 1) // 3) + 1
    modifiers: list[str] = []
    for bucket in freshness_buckets:
        normalized = clean_text(bucket).lower()
        if normalized == "recent":
            modifiers.extend(["today", "this week"])
        elif normalized == "month":
            modifiers.append("this month")
        elif normalized == "quarter":
            modifiers.extend(["this quarter", f"q{quarter} {now.year}"])
        elif normalized == "year":
            modifiers.append(str(now.year))
    return list(dict.fromkeys(modifiers))


def _query_keyword_term(query: str) -> str:
    return clean_text(
        str(query or "")
        .replace('"', "")
        .replace("site:linkedin.com/posts", "")
        .replace("site:linkedin.com/feed/update", "")
    ) or ""


def prepare_backend_queries(queries: list[str], backend: str) -> list[str]:
    if backend != "auth-only":
        return list(queries)
    deduped: list[str] = []
    seen: set[str] = set()
    for query in queries:
        keyword = _query_keyword_term(query)
        if not keyword or keyword in seen:
            continue
        seen.add(keyword)
        deduped.append(keyword)
    return deduped


def query_yield_stats(job_prefix: str | None = None, limit: int = 50) -> list[dict[str, Any]]:
    init_content_db()
    conn = store._connect()
    try:
        where_clause = ""
        params: list[Any] = []
        if job_prefix:
            where_clause = "WHERE jq.job_id LIKE ?"
            params.append(f"{job_prefix}%")
        rows = conn.execute(
            f"""
            SELECT
                jq.query AS query,
                COUNT(*) AS attempt_count,
                SUM(CASE WHEN jq.status = 'completed' THEN 1 ELSE 0 END) AS completed_count,
                SUM(CASE WHEN jq.status = 'failed' THEN 1 ELSE 0 END) AS failed_count,
                SUM(
                    CASE
                        WHEN jq.result_count > 0 AND jq.stored_count > jq.result_count THEN jq.result_count
                        ELSE jq.stored_count
                    END
                ) AS stored_count,
                SUM(jq.result_count) AS result_count,
                MAX(jq.updated_at) AS last_updated_at
            FROM content_harvest_job_queries jq
            {where_clause}
            GROUP BY jq.query
            ORDER BY stored_count DESC, result_count DESC, last_updated_at DESC
            LIMIT ?
            """,
            (*params, max(1, int(limit))),
        ).fetchall()
        stats: list[dict[str, Any]] = []
        for row in rows:
            payload = dict(row)
            result_count = int(payload.get("result_count") or 0)
            stored_count = int(payload.get("stored_count") or 0)
            payload["yield_rate"] = round((stored_count / result_count), 6) if result_count else 0.0
            stats.append(payload)
        return stats
    finally:
        conn.close()


def _query_yield_stats_map(job_prefix: str | None = None) -> dict[str, dict[str, Any]]:
    stats = query_yield_stats(job_prefix=job_prefix, limit=100000)
    return {str(item["query"]): item for item in stats}


def _prioritize_campaign_queries(queries: list[str], *, job_prefix: str | None = None) -> list[str]:
    stats_map = _query_yield_stats_map(job_prefix=job_prefix)

    def sort_key(query: str) -> tuple[int, float, int, str]:
        stats = stats_map.get(query)
        if not stats:
            return (1, 0.0, 0, query)
        stored_count = int(stats.get("stored_count") or 0)
        failed_count = int(stats.get("failed_count") or 0)
        yield_rate = float(stats.get("yield_rate") or 0.0)
        if stored_count > 0:
            tier = 0
        elif failed_count > 0:
            tier = 2
        else:
            tier = 1
        return (tier, -yield_rate, -stored_count, query)

    return sorted(list(queries), key=sort_key)


def _chunk_queries(queries: list[str], chunk_size: int) -> list[list[str]]:
    size = max(1, int(chunk_size))
    return [queries[index : index + size] for index in range(0, len(queries), size)]


def _default_fetch_html(url: str) -> str:
    session = build_session()
    response = request(session, "GET", url)
    return response.text


def _canonical_post_url(url: str | None) -> str | None:
    if not url:
        return None
    parts = urlsplit(url)
    if not parts.scheme or not parts.netloc:
        return None
    return urlunsplit((parts.scheme, parts.netloc, parts.path, "", ""))


def _text_value(value: Any) -> str:
    if isinstance(value, str):
        return clean_text(value) or ""
    if isinstance(value, dict):
        for key in ("text", "accessibilityText"):
            extracted = _text_value(value.get(key))
            if extracted:
                return extracted
    return ""


def _auth_activity_counts(update: dict[str, Any], included_by_urn: dict[str, dict[str, Any]]) -> tuple[int, int]:
    social_detail_urn = update.get("*socialDetail")
    social_detail = included_by_urn.get(social_detail_urn) if social_detail_urn else None
    if not isinstance(social_detail, dict):
        return 0, 0
    counts_urn = social_detail.get("*totalSocialActivityCounts")
    counts = included_by_urn.get(counts_urn) if counts_urn else None
    if not isinstance(counts, dict):
        return 0, 0
    return int(counts.get("numLikes") or 0), int(counts.get("numComments") or 0)


def _auth_result_to_record(
    result: dict[str, Any],
    *,
    industry: str | None = None,
    industries: list[str] | None = None,
    query_topics: list[str] | None = None,
    source_query: str,
) -> dict[str, Any] | None:
    if result.get("source") != "linkedin.auth":
        return None
    url = _canonical_post_url(result.get("url"))
    text = _normalized_post_text(result.get("text"))
    title = clean_text(result.get("title") or "") or _sentence_hook(text)
    if not url or (not text and not title):
        return None
    return {
        "url": url,
        "industry": industry,
        "industries": _normalize_industries(industries, industry),
        "source_query": source_query,
        "title": title or url,
        "author_name": clean_text(result.get("author_name") or "") or None,
        "author_url": _canonical_post_url(result.get("author_url")),
        "published_at": result.get("published_at"),
        "text": text,
        "hook": _sentence_hook(text or title or url),
        "structure": _classify_structure(text or title or ""),
        "word_count": len((text or title or "").split()),
        "reaction_count": int(result.get("reaction_count") or 0),
        "comment_count": int(result.get("comment_count") or 0),
        "metadata": {
            "source": result.get("source") or "linkedin.auth",
            "backend_urn": result.get("backend_urn"),
            "share_audience": result.get("share_audience"),
            "query_industries": _normalize_industries(industries, industry),
            "query_topics": _normalize_topics(query_topics),
        },
    }


def parse_authenticated_search_results(payload: dict[str, Any]) -> list[dict[str, str]]:
    cluster_data = (((payload.get("data") or {}).get("data") or {}).get("searchDashClustersByAll") or {})
    included = payload.get("included") or []
    included_by_urn = {
        item.get("entityUrn"): item
        for item in included
        if isinstance(item, dict) and item.get("entityUrn")
    }
    results: list[dict[str, str]] = []
    seen_urls: set[str] = set()
    for element in cluster_data.get("elements") or []:
        if not isinstance(element, dict):
            continue
        for entry in element.get("items") or []:
            if not isinstance(entry, dict):
                continue
            search_feed_update = ((entry.get("item") or {}).get("searchFeedUpdate") or {})
            update_urn = search_feed_update.get("*update")
            update = included_by_urn.get(update_urn) if update_urn else None
            if not isinstance(update, dict):
                continue
            share_url = _canonical_post_url(((update.get("socialContent") or {}).get("shareUrl")))
            if not share_url or share_url in seen_urls:
                continue
            seen_urls.add(share_url)
            actor = update.get("actor") or {}
            commentary = _text_value(update.get("commentary"))
            actor_name = _text_value(actor.get("name"))
            actor_url = _canonical_post_url(((actor.get("navigationContext") or {}).get("actionTarget")))
            reaction_count, comment_count = _auth_activity_counts(update, included_by_urn)
            title = _sentence_hook(commentary) or actor_name or share_url
            results.append(
                {
                    "title": title,
                    "url": share_url,
                    "snippet": clean_text((((update.get("content") or {}).get("pollComponent") or {}).get("question") or {}).get("text")) or commentary,
                    "source": "linkedin.auth",
                    "text": commentary,
                    "author_name": actor_name,
                    "author_url": actor_url,
                    "published_at": _text_value(actor.get("subDescription")) or None,
                    "reaction_count": reaction_count,
                    "comment_count": comment_count,
                    "backend_urn": ((update.get("metadata") or {}).get("backendUrn")),
                    "share_audience": ((update.get("metadata") or {}).get("shareAudience")),
                }
            )
    return results


def authenticated_post_search(query: str, limit: int = 10, start: int = 0) -> list[dict[str, str]]:
    session, _ = load_session(required=True)
    assert session is not None
    count = max(1, min(int(limit), 50))
    keywords = clean_text(
        query.replace('"', "").replace("site:linkedin.com/posts", "").replace("site:linkedin.com/feed/update", "")
    )
    if not keywords:
        return []
    quoted_keywords = quote(keywords, safe="")
    path = (
        "/voyager/api/graphql?includeWebMetadata=true&variables="
        f"(start:{max(0, int(start))},origin:OTHER,query:(keywords:{quoted_keywords},flagshipSearchIntent:SEARCH_SRP,"
        "queryParameters:List((key:resultType,value:List(CONTENT))),includeFiltersInResponse:false),"
        f"count:{count})&queryId=voyagerSearchDashClusters.05111e1b90ee7fea15bebe9f9410ced9"
    )
    response = voyager_get(session, path)
    payload = parse_json_response(response)
    return parse_authenticated_search_results(payload)[:count]


def _fetch_auth_results(
    query: str,
    per_query: int,
    auth_search_fn: Callable[..., list[dict[str, str]]],
    progress: Callable[[dict[str, Any]], None] | None = None,
) -> list[dict[str, str]]:
    collected: list[dict[str, str]] = []
    start = 0
    while len(collected) < per_query:
        page_size = min(per_query - len(collected), 50)
        try:
            page = auth_search_fn(query, limit=page_size, start=start)
        except TypeError:
            page = auth_search_fn(query, limit=per_query)
            collected.extend(page)
            break
        if not page:
            break
        collected.extend(page)
        start += len(page)
        if progress:
            progress({"event": "query_page", "query": query, "backend": "linkedin.auth", "start": start, "page_count": len(page)})
        if len(page) == 0:
            break
    return collected[:per_query]


def _local_hash_embed_fn(texts: list[str], _model: str, dimensions: int = 256) -> list[list[float]]:
    embeddings: list[list[float]] = []
    for text in texts:
        vector = [0.0] * dimensions
        tokens = re.findall(r"[a-z0-9_#@]+", (text or "").lower())
        for token in tokens:
            digest = hashlib.sha256(token.encode("utf-8")).digest()
            index = int.from_bytes(digest[:2], "big") % dimensions
            sign = 1.0 if digest[2] % 2 == 0 else -1.0
            weight = 1.0 + min(len(token), 24) / 24.0
            vector[index] += sign * weight
        norm = math.sqrt(sum(value * value for value in vector))
        if norm:
            vector = [round(value / norm, 6) for value in vector]
        embeddings.append(vector)
    return embeddings


def _is_retryable_harvest_error(exc: Exception) -> bool:
    lowered = str(exc).lower()
    return any(token in lowered for token in ("timeout", "tempor", "rate", "429", "503", "connection", "locked"))


def _emit_progress(progress: Callable[[dict[str, Any]], None] | None, event: dict[str, Any]) -> None:
    if progress:
        progress(event)


def _sleep_interval(base_seconds: float, jitter_seconds: float) -> None:
    if base_seconds <= 0 and jitter_seconds <= 0:
        return
    delay = max(0.0, float(base_seconds))
    if jitter_seconds > 0:
        delay += random.uniform(0.0, float(jitter_seconds))
    if delay > 0:
        time.sleep(delay)


def _harvest_job_to_dict(row: Any) -> dict[str, Any]:
    payload = dict(row)
    payload["industries"] = json.loads(payload.pop("industries_json", "[]") or "[]")
    payload["queries"] = json.loads(payload.pop("queries_json", "[]") or "[]")
    payload["metadata"] = json.loads(payload.pop("metadata_json", "{}") or "{}")
    return payload


def _load_harvest_job(conn: Any, job_id: str) -> dict[str, Any] | None:
    row = conn.execute("SELECT * FROM content_harvest_jobs WHERE job_id = ?", (job_id,)).fetchone()
    return _harvest_job_to_dict(row) if row else None


def _create_or_reset_harvest_job(
    conn: Any,
    *,
    job_id: str,
    queries: list[str],
    industries: list[str],
    limit: int,
    per_query: int,
    search_timeout: int | None,
    fetch_workers: int,
    query_workers: int,
) -> dict[str, Any]:
    now = store._now_iso()
    conn.execute("DELETE FROM content_harvest_job_queries WHERE job_id = ?", (job_id,))
    conn.execute(
        """
        INSERT INTO content_harvest_jobs
        (job_id, created_at, updated_at, status, limit_value, per_query, search_timeout, fetch_workers, query_workers,
         stored_count, unique_url_count, industries_json, queries_json, metadata_json)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 0, ?, ?, ?)
        ON CONFLICT(job_id) DO UPDATE SET
            updated_at = excluded.updated_at,
            status = excluded.status,
            limit_value = excluded.limit_value,
            per_query = excluded.per_query,
            search_timeout = excluded.search_timeout,
            fetch_workers = excluded.fetch_workers,
            query_workers = excluded.query_workers,
            stored_count = 0,
            unique_url_count = 0,
            industries_json = excluded.industries_json,
            queries_json = excluded.queries_json,
            metadata_json = excluded.metadata_json
        """,
        (
            job_id,
            now,
            now,
            HARVEST_JOB_STATUS_PENDING,
            limit,
            per_query,
            search_timeout,
            fetch_workers,
            query_workers,
            json.dumps(industries, ensure_ascii=False),
            json.dumps(queries, ensure_ascii=False),
            json.dumps({"seen_urls": []}, ensure_ascii=False, sort_keys=True),
        ),
    )
    for query_index, query in enumerate(queries, start=1):
        conn.execute(
            """
            INSERT INTO content_harvest_job_queries
            (job_id, query_index, query, status, stored_count, result_count, last_error, updated_at)
            VALUES (?, ?, ?, ?, 0, 0, NULL, ?)
            """,
            (job_id, query_index, query, HARVEST_JOB_STATUS_PENDING, now),
        )
    return _load_harvest_job(conn, job_id) or {}


def _job_query_rows(conn: Any, job_id: str) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT * FROM content_harvest_job_queries
        WHERE job_id = ?
        ORDER BY query_index ASC
        """,
        (job_id,),
    ).fetchall()
    return [dict(row) for row in rows]


def _job_seen_urls(job: dict[str, Any]) -> list[str]:
    metadata = job.get("metadata") or {}
    values = metadata.get("seen_urls") if isinstance(metadata, dict) else []
    return [str(value) for value in (values or []) if value]


def _update_harvest_job(
    conn: Any,
    job_id: str,
    *,
    status: str | None = None,
    stored_count: int | None = None,
    unique_url_count: int | None = None,
    seen_urls: list[str] | None = None,
) -> dict[str, Any]:
    job = _load_harvest_job(conn, job_id)
    if not job:
        raise ValueError(f"Unknown harvest job: {job_id}")
    metadata = dict(job.get("metadata") or {})
    if seen_urls is not None:
        metadata["seen_urls"] = list(dict.fromkeys(seen_urls))
    now = store._now_iso()
    conn.execute(
        """
        UPDATE content_harvest_jobs
        SET updated_at = ?,
            status = COALESCE(?, status),
            stored_count = COALESCE(?, stored_count),
            unique_url_count = COALESCE(?, unique_url_count),
            metadata_json = ?
        WHERE job_id = ?
        """,
        (
            now,
            status,
            stored_count,
            unique_url_count,
            json.dumps(metadata, ensure_ascii=False, sort_keys=True),
            job_id,
        ),
    )
    return _load_harvest_job(conn, job_id) or {}


def _update_harvest_query(
    conn: Any,
    job_id: str,
    query_index: int,
    *,
    status: str,
    stored_count: int | None = None,
    result_count: int | None = None,
    last_error: str | None = None,
) -> None:
    conn.execute(
        """
        UPDATE content_harvest_job_queries
        SET status = ?,
            stored_count = COALESCE(?, stored_count),
            result_count = COALESCE(?, result_count),
            last_error = ?,
            updated_at = ?
        WHERE job_id = ? AND query_index = ?
        """,
        (status, stored_count, result_count, last_error, store._now_iso(), job_id, query_index),
    )


def _iso_to_datetime(value: str | None) -> datetime | None:
    normalized = str(value or "").strip()
    if not normalized:
        return None
    try:
        if normalized.endswith("Z"):
            normalized = normalized[:-1] + "+00:00"
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _is_stale_timestamp(value: str | None, stale_after_seconds: float) -> bool:
    parsed = _iso_to_datetime(value)
    if parsed is None:
        return False
    age_seconds = (datetime.now(timezone.utc) - parsed).total_seconds()
    return age_seconds >= max(1.0, float(stale_after_seconds))


def _heal_stale_harvest_queries(conn: Any, job_id: str, *, stale_after_seconds: float = _HARVEST_STALE_QUERY_SECONDS) -> list[int]:
    healed_indexes: list[int] = []
    for row in _job_query_rows(conn, job_id):
        if row.get("status") != HARVEST_JOB_STATUS_RUNNING:
            continue
        if not _is_stale_timestamp(row.get("updated_at"), stale_after_seconds):
            continue
        healed_indexes.append(int(row["query_index"]))
        _update_harvest_query(
            conn,
            job_id,
            int(row["query_index"]),
            status=HARVEST_JOB_STATUS_FAILED,
            last_error="stale running query reset for resume",
        )
    return healed_indexes


def _concurrent_phase_timeout(task_count: int, worker_count: int, per_task_timeout: float) -> float:
    if task_count <= 0:
        return max(0.001, float(per_task_timeout))
    workers = max(1, int(worker_count))
    per_task = max(0.001, float(per_task_timeout))
    waves = math.ceil(task_count / workers)
    return per_task * (waves + 1)


def _wait_for_futures(
    futures: list[Any],
    *,
    worker_count: int,
    per_task_timeout: float,
) -> tuple[list[Any], set[Any]]:
    pending = set(futures)
    completed: list[Any] = []
    deadline = time.monotonic() + _concurrent_phase_timeout(len(futures), worker_count, per_task_timeout)
    while pending:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        done, pending = wait(pending, timeout=remaining, return_when=FIRST_COMPLETED)
        if not done:
            break
        completed.extend(done)
    return completed, pending


def _chunk_parallel_items(items: list[Any], worker_count: int) -> list[list[Any]]:
    if not items:
        return []
    size = max(1, int(worker_count)) * 2
    return [items[index : index + size] for index in range(0, len(items), size)]


def _fastembed_embed_fn(texts: list[str], model: str) -> list[list[float]]:
    try:
        from fastembed import TextEmbedding
    except ImportError as exc:
        fail(
            f"fastembed is required for model `{model}`. Install it with `pip install linkedin-discovery-cli[semantic]` or use --model local-hash-v1.",
            code=ExitCode.VALIDATION,
        )
        raise exc

    model_name = model.split(":", 1)[1] if ":" in model else model
    embedder = _cached_fastembed_model(model_name)
    return [vector.astype(float).tolist() for vector in embedder.embed(texts)]


@lru_cache(maxsize=4)
def _cached_fastembed_model(model_name: str) -> Any:
    from fastembed import TextEmbedding

    return TextEmbedding(model_name=model_name)


def _default_embed_fn(texts: list[str], model: str) -> list[list[float]]:
    if model.startswith("fastembed:"):
        return _fastembed_embed_fn(texts, model)
    if model.startswith("local-hash"):
        return _local_hash_embed_fn(texts, model)
    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    if not api_key:
        fail(
            "OPENAI_API_KEY is required for remote embeddings. Use --model local-hash-v1 for local embeddings.",
            code=ExitCode.VALIDATION,
        )
    base_url = os.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1").rstrip("/")
    response = requests.post(
        f"{base_url}/embeddings",
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        json={"model": model, "input": texts},
        timeout=60,
    )
    if response.status_code >= 400:
        fail(f"Embedding request failed with HTTP {response.status_code}: {response.text[:400]}", code=ExitCode.RETRYABLE)
    payload = response.json()
    return [item.get("embedding") or [] for item in payload.get("data") or []]


def embed_posts(
    *,
    limit: int = 100,
    model: str = "text-embedding-3-small",
    embed_fn: Callable[[list[str], str], list[list[float]]] | None = None,
    batch_size: int = 25,
    missing_only: bool = True,
    progress: Callable[[dict[str, Any]], None] | None = None,
) -> dict[str, Any]:
    init_content_db()
    embed_fn = embed_fn or _default_embed_fn
    limit = max(1, int(limit))
    conn = store._connect()
    try:
        where_sql = " WHERE embedding_json IS NULL OR embedding_json = ''" if missing_only else ""
        rows = conn.execute(
            f"SELECT * FROM harvested_posts{where_sql} ORDER BY updated_at DESC LIMIT ?",
            (limit,),
        ).fetchall()
        posts = [_row_to_dict(row) for row in rows]
        embedded_count = 0
        now = store._now_iso()
        for start in range(0, len(posts), max(1, int(batch_size))):
            batch = posts[start : start + max(1, int(batch_size))]
            if progress:
                progress(
                    {
                        "event": "embed_batch_started",
                        "batch_start": start,
                        "batch_size": len(batch),
                        "total": len(posts),
                        "model": model,
                    }
                )
            texts = [str(post.get("text") or post.get("hook") or post.get("title") or "") for post in batch]
            embeddings = embed_fn(texts, model)
            for post, embedding in zip(batch, embeddings):
                post["embedding_model"] = model
                post["embedding_dimensions"] = len(embedding)
                post["embedding"] = embedding
                conn.execute(
                    """
                    UPDATE harvested_posts
                    SET embedding_model = ?, embedding_dimensions = ?, embedding_json = ?, embedded_at = ?, updated_at = ?
                    WHERE url = ?
                    """,
                    (model, len(embedding), json.dumps(embedding), now, now, post["url"]),
                )
                _index_record_for_row(conn, post)
                embedded_count += 1
                if progress:
                    progress(
                        {
                            "event": "embed_post_stored",
                            "url": post["url"],
                            "embedded_count": embedded_count,
                            "total": len(posts),
                            "model": model,
                        }
                    )
            conn.commit()
        if progress:
            progress({"event": "embed_complete", "embedded_count": embedded_count, "total": len(posts), "model": model})
        return {
            "embedded_count": embedded_count,
            "model": model,
            "limit": limit,
        }
    finally:
        conn.close()


def _cosine_similarity(left: list[float] | None, right: list[float] | None) -> float:
    if not left or not right or len(left) != len(right):
        return 0.0
    numerator = sum(float(a) * float(b) for a, b in zip(left, right))
    left_norm = math.sqrt(sum(float(a) * float(a) for a in left))
    right_norm = math.sqrt(sum(float(b) * float(b) for b in right))
    if not left_norm or not right_norm:
        return 0.0
    return numerator / (left_norm * right_norm)


def _retrieval_tokens(text: str) -> set[str]:
    return {
        token
        for token in re.findall(r"[a-z0-9_#@%'-]+", (text or "").lower())
        if len(token) >= 3 and token not in {"the", "and", "for", "with", "that", "this"}
    }


def _lexical_overlap_score(query_tokens: set[str], post: dict[str, Any]) -> float:
    if not query_tokens:
        return 0.0
    post_tokens = _retrieval_tokens(
        " ".join(str(post.get(key) or "") for key in ("title", "hook", "text", "author_name"))
    )
    if not post_tokens:
        return 0.0
    return len(query_tokens & post_tokens) / len(query_tokens)


def retrieve_posts(
    *,
    query_text: str,
    limit: int = 10,
    method: str = "hybrid",
    model: str | None = None,
    industry: str | None = None,
    author: str | None = None,
    embed_fn: Callable[[list[str], str], list[list[float]]] | None = None,
    scan_limit: int = 5000,
    include_vectors: bool = False,
) -> list[dict[str, Any]]:
    init_content_db()
    limit = max(1, int(limit))
    posts = list_posts(limit=max(limit, scan_limit), industry=industry, author=author)
    if not posts:
        return []

    query_text = clean_text(query_text) or ""
    if not query_text:
        return []

    embed_fn = embed_fn or _default_embed_fn
    query_fingerprint = _compute_content_fingerprint(query_text, title=query_text)
    query_tokens = _retrieval_tokens(query_text)

    if model is None:
        model = next((str(post.get("embedding_model")) for post in posts if post.get("embedding_model")), None)
    query_embedding: list[float] | None = None
    if method in {"hybrid", "semantic"} and model:
        query_embedding = embed_fn([query_text], model)[0]

    shortlist_target = max(limit * ANN_SHORTLIST_MULTIPLIER, 200)
    candidate_urls: list[str] = []
    if method in {"hybrid", "semantic"} and query_embedding and model:
        candidate_urls.extend(
            _vector_index_candidates("semantic", model, query_embedding, min(shortlist_target, scan_limit))
        )
    if method in {"hybrid", "fingerprint"}:
        candidate_urls.extend(
            _vector_index_candidates("fingerprint", FINGERPRINT_VERSION, query_fingerprint, min(shortlist_target, scan_limit))
        )
    if not candidate_urls:
        candidate_urls = _ann_candidates(
            query_embedding=query_embedding if method in {"hybrid", "semantic"} else None,
            model=model if method in {"hybrid", "semantic"} else None,
            query_fingerprint=query_fingerprint if method in {"hybrid", "fingerprint"} else None,
            shortlist_size=min(shortlist_target, scan_limit),
        )
    if candidate_urls:
        deduped_candidate_urls = list(dict.fromkeys(candidate_urls))
        indexed_posts = _load_posts_by_urls(deduped_candidate_urls)
        indexed_posts = [post for post in indexed_posts if (not industry or post.get("industry") == industry)]
        if author:
            needle = author.lower()
            indexed_posts = [post for post in indexed_posts if needle in str(post.get("author_name") or "").lower()]
        if len(indexed_posts) >= limit:
            posts = indexed_posts
        else:
            seen_urls = {post.get("url") for post in indexed_posts}
            posts = indexed_posts + [post for post in posts if post.get("url") not in seen_urls]

    results: list[dict[str, Any]] = []
    for post in posts:
        semantic_score = 0.0
        if query_embedding and post.get("embedding_model") == model:
            semantic_score = _cosine_similarity(query_embedding, post.get("embedding"))
        fingerprint_score = _cosine_similarity(query_fingerprint, post.get("fingerprint"))
        lexical_score = _lexical_overlap_score(query_tokens, post)
        if method == "semantic":
            score = semantic_score
        elif method == "fingerprint":
            score = fingerprint_score
        elif method == "lexical":
            score = lexical_score
        else:
            score = (0.70 * semantic_score) + (0.20 * fingerprint_score) + (0.10 * lexical_score)
        item = dict(post)
        embedding_dim, fingerprint_dim = summarize_post_dimensions(item)
        item.update(
            {
                "score": round(score, 6),
                "semantic_score": round(semantic_score, 6),
                "fingerprint_score": round(fingerprint_score, 6),
                "lexical_score": round(lexical_score, 6),
                "embedding_dim": embedding_dim,
                "fingerprint_dim": fingerprint_dim,
            }
        )
        if not include_vectors:
            item.pop("embedding", None)
            item.pop("fingerprint", None)
        results.append(item)
    results.sort(key=lambda item: (item["score"], item.get("reaction_count") or 0, item.get("updated_at") or ""), reverse=True)
    return results[:limit]


def similar_posts(
    *,
    url: str,
    limit: int = 10,
    method: str = "hybrid",
    scan_limit: int = 5000,
    include_vectors: bool = False,
) -> list[dict[str, Any]]:
    init_content_db()
    all_posts = list_posts(limit=max(limit + 1, scan_limit))
    source = next((post for post in all_posts if post.get("url") == url), None)
    if source is None:
        fail(f"Stored post not found for URL `{url}`", code=ExitCode.VALIDATION)

    source_tokens = _retrieval_tokens(
        " ".join(str(source.get(key) or "") for key in ("title", "hook", "text", "author_name"))
    )
    candidate_urls: list[str] = []
    shortlist_target = min(max(limit * ANN_SHORTLIST_MULTIPLIER, 200), scan_limit)
    if method in {"hybrid", "semantic"} and source.get("embedding") and source.get("embedding_model"):
        candidate_urls.extend(
            _vector_index_candidates(
                "semantic",
                str(source.get("embedding_model") or ""),
                source.get("embedding"),
                shortlist_target,
            )
        )
    if method in {"hybrid", "fingerprint"} and source.get("fingerprint"):
        candidate_urls.extend(
            _vector_index_candidates("fingerprint", FINGERPRINT_VERSION, source.get("fingerprint"), shortlist_target)
        )
    if not candidate_urls:
        candidate_urls = _ann_candidates(
            query_embedding=source.get("embedding") if method in {"hybrid", "semantic"} else None,
            model=str(source.get("embedding_model") or "") if method in {"hybrid", "semantic"} else None,
            query_fingerprint=source.get("fingerprint") if method in {"hybrid", "fingerprint"} else None,
            shortlist_size=shortlist_target,
        )
    posts = _load_posts_by_urls(list(dict.fromkeys(candidate_urls))) if candidate_urls else all_posts
    if not posts:
        posts = all_posts
    results: list[dict[str, Any]] = []
    for post in posts:
        if post.get("url") == url:
            continue
        semantic_score = 0.0
        if source.get("embedding") and post.get("embedding") and post.get("embedding_model") == source.get("embedding_model"):
            semantic_score = _cosine_similarity(source.get("embedding"), post.get("embedding"))
        fingerprint_score = _cosine_similarity(source.get("fingerprint"), post.get("fingerprint"))
        lexical_score = _lexical_overlap_score(source_tokens, post)
        if method == "semantic":
            score = semantic_score
        elif method == "fingerprint":
            score = fingerprint_score
        elif method == "lexical":
            score = lexical_score
        else:
            score = (0.70 * semantic_score) + (0.20 * fingerprint_score) + (0.10 * lexical_score)
        item = dict(post)
        embedding_dim, fingerprint_dim = summarize_post_dimensions(item)
        item.update(
            {
                "score": round(score, 6),
                "semantic_score": round(semantic_score, 6),
                "fingerprint_score": round(fingerprint_score, 6),
                "lexical_score": round(lexical_score, 6),
                "embedding_dim": embedding_dim,
                "fingerprint_dim": fingerprint_dim,
            }
        )
        if not include_vectors:
            item.pop("embedding", None)
            item.pop("fingerprint", None)
        results.append(item)
    results.sort(key=lambda item: (item["score"], item.get("reaction_count") or 0, item.get("updated_at") or ""), reverse=True)
    return results[:limit]


def harvest_posts(
    *,
    industry: str | None = None,
    industries: list[str] | None = None,
    topics: list[str] | None = None,
    limit: int = 100,
    per_query: int = 25,
    query_terms: list[str] | None = None,
    search_fn: Callable[..., list[dict[str, str]]] = ddg_html_search,
    auth_search_fn: Callable[..., list[dict[str, str]]] | None = authenticated_post_search,
    fetch_html: Callable[[str], str] | None = None,
    search_timeout: int | None = None,
    fetch_timeout: float | None = None,
    progress: Callable[[dict[str, Any]], None] | None = None,
    fetch_workers: int = 6,
    query_workers: int = 1,
    job_name: str | None = None,
    resume_job: str | None = None,
    retry_budget: int = 2,
    cooldown_seconds: float = 0.0,
    min_request_interval: float = 0.0,
    jitter_seconds: float = 0.0,
    backend: str = "hybrid",
    public_search: str = "ddg",
    searxng_url: str | None = None,
    searxng_engines: list[str] | None = None,
) -> dict[str, Any]:
    init_content_db()
    from linkedin_cli import content_warehouse

    limit = max(1, int(limit))
    per_query = max(1, int(per_query))
    normalized_industries = _normalize_industries(industries, industry)
    normalized_topics = _normalize_topics(topics)
    queries = query_terms or build_harvest_queries(industries=normalized_industries, topics=normalized_topics)
    queries = prepare_backend_queries(queries, backend)
    if backend == "auth-only":
        query_workers = 1
    fetch_html = fetch_html or _default_fetch_html
    if search_fn is ddg_html_search:
        search_fn = resolve_public_search_fn(public_search, searxng_url=searxng_url, searxng_engines=searxng_engines)
    job_id = resume_job or job_name
    archive_job_id = job_id or f"adhoc-{int(time.time())}"
    job: dict[str, Any] | None = None
    query_rows: list[dict[str, Any]] = []
    resolved_search_timeout = max(0.001, float(search_timeout or DEFAULT_TIMEOUT))
    resolved_fetch_timeout = max(0.001, float(fetch_timeout if fetch_timeout is not None else resolved_search_timeout))
    conn = store._connect()
    try:
        if resume_job:
            job = _load_harvest_job(conn, resume_job)
            if not job:
                if queries:
                    job = _create_or_reset_harvest_job(
                        conn,
                        job_id=resume_job,
                        queries=queries,
                        industries=normalized_industries,
                        limit=limit,
                        per_query=per_query,
                        search_timeout=search_timeout,
                        fetch_workers=fetch_workers,
                        query_workers=query_workers,
                    )
                    query_rows = _job_query_rows(conn, resume_job)
                    conn.commit()
                else:
                    fail(f"Unknown harvest job: {resume_job}", code=ExitCode.NOT_FOUND)
            queries = list(job.get("queries") or queries)
            normalized_industries = _normalize_industries(job.get("industries"), None) or normalized_industries
            limit = int(job.get("limit_value") or limit)
            per_query = int(job.get("per_query") or per_query)
            search_timeout = job.get("search_timeout") if job.get("search_timeout") is not None else search_timeout
            resolved_search_timeout = max(0.001, float(search_timeout or DEFAULT_TIMEOUT))
            resolved_fetch_timeout = max(0.001, float(fetch_timeout if fetch_timeout is not None else resolved_search_timeout))
            fetch_workers = int(job.get("fetch_workers") or fetch_workers)
            query_workers = int(job.get("query_workers") or query_workers)
            if str(job.get("status") or "") == HARVEST_JOB_STATUS_COMPLETED or int(job.get("stored_count") or 0) >= limit:
                if str(job.get("status") or "") != HARVEST_JOB_STATUS_COMPLETED:
                    job = _update_harvest_job(
                        conn,
                        resume_job,
                        status=HARVEST_JOB_STATUS_COMPLETED,
                        stored_count=int(job.get("stored_count") or 0),
                        unique_url_count=int(job.get("unique_url_count") or 0),
                        seen_urls=_job_seen_urls(job),
                    )
                    conn.commit()
                return {
                    "industry": normalized_industries[0] if normalized_industries else None,
                    "industries": normalized_industries,
                    "query_count": len(queries),
                    "stored_count": int(job.get("stored_count") or 0),
                    "unique_url_count": int(job.get("unique_url_count") or 0),
                    "queries": queries,
                    "job": {
                        "job_id": job["job_id"],
                        "status": HARVEST_JOB_STATUS_COMPLETED,
                        "stored_count": int(job.get("stored_count") or 0),
                        "unique_url_count": int(job.get("unique_url_count") or 0),
                    },
                }

            _heal_stale_harvest_queries(conn, resume_job)
            query_rows = _job_query_rows(conn, resume_job)
            _update_harvest_job(conn, resume_job, status=HARVEST_JOB_STATUS_RUNNING)
            conn.commit()
        elif job_name:
            job = _create_or_reset_harvest_job(
                conn,
                job_id=job_name,
                queries=queries,
                industries=normalized_industries,
                limit=limit,
                per_query=per_query,
                search_timeout=search_timeout,
                fetch_workers=fetch_workers,
                query_workers=query_workers,
            )
            query_rows = _job_query_rows(conn, job_name)
            conn.commit()
        elif not queries:
            fail("Provide at least one --query or an --industry/--topic pair for content harvest", code=ExitCode.VALIDATION)
    finally:
        conn.close()
    def resolve_query(index: int, query: str) -> tuple[int, str, list[dict[str, str]]]:
        _emit_progress(progress, {"event": "query_started", "query": query, "query_index": index, "query_count": len(queries), "job_id": job_id})
        raw_results: list[dict[str, str]] | None = None
        auth_failed = False
        for attempt in range(max(1, int(retry_budget)) + 1):
            if backend != "public-only" and auth_search_fn is not None and raw_results is None and not auth_failed:
                try:
                    raw_results = _fetch_auth_results(query, per_query, auth_search_fn, progress=progress)
                    _emit_progress(progress, {"event": "query_backend", "query": query, "backend": "linkedin.auth", "result_count": len(raw_results)})
                except SystemExit as exc:
                    auth_failed = True
                    _emit_progress(progress, {"event": "query_backend_failed", "query": query, "backend": "linkedin.auth", "error": str(exc)})
                    raw_results = None
                except Exception as exc:
                    auth_failed = True
                    _emit_progress(progress, {"event": "query_backend_failed", "query": query, "backend": "linkedin.auth", "error": str(exc)})
                    raw_results = None
            if raw_results is not None:
                break
            if backend == "auth-only":
                if auth_failed or auth_search_fn is None:
                    raise RuntimeError(f"authenticated LinkedIn search failed for query `{query}`")
                break
            try:
                try:
                    raw_results = search_fn(query, limit=per_query, timeout=search_timeout)
                except TypeError:
                    raw_results = search_fn(query, limit=per_query)
                _emit_progress(progress, {"event": "query_backend", "query": query, "backend": "public.search", "result_count": len(raw_results)})
                break
            except Exception as exc:
                _emit_progress(progress, {"event": "query_failed", "query": query, "error": str(exc), "attempt": attempt + 1})
                if attempt >= max(1, int(retry_budget)) or not _is_retryable_harvest_error(exc):
                    raise
                _sleep_interval(cooldown_seconds, jitter_seconds)
        return index, query, raw_results

    if not queries:
        fail("Provide at least one --query or an --industry/--topic pair for content harvest", code=ExitCode.VALIDATION)

    stored_count = int(job.get("stored_count") or 0) if job else 0
    unique_urls: list[str] = _job_seen_urls(job or {})
    seen_urls: set[str] = set(unique_urls)
    active_queries: list[tuple[int, str]] = []
    if query_rows:
        for row in query_rows:
            if row.get("status") == HARVEST_JOB_STATUS_COMPLETED:
                continue
            active_queries.append((int(row["query_index"]), str(row["query"])))
    else:
        active_queries = list(enumerate(queries, start=1))

    def mark_query_running(index: int) -> None:
        if not job_id:
            return
        conn = store._connect()
        try:
            _update_harvest_query(conn, job_id, index, status=HARVEST_JOB_STATUS_RUNNING, last_error=None)
            _update_harvest_job(
                conn,
                job_id,
                status=HARVEST_JOB_STATUS_RUNNING,
                stored_count=stored_count,
                unique_url_count=len(seen_urls),
                seen_urls=list(seen_urls),
            )
            conn.commit()
        finally:
            conn.close()

    resolved_queries: list[tuple[int, str, list[dict[str, str]]]] = []
    failed_queries: list[tuple[int, str, Exception]] = []
    if max(1, int(query_workers)) == 1 or len(active_queries) == 1:
        for index, query in active_queries:
            try:
                mark_query_running(index)
                resolved_queries.append(resolve_query(index, query))
            except Exception as exc:
                failed_queries.append((index, query, exc))
                _emit_progress(progress, {"event": "query_failed", "query": query, "error": str(exc), "attempt": max(1, int(retry_budget)) + 1, "terminal": True})
                continue
    else:
        results_by_index: dict[int, tuple[int, str, list[dict[str, str]]]] = {}
        for chunk in _chunk_parallel_items(active_queries, max(1, int(query_workers))):
            future_map: dict[Any, tuple[int, str]] = {}
            pending_futures: set[Any] = set()
            executor = ThreadPoolExecutor(max_workers=max(1, int(query_workers)))
            try:
                for index, query in chunk:
                    mark_query_running(index)
                    future_map[executor.submit(resolve_query, index, query)] = (index, query)
                completed_futures, pending_futures = _wait_for_futures(
                    list(future_map),
                    worker_count=max(1, int(query_workers)),
                    per_task_timeout=resolved_search_timeout,
                )
                for future in completed_futures:
                    query_index, query = future_map[future]
                    try:
                        result = future.result()
                    except Exception as exc:
                        failed_queries.append((query_index, query, exc))
                        _emit_progress(progress, {"event": "query_failed", "query": query, "error": str(exc), "terminal": True})
                        continue
                    results_by_index[result[0]] = result
                if pending_futures:
                    for future in pending_futures:
                        query_index, query = future_map[future]
                        error = TimeoutError(f"query timed out after {resolved_search_timeout}s")
                        failed_queries.append((query_index, query, error))
                        _emit_progress(progress, {"event": "query_failed", "query": query, "error": str(error), "terminal": True, "timeout": True})
            finally:
                executor.shutdown(wait=not pending_futures, cancel_futures=bool(pending_futures))
        resolved_queries = [results_by_index[index] for index, _query in active_queries if index in results_by_index]

    if failed_queries and job_id:
        conn = store._connect()
        try:
            for failed_index, _failed_query, failed_exc in failed_queries:
                _update_harvest_query(conn, job_id, failed_index, status=HARVEST_JOB_STATUS_FAILED, last_error=str(failed_exc))
            _update_harvest_job(conn, job_id, status=HARVEST_JOB_STATUS_RUNNING, stored_count=stored_count, unique_url_count=len(seen_urls), seen_urls=list(seen_urls))
            conn.commit()
        finally:
            conn.close()

    for index, query, raw_results in resolved_queries:
        query_stored_count = 0
        query_failed_exc: Exception | None = None
        if job_id:
            conn = store._connect()
            try:
                _update_harvest_query(conn, job_id, index, status=HARVEST_JOB_STATUS_RUNNING, result_count=len(raw_results))
                _update_harvest_job(conn, job_id, status=HARVEST_JOB_STATUS_RUNNING, stored_count=stored_count, unique_url_count=len(seen_urls), seen_urls=list(seen_urls))
                conn.commit()
            finally:
                conn.close()
        results = filter_linkedin_search_results(raw_results, "posts")
        _emit_progress(progress, {"event": "query_results", "query": query, "result_count": len(results), "job_id": job_id})
        query_industries, query_topics = _query_scope(query, normalized_industries, normalized_topics)
        candidates: list[dict[str, str]] = []
        remaining = limit - stored_count
        for result in results:
            url = result.get("url") or ""
            if not url or url in seen_urls:
                continue
            seen_urls.add(url)
            unique_urls.append(url)
            candidates.append(result)
            if len(candidates) >= remaining:
                break

        for chunk in _chunk_parallel_items(candidates, max(1, int(fetch_workers))):
            future_map: dict[Any, dict[str, str]] = {}
            pending_fetches: set[Any] = set()
            executor = ThreadPoolExecutor(max_workers=max(1, int(fetch_workers)))
            try:
                for result in chunk:
                    direct_record = _auth_result_to_record(
                        result,
                        industry=query_industries[0] if query_industries else None,
                        industries=query_industries,
                        query_topics=query_topics,
                        source_query=query,
                    )
                    if direct_record is not None:
                        stored_record = upsert_post(direct_record)
                        content_warehouse.append_post_shard(job_id=archive_job_id, record=stored_record, query=query)
                        stored_count += 1
                        query_stored_count += 1
                        _emit_progress(
                            progress,
                            {
                                "event": "post_stored",
                                "url": direct_record["url"],
                                "query": query,
                                "stored_count": stored_count,
                                "limit": limit,
                                "mode": "auth-inline",
                                "job_id": job_id,
                            },
                        )
                        if stored_count >= limit:
                            break
                        _sleep_interval(min_request_interval, jitter_seconds)
                        continue
                    future_map[executor.submit(fetch_html, result["url"])] = result
                completed_futures, pending_fetches = _wait_for_futures(
                    list(future_map),
                    worker_count=max(1, int(fetch_workers)),
                    per_task_timeout=resolved_fetch_timeout,
                )
                for future in completed_futures:
                    result = future_map[future]
                    url = result["url"]
                    try:
                        html = future.result()
                    except Exception as exc:
                        _emit_progress(progress, {"event": "fetch_failed", "url": url, "query": query, "error": str(exc), "job_id": job_id})
                        query_failed_exc = exc
                        break
                    record = extract_post_record(url=url, html=html, source_query=query)
                    record["industry"] = query_industries[0] if query_industries else None
                    record["industries"] = query_industries
                    metadata = record.get("metadata") if isinstance(record.get("metadata"), dict) else {}
                    metadata["query_industries"] = query_industries
                    metadata["query_topics"] = query_topics
                    record["metadata"] = metadata
                    stored_record = upsert_post(record)
                    content_warehouse.append_post_shard(job_id=archive_job_id, record=stored_record, query=query)
                    stored_count += 1
                    query_stored_count += 1
                    _emit_progress(
                        progress,
                        {
                            "event": "post_stored",
                            "url": url,
                            "query": query,
                            "stored_count": stored_count,
                            "limit": limit,
                            "job_id": job_id,
                        },
                    )
                    if stored_count >= limit:
                        break
                    _sleep_interval(min_request_interval, jitter_seconds)
                if query_failed_exc is None and pending_fetches:
                    pending_urls = [future_map[future]["url"] for future in pending_fetches]
                    for url in pending_urls:
                        _emit_progress(
                            progress,
                            {
                                "event": "fetch_failed",
                                "url": url,
                                "query": query,
                                "error": f"fetch timed out after {resolved_fetch_timeout}s",
                                "job_id": job_id,
                                "timeout": True,
                            },
                        )
                    query_failed_exc = TimeoutError(f"fetch timed out after {resolved_fetch_timeout}s for {len(pending_urls)} urls")
            finally:
                executor.shutdown(wait=not pending_fetches, cancel_futures=bool(pending_fetches))
            if query_failed_exc is not None or stored_count >= limit:
                break
        if query_failed_exc is not None:
            failed_queries.append((index, query, query_failed_exc))
            if job_id:
                conn = store._connect()
                try:
                    _update_harvest_query(
                        conn,
                        job_id,
                        index,
                        status=HARVEST_JOB_STATUS_FAILED,
                        stored_count=query_stored_count,
                        result_count=len(results),
                        last_error=str(query_failed_exc),
                    )
                    _update_harvest_job(
                        conn,
                        job_id,
                        status=HARVEST_JOB_STATUS_RUNNING,
                        stored_count=stored_count,
                        unique_url_count=len(seen_urls),
                        seen_urls=list(seen_urls),
                    )
                    conn.commit()
                finally:
                    conn.close()
            continue
        if job_id:
            conn = store._connect()
            try:
                _update_harvest_query(conn, job_id, index, status=HARVEST_JOB_STATUS_COMPLETED, stored_count=query_stored_count, result_count=len(results), last_error=None)
                job = _update_harvest_job(
                    conn,
                    job_id,
                    status=HARVEST_JOB_STATUS_RUNNING,
                    stored_count=stored_count,
                    unique_url_count=len(seen_urls),
                    seen_urls=list(seen_urls),
                )
                conn.commit()
            finally:
                conn.close()
        if stored_count >= limit:
            break

    if failed_queries and not resolved_queries and stored_count == 0:
        _failed_index, failed_query, failed_exc = failed_queries[0]
        if job_id:
            conn = store._connect()
            try:
                _update_harvest_job(conn, job_id, status=HARVEST_JOB_STATUS_FAILED, stored_count=stored_count, unique_url_count=len(seen_urls), seen_urls=list(seen_urls))
                conn.commit()
            finally:
                conn.close()
        fail(f"Content harvest search failed for query `{failed_query}`: {failed_exc}", code=ExitCode.RETRYABLE)

    if job_id:
        conn = store._connect()
        try:
            remaining_rows = conn.execute(
                """
                SELECT COUNT(*) AS count
                FROM content_harvest_job_queries
                WHERE job_id = ? AND status != ?
                """,
                (job_id, HARVEST_JOB_STATUS_COMPLETED),
            ).fetchone()
            final_status = (
                HARVEST_JOB_STATUS_COMPLETED
                if stored_count >= limit or int(remaining_rows["count"] or 0) == 0
                else HARVEST_JOB_STATUS_RUNNING
            )
            job = _update_harvest_job(
                conn,
                job_id,
                status=final_status,
                stored_count=stored_count,
                unique_url_count=len(seen_urls),
                seen_urls=list(seen_urls),
            )
            conn.commit()
        finally:
            conn.close()

    summary = {
        "industry": normalized_industries[0] if normalized_industries else industry,
        "industries": normalized_industries,
        "query_count": len(queries),
        "stored_count": stored_count,
        "unique_url_count": len(unique_urls),
        "queries": queries,
        "failed_query_count": len(failed_queries),
        "failed_queries": [{"query_index": index, "query": query, "error": str(exc)} for index, query, exc in failed_queries],
    }
    if job:
        summary["job"] = {
            "job_id": job["job_id"],
            "status": job["status"],
            "stored_count": job["stored_count"],
            "unique_url_count": job["unique_url_count"],
        }
    _emit_progress(progress, {"event": "complete", **summary})
    return summary


def harvest_campaign(
    *,
    industry: str | None = None,
    industries: list[str] | None = None,
    topics: list[str] | None = None,
    query_terms: list[str] | None = None,
    limit: int = 1000,
    per_query: int = 25,
    per_job_limit: int = 1000,
    queries_per_job: int = 24,
    search_timeout: int | None = None,
    fetch_workers: int = 6,
    query_workers: int = 4,
    retry_budget: int = 2,
    cooldown_seconds: float = 0.0,
    min_request_interval: float = 0.0,
    jitter_seconds: float = 0.0,
    job_prefix: str = "campaign",
    materialize: bool = False,
    embed: bool = False,
    embed_model: str = "fastembed:BAAI/bge-small-en-v1.5",
    embed_batch_size: int = 25,
    retrain_every: int = 0,
    train_model_name: str = OUTCOME_MODEL_NAME,
    train_scope: str = "all",
    train_min_samples: int = 100,
    progress: Callable[[dict[str, Any]], None] | None = None,
    backend: str = "hybrid",
    expansion: str = "standard",
    freshness_buckets: list[str] | None = None,
    resume: bool = False,
    prune_min_yield: float | None = None,
    prune_min_attempts: int = 2,
    stop_min_yield_rate: float | None = None,
    stop_window: int = 3,
    speed: str = "balanced",
    public_search: str = "ddg",
    searxng_url: str | None = None,
    searxng_engines: list[str] | None = None,
) -> dict[str, Any]:
    from linkedin_cli import content_warehouse

    init_content_db()
    total_limit = max(1, int(limit))
    job_limit = max(1, int(per_job_limit))
    normalized_industries = _normalize_industries(industries, industry)
    normalized_topics = _normalize_topics(topics)
    queries = list(query_terms or build_harvest_queries(industries=normalized_industries, topics=normalized_topics, expansion=expansion, freshness_buckets=freshness_buckets))
    if not queries:
        fail("Provide at least one query or an industry/topic pair for content harvest-campaign", code=ExitCode.VALIDATION)
    normalized_speed = str(speed or "balanced").strip().lower()
    if normalized_speed not in {"balanced", "max"}:
        fail("speed must be one of: balanced, max", code=ExitCode.VALIDATION)
    deferred_post_processing = {"materialize": False, "embed": False, "retrain_every": 0}
    if backend == "auth-only":
        query_workers = 1
    if normalized_speed == "max":
        queries = _prioritize_campaign_queries(queries, job_prefix=job_prefix)
        fetch_workers = max(1, int(fetch_workers))
        query_workers = 1 if backend == "auth-only" else max(1, min(int(query_workers), 2))
        if prune_min_yield is None:
            prune_min_yield = 0.02
        prune_min_attempts = 1
        deferred_post_processing = {
            "materialize": bool(materialize),
            "embed": bool(embed),
            "retrain_every": int(retrain_every or 0),
        }
        materialize = False
        embed = False
        retrain_every = 0
    pruned_queries: list[dict[str, Any]] = []
    if prune_min_yield is not None:
        stats_map = _query_yield_stats_map(job_prefix=job_prefix)
        kept_queries: list[str] = []
        for query in queries:
            stats = stats_map.get(query)
            if stats and int(stats.get("attempt_count") or 0) >= max(1, int(prune_min_attempts)) and float(stats.get("yield_rate") or 0.0) < float(prune_min_yield):
                pruned_queries.append(
                    {
                        "query": query,
                        "attempt_count": int(stats.get("attempt_count") or 0),
                        "yield_rate": float(stats.get("yield_rate") or 0.0),
                    }
                )
                continue
            kept_queries.append(query)
        queries = kept_queries
    batches = _chunk_queries(queries, max(1, int(queries_per_job)))
    summaries: list[dict[str, Any]] = []
    materialized: list[dict[str, Any]] = []
    embeddings: list[dict[str, Any]] = []
    trainings: list[dict[str, Any]] = []
    stored_total = 0
    resumed_job_count = 0
    stopped_early = False
    stop_reason: str | None = None
    recent_yield_rates: list[float] = []
    if progress:
        progress(
            {
                "event": "campaign_started",
                "query_count": len(queries),
                "job_count": len(batches),
                "limit": total_limit,
                "job_prefix": job_prefix,
                "speed": normalized_speed,
                "query_workers": query_workers,
                "deferred_post_processing": deferred_post_processing,
            }
        )
    for batch_index, batch in enumerate(batches, start=1):
        remaining = total_limit - stored_total
        if remaining <= 0:
            break
        job_name = f"{job_prefix}-{batch_index:03d}"
        if progress:
            progress(
                {
                    "event": "campaign_job_started",
                    "job_id": job_name,
                    "job_index": batch_index,
                    "job_count": len(batches),
                    "query_count": len(batch),
                    "remaining_limit": remaining,
                }
            )
        existing_job = None
        if resume:
            conn = store._connect()
            try:
                existing_job = _load_harvest_job(conn, job_name)
            finally:
                conn.close()
        if resume and existing_job and str(existing_job.get("status") or "") == HARVEST_JOB_STATUS_COMPLETED:
            resumed_job_count += 1
            summary = {
                "industry": normalized_industries[0] if normalized_industries else None,
                "industries": normalized_industries,
                "query_count": len(existing_job.get("queries") or batch),
                "stored_count": int(existing_job.get("stored_count") or 0),
                "unique_url_count": int(existing_job.get("unique_url_count") or 0),
                "queries": list(existing_job.get("queries") or batch),
                "failed_query_count": 0,
                "failed_queries": [],
                "job": {
                    "job_id": existing_job["job_id"],
                    "status": existing_job["status"],
                    "stored_count": int(existing_job.get("stored_count") or 0),
                    "unique_url_count": int(existing_job.get("unique_url_count") or 0),
                },
            }
        else:
            summary = harvest_posts(
                industry=normalized_industries[0] if normalized_industries else None,
                industries=normalized_industries,
                topics=normalized_topics,
                limit=min(job_limit, remaining),
                per_query=per_query,
                query_terms=batch,
                search_timeout=search_timeout,
                fetch_workers=fetch_workers,
                query_workers=query_workers,
                job_name=None if resume else job_name,
                resume_job=job_name if resume else None,
                retry_budget=retry_budget,
                cooldown_seconds=cooldown_seconds,
                min_request_interval=min_request_interval,
                jitter_seconds=jitter_seconds,
                backend=backend,
                public_search=public_search,
                searxng_url=searxng_url,
                searxng_engines=searxng_engines,
                progress=progress,
            )
        job_stored_count = min(int(summary.get("stored_count") or 0), remaining)
        summary["stored_count"] = job_stored_count
        summaries.append(summary)
        stored_total += job_stored_count
        yield_rate = job_stored_count / max(1, len(summary.get("queries") or []) * max(1, int(per_query)))
        recent_yield_rates.append(yield_rate)
        if progress:
            progress(
                {
                    "event": "campaign_job_completed",
                    "job_id": job_name,
                    "job_index": batch_index,
                    "stored_count": job_stored_count,
                    "stored_total": stored_total,
                    "limit": total_limit,
                }
            )
        if stop_min_yield_rate is not None and len(recent_yield_rates) >= max(1, int(stop_window)):
            window = recent_yield_rates[-max(1, int(stop_window)) :]
            if all(rate < float(stop_min_yield_rate) for rate in window):
                stopped_early = True
                stop_reason = "marginal_yield_below_threshold"
                break
        if materialize:
            materialize_summary = content_warehouse.materialize_shards(job_id=job_name)
            materialized.append(materialize_summary)
            if progress:
                progress({"event": "campaign_materialized", "job_id": job_name, **materialize_summary})
        if embed and job_stored_count > 0:
            embed_summary = embed_posts(
                limit=job_stored_count,
                model=embed_model,
                batch_size=embed_batch_size,
                missing_only=True,
            )
            embeddings.append({"job_id": job_name, **embed_summary})
            if progress:
                progress({"event": "campaign_embedded", "job_id": job_name, **embed_summary})
        if retrain_every > 0 and batch_index % max(1, int(retrain_every)) == 0:
            train_summary = train_outcome_model(name=train_model_name, scope=train_scope, min_samples=train_min_samples)
            trainings.append({"job_id": job_name, **train_summary})
            if progress:
                progress({"event": "campaign_trained", "job_id": job_name, **train_summary})
    result = {
        "industries": normalized_industries,
        "query_count": len(queries),
        "job_count": len(summaries),
        "stored_count": stored_total,
        "resumed_job_count": resumed_job_count,
        "speed": normalized_speed,
        "public_search": public_search,
        "deferred_post_processing": deferred_post_processing,
        "pruned_query_count": len(pruned_queries),
        "pruned_queries": pruned_queries,
        "stopped_early": stopped_early,
        "stop_reason": stop_reason,
        "jobs": summaries,
        "materialized_jobs": materialized,
        "embedded_jobs": embeddings,
        "training_runs": trainings,
    }
    if progress:
        progress({"event": "campaign_complete", **result})
    return result
