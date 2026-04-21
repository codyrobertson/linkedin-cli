"""Corpus curation, dedupe, and balanced dataset builders over the local warehouse."""

from __future__ import annotations

import hashlib
import json
import math
import re
from collections import defaultdict
from pathlib import Path
from typing import Any

from linkedin_cli import content
from linkedin_cli.content_warehouse import _table_exists, _warehouse_connect, warehouse_db_path
from linkedin_cli.session import ExitCode, fail


def _normalize_text(text: str) -> str:
    normalized = re.sub(r"\s+", " ", (text or "").strip().lower())
    normalized = re.sub(r"https?://\S+", "", normalized)
    return normalized.strip()


def _simhash64(text: str) -> int:
    tokens = re.findall(r"[a-z0-9][a-z0-9+#.-]{1,}", _normalize_text(text))
    if not tokens:
        return 0
    weights = [0] * 64
    for token in tokens:
        digest = hashlib.sha256(token.encode("utf-8")).digest()
        value = int.from_bytes(digest[:8], "big", signed=False)
        for bit in range(64):
            weights[bit] += 1 if ((value >> bit) & 1) else -1
    result = 0
    for bit, weight in enumerate(weights):
        if weight >= 0:
            result |= (1 << bit)
    return result


def _hamming_distance(left: int, right: int) -> int:
    return (left ^ right).bit_count()


def _quality_score(row: dict[str, Any]) -> float:
    text = str(row.get("text") or "")
    title = str(row.get("title") or "")
    word_count = int(row.get("word_count") or 0)
    fingerprint = content._compute_content_fingerprint(text, title=title)
    outcome_score = float(row.get("outcome_score") or 0.0)
    hashtag_penalty = min(3.0, float(text.count("#")) * 0.3)
    length_penalty = 1.5 if word_count < 40 else (0.6 if word_count > 400 else 0.0)
    question_penalty = 0.5 if str(row.get("structure") or "") == "question" else 0.0
    quality = (
        1.2 * float(fingerprint[0])
        + 1.0 * float(fingerprint[1])
        + 1.1 * float(fingerprint[2])
        + 1.0 * float(fingerprint[3])
        + 1.2 * float(fingerprint[4])
        + 0.9 * float(fingerprint[10])
        + math.log1p(max(0.0, outcome_score))
        - hashtag_penalty
        - length_penalty
        - question_penalty
    )
    return round(quality, 6)


def _tone_for_text(text: str) -> str:
    lowered = _normalize_text(text)
    if any(term in lowered for term in ("i ", "we ", "our ", "me ")):
        return "personal"
    if any(term in lowered for term in ("should", "must", "need to", "stop", "start")):
        return "directive"
    if any(term in lowered for term in ("maybe", "might", "could", "perhaps")):
        return "exploratory"
    return "assertive"


def _proof_level(fingerprint: list[float]) -> str:
    proof = float(fingerprint[10] if len(fingerprint) > 10 else 0.0)
    if proof >= 0.66:
        return "high"
    if proof >= 0.33:
        return "medium"
    return "low"


def _cta_type(text: str) -> str:
    lowered = _normalize_text(text)
    if any(term in lowered for term in ("comment", "reply", "tell me", "dm me")):
        return "engagement"
    if any(term in lowered for term in ("book", "call", "meeting", "demo")):
        return "commercial"
    return "none"


def _author_archetype(row: dict[str, Any]) -> str:
    author_name = _normalize_text(str(row.get("author_name") or ""))
    lowered = _normalize_text(str(row.get("text") or ""))
    if any(term in lowered for term in ("founder", "we raised", "our startup")):
        return "founder"
    if any(term in lowered for term in ("operator", "pipeline", "playbook", "workflow")):
        return "operator"
    if any(term in lowered for term in ("research", "paper", "benchmark")):
        return "researcher"
    if author_name:
        return "creator"
    return "unknown"


def _freshness_bucket(value: str | None) -> str:
    raw = str(value or "")
    if raw.startswith("2026-03") or raw.startswith("2026-02"):
        return "recent"
    if raw.startswith("2025"):
        return "year"
    if raw:
        return "archive"
    return "unknown"


def _topics_for_row(row: dict[str, Any]) -> list[str]:
    text = "\n".join(
        str(part or "")
        for part in (row.get("source_query"), row.get("text"), row.get("title"))
        if part
    )
    return content._extract_topic_tokens(text, limit=8)


def _row_timestamp(row: dict[str, Any]) -> str:
    return str(row.get("published_at") or row.get("harvested_at") or "").strip()


def _row_author_group(row: dict[str, Any]) -> str:
    author_url = str(row.get("author_url") or "").strip().lower()
    if author_url:
        return f"author_url:{author_url}"
    author_name = _normalize_text(str(row.get("author_name") or ""))
    if author_name:
        return f"author_name:{author_name}"
    return f"url:{str(row.get('url') or '').strip()}"


def _author_disjoint_splits(
    rows: list[dict[str, Any]],
    *,
    train_ratio: float,
    val_ratio: float,
) -> dict[str, list[dict[str, Any]]]:
    ordered_rows = sorted(rows, key=lambda item: (_row_timestamp(item), str(item.get("url") or "")))
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in ordered_rows:
        groups[_row_author_group(row)].append(row)
    ordered_groups = sorted(
        groups.items(),
        key=lambda item: (
            min((_row_timestamp(row) for row in item[1]), default=""),
            item[0],
        ),
    )
    row_count = len(ordered_rows)
    train_target = max(0, min(row_count, int(row_count * train_ratio)))
    val_target = max(0, min(row_count - train_target, int(row_count * val_ratio)))
    splits: dict[str, list[dict[str, Any]]] = {"train": [], "val": [], "test": []}
    for group_key, group_rows in ordered_groups:
        del group_key
        if len(splits["train"]) < train_target:
            target = "train"
        elif len(splits["val"]) < val_target:
            target = "val"
        else:
            target = "test"
        splits[target].extend(group_rows)
    for split_name in splits:
        splits[split_name] = sorted(splits[split_name], key=lambda item: (_row_timestamp(item), str(item.get("url") or "")))
    return splits


def curate_corpus(
    *,
    industries: list[str] | None = None,
    min_quality: float = 0.0,
    near_duplicate_hamming: int = 4,
) -> dict[str, Any]:
    conn = _warehouse_connect()
    try:
        if not _table_exists(conn, "content_posts") or not _table_exists(conn, "content_post_industries"):
            fail("Local content warehouse is empty. Run `content materialize` first.", code=ExitCode.NOT_FOUND)
        where_sql = ""
        params: list[Any] = []
        if industries:
            placeholders = ",".join("?" for _ in industries)
            where_sql = (
                "WHERE p.url IN (SELECT DISTINCT url FROM content_post_industries WHERE industry IN (" + placeholders + "))"
            )
            params.extend(industries)
        rows = conn.execute(
            f"""
            SELECT p.url, p.title, p.author_name, p.author_url, p.published_at, p.text, p.hook, p.structure,
                   p.word_count, p.reaction_count, p.comment_count, p.repost_count, p.outcome_score, p.source_query, p.metadata_json,
                   COALESCE((SELECT LIST(industry ORDER BY industry ASC) FROM content_post_industries AS i WHERE i.url = p.url), []) AS industries
            FROM content_posts AS p
            {where_sql}
            ORDER BY p.outcome_score DESC, p.url ASC
            """,
            params,
        ).fetchall()
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS content_post_labels (
                url VARCHAR PRIMARY KEY,
                industries_json VARCHAR NOT NULL,
                topics_json VARCHAR NOT NULL,
                hook_type VARCHAR,
                structure VARCHAR,
                proof_level VARCHAR,
                tone VARCHAR,
                cta_type VARCHAR,
                author_archetype VARCHAR,
                freshness_bucket VARCHAR,
                quality_score DOUBLE NOT NULL,
                metadata_json VARCHAR NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS content_post_curation (
                url VARCHAR PRIMARY KEY,
                canonical_url VARCHAR NOT NULL,
                exact_duplicate BOOLEAN NOT NULL DEFAULT FALSE,
                near_duplicate BOOLEAN NOT NULL DEFAULT FALSE,
                duplicate_reason VARCHAR,
                quality_score DOUBLE NOT NULL,
                keep_row BOOLEAN NOT NULL DEFAULT TRUE,
                metadata_json VARCHAR NOT NULL
            )
            """
        )
        conn.execute("DELETE FROM content_post_labels")
        conn.execute("DELETE FROM content_post_curation")
        seen_exact: dict[str, str] = {}
        seen_simhash_buckets: dict[int, list[tuple[int, str]]] = defaultdict(list)
        stats = {"exact_duplicates": 0, "near_duplicates": 0, "kept_rows": 0}
        for raw in rows:
            row = {
                "url": raw[0],
                "title": raw[1],
                "author_name": raw[2],
                "author_url": raw[3],
                "published_at": raw[4],
                "text": raw[5],
                "hook": raw[6],
                "structure": raw[7],
                "word_count": int(raw[8] or 0),
                "reaction_count": int(raw[9] or 0),
                "comment_count": int(raw[10] or 0),
                "repost_count": int(raw[11] or 0),
                "outcome_score": float(raw[12] or 0.0),
                "source_query": raw[13],
                "metadata": json.loads(raw[14] or "{}"),
                "industries": list(raw[15] or []),
            }
            text = str(row["text"] or "")
            fingerprint = content._compute_content_fingerprint(text, title=str(row["title"] or ""))
            quality_score = _quality_score(row)
            normalized_text = _normalize_text(text)
            exact_key = hashlib.sha256(normalized_text.encode("utf-8")).hexdigest() if normalized_text else row["url"]
            simhash = _simhash64(normalized_text)
            bucket = simhash >> 48
            canonical_url = row["url"]
            exact_duplicate = False
            near_duplicate = False
            duplicate_reason = None
            if exact_key in seen_exact:
                canonical_url = seen_exact[exact_key]
                exact_duplicate = True
                duplicate_reason = "normalized_text_hash"
                stats["exact_duplicates"] += 1
            else:
                for existing_simhash, existing_url in seen_simhash_buckets[bucket]:
                    if _hamming_distance(simhash, existing_simhash) <= max(0, int(near_duplicate_hamming)):
                        canonical_url = existing_url
                        near_duplicate = True
                        duplicate_reason = "simhash"
                        stats["near_duplicates"] += 1
                        break
            keep_row = (not exact_duplicate) and (not near_duplicate) and quality_score >= float(min_quality)
            if keep_row:
                seen_exact[exact_key] = row["url"]
                seen_simhash_buckets[bucket].append((simhash, row["url"]))
                stats["kept_rows"] += 1
            hook_type = content._hook_type(content._sentence_hook(text) or str(row["title"] or ""))
            topics = _topics_for_row(row)
            conn.execute(
                """
                INSERT INTO content_post_labels
                (url, industries_json, topics_json, hook_type, structure, proof_level, tone, cta_type, author_archetype, freshness_bucket, quality_score, metadata_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    row["url"],
                    json.dumps(row["industries"], ensure_ascii=False, sort_keys=True),
                    json.dumps(topics, ensure_ascii=False, sort_keys=True),
                    hook_type,
                    row["structure"],
                    _proof_level(fingerprint),
                    _tone_for_text(text),
                    _cta_type(text),
                    _author_archetype(row),
                    _freshness_bucket(row["published_at"]),
                    quality_score,
                    json.dumps({"simhash64": simhash, "exact_key": exact_key}, ensure_ascii=False, sort_keys=True),
                ),
            )
            conn.execute(
                """
                INSERT INTO content_post_curation
                (url, canonical_url, exact_duplicate, near_duplicate, duplicate_reason, quality_score, keep_row, metadata_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    row["url"],
                    canonical_url,
                    bool(exact_duplicate),
                    bool(near_duplicate),
                    duplicate_reason,
                    quality_score,
                    bool(keep_row),
                    json.dumps({"simhash64": simhash, "topics": topics}, ensure_ascii=False, sort_keys=True),
                ),
            )
        conn.execute(
            """
            CREATE OR REPLACE VIEW curated_content_posts AS
            SELECT
                p.*,
                l.industries_json,
                l.topics_json,
                l.hook_type,
                l.proof_level,
                l.tone,
                l.cta_type,
                l.author_archetype,
                l.freshness_bucket,
                l.quality_score,
                c.canonical_url,
                c.exact_duplicate,
                c.near_duplicate,
                c.duplicate_reason,
                c.keep_row
            FROM content_posts AS p
            INNER JOIN content_post_labels AS l ON l.url = p.url
            INNER JOIN content_post_curation AS c ON c.url = p.url
            WHERE c.keep_row = TRUE
            """
        )
        conn.commit()
        kept_count = int(conn.execute("SELECT COUNT(*) FROM curated_content_posts").fetchone()[0] or 0)
        return {
            "warehouse_path": str(warehouse_db_path()),
            "processed_count": len(rows),
            "kept_count": kept_count,
            "exact_duplicate_count": stats["exact_duplicates"],
            "near_duplicate_count": stats["near_duplicates"],
            "min_quality": float(min_quality),
        }
    finally:
        conn.close()


def curation_stats() -> dict[str, Any]:
    conn = _warehouse_connect(read_only=True)
    try:
        if not _table_exists(conn, "content_post_curation") or not _table_exists(conn, "content_post_labels"):
            return {
                "warehouse_path": str(warehouse_db_path()),
                "curated_count": 0,
                "duplicate_counts": {},
                "by_industry": {},
                "by_structure": {},
            }
        curated_count = int(conn.execute("SELECT COUNT(*) FROM curated_content_posts").fetchone()[0] or 0)
        duplicates = conn.execute(
            """
            SELECT
                SUM(CASE WHEN exact_duplicate THEN 1 ELSE 0 END) AS exact_count,
                SUM(CASE WHEN near_duplicate THEN 1 ELSE 0 END) AS near_count
            FROM content_post_curation
            """
        ).fetchone()
        industries = conn.execute(
            """
            SELECT industry, COUNT(*) AS count
            FROM content_post_industries
            WHERE url IN (SELECT url FROM curated_content_posts)
            GROUP BY industry
            ORDER BY count DESC, industry ASC
            """
        ).fetchall()
        structures = conn.execute(
            """
            SELECT structure, COUNT(*) AS count
            FROM curated_content_posts
            GROUP BY structure
            ORDER BY count DESC, structure ASC
            """
        ).fetchall()
        return {
            "warehouse_path": str(warehouse_db_path()),
            "curated_count": curated_count,
            "duplicate_counts": {
                "exact": int(duplicates[0] or 0),
                "near": int(duplicates[1] or 0),
            },
            "by_industry": {str(row[0]): int(row[1]) for row in industries},
            "by_structure": {str(row[0]): int(row[1]) for row in structures},
        }
    finally:
        conn.close()


def _load_curated_rows(*, industries: list[str] | None = None, topics: list[str] | None = None) -> list[dict[str, Any]]:
    conn = _warehouse_connect(read_only=True)
    try:
        if not _table_exists(conn, "content_post_curation"):
            fail("Curated corpus not found. Run `content curate-corpus` first.", code=ExitCode.NOT_FOUND)
        rows = conn.execute(
            """
            SELECT url, title, author_name, author_url, published_at, text, hook, structure, word_count,
                   reaction_count, comment_count, repost_count, outcome_score, source_query, harvested_at,
                   metadata_json, industries_json, topics_json, hook_type, proof_level, tone, cta_type,
                   author_archetype, freshness_bucket, quality_score
            FROM curated_content_posts
            ORDER BY quality_score DESC, outcome_score DESC, url ASC
            """
        ).fetchall()
    finally:
        conn.close()
    result: list[dict[str, Any]] = []
    wanted_industries = set(industries or [])
    wanted_topics = set(topics or [])
    for row in rows:
        payload = {
            "url": row[0],
            "title": row[1],
            "author_name": row[2],
            "author_url": row[3],
            "published_at": row[4],
            "text": row[5],
            "hook": row[6],
            "structure": row[7],
            "word_count": int(row[8] or 0),
            "reaction_count": int(row[9] or 0),
            "comment_count": int(row[10] or 0),
            "repost_count": int(row[11] or 0),
            "outcome_score": float(row[12] or 0.0),
            "source_query": row[13],
            "harvested_at": row[14],
            "metadata": json.loads(row[15] or "{}"),
            "industries": list(json.loads(row[16] or "[]")),
            "topics": list(json.loads(row[17] or "[]")),
            "hook_type": row[18],
            "proof_level": row[19],
            "tone": row[20],
            "cta_type": row[21],
            "author_archetype": row[22],
            "freshness_bucket": row[23],
            "quality_score": float(row[24] or 0.0),
        }
        if wanted_industries and not (wanted_industries & set(payload["industries"])):
            continue
        if wanted_topics and not (wanted_topics & set(payload["topics"])):
            continue
        result.append(payload)
    return result


def sample_balanced_rows(
    *,
    limit: int,
    industries: list[str] | None = None,
    topics: list[str] | None = None,
    quota_per_industry: int | None = None,
    quota_per_topic: int | None = None,
    quota_per_format: int | None = None,
) -> list[dict[str, Any]]:
    rows = _load_curated_rows(industries=industries, topics=topics)
    selected: list[dict[str, Any]] = []
    used_urls: set[str] = set()
    per_industry: dict[str, int] = defaultdict(int)
    per_topic: dict[str, int] = defaultdict(int)
    per_format: dict[str, int] = defaultdict(int)
    for row in rows:
        if row["url"] in used_urls:
            continue
        if quota_per_industry is not None and any(per_industry[industry] >= quota_per_industry for industry in row["industries"]):
            continue
        if quota_per_topic is not None and row["topics"] and any(per_topic[topic] >= quota_per_topic for topic in row["topics"]):
            continue
        if quota_per_format is not None and per_format[str(row.get("structure") or "unknown")] >= quota_per_format:
            continue
        selected.append(row)
        used_urls.add(row["url"])
        for industry in row["industries"]:
            per_industry[industry] += 1
        for topic in row["topics"]:
            per_topic[topic] += 1
        per_format[str(row.get("structure") or "unknown")] += 1
        if len(selected) >= max(1, int(limit)):
            break
    return selected


def build_holdouts(
    *,
    output_dir: str | Path,
    industries: list[str] | None = None,
    topics: list[str] | None = None,
    train_ratio: float = 0.8,
    val_ratio: float = 0.1,
    quota_per_industry: int | None = None,
    quota_per_topic: int | None = None,
    quota_per_format: int | None = None,
    limit: int = 50000,
    time_holdout_ratio: float = 0.1,
) -> dict[str, Any]:
    rows = sample_balanced_rows(
        limit=limit,
        industries=industries,
        topics=topics,
        quota_per_industry=quota_per_industry,
        quota_per_topic=quota_per_topic,
        quota_per_format=quota_per_format,
    )
    rows = sorted(rows, key=lambda item: (_row_timestamp(item), item["url"]))
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    row_count = len(rows)
    time_holdout_count = max(0, min(row_count, int(row_count * max(0.0, min(0.9, time_holdout_ratio)))))
    time_holdout = rows[-time_holdout_count:] if time_holdout_count else []
    core_rows = rows[:-time_holdout_count] if time_holdout_count else rows
    splits = _author_disjoint_splits(core_rows, train_ratio=train_ratio, val_ratio=val_ratio)
    splits["time_holdout"] = time_holdout
    for split_name, split_rows in splits.items():
        with (output_path / f"{split_name}.jsonl").open("w", encoding="utf-8") as handle:
            for row in split_rows:
                handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")
    metadata = {
        "split_strategy": "author_disjoint_with_time_holdout",
        "author_group_count": len({_row_author_group(row) for row in rows}),
        "time_holdout_ratio": float(time_holdout_ratio),
        "time_holdout_count": len(time_holdout),
    }
    (output_path / "metadata.json").write_text(json.dumps(metadata, ensure_ascii=False, sort_keys=True, indent=2), encoding="utf-8")
    return {
        "output_dir": str(output_path),
        "row_count": row_count,
        "splits": {name: len(split_rows) for name, split_rows in splits.items()},
        "quota_per_industry": quota_per_industry,
        "quota_per_topic": quota_per_topic,
        "quota_per_format": quota_per_format,
        **metadata,
    }


def build_curated_sft_dataset(
    *,
    output_dir: str | Path,
    industries: list[str] | None = None,
    topics: list[str] | None = None,
    limit: int = 50000,
    quota_per_industry: int | None = None,
    quota_per_topic: int | None = None,
    quota_per_format: int | None = None,
    train_ratio: float = 0.8,
    val_ratio: float = 0.1,
) -> dict[str, Any]:
    holdouts = build_holdouts(
        output_dir=output_dir,
        industries=industries,
        topics=topics,
        limit=limit,
        quota_per_industry=quota_per_industry,
        quota_per_topic=quota_per_topic,
        quota_per_format=quota_per_format,
        train_ratio=train_ratio,
        val_ratio=val_ratio,
    )
    output_path = Path(output_dir)
    source_counts: dict[str, int] = {"curated_harvested": 0}
    for split_name in ("train", "val", "test"):
        rows = [
            json.loads(line)
            for line in (output_path / f"{split_name}.jsonl").read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
        with (output_path / f"{split_name}.jsonl").open("w", encoding="utf-8") as handle:
            for row in rows:
                record = {
                    "messages": [
                        {
                            "role": "system",
                            "content": "Write a high-signal LinkedIn post that matches the requested slice and preserves specifics.",
                        },
                        {
                            "role": "user",
                            "content": json.dumps(
                                {
                                    "industries": row.get("industries") or [],
                                    "topics": row.get("topics") or [],
                                    "structure": row.get("structure"),
                                    "hook_type": row.get("hook_type"),
                                    "proof_level": row.get("proof_level"),
                                    "tone": row.get("tone"),
                                    "cta_type": row.get("cta_type"),
                                },
                                ensure_ascii=False,
                                sort_keys=True,
                            ),
                        },
                        {"role": "assistant", "content": row.get("text") or ""},
                    ],
                    "metadata": {
                        "source": "curated_harvested",
                        "url": row.get("url"),
                        "industries": row.get("industries") or [],
                        "topics": row.get("topics") or [],
                        "quality_score": row.get("quality_score"),
                    },
                }
                source_counts["curated_harvested"] += 1
                handle.write(json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n")
    holdouts["source_counts"] = source_counts
    holdouts["dataset_type"] = "curated_sft"
    return holdouts


def build_curated_preference_dataset(
    *,
    output_dir: str | Path,
    industries: list[str] | None = None,
    topics: list[str] | None = None,
    limit: int = 50000,
    quota_per_industry: int | None = None,
    quota_per_topic: int | None = None,
    quota_per_format: int | None = None,
    train_ratio: float = 0.8,
    val_ratio: float = 0.1,
) -> dict[str, Any]:
    rows = sample_balanced_rows(
        limit=limit,
        industries=industries,
        topics=topics,
        quota_per_industry=quota_per_industry,
        quota_per_topic=quota_per_topic,
        quota_per_format=quota_per_format,
    )
    groups: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        industries_key = ",".join(row.get("industries") or ["unknown"])
        groups[(industries_key, str(row.get("structure") or "unknown"))].append(row)
    pairs: list[dict[str, Any]] = []
    for (_industry_key, _structure), grouped_rows in groups.items():
        ordered = sorted(grouped_rows, key=lambda item: (float(item.get("quality_score") or 0.0), float(item.get("outcome_score") or 0.0)), reverse=True)
        if len(ordered) < 2:
            continue
        winners = ordered[: max(1, len(ordered) // 3)]
        losers = list(reversed(ordered[-max(1, len(ordered) // 3) :]))
        for winner, loser in zip(winners, losers):
            if winner["url"] == loser["url"]:
                continue
            pairs.append(
                {
                    "prompt": json.dumps(
                        {
                            "industries": winner.get("industries") or [],
                            "topics": winner.get("topics") or [],
                            "structure": winner.get("structure"),
                        },
                        ensure_ascii=False,
                        sort_keys=True,
                    ),
                    "chosen": winner.get("text") or "",
                    "rejected": loser.get("text") or "",
                    "metadata": {
                        "chosen_url": winner.get("url"),
                        "rejected_url": loser.get("url"),
                        "industries": winner.get("industries") or [],
                        "topics": winner.get("topics") or [],
                        "structure": winner.get("structure"),
                    },
                }
            )
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    row_count = len(pairs)
    train_cutoff = max(0, min(row_count, int(row_count * train_ratio)))
    val_cutoff = max(train_cutoff, min(row_count, train_cutoff + int(row_count * val_ratio)))
    splits = {
        "train": pairs[:train_cutoff],
        "val": pairs[train_cutoff:val_cutoff],
        "test": pairs[val_cutoff:],
    }
    for split_name, split_rows in splits.items():
        with (output_path / f"{split_name}.jsonl").open("w", encoding="utf-8") as handle:
            for row in split_rows:
                handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")
    return {
        "output_dir": str(output_path),
        "row_count": row_count,
        "splits": {name: len(split_rows) for name, split_rows in splits.items()},
        "dataset_type": "curated_preference",
        "quota_per_industry": quota_per_industry,
        "quota_per_topic": quota_per_topic,
        "quota_per_format": quota_per_format,
    }
