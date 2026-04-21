"""Local content shard and DuckDB warehouse helpers."""

from __future__ import annotations

import hashlib
import json
import math
import os
import resource
import time
from pathlib import Path
from typing import Any

from linkedin_cli.session import ExitCode, fail
from linkedin_cli.write import store

try:
    import duckdb
except Exception:  # pragma: no cover - dependency validated in CLI/tests
    duckdb = None


def _normalized_payload_industries(payload: dict[str, Any]) -> list[str]:
    from linkedin_cli import content

    source_query = str(payload.get("source_query") or "").strip()
    metadata = payload.get("metadata") if isinstance(payload.get("metadata"), dict) else {}
    legacy_industries = content._normalize_industries(payload.get("industries"), payload.get("industry"))
    metadata_industries = content._normalize_industries(metadata.get("query_industries"))
    query_industries, _query_topics = content._query_scope(source_query, legacy_industries)
    if query_industries:
        return query_industries
    if metadata_industries:
        return metadata_industries
    return legacy_industries


def shards_dir() -> Path:
    return store.ARTIFACTS_DIR / "content-shards"


def shard_job_dir(job_id: str) -> Path:
    return shards_dir() / (job_id or "adhoc")


def shard_file_path(job_id: str) -> Path:
    return shard_job_dir(job_id) / "posts.jsonl"


def warehouse_db_path() -> Path:
    return store.DB_PATH.parent / "warehouse" / "content.duckdb"


def benchmark_dir() -> Path:
    return store.ARTIFACTS_DIR / "benchmarks" / "content"


def benchmark_report_path(job_id: str) -> Path:
    return benchmark_dir() / f"{job_id}.json"


def _warehouse_connect(*, read_only: bool = False) -> Any:
    if duckdb is None:
        fail("DuckDB is required for local warehouse commands. Install `duckdb` first.", code=ExitCode.VALIDATION)
    path = warehouse_db_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(path), read_only=read_only)


def _content_hash(record: dict[str, Any]) -> str:
    body = "\n".join(
        [
            str(record.get("url") or ""),
            str(record.get("title") or ""),
            str(record.get("text") or ""),
            str(record.get("author_name") or ""),
        ]
    )
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


def append_post_shard(*, job_id: str, record: dict[str, Any], query: str | None = None) -> Path:
    job_dir = shard_job_dir(job_id)
    job_dir.mkdir(parents=True, exist_ok=True)
    path = shard_file_path(job_id)
    payload = {
        "job_id": job_id,
        "url": str(record.get("url") or ""),
        "title": record.get("title"),
        "author_name": record.get("author_name"),
        "author_url": record.get("author_url"),
        "published_at": record.get("published_at"),
        "text": record.get("text"),
        "hook": record.get("hook"),
        "structure": record.get("structure"),
        "word_count": int(record.get("word_count") or 0),
        "reaction_count": int(record.get("reaction_count") or 0),
        "comment_count": int(record.get("comment_count") or 0),
        "repost_count": int(record.get("repost_count") or 0),
        "owned_by_me": bool(record.get("owned_by_me")),
        "outcome_score": float(record.get("outcome_score") or 0.0),
        "source_query": query or record.get("source_query"),
        "industries": [],
        "metadata": record.get("metadata") or {},
        "harvested_at": record.get("updated_at") or store._now_iso(),
        "content_hash": _content_hash(record),
    }
    payload["industries"] = _normalized_payload_industries(payload | {"industries": record.get("industries"), "industry": record.get("industry")})
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=False, sort_keys=True) + "\n")
    return path


def _iter_shard_files(job_id: str | None = None) -> list[Path]:
    if job_id:
        path = shard_file_path(job_id)
        return [path] if path.exists() else []
    return sorted(shards_dir().glob("*/posts.jsonl"))


def _init_warehouse(conn: Any) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS raw_content_posts (
            job_id VARCHAR,
            url VARCHAR,
            title VARCHAR,
            author_name VARCHAR,
            author_url VARCHAR,
            published_at VARCHAR,
            text VARCHAR,
            hook VARCHAR,
            structure VARCHAR,
            word_count INTEGER,
            reaction_count INTEGER,
            comment_count INTEGER,
            repost_count INTEGER,
            owned_by_me BOOLEAN,
            outcome_score DOUBLE,
            source_query VARCHAR,
            harvested_at VARCHAR,
            content_hash VARCHAR,
            metadata_json VARCHAR
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS raw_content_post_industries (
            job_id VARCHAR,
            url VARCHAR,
            industry VARCHAR,
            harvested_at VARCHAR
        )
        """
    )


def _table_exists(conn: Any, table_name: str) -> bool:
    row = conn.execute(
        """
        SELECT COUNT(*) FROM information_schema.tables
        WHERE table_name = ?
        """,
        [table_name],
    ).fetchone()
    return bool(row and int(row[0] or 0) > 0)


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _sql_literal_list(values: list[str]) -> str:
    return ", ".join(_sql_literal(value) for value in values)


def _foundation_post_sql_columns() -> dict[str, str]:
    return {
        "query_industries": "COALESCE(CAST(json_extract(p.metadata_json, '$.query_industries') AS VARCHAR[]), [])",
        "query_topics": "COALESCE(CAST(json_extract(p.metadata_json, '$.query_topics') AS VARCHAR[]), [])",
        "normalized_engagement_score": (
            "CAST("
            "("
            "COALESCE(p.reaction_count, 0) + (2 * COALESCE(p.comment_count, 0)) + (3 * COALESCE(p.repost_count, 0))"
            ") AS DOUBLE) / GREATEST(1, COALESCE(p.word_count, 0))"
        ),
        "proof_level": (
            "CASE "
            "WHEN regexp_matches(lower(COALESCE(p.text, '')), '(proof|case study|benchmark|measured|results|evidence|proved|saved|hours|data)') THEN "
            "    CASE WHEN COALESCE(p.word_count, 0) >= 120 THEN 'high' ELSE 'medium' END "
            "WHEN regexp_matches(lower(COALESCE(p.text, '')), '(workflow|playbook|framework|guide|how to|teardown|lesson)') THEN 'medium' "
            "ELSE 'low' END"
        ),
        "tone": (
            "CASE "
            "WHEN regexp_matches(lower(COALESCE(p.text, '')), '(\\bi\\b|\\bwe\\b|\\bour\\b|\\bme\\b)') THEN 'personal' "
            "WHEN regexp_matches(lower(COALESCE(p.text, '')), '(should|must|need to|stop|start)') THEN 'directive' "
            "WHEN regexp_matches(lower(COALESCE(p.text, '')), '(maybe|might|could|perhaps)') THEN 'exploratory' "
            "ELSE 'assertive' END"
        ),
        "cta_type": (
            "CASE "
            "WHEN regexp_matches(lower(COALESCE(p.text, '')), '(comment|reply|tell me|dm me)') THEN 'engagement' "
            "WHEN regexp_matches(lower(COALESCE(p.text, '')), '(book|call|meeting|demo)') THEN 'commercial' "
            "ELSE 'none' END"
        ),
        "author_archetype": (
            "CASE "
            "WHEN regexp_matches(lower(COALESCE(p.text, '')), '(founder|we raised|our startup)') THEN 'founder' "
            "WHEN regexp_matches(lower(COALESCE(p.text, '')), '(operator|pipeline|playbook|workflow)') THEN 'operator' "
            "WHEN regexp_matches(lower(COALESCE(p.text, '')), '(research|paper|benchmark)') THEN 'researcher' "
            "WHEN COALESCE(p.author_name, '') <> '' THEN 'creator' "
            "ELSE 'unknown' END"
        ),
        "freshness_bucket": (
            "CASE "
            "WHEN COALESCE(p.published_at, '') LIKE '2026-03%' OR COALESCE(p.published_at, '') LIKE '2026-02%' THEN 'recent' "
            "WHEN COALESCE(p.published_at, '') LIKE '2025%' THEN 'year' "
            "WHEN COALESCE(p.published_at, '') <> '' THEN 'archive' "
            "ELSE 'unknown' END"
        ),
    }


def build_foundation_views(*, industries: list[str] | None = None) -> dict[str, Any]:
    conn = _warehouse_connect()
    try:
        if not _table_exists(conn, "content_posts") or not _table_exists(conn, "content_post_industries"):
            fail("Local content warehouse is empty. Run `content materialize` first.", code=ExitCode.NOT_FOUND)
        filtered_industries = [str(industry).strip() for industry in (industries or []) if str(industry).strip()]
        filtered_sql = ""
        if filtered_industries:
            filtered_sql = (
                "WHERE p.url IN (SELECT DISTINCT url FROM content_post_industries WHERE industry IN ("
                + _sql_literal_list(filtered_industries)
                + "))"
            )
        foundation_cols = _foundation_post_sql_columns()

        conn.execute(
            f"""
            CREATE OR REPLACE VIEW content_foundation_posts AS
            SELECT
                p.job_id,
                p.url,
                p.title,
                p.author_name,
                p.author_url,
                p.published_at,
                p.text,
                p.hook,
                p.structure,
                p.word_count,
                p.reaction_count,
                p.comment_count,
                p.repost_count,
                p.owned_by_me,
                p.outcome_score,
                p.source_query,
                p.harvested_at,
                p.content_hash,
                p.metadata_json,
                {foundation_cols["query_industries"]} AS query_industries,
                {foundation_cols["query_topics"]} AS query_topics,
                {foundation_cols["normalized_engagement_score"]} AS normalized_engagement_score,
                {foundation_cols["proof_level"]} AS proof_level,
                {foundation_cols["tone"]} AS tone,
                {foundation_cols["cta_type"]} AS cta_type,
                {foundation_cols["author_archetype"]} AS author_archetype,
                {foundation_cols["freshness_bucket"]} AS freshness_bucket,
                COALESCE(
                    (
                        SELECT LIST(industry ORDER BY industry ASC)
                        FROM (
                            SELECT DISTINCT industry
                            FROM content_post_industries AS i
                            WHERE i.url = p.url
                        )
                    ),
                    []
                ) AS industries
            FROM content_posts AS p
            {filtered_sql}
            """
        )
        conn.execute(
            f"""
            CREATE OR REPLACE VIEW content_foundation_author_stats AS
            SELECT
                p.author_name,
                p.author_url,
                COUNT(*) AS post_count,
                AVG(CAST(p.reaction_count AS DOUBLE)) AS avg_reaction_count,
                AVG(CAST(p.comment_count AS DOUBLE)) AS avg_comment_count,
                AVG(CAST(p.repost_count AS DOUBLE)) AS avg_repost_count,
                AVG(CAST(p.outcome_score AS DOUBLE)) AS avg_outcome_score,
                SUM(CASE WHEN p.owned_by_me THEN 1 ELSE 0 END) AS owned_post_count
            FROM content_posts AS p
            {filtered_sql}
            GROUP BY p.author_name, p.author_url
            """
        )
        industry_filter_sql = ""
        if filtered_industries:
            industry_filter_sql = "WHERE i.industry IN (" + _sql_literal_list(filtered_industries) + ")"
        conn.execute(
            f"""
            CREATE OR REPLACE VIEW content_foundation_industry_stats AS
            SELECT
                i.industry,
                COUNT(DISTINCT p.url) AS post_count,
                AVG(CAST(p.reaction_count AS DOUBLE)) AS avg_reaction_count,
                AVG(CAST(p.comment_count AS DOUBLE)) AS avg_comment_count,
                AVG(CAST(p.repost_count AS DOUBLE)) AS avg_repost_count,
                AVG(CAST(p.outcome_score AS DOUBLE)) AS avg_outcome_score
            FROM content_posts AS p
            INNER JOIN content_post_industries AS i ON i.url = p.url
            {industry_filter_sql}
            GROUP BY i.industry
            """
        )
        conn.execute(
            """
            CREATE OR REPLACE VIEW content_head_public_performance AS
            SELECT
                url,
                normalized_engagement_score AS public_performance_score,
                normalized_engagement_score,
                reaction_count,
                comment_count,
                repost_count
            FROM content_foundation_posts
            """
        )
        conn.execute(
            """
            CREATE OR REPLACE VIEW content_head_persona_style AS
            SELECT
                url,
                author_archetype,
                tone,
                proof_level,
                structure,
                CASE
                    WHEN author_archetype = 'operator' THEN 0.9
                    WHEN author_archetype = 'founder' THEN 0.75
                    WHEN author_archetype = 'researcher' THEN 0.65
                    ELSE 0.55
                END AS persona_style_score,
                CASE
                    WHEN proof_level = 'high' THEN 0.9
                    WHEN proof_level = 'medium' THEN 0.65
                    ELSE 0.35
                END AS proof_heavy_score,
                CASE
                    WHEN tone = 'directive' THEN 0.85
                    WHEN tone = 'personal' THEN 0.7
                    WHEN tone = 'exploratory' THEN 0.55
                    ELSE 0.5
                END AS directive_vs_observational_score
            FROM content_foundation_posts
            """
        )
        conn.execute(
            """
            CREATE OR REPLACE VIEW content_head_business_intent AS
            SELECT
                url,
                cta_type,
                query_industries,
                query_topics,
                author_archetype,
                proof_level,
                CASE
                    WHEN cta_type = 'commercial' THEN 0.9
                    WHEN cta_type = 'engagement' THEN 0.7
                    ELSE 0.35
                END AS cta_alignment_score,
                CASE
                    WHEN list_contains(query_topics, 'workflow') OR list_contains(query_topics, 'growth') THEN 0.8
                    WHEN list_contains(query_topics, 'pipeline') OR list_contains(query_topics, 'revenue') THEN 0.9
                    ELSE 0.5
                END AS topic_commerciality_score,
                CASE
                    WHEN author_archetype = 'operator' THEN 0.8
                    WHEN author_archetype = 'founder' THEN 0.7
                    ELSE 0.55
                END AS business_intent_score
            FROM content_foundation_posts
            """
        )
        if filtered_industries:
            post_count = int(
                conn.execute(
                    "SELECT COUNT(DISTINCT p.url) FROM content_posts AS p INNER JOIN content_post_industries AS i ON i.url = p.url WHERE i.industry IN ("
                    + _sql_literal_list(filtered_industries)
                    + ")"
                ).fetchone()[0]
                or 0
            )
        else:
            post_count = int(conn.execute("SELECT COUNT(*) FROM content_posts").fetchone()[0] or 0)
        summary = {
            "warehouse_path": str(warehouse_db_path()),
            "post_count": post_count,
            "views": [
                "content_foundation_posts",
                "content_foundation_author_stats",
                "content_foundation_industry_stats",
                "content_head_public_performance",
                "content_head_persona_style",
                "content_head_business_intent",
            ],
        }
        if filtered_industries:
            summary["industries"] = filtered_industries
        return summary
    finally:
        conn.close()


def materialize_shards(*, job_id: str | None = None) -> dict[str, Any]:
    files = _iter_shard_files(job_id)
    conn = _warehouse_connect()
    try:
        _init_warehouse(conn)
        rows_loaded = 0
        industry_rows_loaded = 0
        for path in files:
            job_name = path.parent.name
            conn.execute("DELETE FROM raw_content_posts WHERE job_id = ?", [job_name])
            conn.execute("DELETE FROM raw_content_post_industries WHERE job_id = ?", [job_name])
            with path.open("r", encoding="utf-8") as handle:
                for raw_line in handle:
                    raw_line = raw_line.strip()
                    if not raw_line:
                        continue
                    payload = json.loads(raw_line)
                    payload["industries"] = _normalized_payload_industries(payload)
                    conn.execute(
                        """
                        INSERT INTO raw_content_posts
                        (job_id, url, title, author_name, author_url, published_at, text, hook, structure, word_count,
                         reaction_count, comment_count, repost_count, owned_by_me, outcome_score, source_query, harvested_at,
                         content_hash, metadata_json)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        [
                            payload.get("job_id"),
                            payload.get("url"),
                            payload.get("title"),
                            payload.get("author_name"),
                            payload.get("author_url"),
                            payload.get("published_at"),
                            payload.get("text"),
                            payload.get("hook"),
                            payload.get("structure"),
                            int(payload.get("word_count") or 0),
                            int(payload.get("reaction_count") or 0),
                            int(payload.get("comment_count") or 0),
                            int(payload.get("repost_count") or 0),
                            bool(payload.get("owned_by_me")),
                            float(payload.get("outcome_score") or 0.0),
                            payload.get("source_query"),
                            payload.get("harvested_at"),
                            payload.get("content_hash"),
                            json.dumps(payload.get("metadata") or {}, ensure_ascii=False, sort_keys=True),
                        ],
                    )
                    rows_loaded += 1
                    for industry in payload.get("industries") or []:
                        conn.execute(
                            """
                            INSERT INTO raw_content_post_industries
                            (job_id, url, industry, harvested_at)
                            VALUES (?, ?, ?, ?)
                            """,
                            [
                                payload.get("job_id"),
                                payload.get("url"),
                                industry,
                                payload.get("harvested_at"),
                            ],
                        )
                        industry_rows_loaded += 1
        conn.execute(
            """
            CREATE OR REPLACE TABLE content_posts AS
            WITH ranked AS (
                SELECT *,
                       ROW_NUMBER() OVER (
                           PARTITION BY url
                           ORDER BY COALESCE(published_at, harvested_at) DESC, harvested_at DESC, job_id DESC
                       ) AS row_no
                FROM raw_content_posts
            )
            SELECT job_id, url, title, author_name, author_url, published_at, text, hook, structure, word_count,
                   reaction_count, comment_count, repost_count, owned_by_me, outcome_score, source_query, harvested_at,
                   content_hash, metadata_json
            FROM ranked
            WHERE row_no = 1
            """
        )
        conn.execute(
            """
            CREATE OR REPLACE TABLE content_post_industries AS
            SELECT DISTINCT p.url, i.industry
            FROM raw_content_post_industries AS i
            INNER JOIN content_posts AS p ON p.url = i.url
            """
        )
        post_count = int(conn.execute("SELECT COUNT(*) FROM content_posts").fetchone()[0] or 0)
    finally:
        conn.close()
    return {
        "warehouse_path": str(warehouse_db_path()),
        "files_processed": len(files),
        "rows_loaded": rows_loaded,
        "industry_rows_loaded": industry_rows_loaded,
        "post_count": post_count,
    }


def warehouse_stats(*, industry: str | None = None) -> dict[str, Any]:
    conn = _warehouse_connect(read_only=True)
    try:
        if not _table_exists(conn, "content_posts") or not _table_exists(conn, "content_post_industries"):
            return {
                "warehouse_path": str(warehouse_db_path()),
                "post_count": 0,
                "industries": {},
                "job_counts": {},
            }
        if industry:
            post_count = int(
                conn.execute(
                    """
                    SELECT COUNT(DISTINCT p.url)
                    FROM content_posts AS p
                    INNER JOIN content_post_industries AS i ON i.url = p.url
                    WHERE i.industry = ?
                    """,
                    [industry],
                ).fetchone()[0]
                or 0
            )
        else:
            post_count = int(conn.execute("SELECT COUNT(*) FROM content_posts").fetchone()[0] or 0)
        industry_rows = conn.execute(
            """
            SELECT industry, COUNT(DISTINCT url) AS count
            FROM content_post_industries
            GROUP BY industry
            ORDER BY count DESC, industry ASC
            """
        ).fetchall()
        job_rows = conn.execute(
            """
            SELECT job_id, COUNT(*) AS count
            FROM raw_content_posts
            GROUP BY job_id
            ORDER BY count DESC, job_id ASC
            LIMIT 20
            """
        ).fetchall()
        return {
            "warehouse_path": str(warehouse_db_path()),
            "post_count": post_count,
            "industries": {str(row[0]): int(row[1]) for row in industry_rows},
            "job_counts": {str(row[0]): int(row[1]) for row in job_rows},
        }
    finally:
        conn.close()


def build_training_dataset(
    *,
    output_dir: str | Path,
    industries: list[str] | None = None,
    train_ratio: float = 0.8,
    val_ratio: float = 0.1,
) -> dict[str, Any]:
    conn = _warehouse_connect(read_only=True)
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
                   p.word_count, p.reaction_count, p.comment_count, p.repost_count, p.owned_by_me, p.outcome_score,
                   p.source_query, p.harvested_at, p.content_hash, p.metadata_json,
                   COALESCE(
                     (SELECT LIST(industry ORDER BY industry ASC) FROM content_post_industries AS i WHERE i.url = p.url),
                     []
                   ) AS industries
            FROM content_posts AS p
            {where_sql}
            ORDER BY COALESCE(p.published_at, p.harvested_at) ASC, p.url ASC
            """,
            params,
        ).fetchall()
    finally:
        conn.close()

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    row_count = len(rows)
    train_cutoff = max(0, min(row_count, int(row_count * train_ratio)))
    val_cutoff = max(train_cutoff, min(row_count, train_cutoff + int(row_count * val_ratio)))
    splits = {
        "train": rows[:train_cutoff],
        "val": rows[train_cutoff:val_cutoff],
        "test": rows[val_cutoff:],
    }
    for split_name, split_rows in splits.items():
        destination = output_path / f"{split_name}.jsonl"
        with destination.open("w", encoding="utf-8") as handle:
            for row in split_rows:
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
                    "owned_by_me": bool(row[12]),
                    "outcome_score": float(row[13] or 0.0),
                    "source_query": row[14],
                    "harvested_at": row[15],
                    "content_hash": row[16],
                    "metadata": json.loads(row[17] or "{}"),
                    "industries": list(row[18] or []),
                }
                handle.write(json.dumps(payload, ensure_ascii=False, sort_keys=True) + "\n")
    return {
        "output_dir": str(output_path),
        "row_count": row_count,
        "splits": {name: len(split_rows) for name, split_rows in splits.items()},
    }


def build_reward_dataset(
    *,
    output_dir: str | Path,
    industries: list[str] | None = None,
    owned_only: bool = False,
    train_ratio: float = 0.8,
    val_ratio: float = 0.1,
) -> dict[str, Any]:
    conn = _warehouse_connect(read_only=True)
    try:
        if not _table_exists(conn, "content_posts") or not _table_exists(conn, "content_post_industries"):
            fail("Local content warehouse is empty. Run `content materialize` first.", code=ExitCode.NOT_FOUND)
        where: list[str] = []
        params: list[Any] = []
        if industries:
            placeholders = ",".join("?" for _ in industries)
            where.append(
                "p.url IN (SELECT DISTINCT url FROM content_post_industries WHERE industry IN (" + placeholders + "))"
            )
            params.extend(industries)
        if owned_only:
            where.append("p.owned_by_me = TRUE")
        where_sql = f"WHERE {' AND '.join(where)}" if where else ""
        rows = conn.execute(
            f"""
            SELECT p.url, p.title, p.text, p.hook, p.structure, p.word_count, p.reaction_count, p.comment_count,
                   p.repost_count, p.owned_by_me, p.outcome_score, p.published_at, p.harvested_at, p.source_query,
                   p.metadata_json,
                   COALESCE(
                     (SELECT LIST(industry ORDER BY industry ASC) FROM content_post_industries AS i WHERE i.url = p.url),
                     []
                   ) AS industries
            FROM content_posts AS p
            {where_sql}
            ORDER BY COALESCE(p.published_at, p.harvested_at) ASC, p.url ASC
            """,
            params,
        ).fetchall()
    finally:
        conn.close()

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    row_count = len(rows)
    train_cutoff = max(0, min(row_count, int(row_count * train_ratio)))
    val_cutoff = max(train_cutoff, min(row_count, train_cutoff + int(row_count * val_ratio)))
    splits = {
        "train": rows[:train_cutoff],
        "val": rows[train_cutoff:val_cutoff],
        "test": rows[val_cutoff:],
    }
    for split_name, split_rows in splits.items():
        destination = output_path / f"{split_name}.jsonl"
        with destination.open("w", encoding="utf-8") as handle:
            for row in split_rows:
                payload = {
                    "url": row[0],
                    "title": row[1],
                    "text": row[2],
                    "hook": row[3],
                    "structure": row[4],
                    "word_count": int(row[5] or 0),
                    "reaction_count": int(row[6] or 0),
                    "comment_count": int(row[7] or 0),
                    "repost_count": int(row[8] or 0),
                    "owned_by_me": bool(row[9]),
                    "reward": float(row[10] or 0.0),
                    "published_at": row[11],
                    "harvested_at": row[12],
                    "source_query": row[13],
                    "metadata": json.loads(row[14] or "{}"),
                    "industries": list(row[15] or []),
                }
                handle.write(json.dumps(payload, ensure_ascii=False, sort_keys=True) + "\n")
    return {
        "output_dir": str(output_path),
        "row_count": row_count,
        "splits": {name: len(split_rows) for name, split_rows in splits.items()},
        "owned_only": owned_only,
        "industries": list(industries or []),
    }


def build_policy_dataset(
    *,
    output_dir: str | Path,
    policy_name: str,
    context_type: str | None = None,
    train_ratio: float = 0.8,
    val_ratio: float = 0.1,
) -> dict[str, Any]:
    from linkedin_cli import policy

    policy.init_policy_db()
    decisions = [item for item in policy._reward_totals(policy_name=policy_name, context_type=context_type) if item["reward_count"] > 0]
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    row_count = len(decisions)
    train_cutoff = max(0, min(row_count, int(row_count * train_ratio)))
    val_cutoff = max(train_cutoff, min(row_count, train_cutoff + int(row_count * val_ratio)))
    splits = {
        "train": decisions[:train_cutoff],
        "val": decisions[train_cutoff:val_cutoff],
        "test": decisions[val_cutoff:],
    }
    for split_name, split_rows in splits.items():
        destination = output_path / f"{split_name}.jsonl"
        with destination.open("w", encoding="utf-8") as handle:
            for item in split_rows:
                payload = {
                    "decision_id": item["decision_id"],
                    "policy_name": item["policy_name"],
                    "context_type": item["context_type"],
                    "context_key": item["context_key"],
                    "context_features": item["context_features"],
                    "available_actions": item["available_actions"],
                    "chosen_action_id": item["chosen_action_id"],
                    "propensity": float(item["propensity"] or 0.0),
                    "reward": float(item["total_reward"] or 0.0),
                    "reward_count": int(item["reward_count"] or 0),
                    "metadata": item.get("metadata") or {},
                    "created_at": item["created_at"],
                }
                handle.write(json.dumps(payload, ensure_ascii=False, sort_keys=True) + "\n")
    return {
        "output_dir": str(output_path),
        "row_count": row_count,
        "splits": {name: len(split_rows) for name, split_rows in splits.items()},
        "policy_name": policy_name,
        "context_type": context_type,
    }


def generate_benchmark_corpus(
    *,
    job_id: str,
    row_count: int,
    industries: list[str] | None = None,
    topics: list[str] | None = None,
) -> dict[str, Any]:
    count = max(1, int(row_count))
    normalized_industries = list(dict.fromkeys([value for value in (industries or ["ai"]) if value])) or ["ai"]
    normalized_topics = list(dict.fromkeys([value for value in (topics or ["agents"]) if value])) or ["agents"]
    shard = shard_file_path(job_id)
    shard.parent.mkdir(parents=True, exist_ok=True)
    with shard.open("w", encoding="utf-8") as handle:
        for index in range(count):
            industry = normalized_industries[index % len(normalized_industries)]
            topic = normalized_topics[index % len(normalized_topics)]
            published_day = 1 + (index % 28)
            payload = {
                "job_id": job_id,
                "url": f"https://www.linkedin.com/posts/benchmark-{job_id}-{index}",
                "title": f"{industry.title()} {topic.title()} benchmark post {index}",
                "author_name": f"Benchmark Author {index % 1000}",
                "author_url": f"https://www.linkedin.com/in/benchmark-author-{index % 1000}",
                "published_at": f"2026-03-{published_day:02d}T10:00:00Z",
                "text": f"{industry} {topic} benchmark content row {index}. This is synthetic corpus data for local warehouse benchmarking.",
                "hook": f"{industry.title()} {topic.title()} benchmark post {index}.",
                "structure": "insight" if index % 3 else "question",
                "word_count": 16,
                "reaction_count": int(10 + (index % 200)),
                "comment_count": int(1 + (index % 30)),
                "repost_count": int(index % 12),
                "owned_by_me": bool(index % 17 == 0),
                "outcome_score": float(5 + (index % 120) / 10.0),
                "source_query": f'site:linkedin.com/posts "{industry} {topic}"',
                "industries": [industry],
                "metadata": {"synthetic": True, "topic": topic, "row_index": index},
                "harvested_at": f"2026-03-{published_day:02d}T12:00:00Z",
                "content_hash": hashlib.sha256(f"{job_id}:{index}".encode("utf-8")).hexdigest(),
            }
            handle.write(json.dumps(payload, ensure_ascii=False, sort_keys=True) + "\n")
    return {
        "job_id": job_id,
        "row_count": count,
        "industries": normalized_industries,
        "topics": normalized_topics,
        "shard_path": str(shard),
        "shard_bytes": shard.stat().st_size,
    }


def _rss_megabytes() -> float:
    rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    if os.uname().sysname.lower() == "darwin":
        return round(float(rss) / (1024 * 1024), 2)
    return round(float(rss) / 1024, 2)


def benchmark_warehouse(
    *,
    job_id: str,
    dataset_output: str | Path,
    industries: list[str] | None = None,
) -> dict[str, Any]:
    shard = shard_file_path(job_id)
    if not shard.exists():
        fail(f"Benchmark shard not found for job: {job_id}", code=ExitCode.NOT_FOUND)

    generated_row_count = sum(1 for line in shard.read_text(encoding="utf-8").splitlines() if line.strip())

    started = time.perf_counter()
    materialize = materialize_shards(job_id=job_id)
    materialize_seconds = round(time.perf_counter() - started, 4)

    started = time.perf_counter()
    stats = warehouse_stats(industry=industries[0] if industries and len(industries) == 1 else None)
    warehouse_stats_seconds = round(time.perf_counter() - started, 4)

    started = time.perf_counter()
    dataset = build_training_dataset(output_dir=dataset_output, industries=industries)
    build_dataset_seconds = round(time.perf_counter() - started, 4)

    warehouse_path = Path(materialize["warehouse_path"])
    dataset_dir = Path(dataset["output_dir"])
    report = {
        "job_id": job_id,
        "generated_row_count": generated_row_count,
        "materialize_seconds": materialize_seconds,
        "warehouse_stats_seconds": warehouse_stats_seconds,
        "build_dataset_seconds": build_dataset_seconds,
        "warehouse_path": str(warehouse_path),
        "warehouse_bytes": warehouse_path.stat().st_size if warehouse_path.exists() else 0,
        "shard_path": str(shard),
        "shard_bytes": shard.stat().st_size,
        "dataset_output": str(dataset_dir),
        "dataset_bytes": sum(path.stat().st_size for path in dataset_dir.glob("*.jsonl") if path.exists()),
        "peak_rss_mb": _rss_megabytes(),
        "materialize": materialize,
        "warehouse_stats": stats,
        "dataset": dataset,
    }
    report_path = benchmark_report_path(job_id)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    report["report_path"] = str(report_path)
    return report


def benchmark_reports(limit: int = 20) -> list[dict[str, Any]]:
    reports: list[dict[str, Any]] = []
    for path in sorted(benchmark_dir().glob("*.json"), key=lambda item: item.stat().st_mtime, reverse=True)[: max(1, int(limit))]:
        payload = json.loads(path.read_text(encoding="utf-8"))
        payload["report_path"] = str(path)
        reports.append(payload)
    return reports


def train_warehouse_model(
    *,
    name: str,
    industries: list[str] | None = None,
    min_samples: int = 100,
    max_rows: int = 100000,
) -> dict[str, Any]:
    from linkedin_cli import content, modeling

    conn = _warehouse_connect(read_only=True)
    try:
        if not _table_exists(conn, "content_posts"):
            return {
                "trained": False,
                "model_name": name,
                "sample_count": 0,
                "min_samples": int(min_samples),
                "reason": "No warehouse content has been materialized yet.",
            }
        rows = conn.execute(
            """
            SELECT
                cp.url,
                cp.title,
                cp.text,
                cp.word_count,
                cp.outcome_score
            FROM content_posts AS cp
            WHERE (? IS NULL OR EXISTS (
                SELECT 1
                FROM content_post_industries AS cpi
                WHERE cpi.url = cp.url
                  AND cpi.industry IN (SELECT UNNEST(?))
            ))
            ORDER BY cp.outcome_score DESC, cp.url ASC
            LIMIT ?
            """,
            [industries or None, industries or None, max(1, int(max_rows))],
        ).fetchall()
    finally:
        conn.close()
    if len(rows) < max(1, int(min_samples)):
        return {
            "trained": False,
            "model_name": name,
            "sample_count": len(rows),
            "min_samples": int(min_samples),
            "reason": "Not enough warehouse rows matched the requested filters.",
        }

    scores = sorted(float(row[4] or 0.0) for row in rows)
    threshold_index = max(0, int(math.floor(0.75 * (len(scores) - 1))))
    threshold = scores[threshold_index]
    feature_names = content._content_feature_names()
    samples: list[dict[str, Any]] = []
    for row in rows:
        text = str(row[2] or "")
        title = str(row[1] or "")
        fingerprint = content._compute_content_fingerprint(text, title=title)
        vector = content._content_feature_vector(
            text=text,
            title=title,
            word_count=int(row[3] or 0),
            fingerprint=fingerprint,
        )
        samples.append(
            {
                "label": 1.0 if float(row[4] or 0.0) >= threshold else 0.0,
                "features": {feature_names[index]: float(value) for index, value in enumerate(vector)},
                "metadata": {"url": row[0]},
            }
        )
    summary = modeling.train_model(
        task="content_viral_bucket",
        samples=samples,
        artifact_dir=store.ARTIFACTS_DIR / "models" / "warehouse-content",
        model_name=name,
    )
    summary["threshold"] = float(threshold)
    summary["industries"] = list(industries or [])
    return summary


def get_warehouse_model(name: str) -> dict[str, Any] | None:
    from linkedin_cli import modeling

    return modeling.get_model(name)
