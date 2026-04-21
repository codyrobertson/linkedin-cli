from __future__ import annotations

import json
import math
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import duckdb

from linkedin_cli import content_stack
from linkedin_cli import modeling
from linkedin_cli.write import store


class ContentStackedRankingTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tempdir.name) / "state.sqlite"
        self.db_patcher = patch.object(store, "DB_PATH", self.db_path)
        self.db_patcher.start()

        from linkedin_cli import content_warehouse

        self.content_warehouse = content_warehouse

        conn = self.content_warehouse._warehouse_connect()
        try:
            conn.execute(
                """
                CREATE TABLE content_posts (
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
                CREATE TABLE content_post_industries (
                    url VARCHAR,
                    industry VARCHAR
                )
                """
            )
            conn.execute(
                """
                INSERT INTO content_posts VALUES
                (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?),
                (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    "job-1",
                    "https://www.linkedin.com/posts/a-1",
                    "Post A",
                    "Alice",
                    "https://www.linkedin.com/in/alice",
                    "2026-03-27T10:00:00Z",
                    "Alice post text",
                    "Alice post hook",
                    "list",
                    120,
                    42,
                    7,
                    3,
                    True,
                    4.2,
                    'site:linkedin.com/posts "alice"',
                    "2026-03-27T10:05:00Z",
                    "hash-a",
                    '{"query_industries":["ai"],"query_topics":["workflow"]}',
                    "job-1",
                    "https://www.linkedin.com/posts/b-1",
                    "Post B",
                    "Bob",
                    "https://www.linkedin.com/in/bob",
                    "2026-03-27T11:00:00Z",
                    "Bob post text",
                    "Bob post hook",
                    "insight",
                    160,
                    18,
                    5,
                    1,
                    False,
                    2.8,
                    'site:linkedin.com/posts "bob"',
                    "2026-03-27T11:05:00Z",
                    "hash-b",
                    '{"query_industries":["marketing"],"query_topics":["growth"]}',
                ],
            )
            conn.execute(
                """
                INSERT INTO content_post_industries VALUES
                (?, ?),
                (?, ?),
                (?, ?)
                """,
                [
                    "https://www.linkedin.com/posts/a-1",
                    "ai",
                    "https://www.linkedin.com/posts/a-1",
                    "devtools",
                    "https://www.linkedin.com/posts/b-1",
                    "marketing",
                ],
            )
            conn.commit()
        finally:
            conn.close()

    def tearDown(self) -> None:
        self.db_patcher.stop()
        self.tempdir.cleanup()

    def test_build_foundation_views_materializes_expected_relations(self) -> None:
        summary = self.content_warehouse.build_foundation_views()

        self.assertEqual(summary["post_count"], 2)
        self.assertIn("content_foundation_posts", summary["views"])
        self.assertIn("content_foundation_author_stats", summary["views"])
        self.assertIn("content_foundation_industry_stats", summary["views"])
        self.assertIn("content_head_public_performance", summary["views"])
        self.assertIn("content_head_persona_style", summary["views"])
        self.assertIn("content_head_business_intent", summary["views"])

        conn = self.content_warehouse._warehouse_connect(read_only=True)
        try:
            view_rows = conn.execute(
                """
                SELECT table_name, table_type
                FROM information_schema.tables
                WHERE table_name IN (
                    'content_foundation_posts',
                    'content_foundation_author_stats',
                    'content_foundation_industry_stats',
                    'content_head_public_performance',
                    'content_head_persona_style',
                    'content_head_business_intent'
                )
                ORDER BY table_name
                """
            ).fetchall()
            self.assertEqual(
                [(row[0], row[1]) for row in view_rows],
                [
                    ("content_foundation_author_stats", "VIEW"),
                    ("content_foundation_industry_stats", "VIEW"),
                    ("content_foundation_posts", "VIEW"),
                    ("content_head_business_intent", "VIEW"),
                    ("content_head_persona_style", "VIEW"),
                    ("content_head_public_performance", "VIEW"),
                ],
            )

            post_rows = conn.execute(
                """
                SELECT
                    url,
                    author_name,
                    industries,
                    query_industries,
                    query_topics,
                    normalized_engagement_score,
                    proof_level,
                    tone,
                    cta_type,
                    author_archetype,
                    freshness_bucket
                FROM content_foundation_posts
                ORDER BY url
                """
            ).fetchall()
            self.assertEqual(len(post_rows), 2)
            self.assertEqual(post_rows[0][0], "https://www.linkedin.com/posts/a-1")
            self.assertEqual(post_rows[0][2], ["ai", "devtools"])
            self.assertEqual(post_rows[0][3], ["ai"])
            self.assertEqual(post_rows[0][4], ["workflow"])
            self.assertAlmostEqual(post_rows[0][5], (42 + (2 * 7) + (3 * 3)) / 120, places=6)
            self.assertEqual(post_rows[0][6], "low")
            self.assertEqual(post_rows[0][7], "assertive")
            self.assertEqual(post_rows[0][8], "none")
            self.assertEqual(post_rows[0][9], "creator")
            self.assertEqual(post_rows[0][10], "recent")

            author_rows = conn.execute(
                """
                SELECT author_name, post_count
                FROM content_foundation_author_stats
                ORDER BY author_name
                """
            ).fetchall()
            self.assertEqual(author_rows, [("Alice", 1), ("Bob", 1)])
        finally:
            conn.close()

    def test_build_foundation_views_filters_industry_stats_to_requested_industry(self) -> None:
        summary = self.content_warehouse.build_foundation_views(industries=["ai"])

        self.assertEqual(summary["post_count"], 1)
        self.assertEqual(summary["industries"], ["ai"])

        conn = self.content_warehouse._warehouse_connect(read_only=True)
        try:
            rows = conn.execute(
                """
                SELECT industry, post_count
                FROM content_foundation_industry_stats
                ORDER BY industry
                """
            ).fetchall()
            self.assertEqual(rows, [("ai", 1)])
        finally:
            conn.close()

    def test_label_public_performance_uses_conditional_baselines(self) -> None:
        rows = [
            {
                "url": "https://www.linkedin.com/posts/a-1",
                "reaction_count": 100,
                "comment_count": 10,
                "repost_count": 5,
                "query_topics": ["workflow"],
                "industries": ["ai"],
                "freshness_bucket": "recent",
            },
            {
                "url": "https://www.linkedin.com/posts/a-2",
                "reaction_count": 40,
                "comment_count": 2,
                "repost_count": 1,
                "query_topics": ["workflow"],
                "industries": ["ai"],
                "freshness_bucket": "recent",
            },
            {
                "url": "https://www.linkedin.com/posts/b-1",
                "reaction_count": 60,
                "comment_count": 4,
                "repost_count": 1,
                "query_topics": ["growth"],
                "industries": ["marketing"],
                "freshness_bucket": "archive",
            },
            {
                "url": "https://www.linkedin.com/posts/c-1",
                "reaction_count": 20,
                "comment_count": 1,
                "repost_count": 0,
                "query_topics": ["ops"],
                "industries": ["sales"],
                "freshness_bucket": "year",
            },
        ]

        labeled = content_stack.label_public_performance(rows)
        by_url = {row["url"]: row for row in labeled}

        self.assertEqual(len(labeled), 4)
        self.assertAlmostEqual(by_url["https://www.linkedin.com/posts/a-1"]["raw_engagement"], 135.0)
        self.assertAlmostEqual(by_url["https://www.linkedin.com/posts/a-2"]["raw_engagement"], 47.0)
        self.assertAlmostEqual(by_url["https://www.linkedin.com/posts/b-1"]["raw_engagement"], 71.0)
        self.assertAlmostEqual(by_url["https://www.linkedin.com/posts/c-1"]["raw_engagement"], 22.0)
        self.assertAlmostEqual(by_url["https://www.linkedin.com/posts/a-1"]["engagement_signal"], math.log1p(135.0))
        self.assertAlmostEqual(by_url["https://www.linkedin.com/posts/a-2"]["engagement_signal"], math.log1p(47.0))
        self.assertAlmostEqual(
            by_url["https://www.linkedin.com/posts/a-1"]["expected_engagement_signal"],
            math.log1p(47.0),
        )
        self.assertAlmostEqual(
            by_url["https://www.linkedin.com/posts/a-2"]["expected_engagement_signal"],
            math.log1p(135.0),
        )
        self.assertAlmostEqual(
            by_url["https://www.linkedin.com/posts/a-1"]["overperformed_score"],
            math.log1p(135.0) - math.log1p(47.0),
        )
        self.assertAlmostEqual(
            by_url["https://www.linkedin.com/posts/a-2"]["overperformed_score"],
            math.log1p(47.0) - math.log1p(135.0),
        )
        self.assertLess(abs(by_url["https://www.linkedin.com/posts/a-1"]["public_performance_score"]), 5.0)
        self.assertLess(abs(by_url["https://www.linkedin.com/posts/a-2"]["public_performance_score"]), 5.0)
        self.assertEqual(by_url["https://www.linkedin.com/posts/a-1"]["overperformed_label"], 1)
        self.assertEqual(by_url["https://www.linkedin.com/posts/a-2"]["overperformed_label"], 0)
        self.assertEqual(by_url["https://www.linkedin.com/posts/b-1"]["overperformed_label"], 1)
        self.assertEqual(by_url["https://www.linkedin.com/posts/c-1"]["overperformed_label"], 0)

    def test_baseline_key_avoids_text_derived_proxy_axes(self) -> None:
        key = content_stack._baseline_key(
            {
                "query_topics": ["workflow", "ai"],
                "industries": ["ai"],
                "freshness_bucket": "recent",
                "author_archetype": "operator",
                "structure": "list",
                "tone": "assertive",
            }
        )

        self.assertIn("topics:ai|workflow", key)
        self.assertIn("industries:ai", key)
        self.assertIn("freshness_bucket:recent", key)
        self.assertNotIn("author_archetype:", key)
        self.assertNotIn("structure:", key)
        self.assertNotIn("tone:", key)

    def test_project_persona_style_exposes_named_axes(self) -> None:
        projection = content_stack.project_persona_style(
            {
                "tone": "assertive",
                "author_archetype": "operator",
                "cta_type": "none",
                "structure": "insight",
                "word_count": 180,
                "proof_level": "high",
                "text": "We used this workflow to cut CAC and ship faster.",
            }
        )

        self.assertEqual(
            sorted(projection["axes"].keys()),
            [
                "directive_vs_observational",
                "operator_vs_storyteller",
                "proof_heavy_vs_inspirational",
                "tactical_vs_visionary",
            ],
        )
        self.assertGreater(projection["axes"]["operator_vs_storyteller"], 0)
        self.assertGreater(projection["axes"]["tactical_vs_visionary"], 0)
        self.assertGreater(projection["axes"]["proof_heavy_vs_inspirational"], 0)
        self.assertGreater(projection["axes"]["directive_vs_observational"], 0)

    def test_label_business_intent_scores_commercial_posture(self) -> None:
        label = content_stack.label_business_intent(
            {
                "text": "DM me for the teardown. We used this workflow to cut CAC and improve pipeline.",
                "cta_type": "engagement",
                "author_archetype": "operator",
                "topics": ["marketing", "workflow"],
                "proof_level": "high",
            }
        )

        self.assertIn("business_intent_score", label)
        self.assertIn("business_intent_label", label)
        self.assertGreater(label["business_intent_score"], 0)
        self.assertEqual(label["business_intent_label"], 1)

    def test_label_business_intent_requires_more_than_soft_topic_overlap(self) -> None:
        label = content_stack.label_business_intent(
            {
                "text": "Our team learned a lot from this workflow.",
                "cta_type": "none",
                "author_archetype": "operator",
                "topics": ["workflow"],
                "proof_level": "low",
            }
        )

        self.assertGreater(label["business_intent_score"], 0)
        self.assertEqual(label["business_intent_label"], 0)

    def test_stacked_feature_dict_avoids_direct_proxy_label_one_hots(self) -> None:
        features = content_stack._stacked_feature_dict(
            {
                "title": "AI workflow teardown",
                "text": "DM me for the workflow teardown and benchmark.",
                "word_count": 42,
                "tone": "assertive",
                "cta_type": "engagement",
                "author_archetype": "operator",
                "proof_level": "high",
                "freshness_bucket": "recent",
                "topics": ["workflow"],
                "industries": ["ai"],
            }
        )

        disallowed_prefixes = (
            "tone:",
            "cta_type:",
            "author_archetype:",
            "proof_level:",
            "topic:",
            "industry:",
        )
        self.assertFalse(any(str(key).startswith(disallowed_prefixes) for key in features))

    def test_train_stacked_model_writes_three_head_artifact(self) -> None:
        artifact_dir = Path(self.tempdir.name) / "artifacts"
        summary = content_stack.train_stacked_model(
            model_name="foundation-v1",
            artifact_dir=artifact_dir,
            min_samples=2,
        )

        from linkedin_cli import modeling

        stored = modeling.get_model("foundation-v1")

        self.assertTrue(summary["trained"])
        self.assertEqual(
            set(summary["heads"]),
            {"public_performance", "persona_style", "business_intent"},
        )
        self.assertTrue(Path(summary["artifact_path"]).exists())
        self.assertIsNotNone(stored)
        self.assertEqual(stored["task"], "content_stacked_ranking")
        self.assertEqual(
            set((stored.get("heads") or {}).keys()),
            {"public_performance", "persona_style", "business_intent"},
        )
        self.assertEqual((stored.get("metadata") or {}).get("split_strategy"), "grouped")

    def test_train_stacked_model_reports_external_holdout_metrics(self) -> None:
        artifact_dir = Path(self.tempdir.name) / "artifacts"
        holdout_dir = Path(self.tempdir.name) / "holdouts"
        holdout_dir.mkdir(parents=True, exist_ok=True)
        self.content_warehouse.build_foundation_views()

        conn = self.content_warehouse._warehouse_connect(read_only=True)
        try:
            cursor = conn.execute("SELECT * FROM content_foundation_posts ORDER BY url ASC")
            columns = [str(column[0]) for column in (cursor.description or [])]
            rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
        finally:
            conn.close()

        split_payloads = {
            "train": rows,
            "val": rows[:1],
            "test": rows[1:],
            "time_holdout": rows[:1],
        }
        for split_name, split_rows in split_payloads.items():
            with (holdout_dir / f"{split_name}.jsonl").open("w", encoding="utf-8") as handle:
                for row in split_rows:
                    handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")

        summary = content_stack.train_stacked_model(
            model_name="foundation-holdout",
            artifact_dir=artifact_dir,
            holdout_dir=holdout_dir,
            min_samples=2,
        )

        self.assertTrue(summary["trained"])
        self.assertIn("holdout_metrics", summary)
        self.assertIn("val", summary["holdout_metrics"])
        self.assertIn("test", summary["holdout_metrics"])
        self.assertIn("time_holdout", summary["holdout_metrics"])
        self.assertEqual(summary["holdout_metrics"]["val"]["row_count"], 1)
        self.assertIn("accuracy", summary["holdout_metrics"]["val"]["public_performance"])
        self.assertIn("brier_score", summary["holdout_metrics"]["val"]["public_performance"])
        self.assertIn("roc_auc", summary["holdout_metrics"]["val"]["public_performance"])
        self.assertIn("ece", summary["holdout_metrics"]["val"]["public_performance"])
        self.assertIn("r2", summary["holdout_metrics"]["val"]["persona_style"])

    def test_rerank_posts_for_target_blends_foundation_heads(self) -> None:
        artifact_dir = Path(self.tempdir.name) / "artifacts"
        content_stack.train_stacked_model(
            model_name="foundation-v1",
            artifact_dir=artifact_dir,
            min_samples=2,
        )
        target = {
            "company": "Acme AI",
            "buyer_roles": ["vp engineering"],
            "industries": ["ai", "devtools"],
            "problem_keywords": ["workflow"],
            "preferred_cta": ["none"],
            "tone_constraints": ["assertive"],
        }
        conn = self.content_warehouse._warehouse_connect(read_only=True)
        try:
            posts = conn.execute("SELECT * FROM content_foundation_posts ORDER BY url ASC")
            columns = [str(column[0]) for column in (posts.description or [])]
            ranked = content_stack.rerank_for_target(
                posts=[dict(zip(columns, row)) for row in posts.fetchall()],
                target_profile=target,
                model_name="foundation-v1",
            )
        finally:
            conn.close()

        self.assertEqual(ranked[0]["url"], "https://www.linkedin.com/posts/a-1")
        self.assertGreaterEqual(ranked[0]["score_breakdown"]["public_performance"], 0.0)
        self.assertGreaterEqual(ranked[0]["score_breakdown"]["business_intent"], 0.0)
        self.assertGreaterEqual(ranked[0]["score_breakdown"]["target_similarity"], 0.0)

    def test_calibrated_rerank_weights_downweight_weaker_heads(self) -> None:
        model = {
            "metadata": {
                "holdout_metrics": {
                    "test": {
                        "public_performance": {"roc_auc": 0.54, "brier_score": 0.245, "ece": 0.08},
                        "persona_style": {"r2": 0.32},
                        "business_intent": {"r2": 0.18},
                    },
                    "time_holdout": {
                        "public_performance": {"roc_auc": 0.56, "brier_score": 0.242, "ece": 0.06},
                        "persona_style": {"r2": 0.41},
                        "business_intent": {"r2": 0.14},
                    },
                }
            }
        }

        calibrated = content_stack.calibrated_rerank_weights(
            model=model,
            base_weights=dict(content_stack.DEFAULT_RERANK_WEIGHTS),
        )

        self.assertAlmostEqual(sum(calibrated.values()), 1.0, places=6)
        self.assertGreater(calibrated["business_intent"], calibrated["public_performance"])
        self.assertGreater(calibrated["business_intent"], 0.15)
        self.assertGreater(calibrated["business_intent"], 0.0)
        self.assertGreater(calibrated["target_similarity"], 0.0)

    def test_select_best_stacked_model_prefers_stronger_holdout_metrics(self) -> None:
        modeling.init_modeling_db()
        artifact_dir = Path(self.tempdir.name) / "artifacts"
        artifact_dir.mkdir(parents=True, exist_ok=True)

        weak_path = artifact_dir / "weak.json"
        weak_path.write_text(
            json.dumps(
                {
                    "model_name": "weak-model",
                    "task": "content_stacked_ranking",
                    "kind": "multi_head_linear",
                    "feature_names": [],
                    "means": [],
                    "stds": [],
                    "heads": {},
                    "metadata": {
                        "holdout_metrics": {
                            "test": {
                                "public_performance": {"roc_auc": 0.52, "brier_score": 0.248, "ece": 0.09},
                                "persona_style": {"r2": 0.08},
                                "business_intent": {"r2": 0.04},
                            },
                            "time_holdout": {
                                "public_performance": {"roc_auc": 0.53, "brier_score": 0.247, "ece": 0.08},
                                "persona_style": {"r2": 0.09},
                                "business_intent": {"r2": 0.05},
                            },
                        }
                    },
                },
                ensure_ascii=False,
                sort_keys=True,
            ),
            encoding="utf-8",
        )
        strong_path = artifact_dir / "strong.json"
        strong_path.write_text(
            json.dumps(
                {
                    "model_name": "strong-model",
                    "task": "content_stacked_ranking",
                    "kind": "multi_head_linear",
                    "feature_names": [],
                    "means": [],
                    "stds": [],
                    "heads": {},
                    "metadata": {
                        "holdout_metrics": {
                            "test": {
                                "public_performance": {"roc_auc": 0.62, "brier_score": 0.241, "ece": 0.02},
                                "persona_style": {"r2": 0.28},
                                "business_intent": {"r2": 0.18},
                            },
                            "time_holdout": {
                                "public_performance": {"roc_auc": 0.63, "brier_score": 0.24, "ece": 0.03},
                                "persona_style": {"r2": 0.39},
                                "business_intent": {"r2": 0.15},
                            },
                        }
                    },
                },
                ensure_ascii=False,
                sort_keys=True,
            ),
            encoding="utf-8",
        )

        modeling._upsert_registry_record(
            model_name="weak-model",
            task="content_stacked_ranking",
            artifact_path=weak_path,
            metrics={},
            feature_schema=[],
        )
        modeling._upsert_registry_record(
            model_name="strong-model",
            task="content_stacked_ranking",
            artifact_path=strong_path,
            metrics={},
            feature_schema=[],
        )

        selected = content_stack.select_best_stacked_model()

        self.assertEqual(selected["model_name"], "strong-model")
        self.assertGreater(selected["selection_score"], 0.0)
        self.assertGreater(selected["head_quality"]["persona_style"], selected["head_quality"]["public_performance"])

    def test_audit_target_profiles_summarizes_calibrated_vs_raw_overlap(self) -> None:
        artifact_dir = Path(self.tempdir.name) / "artifacts"
        content_stack.train_stacked_model(
            model_name="foundation-v1",
            artifact_dir=artifact_dir,
            min_samples=2,
        )
        conn = self.content_warehouse._warehouse_connect(read_only=True)
        try:
            posts_cur = conn.execute("SELECT * FROM content_foundation_posts ORDER BY url ASC")
            columns = [str(column[0]) for column in (posts_cur.description or [])]
            posts = [dict(zip(columns, row)) for row in posts_cur.fetchall()]
        finally:
            conn.close()

        profiles = {
            "eng_ops_ai": {
                "company": "Acme AI",
                "buyer_roles": ["vp engineering"],
                "industries": ["ai", "devtools"],
                "problem_keywords": ["workflow"],
                "preferred_cta": ["none"],
                "tone_constraints": ["assertive"],
            }
        }

        summary = content_stack.audit_target_profiles(
            profiles=profiles,
            model_name="foundation-v1",
            posts=posts,
            limit=2,
        )

        self.assertEqual(summary["model_name"], "foundation-v1")
        self.assertEqual(summary["sample_count"], len(posts))
        self.assertEqual(len(summary["audits"]), 1)
        audit = summary["audits"][0]
        self.assertEqual(audit["profile_name"], "eng_ops_ai")
        self.assertIn("weights", audit)
        self.assertIn("top_overlap", audit)
        self.assertIn("calibrated_results", audit)
        self.assertIn("raw_results", audit)

    def test_score_text_for_target_scores_arbitrary_draft_text(self) -> None:
        artifact_dir = Path(self.tempdir.name) / "artifacts"
        content_stack.train_stacked_model(
            model_name="foundation-v1",
            artifact_dir=artifact_dir,
            min_samples=2,
        )

        scored = content_stack.score_text_for_target(
            text="AI workflow playbooks work when operators can prove the hours saved.",
            industry="ai",
            topics=["workflow"],
            target_profile={
                "company": "Acme AI",
                "buyer_roles": ["vp engineering"],
                "industries": ["ai"],
                "problem_keywords": ["workflow", "hours_saved"],
                "preferred_cta": ["none"],
                "tone_constraints": ["assertive"],
            },
            model_name="foundation-v1",
        )

        self.assertEqual(scored["model_name"], "foundation-v1")
        self.assertEqual(scored["industry"], "ai")
        self.assertEqual(scored["topics"], ["workflow"])
        self.assertIn("score_breakdown", scored)
        self.assertIn("weights_used", scored)
        self.assertIn("public_performance", scored["score_breakdown"])
        self.assertIn("persona_style", scored["score_breakdown"])
        self.assertIn("business_intent", scored["score_breakdown"])
        self.assertIn("target_similarity", scored["score_breakdown"])
        self.assertGreaterEqual(float(scored["final_score"]), 0.0)
        self.assertLessEqual(float(scored["final_score"]), 1.0)
