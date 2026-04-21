from __future__ import annotations

import sqlite3
import tempfile
import textwrap
import time
import unittest
from pathlib import Path
from unittest.mock import patch
import hashlib

import requests

from linkedin_cli.write import store


PUBLIC_POST_HTML = textwrap.dedent(
    """
    <html>
      <head>
        <title>AI agents are overrated until they save time | Claude Mackenzie</title>
        <meta property="og:description" content="Most AI agents fail because they start with demos, not workflows." />
        <script type="application/ld+json">
          {
            "@context": "http://schema.org",
            "@type": "SocialMediaPosting",
            "@id": "https://www.linkedin.com/posts/claude-mackenzie_agents-demo-activity-1",
            "headline": "AI agents are overrated until they save time",
            "articleBody": "AI agents are overrated until they save time.\\n\\n1. Start with the workflow.\\n2. Remove manual steps.\\n3. Measure saved hours.",
            "datePublished": "2026-03-20T10:00:00Z",
            "author": {
              "@type": "Person",
              "name": "Claude Mackenzie",
              "url": "https://www.linkedin.com/in/claude-mackenzie-06510b3b8/"
            },
            "interactionStatistic": [
              {"@type": "InteractionCounter", "interactionType": "http://schema.org/LikeAction", "userInteractionCount": 120},
              {"@type": "InteractionCounter", "interactionType": "http://schema.org/CommentAction", "userInteractionCount": 18}
            ]
          }
        </script>
      </head>
      <body></body>
    </html>
    """
)

AUTH_SEARCH_RESPONSE = {
    "data": {
        "data": {
            "searchDashClustersByAll": {
                "metadata": {"totalResultCount": 1, "primaryResultType": "CONTENT"},
                "elements": [
                    {
                        "items": [
                            {
                                "item": {
                                    "searchFeedUpdate": {
                                        "*update": "urn:li:fsd_update:(urn:li:activity:7442719393450479616,BLENDED_SEARCH_FEED,EMPTY,DEFAULT,false)"
                                    }
                                }
                            }
                        ]
                    }
                ],
            }
        }
    },
    "included": [
        {
            "entityUrn": "urn:li:fsd_update:(urn:li:activity:7442719393450479616,BLENDED_SEARCH_FEED,EMPTY,DEFAULT,false)",
            "$type": "com.linkedin.voyager.dash.feed.Update",
            "commentary": {
                "text": {
                    "text": "AI agents are finally useful when they finish real workflows."
                }
            },
            "actor": {
                "name": {"text": "Jon Frederick"},
                "description": {"text": "AI operator"},
                "subDescription": {"accessibilityText": "9 hours ago • Visible to anyone on or off LinkedIn"},
                "navigationContext": {"actionTarget": "https://www.linkedin.com/in/jon-frederick-ab1948148"}
            },
            "*socialDetail": "urn:li:fsd_socialDetail:(urn:li:activity:7442719393450479616,urn:li:activity:7442719393450479616,urn:li:highlightedReply:-)",
            "socialContent": {
                "shareUrl": "https://www.linkedin.com/posts/jon-frederick-ab1948148_example-post-activity-7442719393450479616-FK8J?utm_source=social_share_send"
            },
            "metadata": {
                "backendUrn": "urn:li:activity:7442719393450479616",
                "shareAudience": "PUBLIC",
            },
        }
        ,
        {
            "entityUrn": "urn:li:fsd_socialDetail:(urn:li:activity:7442719393450479616,urn:li:activity:7442719393450479616,urn:li:highlightedReply:-)",
            "$type": "com.linkedin.voyager.dash.social.SocialDetail",
            "*totalSocialActivityCounts": "urn:li:fsd_socialActivityCounts:urn:li:activity:7442719393450479616",
        },
        {
            "entityUrn": "urn:li:fsd_socialActivityCounts:urn:li:activity:7442719393450479616",
            "$type": "com.linkedin.voyager.dash.feed.SocialActivityCounts",
            "numLikes": 42,
            "numComments": 7,
            "numShares": 3,
        },
    ],
}


class ContentHarvestTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tempdir.name) / "state.sqlite"
        self.db_patcher = patch.object(store, "DB_PATH", self.db_path)
        self.db_patcher.start()
        store.init_db()

        from linkedin_cli import content

        self.content = content
        self.content.init_content_db()

    def tearDown(self) -> None:
        self.db_patcher.stop()
        self.tempdir.cleanup()

    def test_extract_public_post_fields(self) -> None:
        record = self.content.extract_post_record(
            url="https://www.linkedin.com/posts/claude-mackenzie_agents-demo-activity-1",
            html=PUBLIC_POST_HTML,
            source_query='site:linkedin.com/posts "ai agents"',
        )

        self.assertEqual(record["author_name"], "Claude Mackenzie")
        self.assertEqual(record["reaction_count"], 120)
        self.assertEqual(record["comment_count"], 18)
        self.assertEqual(record["hook"], "AI agents are overrated until they save time.")
        self.assertEqual(record["structure"], "list")
        self.assertGreater(record["word_count"], 10)

    def test_harvest_posts_persists_deduped_results(self) -> None:
        def fake_search(query: str, limit: int = 10) -> list[dict[str, str]]:
            if "ai agents" in query:
                return [
                    {
                        "title": "Agents post",
                        "url": "https://www.linkedin.com/posts/claude-mackenzie_agents-demo-activity-1",
                        "snippet": "demo",
                    }
                ]
            return [
                {
                    "title": "Agents post duplicate",
                    "url": "https://www.linkedin.com/posts/claude-mackenzie_agents-demo-activity-1",
                    "snippet": "duplicate",
                },
                {
                    "title": "Second post",
                    "url": "https://www.linkedin.com/posts/claude-mackenzie_agents-demo-activity-2",
                    "snippet": "second",
                },
            ]

        def fake_fetch(url: str) -> str:
            return PUBLIC_POST_HTML.replace("activity-1", url.rsplit("-", 1)[-1])

        summary = self.content.harvest_posts(
            industry="ai",
            limit=10,
            per_query=5,
            query_terms=["ai agents", "ai workflow"],
            auth_search_fn=None,
            search_fn=fake_search,
            fetch_html=fake_fetch,
        )

        posts = self.content.list_posts(limit=10)

        self.assertEqual(summary["stored_count"], 2)
        self.assertEqual(summary["unique_url_count"], 2)
        self.assertEqual(len(posts), 2)
        self.assertTrue(all(post["industry"] == "ai" for post in posts))

    def test_harvest_posts_persists_query_specific_industry_labels(self) -> None:
        def fake_search(_query: str, limit: int = 10) -> list[dict[str, str]]:
            return [
                {
                    "title": "Agents post",
                    "url": "https://www.linkedin.com/posts/claude-mackenzie_agents-demo-activity-1",
                    "snippet": "demo",
                }
            ]

        summary = self.content.harvest_posts(
            industries=["ai", "fintech"],
            limit=1,
            per_query=5,
            query_terms=['site:linkedin.com/posts "ai agents"'],
            auth_search_fn=None,
            search_fn=fake_search,
            fetch_html=lambda _url: PUBLIC_POST_HTML,
        )

        ai_posts = self.content.list_posts(limit=5, industry="ai")
        fintech_posts = self.content.list_posts(limit=5, industry="fintech")

        self.assertEqual(summary["stored_count"], 1)
        self.assertEqual(len(ai_posts), 1)
        self.assertEqual(len(fintech_posts), 0)
        self.assertEqual(ai_posts[0]["industries"], ["ai"])

    def test_harvest_posts_uses_searxng_public_search(self) -> None:
        class _Resp:
            status_code = 200

            @staticmethod
            def json() -> dict[str, object]:
                return {
                    "results": [
                        {
                            "title": "Agents post",
                            "url": "https://www.linkedin.com/posts/claude-mackenzie_agents-demo-activity-1",
                            "content": "demo",
                        }
                    ]
                }

        with patch("linkedin_cli.search.requests.Session.get", return_value=_Resp()) as get_mock:
            summary = self.content.harvest_posts(
                industry="ai",
                limit=1,
                per_query=5,
                query_terms=['site:linkedin.com/posts "ai agents"'],
                auth_search_fn=None,
                fetch_html=lambda _url: PUBLIC_POST_HTML,
                backend="public-only",
                public_search="searxng",
                searxng_url="http://127.0.0.1:8080",
                searxng_engines=["google"],
            )

        self.assertEqual(summary["stored_count"], 1)
        self.assertGreaterEqual(get_mock.call_count, 1)
        first_url = get_mock.call_args_list[0].kwargs["url"] if "url" in get_mock.call_args_list[0].kwargs else get_mock.call_args_list[0].args[0]
        first_params = get_mock.call_args_list[0].kwargs.get("params") or {}
        self.assertIn("/search", first_url)
        self.assertEqual(first_params.get("engines"), "google")

    def test_stats_summarize_harvested_posts(self) -> None:
        record = self.content.extract_post_record(
            url="https://www.linkedin.com/posts/claude-mackenzie_agents-demo-activity-1",
            html=PUBLIC_POST_HTML,
            source_query='site:linkedin.com/posts "ai agents"',
        )
        record["industry"] = "ai"
        self.content.upsert_post(record)

        stats = self.content.content_stats()

        self.assertEqual(stats["post_count"], 1)
        self.assertEqual(stats["industries"]["ai"], 1)
        self.assertIn("list", stats["structures"])
        self.assertEqual(stats["fingerprinted_count"], 1)

    def test_stats_include_all_post_industries(self) -> None:
        record = self.content.extract_post_record(
            url="https://www.linkedin.com/posts/claude-mackenzie_agents-demo-activity-1",
            html=PUBLIC_POST_HTML,
            source_query='site:linkedin.com/posts "ai agents"',
        )
        record["source_query"] = None
        record["industries"] = ["ai", "fintech"]
        self.content.upsert_post(record)

        stats = self.content.content_stats()

        self.assertEqual(stats["post_count"], 1)
        self.assertEqual(stats["industries"]["ai"], 1)
        self.assertEqual(stats["industries"]["fintech"], 1)

    def test_prioritize_campaign_queries_pushes_failed_queries_to_end(self) -> None:
        conn = store._connect()
        try:
            self.content._create_or_reset_harvest_job(
                conn,
                job_id="speedtest-001",
                queries=["good query", "bad query"],
                industries=["ai"],
                limit=10,
                per_query=5,
                search_timeout=10,
                fetch_workers=2,
                query_workers=2,
            )
            self.content._update_harvest_query(
                conn,
                "speedtest-001",
                1,
                status=self.content.HARVEST_JOB_STATUS_COMPLETED,
                stored_count=5,
                result_count=5,
            )
            self.content._update_harvest_query(
                conn,
                "speedtest-001",
                2,
                status=self.content.HARVEST_JOB_STATUS_FAILED,
                stored_count=0,
                result_count=5,
            )
            conn.commit()
        finally:
            conn.close()

        prioritized = self.content._prioritize_campaign_queries(
            ["unknown query", "bad query", "good query"],
            job_prefix="speedtest",
        )

        self.assertEqual(prioritized[0], "good query")
        self.assertEqual(prioritized[-1], "bad query")

    def test_harvest_campaign_speed_max_defers_post_processing_and_caps_auth_workers(self) -> None:
        captured: dict[str, object] = {}

        def fake_harvest_posts(**kwargs):
            captured.update(kwargs)
            return {
                "industry": "ai",
                "industries": ["ai"],
                "query_count": 2,
                "stored_count": 10,
                "unique_url_count": 10,
                "queries": ["good query", "unknown query"],
                "failed_query_count": 0,
                "failed_queries": [],
                "job": {
                    "job_id": "speed-max-001",
                    "status": self.content.HARVEST_JOB_STATUS_COMPLETED,
                    "stored_count": 10,
                    "unique_url_count": 10,
                },
            }

        with patch.object(self.content, "harvest_posts", side_effect=fake_harvest_posts):
            result = self.content.harvest_campaign(
                industries=["ai"],
                query_terms=["good query", "unknown query"],
                limit=10,
                per_query=5,
                per_job_limit=10,
                queries_per_job=2,
                backend="auth-only",
                materialize=True,
                embed=True,
                retrain_every=2,
                speed="max",
            )

        self.assertEqual(captured["query_workers"], 1)
        self.assertFalse(result["materialized_jobs"])
        self.assertFalse(result["embedded_jobs"])
        self.assertFalse(result["training_runs"])
        self.assertEqual(result["speed"], "max")
        self.assertTrue(result["deferred_post_processing"]["materialize"])
        self.assertTrue(result["deferred_post_processing"]["embed"])
        self.assertEqual(result["deferred_post_processing"]["retrain_every"], 2)

    def test_train_outcome_model_persists_coefficients_and_updates_scoring(self) -> None:
        for index, reactions in enumerate([30, 80, 140, 220], start=1):
            html = (
                PUBLIC_POST_HTML
                .replace("activity-1", f"activity-{index}")
                .replace("userInteractionCount\": 120", f"userInteractionCount\": {reactions}")
                .replace("userInteractionCount\": 18", f"userInteractionCount\": {max(4, reactions // 10)}")
            )
            record = self.content.extract_post_record(
                url=f"https://www.linkedin.com/posts/claude-mackenzie_agents-demo-activity-{index}",
                html=html,
                source_query='site:linkedin.com/posts "ai agents"',
            )
            record["industry"] = "ai"
            record["owned_by_me"] = True
            record["last_synced_at"] = "2026-03-26T00:00:00Z"
            self.content.upsert_post(record)

        trained = self.content.train_outcome_model(min_samples=3, scope="owned")
        model = self.content.get_trained_model()
        scored = self.content.score_draft(
            text="AI agents save time when they remove one manual workflow handoff.",
            industry="ai",
            model="local-hash-v1",
        )

        self.assertTrue(trained["trained"])
        self.assertEqual(trained["sample_count"], 4)
        self.assertIsNotNone(model)
        self.assertEqual(model["scope"], "owned")
        self.assertEqual(scored["prediction_source"], "trained_blend")
        self.assertGreater(scored["model_prediction"], 0)

    def test_embed_posts_respects_limit_above_five_thousand(self) -> None:
        conn = store._connect()
        try:
            for index in range(5005):
                record = {
                    "url": f"https://www.linkedin.com/posts/embed-scale-activity-{index}",
                    "industry": "ai",
                    "source_query": 'site:linkedin.com/posts "ai agents"',
                    "title": f"Embed Scale {index}",
                    "author_name": "Claude Mackenzie",
                    "author_url": "https://www.linkedin.com/in/claude-mackenzie-06510b3b8/",
                    "published_at": "2026-03-20T10:00:00Z",
                    "text": f"AI agents post {index} about workflow automation.",
                    "hook": f"AI agents post {index}.",
                    "structure": "insight",
                    "word_count": 7,
                    "reaction_count": index % 50,
                    "comment_count": index % 10,
                    "metadata": {},
                }
                self.content._upsert_post_conn(conn, record)
            conn.commit()
        finally:
            conn.close()

        summary = self.content.embed_posts(limit=6000, model="local-hash-v1", batch_size=512)
        stats = self.content.content_stats()

        self.assertEqual(summary["embedded_count"], 5005)
        self.assertEqual(stats["embedded_count"], 5005)

    def test_embed_posts_persists_completed_batches_before_failure(self) -> None:
        conn = store._connect()
        try:
            for index in range(3):
                record = {
                    "url": f"https://www.linkedin.com/posts/embed-resume-activity-{index}",
                    "industry": "ai",
                    "source_query": 'site:linkedin.com/posts "ai agents"',
                    "title": f"Embed Resume {index}",
                    "author_name": "Claude Mackenzie",
                    "author_url": "https://www.linkedin.com/in/claude-mackenzie-06510b3b8/",
                    "published_at": "2026-03-20T10:00:00Z",
                    "text": f"AI agents post {index} about workflow automation.",
                    "hook": f"AI agents post {index}.",
                    "structure": "insight",
                    "word_count": 7,
                    "reaction_count": index,
                    "comment_count": index,
                    "metadata": {},
                }
                self.content._upsert_post_conn(conn, record)
            conn.commit()
        finally:
            conn.close()

        calls = {"count": 0}

        def flaky_embed(texts: list[str], _model: str) -> list[list[float]]:
            calls["count"] += 1
            if calls["count"] == 2:
                raise RuntimeError("boom")
            return [[float(i)] * 4 for i, _ in enumerate(texts, start=1)]

        with self.assertRaises(RuntimeError):
            self.content.embed_posts(limit=3, model="local-hash-v1", batch_size=2, embed_fn=flaky_embed)

        stats = self.content.content_stats()
        self.assertEqual(stats["embedded_count"], 2)

    def test_train_outcome_model_supports_topic_only_slice(self) -> None:
        for index, reactions in enumerate([40, 90, 150, 240], start=1):
            record = {
                "url": f"https://www.linkedin.com/posts/topic-only-activity-{index}",
                "industry": "ai",
                "source_query": 'site:linkedin.com/posts "ai workflow"',
                "title": f"Workflow Post {index}",
                "author_name": "Claude Mackenzie",
                "author_url": "https://www.linkedin.com/in/claude-mackenzie-06510b3b8/",
                "published_at": "2026-03-20T10:00:00Z",
                "text": f"AI workflow automation post {index} with concrete proof.",
                "hook": f"AI workflow post {index}.",
                "structure": "insight",
                "word_count": 9,
                "reaction_count": reactions,
                "comment_count": max(4, reactions // 10),
                "owned_by_me": True,
                "last_synced_at": "2026-03-26T00:00:00Z",
                "metadata": {},
            }
            self.content.upsert_post(record)

        trained = self.content.train_outcome_model(min_samples=3, scope="all", topics=["workflow"])

        self.assertTrue(trained["trained"])
        self.assertEqual(trained["topics"], ["workflow"])

    def test_harvest_campaign_chunks_queries_and_materializes(self) -> None:
        calls: list[dict[str, object]] = []

        def fake_harvest_posts(**kwargs):
            calls.append({"kind": "harvest", **kwargs})
            job_name = str(kwargs["job_name"])
            return {
                "industry": kwargs.get("industry"),
                "industries": kwargs.get("industries") or [],
                "query_count": len(kwargs.get("query_terms") or []),
                "stored_count": 7,
                "unique_url_count": 7,
                "queries": list(kwargs.get("query_terms") or []),
                "job": {"job_id": job_name, "status": "completed", "stored_count": 7, "unique_url_count": 7},
            }

        def fake_materialize_shards(*, job_id: str | None = None):
            calls.append({"kind": "materialize", "job_id": job_id})
            return {"job_id": job_id, "rows_loaded": 7}

        def fake_embed_posts(**kwargs):
            calls.append({"kind": "embed", **kwargs})
            return {"embedded_count": 7, "model": kwargs["model"]}

        def fake_train_outcome_model(**kwargs):
            calls.append({"kind": "train", **kwargs})
            return {"trained": True, "model_name": kwargs["name"], "sample_count": 50}

        with patch.object(self.content, "harvest_posts", side_effect=fake_harvest_posts), patch(
            "linkedin_cli.content_warehouse.materialize_shards",
            side_effect=fake_materialize_shards,
        ), patch.object(self.content, "embed_posts", side_effect=fake_embed_posts), patch.object(
            self.content, "train_outcome_model", side_effect=fake_train_outcome_model
        ):
            summary = self.content.harvest_campaign(
                industries=["ai", "fintech"],
                topics=["agents", "workflow"],
                limit=20,
                per_job_limit=10,
                queries_per_job=3,
                job_prefix="campaign-smoke",
                materialize=True,
                embed=True,
                embed_model="local-hash-v1",
                embed_batch_size=8,
                retrain_every=2,
                train_model_name="campaign-model",
                train_scope="all",
                train_min_samples=5,
            )

        harvest_calls = [call for call in calls if call["kind"] == "harvest"]
        materialize_calls = [call for call in calls if call["kind"] == "materialize"]
        embed_calls = [call for call in calls if call["kind"] == "embed"]
        train_calls = [call for call in calls if call["kind"] == "train"]

        self.assertEqual(len(harvest_calls), 3)
        self.assertEqual([call["job_name"] for call in harvest_calls], ["campaign-smoke-001", "campaign-smoke-002", "campaign-smoke-003"])
        self.assertEqual([call["limit"] for call in harvest_calls], [10, 10, 6])
        self.assertEqual(len(materialize_calls), 3)
        self.assertEqual(len(embed_calls), 3)
        self.assertEqual(len(train_calls), 1)
        self.assertEqual(summary["stored_count"], 20)
        self.assertEqual(summary["job_count"], 3)
        self.assertEqual(summary["jobs"][0]["job"]["job_id"], "campaign-smoke-001")

    def test_rebuild_retrieval_index_persists_index_and_drives_retrieve(self) -> None:
        record = self.content.extract_post_record(
            url="https://www.linkedin.com/posts/claude-mackenzie_agents-demo-activity-1",
            html=PUBLIC_POST_HTML,
            source_query='site:linkedin.com/posts "ai agents"',
        )
        record["industry"] = "ai"
        self.content.upsert_post(record)
        self.content.embed_posts(limit=10, model="local-hash-v1", missing_only=False)

        summary = self.content.rebuild_retrieval_index(kind="all", model="local-hash-v1")
        results = self.content.retrieve_posts(
            query_text="AI agents save time by fixing workflows",
            limit=3,
            method="hybrid",
            model="local-hash-v1",
        )

        self.assertGreaterEqual(summary["count"], 2)
        self.assertTrue(any(item["kind"] == "semantic" for item in summary["rebuilt"]))
        self.assertTrue(any(Path(item["path"]).exists() for item in summary["rebuilt"]))
        self.assertEqual(results[0]["url"], record["url"])

    def test_harvest_posts_converts_search_timeouts_into_cli_errors(self) -> None:
        def failing_search(_query: str, limit: int = 10) -> list[dict[str, str]]:
            raise requests.exceptions.ConnectTimeout("search timed out")

        with self.assertRaises(SystemExit) as exc_info:
            self.content.harvest_posts(
                industry="ai",
                limit=10,
                per_query=5,
                query_terms=["site:linkedin.com/posts \"ai agents\""],
                auth_search_fn=None,
                search_fn=failing_search,
            )

        self.assertEqual(exc_info.exception.code, 7)

    def test_build_harvest_queries_combines_industry_and_topics(self) -> None:
        queries = self.content.build_harvest_queries(industry="ai", topics=["agents"])

        self.assertIn('site:linkedin.com/posts "ai agents"', queries)
        self.assertIn('site:linkedin.com/feed/update "ai agents"', queries)

    def test_build_harvest_queries_crosses_multiple_industries_and_topics(self) -> None:
        queries = self.content.build_harvest_queries(
            industries=["ai", "fintech"],
            topics=["agents", "workflow"],
        )

        self.assertIn('site:linkedin.com/posts "ai agents"', queries)
        self.assertIn('site:linkedin.com/posts "ai workflow"', queries)
        self.assertIn('site:linkedin.com/posts "fintech agents"', queries)
        self.assertIn('site:linkedin.com/posts "fintech workflow"', queries)
        self.assertIn('site:linkedin.com/posts "ai"', queries)
        self.assertIn('site:linkedin.com/posts "fintech"', queries)

    def test_build_harvest_queries_broad_expansion_materially_increases_surface(self) -> None:
        standard = self.content.build_harvest_queries(
            industries=["ai", "fintech"],
            topics=["agents", "workflow"],
            expansion="standard",
        )
        broad = self.content.build_harvest_queries(
            industries=["ai", "fintech"],
            topics=["agents", "workflow"],
            expansion="broad",
        )

        self.assertGreater(len(broad), len(standard))
        self.assertIn('site:linkedin.com/posts "ai agents case study"', broad)
        self.assertIn('site:linkedin.com/feed/update "fintech workflow benchmark"', broad)

    def test_build_harvest_queries_broad_prioritizes_pair_expansion_before_bare_terms(self) -> None:
        broad = self.content.build_harvest_queries(
            industries=["ai"],
            topics=["agents"],
            expansion="broad",
        )

        pair_index = broad.index('site:linkedin.com/posts "ai agents case study"')
        bare_industry_index = broad.index('site:linkedin.com/posts "ai"')
        bare_topic_index = broad.index('site:linkedin.com/posts "agents"')

        self.assertLess(pair_index, bare_industry_index)
        self.assertLess(pair_index, bare_topic_index)

    def test_build_harvest_queries_exhaustive_expands_beyond_broad(self) -> None:
        broad = self.content.build_harvest_queries(
            industries=["ai", "fintech"],
            topics=["agents", "workflow"],
            expansion="broad",
        )
        exhaustive = self.content.build_harvest_queries(
            industries=["ai", "fintech"],
            topics=["agents", "workflow"],
            expansion="exhaustive",
        )

        self.assertGreater(len(exhaustive), len(broad))
        self.assertIn('site:linkedin.com/posts "ai agents customer story"', exhaustive)
        self.assertIn('site:linkedin.com/feed/update "workflow fintech roi"', exhaustive)

    def test_build_harvest_queries_recursive_adds_unquoted_and_seed_queries(self) -> None:
        self.content._recursive_harvest_entities = lambda industries, topics, limit=12: [
            {"label": "Grace Gong", "kind": "author"},
            {"label": "OpenAI", "kind": "company"},
        ]

        recursive = self.content.build_harvest_queries(
            industries=["ai"],
            topics=["agents"],
            expansion="recursive",
        )

        self.assertIn("site:linkedin.com/posts ai agents", recursive)
        self.assertIn('site:linkedin.com/posts "Grace Gong" agents', recursive)
        self.assertIn('site:linkedin.com/feed/update "OpenAI" ai', recursive)

    def test_build_harvest_queries_recursive_expands_beyond_exhaustive(self) -> None:
        self.content._recursive_harvest_entities = lambda industries, topics, limit=12: [
            {"label": "Grace Gong", "kind": "author"},
        ]
        exhaustive = self.content.build_harvest_queries(
            industries=["ai"],
            topics=["agents"],
            expansion="exhaustive",
        )
        recursive = self.content.build_harvest_queries(
            industries=["ai"],
            topics=["agents"],
            expansion="recursive",
        )

        self.assertGreater(len(recursive), len(exhaustive))

    def test_build_harvest_queries_recursive_prioritizes_posts_and_unquoted_terms(self) -> None:
        self.content._recursive_harvest_entities = lambda industries, topics, limit=12: [
            {"label": "Grace Gong", "kind": "author"},
        ]

        recursive = self.content.build_harvest_queries(
            industries=["ai"],
            topics=["agents"],
            expansion="recursive",
        )

        posts_unquoted_index = recursive.index("site:linkedin.com/posts ai agents")
        posts_seed_index = recursive.index('site:linkedin.com/posts "Grace Gong" agents')
        feed_quoted_index = recursive.index('site:linkedin.com/feed/update "ai agents"')

        self.assertLess(posts_unquoted_index, feed_quoted_index)
        self.assertLess(posts_seed_index, feed_quoted_index)

    def test_build_harvest_queries_supports_freshness_buckets(self) -> None:
        queries = self.content.build_harvest_queries(
            industries=["ai"],
            topics=["agents"],
            expansion="standard",
            freshness_buckets=["recent", "quarter", "year"],
        )

        self.assertIn('site:linkedin.com/posts "ai agents today"', queries)
        self.assertIn('site:linkedin.com/posts "ai agents this week"', queries)
        self.assertIn('site:linkedin.com/posts "ai agents this quarter"', queries)
        self.assertIn('site:linkedin.com/posts "ai agents 2026"', queries)

    def test_prepare_backend_queries_collapses_auth_only_duplicates(self) -> None:
        prepared = self.content.prepare_backend_queries(
            [
                'site:linkedin.com/posts "ai agents"',
                'site:linkedin.com/feed/update "ai agents"',
                'site:linkedin.com/posts "ai workflow"',
            ],
            "auth-only",
        )

        self.assertEqual(prepared, ["ai agents", "ai workflow"])

    def test_query_yield_stats_aggregates_attempts_and_yield(self) -> None:
        conn = store._connect()
        try:
            conn.execute(
                """
                INSERT INTO content_harvest_jobs
                (job_id, created_at, updated_at, status, limit_value, per_query, search_timeout, fetch_workers, query_workers,
                 stored_count, unique_url_count, industries_json, queries_json, metadata_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "campaign-a-001",
                    "2026-03-26T00:00:00Z",
                    "2026-03-26T00:00:00Z",
                    "completed",
                    10,
                    5,
                    30,
                    1,
                    1,
                    5,
                    5,
                    '["ai"]',
                    '["ai agents"]',
                    "{}",
                ),
            )
            conn.execute(
                """
                INSERT INTO content_harvest_job_queries
                (job_id, query_index, query, status, stored_count, result_count, last_error, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                ("campaign-a-001", 1, "ai agents", "completed", 3, 5, None, "2026-03-26T00:00:00Z"),
            )
            conn.execute(
                """
                INSERT INTO content_harvest_jobs
                (job_id, created_at, updated_at, status, limit_value, per_query, search_timeout, fetch_workers, query_workers,
                 stored_count, unique_url_count, industries_json, queries_json, metadata_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "campaign-a-002",
                    "2026-03-26T01:00:00Z",
                    "2026-03-26T01:00:00Z",
                    "running",
                    10,
                    5,
                    30,
                    1,
                    1,
                    1,
                    1,
                    '["ai"]',
                    '["ai agents","ai workflow"]',
                    "{}",
                ),
            )
            conn.execute(
                """
                INSERT INTO content_harvest_job_queries
                (job_id, query_index, query, status, stored_count, result_count, last_error, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                ("campaign-a-002", 1, "ai agents", "failed", 0, 4, "timeout", "2026-03-26T01:00:00Z"),
            )
            conn.execute(
                """
                INSERT INTO content_harvest_job_queries
                (job_id, query_index, query, status, stored_count, result_count, last_error, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                ("campaign-a-002", 2, "ai workflow", "completed", 1, 4, None, "2026-03-26T01:00:00Z"),
            )
            conn.commit()
        finally:
            conn.close()

        stats = self.content.query_yield_stats(job_prefix="campaign-a", limit=10)

        self.assertEqual(stats[0]["query"], "ai agents")
        self.assertEqual(stats[0]["attempt_count"], 2)
        self.assertEqual(stats[0]["completed_count"], 1)
        self.assertEqual(stats[0]["failed_count"], 1)
        self.assertEqual(stats[0]["stored_count"], 3)
        self.assertEqual(stats[0]["result_count"], 9)
        self.assertAlmostEqual(stats[0]["yield_rate"], 3 / 9, places=4)

    def test_harvest_campaign_resumes_existing_jobs_and_prunes_low_yield_queries(self) -> None:
        conn = store._connect()
        try:
            conn.execute(
                """
                INSERT INTO content_harvest_jobs
                (job_id, created_at, updated_at, status, limit_value, per_query, search_timeout, fetch_workers, query_workers,
                 stored_count, unique_url_count, industries_json, queries_json, metadata_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "resume-campaign-001",
                    "2026-03-26T00:00:00Z",
                    "2026-03-26T00:00:00Z",
                    "completed",
                    10,
                    5,
                    30,
                    1,
                    1,
                    6,
                    6,
                    '["ai"]',
                    '["ai agents"]',
                    "{}",
                ),
            )
            conn.execute(
                """
                INSERT INTO content_harvest_job_queries
                (job_id, query_index, query, status, stored_count, result_count, last_error, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                ("resume-campaign-001", 1, "ai agents", "completed", 6, 10, None, "2026-03-26T00:00:00Z"),
            )
            conn.execute(
                """
                INSERT INTO content_harvest_jobs
                (job_id, created_at, updated_at, status, limit_value, per_query, search_timeout, fetch_workers, query_workers,
                 stored_count, unique_url_count, industries_json, queries_json, metadata_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "resume-campaign-999",
                    "2026-03-26T00:00:00Z",
                    "2026-03-26T00:00:00Z",
                    "completed",
                    10,
                    5,
                    30,
                    1,
                    1,
                    0,
                    0,
                    '["ai"]',
                    '["ai stale"]',
                    "{}",
                ),
            )
            conn.execute(
                """
                INSERT INTO content_harvest_job_queries
                (job_id, query_index, query, status, stored_count, result_count, last_error, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                ("resume-campaign-999", 1, "ai workflow", "completed", 0, 10, None, "2026-03-26T00:00:00Z"),
            )
            conn.commit()
        finally:
            conn.close()

        calls: list[dict[str, object]] = []

        def fake_harvest_posts(**kwargs):
            calls.append(kwargs)
            if kwargs.get("resume_job"):
                return {
                    "industry": "ai",
                    "industries": ["ai"],
                    "query_count": 1,
                    "stored_count": 2,
                    "unique_url_count": 2,
                    "queries": ["ai automation"],
                    "failed_query_count": 0,
                    "failed_queries": [],
                    "job": {"job_id": str(kwargs["resume_job"]), "status": "completed", "stored_count": 2, "unique_url_count": 2},
                }
            return {
                "industry": "ai",
                "industries": ["ai"],
                "query_count": len(kwargs.get("query_terms") or []),
                "stored_count": 4,
                "unique_url_count": 4,
                "queries": list(kwargs.get("query_terms") or []),
                "failed_query_count": 0,
                "failed_queries": [],
                "job": {"job_id": str(kwargs["job_name"]), "status": "completed", "stored_count": 4, "unique_url_count": 4},
            }

        with patch.object(self.content, "harvest_posts", side_effect=fake_harvest_posts):
            summary = self.content.harvest_campaign(
                industries=["ai"],
                topics=["agents", "automation", "workflow"],
                query_terms=["ai agents", "ai automation", "ai workflow"],
                limit=20,
                per_job_limit=10,
                queries_per_job=1,
                job_prefix="resume-campaign",
                resume=True,
                prune_min_yield=0.05,
                prune_min_attempts=1,
            )

        self.assertEqual(summary["resumed_job_count"], 1)
        self.assertEqual(summary["pruned_query_count"], 1)
        self.assertEqual(summary["pruned_queries"][0]["query"], "ai workflow")
        self.assertTrue(any(call.get("resume_job") == "resume-campaign-002" for call in calls))

    def test_harvest_campaign_stops_on_low_marginal_yield(self) -> None:
        calls: list[dict[str, object]] = []

        def fake_harvest_posts(**kwargs):
            calls.append(kwargs)
            return {
                "industry": "ai",
                "industries": ["ai"],
                "query_count": len(kwargs.get("query_terms") or []),
                "stored_count": 0,
                "unique_url_count": 0,
                "queries": list(kwargs.get("query_terms") or []),
                "failed_query_count": 0,
                "failed_queries": [],
                "job": {"job_id": str(kwargs["job_name"]), "status": "completed", "stored_count": 0, "unique_url_count": 0},
            }

        with patch.object(self.content, "harvest_posts", side_effect=fake_harvest_posts):
            summary = self.content.harvest_campaign(
                industries=["ai"],
                topics=["agents", "automation", "workflow"],
                query_terms=["ai agents", "ai automation", "ai workflow"],
                limit=100,
                per_query=10,
                per_job_limit=100,
                queries_per_job=1,
                job_prefix="stop-campaign",
                stop_min_yield_rate=0.05,
                stop_window=2,
            )

        self.assertTrue(summary["stopped_early"])
        self.assertEqual(summary["stop_reason"], "marginal_yield_below_threshold")
        self.assertEqual(len(calls), 2)

    def test_harvest_posts_emits_progress_events(self) -> None:
        events: list[dict[str, object]] = []

        def fake_search(query: str, limit: int = 10) -> list[dict[str, str]]:
            return [
                {
                    "title": "Agents post",
                    "url": "https://www.linkedin.com/posts/claude-mackenzie_agents-demo-activity-1",
                    "snippet": query,
                }
            ]

        def fake_fetch(_url: str) -> str:
            return PUBLIC_POST_HTML

        self.content.harvest_posts(
            industry="ai",
            limit=1,
            per_query=5,
            query_terms=['site:linkedin.com/posts "ai agents"'],
            auth_search_fn=None,
            search_fn=fake_search,
            fetch_html=fake_fetch,
            progress=events.append,
        )

        event_types = [str(event["event"]) for event in events]
        self.assertIn("query_started", event_types)
        self.assertIn("post_stored", event_types)
        self.assertIn("complete", event_types)

    def test_parse_authenticated_search_results_extracts_post_urls(self) -> None:
        results = self.content.parse_authenticated_search_results(AUTH_SEARCH_RESPONSE)

        self.assertEqual(len(results), 1)
        self.assertEqual(
            results[0]["url"],
            "https://www.linkedin.com/posts/jon-frederick-ab1948148_example-post-activity-7442719393450479616-FK8J",
        )
        self.assertEqual(results[0]["source"], "linkedin.auth")
        self.assertEqual(results[0]["author_name"], "Jon Frederick")
        self.assertEqual(results[0]["author_url"], "https://www.linkedin.com/in/jon-frederick-ab1948148")
        self.assertEqual(results[0]["reaction_count"], 42)
        self.assertEqual(results[0]["comment_count"], 7)
        self.assertEqual(results[0]["text"], "AI agents are finally useful when they finish real workflows.")

    def test_harvest_posts_falls_back_to_public_search_when_auth_search_fails(self) -> None:
        calls: list[str] = []

        def auth_search(_query: str, limit: int = 10) -> list[dict[str, str]]:
            calls.append("auth")
            raise RuntimeError("bad auth search")

        def public_search(query: str, limit: int = 10) -> list[dict[str, str]]:
            calls.append("public")
            return [
                {
                    "title": "Agents post",
                    "url": "https://www.linkedin.com/posts/claude-mackenzie_agents-demo-activity-1",
                    "snippet": query,
                }
            ]

        def fake_fetch(_url: str) -> str:
            return PUBLIC_POST_HTML

        summary = self.content.harvest_posts(
            industry="ai",
            limit=1,
            per_query=5,
            query_terms=['site:linkedin.com/posts "ai agents"'],
            auth_search_fn=auth_search,
            search_fn=public_search,
            fetch_html=fake_fetch,
            backend="hybrid",
        )

        self.assertEqual(summary["stored_count"], 1)
        self.assertEqual(calls, ["auth", "public"])

    def test_harvest_posts_auth_only_never_falls_back_to_public_search(self) -> None:
        calls: list[str] = []

        def auth_search(_query: str, limit: int = 10) -> list[dict[str, str]]:
            calls.append("auth")
            raise RuntimeError("bad auth search")

        def public_search(query: str, limit: int = 10) -> list[dict[str, str]]:
            calls.append("public")
            return [
                {
                    "title": "Agents post",
                    "url": "https://www.linkedin.com/posts/claude-mackenzie_agents-demo-activity-1",
                    "snippet": query,
                }
            ]

        with self.assertRaises(SystemExit) as exc_info:
            self.content.harvest_posts(
                industry="ai",
                limit=1,
                per_query=5,
                query_terms=['site:linkedin.com/posts "ai agents"'],
                auth_search_fn=auth_search,
                search_fn=public_search,
                backend="auth-only",
            )

        self.assertEqual(exc_info.exception.code, 7)
        self.assertEqual(calls, ["auth"])

    def test_harvest_posts_continues_past_failed_queries_when_others_succeed(self) -> None:
        calls: list[str] = []

        def auth_search(query: str, limit: int = 10, start: int = 0) -> list[dict[str, str]]:
            calls.append(query)
            if query == "ai automation":
                raise RuntimeError("bad auth search")
            if start > 0:
                return []
            return [
                {
                    "title": "Agents post",
                    "url": "https://www.linkedin.com/posts/claude-mackenzie_agents-demo-activity-1",
                    "snippet": query,
                    "source": "linkedin.auth",
                    "text": "AI agents finish real workflows.",
                }
            ]

        summary = self.content.harvest_posts(
            industries=["ai"],
            topics=["agents"],
            limit=1,
            per_query=5,
            query_terms=["ai automation", "ai agents"],
            auth_search_fn=auth_search,
            backend="auth-only",
        )

        self.assertEqual(summary["stored_count"], 1)
        self.assertEqual(summary["failed_query_count"], 1)
        self.assertEqual(summary["failed_queries"][0]["query"], "ai automation")
        self.assertEqual(calls[:2], ["ai automation", "ai agents"])

    def test_harvest_posts_pages_authenticated_results(self) -> None:
        calls: list[tuple[int, int]] = []

        def auth_search(_query: str, limit: int = 10, start: int = 0) -> list[dict[str, str]]:
            calls.append((start, limit))
            pages = {
                0: [
                    {
                        "title": "First",
                        "url": "https://www.linkedin.com/posts/claude-mackenzie_agents-demo-activity-1",
                        "snippet": "first",
                    }
                ],
                1: [
                    {
                        "title": "Second",
                        "url": "https://www.linkedin.com/posts/claude-mackenzie_agents-demo-activity-2",
                        "snippet": "second",
                    }
                ],
            }
            return pages.get(start, [])

        def fake_fetch(url: str) -> str:
            return PUBLIC_POST_HTML.replace("activity-1", url.rsplit("-", 1)[-1])

        summary = self.content.harvest_posts(
            industry="ai",
            limit=2,
            per_query=2,
            query_terms=['site:linkedin.com/posts "ai agents"'],
            auth_search_fn=auth_search,
            search_fn=lambda _query, limit=10: [],
            fetch_html=fake_fetch,
        )

        self.assertEqual(summary["stored_count"], 2)
        self.assertEqual(calls, [(0, 2), (1, 1)])

    def test_sync_post_outcomes_avoids_recursive_public_upsert_path(self) -> None:
        record = self.content.extract_post_record(
            url="https://www.linkedin.com/posts/claude-mackenzie_agents-demo-activity-1",
            html=PUBLIC_POST_HTML,
            source_query='site:linkedin.com/posts "ai agents"',
        )
        record["industries"] = ["ai"]
        self.content.upsert_post(record)

        with patch.object(self.content, "upsert_post", side_effect=AssertionError("public upsert should not be called")):
            summary = self.content.sync_post_outcomes(
                urls=[record["url"]],
                owned_by_me=True,
                fetch_html=lambda _url: PUBLIC_POST_HTML,
            )

        self.assertEqual(summary["synced_count"], 1)
        posts = self.content.list_posts(limit=5)
        self.assertTrue(posts[0]["owned_by_me"])

    def test_harvest_job_resume_skips_completed_queries(self) -> None:
        calls: list[str] = []

        def fake_search(query: str, limit: int = 10) -> list[dict[str, str]]:
            calls.append(query)
            if query.endswith('"agents"'):
                return [
                    {
                        "title": "Agents post",
                        "url": "https://www.linkedin.com/posts/claude-mackenzie_agents-demo-activity-1",
                        "snippet": "agents",
                    }
                ]
            raise RuntimeError("stop after first query")

        partial = self.content.harvest_posts(
            industries=["ai"],
            limit=5,
            per_query=5,
            query_terms=[
                'site:linkedin.com/posts "agents"',
                'site:linkedin.com/posts "workflow"',
            ],
            auth_search_fn=None,
            search_fn=fake_search,
            fetch_html=lambda _url: PUBLIC_POST_HTML,
            job_name="resume-smoke",
        )

        self.assertEqual(partial["stored_count"], 1)
        self.assertEqual(partial["failed_query_count"], 1)
        calls.clear()

        def resume_search(query: str, limit: int = 10) -> list[dict[str, str]]:
            calls.append(query)
            return [
                {
                    "title": "Workflow post",
                    "url": "https://www.linkedin.com/posts/claude-mackenzie_agents-demo-activity-2",
                    "snippet": "workflow",
                }
            ]

        summary = self.content.harvest_posts(
            industries=["ai"],
            limit=5,
            per_query=5,
            query_terms=[
                'site:linkedin.com/posts "agents"',
                'site:linkedin.com/posts "workflow"',
            ],
            auth_search_fn=None,
            search_fn=resume_search,
            fetch_html=lambda _url: PUBLIC_POST_HTML.replace("activity-1", "activity-2"),
            resume_job="resume-smoke",
        )

        self.assertEqual(calls, ['site:linkedin.com/posts "workflow"'])
        self.assertEqual(summary["stored_count"], 2)
        self.assertEqual(summary["job"]["status"], "completed")

    def test_harvest_posts_records_per_query_stored_count_not_cumulative(self) -> None:
        def fake_search(query: str, limit: int = 10) -> list[dict[str, str]]:
            suffix = "1" if query.endswith('"agents"') else "2"
            return [
                {
                    "title": query,
                    "url": f"https://www.linkedin.com/posts/claude-mackenzie_agents-demo-activity-{suffix}",
                    "snippet": query,
                }
            ]

        summary = self.content.harvest_posts(
            industries=["ai"],
            limit=2,
            per_query=5,
            query_terms=[
                'site:linkedin.com/posts "agents"',
                'site:linkedin.com/posts "workflow"',
            ],
            auth_search_fn=None,
            search_fn=fake_search,
            fetch_html=lambda url: PUBLIC_POST_HTML.replace("activity-1", f"activity-{url.rsplit('-', 1)[-1]}"),
            job_name="per-query-counts",
        )

        conn = store._connect()
        try:
            rows = conn.execute(
                """
                SELECT query_index, stored_count
                FROM content_harvest_job_queries
                WHERE job_id = ?
                ORDER BY query_index
                """,
                ("per-query-counts",),
            ).fetchall()
        finally:
            conn.close()

        self.assertEqual(summary["stored_count"], 2)
        self.assertEqual([(row["query_index"], row["stored_count"]) for row in rows], [(1, 1), (2, 1)])

    def test_harvest_posts_marks_query_running_before_search_returns(self) -> None:
        observed: dict[str, str | None] = {"status": None}

        def fake_search(_query: str, limit: int = 10, timeout: int | None = None) -> list[dict[str, str]]:
            conn = store._connect()
            try:
                row = conn.execute(
                    """
                    SELECT status
                    FROM content_harvest_job_queries
                    WHERE job_id = ? AND query_index = ?
                    """,
                    ("running-before-search", 1),
                ).fetchone()
            finally:
                conn.close()
            observed["status"] = row["status"] if row else None
            return [
                {
                    "title": "Agents post",
                    "url": "https://www.linkedin.com/posts/claude-mackenzie_agents-demo-activity-12",
                    "snippet": "agents",
                }
            ]

        summary = self.content.harvest_posts(
            industries=["ai"],
            limit=1,
            per_query=1,
            query_terms=['site:linkedin.com/posts "agents"'],
            auth_search_fn=None,
            search_fn=fake_search,
            fetch_html=lambda _url: PUBLIC_POST_HTML.replace("activity-1", "activity-12"),
            job_name="running-before-search",
        )

        self.assertEqual(observed["status"], "running")
        self.assertEqual(summary["stored_count"], 1)
        self.assertEqual(summary["job"]["status"], "completed")

    def test_resume_job_uses_saved_job_limits(self) -> None:
        conn = store._connect()
        try:
            conn.execute(
                """
                INSERT INTO content_harvest_jobs
                (job_id, created_at, updated_at, status, limit_value, per_query, search_timeout, fetch_workers, query_workers,
                 stored_count, unique_url_count, industries_json, queries_json, metadata_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "saved-config",
                    "2026-03-26T00:00:00Z",
                    "2026-03-26T00:00:00Z",
                    "failed",
                    7,
                    4,
                    11,
                    2,
                    1,
                    0,
                    0,
                    '["ai"]',
                    '["site:linkedin.com/posts \\"agents\\""]',
                    '{"seen_urls":[]}',
                ),
            )
            conn.execute(
                """
                INSERT INTO content_harvest_job_queries
                (job_id, query_index, query, status, stored_count, result_count, last_error, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "saved-config",
                    1,
                    'site:linkedin.com/posts "agents"',
                    "failed",
                    0,
                    0,
                    "timeout",
                    "2026-03-26T00:00:00Z",
                ),
            )
            conn.commit()
        finally:
            conn.close()

        observed: dict[str, int] = {}

        def fake_search(_query: str, limit: int = 10, timeout: int | None = None) -> list[dict[str, str]]:
            observed["limit"] = limit
            observed["timeout"] = int(timeout or 0)
            return [
                {
                    "title": "Agents post",
                    "url": "https://www.linkedin.com/posts/claude-mackenzie_agents-demo-activity-7",
                    "snippet": "agents",
                }
            ]

        summary = self.content.harvest_posts(
            resume_job="saved-config",
            auth_search_fn=None,
            search_fn=fake_search,
            fetch_html=lambda _url: PUBLIC_POST_HTML.replace("activity-1", "activity-7"),
        )

        self.assertEqual(observed["limit"], 4)
        self.assertEqual(observed["timeout"], 11)
        self.assertEqual(summary["job"]["status"], "completed")

    def test_harvest_job_marks_completed_when_limit_is_reached(self) -> None:
        def fake_search(query: str, limit: int = 10) -> list[dict[str, str]]:
            return [
                {
                    "title": query,
                    "url": "https://www.linkedin.com/posts/claude-mackenzie_agents-demo-activity-9",
                    "snippet": query,
                }
            ]

        summary = self.content.harvest_posts(
            industries=["ai"],
            limit=1,
            per_query=5,
            query_terms=[
                'site:linkedin.com/posts "agents"',
                'site:linkedin.com/posts "workflow"',
            ],
            auth_search_fn=None,
            search_fn=fake_search,
            fetch_html=lambda _url: PUBLIC_POST_HTML.replace("activity-1", "activity-9"),
            job_name="limit-finish",
        )

        self.assertEqual(summary["stored_count"], 1)
        self.assertEqual(summary["job"]["status"], "completed")

    def test_resume_completed_job_returns_saved_summary_without_querying(self) -> None:
        conn = store._connect()
        try:
            conn.execute(
                """
                INSERT INTO content_harvest_jobs
                (job_id, created_at, updated_at, status, limit_value, per_query, search_timeout, fetch_workers, query_workers,
                 stored_count, unique_url_count, industries_json, queries_json, metadata_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "done-job",
                    "2026-03-26T00:00:00Z",
                    "2026-03-26T00:00:00Z",
                    "completed",
                    5,
                    3,
                    11,
                    2,
                    1,
                    5,
                    5,
                    '["ai","fintech"]',
                    '["site:linkedin.com/posts \\"agents\\""]',
                    '{"seen_urls":["https://www.linkedin.com/posts/example"]}',
                ),
            )
            conn.commit()
        finally:
            conn.close()

        def fail_search(_query: str, limit: int = 10, timeout: int | None = None) -> list[dict[str, str]]:
            raise AssertionError("completed jobs should not re-query")

        summary = self.content.harvest_posts(
            resume_job="done-job",
            auth_search_fn=None,
            search_fn=fail_search,
        )

        self.assertEqual(summary["stored_count"], 5)
        self.assertEqual(summary["unique_url_count"], 5)
        self.assertEqual(summary["industries"], ["ai", "fintech"])
        self.assertEqual(summary["job"]["status"], "completed")

    def test_resume_job_heals_running_job_that_already_hit_limit(self) -> None:
        conn = store._connect()
        try:
            conn.execute(
                """
                INSERT INTO content_harvest_jobs
                (job_id, created_at, updated_at, status, limit_value, per_query, search_timeout, fetch_workers, query_workers,
                 stored_count, unique_url_count, industries_json, queries_json, metadata_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "heal-job",
                    "2026-03-26T00:00:00Z",
                    "2026-03-26T00:00:00Z",
                    "running",
                    3,
                    3,
                    11,
                    2,
                    1,
                    3,
                    3,
                    '["ai"]',
                    '["site:linkedin.com/posts \\"agents\\""]',
                    '{"seen_urls":["https://www.linkedin.com/posts/example"]}',
                ),
            )
            conn.commit()
        finally:
            conn.close()

        summary = self.content.harvest_posts(
            resume_job="heal-job",
            auth_search_fn=None,
            search_fn=lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("should not query")),
        )

        self.assertEqual(summary["job"]["status"], "completed")
        jobs = self.content.list_harvest_jobs(limit=5)
        healed = next(job for job in jobs if job["job_id"] == "heal-job")
        self.assertEqual(healed["status"], "completed")

    def test_harvest_posts_supports_parallel_query_resolution(self) -> None:
        def auth_search(query: str, limit: int = 10, start: int = 0) -> list[dict[str, str]]:
            if start > 0:
                return []
            slug = query.split()[-1].replace('"', "")
            return [
                {
                    "title": f"{slug} post",
                    "url": f"https://www.linkedin.com/posts/{slug}-activity-1",
                    "text": f"{slug} workflow insights",
                    "source": "linkedin.auth",
                }
            ]

        summary = self.content.harvest_posts(
            industry="ai",
            limit=2,
            per_query=1,
            query_terms=['site:linkedin.com/posts "agents"', 'site:linkedin.com/posts "workflow"'],
            auth_search_fn=auth_search,
            search_fn=lambda _query, limit=10: [],
            query_workers=2,
        )

        self.assertEqual(summary["stored_count"], 2)
        self.assertEqual(summary["unique_url_count"], 2)

    def test_harvest_posts_times_out_parallel_query_resolution(self) -> None:
        def fake_search(query: str, limit: int = 10, timeout: int | None = None) -> list[dict[str, str]]:
            if query.endswith('"workflow"'):
                time.sleep(0.1)
                return []
            return [
                {
                    "title": "Agents post",
                    "url": "https://www.linkedin.com/posts/claude-mackenzie_agents-demo-activity-11",
                    "snippet": query,
                }
            ]

        summary = self.content.harvest_posts(
            industry="ai",
            limit=2,
            per_query=1,
            query_terms=['site:linkedin.com/posts "agents"', 'site:linkedin.com/posts "workflow"'],
            auth_search_fn=None,
            search_fn=fake_search,
            fetch_html=lambda _url: PUBLIC_POST_HTML.replace("activity-1", "activity-11"),
            query_workers=2,
            search_timeout=0.01,
        )

        self.assertEqual(summary["stored_count"], 1)
        self.assertEqual(summary["failed_query_count"], 1)
        self.assertIn("timed out", summary["failed_queries"][0]["error"])

    def test_harvest_posts_times_out_stuck_fetches_without_hanging_job(self) -> None:
        def fake_search(_query: str, limit: int = 10, timeout: int | None = None) -> list[dict[str, str]]:
            return [
                {
                    "title": "Fast post",
                    "url": "https://www.linkedin.com/posts/claude-mackenzie_agents-demo-activity-21",
                    "snippet": "fast",
                },
                {
                    "title": "Slow post",
                    "url": "https://www.linkedin.com/posts/claude-mackenzie_agents-demo-activity-22",
                    "snippet": "slow",
                },
            ]

        def fake_fetch(url: str) -> str:
            if url.endswith("21"):
                return PUBLIC_POST_HTML.replace("activity-1", "activity-21")
            time.sleep(0.1)
            return PUBLIC_POST_HTML.replace("activity-1", "activity-22")

        summary = self.content.harvest_posts(
            industry="ai",
            limit=5,
            per_query=5,
            query_terms=['site:linkedin.com/posts "agents"'],
            auth_search_fn=None,
            search_fn=fake_search,
            fetch_html=fake_fetch,
            fetch_timeout=0.01,
            job_name="fetch-timeout",
        )

        self.assertEqual(summary["stored_count"], 1)
        self.assertEqual(summary["failed_query_count"], 1)
        self.assertIn("timed out", summary["failed_queries"][0]["error"])

        jobs = self.content.list_harvest_jobs(limit=10)
        job = next(item for item in jobs if item["job_id"] == "fetch-timeout")
        self.assertEqual(job["status"], "running")

    def test_harvest_posts_uses_authenticated_records_without_html_fetch(self) -> None:
        def auth_search(_query: str, limit: int = 10, start: int = 0) -> list[dict[str, str]]:
            self.assertEqual(start, 0)
            return self.content.parse_authenticated_search_results(AUTH_SEARCH_RESPONSE)

        def fail_fetch(_url: str) -> str:
            raise AssertionError("html fetch should not be used for rich authenticated results")

        summary = self.content.harvest_posts(
            industry="ai",
            limit=1,
            per_query=1,
            query_terms=['site:linkedin.com/posts "ai agents"'],
            auth_search_fn=auth_search,
            search_fn=lambda _query, limit=10: [],
            fetch_html=fail_fetch,
        )

        posts = self.content.list_posts(limit=5)
        self.assertEqual(summary["stored_count"], 1)
        self.assertEqual(posts[0]["author_name"], "Jon Frederick")
        self.assertEqual(posts[0]["reaction_count"], 42)
        self.assertEqual(posts[0]["comment_count"], 7)

    def test_local_hash_embeddings_work_without_openai_api_key(self) -> None:
        record = self.content.extract_post_record(
            url="https://www.linkedin.com/posts/claude-mackenzie_agents-demo-activity-1",
            html=PUBLIC_POST_HTML,
            source_query='site:linkedin.com/posts "ai agents"',
        )
        record["industry"] = "ai"
        self.content.upsert_post(record)

        with patch.dict("os.environ", {}, clear=True):
            result = self.content.embed_posts(limit=10, model="local-hash-v1")

        posts = self.content.list_posts(limit=10)
        self.assertEqual(result["embedded_count"], 1)
        self.assertEqual(posts[0]["embedding_model"], "local-hash-v1")
        self.assertEqual(len(posts[0]["embedding"]), 256)
        self.assertTrue(any(value != 0.0 for value in posts[0]["embedding"]))

    def test_embed_posts_persists_embeddings_and_stats(self) -> None:
        record = self.content.extract_post_record(
            url="https://www.linkedin.com/posts/claude-mackenzie_agents-demo-activity-1",
            html=PUBLIC_POST_HTML,
            source_query='site:linkedin.com/posts "ai agents"',
        )
        record["industry"] = "ai"
        self.content.upsert_post(record)

        def fake_embed(texts: list[str], model: str) -> list[list[float]]:
            self.assertEqual(model, "test-embedding-model")
            return [[0.1, 0.2, 0.3] for _ in texts]

        result = self.content.embed_posts(limit=10, model="test-embedding-model", embed_fn=fake_embed)
        posts = self.content.list_posts(limit=10)
        stats = self.content.content_stats()

        self.assertEqual(result["embedded_count"], 1)
        self.assertEqual(posts[0]["embedding_model"], "test-embedding-model")
        self.assertEqual(posts[0]["embedding"], [0.1, 0.2, 0.3])
        self.assertEqual(stats["embedded_count"], 1)

    def test_list_posts_without_vectors_keeps_filtering_and_dimension_metadata(self) -> None:
        record = self.content.extract_post_record(
            url="https://www.linkedin.com/posts/claude-mackenzie_agents-demo-activity-1",
            html=PUBLIC_POST_HTML,
            source_query='site:linkedin.com/posts "ai agents"',
        )
        record["industry"] = "ai"
        second = dict(record)
        second["url"] = "https://www.linkedin.com/posts/other-author_agents-demo-activity-2"
        second["author_name"] = "Other Author"
        second["industry"] = "fintech"

        self.content.upsert_post(record)
        self.content.upsert_post(second)

        def fake_embed(texts: list[str], model: str) -> list[list[float]]:
            return [[0.1, 0.2, 0.3] for _ in texts]

        self.content.embed_posts(limit=10, model="test-embedding-model", embed_fn=fake_embed, missing_only=False)

        posts = self.content.list_posts(limit=10, industry="ai", author="claude", include_vectors=False)

        self.assertEqual(len(posts), 1)
        self.assertEqual(posts[0]["author_name"], "Claude Mackenzie")
        self.assertIsNone(posts[0].get("embedding"))
        self.assertIsNone(posts[0].get("fingerprint"))

        embedding_dim, fingerprint_dim = self.content.summarize_post_dimensions(posts[0])
        self.assertEqual(embedding_dim, 3)
        self.assertEqual(fingerprint_dim, 13)

    def test_upsert_post_persists_13d_content_fingerprint(self) -> None:
        record = self.content.extract_post_record(
            url="https://www.linkedin.com/posts/claude-mackenzie_agents-demo-activity-1",
            html=PUBLIC_POST_HTML,
            source_query='site:linkedin.com/posts "ai agents"',
        )
        record["industry"] = "ai"

        stored = self.content.upsert_post(record)

        self.assertEqual(stored["fingerprint_version"], "content-fingerprint-v1")
        self.assertEqual(len(stored["fingerprint"]), 13)
        self.assertGreater(stored["fingerprint"][0], 0.0)

    def test_retrieve_posts_ranks_semantic_and_fingerprint_matches(self) -> None:
        first = self.content.extract_post_record(
            url="https://www.linkedin.com/posts/claude-mackenzie_agents-demo-activity-1",
            html=PUBLIC_POST_HTML,
            source_query='site:linkedin.com/posts "ai agents"',
        )
        first["industry"] = "ai"
        second = dict(first)
        second["url"] = "https://www.linkedin.com/posts/claude-mackenzie_hiring-sdrs-activity-2"
        second["title"] = "We are hiring SDRs."
        second["text"] = "We are hiring SDRs across Phoenix and Austin."
        second["hook"] = "We are hiring SDRs."
        second["structure"] = "insight"
        second["word_count"] = len(second["text"].split())
        second["reaction_count"] = 5
        second["comment_count"] = 1

        self.content.upsert_post(first)
        self.content.upsert_post(second)

        vectors = {
            "AI agents are overrated until they save time.\n1. Start with the workflow.\n2. Remove manual steps.\n3. Measure saved hours.": [1.0, 0.0, 0.0],
            "We are hiring SDRs across Phoenix and Austin.": [0.0, 1.0, 0.0],
            "agent workflow automation": [0.95, 0.05, 0.0],
        }

        def fake_embed(texts: list[str], model: str) -> list[list[float]]:
            return [vectors[text] for text in texts]

        self.content.embed_posts(limit=10, model="test-embedding-model", embed_fn=fake_embed, missing_only=False)
        results = self.content.retrieve_posts(
            query_text="agent workflow automation",
            limit=2,
            model="test-embedding-model",
            embed_fn=fake_embed,
        )

        self.assertEqual(len(results), 2)
        self.assertEqual(results[0]["url"], first["url"])
        self.assertGreater(results[0]["score"], results[1]["score"])
        self.assertIn("semantic_score", results[0])
        self.assertEqual(results[0]["embedding_dim"], 3)
        self.assertEqual(results[0]["fingerprint_dim"], 13)
        self.assertNotIn("embedding", results[0])
        self.assertNotIn("fingerprint", results[0])

    def test_ranked_patterns_score_and_rewrite_use_outcomes(self) -> None:
        first = self.content.extract_post_record(
            url="https://www.linkedin.com/posts/claude-mackenzie_agents-demo-activity-1",
            html=PUBLIC_POST_HTML,
            source_query='site:linkedin.com/posts "ai agents"',
        )
        first["industry"] = "ai"
        second = dict(first)
        second["url"] = "https://www.linkedin.com/posts/claude-mackenzie_case-study-activity-2"
        second["title"] = "3 ways AI workflows save time"
        second["hook"] = "3 ways AI workflows save time."
        second["text"] = "3 ways AI workflows save time.\n\n1. Remove manual review.\n2. Automate routing.\n3. Measure throughput."
        second["structure"] = "list"
        second["reaction_count"] = 240
        second["comment_count"] = 32
        second["word_count"] = len(second["text"].split())
        second["owned_by_me"] = True

        self.content.upsert_post(first)
        self.content.upsert_post(second)

        def fake_embed(texts: list[str], model: str) -> list[list[float]]:
            return [[1.0, 0.0, 0.0] for _ in texts]

        self.content.embed_posts(limit=10, model="test-embedding-model", embed_fn=fake_embed, missing_only=False)
        patterns = self.content.ranked_patterns(limit=5, industry="ai")
        scored = self.content.score_draft(
            text="AI workflows are saving teams hours every week.\n\nHere is what changed.",
            industry="ai",
            model="test-embedding-model",
            embed_fn=fake_embed,
        )
        rewritten = self.content.rewrite_draft(
            text="AI workflows are saving teams hours every week.",
            industry="ai",
            goal="instructional",
            model="test-embedding-model",
            embed_fn=fake_embed,
        )

        self.assertEqual(patterns["top_posts"][0]["owned_by_me"], True)
        self.assertTrue(patterns["topics"])
        self.assertGreater(scored["predicted_outcome_score"], 0)
        self.assertTrue(scored["recommendations"])
        self.assertIn("1.", rewritten["rewritten"])
        self.assertGreaterEqual(rewritten["score_after"], rewritten["score_before"])

    def test_playbook_and_maximize_use_live_patterns_not_generic_slop(self) -> None:
        strong = self.content.extract_post_record(
            url="https://www.linkedin.com/posts/claude-mackenzie_agents-demo-activity-strong",
            html=PUBLIC_POST_HTML,
            source_query='site:linkedin.com/posts "ai agents"',
        )
        strong["industry"] = "ai"
        strong["owned_by_me"] = True
        strong["last_synced_at"] = "2026-03-26T00:00:00Z"
        strong["title"] = "Today, we're killing 2,000 integration companies."
        strong["hook"] = "Today, we're killing 2,000 integration companies."
        strong["text"] = (
            "Today, we're killing 2,000 integration companies.\n\n"
            "We found one workflow bottleneck, removed the handoff, and measured the savings.\n\n"
            "1. Cut duplicate review.\n"
            "2. Route the task automatically.\n"
            "3. Show the hours saved.\n\n"
            "What breaks first in your workflow?"
        )
        strong["structure"] = "list"
        strong["reaction_count"] = 340
        strong["comment_count"] = 44
        strong["word_count"] = len(strong["text"].split())

        weak = dict(strong)
        weak["url"] = "https://www.linkedin.com/posts/claude-mackenzie_agents-demo-activity-weak"
        weak["title"] = "What do you think about AI workflows?"
        weak["hook"] = "What do you think about AI workflows?"
        weak["text"] = "What do you think about AI workflows? #ai #automation #workflow"
        weak["structure"] = "question"
        weak["reaction_count"] = 8
        weak["comment_count"] = 1
        weak["word_count"] = len(weak["text"].split())

        self.content.upsert_post(strong)
        self.content.upsert_post(weak)
        self.content.train_outcome_model(name="default", min_samples=2, scope="owned")

        playbook = self.content.build_playbook(industry="ai", limit=5)
        maximized = self.content.maximize_draft(
            text="AI workflows can help teams move faster.",
            industry="ai",
            model="local-hash-v1",
            candidate_goals=["engagement", "instructional", "contrarian"],
        )

        self.assertTrue(playbook["learned_signals"]["top_positive"])
        self.assertTrue(playbook["hook_templates"])
        self.assertTrue(playbook["rewrite_rules"])
        self.assertGreaterEqual(maximized["best_score"], maximized["baseline"]["predicted_outcome_score"])
        self.assertIn(maximized["best_variant"]["goal"], {"engagement", "instructional", "contrarian"})
        self.assertTrue(maximized["best_variant"]["rewritten"])

    def test_create_drafts_generates_scored_slice_specific_candidates(self) -> None:
        strong = self.content.extract_post_record(
            url="https://www.linkedin.com/posts/claude-mackenzie_ai-workflow-activity-create-strong",
            html=PUBLIC_POST_HTML,
            source_query='site:linkedin.com/posts "ai workflow"',
        )
        strong["industries"] = ["ai"]
        strong["owned_by_me"] = True
        strong["last_synced_at"] = "2026-03-26T00:00:00Z"
        strong["title"] = "AI workflow bottlenecks are where automation wins."
        strong["hook"] = "AI workflow bottlenecks are where automation wins."
        strong["text"] = (
            "AI workflow bottlenecks are where automation wins.\n\n"
            "We removed the approval handoff, routed the task automatically, and measured cycle-time savings.\n\n"
            "1. Cut duplicate review.\n"
            "2. Route the task automatically.\n"
            "3. Show the hours saved.\n\n"
            "What is still breaking in your workflow?"
        )
        strong["structure"] = "list"
        strong["reaction_count"] = 260
        strong["comment_count"] = 34
        strong["word_count"] = len(strong["text"].split())

        second = dict(strong)
        second["url"] = "https://www.linkedin.com/posts/claude-mackenzie_ai-workflow-activity-create-second"
        second["title"] = "Most AI workflow advice is too generic to ship."
        second["hook"] = "Most AI workflow advice is too generic to ship."
        second["text"] = (
            "Most AI workflow advice is too generic to ship.\n\n"
            "The useful work starts when you map the handoff, kill one bottleneck, and prove the before-and-after."
        )
        second["structure"] = "insight"
        second["reaction_count"] = 210
        second["comment_count"] = 26
        second["word_count"] = len(second["text"].split())

        self.content.upsert_post(strong)
        self.content.upsert_post(second)
        self.content.train_outcome_model(name="default", min_samples=2, scope="owned", industry="ai", topics=["workflow"])

        created = self.content.create_drafts(
            prompt="AI workflow orchestration is still too brittle for most operators.",
            industry="ai",
            topics=["workflow"],
            candidate_goals=["engagement", "instructional", "authority", "contrarian"],
            candidate_count=4,
            model="local-hash-v1",
        )

        self.assertEqual(created["industry"], "ai")
        self.assertEqual(created["topics"], ["workflow"])
        self.assertEqual(created["prompt"], "AI workflow orchestration is still too brittle for most operators.")
        self.assertEqual(len(created["candidates"]), 4)
        self.assertTrue(created["playbook"]["top_examples"])
        self.assertTrue(created["exemplars"])
        self.assertTrue(all(candidate["text"] for candidate in created["candidates"]))
        self.assertTrue(all(candidate["goal"] in {"engagement", "instructional", "authority", "contrarian"} for candidate in created["candidates"]))
        self.assertTrue(all(candidate["score"]["predicted_outcome_score"] > 0 for candidate in created["candidates"]))
        self.assertTrue(all(candidate["references"]["top_example_urls"] for candidate in created["candidates"]))

    def test_create_drafts_can_use_structured_brief_for_fresh_long_form_copy(self) -> None:
        strong = self.content.extract_post_record(
            url="https://www.linkedin.com/posts/claude-mackenzie_ai-workflow-activity-create-brief",
            html=PUBLIC_POST_HTML,
            source_query='site:linkedin.com/posts "ai workflow"',
        )
        strong["industries"] = ["ai"]
        strong["owned_by_me"] = True
        strong["title"] = "AI workflow bottlenecks are where automation wins."
        strong["hook"] = strong["title"]
        strong["text"] = (
            "AI workflow bottlenecks are where automation wins.\n\n"
            "We mapped the handoff, removed the approval delay, and measured the hours saved."
        )
        strong["structure"] = "insight"
        strong["reaction_count"] = 260
        strong["comment_count"] = 34
        strong["word_count"] = len(strong["text"].split())
        self.content.upsert_post(strong)
        self.content.train_outcome_model(name="default", min_samples=1, scope="owned", industry="ai", topics=["workflow"])

        created = self.content.create_drafts(
            prompt="Create a LinkedIn post about why AI workflow automation breaks at the handoff.",
            industry="ai",
            topics=["workflow"],
            candidate_goals=["authority", "launch"],
            candidate_count=2,
            model="local-hash-v1",
            brief={
                "audience": "engineering leaders",
                "objective": "drive demos",
                "tone": "operator",
                "format": "story",
                "length": "long",
                "cta": "Ask readers to reply with the word pattern.",
            },
        )

        self.assertEqual(created["brief"]["audience"], "engineering leaders")
        self.assertEqual(created["brief"]["objective"], "drive demos")
        self.assertEqual(created["brief"]["format"], "story")
        self.assertEqual(created["brief"]["length"], "long")
        self.assertTrue(created["candidates"])
        self.assertTrue(any("pattern" in candidate["text"].lower() for candidate in created["candidates"]))
        self.assertGreaterEqual(max(len(candidate["text"].split()) for candidate in created["candidates"]), 45)
        self.assertTrue(any("What actually matters:" in candidate["text"] for candidate in created["candidates"]))
        self.assertGreaterEqual(max(len([line for line in candidate["text"].splitlines() if line.strip()]) for candidate in created["candidates"]), 8)
        lead_blocks = ["\n".join([line for line in candidate["text"].splitlines() if line.strip()][:4]) for candidate in created["candidates"]]
        self.assertEqual(len(set(lead_blocks)), len(lead_blocks))
        self.assertTrue(any("Engineering leaders" in candidate["text"] for candidate in created["candidates"]))
        self.assertTrue(all("engineering leaders inherit the outage path" not in candidate["text"] for candidate in created["candidates"]))
        self.assertTrue(all(candidate["text"].lower().count("workflow") <= 6 for candidate in created["candidates"]))

    def test_create_drafts_avoids_prompt_instruction_and_exemplar_copy(self) -> None:
        strong = self.content.extract_post_record(
            url="https://www.linkedin.com/posts/claude-mackenzie_ai-workflow-activity-originality",
            html=PUBLIC_POST_HTML,
            source_query='site:linkedin.com/posts "ai workflow"',
        )
        strong["industries"] = ["ai"]
        strong["owned_by_me"] = True
        strong["title"] = "Most AI workflow demos look clean until the handoff breaks."
        strong["hook"] = strong["title"]
        strong["text"] = (
            "Most AI workflow demos look clean until the handoff breaks.\n\n"
            "We mapped the routing layer, fixed the approval path, and measured the hours saved."
        )
        strong["structure"] = "insight"
        strong["reaction_count"] = 220
        strong["comment_count"] = 30
        strong["word_count"] = len(strong["text"].split())
        self.content.upsert_post(strong)
        self.content.train_outcome_model(name="default", min_samples=1, scope="owned", industry="ai", topics=["workflow"])

        created = self.content.create_drafts(
            prompt="Create a LinkedIn post about why AI workflow automation breaks at the handoff.",
            industry="ai",
            topics=["workflow"],
            candidate_goals=["authority", "launch"],
            candidate_count=2,
            model="local-hash-v1",
            brief={
                "audience": "engineering leaders",
                "objective": "drive demos",
                "tone": "operator",
                "format": "story",
                "length": "long",
            },
        )

        texts = [candidate["text"] for candidate in created["candidates"]]
        self.assertTrue(texts)
        self.assertTrue(all("Create a LinkedIn post about" not in text for text in texts))
        self.assertTrue(all("why ai workflow automation breaks at the handoff." not in text.lower() for text in texts))
        self.assertTrue(all("Most AI workflow demos look clean until the handoff breaks." not in text for text in texts))
        self.assertTrue(any("handoff" in text.lower() and "failure" in text.lower() for text in texts))
        self.assertTrue(all("Most teams still handle workflow with brittle handoffs and no proof." not in text for text in texts))

    def test_create_drafts_speed_max_uses_smaller_generation_context(self) -> None:
        with patch.object(self.content, "build_playbook", return_value={"top_examples": [], "hook_templates": [], "rewrite_rules": []}) as mock_playbook:
            with patch.object(self.content, "retrieve_posts", return_value=[]) as mock_retrieve:
                created = self.content.create_drafts(
                    prompt="Create a LinkedIn post about why AI workflow automation breaks at the handoff.",
                    industry="ai",
                    topics=["workflow"],
                    candidate_goals=["authority", "launch"],
                    candidate_count=6,
                    model="local-hash-v1",
                    speed="max",
                )

        self.assertEqual(created["speed"], "max")
        self.assertEqual(mock_playbook.call_args.kwargs["limit"], 4)
        self.assertEqual(mock_retrieve.call_args_list[0].kwargs["limit"], 3)
        self.assertEqual(len(created["candidates"]), 6)

    def test_choose_draft_returns_best_candidate_with_rationale(self) -> None:
        strong = self.content.extract_post_record(
            url="https://www.linkedin.com/posts/claude-mackenzie_ai-workflow-activity-choose-strong",
            html=PUBLIC_POST_HTML,
            source_query='site:linkedin.com/posts "ai workflow"',
        )
        strong["industries"] = ["ai"]
        strong["owned_by_me"] = True
        strong["last_synced_at"] = "2026-03-26T00:00:00Z"
        strong["title"] = "AI workflow systems fail when the proof is missing."
        strong["hook"] = "AI workflow systems fail when the proof is missing."
        strong["text"] = (
            "AI workflow systems fail when the proof is missing.\n\n"
            "We measured the breakpoints, fixed the routing layer, and showed the hours saved."
        )
        strong["structure"] = "insight"
        strong["reaction_count"] = 300
        strong["comment_count"] = 40
        strong["word_count"] = len(strong["text"].split())

        weaker = dict(strong)
        weaker["url"] = "https://www.linkedin.com/posts/claude-mackenzie_ai-workflow-activity-choose-weak"
        weaker["title"] = "What do you think about AI workflows?"
        weaker["hook"] = "What do you think about AI workflows?"
        weaker["text"] = "What do you think about AI workflows? #ai #workflow #automation"
        weaker["structure"] = "question"
        weaker["reaction_count"] = 12
        weaker["comment_count"] = 1
        weaker["word_count"] = len(weaker["text"].split())

        self.content.upsert_post(strong)
        self.content.upsert_post(weaker)
        self.content.train_outcome_model(name="default", min_samples=2, scope="owned", industry="ai", topics=["workflow"])

        chosen = self.content.choose_draft(
            prompt="AI workflow orchestration is still too brittle for most operators.",
            industry="ai",
            topics=["workflow"],
            candidate_goals=["engagement", "instructional", "authority", "contrarian"],
            candidate_count=4,
            model="local-hash-v1",
        )

        self.assertIn("best_candidate", chosen)
        self.assertIn("candidates", chosen)
        self.assertEqual(len(chosen["candidates"]), 4)
        self.assertEqual(chosen["best_candidate"]["rank"], 1)
        self.assertEqual(chosen["best_candidate"]["text"], chosen["candidates"][0]["text"])
        self.assertGreaterEqual(
            chosen["best_candidate"]["score"]["predicted_outcome_score"],
            chosen["candidates"][-1]["score"]["predicted_outcome_score"],
        )
        self.assertTrue(chosen["why_it_won"]["learned_signals"])
        self.assertTrue(chosen["why_it_won"]["recommendations"])
        self.assertTrue(chosen["references"]["top_example_urls"])

    def test_choose_draft_returns_brief_and_uses_it_for_generation(self) -> None:
        strong = self.content.extract_post_record(
            url="https://www.linkedin.com/posts/claude-mackenzie_ai-workflow-activity-choose-brief",
            html=PUBLIC_POST_HTML,
            source_query='site:linkedin.com/posts "ai workflow"',
        )
        strong["industries"] = ["ai"]
        strong["owned_by_me"] = True
        strong["title"] = "AI workflow systems fail when the proof is missing."
        strong["hook"] = strong["title"]
        strong["text"] = (
            "AI workflow systems fail when the proof is missing.\n\n"
            "We measured the breakpoints, fixed the routing layer, and showed the hours saved."
        )
        strong["structure"] = "insight"
        strong["reaction_count"] = 300
        strong["comment_count"] = 40
        strong["word_count"] = len(strong["text"].split())
        self.content.upsert_post(strong)
        self.content.train_outcome_model(name="default", min_samples=1, scope="owned", industry="ai", topics=["workflow"])

        chosen = self.content.choose_draft(
            prompt="Create a LinkedIn post about why AI workflow orchestration still breaks in production.",
            industry="ai",
            topics=["workflow"],
            candidate_goals=["authority", "launch"],
            candidate_count=2,
            model="local-hash-v1",
            brief={
                "audience": "ops leaders",
                "objective": "book calls",
                "tone": "direct",
                "format": "operator",
                "length": "medium",
                "cta": "Ask readers to comment audit.",
            },
        )

        self.assertEqual(chosen["brief"]["audience"], "ops leaders")
        self.assertEqual(chosen["brief"]["objective"], "book calls")
        self.assertEqual(chosen["best_candidate"]["references"]["brief"]["cta"], "Ask readers to comment audit.")
        self.assertIn("audit", chosen["best_candidate"]["text"].lower())

    def test_choose_draft_can_polish_selected_candidate(self) -> None:
        strong = self.content.extract_post_record(
            url="https://www.linkedin.com/posts/claude-mackenzie_ai-workflow-activity-choose-polish-strong",
            html=PUBLIC_POST_HTML,
            source_query='site:linkedin.com/posts "ai workflow"',
        )
        strong["industries"] = ["ai"]
        strong["owned_by_me"] = True
        strong["title"] = "AI workflow systems fail when the proof is missing."
        strong["hook"] = "AI workflow systems fail when the proof is missing."
        strong["text"] = "AI workflow systems fail when the proof is missing.\n\nWe measured the breakpoints, fixed the routing layer, and showed the hours saved."
        strong["structure"] = "insight"
        strong["reaction_count"] = 300
        strong["comment_count"] = 40
        strong["word_count"] = len(strong["text"].split())
        self.content.upsert_post(strong)
        self.content.train_outcome_model(name="default", min_samples=1, scope="owned", industry="ai", topics=["workflow"])

        fake_polished = {
            "best_variant": {
                "goal": "instructional",
                "source": "rewrite",
                "text": "3 ways AI workflow systems fail when the proof is missing.\n1. Measure the break.\n2. Fix the route.\n3. Show the hours saved.",
                "rank": 1,
                "stacked_score": {"final_score": 0.77},
            },
            "variants": [{"text": "variant-1", "rank": 1}],
            "count": 1,
        }
        with patch.object(self.content, "polish_and_score", return_value=fake_polished) as mocked:
            chosen = self.content.choose_draft(
                prompt="AI workflow orchestration is still too brittle for most operators.",
                industry="ai",
                topics=["workflow"],
                candidate_goals=["engagement", "instructional"],
                candidate_count=2,
                model="local-hash-v1",
                polish_selected=True,
                stacked_model_name="foundation-v7-2026-03-30",
                target_profile={"company": "Acme AI", "industries": ["ai"]},
            )

        mocked.assert_called_once()
        self.assertIn("polished_choice", chosen)
        self.assertEqual(chosen["polished_choice"]["best_variant"]["rank"], 1)
        self.assertEqual(chosen["polished_choice"]["best_variant"]["source"], "rewrite")

    def test_polish_and_score_returns_ranked_variants_with_stacked_breakdown(self) -> None:
        strong = self.content.extract_post_record(
            url="https://www.linkedin.com/posts/claude-mackenzie_ai-workflow-activity-polish-strong",
            html=PUBLIC_POST_HTML,
            source_query='site:linkedin.com/posts "ai workflow"',
        )
        strong["industries"] = ["ai"]
        strong["owned_by_me"] = True
        strong["last_synced_at"] = "2026-03-26T00:00:00Z"
        strong["title"] = "AI workflow proof beats AI workflow vibes."
        strong["hook"] = "AI workflow proof beats AI workflow vibes."
        strong["text"] = (
            "AI workflow proof beats AI workflow vibes.\n\n"
            "We mapped the handoff, removed the delay, and showed the hours saved.\n\n"
            "What is still breaking in your workflow?"
        )
        strong["structure"] = "insight"
        strong["reaction_count"] = 220
        strong["comment_count"] = 30
        strong["word_count"] = len(strong["text"].split())

        self.content.upsert_post(strong)
        self.content.train_outcome_model(name="default", min_samples=1, scope="owned", industry="ai", topics=["workflow"])

        def fake_stacked_score(
            *,
            text: str,
            industry: str | None = None,
            topics: list[str] | None = None,
            target_profile: dict[str, object] | None = None,
            model_name: str | None = None,
            auto_calibrate_weights: bool = True,
        ) -> dict[str, object]:
            digest = int(hashlib.sha1(text.encode("utf-8")).hexdigest()[:8], 16)
            final = round((digest % 1000) / 1000.0, 6)
            return {
                "model_name": model_name or "foundation-v1",
                "industry": industry,
                "topics": list(topics or []),
                "target_profile": dict(target_profile or {}),
                "score_breakdown": {
                    "public_performance": round(final * 0.7, 6),
                    "persona_style": round(final * 0.8, 6),
                    "business_intent": round(final * 0.9, 6),
                    "target_similarity": round(final, 6),
                },
                "weights_used": {
                    "public_performance": 0.2,
                    "persona_style": 0.2,
                    "business_intent": 0.3,
                    "target_similarity": 0.3,
                },
                "final_score": final,
            }

        with patch("linkedin_cli.content_stack.score_text_for_target", side_effect=fake_stacked_score):
            polished = self.content.polish_and_score(
                text="AI workflow automation is still too brittle for operators.",
                industry="ai",
                topics=["workflow"],
                candidate_goals=["engagement", "authority"],
                model="local-hash-v1",
                stacked_model_name="foundation-v1",
                target_profile={
                    "company": "Acme AI",
                    "industries": ["ai"],
                    "problem_keywords": ["workflow"],
                },
                limit=2,
            )

        self.assertEqual(polished["industry"], "ai")
        self.assertEqual(polished["topics"], ["workflow"])
        self.assertEqual(polished["model_name"], "foundation-v1")
        self.assertIn("baseline", polished)
        self.assertEqual(len(polished["variants"]), 2)
        self.assertEqual(polished["best_variant"]["rank"], 1)
        self.assertGreaterEqual(
            float(polished["variants"][0]["stacked_score"]["final_score"]),
            float(polished["variants"][-1]["stacked_score"]["final_score"]),
        )
        self.assertEqual(polished["variants"][0]["source"], "rewrite")
        self.assertIn("local_score", polished["variants"][0])
        self.assertIn("stacked_score", polished["variants"][0])
        self.assertIn("score_breakdown", polished["variants"][0]["stacked_score"])

    def test_polish_and_score_falls_back_when_corpus_db_is_unavailable(self) -> None:
        def fake_stacked_score(
            *,
            text: str,
            industry: str | None = None,
            topics: list[str] | None = None,
            target_profile: dict[str, object] | None = None,
            model_name: str | None = None,
            auto_calibrate_weights: bool = True,
        ) -> dict[str, object]:
            return {
                "model_name": model_name or "foundation-v1",
                "industry": industry,
                "topics": list(topics or []),
                "target_profile": dict(target_profile or {}),
                "score_breakdown": {
                    "public_performance": 0.4,
                    "persona_style": 0.6,
                    "business_intent": 0.7,
                    "target_similarity": 0.5,
                },
                "weights_used": {
                    "public_performance": 0.2,
                    "persona_style": 0.2,
                    "business_intent": 0.3,
                    "target_similarity": 0.3,
                },
                "final_score": 0.56,
            }

        with patch.object(self.content, "score_draft", side_effect=sqlite3.DatabaseError("database disk image is malformed")):
            with patch("linkedin_cli.content_stack.score_text_for_target", side_effect=fake_stacked_score):
                polished = self.content.polish_and_score(
                    text="AI workflow automation is still too brittle for operators.",
                    industry="ai",
                    topics=["workflow"],
                    candidate_goals=["engagement", "authority"],
                    stacked_model_name="foundation-v1",
                    target_profile={"company": "Acme AI", "industries": ["ai"]},
                    limit=2,
                )

        self.assertEqual(polished["fallback_mode"], "stacked_only")
        self.assertTrue(polished["warnings"])
        self.assertEqual(len(polished["variants"]), 2)
        self.assertEqual(polished["best_variant"]["rank"], 1)
        self.assertIsNone(polished["baseline"]["local_score"])
        self.assertEqual(polished["variants"][0]["source"], "rewrite")
        self.assertIsNone(polished["variants"][0]["local_score"])

    def test_polish_and_score_can_add_fresh_long_form_variants(self) -> None:
        strong = self.content.extract_post_record(
            url="https://www.linkedin.com/posts/claude-mackenzie_ai-workflow-activity-polish-fresh-strong",
            html=PUBLIC_POST_HTML,
            source_query='site:linkedin.com/posts "ai workflow"',
        )
        strong["industries"] = ["ai"]
        strong["owned_by_me"] = True
        strong["title"] = "AI workflow systems fail when nobody owns the handoff."
        strong["hook"] = strong["title"]
        strong["text"] = (
            "AI workflow systems fail when nobody owns the handoff.\n\n"
            "We mapped the break, fixed the routing layer, and showed the hours saved."
        )
        strong["structure"] = "insight"
        strong["reaction_count"] = 320
        strong["comment_count"] = 44
        strong["word_count"] = len(strong["text"].split())
        self.content.upsert_post(strong)
        self.content.train_outcome_model(name="default", min_samples=1, scope="owned", industry="ai", topics=["workflow"])

        def fake_stacked_score(
            *,
            text: str,
            industry: str | None = None,
            topics: list[str] | None = None,
            target_profile: dict[str, object] | None = None,
            model_name: str | None = None,
            auto_calibrate_weights: bool = True,
        ) -> dict[str, object]:
            return {
                "model_name": model_name or "foundation-v1",
                "industry": industry,
                "topics": list(topics or []),
                "target_profile": dict(target_profile or {}),
                "score_breakdown": {
                    "public_performance": 0.5,
                    "persona_style": 0.6,
                    "business_intent": 0.8 if len(text.split()) > 35 else 0.4,
                    "target_similarity": 0.7,
                },
                "weights_used": {
                    "public_performance": 0.2,
                    "persona_style": 0.2,
                    "business_intent": 0.3,
                    "target_similarity": 0.3,
                },
                "final_score": 0.71 if len(text.split()) > 35 else 0.49,
            }

        with patch("linkedin_cli.content_stack.score_text_for_target", side_effect=fake_stacked_score):
            polished = self.content.polish_and_score(
                text="AI workflow automation is still too brittle for operators.",
                industry="ai",
                topics=["workflow"],
                candidate_goals=["authority", "instructional"],
                model="local-hash-v1",
                stacked_model_name="foundation-v1",
                target_profile={"company": "Acme AI", "industries": ["ai"]},
                limit=4,
                fresh=True,
                long_form=True,
            )

        fresh_variants = [item for item in polished["variants"] if item["source"] == "fresh"]
        self.assertTrue(fresh_variants)
        self.assertGreaterEqual(max(len(item["text"].split()) for item in fresh_variants), 35)
        self.assertIn(polished["best_variant"]["source"], {"fresh", "rewrite", "original"})

    def test_ranked_patterns_and_playbook_gate_off_topic_mass_viral_posts(self) -> None:
        relevant = self.content.extract_post_record(
            url="https://www.linkedin.com/posts/claude-mackenzie_ai-workflow-activity-relevant",
            html=PUBLIC_POST_HTML,
            source_query='site:linkedin.com/posts "ai workflow"',
        )
        relevant["industries"] = ["ai"]
        relevant["title"] = "AI workflow bottlenecks are where automation wins."
        relevant["hook"] = "AI workflow bottlenecks are where automation wins."
        relevant["text"] = (
            "AI workflow bottlenecks are where automation wins.\n\n"
            "We removed the approval handoff, routed the task automatically, and measured cycle-time savings."
        )
        relevant["reaction_count"] = 110
        relevant["comment_count"] = 18
        relevant["word_count"] = len(relevant["text"].split())
        relevant["owned_by_me"] = True
        relevant["last_synced_at"] = "2026-03-26T00:00:00Z"

        off_topic = dict(relevant)
        off_topic["url"] = "https://www.linkedin.com/posts/claude-mackenzie_mass-viral-offtopic"
        off_topic["title"] = "Congress is broken and everyone knows it."
        off_topic["hook"] = "Congress is broken and everyone knows it."
        off_topic["text"] = (
            "Congress is broken and everyone knows it.\n\n"
            "This has nothing to do with AI workflows, automation, or software operations."
        )
        off_topic["reaction_count"] = 5000
        off_topic["comment_count"] = 900
        off_topic["word_count"] = len(off_topic["text"].split())
        off_topic["industries"] = ["ai"]
        off_topic["owned_by_me"] = True
        off_topic["last_synced_at"] = "2026-03-26T00:00:00Z"

        self.content.upsert_post(relevant)
        self.content.upsert_post(off_topic)

        patterns = self.content.ranked_patterns(limit=5, industry="ai", topics=["workflow"])
        playbook = self.content.build_playbook(industry="ai", topics=["workflow"], limit=5)

        self.assertEqual(patterns["top_posts"][0]["url"], relevant["url"])
        self.assertTrue(all("workflow" in (item.get("title") or "").lower() or "workflow" in (item.get("hook") or "").lower() for item in playbook["top_examples"]))
        self.assertTrue(all(item["url"] != off_topic["url"] for item in playbook["top_examples"]))

    def test_train_outcome_model_supports_industry_and_topic_slice(self) -> None:
        records: list[dict[str, object]] = []
        for index, (title, reactions, comments, industries) in enumerate(
            [
                ("AI workflow case study", 180, 24, ["ai"]),
                ("AI workflow bottleneck fix", 130, 16, ["ai"]),
                ("Sales outbound script", 320, 40, ["sales"]),
                ("Recruiting funnel cleanup", 260, 28, ["recruiting"]),
            ],
            start=1,
        ):
            record = self.content.extract_post_record(
                url=f"https://www.linkedin.com/posts/claude-mackenzie_slice-activity-{index}",
                html=PUBLIC_POST_HTML,
                source_query=f'site:linkedin.com/posts "{title.lower()}"',
            )
            record["title"] = title
            record["hook"] = f"{title}."
            record["text"] = f"{title}. We changed the workflow, measured the result, and wrote down what broke."
            record["reaction_count"] = reactions
            record["comment_count"] = comments
            record["word_count"] = len(str(record["text"]).split())
            record["industries"] = industries
            record["owned_by_me"] = True
            record["last_synced_at"] = "2026-03-26T00:00:00Z"
            self.content.upsert_post(record)
            records.append(record)

        trained = self.content.train_outcome_model(
            name="ai-workflow-slice",
            min_samples=2,
            scope="owned",
            industry="ai",
            topics=["workflow"],
        )

        self.assertTrue(trained["trained"])
        self.assertEqual(trained["sample_count"], 2)
        self.assertEqual(trained["model"]["metadata"]["industry"], "ai")
        self.assertEqual(trained["model"]["metadata"]["topics"], ["workflow"])

    def test_sync_post_outcomes_updates_owned_flags_and_snapshots(self) -> None:
        record = self.content.extract_post_record(
            url="https://www.linkedin.com/posts/claude-mackenzie_agents-demo-activity-1",
            html=PUBLIC_POST_HTML,
            source_query='site:linkedin.com/posts "ai agents"',
        )
        self.content.upsert_post(record)

        summary = self.content.sync_post_outcomes(
            urls=[record["url"]],
            owned_by_me=True,
            fetch_html=lambda _url: PUBLIC_POST_HTML,
        )
        posts = self.content.list_posts(limit=5)

        self.assertEqual(summary["synced_count"], 1)
        self.assertEqual(posts[0]["owned_by_me"], 1)
        self.assertGreater(posts[0]["outcome_score"], 0)
        self.assertIsNotNone(posts[0]["last_synced_at"])

    def test_upsert_post_does_not_rerun_heavy_db_backfills_after_init(self) -> None:
        calls = {
            "industries": 0,
            "query_context": 0,
            "fingerprints": 0,
            "dimensions": 0,
            "outcomes": 0,
        }

        def wrap(name: str):
            original = getattr(self.content, name)

            def _wrapped(conn):
                if name == "_backfill_post_industries":
                    calls["industries"] += 1
                elif name == "_backfill_query_context":
                    calls["query_context"] += 1
                elif name == "_backfill_fingerprints":
                    calls["fingerprints"] += 1
                elif name == "_backfill_dimensions":
                    calls["dimensions"] += 1
                elif name == "_backfill_outcome_scores":
                    calls["outcomes"] += 1
                return original(conn)

            return _wrapped

        self.content._CONTENT_DB_INITIALIZED_PATHS.clear()

        with patch.object(self.content, "_backfill_post_industries", wrap("_backfill_post_industries")):
            with patch.object(self.content, "_backfill_query_context", wrap("_backfill_query_context")):
                with patch.object(self.content, "_backfill_fingerprints", wrap("_backfill_fingerprints")):
                    with patch.object(self.content, "_backfill_dimensions", wrap("_backfill_dimensions")):
                        with patch.object(self.content, "_backfill_outcome_scores", wrap("_backfill_outcome_scores")):
                            self.content.init_content_db()

                            first = self.content.extract_post_record(
                                url="https://www.linkedin.com/posts/claude-mackenzie_agents-demo-activity-1",
                                html=PUBLIC_POST_HTML,
                                source_query='site:linkedin.com/posts "ai agents"',
                            )
                            second = self.content.extract_post_record(
                                url="https://www.linkedin.com/posts/claude-mackenzie_agents-demo-activity-2",
                                html=PUBLIC_POST_HTML.replace("activity-1", "activity-2"),
                                source_query='site:linkedin.com/posts "ai workflow"',
                            )
                            self.content.upsert_post(first)
                            self.content.upsert_post(second)

        self.assertEqual(calls["industries"], 1)
        self.assertEqual(calls["query_context"], 1)
        self.assertEqual(calls["fingerprints"], 1)
        self.assertEqual(calls["dimensions"], 1)
        self.assertEqual(calls["outcomes"], 1)

    def test_upsert_post_retries_transient_sqlite_lock(self) -> None:
        record = self.content.extract_post_record(
            url="https://www.linkedin.com/posts/claude-mackenzie_agents-demo-activity-3",
            html=PUBLIC_POST_HTML.replace("activity-1", "activity-3"),
            source_query='site:linkedin.com/posts "ai agents"',
        )

        real_connect = self.content.store._connect
        calls = {"insert_failures": 0, "sleep_calls": 0}

        class _ProxyConnection:
            def __init__(self, inner):
                self._inner = inner

            def execute(self, sql, params=()):
                if "INSERT INTO harvested_posts" in sql and calls["insert_failures"] == 0:
                    calls["insert_failures"] += 1
                    raise self_outer.content.sqlite3.OperationalError("database is locked")
                return self._inner.execute(sql, params)

            def __getattr__(self, name):
                return getattr(self._inner, name)

        def fake_connect():
            return _ProxyConnection(real_connect())

        def fake_sleep(_seconds: float) -> None:
            calls["sleep_calls"] += 1

        self_outer = self
        with patch.object(self.content.store, "_connect", side_effect=fake_connect):
            with patch.object(self.content.time, "sleep", side_effect=fake_sleep):
                stored = self.content.upsert_post(record)

        posts = self.content.list_posts(limit=10)

        self.assertEqual(calls["insert_failures"], 1)
        self.assertEqual(calls["sleep_calls"], 1)
        self.assertEqual(stored["url"], record["url"])
        self.assertEqual(len([post for post in posts if post["url"] == record["url"]]), 1)
