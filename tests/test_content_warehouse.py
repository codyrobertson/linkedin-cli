from __future__ import annotations

import json
import tempfile
import textwrap
import unittest
from pathlib import Path
from unittest.mock import patch

from linkedin_cli.write import store


PUBLIC_POST_HTML = textwrap.dedent(
    """
    <html>
      <head>
        <title>Ship the workflow, not the demo | Claude Mackenzie</title>
        <script type="application/ld+json">
          {
            "@context": "http://schema.org",
            "@type": "SocialMediaPosting",
            "@id": "https://www.linkedin.com/posts/claude-mackenzie_demo-activity-1",
            "headline": "Ship the workflow, not the demo",
            "articleBody": "Ship the workflow, not the demo.\\n\\n1. Start with the ops pain.\\n2. Remove a handoff.\\n3. Measure the hours saved.",
            "datePublished": "2026-03-20T10:00:00Z",
            "author": {
              "@type": "Person",
              "name": "Claude Mackenzie",
              "url": "https://www.linkedin.com/in/claude-mackenzie-06510b3b8/"
            },
            "interactionStatistic": [
              {"@type": "InteractionCounter", "interactionType": "http://schema.org/LikeAction", "userInteractionCount": 75},
              {"@type": "InteractionCounter", "interactionType": "http://schema.org/CommentAction", "userInteractionCount": 12}
            ]
          }
        </script>
      </head>
      <body></body>
    </html>
    """
)


class ContentWarehouseTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tempdir.name) / "state.sqlite"
        self.artifacts_dir = Path(self.tempdir.name) / "artifacts"
        self.db_patcher = patch.object(store, "DB_PATH", self.db_path)
        self.artifacts_patcher = patch.object(store, "ARTIFACTS_DIR", self.artifacts_dir)
        self.db_patcher.start()
        self.artifacts_patcher.start()
        store.init_db()

        from linkedin_cli import content
        from linkedin_cli import content_warehouse

        self.content = content
        self.content_warehouse = content_warehouse
        self.content.init_content_db()

    def tearDown(self) -> None:
        self.artifacts_patcher.stop()
        self.db_patcher.stop()
        self.tempdir.cleanup()

    def test_harvest_posts_writes_local_raw_shard(self) -> None:
        summary = self.content.harvest_posts(
            industries=["ai", "fintech"],
            limit=1,
            per_query=5,
            query_terms=['site:linkedin.com/posts "ai agents"'],
            auth_search_fn=None,
            search_fn=lambda _query, limit=10: [
                {
                    "title": "Agents post",
                    "url": "https://www.linkedin.com/posts/claude-mackenzie_demo-activity-1",
                    "snippet": "agents",
                }
            ],
            fetch_html=lambda _url: PUBLIC_POST_HTML,
            job_name="warehouse-job",
        )

        shard_path = self.content_warehouse.shard_file_path("warehouse-job")
        self.assertEqual(summary["stored_count"], 1)
        self.assertTrue(shard_path.exists())
        rows = [json.loads(line) for line in shard_path.read_text(encoding="utf-8").splitlines() if line.strip()]
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["job_id"], "warehouse-job")
        self.assertEqual(rows[0]["industries"], ["ai"])

    def test_materialize_and_warehouse_stats_work_locally(self) -> None:
        self.content.harvest_posts(
            industries=["ai", "fintech"],
            limit=2,
            per_query=5,
            query_terms=['site:linkedin.com/posts "ai agents"', 'site:linkedin.com/posts "fintech workflow"'],
            auth_search_fn=None,
            search_fn=lambda query, limit=10: [
                {
                    "title": query,
                    "url": f"https://www.linkedin.com/posts/claude-mackenzie_demo-activity-{1 if 'ai' in query else 2}",
                    "snippet": query,
                }
            ],
            fetch_html=lambda url: PUBLIC_POST_HTML.replace("activity-1", url.rsplit("-", 1)[-1]),
            job_name="warehouse-stats",
        )

        summary = self.content_warehouse.materialize_shards(job_id="warehouse-stats")
        stats = self.content_warehouse.warehouse_stats()

        self.assertEqual(summary["rows_loaded"], 2)
        self.assertEqual(stats["post_count"], 2)
        self.assertEqual(stats["industries"]["ai"], 1)
        self.assertEqual(stats["industries"]["fintech"], 1)

    def test_materialize_normalizes_industries_from_source_query_over_polluted_shard_labels(self) -> None:
        self.content_warehouse.append_post_shard(
            job_id="warehouse-normalize",
            query='site:linkedin.com/posts "ai workflow"',
            record={
                "url": "https://www.linkedin.com/posts/claude-mackenzie_demo-activity-99",
                "title": "AI workflow bottlenecks are where automation wins.",
                "author_name": "Claude Mackenzie",
                "author_url": "https://www.linkedin.com/in/claude-mackenzie-06510b3b8/",
                "published_at": "2026-03-20T10:00:00Z",
                "text": "AI workflow bottlenecks are where automation wins. We mapped the handoff and proved the latency drop.",
                "hook": "AI workflow bottlenecks are where automation wins.",
                "structure": "insight",
                "word_count": 16,
                "reaction_count": 75,
                "comment_count": 12,
                "repost_count": 3,
                "owned_by_me": False,
                "outcome_score": 9.4,
                "source_query": 'site:linkedin.com/posts "ai workflow"',
                "industries": ["ai", "fintech", "healthcare"],
                "metadata": {"query_industries": ["ai"]},
                "updated_at": "2026-03-20T12:00:00Z",
            },
        )

        self.content_warehouse.materialize_shards(job_id="warehouse-normalize")
        stats = self.content_warehouse.warehouse_stats()

        self.assertEqual(stats["industries"]["ai"], 1)
        self.assertNotIn("fintech", stats["industries"])
        self.assertNotIn("healthcare", stats["industries"])

    def test_build_training_dataset_splits_locally_by_time(self) -> None:
        for index, date_text in enumerate(
            [
                "2026-03-20T10:00:00Z",
                "2026-03-21T10:00:00Z",
                "2026-03-22T10:00:00Z",
                "2026-03-23T10:00:00Z",
                "2026-03-24T10:00:00Z",
            ],
            start=1,
        ):
            html = PUBLIC_POST_HTML.replace("2026-03-20T10:00:00Z", date_text).replace("activity-1", f"activity-{index}")
            self.content.harvest_posts(
                industries=["ai"],
                limit=1,
                per_query=5,
                query_terms=[f'site:linkedin.com/posts "ai agents {index}"'],
                auth_search_fn=None,
                search_fn=lambda _query, limit=10, idx=index: [
                    {
                        "title": f"Agents {idx}",
                        "url": f"https://www.linkedin.com/posts/claude-mackenzie_demo-activity-{idx}",
                        "snippet": "agents",
                    }
                ],
                fetch_html=lambda _url, html_value=html: html_value,
                job_name=f"dataset-{index}",
            )

        self.content_warehouse.materialize_shards()
        output_dir = Path(self.tempdir.name) / "dataset"
        summary = self.content_warehouse.build_training_dataset(output_dir=output_dir, industries=["ai"])

        self.assertEqual(summary["row_count"], 5)
        self.assertTrue((output_dir / "train.jsonl").exists())
        self.assertTrue((output_dir / "val.jsonl").exists())
        self.assertTrue((output_dir / "test.jsonl").exists())
        train_rows = [line for line in (output_dir / "train.jsonl").read_text(encoding="utf-8").splitlines() if line.strip()]
        val_rows = [line for line in (output_dir / "val.jsonl").read_text(encoding="utf-8").splitlines() if line.strip()]
        test_rows = [line for line in (output_dir / "test.jsonl").read_text(encoding="utf-8").splitlines() if line.strip()]
        self.assertEqual(len(train_rows) + len(val_rows) + len(test_rows), 5)

    def test_generate_benchmark_corpus_writes_requested_rows(self) -> None:
        summary = self.content_warehouse.generate_benchmark_corpus(
            job_id="bench-1",
            row_count=25,
            industries=["ai", "fintech"],
            topics=["agents", "workflow"],
        )

        shard_path = self.content_warehouse.shard_file_path("bench-1")
        rows = [json.loads(line) for line in shard_path.read_text(encoding="utf-8").splitlines() if line.strip()]

        self.assertEqual(summary["row_count"], 25)
        self.assertEqual(summary["industries"], ["ai", "fintech"])
        self.assertEqual(len(rows), 25)
        self.assertEqual(rows[0]["job_id"], "bench-1")

    def test_benchmark_warehouse_persists_report(self) -> None:
        self.content_warehouse.generate_benchmark_corpus(
            job_id="bench-run",
            row_count=40,
            industries=["ai"],
            topics=["agents"],
        )

        summary = self.content_warehouse.benchmark_warehouse(
            job_id="bench-run",
            dataset_output=Path(self.tempdir.name) / "bench-dataset",
            industries=["ai"],
        )
        reports = self.content_warehouse.benchmark_reports(limit=5)

        self.assertEqual(summary["job_id"], "bench-run")
        self.assertEqual(summary["generated_row_count"], 40)
        self.assertGreaterEqual(summary["materialize_seconds"], 0.0)
        self.assertGreaterEqual(summary["warehouse_stats_seconds"], 0.0)
        self.assertGreaterEqual(summary["build_dataset_seconds"], 0.0)
        self.assertTrue(Path(summary["report_path"]).exists())
        self.assertTrue(any(report["job_id"] == "bench-run" for report in reports))

    def test_train_warehouse_model_persists_local_artifact(self) -> None:
        self.content_warehouse.generate_benchmark_corpus(
            job_id="train-run",
            row_count=200,
            industries=["ai", "fintech"],
            topics=["agents", "workflow"],
        )
        self.content_warehouse.materialize_shards(job_id="train-run")

        summary = self.content_warehouse.train_warehouse_model(
            name="warehouse-viral-test",
            industries=["ai"],
            min_samples=50,
        )
        model = self.content_warehouse.get_warehouse_model("warehouse-viral-test")

        self.assertTrue(summary["trained"])
        self.assertEqual(summary["model_name"], "warehouse-viral-test")
        self.assertGreaterEqual(summary["sample_count"], 50)
        self.assertTrue(Path(summary["artifact_path"]).exists())
        self.assertIsNotNone(model)
        self.assertEqual(model["model_name"], "warehouse-viral-test")
