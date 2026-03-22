from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from linkedin_cli.write import store


class DiscoveryAnalyticsTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tempdir.name) / "state.sqlite"
        self.db_patcher = patch.object(store, "DB_PATH", self.db_path)
        self.db_patcher.start()
        store.init_db()

        from linkedin_cli import discovery

        self.discovery = discovery
        self.discovery.init_discovery_db()

    def tearDown(self) -> None:
        self.db_patcher.stop()
        self.tempdir.cleanup()

    def test_search_ingestion_supports_people_companies_and_posts(self) -> None:
        self.discovery.ingest_search_results(
            kind="people",
            query="fintech founder",
            results=[{"url": "https://www.linkedin.com/in/john-doe/", "slug": "john-doe", "title": "John Doe - Founder"}],
            source_label="fintech founder",
        )
        self.discovery.ingest_search_results(
            kind="companies",
            query="ai startup",
            results=[{"url": "https://www.linkedin.com/company/acme/", "slug": "acme", "title": "Acme | LinkedIn"}],
            source_label="ai startup",
        )
        self.discovery.ingest_search_results(
            kind="posts",
            query="ai agents",
            results=[{"url": "https://www.linkedin.com/posts/openai_demo-post-activity-1", "title": "Demo post"}],
            source_label="ai agents",
        )

        queue = self.discovery.list_queue(limit=10)
        entity_types = {item["entity_type"] for item in queue}

        self.assertIn("person", entity_types)
        self.assertIn("company", entity_types)
        self.assertIn("post", entity_types)

    def test_stats_include_reply_acceptance_source_conversion_and_template_performance(self) -> None:
        self.discovery.upsert_prospect("john-doe", "John Doe")
        self.discovery.add_source("john-doe", "search.saved", "founders", dedupe_key="s1")
        self.discovery.record_action_feedback("dm.send", "john-doe", succeeded=True, metadata={"template_name": "intro"})
        self.discovery.add_signal("john-doe", "replied_dm", source="inbox", dedupe_key="reply1")

        self.discovery.upsert_prospect("jane-doe", "Jane Doe")
        self.discovery.add_source("jane-doe", "search.saved", "founders", dedupe_key="s2")
        self.discovery.record_action_feedback("connect", "jane-doe", succeeded=True)
        self.discovery.add_signal("jane-doe", "accepted", source="network", dedupe_key="acc1")

        stats = self.discovery.queue_stats()

        self.assertEqual(stats["reply_rate"], 1.0)
        self.assertEqual(stats["acceptance_rate"], 1.0)
        self.assertGreater(stats["source_conversion"]["search.saved"]["success_rate"], 0)
        self.assertEqual(stats["template_performance"]["intro"]["reply_count"], 1)
