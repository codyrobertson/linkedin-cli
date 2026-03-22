from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from linkedin_cli.write import store


class DiscoveryIngestTests(unittest.TestCase):
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

    def test_ingest_search_results_creates_prospects(self) -> None:
        results = [
            {
                "url": "https://www.linkedin.com/in/john-doe/",
                "title": "John Doe - Founder - Acme",
                "snippet": "Founder at Acme",
                "slug": "john-doe",
                "summary": {"headline": "Founder at Acme"},
            }
        ]

        created = self.discovery.ingest_search_results(
            kind="people",
            query="fintech founder",
            results=results,
            source_label="saved:founders",
        )

        queue = self.discovery.list_queue(limit=10)
        self.assertEqual(created, 1)
        self.assertEqual(queue[0]["profile_key"], "john-doe")
        self.assertEqual(queue[0]["source_count"], 1)

    def test_ingest_inbox_conversations_adds_high_intent_signals(self) -> None:
        conversations = [
            {
                "conversation_urn": "urn:li:msg_conversation:1",
                "participants": [
                    {
                        "profile_key": "john-doe",
                        "display_name": "John Doe",
                        "public_identifier": "john-doe",
                    }
                ],
            }
        ]

        created = self.discovery.ingest_inbox_conversations(conversations)
        queue = self.discovery.list_queue(limit=10)

        self.assertEqual(created, 1)
        self.assertEqual(queue[0]["profile_key"], "john-doe")
        self.assertGreaterEqual(queue[0]["intent_score"], 8.0)
