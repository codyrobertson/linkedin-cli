from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from linkedin_cli.write import store


class DiscoverySignalTests(unittest.TestCase):
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

    def test_manual_engagement_signal_reorders_queue_and_stats(self) -> None:
        self.discovery.upsert_prospect("john-doe", "John Doe")
        self.discovery.add_source("john-doe", "search.query", "fintech founder")

        self.discovery.upsert_prospect("jane-doe", "Jane Doe")
        self.discovery.add_source("jane-doe", "search.query", "fintech founder")
        self.discovery.add_signal("jane-doe", "liked", source="public")

        self.discovery.add_signal("john-doe", "commented", source="public", notes="Asked about pricing")

        queue = self.discovery.list_queue(limit=10)
        stats = self.discovery.queue_stats()

        self.assertEqual(queue[0]["profile_key"], "john-doe")
        self.assertEqual(stats["signals_by_type"]["commented"], 1)
        self.assertEqual(stats["sources_by_type"]["search.query"], 2)
