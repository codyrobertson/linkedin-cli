from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from linkedin_cli.write import store


class DiscoveryStorageTests(unittest.TestCase):
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

    def test_queue_orders_prospects_by_composite_score(self) -> None:
        self.discovery.upsert_prospect(
            profile_key="john-doe",
            display_name="John Doe",
            public_identifier="john-doe",
            headline="Founder at Acme",
        )
        self.discovery.add_source("john-doe", source_type="search.query", source_value="fintech founder")
        self.discovery.add_signal("john-doe", signal_type="replied_dm", source="inbox")

        self.discovery.upsert_prospect(
            profile_key="jane-doe",
            display_name="Jane Doe",
            public_identifier="jane-doe",
            headline="Operator at Beta",
        )
        self.discovery.add_source("jane-doe", source_type="search.query", source_value="fintech founder")
        self.discovery.add_signal("jane-doe", signal_type="liked", source="public")

        queue = self.discovery.list_queue(limit=10)

        self.assertEqual(queue[0]["profile_key"], "john-doe")
        self.assertGreater(queue[0]["score"], queue[1]["score"])
        self.assertEqual(queue[0]["fit_score"], 3.0)
        self.assertEqual(queue[0]["intent_score"], 10.0)

    def test_prospect_detail_includes_sources_and_signals(self) -> None:
        self.discovery.upsert_prospect(
            profile_key="john-doe",
            display_name="John Doe",
            public_identifier="john-doe",
        )
        self.discovery.add_source("john-doe", source_type="search.saved", source_value="founders")
        self.discovery.add_signal("john-doe", signal_type="commented", source="public", notes="commented twice")

        prospect = self.discovery.get_prospect("john-doe")

        self.assertEqual(prospect["profile_key"], "john-doe")
        self.assertEqual(len(prospect["sources"]), 1)
        self.assertEqual(prospect["signals"][0]["signal_type"], "commented")
