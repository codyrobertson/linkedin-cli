from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from linkedin_cli.write import store


class DiscoveryLearningTests(unittest.TestCase):
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

    def test_learning_bonus_changes_queue_order_from_successful_history(self) -> None:
        self.discovery.upsert_prospect("winner", "Winner")
        self.discovery.add_source("winner", "search.saved", "founders", dedupe_key="winner-source")
        self.discovery.record_action_feedback("dm.send", "winner", succeeded=True, metadata={"template_name": "intro"})
        self.discovery.add_signal("winner", "replied_dm", source="inbox", dedupe_key="winner-reply")

        self.discovery.upsert_prospect("candidate-a", "Candidate A")
        self.discovery.add_source("candidate-a", "search.saved", "founders", dedupe_key="a-source")

        self.discovery.upsert_prospect("candidate-b", "Candidate B")
        self.discovery.add_source("candidate-b", "search.query", "founders", dedupe_key="b-source")

        queue = self.discovery.list_queue(limit=10)
        candidate_a = next(item for item in queue if item["profile_key"] == "candidate-a")
        candidate_b = next(item for item in queue if item["profile_key"] == "candidate-b")

        self.assertGreater(candidate_a["learned_score"], 0)
        self.assertGreater(candidate_a["score"], candidate_b["score"])
