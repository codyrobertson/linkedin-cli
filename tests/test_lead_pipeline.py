from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from linkedin_cli.write import store


class LeadPipelineTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tempdir.name) / "state.sqlite"
        self.db_patcher = patch.object(store, "DB_PATH", self.db_path)
        self.db_patcher.start()
        store.init_db()

        from linkedin_cli import lead

        self.lead = lead
        self.lead.init_lead_db()

    def tearDown(self) -> None:
        self.db_patcher.stop()
        self.tempdir.cleanup()

    def test_build_lead_features_from_visible_engager(self) -> None:
        profile = {
            "display_name": "John Doe",
            "headline": "Founder building AI workflow tools for operations teams",
            "location": "San Francisco",
        }
        company = {
            "name": "Acme AI",
            "description": "AI workflow automation for revenue and ops teams",
            "industry": "Software Development",
        }
        signals = [
            {"signal_type": "commented", "source": "public"},
            {"signal_type": "profile_view", "source": "analytics"},
        ]

        built = self.lead.build_lead_features(
            profile_key="john-doe",
            profile=profile,
            company=company,
            signals=signals,
            target_topics=["ai", "workflow", "ops"],
        )

        self.assertGreater(built["fit_score"], 0)
        self.assertIn("ai", built["features"]["topic_overlap_terms"])

    def test_upsert_and_fetch_lead_features(self) -> None:
        built = self.lead.build_lead_features(
            profile_key="john-doe",
            profile={"display_name": "John Doe", "headline": "Founder in AI"},
            company={"name": "Acme AI", "description": "Workflow automation"},
            signals=[{"signal_type": "commented", "source": "public"}],
            target_topics=["ai", "workflow"],
        )
        stored = self.lead.upsert_lead_features("john-doe", built["features"], built["fit_score"])
        fetched = self.lead.get_lead_features("john-doe")

        self.assertEqual(stored["profile_key"], "john-doe")
        self.assertEqual(fetched["profile_key"], "john-doe")
        self.assertGreater(fetched["fit_score"], 0)
