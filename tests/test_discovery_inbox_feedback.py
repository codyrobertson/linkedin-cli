from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from linkedin_cli.write import store


class DiscoveryInboxFeedbackTests(unittest.TestCase):
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

    def test_inbox_ingestion_marks_inbound_and_reply_signals(self) -> None:
        created = self.discovery.ingest_inbox_conversations(
            [
                {
                    "conversation_urn": "urn:li:msg_conversation:1",
                    "participants": [
                        {
                            "profile_key": "john-doe",
                            "public_identifier": "john-doe",
                            "display_name": "John Doe",
                            "member_urn": "urn:li:fsd_profile:john",
                        }
                    ],
                    "messages": [
                        {"message_urn": "m1", "sender_urn": "urn:li:fsd_profile:me", "created_at": 100, "text": "Hi"},
                        {"message_urn": "m2", "sender_urn": "urn:li:fsd_profile:john", "created_at": 200, "text": "Reply"},
                    ],
                }
            ],
            self_member_urn="urn:li:fsd_profile:me",
        )

        prospect = self.discovery.get_prospect("john-doe")
        signal_types = [item["signal_type"] for item in prospect["signals"]]

        self.assertEqual(created, 1)
        self.assertIn("inbound_dm", signal_types)
        self.assertIn("replied_dm", signal_types)

    def test_record_action_feedback_updates_state(self) -> None:
        self.discovery.upsert_prospect("john-doe", "John Doe", public_identifier="john-doe")

        self.discovery.record_action_feedback(
            action_type="dm.send",
            profile_key="john-doe",
            succeeded=True,
            metadata={"template_name": "intro"},
        )
        self.discovery.set_prospect_state("john-doe", "waiting")

        prospect = self.discovery.get_prospect("john-doe")
        self.assertEqual(prospect["state"], "waiting")
        self.assertEqual(prospect["signals"][0]["signal_type"], "outreach_sent")
