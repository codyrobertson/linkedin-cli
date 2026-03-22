from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from linkedin_cli.write import store


class WorkflowStoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tempdir.name) / "state.sqlite"
        self.db_patcher = patch.object(store, "DB_PATH", self.db_path)
        self.db_patcher.start()
        store.init_db()

        from linkedin_cli import workflow

        self.workflow = workflow
        self.workflow.init_workflow_db()

    def tearDown(self) -> None:
        self.db_patcher.stop()
        self.tempdir.cleanup()

    def test_saved_searches_templates_and_contacts_round_trip(self) -> None:
        saved = self.workflow.save_search(
            name="founders",
            kind="people",
            query="fintech founder",
            limit=3,
            enrich=True,
        )
        template = self.workflow.save_template(
            name="intro",
            kind="dm",
            body="Hi {name}, enjoyed meeting you.",
        )
        contact = self.workflow.upsert_contact(
            profile_key="john-doe",
            display_name="John Doe",
            stage="new",
            tags=["lead", "founder"],
            notes="Met at a meetup",
        )

        self.assertEqual(saved["name"], "founders")
        self.assertEqual(template["kind"], "dm")
        self.assertEqual(contact["stage"], "new")
        self.assertEqual(self.workflow.render_template("intro", {"name": "John"}), "Hi John, enjoyed meeting you.")

    def test_inbox_triage_round_trip(self) -> None:
        triage = self.workflow.upsert_inbox_item(
            conversation_urn="urn:li:msg_conversation:1",
            state="follow_up",
            priority="high",
            notes="Reply tomorrow",
        )

        items = self.workflow.list_inbox_items(state="follow_up")

        self.assertEqual(triage["state"], "follow_up")
        self.assertEqual(items[0]["priority"], "high")

    def test_contact_csv_export_and_import(self) -> None:
        self.workflow.upsert_contact(
            profile_key="john-doe",
            display_name="John Doe",
            stage="qualified",
            tags=["lead"],
            notes="Warm intro",
        )

        csv_path = Path(self.tempdir.name) / "contacts.csv"
        self.workflow.export_contacts_csv(csv_path)

        second_tempdir = tempfile.TemporaryDirectory()
        second_db_path = Path(second_tempdir.name) / "state.sqlite"
        with patch.object(store, "DB_PATH", second_db_path):
            self.workflow.init_workflow_db()
            imported = self.workflow.import_contacts_csv(csv_path)
            contacts = self.workflow.list_contacts()

        self.assertEqual(imported, 1)
        self.assertEqual(len(contacts), 1)
        self.assertEqual(contacts[0]["profile_key"], "john-doe")
        second_tempdir.cleanup()
