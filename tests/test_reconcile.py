from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from linkedin_cli.write import reconcile, store


class _FakeResponse:
    status_code = 200
    text = ""

    def __init__(self, payload: dict) -> None:
        self._payload = payload

    def json(self) -> dict:
        return self._payload


class ReconcileActionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tempdir.name) / "state.sqlite"
        self.artifacts_dir = Path(self.tempdir.name) / "artifacts"
        self.db_patcher = patch.object(store, "DB_PATH", self.db_path)
        self.store_artifacts_patcher = patch.object(store, "ARTIFACTS_DIR", self.artifacts_dir)
        self.reconcile_artifacts_patcher = patch.object(reconcile, "ARTIFACTS_DIR", self.artifacts_dir)
        self.db_patcher.start()
        self.store_artifacts_patcher.start()
        self.reconcile_artifacts_patcher.start()
        store.init_db()

    def tearDown(self) -> None:
        self.reconcile_artifacts_patcher.stop()
        self.store_artifacts_patcher.stop()
        self.db_patcher.stop()
        self.tempdir.cleanup()

    def test_reconcile_action_updates_state_and_writes_artifact(self) -> None:
        plan = {
            "action_type": "post.publish",
            "account_id": "1708250765",
            "idempotency_key": "idem-reconcile",
            "target_key": "me",
            "desired": {"text": "Hello world"},
            "reconcile": {"strategy": "feed_text_match"},
        }
        store.create_action(
            action_id="act_reconcile",
            action_type="post.publish",
            account_id="1708250765",
            target_key="me",
            idempotency_key="idem-reconcile",
            plan=plan,
            dry_run=False,
        )
        store.update_state("act_reconcile", "unknown_remote_state")

        payload = {
            "included": [
                {
                    "entityUrn": "urn:li:ugcPost:1",
                    "commentary": {"text": "Hello world"},
                }
            ]
        }

        with patch.object(reconcile, "voyager_get", return_value=_FakeResponse(payload)):
            result = reconcile.reconcile_action(session=None, action_id="act_reconcile")

        action = store.get_action("act_reconcile")
        artifacts = store.list_artifacts("act_reconcile")
        self.assertTrue(result["reconciled"])
        self.assertEqual(action["state"], "succeeded")
        self.assertTrue(any(item["kind"] == "reconcile" for item in artifacts))
