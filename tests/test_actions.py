from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from linkedin_cli.write import store


class ActionStoreEnhancementsTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tempdir.name) / "state.sqlite"
        self.artifacts_dir = Path(self.tempdir.name) / "artifacts"
        self.db_patcher = patch.object(store, "DB_PATH", self.db_path)
        self.artifacts_patcher = patch.object(store, "ARTIFACTS_DIR", self.artifacts_dir)
        self.db_patcher.start()
        self.artifacts_patcher.start()
        store.init_db()

    def tearDown(self) -> None:
        self.artifacts_patcher.stop()
        self.db_patcher.stop()
        self.tempdir.cleanup()

    @staticmethod
    def _plan() -> dict:
        return {
            "action_type": "post.publish",
            "account_id": "1708250765",
            "idempotency_key": "idem-1",
            "target_key": "me",
            "desired": {"text": "hello world"},
            "live_request": {"method": "POST", "path": "/voyager/api/test", "body": {}},
        }

    def test_cancel_action_sets_canceled_state(self) -> None:
        store.create_action(
            action_id="act_cancel",
            action_type="post.publish",
            account_id="1708250765",
            target_key="me",
            idempotency_key="idem-cancel",
            plan=self._plan(),
            dry_run=False,
        )

        action = store.cancel_action("act_cancel", reason="user requested cancel")

        self.assertEqual(action["state"], "canceled")
        self.assertEqual(action["last_error"], "user requested cancel")

    def test_write_and_list_artifacts(self) -> None:
        store.create_action(
            action_id="act_artifact",
            action_type="post.publish",
            account_id="1708250765",
            target_key="me",
            idempotency_key="idem-artifact",
            plan=self._plan(),
            dry_run=True,
        )

        artifact_path = store.write_artifact("act_artifact", "plan", {"hello": "world"})
        artifacts = store.list_artifacts("act_artifact")

        self.assertTrue(artifact_path.exists())
        self.assertEqual(artifacts[0]["kind"], "plan")
        self.assertEqual(artifacts[0]["path"], str(artifact_path))
