from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from linkedin_cli.write import store
from linkedin_cli.write import executor as executor_mod
from linkedin_cli.write.executor import execute_action


class ExecuteActionIdempotencyTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tempdir.name) / "state.sqlite"
        self.db_patcher = patch.object(store, "DB_PATH", self.db_path)
        self.db_patcher.start()
        store.init_db()

        self.lock_patcher = patch.object(executor_mod, "_acquire_lock", lambda: None)
        self.unlock_patcher = patch.object(executor_mod, "_release_lock", lambda _fh: None)
        self.sleep_patcher = patch.object(executor_mod.time, "sleep", lambda *_args, **_kwargs: None)
        self.jitter_patcher = patch.object(executor_mod.random, "uniform", lambda _a, _b: 0)
        self.publish_patcher = patch.object(
            executor_mod,
            "_post_publish",
            lambda _session, _plan: {"http_status": 201, "remote_ref": "ok"},
        )

        for patcher in (
            self.lock_patcher,
            self.unlock_patcher,
            self.sleep_patcher,
            self.jitter_patcher,
            self.publish_patcher,
        ):
            patcher.start()

    def tearDown(self) -> None:
        for patcher in (
            self.publish_patcher,
            self.jitter_patcher,
            self.sleep_patcher,
            self.unlock_patcher,
            self.lock_patcher,
            self.db_patcher,
        ):
            patcher.stop()
        self.tempdir.cleanup()

    @staticmethod
    def _plan(idempotency_key: str) -> dict:
        return {
            "action_type": "post.scheduled",
            "account_id": "1708250765",
            "idempotency_key": idempotency_key,
            "target_key": "me",
            "live_request": {"method": "POST", "path": "/voyager/api/test", "body": {}},
        }

    def test_execute_action_reuses_same_action_id_for_scheduled_posts(self) -> None:
        plan = self._plan("idem-same-action")
        store.create_action(
            action_id="act_same",
            action_type="post.scheduled",
            account_id="1708250765",
            target_key="me",
            idempotency_key=plan["idempotency_key"],
            plan=plan,
            dry_run=False,
            scheduled_at="2026-03-16T00:00:00",
        )

        result = execute_action(
            session=None,
            action_id="act_same",
            plan=plan,
            account_id="1708250765",
            dry_run=False,
        )

        self.assertEqual(result["status"], "succeeded")
        self.assertEqual(store.get_action("act_same")["state"], "succeeded")

    def test_execute_action_still_blocks_true_duplicate_actions(self) -> None:
        plan = self._plan("idem-duplicate")
        store.create_action(
            action_id="act_existing",
            action_type="post.scheduled",
            account_id="1708250765",
            target_key="me",
            idempotency_key=plan["idempotency_key"],
            plan=plan,
            dry_run=False,
            scheduled_at="2026-03-16T00:00:00",
        )

        result = execute_action(
            session=None,
            action_id="act_new",
            plan=plan,
            account_id="1708250765",
            dry_run=False,
        )

        self.assertEqual(result["status"], "duplicate_skipped")
        self.assertEqual(result["existing_action"]["action_id"], "act_existing")


if __name__ == "__main__":
    unittest.main()
