from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from linkedin_cli.write import scheduler, store


class SchedulerDuePostTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tempdir.name) / "state.sqlite"
        self.artifacts_dir = Path(self.tempdir.name) / "artifacts"
        self.store_db_patcher = patch.object(store, "DB_PATH", self.db_path)
        self.scheduler_db_patcher = patch.object(scheduler, "DB_PATH", self.db_path)
        self.artifacts_patcher = patch.object(store, "ARTIFACTS_DIR", self.artifacts_dir)
        self.store_db_patcher.start()
        self.scheduler_db_patcher.start()
        self.artifacts_patcher.start()
        store.init_db()

    def tearDown(self) -> None:
        self.artifacts_patcher.stop()
        self.scheduler_db_patcher.stop()
        self.store_db_patcher.stop()
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

    def test_due_scheduled_posts_compare_parsed_datetimes(self) -> None:
        due_plan = self._plan("due")
        future_plan = self._plan("future")
        store.create_action(
            action_id="act_due",
            action_type="post.scheduled",
            account_id="1708250765",
            target_key="me",
            idempotency_key=due_plan["idempotency_key"],
            plan=due_plan,
            dry_run=False,
            scheduled_at="2026-04-21T09:00:00-07:00",
        )
        store.create_action(
            action_id="act_future",
            action_type="post.scheduled",
            account_id="1708250765",
            target_key="me",
            idempotency_key=future_plan["idempotency_key"],
            plan=future_plan,
            dry_run=False,
            scheduled_at="2026-04-21T10:00:00-07:00",
        )

        due = scheduler.get_due_scheduled_posts(
            now=datetime(2026, 4, 21, 16, 30, tzinfo=timezone.utc)
        )

        self.assertEqual([item["action_id"] for item in due], ["act_due"])


if __name__ == "__main__":
    unittest.main()
