from __future__ import annotations

import os
import sqlite3
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from linkedin_cli.write import guards, store
from linkedin_cli.write.executor import execute_action


class WriteGuardTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tempdir.name) / "state.sqlite"
        self.artifacts_dir = Path(self.tempdir.name) / "artifacts"
        self.db_patcher = patch.object(store, "DB_PATH", self.db_path)
        self.artifacts_patcher = patch.object(store, "ARTIFACTS_DIR", self.artifacts_dir)
        self.db_patcher.start()
        self.artifacts_patcher.start()
        store.init_db()
        self.env_patcher = patch.dict(os.environ, {}, clear=False)
        self.env_patcher.start()
        for key in (
            "LINKEDIN_WRITE_GUARDS",
            "LINKEDIN_WRITE_MAX_HOURLY",
            "LINKEDIN_WRITE_MAX_DAILY",
            "LINKEDIN_WRITE_QUIET_HOURS",
            "LINKEDIN_WRITE_HEALTH_STALE_MINUTES",
        ):
            os.environ.pop(key, None)

    def tearDown(self) -> None:
        self.env_patcher.stop()
        self.artifacts_patcher.stop()
        self.db_patcher.stop()
        self.tempdir.cleanup()

    @staticmethod
    def _plan(idempotency_key: str) -> dict:
        return {
            "action_type": "post.publish",
            "account_id": "1708250765",
            "idempotency_key": idempotency_key,
            "target_key": "me",
            "live_request": {"method": "POST", "path": "/voyager/api/test", "body": {}},
        }

    def test_evaluate_write_guard_blocks_hourly_budget(self) -> None:
        os.environ["LINKEDIN_WRITE_MAX_HOURLY"] = "1"
        os.environ["LINKEDIN_WRITE_MAX_DAILY"] = "99"
        prior_plan = self._plan("prior")
        store.create_action(
            action_id="act_prior",
            action_type="post.publish",
            account_id="1708250765",
            target_key="me",
            idempotency_key="prior",
            plan=prior_plan,
            dry_run=False,
        )
        store.update_state("act_prior", "succeeded")

        decision = guards.evaluate_write_guard("1708250765", "post.publish")

        self.assertFalse(decision["allowed"])
        self.assertEqual(decision["risk_flags"], ["hourly_write_budget"])

    def test_execute_action_marks_blocked_when_guard_blocks(self) -> None:
        os.environ["LINKEDIN_WRITE_MAX_HOURLY"] = "1"
        prior_plan = self._plan("prior-block")
        store.create_action(
            action_id="act_prior_block",
            action_type="post.publish",
            account_id="1708250765",
            target_key="me",
            idempotency_key="prior-block",
            plan=prior_plan,
            dry_run=False,
        )
        store.update_state("act_prior_block", "succeeded")
        plan = self._plan("budget-blocked")

        result = execute_action(
            session=None,
            action_id="act_blocked",
            plan=plan,
            account_id="1708250765",
            dry_run=False,
        )

        action = store.get_action("act_blocked")
        self.assertEqual(result["status"], "blocked")
        self.assertEqual(action["state"], "blocked")
        self.assertIn("hourly_write_budget", action["risk_flags"])

    def test_action_health_report_flags_stuck_unknown_due_and_overdue(self) -> None:
        now = datetime(2026, 4, 21, 18, 0, tzinfo=timezone.utc)
        for action_id, state, idem in [
            ("act_executing", "executing", "exec"),
            ("act_unknown", "unknown_remote_state", "unknown"),
            ("act_retry", "retry_scheduled", "retry"),
            ("act_schedule", "planned", "schedule"),
        ]:
            action_type = "post.scheduled" if action_id == "act_schedule" else "post.publish"
            plan = self._plan(idem)
            plan["action_type"] = action_type
            store.create_action(
                action_id=action_id,
                action_type=action_type,
                account_id="1708250765",
                target_key="me",
                idempotency_key=idem,
                plan=plan,
                dry_run=False,
                scheduled_at=(now - timedelta(minutes=10)).isoformat() if action_id == "act_schedule" else None,
            )
            store.update_state(
                action_id,
                state,
                next_attempt_at=(now - timedelta(minutes=1)).isoformat() if action_id == "act_retry" else None,
            )
        conn = sqlite3.connect(str(self.db_path))
        try:
            conn.execute(
                "UPDATE actions SET updated_at = ? WHERE action_id = ?",
                ((now - timedelta(minutes=45)).isoformat(), "act_executing"),
            )
            conn.commit()
        finally:
            conn.close()

        report = guards.action_health_report(now=now, stale_minutes=30)

        self.assertEqual(report["status"], "needs_attention")
        self.assertEqual(report["counts"]["stuck_executing"], 1)
        self.assertEqual(report["counts"]["unknown_remote_state"], 1)
        self.assertEqual(report["counts"]["due_retries"], 1)
        self.assertEqual(report["counts"]["overdue_scheduled"], 1)


if __name__ == "__main__":
    unittest.main()
