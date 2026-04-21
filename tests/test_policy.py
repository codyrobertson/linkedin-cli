from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from linkedin_cli.write import store


class PolicyTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tempdir.name) / "state.sqlite"
        self.db_patcher = patch.object(store, "DB_PATH", self.db_path)
        self.db_patcher.start()
        store.init_db()

        from linkedin_cli import policy

        self.policy = policy
        self.policy.init_policy_db()

    def tearDown(self) -> None:
        self.db_patcher.stop()
        self.tempdir.cleanup()

    def test_linucb_policy_logs_propensity_and_action(self) -> None:
        actions = [
            {"action_id": "cand_a", "label": "A", "features": [1.0, 0.0], "score": 0.6},
            {"action_id": "cand_b", "label": "B", "features": [0.0, 1.0], "score": 0.4},
        ]

        decision = self.policy.choose_action_linucb(
            policy_name="content-default",
            context_type="content_publish",
            context_key="ctx-1",
            context_features=[1.0, 0.25],
            actions=actions,
            alpha=0.3,
            log_decision=True,
        )

        self.assertIn(decision["chosen_action_id"], {"cand_a", "cand_b"})
        self.assertGreater(decision["propensity"], 0.0)
        self.assertLessEqual(decision["propensity"], 1.0)
        self.assertTrue(decision["decision_id"])

        logged = self.policy.get_policy_decision(decision["decision_id"])
        self.assertEqual(logged["chosen_action_id"], decision["chosen_action_id"])
        self.assertEqual(logged["context_type"], "content_publish")
        self.assertEqual(len(logged["available_actions"]), 2)

    def test_train_policy_and_offline_report(self) -> None:
        for index, (features, chosen_action, reward) in enumerate(
            [
                ([1.0, 0.0], "cand_a", 2.0),
                ([0.9, 0.1], "cand_a", 1.8),
                ([0.0, 1.0], "cand_b", 1.9),
                ([0.1, 0.9], "cand_b", 1.7),
            ],
            start=1,
        ):
            decision = self.policy.choose_action_linucb(
                policy_name="content-default",
                context_type="content_publish",
                context_key=f"ctx-{index}",
                context_features=features,
                actions=[
                    {"action_id": "cand_a", "label": "A", "features": [1.0, 0.0], "score": 0.5},
                    {"action_id": "cand_b", "label": "B", "features": [0.0, 1.0], "score": 0.5},
                ],
                alpha=0.2,
                log_decision=True,
                force_action_id=chosen_action,
            )
            self.policy.record_reward(
                decision["decision_id"],
                reward_type="engagement",
                reward_value=reward,
                payload={"source": "test"},
            )

        trained = self.policy.train_policy(
            policy_name="content-default",
            context_type="content_publish",
            min_samples=4,
            alpha=0.15,
            ridge=0.01,
        )
        report = self.policy.policy_report(policy_name="content-default", context_type="content_publish")

        self.assertTrue(trained["trained"])
        self.assertEqual(trained["sample_count"], 4)
        self.assertIn("cand_a", trained["actions"])
        self.assertIn("cand_b", trained["actions"])
        self.assertEqual(report["decision_count"], 4)
        self.assertEqual(report["reward_count"], 4)
        self.assertIn("ips", report["offline_eval"])
        self.assertIn("snips", report["offline_eval"])
        self.assertGreaterEqual(report["offline_eval"]["ips"]["estimate"], 0.0)
        self.assertGreaterEqual(report["offline_eval"]["snips"]["estimate"], 0.0)
