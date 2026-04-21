from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from linkedin_cli.write import store


class ContentRewardDatasetTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tempdir.name) / "state.sqlite"
        self.db_patcher = patch.object(store, "DB_PATH", self.db_path)
        self.db_patcher.start()
        store.init_db()

        from linkedin_cli import content_warehouse, policy

        self.content_warehouse = content_warehouse
        self.policy = policy
        self.policy.init_policy_db()

    def tearDown(self) -> None:
        self.db_patcher.stop()
        self.tempdir.cleanup()

    def test_build_reward_dataset_respects_owned_only_and_time_splits(self) -> None:
        self.content_warehouse.append_post_shard(
            job_id="reward-dataset",
            record={
                "url": "https://www.linkedin.com/posts/example-owned",
                "title": "Owned AI workflow post",
                "author_name": "Claude",
                "text": "Owned AI workflow post with proof.",
                "hook": "Owned AI workflow post with proof.",
                "structure": "insight",
                "word_count": 6,
                "reaction_count": 120,
                "comment_count": 18,
                "repost_count": 3,
                "owned_by_me": True,
                "outcome_score": 15.0,
                "industries": ["ai"],
                "published_at": "2026-03-20T10:00:00Z",
                "updated_at": "2026-03-20T12:00:00Z",
                "metadata": {"topics": ["workflow"]},
            },
            query='site:linkedin.com/posts "ai workflow"',
        )
        self.content_warehouse.append_post_shard(
            job_id="reward-dataset",
            record={
                "url": "https://www.linkedin.com/posts/example-nonowned",
                "title": "Non-owned AI workflow post",
                "author_name": "Someone Else",
                "text": "Non-owned AI workflow post with proof.",
                "hook": "Non-owned AI workflow post with proof.",
                "structure": "insight",
                "word_count": 6,
                "reaction_count": 80,
                "comment_count": 10,
                "repost_count": 1,
                "owned_by_me": False,
                "outcome_score": 9.0,
                "industries": ["ai"],
                "published_at": "2026-03-21T10:00:00Z",
                "updated_at": "2026-03-21T12:00:00Z",
                "metadata": {"topics": ["workflow"]},
            },
            query='site:linkedin.com/posts "ai workflow"',
        )
        self.content_warehouse.materialize_shards(job_id="reward-dataset")

        output_dir = Path(self.tempdir.name) / "reward-dataset-out"
        summary = self.content_warehouse.build_reward_dataset(
            output_dir=output_dir,
            industries=["ai"],
            owned_only=True,
        )

        train_rows = [json.loads(line) for line in (output_dir / "train.jsonl").read_text(encoding="utf-8").splitlines() if line.strip()]
        val_rows = [json.loads(line) for line in (output_dir / "val.jsonl").read_text(encoding="utf-8").splitlines() if line.strip()]
        test_rows = [json.loads(line) for line in (output_dir / "test.jsonl").read_text(encoding="utf-8").splitlines() if line.strip()]
        combined = train_rows + val_rows + test_rows

        self.assertEqual(summary["row_count"], 1)
        self.assertEqual(len(combined), 1)
        self.assertTrue(combined[0]["owned_by_me"])
        self.assertEqual(combined[0]["reward"], 15.0)
        self.assertEqual(combined[0]["industries"], ["ai"])

    def test_build_policy_dataset_exports_logged_rewards(self) -> None:
        decision = self.policy.choose_action_linucb(
            policy_name="content-default",
            context_type="content_publish",
            context_key="ctx-1",
            context_features=[1.0, 0.0],
            actions=[
                {"action_id": "cand_a", "label": "A", "features": [1.0, 0.0], "score": 0.7},
                {"action_id": "cand_b", "label": "B", "features": [0.0, 1.0], "score": 0.3},
            ],
            alpha=0.2,
            log_decision=True,
        )
        self.policy.record_reward(decision["decision_id"], reward_type="engagement", reward_value=2.5, payload={"source": "test"})

        output_dir = Path(self.tempdir.name) / "policy-dataset-out"
        summary = self.content_warehouse.build_policy_dataset(
            output_dir=output_dir,
            policy_name="content-default",
            context_type="content_publish",
        )

        train_rows = [json.loads(line) for line in (output_dir / "train.jsonl").read_text(encoding="utf-8").splitlines() if line.strip()]
        val_rows = [json.loads(line) for line in (output_dir / "val.jsonl").read_text(encoding="utf-8").splitlines() if line.strip()]
        test_rows = [json.loads(line) for line in (output_dir / "test.jsonl").read_text(encoding="utf-8").splitlines() if line.strip()]
        combined = train_rows + val_rows + test_rows

        self.assertEqual(summary["row_count"], 1)
        self.assertEqual(len(combined), 1)
        self.assertEqual(combined[0]["policy_name"], "content-default")
        self.assertEqual(combined[0]["context_type"], "content_publish")
        self.assertEqual(combined[0]["reward"], 2.5)
        self.assertEqual(combined[0]["chosen_action_id"], decision["chosen_action_id"])
