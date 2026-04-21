from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from linkedin_cli.write import store
from test_content_harvest import PUBLIC_POST_HTML


class ReplayEnvironmentTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tempdir.name) / "state.sqlite"
        self.artifacts_dir = Path(self.tempdir.name) / "artifacts"
        self.db_patcher = patch.object(store, "DB_PATH", self.db_path)
        self.artifacts_patcher = patch.object(store, "ARTIFACTS_DIR", self.artifacts_dir)
        self.db_patcher.start()
        self.artifacts_patcher.start()
        store.init_db()

        from linkedin_cli import content, replay_env

        self.content = content
        self.replay_env = replay_env
        self.content.init_content_db()

    def tearDown(self) -> None:
        self.db_patcher.stop()
        self.artifacts_patcher.stop()
        self.tempdir.cleanup()

    def _seed_posts(self) -> None:
        strong = self.content.extract_post_record(
            url="https://www.linkedin.com/posts/claude-mackenzie_replay-strong",
            html=PUBLIC_POST_HTML,
            source_query='site:linkedin.com/posts "ai workflow"',
        )
        strong["industries"] = ["ai"]
        strong["owned_by_me"] = True
        strong["title"] = "AI workflow bottlenecks are where automation wins."
        strong["hook"] = strong["title"]
        strong["text"] = (
            "AI workflow bottlenecks are where automation wins.\n\n"
            "We removed the approval handoff, routed work automatically, and measured the cycle-time drop."
        )
        strong["reaction_count"] = 250
        strong["comment_count"] = 30
        strong["word_count"] = len(strong["text"].split())
        self.content.upsert_post(strong)
        self.content.train_outcome_model(name="default", min_samples=1, scope="owned", industry="ai", topics=["workflow"])

    def test_replay_trace_reconstructs_request_and_reward(self) -> None:
        self._seed_posts()
        result = self.content.run_autonomy(
            prompt="AI workflow orchestration is still too brittle for most operators.",
            industry="ai",
            topics=["workflow"],
            model="local-hash-v1",
            candidate_count=3,
            mode="review",
        )
        replay = self.replay_env.replay_trace(result["trace_id"], policy_name="content-default")

        self.assertEqual(replay["trace_id"], result["trace_id"])
        self.assertEqual(replay["request"]["task_type"], "content_publish")
        self.assertIn("chosen_action_id", replay["response"])
        self.assertGreaterEqual(replay["step_count"], 3)

