from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from linkedin_cli.write import store
from test_content_harvest import PUBLIC_POST_HTML


class QwenTrainingDatasetTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tempdir.name) / "state.sqlite"
        self.db_patcher = patch.object(store, "DB_PATH", self.db_path)
        self.db_patcher.start()
        store.init_db()

        from linkedin_cli import content, qwen_training

        self.content = content
        self.qwen_training = qwen_training
        self.content.init_content_db()

    def tearDown(self) -> None:
        self.db_patcher.stop()
        self.tempdir.cleanup()

    def _seed_content_queue(self) -> None:
        strong = self.content.extract_post_record(
            url="https://www.linkedin.com/posts/claude-mackenzie_qwen-activity-strong",
            html=PUBLIC_POST_HTML,
            source_query='site:linkedin.com/posts "ai workflow"',
        )
        strong["industries"] = ["ai"]
        strong["owned_by_me"] = True
        strong["last_synced_at"] = "2026-03-26T00:00:00Z"
        strong["title"] = "AI workflow bottlenecks are where automation wins."
        strong["hook"] = "AI workflow bottlenecks are where automation wins."
        strong["text"] = (
            "AI workflow bottlenecks are where automation wins.\n\n"
            "We removed the approval handoff, routed the task automatically, and measured cycle-time savings."
        )
        strong["reaction_count"] = 260
        strong["comment_count"] = 34
        strong["word_count"] = len(strong["text"].split())

        second = dict(strong)
        second["url"] = "https://www.linkedin.com/posts/claude-mackenzie_qwen-activity-second"
        second["title"] = "Most AI workflow advice is too generic to ship."
        second["hook"] = "Most AI workflow advice is too generic to ship."
        second["text"] = (
            "Most AI workflow advice is too generic to ship.\n\n"
            "The useful work starts when you map the handoff, kill one bottleneck, and prove the before-and-after."
        )
        second["reaction_count"] = 210
        second["comment_count"] = 26
        second["word_count"] = len(second["text"].split())

        self.content.upsert_post(strong)
        self.content.upsert_post(second)
        self.content.train_outcome_model(name="default", min_samples=2, scope="owned", industry="ai", topics=["workflow"])
        self.content.queue_drafts(
            prompt="AI workflow orchestration is still too brittle for most operators.",
            industry="ai",
            topics=["workflow"],
            candidate_goals=["engagement", "instructional", "authority", "contrarian"],
            candidate_count=4,
            model="local-hash-v1",
        )

    def test_build_sft_dataset_uses_owned_posts_and_chosen_candidates(self) -> None:
        self._seed_content_queue()
        output_dir = Path(self.tempdir.name) / "qwen-sft"

        summary = self.qwen_training.build_sft_dataset(output_dir=output_dir, industry="ai", topics=["workflow"])

        train_rows = [json.loads(line) for line in (output_dir / "train.jsonl").read_text(encoding="utf-8").splitlines() if line.strip()]
        val_rows = [json.loads(line) for line in (output_dir / "val.jsonl").read_text(encoding="utf-8").splitlines() if line.strip()]
        test_rows = [json.loads(line) for line in (output_dir / "test.jsonl").read_text(encoding="utf-8").splitlines() if line.strip()]
        combined = train_rows + val_rows + test_rows

        self.assertEqual(summary["row_count"], len(combined))
        self.assertGreaterEqual(summary["row_count"], 2)
        self.assertTrue(all(item["messages"][-1]["role"] == "assistant" for item in combined))
        self.assertTrue(any(item["metadata"]["source"] == "content_candidate" for item in combined))
        self.assertTrue(any(item["metadata"]["source"] == "owned_post" for item in combined))

    def test_build_preference_dataset_uses_chosen_vs_rejected_candidates(self) -> None:
        self._seed_content_queue()
        output_dir = Path(self.tempdir.name) / "qwen-pref"

        summary = self.qwen_training.build_preference_dataset(output_dir=output_dir, industry="ai", topics=["workflow"])

        train_rows = [json.loads(line) for line in (output_dir / "train.jsonl").read_text(encoding="utf-8").splitlines() if line.strip()]
        val_rows = [json.loads(line) for line in (output_dir / "val.jsonl").read_text(encoding="utf-8").splitlines() if line.strip()]
        test_rows = [json.loads(line) for line in (output_dir / "test.jsonl").read_text(encoding="utf-8").splitlines() if line.strip()]
        combined = train_rows + val_rows + test_rows

        self.assertEqual(summary["row_count"], len(combined))
        self.assertGreaterEqual(summary["row_count"], 1)
        self.assertTrue(all(item["chosen"] != item["rejected"] for item in combined))
        self.assertTrue(all(item["metadata"]["industry"] == "ai" for item in combined))
