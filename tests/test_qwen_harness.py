from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from linkedin_cli.write import store
from test_content_harvest import PUBLIC_POST_HTML


class QwenHarnessTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tempdir.name) / "state.sqlite"
        self.artifacts_dir = Path(self.tempdir.name) / "artifacts"
        self.db_patcher = patch.object(store, "DB_PATH", self.db_path)
        self.artifacts_patcher = patch.object(store, "ARTIFACTS_DIR", self.artifacts_dir)
        self.db_patcher.start()
        self.artifacts_patcher.start()
        store.init_db()

        from linkedin_cli import content, qwen_training

        self.content = content
        self.qwen_training = qwen_training
        self.content.init_content_db()

    def tearDown(self) -> None:
        self.db_patcher.stop()
        self.artifacts_patcher.stop()
        self.tempdir.cleanup()

    def _seed_training_inputs(self) -> None:
        owned = self.content.extract_post_record(
            url="https://www.linkedin.com/posts/claude-mackenzie_qwen-harness-owned",
            html=PUBLIC_POST_HTML,
            source_query='site:linkedin.com/posts "ai workflow"',
        )
        owned["industries"] = ["ai"]
        owned["owned_by_me"] = True
        owned["last_synced_at"] = "2026-03-27T00:00:00Z"
        owned["title"] = "AI workflow bottlenecks are where automation wins."
        owned["hook"] = "AI workflow bottlenecks are where automation wins."
        owned["text"] = (
            "AI workflow bottlenecks are where automation wins.\n\n"
            "We removed the approval handoff, routed the task automatically, and measured cycle-time savings."
        )
        owned["reaction_count"] = 260
        owned["comment_count"] = 34
        owned["word_count"] = len(owned["text"].split())

        harvested = dict(owned)
        harvested["url"] = "https://www.linkedin.com/posts/operator_qwen-harness-harvested"
        harvested["owned_by_me"] = False
        harvested["title"] = "The only AI workflow playbook that mattered was the one we measured."
        harvested["hook"] = harvested["title"]
        harvested["text"] = (
            "The only AI workflow playbook that mattered was the one we measured.\n\n"
            "We tracked approval latency, replaced the routing step, and documented the cycle-time drop."
        )
        harvested["reaction_count"] = 410
        harvested["comment_count"] = 61
        harvested["word_count"] = len(harvested["text"].split())

        self.content.upsert_post(owned)
        self.content.upsert_post(harvested)
        self.content.train_outcome_model(name="default", min_samples=2, scope="all", industry="ai", topics=["workflow"])
        self.content.queue_drafts(
            prompt="AI workflow orchestration is still too brittle for most operators.",
            industry="ai",
            topics=["workflow"],
            candidate_goals=["engagement", "instructional", "authority", "contrarian"],
            candidate_count=4,
            model="local-hash-v1",
        )

    def test_build_sft_dataset_includes_harvested_exemplars_and_registers_artifact(self) -> None:
        self._seed_training_inputs()
        output_dir = Path(self.tempdir.name) / "qwen-sft"

        summary = self.qwen_training.build_sft_dataset(
            output_dir=output_dir,
            industry="ai",
            topics=["workflow"],
            include_harvested_posts=True,
            max_harvested_posts=10,
        )

        combined = []
        for split_name in ("train", "val", "test"):
            combined.extend(
                json.loads(line)
                for line in (output_dir / f"{split_name}.jsonl").read_text(encoding="utf-8").splitlines()
                if line.strip()
            )
        self.assertEqual(summary["row_count"], len(combined))
        self.assertIn("harvested_exemplar", summary["source_counts"])
        self.assertTrue(any(item["metadata"]["source"] == "harvested_exemplar" for item in combined))

        registry_rows = [
            json.loads(line)
            for line in self.qwen_training.qwen_registry_path().read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
        self.assertTrue(any(item["artifact_type"] == "dataset" and item["phase"] == "sft" for item in registry_rows))

    def test_plan_training_run_writes_manifest_and_registry(self) -> None:
        self._seed_training_inputs()
        output_dir = Path(self.tempdir.name) / "qwen-sft"
        self.qwen_training.build_sft_dataset(output_dir=output_dir, industry="ai", topics=["workflow"])

        summary = self.qwen_training.plan_training_run(
            phase="sft",
            dataset_dir=output_dir,
            base_model="Qwen/Qwen2.5-3B-Instruct",
            output_name="ai-workflow-sft",
            runner="modal",
            wandb_project="linkedin-autonomy",
            wandb_entity="cody",
            dry_run=True,
        )

        manifest_path = Path(summary["manifest_path"])
        self.assertTrue(manifest_path.exists())
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        self.assertEqual(manifest["phase"], "sft")
        self.assertEqual(manifest["dataset"]["train_path"], str(output_dir / "train.jsonl"))
        self.assertEqual(manifest["base_model"], "Qwen/Qwen2.5-3B-Instruct")
        self.assertEqual(manifest["runner"], "modal")
        self.assertEqual(manifest["tracking"]["wandb_project"], "linkedin-autonomy")
        self.assertEqual(manifest["tracking"]["wandb_entity"], "cody")
        self.assertIn("scripts/train_qwen_content.py", summary["command"])

        registry_rows = [
            json.loads(line)
            for line in self.qwen_training.qwen_registry_path().read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
        self.assertTrue(any(item["artifact_type"] == "training_run" and item["phase"] == "sft" for item in registry_rows))
