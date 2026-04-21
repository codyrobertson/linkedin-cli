from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from linkedin_cli.write import store
from test_content_harvest import PUBLIC_POST_HTML


class EvalSuiteTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tempdir.name) / "state.sqlite"
        self.artifacts_dir = Path(self.tempdir.name) / "artifacts"
        self.db_patcher = patch.object(store, "DB_PATH", self.db_path)
        self.artifacts_patcher = patch.object(store, "ARTIFACTS_DIR", self.artifacts_dir)
        self.db_patcher.start()
        self.artifacts_patcher.start()
        store.init_db()

        from linkedin_cli import content, evals, policy, qwen_training, runtime_contract

        self.content = content
        self.evals = evals
        self.policy = policy
        self.qwen_training = qwen_training
        self.runtime_contract = runtime_contract
        self.content.init_content_db()
        self.policy.init_policy_db()

    def tearDown(self) -> None:
        self.db_patcher.stop()
        self.artifacts_patcher.stop()
        self.tempdir.cleanup()

    def _seed_slice(self) -> None:
        strong = self.content.extract_post_record(
            url="https://www.linkedin.com/posts/claude-mackenzie_evals-strong",
            html=PUBLIC_POST_HTML,
            source_query='site:linkedin.com/posts "ai workflow"',
        )
        strong["industries"] = ["ai"]
        strong["owned_by_me"] = True
        strong["last_synced_at"] = "2026-03-27T00:00:00Z"
        strong["title"] = "AI workflow bottlenecks are where automation wins."
        strong["hook"] = strong["title"]
        strong["text"] = (
            "AI workflow bottlenecks are where automation wins.\n\n"
            "We removed the approval handoff, routed the task automatically, and measured cycle-time savings."
        )
        strong["reaction_count"] = 260
        strong["comment_count"] = 34
        strong["word_count"] = len(strong["text"].split())

        second = dict(strong)
        second["url"] = "https://www.linkedin.com/posts/operator_evals-harvested"
        second["owned_by_me"] = False
        second["title"] = "The only AI workflow playbook that mattered was the one we measured."
        second["hook"] = second["title"]
        second["text"] = (
            "The only AI workflow playbook that mattered was the one we measured.\n\n"
            "We tracked approval latency, replaced the routing step, and documented the cycle-time drop."
        )
        second["reaction_count"] = 410
        second["comment_count"] = 61
        second["word_count"] = len(second["text"].split())

        self.content.upsert_post(strong)
        self.content.upsert_post(second)
        self.content.train_outcome_model(name="default", min_samples=2, scope="all", industry="ai", topics=["workflow"])
        self.content.queue_drafts(
            prompt="AI workflow orchestration is still too brittle for most operators.",
            industry="ai",
            topics=["workflow"],
            candidate_goals=["engagement", "instructional", "authority", "contrarian"],
            candidate_count=4,
            model="local-hash-v1",
        )

    def test_evaluate_dataset_handles_sft_and_preference_splits(self) -> None:
        self._seed_slice()
        sft_dir = Path(self.tempdir.name) / "qwen-sft"
        pref_dir = Path(self.tempdir.name) / "qwen-pref"
        self.qwen_training.build_sft_dataset(output_dir=sft_dir, industry="ai", topics=["workflow"])
        self.qwen_training.build_preference_dataset(output_dir=pref_dir, industry="ai", topics=["workflow"])

        sft_report = self.evals.evaluate_dataset(dataset_dir=sft_dir)
        pref_report = self.evals.evaluate_dataset(dataset_dir=pref_dir)

        self.assertEqual(sft_report["phase"], "sft")
        self.assertGreaterEqual(sft_report["row_count"], 2)
        self.assertIn("content_candidate", sft_report["source_counts"])
        self.assertEqual(pref_report["phase"], "preference")
        self.assertGreaterEqual(pref_report["row_count"], 1)
        self.assertTrue(Path(sft_report["artifact_path"]).exists())
        self.assertTrue(Path(pref_report["artifact_path"]).exists())

    def test_evaluate_qwen_generation_reports_diversity_and_score_spread(self) -> None:
        self._seed_slice()

        report = self.evals.evaluate_qwen_generation(
            prompt="AI workflow orchestration is still too brittle for most operators.",
            industry="ai",
            topics=["workflow"],
            candidate_count=4,
            model="local-hash-v1",
            generator="heuristic",
        )

        self.assertEqual(report["candidate_count"], 4)
        self.assertGreater(report["predicted_score"]["best"], report["predicted_score"]["median"])
        self.assertGreaterEqual(report["diversity"]["opening_line_uniqueness"], 0.5)
        self.assertTrue(Path(report["artifact_path"]).exists())

    def test_evaluate_policy_persists_offline_metrics(self) -> None:
        self._seed_slice()
        decision = self.policy.choose_action_linucb(
            policy_name="content-default",
            context_type="content_publish",
            context_key="eval-1",
            context_features=[1.0, 1.0, 10.0],
            actions=[
                {"action_id": "a", "features": [1.0, 0.0, 10.0], "score": 4.0},
                {"action_id": "b", "features": [0.5, 1.0, 9.0], "score": 3.0},
            ],
            log_decision=True,
        )
        assert decision.get("decision_id")
        self.policy.record_reward(decision["decision_id"], reward_type="engagement", reward_value=2.5, payload={"source": "test"})
        self.policy.train_policy(policy_name="content-default", context_type="content_publish", min_samples=1)

        report = self.evals.evaluate_policy(policy_name="content-default", context_type="content_publish")

        self.assertEqual(report["policy_name"], "content-default")
        self.assertGreaterEqual(report["decision_count"], 1)
        self.assertIn("ips", report["offline_eval"])
        self.assertTrue(Path(report["artifact_path"]).exists())

    def test_evaluate_runtime_parses_and_scores_contract_response(self) -> None:
        request = self.runtime_contract.build_runtime_request(
            task_type="content_publish",
            objective="Choose the strongest AI workflow post candidate to publish.",
            context={"industry": "ai", "topics": ["workflow"]},
            actions=[
                {
                    "action_id": "cand_001",
                    "action_type": "publish_post",
                    "payload": {"candidate_id": "cand_001", "text": "Post one"},
                    "score_snapshot": {"predicted_outcome_score": 5.1},
                },
                {
                    "action_id": "cand_002",
                    "action_type": "queue_only",
                    "payload": {"candidate_id": "cand_002", "text": "Post two"},
                    "score_snapshot": {"predicted_outcome_score": 4.0},
                },
            ],
        )
        response = {
            "request_id": request["request_id"],
            "request_fingerprint": request["request_fingerprint"],
            "chosen_action_id": "cand_001",
            "action_type": "publish_post",
            "execute": True,
            "rationale": "Candidate one has the strongest projected score and clearer proof.",
            "payload": {"candidate_id": "cand_001", "text": "Post one"},
        }

        report = self.evals.evaluate_runtime(request=request, response=response)

        self.assertTrue(report["valid"])
        self.assertEqual(report["chosen_action_id"], "cand_001")
        self.assertEqual(report["reward_spec_version"], "viral-v1")
        self.assertTrue(Path(report["artifact_path"]).exists())
