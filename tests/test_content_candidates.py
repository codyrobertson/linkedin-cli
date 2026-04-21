from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from linkedin_cli.write import store
from test_content_harvest import PUBLIC_POST_HTML


class ContentCandidateQueueTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tempdir.name) / "state.sqlite"
        self.db_patcher = patch.object(store, "DB_PATH", self.db_path)
        self.db_patcher.start()
        store.init_db()

        from linkedin_cli import content

        self.content = content
        self.content.init_content_db()

    def tearDown(self) -> None:
        self.db_patcher.stop()
        self.tempdir.cleanup()

    def _seed_owned_ai_workflow_posts(self) -> None:
        strong = self.content.extract_post_record(
            url="https://www.linkedin.com/posts/claude-mackenzie_queue-activity-strong",
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
        second["url"] = "https://www.linkedin.com/posts/claude-mackenzie_queue-activity-second"
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

    def test_queue_drafts_persists_provenance_and_choice(self) -> None:
        self._seed_owned_ai_workflow_posts()

        queued = self.content.queue_drafts(
            prompt="AI workflow orchestration is still too brittle for most operators.",
            industry="ai",
            topics=["workflow"],
            candidate_goals=["engagement", "instructional", "authority", "contrarian"],
            candidate_count=4,
            model="local-hash-v1",
        )

        queue = self.content.list_candidate_queue(limit=10)
        chosen = self.content.get_candidate(queued["best_candidate"]["candidate_id"])

        self.assertEqual(queued["candidate_count"], 4)
        self.assertEqual(len(queue), 4)
        self.assertEqual(sum(1 for item in queue if item["chosen"]), 1)
        self.assertEqual(chosen["candidate_id"], queued["best_candidate"]["candidate_id"])
        self.assertEqual(chosen["status"], "queued")
        self.assertEqual(chosen["generator_source"], "playbook-heuristic-v1")
        self.assertEqual(chosen["topics"], ["workflow"])
        self.assertTrue(chosen["references"]["top_example_urls"])
        self.assertGreater(chosen["score"]["predicted_outcome_score"], 0)

    def test_mark_candidate_published_updates_status(self) -> None:
        self._seed_owned_ai_workflow_posts()

        queued = self.content.queue_drafts(
            prompt="AI workflow orchestration is still too brittle for most operators.",
            industry="ai",
            topics=["workflow"],
            candidate_goals=["engagement", "instructional", "authority", "contrarian"],
            candidate_count=4,
            model="local-hash-v1",
        )

        published = self.content.mark_candidate_published(
            queued["best_candidate"]["candidate_id"],
            post_url="https://www.linkedin.com/posts/claude-mackenzie_autonomous-content-activity-1",
        )

        refreshed = self.content.get_candidate(queued["best_candidate"]["candidate_id"])
        self.assertEqual(published["status"], "published")
        self.assertEqual(published["post_url"], "https://www.linkedin.com/posts/claude-mackenzie_autonomous-content-activity-1")
        self.assertEqual(refreshed["status"], "published")
        self.assertEqual(refreshed["post_url"], "https://www.linkedin.com/posts/claude-mackenzie_autonomous-content-activity-1")

    def test_run_autonomy_persists_trace_and_queues_candidates(self) -> None:
        self._seed_owned_ai_workflow_posts()

        result = self.content.run_autonomy(
            prompt="AI workflow orchestration is still too brittle for most operators.",
            industry="ai",
            topics=["workflow"],
            model="local-hash-v1",
            candidate_goals=["engagement", "authority"],
            candidate_count=4,
            mode="review",
        )

        queue = self.content.list_candidate_queue(limit=10)
        self.assertEqual(result["status"], "completed")
        self.assertEqual(result["execution"]["status"], "queued")
        self.assertEqual(len(result["queued_candidates"]), 4)
        self.assertEqual(len(queue), 4)
        self.assertTrue(result["trace_id"].startswith("tr_"))

    def test_run_autonomy_supports_cloud_decision_provider(self) -> None:
        self._seed_owned_ai_workflow_posts()
        from linkedin_cli import llm_providers

        llm_providers.save_provider_config(
            provider_name="cerebras",
            api_key="secret-token",
            model="gpt-oss-120b",
        )

        with patch("linkedin_cli.llm_providers.requests.post") as post_mock:
            def _mock_post(*args, **kwargs):
                payload = kwargs.get("json") or {}
                messages = payload.get("messages") or []
                user_content = ""
                for message in messages:
                    if message.get("role") == "user":
                        user_content = str(message.get("content") or "")
                        break
                if "\"response_schema\"" in user_content:
                    request_payload = json.loads(user_content)
                    request_actions = request_payload.get("actions") or []
                    chosen_action = request_actions[0] if request_actions else {"action_id": "noop", "action_type": "noop", "payload": {}}
                    response_payload = {
                        "request_id": request_payload["request_id"],
                        "request_fingerprint": request_payload["request_fingerprint"],
                        "chosen_action_id": chosen_action["action_id"],
                        "action_type": chosen_action["action_type"],
                        "execute": False,
                        "rationale": "Candidate one is the cleanest fit for the slice.",
                        "payload": dict(chosen_action.get("payload") or {}),
                    }
                    body = response_payload
                else:
                    body = {
                        "candidates": [
                            {"goal": "engagement", "text": "AI workflow orchestration is still too brittle for operators. We fixed one approval break and measured the cycle-time drop."},
                            {"goal": "authority", "text": "Most AI workflow failures are orchestration failures, not model failures. We replaced one brittle handoff and proved the before-and-after."},
                        ]
                    }

                class _Resp:
                    status_code = 200

                    @staticmethod
                    def json() -> dict[str, object]:
                        return {"choices": [{"message": {"content": json.dumps(body)}}]}

                return _Resp()

            post_mock.side_effect = _mock_post
            result = self.content.run_autonomy(
                prompt="AI workflow orchestration is still too brittle for most operators.",
                industry="ai",
                topics=["workflow"],
                model="local-hash-v1",
                candidate_goals=["engagement", "authority"],
                candidate_count=2,
                generator="cerebras",
                decision_provider="cerebras",
                mode="review",
            )

        self.assertEqual(result["status"], "completed")
        self.assertTrue(str(result["response"]["chosen_action_id"]).startswith("cand_"))
        self.assertEqual(result["response"]["chosen_action_id"], result["decision"]["chosen_action_id"])
        self.assertEqual(result["decision"]["metadata"]["decision_source"], "provider:cerebras")

    def test_limited_autonomy_publishes_selected_candidate(self) -> None:
        self._seed_owned_ai_workflow_posts()

        class _MeResponse:
            status_code = 200
            text = "{}"

            @staticmethod
            def json() -> dict[str, object]:
                return {"data": {"plainId": "1708250765"}}

        def choose_first(*_args, **kwargs):
            actions = kwargs["actions"]
            return {
                "decision_id": "dec_live",
                "chosen_action_id": actions[0]["action_id"],
                "chosen_score": actions[0]["score"],
                "propensity": 1.0,
                "metadata": {},
            }

        with patch("linkedin_cli.policy.choose_action_linucb", side_effect=choose_first):
            with patch("linkedin_cli.content.load_session", return_value=(object(), None)):
                with patch("linkedin_cli.content.voyager_get", return_value=_MeResponse()):
                    with patch(
                        "linkedin_cli.write.executor.execute_action",
                        return_value={
                            "status": "succeeded",
                            "action": {"remote_ref": "urn:li:activity:123"},
                            "result": {"remote_ref": "urn:li:activity:123", "http_status": 201},
                        },
                    ) as execute_mock:
                        result = self.content.run_autonomy(
                            prompt="AI workflow orchestration is still too brittle for most operators.",
                            industry="ai",
                            topics=["workflow"],
                            model="local-hash-v1",
                            candidate_goals=["engagement", "authority"],
                            candidate_count=2,
                            mode="limited",
                        )

        self.assertEqual(result["execution"]["status"], "executed")
        self.assertEqual(result["execution"]["summary"]["publish_action"]["status"], "succeeded")
        self.assertEqual(result["execution"]["summary"]["published_candidate"]["status"], "published")
        self.assertEqual(
            result["execution"]["summary"]["published_candidate"]["post_url"],
            "https://www.linkedin.com/feed/update/urn:li:activity:123/",
        )
        execute_mock.assert_called_once()
        self.assertFalse(execute_mock.call_args.kwargs["dry_run"])
        self.assertEqual(execute_mock.call_args.kwargs["plan"]["action_type"], "post.publish")
