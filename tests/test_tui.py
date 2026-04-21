from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from linkedin_cli.write import store
from test_content_harvest import PUBLIC_POST_HTML


class TUISnapshotTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tempdir.name) / "state.sqlite"
        self.artifacts_dir = Path(self.tempdir.name) / "artifacts"
        self.db_patcher = patch.object(store, "DB_PATH", self.db_path)
        self.artifacts_patcher = patch.object(store, "ARTIFACTS_DIR", self.artifacts_dir)
        self.db_patcher.start()
        self.artifacts_patcher.start()
        store.init_db()

        from linkedin_cli import content, policy, traces, tui

        self.content = content
        self.policy = policy
        self.traces = traces
        self.tui = tui
        self.content.init_content_db()
        self.policy.init_policy_db()
        self.traces.init_trace_db()

    def tearDown(self) -> None:
        self.db_patcher.stop()
        self.artifacts_patcher.stop()
        self.tempdir.cleanup()

    def _seed_state(self) -> None:
        post = self.content.extract_post_record(
            url="https://www.linkedin.com/posts/claude-mackenzie_tui-strong",
            html=PUBLIC_POST_HTML,
            source_query='site:linkedin.com/posts "ai workflow"',
        )
        post["industries"] = ["ai"]
        post["owned_by_me"] = True
        post["title"] = "AI workflow bottlenecks are where automation wins."
        post["hook"] = post["title"]
        post["text"] = (
            "AI workflow bottlenecks are where automation wins.\n\n"
            "We removed the approval handoff and measured the cycle-time drop."
        )
        post["reaction_count"] = 260
        post["comment_count"] = 34
        post["word_count"] = len(post["text"].split())
        self.content.upsert_post(post)
        self.content.train_outcome_model(name="default", min_samples=1, scope="owned", industry="ai", topics=["workflow"])
        self.content.queue_drafts(
            prompt="AI workflow orchestration is still too brittle for operators.",
            industry="ai",
            topics=["workflow"],
            candidate_goals=["engagement", "authority"],
            candidate_count=2,
            model="local-hash-v1",
        )
        trace = self.traces.start_trace(
            trace_type="content_autonomy",
            request_kind="content_publish",
            context_key="ctx_1",
            root_entity_kind="content_prompt",
            root_entity_key="ctx_1",
            metadata={"industry": "ai", "topics": ["workflow"]},
        )
        self.traces.append_trace_step(
            trace["trace_id"],
            step_kind="generation",
            step_name="generate_candidates",
            output_payload={"candidate_count": 2},
        )
        self.traces.finalize_trace(
            trace["trace_id"],
            status=self.traces.TRACE_STATUS_COMPLETED,
            summary={"candidate_count": 2, "chosen_candidate_id": "cand_demo"},
        )
        decision = self.policy.choose_action_linucb(
            policy_name="content-default",
            context_type="content_publish",
            context_key="ctx_1",
            context_features=[1.0, 1.0, 8.0],
            actions=[
                {"action_id": "cand_demo", "features": [1.0, 1.0, 8.0], "score": 4.2},
                {"action_id": "noop", "features": [0.0, 0.0, 0.0], "score": 0.0},
            ],
            log_decision=True,
            metadata={"trace_id": trace["trace_id"], "decision_source": "local-policy"},
        )
        self.policy.record_reward(
            decision["decision_id"],
            reward_type="engagement",
            reward_value=2.5,
            payload={"source": "test"},
            trace_id=trace["trace_id"],
            reward_source="test",
        )

    def test_build_content_dashboard_snapshot_returns_recent_sections(self) -> None:
        self._seed_state()

        snapshot = self.tui.build_content_dashboard_snapshot(limit=5, trace_type="content_autonomy")

        self.assertEqual(snapshot["overview"]["post_count"], 1)
        self.assertEqual(snapshot["overview"]["queued_candidate_count"], 2)
        self.assertEqual(snapshot["overview"]["trace_count"], 1)
        self.assertGreaterEqual(snapshot["overview"]["reward_count"], 1)
        self.assertEqual(len(snapshot["traces"]), 1)
        self.assertEqual(snapshot["traces"][0]["trace_type"], "content_autonomy")
        self.assertEqual(len(snapshot["candidates"]), 2)
        self.assertEqual(snapshot["policy_decisions"][0]["decision_source"], "local-policy")
        self.assertEqual(snapshot["reward_events"][0]["reward_type"], "engagement")

    def test_section_lines_render_all_dashboard_views(self) -> None:
        snapshot = {
            "generated_at": "2026-04-21T16:00:00+00:00",
            "overview": {
                "post_count": 1,
                "curated_count": 2,
                "queued_candidate_count": 3,
                "trace_count": 4,
                "decision_count": 5,
                "reward_count": 6,
            },
            "curation_stats": {"by_industry": {"ai": 2}},
            "traces": [
                {
                    "updated_at": "2026-04-21T16:00:00+00:00",
                    "trace_id": "tr_1",
                    "status": "completed",
                    "trace_type": "content_autonomy",
                    "request_kind": "content_publish",
                    "summary": {"chosen_candidate_id": "cand_1"},
                }
            ],
            "candidates": [
                {
                    "rank": 1,
                    "candidate_id": "cand_1",
                    "status": "queued",
                    "goal": "authority",
                    "score": {"predicted_outcome_score": 0.75},
                    "text": "A long candidate line that should wrap cleanly.",
                }
            ],
            "policy_decisions": [
                {
                    "created_at": "2026-04-21T16:00:00+00:00",
                    "decision_id": "dec_1",
                    "decision_source": "local-policy",
                    "chosen_action_id": "cand_1",
                    "chosen_score": 0.75,
                    "propensity": 1.0,
                    "context_type": "content_publish",
                    "context_key": "ctx_1",
                }
            ],
            "reward_events": [
                {
                    "event_time": "2026-04-21T16:00:00+00:00",
                    "reward_type": "engagement",
                    "reward_value": 2.5,
                    "reward_source": "test",
                    "decision_id": "dec_1",
                    "trace_id": "tr_1",
                }
            ],
        }

        rendered = {
            view: "\n".join(self.tui._section_lines(snapshot, view, 48))
            for view in ["overview", "traces", "queue", "decisions", "rewards"]
        }

        self.assertIn("posts: 1", rendered["overview"])
        self.assertIn("tr_1", rendered["traces"])
        self.assertIn("cand_1", rendered["queue"])
        self.assertIn("dec_1", rendered["decisions"])
        self.assertIn("engagement", rendered["rewards"])

    def test_time_label_and_trim_are_stable_for_bad_inputs(self) -> None:
        self.assertEqual(self.tui._time_label(None), "-")
        self.assertEqual(self.tui._time_label("not-a-date-value"), "not-a-date-value")
        self.assertEqual(self.tui._trim("abcdef", 4), "abc…")
