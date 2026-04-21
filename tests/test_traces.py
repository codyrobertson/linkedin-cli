from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from linkedin_cli.write import store


class TraceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tempdir.name) / "state.sqlite"
        self.artifacts_dir = Path(self.tempdir.name) / "artifacts"
        self.db_patcher = patch.object(store, "DB_PATH", self.db_path)
        self.artifacts_patcher = patch.object(store, "ARTIFACTS_DIR", self.artifacts_dir)
        self.db_patcher.start()
        self.artifacts_patcher.start()
        store.init_db()

        from linkedin_cli import policy, traces

        self.policy = policy
        self.traces = traces
        self.policy.init_policy_db()
        self.traces.init_trace_db()
        from linkedin_cli import discovery

        self.discovery = discovery
        self.discovery.init_discovery_db()

    def tearDown(self) -> None:
        self.db_patcher.stop()
        self.artifacts_patcher.stop()
        self.tempdir.cleanup()

    def test_trace_lifecycle_and_export_persist_steps_and_artifacts(self) -> None:
        trace = self.traces.start_trace(
            trace_type="content_autonomy",
            request_kind="content_publish",
            context_key="ctx-1",
            metadata={"policy_name": "content-default"},
        )
        step = self.traces.append_trace_step(
            trace["trace_id"],
            step_kind="generation",
            step_name="generate_candidates",
            output_payload={"candidate_count": 2},
            artifact_kind="candidates",
            artifact_payload={"candidates": [{"id": "cand_a"}, {"id": "cand_b"}]},
        )
        finalized = self.traces.finalize_trace(trace["trace_id"], status=self.traces.TRACE_STATUS_COMPLETED, summary={"ok": True})
        exported = self.traces.export_trace(trace["trace_id"])

        self.assertEqual(step["step_index"], 1)
        self.assertEqual(finalized["status"], self.traces.TRACE_STATUS_COMPLETED)
        self.assertTrue(Path(exported["output_path"]).exists())
        self.assertEqual(exported["trace"]["steps"][0]["step_name"], "generate_candidates")

    def test_reward_window_attributes_snapshot_delta_and_logs_policy_reward(self) -> None:
        decision = self.policy.choose_action_linucb(
            policy_name="content-default",
            context_type="content_publish",
            context_key="ctx-2",
            context_features=[1.0, 0.0],
            actions=[
                {"action_id": "cand_a", "label": "A", "features": [1.0, 0.0], "score": 1.0},
                {"action_id": "cand_b", "label": "B", "features": [0.0, 1.0], "score": 0.5},
            ],
            log_decision=True,
            force_action_id="cand_a",
        )
        trace = self.traces.start_trace(trace_type="content_autonomy", request_kind="content_publish", context_key="ctx-2")
        window = self.traces.open_reward_window(
            trace_id=trace["trace_id"],
            decision_id=decision["decision_id"],
            action_type="publish_post",
            entity_kind="post",
            entity_key="https://www.linkedin.com/posts/example",
            baseline={"reaction_count": 10, "comment_count": 2, "repost_count": 0},
        )

        attributed = self.traces.attribute_telemetry_event(
            entity_kind="post",
            entity_key="https://www.linkedin.com/posts/example",
            event_type="reaction_snapshot",
            event_time=store._now_iso(),
            payload={"reaction_count": 30, "comment_count": 5, "repost_count": 1},
            source="test",
            source_event_id="evt-1",
        )
        report = self.policy.policy_report(policy_name="content-default", context_type="content_publish")

        self.assertEqual(window["entity_kind"], "post")
        self.assertEqual(attributed["attributed_count"], 1)
        self.assertEqual(report["reward_count"], 1)
        self.assertGreater(report["average_reward"], 0.0)

    def test_reward_window_attributes_dm_reply_signal_and_logs_policy_reward(self) -> None:
        self.discovery.upsert_prospect("john-doe", "John Doe")
        decision = self.policy.choose_action_linucb(
            policy_name="lead-default",
            context_type="prospect_outreach",
            context_key="john-doe",
            context_features=[1.0, 1.0],
            actions=[
                {"action_id": "send_dm", "label": "DM", "features": [1.0, 1.0], "score": 1.0},
                {"action_id": "noop", "label": "Wait", "features": [0.0, 1.0], "score": 0.2},
            ],
            log_decision=True,
            force_action_id="send_dm",
        )
        trace = self.traces.start_trace(trace_type="lead_autonomy", request_kind="prospect_outreach", context_key="john-doe")
        self.traces.open_reward_window(
            trace_id=trace["trace_id"],
            decision_id=decision["decision_id"],
            action_type="send_dm",
            entity_kind="prospect",
            entity_key="john-doe",
            metadata={"profile_key": "john-doe"},
        )

        self.discovery.add_signal(
            "john-doe",
            signal_type="replied_dm",
            source="inbox",
            dedupe_key="reply:john-doe:1",
            metadata={"conversation_urn": "conv:1", "message_urn": "msg:1"},
        )
        report = self.policy.policy_report(policy_name="lead-default", context_type="prospect_outreach")
        trace_payload = self.traces.get_trace(trace["trace_id"])

        self.assertEqual(report["reward_count"], 1)
        self.assertGreater(report["average_reward"], 0.0)
        self.assertEqual(len(trace_payload["reward_events"]), 1)
        self.assertEqual(trace_payload["reward_events"][0]["reward_type"], "prospect_signal")
