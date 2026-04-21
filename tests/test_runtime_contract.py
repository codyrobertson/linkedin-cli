from __future__ import annotations

import json

from linkedin_cli import runtime_contract


def test_runtime_contract_handles_content_publish_request_and_response() -> None:
    request = runtime_contract.build_runtime_request(
        task_type="content_publish",
        objective="Choose the strongest AI workflow post candidate to publish.",
        context={
            "industry": "ai",
            "topics": ["workflow"],
            "policy_name": "content-default",
        },
        actions=[
            {
                "action_id": "cand_001",
                "action_type": "publish_post",
                "label": "publish candidate 1",
                "payload": {"candidate_id": "cand_001", "text": "Post one"},
                "score_snapshot": {"predicted_outcome_score": 5.2},
            },
            {
                "action_id": "cand_002",
                "action_type": "queue_only",
                "label": "queue candidate 2",
                "payload": {"candidate_id": "cand_002", "text": "Post two"},
                "score_snapshot": {"predicted_outcome_score": 4.1},
            },
        ],
    )

    messages = runtime_contract.render_runtime_messages(request)
    parsed = runtime_contract.parse_runtime_response(
        json.dumps(
                {
                    "request_id": request["request_id"],
                    "request_fingerprint": request["request_fingerprint"],
                    "chosen_action_id": "cand_001",
                    "action_type": "publish_post",
                    "execute": True,
                    "rationale": "Candidate 1 has the strongest projected outcome and cleaner proof structure.",
                    "payload": {"candidate_id": "cand_001", "text": "Post one"},
                }
            ),
            request=request,
        )
    result = runtime_contract.build_execution_result(
        request=request,
        response=parsed,
        status="queued",
        artifact_refs=["action:act_123"],
        telemetry={"reward_window_hours": 72},
    )

    assert request["task_type"] == "content_publish"
    assert len(messages) == 2
    assert "Return JSON only" in messages[0]["content"]
    assert parsed["chosen_action_id"] == "cand_001"
    assert result["status"] == "queued"
    assert result["artifact_refs"] == ["action:act_123"]
    assert result["request_fingerprint"] == request["request_fingerprint"]


def test_runtime_contract_handles_comment_and_dm_actions() -> None:
    request = runtime_contract.build_runtime_request(
        task_type="engagement_followup",
        objective="Choose whether to comment publicly or DM the prospect.",
        context={"profile": "john-doe", "post_url": "https://www.linkedin.com/posts/example"},
        actions=[
            {
                "action_id": "reply_comment",
                "action_type": "reply_comment",
                "payload": {"comment_id": "cmt_123", "text": "This is the right bottleneck to fix first."},
            },
            {
                "action_id": "send_dm",
                "action_type": "send_dm",
                "payload": {"profile": "john-doe", "text": "Saw your comment on workflow bottlenecks."},
            },
        ],
    )

    parsed = runtime_contract.parse_runtime_response(
        {
            "request_id": request["request_id"],
            "request_fingerprint": request["request_fingerprint"],
            "chosen_action_id": "send_dm",
            "action_type": "send_dm",
            "execute": False,
            "rationale": "Private follow-up is higher signal than a public reply here.",
            "payload": {"profile": "john-doe", "text": "Saw your comment on workflow bottlenecks."},
        },
        request=request,
    )

    assert parsed["action_type"] == "send_dm"
    assert parsed["execute"] is False
    assert parsed["payload"]["profile"] == "john-doe"


def test_runtime_contract_rejects_payload_mutation_for_chosen_action() -> None:
    request = runtime_contract.build_runtime_request(
        task_type="content_publish",
        objective="Choose a candidate.",
        context={},
        actions=[
            {
                "action_id": "cand_001",
                "action_type": "publish_post",
                "payload": {"candidate_id": "cand_001", "text": "Exact candidate text"},
            }
        ],
    )

    try:
        runtime_contract.parse_runtime_response(
            {
                "request_id": request["request_id"],
                "request_fingerprint": request["request_fingerprint"],
                "chosen_action_id": "cand_001",
                "action_type": "publish_post",
                "execute": True,
                "rationale": "I improved the text.",
                "payload": {"candidate_id": "cand_001", "text": "Mutated text"},
            },
            request=request,
        )
    except SystemExit as exc:
        assert exc.code != 0
    else:
        raise AssertionError("Expected validation failure for mutated chosen-action payload")


def test_runtime_contract_rejects_missing_required_payload_keys() -> None:
    try:
        runtime_contract.build_runtime_request(
            task_type="content_publish",
            objective="Choose a candidate.",
            context={},
            actions=[
                {
                    "action_id": "cand_001",
                    "action_type": "publish_post",
                    "payload": {"candidate_id": "cand_001"},
                }
            ],
        )
    except SystemExit as exc:
        assert exc.code != 0
    else:
        raise AssertionError("Expected validation failure for missing publish_post text payload")
