"""Replay environment for validating autonomy traces against logged decisions."""

from __future__ import annotations

from typing import Any

from linkedin_cli import policy, runtime_contract, traces
from linkedin_cli.session import ExitCode, fail


def _step_by_name(trace_payload: dict[str, Any], name: str) -> dict[str, Any] | None:
    return next((step for step in trace_payload.get("steps") or [] if step.get("step_name") == name), None)


def replay_trace(trace_id: str, *, policy_name: str | None = None) -> dict[str, Any]:
    trace_payload = traces.get_trace(trace_id)
    runtime_request_step = _step_by_name(trace_payload, "runtime_request")
    runtime_response_step = _step_by_name(trace_payload, "runtime_response")
    execution_step = _step_by_name(trace_payload, "execute_action")
    if runtime_request_step is None or runtime_response_step is None:
        fail("Trace does not contain runtime request/response steps for replay", code=ExitCode.VALIDATION)

    request = runtime_request_step.get("output") or {}
    response = runtime_response_step.get("output") or {}
    parsed_response = runtime_contract.parse_runtime_response(response, request=request)
    candidate_policy_name = policy_name or ((trace_payload.get("metadata") or {}).get("policy_name"))
    replay_decision = None
    chosen_matches = None
    if candidate_policy_name:
        replay_decision = policy.choose_action_linucb(
            policy_name=candidate_policy_name,
            context_type=str((trace_payload.get("metadata") or {}).get("context_type") or request.get("task_type") or "content_publish"),
            context_key=str(trace_payload.get("context_key") or request.get("request_id") or trace_id),
            context_features=list(((trace_payload.get("metadata") or {}).get("context_features") or [1.0])),
            actions=[
                {
                    "action_id": item.get("action_id"),
                    "label": item.get("label"),
                    "features": list((item.get("metadata") or {}).get("policy_features") or []),
                    "score": float(((item.get("score_snapshot") or {}).get("predicted_outcome_score") or 0.0)),
                    "metadata": item.get("metadata") or {},
                }
                for item in (request.get("actions") or [])
                if (item.get("metadata") or {}).get("policy_features")
            ],
            alpha=float(((trace_payload.get("metadata") or {}).get("policy_alpha") or 0.2)),
            log_decision=False,
        ) if any((item.get("metadata") or {}).get("policy_features") for item in (request.get("actions") or [])) else None
        if replay_decision:
            chosen_matches = replay_decision["chosen_action_id"] == parsed_response["chosen_action_id"]

    reward_events = trace_payload.get("reward_events") or []
    total_reward = round(sum(float(item.get("reward_value") or 0.0) for item in reward_events), 6)
    return {
        "trace_id": trace_id,
        "trace_type": trace_payload.get("trace_type"),
        "request": request,
        "response": parsed_response,
        "replay_decision": replay_decision,
        "chosen_matches": chosen_matches,
        "execution": execution_step.get("output") if execution_step else None,
        "reward_event_count": len(reward_events),
        "total_reward": total_reward,
        "step_count": len(trace_payload.get("steps") or []),
    }

