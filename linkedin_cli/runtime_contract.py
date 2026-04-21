"""Structured runtime contract for autonomous content and engagement control."""

from __future__ import annotations

import hashlib
import json
import uuid
from typing import Any

from linkedin_cli.session import ExitCode, fail
from linkedin_cli.write import store


ALLOWED_ACTION_TYPES = {
    "generate_candidates",
    "choose_candidate",
    "publish_post",
    "queue_only",
    "reply_comment",
    "send_dm",
    "noop",
}

ACTION_TYPE_SCHEMAS: dict[str, dict[str, Any]] = {
    "generate_candidates": {"required_payload_keys": {"prompt"}},
    "choose_candidate": {"required_payload_keys": {"candidate_id"}},
    "publish_post": {"required_payload_keys": {"candidate_id", "text"}},
    "queue_only": {"required_payload_keys": {"candidate_id", "text"}},
    "reply_comment": {"required_payload_keys": {"comment_id", "text"}},
    "send_dm": {"required_payload_keys": {"profile", "text"}},
    "noop": {"required_payload_keys": set()},
}

CONTENT_REWARD_SPEC = {
    "version": "viral-v1",
    "objective": "maximize niche-relevant LinkedIn distribution and downstream business signal",
    "components": [
        {"name": "reaction_log1p", "weight": 1.0, "description": "reactions are broad reach but relatively shallow"},
        {"name": "comment_log1p", "weight": 1.8, "description": "comments indicate stronger discussion depth"},
        {"name": "repost_log1p", "weight": 2.2, "description": "reposts indicate the post is worth redistributing"},
        {"name": "profile_view_log1p", "weight": 0.8, "description": "profile views proxy for commercial curiosity"},
        {"name": "dm_reply", "weight": 3.0, "description": "direct replies are strong downstream intent"},
        {"name": "meeting_booked", "weight": 6.0, "description": "meetings are the highest-value downstream outcome"},
        {"name": "negative_feedback", "weight": -4.0, "description": "explicit negative feedback must suppress the policy"},
    ],
}


def _now_iso() -> str:
    return store._now_iso()


def _request_fingerprint(*, task_type: str, objective: str, context: dict[str, Any], actions: list[dict[str, Any]], reward_spec: dict[str, Any]) -> str:
    body = json.dumps(
        {
            "task_type": task_type,
            "objective": objective,
            "context": context or {},
            "actions": actions or [],
            "reward_spec": reward_spec or {},
        },
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


def validate_action_payload(action_type: str, payload: dict[str, Any] | None) -> dict[str, Any]:
    normalized_action_type = str(action_type or "").strip()
    if normalized_action_type not in ALLOWED_ACTION_TYPES:
        fail(f"Unsupported runtime action_type: {action_type}", code=ExitCode.VALIDATION)
    normalized_payload = payload if isinstance(payload, dict) else {}
    schema = ACTION_TYPE_SCHEMAS.get(normalized_action_type) or {}
    missing = sorted(key for key in schema.get("required_payload_keys", set()) if not normalized_payload.get(key))
    if missing:
        fail(
            f"Runtime payload for `{normalized_action_type}` is missing required keys: {', '.join(missing)}",
            code=ExitCode.VALIDATION,
        )
    return normalized_payload


def validate_action_envelope(action: dict[str, Any]) -> dict[str, Any]:
    action_id = str(action.get("action_id") or "").strip()
    if not action_id:
        fail("Each runtime action requires action_id", code=ExitCode.VALIDATION)
    action_type = str(action.get("action_type") or "").strip()
    normalized_payload = validate_action_payload(action_type, action.get("payload"))
    return {
        "action_id": action_id,
        "action_type": action_type,
        "label": action.get("label") or action_id,
        "payload": normalized_payload,
        "score_snapshot": action.get("score_snapshot") or {},
        "metadata": action.get("metadata") or {},
    }


def compute_content_reward(telemetry: dict[str, Any]) -> dict[str, Any]:
    def value(key: str) -> float:
        raw = telemetry.get(key)
        return float(raw or 0.0)

    reaction_term = CONTENT_REWARD_SPEC["components"][0]["weight"] * value("reaction_log1p")
    comment_term = CONTENT_REWARD_SPEC["components"][1]["weight"] * value("comment_log1p")
    repost_term = CONTENT_REWARD_SPEC["components"][2]["weight"] * value("repost_log1p")
    profile_view_term = CONTENT_REWARD_SPEC["components"][3]["weight"] * value("profile_view_log1p")
    dm_reply_term = CONTENT_REWARD_SPEC["components"][4]["weight"] * value("dm_reply")
    meeting_term = CONTENT_REWARD_SPEC["components"][5]["weight"] * value("meeting_booked")
    negative_term = CONTENT_REWARD_SPEC["components"][6]["weight"] * value("negative_feedback")
    total = reaction_term + comment_term + repost_term + profile_view_term + dm_reply_term + meeting_term + negative_term
    return {
        "version": CONTENT_REWARD_SPEC["version"],
        "total_reward": round(total, 6),
        "components": {
            "reaction_reward": round(reaction_term, 6),
            "comment_reward": round(comment_term, 6),
            "repost_reward": round(repost_term, 6),
            "profile_view_reward": round(profile_view_term, 6),
            "dm_reply_reward": round(dm_reply_term, 6),
            "meeting_reward": round(meeting_term, 6),
            "negative_feedback_penalty": round(negative_term, 6),
        },
    }


def build_runtime_request(
    *,
    task_type: str,
    objective: str,
    context: dict[str, Any],
    actions: list[dict[str, Any]],
    request_id: str | None = None,
) -> dict[str, Any]:
    if not str(task_type or "").strip():
        fail("Runtime task_type is required", code=ExitCode.VALIDATION)
    if not str(objective or "").strip():
        fail("Runtime objective is required", code=ExitCode.VALIDATION)
    normalized_actions: list[dict[str, Any]] = []
    action_ids: set[str] = set()
    for action in actions:
        normalized = validate_action_envelope(action)
        action_id = normalized["action_id"]
        action_type = normalized["action_type"]
        if not action_id:
            fail("Each runtime action requires action_id", code=ExitCode.VALIDATION)
        if action_id in action_ids:
            fail("Runtime action ids must be unique", code=ExitCode.VALIDATION)
        action_ids.add(action_id)
        normalized_actions.append(normalized)
    reward_spec = CONTENT_REWARD_SPEC
    fingerprint = _request_fingerprint(
        task_type=task_type,
        objective=objective,
        context=context or {},
        actions=normalized_actions,
        reward_spec=reward_spec,
    )
    return {
        "request_id": request_id or f"rt_{uuid.uuid4().hex[:12]}",
        "created_at": _now_iso(),
        "task_type": task_type,
        "objective": objective,
        "context": context or {},
        "actions": normalized_actions,
        "reward_spec": reward_spec,
        "request_fingerprint": fingerprint,
    }


def render_runtime_messages(request: dict[str, Any]) -> list[dict[str, str]]:
    system = (
        "You are the decision layer for a local LinkedIn autonomy harness. "
        "Return JSON only. Never invent actions. Choose exactly one action_id from the provided list."
    )
    user = json.dumps(
        {
            "request_id": request["request_id"],
            "request_fingerprint": request["request_fingerprint"],
            "task_type": request["task_type"],
            "objective": request["objective"],
            "context": request.get("context") or {},
            "actions": request.get("actions") or [],
            "response_schema": {
                "request_id": "string",
                "request_fingerprint": "string",
                "chosen_action_id": "string",
                "action_type": "string",
                "execute": "boolean",
                "rationale": "string",
                "payload": "object",
            },
        },
        ensure_ascii=False,
        sort_keys=True,
        indent=2,
    )
    return [{"role": "system", "content": system}, {"role": "user", "content": user}]


def parse_runtime_response(raw: str | dict[str, Any], *, request: dict[str, Any]) -> dict[str, Any]:
    payload = json.loads(raw) if isinstance(raw, str) else dict(raw)
    if str(payload.get("request_id") or "") != str(request.get("request_id") or ""):
        fail("Runtime response request_id does not match the request", code=ExitCode.VALIDATION)
    if str(payload.get("request_fingerprint") or "") != str(request.get("request_fingerprint") or ""):
        fail("Runtime response request_fingerprint does not match the request", code=ExitCode.VALIDATION)
    action_map = {str(item["action_id"]): item for item in (request.get("actions") or [])}
    chosen_action_id = str(payload.get("chosen_action_id") or "")
    if chosen_action_id not in action_map:
        fail("Runtime response chose an unknown action_id", code=ExitCode.VALIDATION)
    expected_action = action_map[chosen_action_id]
    action_type = str(payload.get("action_type") or "")
    if action_type != str(expected_action.get("action_type") or ""):
        fail("Runtime response action_type does not match the chosen action", code=ExitCode.VALIDATION)
    if not isinstance(payload.get("execute"), bool):
        fail("Runtime response must include execute=true|false", code=ExitCode.VALIDATION)
    rationale = str(payload.get("rationale") or "").strip()
    if not rationale:
        fail("Runtime response rationale is required", code=ExitCode.VALIDATION)
    result_payload = payload.get("payload") or {}
    if not isinstance(result_payload, dict):
        fail("Runtime response payload must be an object", code=ExitCode.VALIDATION)
    validate_action_payload(action_type, result_payload)
    expected_payload = dict(expected_action.get("payload") or {})
    if result_payload != expected_payload:
        fail("Runtime response payload must exactly match the chosen action payload", code=ExitCode.VALIDATION)
    return {
        "request_id": request["request_id"],
        "request_fingerprint": request["request_fingerprint"],
        "chosen_action_id": chosen_action_id,
        "action_type": action_type,
        "execute": bool(payload["execute"]),
        "rationale": rationale,
        "payload": result_payload,
        "action_snapshot": expected_action,
    }


def build_execution_result(
    *,
    request: dict[str, Any],
    response: dict[str, Any],
    status: str,
    artifact_refs: list[str] | None = None,
    telemetry: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "request_id": request["request_id"],
        "request_fingerprint": request.get("request_fingerprint"),
        "task_type": request["task_type"],
        "chosen_action_id": response["chosen_action_id"],
        "action_type": response["action_type"],
        "status": status,
        "executed": bool(response.get("execute")),
        "artifact_refs": list(artifact_refs or []),
        "telemetry": telemetry or {},
        "completed_at": _now_iso(),
    }
