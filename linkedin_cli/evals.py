"""Local evaluation helpers for content, policy, and runtime artifacts."""

from __future__ import annotations

import json
import math
import re
import uuid
from pathlib import Path
from statistics import median
from typing import Any

from linkedin_cli import content, policy, runtime_contract
from linkedin_cli.session import ExitCode, fail
from linkedin_cli.write import store


def evals_dir() -> Path:
    return store.ARTIFACTS_DIR / "evals" / "content"


def _artifact_path(kind: str) -> Path:
    path = evals_dir() / kind
    path.mkdir(parents=True, exist_ok=True)
    return path / f"{store._now_iso().replace(':', '-').replace('+00:00', 'Z')}_{uuid.uuid4().hex[:8]}.json"


def _write_report(kind: str, payload: dict[str, Any]) -> str:
    path = _artifact_path(kind)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return str(path)


def _load_jsonl(path: str | Path) -> list[dict[str, Any]]:
    file_path = Path(path)
    if not file_path.exists():
        fail(f"Dataset file not found: {file_path}", code=ExitCode.NOT_FOUND)
    return [json.loads(line) for line in file_path.read_text(encoding="utf-8").splitlines() if line.strip()]


def _token_set(text: str) -> set[str]:
    return {token for token in re.findall(r"[a-z0-9][a-z0-9+#.-]{2,}", (text or "").lower()) if token}


def _pairwise_jaccard_distance(texts: list[str]) -> float:
    if len(texts) < 2:
        return 0.0
    distances: list[float] = []
    token_sets = [_token_set(text) for text in texts]
    for index, left in enumerate(token_sets):
        for right in token_sets[index + 1 :]:
            union = left | right
            if not union:
                distances.append(0.0)
                continue
            overlap = len(left & right) / len(union)
            distances.append(1.0 - overlap)
    return round(sum(distances) / len(distances), 6) if distances else 0.0


def evaluate_dataset(dataset_dir: str | Path) -> dict[str, Any]:
    dataset_path = Path(dataset_dir)
    rows: list[dict[str, Any]] = []
    split_counts: dict[str, int] = {}
    for split_name in ("train", "val", "test"):
        split_rows = _load_jsonl(dataset_path / f"{split_name}.jsonl")
        split_counts[split_name] = len(split_rows)
        rows.extend(split_rows)
    if not rows:
        fail("Dataset is empty", code=ExitCode.NOT_FOUND)
    sample = rows[0]
    if "messages" in sample:
        phase = "sft"
        source_counts: dict[str, int] = {}
        assistant_lengths: list[int] = []
        for item in rows:
            metadata = item.get("metadata") or {}
            source = str(metadata.get("source") or "unknown")
            source_counts[source] = source_counts.get(source, 0) + 1
            messages = item.get("messages") or []
            assistant = next((msg for msg in reversed(messages) if msg.get("role") == "assistant"), {})
            assistant_lengths.append(len(str(assistant.get("content") or "").split()))
        report = {
            "phase": phase,
            "row_count": len(rows),
            "split_counts": split_counts,
            "source_counts": source_counts,
            "average_assistant_word_count": round(sum(assistant_lengths) / len(assistant_lengths), 4) if assistant_lengths else 0.0,
        }
    elif "chosen" in sample and "rejected" in sample:
        phase = "preference"
        report = {
            "phase": phase,
            "row_count": len(rows),
            "split_counts": split_counts,
            "source_counts": {
                str((item.get("metadata") or {}).get("source") or "unknown"): sum(
                    1 for row in rows if str((row.get("metadata") or {}).get("source") or "unknown") == str((item.get("metadata") or {}).get("source") or "unknown")
                )
                for item in rows
            },
            "average_chosen_word_count": round(sum(len(str(item.get("chosen") or "").split()) for item in rows) / len(rows), 4),
            "average_rejected_word_count": round(sum(len(str(item.get("rejected") or "").split()) for item in rows) / len(rows), 4),
        }
    else:
        phase = "generic"
        report = {"phase": phase, "row_count": len(rows), "split_counts": split_counts}
    report["dataset_dir"] = str(dataset_path)
    report["artifact_path"] = _write_report("dataset", report)
    return report


def evaluate_qwen_generation(
    *,
    prompt: str,
    industry: str | None = None,
    topics: list[str] | None = None,
    candidate_count: int = 8,
    model: str | None = None,
    generator: str = "heuristic",
) -> dict[str, Any]:
    if generator not in {"heuristic", "qwen-local"}:
        fail("Generator must be one of: heuristic, qwen-local", code=ExitCode.VALIDATION)
    created = content.create_drafts(
        prompt=prompt,
        industry=industry,
        topics=list(topics or []),
        model=model,
        candidate_count=candidate_count,
    )
    candidates = list(created.get("candidates") or [])
    if not candidates:
        fail("No candidates produced for evaluation", code=ExitCode.NOT_FOUND)
    scores = [float((candidate.get("score") or {}).get("predicted_outcome_score") or 0.0) for candidate in candidates]
    texts = [str(candidate.get("text") or "") for candidate in candidates]
    openings = [text.splitlines()[0].strip() for text in texts if text.strip()]
    uniqueness = len(set(openings)) / max(1, len(openings))
    report = {
        "generator": generator,
        "prompt": prompt,
        "industry": industry,
        "topics": list(topics or []),
        "candidate_count": len(candidates),
        "predicted_score": {
            "best": round(max(scores), 6),
            "median": round(float(median(scores)), 6),
            "average": round(sum(scores) / len(scores), 6),
            "spread": round(max(scores) - min(scores), 6),
        },
        "diversity": {
            "opening_line_uniqueness": round(uniqueness, 6),
            "pairwise_jaccard_distance": _pairwise_jaccard_distance(texts),
            "goal_coverage": len({str(candidate.get("goal") or "") for candidate in candidates}),
        },
        "top_candidate": candidates[0],
    }
    report["artifact_path"] = _write_report("generation", report)
    return report


def evaluate_policy(*, policy_name: str, context_type: str | None = None) -> dict[str, Any]:
    report = policy.policy_report(policy_name=policy_name, context_type=context_type)
    by_action = report.get("by_action") or {}
    total = sum(int(value) for value in by_action.values())
    entropy = 0.0
    if total > 0:
        for count in by_action.values():
            probability = float(count) / float(total)
            if probability > 0:
                entropy -= probability * math.log(probability, 2)
    payload = {
        "policy_name": policy_name,
        "context_type": context_type,
        "decision_count": int(report.get("decision_count") or 0),
        "reward_count": int(report.get("reward_count") or 0),
        "average_reward": float(report.get("average_reward") or 0.0),
        "offline_eval": report.get("offline_eval") or {},
        "action_entropy": round(entropy, 6),
        "by_action": by_action,
        "policy": report.get("policy"),
    }
    payload["artifact_path"] = _write_report("policy", payload)
    return payload


def evaluate_runtime(
    *,
    request: dict[str, Any] | None = None,
    response: dict[str, Any] | str | None = None,
    request_file: str | Path | None = None,
    response_file: str | Path | None = None,
) -> dict[str, Any]:
    if request is None:
        if not request_file:
            fail("Provide request or request_file for runtime evaluation", code=ExitCode.VALIDATION)
        request = json.loads(Path(request_file).read_text(encoding="utf-8"))
    if response is None:
        if not response_file:
            fail("Provide response or response_file for runtime evaluation", code=ExitCode.VALIDATION)
        response = json.loads(Path(response_file).read_text(encoding="utf-8"))
    parsed = runtime_contract.parse_runtime_response(response, request=request)
    execution = runtime_contract.build_execution_result(
        request=request,
        response=parsed,
        status="validated",
        artifact_refs=[],
        telemetry={},
    )
    payload = {
        "valid": True,
        "request_id": request["request_id"],
        "task_type": request["task_type"],
        "chosen_action_id": parsed["chosen_action_id"],
        "action_type": parsed["action_type"],
        "execute": bool(parsed["execute"]),
        "reward_spec_version": str((request.get("reward_spec") or {}).get("version") or ""),
        "execution_preview": execution,
    }
    payload["artifact_path"] = _write_report("runtime", payload)
    return payload
