"""Provider-backed content generation helpers."""

from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any

import requests

from linkedin_cli.session import DEFAULT_TIMEOUT, ExitCode, fail
from linkedin_cli.write import store


DEFAULT_PROVIDER_NAME = "cerebras"
DEFAULT_CEREBRAS_BASE_URL = "https://api.cerebras.ai/v1"
DEFAULT_CEREBRAS_MODEL = "gpt-oss-120b"
MAX_PROVIDER_GENERATION_ATTEMPTS = 3

_CASE_STUDY_PATTERNS = (
    re.compile(r"\b(?:our|one|a)\s+client\b", flags=re.IGNORECASE),
    re.compile(r"\bfortune[\s-]*\d+\b", flags=re.IGNORECASE),
    re.compile(r"\ba\s+(?:retailer|neobank|startup|bank|fintech startup|series [a-z] company)\b", flags=re.IGNORECASE),
    re.compile(r"\bwhen\s+a[n]?\s+[^.]{0,80}\b(?:team|company|startup|retailer|neobank|bank|firm|business)\b", flags=re.IGNORECASE),
    re.compile(r"\bwhen\s+(?:we|i)\s+(?:first\s+)?(?:tried|attempted|stitched|wired|integrated|hooked|plugged|rolled\s+out|deployed|launched)\b", flags=re.IGNORECASE),
    re.compile(r"\b(?:a|the)\s+team\s+(?:spent|saw|found|discovered|rewrote|rebuilt|scrambled)\b", flags=re.IGNORECASE),
    re.compile(r"\b(?:i['’]ve|i have|we['’]ve|we have)\s+(?:consulted|advised|worked with|seen|watched)\b", flags=re.IGNORECASE),
    re.compile(r"\bacross\s+(?:one|two|three|four|five|\d+)\s+[^.]{0,50}\bteams\b", flags=re.IGNORECASE),
    re.compile(r"\bour\s+internal\s+[^.]{0,40}\b(?:system|workflow|pipeline|service|tool|stack)\b", flags=re.IGNORECASE),
)
_EVIDENCE_VERB_PATTERNS = (
    re.compile(r"\b(?:we|i)\s+(?:helped|worked with|tested|saw|found|discovered|redesigned|rebuilt|lifted|cut|reduced|improved|shaved|recaptured|generated|saved|grew)\b", flags=re.IGNORECASE),
    re.compile(r"\b(?:we|i)\s+(?:built|implemented|deployed|shipped|launched|rolled\s+out)\b", flags=re.IGNORECASE),
)
_NUMERIC_CLAIM_PATTERNS = (
    re.compile(r"\$\s?\d[\d,]*(?:\.\d+)?\s*(?:[kmb]|million|billion|thousand)?", flags=re.IGNORECASE),
    re.compile(r"\b\d+(?:\.\d+)?%", flags=re.IGNORECASE),
    re.compile(r"\b\d+(?:\.\d+)?x\b", flags=re.IGNORECASE),
    re.compile(r"\bin\s+\d+\s+(?:day|days|week|weeks|month|months|quarter|quarters|year|years)\b", flags=re.IGNORECASE),
)
_PROMPT_INSTRUCTION_PREFIX = re.compile(
    r"^(?:create|write|draft|generate)\s+(?:a\s+)?(?:linkedin\s+post|post|draft|thread)\s+(?:about|on|for)\s+",
    flags=re.IGNORECASE,
)


def provider_config_path() -> Path:
    return store.DB_PATH.parent / "providers.json"


def _load_all_configs() -> dict[str, Any]:
    path = provider_config_path()
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _save_all_configs(payload: dict[str, Any]) -> None:
    path = provider_config_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")


def save_provider_config(
    *,
    provider_name: str,
    api_key: str,
    model: str,
    base_url: str | None = None,
) -> dict[str, Any]:
    normalized_name = str(provider_name or "").strip().lower()
    if normalized_name != "cerebras":
        fail("Only the cerebras provider is currently supported", code=ExitCode.VALIDATION)
    if not str(api_key or "").strip():
        fail("Provider api key is required", code=ExitCode.VALIDATION)
    if not str(model or "").strip():
        fail("Provider model is required", code=ExitCode.VALIDATION)
    config = {
        "provider_name": normalized_name,
        "api_key": str(api_key).strip(),
        "base_url": str(base_url or DEFAULT_CEREBRAS_BASE_URL).rstrip("/"),
        "model": str(model).strip(),
    }
    payload = _load_all_configs()
    payload[normalized_name] = config
    _save_all_configs(payload)
    return {**config, "api_key": "***"}


def load_provider_config(provider_name: str = DEFAULT_PROVIDER_NAME) -> dict[str, Any]:
    normalized_name = str(provider_name or DEFAULT_PROVIDER_NAME).strip().lower()
    if normalized_name != "cerebras":
        fail("Only the cerebras provider is currently supported", code=ExitCode.VALIDATION)
    payload = _load_all_configs().get(normalized_name) or {}
    api_key = (
        os.getenv("CEREBRAS_API_KEY", "").strip()
        or str(payload.get("api_key") or "").strip()
    )
    if not api_key:
        fail("Cerebras API key not configured. Set CEREBRAS_API_KEY or run `linkedin content provider-set --provider cerebras`.", code=ExitCode.VALIDATION)
    return {
        "provider_name": normalized_name,
        "api_key": api_key,
        "base_url": str(os.getenv("CEREBRAS_BASE_URL", "") or payload.get("base_url") or DEFAULT_CEREBRAS_BASE_URL).rstrip("/"),
        "model": str(os.getenv("CEREBRAS_MODEL", "") or payload.get("model") or DEFAULT_CEREBRAS_MODEL).strip(),
    }


def is_provider_configured(provider_name: str = DEFAULT_PROVIDER_NAME) -> bool:
    normalized_name = str(provider_name or DEFAULT_PROVIDER_NAME).strip().lower()
    if normalized_name != "cerebras":
        return False
    payload = _load_all_configs().get(normalized_name) or {}
    api_key = (
        os.getenv("CEREBRAS_API_KEY", "").strip()
        or str(payload.get("api_key") or "").strip()
    )
    model = str(os.getenv("CEREBRAS_MODEL", "") or payload.get("model") or DEFAULT_CEREBRAS_MODEL).strip()
    return bool(api_key and model)


def _extract_json_object(text: str) -> dict[str, Any]:
    raw = str(text or "").strip()
    if not raw:
        fail("Provider returned empty content", code=ExitCode.RETRYABLE)
    fenced = re.search(r"```(?:json)?\s*(\{.*\})\s*```", raw, flags=re.DOTALL)
    if fenced:
        raw = fenced.group(1)
    if not raw.startswith("{"):
        start = raw.find("{")
        end = raw.rfind("}")
        if start >= 0 and end > start:
            raw = raw[start : end + 1]
    try:
        payload = json.loads(raw)
    except Exception as exc:
        fail(f"Provider returned invalid JSON: {exc}", code=ExitCode.RETRYABLE)
    if not isinstance(payload, dict):
        fail("Provider JSON payload must be an object", code=ExitCode.RETRYABLE)
    return payload


def _provider_chat_completion(
    *,
    provider_name: str,
    messages: list[dict[str, str]],
    temperature: float,
    max_completion_tokens: int,
) -> dict[str, Any]:
    config = load_provider_config(provider_name)
    response = requests.post(
        f"{config['base_url']}/chat/completions",
        headers={
            "Authorization": f"Bearer {config['api_key']}",
            "Content-Type": "application/json",
        },
        json={
            "model": config["model"],
            "messages": messages,
            "response_format": {"type": "json_object"},
            "temperature": temperature,
            "max_completion_tokens": max_completion_tokens,
        },
        timeout=DEFAULT_TIMEOUT,
    )
    if response.status_code >= 400:
        snippet = response.text[:500].strip().replace("\n", " ") if response.text else ""
        fail(f"Provider request failed - HTTP {response.status_code}: {snippet}", code=ExitCode.RETRYABLE)
    try:
        payload = response.json()
    except Exception as exc:
        fail(f"Provider returned non-JSON response: {exc}", code=ExitCode.RETRYABLE)
    choices = payload.get("choices") or []
    message = ((choices[0] or {}).get("message") or {}) if choices else {}
    content = str(message.get("content") or "").strip()
    if not content:
        fail("Provider returned empty content", code=ExitCode.RETRYABLE)
    return {
        "provider": config["provider_name"],
        "model": config["model"],
        "payload": payload,
        "content": content,
    }


def validate_candidate_truthfulness(*, text: str, prompt: str) -> list[str]:
    normalized_text = str(text or "").strip()
    normalized_prompt = str(prompt or "").strip()
    if not normalized_text:
        return ["empty candidate text"]

    issues: list[str] = []
    lowered_prompt = normalized_prompt.lower()
    for pattern in _CASE_STUDY_PATTERNS:
        if pattern.search(normalized_text):
            issues.append("unsupported case-study or client claim")
            break
    for pattern in _EVIDENCE_VERB_PATTERNS:
        if pattern.search(normalized_text):
            issues.append("unsupported first-person performance claim")
            break
    for pattern in _NUMERIC_CLAIM_PATTERNS:
        for match in pattern.findall(normalized_text):
            token = str(match).strip()
            if token and token.lower() not in lowered_prompt:
                issues.append(f"unsupported numeric business claim: {token}")
                break
        if issues and issues[-1].startswith("unsupported numeric"):
            break
    return issues


def _normalize_overlap_text(value: str) -> str:
    lowered = str(value or "").lower()
    lowered = re.sub(r"[^a-z0-9\s]", " ", lowered)
    return re.sub(r"\s+", " ", lowered).strip()


def _trigrams(value: str) -> set[str]:
    tokens = [token for token in _normalize_overlap_text(value).split() if token]
    if len(tokens) < 3:
        return set()
    return {" ".join(tokens[index : index + 3]) for index in range(len(tokens) - 2)}


def _latent_example_constraints(playbook: dict[str, Any], exemplars: list[dict[str, Any]]) -> dict[str, Any]:
    hook_types = [
        str(item.get("hook_type") or "").strip().lower()
        for item in (playbook.get("hook_templates") or [])
        if str(item.get("hook_type") or "").strip()
    ]
    structures = [
        str(item.get("structure") or "").strip().lower()
        for item in (playbook.get("winning_structures") or [])
        if str(item.get("structure") or "").strip()
    ]
    proof_bias = any(
        str(item.get("feature") or "").strip().lower() == "proof"
        for item in (playbook.get("learned_signals") or {}).get("top_positive", [])
    )
    cta_bias = any("cta" in str(rule or "").lower() for rule in (playbook.get("rewrite_rules") or []))
    return {
        "preferred_hook_types": list(dict.fromkeys(hook_types))[:3],
        "preferred_structures": list(dict.fromkeys(structures))[:3],
        "proof_density": "high" if proof_bias else "medium",
        "cta_style": "direct" if cta_bias else "light",
        "example_count": len(exemplars[:5]),
    }


def validate_candidate_originality(*, text: str, prompt: str, exemplars: list[dict[str, Any]]) -> list[str]:
    normalized_text = _normalize_overlap_text(text)
    normalized_prompt = _normalize_overlap_text(prompt)
    issues: list[str] = []
    stripped_prompt = _PROMPT_INSTRUCTION_PREFIX.sub("", normalized_prompt).strip()
    if normalized_prompt and normalized_text.startswith(normalized_prompt[: min(len(normalized_prompt), 48)]):
        issues.append("candidate reuses prompt instruction text")
    if stripped_prompt and normalized_text.startswith(stripped_prompt) and normalized_prompt != stripped_prompt:
        issues.append("candidate opens by restating the prompt subject too literally")

    candidate_ngrams = _trigrams(text)
    for exemplar in exemplars[:8]:
        for field in ("hook", "title", "text"):
            source = str((exemplar or {}).get(field) or "").strip()
            if not source:
                continue
            normalized_source = _normalize_overlap_text(source)
            if len(normalized_source) >= 40 and normalized_source in normalized_text:
                issues.append(f"candidate reuses exemplar {field}")
                return issues
            source_ngrams = _trigrams(source)
            if candidate_ngrams and source_ngrams:
                overlap = len(candidate_ngrams & source_ngrams) / float(max(1, min(len(candidate_ngrams), len(source_ngrams))))
                if overlap >= 0.6:
                    issues.append(f"candidate is too close to exemplar {field}")
                    return issues
    return issues


def _candidate_request_messages(
    *,
    prompt: str,
    industry: str | None,
    topics: list[str],
    candidate_goals: list[str],
    candidate_count: int,
    playbook: dict[str, Any],
    exemplars: list[dict[str, Any]],
    brief: dict[str, Any] | None = None,
    truth_mode: str = "strict",
    rejection_notes: list[str] | None = None,
) -> list[dict[str, str]]:
    learned = playbook.get("learned_signals") or {}
    system = (
        "You write high-signal LinkedIn posts for niche B2B operators. "
        "Return JSON only. Do not include markdown fences. "
        "Generate candidate posts that are specific, concrete, proof-heavy, and non-generic. "
        "Never use hashtag spam. Never output placeholders. "
        "Output schema: {\"candidates\": [{\"goal\": str, \"text\": str}]}"
    )
    if str(truth_mode or "strict").strip().lower() == "strict":
        system += (
            " Strict truth mode is on. Do not invent clients, employers, experiments, results, percentages, dollar values, "
            "time-to-result claims, case studies, or named company details unless they were explicitly provided in the prompt. "
            "If the prompt does not supply hard facts, write strong opinion, observation, or process-oriented posts without fabricated evidence."
        )
    user = json.dumps(
        {
            "task": "Generate LinkedIn post candidates",
            "prompt": prompt,
            "industry": industry,
            "topics": topics,
            "candidate_goals": candidate_goals,
            "candidate_count": candidate_count,
            "brief": dict(brief or {}),
            "example_constraints": _latent_example_constraints(playbook, exemplars),
            "playbook": {
                "hook_templates": playbook.get("hook_templates") or [],
                "rewrite_rules": playbook.get("rewrite_rules") or [],
                "winning_structures": playbook.get("winning_structures") or [],
                "winning_topics": playbook.get("winning_topics") or [],
                "learned_signals": learned,
            },
            "requirements": [
                "each candidate must be a complete LinkedIn post",
                "vary framing across goals",
                "lead with a strong first line",
                "include proof or consequence where possible",
                "make the slice clearly relevant",
                "do not restate the prompt instruction",
                "do not copy exemplar wording or hooks",
            ],
            "truth_mode": truth_mode,
            "rejection_notes": list(rejection_notes or []),
        },
        ensure_ascii=False,
        indent=2,
        sort_keys=True,
    )
    return [{"role": "system", "content": system}, {"role": "user", "content": user}]


def generate_candidates_via_provider(
    *,
    provider_name: str,
    prompt: str,
    industry: str | None,
    topics: list[str],
    candidate_goals: list[str],
    candidate_count: int,
    playbook: dict[str, Any],
    exemplars: list[dict[str, Any]],
    brief: dict[str, Any] | None = None,
    truth_mode: str = "strict",
) -> dict[str, Any]:
    accepted: list[dict[str, Any]] = []
    rejection_notes: list[str] = []
    completion: dict[str, Any] | None = None
    for _attempt in range(MAX_PROVIDER_GENERATION_ATTEMPTS):
        remaining = max(1, int(candidate_count) - len(accepted))
        completion = _provider_chat_completion(
            provider_name=provider_name,
            messages=_candidate_request_messages(
                prompt=prompt,
                industry=industry,
                topics=topics,
                candidate_goals=candidate_goals,
                candidate_count=remaining,
                playbook=playbook,
                exemplars=exemplars,
                brief=brief,
                truth_mode=truth_mode,
                rejection_notes=rejection_notes[-5:],
            ),
            temperature=0.8,
            max_completion_tokens=2200,
        )
        data = _extract_json_object(completion["content"])
        candidates = data.get("candidates")
        if not isinstance(candidates, list) or not candidates:
            rejection_notes.append("provider returned no candidates")
            continue
        for item in candidates:
            if not isinstance(item, dict):
                continue
            goal = str(item.get("goal") or candidate_goals[len(accepted) % len(candidate_goals)]).strip().lower()
            text = str(item.get("text") or "").strip()
            if not text:
                continue
            issues = validate_candidate_truthfulness(text=text, prompt=prompt) if str(truth_mode or "strict").strip().lower() == "strict" else []
            issues.extend(validate_candidate_originality(text=text, prompt=prompt, exemplars=exemplars))
            if issues:
                rejection_notes.extend(issues)
                continue
            accepted.append(
                {
                    "candidate_id": f"draft-{len(accepted) + 1:02d}",
                    "goal": goal,
                    "text": text,
                    "generator": {
                        "source": f"{completion['provider']}:{completion['model']}",
                        "provider": completion["provider"],
                        "model": completion["model"],
                    },
                }
            )
            if len(accepted) >= max(1, int(candidate_count)):
                break
        if len(accepted) >= max(1, int(candidate_count)):
            break
    if not accepted:
        reason = rejection_notes[0] if rejection_notes else "Provider returned zero usable candidates"
        fail(f"Provider returned zero usable candidates: {reason}", code=ExitCode.RETRYABLE)
    assert completion is not None
    return {
        "provider": completion["provider"],
        "model": completion["model"],
        "candidates": accepted[: max(1, int(candidate_count))],
        "raw_response": completion["payload"],
        "rejection_notes": rejection_notes,
    }


def decide_runtime_action_via_provider(
    *,
    provider_name: str,
    request: dict[str, Any],
) -> dict[str, Any]:
    from linkedin_cli import runtime_contract

    messages = runtime_contract.render_runtime_messages(request)
    completion = _provider_chat_completion(
        provider_name=provider_name,
        messages=messages,
        temperature=0.1,
        max_completion_tokens=1400,
    )
    response_payload = runtime_contract.parse_runtime_response(
        _extract_json_object(completion["content"]),
        request=request,
    )
    return {
        "provider": completion["provider"],
        "model": completion["model"],
        "messages": messages,
        "raw_content": completion["content"],
        "raw_response": completion["payload"],
        "response": response_payload,
    }
