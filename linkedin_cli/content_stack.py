"""Stacked content ranking helpers built on top of scraped LinkedIn rows."""

from __future__ import annotations

from collections import defaultdict
import re
import json
import math
from pathlib import Path
from typing import Any

import numpy as np

DEFAULT_RERANK_WEIGHTS = {
    "public_performance": 0.3,
    "persona_style": 0.2,
    "business_intent": 0.3,
    "target_similarity": 0.2,
}


def _clamp_unit(value: float) -> float:
    return max(0.0, min(1.0, float(value)))


def _raw_engagement(row: dict[str, Any]) -> float:
    return float(row.get("reaction_count") or 0) + (2.0 * float(row.get("comment_count") or 0)) + (
        3.0 * float(row.get("repost_count") or 0)
    )


def _baseline_key(row: dict[str, Any]) -> str:
    topics = row.get("query_topics") or []
    parts: list[str] = []
    if isinstance(topics, list) and topics:
        parts.append(f"topics:{'|'.join(sorted(str(topic) for topic in topics if str(topic).strip()))}")
    industries = row.get("industries") or []
    if isinstance(industries, list) and industries:
        parts.append(f"industries:{'|'.join(sorted(str(industry) for industry in industries if str(industry).strip()))}")
    freshness_bucket = str(row.get("freshness_bucket") or "").strip()
    if freshness_bucket:
        parts.append(f"freshness_bucket:{freshness_bucket}")
    return "|".join(parts) if parts else "global"


def _mean(values: list[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def _combined_text(row: dict[str, Any]) -> str:
    parts = [
        str(row.get("text") or ""),
        str(row.get("title") or ""),
        str(row.get("hook") or ""),
    ]
    return " ".join(part for part in parts if part).lower()


def _row_topics(row: dict[str, Any]) -> list[str]:
    topics = row.get("topics")
    if not isinstance(topics, list) or not topics:
        topics = row.get("query_topics")
    if not isinstance(topics, list):
        return []
    return [str(topic).strip().lower() for topic in topics if str(topic).strip()]


def _contains_any(text: str, terms: list[str]) -> bool:
    return any(_term_in_text(text, term) for term in terms)


def _term_in_text(text: str, term: str) -> bool:
    escaped = re.escape(term.lower())
    pattern = rf"(?<!\w){escaped}(?!\w)"
    return bool(re.search(pattern, text))


def _matching_terms(text: str, terms: list[str]) -> list[str]:
    return [term for term in terms if _term_in_text(text, term)]


def _clamp_axis(score: float) -> float:
    return max(-1.0, min(1.0, score))


def _normalize_token(value: Any) -> str:
    return str(value or "").strip().lower().replace(" ", "_")


def _list_strings(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item).strip() for item in value if str(item).strip()]


def _stacked_feature_dict(row: dict[str, Any]) -> dict[str, float]:
    from linkedin_cli import content

    text = str(row.get("text") or "")
    title = str(row.get("title") or "")
    word_count = int(row.get("word_count") or 0)
    fingerprint = content._compute_content_fingerprint(text, title=title)
    vector = content._content_feature_vector(
        text=text,
        title=title,
        word_count=word_count,
        fingerprint=fingerprint,
    )
    feature_names = content._content_feature_names()
    features = {feature_names[index]: float(value) for index, value in enumerate(vector)}
    features["query_topic_count"] = float(len(_row_topics(row)))
    industries = _list_strings(row.get("industries")) or _list_strings(row.get("query_industries"))
    features["industry_count"] = float(len(industries))
    features["recent_bucket"] = 1.0 if _normalize_token(row.get("freshness_bucket")) == "recent" else 0.0
    return features


def _unit_interval(value: float) -> float:
    return round((math.tanh(float(value)) + 1.0) / 2.0, 6)


def _normalized_profile_tokens(values: Any) -> list[str]:
    if not isinstance(values, list):
        return []
    return [_normalize_token(value) for value in values if _normalize_token(value)]


def project_persona_style(row: dict[str, Any]) -> dict[str, Any]:
    """Project a row onto a small set of interpretable persona axes."""

    text = _combined_text(row)
    archetype = str(row.get("author_archetype") or "").strip().lower()
    tone = str(row.get("tone") or "").strip().lower()
    structure = str(row.get("structure") or "").strip().lower()
    cta_type = str(row.get("cta_type") or "").strip().lower()
    proof_level = str(row.get("proof_level") or "").strip().lower()
    word_count = int(row.get("word_count") or 0)

    operator_vs_storyteller = 0.0
    if archetype == "operator":
        operator_vs_storyteller += 0.8
    elif archetype == "founder":
        operator_vs_storyteller += 0.45
    elif archetype == "researcher":
        operator_vs_storyteller += 0.2
    elif archetype == "creator":
        operator_vs_storyteller += 0.1
    if _contains_any(text, ["workflow", "playbook", "pipeline", "revenue", "cac", "roi", "team", "system", "ship"]):
        operator_vs_storyteller += 0.25
    if _contains_any(text, ["story", "journey", "personal", "i learned", "my "]) or tone == "personal":
        operator_vs_storyteller -= 0.35
    if cta_type == "commercial":
        operator_vs_storyteller += 0.1

    tactical_vs_visionary = 0.0
    if structure in {"list", "guide", "framework", "playbook", "teardown"}:
        tactical_vs_visionary += 0.5
    if _contains_any(text, ["how to", "steps", "framework", "workflow", "playbook", "checklist", "implementation", "template"]):
        tactical_vs_visionary += 0.35
    if _contains_any(text, ["future", "vision", "transform", "category", "paradigm", "opportunity", "reimagine"]):
        tactical_vs_visionary -= 0.35
    if word_count >= 180:
        tactical_vs_visionary -= 0.1

    proof_heavy_vs_inspirational = 0.0
    if proof_level == "high":
        proof_heavy_vs_inspirational += 0.7
    elif proof_level == "medium":
        proof_heavy_vs_inspirational += 0.4
    if _contains_any(text, ["proof", "case study", "benchmark", "results", "measured", "saved", "reduced", "increased", "data", "cac", "pipeline", "revenue"]):
        proof_heavy_vs_inspirational += 0.25
    if _contains_any(text, ["inspiring", "vision", "believe", "mission", "dream", "possibility", "hope"]):
        proof_heavy_vs_inspirational -= 0.35

    directive_vs_observational = 0.0
    if tone == "directive":
        directive_vs_observational += 0.7
    elif tone == "assertive":
        directive_vs_observational += 0.35
    elif tone == "exploratory":
        directive_vs_observational -= 0.25
    if _contains_any(text, ["should", "must", "need to", "start", "stop", "use ", "do this", "try this", "don’t", "don't"]):
        directive_vs_observational += 0.25
    if _contains_any(text, ["noticed", "observed", "we saw", "in practice", "for example", "maybe", "might", "could"]):
        directive_vs_observational -= 0.15
    if cta_type == "commercial":
        directive_vs_observational += 0.1

    axes = {
        "operator_vs_storyteller": _clamp_axis(operator_vs_storyteller),
        "tactical_vs_visionary": _clamp_axis(tactical_vs_visionary),
        "proof_heavy_vs_inspirational": _clamp_axis(proof_heavy_vs_inspirational),
        "directive_vs_observational": _clamp_axis(directive_vs_observational),
    }
    return {
        "axes": axes,
        "persona_style_score": _mean(list(axes.values())),
    }


def label_business_intent(row: dict[str, Any]) -> dict[str, Any]:
    """Assign a deterministic business-intent proxy score from local signals."""

    text = _combined_text(row)
    archetype = str(row.get("author_archetype") or "").strip().lower()
    cta_type = str(row.get("cta_type") or "").strip().lower()
    proof_level = str(row.get("proof_level") or "").strip().lower()
    topics = _row_topics(row)

    commercial_terms = [
        "demo",
        "pipeline",
        "revenue",
        "call",
        "team",
        "customer",
        "customers",
        "roi",
        "cac",
        "lead",
        "leads",
        "deal",
        "book",
        "meeting",
        "close",
        "convert",
        "pricing",
        "trial",
        "pilot",
        "onboarding",
        "retention",
        "churn",
    ]
    cta_terms = [
        "dm",
        "dm me",
        "book a call",
        "book",
        "join",
        "subscribe",
        "comment",
        "reply",
        "download",
        "register",
    ]
    business_topics = {
        "ai",
        "b2b",
        "cloud",
        "cybersecurity",
        "data",
        "devtools",
        "fintech",
        "growth",
        "healthcare",
        "marketing",
        "recruiting",
        "sales",
        "workflow",
        "infrastructure",
        "enterprise",
    }

    commercial_verb_hits = _matching_terms(text, commercial_terms)
    cta_hits = _matching_terms(text, cta_terms)
    topic_hits = [topic for topic in topics if topic in business_topics]

    cta_score = 0.0
    if cta_type == "commercial":
        cta_score += 0.7
    elif cta_type == "engagement":
        cta_score += 0.25
    if cta_hits:
        cta_score += min(0.4, 0.15 + (0.1 * len(cta_hits)))

    archetype_score = 0.0
    if archetype == "operator":
        archetype_score += 0.35
    elif archetype == "founder":
        archetype_score += 0.3
    elif archetype == "researcher":
        archetype_score += 0.15
    elif archetype == "creator":
        archetype_score += 0.05

    topic_score = min(0.3, 0.1 * len(topic_hits))
    if topics and not topic_hits:
        topic_score -= 0.05

    proof_score = 0.0
    if proof_level == "high":
        proof_score = 0.25
    elif proof_level == "medium":
        proof_score = 0.15

    commercial_verb_score = min(0.35, 0.08 * len(commercial_verb_hits))

    business_intent_score = _clamp_axis(
        cta_score + archetype_score + topic_score + proof_score + commercial_verb_score
    )
    return {
        "cta_score": cta_score,
        "archetype_score": archetype_score,
        "topic_score": topic_score,
        "proof_score": proof_score,
        "commercial_verb_score": commercial_verb_score,
        "matched_cta_terms": cta_hits,
        "matched_topics": topic_hits,
        "matched_commercial_terms": commercial_verb_hits,
        "business_intent_score": business_intent_score,
        "business_intent_label": 1
        if business_intent_score >= 0.5
        and (cta_hits or cta_type == "commercial" or len(commercial_verb_hits) >= 2)
        else 0,
    }


def label_public_performance(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Label rows with a conditional public-performance proxy score."""

    enriched: list[dict[str, Any]] = []
    grouped_scores: dict[str, list[float]] = defaultdict(list)
    global_scores: list[float] = []

    for row in rows:
        raw_engagement = _raw_engagement(row)
        engagement_signal = math.log1p(max(0.0, raw_engagement))
        key = _baseline_key(row)
        grouped_scores[key].append(engagement_signal)
        global_scores.append(engagement_signal)
        enriched.append(
            {
                **row,
                "_baseline_key": key,
                "raw_engagement": raw_engagement,
                "engagement_signal": engagement_signal,
            }
        )

    for index, row in enumerate(enriched):
        key = row.pop("_baseline_key")
        bucket_scores = grouped_scores.get(key, [])
        if len(bucket_scores) > 1:
            expected_signal = (sum(bucket_scores) - row["engagement_signal"]) / (len(bucket_scores) - 1)
        else:
            others = [score for idx, score in enumerate(global_scores) if idx != index]
            expected_signal = _mean(others) if others else row["engagement_signal"]
        row["expected_engagement_signal"] = expected_signal
        row["expected_engagement"] = expected_signal
        row["overperformed_score"] = row["engagement_signal"] - expected_signal
        row["overperformed_label"] = 1 if row["overperformed_score"] > 0 else 0
        row["public_performance_score"] = round(max(-5.0, min(5.0, row["overperformed_score"])), 6)
    return enriched


def _warehouse_rows(*, industries: list[str] | None = None) -> list[dict[str, Any]]:
    from linkedin_cli import content_warehouse

    content_warehouse.build_foundation_views(industries=industries)
    conn = content_warehouse._warehouse_connect(read_only=True)
    try:
        cursor = conn.execute("SELECT * FROM content_foundation_posts ORDER BY url ASC")
        columns = [str(column[0]) for column in (cursor.description or [])]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]
    finally:
        conn.close()


def _rows_to_stacked_samples(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    labeled_rows = label_public_performance(rows)
    samples: list[dict[str, Any]] = []
    for row in labeled_rows:
        persona_projection = project_persona_style(row)
        business_label = label_business_intent(row)
        samples.append(
            {
                "features": _stacked_feature_dict(row),
                "labels": {
                    "public_performance": float(row.get("overperformed_label") or 0.0),
                    "persona_style": float(persona_projection.get("persona_style_score") or 0.0),
                    "business_intent": float(business_label.get("business_intent_score") or 0.0),
                },
                "metadata": {
                    "url": row.get("url"),
                    "group_key": str(row.get("author_url") or row.get("author_name") or row.get("content_hash") or row.get("url") or ""),
                },
            }
        )
    return samples


def _load_holdout_rows(holdout_dir: Path) -> dict[str, list[dict[str, Any]]]:
    splits: dict[str, list[dict[str, Any]]] = {}
    for split_name in ("train", "val", "test", "time_holdout"):
        path = Path(holdout_dir) / f"{split_name}.jsonl"
        if not path.exists():
            splits[split_name] = []
            continue
        splits[split_name] = [
            json.loads(line)
            for line in path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
    return splits


def train_stacked_model(
    *,
    model_name: str,
    artifact_dir: Path | None = None,
    industries: list[str] | None = None,
    min_samples: int = 10,
    holdout_dir: Path | None = None,
) -> dict[str, Any]:
    from linkedin_cli import modeling
    from linkedin_cli.write import store

    holdout_splits = _load_holdout_rows(Path(holdout_dir)) if holdout_dir else {}
    rows = holdout_splits.get("train") or _warehouse_rows(industries=industries)
    if len(rows) < max(1, int(min_samples)):
        return {
            "trained": False,
            "model_name": model_name,
            "sample_count": len(rows),
            "min_samples": int(min_samples),
            "reason": "Not enough foundation rows matched the requested filters.",
        }

    samples = _rows_to_stacked_samples(rows)

    summary = modeling.train_multi_head_model(
        task="content_stacked_ranking",
        samples=samples,
        artifact_dir=artifact_dir or (store.ARTIFACTS_DIR / "models" / "content-stacked"),
        model_name=model_name,
        metadata={"rerank_weights": DEFAULT_RERANK_WEIGHTS, "holdout_dir": str(holdout_dir) if holdout_dir else ""},
        head_types={"public_performance": "classification"},
    )
    summary["industries"] = list(industries or [])
    if holdout_splits:
        model = modeling.get_model(model_name)
        holdout_metrics: dict[str, Any] = {}
        if model:
            for split_name, split_rows in holdout_splits.items():
                if not split_rows:
                    continue
                split_samples = _rows_to_stacked_samples(split_rows)
                split_metrics: dict[str, Any] = {"row_count": len(split_samples)}
                for head_name in ("public_performance", "persona_style", "business_intent"):
                    labels = np.asarray([float((sample.get("labels") or {}).get(head_name) or 0.0) for sample in split_samples], dtype=np.float64)
                    preds = np.asarray(
                        [
                            float(_predict_head_values(model=model, row=row, normalize=False).get(head_name) or 0.0)
                            for row in split_rows
                        ],
                        dtype=np.float64,
                    )
                    head_payload = (model.get("heads") or {}).get(head_name) or {}
                    if str(head_payload.get("kind") or "regression") == "classification":
                        split_metrics[head_name] = modeling._classification_metrics(labels, preds)
                    else:
                        split_metrics[head_name] = modeling._regression_metrics(labels, preds)
                holdout_metrics[split_name] = split_metrics
        summary["holdout_metrics"] = holdout_metrics
        artifact_path = Path(summary.get("artifact_path") or "")
        if artifact_path.exists():
            artifact = json.loads(artifact_path.read_text(encoding="utf-8"))
            metadata = artifact.setdefault("metadata", {})
            metadata["holdout_metrics"] = holdout_metrics
            artifact_path.write_text(json.dumps(artifact, ensure_ascii=False, sort_keys=True), encoding="utf-8")
    return summary


def build_target_profile_vector(target_profile: dict[str, Any]) -> dict[str, Any]:
    metadata = target_profile if isinstance(target_profile, dict) else {}
    weights = dict(DEFAULT_RERANK_WEIGHTS)
    if isinstance(metadata.get("weights"), dict):
        for key, value in metadata["weights"].items():
            if key in weights:
                weights[key] = float(value)
    return {
        "company": str(metadata.get("company") or "").strip(),
        "buyer_roles": _normalized_profile_tokens(metadata.get("buyer_roles")),
        "industries": _normalized_profile_tokens(metadata.get("industries")),
        "problem_keywords": _normalized_profile_tokens(metadata.get("problem_keywords")),
        "preferred_cta": _normalized_profile_tokens(metadata.get("preferred_cta")),
        "tone_constraints": _normalized_profile_tokens(metadata.get("tone_constraints")),
        "weights": weights,
    }


def _infer_draft_tone(text: str) -> str:
    lowered = text.lower()
    if any(term in lowered for term in ("should", "must", "need to", "start ", "stop ")):
        return "directive"
    if any(term in lowered for term in (" i ", "\ni ", " my ", " we ", " our ")):
        return "personal"
    if "?" in text:
        return "exploratory"
    return "assertive"


def _infer_draft_cta_type(text: str) -> str:
    lowered = text.lower()
    if any(term in lowered for term in ("dm me", "book a call", "book time", "schedule", "register", "download")):
        return "commercial"
    if "?" in text or any(term in lowered for term in ("comment", "reply", "what are you seeing", "what breaks")):
        return "engagement"
    return "none"


def _infer_draft_proof_level(text: str) -> str:
    lowered = text.lower()
    if re.search(r"\b\d+(?:\.\d+)?%?\b", lowered) and any(
        term in lowered for term in ("saved", "reduced", "increased", "hours", "minutes", "days", "roi", "pipeline", "revenue")
    ):
        return "high"
    if any(term in lowered for term in ("case study", "benchmark", "results", "proof", "measured", "saved", "reduced", "increased")):
        return "medium"
    return "low"


def _infer_draft_archetype(text: str) -> str:
    lowered = text.lower()
    if any(term in lowered for term in ("workflow", "playbook", "operator", "ops", "pipeline", "system", "handoff", "implementation")):
        return "operator"
    if any(term in lowered for term in ("research", "paper", "benchmark", "study")):
        return "researcher"
    if any(term in lowered for term in ("founder", "category", "market", "company", "startup")):
        return "founder"
    return "creator"


def _draft_row_from_text(*, text: str, industry: str | None = None, topics: list[str] | None = None) -> dict[str, Any]:
    from linkedin_cli import content

    normalized = content._normalized_post_text(text)
    hook = content._sentence_hook(normalized)
    normalized_topics = [str(topic).strip().lower() for topic in list(topics or []) if str(topic).strip()]
    industries = [str(industry).strip().lower()] if str(industry or "").strip() else []
    return {
        "url": "",
        "title": hook,
        "hook": hook,
        "text": normalized,
        "word_count": len(normalized.split()),
        "structure": content._classify_structure(normalized),
        "industries": industries,
        "query_industries": industries,
        "topics": normalized_topics,
        "query_topics": normalized_topics,
        "freshness_bucket": "recent",
        "tone": _infer_draft_tone(normalized),
        "cta_type": _infer_draft_cta_type(normalized),
        "proof_level": _infer_draft_proof_level(normalized),
        "author_archetype": _infer_draft_archetype(normalized),
        "reaction_count": 0,
        "comment_count": 0,
        "repost_count": 0,
    }


def _head_quality_score(model: dict[str, Any], head_name: str) -> float:
    metadata = model.get("metadata") if isinstance(model, dict) else {}
    holdout_metrics = (metadata or {}).get("holdout_metrics") if isinstance(metadata, dict) else {}
    split_names = ("test", "time_holdout", "val")
    per_split_scores: list[float] = []
    for split_name in split_names:
        split_metrics = (holdout_metrics or {}).get(split_name) if isinstance(holdout_metrics, dict) else None
        head_metrics = (split_metrics or {}).get(head_name) if isinstance(split_metrics, dict) else None
        if not isinstance(head_metrics, dict):
            continue
        if head_name == "public_performance":
            auc = _clamp_unit((float(head_metrics.get("roc_auc") or 0.5) - 0.5) / 0.5)
            brier = 1.0 - _clamp_unit(float(head_metrics.get("brier_score") or 0.25) / 0.25)
            ece = 1.0 - _clamp_unit(float(head_metrics.get("ece") or 0.1) / 0.1)
            per_split_scores.append((0.6 * auc) + (0.25 * brier) + (0.15 * ece))
        else:
            per_split_scores.append(_clamp_unit(float(head_metrics.get("r2") or 0.0)))
    if per_split_scores:
        return round(_mean(per_split_scores), 6)

    head_payload = ((model.get("heads") or {}).get(head_name) or {}) if isinstance(model, dict) else {}
    metrics = (head_payload.get("metrics") or {}) if isinstance(head_payload, dict) else {}
    if head_name == "public_performance":
        auc = _clamp_unit((float(metrics.get("validation_roc_auc") or 0.5) - 0.5) / 0.5)
        brier = 1.0 - _clamp_unit(float(metrics.get("validation_brier_score") or 0.25) / 0.25)
        ece = 1.0 - _clamp_unit(float(metrics.get("validation_ece") or 0.1) / 0.1)
        return round((0.6 * auc) + (0.25 * brier) + (0.15 * ece), 6)
    return round(_clamp_unit(float(metrics.get("validation_r2") or 0.0)), 6)


def calibrated_rerank_weights(*, model: dict[str, Any], base_weights: dict[str, float]) -> dict[str, float]:
    strategic_floors = {
        "public_performance": 0.12,
        "persona_style": 0.2,
        "business_intent": 0.4,
    }
    weighted = dict(base_weights)
    for head_name in ("public_performance", "persona_style", "business_intent"):
        quality = _head_quality_score(model, head_name)
        weighted[head_name] = float(base_weights.get(head_name) or 0.0) * max(float(strategic_floors.get(head_name) or 0.1), quality)
    total = sum(float(weighted.get(key) or 0.0) for key in DEFAULT_RERANK_WEIGHTS)
    if total <= 1e-9:
        return dict(base_weights)
    keys = list(DEFAULT_RERANK_WEIGHTS.keys())
    normalized: dict[str, float] = {}
    running = 0.0
    for key in keys[:-1]:
        value = round(float(weighted.get(key) or 0.0) / total, 6)
        normalized[key] = value
        running += value
    normalized[keys[-1]] = round(max(0.0, 1.0 - running), 6)
    return normalized


def score_text_for_target(
    *,
    text: str,
    industry: str | None = None,
    topics: list[str] | None = None,
    target_profile: dict[str, Any] | None = None,
    model_name: str | None = None,
    auto_calibrate_weights: bool = True,
) -> dict[str, Any]:
    from linkedin_cli import modeling

    selected = select_best_stacked_model() if not model_name else None
    resolved_model_name = str(model_name or (selected or {}).get("model_name") or "").strip()
    if not resolved_model_name:
        raise ValueError("No stacked model name was provided and automatic selection failed")
    model = modeling.get_model(resolved_model_name)
    if not model:
        raise ValueError(f"Model not found: {resolved_model_name}")

    row = _draft_row_from_text(text=text, industry=industry, topics=topics)
    target_vector = build_target_profile_vector(target_profile or {})
    weights = dict(target_vector["weights"] if target_profile else DEFAULT_RERANK_WEIGHTS)
    if not target_profile:
        weights["target_similarity"] = 0.0
        total = sum(float(weights.get(key) or 0.0) for key in DEFAULT_RERANK_WEIGHTS)
        if total > 0:
            for key in DEFAULT_RERANK_WEIGHTS:
                weights[key] = float(weights.get(key) or 0.0) / total
    if auto_calibrate_weights:
        calibrated = calibrated_rerank_weights(model=model, base_weights=weights)
        if not target_profile:
            calibrated["target_similarity"] = 0.0
            total = sum(float(calibrated.get(key) or 0.0) for key in DEFAULT_RERANK_WEIGHTS)
            if total > 0:
                for key in DEFAULT_RERANK_WEIGHTS:
                    calibrated[key] = round(float(calibrated.get(key) or 0.0) / total, 6)
        weights = calibrated

    head_scores = _predict_head_values(model=model, row=row, normalize=True)
    target_similarity = _target_similarity_score(row, target_vector) if target_profile else 0.0
    score_breakdown = {
        "public_performance": float(head_scores.get("public_performance") or 0.0),
        "persona_style": float(head_scores.get("persona_style") or 0.0),
        "business_intent": float(head_scores.get("business_intent") or 0.0),
        "target_similarity": float(target_similarity),
    }
    final_score = sum(float(score_breakdown[key]) * float(weights.get(key) or 0.0) for key in DEFAULT_RERANK_WEIGHTS)
    return {
        "model_name": resolved_model_name,
        "selected_model": selected,
        "industry": industry,
        "topics": list(topics or []),
        "target_profile": dict(target_profile or {}),
        "score_breakdown": score_breakdown,
        "weights_used": {key: round(float(weights.get(key) or 0.0), 6) for key in DEFAULT_RERANK_WEIGHTS},
        "final_score": round(final_score, 6),
    }


def select_best_stacked_model() -> dict[str, Any]:
    from linkedin_cli import modeling
    from linkedin_cli.write import store

    modeling.init_modeling_db()
    conn = store._connect()
    try:
        rows = conn.execute(
            """
            SELECT model_name
            FROM model_registry
            WHERE task_type = ?
            ORDER BY updated_at DESC, model_name DESC
            """,
            ("content_stacked_ranking",),
        ).fetchall()
    finally:
        conn.close()
    candidates: list[dict[str, Any]] = []
    for row in rows:
        model_name = str(row[0])
        model = modeling.get_model(model_name)
        if not model:
            continue
        head_quality = {
            "public_performance": _head_quality_score(model, "public_performance"),
            "persona_style": _head_quality_score(model, "persona_style"),
            "business_intent": _head_quality_score(model, "business_intent"),
        }
        selection_score = (
            head_quality["public_performance"] * 0.3
            + head_quality["persona_style"] * 0.35
            + head_quality["business_intent"] * 0.35
        )
        candidates.append(
            {
                "model_name": model_name,
                "selection_score": round(selection_score, 6),
                "head_quality": head_quality,
                "artifact_path": model.get("artifact_path"),
            }
        )
    if not candidates:
        raise ValueError("No stored stacked models found")
    candidates.sort(key=lambda item: (float(item["selection_score"]), str(item["model_name"])), reverse=True)
    return candidates[0]


def _predict_head_values(*, model: dict[str, Any], row: dict[str, Any], normalize: bool) -> dict[str, float]:
    if str(model.get("kind") or "") != "multi_head_linear":
        raise ValueError("Model is not a stacked multi-head artifact")
    feature_names = [str(name) for name in model.get("feature_names") or []]
    features = _stacked_feature_dict(row)
    vector = [float(features.get(name) or 0.0) for name in feature_names]
    means = model.get("means") or [0.0] * len(feature_names)
    stds = [1.0 if float(value or 0.0) < 1e-6 else float(value) for value in (model.get("stds") or [1.0] * len(feature_names))]
    normalized = [(value - float(mean)) / float(std) for value, mean, std in zip(vector, means, stds)]
    scores: dict[str, float] = {}
    for head_name, payload in (model.get("heads") or {}).items():
        weights = [float(value) for value in (payload.get("weights") or [])]
        intercept = float(payload.get("intercept") or 0.0)
        raw_score = sum(value * weight for value, weight in zip(normalized, weights)) + intercept
        head_kind = str(payload.get("kind") or "regression")
        if head_kind == "classification":
            calibration = payload.get("calibration") if isinstance(payload, dict) else None
            if isinstance(calibration, dict) and str(calibration.get("method") or "") == "platt":
                slope = float(calibration.get("slope") or 1.0)
                offset = float(calibration.get("intercept") or 0.0)
                raw_score = (raw_score * slope) + offset
            prob = float(1.0 / (1.0 + math.exp(-max(-30.0, min(30.0, raw_score)))))
            scores[str(head_name)] = prob
        else:
            scores[str(head_name)] = _unit_interval(raw_score) if normalize else float(raw_score)
    return scores


def _predict_head_scores(*, model_name: str, row: dict[str, Any]) -> dict[str, float]:
    from linkedin_cli import modeling

    model = modeling.get_model(model_name)
    if not model:
        raise ValueError(f"Model not found: {model_name}")
    return _predict_head_values(model=model, row=row, normalize=True)


def _target_similarity_score(row: dict[str, Any], target_vector: dict[str, Any]) -> float:
    row_industries = {_normalize_token(value) for value in (_list_strings(row.get("industries")) or _list_strings(row.get("query_industries")))}
    row_topics = {_normalize_token(value) for value in _row_topics(row)}
    text = _combined_text(row)
    tone = _normalize_token(row.get("tone"))
    cta_type = _normalize_token(row.get("cta_type"))
    component_scores: list[float] = []

    target_industries = set(target_vector.get("industries") or [])
    if target_industries:
        component_scores.append(len(row_industries & target_industries) / float(len(target_industries)))

    problem_keywords = list(target_vector.get("problem_keywords") or [])
    if problem_keywords:
        matches = 0
        for keyword in problem_keywords:
            phrase = keyword.replace("_", " ")
            if _term_in_text(text, phrase) or keyword in row_topics:
                matches += 1
        component_scores.append(matches / float(len(problem_keywords)))

    preferred_cta = set(target_vector.get("preferred_cta") or [])
    if preferred_cta:
        component_scores.append(1.0 if cta_type in preferred_cta else 0.0)

    tone_constraints = set(target_vector.get("tone_constraints") or [])
    if tone_constraints:
        component_scores.append(1.0 if tone in tone_constraints else 0.0)

    buyer_roles = list(target_vector.get("buyer_roles") or [])
    if buyer_roles:
        role_match = False
        for role in buyer_roles:
            parts = [part for part in role.split("_") if len(part) >= 3]
            if any(_term_in_text(text, part) for part in parts):
                role_match = True
                break
        component_scores.append(1.0 if role_match else 0.5)

    return round(_mean(component_scores) if component_scores else 0.5, 6)


def rerank_for_target(
    *,
    posts: list[dict[str, Any]] | None,
    target_profile: dict[str, Any],
    model_name: str,
    auto_calibrate_weights: bool = True,
) -> list[dict[str, Any]]:
    from linkedin_cli import modeling

    target_vector = build_target_profile_vector(target_profile)
    weights = dict(target_vector["weights"])
    model = modeling.get_model(model_name)
    if not model:
        raise ValueError(f"Model not found: {model_name}")
    if auto_calibrate_weights:
        weights = calibrated_rerank_weights(model=model, base_weights=weights)
    ranked: list[dict[str, Any]] = []
    source_posts = posts if posts is not None else _warehouse_rows()
    for row in source_posts:
        head_scores = _predict_head_values(model=model, row=row, normalize=True)
        target_similarity = _target_similarity_score(row, target_vector)
        score_breakdown = {
            "public_performance": float(head_scores.get("public_performance") or 0.0),
            "persona_style": float(head_scores.get("persona_style") or 0.0),
            "business_intent": float(head_scores.get("business_intent") or 0.0),
            "target_similarity": target_similarity,
        }
        final_score = sum(float(score_breakdown[key]) * float(weights.get(key) or 0.0) for key in score_breakdown)
        ranked.append(
            {
                **row,
                "score_breakdown": score_breakdown,
                "weights_used": dict(weights),
                "final_score": round(final_score, 6),
                "target_profile": {
                    "company": target_vector["company"],
                    "industries": list(target_vector["industries"]),
                },
            }
        )
    ranked.sort(key=lambda item: (float(item.get("final_score") or 0.0), str(item.get("url") or "")), reverse=True)
    return ranked


def audit_target_profiles(
    *,
    profiles: dict[str, dict[str, Any]],
    model_name: str,
    posts: list[dict[str, Any]] | None = None,
    limit: int = 10,
) -> dict[str, Any]:
    source_posts = posts if posts is not None else _warehouse_rows()
    audits: list[dict[str, Any]] = []
    for profile_name, target_profile in profiles.items():
        calibrated = rerank_for_target(
            posts=source_posts,
            target_profile=target_profile,
            model_name=model_name,
            auto_calibrate_weights=True,
        )[:limit]
        raw = rerank_for_target(
            posts=source_posts,
            target_profile=target_profile,
            model_name=model_name,
            auto_calibrate_weights=False,
        )[:limit]
        calibrated_urls = [str(row.get("url") or "") for row in calibrated]
        raw_urls = [str(row.get("url") or "") for row in raw]
        audits.append(
            {
                "profile_name": profile_name,
                "target_profile": target_profile,
                "weights": dict(calibrated[0].get("weights_used") or {}) if calibrated else {},
                "top_overlap": len(set(calibrated_urls) & set(raw_urls)),
                "calibrated_results": calibrated,
                "raw_results": raw,
            }
        )
    return {
        "model_name": model_name,
        "sample_count": len(source_posts),
        "limit": int(limit),
        "audits": audits,
    }
