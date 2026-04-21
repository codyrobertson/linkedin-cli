"""Local probabilistic modeling helpers for lead and content ranking tasks."""

from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any

import numpy as np

from linkedin_cli.write import store


def init_modeling_db() -> None:
    conn = store._connect()
    try:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS model_registry (
                model_name TEXT PRIMARY KEY,
                task_type TEXT NOT NULL,
                artifact_path TEXT NOT NULL,
                metrics_json TEXT NOT NULL,
                feature_schema_json TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            """
        )
        conn.commit()
    finally:
        conn.close()


def _sigmoid(values: np.ndarray) -> np.ndarray:
    clipped = np.clip(values, -30.0, 30.0)
    return 1.0 / (1.0 + np.exp(-clipped))


def _feature_names(samples: list[dict[str, Any]]) -> list[str]:
    return sorted({str(key) for sample in samples for key in (sample.get("features") or {})})


def _matrix_from_samples(samples: list[dict[str, Any]], feature_names: list[str]) -> tuple[np.ndarray, np.ndarray]:
    rows = [
        [float((sample.get("features") or {}).get(name) or 0.0) for name in feature_names]
        for sample in samples
    ]
    labels = [float(sample.get("label") or 0.0) for sample in samples]
    return np.asarray(rows, dtype=np.float64), np.asarray(labels, dtype=np.float64)


def _feature_matrix(samples: list[dict[str, Any]], feature_names: list[str]) -> np.ndarray:
    rows = [
        [float((sample.get("features") or {}).get(name) or 0.0) for name in feature_names]
        for sample in samples
    ]
    return np.asarray(rows, dtype=np.float64)


def _sample_groups(samples: list[dict[str, Any]]) -> list[str]:
    return [str(((sample.get("metadata") or {}).get("group_key") or "")).strip() for sample in samples]


def _train_validation_split(labels: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    positives = [index for index, label in enumerate(labels.tolist()) if label >= 0.5]
    negatives = [index for index, label in enumerate(labels.tolist()) if label < 0.5]
    validation = set(positives[::4] + negatives[::4])
    if len(validation) < 2:
        validation = set(range(min(len(labels), max(1, len(labels) // 4))))
    train = [index for index in range(len(labels)) if index not in validation]
    if not train:
        train = list(range(len(labels)))
        validation = set()
    return np.asarray(train, dtype=np.int64), np.asarray(sorted(validation), dtype=np.int64)


def _grouped_row_split(groups: list[str]) -> tuple[np.ndarray, np.ndarray, dict[str, Any]]:
    normalized_groups = [group if group else f"row:{index}" for index, group in enumerate(groups)]
    ordered_groups = sorted(dict.fromkeys(normalized_groups))
    if len(ordered_groups) <= 1:
        indices = np.asarray(list(range(len(normalized_groups))), dtype=np.int64)
        return (
            indices,
            np.asarray([], dtype=np.int64),
            {
                "strategy": "grouped",
                "train_groups": ordered_groups,
                "validation_groups": [],
                "train_group_count": len(ordered_groups),
                "validation_group_count": 0,
            },
        )
    validation_group_count = max(1, len(ordered_groups) // 4)
    validation_groups = ordered_groups[:validation_group_count]
    validation_set = set(validation_groups)
    train = [index for index, group in enumerate(normalized_groups) if group not in validation_set]
    validation = [index for index, group in enumerate(normalized_groups) if group in validation_set]
    return (
        np.asarray(train, dtype=np.int64),
        np.asarray(validation, dtype=np.int64),
        {
            "strategy": "grouped",
            "train_groups": [group for group in ordered_groups if group not in validation_set],
            "validation_groups": validation_groups,
            "train_group_count": len([group for group in ordered_groups if group not in validation_set]),
            "validation_group_count": len(validation_groups),
        },
    )


def _standardize(matrix: np.ndarray) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    if matrix.size == 0:
        empty = np.asarray([], dtype=np.float64)
        return matrix, empty, empty
    means = matrix.mean(axis=0)
    stds = matrix.std(axis=0)
    stds = np.where(stds < 1e-6, 1.0, stds)
    return (matrix - means) / stds, means, stds


def _fit_logistic_regression(
    rows: np.ndarray,
    labels: np.ndarray,
    *,
    learning_rate: float = 0.12,
    epochs: int = 900,
    l2: float = 0.01,
) -> tuple[np.ndarray, float]:
    if rows.size == 0:
        return np.asarray([], dtype=np.float64), 0.0
    weights = np.zeros(rows.shape[1], dtype=np.float64)
    positive_rate = float(np.clip(labels.mean(), 1e-4, 1 - 1e-4))
    intercept = math.log(positive_rate / (1.0 - positive_rate))
    sample_count = float(len(labels))
    for _ in range(max(200, epochs)):
        logits = rows @ weights + intercept
        probs = _sigmoid(logits)
        errors = probs - labels
        grad_w = (rows.T @ errors) / sample_count + (l2 * weights)
        grad_b = float(errors.mean())
        weights -= learning_rate * grad_w
        intercept -= learning_rate * grad_b
    return weights, float(intercept)


def _fit_ridge_regression(
    rows: np.ndarray,
    labels: np.ndarray,
    *,
    l2: float = 0.05,
) -> tuple[np.ndarray, float]:
    if rows.size == 0:
        return np.asarray([], dtype=np.float64), 0.0
    augmented = np.hstack([rows, np.ones((rows.shape[0], 1), dtype=np.float64)])
    regularizer = np.eye(augmented.shape[1], dtype=np.float64) * float(l2)
    regularizer[-1, -1] = 0.0
    coefficients = np.linalg.pinv(augmented.T @ augmented + regularizer) @ augmented.T @ labels
    return coefficients[:-1], float(coefficients[-1])


def _classification_metrics(labels: np.ndarray, probs: np.ndarray) -> dict[str, float]:
    def _roc_auc_score(labels_array: np.ndarray, probs_array: np.ndarray) -> float:
        if labels_array.size == 0:
            return 0.5
        positives = labels_array >= 0.5
        negatives = labels_array < 0.5
        positive_count = int(positives.sum())
        negative_count = int(negatives.sum())
        if positive_count == 0 or negative_count == 0:
            return 0.5
        order = np.argsort(probs_array)
        ranks = np.empty_like(order, dtype=np.float64)
        ranks[order] = np.arange(1, len(probs_array) + 1, dtype=np.float64)
        positive_rank_sum = float(ranks[positives].sum())
        auc = (positive_rank_sum - (positive_count * (positive_count + 1) / 2.0)) / float(positive_count * negative_count)
        return max(0.0, min(1.0, auc))

    def _ece_score(labels_array: np.ndarray, probs_array: np.ndarray, bins: int = 10) -> float:
        if labels_array.size == 0:
            return 0.0
        edges = np.linspace(0.0, 1.0, bins + 1)
        total = float(len(labels_array))
        ece = 0.0
        for index in range(bins):
            lower = edges[index]
            upper = edges[index + 1]
            if index == bins - 1:
                mask = (probs_array >= lower) & (probs_array <= upper)
            else:
                mask = (probs_array >= lower) & (probs_array < upper)
            if not np.any(mask):
                continue
            bucket_probs = probs_array[mask]
            bucket_labels = labels_array[mask]
            ece += (len(bucket_probs) / total) * abs(float(bucket_probs.mean()) - float(bucket_labels.mean()))
        return ece

    if labels.size == 0:
        return {
            "accuracy": 0.0,
            "precision": 0.0,
            "recall": 0.0,
            "brier_score": 0.0,
            "roc_auc": 0.5,
            "ece": 0.0,
            "positive_rate": 0.0,
        }
    preds = (probs >= 0.5).astype(np.float64)
    accuracy = float((preds == labels).mean())
    true_positive = float(((preds == 1.0) & (labels == 1.0)).sum())
    predicted_positive = float((preds == 1.0).sum())
    actual_positive = float((labels == 1.0).sum())
    precision = 0.0 if predicted_positive <= 0 else true_positive / predicted_positive
    recall = 0.0 if actual_positive <= 0 else true_positive / actual_positive
    brier = float(np.mean((probs - labels) ** 2))
    roc_auc = _roc_auc_score(labels, probs)
    ece = _ece_score(labels, probs)
    return {
        "accuracy": round(accuracy, 4),
        "precision": round(precision, 4),
        "recall": round(recall, 4),
        "brier_score": round(brier, 4),
        "roc_auc": round(roc_auc, 4),
        "ece": round(ece, 4),
        "positive_rate": round(float(labels.mean()), 4),
    }


def _fit_platt_scaler(logits: np.ndarray, labels: np.ndarray) -> dict[str, float]:
    if logits.size == 0 or labels.size == 0 or len(np.unique(labels)) < 2:
        return {"method": "identity", "slope": 1.0, "intercept": 0.0}
    rows = logits.reshape(-1, 1)
    slope, intercept = _fit_logistic_regression(rows, labels, learning_rate=0.1, epochs=500, l2=0.01)
    slope_value = float(slope[0]) if slope.size else 1.0
    return {"method": "platt", "slope": slope_value, "intercept": float(intercept)}


def _apply_calibration(logits: np.ndarray, calibration: dict[str, Any] | None) -> np.ndarray:
    payload = calibration or {"method": "identity", "slope": 1.0, "intercept": 0.0}
    method = str(payload.get("method") or "identity")
    if method == "platt":
        slope = float(payload.get("slope") or 1.0)
        intercept = float(payload.get("intercept") or 0.0)
        return _sigmoid((logits * slope) + intercept)
    return _sigmoid(logits)


def _regression_metrics(labels: np.ndarray, preds: np.ndarray) -> dict[str, float]:
    if labels.size == 0:
        return {"mae": 0.0, "rmse": 0.0, "r2": 0.0}
    errors = preds - labels
    mae = float(np.mean(np.abs(errors)))
    rmse = float(np.sqrt(np.mean(errors**2)))
    total_sum_squares = float(np.sum((labels - labels.mean()) ** 2))
    residual_sum_squares = float(np.sum(errors**2))
    r2 = 0.0 if total_sum_squares <= 1e-9 else 1.0 - (residual_sum_squares / total_sum_squares)
    return {
        "mae": round(mae, 4),
        "rmse": round(rmse, 4),
        "r2": round(r2, 4),
    }


def _row_split(sample_count: int) -> tuple[np.ndarray, np.ndarray]:
    if sample_count <= 1:
        return np.asarray([0], dtype=np.int64), np.asarray([], dtype=np.int64)
    validation = sorted(set(range(0, sample_count, 4)))
    if not validation:
        validation = [sample_count - 1]
    train = [index for index in range(sample_count) if index not in validation]
    if not train:
        train = list(range(sample_count))
        validation = []
    return np.asarray(train, dtype=np.int64), np.asarray(validation, dtype=np.int64)


def _artifact_payload(
    *,
    model_name: str,
    task: str,
    feature_names: list[str],
    means: np.ndarray,
    stds: np.ndarray,
    weights: np.ndarray,
    intercept: float,
    metrics: dict[str, float],
    sample_count: int,
) -> dict[str, Any]:
    return {
        "model_name": model_name,
        "task": task,
        "kind": "logistic_regression",
        "feature_names": feature_names,
        "means": means.tolist(),
        "stds": stds.tolist(),
        "weights": weights.tolist(),
        "intercept": float(intercept),
        "metrics": metrics,
        "sample_count": int(sample_count),
    }


def _upsert_registry_record(
    *,
    model_name: str,
    task: str,
    artifact_path: Path,
    metrics: dict[str, Any],
    feature_schema: list[str],
) -> None:
    now = store._now_iso()
    conn = store._connect()
    try:
        conn.execute(
            """
            INSERT INTO model_registry
            (model_name, task_type, artifact_path, metrics_json, feature_schema_json, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(model_name) DO UPDATE SET
                task_type = excluded.task_type,
                artifact_path = excluded.artifact_path,
                metrics_json = excluded.metrics_json,
                feature_schema_json = excluded.feature_schema_json,
                updated_at = excluded.updated_at
            """,
            (
                model_name,
                task,
                str(artifact_path),
                json.dumps(metrics, ensure_ascii=False, sort_keys=True),
                json.dumps(feature_schema, ensure_ascii=False),
                now,
                now,
            ),
        )
        conn.commit()
    finally:
        conn.close()


def train_model(
    *,
    task: str,
    samples: list[dict[str, Any]],
    artifact_dir: Path,
    model_name: str | None = None,
) -> dict[str, Any]:
    init_modeling_db()
    resolved_name = model_name or task
    feature_names = _feature_names(samples)
    matrix, labels = _matrix_from_samples(samples, feature_names)
    artifact_dir.mkdir(parents=True, exist_ok=True)
    artifact_path = artifact_dir / f"{resolved_name}.json"

    if matrix.size == 0 or len(np.unique(labels)) < 2:
        means = np.zeros(matrix.shape[1] if matrix.ndim == 2 else 0, dtype=np.float64)
        stds = np.ones(matrix.shape[1] if matrix.ndim == 2 else 0, dtype=np.float64)
        positive_rate = float(labels.mean()) if labels.size else 0.0
        intercept = math.log(max(positive_rate, 1e-4) / max(1.0 - positive_rate, 1e-4)) if 0 < positive_rate < 1 else 0.0
        weights = np.zeros(matrix.shape[1] if matrix.ndim == 2 else 0, dtype=np.float64)
        probs = np.full(labels.shape, positive_rate, dtype=np.float64) if labels.size else np.asarray([], dtype=np.float64)
        metrics = _classification_metrics(labels, probs)
    else:
        groups = _sample_groups(samples)
        if any(groups):
            train_idx, val_idx, _split_metadata = _grouped_row_split(groups)
        else:
            train_idx, val_idx = _train_validation_split(labels)
        train_matrix = matrix[train_idx]
        train_labels = labels[train_idx]
        train_scaled, means, stds = _standardize(train_matrix)
        weights, intercept = _fit_logistic_regression(train_scaled, train_labels)
        train_probs = _sigmoid(train_scaled @ weights + intercept)
        metrics = {f"train_{key}": value for key, value in _classification_metrics(train_labels, train_probs).items()}
        if val_idx.size:
            val_scaled = (matrix[val_idx] - means) / stds
            val_probs = _sigmoid(val_scaled @ weights + intercept)
            validation_metrics = _classification_metrics(labels[val_idx], val_probs)
            for key, value in validation_metrics.items():
                metrics[f"validation_{key}"] = value
            metrics["brier_score"] = validation_metrics["brier_score"]
        else:
            metrics["validation_accuracy"] = metrics["train_accuracy"]
            metrics["validation_precision"] = metrics["train_precision"]
            metrics["validation_recall"] = metrics["train_recall"]
            metrics["validation_brier_score"] = metrics["train_brier_score"]
            metrics["brier_score"] = metrics["train_brier_score"]

    artifact = _artifact_payload(
        model_name=resolved_name,
        task=task,
        feature_names=feature_names,
        means=means,
        stds=stds,
        weights=weights,
        intercept=intercept,
        metrics=metrics,
        sample_count=len(samples),
    )
    artifact_path.write_text(json.dumps(artifact, ensure_ascii=False, sort_keys=True), encoding="utf-8")
    _upsert_registry_record(
        model_name=resolved_name,
        task=task,
        artifact_path=artifact_path,
        metrics=metrics,
        feature_schema=feature_names,
    )

    return {
        "task": task,
        "model_name": resolved_name,
        "artifact_path": str(artifact_path),
        "metrics": metrics,
        "feature_names": feature_names,
        "trained": True,
        "sample_count": len(samples),
    }


def train_multi_head_model(
    *,
    task: str,
    samples: list[dict[str, Any]],
    artifact_dir: Path,
    model_name: str | None = None,
    metadata: dict[str, Any] | None = None,
    head_types: dict[str, str] | None = None,
) -> dict[str, Any]:
    init_modeling_db()
    resolved_name = model_name or task
    if not samples:
        return {
            "task": task,
            "model_name": resolved_name,
            "artifact_path": "",
            "metrics": {},
            "feature_names": [],
            "heads": [],
            "trained": False,
            "sample_count": 0,
            "reason": "No samples provided.",
        }
    head_names = sorted(
        {
            str(head_name)
            for sample in samples
            for head_name in ((sample.get("labels") or {}).keys())
            if str(head_name).strip()
        }
    )
    feature_names = _feature_names(samples)
    matrix = _feature_matrix(samples, feature_names)
    groups = _sample_groups(samples)
    if any(groups):
        train_idx, val_idx, split_metadata = _grouped_row_split(groups)
    else:
        train_idx, val_idx = _row_split(len(samples))
        split_metadata = {
            "strategy": "row",
            "train_groups": [],
            "validation_groups": [],
            "train_group_count": 0,
            "validation_group_count": 0,
        }
    train_scaled, means, stds = _standardize(matrix[train_idx] if train_idx.size else matrix)
    val_scaled = (matrix[val_idx] - means) / stds if val_idx.size else np.asarray([], dtype=np.float64)

    artifact_heads: dict[str, Any] = {}
    summary_metrics: dict[str, Any] = {}
    for head_name in head_names:
        head_kind = str((head_types or {}).get(head_name) or "regression").strip().lower() or "regression"
        labels = np.asarray([float((sample.get("labels") or {}).get(head_name) or 0.0) for sample in samples], dtype=np.float64)
        train_labels = labels[train_idx] if train_idx.size else labels
        if head_kind == "classification":
            calibration = {"method": "identity", "slope": 1.0, "intercept": 0.0}
            if train_scaled.size == 0 or train_labels.size == 0 or len(np.unique(train_labels)) < 2:
                weights = np.zeros(train_scaled.shape[1] if train_scaled.ndim == 2 else 0, dtype=np.float64)
                positive_rate = float(train_labels.mean()) if train_labels.size else 0.0
                intercept = (
                    math.log(max(positive_rate, 1e-4) / max(1.0 - positive_rate, 1e-4))
                    if 0 < positive_rate < 1
                    else 0.0
                )
                train_probs = np.full(train_labels.shape, positive_rate, dtype=np.float64) if train_labels.size else np.asarray([], dtype=np.float64)
                metrics = {f"train_{key}": value for key, value in _classification_metrics(train_labels, train_probs).items()}
                if val_idx.size:
                    val_probs = np.full(labels[val_idx].shape, positive_rate, dtype=np.float64)
                    validation_metrics = _classification_metrics(labels[val_idx], val_probs)
                    for key, value in validation_metrics.items():
                        metrics[f"validation_{key}"] = value
                else:
                    metrics["validation_accuracy"] = metrics.get("train_accuracy", 0.0)
                    metrics["validation_precision"] = metrics.get("train_precision", 0.0)
                    metrics["validation_recall"] = metrics.get("train_recall", 0.0)
                    metrics["validation_brier_score"] = metrics.get("train_brier_score", 0.0)
            else:
                weights, intercept = _fit_logistic_regression(train_scaled, train_labels)
                train_logits = train_scaled @ weights + intercept
                train_probs = _sigmoid(train_logits)
                metrics = {f"train_{key}": value for key, value in _classification_metrics(train_labels, train_probs).items()}
                if val_idx.size:
                    val_logits = val_scaled @ weights + intercept
                    calibration = _fit_platt_scaler(val_logits, labels[val_idx])
                    val_probs = _apply_calibration(val_logits, calibration)
                    validation_metrics = _classification_metrics(labels[val_idx], val_probs)
                    for key, value in validation_metrics.items():
                        metrics[f"validation_{key}"] = value
                else:
                    metrics["validation_accuracy"] = metrics["train_accuracy"]
                    metrics["validation_precision"] = metrics["train_precision"]
                    metrics["validation_recall"] = metrics["train_recall"]
                    metrics["validation_brier_score"] = metrics["train_brier_score"]
        else:
            if train_scaled.size == 0 or train_labels.size == 0:
                weights = np.asarray([], dtype=np.float64)
                intercept = 0.0
                train_preds = np.asarray([], dtype=np.float64)
                metrics = _regression_metrics(train_labels, train_preds)
                if not metrics:
                    metrics = {"mae": 0.0, "rmse": 0.0, "r2": 0.0}
            else:
                weights, intercept = _fit_ridge_regression(train_scaled, train_labels)
                train_preds = train_scaled @ weights + intercept
                metrics = {f"train_{key}": value for key, value in _regression_metrics(train_labels, train_preds).items()}
                if val_idx.size:
                    val_preds = val_scaled @ weights + intercept
                    validation_metrics = _regression_metrics(labels[val_idx], val_preds)
                    for key, value in validation_metrics.items():
                        metrics[f"validation_{key}"] = value
                    metrics["rmse"] = validation_metrics["rmse"]
                else:
                    metrics["validation_mae"] = metrics["train_mae"]
                    metrics["validation_rmse"] = metrics["train_rmse"]
                    metrics["validation_r2"] = metrics["train_r2"]
                    metrics["rmse"] = metrics["train_rmse"]
        artifact_heads[head_name] = {
            "kind": head_kind,
            "weights": weights.tolist(),
            "intercept": float(intercept),
            "metrics": metrics,
            "label_mean": round(float(labels.mean()) if labels.size else 0.0, 6),
            "calibration": calibration if head_kind == "classification" else None,
        }
        summary_metrics[head_name] = metrics

    artifact_dir.mkdir(parents=True, exist_ok=True)
    artifact_path = artifact_dir / f"{resolved_name}.json"
    created_at = store._now_iso()
    artifact = {
        "model_name": resolved_name,
        "task": task,
        "kind": "multi_head_linear",
        "feature_names": feature_names,
        "means": means.tolist(),
        "stds": stds.tolist(),
        "heads": artifact_heads,
        "sample_count": len(samples),
        "created_at": created_at,
        "metadata": {"split_strategy": split_metadata["strategy"], "split": split_metadata} | (metadata or {}),
    }
    artifact_path.write_text(json.dumps(artifact, ensure_ascii=False, sort_keys=True), encoding="utf-8")
    _upsert_registry_record(
        model_name=resolved_name,
        task=task,
        artifact_path=artifact_path,
        metrics=summary_metrics,
        feature_schema=feature_names,
    )
    return {
        "task": task,
        "model_name": resolved_name,
        "artifact_path": str(artifact_path),
        "metrics": summary_metrics,
        "feature_names": feature_names,
        "heads": head_names,
        "trained": True,
        "sample_count": len(samples),
    }


def get_model(model_name: str) -> dict[str, Any] | None:
    init_modeling_db()
    conn = store._connect()
    try:
        row = conn.execute("SELECT * FROM model_registry WHERE model_name = ?", (model_name,)).fetchone()
    finally:
        conn.close()
    if row is None:
        return None
    result = dict(row)
    result["metrics"] = json.loads(result.pop("metrics_json") or "{}")
    result["feature_names"] = json.loads(result.pop("feature_schema_json") or "[]")
    artifact_path = Path(result["artifact_path"])
    if artifact_path.exists():
        artifact = json.loads(artifact_path.read_text(encoding="utf-8"))
        result.update(artifact)
    return result


def predict_probability(model_name: str, features: dict[str, float]) -> float:
    model = get_model(model_name)
    if not model:
        raise ValueError(f"Model not found: {model_name}")
    feature_names = [str(name) for name in model.get("feature_names") or []]
    vector = np.asarray([float(features.get(name) or 0.0) for name in feature_names], dtype=np.float64)
    means = np.asarray(model.get("means") or [0.0] * len(feature_names), dtype=np.float64)
    stds = np.asarray(model.get("stds") or [1.0] * len(feature_names), dtype=np.float64)
    stds = np.where(stds < 1e-6, 1.0, stds)
    normalized = (vector - means) / stds if feature_names else np.asarray([], dtype=np.float64)
    weights = np.asarray(model.get("weights") or [0.0] * len(feature_names), dtype=np.float64)
    intercept = float(model.get("intercept") or 0.0)
    score = float(_sigmoid(np.asarray([normalized @ weights + intercept], dtype=np.float64))[0])
    return round(score, 6)
