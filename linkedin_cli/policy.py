"""Local policy selection, logging, and offline evaluation helpers."""

from __future__ import annotations

import json
import math
import uuid
from typing import Any

import numpy as np

from linkedin_cli.session import ExitCode, fail
from linkedin_cli.write import store


DEFAULT_POLICY_TYPE = "linucb"


def _ensure_column(conn: Any, table: str, column: str, ddl: str) -> None:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    existing = {row["name"] if hasattr(row, "keys") else row[1] for row in rows}
    if column not in existing:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {ddl}")


def init_policy_db() -> None:
    conn = store._connect()
    try:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS content_policies (
                policy_name TEXT PRIMARY KEY,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                policy_type TEXT NOT NULL,
                alpha REAL NOT NULL DEFAULT 0.2,
                ridge REAL NOT NULL DEFAULT 0.01,
                feature_dim INTEGER NOT NULL DEFAULT 0,
                actions_json TEXT NOT NULL DEFAULT '[]',
                parameters_json TEXT NOT NULL DEFAULT '{}',
                metrics_json TEXT NOT NULL DEFAULT '{}',
                metadata_json TEXT NOT NULL DEFAULT '{}'
            );

            CREATE TABLE IF NOT EXISTS policy_decisions (
                decision_id TEXT PRIMARY KEY,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                policy_name TEXT NOT NULL,
                context_type TEXT NOT NULL,
                context_key TEXT NOT NULL,
                context_features_json TEXT NOT NULL DEFAULT '[]',
                chosen_action_id TEXT NOT NULL,
                chosen_score REAL NOT NULL DEFAULT 0,
                propensity REAL NOT NULL DEFAULT 0,
                available_actions_json TEXT NOT NULL DEFAULT '[]',
                metadata_json TEXT NOT NULL DEFAULT '{}'
            );

            CREATE INDEX IF NOT EXISTS idx_policy_decisions_policy
                ON policy_decisions(policy_name, context_type, created_at DESC);

            CREATE TABLE IF NOT EXISTS policy_rewards (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                decision_id TEXT NOT NULL,
                reward_type TEXT NOT NULL,
                reward_value REAL NOT NULL,
                event_time TEXT NOT NULL,
                dedupe_key TEXT,
                trace_id TEXT,
                window_id TEXT,
                reward_source TEXT,
                payload_json TEXT NOT NULL DEFAULT '{}',
                FOREIGN KEY (decision_id) REFERENCES policy_decisions(decision_id) ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS idx_policy_rewards_decision
                ON policy_rewards(decision_id, event_time DESC);
            """
        )
        _ensure_column(conn, "policy_rewards", "dedupe_key", "TEXT")
        _ensure_column(conn, "policy_rewards", "trace_id", "TEXT")
        _ensure_column(conn, "policy_rewards", "window_id", "TEXT")
        _ensure_column(conn, "policy_rewards", "reward_source", "TEXT")
        conn.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_policy_rewards_dedupe
                ON policy_rewards(dedupe_key)
            """
        )
        conn.commit()
    finally:
        conn.close()


def _softmax(values: list[float]) -> list[float]:
    if not values:
        return []
    max_value = max(values)
    exps = [math.exp(value - max_value) for value in values]
    denom = sum(exps) or 1.0
    return [value / denom for value in exps]


def _sanitize_actions(actions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not actions:
        fail("Policy actions are required", code=ExitCode.VALIDATION)
    sanitized: list[dict[str, Any]] = []
    feature_dim: int | None = None
    for action in actions:
        action_id = str(action.get("action_id") or "").strip()
        if not action_id:
            fail("Each policy action requires action_id", code=ExitCode.VALIDATION)
        features = [float(value) for value in (action.get("features") or [])]
        if feature_dim is None:
            feature_dim = len(features)
        if feature_dim != len(features):
            fail("All policy actions must share the same feature dimension", code=ExitCode.VALIDATION)
        sanitized.append(
            {
                "action_id": action_id,
                "label": action.get("label") or action_id,
                "features": features,
                "score": float(action.get("score") or 0.0),
                "metadata": action.get("metadata") or {},
            }
        )
    return sanitized


def _decision_row_to_dict(row: Any) -> dict[str, Any]:
    item = dict(row)
    item["context_features"] = json.loads(item.pop("context_features_json") or "[]")
    item["available_actions"] = json.loads(item.pop("available_actions_json") or "[]")
    item["metadata"] = json.loads(item.pop("metadata_json") or "{}")
    return item


def _policy_row_to_dict(row: Any) -> dict[str, Any]:
    item = dict(row)
    item["actions"] = json.loads(item.pop("actions_json") or "[]")
    item["parameters"] = json.loads(item.pop("parameters_json") or "{}")
    item["metrics"] = json.loads(item.pop("metrics_json") or "{}")
    item["metadata"] = json.loads(item.pop("metadata_json") or "{}")
    return item


def get_policy(policy_name: str) -> dict[str, Any] | None:
    init_policy_db()
    conn = store._connect()
    try:
        row = conn.execute("SELECT * FROM content_policies WHERE policy_name = ?", (policy_name,)).fetchone()
        return _policy_row_to_dict(row) if row is not None else None
    finally:
        conn.close()


def log_policy_decision(
    *,
    policy_name: str,
    context_type: str,
    context_key: str,
    context_features: list[float],
    available_actions: list[dict[str, Any]],
    chosen_action_id: str,
    chosen_score: float,
    propensity: float,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    init_policy_db()
    decision_id = f"pd_{uuid.uuid4().hex[:16]}"
    now = store._now_iso()
    conn = store._connect()
    try:
        conn.execute(
            """
            INSERT INTO policy_decisions
            (decision_id, created_at, updated_at, policy_name, context_type, context_key, context_features_json,
             chosen_action_id, chosen_score, propensity, available_actions_json, metadata_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                decision_id,
                now,
                now,
                policy_name,
                context_type,
                context_key,
                json.dumps([float(value) for value in context_features], ensure_ascii=False, sort_keys=True),
                chosen_action_id,
                float(chosen_score),
                float(propensity),
                json.dumps(available_actions, ensure_ascii=False, sort_keys=True),
                json.dumps(metadata or {}, ensure_ascii=False, sort_keys=True),
            ),
        )
        conn.commit()
        row = conn.execute("SELECT * FROM policy_decisions WHERE decision_id = ?", (decision_id,)).fetchone()
        assert row is not None
        return _decision_row_to_dict(row)
    finally:
        conn.close()


def get_policy_decision(decision_id: str) -> dict[str, Any]:
    init_policy_db()
    conn = store._connect()
    try:
        row = conn.execute("SELECT * FROM policy_decisions WHERE decision_id = ?", (decision_id,)).fetchone()
        if row is None:
            fail(f"Policy decision not found: {decision_id}", code=ExitCode.NOT_FOUND)
        return _decision_row_to_dict(row)
    finally:
        conn.close()


def record_reward(
    decision_id: str,
    *,
    reward_type: str,
    reward_value: float,
    payload: dict[str, Any] | None = None,
    dedupe_key: str | None = None,
    trace_id: str | None = None,
    window_id: str | None = None,
    reward_source: str | None = None,
    event_time: str | None = None,
) -> dict[str, Any]:
    init_policy_db()
    now = event_time or store._now_iso()
    conn = store._connect()
    try:
        exists = conn.execute("SELECT 1 FROM policy_decisions WHERE decision_id = ?", (decision_id,)).fetchone()
        if exists is None:
            fail(f"Policy decision not found: {decision_id}", code=ExitCode.NOT_FOUND)
        conn.execute(
            """
            INSERT INTO policy_rewards
            (decision_id, reward_type, reward_value, event_time, dedupe_key, trace_id, window_id, reward_source, payload_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(dedupe_key) DO NOTHING
            """,
            (
                decision_id,
                reward_type,
                float(reward_value),
                now,
                dedupe_key,
                trace_id,
                window_id,
                reward_source,
                json.dumps(payload or {}, ensure_ascii=False, sort_keys=True),
            ),
        )
        conn.commit()
        if dedupe_key:
            row = conn.execute("SELECT * FROM policy_rewards WHERE dedupe_key = ?", (dedupe_key,)).fetchone()
        else:
            row = conn.execute("SELECT * FROM policy_rewards WHERE decision_id = ? ORDER BY id DESC LIMIT 1", (decision_id,)).fetchone()
        assert row is not None
        item = dict(row)
        item["payload"] = json.loads(item.pop("payload_json") or "{}")
        return item
    finally:
        conn.close()


def _policy_scores_for_actions(
    *,
    policy_name: str,
    actions: list[dict[str, Any]],
    alpha: float,
) -> list[dict[str, Any]]:
    policy = get_policy(policy_name)
    if not policy:
        return [
            {
                "action_id": action["action_id"],
                "score": float(action.get("score") or 0.0),
                "ucb_score": float(action.get("score") or 0.0),
                "features": action["features"],
            }
            for action in actions
        ]

    parameters = policy.get("parameters") or {}
    scored: list[dict[str, Any]] = []
    for action in actions:
        feature_vector = np.array(action["features"], dtype=float)
        action_params = parameters.get(action["action_id"]) or {}
        theta = np.array(action_params.get("theta") or [0.0] * len(feature_vector), dtype=float)
        a_inv = np.array(action_params.get("a_inv") or np.eye(len(feature_vector)).tolist(), dtype=float)
        mean_score = float(theta @ feature_vector)
        bonus = float(alpha * math.sqrt(max(0.0, float(feature_vector @ a_inv @ feature_vector))))
        scored.append(
            {
                "action_id": action["action_id"],
                "score": mean_score,
                "ucb_score": mean_score + bonus,
                "features": action["features"],
            }
        )
    return scored


def choose_action_linucb(
    *,
    policy_name: str,
    context_type: str,
    context_key: str,
    context_features: list[float],
    actions: list[dict[str, Any]],
    alpha: float = 0.2,
    log_decision: bool = False,
    force_action_id: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    init_policy_db()
    sanitized_actions = _sanitize_actions(actions)
    scored_actions = _policy_scores_for_actions(policy_name=policy_name, actions=sanitized_actions, alpha=alpha)
    probabilities = _softmax([float(action["ucb_score"]) for action in scored_actions])
    for action, probability in zip(scored_actions, probabilities):
        action["propensity"] = float(probability)
    chosen = next((action for action in scored_actions if action["action_id"] == force_action_id), None)
    if chosen is None:
        chosen = max(scored_actions, key=lambda item: (float(item["ucb_score"]), float(item["score"])))
    decision = {
        "policy_name": policy_name,
        "context_type": context_type,
        "context_key": context_key,
        "context_features": [float(value) for value in context_features],
        "chosen_action_id": chosen["action_id"],
        "chosen_score": float(chosen["ucb_score"]),
        "propensity": float(chosen["propensity"]),
        "available_actions": scored_actions,
    }
    if log_decision:
        logged = log_policy_decision(
            policy_name=policy_name,
            context_type=context_type,
            context_key=context_key,
            context_features=[float(value) for value in context_features],
            available_actions=scored_actions,
            chosen_action_id=chosen["action_id"],
            chosen_score=float(chosen["ucb_score"]),
            propensity=float(chosen["propensity"]),
            metadata=metadata,
        )
        decision["decision_id"] = logged["decision_id"]
    return decision


def _reward_totals(*, policy_name: str | None = None, context_type: str | None = None) -> list[dict[str, Any]]:
    init_policy_db()
    conn = store._connect()
    try:
        where: list[str] = []
        params: list[Any] = []
        if policy_name:
            where.append("pd.policy_name = ?")
            params.append(policy_name)
        if context_type:
            where.append("pd.context_type = ?")
            params.append(context_type)
        where_sql = f"WHERE {' AND '.join(where)}" if where else ""
        rows = conn.execute(
            f"""
            SELECT
                pd.*,
                COALESCE(SUM(pr.reward_value), 0) AS total_reward,
                COUNT(pr.id) AS reward_count
            FROM policy_decisions pd
            LEFT JOIN policy_rewards pr ON pr.decision_id = pd.decision_id
            {where_sql}
            GROUP BY pd.decision_id
            ORDER BY pd.created_at ASC
            """,
            params,
        ).fetchall()
        results: list[dict[str, Any]] = []
        for row in rows:
            item = _decision_row_to_dict(row)
            item["total_reward"] = float(row["total_reward"] or 0.0)
            item["reward_count"] = int(row["reward_count"] or 0)
            results.append(item)
        return results
    finally:
        conn.close()


def train_policy(
    *,
    policy_name: str,
    context_type: str | None = None,
    min_samples: int = 25,
    alpha: float = 0.2,
    ridge: float = 0.01,
) -> dict[str, Any]:
    decisions = [item for item in _reward_totals(policy_name=policy_name, context_type=context_type) if item["reward_count"] > 0]
    if len(decisions) < min_samples:
        return {
            "trained": False,
            "policy_name": policy_name,
            "sample_count": len(decisions),
            "reason": f"Need at least {min_samples} rewarded decisions",
        }
    grouped: dict[str, list[tuple[list[float], float]]] = {}
    feature_dim = 0
    for item in decisions:
        chosen_action_id = str(item["chosen_action_id"])
        chosen_action = next((action for action in item["available_actions"] if action.get("action_id") == chosen_action_id), None)
        if not chosen_action:
            continue
        features = [float(value) for value in (chosen_action.get("features") or [])]
        feature_dim = max(feature_dim, len(features))
        grouped.setdefault(chosen_action_id, []).append((features, float(item["total_reward"])))
    now = store._now_iso()
    actions = sorted(grouped)
    parameters: dict[str, Any] = {}
    action_metrics: dict[str, Any] = {}
    for action_id, rows in grouped.items():
        x = np.array([features for features, _reward in rows], dtype=float)
        y = np.array([reward for _features, reward in rows], dtype=float)
        a = x.T @ x + (ridge * np.eye(x.shape[1]))
        b = x.T @ y
        theta = np.linalg.solve(a, b)
        prediction = x @ theta
        mae = float(np.mean(np.abs(prediction - y)))
        rmse = float(np.sqrt(np.mean((prediction - y) ** 2)))
        parameters[action_id] = {
            "theta": theta.tolist(),
            "a_inv": np.linalg.inv(a).tolist(),
            "sample_count": int(len(rows)),
        }
        action_metrics[action_id] = {"mae": round(mae, 4), "rmse": round(rmse, 4), "sample_count": int(len(rows))}
    conn = store._connect()
    try:
        existing = conn.execute("SELECT created_at FROM content_policies WHERE policy_name = ?", (policy_name,)).fetchone()
        created_at = existing["created_at"] if existing else now
        conn.execute(
            """
            INSERT INTO content_policies
            (policy_name, created_at, updated_at, policy_type, alpha, ridge, feature_dim, actions_json, parameters_json, metrics_json, metadata_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(policy_name) DO UPDATE SET
                updated_at = excluded.updated_at,
                policy_type = excluded.policy_type,
                alpha = excluded.alpha,
                ridge = excluded.ridge,
                feature_dim = excluded.feature_dim,
                actions_json = excluded.actions_json,
                parameters_json = excluded.parameters_json,
                metrics_json = excluded.metrics_json,
                metadata_json = excluded.metadata_json
            """,
            (
                policy_name,
                created_at,
                now,
                DEFAULT_POLICY_TYPE,
                float(alpha),
                float(ridge),
                int(feature_dim),
                json.dumps(actions, ensure_ascii=False, sort_keys=True),
                json.dumps(parameters, ensure_ascii=False, sort_keys=True),
                json.dumps(action_metrics, ensure_ascii=False, sort_keys=True),
                json.dumps({"context_type": context_type}, ensure_ascii=False, sort_keys=True),
            ),
        )
        conn.commit()
    finally:
        conn.close()
    return {
        "trained": True,
        "policy_name": policy_name,
        "sample_count": len(decisions),
        "actions": actions,
        "feature_dim": feature_dim,
        "metrics": action_metrics,
        "policy": get_policy(policy_name),
    }


def evaluate_policy_ips(*, policy_name: str, context_type: str | None = None) -> dict[str, Any]:
    decisions = [item for item in _reward_totals(policy_name=policy_name, context_type=context_type) if item["reward_count"] > 0]
    if not decisions:
        return {"estimate": 0.0, "matched_count": 0, "sample_count": 0}
    weighted_sum = 0.0
    matched_count = 0
    for item in decisions:
        replay = choose_action_linucb(
            policy_name=policy_name,
            context_type=item["context_type"],
            context_key=item["context_key"],
            context_features=item["context_features"],
            actions=item["available_actions"],
            alpha=float((get_policy(policy_name) or {}).get("alpha") or 0.2),
            log_decision=False,
        )
        target = next((action for action in replay["available_actions"] if action["action_id"] == replay["chosen_action_id"]), None)
        if target and replay["chosen_action_id"] == item["chosen_action_id"] and float(item["propensity"] or 0.0) > 0:
            matched_count += 1
            weighted_sum += float(item["total_reward"]) * (float(target["propensity"]) / float(item["propensity"]))
    return {
        "estimate": round(weighted_sum / max(1, len(decisions)), 6),
        "matched_count": matched_count,
        "sample_count": len(decisions),
    }


def evaluate_policy_snips(*, policy_name: str, context_type: str | None = None) -> dict[str, Any]:
    decisions = [item for item in _reward_totals(policy_name=policy_name, context_type=context_type) if item["reward_count"] > 0]
    if not decisions:
        return {"estimate": 0.0, "matched_count": 0, "sample_count": 0}
    numerator = 0.0
    denominator = 0.0
    matched_count = 0
    for item in decisions:
        replay = choose_action_linucb(
            policy_name=policy_name,
            context_type=item["context_type"],
            context_key=item["context_key"],
            context_features=item["context_features"],
            actions=item["available_actions"],
            alpha=float((get_policy(policy_name) or {}).get("alpha") or 0.2),
            log_decision=False,
        )
        target = next((action for action in replay["available_actions"] if action["action_id"] == replay["chosen_action_id"]), None)
        if target and replay["chosen_action_id"] == item["chosen_action_id"] and float(item["propensity"] or 0.0) > 0:
            matched_count += 1
            weight = float(target["propensity"]) / float(item["propensity"])
            numerator += weight * float(item["total_reward"])
            denominator += weight
    return {
        "estimate": round((numerator / denominator) if denominator > 0 else 0.0, 6),
        "matched_count": matched_count,
        "sample_count": len(decisions),
    }


def policy_report(*, policy_name: str, context_type: str | None = None) -> dict[str, Any]:
    decisions = _reward_totals(policy_name=policy_name, context_type=context_type)
    reward_count = sum(int(item["reward_count"]) for item in decisions)
    avg_reward = (
        round(sum(float(item["total_reward"]) for item in decisions) / len([item for item in decisions if item["reward_count"] > 0]), 6)
        if any(item["reward_count"] > 0 for item in decisions)
        else 0.0
    )
    by_action: dict[str, int] = {}
    for item in decisions:
        action_id = str(item["chosen_action_id"])
        by_action[action_id] = by_action.get(action_id, 0) + 1
    return {
        "policy": get_policy(policy_name),
        "decision_count": len(decisions),
        "reward_count": reward_count,
        "average_reward": avg_reward,
        "by_action": by_action,
        "offline_eval": {
            "ips": evaluate_policy_ips(policy_name=policy_name, context_type=context_type),
            "snips": evaluate_policy_snips(policy_name=policy_name, context_type=context_type),
        },
    }
