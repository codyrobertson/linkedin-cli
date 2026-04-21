"""Persisted local vector retrieval index helpers."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import numpy as np

try:
    import hnswlib  # type: ignore
except Exception:  # pragma: no cover - optional backend
    hnswlib = None


def _normalize_rows(matrix: np.ndarray) -> np.ndarray:
    if matrix.size == 0:
        return matrix
    norms = np.linalg.norm(matrix, axis=1, keepdims=True)
    norms = np.where(norms <= 1e-12, 1.0, norms)
    return matrix / norms


def _normalize_vector(vector: np.ndarray) -> np.ndarray:
    if vector.size == 0:
        return vector
    norm = np.linalg.norm(vector)
    if norm <= 1e-12:
        return vector
    return vector / norm


def build_index(
    items: list[dict[str, Any]],
    *,
    index_name: str = "default",
    metric: str = "cosine",
    engine: str | None = None,
) -> dict[str, Any]:
    if not items:
        return {
            "index_name": index_name,
            "engine": "numpy-exact",
            "metric": metric,
            "count": 0,
            "dimension": 0,
            "ids": [],
            "vectors": np.zeros((0, 0), dtype=np.float32),
        }
    ids = [str(item["id"]) for item in items]
    vectors = np.asarray([[float(value) for value in item.get("vector") or []] for item in items], dtype=np.float32)
    if vectors.ndim != 2 or vectors.shape[1] == 0:
        raise ValueError("Index items must include non-empty fixed-width vectors")
    if len({tuple(row.shape) for row in vectors}) != 1:
        raise ValueError("All retrieval vectors must have the same dimensionality")
    normalized = _normalize_rows(vectors) if metric == "cosine" else vectors
    resolved_engine = "hnswlib" if engine == "hnswlib" and hnswlib is not None else "numpy-exact"
    index: dict[str, Any] = {
        "index_name": index_name,
        "engine": resolved_engine,
        "metric": metric,
        "count": int(len(ids)),
        "dimension": int(normalized.shape[1]),
        "ids": ids,
        "vectors": normalized,
    }
    if resolved_engine == "hnswlib":
        ann = hnswlib.Index(space="cosine", dim=int(normalized.shape[1]))
        ann.init_index(max_elements=len(ids), ef_construction=max(100, len(ids)), M=16)
        ann.add_items(normalized, np.arange(len(ids)))
        ann.set_ef(max(50, min(200, len(ids))))
        index["_hnsw_index"] = ann
    return index


def query(index: dict[str, Any], vector: list[float], limit: int = 10) -> list[dict[str, Any]]:
    count = int(index.get("count") or 0)
    if count <= 0:
        return []
    query_vector = np.asarray([float(value) for value in vector], dtype=np.float32)
    if int(index.get("dimension") or 0) != int(query_vector.shape[0]):
        return []
    if index.get("metric") == "cosine":
        query_vector = _normalize_vector(query_vector)
    limit = max(1, min(int(limit), count))
    if index.get("engine") == "hnswlib" and index.get("_hnsw_index") is not None:
        labels, distances = index["_hnsw_index"].knn_query(query_vector, k=limit)
        rows: list[dict[str, Any]] = []
        for label, distance in zip(labels[0].tolist(), distances[0].tolist()):
            rows.append({"id": index["ids"][int(label)], "score": round(1.0 - float(distance), 6)})
        return rows
    matrix = np.asarray(index.get("vectors"), dtype=np.float32)
    scores = matrix @ query_vector
    order = np.argsort(-scores)[:limit]
    return [{"id": index["ids"][int(position)], "score": round(float(scores[int(position)]), 6)} for position in order]


def save_index(index: dict[str, Any], path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    manifest = {
        "index_name": index.get("index_name") or "default",
        "engine": index.get("engine") or "numpy-exact",
        "metric": index.get("metric") or "cosine",
        "count": int(index.get("count") or 0),
        "dimension": int(index.get("dimension") or 0),
    }
    (path / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, sort_keys=True), encoding="utf-8")
    (path / "ids.json").write_text(json.dumps(index.get("ids") or [], ensure_ascii=False), encoding="utf-8")
    np.save(path / "vectors.npy", np.asarray(index.get("vectors"), dtype=np.float32))
    if manifest["engine"] == "hnswlib" and index.get("_hnsw_index") is not None:
        index["_hnsw_index"].save_index(str(path / "index.bin"))
    return path


def load_index(path: Path) -> dict[str, Any]:
    manifest = json.loads((path / "manifest.json").read_text(encoding="utf-8"))
    ids = json.loads((path / "ids.json").read_text(encoding="utf-8"))
    vectors = np.load(path / "vectors.npy")
    index: dict[str, Any] = {
        "index_name": manifest.get("index_name") or "default",
        "engine": manifest.get("engine") or "numpy-exact",
        "metric": manifest.get("metric") or "cosine",
        "count": int(manifest.get("count") or len(ids)),
        "dimension": int(manifest.get("dimension") or (vectors.shape[1] if vectors.ndim == 2 else 0)),
        "ids": ids,
        "vectors": np.asarray(vectors, dtype=np.float32),
    }
    if index["engine"] == "hnswlib" and hnswlib is not None and (path / "index.bin").exists():
        ann = hnswlib.Index(space="cosine", dim=index["dimension"])
        ann.load_index(str(path / "index.bin"))
        ann.set_ef(max(50, min(200, index["count"])))
        index["_hnsw_index"] = ann
    elif index["engine"] == "hnswlib":
        index["engine"] = "numpy-exact"
    return index


def export_jsonl(items: list[dict[str, Any]], path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = []
    for item in items:
        lines.append(
            json.dumps(
                {
                    "id": str(item["id"]),
                    "vector": [float(value) for value in item.get("vector") or []],
                    "payload": item.get("payload") or {},
                },
                ensure_ascii=False,
                sort_keys=True,
            )
        )
    path.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")
    return path
