from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
import json


class RetrievalIndexTests(unittest.TestCase):
    def setUp(self) -> None:
        from linkedin_cli import retrieval_index

        self.retrieval_index = retrieval_index
        self.tempdir = tempfile.TemporaryDirectory()
        self.index_path = Path(self.tempdir.name) / "content-index"

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def test_build_index_returns_candidate_neighbors(self) -> None:
        items = [
            {"id": "a", "vector": [1.0, 0.0, 0.0]},
            {"id": "b", "vector": [0.9, 0.1, 0.0]},
            {"id": "c", "vector": [0.0, 1.0, 0.0]},
        ]
        index = self.retrieval_index.build_index(items, index_name="content-semantic")

        results = self.retrieval_index.query(index, [1.0, 0.0, 0.0], limit=2)

        self.assertEqual(len(results), 2)
        self.assertEqual(results[0]["id"], "a")
        self.assertIn(index["engine"], {"numpy-exact", "hnswlib"})
        self.assertEqual(index["count"], 3)
        self.assertEqual(index["dimension"], 3)

    def test_save_and_load_index_round_trip(self) -> None:
        items = [{"id": "a", "vector": [1.0, 0.0, 0.0]}]
        index = self.retrieval_index.build_index(items, index_name="content-semantic")
        self.retrieval_index.save_index(index, self.index_path)
        loaded = self.retrieval_index.load_index(self.index_path)

        results = self.retrieval_index.query(loaded, [1.0, 0.0, 0.0], limit=1)
        self.assertEqual(results[0]["id"], "a")
        self.assertEqual(loaded["count"], 1)
        self.assertEqual(loaded["index_name"], "content-semantic")
        self.assertTrue((self.index_path / "manifest.json").exists())
        self.assertTrue((self.index_path / "vectors.npy").exists())

    def test_export_jsonl_writes_external_ann_payload(self) -> None:
        items = [
            {
                "id": "post-1",
                "vector": [1.0, 0.0, 0.0],
                "payload": {
                    "url": "https://www.linkedin.com/posts/post-1",
                    "industries": ["ai", "fintech"],
                    "updated_at": "2026-03-26T00:00:00Z",
                },
            }
        ]

        export_path = self.retrieval_index.export_jsonl(items, self.index_path / "semantic.jsonl")

        rows = [json.loads(line) for line in export_path.read_text(encoding="utf-8").splitlines() if line.strip()]
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["id"], "post-1")
        self.assertEqual(rows[0]["vector"], [1.0, 0.0, 0.0])
        self.assertEqual(rows[0]["payload"]["industries"], ["ai", "fintech"])
