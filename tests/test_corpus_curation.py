from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from linkedin_cli.write import store


class CorpusCurationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tempdir.name) / "state.sqlite"
        self.artifacts_dir = Path(self.tempdir.name) / "artifacts"
        self.db_patcher = patch.object(store, "DB_PATH", self.db_path)
        self.artifacts_patcher = patch.object(store, "ARTIFACTS_DIR", self.artifacts_dir)
        self.db_patcher.start()
        self.artifacts_patcher.start()
        store.init_db()

        from linkedin_cli import content_warehouse, corpus_curation

        self.content_warehouse = content_warehouse
        self.corpus_curation = corpus_curation

    def tearDown(self) -> None:
        self.db_patcher.stop()
        self.artifacts_patcher.stop()
        self.tempdir.cleanup()

    def _append_record(
        self,
        *,
        job_id: str,
        url: str,
        title: str,
        text: str,
        industry: str,
        structure: str = "insight",
        outcome_score: float = 12.0,
        author_name: str = "Operator",
        author_url: str = "https://www.linkedin.com/in/operator",
        published_at: str = "2026-03-20T10:00:00Z",
    ) -> None:
        self.content_warehouse.append_post_shard(
            job_id=job_id,
            record={
                "url": url,
                "title": title,
                "author_name": author_name,
                "author_url": author_url,
                "published_at": published_at,
                "text": text,
                "hook": title,
                "structure": structure,
                "word_count": len(text.split()),
                "reaction_count": 120,
                "comment_count": 18,
                "repost_count": 4,
                "owned_by_me": False,
                "outcome_score": outcome_score,
                "source_query": f'site:linkedin.com/posts "{industry} workflow"',
                "industries": [industry],
                "metadata": {},
                "updated_at": "2026-03-20T12:00:00Z",
            },
        )

    def test_curate_corpus_dedupes_and_builds_holdouts(self) -> None:
        self._append_record(
            job_id="curate-1",
            url="https://www.linkedin.com/posts/1",
            title="AI workflow bottlenecks are where automation wins.",
            text="AI workflow bottlenecks are where automation wins. We mapped the handoff and proved the latency drop.",
            industry="ai",
        )
        self._append_record(
            job_id="curate-1",
            url="https://www.linkedin.com/posts/2",
            title="AI workflow bottlenecks are where automation wins.",
            text="AI workflow bottlenecks are where automation wins. We mapped the handoff and proved the latency drop.",
            industry="ai",
        )
        self._append_record(
            job_id="curate-1",
            url="https://www.linkedin.com/posts/3",
            title="Marketing workflow debt compounds every week.",
            text="Marketing workflow debt compounds every week. We replaced the approval maze and saw campaign velocity climb.",
            industry="marketing",
            structure="how_to",
            outcome_score=14.0,
        )
        self.content_warehouse.materialize_shards(job_id="curate-1")

        curated = self.corpus_curation.curate_corpus(min_quality=0.1)
        holdouts_dir = Path(self.tempdir.name) / "holdouts"
        holdouts = self.corpus_curation.build_holdouts(
            output_dir=holdouts_dir,
            limit=10,
            quota_per_industry=2,
            quota_per_format=2,
        )
        stats = self.corpus_curation.curation_stats()

        self.assertEqual(curated["processed_count"], 3)
        self.assertEqual(curated["exact_duplicate_count"], 1)
        self.assertGreaterEqual(curated["kept_count"], 2)
        self.assertTrue((holdouts_dir / "train.jsonl").exists())
        self.assertTrue((holdouts_dir / "time_holdout.jsonl").exists())
        self.assertTrue((holdouts_dir / "metadata.json").exists())
        self.assertEqual(holdouts["row_count"], sum(holdouts["splits"].values()))
        self.assertEqual(holdouts["split_strategy"], "author_disjoint_with_time_holdout")
        self.assertGreaterEqual(stats["curated_count"], 2)

    def test_build_holdouts_is_author_disjoint_and_writes_recent_time_holdout(self) -> None:
        rows = [
            ("https://www.linkedin.com/posts/1", "A1", "https://www.linkedin.com/in/a1", "2026-03-01T10:00:00Z"),
            ("https://www.linkedin.com/posts/2", "A1", "https://www.linkedin.com/in/a1", "2026-03-02T10:00:00Z"),
            ("https://www.linkedin.com/posts/3", "A2", "https://www.linkedin.com/in/a2", "2026-03-03T10:00:00Z"),
            ("https://www.linkedin.com/posts/4", "A2", "https://www.linkedin.com/in/a2", "2026-03-04T10:00:00Z"),
            ("https://www.linkedin.com/posts/5", "A3", "https://www.linkedin.com/in/a3", "2026-03-05T10:00:00Z"),
            ("https://www.linkedin.com/posts/6", "A4", "https://www.linkedin.com/in/a4", "2026-03-06T10:00:00Z"),
        ]
        for index, (url, author_name, author_url, published_at) in enumerate(rows, start=1):
            self._append_record(
                job_id="curate-3",
                url=url,
                title=f"Workflow lesson {index}",
                text=f"Workflow lesson {index}. We measured the change and wrote the playbook.",
                industry="ai",
                author_name=author_name,
                author_url=author_url,
                published_at=published_at,
                outcome_score=10.0 + index,
            )
        self.content_warehouse.materialize_shards(job_id="curate-3")
        self.corpus_curation.curate_corpus(min_quality=0.1)

        holdouts_dir = Path(self.tempdir.name) / "holdouts-author"
        summary = self.corpus_curation.build_holdouts(output_dir=holdouts_dir, limit=10, time_holdout_ratio=0.2)

        split_authors: dict[str, set[str]] = {}
        for split_name in ("train", "val", "test"):
            split_rows = [
                json.loads(line)
                for line in (holdouts_dir / f"{split_name}.jsonl").read_text(encoding="utf-8").splitlines()
                if line.strip()
            ]
            split_authors[split_name] = {str(row.get("author_url") or row.get("author_name")) for row in split_rows}
        time_holdout_rows = [
            json.loads(line)
            for line in (holdouts_dir / "time_holdout.jsonl").read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
        metadata = json.loads((holdouts_dir / "metadata.json").read_text(encoding="utf-8"))

        self.assertTrue(split_authors["train"].isdisjoint(split_authors["val"]))
        self.assertTrue(split_authors["train"].isdisjoint(split_authors["test"]))
        self.assertTrue(split_authors["val"].isdisjoint(split_authors["test"]))
        self.assertEqual(summary["time_holdout_count"], len(time_holdout_rows))
        self.assertEqual(metadata["split_strategy"], "author_disjoint_with_time_holdout")
        self.assertGreaterEqual(len(time_holdout_rows), 1)

    def test_build_curated_sft_and_preference_datasets(self) -> None:
        for index, industry in enumerate(["ai", "marketing", "ai", "marketing"], start=1):
            self._append_record(
                job_id="curate-2",
                url=f"https://www.linkedin.com/posts/{index}",
                title=f"{industry.title()} workflow lesson {index}",
                text=f"{industry.title()} workflow lesson {index}. We cut the bottleneck, measured the before-and-after, and documented the playbook.",
                industry=industry,
                structure="insight" if index % 2 else "how_to",
                outcome_score=10.0 + index,
            )
        self.content_warehouse.materialize_shards(job_id="curate-2")
        self.corpus_curation.curate_corpus(min_quality=0.1)

        sft_dir = Path(self.tempdir.name) / "curated-sft"
        pref_dir = Path(self.tempdir.name) / "curated-pref"
        sft = self.corpus_curation.build_curated_sft_dataset(output_dir=sft_dir, limit=10, quota_per_industry=2)
        pref = self.corpus_curation.build_curated_preference_dataset(output_dir=pref_dir, limit=10, quota_per_industry=2)

        train_rows = [
            json.loads(line)
            for line in (sft_dir / "train.jsonl").read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
        pref_rows = [
            json.loads(line)
            for line in (pref_dir / "train.jsonl").read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]

        self.assertEqual(sft["dataset_type"], "curated_sft")
        self.assertEqual(pref["dataset_type"], "curated_preference")
        self.assertTrue(train_rows)
        self.assertTrue(pref_rows)
        self.assertEqual(train_rows[0]["metadata"]["source"], "curated_harvested")
        self.assertIn("chosen", pref_rows[0])
        self.assertIn("rejected", pref_rows[0])
