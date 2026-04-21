from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from linkedin_cli.write import store


class ModelingTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tempdir.name) / "state.sqlite"
        self.db_patcher = patch.object(store, "DB_PATH", self.db_path)
        self.db_patcher.start()
        store.init_db()

        from linkedin_cli import modeling

        self.modeling = modeling
        self.modeling.init_modeling_db()

    def tearDown(self) -> None:
        self.db_patcher.stop()
        self.tempdir.cleanup()

    def test_train_reply_model_persists_probabilistic_artifact_and_predictions(self) -> None:
        samples = [
            {"features": {"fit_score": 0.9, "commented": 1.0, "profile_view": 1.0}, "label": 1},
            {"features": {"fit_score": 0.8, "commented": 1.0, "profile_view": 0.0}, "label": 1},
            {"features": {"fit_score": 0.72, "commented": 1.0, "profile_view": 1.0}, "label": 1},
            {"features": {"fit_score": 0.66, "commented": 0.0, "profile_view": 1.0}, "label": 1},
            {"features": {"fit_score": 0.2, "commented": 0.0, "profile_view": 0.0}, "label": 0},
            {"features": {"fit_score": 0.1, "commented": 0.0, "profile_view": 0.0}, "label": 0},
            {"features": {"fit_score": 0.14, "commented": 0.0, "profile_view": 1.0}, "label": 0},
            {"features": {"fit_score": 0.05, "commented": 0.0, "profile_view": 0.0}, "label": 0},
        ]

        result = self.modeling.train_model(task="reply_likelihood", samples=samples, artifact_dir=Path(self.tempdir.name))
        stored = self.modeling.get_model("reply_likelihood")
        high_intent = self.modeling.predict_probability(
            "reply_likelihood",
            {"fit_score": 0.92, "commented": 1.0, "profile_view": 1.0},
        )
        low_intent = self.modeling.predict_probability(
            "reply_likelihood",
            {"fit_score": 0.08, "commented": 0.0, "profile_view": 0.0},
        )

        self.assertTrue(result["trained"])
        self.assertIn("feature_names", result)
        self.assertGreaterEqual(result["metrics"]["train_accuracy"], 0.5)
        self.assertIn("validation_accuracy", result["metrics"])
        self.assertIn("brier_score", result["metrics"])
        self.assertTrue(Path(result["artifact_path"]).exists())
        self.assertEqual(result["task"], "reply_likelihood")
        self.assertIsNotNone(stored)
        self.assertGreater(high_intent, low_intent)
        self.assertGreater(high_intent, 0.5)
        self.assertLess(low_intent, 0.5)

    def test_train_multi_head_model_tracks_grouped_split_metadata(self) -> None:
        samples = [
            {"features": {"f1": 0.9}, "labels": {"head_a": 0.8, "head_b": 0.4}, "metadata": {"group_key": "author:a"}},
            {"features": {"f1": 0.85}, "labels": {"head_a": 0.75, "head_b": 0.45}, "metadata": {"group_key": "author:a"}},
            {"features": {"f1": 0.2}, "labels": {"head_a": 0.1, "head_b": 0.2}, "metadata": {"group_key": "author:b"}},
            {"features": {"f1": 0.15}, "labels": {"head_a": 0.05, "head_b": 0.25}, "metadata": {"group_key": "author:b"}},
        ]

        result = self.modeling.train_multi_head_model(
            task="stacked_test",
            samples=samples,
            artifact_dir=Path(self.tempdir.name),
            model_name="stacked_test",
        )
        stored = self.modeling.get_model("stacked_test")

        self.assertTrue(result["trained"])
        self.assertEqual(set(result["heads"]), {"head_a", "head_b"})
        self.assertEqual((stored.get("metadata") or {}).get("split_strategy"), "grouped")
        split = (stored.get("metadata") or {}).get("split") or {}
        self.assertEqual(split.get("train_group_count"), 1)
        self.assertEqual(split.get("validation_group_count"), 1)

    def test_train_multi_head_model_supports_mixed_regression_and_classification_heads(self) -> None:
        samples = [
            {"features": {"f1": 0.95}, "labels": {"regression_head": 0.9, "classification_head": 1.0}, "metadata": {"group_key": "author:a"}},
            {"features": {"f1": 0.85}, "labels": {"regression_head": 0.8, "classification_head": 1.0}, "metadata": {"group_key": "author:a"}},
            {"features": {"f1": 0.2}, "labels": {"regression_head": 0.2, "classification_head": 0.0}, "metadata": {"group_key": "author:b"}},
            {"features": {"f1": 0.1}, "labels": {"regression_head": 0.1, "classification_head": 0.0}, "metadata": {"group_key": "author:b"}},
        ]

        result = self.modeling.train_multi_head_model(
            task="stacked_mixed",
            samples=samples,
            artifact_dir=Path(self.tempdir.name),
            model_name="stacked_mixed",
            head_types={"regression_head": "regression", "classification_head": "classification"},
        )
        stored = self.modeling.get_model("stacked_mixed")

        self.assertTrue(result["trained"])
        self.assertEqual((stored.get("heads") or {}).get("regression_head", {}).get("kind"), "regression")
        self.assertEqual((stored.get("heads") or {}).get("classification_head", {}).get("kind"), "classification")
        classification_metrics = ((stored.get("heads") or {}).get("classification_head") or {}).get("metrics") or {}
        self.assertIn("validation_accuracy", classification_metrics)
        self.assertIn("validation_brier_score", classification_metrics)
        self.assertIn("validation_roc_auc", classification_metrics)
        self.assertIn("validation_ece", classification_metrics)
        self.assertIn("calibration", (stored.get("heads") or {}).get("classification_head", {}))
        regression_metrics = ((stored.get("heads") or {}).get("regression_head") or {}).get("metrics") or {}
        self.assertIn("validation_r2", regression_metrics)
