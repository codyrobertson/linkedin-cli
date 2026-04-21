from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from linkedin_cli.write import store
from linkedin_cli.write import executor as executor_mod
from linkedin_cli.write.executor import execute_action


class ExecuteActionIdempotencyTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tempdir.name) / "state.sqlite"
        self.artifacts_dir = Path(self.tempdir.name) / "artifacts"
        self.db_patcher = patch.object(store, "DB_PATH", self.db_path)
        self.artifacts_patcher = patch.object(store, "ARTIFACTS_DIR", self.artifacts_dir)
        self.db_patcher.start()
        self.artifacts_patcher.start()
        store.init_db()

        self.lock_patcher = patch.object(executor_mod, "_acquire_lock", lambda: None)
        self.unlock_patcher = patch.object(executor_mod, "_release_lock", lambda _fh: None)
        self.sleep_patcher = patch.object(executor_mod.time, "sleep", lambda *_args, **_kwargs: None)
        self.jitter_patcher = patch.object(executor_mod.random, "uniform", lambda _a, _b: 0)
        self.publish_patcher = patch.object(
            executor_mod,
            "_post_publish",
            lambda _session, _plan: {"http_status": 201, "remote_ref": "ok"},
        )

        for patcher in (
            self.lock_patcher,
            self.unlock_patcher,
            self.sleep_patcher,
            self.jitter_patcher,
            self.publish_patcher,
        ):
            patcher.start()

    def tearDown(self) -> None:
        for patcher in (
            self.publish_patcher,
            self.jitter_patcher,
            self.sleep_patcher,
            self.unlock_patcher,
            self.lock_patcher,
            self.artifacts_patcher,
            self.db_patcher,
        ):
            patcher.stop()
        self.tempdir.cleanup()

    @staticmethod
    def _plan(idempotency_key: str) -> dict:
        return {
            "action_type": "post.scheduled",
            "account_id": "1708250765",
            "idempotency_key": idempotency_key,
            "target_key": "me",
            "live_request": {"method": "POST", "path": "/voyager/api/test", "body": {}},
        }

    def test_execute_action_reuses_same_action_id_for_scheduled_posts(self) -> None:
        plan = self._plan("idem-same-action")
        store.create_action(
            action_id="act_same",
            action_type="post.scheduled",
            account_id="1708250765",
            target_key="me",
            idempotency_key=plan["idempotency_key"],
            plan=plan,
            dry_run=False,
            scheduled_at="2026-03-16T00:00:00",
        )

        result = execute_action(
            session=None,
            action_id="act_same",
            plan=plan,
            account_id="1708250765",
            dry_run=False,
        )

        self.assertEqual(result["status"], "succeeded")
        self.assertEqual(store.get_action("act_same")["state"], "succeeded")

    def test_execute_action_still_blocks_true_duplicate_actions(self) -> None:
        plan = self._plan("idem-duplicate")
        store.create_action(
            action_id="act_existing",
            action_type="post.scheduled",
            account_id="1708250765",
            target_key="me",
            idempotency_key=plan["idempotency_key"],
            plan=plan,
            dry_run=False,
            scheduled_at="2026-03-16T00:00:00",
        )

        result = execute_action(
            session=None,
            action_id="act_new",
            plan=plan,
            account_id="1708250765",
            dry_run=False,
        )

        self.assertEqual(result["status"], "duplicate_skipped")
        self.assertEqual(result["existing_action"]["action_id"], "act_existing")

    def test_execute_action_promotes_dry_run_to_live_execution(self) -> None:
        plan = self._plan("idem-dry-run-then-live")

        dry_result = execute_action(
            session=None,
            action_id="act_dry",
            plan=plan,
            account_id="1708250765",
            dry_run=True,
        )
        live_result = execute_action(
            session=None,
            action_id="act_live",
            plan=plan,
            account_id="1708250765",
            dry_run=False,
        )

        action = store.get_action("act_dry")
        self.assertEqual(dry_result["status"], "dry_run")
        self.assertEqual(live_result["status"], "succeeded")
        self.assertIsNotNone(action)
        self.assertEqual(action["state"], "succeeded")
        self.assertEqual(action["dry_run"], 0)
        self.assertIsNone(store.get_action("act_live"))

    def test_execute_action_marks_retry_scheduled_on_retryable_http_error(self) -> None:
        plan = self._plan("idem-retryable")

        with patch.object(
            executor_mod,
            "_post_publish",
            lambda _session, _plan: {"http_status": 429, "error": "rate limited"},
        ):
            result = execute_action(
                session=None,
                action_id="act_retryable",
                plan=plan,
                account_id="1708250765",
                dry_run=False,
            )

        self.assertEqual(result["status"], "retry_scheduled")
        self.assertEqual(store.get_action("act_retryable")["state"], "retry_scheduled")

    def test_execute_action_marks_unknown_remote_state_on_timeout(self) -> None:
        plan = self._plan("idem-timeout")

        with patch.object(
            executor_mod,
            "_post_publish",
            side_effect=TimeoutError("request timed out after send"),
        ):
            result = execute_action(
                session=None,
                action_id="act_timeout",
                plan=plan,
                account_id="1708250765",
                dry_run=False,
            )

        self.assertEqual(result["status"], "unknown_remote_state")
        self.assertEqual(store.get_action("act_timeout")["state"], "unknown_remote_state")

    def test_image_publish_replaces_graphql_media_placeholder(self) -> None:
        image_path = Path(self.tempdir.name) / "image.jpg"
        image_path.write_bytes(b"jpeg-bytes")
        plan = {
            "action_type": "post.image_publish",
            "live_request": {
                "steps": [
                    {
                        "name": "register_upload",
                        "method": "POST",
                        "path": "/voyager/api/voyagerMediaUploadMetadata?action=upload",
                        "body": {"fileSize": 10, "filename": "image.jpg"},
                    },
                    {
                        "name": "upload_image",
                        "method": "PUT",
                        "url_from": "register_upload.uploadUrl",
                        "body_type": "binary",
                        "file_path": str(image_path),
                    },
                    {
                        "name": "publish_post",
                        "method": "POST",
                        "path": "/voyager/api/graphql?action=execute&queryId=shares",
                        "body": {
                            "variables": {
                                "post": {
                                    "commentary": {"text": "hello", "attributesV2": []},
                                    "media": [{"status": "READY", "media_urn_from": "register_upload.urn"}],
                                }
                            }
                        },
                    },
                ]
            },
        }

        class FakeResponse:
            def __init__(self, status_code: int, payload: dict[str, object]):
                self.status_code = status_code
                self._payload = payload
                self.text = "{}"

            def json(self) -> dict[str, object]:
                return self._payload

        class FakeSession:
            def __init__(self) -> None:
                self.cookies = []
                self.published_body = None

            def post(self, url: str, **kwargs):
                if "voyagerMediaUploadMetadata" in url:
                    return FakeResponse(
                        200,
                        {
                            "value": {
                                "uploadUrl": "https://upload.example/image",
                                "urn": "urn:li:digitalmediaAsset:image-1",
                            }
                        },
                    )
                self.published_body = kwargs["json"]
                return FakeResponse(
                    201,
                    {
                        "data": {
                            "data": {
                                "createContentcreationDashShares": {
                                    "resourceKey": "urn:li:share:1",
                                }
                            }
                        }
                    },
                )

            def put(self, _url: str, **_kwargs):
                return FakeResponse(201, {})

        session = FakeSession()

        result = executor_mod._image_post_publish(session, plan)

        self.assertEqual(result["http_status"], 201)
        self.assertEqual(result["remote_ref"], "urn:li:share:1")
        media_item = session.published_body["variables"]["post"]["media"][0]
        self.assertEqual(media_item["media"], "urn:li:digitalmediaAsset:image-1")
        self.assertNotIn("media_urn_from", media_item)


if __name__ == "__main__":
    unittest.main()
