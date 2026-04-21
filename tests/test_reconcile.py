from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from linkedin_cli.write import reconcile, store


class _FakeResponse:
    status_code = 200
    url = "https://www.linkedin.com/test"
    headers = {}
    text = ""

    def __init__(self, payload: dict) -> None:
        self._payload = payload
        self.text = ""

    def json(self) -> dict:
        return self._payload


class ReconcileActionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tempdir.name) / "state.sqlite"
        self.artifacts_dir = Path(self.tempdir.name) / "artifacts"
        self.db_patcher = patch.object(store, "DB_PATH", self.db_path)
        self.store_artifacts_patcher = patch.object(store, "ARTIFACTS_DIR", self.artifacts_dir)
        self.db_patcher.start()
        self.store_artifacts_patcher.start()
        store.init_db()

    def tearDown(self) -> None:
        self.store_artifacts_patcher.stop()
        self.db_patcher.stop()
        self.tempdir.cleanup()

    def test_reconcile_action_updates_state_and_writes_artifact(self) -> None:
        plan = {
            "action_type": "post.publish",
            "account_id": "1708250765",
            "idempotency_key": "idem-reconcile",
            "target_key": "me",
            "desired": {"text": "Hello world"},
            "reconcile": {"strategy": "feed_text_match"},
        }
        store.create_action(
            action_id="act_reconcile",
            action_type="post.publish",
            account_id="1708250765",
            target_key="me",
            idempotency_key="idem-reconcile",
            plan=plan,
            dry_run=False,
        )
        store.update_state("act_reconcile", "unknown_remote_state")

        payload = {
            "included": [
                {
                    "entityUrn": "urn:li:ugcPost:1",
                    "commentary": {"text": "Hello world"},
                }
            ]
        }

        with patch.object(reconcile, "voyager_get", return_value=_FakeResponse(payload)):
            result = reconcile.reconcile_action(session=None, action_id="act_reconcile")

        action = store.get_action("act_reconcile")
        artifacts = store.list_artifacts("act_reconcile")
        self.assertTrue(result["reconciled"])
        self.assertEqual(action["state"], "succeeded")
        self.assertTrue(any(item["kind"] == "reconcile" for item in artifacts))

    def test_reconcile_profile_edit_updates_state_on_match(self) -> None:
        plan = {
            "action_type": "profile.edit.headline",
            "account_id": "1708250765",
            "idempotency_key": "idem-profile",
            "target_key": "profile.headline",
            "desired": {"field": "headline", "value": "Building durable agent systems"},
        }
        store.create_action(
            action_id="act_profile",
            action_type="profile.edit.headline",
            account_id="1708250765",
            target_key="profile.headline",
            idempotency_key="idem-profile",
            plan=plan,
            dry_run=False,
        )
        store.update_state("act_profile", "unknown_remote_state")
        payload = {
            "included": [
                {
                    "$type": "com.linkedin.voyager.dash.identity.profile.Profile",
                    "headline": "Building durable agent systems",
                }
            ]
        }

        with patch.object(reconcile, "voyager_get", return_value=_FakeResponse(payload)):
            result = reconcile.reconcile_action(session=None, action_id="act_profile")

        self.assertTrue(result["reconciled"])
        self.assertEqual(store.get_action("act_profile")["state"], "succeeded")

    def test_reconcile_dm_updates_state_on_message_match(self) -> None:
        conversation_urn = "urn:li:msg_conversation:(urn:li:fsd_profile:me,2-demo)"
        plan = {
            "action_type": "dm.send",
            "account_id": "1708250765",
            "idempotency_key": "idem-dm",
            "target_key": conversation_urn,
            "desired": {"conversation_urn": conversation_urn, "message_text": "Checking in on this"},
        }
        store.create_action(
            action_id="act_dm",
            action_type="dm.send",
            account_id="1708250765",
            target_key=conversation_urn,
            idempotency_key="idem-dm",
            plan=plan,
            dry_run=False,
        )
        store.update_state("act_dm", "unknown_remote_state")
        payload = {
            "included": [
                {
                    "$type": "com.linkedin.voyager.messaging.Message",
                    "entityUrn": "urn:li:msg:1",
                    "*conversation": conversation_urn,
                    "body": {"text": "Checking in on this"},
                }
            ]
        }

        with patch.object(reconcile, "voyager_get", return_value=_FakeResponse(payload)):
            result = reconcile.reconcile_action(session=None, action_id="act_dm")

        action = store.get_action("act_dm")
        self.assertTrue(result["reconciled"])
        self.assertEqual(action["state"], "succeeded")
        self.assertEqual(action["remote_ref"], "urn:li:msg:1")

    def test_reconcile_comment_updates_state_on_bootstrap_comment_match(self) -> None:
        import html
        import json

        plan = {
            "action_type": "comment.post",
            "account_id": "1708250765",
            "idempotency_key": "idem-comment",
            "target_key": "comment.urn:li:ugcPost:1",
            "desired": {
                "post_url": "https://www.linkedin.com/posts/example-activity-1",
                "text": "Useful point.",
            },
        }
        store.create_action(
            action_id="act_comment",
            action_type="comment.post",
            account_id="1708250765",
            target_key="comment.urn:li:ugcPost:1",
            idempotency_key="idem-comment",
            plan=plan,
            dry_run=False,
        )
        store.update_state("act_comment", "unknown_remote_state")
        body = {
            "included": [
                {
                    "$type": "com.linkedin.voyager.dash.social.Comment",
                    "entityUrn": "urn:li:comment:1",
                    "commentary": {"text": "Useful point."},
                }
            ]
        }
        meta = {"request": "/voyager/api/graphql?comments", "status": 200, "body": "comment-body"}

        class HtmlResponse:
            status_code = 200
            url = "https://www.linkedin.com/posts/example-activity-1"
            headers = {}
            text = (
                f'<code id="datalet-bpr-guid-1">{html.escape(json.dumps(meta))}</code>'
                f'<code id="comment-body">{html.escape(json.dumps(body))}</code>'
            )

        with patch.object(reconcile, "session_request", return_value=HtmlResponse()):
            result = reconcile.reconcile_action(session=None, action_id="act_comment")

        action = store.get_action("act_comment")
        self.assertTrue(result["reconciled"])
        self.assertEqual(action["state"], "succeeded")
        self.assertEqual(action["remote_ref"], "urn:li:comment:1")

    def test_reconcile_action_writes_artifact_for_unsupported_action(self) -> None:
        plan = {
            "action_type": "endorse",
            "account_id": "1708250765",
            "idempotency_key": "idem-endorse",
            "target_key": "endorse.jane",
        }
        store.create_action(
            action_id="act_endorse",
            action_type="endorse",
            account_id="1708250765",
            target_key="endorse.jane",
            idempotency_key="idem-endorse",
            plan=plan,
            dry_run=False,
        )

        result = reconcile.reconcile_action(session=None, action_id="act_endorse")

        self.assertFalse(result["reconciled"])
        self.assertIn("No reconciler", result["reason"])
        self.assertTrue(any(item["kind"] == "reconcile" for item in store.list_artifacts("act_endorse")))

    def test_reconcile_connect_updates_state_on_pending_profile_state(self) -> None:
        plan = {
            "action_type": "connect",
            "account_id": "1708250765",
            "idempotency_key": "idem-connect",
            "target_key": "connect.jane",
            "desired": {"vanity_name": "jane", "target_urn": "urn:li:fsd_profile:jane"},
        }
        store.create_action(
            action_id="act_connect",
            action_type="connect",
            account_id="1708250765",
            target_key="connect.jane",
            idempotency_key="idem-connect",
            plan=plan,
            dry_run=False,
        )
        store.update_state("act_connect", "unknown_remote_state")

        class HtmlResponse:
            text = "<html><body><button>Pending</button></body></html>"

        with patch.object(reconcile, "session_request", return_value=HtmlResponse()):
            result = reconcile.reconcile_action(session=None, action_id="act_connect")

        action = store.get_action("act_connect")
        self.assertTrue(result["reconciled"])
        self.assertEqual(action["state"], "succeeded")
        self.assertEqual(action["remote_ref"], "urn:li:fsd_profile:jane")

    def test_reconcile_follow_updates_state_on_following_profile_state(self) -> None:
        plan = {
            "action_type": "follow",
            "account_id": "1708250765",
            "idempotency_key": "idem-follow",
            "target_key": "follow.jane",
            "desired": {"vanity_name": "jane", "target_urn": "urn:li:fsd_profile:jane"},
        }
        store.create_action(
            action_id="act_follow",
            action_type="follow",
            account_id="1708250765",
            target_key="follow.jane",
            idempotency_key="idem-follow",
            plan=plan,
            dry_run=False,
        )
        store.update_state("act_follow", "unknown_remote_state")

        class HtmlResponse:
            text = "<html><body><button>Following</button></body></html>"

        with patch.object(reconcile, "session_request", return_value=HtmlResponse()):
            result = reconcile.reconcile_action(session=None, action_id="act_follow")

        self.assertTrue(result["reconciled"])
        self.assertEqual(store.get_action("act_follow")["state"], "succeeded")

    def test_reconcile_experience_updates_state_on_position_match(self) -> None:
        plan = {
            "action_type": "experience.add",
            "account_id": "1708250765",
            "idempotency_key": "idem-exp",
            "target_key": "experience.Acme.Operator",
            "desired": {"title": "Operator", "company": "Acme"},
        }
        store.create_action(
            action_id="act_exp",
            action_type="experience.add",
            account_id="1708250765",
            target_key="experience.Acme.Operator",
            idempotency_key="idem-exp",
            plan=plan,
            dry_run=False,
        )
        store.update_state("act_exp", "unknown_remote_state")
        payload = {
            "included": [
                {
                    "$type": "com.linkedin.voyager.dash.identity.profile.Position",
                    "entityUrn": "urn:li:fsd_position:1",
                    "title": {"localized": {"en_US": "Operator"}},
                    "companyName": {"localized": {"en_US": "Acme"}},
                }
            ]
        }

        with patch.object(reconcile, "voyager_get", return_value=_FakeResponse(payload)):
            result = reconcile.reconcile_action(session=None, action_id="act_exp")

        action = store.get_action("act_exp")
        self.assertTrue(result["reconciled"])
        self.assertEqual(action["state"], "succeeded")
        self.assertEqual(action["remote_ref"], "urn:li:fsd_position:1")
