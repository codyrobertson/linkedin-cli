from __future__ import annotations

import html
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import requests

from linkedin_cli.write import store


PUBLIC_POST_HTML = """
<html>
  <head>
    <title>Example Post | Alice Example | 12 comments</title>
    <script type="application/ld+json">
      {
        "@context": "http://schema.org",
        "@type": "SocialMediaPosting",
        "@id": "https://www.linkedin.com/posts/alice-example_demo-post-activity-1",
        "headline": "Example post",
        "comment": [
          {"@type": "Comment", "text": "Great post", "author": {"@type": "Person", "name": "John Doe", "url": "https://www.linkedin.com/in/john-doe/" }},
          {"@type": "Comment", "text": "How are you using this in AI workflows?", "author": {"@type": "Person", "name": "Jane Doe", "url": "https://www.linkedin.com/in/jane-doe/" }}
        ]
      }
    </script>
  </head>
  <body></body>
</html>
"""


def _bootstrap_post_html(thread_urn: str = "urn:li:ugcPost:7441834638312009728") -> str:
    body = {
        "included": [
            {
                "$type": "com.linkedin.voyager.dash.social.SocialDetail",
                "entityUrn": f"urn:li:fsd_socialDetail:({thread_urn},{thread_urn},urn:li:highlightedReply:-)",
                "*socialPermissions": f"urn:li:fsd_socialPermissions:({thread_urn},urn:li:fsd_profile:abc)",
            },
            {
                "$type": "com.linkedin.voyager.dash.social.SocialPermissions",
                "entityUrn": f"urn:li:fsd_socialPermissions:({thread_urn},urn:li:fsd_profile:abc)",
                "canPostComments": True,
            },
        ]
    }
    meta = {"request": "/voyager/api/graphql?includeWebMetadata=true", "status": 200, "body": "bpr-guid-2"}
    return f"""
<html>
  <body>
    <code id="datalet-bpr-guid-1">{html.escape(json.dumps(meta))}</code>
    <code id="bpr-guid-2">{html.escape(json.dumps(body))}</code>
  </body>
</html>
"""


def _bootstrap_comment_page_html(
    *,
    activity_urn: str = "urn:li:activity:7442650270389583872",
    comment_urn: str = "urn:li:comment:(activity:7442650270389583872,7442658458996391936)",
    author_name: str = "Sonu Goswami",
    author_url: str = "https://www.linkedin.com/in/sonu-goswami-6209a3146",
    text: str = "Accountability is what actually scales.",
) -> str:
    body = {
        "included": [
            {
                "$type": "com.linkedin.voyager.dash.social.Comment",
                "urn": comment_urn,
                "entityUrn": "urn:li:fsd_comment:(7442658458996391936,urn:li:activity:7442650270389583872)",
                "commentary": {
                    "text": text,
                },
                "commenter": {
                    "navigationUrl": author_url,
                    "title": {"text": author_name},
                },
            }
        ]
    }
    meta = {"request": "/voyager/api/graphql?includeWebMetadata=true", "status": 200, "body": "bpr-guid-4"}
    return f"""
<html>
  <body>
    <code id="datalet-bpr-guid-3">{html.escape(json.dumps(meta))}</code>
    <code id="bpr-guid-4">{html.escape(json.dumps(body))}</code>
  </body>
</html>
"""


class CommentQueueTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tempdir.name) / "state.sqlite"
        self.artifacts_dir = Path(self.tempdir.name) / "artifacts"
        self.db_patcher = patch.object(store, "DB_PATH", self.db_path)
        self.artifacts_patcher = patch.object(store, "ARTIFACTS_DIR", self.artifacts_dir)
        self.db_patcher.start()
        self.artifacts_patcher.start()
        store.init_db()

        from linkedin_cli import comment, discovery, lead, workflow
        from linkedin_cli.write import executor as executor_mod

        self.comment = comment
        self.discovery = discovery
        self.lead = lead
        self.workflow = workflow
        self.executor_lock_patcher = patch.object(executor_mod, "_acquire_lock", lambda: None)
        self.executor_unlock_patcher = patch.object(executor_mod, "_release_lock", lambda _fh: None)
        self.executor_sleep_patcher = patch.object(executor_mod.time, "sleep", lambda *_args, **_kwargs: None)
        self.executor_jitter_patcher = patch.object(executor_mod.random, "uniform", lambda _a, _b: 0)
        self.executor_warmup_patcher = patch.object(executor_mod, "voyager_get", lambda *_args, **_kwargs: None)
        for patcher in (
            self.executor_lock_patcher,
            self.executor_unlock_patcher,
            self.executor_sleep_patcher,
            self.executor_jitter_patcher,
            self.executor_warmup_patcher,
        ):
            patcher.start()
        self.discovery.init_discovery_db()
        self.lead.init_lead_db()
        self.workflow.init_workflow_db()
        self.comment.init_comment_db()

    def tearDown(self) -> None:
        for patcher in (
            self.executor_warmup_patcher,
            self.executor_jitter_patcher,
            self.executor_sleep_patcher,
            self.executor_unlock_patcher,
            self.executor_lock_patcher,
        ):
            patcher.stop()
        self.artifacts_patcher.stop()
        self.db_patcher.stop()
        self.tempdir.cleanup()

    def test_queue_post_comments_and_build_draft(self) -> None:
        self.discovery.upsert_prospect(
            profile_key="jane-doe",
            display_name="Jane Doe",
            public_identifier="jane-doe",
            headline="Founder building AI workflow systems",
            company="Acme AI",
        )
        self.discovery.add_signal("jane-doe", signal_type="commented", source="public")
        self.discovery.add_signal("jane-doe", signal_type="profile_view", source="analytics")
        self.lead.run_autopilot(target_topics=["ai", "workflow"], limit=10, dry_run=False)

        queued = self.comment.queue_post_comments(
            post_url="https://www.linkedin.com/posts/alice-example_demo-post-activity-1",
            html=PUBLIC_POST_HTML,
        )
        items = self.comment.list_comment_queue(post_url="https://www.linkedin.com/posts/alice-example_demo-post-activity-1")
        draft = self.comment.draft_comment_reply(
            post_url="https://www.linkedin.com/posts/alice-example_demo-post-activity-1",
            author_profile_key="jane-doe",
            tone="expert",
        )

        self.assertEqual(queued["queued_count"], 2)
        self.assertEqual(len(items), 2)
        self.assertEqual(draft["author_profile_key"], "jane-doe")
        self.assertIn("workflow", draft["draft_reply"].lower())
        self.assertEqual(draft["tone"], "expert")

    def test_publish_comment_dry_run_builds_captured_request_shape(self) -> None:
        session = requests.Session()
        session.cookies.set("JSESSIONID", '"ajax:12345"', domain=".linkedin.com")

        with patch("linkedin_cli.comment.request") as request_mock:
            request_mock.return_value.text = _bootstrap_post_html()
            result = self.comment.publish_post_comment(
                session=session,
                post_url="https://www.linkedin.com/posts/alice-example_demo-post-activity-7441834639289360385-gWUt",
                text="hello from codex",
                execute=False,
            )

        self.assertEqual(result["status"], "dry_run")
        self.assertEqual(result["request"]["path"], "/voyager/api/voyagerSocialDashNormComments?decorationId=com.linkedin.voyager.dash.deco.social.NormComment-43")
        self.assertEqual(result["request"]["body"]["threadUrn"], "urn:li:ugcPost:7441834638312009728")
        self.assertEqual(result["request"]["body"]["commentary"]["text"], "hello from codex")
        self.assertEqual(result["request"]["headers"]["X-Li-Pem-Metadata"], "Voyager - Feed - Comments=create-a-comment")
        self.assertEqual(result["request"]["headers"]["csrf-token"], "ajax:12345")

    def test_publish_comment_execute_posts_and_marks_queue_replied(self) -> None:
        self.comment.queue_post_comments(
            post_url="https://www.linkedin.com/posts/alice-example_demo-post-activity-7441834639289360385-gWUt",
            html=_bootstrap_comment_page_html(),
        )
        drafted = self.comment.draft_comment_reply(
            post_url="https://www.linkedin.com/posts/alice-example_demo-post-activity-7441834639289360385-gWUt",
            author_profile_key="sonu-goswami-6209a3146",
            tone="expert",
        )

        class FakeResponse:
            status_code = 201
            text = '{"entityUrn":"urn:li:comment:(urn:li:ugcPost:7441834638312009728,1)"}'

            def json(self) -> dict[str, str]:
                return {"entityUrn": "urn:li:comment:(urn:li:ugcPost:7441834638312009728,1)"}

        session = requests.Session()
        session.cookies.set("JSESSIONID", '"ajax:12345"', domain=".linkedin.com")
        post_calls: list[dict[str, object]] = []

        def fake_post(url: str, **kwargs: object) -> FakeResponse:
            post_calls.append({"url": url, **kwargs})
            return FakeResponse()

        session.post = fake_post  # type: ignore[assignment]

        with patch("linkedin_cli.comment.request") as request_mock:
            request_mock.return_value.text = "<html></html>"
            result = self.comment.publish_post_comment(
                session=session,
                post_url="https://www.linkedin.com/posts/alice-example_demo-post-activity-7441834639289360385-gWUt",
                comment_id=drafted["comment_id"],
                execute=True,
            )

        self.assertEqual(result["status"], "succeeded")
        self.assertEqual(len(post_calls), 1)
        self.assertEqual(post_calls[0]["url"], "https://www.linkedin.com/voyager/api/voyagerSocialDashNormComments?decorationId=com.linkedin.voyager.dash.deco.social.NormComment-43")
        self.assertEqual(
            post_calls[0]["json"]["commentary"]["text"],  # type: ignore[index]
            drafted["draft_reply"],
        )
        self.assertEqual(
            post_calls[0]["json"]["threadUrn"],  # type: ignore[index]
            "urn:li:comment:(activity:7442650270389583872,7442658458996391936)",
        )
        self.assertEqual(
            post_calls[0]["headers"]["X-Li-Pem-Metadata"],  # type: ignore[index]
            "Voyager - Feed - Comments=create-a-comment-reply",
        )
        action = store.find_by_idempotency_key("me", result["action"]["idempotency_key"])
        self.assertIsNotNone(action)
        self.assertEqual(action["state"], "succeeded")
        item = self.comment.list_comment_queue(
            post_url="https://www.linkedin.com/posts/alice-example_demo-post-activity-7441834639289360385-gWUt",
            state="replied",
        )[0]
        self.assertEqual(item["comment_id"], drafted["comment_id"])
        self.assertEqual(item["metadata"]["delivery_mode"], "comment_reply")
        stats = store.telemetry_stats()
        self.assertEqual(stats["by_event_type"].get("comment_reply_posted"), 1)

    def test_queue_post_comments_extracts_comment_urn_from_bootstrap(self) -> None:
        queued = self.comment.queue_post_comments(
            post_url="https://www.linkedin.com/posts/alice-example_demo-post-activity-7441834639289360385-gWUt",
            html=_bootstrap_comment_page_html(),
        )
        self.assertEqual(queued["queued_count"], 1)
        item = self.comment.list_comment_queue(post_url="https://www.linkedin.com/posts/alice-example_demo-post-activity-7441834639289360385-gWUt")[0]
        self.assertEqual(
            item["metadata"]["comment_urn"],
            "urn:li:comment:(activity:7442650270389583872,7442658458996391936)",
        )

    def test_publish_comment_backfills_missing_comment_urn_from_refresh(self) -> None:
        self.comment.queue_post_comments(
            post_url="https://www.linkedin.com/posts/alice-example_demo-post-activity-7441834639289360385-gWUt",
            html=PUBLIC_POST_HTML,
        )
        drafted = self.comment.draft_comment_reply(
            post_url="https://www.linkedin.com/posts/alice-example_demo-post-activity-7441834639289360385-gWUt",
            author_profile_key="jane-doe",
            tone="expert",
        )

        class FakeResponse:
            status_code = 201
            text = '{"entityUrn":"urn:li:comment:(urn:li:ugcPost:7441834638312009728,2)"}'

            def json(self) -> dict[str, str]:
                return {"entityUrn": "urn:li:comment:(urn:li:ugcPost:7441834638312009728,2)"}

        session = requests.Session()
        session.cookies.set("JSESSIONID", '"ajax:12345"', domain=".linkedin.com")
        post_calls: list[dict[str, object]] = []

        def fake_post(url: str, **kwargs: object) -> FakeResponse:
            post_calls.append({"url": url, **kwargs})
            return FakeResponse()

        session.post = fake_post  # type: ignore[assignment]

        with patch("linkedin_cli.comment.request") as request_mock:
            request_mock.return_value.text = _bootstrap_comment_page_html(
                author_name="Jane Doe",
                author_url="https://www.linkedin.com/in/jane-doe/",
                text="How are you using this in AI workflows?",
            )
            result = self.comment.publish_post_comment(
                session=session,
                post_url="https://www.linkedin.com/posts/alice-example_demo-post-activity-7441834639289360385-gWUt",
                comment_id=drafted["comment_id"],
                execute=True,
            )

        self.assertEqual(result["status"], "succeeded")
        self.assertEqual(
            post_calls[0]["json"]["threadUrn"],  # type: ignore[index]
            "urn:li:comment:(activity:7442650270389583872,7442658458996391936)",
        )
        refreshed = self.comment.list_comment_queue(
            post_url="https://www.linkedin.com/posts/alice-example_demo-post-activity-7441834639289360385-gWUt",
            state="replied",
        )[0]
        self.assertEqual(
            refreshed["metadata"]["comment_urn"],
            "urn:li:comment:(activity:7442650270389583872,7442658458996391936)",
        )
