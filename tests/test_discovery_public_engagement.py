from __future__ import annotations

import tempfile
import textwrap
import unittest
from pathlib import Path
from unittest.mock import patch

from linkedin_cli.write import store


PUBLIC_POST_HTML = textwrap.dedent(
    """
    <html>
      <head>
        <title>Example Post | Alice Example | 12 comments</title>
        <script type="application/ld+json">
          {
            "@context": "http://schema.org",
            "@type": "SocialMediaPosting",
            "@id": "https://www.linkedin.com/posts/alice-example_demo-post-activity-1",
            "headline": "Example post",
            "interactionStatistic": [
              {"@type": "InteractionCounter", "interactionType": "http://schema.org/LikeAction", "userInteractionCount": 25},
              {"@type": "InteractionCounter", "interactionType": "http://schema.org/CommentAction", "userInteractionCount": 12}
            ],
            "comment": [
              {"@type": "Comment", "text": "Great post", "author": {"@type": "Person", "name": "John Doe", "url": "https://www.linkedin.com/in/john-doe/" }},
              {"@type": "Comment", "text": "Interesting", "author": {"@type": "Organization", "name": "Acme Corp", "url": "https://www.linkedin.com/company/acme/" }}
            ]
          }
        </script>
      </head>
      <body>
        <a href="https://www.linkedin.com/in/john-doe/?trk=public_post_comment_actor-name" data-tracking-control-name="public_post_comment_actor-name">John Doe</a>
        <a href="https://www.linkedin.com/company/acme/?trk=public_post_comment_actor-name" data-tracking-control-name="public_post_comment_actor-name">Acme Corp</a>
      </body>
    </html>
    """
)


class DiscoveryPublicEngagementTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tempdir.name) / "state.sqlite"
        self.db_patcher = patch.object(store, "DB_PATH", self.db_path)
        self.db_patcher.start()
        store.init_db()

        from linkedin_cli import discovery

        self.discovery = discovery
        self.discovery.init_discovery_db()

    def tearDown(self) -> None:
        self.db_patcher.stop()
        self.tempdir.cleanup()

    def test_public_post_html_ingests_commenters_and_counts(self) -> None:
        summary = self.discovery.ingest_public_post_engagement(
            target_key="alice-example",
            post_url="https://www.linkedin.com/posts/alice-example_demo-post-activity-1",
            html=PUBLIC_POST_HTML,
        )

        queue = self.discovery.list_queue(limit=10)
        john = self.discovery.get_prospect("john-doe")

        self.assertEqual(summary["commenter_count"], 2)
        self.assertEqual(summary["reaction_count"], 25)
        self.assertTrue(any(item["profile_key"] == "john-doe" for item in queue))
        self.assertEqual(john["signals"][0]["signal_type"], "commented")
