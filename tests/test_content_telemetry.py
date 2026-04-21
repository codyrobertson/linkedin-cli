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
        <title>Ship the workflow, not the demo | Claude Mackenzie</title>
        <script type="application/ld+json">
          {
            "@context": "http://schema.org",
            "@type": "SocialMediaPosting",
            "@id": "https://www.linkedin.com/posts/claude-mackenzie_demo-activity-1",
            "headline": "Ship the workflow, not the demo",
            "articleBody": "Ship the workflow, not the demo.\\n\\n1. Start with the ops pain.\\n2. Remove a handoff.\\n3. Measure the hours saved.",
            "author": {
              "@type": "Person",
              "name": "Claude Mackenzie",
              "url": "https://www.linkedin.com/in/claude-mackenzie-06510b3b8/"
            },
            "interactionStatistic": [
              {"@type": "InteractionCounter", "interactionType": "http://schema.org/LikeAction", "userInteractionCount": 75},
              {"@type": "InteractionCounter", "interactionType": "http://schema.org/CommentAction", "userInteractionCount": 12}
            ]
          }
        </script>
      </head>
      <body></body>
    </html>
    """
)


class ContentTelemetryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tempdir.name) / "state.sqlite"
        self.db_patcher = patch.object(store, "DB_PATH", self.db_path)
        self.db_patcher.start()
        store.init_db()

        from linkedin_cli import content

        self.content = content
        self.content.init_content_db()

    def tearDown(self) -> None:
        self.db_patcher.stop()
        self.tempdir.cleanup()

    def test_sync_owned_post_telemetry_records_snapshot_and_events(self) -> None:
        record = self.content.extract_post_record(
            url="https://www.linkedin.com/posts/claude-mackenzie_demo-activity-1",
            html=PUBLIC_POST_HTML,
            source_query='site:linkedin.com/posts "workflow"',
        )
        record["industry"] = "ai"
        record["owned_by_me"] = True
        self.content.upsert_post(record)

        summary = self.content.sync_owned_post_telemetry(
            urls=["https://www.linkedin.com/posts/claude-mackenzie_demo-activity-1"],
            fetch_html=lambda _url: PUBLIC_POST_HTML,
        )

        self.assertEqual(summary["synced_count"], 1)
        self.assertGreaterEqual(summary["events_written"], 1)
        self.assertEqual(summary["snapshots"][0]["reaction_count"], 75)
