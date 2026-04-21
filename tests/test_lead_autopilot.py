from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

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
        "interactionStatistic": [
          {"@type": "InteractionCounter", "interactionType": "http://schema.org/LikeAction", "userInteractionCount": 25},
          {"@type": "InteractionCounter", "interactionType": "http://schema.org/CommentAction", "userInteractionCount": 12}
        ],
        "comment": [
          {"@type": "Comment", "text": "Great post", "author": {"@type": "Person", "name": "John Doe", "url": "https://www.linkedin.com/in/john-doe/" }}
        ]
      }
    </script>
  </head>
  <body>
    <a href="https://www.linkedin.com/in/john-doe/?trk=public_post_comment_actor-name" data-tracking-control-name="public_post_comment_actor-name">John Doe</a>
    <a href="https://www.linkedin.com/in/jane-doe/?trk=reaction_actor" data-tracking-control-name="public_post_reaction_actor-name">Jane Doe</a>
  </body>
</html>
"""


class LeadAutopilotTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tempdir.name) / "state.sqlite"
        self.db_patcher = patch.object(store, "DB_PATH", self.db_path)
        self.db_patcher.start()
        store.init_db()

        from linkedin_cli import discovery, lead, workflow

        self.discovery = discovery
        self.lead = lead
        self.workflow = workflow
        self.discovery.init_discovery_db()
        self.lead.init_lead_db()
        self.workflow.init_workflow_db()

    def tearDown(self) -> None:
        self.db_patcher.stop()
        self.tempdir.cleanup()

    def test_autopilot_routes_high_fit_engagers_into_ready_state(self) -> None:
        self.discovery.upsert_prospect(
            profile_key="john-doe",
            display_name="John Doe",
            public_identifier="john-doe",
            headline="Founder building AI workflow tooling for ops teams",
            company="Acme AI",
        )
        self.discovery.add_signal("john-doe", signal_type="commented", source="public")
        self.discovery.add_signal("john-doe", signal_type="profile_view", source="analytics")
        self.discovery.add_signal("john-doe", signal_type="replied_dm", source="inbox")
        self.discovery.upsert_prospect(
            profile_key="jane-lowfit",
            display_name="Jane Lowfit",
            public_identifier="jane-lowfit",
            headline="Generalist consultant",
            company="Services Co",
        )

        summary = self.lead.run_autopilot(
            target_topics=["ai", "workflow", "ops"],
            limit=10,
            min_fit=0.25,
            min_reply=0.25,
            min_deal=0.2,
            sync_contacts=True,
            dry_run=False,
        )
        ranked = self.lead.rank_leads(limit=10)
        updated = self.discovery.get_prospect("john-doe")
        contact = self.workflow.get_contact("john-doe")

        self.assertEqual(summary["routed"]["ready"], 1)
        self.assertEqual(summary["recommendations"][0]["recommended_state"], "ready")
        self.assertGreater(summary["recommendations"][0]["reply_likelihood"], 0.5)
        self.assertEqual(ranked[0]["profile_key"], "john-doe")
        self.assertIsNotNone(updated)
        self.assertGreater(updated["fit_score"], 0)
        self.assertGreater(updated["reply_likelihood"], 0)
        self.assertIsNotNone(contact)
        self.assertEqual(contact["stage"], "qualified")

    def test_autopilot_can_source_visible_engagers_from_post_urls(self) -> None:
        summary = self.lead.run_autopilot(
            target_topics=["ai"],
            post_urls=["https://www.linkedin.com/posts/alice-example_demo-post-activity-1"],
            fetch_html=lambda _url: PUBLIC_POST_HTML,
            limit=10,
            min_fit=0.1,
            min_reply=0.1,
            sync_contacts=True,
            dry_run=False,
        )

        ranked = self.lead.rank_leads(limit=10)
        john = self.discovery.get_prospect("john-doe")

        self.assertEqual(summary["ingested_posts"], 1)
        self.assertGreaterEqual(summary["count"], 1)
        self.assertTrue(any(item["profile_key"] == "john-doe" for item in ranked))
        self.assertIsNotNone(john)
        self.assertGreater(john["fit_score"], 0)
