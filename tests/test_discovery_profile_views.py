from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from linkedin_cli.write import store


PROFILE_VIEWS_PAYLOAD = {
    "data": {
        "data": {
            "premiumDashAnalyticsViewByAnalyticsEntity": {
                "elements": ["urn:li:fsd_edgeInsightsAnalyticsView:(WVMP,urn:li:wvmp:1)"]
            }
        }
    },
    "included": [
        {
            "$type": "com.linkedin.voyager.dash.edgeinsightsanalytics.Card",
            "entityUrn": "urn:li:fsd_edgeInsightsAnalyticsCard:(WVMP,urn:li:wvmp:1,ANALYTICS,SUMMARY,VIEWERS_COUNT)",
            "component": {
                "summary": {
                    "keyMetrics": {
                        "items": [
                            {
                                "title": {"text": "4"},
                                "description": {"text": "Profile viewers in the past 90 days"},
                            }
                        ]
                    }
                }
            },
        },
        {
            "$type": "com.linkedin.voyager.dash.edgeinsightsanalytics.View",
            "entityUrn": "urn:li:fsd_edgeInsightsAnalyticsView:(WVMP,urn:li:wvmp:1)",
            "title": {"text": "Who's viewed your profile"},
            "sections": [],
        },
        {
            "$type": "com.linkedin.voyager.dash.identity.profile.Profile",
            "entityUrn": "urn:li:fsd_profile:viewer-1",
            "publicIdentifier": "john-doe",
            "firstName": "John",
            "lastName": "Doe",
            "headline": "Founder at Acme",
            "wvmpProfileActions": {"message": True},
        },
        {
            "$type": "com.linkedin.voyager.dash.edgeinsightsanalytics.AnalyticsEntityLockup",
            "entityUrn": "urn:li:fsd_edgeInsightsAnalyticsEntityLockup:1",
            "blurred": False,
            "entityLockup": {
                "title": {"text": "John Doe"},
                "subtitle": {"text": "Founder at Acme"},
                "navigationUrl": {"url": "https://www.linkedin.com/in/john-doe/"},
                "actionData": {"entityProfile": "urn:li:fsd_profile:viewer-1"},
            },
        },
    ],
}


class DiscoveryProfileViewsTests(unittest.TestCase):
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

    def test_parse_profile_view_analytics_payload_extracts_summary_and_viewers(self) -> None:
        parsed = self.discovery.parse_profile_view_analytics_payload(PROFILE_VIEWS_PAYLOAD)

        self.assertEqual(parsed["viewer_count"], 4)
        self.assertEqual(parsed["available_viewer_count"], 1)
        self.assertEqual(parsed["view_title"], "Who's viewed your profile")
        self.assertEqual(parsed["viewers"][0]["public_identifier"], "john-doe")
        self.assertEqual(parsed["viewers"][0]["profile_url"], "https://www.linkedin.com/in/john-doe")

    def test_ingest_profile_view_analytics_creates_profile_view_signal(self) -> None:
        summary = self.discovery.ingest_profile_view_analytics("me", PROFILE_VIEWS_PAYLOAD)

        john = self.discovery.get_prospect("john-doe")

        self.assertEqual(summary["viewer_count"], 4)
        self.assertEqual(summary["ingested_count"], 1)
        self.assertEqual(john["signals"][0]["signal_type"], "profile_view")
