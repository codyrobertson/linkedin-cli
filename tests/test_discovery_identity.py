from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from linkedin_cli.write import store


class DiscoveryIdentityTests(unittest.TestCase):
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

    def test_alias_resolution_merges_member_urn_and_public_slug(self) -> None:
        self.discovery.upsert_prospect(
            profile_key="john-doe",
            display_name="John Doe",
            public_identifier="john-doe",
            member_urn="urn:li:fsd_profile:abc123",
            profile_url="https://www.linkedin.com/in/john-doe/",
        )

        resolved = self.discovery.resolve_profile_key(
            public_identifier="john-doe",
            member_urn="urn:li:fsd_profile:abc123",
            profile_url="https://www.linkedin.com/in/john-doe/",
            display_name="John Doe",
        )

        self.assertEqual(resolved, "john-doe")

    def test_duplicate_sources_and_signals_are_deduped(self) -> None:
        self.discovery.upsert_prospect("john-doe", "John Doe", public_identifier="john-doe")

        self.discovery.add_source(
            "john-doe",
            source_type="search.query",
            source_value="fintech founder",
            dedupe_key="search:fintech founder:john-doe",
        )
        self.discovery.add_source(
            "john-doe",
            source_type="search.query",
            source_value="fintech founder",
            dedupe_key="search:fintech founder:john-doe",
        )
        self.discovery.add_signal(
            "john-doe",
            signal_type="commented",
            source="public",
            dedupe_key="post:1:comment:john-doe",
        )
        self.discovery.add_signal(
            "john-doe",
            signal_type="commented",
            source="public",
            dedupe_key="post:1:comment:john-doe",
        )

        prospect = self.discovery.get_prospect("john-doe")

        self.assertEqual(len(prospect["sources"]), 1)
        self.assertEqual(len(prospect["signals"]), 1)
