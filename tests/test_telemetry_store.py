from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from linkedin_cli.write import store


class TelemetryStoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tempdir.name) / "state.sqlite"
        self.db_patcher = patch.object(store, "DB_PATH", self.db_path)
        self.db_patcher.start()
        store.init_db()

    def tearDown(self) -> None:
        self.db_patcher.stop()
        self.tempdir.cleanup()

    def test_append_telemetry_event_dedupes_by_key(self) -> None:
        first = store.append_telemetry_event(
            entity_kind="post",
            entity_key="url-1",
            event_type="reaction_snapshot",
            dedupe_key="dedupe-1",
            payload={"count": 5},
            source="test",
        )
        second = store.append_telemetry_event(
            entity_kind="post",
            entity_key="url-1",
            event_type="reaction_snapshot",
            dedupe_key="dedupe-1",
            payload={"count": 5},
            source="test",
        )

        self.assertEqual(first["id"], second["id"])
        self.assertEqual(len(store.list_telemetry_events()), 1)

    def test_telemetry_stats_counts_events_by_type(self) -> None:
        store.append_telemetry_event("post", "url-1", "reaction_snapshot", "snap-1", {"count": 5}, source="test")
        store.append_telemetry_event("lead", "john-doe", "reply", "reply-1", {"reply": True}, source="test")

        stats = store.telemetry_stats()

        self.assertEqual(stats["event_count"], 2)
        self.assertEqual(stats["by_event_type"]["reaction_snapshot"], 1)
        self.assertEqual(stats["by_entity_kind"]["lead"], 1)

    def test_connect_sets_nonzero_busy_timeout(self) -> None:
        conn = store._connect()
        try:
            timeout_ms = int(conn.execute("PRAGMA busy_timeout").fetchone()[0] or 0)
        finally:
            conn.close()

        self.assertGreaterEqual(timeout_ms, 5000)
