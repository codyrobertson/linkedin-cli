from __future__ import annotations

from linkedin_cli.cli import build_parser


def test_parser_accepts_discover_commands() -> None:
    parser = build_parser()

    ingest_search = parser.parse_args(
        ["discover", "ingest-search", "--kind", "people", "--query", "fintech founder", "--limit", "5"]
    )
    ingest_inbox = parser.parse_args(["discover", "ingest-inbox", "--limit", "10"])
    ingest_engagement = parser.parse_args(["discover", "ingest-engagement", "--target", "openai", "--limit", "3"])
    signal_add = parser.parse_args(
        ["discover", "signal", "add", "--profile", "john-doe", "--type", "commented", "--source", "public"]
    )
    state_set = parser.parse_args(["discover", "state", "set", "john-doe", "--state", "engaged"])
    queue = parser.parse_args(["discover", "queue", "--why"])
    show = parser.parse_args(["discover", "show", "john-doe"])
    stats = parser.parse_args(["discover", "stats"])

    assert ingest_search.discover_command == "ingest-search"
    assert ingest_inbox.discover_command == "ingest-inbox"
    assert ingest_engagement.discover_command == "ingest-engagement"
    assert signal_add.discover_command == "signal"
    assert signal_add.discover_signal_command == "add"
    assert state_set.discover_command == "state"
    assert state_set.discover_state_command == "set"
    assert queue.discover_command == "queue"
    assert show.discover_command == "show"
    assert stats.discover_command == "stats"
