from __future__ import annotations

from linkedin_cli.cli import build_parser


def test_parser_accepts_global_output_modes() -> None:
    parser = build_parser()
    args = parser.parse_args(["--table", "action", "list"])

    assert args.output_mode == "table"
    assert args.command == "action"
    assert args.action_command == "list"


def test_parser_accepts_doctor_command() -> None:
    parser = build_parser()
    args = parser.parse_args(["doctor"])

    assert args.command == "doctor"


def test_parser_accepts_richer_action_commands() -> None:
    parser = build_parser()

    reconcile_args = parser.parse_args(["action", "reconcile", "act_123"])
    cancel_args = parser.parse_args(["action", "cancel", "act_123"])
    artifact_args = parser.parse_args(["action", "artifacts", "act_123"])

    assert reconcile_args.action_command == "reconcile"
    assert cancel_args.action_command == "cancel"
    assert artifact_args.action_command == "artifacts"


def test_parser_accepts_workflow_commands() -> None:
    parser = build_parser()

    save_args = parser.parse_args(
        ["workflow", "search", "save", "--name", "founders", "--kind", "people", "--query", "fintech founder"]
    )
    template_args = parser.parse_args(
        ["workflow", "template", "save", "--name", "intro", "--kind", "dm", "--body", "Hi {name}"]
    )
    contact_args = parser.parse_args(
        ["workflow", "contact", "upsert", "--profile", "john-doe", "--name", "John Doe", "--stage", "new"]
    )
    inbox_args = parser.parse_args(
        ["workflow", "inbox", "upsert", "--conversation", "urn:li:msg_conversation:1", "--state", "follow_up"]
    )

    assert save_args.workflow_command == "search"
    assert save_args.workflow_search_command == "save"
    assert template_args.workflow_command == "template"
    assert template_args.workflow_template_command == "save"
    assert contact_args.workflow_command == "contact"
    assert contact_args.workflow_contact_command == "upsert"
    assert inbox_args.workflow_command == "inbox"
    assert inbox_args.workflow_inbox_command == "upsert"
