from __future__ import annotations

from linkedin_cli.output import quiet_value, render_table


def test_render_table_for_sequence_of_dicts() -> None:
    table = render_table(
        [
            {"action_id": "act_1", "state": "failed"},
            {"action_id": "act_2", "state": "succeeded"},
        ]
    )

    assert "action_id" in table
    assert "state" in table
    assert "act_1" in table
    assert "succeeded" in table


def test_quiet_value_prefers_identifiers_then_messages() -> None:
    assert quiet_value({"action_id": "act_123", "message": "planned"}) == "act_123"
    assert quiet_value({"message": "saved"}) == "saved"
