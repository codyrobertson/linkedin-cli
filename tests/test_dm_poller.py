from __future__ import annotations

import json

from linkedin_cli.integrations import dm_poller


def test_notify_discord_uses_configured_inbox_channel(monkeypatch) -> None:
    monkeypatch.setenv("DISCORD_INBOX_CHANNEL", "https://discord.example/webhook")
    monkeypatch.delenv("EMAIL_WEBHOOK_URL", raising=False)
    calls = []

    def fake_urlopen(request, timeout: int):
        calls.append((request, timeout))

        class _Response:
            status = 204

        return _Response()

    monkeypatch.setattr(dm_poller.urllib.request, "urlopen", fake_urlopen)

    summary = dm_poller.notify_discord(
        [
            {
                "sender_urn": "urn:li:fsd_profile:jane",
                "text": "Can you send details?",
                "created_at": 1,
                "message_urn": "urn:li:msg:1",
            }
        ]
    )

    request, timeout = calls[0]
    payload = json.loads(request.data.decode())
    assert summary == {"attempted": 1, "delivered": 1, "failed": 0}
    assert request.full_url == "https://discord.example/webhook"
    assert timeout == 10
    assert "New LinkedIn DM" in payload["content"]


def test_extract_new_messages_filters_old_and_non_messages() -> None:
    data = {
        "included": [
            {"$type": "com.linkedin.voyager.messaging.Message", "createdAt": 100, "body": {"text": "old"}},
            {
                "$type": "com.linkedin.voyager.messaging.Message",
                "createdAt": 200,
                "body": {"text": "new"},
                "*sender": "urn:li:fsd_profile:jane",
                "entityUrn": "urn:li:msg:2",
            },
            {"$type": "com.linkedin.voyager.messaging.MessageDelivery", "createdAt": 300, "body": {"text": "skip"}},
        ]
    }

    messages = dm_poller.extract_new_messages(data, last_ts=150)

    assert messages == [
        {
            "text": "new",
            "sender_urn": "urn:li:fsd_profile:jane",
            "created_at": 200,
            "message_urn": "urn:li:msg:2",
        }
    ]
