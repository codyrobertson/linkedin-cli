from __future__ import annotations

import pytest

from linkedin_cli.write.plans import (
    build_comment_plan,
    build_connect_plan,
    build_dm_plan,
    build_experience_plan,
    build_follow_plan,
    build_image_post_plan,
    build_profile_edit_plan,
)


def test_image_post_plan_uses_graphql_post_media_shape() -> None:
    plan = build_image_post_plan(
        account_id="1708250765",
        text="hello image",
        image_path="/tmp/image.jpg",
        image_size=123,
        image_filename="image.jpg",
        visibility="anyone",
    )

    publish_body = plan["live_request"]["steps"][2]["body"]

    assert plan["action_type"] == "post.image_publish"
    assert publish_body["variables"]["post"]["commentary"]["text"] == "hello image"
    assert publish_body["variables"]["post"]["media"] == [
        {"status": "READY", "media_urn_from": "register_upload.urn"}
    ]
    assert "specificContent" not in publish_body


def test_connect_plan_includes_custom_invitation_message() -> None:
    plan = build_connect_plan(
        account_id="1708250765",
        vanity_name="jane-doe",
        page_key="d_profile_view_base",
        member_urn="urn:li:fsd_profile:jane",
        message="  Saw your work on agent systems.  ",
    )

    assert plan["desired"]["message"] == "Saw your work on agent systems."
    assert plan["live_request"]["body"]["variables"]["message"] == "Saw your work on agent systems."


def test_follow_plan_uses_active_follow_state() -> None:
    plan = build_follow_plan(
        account_id="1708250765",
        target_member_urn="urn:li:fsd_profile:jane",
        page_key="d_profile_view_base",
        vanity_name="jane-doe",
    )

    assert plan["action_type"] == "follow"
    assert plan["live_request"]["body"]["variables"] == {
        "followState": "FOLLOW_ACTIVE",
        "to": "urn:li:fsd_profile:jane",
    }


def test_profile_edit_plan_targets_requested_field() -> None:
    plan = build_profile_edit_plan(
        account_id="1708250765",
        field="headline",
        value="Building durable agent systems",
        member_hash="abc123",
    )

    input_item = plan["live_request"]["body"]["variables"]["formElementInputs"][0]

    assert plan["action_type"] == "profile.edit.headline"
    assert input_item["formElementUrn"] == "urn:li:fsd_profileEditFormElement:(TOP_CARD,urn:li:fsd_profile:abc123,/headline)"
    assert input_item["formElementInputValues"] == [{"textInputValue": "Building durable agent systems"}]


def test_dm_plan_targets_existing_conversation() -> None:
    plan = build_dm_plan(
        account_id="1708250765",
        conversation_urn="urn:li:msg_conversation:(urn:li:fsd_profile:me,2-abcd)",
        message_text="Replying here",
        mailbox_urn="urn:li:fsd_profile:me",
    )

    body = plan["live_request"]["body"]

    assert body["conversationUrn"] == "urn:li:msg_conversation:(urn:li:fsd_profile:me,2-abcd)"
    assert body["hostRecipientUrns"] == []
    assert body["dedupeByClientGeneratedToken"] is True


def test_dm_plan_targets_new_recipient() -> None:
    plan = build_dm_plan(
        account_id="1708250765",
        recipient_urn="jane",
        message_text="Hello Jane",
        mailbox_urn="urn:li:fsd_profile:me",
    )

    body = plan["live_request"]["body"]

    assert "conversationUrn" not in body
    assert body["hostRecipientUrns"] == ["urn:li:fsd_profile:jane"]


def test_comment_plan_has_action_lifecycle_shape() -> None:
    plan = build_comment_plan(
        account_id="1708250765",
        post_url="https://www.linkedin.com/posts/example-activity-1",
        thread_urn="urn:li:ugcPost:1",
        text=" Useful point. ",
        activity_urn="urn:li:activity:1",
    )

    assert plan["action_type"] == "comment.post"
    assert plan["live_request"]["path"].startswith("/voyager/api/voyagerSocialDashNormComments")
    assert plan["live_request"]["body"]["threadUrn"] == "urn:li:ugcPost:1"
    assert plan["live_request"]["body"]["commentary"]["text"] == "Useful point."


def test_experience_plan_rejects_invalid_date_ranges() -> None:
    with pytest.raises(ValueError, match="end date"):
        build_experience_plan(
            account_id="1708250765",
            title="Operator",
            company="Acme",
            start_month=5,
            start_year=2026,
            end_month=4,
            end_year=2026,
        )
