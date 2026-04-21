from __future__ import annotations

from pathlib import Path

from linkedin_cli.comment import extract_comment_context
from linkedin_cli.sandbox import LinkedInSandbox
from linkedin_cli.write import executor as executor_mod
from linkedin_cli.write import store
from linkedin_cli.write.executor import execute_action
from linkedin_cli.write.plans import (
    build_comment_plan,
    build_connect_plan,
    build_dm_plan,
    build_experience_plan,
    build_follow_plan,
    build_image_post_plan,
    build_post_plan,
    build_profile_edit_plan,
)
from linkedin_cli.write.reconcile import reconcile_action


def _configure_action_store(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(store, "DB_PATH", tmp_path / "state.sqlite")
    monkeypatch.setattr(store, "ARTIFACTS_DIR", tmp_path / "artifacts")
    monkeypatch.setattr(executor_mod, "LOCK_DIR", tmp_path / "locks")
    monkeypatch.setattr(executor_mod, "LOCK_FILE", tmp_path / "locks" / "account.lock")
    monkeypatch.setattr(executor_mod.time, "sleep", lambda _seconds: None)
    monkeypatch.setenv("LINKEDIN_WRITE_GUARDS", "0")
    store.init_db()


def _execute_and_reconcile(sandbox: LinkedInSandbox, action_id: str, plan: dict) -> tuple[dict, dict]:
    result = execute_action(
        session=sandbox.session,
        action_id=action_id,
        plan=plan,
        account_id=sandbox.account_id,
        dry_run=False,
    )
    assert result["status"] == "succeeded", result
    reconcile_result = reconcile_action(sandbox.session, action_id)
    assert reconcile_result["reconciled"] is True, reconcile_result
    return result, reconcile_result


def test_linkedin_sandbox_executes_and_reconciles_write_surface(monkeypatch, tmp_path) -> None:
    _configure_action_store(monkeypatch, tmp_path)
    sandbox = LinkedInSandbox()

    text_plan = build_post_plan(sandbox.account_id, "Sandbox text post", visibility="connections")
    _execute_and_reconcile(sandbox, "sbx_post_text", text_plan)
    text_post = sandbox.posts[0]
    assert text_post["text"] == "Sandbox text post"
    comment_context = extract_comment_context(text_post["url"], sandbox.session.get(text_post["url"]).text)
    assert comment_context["thread_urn"] == text_post["entityUrn"]
    assert comment_context["can_post_comments"] is True

    image_path = tmp_path / "sandbox-image.jpg"
    image_path.write_bytes(b"fake image bytes")
    image_plan = build_image_post_plan(
        sandbox.account_id,
        "Sandbox image post",
        str(image_path),
        image_path.stat().st_size,
        image_path.name,
        visibility="anyone",
    )
    _execute_and_reconcile(sandbox, "sbx_post_image", image_plan)
    assert sandbox.posts[0]["media"]
    assert any(upload["uploaded"] for upload in sandbox.uploads.values())

    profile_plan = build_profile_edit_plan(
        sandbox.account_id,
        "headline",
        "Building local LinkedIn sandbox coverage",
        member_hash=sandbox.member_hash,
    )
    _execute_and_reconcile(sandbox, "sbx_profile_headline", profile_plan)
    assert sandbox.profile["headline"] == "Building local LinkedIn sandbox coverage"

    experience_plan = build_experience_plan(
        sandbox.account_id,
        title="Autonomous Agent Operator",
        company="Sandbox Labs",
        start_month=4,
        start_year=2026,
    )
    _execute_and_reconcile(sandbox, "sbx_experience", experience_plan)
    assert sandbox.positions[0]["entityUrn"].startswith("urn:li:fsd_position:")

    target = sandbox.target_profiles["jane-sandbox"]
    connect_plan = build_connect_plan(
        sandbox.account_id,
        vanity_name="jane-sandbox",
        page_key="profile_view_base",
        member_urn=target["target_urn"],
        message="Testing from the local sandbox",
    )
    _execute_and_reconcile(sandbox, "sbx_connect", connect_plan)
    assert target["connection_state"] == "pending"

    follow_plan = build_follow_plan(
        sandbox.account_id,
        target_member_urn=target["target_urn"],
        page_key="profile_view_base",
        vanity_name="jane-sandbox",
    )
    _execute_and_reconcile(sandbox, "sbx_follow", follow_plan)
    assert target["follow_state"] == "following"

    dm_plan = build_dm_plan(
        sandbox.account_id,
        conversation_urn=sandbox.default_conversation_urn,
        message_text="Sandbox DM smoke",
        mailbox_urn=sandbox.mailbox_urn,
    )
    _execute_and_reconcile(sandbox, "sbx_dm", dm_plan)
    assert sandbox.messages[0]["body"]["text"] == "Sandbox DM smoke"

    comment_plan = build_comment_plan(
        sandbox.account_id,
        post_url=text_post["url"],
        thread_urn=text_post["entityUrn"],
        text="Sandbox comment smoke",
    )
    _execute_and_reconcile(sandbox, "sbx_comment", comment_plan)
    assert sandbox.comments_by_thread[text_post["entityUrn"]][0]["commentary"]["text"] == "Sandbox comment smoke"


def test_linkedin_sandbox_returns_http_errors_for_unknown_routes() -> None:
    sandbox = LinkedInSandbox()

    response = sandbox.session.get("https://www.linkedin.com/voyager/api/doesNotExist")

    assert response.status_code == 404
    assert "No sandbox route" in response.text
