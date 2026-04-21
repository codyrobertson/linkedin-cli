from __future__ import annotations

import os
import uuid

import pytest

from linkedin_cli.session import CliError
from linkedin_cli.session import load_env_file, load_session
from linkedin_cli.voyager import parse_json_response, voyager_get
from linkedin_cli.write.executor import execute_action
from linkedin_cli.write.plans import build_dm_plan, build_post_plan
from linkedin_cli.write.reconcile import reconcile_action


LIVE_CONFIRM = "I_UNDERSTAND_THIS_WRITES_TO_LINKEDIN"


def _require_live_read() -> None:
    if os.getenv("LINKEDIN_LIVE_E2E") != "1":
        pytest.skip("Set LINKEDIN_LIVE_E2E=1 to run authenticated live LinkedIn smoke tests")


def _require_live_write() -> None:
    _require_live_read()
    if os.getenv("LINKEDIN_LIVE_E2E_WRITE") != "1":
        pytest.skip("Set LINKEDIN_LIVE_E2E_WRITE=1 to run live write smoke tests")
    if os.getenv("LINKEDIN_LIVE_E2E_CONFIRM") != LIVE_CONFIRM:
        pytest.skip(f"Set LINKEDIN_LIVE_E2E_CONFIRM={LIVE_CONFIRM} to acknowledge live LinkedIn writes")


def _session_and_account():
    load_env_file()
    session, _ = load_session(required=True)
    try:
        data = parse_json_response(voyager_get(session, "/voyager/api/me"))
    except CliError as exc:
        pytest.fail(exc.message)
    me = data.get("data") or data
    account_id = str(me.get("plainId") or "")
    if not account_id:
        for item in data.get("included") or []:
            urn = item.get("entityUrn") if isinstance(item, dict) else ""
            if str(urn).startswith("urn:li:fs_miniProfile:"):
                account_id = str(urn).split(":")[-1]
                break
    if not account_id:
        pytest.fail("Could not resolve account id from /voyager/api/me")
    return session, account_id, data


def test_live_authenticated_voyager_me_smoke() -> None:
    _require_live_read()

    _session, account_id, data = _session_and_account()

    assert account_id
    assert data


def test_live_text_post_publish_and_reconcile() -> None:
    _require_live_write()
    session, account_id, _data = _session_and_account()
    text = os.getenv("LINKEDIN_LIVE_E2E_POST_TEXT", "").strip()
    if not text:
        pytest.skip("Set LINKEDIN_LIVE_E2E_POST_TEXT to the exact text to publish")
    text = f"{text}\n\nlive-smoke:{uuid.uuid4().hex[:8]}"
    plan = build_post_plan(account_id, text, visibility=os.getenv("LINKEDIN_LIVE_E2E_VISIBILITY", "connections"))
    action_id = f"act_live_{uuid.uuid4().hex[:12]}"

    result = execute_action(
        session=session,
        action_id=action_id,
        plan=plan,
        account_id=account_id,
        dry_run=False,
    )

    assert result["status"] == "succeeded"
    reconcile_result = reconcile_action(session, action_id)
    assert reconcile_result["reconciled"] is True


def test_live_dm_send_existing_conversation() -> None:
    _require_live_write()
    conversation_urn = os.getenv("LINKEDIN_LIVE_E2E_DM_CONVERSATION", "").strip()
    mailbox_urn = os.getenv("LINKEDIN_LIVE_E2E_MAILBOX_URN", "").strip()
    message = os.getenv("LINKEDIN_LIVE_E2E_DM_TEXT", "").strip()
    if not conversation_urn or not mailbox_urn or not message:
        pytest.skip("Set LINKEDIN_LIVE_E2E_DM_CONVERSATION, LINKEDIN_LIVE_E2E_MAILBOX_URN, and LINKEDIN_LIVE_E2E_DM_TEXT")
    session, account_id, _data = _session_and_account()
    message = f"{message} live-smoke:{uuid.uuid4().hex[:8]}"
    plan = build_dm_plan(
        account_id=account_id,
        conversation_urn=conversation_urn,
        message_text=message,
        mailbox_urn=mailbox_urn,
    )
    action_id = f"act_live_{uuid.uuid4().hex[:12]}"

    result = execute_action(
        session=session,
        action_id=action_id,
        plan=plan,
        account_id=account_id,
        dry_run=False,
    )

    assert result["status"] == "succeeded"
    reconcile_result = reconcile_action(session, action_id)
    assert reconcile_result["reconciled"] is True
