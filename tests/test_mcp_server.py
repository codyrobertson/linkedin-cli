from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from linkedin_cli import mcp_server as mcp_mod
from linkedin_cli.mcp_server import LinkedInMCPServer


def _send(server: LinkedInMCPServer, payload: dict) -> dict:
    raw = server.handle_json_message(json.dumps(payload))
    assert raw is not None
    return json.loads(raw)


def test_mcp_server_initializes_lists_tools_and_calls_sandbox(tmp_path) -> None:
    server = LinkedInMCPServer(state_dir=tmp_path)
    try:
        init = _send(
            server,
            {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": {
                    "protocolVersion": "2025-06-18",
                    "capabilities": {},
                    "clientInfo": {"name": "pytest", "version": "1"},
                },
            },
        )
        assert init["result"]["capabilities"]["tools"]["listChanged"] is False

        listed = _send(server, {"jsonrpc": "2.0", "id": 2, "method": "tools/list"})
        tool_names = {tool["name"] for tool in listed["result"]["tools"]}
        assert "linkedin_sandbox_run_write_surface" in tool_names
        assert "linkedin_live_read_smoke" in tool_names
        assert "linkedin_live_publish_post" not in tool_names

        called = _send(
            server,
            {
                "jsonrpc": "2.0",
                "id": 3,
                "method": "tools/call",
                "params": {
                    "name": "linkedin_sandbox_publish_post",
                    "arguments": {"text": "hello from mcp", "visibility": "connections"},
                },
            },
        )

        result = called["result"]
        assert result["isError"] is False
        assert result["structuredContent"]["ok"] is True
        assert result["structuredContent"]["reconcile"]["reconciled"] is True
        assert result["structuredContent"]["state"]["posts"][0]["text"] == "hello from mcp"
    finally:
        server.close()


def test_mcp_server_lists_live_write_tools_when_enabled(tmp_path) -> None:
    server = LinkedInMCPServer(
        state_dir=tmp_path / "sandbox",
        enable_live_writes=True,
        live_state_dir=tmp_path / "live",
    )
    try:
        listed = _send(server, {"jsonrpc": "2.0", "id": 1, "method": "tools/list"})
        tool_names = {tool["name"] for tool in listed["result"]["tools"]}

        assert "linkedin_live_publish_post" in tool_names
        assert "linkedin_live_profile_edit" in tool_names
        assert "linkedin_live_experience_add" in tool_names
        assert "linkedin_live_connect" in tool_names
        assert "linkedin_live_follow" in tool_names
        assert "linkedin_live_send_dm" in tool_names
        assert "linkedin_live_comment" in tool_names
        assert "linkedin_live_action_health" in tool_names
        assert "linkedin_live_reconcile_action" in tool_names
    finally:
        server.close()


def test_mcp_server_live_write_tools_use_executor_when_enabled(monkeypatch, tmp_path) -> None:
    server = LinkedInMCPServer(
        state_dir=tmp_path / "sandbox",
        enable_live_writes=True,
        live_state_dir=tmp_path / "live",
    )
    monkeypatch.setattr(mcp_mod, "load_env_file", lambda: None)
    monkeypatch.setattr(mcp_mod, "load_session", lambda required=True: (server.sandbox.session, {}))
    monkeypatch.setattr(mcp_mod, "_get_account_id", lambda session: server.sandbox.account_id)
    monkeypatch.setattr(
        mcp_mod,
        "_resolve_mwlite_profile_context",
        lambda session, profile: {
            "slug": profile,
            "page_key": "profile_view_base",
            "member_urn": f"urn:li:fsd_profile:{profile}",
            "vanity_name": profile,
            "connection_state": "none",
            "message_locked": False,
        },
    )
    monkeypatch.delenv("LINKEDIN_WRITE_QUIET_HOURS", raising=False)
    monkeypatch.setenv("LINKEDIN_WRITE_MAX_HOURLY", "1000")
    monkeypatch.setenv("LINKEDIN_WRITE_MAX_DAILY", "1000")

    try:
        called = _send(
            server,
            {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "tools/call",
                "params": {
                    "name": "linkedin_live_publish_post",
                    "arguments": {"text": "hello from live mcp", "visibility": "connections"},
                },
            },
        )

        result = called["result"]
        structured = result["structuredContent"]
        assert result["isError"] is False
        assert structured["ok"] is True
        assert structured["live"] is True
        assert structured["dry_run"] is False
        assert structured["execution"]["status"] == "succeeded"
        assert structured["reconcile"]["reconciled"] is True
        assert server.sandbox.posts[0]["text"] == "hello from live mcp"

        profile_edit = _send(
            server,
            {
                "jsonrpc": "2.0",
                "id": 2,
                "method": "tools/call",
                "params": {
                    "name": "linkedin_live_profile_edit",
                    "arguments": {"field": "headline", "value": "Live MCP headline"},
                },
            },
        )
        assert profile_edit["result"]["structuredContent"]["execution"]["status"] == "succeeded"
        assert server.sandbox.profile["headline"] == "Live MCP headline"

        experience = _send(
            server,
            {
                "jsonrpc": "2.0",
                "id": 3,
                "method": "tools/call",
                "params": {
                    "name": "linkedin_live_experience_add",
                    "arguments": {"title": "Agent Operator", "company": "Live MCP Labs", "start_month": 4, "start_year": 2026},
                },
            },
        )
        assert experience["result"]["structuredContent"]["execution"]["status"] == "succeeded"
        assert server.sandbox.positions[0]["title"]["localized"]["en_US"] == "Agent Operator"

        connect = _send(
            server,
            {
                "jsonrpc": "2.0",
                "id": 4,
                "method": "tools/call",
                "params": {
                    "name": "linkedin_live_connect",
                    "arguments": {"profile": "jane-sandbox", "message": "hello"},
                },
            },
        )
        assert connect["result"]["structuredContent"]["execution"]["status"] == "succeeded"
        assert server.sandbox.target_profiles["jane-sandbox"]["connection_state"] == "pending"

        follow = _send(
            server,
            {
                "jsonrpc": "2.0",
                "id": 5,
                "method": "tools/call",
                "params": {"name": "linkedin_live_follow", "arguments": {"profile": "jane-sandbox"}},
            },
        )
        assert follow["result"]["structuredContent"]["execution"]["status"] == "succeeded"
        assert server.sandbox.target_profiles["jane-sandbox"]["follow_state"] == "following"

        dm = _send(
            server,
            {
                "jsonrpc": "2.0",
                "id": 6,
                "method": "tools/call",
                "params": {
                    "name": "linkedin_live_send_dm",
                    "arguments": {"conversation_urn": server.sandbox.default_conversation_urn, "message_text": "live mcp dm"},
                },
            },
        )
        assert dm["result"]["structuredContent"]["execution"]["status"] == "succeeded"
        assert server.sandbox.messages[0]["body"]["text"] == "live mcp dm"

        comment = _send(
            server,
            {
                "jsonrpc": "2.0",
                "id": 7,
                "method": "tools/call",
                "params": {
                    "name": "linkedin_live_comment",
                    "arguments": {"post_url": server.sandbox.posts[0]["url"], "text": "live mcp comment"},
                },
            },
        )
        assert comment["result"]["structuredContent"]["execution"]["status"] == "succeeded"
        comment_threads = list(server.sandbox.comments_by_thread.values())
        assert comment_threads[0][0]["commentary"]["text"] == "live mcp comment"

        assert (tmp_path / "live" / "state.sqlite").exists()
    finally:
        server.close()


def test_mcp_server_reports_tool_errors_inside_call_result(tmp_path) -> None:
    server = LinkedInMCPServer(state_dir=tmp_path)
    try:
        called = _send(
            server,
            {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "tools/call",
                "params": {
                    "name": "linkedin_sandbox_publish_post",
                    "arguments": {},
                },
            },
        )

        assert called["result"]["isError"] is True
        assert called["result"]["structuredContent"]["ok"] is False
        assert "text is required" in called["result"]["structuredContent"]["error"]
    finally:
        server.close()


def test_mcp_server_supports_stdio_subprocess(tmp_path) -> None:
    repo_root = Path(__file__).resolve().parents[1]
    proc = subprocess.Popen(
        [sys.executable, "-m", "linkedin_cli.mcp_server", "--state-dir", str(tmp_path)],
        cwd=str(repo_root),
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    assert proc.stdin is not None
    assert proc.stdout is not None
    try:
        proc.stdin.write(
            json.dumps(
                {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "initialize",
                    "params": {"protocolVersion": "2025-06-18", "capabilities": {}},
                }
            )
            + "\n"
        )
        proc.stdin.flush()
        init = json.loads(proc.stdout.readline())
        assert init["result"]["serverInfo"]["name"] == "linkedin-cli-sandbox"

        proc.stdin.write(
            json.dumps(
                {
                    "jsonrpc": "2.0",
                    "id": 2,
                    "method": "tools/call",
                    "params": {
                        "name": "linkedin_sandbox_run_write_surface",
                        "arguments": {"prefix": "stdio mcp", "reset": True},
                    },
                }
            )
            + "\n"
        )
        proc.stdin.flush()
        called = json.loads(proc.stdout.readline())
        assert called["result"]["isError"] is False
        assert called["result"]["structuredContent"]["ok"] is True
        assert called["result"]["structuredContent"]["state"]["posts"]
    finally:
        if proc.stdin:
            proc.stdin.close()
        proc.wait(timeout=10)
        stderr = proc.stderr.read() if proc.stderr else ""
        assert proc.returncode == 0, stderr
