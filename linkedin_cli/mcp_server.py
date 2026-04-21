"""MCP stdio server exposing LinkedIn sandbox tools.

The implementation is intentionally dependency-light. It speaks the small MCP
surface needed by agent clients over line-delimited JSON-RPC stdio:
initialize, ping, tools/list, tools/call, and initialized notifications.
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import sys
import tempfile
import time
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Callable, Iterator

from linkedin_cli.config import CONFIG_DIR
from linkedin_cli.comment import publish_post_comment
from linkedin_cli.sandbox import LinkedInSandbox
from linkedin_cli.session import CliError, load_env_file, load_session
from linkedin_cli.voyager import parse_json_response, voyager_get
from linkedin_cli.write import executor as executor_mod
from linkedin_cli.write import store
from linkedin_cli.write.executor import execute_action
from linkedin_cli.write.guards import action_health_report
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

from linkedin_cli.cli import (
    _get_account_id,
    _get_my_member_hash,
    _get_my_urn,
    _resolve_mwlite_profile_context,
    _resolve_profile_urn,
)


PROTOCOL_VERSION = "2025-06-18"
SUPPORTED_PROTOCOL_VERSIONS = {"2025-11-25", "2025-06-18", "2025-03-26", "2024-11-05"}
SERVER_NAME = "linkedin-cli-sandbox"
SERVER_VERSION = "0.1.0"


JsonDict = dict[str, Any]
ToolHandler = Callable[[JsonDict], JsonDict]


class MCPError(Exception):
    def __init__(self, code: int, message: str, data: Any | None = None) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.data = data


def _schema_string(description: str, *, default: str | None = None, enum: list[str] | None = None) -> JsonDict:
    schema: JsonDict = {"type": "string", "description": description}
    if default is not None:
        schema["default"] = default
    if enum:
        schema["enum"] = enum
    return schema


def _schema_bool(description: str, *, default: bool = False) -> JsonDict:
    return {"type": "boolean", "description": description, "default": default}


def _schema_int(description: str, *, minimum: int | None = None, maximum: int | None = None) -> JsonDict:
    schema: JsonDict = {"type": "integer", "description": description}
    if minimum is not None:
        schema["minimum"] = minimum
    if maximum is not None:
        schema["maximum"] = maximum
    return schema


def _text_result(text: str, structured: JsonDict, *, is_error: bool = False) -> JsonDict:
    return {
        "content": [{"type": "text", "text": text}],
        "structuredContent": structured,
        "isError": is_error,
    }


@contextmanager
def _temporary_env(updates: dict[str, str]) -> Iterator[None]:
    previous = {key: os.environ.get(key) for key in updates}
    try:
        os.environ.update(updates)
        yield
    finally:
        for key, value in previous.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


class LinkedInMCPServer:
    def __init__(
        self,
        *,
        state_dir: Path | None = None,
        enable_live_writes: bool = False,
        live_state_dir: Path | None = None,
    ) -> None:
        self._owned_tempdir: tempfile.TemporaryDirectory[str] | None = None
        if state_dir is None:
            self._owned_tempdir = tempfile.TemporaryDirectory(prefix="linkedin-mcp-sandbox-")
            state_dir = Path(self._owned_tempdir.name)
        self.state_dir = state_dir
        self.live_state_dir = live_state_dir or CONFIG_DIR
        self.enable_live_writes = enable_live_writes
        self.sandbox = LinkedInSandbox()
        self._configure_action_store(live=False)
        self.tools: dict[str, tuple[JsonDict, ToolHandler]] = self._build_tools()

    def close(self) -> None:
        if self._owned_tempdir is not None:
            self._owned_tempdir.cleanup()
            self._owned_tempdir = None

    def _configure_action_store(self, *, live: bool) -> None:
        base_dir = self.live_state_dir if live else self.state_dir
        base_dir.mkdir(parents=True, exist_ok=True)
        store.DB_PATH = base_dir / "state.sqlite"
        store.ARTIFACTS_DIR = base_dir / "artifacts"
        executor_mod.LOCK_DIR = base_dir / "locks"
        executor_mod.LOCK_FILE = executor_mod.LOCK_DIR / "account.lock"
        executor_mod.time.sleep = lambda _seconds: None
        store.init_db()

    def reset(self, args: JsonDict | None = None) -> JsonDict:
        args = args or {}
        self._configure_action_store(live=False)
        self.sandbox = LinkedInSandbox(
            account_id=str(args.get("account_id") or "1708250765"),
            member_hash=str(args.get("member_hash") or "sandbox-member"),
            public_identifier=str(args.get("public_identifier") or "sandbox-user"),
        )
        if store.DB_PATH.exists():
            store.DB_PATH.unlink()
        store.init_db()
        return self._state_payload()

    def _action_id(self, prefix: str) -> str:
        return f"mcp_{prefix}_{uuid.uuid4().hex[:12]}"

    def _execute_and_reconcile(self, action_id: str, plan: JsonDict) -> JsonDict:
        self._configure_action_store(live=False)
        with _temporary_env({"LINKEDIN_WRITE_GUARDS": "0"}):
            result = execute_action(
                session=self.sandbox.session,
                action_id=action_id,
                plan=plan,
                account_id=self.sandbox.account_id,
                dry_run=False,
            )
        reconcile_result: JsonDict | None = None
        if result.get("status") == "succeeded":
            reconcile_result = reconcile_action(self.sandbox.session, action_id)
        return {
            "action_id": action_id,
            "execution": _compact_execution_result(result),
            "reconcile": reconcile_result,
            "state": self._state_payload(),
        }

    def _load_live_session_and_account(self) -> tuple[Any, str]:
        self._configure_action_store(live=True)
        load_env_file()
        session, _meta = load_session(required=True)
        assert session is not None
        return session, _get_account_id(session)

    def _execute_live_plan(
        self,
        *,
        session: Any,
        account_id: str,
        action_id: str,
        plan: JsonDict,
        dry_run: bool = False,
    ) -> JsonDict:
        self._configure_action_store(live=True)
        result = execute_action(
            session=session,
            action_id=action_id,
            plan=plan,
            account_id=account_id,
            dry_run=dry_run,
        )
        reconcile_result: JsonDict | None = None
        if result.get("status") == "succeeded" and not dry_run:
            try:
                reconcile_result = reconcile_action(session, action_id)
            except Exception as exc:
                reconcile_result = {"reconciled": False, "error": str(exc)}
        return {
            "action_id": action_id,
            "execution": _compact_execution_result(result),
            "reconcile": reconcile_result,
            "live": True,
            "dry_run": dry_run,
            "action_health": action_health_report(),
        }

    def _state_payload(self) -> JsonDict:
        self._configure_action_store(live=False)
        return {
            "account": {
                "account_id": self.sandbox.account_id,
                "profile_urn": self.sandbox.profile_urn,
                "mailbox_urn": self.sandbox.mailbox_urn,
                "public_identifier": self.sandbox.public_identifier,
                "default_conversation_urn": self.sandbox.default_conversation_urn,
            },
            "profile": self.sandbox.profile,
            "positions": self.sandbox.positions,
            "posts": self.sandbox.posts,
            "messages": self.sandbox.messages,
            "comments_by_thread": self.sandbox.comments_by_thread,
            "target_profiles": self.sandbox.target_profiles,
            "uploads": {
                key: {k: v for k, v in value.items() if k != "data"}
                for key, value in self.sandbox.uploads.items()
            },
            "requests": self.sandbox.requests[-25:],
            "action_health": action_health_report(),
        }

    def _build_tools(self) -> dict[str, tuple[JsonDict, ToolHandler]]:
        tools: dict[str, tuple[JsonDict, ToolHandler]] = {
            "linkedin_sandbox_reset": (
                {
                    "name": "linkedin_sandbox_reset",
                    "title": "Reset LinkedIn Sandbox",
                    "description": "Reset the in-memory LinkedIn-like sandbox and local action store. Safe: no real network or LinkedIn writes.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "account_id": _schema_string("Sandbox account id.", default="1708250765"),
                            "member_hash": _schema_string("Sandbox fsd_profile hash.", default="sandbox-member"),
                            "public_identifier": _schema_string("Sandbox public profile identifier.", default="sandbox-user"),
                        },
                        "additionalProperties": False,
                    },
                    "annotations": {"destructiveHint": False, "openWorldHint": False},
                },
                self.tool_reset,
            ),
            "linkedin_sandbox_state": (
                {
                    "name": "linkedin_sandbox_state",
                    "title": "Inspect LinkedIn Sandbox",
                    "description": "Return current sandbox profile, posts, messages, comments, request log, and action health.",
                    "inputSchema": {"type": "object", "additionalProperties": False},
                    "annotations": {"readOnlyHint": True, "openWorldHint": False},
                },
                self.tool_state,
            ),
            "linkedin_sandbox_publish_post": (
                {
                    "name": "linkedin_sandbox_publish_post",
                    "title": "Sandbox Publish Post",
                    "description": "Publish a text or image post into the sandbox through the real planner, executor, store, and reconciler.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "text": _schema_string("Post text to publish in the sandbox."),
                            "visibility": _schema_string("Post visibility.", default="connections", enum=["connections", "anyone", "public"]),
                            "image_base64": _schema_string("Optional base64 image bytes for image-post flow."),
                            "image_filename": _schema_string("Filename for image-post flow.", default="sandbox-image.jpg"),
                        },
                        "required": ["text"],
                        "additionalProperties": False,
                    },
                    "annotations": {"destructiveHint": False, "openWorldHint": False},
                },
                self.tool_publish_post,
            ),
            "linkedin_sandbox_profile_edit": (
                {
                    "name": "linkedin_sandbox_profile_edit",
                    "title": "Sandbox Profile Edit",
                    "description": "Edit a sandbox profile field through the real profile edit plan and executor.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "field": _schema_string("Profile field to edit.", enum=["headline", "about", "website", "location"]),
                            "value": _schema_string("Desired sandbox profile value."),
                        },
                        "required": ["field", "value"],
                        "additionalProperties": False,
                    },
                    "annotations": {"destructiveHint": False, "openWorldHint": False},
                },
                self.tool_profile_edit,
            ),
            "linkedin_sandbox_experience_add": (
                {
                    "name": "linkedin_sandbox_experience_add",
                    "title": "Sandbox Experience Add",
                    "description": "Add a sandbox experience entry through the real experience plan and executor.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "title": _schema_string("Experience title."),
                            "company": _schema_string("Company name."),
                            "description": _schema_string("Optional description."),
                            "location": _schema_string("Optional location."),
                            "start_month": _schema_int("Optional start month.", minimum=1, maximum=12),
                            "start_year": _schema_int("Optional start year.", minimum=1900),
                            "end_month": _schema_int("Optional end month.", minimum=1, maximum=12),
                            "end_year": _schema_int("Optional end year.", minimum=1900),
                        },
                        "required": ["title", "company"],
                        "additionalProperties": False,
                    },
                    "annotations": {"destructiveHint": False, "openWorldHint": False},
                },
                self.tool_experience_add,
            ),
            "linkedin_sandbox_connect_follow": (
                {
                    "name": "linkedin_sandbox_connect_follow",
                    "title": "Sandbox Connect Follow",
                    "description": "Send a sandbox connect invite and/or follow mutation for a target profile.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "vanity_name": _schema_string("Target public profile slug.", default="jane-sandbox"),
                            "target_urn": _schema_string("Optional target profile URN."),
                            "message": _schema_string("Optional connection note."),
                            "connect": _schema_bool("Whether to send a connect invite.", default=True),
                            "follow": _schema_bool("Whether to follow the profile.", default=True),
                        },
                        "additionalProperties": False,
                    },
                    "annotations": {"destructiveHint": False, "openWorldHint": False},
                },
                self.tool_connect_follow,
            ),
            "linkedin_sandbox_send_dm": (
                {
                    "name": "linkedin_sandbox_send_dm",
                    "title": "Sandbox Send DM",
                    "description": "Send a sandbox DM through the real DM plan and executor.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "message_text": _schema_string("DM text."),
                            "conversation_urn": _schema_string("Existing conversation URN. Defaults to sandbox thread."),
                            "recipient_urn": _schema_string("Optional recipient URN for new-conversation flow."),
                        },
                        "required": ["message_text"],
                        "additionalProperties": False,
                    },
                    "annotations": {"destructiveHint": False, "openWorldHint": False},
                },
                self.tool_send_dm,
            ),
            "linkedin_sandbox_comment": (
                {
                    "name": "linkedin_sandbox_comment",
                    "title": "Sandbox Comment",
                    "description": "Post a sandbox comment against a sandbox post through the real comment plan and executor.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "text": _schema_string("Comment text."),
                            "post_url": _schema_string("Sandbox post URL. Defaults to latest sandbox post."),
                            "thread_urn": _schema_string("Sandbox thread URN. Defaults to latest sandbox post entity URN."),
                        },
                        "required": ["text"],
                        "additionalProperties": False,
                    },
                    "annotations": {"destructiveHint": False, "openWorldHint": False},
                },
                self.tool_comment,
            ),
            "linkedin_sandbox_run_write_surface": (
                {
                    "name": "linkedin_sandbox_run_write_surface",
                    "title": "Run Sandbox Write Surface",
                    "description": "Run a complete sandbox smoke covering post, image post, profile edit, experience, connect, follow, DM, and comment.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "prefix": _schema_string("Text prefix for generated sandbox writes.", default="MCP sandbox"),
                            "reset": _schema_bool("Reset sandbox before running.", default=True),
                        },
                        "additionalProperties": False,
                    },
                    "annotations": {"destructiveHint": False, "openWorldHint": False},
                },
                self.tool_run_write_surface,
            ),
            "linkedin_live_read_smoke": (
                {
                    "name": "linkedin_live_read_smoke",
                    "title": "Live LinkedIn Read Smoke",
                    "description": "Read-only live LinkedIn session smoke. Checks /voyager/api/me using the saved session. Never writes.",
                    "inputSchema": {"type": "object", "additionalProperties": False},
                    "annotations": {"readOnlyHint": True, "openWorldHint": True},
                },
                self.tool_live_read_smoke,
            ),
        }
        if self.enable_live_writes:
            tools.update(self._build_live_write_tools())
        return tools

    def _build_live_write_tools(self) -> dict[str, tuple[JsonDict, ToolHandler]]:
        write_annotations = {"destructiveHint": True, "openWorldHint": True}
        return {
            "linkedin_live_publish_post": (
                {
                    "name": "linkedin_live_publish_post",
                    "title": "Live Publish Post",
                    "description": "Publish a real LinkedIn text or image post through the planner, executor, action store, write lock, and reconciler.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "text": _schema_string("Post text to publish."),
                            "visibility": _schema_string("Post visibility.", default="connections", enum=["connections", "anyone", "public"]),
                            "image_base64": _schema_string("Optional base64 image bytes."),
                            "image_filename": _schema_string("Filename for image upload.", default="mcp-image.jpg"),
                            "image_path": _schema_string("Optional existing local image path. Ignored when image_base64 is provided."),
                            "dry_run": _schema_bool("Plan and persist the action without sending the live write.", default=False),
                        },
                        "required": ["text"],
                        "additionalProperties": False,
                    },
                    "annotations": write_annotations,
                },
                self.tool_live_publish_post,
            ),
            "linkedin_live_profile_edit": (
                {
                    "name": "linkedin_live_profile_edit",
                    "title": "Live Profile Edit",
                    "description": "Edit the authenticated LinkedIn profile headline, about, website, or location through the live executor.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "field": _schema_string("Profile field to edit.", enum=["headline", "about", "website", "location"]),
                            "value": _schema_string("New field value."),
                            "dry_run": _schema_bool("Plan and persist the action without sending the live write.", default=False),
                        },
                        "required": ["field", "value"],
                        "additionalProperties": False,
                    },
                    "annotations": write_annotations,
                },
                self.tool_live_profile_edit,
            ),
            "linkedin_live_experience_add": (
                {
                    "name": "linkedin_live_experience_add",
                    "title": "Live Experience Add",
                    "description": "Add a real LinkedIn experience entry through the live executor.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "title": _schema_string("Experience title."),
                            "company": _schema_string("Company name."),
                            "description": _schema_string("Optional description."),
                            "location": _schema_string("Optional location."),
                            "start_month": _schema_int("Optional start month.", minimum=1, maximum=12),
                            "start_year": _schema_int("Optional start year.", minimum=1900),
                            "end_month": _schema_int("Optional end month.", minimum=1, maximum=12),
                            "end_year": _schema_int("Optional end year.", minimum=1900),
                            "dry_run": _schema_bool("Plan and persist the action without sending the live write.", default=False),
                        },
                        "required": ["title", "company"],
                        "additionalProperties": False,
                    },
                    "annotations": write_annotations,
                },
                self.tool_live_experience_add,
            ),
            "linkedin_live_connect": (
                {
                    "name": "linkedin_live_connect",
                    "title": "Live Connect",
                    "description": "Send a real LinkedIn connection invite to a profile URL or slug.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "profile": _schema_string("Target LinkedIn profile URL or public slug."),
                            "message": _schema_string("Optional connection note."),
                            "dry_run": _schema_bool("Plan and persist the action without sending the live write.", default=False),
                        },
                        "required": ["profile"],
                        "additionalProperties": False,
                    },
                    "annotations": write_annotations,
                },
                self.tool_live_connect,
            ),
            "linkedin_live_follow": (
                {
                    "name": "linkedin_live_follow",
                    "title": "Live Follow",
                    "description": "Follow a real LinkedIn profile URL or slug.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "profile": _schema_string("Target LinkedIn profile URL or public slug."),
                            "dry_run": _schema_bool("Plan and persist the action without sending the live write.", default=False),
                        },
                        "required": ["profile"],
                        "additionalProperties": False,
                    },
                    "annotations": write_annotations,
                },
                self.tool_live_follow,
            ),
            "linkedin_live_send_dm": (
                {
                    "name": "linkedin_live_send_dm",
                    "title": "Live Send DM",
                    "description": "Send a real LinkedIn DM to an existing conversation, recipient URN, or profile URL/slug.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "message_text": _schema_string("DM text."),
                            "conversation_urn": _schema_string("Existing LinkedIn conversation URN."),
                            "recipient_urn": _schema_string("Recipient fsd_profile URN or member hash for a new conversation."),
                            "to_profile": _schema_string("Target LinkedIn profile URL or public slug for a new conversation."),
                            "dry_run": _schema_bool("Plan and persist the action without sending the live write.", default=False),
                        },
                        "required": ["message_text"],
                        "additionalProperties": False,
                    },
                    "annotations": write_annotations,
                },
                self.tool_live_send_dm,
            ),
            "linkedin_live_comment": (
                {
                    "name": "linkedin_live_comment",
                    "title": "Live Comment",
                    "description": "Post a real LinkedIn public comment or queued comment reply through the live executor.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "post_url": _schema_string("LinkedIn post URL."),
                            "text": _schema_string("Comment text. Optional when comment_id or author_profile_key points to a queued draft."),
                            "comment_id": _schema_string("Optional queued comment id for reply flow."),
                            "author_profile_key": _schema_string("Optional queued author profile key for reply flow."),
                            "dry_run": _schema_bool("Plan and persist the action without sending the live write.", default=False),
                        },
                        "required": ["post_url"],
                        "additionalProperties": False,
                    },
                    "annotations": write_annotations,
                },
                self.tool_live_comment,
            ),
            "linkedin_live_action_health": (
                {
                    "name": "linkedin_live_action_health",
                    "title": "Live Action Health",
                    "description": "Return live action-store health for stuck, unknown, retry, and scheduled work.",
                    "inputSchema": {"type": "object", "additionalProperties": False},
                    "annotations": {"readOnlyHint": True, "openWorldHint": False},
                },
                self.tool_live_action_health,
            ),
            "linkedin_live_reconcile_action": (
                {
                    "name": "linkedin_live_reconcile_action",
                    "title": "Live Reconcile Action",
                    "description": "Reconcile a live action id against LinkedIn without executing a new write.",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "action_id": _schema_string("Live action id to reconcile."),
                        },
                        "required": ["action_id"],
                        "additionalProperties": False,
                    },
                    "annotations": {"readOnlyHint": True, "openWorldHint": True},
                },
                self.tool_live_reconcile_action,
            ),
        }

    def tool_reset(self, args: JsonDict) -> JsonDict:
        state = self.reset(args)
        return _text_result("Sandbox reset.", {"ok": True, "state": state})

    def tool_state(self, _args: JsonDict) -> JsonDict:
        return _text_result("Sandbox state returned.", {"ok": True, "state": self._state_payload()})

    def tool_publish_post(self, args: JsonDict) -> JsonDict:
        text = _required_str(args, "text")
        visibility = str(args.get("visibility") or "connections")
        image_base64 = str(args.get("image_base64") or "").strip()
        if image_base64:
            raw = base64.b64decode(image_base64)
            filename = str(args.get("image_filename") or "sandbox-image.jpg")
            image_path = self.state_dir / filename
            image_path.write_bytes(raw)
            plan = build_image_post_plan(
                self.sandbox.account_id,
                text,
                str(image_path),
                len(raw),
                filename,
                visibility=visibility,
            )
            action_prefix = "image_post"
        else:
            plan = build_post_plan(self.sandbox.account_id, text, visibility=visibility)
            action_prefix = "post"
        result = self._execute_and_reconcile(self._action_id(action_prefix), plan)
        return _text_result("Sandbox post published and reconciled.", {"ok": True, **result})

    def tool_profile_edit(self, args: JsonDict) -> JsonDict:
        plan = build_profile_edit_plan(
            self.sandbox.account_id,
            _required_str(args, "field"),
            _required_str(args, "value"),
            member_hash=self.sandbox.member_hash,
        )
        result = self._execute_and_reconcile(self._action_id("profile"), plan)
        return _text_result("Sandbox profile edit executed and reconciled.", {"ok": True, **result})

    def tool_experience_add(self, args: JsonDict) -> JsonDict:
        plan = build_experience_plan(
            self.sandbox.account_id,
            title=_required_str(args, "title"),
            company=_required_str(args, "company"),
            description=args.get("description"),
            location=args.get("location"),
            start_month=_optional_int(args.get("start_month")),
            start_year=_optional_int(args.get("start_year")),
            end_month=_optional_int(args.get("end_month")),
            end_year=_optional_int(args.get("end_year")),
        )
        result = self._execute_and_reconcile(self._action_id("experience"), plan)
        return _text_result("Sandbox experience added and reconciled.", {"ok": True, **result})

    def tool_connect_follow(self, args: JsonDict) -> JsonDict:
        vanity_name = str(args.get("vanity_name") or "jane-sandbox")
        target_urn = str(args.get("target_urn") or f"urn:li:fsd_profile:{vanity_name}")
        self.sandbox.target_profiles.setdefault(
            vanity_name,
            {
                "vanity_name": vanity_name,
                "target_urn": target_urn,
                "connection_state": "none",
                "follow_state": "none",
            },
        )
        results: list[JsonDict] = []
        if bool(args.get("connect", True)):
            connect_plan = build_connect_plan(
                self.sandbox.account_id,
                vanity_name=vanity_name,
                page_key="profile_view_base",
                member_urn=target_urn,
                message=args.get("message"),
            )
            results.append(self._execute_and_reconcile(self._action_id("connect"), connect_plan))
        if bool(args.get("follow", True)):
            follow_plan = build_follow_plan(
                self.sandbox.account_id,
                target_member_urn=target_urn,
                page_key="profile_view_base",
                vanity_name=vanity_name,
            )
            results.append(self._execute_and_reconcile(self._action_id("follow"), follow_plan))
        return _text_result("Sandbox connect/follow actions executed.", {"ok": True, "results": results, "state": self._state_payload()})

    def tool_send_dm(self, args: JsonDict) -> JsonDict:
        conversation_urn = args.get("conversation_urn") or None
        recipient_urn = args.get("recipient_urn") or None
        if not conversation_urn and not recipient_urn:
            conversation_urn = self.sandbox.default_conversation_urn
        plan = build_dm_plan(
            self.sandbox.account_id,
            conversation_urn=conversation_urn,
            recipient_urn=recipient_urn,
            message_text=_required_str(args, "message_text"),
            mailbox_urn=self.sandbox.mailbox_urn,
        )
        result = self._execute_and_reconcile(self._action_id("dm"), plan)
        return _text_result("Sandbox DM sent and reconciled.", {"ok": True, **result})

    def tool_comment(self, args: JsonDict) -> JsonDict:
        if not self.sandbox.posts:
            self.sandbox.seed_post("Sandbox seed post for comments")
        post = self.sandbox.posts[0]
        post_url = str(args.get("post_url") or post["url"])
        thread_urn = str(args.get("thread_urn") or post["entityUrn"])
        plan = build_comment_plan(
            self.sandbox.account_id,
            post_url=post_url,
            thread_urn=thread_urn,
            text=_required_str(args, "text"),
        )
        result = self._execute_and_reconcile(self._action_id("comment"), plan)
        return _text_result("Sandbox comment posted and reconciled.", {"ok": True, **result})

    def tool_run_write_surface(self, args: JsonDict) -> JsonDict:
        if bool(args.get("reset", True)):
            self.reset({})
        prefix = str(args.get("prefix") or "MCP sandbox")
        outputs: list[JsonDict] = []
        outputs.append(self.tool_publish_post({"text": f"{prefix} text post", "visibility": "connections"})["structuredContent"])
        outputs.append(
            self.tool_publish_post(
                {
                    "text": f"{prefix} image post",
                    "visibility": "anyone",
                    "image_base64": base64.b64encode(b"sandbox image bytes").decode("ascii"),
                    "image_filename": "mcp-sandbox-image.jpg",
                }
            )["structuredContent"]
        )
        outputs.append(self.tool_profile_edit({"field": "headline", "value": f"{prefix} headline"})["structuredContent"])
        outputs.append(self.tool_experience_add({"title": "Agent Operator", "company": "Sandbox Labs", "start_month": 4, "start_year": 2026})["structuredContent"])
        outputs.append(self.tool_connect_follow({"vanity_name": "jane-sandbox", "message": f"{prefix} connect note"})["structuredContent"])
        outputs.append(self.tool_send_dm({"message_text": f"{prefix} DM"})["structuredContent"])
        latest_text_post = next((post for post in self.sandbox.posts if post["text"] == f"{prefix} text post"), self.sandbox.posts[-1])
        outputs.append(
            self.tool_comment(
                {
                    "post_url": latest_text_post["url"],
                    "thread_urn": latest_text_post["entityUrn"],
                    "text": f"{prefix} comment",
                }
            )["structuredContent"]
        )
        return _text_result(
            "Sandbox write surface completed.",
            {"ok": True, "results": outputs, "state": self._state_payload()},
        )

    def tool_live_publish_post(self, args: JsonDict) -> JsonDict:
        session, account_id = self._load_live_session_and_account()
        text = _required_str(args, "text")
        visibility = str(args.get("visibility") or "connections")
        dry_run = bool(args.get("dry_run", False))
        image_base64 = str(args.get("image_base64") or "").strip()
        image_path_arg = str(args.get("image_path") or "").strip()

        if image_base64:
            raw = base64.b64decode(image_base64, validate=True)
            filename = Path(str(args.get("image_filename") or "mcp-image.jpg")).name
            image_dir = self.live_state_dir / "mcp_uploads"
            image_dir.mkdir(parents=True, exist_ok=True)
            image_path = image_dir / filename
            image_path.write_bytes(raw)
            plan = build_image_post_plan(
                account_id=account_id,
                text=text,
                image_path=str(image_path),
                image_size=len(raw),
                image_filename=filename,
                visibility=visibility,
            )
            action_prefix = "live_image_post"
        elif image_path_arg:
            image_path = Path(image_path_arg).expanduser().resolve()
            if not image_path.exists():
                raise ValueError(f"Image file not found: {image_path}")
            plan = build_image_post_plan(
                account_id=account_id,
                text=text,
                image_path=str(image_path),
                image_size=image_path.stat().st_size,
                image_filename=image_path.name,
                visibility=visibility,
            )
            action_prefix = "live_image_post"
        else:
            plan = build_post_plan(account_id, text, visibility=visibility)
            action_prefix = "live_post"

        result = self._execute_live_plan(
            session=session,
            account_id=account_id,
            action_id=self._action_id(action_prefix),
            plan=plan,
            dry_run=dry_run,
        )
        verb = "planned" if dry_run else "executed"
        return _text_result(f"Live post {verb}.", {"ok": True, **result})

    def tool_live_profile_edit(self, args: JsonDict) -> JsonDict:
        session, account_id = self._load_live_session_and_account()
        dry_run = bool(args.get("dry_run", False))
        plan = build_profile_edit_plan(
            account_id,
            _required_str(args, "field"),
            _required_str(args, "value"),
            member_hash=_get_my_member_hash(session),
        )
        result = self._execute_live_plan(
            session=session,
            account_id=account_id,
            action_id=self._action_id("live_profile"),
            plan=plan,
            dry_run=dry_run,
        )
        verb = "planned" if dry_run else "executed"
        return _text_result(f"Live profile edit {verb}.", {"ok": True, **result})

    def tool_live_experience_add(self, args: JsonDict) -> JsonDict:
        session, account_id = self._load_live_session_and_account()
        dry_run = bool(args.get("dry_run", False))
        plan = build_experience_plan(
            account_id,
            title=_required_str(args, "title"),
            company=_required_str(args, "company"),
            description=args.get("description"),
            location=args.get("location"),
            start_month=_optional_int(args.get("start_month")),
            start_year=_optional_int(args.get("start_year")),
            end_month=_optional_int(args.get("end_month")),
            end_year=_optional_int(args.get("end_year")),
        )
        result = self._execute_live_plan(
            session=session,
            account_id=account_id,
            action_id=self._action_id("live_experience"),
            plan=plan,
            dry_run=dry_run,
        )
        verb = "planned" if dry_run else "executed"
        return _text_result(f"Live experience add {verb}.", {"ok": True, **result})

    def tool_live_connect(self, args: JsonDict) -> JsonDict:
        session, account_id = self._load_live_session_and_account()
        dry_run = bool(args.get("dry_run", False))
        context = _resolve_mwlite_profile_context(session, _required_str(args, "profile"))
        plan = build_connect_plan(
            account_id=account_id,
            vanity_name=context["vanity_name"],
            page_key=context["page_key"],
            member_urn=context["member_urn"],
            message=args.get("message"),
        )
        result = self._execute_live_plan(
            session=session,
            account_id=account_id,
            action_id=self._action_id("live_connect"),
            plan=plan,
            dry_run=dry_run,
        )
        verb = "planned" if dry_run else "executed"
        return _text_result(f"Live connect {verb}.", {"ok": True, **result})

    def tool_live_follow(self, args: JsonDict) -> JsonDict:
        session, account_id = self._load_live_session_and_account()
        dry_run = bool(args.get("dry_run", False))
        context = _resolve_mwlite_profile_context(session, _required_str(args, "profile"))
        plan = build_follow_plan(
            account_id=account_id,
            target_member_urn=context["member_urn"],
            page_key=context["page_key"],
            vanity_name=context["vanity_name"],
        )
        result = self._execute_live_plan(
            session=session,
            account_id=account_id,
            action_id=self._action_id("live_follow"),
            plan=plan,
            dry_run=dry_run,
        )
        verb = "planned" if dry_run else "executed"
        return _text_result(f"Live follow {verb}.", {"ok": True, **result})

    def tool_live_send_dm(self, args: JsonDict) -> JsonDict:
        session, account_id = self._load_live_session_and_account()
        dry_run = bool(args.get("dry_run", False))
        conversation_urn = str(args.get("conversation_urn") or "").strip() or None
        recipient_urn = str(args.get("recipient_urn") or "").strip() or None
        to_profile = str(args.get("to_profile") or "").strip()
        if to_profile:
            context = _resolve_mwlite_profile_context(session, to_profile)
            if not conversation_urn and context.get("message_locked"):
                raise ValueError("LinkedIn has direct messaging locked for this profile from your account.")
            recipient_urn = _resolve_profile_urn(session, to_profile)
        if not conversation_urn and not recipient_urn:
            raise ValueError("Either conversation_urn, recipient_urn, or to_profile is required.")
        plan = build_dm_plan(
            account_id=account_id,
            conversation_urn=conversation_urn,
            recipient_urn=recipient_urn,
            message_text=_required_str(args, "message_text"),
            mailbox_urn=_get_my_urn(session),
        )
        result = self._execute_live_plan(
            session=session,
            account_id=account_id,
            action_id=self._action_id("live_dm"),
            plan=plan,
            dry_run=dry_run,
        )
        verb = "planned" if dry_run else "executed"
        return _text_result(f"Live DM {verb}.", {"ok": True, **result})

    def tool_live_comment(self, args: JsonDict) -> JsonDict:
        session, account_id = self._load_live_session_and_account()
        dry_run = bool(args.get("dry_run", False))
        result = publish_post_comment(
            session=session,
            post_url=_required_str(args, "post_url"),
            text=args.get("text"),
            comment_id=args.get("comment_id"),
            author_profile_key=args.get("author_profile_key"),
            execute=not dry_run,
            account_id=account_id,
        )
        action = result.get("action") or {}
        action_id = action.get("action_id")
        reconcile_result: JsonDict | None = None
        if action_id and result.get("status") == "succeeded" and not dry_run:
            try:
                reconcile_result = reconcile_action(session, str(action_id))
            except Exception as exc:
                reconcile_result = {"reconciled": False, "error": str(exc)}
        verb = "planned" if dry_run else "executed"
        return _text_result(
            f"Live comment {verb}.",
            {
                "ok": True,
                "live": True,
                "dry_run": dry_run,
                "action_id": action_id,
                "execution": _compact_execution_result(result),
                "reconcile": reconcile_result,
                "action_health": action_health_report(),
            },
        )

    def tool_live_action_health(self, _args: JsonDict) -> JsonDict:
        self._configure_action_store(live=True)
        return _text_result("Live action health returned.", {"ok": True, "action_health": action_health_report()})

    def tool_live_reconcile_action(self, args: JsonDict) -> JsonDict:
        self._configure_action_store(live=True)
        load_env_file()
        session, _meta = load_session(required=True)
        assert session is not None
        action_id = _required_str(args, "action_id")
        result = reconcile_action(session, action_id)
        return _text_result("Live action reconciled.", {"ok": True, "action_id": action_id, "reconcile": result})

    def tool_live_read_smoke(self, _args: JsonDict) -> JsonDict:
        load_env_file()
        try:
            session, _meta = load_session(required=True)
            assert session is not None
            response = voyager_get(session, "/voyager/api/me")
            data = parse_json_response(response)
        except CliError as exc:
            return _text_result(exc.message, {"ok": False, "error": exc.message}, is_error=True)
        except Exception as exc:
            return _text_result(str(exc), {"ok": False, "error": str(exc)}, is_error=True)
        return _text_result(
            "Live LinkedIn read smoke passed.",
            {"ok": True, "status": response.status_code, "data": data},
        )

    def handle_request(self, message: JsonDict) -> JsonDict | None:
        if message.get("jsonrpc") != "2.0":
            raise MCPError(-32600, "Invalid JSON-RPC message")
        method = message.get("method")
        request_id = message.get("id")
        if request_id is None and str(method or "").startswith("notifications/"):
            return None
        if method == "initialize":
            requested = str((message.get("params") or {}).get("protocolVersion") or "")
            protocol_version = requested if requested in SUPPORTED_PROTOCOL_VERSIONS else PROTOCOL_VERSION
            live_note = (
                "Live write tools are enabled and can publish, comment, connect, follow, DM, and edit the saved LinkedIn account."
                if self.enable_live_writes
                else "Live write tools are disabled; restart with --enable-live-writes or LINKEDIN_MCP_ENABLE_LIVE_WRITES=1 for full live-write access."
            )
            return _response(
                request_id,
                {
                    "protocolVersion": protocol_version,
                    "capabilities": {"tools": {"listChanged": False}},
                    "serverInfo": {"name": SERVER_NAME, "version": SERVER_VERSION},
                    "instructions": (
                        "Use linkedin_sandbox_* tools for deterministic local LinkedIn write-path testing. "
                        "linkedin_live_read_smoke is available for read-only saved-session checks. "
                        f"{live_note}"
                    ),
                },
            )
        if method == "ping":
            return _response(request_id, {})
        if method == "tools/list":
            tools = [definition for definition, _handler in self.tools.values()]
            return _response(request_id, {"tools": tools})
        if method == "tools/call":
            params = message.get("params") or {}
            if not isinstance(params, dict):
                raise MCPError(-32602, "tools/call params must be an object")
            name = params.get("name")
            arguments = params.get("arguments") or {}
            if not isinstance(name, str) or name not in self.tools:
                raise MCPError(-32602, f"Unknown tool: {name}")
            if not isinstance(arguments, dict):
                raise MCPError(-32602, "Tool arguments must be an object")
            _definition, handler = self.tools[name]
            try:
                result = handler(arguments)
            except Exception as exc:
                result = _text_result(str(exc), {"ok": False, "error": str(exc)}, is_error=True)
            return _response(request_id, result)
        raise MCPError(-32601, f"Method not found: {method}")

    def handle_json_message(self, raw: str) -> str | None:
        try:
            message = json.loads(raw)
        except json.JSONDecodeError as exc:
            return json.dumps(_error_response(None, -32700, f"Parse error: {exc}"), separators=(",", ":"))
        try:
            if isinstance(message, list):
                responses = []
                for item in message:
                    if not isinstance(item, dict):
                        responses.append(_error_response(None, -32600, "Invalid batch item"))
                        continue
                    response = self.handle_request(item)
                    if response is not None:
                        responses.append(response)
                return json.dumps(responses, separators=(",", ":")) if responses else None
            if not isinstance(message, dict):
                raise MCPError(-32600, "Invalid JSON-RPC message")
            response = self.handle_request(message)
            return json.dumps(response, separators=(",", ":")) if response is not None else None
        except MCPError as exc:
            request_id = message.get("id") if isinstance(message, dict) else None
            return json.dumps(_error_response(request_id, exc.code, exc.message, exc.data), separators=(",", ":"))
        except Exception as exc:
            request_id = message.get("id") if isinstance(message, dict) else None
            return json.dumps(_error_response(request_id, -32603, str(exc)), separators=(",", ":"))


def _compact_execution_result(result: JsonDict) -> JsonDict:
    action = result.get("action") or {}
    return {
        "status": result.get("status"),
        "message": result.get("message"),
        "remote_ref": ((result.get("result") or {}).get("remote_ref")),
        "http_status": ((result.get("result") or {}).get("http_status")),
        "action": {
            "action_id": action.get("action_id"),
            "action_type": action.get("action_type"),
            "state": action.get("state"),
            "target_key": action.get("target_key"),
            "remote_ref": action.get("remote_ref"),
        },
    }


def _required_str(args: JsonDict, key: str) -> str:
    value = str(args.get(key) or "").strip()
    if not value:
        raise ValueError(f"{key} is required")
    return value


def _optional_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    return int(value)


def _env_truthy(name: str) -> bool:
    return os.getenv(name, "").strip().lower() in {"1", "true", "yes", "on"}


def _response(request_id: Any, result: JsonDict) -> JsonDict:
    return {"jsonrpc": "2.0", "id": request_id, "result": result}


def _error_response(request_id: Any, code: int, message: str, data: Any | None = None) -> JsonDict:
    error: JsonDict = {"code": code, "message": message}
    if data is not None:
        error["data"] = data
    return {"jsonrpc": "2.0", "id": request_id, "error": error}


def run_stdio(server: LinkedInMCPServer) -> None:
    try:
        for raw_line in sys.stdin:
            raw = raw_line.strip()
            if not raw:
                continue
            response = server.handle_json_message(raw)
            if response is not None:
                sys.stdout.write(response + "\n")
                sys.stdout.flush()
    finally:
        server.close()


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Run the LinkedIn CLI sandbox MCP stdio server")
    parser.add_argument("--state-dir", help="Directory for the sandbox action store. Defaults to a temporary directory.")
    parser.add_argument(
        "--enable-live-writes",
        action="store_true",
        default=_env_truthy("LINKEDIN_MCP_ENABLE_LIVE_WRITES"),
        help="Expose live write tools that use the saved LinkedIn session and real action store.",
    )
    parser.add_argument(
        "--live-state-dir",
        help="Directory for the live action store. Defaults to the normal linkedin-cli config directory.",
    )
    args = parser.parse_args(argv)
    server = LinkedInMCPServer(
        state_dir=Path(args.state_dir).expanduser() if args.state_dir else None,
        enable_live_writes=bool(args.enable_live_writes),
        live_state_dir=Path(args.live_state_dir).expanduser() if args.live_state_dir else None,
    )
    run_stdio(server)


if __name__ == "__main__":
    main()
