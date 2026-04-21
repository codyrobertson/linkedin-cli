# LinkedIn CLI MCP Server

This package exposes a stdio MCP server for agent tools:

```bash
linkedin-mcp
```

or from a checkout:

```bash
python -m linkedin_cli.mcp_server
```

By default, write tools use the local `LinkedInSandbox`; they never send
traffic to LinkedIn and are intended for agent regression tests before any live
run. The default live tool is `linkedin_live_read_smoke`, which only checks the
saved session with `/voyager/api/me`.

Example client config:

```json
{
  "mcpServers": {
    "linkedin-sandbox": {
      "command": "python",
      "args": ["-m", "linkedin_cli.mcp_server"],
      "cwd": "/Users/Cody/code_projects/linkedin-cli"
    }
  }
}
```

Available tools:

- `linkedin_sandbox_reset`
- `linkedin_sandbox_state`
- `linkedin_sandbox_publish_post`
- `linkedin_sandbox_profile_edit`
- `linkedin_sandbox_experience_add`
- `linkedin_sandbox_connect_follow`
- `linkedin_sandbox_send_dm`
- `linkedin_sandbox_comment`
- `linkedin_sandbox_run_write_surface`
- `linkedin_live_read_smoke`

## Live Write Mode

To expose full live-write access over MCP, start the server with an explicit
opt-in:

```bash
python -m linkedin_cli.mcp_server --enable-live-writes
```

or:

```bash
LINKEDIN_MCP_ENABLE_LIVE_WRITES=1 linkedin-mcp
```

Example client config for a full-access agent:

```json
{
  "mcpServers": {
    "linkedin-live": {
      "command": "python",
      "args": ["-m", "linkedin_cli.mcp_server", "--enable-live-writes"],
      "cwd": "/Users/Cody/code_projects/linkedin-cli"
    }
  }
}
```

Live write tools added in this mode:

- `linkedin_live_publish_post`
- `linkedin_live_profile_edit`
- `linkedin_live_experience_add`
- `linkedin_live_connect`
- `linkedin_live_follow`
- `linkedin_live_send_dm`
- `linkedin_live_comment`
- `linkedin_live_action_health`
- `linkedin_live_reconcile_action`

These tools use the saved LinkedIn session, the normal `linkedin-cli` live
action store, the write lock, idempotency records, retry/unknown-state handling,
and local guardrails. They can publish, comment, connect, follow, DM, and edit
the authenticated account. Most live write tools accept `dry_run: true` to plan
and persist the action without sending the live write.

Use `--live-state-dir` when an MCP deployment needs an isolated live action
store instead of the default `linkedin-cli` config directory.

The server implements MCP over newline-delimited JSON-RPC on stdio. It supports
`initialize`, `notifications/initialized`, `ping`, `tools/list`, and
`tools/call`.
