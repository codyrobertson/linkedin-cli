# linkedin-cli

Unofficial LinkedIn CLI using cookie-based session authentication. No official API keys or OAuth application needed.

Provides profile/company lookup, search, Voyager API access, and a safe write system with dry-run defaults for posting, messaging, profile editing, and more.

It now also includes operator-oriented UX commands (`doctor`, shell completion, table/quiet output), richer action inspection/reconciliation, local workflow tools for saved searches, templates, and lightweight contact tracking, and a unified discovery queue that merges search, inbox, and engagement feedback with adaptive scoring.

## Install

```bash
# From PyPI
pipx install linkedin-discovery-cli

# Or with pip
pip install linkedin-discovery-cli

# Local development install from the repo root
pip install -e .[dev]

# Optional browser capture support
pip install playwright && python -m playwright install chromium firefox

# Optional local Qwen training support
pip install -e .[qwen]
```

The published package name is `linkedin-discovery-cli`. The main console command remains `linkedin`; the MCP server entrypoint is `linkedin-mcp`.

## Quick start

```bash
# Set credentials (or put them in ~/.config/linkedin-cli/.env)
export LINKEDIN_USERNAME="you@example.com"
export LINKEDIN_PASSWORD="your-password"

# Log in (form auth or interactive browser capture)
linkedin login
linkedin login --browser --browser-name chrome
linkedin login --browser --browser-name brave
linkedin login --browser --browser-name firefox

# Check session status
linkedin status

# Run local diagnostics
linkedin doctor

# Look up a profile
linkedin profile john-doe

# Look up a company
linkedin company openai

# Search people
linkedin search people "machine learning engineer San Francisco"

# Harvest LinkedIn posts into the local content library
# Uses authenticated LinkedIn search first, then falls back to public search if needed
linkedin content harvest --industry ai --topic agents --limit 100 --per-query 25

# Optional: run a local SearXNG instance for faster public discovery
docker compose up -d searxng
linkedin content harvest \
  --industry ai \
  --topic workflow \
  --backend public-only \
  --public-search searxng \
  --searxng-url http://127.0.0.1:8080 \
  --limit 100 \
  --per-query 25

# Review harvested posts and aggregate stats
linkedin content list --industry ai --limit 20
linkedin content stats
linkedin content build-sft-dataset --output .artifacts/qwen/sft-ai-workflow --industry ai --topic workflow
linkedin content train-qwen --phase sft --dataset-dir .artifacts/qwen/sft-ai-workflow --base-model Qwen/Qwen2.5-3B-Instruct --dry-run
linkedin content train-qwen --phase sft --dataset-dir .artifacts/qwen/sft-ai-workflow --runner modal --wandb-project linkedin-autonomy --wandb-entity your-team --dry-run

# Search with enrichment (fetches full profile for each result)
linkedin search people "CTO fintech" --enrich --limit 3

# Find someone's recent posts
linkedin activity john-doe

# Fetch raw Voyager API data
linkedin voyager /voyager/api/me

# Publish a post (dry-run by default)
linkedin post publish --text "Hello LinkedIn!"

# Actually publish
linkedin post publish --text "Hello LinkedIn!" --execute

# Send a DM (dry-run)
linkedin dm send --to john-doe --message "Hey, let's connect"

# List recent DM conversations
linkedin dm list

# Save and rerun a search locally
linkedin workflow search save --name founders --kind people --query "fintech founder"
linkedin workflow search run founders

# Ingest people into the unified discovery queue
linkedin discover ingest-search --kind people --query "fintech founder"
linkedin discover ingest-inbox
linkedin discover ingest-engagement --target openai
linkedin discover queue --why

# Save a reusable DM template
linkedin workflow template save --name intro --kind dm --body "Hi {name}, enjoyed meeting you."

# Track a contact locally
linkedin workflow contact upsert --profile john-doe --name "John Doe" --stage qualified --tags lead,founder

# Triage an inbox thread locally
linkedin workflow inbox upsert --conversation urn:li:msg_conversation:123 --state follow_up --priority high

# Feed public/manual engagement back into the queue
linkedin discover signal add --profile john-doe --type commented --source public --notes "Asked about pricing"

# Manually move a prospect through the queue
linkedin discover state set john-doe --state engaged

# Run the MCP server for agent tools
linkedin-mcp

# Run MCP with full live-write access for agents
linkedin-mcp --enable-live-writes
```

## MCP server for agents

`linkedin-mcp` exposes the CLI as a stdio MCP server so agent clients can use
the LinkedIn sandbox, session smoke tests, and, when explicitly enabled, live
write tools.

Default mode is safe for agent regression runs:

```bash
linkedin-mcp
```

It exposes local sandbox tools for publish/profile/experience/connect/follow/DM
and comments, plus `linkedin_live_read_smoke` for a read-only saved-session
check. Sandbox tools do not send traffic to LinkedIn.

Full-access live mode is explicit:

```bash
linkedin-mcp --enable-live-writes
```

or:

```bash
LINKEDIN_MCP_ENABLE_LIVE_WRITES=1 linkedin-mcp
```

Live mode adds MCP tools for:

- `linkedin_live_publish_post`
- `linkedin_live_profile_edit`
- `linkedin_live_experience_add`
- `linkedin_live_connect`
- `linkedin_live_follow`
- `linkedin_live_send_dm`
- `linkedin_live_comment`
- `linkedin_live_action_health`
- `linkedin_live_reconcile_action`

These tools use the saved LinkedIn session, the normal action store, the
single-write lock, idempotency records, retry/unknown-state handling, and local
write guardrails. Most live write tools also accept `dry_run: true`.

Example MCP client config:

```json
{
  "mcpServers": {
    "linkedin-live": {
      "command": "linkedin-mcp",
      "args": ["--enable-live-writes"]
    }
  }
}
```

Use `--live-state-dir /path/to/state` if a particular agent deployment needs an
isolated live action store instead of `~/.config/linkedin-cli/state.sqlite`.

## Output modes

By default, commands render pretty JSON. Global flags can change the output shape:

```bash
linkedin --json action show act_123
linkedin --table action list
linkedin --quiet workflow contact list
linkedin --brief voyager /voyager/api/me
```

## Operator UX

```bash
# Diagnose local config/session/db health
linkedin doctor

# Generate a basic completion script
linkedin completion bash
linkedin completion zsh
```

## Local SearXNG discovery

For large public-only or hybrid content harvest campaigns, run a local SearXNG instance from this repo:

```bash
./scripts/bootstrap_searxng.sh
curl 'http://127.0.0.1:8080/search?q=site%3Alinkedin.com%2Fposts%20ai%20workflow&format=json'
```

Then point the harvest commands at the local endpoint:

```bash
linkedin content harvest-campaign \
  --industry ai \
  --industry fintech \
  --topic agents \
  --topic workflow \
  --backend public-only \
  --public-search searxng \
  --searxng-url http://127.0.0.1:8080 \
  --searxng-engine google \
  --speed max \
  --per-query 50 \
  --limit 100000 \
  --per-job-limit 1000 \
  --queries-per-job 50 \
  --job-prefix million-corpus-searxng
```

Use `--backend hybrid` if you want authenticated LinkedIn search first and SearXNG as the public fallback.

## Stacked ranking

You can train a general-purpose stacked ranking artifact from the local DuckDB warehouse, then rerank posts for a specific company or buyer target without retraining the model:

```bash
# Build the warehouse-backed feature views
linkedin content build-foundation-views --industry ai

# Train the three-head foundation model
linkedin content train-stacked-model --name foundation-v1 --industry ai --min-samples 100

# Rerank warehouse posts for a target profile
linkedin content rerank-target --model-name foundation-v1 --target-file target.json --limit 10
```

Example `target.json`:

```json
{
  "company": "Acme AI",
  "buyer_roles": ["vp engineering"],
  "industries": ["ai", "devtools"],
  "problem_keywords": ["workflow", "orchestration"],
  "preferred_cta": ["soft_authority", "commercial"],
  "tone_constraints": ["assertive"]
}
```

Each reranked row returns a score breakdown for `public_performance`, `persona_style`, `business_intent`, and `target_similarity`.

## Configuration

### Config directory

By default, all data is stored in `~/.config/linkedin-cli/`. Override with:

```bash
export LINKEDIN_CLI_HOME=/path/to/custom/config
```

### Files in the config directory

| File | Purpose |
|------|---------|
| `session.json` | Saved LinkedIn session cookies |
| `state.sqlite` | Action store for write operations |
| `.env` | Environment variables (credentials, etc.) |
| `dm_poll_state.json` | DM poller state tracking |
| `locks/` | Single-write lock files |

### Environment variables

| Variable | Required | Description |
|----------|----------|-------------|
| `LINKEDIN_USERNAME` | For login | LinkedIn email/username |
| `LINKEDIN_PASSWORD` | For login | LinkedIn password |
| `LINKEDIN_USER_AGENT` | No | Custom browser user agent |
| `LINKEDIN_CLI_HOME` | No | Override config directory |
| `LINKEDIN_MCP_ENABLE_LIVE_WRITES` | No | Expose full live-write tools from `linkedin-mcp` when set to `1`, `true`, `yes`, or `on` |
| `LINKEDIN_WRITE_GUARDS` | No | Set to `0`/`false`/`off`/`no` to disable local write guardrails |
| `LINKEDIN_WRITE_MAX_HOURLY` | No | Override live write hourly budget guard |
| `LINKEDIN_WRITE_MAX_DAILY` | No | Override live write daily budget guard |
| `LINKEDIN_WRITE_QUIET_HOURS` | No | Block live writes during a local time window such as `22:00-07:00` |
| `BRAVE_EXECUTABLE_PATH` | No | Custom Brave executable path for `--browser-name brave` |

## Commands

### Read commands

| Command | Description |
|---------|-------------|
| `login` | Authenticate via web form flow, or `--browser` to capture cookies from Chrome/Firefox/Brave |
| `logout` | Remove saved session |
| `status` | Inspect session health and account info |
| `doctor` | Check local config, session, and SQLite state health |
| `completion SHELL` | Print a basic bash/zsh completion script |
| `html URL` | Fetch an authenticated LinkedIn URL |
| `voyager PATH` | Call a Voyager API endpoint directly |
| `profile TARGET` | Fetch and summarize a profile |
| `company TARGET` | Fetch and summarize a company |
| `search KIND QUERY` | Search people, companies, or posts |
| `activity TARGET` | Find public posts for a person |
| `content harvest|list|stats` | Harvest LinkedIn posts with auth-first search and inspect the local content library |
| `content build-sft-dataset|build-preference-dataset` | Build local Qwen tuning corpora from owned posts, harvested exemplars, and candidate decisions |
| `content train-qwen|qwen-runs` | Plan or run local Qwen SFT/preference jobs and inspect recorded runs |
| `discover ...` | Build and inspect the ranked prospect queue |
| `snapshot` | Snapshot authenticated user profile |

### Write commands

All write commands default to **dry-run mode**. Pass `--execute` to actually perform the action.

| Command | Description |
|---------|-------------|
| `post publish` | Publish a text or image post |
| `edit FIELD` | Edit a profile field (headline, about, website, location) |
| `experience add` | Add a position/experience entry |
| `connect` | Send a connection request |
| `follow` | Follow a profile |
| `dm send` | Send a direct message |
| `schedule` | Schedule a post for future publishing |

### Action management

| Command | Description |
|---------|-------------|
| `action list` | List recent actions from the store |
| `action show ID` | Show details for a specific action |
| `action retry ID` | Retry a failed action |
| `action reconcile ID` | Re-check uncertain action state against LinkedIn |
| `action cancel ID` | Cancel a pending/retryable action locally |
| `action artifacts ID` | Show persisted action artifacts |

### Workflow commands

| Command | Description |
|---------|-------------|
| `workflow search save|list|run|delete` | Save and replay useful LinkedIn searches |
| `workflow template save|list|show|render|delete` | Manage reusable DM/post templates |
| `workflow contact upsert|list|show|delete` | Track local contact records and lead stages |
| `workflow contact export|import` | Round-trip contacts as CSV |
| `workflow inbox upsert|list|show|delete` | Track local inbox triage state for conversations |

### Discovery commands

| Command | Description |
|---------|-------------|
| `discover ingest-search` | Ingest people from a live query or saved search into the queue |
| `discover ingest-inbox` | Ingest recent inbox participants into the queue |
| `discover ingest-engagement` | Ingest public commenters and engagement from recent public posts |
| `discover signal add` | Attach engagement feedback to a prospect |
| `discover state set` | Update the queue state for a prospect |
| `discover queue` | Show the ranked prospect queue |
| `discover show` | Show one prospect with sources and signals |
| `discover stats` | Show queue source/signal summary metrics |

## Write system

The write system uses a **plan -> persist -> execute -> reconcile** pipeline:

1. Every write command builds a normalized action plan
2. An idempotency key is computed from the intended effect
3. The action is persisted to SQLite before any network write
4. Dry-run is the default; `--execute` triggers live execution
5. A single-write lock prevents concurrent mutations
6. Warm-up GETs refresh the session before writes
7. 2-5 second jitter is added before POSTs to appear natural
8. Results are recorded with full attempt tracking

### Idempotency

Actions are deduplicated by `(account_id, idempotency_key)`. If you run the same command twice, the second invocation detects the duplicate and skips it.

### Safety features

- **Dry-run by default**: Every write command shows what would happen without doing it
- **Single-write lock**: Only one write operation runs at a time per account
- **Jitter**: Random 2-5 second delay before live writes
- **Warm-up GETs**: Session is refreshed with a read before any write
- **Action store**: Full audit trail of every action, attempt, and state transition
- **Artifacts**: Persisted plan/result/reconcile files for action inspection
- **Idempotency**: Duplicate actions are detected and skipped
- **Diagnostics**: `doctor` checks local config, session, and DB readiness
- **Discovery queue**: Merge search, inbox, and engagement feedback into one ranked prospect list
- **Adaptive learning**: Source and signal performance can influence future queue ranking
- **Daily caps concept**: Architecture supports configurable daily limits (see `docs/architecture.md`)

## Running as a module

```bash
python -m linkedin_cli login
python -m linkedin_cli status
```

## Distribution

Packaging and release automation live in:

- `.github/workflows/ci.yml`
- `.github/workflows/release.yml`
- `.github/workflows/publish-pypi.yml`
- `packaging/homebrew/linkedin-discovery-cli.rb`
- `docs/distribution.md`

The Homebrew formula in this repo is a starter template. Copy it into a dedicated tap repo after the first tagged release so you can replace the source archive SHA with the real PyPI sdist digest.

## DM Poller

The DM poller checks for new messages and optionally forwards them to a Discord webhook:

```bash
python -m linkedin_cli.integrations.dm_poller check
python -m linkedin_cli.integrations.dm_poller check --quiet
```

## Scheduler

The scheduler checks for due scheduled posts and executes them:

```bash
python -m linkedin_cli.write.scheduler tick
python -m linkedin_cli.write.scheduler tick --dry-run
```

## Disclaimer

This tool is **unofficial** and not affiliated with or endorsed by LinkedIn. It uses cookie-based session authentication to access LinkedIn's web interface and internal Voyager API endpoints.

Using this tool may violate LinkedIn's Terms of Service. Use at your own risk. The authors are not responsible for any account restrictions, suspensions, or other consequences resulting from the use of this tool.

This tool is intended for personal productivity and single-account use. Do not use it for bulk automation, scraping, or any activity that could harm LinkedIn's platform or other users.
