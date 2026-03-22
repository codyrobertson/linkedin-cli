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
```

The published package name is `linkedin-discovery-cli`. The console command remains `linkedin`.

## Quick start

```bash
# Set credentials (or put them in ~/.config/linkedin-cli/.env)
export LINKEDIN_USERNAME="you@example.com"
export LINKEDIN_PASSWORD="your-password"

# Log in
linkedin login

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
```

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

## Commands

### Read commands

| Command | Description |
|---------|-------------|
| `login` | Authenticate via web form flow |
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
