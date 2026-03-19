# linkedin-cli

Unofficial LinkedIn CLI using cookie-based session authentication. No official API keys or OAuth application needed.

Provides profile/company lookup, search, Voyager API access, and a safe write system with dry-run defaults for posting, messaging, profile editing, and more.

## Install

```bash
# From the repo root
pip install .

# Or with pipx for isolated install
pipx install .
```

## Quick start

```bash
# Set credentials (or put them in ~/.config/linkedin-cli/.env)
export LINKEDIN_USERNAME="you@example.com"
export LINKEDIN_PASSWORD="your-password"

# Log in
linkedin login

# Check session status
linkedin status

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
| `html URL` | Fetch an authenticated LinkedIn URL |
| `voyager PATH` | Call a Voyager API endpoint directly |
| `profile TARGET` | Fetch and summarize a profile |
| `company TARGET` | Fetch and summarize a company |
| `search KIND QUERY` | Search people, companies, or posts |
| `activity TARGET` | Find public posts for a person |
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
- **Idempotency**: Duplicate actions are detected and skipped
- **Daily caps concept**: Architecture supports configurable daily limits (see `docs/architecture.md`)

## Running as a module

```bash
python -m linkedin_cli login
python -m linkedin_cli status
```

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
