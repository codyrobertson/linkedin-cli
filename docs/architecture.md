# Pragmatic architecture for write-capable LinkedIn CLI

Context: `tools/linkedin_cli.py` already has working cookie-backed auth/session handling and read-only fetches, but there is no official API access. This design aims to expand usefulness fast while minimizing duplicate actions, brittle behavior, and account risk.

Published distribution:
- PyPI package name: `linkedin-discovery-cli`
- Import/module path: `linkedin_cli`
- Operator command: `linkedin`

## 1) Core recommendation

Use a **plan -> persist -> execute -> reconcile -> notify** pipeline for all write actions.

For operator trust, pair that pipeline with:
- explicit local diagnostics (`doctor`)
- inspectable action artifacts (`plan`, `last_result`, `reconcile`, `cancel`)
- a local workflow layer for saved searches, templates, and contact state that does not depend on LinkedIn writes
- a unified discovery queue that merges search, inbox, and engagement signals into one ranked prospect surface
- adaptive ranking that reweights sources and signals from observed positive outcomes

Do **not** let commands directly POST/PUT and exit.

Instead, every write-capable command should:

1. Resolve the target and build a normalized `ActionPlan`.
2. Compute an idempotency key.
3. Persist the action before any network write.
4. Default to `dry-run` unless explicitly executed.
5. Execute through one shared runner with locks, pacing, retries, and reconciliation.
6. Persist final state plus request/response artifacts.
7. Emit webhook events from persisted state transitions.

This keeps the CLI maintainable even when endpoints/payloads are uncertain.

## 2) What should be live vs scaffolded

### Live first
These are the fastest, highest-value actions if the exact underlying request can be confirmed from a browser session or stable Voyager endpoint.

1. **`post publish`**
   - Scope: text-only posts first.
   - Optional later: one image attachment after request format is validated.
   - Why live: high value, deterministic, easy to reconcile by checking recent feed activity.

2. **`dm send`**
   - Scope: send into an **existing conversation** first.
   - Optional later: resolve recipient slug/profile URL to existing thread.
   - Why live: useful, reconcilable via recent conversation events.

3. **`profile export --me` / `profile snapshot --me`**
   - Read-only but critical for safe profile edits.
   - Use before every profile write.

4. **`profile edit headline|about|website|location`**
   - Only for top-level scalar-ish fields after endpoint shape is known.
   - Why live: lower complexity than experience/education edits, easy to diff and verify.

### Dry-run/scaffold only at first
These are either high-risk, harder to reconcile, or structurally complex.

1. **`profile edit experience|education|certification|skill`**
   - Usually nested objects with validation, IDs, ordering, and edge cases.
   - Scaffold the plan/diff/export flow first.

2. **`dm send --new-thread`**
   - Creating new conversations is riskier than replying in an existing thread.
   - Implement only after conversation bootstrap/resolution is reliable.

3. **`connect`, `follow`, `invite`, `endorse`, `withdraw-invite`, bulk outreach**
   - Highest anti-abuse risk.
   - Keep disabled or `--unsafe-live` only.

4. **Media-heavy posting, polls, articles, comments, reactions**
   - Add only after text-post path is stable and artifacts are captured.

### Safe default policy
- All new write commands should start as `dry-run` by default.
- Live execution should require `--execute`.
- Riskier classes should additionally require `--yes` or `--unsafe-live`.

## 3) Recommended command surface

Keep the command surface small and layered.

### High-level commands

```bash
linkedin post publish --text-file post.txt [--visibility anyone] [--execute]
linkedin dm send --conversation CONV_URN --message-file msg.txt [--execute]
linkedin dm send --to PROFILE_URL --message-file msg.txt [--execute]
linkedin profile snapshot --me [--output profile.json]
linkedin profile edit headline --value "..." [--execute]
linkedin profile edit about --file about.txt [--execute]
linkedin profile edit website --label PERSONAL --url https://... [--execute]
linkedin profile edit location --country US --postal-code 10001 [--execute]
```

### Operational commands

```bash
linkedin action list [--state pending|unknown|failed|succeeded]
linkedin action show ACTION_ID
linkedin action retry ACTION_ID
linkedin action reconcile ACTION_ID
linkedin action cancel ACTION_ID
linkedin webhook test
linkedin webhook replay DELIVERY_ID
```

### Optional low-level escape hatch

```bash
linkedin voyager-write --method POST --path /voyager/api/... --body-file payload.json [--execute]
```

This should be hidden/advanced and still use the same persistence + idempotency + webhook machinery.

## 4) Persistence model

Use **SQLite as the source of truth** plus a small artifact directory for debugging.

### Files/directories

Under `~/.hermes/linkedin/`:

- `session.json`
  - Existing cookie/session file.
- `config.json`
  - Safe defaults, caps, webhook config, pacing.
- `state.sqlite`
  - Authoritative action state store.
- `artifacts/<action_id>/plan.json`
  - Resolved plan used for execution.
- `artifacts/<action_id>/request.json`
  - Redacted request metadata/body.
- `artifacts/<action_id>/response.json`
  - Redacted response metadata/body.
- `artifacts/<action_id>/reconcile.json`
  - Evidence used to decide whether remote state changed.
- `artifacts/<action_id>/last_result.json`
  - Latest execution or retry scheduling outcome.
- `artifacts/<action_id>/cancel.json`
  - Local cancellation metadata when an action is canceled.
- `locks/account.lock`
  - Single-account write lock.

### Why SQLite instead of JSONL-only

Because you need:
- exact-once-ish local bookkeeping,
- state transitions,
- retries,
- webhook delivery tracking,
- querying by idempotency key / target / status,
- future scheduling or queueing.

A single SQLite file is still simple enough for CLI use.

It is also a pragmatic place to store lightweight workflow metadata:
- saved searches
- reusable message/post templates
- contact notes, tags, and lead stage
- inbox triage state for conversation follow-up
- prospect records, discovery sources, and engagement signals for ranking
- prospect aliases and dedupe keys to merge identity across search, inbox, and public engagement

## 5) Minimal SQLite schema

### `actions`
- `action_id TEXT PRIMARY KEY`
- `created_at TEXT`
- `updated_at TEXT`
- `account_id TEXT`
- `action_type TEXT`  
  Examples: `post.publish`, `dm.send`, `profile.edit.headline`
- `target_key TEXT`  
  Examples: conversation URN, `me`, profile field name
- `idempotency_key TEXT`
- `desired_fingerprint TEXT`
- `state TEXT`  
  `planned|dry_run|executing|unknown_remote_state|retry_scheduled|succeeded|failed|duplicate_skipped|blocked`
- `dry_run INTEGER`
- `plan_path TEXT`
- `last_error TEXT`
- `remote_ref TEXT`  
  Post URN / conversation event URN / profile version marker if known
- `attempt_count INTEGER`
- `next_attempt_at TEXT`
- `risk_flags TEXT`  
  JSON array
- `webhook_status TEXT`

Unique index:
- `(account_id, idempotency_key)`

### `attempts`
- `attempt_id TEXT PRIMARY KEY`
- `action_id TEXT`
- `attempt_no INTEGER`
- `started_at TEXT`
- `finished_at TEXT`
- `request_path TEXT`
- `request_method TEXT`
- `http_status INTEGER`
- `outcome TEXT`  
  `transport_error|http_error|success|unknown`
- `error TEXT`
- `request_artifact_path TEXT`
- `response_artifact_path TEXT`

### Optional local workflow tables
- `saved_searches`
- `templates`
- `contacts`
- `inbox_triage`
- `prospects`
- `prospect_sources`
- `prospect_signals`
- `prospect_aliases`

### `webhook_deliveries`
- `delivery_id TEXT PRIMARY KEY`
- `action_id TEXT`
- `event_type TEXT`
- `target_url TEXT`
- `payload_hash TEXT`
- `state TEXT`  
  `pending|delivered|failed|retry_scheduled`
- `attempt_count INTEGER`
- `last_error TEXT`
- `next_attempt_at TEXT`

### `profile_snapshots`
- `snapshot_id TEXT PRIMARY KEY`
- `account_id TEXT`
- `captured_at TEXT`
- `source TEXT`
- `artifact_path TEXT`
- `fingerprint TEXT`

## 6) Action plan shape

Every write operation should normalize into one shared structure, something like:

```json
{
  "action_id": "act_...",
  "action_type": "dm.send",
  "account_id": "member:123",
  "target": {
    "conversation_urn": "urn:li:msg_conversation:...",
    "recipient_member_ids": ["123"]
  },
  "desired": {
    "message_text": "Hello there",
    "attachments": []
  },
  "live_request": {
    "method": "POST",
    "path": "/voyager/api/...",
    "headers": {},
    "body": {}
  },
  "reconcile": {
    "strategy": "conversation_contains_message_hash",
    "window_minutes": 10
  },
  "risk": {
    "class": "medium",
    "requires_execute_flag": true,
    "daily_cap_key": "dm_send"
  }
}
```

High-level commands should compile into this plan. The executor should only understand plans.

## 7) Idempotency strategy

### Principle
Idempotency should be based on the **intended remote effect**, not the raw CLI invocation.

### Key formula
Use a canonical JSON serialization of normalized intent, then hash it.

```text
idempotency_key = sha256(
  account_id,
  action_type,
  normalized_target,
  normalized_desired_state,
  meaningful_options
)
```

### Suggested normalized inputs by action

#### `post.publish`
Include:
- account_id
- normalized text (`trim`, normalize line endings, collapse trailing whitespace)
- visibility
- media content hashes if attachments exist
- optional scheduled bucket if scheduled posting is later added

Do **not** include ephemeral timestamps unless they are semantically part of the post.

#### `dm.send`
Include:
- account_id
- conversation URN if known, otherwise sorted resolved recipient member IDs
- normalized message text
- attachment hashes
- message mode (`reply_existing` vs `new_thread`)

#### `profile.edit.*`
Include:
- account_id
- field name or patch path
- normalized desired value

### Duplicate handling rules

If an action with the same `(account_id, idempotency_key)` already exists:
- `planned|executing|retry_scheduled|unknown_remote_state|succeeded` -> return existing action and do not create another.
- `failed` -> allow `action retry` but keep same action id; do not create a second live attempt unless forced.
- `duplicate_skipped` -> return skipped result.

### Dedupe windows
Pragmatic defaults:
- posts: block exact duplicates for 24h unless `--force`
- DMs: block exact duplicates in same conversation for 7d unless `--force`
- profile edits: block indefinitely if desired state already matches remote/current snapshot

## 8) Retry and error recovery

### Before any write
Persist action as `planned`.

### During execution
State transitions:

```text
planned -> executing -> succeeded
planned -> dry_run
executing -> retry_scheduled
executing -> unknown_remote_state
executing -> failed
executing -> blocked
```

### Retry only when appropriate
Auto-retry only on:
- network timeout / connection reset,
- 429,
- 5xx,
- explicit transient parser/extraction failures.

Do **not** auto-retry on:
- 400 validation errors,
- 401/403 auth/challenge failures,
- obvious anti-abuse responses,
- target-not-found errors,
- message/profile policy rejections.

### Unknown remote state handling
This is the most important anti-duplication rule.

If the client times out after sending a write, do **not** immediately resend. Instead:

1. Mark action `unknown_remote_state`.
2. Run reconciler based on action type.
3. Only retry if reconciler finds no evidence the action landed.

### Reconciliation strategies

#### Post publish
- Fetch the actor's recent posts/feed items.
- Compare normalized text hash and recent timestamp window.
- If matched, mark `succeeded` and store discovered post URN.

#### DM send
- Fetch recent events in the target conversation.
- Compare normalized text hash, sender, and recent timestamp window.
- If matched, mark `succeeded` and store message/event URN.

#### Profile edit
- Re-fetch current profile snapshot.
- Compare desired field(s).
- If matched, mark `succeeded`.

### Backoff
Use conservative exponential backoff with jitter, e.g.:
- 1st retry: 2-5 min
- 2nd retry: 10-15 min
- 3rd retry: 30-60 min
- max 3 auto-retries

## 9) Webhook model

Emit webhooks from durable state changes, not directly from volatile in-memory results.

### Event types
- `linkedin.action.planned`
- `linkedin.action.dry_run`
- `linkedin.action.executing`
- `linkedin.action.retry_scheduled`
- `linkedin.action.unknown_remote_state`
- `linkedin.action.succeeded`
- `linkedin.action.failed`
- `linkedin.action.duplicate_skipped`
- `linkedin.action.blocked`

### Payload shape

```json
{
  "event_id": "evt_...",
  "event_type": "linkedin.action.succeeded",
  "occurred_at": "2026-03-16T...Z",
  "action": {
    "action_id": "act_...",
    "action_type": "post.publish",
    "account_id": "member:123",
    "target_key": "me",
    "idempotency_key": "...",
    "state": "succeeded",
    "attempt_count": 1,
    "remote_ref": "urn:li:activity:..."
  },
  "summary": {
    "dry_run": false,
    "risk_flags": [],
    "message": "Post published"
  }
}
```

### Delivery rules
- HMAC-sign payloads with a shared secret.
- Header examples:
  - `X-Hermes-Event-Id`
  - `X-Hermes-Event-Type`
  - `X-Hermes-Signature`
- At-least-once delivery.
- Track webhook deliveries separately from action execution.
- Webhook failures should not roll back successful LinkedIn actions.

## 10) Stealth and account-safety controls

The goal is to look like a cautious human using one browser session, not a bulk automation system.

### Strong defaults
1. **Dry-run by default** for every write.
2. **Single-account, single-write lock**. No parallel writes.
3. **Human pacing**:
   - add 2-8s jitter before live writes,
   - add cool-down after success,
   - configurable daily caps.
4. **Reuse saved session + user agent** from `session.json`.
5. **Do not rotate user agents or IPs aggressively**; that can increase suspicion.
6. **Warm-up GET** before POST for risky operations:
   - fetch feed/profile/conversation first to refresh cookies/context.
7. **No bulk mode by default**.
8. **No automatic retries on abuse/challenge signals**.
9. **No hidden infinite loops / daemons** firing LinkedIn writes.
10. **Redact sensitive tokens/cookies** in artifacts and webhook payloads.

### Recommended caps
- text posts: <= 5/day automated
- DMs: <= 20/day automated, much lower if new targets
- profile edits: <= 5/day automated

### Risk flags to surface
- same exact content recently sent
- target resolution ambiguous
- session age changed recently / fresh login
- endpoint shape changed
- challenge/checkpoint redirect seen
- 429 / anti-abuse hints

If any high-risk flag appears, degrade to `blocked` or require `--force`.

## 11) Execution architecture in code

Recommended internal modules if implementation starts now:

- `linkedin_cli.py`
  - argument parsing only
- `linkedin/session.py`
  - current cookie/session helpers
- `linkedin/write/plans.py`
  - build normalized ActionPlans from CLI args
- `linkedin/write/store.py`
  - SQLite persistence
- `linkedin/write/executor.py`
  - lock, pacing, execute, retry decisions
- `linkedin/write/reconcile.py`
  - action-specific read-after-write verification
- `linkedin/write/webhooks.py`
  - signed delivery and retry
- `linkedin/write/risk.py`
  - caps, dry-run defaults, guardrails

This avoids bloating the current single-file CLI too quickly.

## 12) Most pragmatic implementation order

1. **Add shared action store + action lifecycle commands**
   - `action list/show/retry/reconcile`
2. **Implement dry-run planning for all write commands**
   - no live writes yet
3. **Implement live `post publish` (text only)**
   - with reconciliation against recent posts
4. **Implement live `dm send` for existing conversation only**
   - with reconciliation against recent events
5. **Implement `profile snapshot --me`**
6. **Implement live `profile edit headline/about/website/location`**
   - only after snapshot/diff/reconcile exists
7. **Keep everything else scaffolded** until request capture proves stable

## 13) Practical defaults to recommend to the parent agent

### Best balance of usefulness and maintainability
- **Live now**: text post publish, DM reply into existing thread, profile snapshot.
- **Live next**: profile headline/about/location/website edits.
- **Scaffold only**: new-thread DMs, experience/education edits, invites/connect/follow, bulk actions, media-heavy posting.

### Safe defaults
- every write defaults to dry-run
- `--execute` required for live action
- lock one write at a time per account
- durable action store before network write
- reconcile before retrying any uncertain write
- webhook on state transitions only
- skip duplicate actions using deterministic idempotency keys

## 14) Notes from current codebase

Observed in `tools/linkedin_cli.py`:
- existing session persistence already lives at `~/.hermes/linkedin/session.json`
- existing helpers already provide:
  - cookie-backed session loading/saving,
  - CSRF extraction from `JSESSIONID`,
  - Voyager GET requests,
  - profile/company reads,
  - `status`/`html`/`voyager` commands.

That means the cleanest path is to **keep session/auth code as-is** and add a separate write/action layer on top, rather than mixing ad hoc POST logic directly into each command.
