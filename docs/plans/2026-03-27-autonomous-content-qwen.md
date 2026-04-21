# Autonomous Content + Qwen System Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build a fully local autonomous content system that can learn niche-specific winning formats, generate multiple post candidates, choose and execute the best action, log every decision and outcome, and train a small local Qwen model for content generation without collapsing the system into one opaque agent.

**Architecture:** The system has five separate layers: `corpus -> generator -> rankers -> policy -> executors`. Qwen is only the generator. Existing content and lead models remain rankers. A policy layer chooses actions from scored candidates and logs propensities and rewards. LinkedIn CLI remains the executor and telemetry collector. This separation is mandatory so the system can be debugged, benchmarked, retrained, and audited locally.

**Tech Stack:** Python 3.11, existing `linkedin_cli/content.py` and warehouse stack, DuckDB, local JSON/JSONL artifacts, optional local fine-tuning stack for Qwen, pi-mono-compatible runtime contract, no hosted vendors.

---

## Implementation Standard

This plan requires full production implementations, not thin happy-path patches.

Every task must include:
- complete CLI wiring
- persistence where state matters
- auditability and artifact traces
- negative-path handling
- deterministic tests for success and failure cases
- docs or runbook updates when a user-facing workflow changes

Forbidden implementation style:
- stub commands that only print placeholders
- fake model layers with hardcoded outputs
- queue records without provenance
- autonomy features without policy logs
- training commands without reproducible dataset artifacts
- code that passes a narrow test but omits the real workflow

For this plan, "done" means:
- the feature is usable end-to-end from the CLI
- the resulting state can be inspected locally
- failures are explicit and recoverable
- test coverage includes realistic edge cases
- live-safe execution paths are separated from offline and training paths

## Why The Earlier Plan Was Weak

The earlier version was too feature-driven. It assumed that adding `create`, `choose`, and some RL scaffolding would naturally produce autonomy. It would not. The real risks are:

- generation, scoring, and action choice getting mixed together
- RL being applied before rewards and action logs are trustworthy
- the content model learning broad LinkedIn virality instead of niche relevance
- “full auto” becoming an un-debuggable script pile with no policy evidence

This stronger plan fixes that by locking:

- explicit system boundaries
- hard data contracts
- offline evaluation gates
- phased training from SFT to preferences to policy learning
- a concrete distinction between content optimization and action optimization

## System Boundaries

### 1. Corpus Layer

Purpose:
- harvest and normalize posts
- track industries, topics, exemplars, and outcomes
- provide high-signal slices such as `ai + workflow`

Owned by:
- `linkedin_cli/content.py`
- `linkedin_cli/content_warehouse.py`

Outputs:
- slice playbooks
- exemplar sets
- outcome datasets
- Qwen SFT and preference corpora

### 2. Generator Layer

Purpose:
- produce diverse candidate drafts for a specific slice and goal

Owned by:
- `linkedin_cli/content.py`
- later `linkedin_cli/qwen_training.py`

Key rule:
- Qwen writes candidates; it does not decide whether they are good enough to publish.

### 3. Ranker Layer

Purpose:
- estimate `P(content_success)`
- estimate `P(reply)`, `P(connect_accept)`, `P(meeting)` for leads

Owned by:
- existing model code in `linkedin_cli/content.py`
- `linkedin_cli/modeling.py`

Key rule:
- rankers consume candidates and context; they do not call live actions.

### 4. Policy Layer

Purpose:
- choose among ranked actions
- log chosen action, available alternatives, and propensity
- support offline IPS/SNIPS evaluation

Owned by:
- new `linkedin_cli/policy.py`

Key rule:
- policy chooses from scored options; it does not generate text.

### 5. Executor Layer

Purpose:
- publish posts
- reply to comments
- send DMs
- collect outcomes

Owned by:
- existing CLI commands
- `linkedin_cli/comment.py`
- `linkedin_cli/lead.py`
- `linkedin_cli/write/*`

Key rule:
- executors perform deterministic actions and return artifacts and telemetry.

## Training Strategy

### Content Model Training

This is not one model.

We need:
- retrieval over winning niche posts
- rankers for predicted content outcome
- a local Qwen generator for candidate production

### Qwen Training Phases

#### Phase 1: SFT

Train Qwen on:
- owned high-performing posts
- cleaned high-performing corpus posts
- chosen candidates from the content queue
- prompt-to-draft pairs built from slice playbooks and exemplars

Goal:
- make Qwen write in-domain, non-generic, niche-specific LinkedIn content

#### Phase 2: Preference Optimization

Train Qwen on:
- candidate pairs where one draft scored or performed better than another
- maximize outputs vs weaker rewrites
- chosen vs rejected candidates from the queue

Use:
- DPO or ORPO style preference optimization, locally

Goal:
- make Qwen prefer better formats without putting it in a live RL loop yet

#### Phase 3: Policy Learning

This is where RL belongs.

But not inside the generator.

Use a contextual bandit or logged-action policy for:
- whether to publish now vs wait
- which candidate to post
- whether to reply publicly vs DM vs do nothing
- which lead to contact next

Do **not** start with PPO/GRPO over live LinkedIn execution. That is the wrong first RL target. It is high-variance, easy to reward-hack, and impossible to debug cleanly.

The right first RL target is:
- logged, offline, contextual policy optimization over stored decisions and outcomes

## Hard Success Metrics

The system is not “done” because it compiles.

### Content Metrics

- `content create` produces at least `8` structurally distinct candidates for a slice
- `content choose` emits a clear winner with model rationale
- slice-specific playbooks avoid off-topic mass-viral contamination
- chosen candidates outperform the median generated candidate in offline scoring

### Policy Metrics

- every publish/reply/DM decision logs:
  - context
  - candidate set
  - chosen action
  - propensity
  - eventual reward
- IPS/SNIPS can evaluate a new policy offline before it becomes default

### Training Metrics

- Qwen SFT dataset is reproducible from local artifacts
- preference dataset is reproducible from candidate decisions
- reward dataset is time-split and slice-aware
- each model artifact has:
  - data source summary
  - sample count
  - train/val/test metrics
  - created-at timestamp

## Data Contracts To Add

### `content_candidates`

Stores:
- candidate id
- prompt seed
- slice `(industry, topics)`
- generator source (`heuristic`, `qwen-sft`, `qwen-pref`)
- candidate text
- score summary
- chosen flag
- publish status
- exemplar refs
- creation timestamp

### `policy_decisions`

Stores:
- policy name
- context type (`content_publish`, `comment_reply`, `lead_dm`)
- context payload
- available actions
- chosen action
- propensity
- score snapshot
- execution mode
- timestamp

### `policy_rewards`

Stores:
- decision id
- reward type
- reward value
- attribution window
- raw telemetry

### `qwen_artifacts`

Stores:
- model name
- phase (`sft`, `preference`)
- dataset path
- config path
- metrics
- created-at

## Build Order

### Task 1: Content Create + Choose Core

**Files:**
- Modify: `linkedin_cli/content.py`
- Modify: `linkedin_cli/cli.py`
- Modify: `tests/test_content_harvest.py`
- Modify: `tests/test_cli_surface.py`

**Step 1: Write the failing tests**

Add tests for:
- `content create` producing multiple slice-specific candidates
- `content choose` selecting the highest-ranked candidate with rationale
- parser coverage for both commands

**Step 2: Run tests to verify failure**

Run:
```bash
/Users/Cody/mambaforge/bin/python -m pytest tests/test_content_harvest.py -k "create or choose" -q
/Users/Cody/mambaforge/bin/python -m pytest tests/test_cli_surface.py -k "create or choose" -q
```

**Step 3: Write full implementation**

Add:
- `create_drafts(...)`
- `choose_draft(...)`
- CLI commands:
  - `linkedin content create`
  - `linkedin content choose`

`create` must:
- load slice playbook
- retrieve slice exemplars
- generate candidates across at least:
  - narrative
  - proof-heavy how-to
  - authority
  - contrarian
  - launch/announcement
- score each candidate

`choose` must:
- rank candidates
- expose why the winner won
- output references to the playbook and exemplars used

**Step 4: Run tests to verify pass**

Run:
```bash
/Users/Cody/mambaforge/bin/python -m pytest tests/test_content_harvest.py -k "create or choose" -q
/Users/Cody/mambaforge/bin/python -m pytest tests/test_cli_surface.py -k "create or choose" -q
```

**Step 5: Commit**

```bash
git add linkedin_cli/content.py linkedin_cli/cli.py tests/test_content_harvest.py tests/test_cli_surface.py
git commit -m "feat: add content create and choose core"
```

### Task 2: Candidate Queue + Provenance

**Files:**
- Modify: `linkedin_cli/content.py`
- Modify: `linkedin_cli/write/store.py`
- Modify: `linkedin_cli/cli.py`
- Create: `tests/test_content_candidates.py`

**Step 1: Write the failing test**

Test candidate persistence with:
- slice metadata
- exemplar refs
- chosen state
- publish status
- generator source

**Step 2: Run test to verify failure**

Run:
```bash
/Users/Cody/mambaforge/bin/python -m pytest tests/test_content_candidates.py -q
```

**Step 3: Implement full production flow**

Add:
- `content queue`
- `content show-candidate`
- `content mark-published`
- persistent candidate storage

**Step 4: Run tests to verify pass**

Run:
```bash
/Users/Cody/mambaforge/bin/python -m pytest tests/test_content_candidates.py -q
```

**Step 5: Commit**

```bash
git add linkedin_cli/content.py linkedin_cli/write/store.py linkedin_cli/cli.py tests/test_content_candidates.py
git commit -m "feat: persist candidate queue and provenance"
```

### Task 3: Policy Logging + Offline Evaluation

**Files:**
- Create: `linkedin_cli/policy.py`
- Modify: `linkedin_cli/content.py`
- Modify: `linkedin_cli/lead.py`
- Modify: `linkedin_cli/cli.py`
- Create: `tests/test_policy.py`

**Step 1: Write the failing test**

Test:
- decision logging
- propensity capture
- reward logging
- IPS and SNIPS evaluation

**Step 2: Run test to verify failure**

Run:
```bash
/Users/Cody/mambaforge/bin/python -m pytest tests/test_policy.py -q
```

**Step 3: Implement full production flow**

Add:
- `log_decision(...)`
- `record_reward(...)`
- `evaluate_ips(...)`
- `evaluate_snips(...)`
- CLI:
  - `content policy-report`
  - `content train-policy`

**Step 4: Run tests to verify pass**

Run:
```bash
/Users/Cody/mambaforge/bin/python -m pytest tests/test_policy.py -q
```

**Step 5: Commit**

```bash
git add linkedin_cli/policy.py linkedin_cli/content.py linkedin_cli/lead.py linkedin_cli/cli.py tests/test_policy.py
git commit -m "feat: add policy logging and offline evaluation"
```

### Task 4: Reward Dataset Builders

**Files:**
- Modify: `linkedin_cli/content.py`
- Modify: `linkedin_cli/content_warehouse.py`
- Create: `tests/test_content_rewards.py`

**Step 1: Write the failing test**

Test that owned-post rewards and policy rewards can be built with:
- time-split train/val/test
- slice filters
- normalized outcome targets

**Step 2: Run test to verify failure**

Run:
```bash
/Users/Cody/mambaforge/bin/python -m pytest tests/test_content_rewards.py -q
```

**Step 3: Implement full production flow**

Add:
- `content build-reward-dataset`
- `content build-policy-dataset`

**Step 4: Run tests to verify pass**

Run:
```bash
/Users/Cody/mambaforge/bin/python -m pytest tests/test_content_rewards.py -q
```

**Step 5: Commit**

```bash
git add linkedin_cli/content.py linkedin_cli/content_warehouse.py tests/test_content_rewards.py
git commit -m "feat: add reward dataset builders"
```

### Task 5: Local Qwen Dataset Builders

**Files:**
- Create: `linkedin_cli/qwen_training.py`
- Modify: `linkedin_cli/content.py`
- Modify: `linkedin_cli/cli.py`
- Create: `tests/test_qwen_training.py`

**Step 1: Write the failing test**

Test:
- SFT dataset generation
- preference dataset generation
- artifact metadata

**Step 2: Run test to verify failure**

Run:
```bash
/Users/Cody/mambaforge/bin/python -m pytest tests/test_qwen_training.py -q
```

**Step 3: Implement full production flow**

Add:
- `content build-sft-dataset`
- `content build-preference-dataset`
- local artifact registry for training corpora

**Step 4: Run tests to verify pass**

Run:
```bash
/Users/Cody/mambaforge/bin/python -m pytest tests/test_qwen_training.py -q
```

**Step 5: Commit**

```bash
git add linkedin_cli/qwen_training.py linkedin_cli/content.py linkedin_cli/cli.py tests/test_qwen_training.py
git commit -m "feat: add qwen dataset builders"
```

### Task 6: Local Qwen Training Harness

**Files:**
- Modify: `pyproject.toml`
- Create: `scripts/train_qwen_content.py`
- Modify: `Dockerfile`
- Modify: `docker-compose.yml`
- Create: `tests/test_qwen_harness.py`

**Step 1: Write the failing test**

Test:
- config generation
- local output paths
- artifact registry updates

**Step 2: Run test to verify failure**

Run:
```bash
/Users/Cody/mambaforge/bin/python -m pytest tests/test_qwen_harness.py -q
```

**Step 3: Implement full production flow**

Support:
- local-only SFT runs
- local-only preference runs
- output under `.artifacts/qwen`
- no vendor dependencies

**Step 4: Run tests to verify pass**

Run:
```bash
/Users/Cody/mambaforge/bin/python -m pytest tests/test_qwen_harness.py -q
```

**Step 5: Commit**

```bash
git add pyproject.toml scripts/train_qwen_content.py Dockerfile docker-compose.yml tests/test_qwen_harness.py
git commit -m "feat: add local qwen harness"
```

### Task 7: Runtime Contract For pi-mono Or Equivalent

**Files:**
- Create: `docs/architecture/autonomous-content-runtime.md`
- Create: `linkedin_cli/runtime_contract.py`
- Modify: `README.md`
- Create: `tests/test_runtime_contract.py`

**Step 1: Write the failing test**

Test that the runtime contract can carry:
- content candidate generation requests
- choose/publish decisions
- comment reply decisions
- DM decisions

**Step 2: Run test to verify failure**

Run:
```bash
/Users/Cody/mambaforge/bin/python -m pytest tests/test_runtime_contract.py -q
```

**Step 3: Implement full production flow**

Define:
- structured context
- candidate action list
- score snapshots
- chosen action
- execution response

**Step 4: Run tests to verify pass**

Run:
```bash
/Users/Cody/mambaforge/bin/python -m pytest tests/test_runtime_contract.py -q
```

**Step 5: Commit**

```bash
git add docs/architecture/autonomous-content-runtime.md linkedin_cli/runtime_contract.py README.md tests/test_runtime_contract.py
git commit -m "feat: add runtime contract for autonomous control"
```

### Task 8: Autonomous Execution Modes

**Files:**
- Modify: `linkedin_cli/cli.py`
- Modify: `linkedin_cli/policy.py`
- Modify: `linkedin_cli/content.py`
- Create: `tests/test_autonomy_mode.py`

**Step 1: Write the failing test**

Test:
- `queue`
- `approval`
- `full-auto`
- explicit enable flag
- deterministic audit trail

**Step 2: Run test to verify failure**

Run:
```bash
/Users/Cody/mambaforge/bin/python -m pytest tests/test_autonomy_mode.py -q
```

**Step 3: Implement full production flow**

Add:
- `--autonomy-mode queue|approval|full-auto`
- full-auto decision logging requirement
- replayable action records

**Step 4: Run tests to verify pass**

Run:
```bash
/Users/Cody/mambaforge/bin/python -m pytest tests/test_autonomy_mode.py -q
```

**Step 5: Commit**

```bash
git add linkedin_cli/cli.py linkedin_cli/policy.py linkedin_cli/content.py tests/test_autonomy_mode.py
git commit -m "feat: add autonomous execution modes"
```

### Task 9: End-to-End Local Verification

**Files:**
- Modify: `README.md`
- Create: `docs/runbooks/local-autonomous-content.md`

**Step 1: Run focused suites**

Run:
```bash
/Users/Cody/mambaforge/bin/python -m pytest tests/test_content_harvest.py tests/test_content_candidates.py tests/test_policy.py tests/test_content_rewards.py tests/test_qwen_training.py tests/test_qwen_harness.py tests/test_runtime_contract.py tests/test_autonomy_mode.py -q
```

**Step 2: Run full suite**

Run:
```bash
/Users/Cody/mambaforge/bin/python -m pytest -q
```

**Step 3: Local smoke**

Run:
```bash
/Users/Cody/mambaforge/bin/python -m linkedin_cli --json content create --industry ai --topic workflow --prompt "AI workflow orchestration is still too brittle"
/Users/Cody/mambaforge/bin/python -m linkedin_cli --json content choose --industry ai --topic workflow --prompt "AI workflow orchestration is still too brittle"
/Users/Cody/mambaforge/bin/python -m linkedin_cli --json content build-sft-dataset --industry ai --topic workflow --output .artifacts/qwen/sft-smoke
```

**Step 4: Commit docs**

```bash
git add README.md docs/runbooks/local-autonomous-content.md
git commit -m "docs: add autonomous content local runbook"
```

## Rollout Gates

Do not skip these:

1. `content create` and `choose` must work before any Qwen work.
2. Policy decision logging must exist before any RL or bandit training.
3. Qwen SFT must exist before preference optimization.
4. Preference optimization must exist before any autonomous full-auto mode is enabled.
5. Full-auto must never become default without offline policy evidence.

## Hard Rules

- Do not merge generation and policy into one model.
- Do not run online RL directly against live LinkedIn actions as the first policy loop.
- Keep every model artifact reproducible from local stored data.
- Every autonomous decision must be replayable from logged context and artifacts.
- Prefer simpler methods first:
  - SFT before preference optimization
  - preference optimization before RL
  - contextual bandit before heavier RL

Plan complete and saved to `docs/plans/2026-03-27-autonomous-content-qwen.md`. Two execution options:

**1. Subagent-Driven (this session)** - I dispatch fresh subagent per task, review between tasks, fast iteration

**2. Parallel Session (separate)** - Open new session with executing-plans, batch execution with checkpoints

**Which approach?**
