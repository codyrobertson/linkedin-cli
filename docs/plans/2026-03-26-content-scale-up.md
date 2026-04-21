# Content Scale-Up Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Make content harvesting and training reliable enough to support multi-industry corpora and long-running resumable harvest jobs.

**Architecture:** Extend the local operational schema from single-industry rows to normalized post-industry mappings, remove nested write-connection patterns that trigger SQLite lock contention, add resumable harvest job state to split acquisition from storage, and add external vector export so large corpora can be moved into a dedicated ANN service. Keep the current CLI as the control plane while preserving backward compatibility for existing local state.

**Tech Stack:** Python 3.11, SQLite, argparse CLI, local persisted retrieval index, JSONL export for external ANN backends.

---

### Task 1: Multi-Industry Storage

**Files:**
- Modify: `linkedin_cli/content.py`
- Modify: `linkedin_cli/cli.py`
- Test: `tests/test_content_harvest.py`

**Step 1: Write the failing tests**

Add tests that prove:
- `harvest_posts()` accepts multiple industries and persists them to each stored post.
- `list_posts(industry=...)` filters through a normalized industry mapping, not a single `industry` column.
- `content_stats()` counts posts under every mapped industry.

**Step 2: Run the targeted tests to verify they fail**

Run: `python -m pytest -q tests/test_content_harvest.py -k "multi_industry or stats_include_multiple_industries"`

Expected: FAIL because `harvest_posts()` only accepts one `industry` and stats/listing only read `harvested_posts.industry`.

**Step 3: Write the minimal implementation**

Implement:
- `harvested_post_industries(url, industry, created_at, updated_at)` table.
- Helper functions to upsert and read industry mappings.
- `harvest_posts(..., industries=[...])` and CLI pass-through for repeatable `--industry`.
- Backward-compatible derived `industry` field on returned records, using the first sorted mapped industry when needed.

**Step 4: Run the targeted tests to verify they pass**

Run: `python -m pytest -q tests/test_content_harvest.py -k "multi_industry or stats_include_multiple_industries"`

Expected: PASS

**Step 5: Commit**

```bash
git add linkedin_cli/content.py linkedin_cli/cli.py tests/test_content_harvest.py docs/plans/2026-03-26-content-scale-up.md
git commit -m "feat: normalize harvested post industries"
```

### Task 2: Lock-Safe Outcome Sync

**Files:**
- Modify: `linkedin_cli/content.py`
- Test: `tests/test_content_harvest.py`

**Step 1: Write the failing test**

Add a test that patches `init_content_db()` or wraps `store._connect()` to prove `sync_post_outcomes()` currently re-enters schema initialization / nested write connections while already holding a write connection.

**Step 2: Run the targeted test to verify it fails**

Run: `python -m pytest -q tests/test_content_harvest.py -k lock_safe_sync`

Expected: FAIL because `sync_post_outcomes()` calls `upsert_post()`, which calls `init_content_db()` and opens another connection.

**Step 3: Write the minimal implementation**

Implement:
- connection-aware internal write helpers (for example `_upsert_post_conn(...)`).
- `upsert_post()` becomes a wrapper that initializes once and then delegates.
- `sync_post_outcomes()` and `sync_owned_post_telemetry()` use one connection for the whole batch.

**Step 4: Run the targeted test to verify it passes**

Run: `python -m pytest -q tests/test_content_harvest.py -k lock_safe_sync`

Expected: PASS

**Step 5: Commit**

```bash
git add linkedin_cli/content.py tests/test_content_harvest.py
git commit -m "fix: make content outcome sync lock-safe"
```

### Task 3: Resumable Harvest Jobs

**Files:**
- Modify: `linkedin_cli/content.py`
- Modify: `linkedin_cli/cli.py`
- Test: `tests/test_content_harvest.py`

**Step 1: Write the failing tests**

Add tests that prove:
- a harvest job can be created with query plans and checkpoints
- a partial run saves progress
- a resumed run continues from the remaining queries/pages instead of restarting from scratch

**Step 2: Run the targeted tests to verify they fail**

Run: `python -m pytest -q tests/test_content_harvest.py -k harvest_job`

Expected: FAIL because no job tables or resume-aware APIs exist.

**Step 3: Write the minimal implementation**

Implement:
- `content_harvest_jobs` and `content_harvest_job_queries` tables.
- `start_harvest_job()`, `resume_harvest_job()`, and progress checkpoint updates.
- CLI surfaces:
  - `content harvest --job-name ...`
  - `content harvest --resume-job <job-id>`
  - `content harvest-jobs`

**Step 4: Run the targeted tests to verify they pass**

Run: `python -m pytest -q tests/test_content_harvest.py -k harvest_job`

Expected: PASS

**Step 5: Commit**

```bash
git add linkedin_cli/content.py linkedin_cli/cli.py tests/test_content_harvest.py
git commit -m "feat: add resumable content harvest jobs"
```

### Task 4: External ANN Export Prep

**Files:**
- Modify: `linkedin_cli/retrieval_index.py`
- Modify: `linkedin_cli/content.py`
- Modify: `linkedin_cli/cli.py`
- Test: `tests/test_retrieval_index.py`
- Test: `tests/test_content_harvest.py`

**Step 1: Write the failing tests**

Add tests that prove:
- stored vectors can be exported as JSONL payloads for an external ANN backend
- exports include stable ids, vector payloads, industry/topic metadata, and timestamps
- the CLI returns the export path and record counts

**Step 2: Run the targeted tests to verify they fail**

Run: `python -m pytest -q tests/test_retrieval_index.py tests/test_content_harvest.py -k export`

Expected: FAIL because no export helper or CLI path exists.

**Step 3: Write the minimal implementation**

Implement:
- JSONL export helper in `retrieval_index.py`
- content-facing wrapper that exports `semantic`, `fingerprint`, or `all`
- CLI surface:
  - `content export-index --kind semantic --output <dir>`

**Step 4: Run the targeted tests to verify they pass**

Run: `python -m pytest -q tests/test_retrieval_index.py tests/test_content_harvest.py -k export`

Expected: PASS

**Step 5: Commit**

```bash
git add linkedin_cli/retrieval_index.py linkedin_cli/content.py linkedin_cli/cli.py tests/test_retrieval_index.py tests/test_content_harvest.py
git commit -m "feat: export content vectors for external ann backends"
```

### Task 5: Full Verification

**Files:**
- Modify: `linkedin_cli/content.py`
- Modify: `linkedin_cli/cli.py`
- Modify: `linkedin_cli/retrieval_index.py`
- Modify: `tests/test_content_harvest.py`
- Modify: `tests/test_retrieval_index.py`

**Step 1: Run targeted tests**

Run:
- `python -m pytest -q tests/test_content_harvest.py`
- `python -m pytest -q tests/test_retrieval_index.py`

Expected: PASS

**Step 2: Run the full suite**

Run: `python -m pytest -q`

Expected: PASS

**Step 3: Run live smoke checks**

Run:
- `python -m linkedin_cli --json content harvest --industry ai --industry fintech --topic agents --limit 20 --per-query 10 --job-name scale-smoke`
- `python -m linkedin_cli --json content harvest --resume-job scale-smoke`
- `python -m linkedin_cli --json content stats`
- `python -m linkedin_cli --json content export-index --kind semantic --output .artifacts/content-index`

Expected:
- industries are not collapsed to one value
- resume returns job progress instead of restarting
- stats show multiple industries
- export writes JSONL manifests successfully

**Step 4: Commit**

```bash
git add linkedin_cli/content.py linkedin_cli/cli.py linkedin_cli/retrieval_index.py tests/test_content_harvest.py tests/test_retrieval_index.py docs/plans/2026-03-26-content-scale-up.md
git commit -m "feat: scale content harvest storage and indexing"
```
