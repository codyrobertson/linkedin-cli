# LinkedIn CLI Discovery Queue Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build a unified local prospect queue that merges discovery from search, inbox, and engagement signals into one ranked operator surface.

**Architecture:** Use the existing SQLite state database as the source of truth. Add discovery-specific tables and helper functions in a new module, then expose a `discover` command group in the CLI that can ingest, score, inspect, and summarize prospects without introducing external services.

**Tech Stack:** Python 3.11+, argparse, sqlite3, requests, unittest/pytest

---

### Task 1: Discovery storage and scoring

**Files:**
- Create: `linkedin_cli/discovery.py`
- Modify: `linkedin_cli/write/store.py`
- Test: `tests/test_discovery.py`

**Step 1: Write the failing tests**
- Cover prospect upsert, source attachment, signal attachment, score recalculation, and ranked queue listing.

**Step 2: Run tests to verify failure**

Run: `pytest tests/test_discovery.py -v`

**Step 3: Write minimal implementation**
- Add discovery tables.
- Implement deterministic score calculation with fit, intent, freshness, saturation, and staleness components.

**Step 4: Run tests to verify pass**

Run: `pytest tests/test_discovery.py -v`

### Task 2: Discovery CLI surface

**Files:**
- Modify: `linkedin_cli/cli.py`
- Test: `tests/test_discover_cli.py`

**Step 1: Write the failing tests**
- Cover parser support for `discover ingest-search`, `discover ingest-inbox`, `discover signal add`, `discover queue`, `discover show`, and `discover stats`.

**Step 2: Run tests to verify failure**

Run: `pytest tests/test_discover_cli.py -v`

**Step 3: Write minimal implementation**
- Add the command tree and handlers.
- Reuse existing search and inbox read paths where possible.

**Step 4: Run tests to verify pass**

Run: `pytest tests/test_discover_cli.py -v`

### Task 3: Search and inbox ingestion

**Files:**
- Modify: `linkedin_cli/cli.py`
- Modify: `linkedin_cli/search.py`
- Test: `tests/test_discovery_ingest.py`

**Step 1: Write the failing tests**
- Cover ingesting people from a query/saved search.
- Cover ingesting conversation participants from inbox reads.

**Step 2: Run tests to verify failure**

Run: `pytest tests/test_discovery_ingest.py -v`

**Step 3: Write minimal implementation**
- Normalize search results into prospects.
- Normalize inbox participants into prospects and attach strong-intent signals.

**Step 4: Run tests to verify pass**

Run: `pytest tests/test_discovery_ingest.py -v`

### Task 4: Engagement feedback loop

**Files:**
- Modify: `linkedin_cli/discovery.py`
- Modify: `linkedin_cli/cli.py`
- Test: `tests/test_discovery_signals.py`

**Step 1: Write the failing tests**
- Cover manual/public engagement signal ingestion.
- Cover score updates and queue reordering after signals.
- Cover per-source and per-signal summary stats.

**Step 2: Run tests to verify failure**

Run: `pytest tests/test_discovery_signals.py -v`

**Step 3: Write minimal implementation**
- Add signal types and weights.
- Add summary and stats helpers.

**Step 4: Run tests to verify pass**

Run: `pytest tests/test_discovery_signals.py -v`

### Task 5: Docs and verification

**Files:**
- Modify: `README.md`
- Modify: `docs/architecture.md`

**Step 1: Update docs**
- Document the `discover` command group, queue semantics, and score/feedback concepts.

**Step 2: Run verification**

Run: `pytest -q`

Run: `python -m linkedin_cli discover --help`

Run: `python -m linkedin_cli discover queue --help`
