# LinkedIn CLI Discovery Feedback Loop Phase 2 Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Close the biggest gaps in the discovery system by adding automatic public engagement ingestion, deeper DM feedback, identity dedupe/merge, queue lifecycle hooks, broader search ingestion, richer analytics, and adaptive scoring.

**Architecture:** Extend the discovery SQLite layer with aliases, dedupe keys, and queue-state operations. Add one new engagement ingestion path that scrapes public LinkedIn post pages for commenters and engagement counts, deepen inbox ingestion using message records already available from Voyager, and make ranking partially data-driven by learning source/signal multipliers from historical positive outcomes.

**Tech Stack:** Python 3.11+, argparse, sqlite3, requests, BeautifulSoup, unittest/pytest

---

### Task 1: Discovery identity and dedupe foundation

**Files:**
- Modify: `linkedin_cli/discovery.py`
- Test: `tests/test_discovery_identity.py`

**Step 1: Write the failing test**
- Cover alias-based identity resolution with slug, member URN, URL, and display-name fallback.
- Cover deduped source/signal insertion with stable dedupe keys.

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_discovery_identity.py -v`

**Step 3: Write minimal implementation**
- Add alias storage.
- Resolve canonical prospect keys before writes.
- Add dedupe keys or uniqueness windows for sources/signals.

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_discovery_identity.py -v`

### Task 2: Public engagement ingestion

**Files:**
- Modify: `linkedin_cli/discovery.py`
- Modify: `linkedin_cli/cli.py`
- Test: `tests/test_discovery_public_engagement.py`

**Step 1: Write the failing test**
- Cover ingesting comment actors from a public post page payload.
- Cover aggregate reaction/comment counts being captured in metadata.

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_discovery_public_engagement.py -v`

**Step 3: Write minimal implementation**
- Parse public post HTML / JSON-LD for commenter identities and counts.
- Add `discover ingest-engagement`.

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_discovery_public_engagement.py -v`

### Task 3: Deeper inbox feedback and queue lifecycle

**Files:**
- Modify: `linkedin_cli/cli.py`
- Modify: `linkedin_cli/discovery.py`
- Test: `tests/test_discovery_inbox_feedback.py`

**Step 1: Write the failing test**
- Cover inbound/outbound DM signals and reply direction inference.
- Cover `discover state set`.
- Cover discovery state/signal updates after live `dm send`, `connect`, and `follow`.

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_discovery_inbox_feedback.py -v`

**Step 3: Write minimal implementation**
- Reuse message records from inbox payloads.
- Add queue state mutation helpers and command.
- Hook successful write commands into discovery updates.

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_discovery_inbox_feedback.py -v`

### Task 4: Broader search ingestion and analytics

**Files:**
- Modify: `linkedin_cli/discovery.py`
- Modify: `linkedin_cli/cli.py`
- Test: `tests/test_discovery_analytics.py`

**Step 1: Write the failing test**
- Cover ingesting `people`, `companies`, and `posts`.
- Cover reply rate, acceptance rate, source conversion, and template performance stats.

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_discovery_analytics.py -v`

**Step 3: Write minimal implementation**
- Broaden entity ingestion.
- Compute richer aggregate stats from signals/sources/metadata.

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_discovery_analytics.py -v`

### Task 5: Adaptive ranking

**Files:**
- Modify: `linkedin_cli/discovery.py`
- Test: `tests/test_discovery_learning.py`

**Step 1: Write the failing test**
- Cover source/signal score changes after positive outcomes are observed.
- Cover queue order changing because of learned multipliers, not only static weights.

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_discovery_learning.py -v`

**Step 3: Write minimal implementation**
- Learn source and signal multipliers from accepted/replied/engaged/won outcomes.
- Add learned score components to queue explanations.

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_discovery_learning.py -v`

### Task 6: Docs and verification

**Files:**
- Modify: `README.md`
- Modify: `docs/architecture.md`

**Step 1: Update docs**
- Document `discover ingest-engagement`, queue state management, adaptive scoring, and richer stats.

**Step 2: Run full verification**

Run: `pytest -q`

Run: `python -m linkedin_cli discover --help`

Run: `python -m linkedin_cli discover ingest-engagement --help`

Run: `python -m linkedin_cli discover stats`
