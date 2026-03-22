# LinkedIn CLI Foundation Upgrades Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Improve linkedin-cli across operator UX, reliability/safety, and local workflow support without changing the core session model.

**Architecture:** Extend the existing argparse CLI and SQLite store instead of introducing new services. Add small, composable modules for formatting and workflow persistence, then harden executor and reconciliation behavior around the existing write pipeline.

**Tech Stack:** Python 3.11+, argparse, sqlite3, requests, unittest/pytest

---

### Task 1: CLI UX foundation

**Files:**
- Create: `linkedin_cli/output.py`
- Modify: `linkedin_cli/cli.py`
- Test: `tests/test_output.py`

**Step 1: Write failing tests**
- Cover JSON, table, and quiet output formatting.
- Cover doctor command exit behavior for missing config/session.

**Step 2: Run tests to verify failure**

Run: `pytest tests/test_output.py -v`

**Step 3: Write minimal implementation**
- Add output helpers shared by read and action commands.
- Add global output-mode flags and `doctor`.

**Step 4: Run tests to verify pass**

Run: `pytest tests/test_output.py -v`

### Task 2: Action lifecycle improvements

**Files:**
- Modify: `linkedin_cli/write/store.py`
- Modify: `linkedin_cli/write/reconcile.py`
- Modify: `linkedin_cli/cli.py`
- Test: `tests/test_actions.py`

**Step 1: Write failing tests**
- Cover action cancel, reconcile, and artifact/plan inspection behavior.
- Cover state-transition guardrails for canceled actions.

**Step 2: Run tests to verify failure**

Run: `pytest tests/test_actions.py -v`

**Step 3: Write minimal implementation**
- Add cancel state and store helpers.
- Add CLI commands for `action reconcile`, `action cancel`, and `action artifacts`.

**Step 4: Run tests to verify pass**

Run: `pytest tests/test_actions.py -v`

### Task 3: Reliability and safety hardening

**Files:**
- Modify: `linkedin_cli/write/executor.py`
- Modify: `linkedin_cli/write/reconcile.py`
- Modify: `linkedin_cli/session.py`
- Test: `tests/test_executor.py`
- Test: `tests/test_reconcile.py`

**Step 1: Write failing tests**
- Cover retryable vs terminal failures.
- Cover unknown remote state on transport uncertainty.
- Cover reconciliation status updates.

**Step 2: Run tests to verify failure**

Run: `pytest tests/test_executor.py tests/test_reconcile.py -v`

**Step 3: Write minimal implementation**
- Classify error outcomes.
- Avoid immediate duplicate writes after uncertain outcomes.
- Improve reconcile dispatch and evidence returned to users.

**Step 4: Run tests to verify pass**

Run: `pytest tests/test_executor.py tests/test_reconcile.py -v`

### Task 4: Local workflow features

**Files:**
- Create: `linkedin_cli/workflow.py`
- Modify: `linkedin_cli/write/store.py`
- Modify: `linkedin_cli/cli.py`
- Test: `tests/test_workflow.py`

**Step 1: Write failing tests**
- Cover saved searches, templates, contact notes/tags, and CSV import/export.

**Step 2: Run tests to verify failure**

Run: `pytest tests/test_workflow.py -v`

**Step 3: Write minimal implementation**
- Add local SQLite-backed workflow tables and helper APIs.
- Expose CLI commands for CRUD and CSV round-trips.

**Step 4: Run tests to verify pass**

Run: `pytest tests/test_workflow.py -v`

### Task 5: Docs and full verification

**Files:**
- Modify: `README.md`
- Modify: `docs/architecture.md`

**Step 1: Update docs**
- Document new commands, output modes, reliability behavior, and workflow storage.

**Step 2: Run focused verification**

Run: `pytest -q`

**Step 3: Run CLI smoke checks**

Run: `python -m linkedin_cli --help`

Run: `python -m linkedin_cli doctor --help`

Run: `python -m linkedin_cli workflow --help`
