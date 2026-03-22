# Packaging Distribution Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Publish the CLI as an installable Python package with GitHub-based CI, GitHub Releases, PyPI Trusted Publishing, and a starter Homebrew formula path.

**Architecture:** Keep the import package as `linkedin_cli`, rename only the published distribution to an available PyPI-safe name, and preserve the `linkedin` console script for operator continuity. Add repository-local GitHub Actions for test/build/release automation, plus a Homebrew formula template that can be copied into a tap once the first release tarball exists.

**Tech Stack:** Python 3.11+, setuptools, GitHub Actions, PyPI Trusted Publishing, Homebrew formula DSL, pytest

---

### Task 1: Lock the packaging contract with tests

**Files:**
- Create: `tests/test_packaging_distribution.py`
- Modify: none

**Step 1: Write the failing test**

Add tests that parse `pyproject.toml` and assert:
- `project.name == "linkedin-discovery-cli"`
- `project.scripts["linkedin"]` still points to `linkedin_cli.cli:main`
- repository URLs still point at the existing GitHub repository

Add file-system tests that assert:
- `.github/workflows/ci.yml` exists and contains `pytest -q`
- `.github/workflows/release.yml` exists and contains both `python -m build` and GitHub release creation
- `.github/workflows/publish-pypi.yml` exists and contains `id-token: write`
- `packaging/homebrew/linkedin-discovery-cli.rb` exists and contains the formula class name and `pip install`

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_packaging_distribution.py -q`
Expected: FAIL because the package name and workflow/formula files do not exist yet.

**Step 3: Write minimal implementation**

Create the missing workflows and Homebrew scaffold, then update `pyproject.toml` to satisfy the metadata assertions.

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_packaging_distribution.py -q`
Expected: PASS

### Task 2: Add package metadata and release automation

**Files:**
- Modify: `pyproject.toml`
- Create: `.github/workflows/ci.yml`
- Create: `.github/workflows/release.yml`
- Create: `.github/workflows/publish-pypi.yml`
- Modify: `linkedin_cli/__init__.py`

**Step 1: Write the failing test**

Reuse the failing packaging test from Task 1.

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_packaging_distribution.py -q`
Expected: FAIL on metadata/workflow assertions.

**Step 3: Write minimal implementation**

Update package metadata:
- rename the published distribution to `linkedin-discovery-cli`
- add package URLs for repository, issues, and releases
- add optional dev tooling for building packages if needed

Add GitHub Actions:
- `ci.yml`: run tests on push and pull request
- `release.yml`: build distributions and publish a GitHub Release on tag pushes
- `publish-pypi.yml`: publish to PyPI with Trusted Publishing on GitHub release publication

Keep the module name and console entry point stable.

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_packaging_distribution.py -q`
Expected: PASS

### Task 3: Add Homebrew scaffold and operator docs

**Files:**
- Create: `packaging/homebrew/linkedin-discovery-cli.rb`
- Modify: `README.md`
- Modify: `docs/architecture.md`
- Create: `docs/distribution.md`

**Step 1: Write the failing test**

Reuse the failing packaging test from Task 1.

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_packaging_distribution.py -q`
Expected: FAIL on missing formula/docs references before implementation.

**Step 3: Write minimal implementation**

Add:
- a Homebrew formula template that installs from the Python sdist tarball via `virtualenv_install_with_resources`
- README install/publish guidance
- a distribution runbook with tag, release, PyPI, and Homebrew steps

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_packaging_distribution.py -q`
Expected: PASS

### Task 4: Verify the full distribution surface

**Files:**
- Modify: none unless verification exposes gaps

**Step 1: Run focused packaging tests**

Run: `pytest tests/test_packaging_distribution.py -q`
Expected: PASS

**Step 2: Run the full test suite**

Run: `pytest -q`
Expected: PASS

**Step 3: Validate package build**

Run: `python -m build`
Expected: source distribution and wheel created under `dist/`

**Step 4: Inspect CLI install metadata**

Run: `python -m pip install -e . && linkedin --help`
Expected: editable install succeeds and the `linkedin` command still resolves.
