# Growth Agent V2 Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build a production-grade growth agent loop for LinkedIn that can publish content, ingest visible engagement, enrich and rank leads, draft outreach and comment replies, learn from outcomes, and ship through a repeatable release pipeline.

**Architecture:** Extend the current CLI into four cooperating subsystems: telemetry collection, retrieval and ranking, policy learning, and controlled execution. Keep the system local-first and auditable: every model decision must be explainable from stored events, retrieved exemplars, and explicit policy scores rather than opaque side effects.

**Tech Stack:** Python 3.11, SQLite, requests, BeautifulSoup, fastembed, `hnswlib` for ANN, scikit-learn for ranking/calibration, local JSON model artifacts, GitHub Actions, PyPI Trusted Publishing.

---

## Scope and non-goals

- Scope:
  - owned-post telemetry sync
  - engagement-to-lead autopilot
  - trained content and lead ranking
  - ANN retrieval for 5k to 50k posts and leads
  - comment drafting and reply execution
  - offline evaluation for ranking and policy changes
  - release automation and publish readiness checks
- Non-goals:
  - pretending hidden/private LinkedIn data is available when the account does not expose it
  - end-to-end fully autonomous send/publish without policy guardrails
  - heavyweight distributed infrastructure before local-first reliability is proven

## Research anchors

- LinUCB bandits for action selection:
  - Li et al., “A Contextual-Bandit Approach to Personalized News Article Recommendation”
  - https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/p661.pdf
- Practical contextual bandits:
  - Foster et al., “Practical Contextual Bandits with Regression Oracles”
  - https://proceedings.mlr.press/v80/foster18a.html
- Logged-bandit / counterfactual evaluation:
  - Swaminathan & Joachims, “Counterfactual Risk Minimization: Learning from Logged Bandit Feedback”
  - https://arxiv.org/abs/1502.02362
  - Joachims & Swaminathan, “Counterfactual Evaluation and Learning for Search, Recommendation and Ad Placement”
  - https://ir.webis.de/anthology/2016.sigirconf_conference-2016.218/
- Two-stage retrieval and ranking:
  - Covington et al., “Deep Neural Networks for YouTube Recommendations”
  - https://research.google/pubs/deep-neural-networks-for-youtube-recommendations/
  - Yi et al., “Sampling-Bias-Corrected Neural Modeling for Large Corpus Item Recommendations”
  - https://research.google/pubs/sampling-bias-corrected-neural-modeling-for-large-corpus-item-recommendations/
- Early virality prediction:
  - Guo et al., “Toward Early and Order-of-Magnitude Cascade Prediction in Social Networks”
  - https://arxiv.org/abs/1608.02646

## Command surface to build

- `linkedin telemetry sync --owned-posts --since 2026-03-01`
- `linkedin telemetry stats`
- `linkedin lead autopilot run --post-url <url> --dry-run`
- `linkedin lead autopilot run --all-owned --min-fit 0.55 --min-reply 0.35`
- `linkedin lead rank --source owned-engagers --limit 50`
- `linkedin lead show --profile <slug>`
- `linkedin comment queue --post-url <url>`
- `linkedin comment draft --post-url <url> --comment-id <id> --tone expert`
- `linkedin comment reply --post-url <url> --comment-id <id> --text "..." --execute`
- `linkedin content train-ranker --target viral_bucket`
- `linkedin content train-ranker --target reply_likelihood`
- `linkedin content train-policy --policy linucb`
- `linkedin content recommend --goal engagement`
- `linkedin retrieve rebuild-index`
- `linkedin release doctor`
- `linkedin release cut --version X.Y.Z`

## Schema changes

### New tables

- `telemetry_events`
  - `id INTEGER PRIMARY KEY`
  - `entity_kind TEXT NOT NULL`
  - `entity_key TEXT NOT NULL`
  - `event_type TEXT NOT NULL`
  - `event_time TEXT NOT NULL`
  - `source TEXT NOT NULL`
  - `payload_json TEXT NOT NULL`
  - `dedupe_key TEXT UNIQUE`
- `owned_post_snapshots`
  - `url TEXT NOT NULL`
  - `captured_at TEXT NOT NULL`
  - `reaction_count INTEGER NOT NULL`
  - `comment_count INTEGER NOT NULL`
  - `repost_count INTEGER NOT NULL DEFAULT 0`
  - `viewer_count INTEGER`
  - `payload_json TEXT NOT NULL`
- `lead_features`
  - `profile_key TEXT PRIMARY KEY`
  - `feature_version TEXT NOT NULL`
  - `features_json TEXT NOT NULL`
  - `updated_at TEXT NOT NULL`
- `lead_labels`
  - `id INTEGER PRIMARY KEY`
  - `profile_key TEXT NOT NULL`
  - `label_type TEXT NOT NULL`
  - `label_value REAL NOT NULL`
  - `label_time TEXT NOT NULL`
  - `metadata_json TEXT NOT NULL`
- `model_registry`
  - `model_name TEXT PRIMARY KEY`
  - `task_type TEXT NOT NULL`
  - `artifact_path TEXT NOT NULL`
  - `metrics_json TEXT NOT NULL`
  - `feature_schema_json TEXT NOT NULL`
  - `created_at TEXT NOT NULL`
  - `updated_at TEXT NOT NULL`
- `policy_decisions`
  - `decision_id TEXT PRIMARY KEY`
  - `policy_name TEXT NOT NULL`
  - `context_json TEXT NOT NULL`
  - `action_json TEXT NOT NULL`
  - `propensity REAL NOT NULL`
  - `reward REAL`
  - `created_at TEXT NOT NULL`
  - `updated_at TEXT NOT NULL`
- `comment_queue`
  - `comment_id TEXT PRIMARY KEY`
  - `post_url TEXT NOT NULL`
  - `author_profile_key TEXT`
  - `body TEXT NOT NULL`
  - `state TEXT NOT NULL`
  - `priority REAL NOT NULL DEFAULT 0`
  - `draft_reply TEXT`
  - `updated_at TEXT NOT NULL`

### Existing table extensions

- `harvested_posts`
  - add `repost_count INTEGER NOT NULL DEFAULT 0`
  - add `impression_proxy REAL`
  - add `viral_bucket TEXT`
  - add `retrieval_doc_type TEXT NOT NULL DEFAULT 'content'`
- `prospects`
  - add `fit_score REAL NOT NULL DEFAULT 0`
  - add `reply_likelihood REAL NOT NULL DEFAULT 0`
  - add `deal_likelihood REAL NOT NULL DEFAULT 0`
  - add `last_enriched_at TEXT`

## Model stack

### 1. Retrieval layer

- Candidate generation:
  - content: dense embedding ANN via `hnswlib`
  - leads: dense profile/company/activity vectors via `hnswlib`
  - lexical fallback: SQLite FTS or BM25-lite token overlap
- Ranker inputs:
  - dense similarity
  - fingerprint similarity
  - lexical overlap
  - engagement history
  - profile/company features

### 2. Supervised rankers

- Start with scikit-learn models:
  - `HistGradientBoostingRegressor` for continuous outcome score
  - `HistGradientBoostingClassifier` or logistic regression for:
    - `P(reply)`
    - `P(connect_accept)`
    - `P(meeting)`
    - `P(viral_bucket)`
- Calibrate probabilities with isotonic regression or Platt scaling.
- Persist artifacts under `packaging/models/` or `.benchmarks/models/`.

### 3. Policy layer

- Start with local LinUCB for:
  - hook template selection
  - CTA selection
  - send timing bucket
  - outreach template choice
- Log every policy decision with propensities.
- Evaluate policy changes offline with IPS and SNIPS before enabling live execution.

### 4. Labels and rewards

- Content rewards:
  - normalized reactions
  - normalized comments
  - reposts
  - impression proxy
  - viral-bucket label
- Lead rewards:
  - replied
  - accepted
  - scheduled meeting
  - advanced pipeline stage
  - won

## Live-safe rollout order

1. Telemetry and event logging only
2. Draft-only and dry-run ranking
3. Lead autopilot in recommendation-only mode
4. Human-approved DM/comment drafts
5. Limited execution with low daily caps
6. Policy learning from logged outcomes
7. Wider automation after offline evaluation clears thresholds

## Task 1: Telemetry event log foundation

**Files:**
- Modify: `linkedin_cli/write/store.py`
- Modify: `linkedin_cli/content.py`
- Modify: `linkedin_cli/discovery.py`
- Create: `tests/test_telemetry_store.py`

**Step 1: Write the failing test**

```python
def test_append_telemetry_event_dedupes_by_key():
    first = telemetry.append_event("post", "url-1", "reaction_snapshot", "dedupe-1", {"count": 5})
    second = telemetry.append_event("post", "url-1", "reaction_snapshot", "dedupe-1", {"count": 5})
    assert first["id"] == second["id"]
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_telemetry_store.py::test_append_telemetry_event_dedupes_by_key -v`
Expected: FAIL because telemetry helpers do not exist.

**Step 3: Write minimal implementation**

- Add telemetry schema creation to `linkedin_cli/write/store.py`.
- Add helper functions:
  - `append_telemetry_event(...)`
  - `list_telemetry_events(...)`
  - `telemetry_stats(...)`

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_telemetry_store.py -v`
Expected: PASS

**Step 5: Commit**

```bash
git add linkedin_cli/write/store.py tests/test_telemetry_store.py
git commit -m "feat: add telemetry event store"
```

## Task 2: Owned-post telemetry sync

**Files:**
- Modify: `linkedin_cli/content.py`
- Modify: `linkedin_cli/cli.py`
- Create: `tests/test_content_telemetry.py`

**Step 1: Write the failing test**

```python
def test_sync_owned_post_telemetry_records_snapshot_and_events():
    summary = content.sync_owned_post_telemetry(urls=["https://www.linkedin.com/posts/example"], fetch_html=fake_html)
    assert summary["synced_count"] == 1
    assert summary["events_written"] >= 1
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_content_telemetry.py::test_sync_owned_post_telemetry_records_snapshot_and_events -v`
Expected: FAIL because sync function does not exist.

**Step 3: Write minimal implementation**

- Extend `extract_post_record(...)` to capture repost count where visible.
- Add `sync_owned_post_telemetry(...)`.
- Add CLI:
  - `telemetry sync --owned-posts`
  - `telemetry stats`

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_content_telemetry.py -v`
Expected: PASS

**Step 5: Commit**

```bash
git add linkedin_cli/content.py linkedin_cli/cli.py tests/test_content_telemetry.py
git commit -m "feat: add owned post telemetry sync"
```

## Task 3: Engagement-to-lead enrichment pipeline

**Files:**
- Modify: `linkedin_cli/discovery.py`
- Modify: `linkedin_cli/workflow.py`
- Create: `linkedin_cli/lead.py`
- Create: `tests/test_lead_pipeline.py`

**Step 1: Write the failing test**

```python
def test_build_lead_features_from_visible_engager():
    lead = lead.build_lead_features(profile_key="john-doe", profile=profile, company=company, signals=signals)
    assert lead["fit_score"] > 0
    assert "ai" in lead["features"]["topic_overlap_terms"]
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_lead_pipeline.py::test_build_lead_features_from_visible_engager -v`
Expected: FAIL because `linkedin_cli.lead` does not exist.

**Step 3: Write minimal implementation**

- Create `linkedin_cli/lead.py` with:
  - `build_lead_features(...)`
  - `upsert_lead_features(...)`
  - `score_lead(...)`
- Use current discovery signals plus profile/company summary fields.
- Sync scores back into `prospects`.

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_lead_pipeline.py -v`
Expected: PASS

**Step 5: Commit**

```bash
git add linkedin_cli/lead.py linkedin_cli/discovery.py linkedin_cli/workflow.py tests/test_lead_pipeline.py
git commit -m "feat: add lead enrichment and scoring"
```

## Task 4: Engagement-to-lead autopilot

**Files:**
- Modify: `linkedin_cli/cli.py`
- Modify: `linkedin_cli/lead.py`
- Create: `tests/test_lead_autopilot.py`

**Step 1: Write the failing test**

```python
def test_autopilot_routes_high_fit_engagers_into_ready_state():
    summary = lead.run_autopilot(post_urls=["https://www.linkedin.com/posts/example"], dry_run=True)
    assert summary["routed"]["ready"] >= 1
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_lead_autopilot.py::test_autopilot_routes_high_fit_engagers_into_ready_state -v`
Expected: FAIL because autopilot does not exist.

**Step 3: Write minimal implementation**

- Add:
  - `lead.run_autopilot(...)`
  - CLI `lead autopilot run`
  - CLI `lead rank`
  - CLI `lead show`
- Route thresholds:
  - high fit + reply -> `ready`
  - medium fit -> `watch`
  - already contacted -> do not downgrade

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_lead_autopilot.py -v`
Expected: PASS

**Step 5: Commit**

```bash
git add linkedin_cli/cli.py linkedin_cli/lead.py tests/test_lead_autopilot.py
git commit -m "feat: add engagement-to-lead autopilot"
```

## Task 5: ANN retrieval for posts and leads

**Files:**
- Modify: `pyproject.toml`
- Modify: `linkedin_cli/content.py`
- Modify: `linkedin_cli/lead.py`
- Create: `linkedin_cli/retrieval_index.py`
- Create: `tests/test_retrieval_index.py`

**Step 1: Write the failing test**

```python
def test_hnsw_index_returns_candidate_neighbors():
    index = retrieval_index.build_index(items)
    results = retrieval_index.query(index, vector, limit=5)
    assert len(results) == 5
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_retrieval_index.py::test_hnsw_index_returns_candidate_neighbors -v`
Expected: FAIL because retrieval index module does not exist.

**Step 3: Write minimal implementation**

- Add dependency: `hnswlib`.
- Create `retrieval_index.py` with:
  - `build_hnsw_index(...)`
  - `query_hnsw_index(...)`
  - `save_index(...)`
  - `load_index(...)`
- Replace shortlist-only banding with ANN-first retrieval plus current reranker.

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_retrieval_index.py -v`
Expected: PASS

**Step 5: Commit**

```bash
git add pyproject.toml linkedin_cli/retrieval_index.py linkedin_cli/content.py linkedin_cli/lead.py tests/test_retrieval_index.py
git commit -m "feat: add hnsw retrieval index"
```

## Task 6: Supervised rankers and calibration

**Files:**
- Modify: `pyproject.toml`
- Create: `linkedin_cli/modeling.py`
- Modify: `linkedin_cli/content.py`
- Modify: `linkedin_cli/lead.py`
- Create: `tests/test_modeling.py`

**Step 1: Write the failing test**

```python
def test_train_reply_model_persists_metrics_and_artifact():
    result = modeling.train_model(task="reply_likelihood", samples=samples)
    assert result["metrics"]["auc"] >= 0
    assert Path(result["artifact_path"]).exists()
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_modeling.py::test_train_reply_model_persists_metrics_and_artifact -v`
Expected: FAIL because modeling module does not exist.

**Step 3: Write minimal implementation**

- Add dependency: `scikit-learn`.
- Create `modeling.py` with:
  - feature assembly
  - train/val/test split
  - fit + metrics
  - calibration
  - artifact persistence
- Wire predictions into:
  - `content score-draft`
  - `lead rank`
  - autopilot routing

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_modeling.py -v`
Expected: PASS

**Step 5: Commit**

```bash
git add pyproject.toml linkedin_cli/modeling.py linkedin_cli/content.py linkedin_cli/lead.py tests/test_modeling.py
git commit -m "feat: add trained rankers and calibration"
```

## Task 7: Contextual policy learning and offline evaluation

**Files:**
- Create: `linkedin_cli/policy.py`
- Modify: `linkedin_cli/content.py`
- Modify: `linkedin_cli/lead.py`
- Modify: `linkedin_cli/cli.py`
- Create: `tests/test_policy.py`

**Step 1: Write the failing test**

```python
def test_linucb_policy_logs_propensity_and_action():
    decision = policy.choose_action(context, actions)
    assert 0 < decision["propensity"] <= 1
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_policy.py::test_linucb_policy_logs_propensity_and_action -v`
Expected: FAIL because policy module does not exist.

**Step 3: Write minimal implementation**

- Add `policy.py` with:
  - `choose_action_linucb(...)`
  - `log_policy_decision(...)`
  - `evaluate_policy_ips(...)`
  - `evaluate_policy_snips(...)`
- Add CLI:
  - `content train-policy`
  - `content recommend`

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_policy.py -v`
Expected: PASS

**Step 5: Commit**

```bash
git add linkedin_cli/policy.py linkedin_cli/content.py linkedin_cli/lead.py linkedin_cli/cli.py tests/test_policy.py
git commit -m "feat: add contextual policy layer"
```

## Task 8: Comment queue and reply execution

**Files:**
- Modify: `linkedin_cli/cli.py`
- Modify: `linkedin_cli/discovery.py`
- Create: `linkedin_cli/comments.py`
- Create: `tests/test_comments.py`

**Step 1: Write the failing test**

```python
def test_comment_queue_builds_reply_candidates_for_owned_post():
    queue = comments.build_comment_queue(post_url="https://www.linkedin.com/posts/example")
    assert queue["count"] >= 1
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_comments.py::test_comment_queue_builds_reply_candidates_for_owned_post -v`
Expected: FAIL because comments module does not exist.

**Step 3: Write minimal implementation**

- Create `comments.py` with:
  - `build_comment_queue(...)`
  - `draft_comment_reply(...)`
  - `mark_comment_state(...)`
- Add CLI:
  - `comment queue`
  - `comment draft`
  - `comment reply`
- If no reliable write endpoint exists for public comment reply, keep `reply` dry-run only and persist artifacts until the endpoint is validated.

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_comments.py -v`
Expected: PASS

**Step 5: Commit**

```bash
git add linkedin_cli/comments.py linkedin_cli/cli.py linkedin_cli/discovery.py tests/test_comments.py
git commit -m "feat: add comment response workflow"
```

## Task 9: Release and publish automation

**Files:**
- Create: `linkedin_cli/release.py`
- Modify: `linkedin_cli/cli.py`
- Modify: `.github/workflows/release.yml`
- Modify: `.github/workflows/publish-pypi.yml`
- Create: `tests/test_release_cli.py`

**Step 1: Write the failing test**

```python
def test_release_doctor_reports_missing_publish_credentials():
    result = release.release_doctor(env={})
    assert result["ready"] is False
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_release_cli.py::test_release_doctor_reports_missing_publish_credentials -v`
Expected: FAIL because release module does not exist.

**Step 3: Write minimal implementation**

- Create `release.py` with:
  - `release_doctor(...)`
  - `cut_release(...)`
  - version consistency checks
  - build/test/tag command generation
- Add CLI:
  - `release doctor`
  - `release cut --version X.Y.Z`

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_release_cli.py -v`
Expected: PASS

**Step 5: Commit**

```bash
git add linkedin_cli/release.py linkedin_cli/cli.py .github/workflows/release.yml .github/workflows/publish-pypi.yml tests/test_release_cli.py
git commit -m "feat: add release automation"
```

## Task 10: Full-loop verification harness

**Files:**
- Create: `tests/test_growth_loop.py`
- Create: `.benchmarks/growth_loop_smoke.py`
- Modify: `README.md`

**Step 1: Write the failing test**

```python
def test_growth_loop_dry_run_connects_content_to_lead_routing():
    summary = smoke.run_growth_loop_dry_run()
    assert summary["drafted_dm_count"] >= 1
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_growth_loop.py::test_growth_loop_dry_run_connects_content_to_lead_routing -v`
Expected: FAIL because smoke harness does not exist.

**Step 3: Write minimal implementation**

- Add a dry-run smoke harness that runs:
  - content scoring
  - owned-post telemetry sync
  - engager ingestion
  - lead ranking
  - DM draft generation
- Document exact operator workflow in `README.md`.

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_growth_loop.py -v`
Expected: PASS

**Step 5: Commit**

```bash
git add tests/test_growth_loop.py .benchmarks/growth_loop_smoke.py README.md
git commit -m "test: add growth loop smoke harness"
```

## Implementation notes

- Hidden/private telemetry:
  - do not invent unavailable entities
  - persist summary counts even when identities are gated
  - mark source reliability in metadata
- Message safety:
  - every automated send path needs daily caps, per-profile cooldowns, and dedupe by recent action
- Model safety:
  - every model artifact must save feature schema and metrics
  - offline evaluation must pass thresholds before a policy/model becomes default
- Retrieval safety:
  - keep current scan-based fallback behind ANN queries so the CLI still works if the index is absent or stale

## Acceptance criteria

- Operator can publish a post, sync telemetry, ingest visible engagers, inspect ranked leads, and draft follow-up DMs from one CLI workflow.
- Content and lead scoring use persisted trained models, not only heuristics.
- Retrieval remains responsive at 5k to 50k corpus size on a laptop.
- Policy decisions are logged with propensities and can be evaluated offline.
- Release doctor clearly reports whether PyPI/GitHub publishing is actually ready.

## Verification commands

```bash
/Users/Cody/mambaforge/bin/python -m pytest -q
/Users/Cody/mambaforge/bin/python -m py_compile linkedin_cli/*.py linkedin_cli/write/*.py
/Users/Cody/mambaforge/bin/python -m linkedin_cli --json telemetry stats
/Users/Cody/mambaforge/bin/python -m linkedin_cli --json content model
/Users/Cody/mambaforge/bin/python -m linkedin_cli --json lead autopilot run --all-owned --dry-run
/Users/Cody/mambaforge/bin/python -m linkedin_cli --json release doctor
```

Plan complete and saved to `docs/plans/2026-03-26-growth-agent-v2.md`. Two execution options:

**1. Subagent-Driven (this session)** - I dispatch fresh subagent per task, review between tasks, fast iteration

**2. Parallel Session (separate)** - Open new session with executing-plans, batch execution with checkpoints

**Which approach?**
