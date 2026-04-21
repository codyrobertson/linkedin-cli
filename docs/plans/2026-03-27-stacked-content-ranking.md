# Stacked Content Ranking Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build a scalable general-first content ranking stack that learns broad LinkedIn latent structure from scraped posts, then supports target-specific reranking for a company or buyer persona.

**Architecture:** Add a shared representation layer over the DuckDB warehouse, train three interpretable heads (`public_performance`, `persona_style`, `business_intent`), and expose a lightweight target-profile reranker that blends those heads without retraining the full stack. Keep the first version offline, warehouse-backed, and fully testable from local artifacts.

**Tech Stack:** Python, DuckDB, SQLite, existing `content_warehouse`, `corpus_curation`, `modeling`, JSON artifacts, pytest.

---

### Task 1: Define warehouse-backed latent labels and feature views

**Files:**
- Modify: `linkedin_cli/content_warehouse.py`
- Modify: `linkedin_cli/corpus_curation.py`
- Create: `tests/test_content_stacked_ranking.py`

**Step 1: Write the failing test**

```python
def test_build_foundation_views_materializes_head_features(tmp_path):
    summary = content_stack.build_foundation_views(industries=["ai"])
    assert summary["post_count"] > 0
    assert "public_performance" in summary["views"]
    assert "persona_style" in summary["views"]
    assert "business_intent" in summary["views"]
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_content_stacked_ranking.py::test_build_foundation_views_materializes_head_features -v`
Expected: FAIL with `ImportError` or missing `content_stack`.

**Step 3: Write minimal implementation**

- Add warehouse queries that expose a stable row per post with:
  - normalized engagement fields
  - author-level aggregates
  - query/topic metadata
  - curation labels already derivable from `corpus_curation`
- Extend `corpus_curation` labels to include:
  - `cta_type`
  - `author_archetype`
  - `tone`
  - `proof_level`
  - `freshness_bucket`
- In `content_warehouse.py`, create or replace views/tables such as:
  - `content_foundation_posts`
  - `content_head_public_performance`
  - `content_head_persona_style`
  - `content_head_business_intent`

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_content_stacked_ranking.py::test_build_foundation_views_materializes_head_features -v`
Expected: PASS

**Step 5: Commit**

```bash
git add linkedin_cli/content_warehouse.py linkedin_cli/corpus_curation.py tests/test_content_stacked_ranking.py
git commit -m "feat: add stacked ranking foundation warehouse views"
```

### Task 2: Add normalized public-performance proxy labels

**Files:**
- Create: `linkedin_cli/content_stack.py`
- Modify: `tests/test_content_stacked_ranking.py`

**Step 1: Write the failing test**

```python
def test_public_performance_label_uses_conditional_baseline():
    rows = [
        {"reaction_count": 100, "comment_count": 10, "author_post_count": 500, "topic_cluster": "ai"},
        {"reaction_count": 40, "comment_count": 3, "author_post_count": 20, "topic_cluster": "ai"},
    ]
    labels = content_stack.label_public_performance(rows)
    assert labels[0]["expected_engagement"] != labels[0]["raw_engagement"]
    assert all("overperformed_score" in item for item in labels)
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_content_stacked_ranking.py::test_public_performance_label_uses_conditional_baseline -v`
Expected: FAIL with missing function.

**Step 3: Write minimal implementation**

- In `content_stack.py`, add:
  - `label_public_performance(rows)`
  - raw engagement computation using reactions/comments/reposts
  - conditional baseline using simple grouping by topic/author-archetype/freshness bucket
  - `overperformed_score = raw - expected`
  - binary and continuous labels for later modeling
- Keep v1 simple and deterministic; do not add a full hierarchical model yet.

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_content_stacked_ranking.py::test_public_performance_label_uses_conditional_baseline -v`
Expected: PASS

**Step 5: Commit**

```bash
git add linkedin_cli/content_stack.py tests/test_content_stacked_ranking.py
git commit -m "feat: add conditional public performance proxy labels"
```

### Task 3: Add persona-style axes and business-intent labels

**Files:**
- Modify: `linkedin_cli/content_stack.py`
- Modify: `tests/test_content_stacked_ranking.py`

**Step 1: Write the failing tests**

```python
def test_persona_style_projection_exposes_named_axes():
    projection = content_stack.project_persona_style({
        "tone": "assertive",
        "author_archetype": "operator",
        "cta_type": "none",
        "structure": "insight",
        "word_count": 180,
    })
    assert "operator_vs_storyteller" in projection["axes"]
    assert "tactical_vs_visionary" in projection["axes"]


def test_business_intent_label_detects_commercial_posture():
    label = content_stack.label_business_intent({
        "text": "DM me for the teardown. We used this workflow to cut CAC.",
        "cta_type": "engagement",
        "author_archetype": "operator",
        "topics": ["marketing", "workflow"],
    })
    assert label["business_intent_score"] > 0
```

**Step 2: Run tests to verify they fail**

Run: `pytest tests/test_content_stacked_ranking.py -k "persona_style_projection or business_intent_label" -v`
Expected: FAIL with missing functions.

**Step 3: Write minimal implementation**

- Add `project_persona_style(row)` that emits named axes:
  - `operator_vs_storyteller`
  - `tactical_vs_visionary`
  - `proof_heavy_vs_inspirational`
  - `directive_vs_observational`
- Add `label_business_intent(row)` using existing curation signals:
  - CTA vocabulary
  - archetype
  - topics
  - proof level
  - commercial verbs such as `demo`, `pipeline`, `revenue`, `call`, `team`, `customer`
- Emit both component features and an aggregate `business_intent_score`.

**Step 4: Run tests to verify they pass**

Run: `pytest tests/test_content_stacked_ranking.py -k "persona_style_projection or business_intent_label" -v`
Expected: PASS

**Step 5: Commit**

```bash
git add linkedin_cli/content_stack.py tests/test_content_stacked_ranking.py
git commit -m "feat: add persona style and business intent projections"
```

### Task 4: Train the three-head foundation model

**Files:**
- Modify: `linkedin_cli/modeling.py`
- Modify: `linkedin_cli/content_stack.py`
- Modify: `tests/test_content_stacked_ranking.py`

**Step 1: Write the failing test**

```python
def test_train_stacked_model_writes_three_head_artifact(tmp_path):
    summary = content_stack.train_stacked_model(model_name="foundation-v1", artifact_dir=tmp_path)
    assert summary["trained"] is True
    assert set(summary["heads"]) == {"public_performance", "persona_style", "business_intent"}
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_content_stacked_ranking.py::test_train_stacked_model_writes_three_head_artifact -v`
Expected: FAIL with missing training function.

**Step 3: Write minimal implementation**

- Reuse `modeling.py` for simple head training where possible.
- If `modeling.py` is too classification-specific, add a small regression helper there instead of forking logic in `content_stack.py`.
- Write a single artifact payload containing:
  - shared feature schema
  - head metrics
  - coefficients/weights per head
  - creation metadata
- Register the artifact in SQLite model registry with a task type like `content_stacked_ranking`.

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_content_stacked_ranking.py::test_train_stacked_model_writes_three_head_artifact -v`
Expected: PASS

**Step 5: Commit**

```bash
git add linkedin_cli/modeling.py linkedin_cli/content_stack.py tests/test_content_stacked_ranking.py
git commit -m "feat: train stacked content ranking foundation model"
```

### Task 5: Add target-profile specialization and reranking

**Files:**
- Modify: `linkedin_cli/content_stack.py`
- Create: `docs/architecture/stacked-content-ranking.md`
- Modify: `tests/test_content_stacked_ranking.py`

**Step 1: Write the failing test**

```python
def test_rerank_posts_for_target_blends_foundation_heads():
    target = {
        "company": "Acme AI",
        "buyer_roles": ["vp engineering"],
        "industries": ["ai", "devtools"],
        "preferred_cta": ["soft_authority", "commercial"],
    }
    ranked = content_stack.rerank_for_target(posts=[...], target_profile=target, model_name="foundation-v1")
    assert ranked[0]["score_breakdown"]["public_performance"] >= 0
    assert ranked[0]["score_breakdown"]["business_intent"] >= 0
    assert ranked[0]["score_breakdown"]["target_similarity"] >= 0
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_content_stacked_ranking.py::test_rerank_posts_for_target_blends_foundation_heads -v`
Expected: FAIL with missing reranker.

**Step 3: Write minimal implementation**

- Define a target profile schema:
  - `company`
  - `buyer_roles`
  - `industries`
  - `problem_keywords`
  - `preferred_cta`
  - `tone_constraints`
- Implement a target vector builder from profile metadata.
- Add reranking that returns:
  - `public_performance`
  - `persona_style`
  - `business_intent`
  - `target_similarity`
  - final blended score
- Keep weights explicit and configurable in artifact metadata.

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_content_stacked_ranking.py::test_rerank_posts_for_target_blends_foundation_heads -v`
Expected: PASS

**Step 5: Commit**

```bash
git add linkedin_cli/content_stack.py docs/architecture/stacked-content-ranking.md tests/test_content_stacked_ranking.py
git commit -m "feat: add target profile reranking for stacked model"
```

### Task 6: Expose CLI commands and end-to-end verification

**Files:**
- Modify: `linkedin_cli/cli.py`
- Modify: `README.md`
- Modify: `tests/test_cli_surface.py`
- Modify: `tests/test_content_stacked_ranking.py`

**Step 1: Write the failing tests**

```python
def test_cli_supports_train_stacked_model():
    parser = build_parser()
    args = parser.parse_args(["content", "train-stacked-model", "--name", "foundation-v1"])
    assert args.name == "foundation-v1"


def test_cli_supports_rerank_target():
    parser = build_parser()
    args = parser.parse_args(["content", "rerank-target", "--model-name", "foundation-v1", "--target-file", "target.json"])
    assert args.model_name == "foundation-v1"
```

**Step 2: Run tests to verify they fail**

Run: `pytest tests/test_cli_surface.py -k "train_stacked_model or rerank_target" -v`
Expected: FAIL because parser commands do not exist.

**Step 3: Write minimal implementation**

- Add CLI commands:
  - `linkedin content build-foundation-views`
  - `linkedin content train-stacked-model`
  - `linkedin content rerank-target`
- Add README examples for:
  - training the foundation model
  - supplying a target profile JSON
  - inspecting score breakdowns
- Add one end-to-end test using a tiny synthetic warehouse fixture.

**Step 4: Run tests to verify they pass**

Run:
- `pytest tests/test_cli_surface.py -k "train_stacked_model or rerank_target" -v`
- `pytest tests/test_content_stacked_ranking.py -v`

Expected: PASS

**Step 5: Run final verification**

Run: `python -m pytest -q`
Expected: PASS

**Step 6: Commit**

```bash
git add linkedin_cli/cli.py README.md tests/test_cli_surface.py tests/test_content_stacked_ranking.py
git commit -m "feat: expose stacked content ranking CLI"
```

### Notes

- Do not start with neural embeddings or deep representation learning in v1. Reuse the current warehouse features, fingerprints, and curation labels first.
- Keep all outputs interpretable. Every score returned to the user should include a breakdown.
- Use DuckDB for bulk feature assembly and SQLite only for artifact registration.
- Treat target specialization as a reranker, not a retrain, unless later evidence shows that target-specific adapters are necessary.
