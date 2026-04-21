# Stacked Content Ranking

This stack separates reusable market learning from target-specific reranking.

## Foundation model

The foundation artifact is trained from warehouse-backed scraped posts and stores three heads:

- `public_performance`
- `persona_style`
- `business_intent`

Each head shares the same feature schema and normalization statistics, but owns its own linear weights and metrics. The artifact is registered in SQLite as `content_stacked_ranking`.

## Training flow

1. Build `content_foundation_posts` and companion views in DuckDB.
2. Label rows with:
   - conditional public-performance over baseline
   - persona-style projection
   - business-intent proxy score
3. Train a shared multi-head linear artifact.
4. Persist the artifact plus default rerank weights.

## Target reranking

Target specialization does not retrain the foundation model.

A target profile defines:

- `company`
- `buyer_roles`
- `industries`
- `problem_keywords`
- `preferred_cta`
- `tone_constraints`

The reranker blends four scores:

- `public_performance`
- `persona_style`
- `business_intent`
- `target_similarity`

Default weights live in the artifact metadata and can be overridden per target profile.
