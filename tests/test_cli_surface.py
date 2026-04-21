from __future__ import annotations

from types import SimpleNamespace

import pytest

from linkedin_cli.cli import build_parser
from linkedin_cli import cli
from linkedin_cli.session import CliError


def test_parser_accepts_global_output_modes() -> None:
    parser = build_parser()
    args = parser.parse_args(["--table", "action", "list"])

    assert args.output_mode == "table"
    assert args.command == "action"
    assert args.action_command == "list"


def test_parser_accepts_browser_login_options() -> None:
    parser = build_parser()

    browser_args = parser.parse_args(["login", "--browser", "--browser-name", "firefox"])
    assert browser_args.browser is True
    assert browser_args.browser_name == "firefox"

    brave_args = parser.parse_args(["login", "--browser", "--browser-name", "brave", "--timeout", "45"])
    assert brave_args.browser_name == "brave"
    assert brave_args.timeout == 45


def test_parser_accepts_doctor_command() -> None:
    parser = build_parser()
    args = parser.parse_args(["doctor"])

    assert args.command == "doctor"


def test_parser_accepts_richer_action_commands() -> None:
    parser = build_parser()

    reconcile_args = parser.parse_args(["action", "reconcile", "act_123"])
    cancel_args = parser.parse_args(["action", "cancel", "act_123"])
    artifact_args = parser.parse_args(["action", "artifacts", "act_123"])
    health_args = parser.parse_args(["action", "health", "--stale-minutes", "15"])

    assert reconcile_args.action_command == "reconcile"
    assert cancel_args.action_command == "cancel"
    assert artifact_args.action_command == "artifacts"
    assert health_args.action_command == "health"
    assert health_args.stale_minutes == 15


def test_parser_accepts_workflow_commands() -> None:
    parser = build_parser()

    save_args = parser.parse_args(
        ["workflow", "search", "save", "--name", "founders", "--kind", "people", "--query", "fintech founder"]
    )
    template_args = parser.parse_args(
        ["workflow", "template", "save", "--name", "intro", "--kind", "dm", "--body", "Hi {name}"]
    )
    run_args = parser.parse_args(["workflow", "search", "run", "founders", "--ingest-discovery", "--save-contacts"])
    contact_args = parser.parse_args(
        ["workflow", "contact", "upsert", "--profile", "john-doe", "--name", "John Doe", "--stage", "new"]
    )
    sync_args = parser.parse_args(["workflow", "contact", "sync-discovery", "--limit", "25", "--state", "engaged"])
    inbox_args = parser.parse_args(
        ["workflow", "inbox", "upsert", "--conversation", "urn:li:msg_conversation:1", "--state", "follow_up"]
    )

    assert save_args.workflow_command == "search"
    assert save_args.workflow_search_command == "save"
    assert run_args.workflow_search_command == "run"
    assert run_args.ingest_discovery is True
    assert run_args.save_contacts is True
    assert template_args.workflow_command == "template"
    assert template_args.workflow_template_command == "save"
    assert contact_args.workflow_command == "contact"
    assert contact_args.workflow_contact_command == "upsert"
    assert sync_args.workflow_contact_command == "sync-discovery"
    assert inbox_args.workflow_command == "inbox"
    assert inbox_args.workflow_inbox_command == "upsert"


def test_parser_accepts_content_commands() -> None:
    parser = build_parser()

    harvest_args = parser.parse_args(
        [
            "content", "harvest",
            "--industry", "ai",
            "--industry", "fintech",
            "--topic", "agents",
            "--topic", "workflow",
            "--limit", "100",
            "--per-query", "25",
            "--backend", "auth-only",
            "--public-search", "searxng",
            "--searxng-url", "http://127.0.0.1:8080",
            "--searxng-engine", "google",
            "--expansion", "broad",
            "--query-workers", "4",
            "--job-name", "scale-smoke",
            "--retry-budget", "3",
        ]
    )
    list_args = parser.parse_args(["content", "list", "--limit", "20"])
    stats_args = parser.parse_args(["content", "stats"])
    patterns_args = parser.parse_args(["content", "patterns", "--limit", "8", "--owned-only"])
    embed_args = parser.parse_args(["content", "embed", "--limit", "50"])
    train_args = parser.parse_args(["content", "train-model", "--scope", "auto", "--min-samples", "5", "--industry", "ai", "--topic", "workflow"])
    model_args = parser.parse_args(["content", "model"])
    score_args = parser.parse_args(["content", "score-draft", "--text", "hello world", "--industry", "ai", "--topic", "workflow"])
    rewrite_args = parser.parse_args(["content", "rewrite", "--text", "hello world", "--industry", "ai", "--topic", "workflow", "--goal", "contrarian"])
    playbook_args = parser.parse_args(["content", "playbook", "--industry", "ai", "--topic", "workflow", "--limit", "6"])
    maximize_args = parser.parse_args(["content", "maximize", "--text", "hello world", "--industry", "ai", "--topic", "workflow"])
    polish_args = parser.parse_args(
        [
            "content",
            "polish-and-score",
            "--text",
            "hello world",
            "--industry",
            "ai",
            "--topic",
            "workflow",
            "--target-file",
            "target.json",
            "--goal",
            "engagement",
            "--goal",
            "authority",
            "--limit",
            "3",
            "--fresh",
            "--long-form",
        ]
    )
    create_args = parser.parse_args(
        [
            "content", "create",
            "--prompt", "AI workflow orchestration is still too brittle",
            "--industry", "ai",
            "--topic", "workflow",
            "--goal", "engagement",
            "--goal", "authority",
            "--count", "6",
            "--generator", "cerebras",
            "--speed", "max",
            "--audience", "engineering leaders",
            "--objective", "drive demos",
            "--tone", "operator",
            "--format", "story",
            "--length", "long",
            "--cta", "Invite replies with the word pattern",
        ]
    )
    choose_args = parser.parse_args(
        [
            "content", "choose",
            "--prompt", "AI workflow orchestration is still too brittle",
            "--industry", "ai",
            "--topic", "workflow",
            "--goal", "engagement",
            "--goal", "authority",
            "--count", "6",
            "--generator", "cerebras",
            "--speed", "max",
            "--audience", "ops leaders",
            "--objective", "book calls",
            "--tone", "direct",
            "--format", "operator",
            "--length", "medium",
            "--cta", "Ask readers to comment audit",
        ]
    )
    choose_polish_args = parser.parse_args(
        ["content", "choose", "--prompt", "AI workflow orchestration is still too brittle", "--industry", "ai", "--topic", "workflow", "--polish", "--target-file", "target.json", "--stacked-model-name", "foundation-v7"]
    )
    queue_args = parser.parse_args(
        ["content", "queue", "--prompt", "AI workflow orchestration is still too brittle", "--industry", "ai", "--topic", "workflow", "--count", "6", "--generator", "cerebras"]
    )
    show_candidate_args = parser.parse_args(["content", "show-candidate", "--candidate-id", "cand_123"])
    mark_published_args = parser.parse_args(
        ["content", "mark-published", "--candidate-id", "cand_123", "--post-url", "https://www.linkedin.com/posts/example"]
    )
    trace_list_args = parser.parse_args(["content", "trace-list", "--limit", "5", "--trace-type", "content_autonomy"])
    trace_show_args = parser.parse_args(["content", "trace-show", "--trace-id", "tr_123"])
    trace_export_args = parser.parse_args(["content", "trace-export", "--trace-id", "tr_123", "--output", ".artifacts/traces/tr_123.json"])
    tui_args = parser.parse_args(["content", "tui", "--refresh", "2.5", "--limit", "12", "--trace-type", "content_autonomy"])
    replay_args = parser.parse_args(["content", "replay", "--trace-id", "tr_123", "--policy-name", "content-default"])
    autonomy_args = parser.parse_args(
        [
            "content", "autonomy-run",
            "--prompt", "AI workflow orchestration is still too brittle",
            "--industry", "ai",
            "--topic", "workflow",
            "--count", "6",
            "--mode", "limited",
            "--post-url", "https://www.linkedin.com/posts/example",
            "--generator", "cerebras",
            "--decision-provider", "cerebras",
            "--speed", "max",
            "--audience", "platform teams",
            "--objective", "attract inbound leads",
            "--tone", "authoritative",
            "--format", "story",
            "--length", "long",
            "--cta", "Ask readers to DM workflow",
        ]
    )
    autonomy_polish_args = parser.parse_args(
        ["content", "autonomy-run", "--prompt", "AI workflow orchestration is still too brittle", "--industry", "ai", "--topic", "workflow", "--polish-selected", "--target-file", "target.json", "--stacked-model-name", "foundation-v7"]
    )
    train_policy_args = parser.parse_args(
        ["content", "train-policy", "--policy-name", "content-default", "--context-type", "content_publish", "--min-samples", "25", "--alpha", "0.15", "--ridge", "0.01"]
    )
    policy_report_args = parser.parse_args(
        ["content", "policy-report", "--policy-name", "content-default", "--context-type", "content_publish"]
    )
    sync_args = parser.parse_args(["content", "sync-outcomes", "--url", "https://www.linkedin.com/posts/example", "--owned"])
    retrieve_args = parser.parse_args(["content", "retrieve", "--text", "agent workflow automation", "--limit", "5"])
    similar_args = parser.parse_args(["content", "similar", "--url", "https://www.linkedin.com/posts/example", "--limit", "5"])
    jobs_args = parser.parse_args(["content", "harvest-jobs", "--limit", "10"])
    query_stats_args = parser.parse_args(["content", "query-stats", "--job-prefix", "real-corpus", "--limit", "15"])
    export_args = parser.parse_args(["content", "export-index", "--kind", "semantic", "--output", ".artifacts/index"])
    materialize_args = parser.parse_args(["content", "materialize", "--job-id", "warehouse-job"])
    warehouse_stats_args = parser.parse_args(["content", "warehouse-stats", "--industry", "ai"])
    dataset_args = parser.parse_args(["content", "build-dataset", "--output", ".artifacts/dataset", "--industry", "ai"])
    warehouse_train_args = parser.parse_args(
        ["content", "train-warehouse-model", "--name", "warehouse-viral", "--industry", "ai", "--min-samples", "1000"]
    )
    warehouse_model_args = parser.parse_args(["content", "warehouse-model", "--name", "warehouse-viral"])
    foundation_view_args = parser.parse_args(["content", "build-foundation-views", "--industry", "ai"])
    stacked_train_args = parser.parse_args(
        ["content", "train-stacked-model", "--name", "foundation-v1", "--industry", "ai", "--min-samples", "100"]
    )
    rerank_target_args = parser.parse_args(
        ["content", "rerank-target", "--model-name", "foundation-v1", "--target-file", "target.json", "--limit", "7"]
    )
    rerank_target_auto_args = parser.parse_args(
        ["content", "rerank-target", "--target-file", "target.json", "--limit", "7"]
    )
    select_stacked_args = parser.parse_args(["content", "select-stacked-model"])
    audit_targets_args = parser.parse_args(
        ["content", "audit-targets", "--target-file", "a.json", "--target-file", "b.json", "--limit", "5", "--sample-size", "500"]
    )
    generate_bench_args = parser.parse_args(
        ["content", "generate-benchmark-corpus", "--job-id", "bench-500k", "--rows", "500000", "--industry", "ai"]
    )
    benchmark_args = parser.parse_args(
        ["content", "benchmark-warehouse", "--job-id", "bench-500k", "--dataset-output", ".artifacts/bench-dataset"]
    )
    benchmark_report_args = parser.parse_args(["content", "benchmark-report", "--limit", "5"])
    reward_dataset_args = parser.parse_args(
        ["content", "build-reward-dataset", "--output", ".artifacts/rewards", "--industry", "ai", "--owned-only"]
    )
    policy_dataset_args = parser.parse_args(
        ["content", "build-policy-dataset", "--output", ".artifacts/policy", "--policy-name", "content-default", "--context-type", "content_publish"]
    )
    sft_dataset_args = parser.parse_args(
        ["content", "build-sft-dataset", "--output", ".artifacts/qwen-sft", "--industry", "ai", "--topic", "workflow"]
    )
    preference_dataset_args = parser.parse_args(
        ["content", "build-preference-dataset", "--output", ".artifacts/qwen-pref", "--industry", "ai", "--topic", "workflow"]
    )
    eval_dataset_args = parser.parse_args(["content", "eval-dataset", "--dataset-dir", ".artifacts/qwen-sft"])
    eval_qwen_args = parser.parse_args(
        ["content", "eval-qwen", "--prompt", "AI workflow orchestration is still too brittle", "--industry", "ai", "--topic", "workflow", "--count", "6", "--generator", "heuristic"]
    )
    eval_policy_args = parser.parse_args(
        ["content", "eval-policy", "--policy-name", "content-default", "--context-type", "content_publish"]
    )
    eval_runtime_args = parser.parse_args(
        ["content", "eval-runtime", "--request-file", ".artifacts/runtime-request.json", "--response-file", ".artifacts/runtime-response.json"]
    )
    curate_args = parser.parse_args(["content", "curate-corpus", "--industry", "ai", "--min-quality", "0.5", "--near-duplicate-hamming", "3"])
    curation_stats_args = parser.parse_args(["content", "curation-stats"])
    holdouts_args = parser.parse_args(
        ["content", "build-holdouts", "--output", ".artifacts/holdouts", "--industry", "ai", "--topic", "workflow", "--limit", "1000", "--quota-per-industry", "100", "--quota-per-topic", "50", "--quota-per-format", "25"]
    )
    curated_sft_args = parser.parse_args(
        ["content", "build-curated-sft", "--output", ".artifacts/curated-sft", "--industry", "ai", "--limit", "1000", "--quota-per-industry", "100"]
    )
    curated_pref_args = parser.parse_args(
        ["content", "build-curated-preference", "--output", ".artifacts/curated-pref", "--industry", "ai", "--limit", "1000", "--quota-per-format", "25"]
    )
    train_qwen_args = parser.parse_args(
        ["content", "train-qwen", "--phase", "sft", "--dataset-dir", ".artifacts/qwen-sft", "--base-model", "Qwen/Qwen2.5-3B-Instruct", "--output-name", "ai-workflow-sft", "--runner", "modal", "--wandb-project", "linkedin-autonomy", "--wandb-entity", "cody", "--dry-run"]
    )
    qwen_runs_args = parser.parse_args(["content", "qwen-runs", "--limit", "5"])
    provider_set_args = parser.parse_args(
        ["content", "provider-set", "--provider", "cerebras", "--model", "gpt-oss-120b", "--api-key", "secret-token"]
    )
    provider_show_args = parser.parse_args(["content", "provider-show", "--provider", "cerebras"])
    campaign_args = parser.parse_args(
        [
            "content", "harvest-campaign",
            "--industry", "ai",
            "--industry", "fintech",
            "--topic", "agents",
            "--topic", "workflow",
            "--limit", "10000",
            "--per-job-limit", "1000",
            "--queries-per-job", "24",
            "--speed", "max",
            "--backend", "auth-only",
            "--public-search", "searxng",
            "--searxng-url", "http://127.0.0.1:8080",
            "--searxng-engine", "google",
            "--expansion", "exhaustive",
            "--freshness-bucket", "recent",
            "--freshness-bucket", "quarter",
            "--job-prefix", "real-corpus",
            "--resume",
            "--prune-min-yield", "0.02",
            "--prune-min-attempts", "2",
            "--stop-min-yield-rate", "0.01",
            "--stop-window", "3",
            "--materialize",
            "--embed",
            "--retrain-every", "2",
        ]
    )
    publish_args = parser.parse_args(["post", "publish", "--text", "Hello", "--score"])

    assert harvest_args.content_command == "harvest"
    assert harvest_args.industry == ["ai", "fintech"]
    assert harvest_args.topic == ["agents", "workflow"]
    assert harvest_args.per_query == 25
    assert harvest_args.backend == "auth-only"
    assert harvest_args.public_search == "searxng"
    assert harvest_args.searxng_url == "http://127.0.0.1:8080"
    assert harvest_args.searxng_engine == ["google"]
    assert harvest_args.expansion == "broad"
    assert harvest_args.query_workers == 4
    assert harvest_args.job_name == "scale-smoke"
    assert harvest_args.retry_budget == 3
    assert list_args.content_command == "list"
    assert stats_args.content_command == "stats"
    assert patterns_args.content_command == "patterns"
    assert patterns_args.owned_only is True
    assert embed_args.content_command == "embed"
    assert embed_args.model == "fastembed:BAAI/bge-small-en-v1.5"
    assert train_args.content_command == "train-model"
    assert train_args.scope == "auto"
    assert train_args.min_samples == 5
    assert train_args.industry == "ai"
    assert train_args.topic == ["workflow"]
    assert model_args.content_command == "model"
    assert score_args.content_command == "score-draft"
    assert score_args.topic == ["workflow"]
    assert rewrite_args.goal == "contrarian"
    assert rewrite_args.topic == ["workflow"]
    assert playbook_args.content_command == "playbook"
    assert playbook_args.limit == 6
    assert playbook_args.topic == ["workflow"]
    assert maximize_args.content_command == "maximize"
    assert maximize_args.topic == ["workflow"]
    assert polish_args.content_command == "polish-and-score"
    assert polish_args.target_file == "target.json"
    assert polish_args.goal == ["engagement", "authority"]
    assert polish_args.limit == 3
    assert polish_args.fresh is True
    assert polish_args.long_form is True
    assert polish_args.auto_calibrate_weights is True
    assert create_args.content_command == "create"
    assert create_args.goal == ["engagement", "authority"]
    assert create_args.count == 6
    assert create_args.generator == "cerebras"
    assert create_args.speed == "max"
    assert create_args.audience == "engineering leaders"
    assert create_args.objective == "drive demos"
    assert create_args.tone == "operator"
    assert create_args.format == "story"
    assert create_args.length == "long"
    assert create_args.cta == "Invite replies with the word pattern"
    assert choose_args.content_command == "choose"
    assert choose_args.topic == ["workflow"]
    assert choose_args.count == 6
    assert choose_args.generator == "cerebras"
    assert choose_args.speed == "max"
    assert choose_args.audience == "ops leaders"
    assert choose_args.objective == "book calls"
    assert choose_args.tone == "direct"
    assert choose_args.format == "operator"
    assert choose_args.length == "medium"
    assert choose_args.cta == "Ask readers to comment audit"
    assert choose_polish_args.polish is True
    assert choose_polish_args.target_file == "target.json"
    assert choose_polish_args.stacked_model_name == "foundation-v7"
    assert queue_args.content_command == "queue"
    assert queue_args.prompt == "AI workflow orchestration is still too brittle"
    assert queue_args.generator == "cerebras"
    assert show_candidate_args.content_command == "show-candidate"
    assert show_candidate_args.candidate_id == "cand_123"
    assert mark_published_args.content_command == "mark-published"
    assert mark_published_args.post_url == "https://www.linkedin.com/posts/example"
    assert trace_list_args.content_command == "trace-list"
    assert trace_list_args.trace_type == "content_autonomy"
    assert trace_show_args.trace_id == "tr_123"
    assert trace_export_args.output == ".artifacts/traces/tr_123.json"
    assert tui_args.content_command == "tui"
    assert tui_args.refresh == 2.5
    assert tui_args.limit == 12
    assert tui_args.trace_type == "content_autonomy"
    assert replay_args.content_command == "replay"
    assert replay_args.policy_name == "content-default"
    assert autonomy_args.content_command == "autonomy-run"
    assert autonomy_args.mode == "limited"
    assert autonomy_args.post_url == "https://www.linkedin.com/posts/example"
    assert autonomy_args.generator == "cerebras"
    assert autonomy_args.decision_provider == "cerebras"
    assert autonomy_args.speed == "max"
    assert autonomy_args.audience == "platform teams"
    assert autonomy_args.objective == "attract inbound leads"
    assert autonomy_args.tone == "authoritative"
    assert autonomy_args.format == "story"
    assert autonomy_args.length == "long"
    assert autonomy_args.cta == "Ask readers to DM workflow"
    assert autonomy_polish_args.polish_selected is True
    assert autonomy_polish_args.target_file == "target.json"
    assert autonomy_polish_args.stacked_model_name == "foundation-v7"
    assert train_policy_args.content_command == "train-policy"
    assert train_policy_args.policy_name == "content-default"
    assert train_policy_args.context_type == "content_publish"
    assert train_policy_args.min_samples == 25
    assert policy_report_args.content_command == "policy-report"
    assert policy_report_args.policy_name == "content-default"
    assert sync_args.owned is True
    assert retrieve_args.content_command == "retrieve"
    assert retrieve_args.method == "hybrid"
    assert similar_args.content_command == "similar"
    assert jobs_args.content_command == "harvest-jobs"
    assert query_stats_args.content_command == "query-stats"
    assert query_stats_args.job_prefix == "real-corpus"
    assert export_args.content_command == "export-index"
    assert export_args.kind == "semantic"
    assert materialize_args.content_command == "materialize"
    assert materialize_args.job_id == "warehouse-job"
    assert warehouse_stats_args.content_command == "warehouse-stats"
    assert warehouse_stats_args.industry == "ai"
    assert dataset_args.content_command == "build-dataset"
    assert dataset_args.industry == ["ai"]
    assert warehouse_train_args.content_command == "train-warehouse-model"
    assert warehouse_train_args.industry == ["ai"]
    assert warehouse_model_args.content_command == "warehouse-model"
    assert foundation_view_args.content_command == "build-foundation-views"
    assert foundation_view_args.industry == ["ai"]
    assert stacked_train_args.content_command == "train-stacked-model"
    assert stacked_train_args.name == "foundation-v1"
    assert stacked_train_args.industry == ["ai"]
    assert rerank_target_args.content_command == "rerank-target"
    assert rerank_target_auto_args.model_name is None
    assert rerank_target_auto_args.auto_calibrate_weights is True
    assert select_stacked_args.content_command == "select-stacked-model"
    assert audit_targets_args.content_command == "audit-targets"
    assert audit_targets_args.target_file == ["a.json", "b.json"]
    assert audit_targets_args.sample_size == 500
    assert rerank_target_args.model_name == "foundation-v1"
    assert rerank_target_args.target_file == "target.json"
    assert rerank_target_args.limit == 7
    assert generate_bench_args.content_command == "generate-benchmark-corpus"
    assert generate_bench_args.rows == 500000
    assert benchmark_args.content_command == "benchmark-warehouse"
    assert benchmark_report_args.content_command == "benchmark-report"
    assert reward_dataset_args.content_command == "build-reward-dataset"
    assert reward_dataset_args.owned_only is True
    assert reward_dataset_args.industry == ["ai"]
    assert policy_dataset_args.content_command == "build-policy-dataset"
    assert policy_dataset_args.policy_name == "content-default"
    assert sft_dataset_args.content_command == "build-sft-dataset"
    assert sft_dataset_args.topic == ["workflow"]
    assert preference_dataset_args.content_command == "build-preference-dataset"
    assert eval_dataset_args.content_command == "eval-dataset"
    assert eval_qwen_args.content_command == "eval-qwen"
    assert eval_qwen_args.generator == "heuristic"
    assert eval_policy_args.content_command == "eval-policy"
    assert eval_runtime_args.content_command == "eval-runtime"
    assert eval_runtime_args.request_file == ".artifacts/runtime-request.json"
    assert curate_args.content_command == "curate-corpus"
    assert curate_args.min_quality == 0.5
    assert curate_args.near_duplicate_hamming == 3
    assert curation_stats_args.content_command == "curation-stats"
    assert holdouts_args.content_command == "build-holdouts"
    assert holdouts_args.quota_per_format == 25
    assert curated_sft_args.content_command == "build-curated-sft"
    assert curated_sft_args.quota_per_industry == 100
    assert curated_pref_args.content_command == "build-curated-preference"
    assert curated_pref_args.quota_per_format == 25
    assert train_qwen_args.content_command == "train-qwen"
    assert train_qwen_args.phase == "sft"
    assert train_qwen_args.base_model == "Qwen/Qwen2.5-3B-Instruct"
    assert train_qwen_args.runner == "modal"


def test_content_generation_defaults_to_auto_generator() -> None:
    parser = build_parser()

    create_args = parser.parse_args(["content", "create", "--prompt", "hello"])
    choose_args = parser.parse_args(["content", "choose", "--prompt", "hello"])
    queue_args = parser.parse_args(["content", "queue", "--prompt", "hello"])
    autonomy_args = parser.parse_args(["content", "autonomy-run", "--prompt", "hello"])

    assert create_args.generator == "auto"
    assert choose_args.generator == "auto"
    assert queue_args.generator == "auto"
    assert autonomy_args.generator == "auto"


def test_parser_accepts_discover_profile_views_command() -> None:
    parser = build_parser()
    args = parser.parse_args(["discover", "ingest-profile-views", "--html-fallback"])

    assert args.command == "discover"
    assert args.discover_command == "ingest-profile-views"
    assert args.html_fallback is True


def test_parser_accepts_telemetry_commands() -> None:
    parser = build_parser()
    sync_args = parser.parse_args(["telemetry", "sync", "--owned-posts"])
    stats_args = parser.parse_args(["telemetry", "stats"])

    assert sync_args.command == "telemetry"
    assert sync_args.telemetry_command == "sync"
    assert sync_args.owned_posts is True
    assert stats_args.telemetry_command == "stats"


def test_parser_accepts_lead_and_index_commands() -> None:
    parser = build_parser()
    lead_run = parser.parse_args(
        ["lead", "autopilot", "run", "--limit", "25", "--min-fit", "0.4", "--min-reply", "0.35", "--sync-contacts", "--post-url", "https://www.linkedin.com/posts/example"]
    )
    lead_rank = parser.parse_args(["lead", "rank", "--limit", "10"])
    lead_show = parser.parse_args(["lead", "show", "--profile", "john-doe"])
    rebuild = parser.parse_args(["content", "rebuild-index", "--kind", "semantic", "--model", "local-hash-v1"])
    comment_queue = parser.parse_args(["comment", "queue", "--post-url", "https://www.linkedin.com/posts/example"])
    comment_draft = parser.parse_args(["comment", "draft", "--post-url", "https://www.linkedin.com/posts/example", "--profile", "john-doe", "--tone", "expert"])
    comment_execute = parser.parse_args(["comment", "execute", "--post-url", "https://www.linkedin.com/posts/example", "--comment-id", "cmt_123", "--execute"])

    assert lead_run.command == "lead"
    assert lead_run.lead_command == "autopilot"
    assert lead_run.lead_autopilot_command == "run"
    assert lead_run.sync_contacts is True
    assert lead_run.post_url == ["https://www.linkedin.com/posts/example"]
    assert lead_rank.lead_command == "rank"
    assert lead_show.lead_command == "show"
    assert rebuild.content_command == "rebuild-index"
    assert rebuild.kind == "semantic"
    assert comment_queue.command == "comment"
    assert comment_queue.comment_command == "queue"
    assert comment_draft.comment_command == "draft"
    assert comment_draft.tone == "expert"
    assert comment_execute.comment_command == "execute"
    assert comment_execute.comment_id == "cmt_123"
    assert comment_execute.execute is True


def test_cmd_content_harvest_allows_resume_job_without_query_expansion(monkeypatch) -> None:
    from linkedin_cli import content

    captured: dict[str, object] = {}

    monkeypatch.setattr(content, "init_content_db", lambda: None)

    def fake_harvest_posts(**kwargs):
        captured["kwargs"] = kwargs
        return {"stored_count": 0, "job": {"job_id": kwargs["resume_job"], "status": "running"}}

    monkeypatch.setattr(content, "harvest_posts", fake_harvest_posts)
    monkeypatch.setattr(cli, "pretty_print", lambda payload: captured.setdefault("payload", payload))

    cli.cmd_content_harvest(
        SimpleNamespace(
            limit=10,
            per_query=5,
            search_timeout=30,
            fetch_workers=2,
            query_workers=1,
            query=[],
            topic=[],
            industry=[],
            embed=False,
            embed_model="local-hash-v1",
            embed_batch_size=10,
            job_name=None,
            resume_job="resume-smoke",
            retry_budget=2,
            cooldown_seconds=1.5,
            min_request_interval=0.25,
            jitter_seconds=0.35,
        )
    )

    assert captured["kwargs"]["resume_job"] == "resume-smoke"
    assert captured["payload"]["job"]["job_id"] == "resume-smoke"


def test_cmd_content_harvest_campaign_streams_nested_progress(monkeypatch, capsys) -> None:
    from linkedin_cli import content

    monkeypatch.setattr(content, "init_content_db", lambda: None)
    monkeypatch.setattr(content, "build_harvest_queries", lambda **kwargs: ["ai agents"])
    monkeypatch.setattr(content, "prepare_backend_queries", lambda queries, backend: queries)

    def fake_harvest_campaign(**kwargs):
        progress = kwargs["progress"]
        progress({"event": "campaign_started", "job_count": 1, "query_count": 1})
        progress({"event": "campaign_job_started", "job_id": "real-corpus-001", "job_index": 1, "job_count": 1, "query_count": 1})
        progress({"event": "query_started", "query_index": 1, "query_count": 1, "query": "ai agents", "job_id": "real-corpus-001"})
        progress({"event": "query_page", "query": "ai agents", "start": 50, "page_count": 50})
        progress({"event": "post_stored", "stored_count": 25, "limit": 1000, "url": "https://www.linkedin.com/posts/example", "job_id": "real-corpus-001"})
        progress({"event": "campaign_complete", "stored_count": 25, "job_count": 1})
        return {"stored_count": 25, "job_count": 1}

    monkeypatch.setattr(content, "harvest_campaign", fake_harvest_campaign)
    monkeypatch.setattr(cli, "pretty_print", lambda payload: None)

    cli.cmd_content_harvest_campaign(
        SimpleNamespace(
            limit=1000,
            per_query=100,
            per_job_limit=1000,
            queries_per_job=50,
            search_timeout=30,
            fetch_workers=6,
            query_workers=4,
            query=[],
            topic=["agents"],
            industry=["ai"],
            expansion="exhaustive",
            backend="auth-only",
            freshness_bucket=[],
            retry_budget=2,
            cooldown_seconds=1.5,
            min_request_interval=0.25,
            jitter_seconds=0.35,
            job_prefix="real-corpus",
            resume=False,
            prune_min_yield=None,
            prune_min_attempts=2,
            stop_min_yield_rate=None,
            stop_window=3,
            materialize=False,
            embed=False,
            embed_model="local-hash-v1",
            embed_batch_size=25,
            retrain_every=0,
            train_model_name="real-corpus-live",
            train_scope="all",
            train_min_samples=100,
        )
    )

    stderr = capsys.readouterr().err
    assert "campaign started: 1 jobs across 1 queries" in stderr
    assert "query 1/1: ai agents" in stderr
    assert "page advanced for ai agents: start=50 page_count=50" in stderr
    assert "stored 25/1000: https://www.linkedin.com/posts/example" in stderr


def test_cmd_schedule_rejects_unsupported_image(monkeypatch) -> None:
    monkeypatch.setattr(cli, "load_session", lambda required=True: (object(), {}))
    monkeypatch.setattr(cli, "_get_account_id", lambda _session: "1708250765")

    with pytest.raises(CliError) as exc_info:
        cli.cmd_schedule(
            SimpleNamespace(
                text="hello",
                text_file=None,
                image="/tmp/image.jpg",
                at="2026-04-21T09:00:00-07:00",
                visibility="anyone",
            )
        )

    assert "Scheduled image posts are not supported" in exc_info.value.message


def test_cmd_comment_execute_passes_account_id_to_safe_comment_executor(monkeypatch) -> None:
    from linkedin_cli import comment

    captured: dict[str, object] = {}
    monkeypatch.setattr(cli, "load_session", lambda required=True: ("session", {}))
    monkeypatch.setattr(cli, "_get_account_id", lambda _session: "1708250765")
    monkeypatch.setattr(comment, "init_comment_db", lambda: None)
    monkeypatch.setattr(cli, "pretty_print", lambda payload: captured.setdefault("payload", payload))

    def fake_publish_post_comment(**kwargs):
        captured["kwargs"] = kwargs
        return {"status": "dry_run"}

    monkeypatch.setattr(comment, "publish_post_comment", fake_publish_post_comment)

    cli.cmd_comment_execute(
        SimpleNamespace(
            post_url="https://www.linkedin.com/posts/example-activity-1",
            text="reply",
            text_file=None,
            comment_id="cmt_1",
            profile=None,
            execute=True,
        )
    )

    assert captured["kwargs"]["account_id"] == "1708250765"
    assert captured["kwargs"]["execute"] is True
    assert captured["payload"] == {"status": "dry_run"}


def test_cmd_dm_send_conversation_builds_targeted_plan(monkeypatch) -> None:
    from linkedin_cli.write import executor as executor_mod

    captured: dict[str, object] = {}
    monkeypatch.setattr(cli, "load_session", lambda required=True: ("session", {}))
    monkeypatch.setattr(cli, "_get_account_id", lambda _session: "1708250765")
    monkeypatch.setattr(cli, "_get_my_urn", lambda _session: "urn:li:fsd_profile:me")
    monkeypatch.setattr(cli, "pretty_print", lambda payload: captured.setdefault("payload", payload))

    def fake_execute_action(**kwargs):
        captured["execute_kwargs"] = kwargs
        return {"status": "dry_run", "action": {"action_id": kwargs["action_id"]}}

    monkeypatch.setattr(executor_mod, "execute_action", fake_execute_action)

    conversation_urn = "urn:li:msg_conversation:(urn:li:fsd_profile:me,2-demo)"
    cli.cmd_dm_send(
        SimpleNamespace(
            message="Checking in",
            message_file=None,
            conversation=conversation_urn,
            to=None,
            execute=False,
        )
    )

    plan = captured["execute_kwargs"]["plan"]
    assert plan["live_request"]["body"]["conversationUrn"] == conversation_urn
    assert plan["live_request"]["body"]["hostRecipientUrns"] == []
    assert captured["execute_kwargs"]["dry_run"] is True


def test_cmd_post_publish_image_builds_image_action(monkeypatch, tmp_path) -> None:
    from linkedin_cli.write import executor as executor_mod

    image_path = tmp_path / "image.jpg"
    image_path.write_bytes(b"jpeg")
    captured: dict[str, object] = {}
    monkeypatch.setattr(cli, "load_session", lambda required=True: ("session", {}))
    monkeypatch.setattr(cli, "_get_account_id", lambda _session: "1708250765")
    monkeypatch.setattr(cli, "pretty_print", lambda payload: captured.setdefault("payload", payload))

    def fake_execute_action(**kwargs):
        captured["execute_kwargs"] = kwargs
        return {"status": "dry_run", "action": {"action_id": kwargs["action_id"]}}

    monkeypatch.setattr(executor_mod, "execute_action", fake_execute_action)

    cli.cmd_post_publish(
        SimpleNamespace(
            text="image post",
            text_file=None,
            image=str(image_path),
            visibility="anyone",
            execute=False,
            score=False,
            score_model="local-hash-v1",
        )
    )

    plan = captured["execute_kwargs"]["plan"]
    assert plan["action_type"] == "post.image_publish"
    assert plan["desired"]["image_filename"] == "image.jpg"
    assert captured["execute_kwargs"]["dry_run"] is True


def test_cmd_action_health_prints_guard_report(monkeypatch) -> None:
    from linkedin_cli.write import guards
    from linkedin_cli.write import store as store_mod

    captured: dict[str, object] = {}
    monkeypatch.setattr(store_mod, "init_db", lambda: None)
    monkeypatch.setattr(
        guards,
        "action_health_report",
        lambda **kwargs: {"status": "ok", "stale_minutes": kwargs["stale_minutes"]},
    )
    monkeypatch.setattr(cli, "pretty_print", lambda payload: captured.setdefault("payload", payload))

    cli.cmd_action_health(SimpleNamespace(stale_minutes=15))

    assert captured["payload"] == {"status": "ok", "stale_minutes": 15}
