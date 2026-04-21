"""Local dataset builders and training harness helpers for Qwen content models."""

from __future__ import annotations

import json
import uuid
from pathlib import Path
from typing import Any

from linkedin_cli import content
from linkedin_cli.runtime_contract import CONTENT_REWARD_SPEC
from linkedin_cli.session import ExitCode, fail
from linkedin_cli.write import store


DEFAULT_BASE_MODEL = "Qwen/Qwen2.5-3B-Instruct"
DEFAULT_SFT_SYSTEM_PROMPT = "You write high-signal LinkedIn posts that are concrete, specific, and non-generic."
DEFAULT_REWARD_SPEC_VERSION = "viral-v1"


def qwen_artifacts_dir() -> Path:
    return store.ARTIFACTS_DIR / "qwen"


def qwen_registry_path() -> Path:
    return qwen_artifacts_dir() / "registry.jsonl"


def qwen_runs_dir() -> Path:
    return qwen_artifacts_dir() / "runs"


def _normalize_topics(topics: list[str] | None) -> list[str]:
    return [value for value in content._normalize_topics(topics) if value]


def _split_rows(rows: list[dict[str, Any]], *, train_ratio: float, val_ratio: float) -> dict[str, list[dict[str, Any]]]:
    row_count = len(rows)
    train_cutoff = max(0, min(row_count, int(row_count * train_ratio)))
    val_cutoff = max(train_cutoff, min(row_count, train_cutoff + int(row_count * val_ratio)))
    return {
        "train": rows[:train_cutoff],
        "val": rows[train_cutoff:val_cutoff],
        "test": rows[val_cutoff:],
    }


def _append_registry_row(payload: dict[str, Any]) -> None:
    path = qwen_registry_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=False, sort_keys=True) + "\n")


def _register_artifact(
    *,
    artifact_type: str,
    phase: str,
    path: str | Path,
    status: str,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    record = {
        "artifact_id": f"qwen_{artifact_type}_{uuid.uuid4().hex[:12]}",
        "artifact_type": artifact_type,
        "phase": phase,
        "created_at": store._now_iso(),
        "status": status,
        "path": str(Path(path)),
        "metadata": metadata or {},
    }
    _append_registry_row(record)
    return record


def _write_jsonl_splits(
    output_dir: str | Path,
    rows: list[dict[str, Any]],
    *,
    train_ratio: float,
    val_ratio: float,
) -> dict[str, Any]:
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    splits = _split_rows(rows, train_ratio=train_ratio, val_ratio=val_ratio)
    for split_name, split_rows in splits.items():
        destination = output_path / f"{split_name}.jsonl"
        with destination.open("w", encoding="utf-8") as handle:
            for payload in split_rows:
                handle.write(json.dumps(payload, ensure_ascii=False, sort_keys=True) + "\n")
    return {
        "output_dir": str(output_path),
        "row_count": len(rows),
        "splits": {name: len(items) for name, items in splits.items()},
    }


def _candidate_matches_slice(candidate: dict[str, Any], *, industry: str | None, topics: list[str] | None) -> bool:
    if industry and (candidate.get("industry") or "") != industry:
        return False
    normalized_topics = _normalize_topics(topics)
    if not normalized_topics:
        return True
    candidate_topics = {value for value in _normalize_topics(candidate.get("topics") or []) if value}
    if candidate_topics & set(normalized_topics):
        return True
    pseudo_post = {
        "text": "\n".join(
            [
                str(candidate.get("prompt") or ""),
                str(candidate.get("text") or ""),
            ]
        ),
        "industries": [candidate.get("industry")] if candidate.get("industry") else [],
        "metadata": candidate.get("metadata") or {},
    }
    return content._post_relevance_score(pseudo_post, industry=industry, topics=normalized_topics) >= 0.35


def _load_candidate_rows(*, industry: str | None, topics: list[str] | None) -> list[dict[str, Any]]:
    content.init_content_db()
    conn = store._connect()
    try:
        rows = conn.execute(
            """
            SELECT *
            FROM content_candidates
            ORDER BY created_at ASC, rank ASC, candidate_id ASC
            """
        ).fetchall()
        candidates = [content._candidate_row_to_dict(row) for row in rows]
    finally:
        conn.close()
    return [item for item in candidates if _candidate_matches_slice(item, industry=industry, topics=topics)]


def _load_slice_posts(*, industry: str | None, topics: list[str] | None, owned_only: bool | None = None) -> list[dict[str, Any]]:
    content.init_content_db()
    conn = store._connect()
    try:
        where = []
        params: list[Any] = []
        if owned_only is True:
            where.append("owned_by_me = 1")
        elif owned_only is False:
            where.append("owned_by_me = 0")
        where_sql = f"WHERE {' AND '.join(where)}" if where else ""
        rows = conn.execute(
            f"""
            SELECT *
            FROM harvested_posts
            {where_sql}
            ORDER BY COALESCE(published_at, updated_at) ASC, url ASC
            """,
            params,
        ).fetchall()
        posts = content._attach_industries(conn, [content._row_to_dict(row) for row in rows])
    finally:
        conn.close()
    return content._filter_posts_by_relevance(posts, industry=industry, topics=topics)


def _candidate_messages(candidate: dict[str, Any]) -> list[dict[str, str]]:
    goal = str(candidate.get("goal") or "engagement")
    industry = str(candidate.get("industry") or "general")
    topics = ", ".join(candidate.get("topics") or []) or "general LinkedIn strategy"
    prompt = str(candidate.get("prompt") or "").strip()
    user_prompt = (
        f"Write a LinkedIn post for the {industry} slice focused on {topics}. "
        f"Optimization goal: {goal}. Seed idea: {prompt}"
    )
    return [
        {"role": "system", "content": DEFAULT_SFT_SYSTEM_PROMPT},
        {"role": "user", "content": user_prompt},
        {"role": "assistant", "content": str(candidate.get("text") or "").strip()},
    ]


def _post_messages(post: dict[str, Any], *, industry: str | None, topics: list[str] | None) -> list[dict[str, str]]:
    industries = ", ".join(post.get("industries") or ([industry] if industry else [])) or "general"
    topic_phrase = ", ".join(_normalize_topics(topics)) or "LinkedIn growth"
    title = str(post.get("title") or post.get("hook") or "high-performing post").strip()
    prompt = f"Write a LinkedIn post for {industries} about {topic_phrase}. Angle: {title}"
    return [
        {"role": "system", "content": DEFAULT_SFT_SYSTEM_PROMPT},
        {"role": "user", "content": prompt},
        {"role": "assistant", "content": str(post.get("text") or "").strip()},
    ]


def _top_harvested_exemplars(
    *,
    industry: str | None,
    topics: list[str] | None,
    max_posts: int,
) -> list[dict[str, Any]]:
    harvested = [
        post
        for post in _load_slice_posts(industry=industry, topics=topics, owned_only=False)
        if float(post.get("outcome_score") or 0.0) > 0
    ]
    harvested.sort(
        key=lambda post: (
            float(post.get("outcome_score") or 0.0),
            int(post.get("comment_count") or 0),
            int(post.get("reaction_count") or 0),
        ),
        reverse=True,
    )
    return harvested[: max(0, int(max_posts))]


def build_sft_dataset(
    *,
    output_dir: str | Path,
    industry: str | None = None,
    topics: list[str] | None = None,
    train_ratio: float = 0.8,
    val_ratio: float = 0.1,
    include_owned_posts: bool = True,
    include_candidates: bool = True,
    include_harvested_posts: bool = True,
    max_harvested_posts: int = 500,
) -> dict[str, Any]:
    normalized_topics = _normalize_topics(topics)
    rows: list[dict[str, Any]] = []
    source_counts: dict[str, int] = {}

    if include_candidates:
        for candidate in _load_candidate_rows(industry=industry, topics=normalized_topics):
            if not candidate.get("chosen"):
                continue
            rows.append(
                {
                    "messages": _candidate_messages(candidate),
                    "metadata": {
                        "source": "content_candidate",
                        "candidate_id": candidate.get("candidate_id"),
                        "industry": candidate.get("industry"),
                        "topics": candidate.get("topics") or [],
                        "goal": candidate.get("goal"),
                        "rank": int(candidate.get("rank") or 0),
                        "chosen": bool(candidate.get("chosen")),
                        "status": candidate.get("status"),
                    },
                }
            )
            source_counts["content_candidate"] = source_counts.get("content_candidate", 0) + 1

    if include_owned_posts:
        for post in _load_slice_posts(industry=industry, topics=normalized_topics, owned_only=True):
            rows.append(
                {
                    "messages": _post_messages(post, industry=industry, topics=normalized_topics),
                    "metadata": {
                        "source": "owned_post",
                        "url": post.get("url"),
                        "industry": industry,
                        "topics": normalized_topics,
                        "reaction_count": int(post.get("reaction_count") or 0),
                        "comment_count": int(post.get("comment_count") or 0),
                        "outcome_score": float(post.get("outcome_score") or 0.0),
                    },
                }
            )
            source_counts["owned_post"] = source_counts.get("owned_post", 0) + 1

    if include_harvested_posts:
        for post in _top_harvested_exemplars(industry=industry, topics=normalized_topics, max_posts=max_harvested_posts):
            rows.append(
                {
                    "messages": _post_messages(post, industry=industry, topics=normalized_topics),
                    "metadata": {
                        "source": "harvested_exemplar",
                        "url": post.get("url"),
                        "industry": industry,
                        "topics": normalized_topics,
                        "reaction_count": int(post.get("reaction_count") or 0),
                        "comment_count": int(post.get("comment_count") or 0),
                        "outcome_score": float(post.get("outcome_score") or 0.0),
                    },
                }
            )
            source_counts["harvested_exemplar"] = source_counts.get("harvested_exemplar", 0) + 1

    if not rows:
        fail("No SFT rows matched the requested slice", code=ExitCode.NOT_FOUND)

    split_summary = _write_jsonl_splits(output_dir, rows, train_ratio=train_ratio, val_ratio=val_ratio)
    artifact = _register_artifact(
        artifact_type="dataset",
        phase="sft",
        path=output_dir,
        status="ready",
        metadata={
            "industry": industry,
            "topics": normalized_topics,
            "row_count": split_summary["row_count"],
            "source_counts": source_counts,
            "reward_spec_version": DEFAULT_REWARD_SPEC_VERSION,
        },
    )
    return {
        **split_summary,
        "industry": industry,
        "topics": normalized_topics,
        "source_counts": source_counts,
        "artifact_id": artifact["artifact_id"],
    }


def build_preference_dataset(
    *,
    output_dir: str | Path,
    industry: str | None = None,
    topics: list[str] | None = None,
    train_ratio: float = 0.8,
    val_ratio: float = 0.1,
) -> dict[str, Any]:
    normalized_topics = _normalize_topics(topics)
    rows: list[dict[str, Any]] = []
    grouped: dict[str, list[dict[str, Any]]] = {}
    for candidate in _load_candidate_rows(industry=industry, topics=normalized_topics):
        prompt_key = f"{candidate.get('industry') or ''}::{candidate.get('prompt') or ''}"
        grouped.setdefault(prompt_key, []).append(candidate)

    for prompt_key, candidates in grouped.items():
        chosen = sorted(
            [item for item in candidates if item.get("chosen")],
            key=lambda item: (int(item.get("rank") or 9999), item.get("candidate_id") or ""),
        )
        rejected = sorted(
            [item for item in candidates if not item.get("chosen")],
            key=lambda item: (int(item.get("rank") or 9999), item.get("candidate_id") or ""),
        )
        if not chosen or not rejected:
            continue
        winner = chosen[0]
        for loser in rejected:
            rows.append(
                {
                    "prompt": str(winner.get("prompt") or ""),
                    "chosen": str(winner.get("text") or "").strip(),
                    "rejected": str(loser.get("text") or "").strip(),
                    "metadata": {
                        "source": "content_candidate_pair",
                        "industry": winner.get("industry"),
                        "topics": winner.get("topics") or [],
                        "chosen_candidate_id": winner.get("candidate_id"),
                        "rejected_candidate_id": loser.get("candidate_id"),
                        "chosen_goal": winner.get("goal"),
                        "rejected_goal": loser.get("goal"),
                        "prompt_key": prompt_key,
                        "reward_spec_version": DEFAULT_REWARD_SPEC_VERSION,
                    },
                }
            )

    if not rows:
        fail("No preference rows matched the requested slice", code=ExitCode.NOT_FOUND)
    split_summary = _write_jsonl_splits(output_dir, rows, train_ratio=train_ratio, val_ratio=val_ratio)
    artifact = _register_artifact(
        artifact_type="dataset",
        phase="preference",
        path=output_dir,
        status="ready",
        metadata={
            "industry": industry,
            "topics": normalized_topics,
            "row_count": split_summary["row_count"],
            "reward_spec_version": DEFAULT_REWARD_SPEC_VERSION,
        },
    )
    return {
        **split_summary,
        "industry": industry,
        "topics": normalized_topics,
        "artifact_id": artifact["artifact_id"],
    }


def _dataset_file(dataset_dir: str | Path, split_name: str) -> Path:
    path = Path(dataset_dir) / f"{split_name}.jsonl"
    if not path.exists():
        fail(f"Missing dataset split: {path}", code=ExitCode.NOT_FOUND)
    return path


def _count_jsonl_rows(path: Path) -> int:
    with path.open("r", encoding="utf-8") as handle:
        return sum(1 for line in handle if line.strip())


def plan_training_run(
    *,
    phase: str,
    dataset_dir: str | Path,
    base_model: str = DEFAULT_BASE_MODEL,
    output_name: str | None = None,
    runner: str = "local",
    wandb_project: str | None = None,
    wandb_entity: str | None = None,
    modal_app_name: str | None = None,
    learning_rate: float = 2e-4,
    epochs: float = 1.0,
    lora_rank: int = 16,
    per_device_batch_size: int = 2,
    gradient_accumulation_steps: int = 8,
    dry_run: bool = False,
) -> dict[str, Any]:
    normalized_phase = str(phase or "").strip().lower()
    if normalized_phase not in {"sft", "preference"}:
        fail("Qwen training phase must be one of: sft, preference", code=ExitCode.VALIDATION)
    normalized_runner = str(runner or "local").strip().lower()
    if normalized_runner not in {"local", "modal"}:
        fail("Qwen runner must be one of: local, modal", code=ExitCode.VALIDATION)
    dataset_path = Path(dataset_dir)
    train_path = _dataset_file(dataset_path, "train")
    val_path = _dataset_file(dataset_path, "val")
    test_path = _dataset_file(dataset_path, "test")
    run_id = f"qwen_{normalized_phase}_{uuid.uuid4().hex[:10]}"
    run_dir = qwen_runs_dir() / (output_name or run_id)
    run_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = run_dir / "manifest.json"
    model_output_dir = run_dir / "model"
    manifest = {
        "run_id": run_id,
        "phase": normalized_phase,
        "created_at": store._now_iso(),
        "runner": normalized_runner,
        "base_model": base_model,
        "dataset": {
            "dataset_dir": str(dataset_path),
            "train_path": str(train_path),
            "val_path": str(val_path),
            "test_path": str(test_path),
            "train_rows": _count_jsonl_rows(train_path),
            "val_rows": _count_jsonl_rows(val_path),
            "test_rows": _count_jsonl_rows(test_path),
        },
        "reward_spec": CONTENT_REWARD_SPEC,
        "tracking": {
            "wandb_project": wandb_project,
            "wandb_entity": wandb_entity,
        },
        "remote": {
            "modal_app_name": modal_app_name or "linkedin-qwen-train",
        },
        "training": {
            "learning_rate": float(learning_rate),
            "epochs": float(epochs),
            "lora_rank": int(lora_rank),
            "per_device_batch_size": int(per_device_batch_size),
            "gradient_accumulation_steps": int(gradient_accumulation_steps),
        },
        "output_dir": str(model_output_dir),
        "dry_run": bool(dry_run),
    }
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    artifact = _register_artifact(
        artifact_type="training_run",
        phase=normalized_phase,
        path=manifest_path,
        status="planned" if dry_run else "ready",
        metadata={
            "run_id": run_id,
            "base_model": base_model,
            "output_dir": str(model_output_dir),
            "dataset_dir": str(dataset_path),
            "runner": normalized_runner,
            "wandb_project": wandb_project,
            "wandb_entity": wandb_entity,
        },
    )
    command = f"python scripts/train_qwen_content.py --manifest {manifest_path}"
    if normalized_runner == "modal":
        command += " --runner modal"
    return {
        "run_id": run_id,
        "phase": normalized_phase,
        "manifest_path": str(manifest_path),
        "output_dir": str(model_output_dir),
        "registry_path": str(qwen_registry_path()),
        "artifact_id": artifact["artifact_id"],
        "command": command,
        "runner": normalized_runner,
        "dry_run": bool(dry_run),
    }


def list_qwen_runs(limit: int = 20) -> list[dict[str, Any]]:
    path = qwen_registry_path()
    if not path.exists():
        return []
    rows = [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    rows = [row for row in rows if row.get("artifact_type") == "training_run"]
    rows.sort(key=lambda row: row.get("created_at") or "", reverse=True)
    return rows[: max(1, int(limit))]


def _load_jsonl(path: str | Path) -> list[dict[str, Any]]:
    return [
        json.loads(line)
        for line in Path(path).read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def _messages_to_text(messages: list[dict[str, str]], tokenizer: Any | None = None) -> str:
    if tokenizer is not None and hasattr(tokenizer, "apply_chat_template"):
        try:
            return str(tokenizer.apply_chat_template(messages, tokenize=False, add_generation_prompt=False))
        except Exception:
            pass
    return "\n".join(f"{item.get('role', 'user')}: {item.get('content', '')}" for item in messages)


def _run_training_via_modal(manifest: dict[str, Any], manifest_path: str | Path, *, dry_run: bool = False) -> dict[str, Any]:
    try:
        import modal
    except Exception as exc:  # pragma: no cover - exercised in live usage, not tests
        fail(
            f"Modal runner requires the `modal` package and a configured account. ({exc})",
            code=ExitCode.VALIDATION,
        )

    image = (
        modal.Image.debian_slim()
        .pip_install(
            "linkedin-discovery-cli[qwen]",
            "wandb>=0.18",
        )
        .add_local_dir(str(Path.cwd()), remote_path="/workspace")
    )
    app = modal.App(manifest.get("remote", {}).get("modal_app_name") or "linkedin-qwen-train", image=image)

    @app.function(timeout=60 * 60 * 8, gpu="A10G")
    def _remote_run(serialized_manifest: str) -> dict[str, Any]:
        from linkedin_cli import qwen_training as _qt

        remote_manifest_path = Path("/tmp/qwen_manifest.json")
        remote_manifest_path.write_text(serialized_manifest, encoding="utf-8")
        return _qt.run_training_manifest(remote_manifest_path, dry_run=False)

    if dry_run:
        return {
            "run_id": manifest["run_id"],
            "phase": manifest["phase"],
            "runner": "modal",
            "status": "planned",
            "manifest_path": str(Path(manifest_path)),
            "output_dir": manifest["output_dir"],
        }
    return _remote_run.remote(json.dumps(manifest, ensure_ascii=False))


def run_training_manifest(manifest_path: str | Path, *, dry_run: bool = False, runner: str | None = None) -> dict[str, Any]:
    manifest = json.loads(Path(manifest_path).read_text(encoding="utf-8"))
    resolved_runner = str(runner or manifest.get("runner") or "local").strip().lower()
    if dry_run or manifest.get("dry_run"):
        return {
            "run_id": manifest["run_id"],
            "phase": manifest["phase"],
            "runner": resolved_runner,
            "status": "planned",
            "manifest_path": str(Path(manifest_path)),
            "output_dir": manifest["output_dir"],
        }
    if resolved_runner == "modal":
        return _run_training_via_modal(manifest, manifest_path, dry_run=False)

    try:
        import torch
        from datasets import Dataset
        from peft import LoraConfig, get_peft_model
        from transformers import AutoModelForCausalLM, AutoTokenizer, DataCollatorForLanguageModeling, Trainer, TrainingArguments
        try:
            import wandb
        except Exception:
            wandb = None
    except Exception as exc:  # pragma: no cover - exercised in live usage, not tests
        fail(
            f"Local Qwen training dependencies are missing. Install `linkedin-discovery-cli[qwen]`. ({exc})",
            code=ExitCode.VALIDATION,
        )

    phase = str(manifest.get("phase") or "")
    train_rows = _load_jsonl(manifest["dataset"]["train_path"])
    val_rows = _load_jsonl(manifest["dataset"]["val_path"])
    base_model = str(manifest.get("base_model") or DEFAULT_BASE_MODEL)
    output_dir = Path(manifest["output_dir"])
    output_dir.mkdir(parents=True, exist_ok=True)

    tokenizer = AutoTokenizer.from_pretrained(base_model, trust_remote_code=True)
    if tokenizer.pad_token is None:
        tokenizer.pad_token = tokenizer.eos_token

    model = AutoModelForCausalLM.from_pretrained(base_model, trust_remote_code=True)
    tracking = manifest.get("tracking") or {}
    if wandb is not None and tracking.get("wandb_project"):
        wandb.init(
            project=tracking.get("wandb_project"),
            entity=tracking.get("wandb_entity"),
            name=manifest["run_id"],
            config=manifest,
        )
    training_cfg = manifest.get("training") or {}
    lora_config = LoraConfig(
        r=int(training_cfg.get("lora_rank") or 16),
        lora_alpha=max(16, int(training_cfg.get("lora_rank") or 16) * 2),
        lora_dropout=0.05,
        bias="none",
        task_type="CAUSAL_LM",
    )
    model = get_peft_model(model, lora_config)

    if phase == "sft":
        train_dataset = Dataset.from_list([{"text": _messages_to_text(item["messages"], tokenizer)} for item in train_rows])
        eval_dataset = Dataset.from_list([{"text": _messages_to_text(item["messages"], tokenizer)} for item in val_rows])

        def tokenize(batch: dict[str, list[str]]) -> dict[str, Any]:
            tokens = tokenizer(batch["text"], truncation=True, padding="max_length", max_length=1024)
            tokens["labels"] = [list(item) for item in tokens["input_ids"]]
            return tokens

        train_dataset = train_dataset.map(tokenize, batched=True, remove_columns=["text"])
        eval_dataset = eval_dataset.map(tokenize, batched=True, remove_columns=["text"])
        trainer = Trainer(
            model=model,
            args=TrainingArguments(
                output_dir=str(output_dir),
                learning_rate=float(training_cfg.get("learning_rate") or 2e-4),
                num_train_epochs=float(training_cfg.get("epochs") or 1.0),
                per_device_train_batch_size=int(training_cfg.get("per_device_batch_size") or 2),
                per_device_eval_batch_size=int(training_cfg.get("per_device_batch_size") or 2),
                gradient_accumulation_steps=int(training_cfg.get("gradient_accumulation_steps") or 8),
                evaluation_strategy="epoch",
                save_strategy="epoch",
                logging_steps=10,
                report_to=[],
                remove_unused_columns=False,
            ),
            train_dataset=train_dataset,
            eval_dataset=eval_dataset,
            tokenizer=tokenizer,
            data_collator=DataCollatorForLanguageModeling(tokenizer=tokenizer, mlm=False),
        )
        train_result = trainer.train()
        trainer.save_model(str(output_dir))
        tokenizer.save_pretrained(str(output_dir))
        metrics = {key: float(value) for key, value in train_result.metrics.items() if isinstance(value, (int, float))}
    else:  # preference
        try:
            from datasets import Dataset
            from trl import DPOConfig, DPOTrainer
        except Exception as exc:  # pragma: no cover - exercised in live usage, not tests
            fail(
                f"Preference training requires `trl` in the qwen extra. ({exc})",
                code=ExitCode.VALIDATION,
            )
        train_dataset = Dataset.from_list(train_rows)
        eval_dataset = Dataset.from_list(val_rows)
        trainer = DPOTrainer(
            model=model,
            ref_model=None,
            args=DPOConfig(
                output_dir=str(output_dir),
                learning_rate=float(training_cfg.get("learning_rate") or 2e-4),
                num_train_epochs=float(training_cfg.get("epochs") or 1.0),
                per_device_train_batch_size=int(training_cfg.get("per_device_batch_size") or 2),
                per_device_eval_batch_size=int(training_cfg.get("per_device_batch_size") or 2),
                gradient_accumulation_steps=int(training_cfg.get("gradient_accumulation_steps") or 8),
                report_to=[],
            ),
            processing_class=tokenizer,
            train_dataset=train_dataset,
            eval_dataset=eval_dataset,
        )
        train_result = trainer.train()
        trainer.save_model(str(output_dir))
        tokenizer.save_pretrained(str(output_dir))
        metrics = {key: float(value) for key, value in train_result.metrics.items() if isinstance(value, (int, float))}

    result_path = output_dir / "result.json"
    result_payload = {
        "run_id": manifest["run_id"],
        "phase": phase,
        "runner": resolved_runner,
        "output_dir": str(output_dir),
        "metrics": metrics,
        "completed_at": store._now_iso(),
    }
    result_path.write_text(json.dumps(result_payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    _register_artifact(
        artifact_type="trained_model",
        phase=phase,
        path=output_dir,
        status="completed",
        metadata={"run_id": manifest["run_id"], "metrics": metrics},
    )
    return result_payload
