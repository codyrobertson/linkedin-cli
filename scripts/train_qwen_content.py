#!/usr/bin/env python3
"""Local entrypoint for Qwen SFT / preference training."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from linkedin_cli import qwen_training


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run a local Qwen content training job from a manifest.")
    parser.add_argument("--manifest", required=True, help="Path to a generated Qwen training manifest")
    parser.add_argument("--runner", choices=["local", "modal"], help="Override the manifest runner")
    parser.add_argument("--dry-run", action="store_true", help="Only validate and print the manifest")
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    summary = qwen_training.run_training_manifest(Path(args.manifest), dry_run=args.dry_run, runner=args.runner)
    print(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
