from __future__ import annotations

import contextlib
import importlib.util
import io
import os
import subprocess
import sys
import types
import builtins
from pathlib import Path


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "run_ultrabroad_scrape.py"
LAUNCHER_PATH = Path(__file__).resolve().parents[1] / "scripts" / "launch_ultrabroad_scrape.py"


def _load_script_module():
    spec = importlib.util.spec_from_file_location("run_ultrabroad_scrape_test", SCRIPT_PATH)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _load_launcher_module():
    spec = importlib.util.spec_from_file_location("launch_ultrabroad_scrape_test", LAUNCHER_PATH)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_run_ultrabroad_scrape_imports_without_pythonpath(tmp_path):
    probe = (
        "import importlib.util; "
        f"spec=importlib.util.spec_from_file_location('runner', r'{SCRIPT_PATH}'); "
        "module=importlib.util.module_from_spec(spec); "
        "spec.loader.exec_module(module); "
        "print(callable(module.build_queries))"
    )
    env = os.environ.copy()
    env.pop("PYTHONPATH", None)
    result = subprocess.run(
        [sys.executable, "-c", probe],
        cwd=tmp_path,
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "True"


def test_emit_progress_logs_periodic_query_starts():
    module = _load_script_module()
    stream = io.StringIO()
    with contextlib.redirect_stdout(stream):
        module.emit_progress(
            {
                "event": "query_started",
                "query": "site:linkedin.com/posts ai outbound",
                "query_index": 10,
                "query_count": 100,
                "job_id": "million-corpus-searxng-ultrabroad-003",
            }
        )
    output = stream.getvalue()
    assert "query 10/100" in output
    assert "site:linkedin.com/posts ai outbound" in output


def test_build_queries_uses_recursive_core_planner(monkeypatch):
    module = _load_script_module()
    captured = {}

    def fake_build_harvest_queries(**kwargs):
        captured.update(kwargs)
        return ["site:linkedin.com/posts ai agents"]

    monkeypatch.setattr(module.content, "build_harvest_queries", fake_build_harvest_queries)
    queries = module.build_queries()

    assert queries == ["site:linkedin.com/posts ai agents"]
    assert captured["industries"] == module.INDUSTRIES[: module.WAVE_INDUSTRY_COUNT]
    assert captured["topics"] == module.TOPICS
    assert captured["expansion"] == "recursive"


def test_runner_uses_searxng_url_env(monkeypatch):
    monkeypatch.setenv("SEARXNG_URL", "http://searxng:8080")
    module = _load_script_module()
    assert module.SEARXNG_URL == "http://searxng:8080"


def test_ultrabroad_taxonomy_is_materially_wider():
    module = _load_script_module()
    assert len(module.INDUSTRIES) >= 150
    assert len(module.TOPICS) >= 150


def test_build_queries_is_wave_limited(monkeypatch):
    module = _load_script_module()
    monkeypatch.setattr(module, "INDUSTRIES", ["i1", "i2", "i3", "i4", "i5"])
    monkeypatch.setattr(module, "WAVE_INDUSTRY_COUNT", 2)
    monkeypatch.setattr(module, "WAVE_INDEX", 1)
    monkeypatch.setattr(module, "QUERY_WAVE_SIZE", 3)
    captured = {}

    def fake_build_harvest_queries(**kwargs):
        captured.update(kwargs)
        return ["q1", "q2", "q3", "q4", "q5"]

    monkeypatch.setattr(
        module.content,
        "build_harvest_queries",
        fake_build_harvest_queries,
    )

    assert module.build_queries() == ["q1", "q2", "q3"]
    assert captured["industries"] == ["i3", "i4"]


def test_build_queries_filters_mismatched_pairs():
    module = _load_script_module()
    filtered = module._filter_queries(
        [
            "site:linkedin.com/posts machine learning upsell",
            "site:linkedin.com/posts machine learning mlops",
            "site:linkedin.com/posts sales upsell",
            "site:linkedin.com/posts healthcare claims automation",
        ]
    )

    assert "site:linkedin.com/posts machine learning mlops" in filtered
    assert "site:linkedin.com/posts sales upsell" in filtered
    assert "site:linkedin.com/posts healthcare claims automation" in filtered
    assert "site:linkedin.com/posts machine learning upsell" not in filtered


def test_main_uses_recursive_campaign_prefix(monkeypatch):
    module = _load_script_module()
    monkeypatch.setattr(module, "build_queries", lambda: ["site:linkedin.com/posts ai agents"])
    monkeypatch.setattr(module, "emit_progress", lambda event: None)
    monkeypatch.setattr(builtins, "print", lambda *args, **kwargs: None)
    captured = {}

    def fake_harvest_campaign(**kwargs):
        captured.update(kwargs)
        return {"stored_count": 0}

    monkeypatch.setattr(module, "content", types.SimpleNamespace(harvest_campaign=fake_harvest_campaign))
    module.main()

    assert captured["job_prefix"] == "million-corpus-searxng-ultrabroad-recursive-v4-wave1"


def test_launch_detached_uses_docker_compose_service(monkeypatch):
    module = _load_launcher_module()
    calls = []

    def fake_run(cmd, **kwargs):
        calls.append((cmd, kwargs))
        return types.SimpleNamespace(stdout="container-123\n")

    monkeypatch.setattr(module.subprocess, "run", fake_run)
    container_id = module.launch_detached()

    assert container_id == "container-123"
    assert calls[0][0] == ["docker", "compose", "rm", "-sf", module.SERVICE_NAME]
    assert calls[0][1]["check"] is False
    assert calls[1][0] == ["docker", "compose", "up", "-d", "--build", module.SERVICE_NAME]
    assert calls[1][1]["check"] is True
    assert calls[2][0] == ["docker", "compose", "ps", "-q", module.SERVICE_NAME]
    assert calls[2][1]["capture_output"] is True
    assert calls[2][1]["text"] is True
    for _, kwargs in calls:
        assert kwargs["cwd"] == str(LAUNCHER_PATH.parents[1])
