from __future__ import annotations

from pathlib import Path

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover - Python < 3.11 in local test env
    import tomli as tomllib


ROOT = Path(__file__).resolve().parents[1]


def read_text(relative_path: str) -> str:
    return (ROOT / relative_path).read_text(encoding="utf-8")


def test_pyproject_uses_publishable_distribution_name() -> None:
    pyproject = tomllib.loads(read_text("pyproject.toml"))

    assert pyproject["project"]["name"] == "linkedin-discovery-cli"
    assert pyproject["project"]["license"] == "MIT"
    assert pyproject["project"]["scripts"]["linkedin"] == "linkedin_cli.cli:main"
    assert pyproject["project"]["urls"]["Repository"] == "https://github.com/codyrobertson/linkedin-cli"
    assert "License :: OSI Approved :: MIT License" not in pyproject["project"]["classifiers"]


def test_ci_workflow_runs_pytest() -> None:
    workflow = read_text(".github/workflows/ci.yml")

    assert "pytest -q" in workflow
    assert "pull_request:" in workflow
    assert "push:" in workflow


def test_release_workflow_builds_and_publishes_github_release() -> None:
    workflow = read_text(".github/workflows/release.yml")

    assert "python -m build" in workflow
    assert "softprops/action-gh-release" in workflow
    assert "refs/tags/v" in workflow


def test_publish_workflow_uses_trusted_publishing() -> None:
    workflow = read_text(".github/workflows/publish-pypi.yml")

    assert "id-token: write" in workflow
    assert "pypa/gh-action-pypi-publish" in workflow
    assert "release" in workflow
    assert "workflow_dispatch:" in workflow


def test_homebrew_formula_template_exists() -> None:
    formula = read_text("packaging/homebrew/linkedin-discovery-cli.rb")

    assert 'class LinkedinDiscoveryCli < Formula' in formula
    assert 'virtualenv_install_with_resources' in formula
    assert 'linkedin-discovery-cli' in formula
