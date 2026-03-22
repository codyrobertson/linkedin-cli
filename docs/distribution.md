# Distribution Runbook

This repository publishes as the PyPI distribution `linkedin-discovery-cli` while keeping the import path `linkedin_cli` and the console command `linkedin`.

## Release flow

1. Update the version in `pyproject.toml` and `linkedin_cli/__init__.py`.
2. Run local verification:
   - `pytest -q`
   - `python -m build`
3. Commit the version bump.
4. Create and push a tag like `v0.1.0`.
5. GitHub Actions runs `.github/workflows/release.yml` to:
   - run the test suite
   - build the wheel and sdist
   - create a GitHub Release and upload the `dist/*` assets
6. Publishing the GitHub Release triggers `.github/workflows/publish-pypi.yml`, which:
   - checks out the tagged source
   - rebuilds the distributions
   - publishes to PyPI using Trusted Publishing

## PyPI setup

Before the first publish:

1. Create the `linkedin-discovery-cli` project on PyPI.
2. Configure a Trusted Publisher for this GitHub repository.
3. Point it at the `publish-pypi.yml` workflow and the `release` event.

## Homebrew

The formula template lives at `packaging/homebrew/linkedin-discovery-cli.rb`.

Recommended flow:

1. Wait until the PyPI sdist for the tagged release exists.
2. Copy the formula into a dedicated tap repository such as `homebrew-linkedin-discovery-cli`.
3. Replace the package `url` and `sha256` with the real PyPI sdist values for the release.
4. Regenerate or refresh the Python resource blocks if dependency versions changed.
5. Publish the tap, then install with `brew install <owner>/tap/linkedin-discovery-cli`.

## Notes

- `release.yml` creates GitHub Releases; it does not publish to PyPI directly.
- `publish-pypi.yml` expects the PyPI Trusted Publisher configuration to exist already.
- The Homebrew formula is intentionally a starter template because the release tarball SHA only exists after the package is published.
