from __future__ import annotations

import subprocess
import sys
from pathlib import Path


def _bootstrap_repo_root() -> Path:
    repo_root = Path(__file__).resolve().parents[1]
    repo_root_str = str(repo_root)
    if repo_root_str not in sys.path:
        sys.path.insert(0, repo_root_str)
    return repo_root


REPO_ROOT = _bootstrap_repo_root()
SERVICE_NAME = "ultrabroad-scrape"


def launch_detached() -> str:
    subprocess.run(
        ["docker", "compose", "rm", "-sf", SERVICE_NAME],
        cwd=str(REPO_ROOT),
        check=False,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    subprocess.run(
        ["docker", "compose", "up", "-d", "--build", SERVICE_NAME],
        cwd=str(REPO_ROOT),
        check=True,
    )
    result = subprocess.run(
        ["docker", "compose", "ps", "-q", SERVICE_NAME],
        cwd=str(REPO_ROOT),
        check=True,
        capture_output=True,
        text=True,
    )
    container_id = result.stdout.strip()
    if not container_id:
        raise RuntimeError("docker compose did not return a container id for ultrabroad-scrape")
    return container_id


def main() -> None:
    container_id = launch_detached()
    print(f"Container: {container_id}")
    print(f"Logs: docker compose logs -f {SERVICE_NAME}")


if __name__ == "__main__":
    main()
