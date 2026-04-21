#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

export PATH="/usr/local/bin:$HOME/.local/bin:$PATH"

docker compose up -d searxng

for _ in $(seq 1 30); do
  if docker compose ps searxng | grep -q "Up"; then
    break
  fi
  sleep 1
done

python - <<'PY'
import subprocess
from pathlib import Path

container_name = "linkedin-cli-searxng-1"
read_cmd = [
    "docker",
    "exec",
    container_name,
    "python",
    "-c",
    "from pathlib import Path; print(Path('/etc/searxng/settings.yml').read_text())",
]
text = subprocess.check_output(read_cmd, text=True)
old = "  formats:\n    - html\n"
new = "  formats:\n    - html\n    - json\n"
if new not in text:
    if old not in text:
        raise SystemExit("expected html-only formats block not found in settings.yml")
    text = text.replace(old, new, 1)
    patch_cmd = [
        "docker",
        "exec",
        "-i",
        container_name,
        "python",
        "-c",
        "from pathlib import Path; import sys; Path('/etc/searxng/settings.yml').write_text(sys.stdin.read())",
    ]
    subprocess.run(patch_cmd, input=text, text=True, check=True)
PY

docker restart linkedin-cli-searxng-1 >/dev/null

for _ in $(seq 1 30); do
  if curl -fsS "http://127.0.0.1:8080/search?q=site%3Alinkedin.com%2Fposts%20ai%20workflow&format=json" >/dev/null; then
    echo "SearXNG JSON API ready at http://127.0.0.1:8080"
    exit 0
  fi
  sleep 1
done

echo "SearXNG started but JSON API did not become ready in time" >&2
exit 1
