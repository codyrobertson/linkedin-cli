"""Centralized configuration for linkedin-cli.

All hardcoded paths have been replaced with configurable values.
Set LINKEDIN_CLI_HOME to override the default config directory.
"""

from __future__ import annotations

import os
from pathlib import Path

# ---------------------------------------------------------------------------
# Config directory
# ---------------------------------------------------------------------------

def _resolve_config_dir() -> Path:
    """Resolve the configuration directory.

    Priority:
      1. LINKEDIN_CLI_HOME environment variable
      2. ~/.config/linkedin-cli/
    """
    env_home = os.environ.get("LINKEDIN_CLI_HOME")
    if env_home:
        return Path(env_home).expanduser().resolve()
    return Path.home() / ".config" / "linkedin-cli"


CONFIG_DIR: Path = _resolve_config_dir()

# ---------------------------------------------------------------------------
# Env file location
# ---------------------------------------------------------------------------

ENV_FILE: Path = CONFIG_DIR / ".env"

# ---------------------------------------------------------------------------
# Timeouts and user agents
# ---------------------------------------------------------------------------

DEFAULT_TIMEOUT = 30

DEFAULT_USER_AGENT = os.getenv(
    "LINKEDIN_USER_AGENT",
    (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/137.0.0.0 Safari/537.36"
    ),
)

MOBILE_USER_AGENT = (
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) "
    "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1"
)
