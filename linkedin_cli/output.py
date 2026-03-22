"""Output helpers for linkedin-cli."""

from __future__ import annotations

import json
from typing import Any


VERBOSE_SKIP_KEYS = {
    "$type",
    "$recipeTypes",
    "$anti_abuse_metadata",
    "trackingId",
    "versionTag",
    "entityUrn",
    "dashEntityUrn",
    "objectUrn",
    "plan_json",
    "risk_flags",
    "next_attempt_at",
    "desired_fingerprint",
}


def strip_verbose(data: Any) -> Any:
    if isinstance(data, dict):
        output: dict[str, Any] = {}
        for key, value in data.items():
            if key in VERBOSE_SKIP_KEYS:
                continue
            output[key] = strip_verbose(value)
        return output
    if isinstance(data, list):
        return [strip_verbose(item) for item in data]
    return data


def render_json(data: Any, *, brief: bool = False) -> str:
    if brief:
        data = strip_verbose(data)
        return json.dumps(data, ensure_ascii=False, separators=(",", ":"))
    return json.dumps(data, indent=2, ensure_ascii=False)


def _table_rows(data: Any) -> list[dict[str, Any]]:
    if isinstance(data, list):
        return [row for row in data if isinstance(row, dict)]
    if isinstance(data, dict):
        list_values = [value for value in data.values() if isinstance(value, list) and value and all(isinstance(i, dict) for i in value)]
        if len(list_values) == 1:
            return list_values[0]
        if all(not isinstance(value, (dict, list)) for value in data.values()):
            return [data]
    return []


def render_table(data: Any) -> str:
    rows = _table_rows(data)
    if not rows:
        return render_json(data)

    columns: list[str] = []
    for row in rows:
        for key in row.keys():
            if key not in columns:
                columns.append(key)

    widths = {
        column: max(
            len(column),
            max((len(str((row.get(column, "")))) for row in rows), default=0),
        )
        for column in columns
    }

    header = " | ".join(column.ljust(widths[column]) for column in columns)
    divider = "-+-".join("-" * widths[column] for column in columns)
    body = [
        " | ".join(str(row.get(column, "")).ljust(widths[column]) for column in columns)
        for row in rows
    ]
    return "\n".join([header, divider, *body])


def quiet_value(data: Any) -> str:
    if isinstance(data, dict):
        for key in ("action_id", "id", "name", "profile_key", "path", "remote_ref", "message", "status"):
            value = data.get(key)
            if value:
                return str(value)
        if len(data) == 1:
            only = next(iter(data.values()))
            return quiet_value(only)
        return render_json(data, brief=True)
    if isinstance(data, list):
        if not data:
            return ""
        return "\n".join(quiet_value(item) for item in data)
    return str(data)


def render_output(data: Any, *, mode: str = "json", brief: bool = False) -> str:
    if mode == "quiet":
        return quiet_value(data)
    if mode == "table":
        return render_table(data)
    return render_json(data, brief=brief)
