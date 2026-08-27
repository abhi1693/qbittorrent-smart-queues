"""Environment-backed configuration helpers.

Keeping parsing in one place makes configuration precedence explicit and lets
provider implementations support renamed settings without duplicating fallback
logic.
"""

from __future__ import annotations

import os
from collections.abc import Iterable


def first_env(names: Iterable[str], default: str | None = None) -> str | None:
    """Return the first configured, non-empty environment value."""

    for name in names:
        value = os.environ.get(name)
        if value is not None and value.strip():
            return value
    return default


def env_str(names: str | Iterable[str], default: str = "") -> str:
    """Read a stripped string from one name or an ordered alias list."""

    if isinstance(names, str):
        names = (names,)
    value = first_env(names)
    return default if value is None else value.strip()


def env_bool(names: str | Iterable[str], default: bool = False) -> bool:
    """Read a boolean, accepting the app's existing truthy spellings."""

    if isinstance(names, str):
        names = (names,)
    value = first_env(names)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def env_int(names: str | Iterable[str], default: int) -> int:
    """Read an integer from one name or an ordered alias list."""

    if isinstance(names, str):
        names = (names,)
    value = first_env(names)
    return default if value is None else int(value)


def env_float(names: str | Iterable[str], default: float) -> float:
    """Read a float from one name or an ordered alias list."""

    if isinstance(names, str):
        names = (names,)
    value = first_env(names)
    return default if value is None else float(value)


def split_lines_or_csv(value: str | None) -> list[str]:
    if not value:
        return []
    return [item for line in value.replace(",", "\n").splitlines() if (item := line.strip())]


def split_key_value_lines(value: str | None) -> dict[str, str]:
    items: dict[str, str] = {}
    for item in split_lines_or_csv(value):
        if "=" not in item:
            continue
        key, item_value = (part.strip() for part in item.split("=", 1))
        if key and item_value:
            items[key] = item_value
    return items
