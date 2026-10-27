from __future__ import annotations

import tomllib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class StrategyConfig:
    type: str
    instance_id: str
    params: dict[str, Any] = field(default_factory=dict)
    options: dict[str, Any] = field(default_factory=dict)


def load_config(path: Path) -> dict[str, Any]:
    with path.open("rb") as stream:
        config = tomllib.load(stream)
    if not isinstance(config.get("strategies", []), list):
        raise ValueError("'strategies' must be a list")
    return config


def enabled_strategies(config: dict[str, Any]) -> list[StrategyConfig]:
    result: list[StrategyConfig] = []
    ids: set[str] = set()
    for index, entry in enumerate(config.get("strategies", [])):
        if not isinstance(entry, dict):
            raise ValueError(f"strategies[{index}] must be a table")
        if not isinstance(entry.get("enabled", True), bool):
            raise ValueError(f"strategies[{index}].enabled must be a boolean")
        if not entry.get("enabled", True):
            continue
        kind = entry.get("type")
        identifier = entry.get("instance_id")
        if not isinstance(kind, str) or not kind.strip():
            raise ValueError(f"strategies[{index}] needs a nonempty type")
        if not isinstance(identifier, str) or not identifier.strip():
            raise ValueError(f"strategies[{index}] needs a nonempty instance_id")
        if identifier in ids:
            raise ValueError(f"Duplicate instance_id: {identifier}")
        ids.add(identifier)
        params = entry.get("params", {})
        if not isinstance(params, dict):
            raise ValueError(f"strategies[{index}].params must be a table")
        result.append(StrategyConfig(kind, identifier, dict(params),
                                     {k: v for k, v in entry.items()
                                      if k not in {"type", "instance_id", "params", "enabled"}}))
    return result
