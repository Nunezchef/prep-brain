from __future__ import annotations

from collections import defaultdict
from typing import Dict, List

from services.commands_registry import command_specs, get_group_order


def grouped_command_lines() -> Dict[str, List[str]]:
    grouped: Dict[str, List[str]] = defaultdict(list)
    for spec in command_specs():
        line = f"{spec.usage} — {spec.description}"
        if spec.admin_only:
            line += " (admin)"
        grouped[spec.group].append(line)

    ordered: Dict[str, List[str]] = {}
    for group in get_group_order():
        if group in grouped:
            ordered[group] = grouped[group]
    return ordered
