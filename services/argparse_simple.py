from __future__ import annotations

import shlex
from typing import List


def split_command_line(text: str) -> List[str]:
    """Split command text while preserving quoted segments."""
    raw = str(text or "").strip()
    if not raw:
        return []
    try:
        return shlex.split(raw)
    except ValueError:
        # Fall back to a simple split if quotes are unbalanced.
        return raw.split()
