import logging
import re
import shutil
import subprocess
from pathlib import Path
from typing import Iterable, List, Optional, Sequence
import time

from services import metrics

logger = logging.getLogger(__name__)

ALLOWED_COMMANDS = {
    "ffmpeg",
    "whisper-cli",
    "ollama",
    "pgrep",
}

SECRET_PATTERN = re.compile(
    r"(?i)\b(token|api[_-]?key|password|secret|authorization)\b\s*([=:])\s*([^\s]+)"
)


def _redact_arg(arg: str) -> str:
    value = str(arg)
    value = SECRET_PATTERN.sub(r"\1\2[REDACTED]", value)
    value = re.sub(r"(?i)\bBearer\s+[A-Za-z0-9._\-]+\b", "Bearer [REDACTED]", value)
    return value


class CommandRunner:
    """Runs a strict allowlist of internal commands with shell disabled."""

    def __init__(self, allowed_commands: Optional[Iterable[str]] = None):
        self.allowed_commands = set(allowed_commands or ALLOWED_COMMANDS)

    def is_allowed(self, command: str) -> bool:
        binary = Path(str(command)).name
        return binary in self.allowed_commands

    def run(
        self,
        args: Sequence[str],
        *,
        check: bool = False,
        capture_output: bool = False,
        text: bool = False,
        timeout: Optional[float] = None,
        cwd: Optional[str] = None,
    ) -> subprocess.CompletedProcess:
        if not args:
            raise ValueError("CommandRunner requires at least one arg.")

        command = str(args[0])
        binary = Path(command).name
        if not self.is_allowed(binary):
            raise ValueError(f"Blocked command '{binary}'. Not in allowlist.")

        # Ensure command exists when provided as a bare binary name.
        if Path(command).name == command and shutil.which(command) is None:
            raise FileNotFoundError(f"Command '{command}' not found in PATH.")

        printable_args: List[str] = [_redact_arg(str(part)) for part in args]
        logger.debug("CommandRunner executing: %s", " ".join(printable_args))

        start_ts = time.time()
        success = False
        try:
            result = subprocess.run(
                [str(part) for part in args],
                shell=False,
                check=check,
                capture_output=capture_output,
                text=text,
                timeout=timeout,
                cwd=cwd,
            )
            success = result.returncode == 0
            return result
        except Exception:
            raise
        finally:
            duration_ms = (time.time() - start_ts) * 1000
            metrics.record_command(command=binary, duration_ms=duration_ms, success=success)
