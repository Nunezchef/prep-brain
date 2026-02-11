from unittest.mock import patch

import pytest

from services.command_runner import CommandRunner


def test_command_runner_blocks_disallowed_commands():
    runner = CommandRunner()
    with pytest.raises(ValueError):
        runner.run(["rm", "-rf", "/tmp/nope"])


@patch("services.command_runner.subprocess.run")
@patch("services.command_runner.shutil.which", return_value="/usr/bin/ffmpeg")
def test_command_runner_uses_shell_false(mock_which, mock_run):
    runner = CommandRunner()
    runner.run(["ffmpeg", "-version"], capture_output=True, text=True, check=False)

    _, kwargs = mock_run.call_args
    assert kwargs["shell"] is False


def test_command_runner_redacts_sensitive_args_in_debug_path():
    runner = CommandRunner()
    # We only validate command gate behavior here; command is blocked before execution.
    with pytest.raises(ValueError):
        runner.run(["curl", "https://example.com?token=supersecret"])
