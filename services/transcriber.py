from pathlib import Path
import subprocess

MODEL_PATH = Path("models/ggml-medium.bin")

import logging
from services.command_runner import CommandRunner

logger = logging.getLogger(__name__)
COMMAND_RUNNER = CommandRunner()

def transcribe_file(audio_path: str) -> str:
    if not MODEL_PATH.exists():
        raise RuntimeError(f"Whisper model not found: {MODEL_PATH} (download it into ./models/)")

    logger.info(f"Transcribing file: {audio_path}")
    try:
        # whisper-cli prints transcript to stdout with -otxt + -nt
        result = COMMAND_RUNNER.run(
            ["whisper-cli", "-m", str(MODEL_PATH), "-f", audio_path, "-nt", "-otxt"],
            capture_output=True,
            text=True,
            check=True,
        )
        text = result.stdout.strip()
        logger.info(f"Transcription result: {text[:50]}...")
        return text or "(No speech detected.)"
    except subprocess.CalledProcessError as e:
        logger.error(f"Whisper transcription failed: {e.stderr}")
        return "(Transcription failed)"
