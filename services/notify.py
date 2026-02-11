from typing import Dict


def silent_success() -> Dict[str, object]:
    return {"silent": True, "message": ""}


def alert_required(msg: str) -> Dict[str, object]:
    return {"silent": False, "level": "alert", "message": str(msg or "").strip()}


def status_line(msg: str) -> Dict[str, object]:
    return {"silent": False, "level": "status", "message": str(msg or "").strip()}
