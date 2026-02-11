from __future__ import annotations

from services.autonomy import AutonomyWorker, get_autonomy_status_snapshot


def get_status_snapshot():
    return get_autonomy_status_snapshot()
