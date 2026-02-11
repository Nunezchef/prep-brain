import asyncio
from pathlib import Path

from services import autonomy, memory

TEST_DB = Path("test_autonomy_heartbeat.db")


def _run(coro):
    return asyncio.run(coro)


def test_autonomy_tick_updates_heartbeat(monkeypatch):
    original_db = memory.DB_PATH
    memory.DB_PATH = TEST_DB
    memory.init_db()

    worker = autonomy.AutonomyWorker()

    async def _noop(*args, **kwargs):
        return None

    async def _jobs(*args, **kwargs):
        return 0

    monkeypatch.setattr(worker, "process_ingest_jobs", _jobs)
    monkeypatch.setattr(worker, "evaluate_documents", _noop)
    monkeypatch.setattr(worker, "enrich_drafts", _noop)
    monkeypatch.setattr(worker, "promote_drafts", _noop)
    monkeypatch.setattr(worker, "run_cycle", _noop)
    monkeypatch.setattr(worker, "_acquire_singleton", lambda: True)

    try:
        _run(worker.run_background_tick())
        snapshot = autonomy.get_autonomy_status_snapshot()
        assert int(snapshot.get("is_running") or 0) == 1
        assert str(snapshot.get("last_tick_at") or "") != ""
        assert int(snapshot.get("queue_pending_ingests") or 0) >= 0
    finally:
        memory.DB_PATH = original_db
        if TEST_DB.exists():
            TEST_DB.unlink()
