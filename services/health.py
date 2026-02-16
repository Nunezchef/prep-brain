"""Health check functions for system dependencies."""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Any

import requests

from prep_brain.config import load_config
from services import memory

logger = logging.getLogger(__name__)


@dataclass
class HealthCheckResult:
    """Result of a single health check."""

    name: str
    healthy: bool
    latency_ms: float
    message: str
    details: dict[str, Any] | None = None


@dataclass
class SystemHealth:
    """Aggregated system health status."""

    healthy: bool
    checks: list[HealthCheckResult]
    timestamp: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "healthy": self.healthy,
            "timestamp": self.timestamp,
            "checks": [
                {
                    "name": c.name,
                    "healthy": c.healthy,
                    "latency_ms": round(c.latency_ms, 2),
                    "message": c.message,
                    "details": c.details,
                }
                for c in self.checks
            ],
        }


def check_sqlite() -> HealthCheckResult:
    """Check SQLite database connectivity and basic operations."""
    start = time.perf_counter()
    try:
        con = memory.get_conn()
        # Test read
        cur = con.execute("SELECT COUNT(*) FROM recipes")
        recipe_count = cur.fetchone()[0]
        # Test write capability (to a safe table)
        con.execute("SELECT 1")
        con.close()
        latency = (time.perf_counter() - start) * 1000
        return HealthCheckResult(
            name="sqlite",
            healthy=True,
            latency_ms=latency,
            message="Database operational",
            details={"recipe_count": recipe_count, "db_path": str(memory.get_db_path())},
        )
    except Exception as e:
        latency = (time.perf_counter() - start) * 1000
        logger.error(f"SQLite health check failed: {e}")
        return HealthCheckResult(
            name="sqlite",
            healthy=False,
            latency_ms=latency,
            message=f"Database error: {str(e)[:100]}",
        )


def check_ollama() -> HealthCheckResult:
    """Check Ollama LLM service connectivity."""
    config = load_config()
    base_url = config.get("ollama", {}).get("base_url", "http://localhost:11434")
    start = time.perf_counter()
    try:
        # Check if Ollama is responding
        response = requests.get(f"{base_url}/api/tags", timeout=5)
        latency = (time.perf_counter() - start) * 1000
        if response.status_code == 200:
            data = response.json()
            models = [m.get("name", "unknown") for m in data.get("models", [])]
            configured_model = config.get("ollama", {}).get("model", "")
            model_available = any(configured_model in m for m in models)
            return HealthCheckResult(
                name="ollama",
                healthy=True,
                latency_ms=latency,
                message="Ollama operational",
                details={
                    "base_url": base_url,
                    "models_available": len(models),
                    "configured_model": configured_model,
                    "model_loaded": model_available,
                },
            )
        else:
            return HealthCheckResult(
                name="ollama",
                healthy=False,
                latency_ms=latency,
                message=f"Ollama returned status {response.status_code}",
            )
    except requests.exceptions.ConnectionError:
        latency = (time.perf_counter() - start) * 1000
        return HealthCheckResult(
            name="ollama",
            healthy=False,
            latency_ms=latency,
            message=f"Cannot connect to Ollama at {base_url}",
        )
    except Exception as e:
        latency = (time.perf_counter() - start) * 1000
        logger.error(f"Ollama health check failed: {e}")
        return HealthCheckResult(
            name="ollama",
            healthy=False,
            latency_ms=latency,
            message=f"Ollama error: {str(e)[:100]}",
        )


def check_chromadb() -> HealthCheckResult:
    """Check ChromaDB vector store connectivity."""
    start = time.perf_counter()
    try:
        # Import here to avoid circular imports
        from services.rag import get_collection

        collection = get_collection()
        count = collection.count()
        latency = (time.perf_counter() - start) * 1000
        return HealthCheckResult(
            name="chromadb",
            healthy=True,
            latency_ms=latency,
            message="ChromaDB operational",
            details={"document_count": count},
        )
    except Exception as e:
        latency = (time.perf_counter() - start) * 1000
        logger.error(f"ChromaDB health check failed: {e}")
        return HealthCheckResult(
            name="chromadb",
            healthy=False,
            latency_ms=latency,
            message=f"ChromaDB error: {str(e)[:100]}",
        )


def check_autonomy() -> HealthCheckResult:
    """Check autonomy loop status."""
    start = time.perf_counter()
    try:
        con = memory.get_conn()
        row = con.execute("SELECT * FROM autonomy_status WHERE id = 1").fetchone()
        con.close()
        latency = (time.perf_counter() - start) * 1000

        if not row:
            return HealthCheckResult(
                name="autonomy",
                healthy=False,
                latency_ms=latency,
                message="Autonomy status not initialized",
            )

        is_running = bool(row["is_running"])
        last_tick = row["last_tick_at"]
        last_error = row["last_error"]

        # Check if last tick was within reasonable time (5 minutes for poll interval)
        healthy = is_running and last_tick is not None

        return HealthCheckResult(
            name="autonomy",
            healthy=healthy,
            latency_ms=latency,
            message="Autonomy running" if healthy else "Autonomy not running",
            details={
                "is_running": is_running,
                "last_tick_at": last_tick,
                "last_error": last_error[:100] if last_error else None,
                "pending_drafts": row["queue_pending_drafts"],
                "pending_ingests": row["queue_pending_ingests"],
            },
        )
    except Exception as e:
        latency = (time.perf_counter() - start) * 1000
        logger.error(f"Autonomy health check failed: {e}")
        return HealthCheckResult(
            name="autonomy",
            healthy=False,
            latency_ms=latency,
            message=f"Autonomy check error: {str(e)[:100]}",
        )


def get_system_health() -> SystemHealth:
    """Run all health checks and return aggregated status."""
    from datetime import datetime

    checks = [
        check_sqlite(),
        check_ollama(),
        check_chromadb(),
        check_autonomy(),
    ]

    # System is healthy only if all critical checks pass
    # (Ollama and autonomy can be degraded without full failure)
    critical_checks = ["sqlite", "chromadb"]
    healthy = all(c.healthy for c in checks if c.name in critical_checks)

    return SystemHealth(
        healthy=healthy,
        checks=checks,
        timestamp=datetime.utcnow().isoformat() + "Z",
    )


def format_health_telegram(health: SystemHealth) -> str:
    """Format health status for Telegram display."""
    status_icon = "✅" if health.healthy else "❌"
    lines = [f"{status_icon} **System Health**", ""]

    for check in health.checks:
        icon = "✅" if check.healthy else "❌"
        lines.append(f"{icon} **{check.name}**: {check.message} ({check.latency_ms:.0f}ms)")
        if check.details:
            for key, value in check.details.items():
                if value is not None:
                    lines.append(f"   • {key}: {value}")

    return "\n".join(lines)
