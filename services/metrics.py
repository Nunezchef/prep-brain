"""Simple metrics collection for observability."""
from __future__ import annotations

import logging
import threading
import time
from collections import defaultdict
from collections.abc import Generator
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class MetricPoint:
    """A single metric data point."""

    name: str
    value: float
    timestamp: datetime
    labels: dict[str, str] = field(default_factory=dict)


class MetricsCollector:
    """Thread-safe metrics collector with in-memory storage."""

    def __init__(self, max_history: int = 1000):
        self._lock = threading.RLock()
        self._counters: dict[str, float] = defaultdict(float)
        self._gauges: dict[str, float] = {}
        self._histograms: dict[str, list[float]] = defaultdict(list)
        self._history: list[MetricPoint] = []
        self._max_history = max_history

    def increment(self, name: str, value: float = 1.0, labels: dict[str, str] | None = None) -> None:
        """Increment a counter metric."""
        key = self._make_key(name, labels)
        with self._lock:
            self._counters[key] += value
            self._add_history(name, self._counters[key], labels)

    def gauge(self, name: str, value: float, labels: dict[str, str] | None = None) -> None:
        """Set a gauge metric to a specific value."""
        key = self._make_key(name, labels)
        with self._lock:
            self._gauges[key] = value
            self._add_history(name, value, labels)

    def histogram(self, name: str, value: float, labels: dict[str, str] | None = None) -> None:
        """Record a value in a histogram metric."""
        key = self._make_key(name, labels)
        with self._lock:
            self._histograms[key].append(value)
            # Keep last 100 values per histogram
            if len(self._histograms[key]) > 100:
                self._histograms[key] = self._histograms[key][-100:]
            self._add_history(name, value, labels)

    @contextmanager
    def timer(self, name: str, labels: dict[str, str] | None = None) -> Generator[None, None, None]:
        """Context manager to time an operation and record as histogram."""
        start = time.perf_counter()
        try:
            yield
        finally:
            elapsed_ms = (time.perf_counter() - start) * 1000
            self.histogram(f"{name}_duration_ms", elapsed_ms, labels)
            self.increment(f"{name}_count", labels=labels)

    def _make_key(self, name: str, labels: dict[str, str] | None) -> str:
        """Create a unique key from name and labels."""
        if not labels:
            return name
        label_str = ",".join(f"{k}={v}" for k, v in sorted(labels.items()))
        return f"{name}{{{label_str}}}"

    def _add_history(self, name: str, value: float, labels: dict[str, str] | None) -> None:
        """Add to history (called within lock)."""
        self._history.append(
            MetricPoint(
                name=name,
                value=value,
                timestamp=datetime.now(timezone.utc),
                labels=labels or {},
            )
        )
        if len(self._history) > self._max_history:
            self._history = self._history[-self._max_history:]

    def get_counter(self, name: str, labels: dict[str, str] | None = None) -> float:
        """Get current counter value."""
        key = self._make_key(name, labels)
        with self._lock:
            return self._counters.get(key, 0.0)

    def get_gauge(self, name: str, labels: dict[str, str] | None = None) -> float | None:
        """Get current gauge value."""
        key = self._make_key(name, labels)
        with self._lock:
            return self._gauges.get(key)

    def get_histogram_stats(self, name: str, labels: dict[str, str] | None = None) -> dict[str, float]:
        """Get histogram statistics (count, min, max, avg, p50, p95, p99)."""
        key = self._make_key(name, labels)
        with self._lock:
            values = self._histograms.get(key, [])
            if not values:
                return {}
            sorted_values = sorted(values)
            count = len(sorted_values)
            return {
                "count": count,
                "min": sorted_values[0],
                "max": sorted_values[-1],
                "avg": sum(sorted_values) / count,
                "p50": sorted_values[int(count * 0.5)],
                "p95": sorted_values[int(count * 0.95)] if count > 1 else sorted_values[-1],
                "p99": sorted_values[int(count * 0.99)] if count > 1 else sorted_values[-1],
            }

    def get_all_metrics(self) -> dict[str, Any]:
        """Get all current metric values."""
        with self._lock:
            return {
                "counters": dict(self._counters),
                "gauges": dict(self._gauges),
                "histograms": {
                    name: self.get_histogram_stats(name.split("{")[0])
                    for name in self._histograms.keys()
                },
                "collected_at": datetime.now(timezone.utc).isoformat(),
            }

    def get_recent_history(self, since: datetime | None = None, limit: int = 100) -> list[dict[str, Any]]:
        """Get recent metric history."""
        cutoff = since or (datetime.now(timezone.utc) - timedelta(hours=1))
        with self._lock:
            filtered = [
                {
                    "name": p.name,
                    "value": p.value,
                    "timestamp": p.timestamp.isoformat() + "Z",
                    "labels": p.labels,
                }
                for p in self._history
                if p.timestamp >= cutoff
            ]
            return filtered[-limit:]

    def reset(self) -> None:
        """Reset all metrics (useful for testing)."""
        with self._lock:
            self._counters.clear()
            self._gauges.clear()
            self._histograms.clear()
            self._history.clear()


# Global metrics instance
metrics = MetricsCollector()


# Convenience functions for common metrics
def record_llm_call(model: str, duration_ms: float, success: bool) -> None:
    """Record an LLM call metric."""
    labels = {"model": model, "success": str(success).lower()}
    metrics.histogram("llm_call_duration_ms", duration_ms, labels)
    metrics.increment("llm_calls_total", labels=labels)
    if not success:
        metrics.increment("llm_errors_total", labels={"model": model})


def record_command(command: str, duration_ms: float, success: bool = True) -> None:
    """Record a Telegram command metric."""
    labels = {"command": command, "success": str(success).lower()}
    metrics.histogram("command_duration_ms", duration_ms, labels)
    metrics.increment("commands_total", labels=labels)


def record_rag_query(query_type: str, chunks_retrieved: int, duration_ms: float) -> None:
    """Record a RAG query metric."""
    metrics.histogram("rag_query_duration_ms", duration_ms, {"type": query_type})
    metrics.gauge("rag_chunks_retrieved", chunks_retrieved, {"type": query_type})
    metrics.increment("rag_queries_total", labels={"type": query_type})


def record_autonomy_tick(action: str, duration_ms: float, success: bool) -> None:
    """Record an autonomy loop tick metric."""
    labels = {"action": action, "success": str(success).lower()}
    metrics.histogram("autonomy_tick_duration_ms", duration_ms, labels)
    metrics.increment("autonomy_ticks_total", labels=labels)


def record_ingest(source_type: str, chunks: int, duration_ms: float, success: bool) -> None:
    """Record a document ingest metric."""
    labels = {"source_type": source_type, "success": str(success).lower()}
    metrics.histogram("ingest_duration_ms", duration_ms, labels)
    metrics.gauge("ingest_chunks", chunks, labels)
    metrics.increment("ingests_total", labels=labels)


def record_error(component: str, error_type: str) -> None:
    """Record an error metric."""
    metrics.increment("errors_total", labels={"component": component, "type": error_type})


def format_metrics_telegram() -> str:
    """Format metrics for Telegram display."""
    data = metrics.get_all_metrics()
    lines = ["📊 **System Metrics**", ""]

    # Counters
    if data["counters"]:
        lines.append("**Counters:**")
        for name, value in sorted(data["counters"].items())[:10]:
            lines.append(f"  • {name}: {value:.0f}")

    # Gauges
    if data["gauges"]:
        lines.append("")
        lines.append("**Gauges:**")
        for name, value in sorted(data["gauges"].items())[:10]:
            lines.append(f"  • {name}: {value:.2f}")

    # Key histograms
    if data["histograms"]:
        lines.append("")
        lines.append("**Latencies (ms):**")
        for name, stats in sorted(data["histograms"].items())[:5]:
            if stats:
                lines.append(
                    f"  • {name}: avg={stats['avg']:.1f} p95={stats['p95']:.1f} (n={stats['count']:.0f})"
                )

    return "\n".join(lines)
