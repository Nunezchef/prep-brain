from datetime import datetime, timedelta, timezone
import time
import threading
import pytest
from unittest.mock import patch, MagicMock
from services.metrics import MetricsCollector, MetricPoint, record_llm_call, record_rag_query

class TestMetricsCollector:
    def test_increment(self):
        collector = MetricsCollector()
        collector.increment("test_counter")
        assert collector.get_counter("test_counter") == 1.0
        
        collector.increment("test_counter", 2.5)
        assert collector.get_counter("test_counter") == 3.5
        
        # Test with labels
        collector.increment("labeled_counter", labels={"env": "prod"})
        assert collector.get_counter("labeled_counter", labels={"env": "prod"}) == 1.0
        assert collector.get_counter("labeled_counter", labels={"env": "dev"}) == 0.0

    def test_gauge(self):
        collector = MetricsCollector()
        collector.gauge("test_gauge", 10.0)
        assert collector.get_gauge("test_gauge") == 10.0
        
        collector.gauge("test_gauge", 5.0)
        assert collector.get_gauge("test_gauge") == 5.0
        
        # Test with labels
        collector.gauge("labeled_gauge", 20.0, labels={"gpu": "true"})
        assert collector.get_gauge("labeled_gauge", labels={"gpu": "true"}) == 20.0

    def test_histogram(self):
        collector = MetricsCollector()
        values = [10, 20, 30, 40, 50]
        for v in values:
            collector.histogram("test_hist", v)
            
        stats = collector.get_histogram_stats("test_hist")
        assert stats["count"] == 5
        assert stats["min"] == 10
        assert stats["max"] == 50
        assert stats["avg"] == 30.0
        assert stats["p50"] == 30
        
        # Test rolling window (max 100 items)
        for i in range(150):
            collector.histogram("rolling_hist", i)
            
        stats = collector.get_histogram_stats("rolling_hist")
        assert stats["count"] == 100
        assert stats["min"] == 50  # 0-49 should be dropped
        assert stats["max"] == 149

    def test_timer(self):
        collector = MetricsCollector()
        with patch("time.perf_counter", side_effect=[0.0, 0.1]):  # 100ms elapsed
            with collector.timer("test_op"):
                pass
                
        # Timer records a histogram for duration and increments a counter
        stats = collector.get_histogram_stats("test_op_duration_ms")
        assert stats["count"] == 1
        assert stats["avg"] == pytest.approx(100.0)
        
        assert collector.get_counter("test_op_count") == 1.0

    def test_history_limit(self):
        collector = MetricsCollector(max_history=5)
        for i in range(10):
            collector.increment("counter_hist", 1.0)
            
        history = collector.get_recent_history(limit=100)
        assert len(history) == 5
        assert history[-1]["value"] == 10.0

    def test_get_recent_history_filter(self):
        collector = MetricsCollector()
        
        # Add old point (simulated)
        old_point = MetricPoint(
            name="old_metric", 
            value=1.0, 
            timestamp=datetime.now(timezone.utc) - timedelta(hours=2),
            labels={}
        )
        collector._history.append(old_point)
        
        # Add new point
        collector.increment("new_metric")
        
        recent = collector.get_recent_history(since=datetime.now(timezone.utc) - timedelta(minutes=30))
        assert len(recent) == 1
        assert recent[0]["name"] == "new_metric"

    def test_thread_safety(self):
        collector = MetricsCollector()
        threads = []
        
        def worker():
            for _ in range(100):
                collector.increment("thread_counter")
                
        for _ in range(10):
            t = threading.Thread(target=worker)
            threads.append(t)
            t.start()
            
        for t in threads:
            t.join()
            
        assert collector.get_counter("thread_counter") == 1000.0

    def test_reset(self):
        collector = MetricsCollector()
        collector.increment("foo")
        collector.reset()
        assert collector.get_counter("foo") == 0.0
        headers = collector.get_all_metrics()
        assert not headers["counters"]
        assert not headers["gauges"]
        assert not headers["histograms"]

    def test_convenience_functions(self):
        # We need to access the global 'metrics' object in services.metrics
        # or mock it. Since we want to test the functions, we can inspect side effects on the global object.
        from services.metrics import metrics as global_metrics
        global_metrics.reset()
        
        record_llm_call("gpt-4", 500.0, True)
        assert global_metrics.get_counter("llm_calls_total", labels={"model": "gpt-4", "success": "true"}) == 1.0
        stats = global_metrics.get_histogram_stats("llm_call_duration_ms", labels={"model": "gpt-4", "success": "true"})
        assert stats["count"] == 1
        assert stats["avg"] == 500.0
        
        record_llm_call("gpt-4", 100.0, False)
        assert global_metrics.get_counter("llm_errors_total", labels={"model": "gpt-4"}) == 1.0

    def test_record_rag_query(self):
        from services.metrics import metrics as global_metrics
        global_metrics.reset()
        
        record_rag_query("search", 5, 200.0)
        
        assert global_metrics.get_counter("rag_queries_total", labels={"type": "search"}) == 1.0
        assert global_metrics.get_gauge("rag_chunks_retrieved", labels={"type": "search"}) == 5.0
        stats = global_metrics.get_histogram_stats("rag_query_duration_ms", labels={"type": "search"})
        assert stats["avg"] == 200.0

    def test_format_metrics_telegram(self):
        from services.metrics import format_metrics_telegram, metrics as global_metrics
        global_metrics.reset()
        
        global_metrics.increment("test_counter", 10.0)
        global_metrics.gauge("test_gauge", 5.5)
        global_metrics.histogram("test_hist", 100.0)
        
        output = format_metrics_telegram()
        assert "System Metrics" in output
        assert "Counters:" in output
        assert "test_counter: 10" in output
        assert "Gauges:" in output
        assert "test_gauge: 5.50" in output
        assert "Latencies (ms):" in output
        assert "test_hist: avg=100.0" in output
