import time
from datetime import datetime, timedelta
from typing import Dict, List
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("MonitoringSystem")

class SLAMonitor:
    def __init__(self):
        self.metrics = {
            "p95_latency_seconds": 0.0,
            "docs_per_hour": 0,
            "error_rate_percent": 0.0,
            "review_queue_depth": 0,
            "sla_breach_percent": 0.0
        }
        self.alerts: List[str] = []
        # Track active alert keys to avoid duplicate accumulation on each evaluate
        self._active_alert_keys: set = set()

    def record_metrics(self, current_metrics: Dict):
        self.metrics.update(current_metrics)
        self.evaluate_slas()

    def evaluate_slas(self):
        # Re-evaluate all SLAs and update the active set (clears resolved alerts)
        new_alert_keys: set = set()

        if self.metrics["p95_latency_seconds"] > 30:
            key = "p95_latency"
            new_alert_keys.add(key)
            if key not in self._active_alert_keys:
                self.trigger_alert("Critical", "p95_latency_seconds > 30s")

        if self.metrics["docs_per_hour"] > 0 and self.metrics["docs_per_hour"] < 4500:
            key = "throughput"
            new_alert_keys.add(key)
            if key not in self._active_alert_keys:
                self.trigger_alert("Warning", "docs_per_hour fell below 4500 threshold")

        if self.metrics["error_rate_percent"] > 1.0:
            key = "error_rate"
            new_alert_keys.add(key)
            if key not in self._active_alert_keys:
                self.trigger_alert("Critical", "error_rate_percent > 1.0%")

        if self.metrics["review_queue_depth"] > 500:
            key = "queue_depth"
            new_alert_keys.add(key)
            if key not in self._active_alert_keys:
                self.trigger_alert("Warning", "review_queue_depth exceeded 500")

        if self.metrics["sla_breach_percent"] > 0.1:
            key = "sla_breach"
            new_alert_keys.add(key)
            if key not in self._active_alert_keys:
                self.trigger_alert("Critical", "sla_breach_percent exceeded 0.1%")

        self._active_alert_keys = new_alert_keys

    def trigger_alert(self, severity: str, message: str):
        alert_msg = f"[{severity}] {message} at {datetime.now().isoformat()}"
        logger.warning(alert_msg)
        self.alerts.append(alert_msg)

    def get_status(self) -> Dict:
        return {
            "system_status": "DEGRADED" if self._active_alert_keys else "HEALTHY",
            "active_alerts": len(self._active_alert_keys),
            "recent_alert_log": self.alerts[-20:],  # last 20 alert entries
            "metrics": self.metrics
        }
