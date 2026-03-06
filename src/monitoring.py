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
        self.alerts = []

    def record_metrics(self, current_metrics: Dict):
        self.metrics.update(current_metrics)
        self.evaluate_slas()

    def evaluate_slas(self):
        # Latency SLA
        if self.metrics["p95_latency_seconds"] > 30:
            self.trigger_alert("Critical", "p95_latency_seconds > 30s")
            
        # Throughput SLA
        if self.metrics["docs_per_hour"] < 4500:
            self.trigger_alert("Warning", "docs_per_hour fell below 4500 threshold")
            
        # Error Rate SLA
        if self.metrics["error_rate_percent"] > 1.0:
            self.trigger_alert("Critical", "error_rate_percent > 1.0%")
            
        # Queue Depth SLA
        if self.metrics["review_queue_depth"] > 500:
            self.trigger_alert("Warning", "review_queue_depth exceeded 500")
            
        # SLA Breach Percent
        if self.metrics["sla_breach_percent"] > 0.1:
            self.trigger_alert("Critical", "sla_breach_percent exceeded 0.1%")

    def trigger_alert(self, severity: str, message: str):
        alert_msg = f"[{severity}] {message} at {datetime.now()}"
        logger.warning(alert_msg)
        self.alerts.append(alert_msg)

    def get_status(self) -> Dict:
        return {
            "system_status": "DEGRADED" if self.alerts else "HEALTHY",
            "active_alerts": len(self.alerts),
            "metrics": self.metrics
        }
