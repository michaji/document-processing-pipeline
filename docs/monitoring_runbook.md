# Monitoring & Alerting Runbook

## High Priority Alerts (Critical)

### 1. P95 Latency > 30s
**Symptoms**: The system takes too long to process single documents. Clients experience timeouts.
**Troubleshooting Steps**:
1. Check the `LLM API Latency` dashboard. If external APIs are slow, verify circuit breaker status.
2. Check internal queue metrics (Message Bus/RabbitMQ). Look for blocked consumers.
3. Check application metrics for `OOM` (Out of Memory) or slow db queries.

### 2. Error Rate > 1%
**Symptoms**: Elevated failure rate in document processing.
**Troubleshooting Steps**:
1. Search logs for `ERROR` level messages in `src/extraction_module.py`.
2. Determine if errors correlate with a specific document format or tenant.
3. Roll back any recent ML model deployments if failures are model-related.

### 3. SLA Breach Percent > 0.1%
**Symptoms**: End-to-end processing (including human review) takes longer than guaranteed.
**Troubleshooting Steps**:
1. Look at processing latencies vs human review queue depth.
2. If human review queue is high, escalate to operations to allocate more human reviewers.
3. If ingestion is slow, investigate parsing/OCR nodes.

## Medium Priority Alerts (Warning)

### 1. Throughput < 4500 Docs/Hour
**Symptoms**: Volume of processed documents handles less than peak capacity.
**Troubleshooting Steps**:
1. Verify ingestion feed is sending enough documents.
2. If feed is full, check for bottlenecked worker nodes.
3. Scale out worker pods using HPA (Horizontal Pod Autoscaler).

### 2. Queue Depth > 500
**Symptoms**: Backlog in manual review queue.
**Troubleshooting Steps**:
1. Ensure the UI is accessible and reviewers are active.
2. Determine if the ML model confidence threshold is misconfigured, causing too many documents to fall back to review.
