# Module Architecture Design

## Overview
This document describes the production-grade architecture of the document **extraction module** — the core processing unit responsible for transforming raw document bytes into validated, structured financial data at scale (5,000 documents/hour peak throughput).

---

## 1.1 Module Interface & Data Flow

### Input Contract
Every document submission must provide:

| Field | Type | Required | Description |
|---|---|---|---|
| `document_id` | `string` | ✅ | Globally unique identifier (e.g. UUID) |
| `payload` | `bytes` | ✅ | Raw document content (PDF, image, etc.) |
| `document_type` | `enum` | ✅ | One of: `invoice`, `contract`, `receipt`, `po` |
| `tenant_id` | `string` | ✅ | Multi-tenant isolation key |
| `manual_corrections` | `dict` | ❌ | Field-keyed dict of human overrides; source tagged as `HUMAN` |

### Output Contract
Every successful result includes:

| Field | Type | Description |
|---|---|---|
| `document_id` | `string` | Echoed from input |
| `schema_version` | `string` | e.g. `"1.0"` — enables backward compat |
| `processed_at` | `ISO-8601` | UTC timestamp of processing completion |
| `content_hash` | `string` | SHA-256 of raw payload — part of idempotency key |
| `needs_review` | `bool` | `true` if any field confidence < threshold |
| `validation_errors` | `list[str]` | Empty list if all validations pass |
| `quality_metrics` | `object` | Aggregate confidence stats — see §Quality Metrics |
| `audit_trail` | `list[event]` | Ordered list of processing events with timestamps |
| `<field>` | varies | Extracted field value |
| `<field>_confidence` | `float` | 0.0–1.0 confidence score for that field |
| `<field>_source` | `enum` | `AI` or `HUMAN` |

Both outputs (JSON + Parquet) carry the same schema, with Parquet using Snappy compression and all nested structures serialised as JSON strings for columnar compatibility.

### Schema Versioning
- Every output record includes `schema_version`.
- Breaking schema changes bump the major version (e.g. `1.0` → `2.0`) and are deployed alongside a migration script.
- Consumers must check `schema_version` before parsing; older versions are retired after a deprecation window of 90 days.

### Internal Processing Pipeline Stages

```
[Ingest] → [Idempotency Check] → [QUEUED] → [PROCESSING]
    → [LLM Extract + Retry] → [Field Preservation] → [Validation]
    → [Confidence Routing] → [Quality Metrics] → [Dual Output Write]
    → [COMPLETED | REVIEW_PENDING | FAILED]
```

1. **Ingest & Validate**: Verify payload is non-empty, `document_type` is known, schema version is compatible.
2. **Idempotency Gate**: Compute `SHA-256(document_id + SHA-256(payload))`. If key exists as `COMPLETED`, return cached output immediately. If `PROCESSING`, reject as a concurrent duplicate.
3. **State → QUEUED → PROCESSING**: Atomically update the document state to prevent concurrent duplicates.
4. **LLM Extraction**: Call the extraction backend. Wrapped in retry + circuit breaker (see §1.3).
5. **Field Preservation Merge**: Apply `manual_corrections` on top of the AI output. Human values always win and are flagged with `_source = HUMAN` and `_confidence = 1.0`.
6. **Schema Validation**: Run cross-field rules (e.g. `tax_amount ≤ total_amount`, required fields present, enum values valid).
7. **Confidence-Based Routing**: If any field's `_confidence < threshold`, set `needs_review = true` and transition to `REVIEW_PENDING`.
8. **Quality Metrics**: Compute aggregate confidence statistics for monitoring and dashboards.
9. **Dual Output Write**: Atomically write JSON and Parquet using a temp-file + rename strategy to ensure zero partial writes.

### Idempotency Strategy
Key design decision: **content-addressed idempotency**.
- Key = `SHA-256(document_id + content_hash)`.
- This ensures **same document + same content → same key → same cached result**.
- Re-submitting a document that was manually corrected will have a different `document_id` or trigger a reprocess via explicit `force_reprocess` flag — preventing stale cache hits from hiding corrections.
- Backing store is a thread-safe in-memory dict in development; in production, use **Redis** (`SET NX`) or **DynamoDB conditional writes** for distributed idempotency with TTL of 90 days.

---

## 1.2 State Management

### Processing Status Transitions

```
            ┌─────────────────────────────────────────────┐
            │                                             │
QUEUED ──▶ PROCESSING ──▶ COMPLETED                      │
                │                                         │
                ├──▶ REVIEW_PENDING ──▶ (human resolves) ─┘
                │
                └──▶ FAILED (after retry exhaustion)
```

| Status | Description |
|---|---|
| `QUEUED` | Message received, not yet picked up by a worker |
| `PROCESSING` | Worker has claimed the document; concurrent duplicates are rejected |
| `COMPLETED` | All fields extracted, validated; outputs written |
| `REVIEW_PENDING` | Low-confidence fields detected; routed to human review queue |
| `FAILED` | Unrecoverable after all retries; dead-lettered for investigation |

State is persisted atomically. In production the `PROCESSING` claim uses a compare-and-swap (CAS) operation against the idempotency store to prevent two workers claiming the same document.

### Field-Level Locking (Preserving Manual Corrections)
Each extracted field is stored as a triplet:
```json
{
  "vendor_name": "Acme Corp",
  "vendor_name_confidence": 0.88,
  "vendor_name_source": "AI"
}
```
When manual corrections are submitted:
1. The correction is saved with `_source = HUMAN` and `_confidence = 1.0`.
2. On any future reprocessing, the system checks `_source` before overwriting: **any `HUMAN`-sourced field is injected back from the database and the AI output for that field is discarded**.
3. Correction events are appended to the `audit_trail` with original AI value, human value, and reviewer identity — providing full compliance lineage.

---

## 1.3 Error Handling & Scalability

### Retry Logic — Exponential Backoff with Full Jitter
```
delay = random.uniform(0, base_delay * 2^attempt)
```
- `base_delay = 1.0s`, max jitter cap `= 30s`
- Max 3 retries for transient errors (`ConnectionError`, `TimeoutError`, HTTP 429/5xx)
- Non-retryable errors (bad document format, schema mismatch) fail immediately → `FAILED`

Full jitter (vs. equal jitter) is used to prevent the **thundering herd problem** where all retrying workers hit the LLM API simultaneously after a burst failure.

### Circuit Breaker
Implemented for all external services (LLM, OCR APIs):
- **Sliding window**: tracks success/failure ratio over the last 20 calls.
- **Threshold**: Opens at ≥ 15% failure rate.
- **Recovery**: Transitions to `HALF_OPEN` after 30s; a single success closes it.
- When `OPEN`, calls fail immediately (fast-fail) and are queued for retry when the circuit closes — **preventing cascading failure** into downstream services.

### Scalability — 5,000 Documents/Hour
| Design Element | Approach |
|---|---|
| **Stateless workers** | No in-process state between documents; all state in store |
| **Message queue decoupling** | SQS/RabbitMQ absorbs bursts; workers pull at their own pace |
| **Horizontal autoscaling** | Worker pods scale based on queue depth (HPA in Kubernetes) |
| **Thread-safe concurrency** | `IdempotencyStore` uses `threading.Lock` internally |
| **Async I/O** | Integration layer uses `asyncio` for concurrent LLM + file writes |
| **Throughput math** | 5,000/hr = ~1.39 docs/sec; with 10 workers at 15s avg latency each, capacity = 10/15 × 3600 = 2,400 docs/hr per pod; **2–3 pods** covers the peak comfortably |

### Zero Data Loss Guarantee
- Dual-write strategy: JSON written first, then Parquet via atomic temp-rename.
- If a worker crashes mid-write: the idempotency key remains as `PROCESSING`, a watchdog process moves timed-out `PROCESSING` items back to `QUEUED` for reprocessing.
- Message queue uses **at-least-once** delivery with `ack` only sent after successful dual write; no message is ever dropped.

### Audit Trail
Every state change and field correction is appended to `audit_trail` as a timestamped event:
```json
{ "timestamp": "2024-01-15T12:00:00Z", "event": "field_corrected",
  "details": { "field": "vendor_name", "original_ai_value": "ACME", "human_value": "Acme Corp" } }
```
This satisfies **compliance lineage requirements** — a full reconstruction of how every output value was derived is available on demand.
