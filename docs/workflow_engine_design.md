# Workflow Engine Design

## Overview
The workflow engine orchestrates multi-stage document processing as a **Directed Acyclic Graph (DAG)**. Each node is an isolated, stateless processing function. Edges encode data dependencies. Independent branches execute in parallel. The engine integrates directly with the `ExtractionModule` (Part A) as the `extract` step.

---

## DAG Representation

### Graph Structure

```
                    ┌─────────┐
                    │  parse  │  (validate payload, fingerprint)
                    └────┬────┘
                         │
                    ┌────▼────┐
                    │ extract │  (LLM extraction via ExtractionModule)
                    └────┬────┘
                         │
                    ┌────▼────┐
                    │ enrich  │  (vendor lookup, currency normalisation)
                    └────┬────┘
              ┌──────────┴──────────┐
              │ condition check     │
    ┌─────────▼───────┐   ┌────────▼────────────┐
    │     publish     │   │  publish_for_review │
    │ (direct sink)   │   │  (review queue)     │
    └─────────────────┘   └─────────────────────┘
```

Nodes are `StepConfig` objects containing:
- `name`: unique identifier
- `func`: a callable with kwargs matching upstream step names
- `inputs`: list of upstream step names (edges)
- `condition`: optional predicate `(context: Dict) → bool` for conditional routing
- `retry`: `RetryConfig(max_attempts, base_delay, max_delay)`
- `rate_limited`: bool — whether to acquire a rate-limiter token before executing
- `fan_out_from`: enables scatter/gather over a list produced by an upstream step

### Cycle Detection
The `DAGWorkflow.validate()` method runs **Kahn's algorithm** (BFS-based topological sort):
1. Compute in-degree for all nodes.
2. Seed a queue with all zero-in-degree nodes.
3. Process the queue: decrement in-degree of children; re-enqueue any that reach zero.
4. If `len(topo_order) != len(nodes)` → cycle detected → `CycleError` raised.

This is `O(V + E)` and runs synchronously before any execution begins, so invalid graphs are rejected immediately.

### Schema Validation
Every step declares its expected inputs by name. Before execution the engine verifies:
- All declared inputs reference registered steps.
- No orphan nodes exist.
- The resulting graph is acyclic.
Runtime type checking is intentionally left lightweight (duck-typed Python) to avoid over-engineering; production environments can add Pydantic step schemas.

---

## Parallelism Model

### Document-Level Parallelism
The `WorkflowExecutor.execute_batch()` method submits each document as an independent workflow to a `ThreadPoolExecutor`. A `Semaphore(max_parallel_documents=20)` bounds total concurrency, preventing memory exhaustion under burst load.

```
Batch of N documents
    │
    ├── doc-0 ──→ ThreadPool Worker 1 ──→ full DAG execution
    ├── doc-1 ──→ ThreadPool Worker 2 ──→ full DAG execution
    ├── ...
    └── doc-N ──→ ThreadPool Worker N ──→ full DAG execution
```

At `max_parallel_documents=20` and a P95 latency of 15s/document, expected throughput:
```
(20 workers × 3600s) / 15s = 4,800 docs/hr per executor instance
```
Two executor instances → 9,600/hr, comfortably beyond the 5,000/hr peak.

### Step-Level Parallelism
Within a single document workflow, the execution engine identifies **independent branches** — steps whose dependencies are all already satisfied — and submits them to the pool simultaneously. A second `Semaphore(max_parallel_steps=10)` limits total concurrent step executions across all workflows to prevent thread starvation.

Example: after `enrich` completes, both `publish` and `publish_for_review` become eligible. They are submitted together; only one will run (the other is skipped via condition predicate), but the pattern naturally handles true multi-branch parallelism.

### Rate Limiting
`TokenBucketRateLimiter` (thread-safe, monotonic refill):
- Configured at 10 tokens/second, burst capacity of 10.
- Any step marked `rate_limited=True` calls `acquire()` before execution.
- If the bucket is empty, the calling thread sleeps for exactly the deficit time — no busy-polling.
- In production, this is backed by a **Redis atomic INCRBY + EXPIRE** for distributed rate limiting across worker pods.

```
Token Bucket:  capacity=10, refill=10/s
    ┌──────────────────────────┐
    │  ●●●●●●●●●●  (full)      │  → immediate acquire
    │  ●●●●○○○○○○  (half)      │  → immediate acquire
    │  ○○○○○○○○○○  (empty)     │  → sleep(1/rate) seconds
    └──────────────────────────┘
```

---

## Execution Semantics

### Fan-Out (Scatter)
A step with `fan_out_from="parse"` receives the **list** produced by the `parse` step and distributes each item to independent subtask executions. An inner `ThreadPoolExecutor` handles the scatter:
```python
# e.g. multi-page document: parse returns [page_1_bytes, page_2_bytes, ...]
# extract_page step fans out over each page in parallel
```

### Fan-In (Gather / Reduce)
After fan-out completes, all subtask outputs are collected into a single list. The next step in the DAG receives this aggregated list as its input, acting as the reduce/join node.

```
extract_page (fan-out)
    ├── page-0 ──→ LLM extraction
    ├── page-1 ──→ LLM extraction
    └── page-2 ──→ LLM extraction
          ↓
merge_pages (fan-in) ← receives [result_0, result_1, result_2]
```

### Conditional Routing
Each step can specify a `condition: Callable[[context], bool]`. Before the step executes:
- If `condition(context) == True` → execute normally.
- If `condition(context) == False` → step is immediately marked `SKIPPED`; downstream steps that depend only on this step are also skipped transitively.

This enables clean `if/else` branching without modifying the graph structure:
```python
publish       condition=lambda ctx: not ctx["enrich"]["needs_review"]
publish_for_review  condition=lambda ctx: ctx["enrich"]["needs_review"]
```

---

## Failure Handling

### Per-Step Retry
Each `StepConfig` carries a `RetryConfig`:
```python
RetryConfig(max_attempts=3, base_delay=1.0, max_delay=30.0)
```
Retry uses **full jitter** (random uniform between 0 and `base_delay × 2^attempt`, capped at `max_delay`):
```
delay = min(uniform(0, 1.0 × 2^attempt), 30.0)
```
Full jitter is preferred over equal jitter to avoid the thundering-herd problem when many steps retry simultaneously after a shared dependency fails.

### Workflow Timeout
A `deadline = monotonic() + workflow_timeout_seconds` is set at the start of each workflow. The execution loop polls against this deadline every 1 second. If exceeded:
1. All pending futures are cancelled.
2. `WorkflowTimeoutError` is raised.
3. `WorkflowResult.status` is set to `TIMED_OUT`.

The default timeout is **300 seconds (5 minutes)**, matching the batch processing SLA.

### Partial Completion & Checkpointing
The `CheckpointStore` saves each step's `StepResult` immediately after completion. On a worker crash:
1. A watchdog detects the in-progress workflow via the store TTL.
2. A new worker calls `executor.execute(..., resume=True)`.
3. Already-completed steps are loaded from the checkpoint and their outputs injected into context.
4. Execution continues from the first incomplete step — **zero re-work**.

In production, `CheckpointStore` is backed by **Redis HSET** with a 90-minute TTL:
```
HSET workflow:{workflow_id} step:{step_name} {serialised_StepResult}
EXPIRE workflow:{workflow_id} 5400
```

### Dead-Letter Handling
If a workflow exceeds `max_attempts` for a critical step and has no further retries, the workflow is marked `FAILED` and the document ID is written to a dead-letter queue (Redis list or SQS DLQ) for manual investigation. Monitoring alerts fire at >1% dead-letter rate (see Part D SLA definitions).

---

## Infrastructure Integration

| Component | Usage in Workflow Engine |
|---|---|
| **Redis** | Distributed rate-limit counters, checkpoint store for mid-workflow recovery, distributed idempotency keys with TTL |
| **PostgreSQL** | Long-term audit trail persistence, document metadata, workflow execution history for compliance reporting |
| **Groq (LLM)** | Invoked inside the `extract` step via `ExtractionModule._llm_extract()`, rate-limited via `TokenBucketRateLimiter` |
| **ThreadPoolExecutor** | Step-level and document-level parallelism with Python's GIL releasing for I/O-bound LLM/API work |

---

## Throughput Analysis

| Parameter | Value |
|---|---|
| `max_parallel_documents` | 20 |
| `max_parallel_steps` | 10 |
| Avg step latency (LLM) | ~3s |
| P95 document latency | <30s (6 sequential steps × ~5s) |
| Docs/hr per executor | 20 × 3600 / 30 = **2,400** |
| Executor instances needed for 5k/hr | **3** (with headroom) |
| Batch SLA (100 docs) | ~100/20 × 30s = **150s < 5 min ✅** |
