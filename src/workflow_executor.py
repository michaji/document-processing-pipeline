"""
workflow_executor.py
====================
Production-grade DAG-based workflow executor for document processing.

Key capabilities:
  - Full DAG validation with cycle detection (Kahn's algorithm)
  - True parallel execution of independent branches via ThreadPoolExecutor
  - Step-level and document-level concurrency control (semaphores)
  - Token-bucket rate limiter for external API calls (e.g. Groq/LLM)
  - Conditional routing predicates on edges
  - Fan-out / fan-in semantics (map → reduce)
  - Workflow-level SLA timeout (5-min default)
  - Per-step retry with exponential backoff + full jitter
  - Checkpoint store for partial-completion recovery
  - Full integration with ExtractionModule from Part A
  - Redis-backed checkpoint store (falls back to in-memory)
"""

import inspect
import json
import logging
import os
import random
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed, Future, wait, FIRST_EXCEPTION
from datetime import datetime, timezone

from src.infrastructure import redis_client
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Enums & Dataclasses
# ─────────────────────────────────────────────────────────────────────────────

class WorkflowStatus(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    TIMED_OUT = "TIMED_OUT"
    PARTIALLY_COMPLETED = "PARTIALLY_COMPLETED"


class StepStatus(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"


@dataclass
class RetryConfig:
    max_attempts: int = 3
    base_delay: float = 1.0
    max_delay: float = 30.0


@dataclass
class StepConfig:
    """Configuration for a single DAG node / workflow step."""
    name: str
    func: Callable
    inputs: List[str] = field(default_factory=list)          # upstream step names
    retry: RetryConfig = field(default_factory=RetryConfig)
    timeout_seconds: Optional[float] = None
    condition: Optional[Callable[[Dict[str, Any]], bool]] = None  # conditional routing
    # Fan-out: if set, this step maps over a list produced by `fan_out_from`
    fan_out_from: Optional[str] = None
    # Rate-limited: if True, acquires a token before executing
    rate_limited: bool = False


@dataclass
class StepResult:
    step_name: str
    status: StepStatus
    output: Any = None
    error: Optional[str] = None
    attempts: int = 0
    started_at: Optional[float] = None
    finished_at: Optional[float] = None

    @property
    def duration_seconds(self) -> Optional[float]:
        if self.started_at and self.finished_at:
            return round(self.finished_at - self.started_at, 3)
        return None


@dataclass
class WorkflowResult:
    workflow_id: str
    status: WorkflowStatus
    step_results: Dict[str, StepResult] = field(default_factory=dict)
    started_at: Optional[float] = None
    finished_at: Optional[float] = None
    error: Optional[str] = None

    @property
    def duration_seconds(self) -> Optional[float]:
        if self.started_at and self.finished_at:
            return round(self.finished_at - self.started_at, 3)
        return None

    @property
    def completed_steps(self) -> List[str]:
        return [n for n, r in self.step_results.items() if r.status == StepStatus.COMPLETED]

    @property
    def failed_steps(self) -> List[str]:
        return [n for n, r in self.step_results.items() if r.status == StepStatus.FAILED]


# ─────────────────────────────────────────────────────────────────────────────
# Exceptions
# ─────────────────────────────────────────────────────────────────────────────

class CycleError(Exception):
    """Raised when the DAG contains a cycle."""

class WorkflowTimeoutError(Exception):
    """Raised when the overall workflow exceeds its SLA timeout."""

class StepConditionNotMet(Exception):
    """Raised when a step's condition predicate evaluates to False — step is skipped."""


# ─────────────────────────────────────────────────────────────────────────────
# Token-Bucket Rate Limiter
# ─────────────────────────────────────────────────────────────────────────────

class TokenBucketRateLimiter:
    """
    Thread-safe token bucket rate limiter.
    Allows `rate` requests per second with a burst of `burst` tokens.
    Callers block via `acquire()` until a token is available.
    """
    def __init__(self, rate: float = 10.0, burst: int = 10):
        self._rate = rate          # tokens added per second
        self._burst = burst        # maximum token bucket size
        self._tokens = float(burst)
        self._last_refill = time.monotonic()
        self._lock = threading.Lock()

    def acquire(self, tokens: int = 1) -> float:
        """Block until `tokens` tokens are available. Returns wait time."""
        with self._lock:
            now = time.monotonic()
            elapsed = now - self._last_refill
            self._tokens = min(self._burst, self._tokens + elapsed * self._rate)
            self._last_refill = now

            if self._tokens >= tokens:
                self._tokens -= tokens
                return 0.0
            else:
                deficit = tokens - self._tokens
                wait_time = deficit / self._rate
                time.sleep(wait_time)
                self._tokens = 0.0
                return wait_time


# ─────────────────────────────────────────────────────────────────────────────
# Checkpoint Store (in-memory; swap backend for Redis in production)
# ─────────────────────────────────────────────────────────────────────────────

class CheckpointStore:
    """
    Persists intermediate step results so a crashed workflow can resume.
    
    Production swap: replace _store with Redis HSET/HGET calls.
    Key structure: workflow_id → {step_name: StepResult}
    """
    def __init__(self):
        self._store: Dict[str, Dict[str, StepResult]] = {}
        self._lock = threading.Lock()
        self.redis = redis_client
        self.ttl = int(os.getenv("REDIS_TTL_SECONDS", "7776000"))

    def _serialize(self, result: StepResult) -> str:
        data = {
            "step_name": result.step_name,
            "status": result.status.value,
            "output": result.output,
            "error": result.error,
            "attempts": result.attempts,
            "started_at": result.started_at,
            "finished_at": result.finished_at,
        }
        return json.dumps(data, default=str)
        
    def _deserialize(self, data_str: str) -> StepResult:
        data = json.loads(data_str)
        return StepResult(
            step_name=data["step_name"],
            status=StepStatus(data["status"]),
            output=data["output"],
            error=data["error"],
            attempts=data["attempts"],
            started_at=data["started_at"],
            finished_at=data["finished_at"],
        )

    def save(self, workflow_id: str, step_name: str, result: StepResult):
        if self.redis:
            key = f"checkpoint:{workflow_id}"
            self.redis.hset(key, step_name, self._serialize(result))
            self.redis.expire(key, self.ttl)
            return

        with self._lock:
            if workflow_id not in self._store:
                self._store[workflow_id] = {}
            self._store[workflow_id][step_name] = result

    def load(self, workflow_id: str) -> Dict[str, StepResult]:
        if self.redis:
            key = f"checkpoint:{workflow_id}"
            raw = self.redis.hgetall(key)
            return {step: self._deserialize(data) for step, data in raw.items()}

        with self._lock:
            return dict(self._store.get(workflow_id, {}))

    def get_step(self, workflow_id: str, step_name: str) -> Optional[StepResult]:
        if self.redis:
            key = f"checkpoint:{workflow_id}"
            val = self.redis.hget(key, step_name)
            if val:
                return self._deserialize(val)
            return None

        with self._lock:
            return self._store.get(workflow_id, {}).get(step_name)

    def clear(self, workflow_id: str):
        if self.redis:
            key = f"checkpoint:{workflow_id}"
            self.redis.delete(key)
            return

        with self._lock:
            self._store.pop(workflow_id, None)


# Shared singletons (one per process; production would be Redis-backed)
_checkpoint_store = CheckpointStore()


# ─────────────────────────────────────────────────────────────────────────────
# DAG Builder
# ─────────────────────────────────────────────────────────────────────────────

class DAGWorkflow:
    """
    Build and validate a Directed Acyclic Graph of processing steps.
    
    Usage:
        wf = DAGWorkflow("invoice-pipeline")
        wf.add_step(StepConfig("parse",    func=parse_fn))
        wf.add_step(StepConfig("extract",  func=extract_fn,  inputs=["parse"]))
        wf.add_step(StepConfig("validate", func=validate_fn, inputs=["extract"]))
        wf.add_step(StepConfig("publish",  func=publish_fn,  inputs=["validate"]))
    """
    def __init__(self, name: str):
        self.name = name
        self._steps: Dict[str, StepConfig] = {}

    def add_step(self, config: StepConfig) -> "DAGWorkflow":
        if config.name in self._steps:
            raise ValueError(f"Step '{config.name}' already registered in workflow '{self.name}'")
        self._steps[config.name] = config
        return self

    def validate(self) -> List[str]:
        """
        Validates the DAG:
        1. All declared inputs reference existing steps.
        2. No cycles (Kahn's topological sort algorithm).
        Returns ordered list of step names (topological order).
        Raises CycleError or ValueError on invalid graph.
        """
        # Check all inputs reference existing steps
        for step_name, step in self._steps.items():
            for inp in step.inputs:
                if inp not in self._steps:
                    raise ValueError(
                        f"Step '{step_name}' declares input '{inp}' which is not a registered step."
                    )

        # Kahn's algorithm — cycle detection + topological order
        in_degree: Dict[str, int] = {name: 0 for name in self._steps}
        children: Dict[str, List[str]] = {name: [] for name in self._steps}

        for step_name, step in self._steps.items():
            for inp in step.inputs:
                in_degree[step_name] += 1
                children[inp].append(step_name)

        queue = [name for name, deg in in_degree.items() if deg == 0]
        topo_order: List[str] = []

        while queue:
            node = queue.pop(0)
            topo_order.append(node)
            for child in children[node]:
                in_degree[child] -= 1
                if in_degree[child] == 0:
                    queue.append(child)

        if len(topo_order) != len(self._steps):
            # Find the cycle members
            cycle_members = [n for n in self._steps if n not in topo_order]
            raise CycleError(f"Cycle detected in DAG involving steps: {cycle_members}")

        return topo_order

    @property
    def steps(self) -> Dict[str, StepConfig]:
        return self._steps


# ─────────────────────────────────────────────────────────────────────────────
# Workflow Executor
# ─────────────────────────────────────────────────────────────────────────────

class WorkflowExecutor:
    """
    Executes a DAGWorkflow with:
      - True parallel execution of independent branches
      - Semaphore-controlled concurrency (document-level + step-level)
      - Token-bucket rate limiting for rate-sensitive steps
      - Fan-out / fan-in: scatter work across items, then reduce
      - Conditional routing: skip steps based on upstream outputs
      - Per-step retry with full-jitter exponential backoff
      - Workflow SLA timeout
      - Checkpoint-based partial recovery
    """

    def __init__(
        self,
        max_parallel_steps: int = 10,
        max_parallel_documents: int = 20,
        workflow_timeout_seconds: float = 300.0,
        rate_limiter: Optional[TokenBucketRateLimiter] = None,
        checkpoint_store: Optional[CheckpointStore] = None,
    ):
        # Semaphore limiting simultaneous steps across ALL running workflows
        self._step_semaphore = threading.Semaphore(max_parallel_steps)
        # Semaphore limiting simultaneous document workflows
        self._doc_semaphore = threading.Semaphore(max_parallel_documents)
        self._workflow_timeout = workflow_timeout_seconds
        self._rate_limiter = rate_limiter or TokenBucketRateLimiter(rate=10.0, burst=10)
        self._checkpoint = checkpoint_store or _checkpoint_store

    # ── Public API ────────────────────────────────────────────────────────────

    def execute(
        self,
        workflow: DAGWorkflow,
        initial_context: Dict[str, Any] = None,
        workflow_id: Optional[str] = None,
        resume: bool = False,
    ) -> WorkflowResult:
        """
        Execute a complete DAG workflow.

        Args:
            workflow:         Validated DAGWorkflow instance.
            initial_context:  Dict of pre-seeded step outputs (e.g. parsed document bytes).
            workflow_id:      Stable ID for idempotent tracking / resume.
            resume:           If True, load checkpointed step results and skip already-completed steps.

        Returns:
            WorkflowResult with per-step outcomes and overall status.
        """
        workflow_id = workflow_id or str(uuid.uuid4())
        topo_order = workflow.validate()     # raises CycleError / ValueError if invalid
        context: Dict[str, Any] = dict(initial_context or {})
        step_results: Dict[str, StepResult] = {}

        # Load checkpointed results if resuming
        if resume:
            checkpointed = self._checkpoint.load(workflow_id)
            for step_name, sr in checkpointed.items():
                if sr.status == StepStatus.COMPLETED:
                    step_results[step_name] = sr
                    context[step_name] = sr.output
                    logger.info("[%s] Resumed step '%s' from checkpoint.", workflow_id, step_name)

        result = WorkflowResult(
            workflow_id=workflow_id,
            status=WorkflowStatus.RUNNING,
            step_results=step_results,
            started_at=time.monotonic(),
        )

        with self._doc_semaphore:
            try:
                self._execute_dag(
                    workflow=workflow,
                    topo_order=topo_order,
                    context=context,
                    step_results=step_results,
                    workflow_id=workflow_id,
                )
                failed = [n for n, r in step_results.items() if r.status == StepStatus.FAILED]
                if failed:
                    result.status = WorkflowStatus.PARTIALLY_COMPLETED
                    result.error = f"Steps failed: {failed}"
                else:
                    result.status = WorkflowStatus.COMPLETED

            except WorkflowTimeoutError as exc:
                result.status = WorkflowStatus.TIMED_OUT
                result.error = str(exc)
                logger.error("[%s] Workflow timed out: %s", workflow_id, exc)

            except Exception as exc:
                result.status = WorkflowStatus.FAILED
                result.error = str(exc)
                logger.error("[%s] Workflow failed: %s", workflow_id, exc)

            finally:
                result.finished_at = time.monotonic()
                result.step_results = step_results
                logger.info(
                    "[%s] Workflow %s in %.2fs | steps done=%s failed=%s",
                    workflow_id,
                    result.status,
                    result.duration_seconds or 0,
                    len(result.completed_steps),
                    len(result.failed_steps),
                )

        return result

    def execute_batch(
        self,
        workflow_factory: Callable[[], DAGWorkflow],
        documents: List[Dict[str, Any]],
        workflow_id_prefix: str = "batch",
    ) -> List[WorkflowResult]:
        """
        Execute the same workflow over a batch of documents concurrently.
        Document-level parallelism controlled by _doc_semaphore.
        Returns list of WorkflowResult in order of completion.
        """
        results: List[WorkflowResult] = []
        results_lock = threading.Lock()

        with ThreadPoolExecutor(thread_name_prefix="doc-worker") as pool:
            futures: Dict[Future, str] = {}
            for i, doc_context in enumerate(documents):
                wf_id = f"{workflow_id_prefix}-{i}"
                future = pool.submit(
                    self.execute,
                    workflow=workflow_factory(),
                    initial_context=doc_context,
                    workflow_id=wf_id,
                )
                futures[future] = wf_id

            for future in as_completed(futures):
                wf_id = futures[future]
                try:
                    wf_result = future.result()
                    with results_lock:
                        results.append(wf_result)
                except Exception as exc:
                    logger.error("Document workflow %s raised: %s", wf_id, exc)
                    with results_lock:
                        results.append(WorkflowResult(
                            workflow_id=wf_id,
                            status=WorkflowStatus.FAILED,
                            error=str(exc),
                        ))

        return results

    # ── Internal DAG execution ────────────────────────────────────────────────

    def _execute_dag(
        self,
        workflow: DAGWorkflow,
        topo_order: List[str],
        context: Dict[str, Any],
        step_results: Dict[str, StepResult],
        workflow_id: str,
    ):
        """
        Core execution loop. Runs levels of the DAG in parallel.
        Steps whose inputs are all satisfied are submitted to the thread pool
        together, providing true branch-level parallelism.
        """
        deadline = time.monotonic() + self._workflow_timeout
        steps = workflow.steps

        # Track which steps are done (completed OR skipped OR pre-seeded from initial_context)
        done: Set[str] = set(step_results.keys())  # pre-loaded from checkpoint
        done.update(context.keys())  # initial_context keys count as satisfied dependencies
        context_lock = threading.Lock()

        with ThreadPoolExecutor(thread_name_prefix="step-worker") as pool:
            pending_futures: Dict[Future, str] = {}

            def submit_ready_steps():
                """Find steps whose dependencies are all satisfied and submit them."""
                for step_name in topo_order:
                    if step_name in done:
                        continue
                    step = steps[step_name]
                    deps_satisfied = all(dep in done for dep in step.inputs)
                    already_submitted = any(
                        v == step_name for v in pending_futures.values()
                    )
                    if deps_satisfied and not already_submitted:
                        f = pool.submit(
                            self._execute_step,
                            step=step,
                            context=context,
                            context_lock=context_lock,
                            workflow_id=workflow_id,
                            deadline=deadline,
                        )
                        pending_futures[f] = step_name

            # Initial submission
            submit_ready_steps()

            while pending_futures:
                if time.monotonic() > deadline:
                    # Cancel all pending futures
                    for f in list(pending_futures):
                        f.cancel()
                    raise WorkflowTimeoutError(
                        f"Workflow '{workflow_id}' exceeded {self._workflow_timeout}s SLA timeout."
                    )

                # Wait for at least one future to finish
                done_futures, _ = wait(
                    list(pending_futures.keys()),
                    timeout=1.0,           # poll every second to check deadline
                    return_when=FIRST_EXCEPTION,
                )

                for future in done_futures:
                    step_name = pending_futures.pop(future)
                    try:
                        step_result: StepResult = future.result()
                    except Exception as exc:
                        step_result = StepResult(
                            step_name=step_name,
                            status=StepStatus.FAILED,
                            error=str(exc),
                        )
                        logger.error("[%s] Step '%s' raised unexpectedly: %s", workflow_id, step_name, exc)

                    step_results[step_name] = step_result
                    self._checkpoint.save(workflow_id, step_name, step_result)
                    done.add(step_name)

                    if step_result.status == StepStatus.COMPLETED:
                        with context_lock:
                            context[step_name] = step_result.output

                    # After each completion, check if new steps are now unblocked
                    submit_ready_steps()

    def _execute_step(
        self,
        step: StepConfig,
        context: Dict[str, Any],
        context_lock: threading.Lock,
        workflow_id: str,
        deadline: float,
    ) -> StepResult:
        """
        Execute a single step, handling:
          - Condition check (skip if predicate is False)
          - Rate limiting (if step.rate_limited)
          - Fan-out / fan-in (if step.fan_out_from is set)
          - Semaphore (limits simultaneous steps globally)
          - Retry with full-jitter backoff
        """
        result = StepResult(step_name=step.name, status=StepStatus.PENDING)

        # ── Condition check ───────────────────────────────────────────────────
        if step.condition is not None:
            with context_lock:
                ctx_snapshot = dict(context)
            try:
                if not step.condition(ctx_snapshot):
                    logger.info("[%s] Step '%s' skipped — condition not met.", workflow_id, step.name)
                    result.status = StepStatus.SKIPPED
                    return result
            except Exception as exc:
                logger.warning("[%s] Step '%s' condition raised — skipping: %s", workflow_id, step.name, exc)
                result.status = StepStatus.SKIPPED
                return result

        # ── Rate limiter ──────────────────────────────────────────────────────
        if step.rate_limited:
            wait_time = self._rate_limiter.acquire()
            if wait_time > 0:
                logger.debug("[%s] Step '%s' rate-limited, waited %.2fs.", workflow_id, step.name, wait_time)

        # ── Semaphore acquisition ─────────────────────────────────────────────
        with self._step_semaphore:
            result.started_at = time.monotonic()
            result.status = StepStatus.RUNNING

            # ── Collect inputs from context using introspection ────────────────
            # Pass ALL context values that match the function's parameter names.
            # This allows initial_context keys (e.g. document_id, payload) to flow
            # into root steps that declare no DAG inputs but need those values.
            with context_lock:
                full_context = dict(context)
            try:
                sig = inspect.signature(step.func)
                kwargs = {
                    k: full_context[k]
                    for k in sig.parameters
                    if k in full_context
                }
            except (ValueError, TypeError):
                # Fallback: only named DAG inputs
                kwargs = {inp: full_context[inp] for inp in step.inputs if inp in full_context}

            # ── Fan-out / fan-in ──────────────────────────────────────────────
            if step.fan_out_from:
                output = self._execute_fan_out(step, full_context, workflow_id, deadline)
                result.output = output
                result.status = StepStatus.COMPLETED
                result.finished_at = time.monotonic()
                return result

            # ── Standard step with retry ──────────────────────────────────────
            last_exc: Optional[Exception] = None
            for attempt in range(step.retry.max_attempts):
                if time.monotonic() > deadline:
                    raise WorkflowTimeoutError("Deadline exceeded mid-step.")
                try:
                    output = step.func(**kwargs)
                    result.output = output
                    result.status = StepStatus.COMPLETED
                    result.attempts = attempt + 1
                    result.finished_at = time.monotonic()
                    logger.info(
                        "[%s] Step '%s' completed in %.2fs (attempt %d).",
                        workflow_id, step.name,
                        result.duration_seconds or 0,
                        attempt + 1,
                    )
                    return result

                except Exception as exc:
                    last_exc = exc
                    if attempt < step.retry.max_attempts - 1:
                        delay = min(
                            random.uniform(0, step.retry.base_delay * (2 ** attempt)),
                            step.retry.max_delay,
                        )
                        logger.warning(
                            "[%s] Step '%s' attempt %d/%d failed (%s). Retrying in %.2fs.",
                            workflow_id, step.name, attempt + 1, step.retry.max_attempts, exc, delay,
                        )
                        time.sleep(delay)
                    else:
                        logger.error(
                            "[%s] Step '%s' FAILED after %d attempts: %s",
                            workflow_id, step.name, attempt + 1, exc,
                        )

            result.status = StepStatus.FAILED
            result.error = str(last_exc)
            result.attempts = step.retry.max_attempts
            result.finished_at = time.monotonic()
            return result

    def _execute_fan_out(
        self,
        step: StepConfig,
        kwargs: Dict[str, Any],
        workflow_id: str,
        deadline: float,
    ) -> List[Any]:
        """
        Fan-out: apply `step.func` to each item in the list produced by
        the `step.fan_out_from` upstream step, then collect results (fan-in).
        Uses a nested ThreadPoolExecutor bounded by the step semaphore slots.
        """
        source_output = kwargs.get(step.fan_out_from, [])
        # Also check full context via kwargs—fan_out_from may be a step name whose output is a list
        if not isinstance(source_output, list):
            source_output = [source_output]

        outputs: List[Any] = []
        errors: List[str] = []
        outputs_lock = threading.Lock()

        # Build extra kwargs for each subtask (everything except the fan_out source)
        try:
            sig = inspect.signature(step.func)
            extra_kwargs = {
                k: v for k, v in kwargs.items()
                if k != step.fan_out_from and k in sig.parameters
            }
        except (ValueError, TypeError):
            extra_kwargs = {k: v for k, v in kwargs.items() if k != step.fan_out_from}

        with ThreadPoolExecutor(thread_name_prefix=f"fanout-{step.name}") as fanout_pool:
            item_futures = {
                fanout_pool.submit(step.func, item=item, **extra_kwargs): i
                for i, item in enumerate(source_output)
            }
            for future in as_completed(item_futures):
                idx = item_futures[future]
                try:
                    output = future.result()
                    with outputs_lock:
                        outputs.append(output)
                except Exception as exc:
                    logger.error("[%s] Fan-out step '%s' item %d failed: %s", workflow_id, step.name, idx, exc)
                    errors.append(str(exc))

        if errors:
            logger.warning("[%s] Fan-out '%s' had %d errors: %s", workflow_id, step.name, len(errors), errors)

        return outputs


# ─────────────────────────────────────────────────────────────────────────────
# Pre-built Document Processing Pipeline
# ─────────────────────────────────────────────────────────────────────────────

def build_invoice_pipeline(extraction_module=None) -> DAGWorkflow:
    """
    Factory that returns a pre-configured DAGWorkflow for invoice processing.
    Integrates directly with ExtractionModule from Part A.

    Pipeline stages:
      parse → extract → validate_confidence → [review_route OR publish]

    Usage:
        from src.extraction_module import ExtractionModule
        module = ExtractionModule()
        workflow = build_invoice_pipeline(extraction_module=module)
        executor = WorkflowExecutor()
        result = executor.execute(
            workflow,
            initial_context={"document_id": "INV-001", "payload": pdf_bytes}
        )
    """
    from src.extraction_module import ExtractionModule, ProcessingStatus

    _module = extraction_module or ExtractionModule()

    # ── Step functions ────────────────────────────────────────────────────────

    def parse(document_id: str, payload: bytes) -> Dict[str, Any]:
        """Lightweight preprocessing — validate and fingerprint the document."""
        import hashlib
        if not payload:
            raise ValueError("Empty document payload")
        return {
            "document_id": document_id,
            "payload": payload,
            "size_bytes": len(payload),
            "content_hash": hashlib.sha256(payload).hexdigest(),
        }

    def extract(parse: Dict[str, Any]) -> Dict[str, Any]:
        """Call ExtractionModule (Part A) — full LLM extraction with retry + circuit breaker."""
        status, output = _module.process_document(
            document_id=parse["document_id"],
            payload=parse["payload"],
            document_type="invoice",
        )
        return {"extraction_status": str(status), "data": output}

    def enrich(extract: Dict[str, Any]) -> Dict[str, Any]:
        """Optional enrichment step — e.g., vendor lookup, currency normalisation."""
        data = extract["data"]
        enriched = {
            **data,
            "enriched": True,
            "enriched_at": time.time(),
        }
        return enriched

    def publish(enrich: Dict[str, Any]) -> Dict[str, str]:
        """Write to downstream systems — outputs already written by ExtractionModule."""
        doc_id = enrich.get("document_id", "unknown")
        logger.info("Publishing document %s to downstream sinks.", doc_id)
        return {"published": True, "document_id": doc_id}

    def publish_for_review(enrich: Dict[str, Any]) -> Dict[str, str]:
        """Route to human review queue instead of direct publish."""
        doc_id = enrich.get("document_id", "unknown")
        logger.info("Document %s routed to REVIEW queue.", doc_id)
        return {"published": False, "review_queued": True, "document_id": doc_id}

    # ── Conditions ────────────────────────────────────────────────────────────

    def needs_review(context: Dict[str, Any]) -> bool:
        enrich_result = context.get("enrich", {})
        return enrich_result.get("needs_review", False)

    def does_not_need_review(context: Dict[str, Any]) -> bool:
        return not needs_review(context)

    # ── Build DAG ─────────────────────────────────────────────────────────────

    wf = DAGWorkflow("invoice-pipeline")
    wf.add_step(StepConfig(
        name="parse",
        func=parse,
        rate_limited=False,
    ))
    wf.add_step(StepConfig(
        name="extract",
        func=extract,
        inputs=["parse"],
        rate_limited=True,   # LLM calls are rate-limited
        retry=RetryConfig(max_attempts=3, base_delay=1.0, max_delay=30.0),
    ))
    wf.add_step(StepConfig(
        name="enrich",
        func=enrich,
        inputs=["extract"],
    ))
    wf.add_step(StepConfig(
        name="publish",
        func=publish,
        inputs=["enrich"],
        condition=does_not_need_review,
    ))
    wf.add_step(StepConfig(
        name="publish_for_review",
        func=publish_for_review,
        inputs=["enrich"],
        condition=needs_review,
    ))

    return wf
