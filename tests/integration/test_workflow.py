"""
tests/integration/test_workflow.py
Integration tests for the WorkflowExecutor + DAGWorkflow + ExtractionModule.

Covers:
  - Topological ordering and correct execution sequence
  - True parallel branch execution
  - Conditional routing (skip logic)
  - Fan-out / fan-in semantics
  - Step retry on transient failure
  - Workflow SLA timeout enforcement
  - Partial completion and checkpoint resume
  - Rate limiter token acquisition
  - Batch document processing (document-level parallelism)
  - Full end-to-end invoice pipeline (Part A + Part B integrated)
"""
import threading
import time
import pytest

from src.workflow_executor import (
    DAGWorkflow,
    WorkflowExecutor,
    WorkflowStatus,
    StepStatus,
    StepConfig,
    RetryConfig,
    TokenBucketRateLimiter,
    CheckpointStore,
    CycleError,
    WorkflowTimeoutError,
    build_invoice_pipeline,
)


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def make_executor(**kwargs) -> WorkflowExecutor:
    """Fresh executor with a fast timeout by default for tests."""
    defaults = dict(
        max_parallel_steps=10,
        max_parallel_documents=5,
        workflow_timeout_seconds=30.0,
    )
    defaults.update(kwargs)
    return WorkflowExecutor(**defaults)


def simple_step(value):
    """Returns whatever is passed in — used for pass-through steps."""
    return value


# ─────────────────────────────────────────────────────────────────────────────
# DAG Validation
# ─────────────────────────────────────────────────────────────────────────────

class TestDAGValidation:

    def test_valid_linear_dag(self):
        wf = DAGWorkflow("linear")
        wf.add_step(StepConfig("a", func=lambda: 1))
        wf.add_step(StepConfig("b", func=lambda a: a + 1, inputs=["a"]))
        wf.add_step(StepConfig("c", func=lambda b: b + 1, inputs=["b"]))
        order = wf.validate()
        assert order == ["a", "b", "c"]

    def test_cycle_detection_raises(self):
        wf = DAGWorkflow("cycle")
        wf.add_step(StepConfig("a", func=lambda b: b, inputs=["b"]))
        wf.add_step(StepConfig("b", func=lambda a: a, inputs=["a"]))
        with pytest.raises(CycleError):
            wf.validate()

    def test_missing_input_raises(self):
        wf = DAGWorkflow("missing")
        wf.add_step(StepConfig("a", func=lambda x: x, inputs=["nonexistent"]))
        with pytest.raises(ValueError, match="nonexistent"):
            wf.validate()

    def test_duplicate_step_raises(self):
        wf = DAGWorkflow("dup")
        wf.add_step(StepConfig("a", func=lambda: 1))
        with pytest.raises(ValueError, match="already registered"):
            wf.add_step(StepConfig("a", func=lambda: 2))

    def test_diamond_dag_valid(self):
        """B and C both depend on A; D depends on B and C."""
        wf = DAGWorkflow("diamond")
        wf.add_step(StepConfig("a", func=lambda: 1))
        wf.add_step(StepConfig("b", func=lambda a: a, inputs=["a"]))
        wf.add_step(StepConfig("c", func=lambda a: a, inputs=["a"]))
        wf.add_step(StepConfig("d", func=lambda b, c: b + c, inputs=["b", "c"]))
        order = wf.validate()
        assert order.index("a") < order.index("b")
        assert order.index("a") < order.index("c")
        assert order.index("b") < order.index("d")
        assert order.index("c") < order.index("d")


# ─────────────────────────────────────────────────────────────────────────────
# Execution Order
# ─────────────────────────────────────────────────────────────────────────────

class TestExecutionOrder:

    def test_linear_pipeline_executes_in_order(self):
        order_log = []

        wf = DAGWorkflow("order")
        wf.add_step(StepConfig("a", func=lambda: order_log.append("a") or 1))
        wf.add_step(StepConfig("b", func=lambda a: order_log.append("b") or a + 1, inputs=["a"]))
        wf.add_step(StepConfig("c", func=lambda b: order_log.append("c") or b + 1, inputs=["b"]))

        result = make_executor().execute(wf)
        assert result.status == WorkflowStatus.COMPLETED
        assert order_log == ["a", "b", "c"]

    def test_step_receives_upstream_output(self):
        wf = DAGWorkflow("outputs")
        wf.add_step(StepConfig("produce", func=lambda: {"key": "value"}))
        wf.add_step(StepConfig("consume", func=lambda produce: produce["key"], inputs=["produce"]))

        result = make_executor().execute(wf)
        assert result.status == WorkflowStatus.COMPLETED
        assert result.step_results["consume"].output == "value"

    def test_initial_context_seeds_steps(self):
        wf = DAGWorkflow("seeded")
        # seed_value comes from initial_context — no need to list as a DAG input
        wf.add_step(StepConfig("use_seed", func=lambda seed_value: seed_value * 2))

        result = make_executor().execute(wf, initial_context={"seed_value": 21})
        assert result.step_results["use_seed"].output == 42


# ─────────────────────────────────────────────────────────────────────────────
# Parallel Execution
# ─────────────────────────────────────────────────────────────────────────────

class TestParallelExecution:

    def test_independent_branches_run_concurrently(self):
        """B and C both depend on A but not on each other — should overlap."""
        thread_log = []

        def slow_b(a):
            thread_log.append(("b_start", threading.current_thread().name))
            time.sleep(0.2)
            thread_log.append(("b_end", threading.current_thread().name))
            return "b"

        def slow_c(a):
            thread_log.append(("c_start", threading.current_thread().name))
            time.sleep(0.2)
            thread_log.append(("c_end", threading.current_thread().name))
            return "c"

        wf = DAGWorkflow("parallel")
        wf.add_step(StepConfig("a", func=lambda: "a"))
        wf.add_step(StepConfig("b", func=slow_b, inputs=["a"]))
        wf.add_step(StepConfig("c", func=slow_c, inputs=["a"]))
        wf.add_step(StepConfig("d", func=lambda b, c: b + c, inputs=["b", "c"]))

        start = time.monotonic()
        result = make_executor().execute(wf)
        elapsed = time.monotonic() - start

        assert result.status == WorkflowStatus.COMPLETED
        assert result.step_results["d"].output == "bc"
        # If sequential: >0.4s. If parallel: ~0.2s. Allow generous margin.
        assert elapsed < 0.35, f"Expected parallel execution but took {elapsed:.2f}s"

    def test_document_batch_concurrency(self):
        """Execute 20 documents concurrently via execute_batch."""
        wf_call_count = {"n": 0}
        count_lock = threading.Lock()

        def counting_step(doc_id: str) -> str:
            time.sleep(0.05)
            with count_lock:
                wf_call_count["n"] += 1
            return doc_id

        def wf_factory():
            wf = DAGWorkflow("batch-doc")
            wf.add_step(StepConfig("counting_step", func=counting_step))
            return wf

        documents = [{"doc_id": f"doc-{i}"} for i in range(20)]
        executor = make_executor(max_parallel_documents=10)
        results = executor.execute_batch(wf_factory, documents)

        assert len(results) == 20
        assert all(r.status == WorkflowStatus.COMPLETED for r in results)
        assert wf_call_count["n"] == 20


# ─────────────────────────────────────────────────────────────────────────────
# Conditional Routing
# ─────────────────────────────────────────────────────────────────────────────

class TestConditionalRouting:

    def _build_routing_workflow(self, flag_value: bool) -> tuple:
        executed = {"publish": False, "review": False}

        def produce():
            return {"needs_review": flag_value}

        def publish_step(produce):
            executed["publish"] = True
            return "published"

        def review_step(produce):
            executed["review"] = True
            return "reviewed"

        wf = DAGWorkflow("routing")
        wf.add_step(StepConfig("produce", func=produce))
        wf.add_step(StepConfig(
            "publish", func=publish_step, inputs=["produce"],
            condition=lambda ctx: not ctx["produce"]["needs_review"],
        ))
        wf.add_step(StepConfig(
            "review", func=review_step, inputs=["produce"],
            condition=lambda ctx: ctx["produce"]["needs_review"],
        ))
        return wf, executed

    def test_condition_true_executes_step(self):
        wf, executed = self._build_routing_workflow(flag_value=False)
        result = make_executor().execute(wf)
        assert executed["publish"] is True
        assert executed["review"] is False
        assert result.step_results["review"].status == StepStatus.SKIPPED

    def test_condition_false_skips_step(self):
        wf, executed = self._build_routing_workflow(flag_value=True)
        result = make_executor().execute(wf)
        assert executed["review"] is True
        assert executed["publish"] is False
        assert result.step_results["publish"].status == StepStatus.SKIPPED


# ─────────────────────────────────────────────────────────────────────────────
# Fan-Out / Fan-In
# ─────────────────────────────────────────────────────────────────────────────

class TestFanOutFanIn:

    def test_fan_out_processes_all_items(self):
        def produce_pages():
            return [f"page-{i}" for i in range(5)]

        def process_page(item: str) -> str:
            return item.upper()

        wf = DAGWorkflow("fanout")
        wf.add_step(StepConfig("produce_pages", func=produce_pages))
        wf.add_step(StepConfig(
            "process_page",
            func=process_page,
            inputs=["produce_pages"],
            fan_out_from="produce_pages",
        ))

        result = make_executor().execute(wf)
        assert result.status == WorkflowStatus.COMPLETED
        fan_out_result = result.step_results["process_page"].output
        assert isinstance(fan_out_result, list)
        assert len(fan_out_result) == 5
        assert all(s == s.upper() for s in fan_out_result)


# ─────────────────────────────────────────────────────────────────────────────
# Retry Logic
# ─────────────────────────────────────────────────────────────────────────────

class TestRetryLogic:

    def test_step_retries_on_transient_failure(self):
        call_count = {"n": 0}

        def flaky_step():
            call_count["n"] += 1
            if call_count["n"] < 3:
                raise ConnectionError("transient")
            return "success"

        wf = DAGWorkflow("retry")
        wf.add_step(StepConfig(
            "flaky", func=flaky_step,
            retry=RetryConfig(max_attempts=3, base_delay=0, max_delay=0),
        ))

        result = make_executor().execute(wf)
        assert result.status == WorkflowStatus.COMPLETED
        assert result.step_results["flaky"].status == StepStatus.COMPLETED
        assert result.step_results["flaky"].attempts == 3

    def test_step_fails_after_exhausting_retries(self):
        def always_fail():
            raise RuntimeError("always fails")

        wf = DAGWorkflow("exhaust")
        wf.add_step(StepConfig(
            "bad_step", func=always_fail,
            retry=RetryConfig(max_attempts=2, base_delay=0, max_delay=0),
        ))

        result = make_executor().execute(wf)
        # Workflow itself completes (partially), step is FAILED
        assert result.step_results["bad_step"].status == StepStatus.FAILED
        assert result.status in (WorkflowStatus.PARTIALLY_COMPLETED, WorkflowStatus.FAILED)


# ─────────────────────────────────────────────────────────────────────────────
# Timeout
# ─────────────────────────────────────────────────────────────────────────────

class TestWorkflowTimeout:

    def test_workflow_times_out(self):
        def slow_step():
            time.sleep(10)  # way longer than timeout
            return "done"

        wf = DAGWorkflow("timeout")
        wf.add_step(StepConfig("slow", func=slow_step, retry=RetryConfig(max_attempts=1, base_delay=0)))

        # Very short timeout
        executor = make_executor(workflow_timeout_seconds=0.5)
        result = executor.execute(wf)
        assert result.status == WorkflowStatus.TIMED_OUT


# ─────────────────────────────────────────────────────────────────────────────
# Checkpoint / Resume
# ─────────────────────────────────────────────────────────────────────────────

class TestCheckpointResume:

    def test_resume_skips_completed_steps(self):
        executed = {"a": 0, "b": 0}

        def step_a():
            executed["a"] += 1
            return "a_result"

        def step_b(a):
            executed["b"] += 1
            return "b_result"

        store = CheckpointStore()
        executor = make_executor(checkpoint_store=store)

        wf = DAGWorkflow("resume")
        wf.add_step(StepConfig("a", func=step_a))
        wf.add_step(StepConfig("b", func=step_b, inputs=["a"]))

        # First run — full execution
        r1 = executor.execute(wf, workflow_id="wf-resume-1")
        assert r1.status == WorkflowStatus.COMPLETED
        assert executed["a"] == 1
        assert executed["b"] == 1

        # Second run with resume=True — step 'a' already checkpointed, should skip
        r2 = executor.execute(wf, workflow_id="wf-resume-1", resume=True)
        assert r2.status == WorkflowStatus.COMPLETED
        # Step 'a' should NOT have been re-executed
        assert executed["a"] == 1
        # Step 'b' is re-executed because it wasn't checkpointed as complete
        # (in real scenario this would also be already complete)


# ─────────────────────────────────────────────────────────────────────────────
# Rate Limiter
# ─────────────────────────────────────────────────────────────────────────────

class TestRateLimiter:

    def test_rate_limiter_allows_burst(self):
        rl = TokenBucketRateLimiter(rate=100.0, burst=10)
        # Should be instantaneous for first 10
        start = time.monotonic()
        for _ in range(10):
            rl.acquire()
        elapsed = time.monotonic() - start
        assert elapsed < 0.1

    def test_rate_limiter_throttles_excess(self):
        rl = TokenBucketRateLimiter(rate=5.0, burst=5)
        # Drain the burst
        for _ in range(5):
            rl.acquire()
        # Next acquire should take ~0.2s (1/5 rate)
        start = time.monotonic()
        rl.acquire()
        elapsed = time.monotonic() - start
        assert elapsed >= 0.15, f"Expected throttling, got {elapsed:.3f}s"


# ─────────────────────────────────────────────────────────────────────────────
# End-to-End: Part A + Part B Integration
# ─────────────────────────────────────────────────────────────────────────────

class TestEndToEnd:

    def test_invoice_pipeline_completes(self, tmp_path):
        from src.extraction_module import ExtractionModule

        module = ExtractionModule(output_dir=str(tmp_path), use_mock_llm=True)
        wf = build_invoice_pipeline(extraction_module=module)

        executor = make_executor(
            workflow_timeout_seconds=60.0,
            rate_limiter=TokenBucketRateLimiter(rate=100.0, burst=100),
        )
        result = executor.execute(wf, initial_context={
            "document_id": "e2e-INV-001",
            "payload": b"sample invoice content for end to end test",
        })

        assert result.status == WorkflowStatus.COMPLETED
        assert "parse" in result.completed_steps
        assert "extract" in result.completed_steps
        assert "enrich" in result.completed_steps
        # either publish or publish_for_review should be complete
        assert (
            "publish" in result.completed_steps
            or "publish_for_review" in result.completed_steps
        )

    def test_batch_invoice_pipeline(self, tmp_path):
        from src.extraction_module import ExtractionModule

        module = ExtractionModule(output_dir=str(tmp_path), use_mock_llm=True)
        executor = make_executor(
            workflow_timeout_seconds=60.0,
            rate_limiter=TokenBucketRateLimiter(rate=100.0, burst=100),
        )

        documents = [
            {"document_id": f"batch-INV-{i}", "payload": f"invoice {i}".encode()}
            for i in range(5)
        ]

        results = executor.execute_batch(
            workflow_factory=lambda: build_invoice_pipeline(extraction_module=module),
            documents=documents,
        )

        assert len(results) == 5
        completed = [r for r in results if r.status == WorkflowStatus.COMPLETED]
        assert len(completed) == 5
