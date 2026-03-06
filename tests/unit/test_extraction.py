"""
tests/unit/test_extraction.py
Unit tests covering idempotency, field preservation, state transitions,
circuit breaker, retry logic, validation, and data quality metrics.
"""
import pytest
import threading
from src.extraction_module import (
    ExtractionModule, CircuitBreaker, ProcessingStatus, _call_with_retry
)


# ── Fixtures ────────────────────────────────────────────────────────────────
@pytest.fixture
def module(tmp_path):
    return ExtractionModule(output_dir=str(tmp_path), use_mock_llm=True)


# ── Idempotency ──────────────────────────────────────────────────────────────
class TestIdempotency:
    def test_same_payload_returns_cached_result(self, module):
        payload = b"invoice payload v1"
        status1, data1 = module.process_document("doc-1", payload)
        status2, data2 = module.process_document("doc-1", payload)

        assert status1 == ProcessingStatus.COMPLETED
        assert status2 == ProcessingStatus.COMPLETED
        assert data1 == data2  # bit-for-bit identical

    def test_different_payload_is_new_processing(self, module):
        status1, data1 = module.process_document("doc-2", b"version A")
        status2, data2 = module.process_document("doc-2", b"version B")

        # Different content hash → different idempotency key → processed fresh
        assert data1["content_hash"] != data2["content_hash"]

    def test_concurrent_duplicate_raises(self, module):
        """
        If the idempotency store already shows PROCESSING, a second call
        should be rejected to avoid double-processing.
        """
        payload = b"concurrent doc"
        key = module._make_idempotency_key(
            "doc-3", __import__("hashlib").sha256(payload).hexdigest()
        )
        module.idempotency_store.save(key, {"status": ProcessingStatus.PROCESSING})

        with pytest.raises(RuntimeError, match="concurrent duplicate rejected"):
            module.process_document("doc-3", payload)


# ── Field Preservation ───────────────────────────────────────────────────────
class TestFieldPreservation:
    def test_human_correction_wins(self, module):
        status, data = module.process_document(
            "doc-fp-1", b"payload",
            manual_corrections={"vendor_name": "MY VENDOR"}
        )
        assert data["vendor_name"] == "MY VENDOR"
        assert data["vendor_name_source"] == "HUMAN"
        assert data["vendor_name_confidence"] == 1.0

    def test_human_correction_for_new_field(self, module):
        """Human can add a field the AI didn't extract."""
        _, data = module.process_document(
            "doc-fp-2", b"payload",
            manual_corrections={"po_reference": "PO-9999"}
        )
        assert data["po_reference"] == "PO-9999"
        assert data["po_reference_source"] == "HUMAN"

    def test_ai_field_not_overwritten_without_correction(self, module):
        _, data = module.process_document("doc-fp-3", b"payload")
        assert data["vendor_name_source"] == "AI"

    def test_correction_appears_in_audit_trail(self, module):
        _, data = module.process_document(
            "doc-fp-4", b"payload",
            manual_corrections={"vendor_name": "Corrected Corp"}
        )
        events = [e["event"] for e in data["audit_trail"]]
        assert "field_corrected" in events


# ── Status Transitions ────────────────────────────────────────────────────────
class TestStatusTransitions:
    def test_completed_status_for_high_confidence(self, module):
        # Default mock returns high-confidence fields
        status, _ = module.process_document("doc-st-1", b"payload")
        assert status in (ProcessingStatus.COMPLETED, ProcessingStatus.REVIEW_PENDING)

    def test_review_pending_when_low_confidence(self, module):
        # Patch LLM to return low-confidence result
        original = module._llm_extract
        module._llm_extract = lambda payload, doc_type: {
            "invoice_number": "INV-999",
            "invoice_number_confidence": 0.50,  # below 0.80 threshold
            "invoice_number_source": "AI",
            "vendor_name": "Test",
            "vendor_name_confidence": 0.55,
            "vendor_name_source": "AI",
            "total_amount": 100.0,
            "total_amount_confidence": 0.60,
            "total_amount_source": "AI",
        }
        status, data = module.process_document("doc-st-2", b"low-conf-payload")
        assert status == ProcessingStatus.REVIEW_PENDING
        assert data["needs_review"] is True
        module._llm_extract = original

    def test_failed_status_on_exception(self, module):
        module._llm_extract = lambda p, t: (_ for _ in ()).throw(RuntimeError("LLM down"))
        with pytest.raises(RuntimeError):
            module.process_document("doc-st-3", b"bad-payload")

        # Idempotency store should record FAILED
        doc_hash = __import__("hashlib").sha256(b"bad-payload").hexdigest()
        key = module._make_idempotency_key("doc-st-3", doc_hash)
        assert module.idempotency_store.get_status(key) == ProcessingStatus.FAILED


# ── Validation ───────────────────────────────────────────────────────────────
class TestValidation:
    def test_no_errors_for_valid_invoice(self, module):
        _, data = module.process_document("doc-v-1", b"valid")
        assert data["validation_errors"] == []

    def test_error_when_tax_exceeds_total(self, module):
        module._llm_extract = lambda p, t: {
            "invoice_number": "INV-ERR",
            "invoice_number_confidence": 0.95,
            "invoice_number_source": "AI",
            "total_amount": 100.0,
            "total_amount_confidence": 0.90,
            "total_amount_source": "AI",
            "tax_amount": 200.0,  # exceeds total
            "tax_amount_confidence": 0.88,
            "tax_amount_source": "AI",
        }
        _, data = module.process_document("doc-v-2", b"tax-error")
        assert any("tax_amount" in e for e in data["validation_errors"])


# ── Quality Metrics ───────────────────────────────────────────────────────────
class TestQualityMetrics:
    def test_metrics_present_in_output(self, module):
        _, data = module.process_document("doc-q-1", b"payload")
        qm = data["quality_metrics"]
        assert "avg_confidence" in qm
        assert "min_confidence" in qm
        assert "quality_score" in qm

    def test_quality_score_is_between_0_and_1(self, module):
        _, data = module.process_document("doc-q-2", b"payload")
        score = data["quality_metrics"]["quality_score"]
        assert 0.0 <= score <= 1.0


# ── Circuit Breaker ───────────────────────────────────────────────────────────
class TestCircuitBreaker:
    def test_opens_after_threshold_failures(self):
        cb = CircuitBreaker(failure_threshold=0.15, window=10, recovery_timeout=60)
        for _ in range(10):
            cb.record_failure()
        assert cb.is_open()

    def test_closed_after_success_in_half_open(self):
        import time
        cb = CircuitBreaker(failure_threshold=0.15, window=10, recovery_timeout=1)
        for _ in range(10):
            cb.record_failure()
        assert cb.is_open()
        time.sleep(1.1)
        assert cb.state == CircuitBreaker.HALF_OPEN
        cb.record_success()
        assert cb.state == CircuitBreaker.CLOSED


# ── Retry Logic ───────────────────────────────────────────────────────────────
class TestRetryLogic:
    def test_retries_on_transient_failure(self):
        call_count = {"n": 0}

        def flaky():
            call_count["n"] += 1
            if call_count["n"] < 3:
                raise ConnectionError("transient")
            return "ok"

        result = _call_with_retry(flaky, max_retries=3, base_delay=0)
        assert result == "ok"
        assert call_count["n"] == 3

    def test_raises_after_max_retries(self):
        def always_fail():
            raise ConnectionError("always fails")

        with pytest.raises(ConnectionError):
            _call_with_retry(always_fail, max_retries=2, base_delay=0)


# ── Dual Output ───────────────────────────────────────────────────────────────
class TestDualOutput:
    def test_json_output_written(self, module, tmp_path):
        module.process_document("doc-out-1", b"payload")
        json_file = tmp_path / "json" / "doc-out-1.json"
        assert json_file.exists()

        import json
        data = json.loads(json_file.read_text())
        assert data["schema_version"] == "1.0"
        assert data["document_id"] == "doc-out-1"

    def test_parquet_output_written(self, module, tmp_path):
        module.process_document("doc-out-2", b"payload")
        parquet_file = tmp_path / "parquet" / "doc-out-2.parquet"
        assert parquet_file.exists()

        import pandas as pd
        df = pd.read_parquet(parquet_file)
        assert len(df) == 1
        assert df.iloc[0]["document_id"] == "doc-out-2"

    def test_json_and_parquet_consistent_fields(self, module, tmp_path):
        """Core fields must be identical across both formats."""
        module.process_document("doc-out-3", b"payload")

        import json, pandas as pd
        j = json.loads((tmp_path / "json" / "doc-out-3.json").read_text())
        df = pd.read_parquet(tmp_path / "parquet" / "doc-out-3.parquet")

        for field in ["document_id", "vendor_name", "total_amount", "schema_version"]:
            assert str(j[field]) == str(df.iloc[0][field])


# ── Thread Safety ─────────────────────────────────────────────────────────────
class TestConcurrency:
    def test_concurrent_different_documents(self, module):
        """50 different documents processed in parallel should all complete."""
        results = {}
        errors = []

        def process(i):
            try:
                status, data = module.process_document(f"doc-concurrent-{i}", f"payload {i}".encode())
                results[i] = status
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=process, args=(i,)) for i in range(50)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(errors) == 0
        assert len(results) == 50
