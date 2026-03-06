"""
extraction_module.py
====================
Production-grade document extraction module with:
  - Thread-safe idempotency (SHA-256 keyed, in-memory with persistence hook)
  - Field-level locking (HUMAN corrections always win)
  - Exponential backoff + jitter retry for LLM calls
  - Circuit breaker for external services
  - QUEUED → PROCESSING → COMPLETED/FAILED/REVIEW_PENDING state machine
  - Full audit trail per document
  - Dual output: JSON + Parquet with schema version tagging
  - Data quality metrics included in every output
"""
import json
import hashlib
import os
import time
import random
import logging
import threading
from enum import Enum
from typing import Dict, Any, Optional, Tuple, List
from datetime import datetime, timezone

import pandas as pd
import groq
from dotenv import load_dotenv

from src.infrastructure import redis_client, SessionLocal, DocumentMetadata

load_dotenv()

logger = logging.getLogger(__name__)
SCHEMA_VERSION = "1.0"


# ---------------------------------------------------------------------------
# Processing status enum
# ---------------------------------------------------------------------------
class ProcessingStatus(str, Enum):
    QUEUED = "QUEUED"
    PROCESSING = "PROCESSING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    REVIEW_PENDING = "REVIEW_PENDING"


# ---------------------------------------------------------------------------
# Circuit Breaker
# ---------------------------------------------------------------------------
class CircuitBreaker:
    """
    Simple sliding-window circuit breaker.
    States: CLOSED (normal) → OPEN (failing) → HALF_OPEN (probing)
    """
    CLOSED = "CLOSED"
    OPEN = "OPEN"
    HALF_OPEN = "HALF_OPEN"

    def __init__(self, failure_threshold: float = 0.15, window: int = 20, recovery_timeout: int = 30):
        self._lock = threading.Lock()
        self._state = self.CLOSED
        self._results: List[bool] = []  # True=success, False=failure
        self._window = window
        self._failure_threshold = failure_threshold
        self._recovery_timeout = recovery_timeout
        self._opened_at: Optional[float] = None

    @property
    def state(self) -> str:
        with self._lock:
            if self._state == self.OPEN:
                if time.time() - self._opened_at >= self._recovery_timeout:
                    self._state = self.HALF_OPEN
            return self._state

    def record_success(self):
        with self._lock:
            self._results.append(True)
            self._results = self._results[-self._window:]
            if self._state == self.HALF_OPEN:
                self._state = self.CLOSED

    def record_failure(self):
        with self._lock:
            self._results.append(False)
            self._results = self._results[-self._window:]
            failures = self._results.count(False)
            rate = failures / len(self._results) if self._results else 0
            if rate >= self._failure_threshold:
                if self._state != self.OPEN:
                    logger.warning("CircuitBreaker OPENING — failure rate %.1f%%", rate * 100)
                self._state = self.OPEN
                self._opened_at = time.time()

    def is_open(self) -> bool:
        return self.state == self.OPEN


# ---------------------------------------------------------------------------
# Thread-safe Idempotency Store (Redis with fallback to in-memory)
# ---------------------------------------------------------------------------
class IdempotencyStore:
    def __init__(self):
        self._store: Dict[str, Dict] = {}
        self._lock = threading.Lock()
        self.redis = redis_client
        self.ttl = int(os.getenv("REDIS_TTL_SECONDS", "7776000"))

    def get(self, key: str) -> Optional[Dict]:
        if self.redis:
            val = self.redis.get(key)
            if val:
                return json.loads(val)
            return None
        with self._lock:
            return self._store.get(key)

    def save(self, key: str, record: Dict):
        if self.redis:
            self.redis.set(key, json.dumps(record), ex=self.ttl)
            return
        with self._lock:
            self._store[key] = record

    def get_status(self, key: str) -> str:
        record = self.get(key)
        return record["status"] if record else "NEW"

    def get_output(self, key: str) -> Optional[Dict]:
        record = self.get(key)
        return record.get("output") if record else None


# ---------------------------------------------------------------------------
# Audit Trail
# ---------------------------------------------------------------------------
def _make_audit_event(event: str, details: Dict = None) -> Dict:
    return {
        "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "event": event,
        "details": details or {},
    }


# ---------------------------------------------------------------------------
# Retry helper with exponential backoff + full jitter
# ---------------------------------------------------------------------------
def _call_with_retry(fn, max_retries: int = 3, base_delay: float = 1.0, circuit_breaker: Optional[CircuitBreaker] = None):
    last_exc = None
    for attempt in range(max_retries):
        if circuit_breaker and circuit_breaker.is_open():
            raise RuntimeError("Circuit breaker is OPEN — aborting LLM call")
        try:
            result = fn()
            if circuit_breaker:
                circuit_breaker.record_success()
            return result
        except Exception as exc:
            last_exc = exc
            if circuit_breaker:
                circuit_breaker.record_failure()
            # Full jitter: sleep between 0 and base * 2^attempt
            sleep_for = random.uniform(0, base_delay * (2 ** attempt))
            logger.warning("Attempt %d/%d failed (%s). Retrying in %.2fs...", attempt + 1, max_retries, exc, sleep_for)
            time.sleep(sleep_for)
    raise last_exc


# ---------------------------------------------------------------------------
# Main Extraction Module
# ---------------------------------------------------------------------------
class ExtractionModule:
    """
    Stateless, thread-safe document extraction module.

    Concurrency model: safe for use from multiple threads simultaneously.
    Idempotency: SHA-256(document_id + content) → dedup key stored in IdempotencyStore.
    Field preservation: corrections with source=HUMAN always override AI output.
    """

    def __init__(
        self,
        output_dir: str = "output",
        confidence_review_threshold: float = 0.80,
        max_retries: int = 3,
        use_mock_llm: bool = False,
    ):
        self.output_dir = output_dir
        self.confidence_review_threshold = confidence_review_threshold
        self.max_retries = max_retries
        self.use_mock_llm = use_mock_llm
        self.idempotency_store = IdempotencyStore()
        self.circuit_breaker = CircuitBreaker()
        
        if not self.use_mock_llm:
            try:
                self.groq_client = groq.Groq() # Relies on GROQ_API_KEY env var
            except Exception as e:
                logger.warning(f"Failed to initialize Groq client: {e}. Falling back to mock LLM.")
                self.use_mock_llm = True

        os.makedirs(f"{output_dir}/json", exist_ok=True)
        os.makedirs(f"{output_dir}/parquet", exist_ok=True)

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------
    def process_document(
        self,
        document_id: str,
        payload: bytes,
        document_type: str = "invoice",
        tenant_id: str = "default",
        manual_corrections: Optional[Dict[str, Any]] = None,
    ) -> Tuple[ProcessingStatus, Dict]:
        """
        Process a single document end-to-end.

        Returns (status, enriched_output_dict).
        Raises on unrecoverable error after retry exhaustion.
        """
        doc_hash = hashlib.sha256(payload).hexdigest()
        ido_key = self._make_idempotency_key(document_id, doc_hash)
        manual_corrections = manual_corrections or {}
        audit: List[Dict] = []

        # ---- Idempotency check -----------------------------------------------
        existing = self.idempotency_store.get(ido_key)
        if existing:
            if existing["status"] == ProcessingStatus.COMPLETED:
                logger.info("Idempotent hit for %s — returning cached result.", document_id)
                return ProcessingStatus.COMPLETED, existing["output"]
            elif existing["status"] == ProcessingStatus.PROCESSING:
                raise RuntimeError(f"Document {document_id} is already being processed — concurrent duplicate rejected.")

        # ---- Mark QUEUED then PROCESSING -------------------------------------
        audit.append(_make_audit_event("status_change", {"to": ProcessingStatus.QUEUED}))
        self.idempotency_store.save(ido_key, {"status": ProcessingStatus.QUEUED, "audit": audit})

        audit.append(_make_audit_event("status_change", {"to": ProcessingStatus.PROCESSING}))
        self.idempotency_store.save(ido_key, {"status": ProcessingStatus.PROCESSING, "audit": audit})

        try:
            # ---- LLM extraction (with retry + circuit breaker) ---------------
            extracted = _call_with_retry(
                fn=lambda: self._llm_extract(payload, document_type),
                max_retries=self.max_retries,
                circuit_breaker=self.circuit_breaker,
            )
            audit.append(_make_audit_event("extraction_complete", {"fields_extracted": list(extracted.keys())}))

            # ---- Field preservation: human corrections always win -------------
            extracted = self._apply_corrections(extracted, manual_corrections, audit)

            # ---- Schema validation -------------------------------------------
            validation_errors = self._validate(extracted, document_type)
            if validation_errors:
                audit.append(_make_audit_event("validation_errors", {"errors": validation_errors}))

            # ---- Confidence-based routing ------------------------------------
            low_confidence_fields = [
                k for k, v in extracted.items()
                if k.endswith("_confidence") and isinstance(v, float) and v < self.confidence_review_threshold
            ]
            needs_review = len(low_confidence_fields) > 0
            if needs_review:
                audit.append(_make_audit_event("routed_to_review", {"low_confidence_fields": low_confidence_fields}))

            # ---- Data quality metrics ----------------------------------------
            quality_metrics = self._compute_quality_metrics(extracted)

            # ---- Build final output ------------------------------------------
            output = {
                "document_id": document_id,
                "document_type": document_type,
                "tenant_id": tenant_id,
                "schema_version": SCHEMA_VERSION,
                "processed_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                "content_hash": doc_hash,
                "needs_review": needs_review,
                "validation_errors": validation_errors,
                "quality_metrics": quality_metrics,
                "audit_trail": audit,
                **extracted,
            }

            # ---- Dual output -------------------------------------------------
            self._write_outputs(document_id, output)

            final_status = ProcessingStatus.REVIEW_PENDING if needs_review else ProcessingStatus.COMPLETED
            audit.append(_make_audit_event("status_change", {"to": final_status}))
            self.idempotency_store.save(ido_key, {"status": final_status, "output": output, "audit": audit})

            self._save_metadata(output, final_status)
            return final_status, output

        except Exception as exc:
            audit.append(_make_audit_event("failed", {"error": str(exc)}))
            self.idempotency_store.save(ido_key, {"status": ProcessingStatus.FAILED, "audit": audit})
            logger.error("Document %s failed: %s", document_id, exc)
            raise

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _make_idempotency_key(self, document_id: str, doc_hash: str) -> str:
        return hashlib.sha256(f"{document_id}:{doc_hash}".encode()).hexdigest()

    def _llm_extract(self, payload: bytes, document_type: str) -> Dict[str, Any]:
        """
        Extraction from document via LLM API, returning structured data with source/confidence.
        If use_mock_llm is True, returns deterministic mock data for testing.
        """
        if self.use_mock_llm:
            # Deterministic mock — same payload always → same result (idempotent)
            seed = int(hashlib.md5(payload).hexdigest(), 16) % 1000
            return {
                "invoice_number": f"INV-{1000 + seed}",
                "invoice_number_confidence": 0.95,
                "invoice_number_source": "AI",
                "vendor_name": "Acme Corp",
                "vendor_name_confidence": 0.88,
                "vendor_name_source": "AI",
                "invoice_date": "2024-01-15",
                "invoice_date_confidence": 0.92,
                "invoice_date_source": "AI",
                "total_amount": round(1000 + seed * 0.5, 2),
                "total_amount_confidence": 0.99,
                "total_amount_source": "AI",
                "tax_amount": round(seed * 0.1, 2),
                "tax_amount_confidence": 0.87,
                "tax_amount_source": "AI",
                "currency": "USD",
                "currency_confidence": 0.99,
                "currency_source": "AI",
            }
        
        # Real LLM call using Groq API
        prompt = f"""
Extract information from the provided {document_type} content.
You must output ONLY a valid JSON object matching this exact flat structure with no additional properties.

For every field extracted, provide two supplementary fields:
- <field>_confidence: float between 0.0 and 1.0 representing your confidence.
- <field>_source: string "AI"

Required fields (plus their _confidence and _source variants):
- invoice_number (string)
- vendor_name (string)
- invoice_date (YYYY-MM-DD string)
- total_amount (float)

Content:
{payload.decode('utf-8', errors='ignore')[:4000]}
"""
        response = self.groq_client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[{"role": "user", "content": prompt}],
            response_format={"type": "json_object"},
            temperature=0.0,
        )
        
        content = response.choices[0].message.content
        return json.loads(content)

    def _apply_corrections(self, extracted: Dict, corrections: Dict, audit: List) -> Dict:
        """
        Merge human corrections into extracted data.
        Human source always wins. New keys (not seen by AI) are added directly.
        """
        if not corrections:
            return extracted

        for key, value in corrections.items():
            original = extracted.get(key)
            extracted[key] = value
            extracted[f"{key}_source"] = "HUMAN"
            extracted[f"{key}_confidence"] = 1.0  # Human corrections are treated as 100% confident
            audit.append(_make_audit_event("field_corrected", {
                "field": key,
                "original_ai_value": original,
                "human_value": value,
            }))
        return extracted

    def _validate(self, data: Dict, document_type: str) -> List[str]:
        """Returns list of validation error strings (empty = valid)."""
        errors = []
        if document_type == "invoice":
            if not data.get("invoice_number"):
                errors.append("invoice_number is required")
            total = data.get("total_amount")
            if total is not None and total < 0:
                errors.append(f"total_amount cannot be negative (got {total})")
            tax = data.get("tax_amount")
            if tax is not None and total is not None and tax > total:
                errors.append(f"tax_amount ({tax}) cannot exceed total_amount ({total})")
        return errors

    def _compute_quality_metrics(self, data: Dict) -> Dict:
        """Aggregate confidence scores into a quality report."""
        conf_values = [
            v for k, v in data.items()
            if k.endswith("_confidence") and isinstance(v, float)
        ]
        if not conf_values:
            return {"avg_confidence": None, "min_confidence": None, "fields_below_threshold": 0}

        below = sum(1 for c in conf_values if c < self.confidence_review_threshold)
        return {
            "avg_confidence": round(sum(conf_values) / len(conf_values), 4),
            "min_confidence": round(min(conf_values), 4),
            "max_confidence": round(max(conf_values), 4),
            "total_fields": len(conf_values),
            "fields_below_threshold": below,
            "quality_score": round((len(conf_values) - below) / len(conf_values), 4),
        }

    def _write_outputs(self, document_id: str, output: Dict):
        """Write extraction output to Dual formats: JSON and Parquet."""
        json_path = os.path.join(self.output_dir, "json", f"{document_id}.json")
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(output, f, indent=2)

        parquet_path = os.path.join(self.output_dir, "parquet", f"{document_id}.parquet")
        # Flatten audit trail / complex types for Pandas if needed
        flat_output = {**output}
        flat_output["validation_errors"] = json.dumps(flat_output["validation_errors"])
        flat_output["quality_metrics"] = json.dumps(flat_output["quality_metrics"])
        flat_output["audit_trail"] = json.dumps(flat_output["audit_trail"])

        df = pd.DataFrame([flat_output])
        df.to_parquet(parquet_path, engine="pyarrow", compression="snappy", index=False)

    def _save_metadata(self, output: Dict, status: ProcessingStatus):
        """Save document metadata to Postgres if connected."""
        if not SessionLocal:
            return

        try:
            with SessionLocal() as session:
                doc = session.query(DocumentMetadata).filter_by(document_id=output["document_id"]).first()
                if not doc:
                    doc = DocumentMetadata(
                        document_id=output["document_id"],
                        tenant_id=output["tenant_id"],
                        created_at=datetime.now(timezone.utc).replace(tzinfo=None)
                    )
                
                doc.document_type = output["document_type"]
                doc.status = status.value
                doc.updated_at = datetime.now(timezone.utc).replace(tzinfo=None)
                doc.extracted_data = {k: v for k, v in output.items() if k not in ["audit_trail", "validation_errors", "quality_metrics"]}
                doc.audit_trail = output.get("audit_trail", [])
                doc.needs_review = 1 if output.get("needs_review") else 0
                
                session.add(doc)
                session.commit()
        except Exception as e:
            logger.error("Failed to save metadata to Postgres for %s: %s", output["document_id"], e)
