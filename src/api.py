from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Dict, Any, List, Optional
from datetime import datetime, timezone, timedelta

from src.infrastructure import init_db
from src.monitoring import SLAMonitor
from src.review_queue import ReviewQueueManager

sla_monitor = SLAMonitor()
queue_manager = ReviewQueueManager()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize DB tables on startup, then yield."""
    init_db()
    yield


app = FastAPI(title="Document Processing API - Review Dashboard", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class ClaimRequest(BaseModel):
    user_id: str

class SubmitReviewRequest(BaseModel):
    user_id: str
    action: str  # COMPLETED, REJECTED
    corrections: Optional[Dict[str, Any]] = None
    reason: Optional[str] = None

@app.get("/api/queue")
def get_queue(limit: int = 50, offset: int = 0):
    items = queue_manager.get_pending_items(limit, offset)
    result = []
    
    for item in items:
        # Determine UI Priority
        priority_cat = "good"
        if item["priority"] >= 80:
            priority_cat = "urgent"
        elif item["priority"] >= 50:
            priority_cat = "warning"
            
        # Both are naive UTC datetimes — use UTC now to avoid timezone drift
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        sla_dt = datetime.fromisoformat(item["sla_deadline"].rstrip("Z"))
        hours_remaining = int((sla_dt - now).total_seconds() / 3600)
        
        # Build UI friendly fields array
        fields = []
        extracted = item["extracted_data"]
        for key, value in extracted.items():
            if not key.endswith("_confidence") and not key.endswith("_source"):
                conf_key = f"{key}_confidence"
                conf = extracted.get(conf_key, 1.0)
                fields.append({
                    "key": key.replace("_", " ").title(),
                    "raw_key": key,
                    "value": str(value) if value is not None else "",
                    "confidence": conf
                })
        
        result.append({
            "id": item["id"],
            "documentId": item["document_id"],
            "status": priority_cat,
            "slaHoursRemaining": max(0, hours_remaining),
            "priority": item["priority"],
            "fields": fields
        })
    return result

@app.post("/api/queue/{item_id}/claim")
def claim_item(item_id: str, req: ClaimRequest):
    success = queue_manager.claim_item(item_id, req.user_id)
    if not success:
        raise HTTPException(status_code=400, detail="Item already claimed or does not exist")
    return {"status": "success"}

@app.post("/api/queue/{item_id}/release")
def release_item(item_id: str, req: ClaimRequest):
    success = queue_manager.release_item(item_id, req.user_id)
    if not success:
        raise HTTPException(status_code=400, detail="Failed to release item")
    return {"status": "success"}

@app.post("/api/queue/{item_id}/submit")
def submit_review(item_id: str, req: SubmitReviewRequest):
    # Auto-claim if necessary to simplify UI constraints for demo purposes
    queue_manager.claim_item(item_id, req.user_id)
    
    success = queue_manager.submit_review(
        item_id=item_id,
        user_id=req.user_id,
        corrections=req.corrections or {},
        action=req.action,
        rejection_reason=req.reason
    )
    if not success:
        raise HTTPException(status_code=400, detail="Failed to submit review")
    return {"status": "success"}

@app.get("/api/stats")
def get_stats():
    stats = queue_manager.get_queue_stats()
    if stats:
        pending = stats.get("pending_items", 0)
        breached = stats.get("sla_breached", 0)
        sla_monitor.record_metrics({
            "review_queue_depth": pending,
            "sla_breach_percent": round(breached / max(pending, 1) * 100, 2),
        })
    return stats


@app.get("/api/health")
def get_health():
    """Returns SLA monitor system status and active alert count."""
    return sla_monitor.get_status()


@app.post("/api/seed")
def seed_demo_data(count: int = 10):
    """Seed the review queue with realistic sample documents for demo/testing."""
    import time as _time
    vendors = ["Acme Corp", "TechStart Inc", "Global Supply Co", "Premier Services", "Digital Solutions"]
    confidences = [0.75, 0.92, 0.65, 0.88, 0.71, 0.95, 0.58, 0.82, 0.99, 0.70]
    priorities = [90, 70, 50, 30, 80, 60, 40, 85, 55, 45]
    sla_hours_list = [2, 8, 24, 48, 4, 12, 36, 1, 6, 18]
    # Use a short epoch-based suffix so repeated seed calls always produce new doc IDs
    epoch_tag = str(int(_time.time()))[-6:]

    created_ids = []
    for i in range(count):
        doc_id = f"INV-{epoch_tag}-{i + 1:03d}"
        vendor_conf = confidences[i % len(confidences)]
        total_conf = confidences[(i + 3) % len(confidences)]
        extracted_data = {
            "invoice_number": doc_id,
            "invoice_number_confidence": 0.95,
            "invoice_number_source": "AI",
            "vendor_name": vendors[i % len(vendors)],
            "vendor_name_confidence": vendor_conf,
            "vendor_name_source": "AI",
            "invoice_date": "2026-03-01",
            "invoice_date_confidence": 0.92,
            "invoice_date_source": "AI",
            "total_amount": round(500 + i * 123.45, 2),
            "total_amount_confidence": total_conf,
            "total_amount_source": "AI",
            "tax_amount": round(50 + i * 12.34, 2),
            "tax_amount_confidence": 0.87,
            "tax_amount_source": "AI",
            "currency": "USD",
            "currency_confidence": 0.99,
            "currency_source": "AI",
        }
        item_id = queue_manager.add_to_queue(
            document_id=doc_id,
            extracted_data=extracted_data,
            priority=priorities[i % len(priorities)],
            sla_hours=sla_hours_list[i % len(sla_hours_list)],
        )
        if item_id:
            created_ids.append(item_id)

    return {"seeded": len(created_ids), "item_ids": created_ids}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("src.api:app", host="0.0.0.0", port=8000, reload=True)
