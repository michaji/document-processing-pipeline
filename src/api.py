import uvicorn
from fastapi import FastAPI, HTTPException, Body
from pydantic import BaseModel
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta

from src.review_queue import ReviewQueueManager, ReviewItem
from src.extraction_module import ExtractionModule
from src.monitoring import SLAMonitor

app = FastAPI(title="Document Processing API")

queue_manager = ReviewQueueManager()
extraction_module = ExtractionModule()
sla_monitor = SLAMonitor()

# Seed the queue with some mock items for testing the UI
mock_item_1 = ReviewItem("INV-001", {
    "invoice_number": "INV-1001",
    "invoice_number_confidence": 0.98,
    "vendor_name": "Acme Corp",
    "vendor_name_confidence": 0.67,
    "total_amount": 1234.56,
    "total_amount_confidence": 0.72
}, datetime.now() + timedelta(hours=2), priority=90)

mock_item_2 = ReviewItem("INV-002", {
    "invoice_number": "XYZ-999",
    "invoice_number_confidence": 0.95,
    "vendor_name": "TechFlow Ltd",
    "vendor_name_confidence": 0.89,
    "total_amount": 450.00,
    "total_amount_confidence": 0.40
}, datetime.now() + timedelta(hours=4), priority=60)

queue_manager.add_item(mock_item_1)
queue_manager.add_item(mock_item_2)

class ClaimRequest(BaseModel):
    user_id: str

class SubmitReviewRequest(BaseModel):
    user_id: str
    action: str  # APPROVE, CORRECT, REJECT
    corrections: Optional[Dict[str, Any]] = None
    reason: Optional[str] = ""

@app.get("/api/queue")
def get_queue():
    items = queue_manager.get_pending_items()
    result = []
    for item in items:
        # Determine priority category for UI
        priority_cat = "good"
        if item.priority >= 80:
            priority_cat = "critical"
        elif item.priority >= 50:
            priority_cat = "warn"
            
        fields = []
        for key, value in item.data.items():
            if not key.endswith("_confidence") and not key.endswith("_source"):
                conf_key = f"{key}_confidence"
                conf = item.data.get(conf_key, 1.0)
                fields.append({
                    "key": key,
                    "label": key.replace("_", " ").title(),
                    "value": str(value),
                    "confidence": conf,
                    "required": True
                })
        
        result.append({
            "id": item.document_id,
            "queue_id": item.id,
            "documentType": "Invoice",
            "sla_hours": max(0, int((item.sla_deadline - datetime.now()).total_seconds() / 3600)),
            "priority": priority_cat,
            "previewImage": "https://images.unsplash.com/photo-1586281380349-632531db7ed4?w=800&q=80",
            "fields": fields
        })
    return result

@app.post("/api/queue/{item_id}/claim")
def claim_item(item_id: str, req: ClaimRequest):
    success = queue_manager.claim_item(item_id, req.user_id)
    if not success:
        raise HTTPException(status_code=400, detail="Item cannot be claimed")
    return {"status": "success"}

@app.post("/api/queue/{item_id}/submit")
def submit_review(item_id: str, req: SubmitReviewRequest):
    try:
        # For simplicity in testing, we might not have explicitly claimed it in the UI mock, 
        # so let's force claim it if it's pending.
        item = queue_manager._queue.get(item_id)
        if item and item.status == "PENDING":
            queue_manager.claim_item(item_id, req.user_id)
            
        result = queue_manager.submit_review(
            item_id=item_id,
            user_id=req.user_id,
            action=req.action,
            corrections=req.corrections,
            reason=req.reason
        )
        return {"status": "success", "completed_at": result.completed_at}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/api/stats")
def get_stats():
    return queue_manager.get_stats()

if __name__ == "__main__":
    uvicorn.run("src.api:app", host="0.0.0.0", port=8000, reload=True)
