from fastapi import FastAPI, HTTPException, Body
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta

from src.review_queue import ReviewQueueManager

app = FastAPI(title="Document Processing API - Review Dashboard")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

queue_manager = ReviewQueueManager()

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
            
        now = datetime.now()
        # Parse ISO SLA
        sla_dt = datetime.fromisoformat(item["sla_deadline"].replace("Z", "+00:00")).replace(tzinfo=None)
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
    return queue_manager.get_queue_stats()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("src.api:app", host="0.0.0.0", port=8000, reload=True)
