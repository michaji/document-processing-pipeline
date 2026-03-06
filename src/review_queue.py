import threading
from datetime import datetime
from typing import Dict, List, Optional
import uuid

class ReviewItem:
    def __init__(self, document_id: str, data: Dict, sla_deadline: datetime, priority: int = 50):
        self.id = str(uuid.uuid4())
        self.document_id = document_id
        self.data = data
        self.status = "PENDING"
        self.assigned_to = None
        self.sla_deadline = sla_deadline
        self.priority = priority
        self.created_at = datetime.now()
        self.completed_at = None
        self.audit_trail = []

class ReviewQueueManager:
    def __init__(self):
        self._queue: Dict[str, ReviewItem] = {}
        self._lock = threading.Lock()
        
    def add_item(self, item: ReviewItem):
        with self._lock:
            self._queue[item.id] = item
            
    def get_pending_items(self) -> List[ReviewItem]:
        with self._lock:
            pending = [item for item in self._queue.values() if item.status == "PENDING"]
            # Sort by SLA deadline, then priority
            return sorted(pending, key=lambda x: (x.sla_deadline, -x.priority))
            
    def claim_item(self, item_id: str, user_id: str) -> bool:
        """Atomic claim operation."""
        with self._lock:
            item = self._queue.get(item_id)
            if item and item.status == "PENDING":
                item.status = "CLAIMED"
                item.assigned_to = user_id
                item.audit_trail.append(f"Claimed by {user_id} at {datetime.now()}")
                return True
        return False
        
    def submit_review(self, item_id: str, user_id: str, action: str, corrections: Dict = None, reason: str = ""):
        with self._lock:
            item = self._queue.get(item_id)
            if not item or item.status != "CLAIMED" or item.assigned_to != user_id:
                raise ValueError("Invalid item state or assignment")
                
            item.status = action # APPROVE, CORRECT, REJECT
            item.completed_at = datetime.now()
            
            if corrections and action == "CORRECT":
                for k, v in corrections.items():
                    item.data[k] = v
                    item.data[f"{k}_source"] = "HUMAN"
                    
            item.audit_trail.append(f"Submitted {action} by {user_id} at {datetime.now()} reason: {reason}")
            return item

    def get_stats(self) -> Dict:
        with self._lock:
            now = datetime.now()
            completed = [i for i in self._queue.values() if i.completed_at]
            today_completed = [i for i in completed if i.completed_at.date() == now.date()]
            
            review_times = [(i.completed_at - i.created_at).total_seconds() for i in completed]
            avg_time = sum(review_times) / len(review_times) if review_times else 0
            
            pending = self.get_pending_items()
            breached = [i for i in pending if i.sla_deadline < now]
            
            return {
                "items_reviewed_today": len(today_completed),
                "average_review_time_seconds": avg_time,
                "queue_depth": len(pending),
                "sla_breached": len(breached)
            }
