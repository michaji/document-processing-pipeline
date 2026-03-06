"""
review_queue.py
Implements the Review Queue Manager with atomic claims, correction tracking, and SLA monitoring.
"""

import uuid
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List, Optional
from sqlalchemy import update, and_, desc, asc, func
from sqlalchemy.orm import Session
from src.infrastructure import ReviewItem, DocumentMetadata, SessionLocal

logger = logging.getLogger(__name__)

class ReviewQueueManager:
    """Manages the lifecycle of documents requiring human human intervention."""

    def __init__(self):
        self._session_maker = SessionLocal
        
    def _get_utc_now(self):
        return datetime.now(timezone.utc).replace(tzinfo=None)

    def add_to_queue(self, document_id: str, extracted_data: Dict[str, Any], priority: int = 50, sla_hours: int = 24) -> Optional[str]:
        """Adds a document output to the review queue."""
        if not self._session_maker:
            logger.warning("No DB session available. Review queue is disabled.")
            return None

        with self._session_maker() as session:
            try:
                # Check if it already exists
                existing = session.query(ReviewItem).filter_by(document_id=document_id).first()
                if existing:
                    return existing.id

                item_id = str(uuid.uuid4())
                now = self._get_utc_now()
                sla_deadline = now + timedelta(hours=sla_hours)
                
                new_item = ReviewItem(
                    id=item_id,
                    document_id=document_id,
                    extracted_data=extracted_data,
                    status="PENDING",
                    priority=priority,
                    sla_deadline=sla_deadline,
                    created_at=now,
                    updated_at=now
                )
                session.add(new_item)
                session.commit()
                return item_id
            except Exception as e:
                logger.error(f"Failed to add to queue: {e}")
                session.rollback()
                return None

    def get_pending_items(self, limit: int = 50, offset: int = 0) -> List[Dict[str, Any]]:
        """Fetch pending items sorted by priority (DESC) and SLA deadline (ASC)."""
        if not self._session_maker:
            return []
            
        with self._session_maker() as session:
            try:
                items = session.query(ReviewItem).filter_by(status="PENDING") \
                    .order_by(desc(ReviewItem.priority), asc(ReviewItem.sla_deadline)) \
                    .offset(offset).limit(limit).all()
                    
                return [{
                    "id": item.id,
                    "document_id": item.document_id,
                    "status": item.status,
                    "priority": item.priority,
                    "sla_deadline": item.sla_deadline.isoformat() + "Z",
                    "created_at": item.created_at.isoformat() + "Z",
                    "extracted_data": item.extracted_data
                } for item in items]
            except Exception as e:
                logger.error(f"Failed to fetch pending items: {e}")
                return []

    def claim_item(self, item_id: str, user_id: str) -> bool:
        """Atomically claim a review item. Returns True if successfully claimed."""
        if not self._session_maker:
            return False

        with self._session_maker() as session:
            try:
                stmt = (
                    update(ReviewItem)
                    .where(and_(ReviewItem.id == item_id, ReviewItem.status == "PENDING"))
                    .values(
                        status="CLAIMED",
                        assigned_to=user_id,
                        updated_at=self._get_utc_now()
                    )
                )
                result = session.execute(stmt)
                session.commit()
                return result.rowcount > 0
            except Exception as e:
                logger.error(f"Failed to claim item {item_id}: {e}")
                session.rollback()
                return False

    def release_item(self, item_id: str, user_id: str) -> bool:
        """Atomically release a claimed item back to the queue."""
        if not self._session_maker:
            return False

        with self._session_maker() as session:
            try:
                stmt = (
                    update(ReviewItem)
                    .where(and_(ReviewItem.id == item_id, ReviewItem.status == "CLAIMED", ReviewItem.assigned_to == user_id))
                    .values(
                        status="PENDING",
                        assigned_to=None,
                        updated_at=self._get_utc_now()
                    )
                )
                result = session.execute(stmt)
                session.commit()
                return result.rowcount > 0
            except Exception as e:
                logger.error(f"Failed to release item {item_id}: {e}")
                session.rollback()
                return False

    def submit_review(self, item_id: str, user_id: str, corrections: Dict[str, Any], action: str = "COMPLETED", rejection_reason: Optional[str] = None) -> bool:
        """
        Record the final review, updating the item to COMPLETED or REJECTED.
        Also propagates the corrections back into the DocumentMetadata.
        """
        if not self._session_maker:
            return False

        if action not in ["COMPLETED", "REJECTED"]:
            logger.error("Invalid review action. Must be COMPLETED or REJECTED.")
            return False

        with self._session_maker() as session:
            try:
                item = session.query(ReviewItem).filter_by(id=item_id, status="CLAIMED", assigned_to=user_id).first()
                if not item:
                    return False
                    
                doc_meta = session.query(DocumentMetadata).filter_by(document_id=item.document_id).first()

                now = self._get_utc_now()
                
                # Apply corrections to the extracted payload
                new_data = dict(item.extracted_data)
                audit_trail = list(doc_meta.audit_trail) if doc_meta and doc_meta.audit_trail else []
                
                for field, human_value in corrections.items():
                    original_val = new_data.get(field)
                    new_data[field] = human_value
                    new_data[f"{field}_source"] = "HUMAN"
                    new_data[f"{field}_confidence"] = 1.0
                    
                    audit_trail.append({
                        "timestamp": now.isoformat() + "Z",
                        "event": "field_corrected_in_review",
                        "details": {
                            "field": field,
                            "original_value": original_val,
                            "human_value": human_value,
                            "reviewer": user_id
                        }
                    })

                if rejection_reason:
                    audit_trail.append({
                        "timestamp": now.isoformat() + "Z",
                        "event": "document_rejected",
                        "details": {
                            "reason": rejection_reason,
                            "reviewer": user_id
                        }
                    })

                # Update the Review Queue Tracker
                item.status = action
                item.extracted_data = new_data
                item.updated_at = now

                # Sync corrections up to the final Document Table
                if doc_meta:
                    doc_meta.extracted_data = new_data
                    doc_meta.audit_trail = audit_trail
                    doc_meta.needs_review = 0 # Resolve review
                    doc_meta.status = action
                    doc_meta.updated_at = now

                session.commit()
                return True
            except Exception as e:
                logger.error(f"Failed to submit review for item {item_id}: {e}")
                session.rollback()
                return False

    def get_queue_stats(self) -> Dict[str, Any]:
        """Returns SLA, queue depth, and metrics for the dashboard."""
        if not self._session_maker:
            return {}

        with self._session_maker() as session:
            try:
                total_pending = session.query(func.count(ReviewItem.id)).filter_by(status="PENDING").scalar()
                
                now = self._get_utc_now()
                sla_breached = session.query(func.count(ReviewItem.id)).filter(
                    and_(ReviewItem.status == "PENDING", ReviewItem.sla_deadline < now)
                ).scalar()

                # Get completions from today
                today_start = datetime(now.year, now.month, now.day)
                reviewed_today = session.query(func.count(ReviewItem.id)).filter(
                    and_(ReviewItem.status.in_(["COMPLETED", "REJECTED"]), ReviewItem.updated_at >= today_start)
                ).scalar()
                
                # Calculate avg duration for items completed today
                completed_items = session.query(ReviewItem).filter(
                    and_(ReviewItem.status == "COMPLETED", ReviewItem.updated_at >= today_start)
                ).all()
                
                total_duration = 0
                for item in completed_items:
                    # simplistic approximation (creation to update)
                    diff = (item.updated_at - item.created_at).total_seconds()
                    total_duration += diff
                    
                avg_time = int(total_duration / len(completed_items)) if completed_items else 0

                return {
                    "pending_items": total_pending,
                    "sla_breached": sla_breached,
                    "reviewed_today": reviewed_today,
                    "avg_review_time_seconds": avg_time
                }
            except Exception as e:
                logger.error(f"Failed to fetch review stats: {e}")
                return {}
