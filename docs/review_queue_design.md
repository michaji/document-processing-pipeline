# Review Queue System Design

## 1. Data Model
The Review Queue system relies on the `ReviewItem` and `DocumentMetadata` tables to track items requiring human intervention.

### Review Item Structure (`ReviewItem` Table)
* `id` (String): Unique identifier for the queue item (e.g., UUID-based).
* `document_id` (String): Foreign key to the processed document.
* `extracted_data` (JSON): The current snapshot of extracted fields and confidence scores.
* `status` (String): Lifecycle state of the review item:
    * `PENDING`: Waiting in the queue.
    * `CLAIMED`: Currently being reviewed by a human operator.
    * `COMPLETED`: Review finished, data corrected/approved.
    * `REJECTED`: Document cannot be processed or is invalid.
* `assigned_to` (String): ID or username of the human operator who claimed the item.
* `priority` (Integer): Determines sorting order. Higher values indicate higher priority.
* `sla_deadline` (DateTime): The exact time by which the review must be completed.
* `created_at` (DateTime): When the item entered the review queue.
* `updated_at` (DateTime): Last modification timestamp.

## 2. Queue Operations

### Priority Calculation & SLA-aware Ordering
The queue is organized using a combination of calculated `priority` and `sla_deadline`.
When fetching pending items, the primary sort is `priority DESC` (urgent items first), and the secondary sort is `sla_deadline ASC` (items closest to breaching SLA are bumped ahead of items with more breathing room).

### Load-Balanced Assignment & Atomic Claim
To prevent multiple reviewers from simultaneously opening the same document, claiming an item uses an atomic database operation:
```sql
UPDATE review_queue 
SET status = 'CLAIMED', assigned_to = :user_id, updated_at = NOW() 
WHERE id = :item_id AND status = 'PENDING'
```
If the rows affected is `0`, another user has already claimed the document (optimistic concurrency control).

## 3. API Design
The REST API allows the dashboard to consume queue data securely.

* `GET /api/reviews/queue` -> List pending items. Supports `?page=X`, `?limit=Y`, `?sort=priority|sla`.
* `POST /api/reviews/{id}/claim` -> Atomically assigns the item to the current user (moves to `CLAIMED` status).
* `POST /api/reviews/{id}/release` -> Releases the item back to `PENDING` if the user closes it without reviewing.
* `POST /api/reviews/{id}/submit` -> Submits human corrections, updates `extracted_data`, marks as `COMPLETED`.
* `POST /api/reviews/{id}/reject` -> Marks document unprocessable with a rejection reason.
* `GET /api/reviews/stats` -> Returns total items pending, SLA breach stats, user performance metrics.

## 4. Feedback Loop
Corrections made in the Review UI are fed back into the pipeline:
1. **Audit Trail Update**: The original extraction output is amended, injecting human modifications with `source: HUMAN` and `confidence: 1.0`.
2. **Metadata Sync**: The `document_metadata` table is synced with the finalized human-corrected JSON payload.
3. **Training Data Corpus**: All `COMPLETED` reviews with deltas (fields flagged as `HUMAN` that diverge from the `AI` extract) are indexed using a background listener to systematically fine-tune or few-shot prompt subsequent generations.
